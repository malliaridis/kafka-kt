/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.timeline

import java.util.NoSuchElementException

/**
 * `SnapshottableHashTable` implements a hash table that supports creating point-in-time snapshots. Each snapshot
 * is immutable once it is created; the past cannot be changed. We handle divergences between the current state
 * and historical state by copying a reference to elements that have been deleted or overwritten into the most
 * recent snapshot tier.
 *
 * Note that there are no keys in `SnapshottableHashTable`, only values. So it more similar to a hash set
 * than a hash map. The subclasses implement full-featured maps and sets using this class as a building block.
 *
 * Each snapshot tier contains a size and a hash table. The size reflects the size at the time the snapshot was taken.
 * Note that, as an optimization, snapshot tiers will be `null` if they don't contain anything. So for example,
 * if snapshot 20 of Object O contains the same entries as snapshot 10 of that object, the snapshot 20 tier for
 * object O will be `null`.
 *
 * The current tier's data is stored in the fields inherited from [BaseHashTable]. It would be conceptually simpler
 * to have a separate `BaseHashTable` object, but since Java doesn't have value types, subclassing is the only way
 * to avoid another pointer indirection and the associated extra memory cost.
 *
 * Note that each element in the hash table contains a start epoch, and a value. The start epoch is there
 * to identify when the object was first inserted. This in turn determines which snapshots it is a member of.
 *
 * In order to retrieve an object from snapshot `E`, we start by checking to see if the object exists in
 * the "current" hash tier. If it does, and its `startEpoch` extends back to `E`, we return that object.
 * Otherwise, we check all the snapshot tiers, starting with `E`, and ending with the most recent snapshot,
 * to see if the object is there. As an optimization, if we encounter the object in a snapshot tier but its epoch
 * is too new, we know that its value at epoch `E` must be `null`, so we can return that immediately.
 *
 * The class hierarchy looks like this:
 *
 * ```
 * Revertable       BaseHashTable
 * ↑              ↑
 * SnapshottableHashTable → SnapshotRegistry → Snapshot
 * ↑             ↑
 * TimelineHashSet       TimelineHashMap
 * ```
 *
 * `BaseHashTable` is a simple hash table that uses separate chaining. The interface is pretty bare-bones
 * since this class is not intended to be used directly by end-users.
 *
 * This class, [SnapshottableHashTable], has the logic for snapshotting and iterating over snapshots.
 * This is the core of the snapshotted hash table code and handles the tiering.
 *
 * TimelineHashSet and TimelineHashMap are mostly wrappers around this `SnapshottableHashTable` class.
 * They implement standard Java APIs for Set and Map, respectively. There's a fair amount of boilerplate for this,
 * but it's necessary so that timeline data structures can be used while writing idiomatic Java code.
 * The accessor APIs have two versions -- one that looks at the current state, and one that looks at a historical
 * snapshotted state. Mutation APIs only ever mutate the current state.
 *
 * One very important feature of `SnapshottableHashTable` is that we support iterating over a snapshot even while
 * changes are being made to the current state. See the Javadoc for the iterator for more information about
 * how this is accomplished.
 *
 * All of these classes require external synchronization, and don't support `null` keys or values.
 */
open class SnapshottableHashTable<T : SnapshottableHashTable.ElementWithStartEpoch?>(
    private val snapshotRegistry: SnapshotRegistry,
    expectedSize: Int,
) : BaseHashTable<T>(expectedSize), Revertable {

    init {
        snapshotRegistry.register(this)
    }

    fun snapshottableSize(epoch: Long): Int {
        return if (epoch == LATEST_EPOCH) baseSize()
        else {
            val iterator = snapshotRegistry.iterator(epoch)
            while (iterator.hasNext()) {
                val snapshot = iterator.next()
                val tier: HashTier<T>? = snapshot.getDelta(this@SnapshottableHashTable)

                if (tier != null) return tier.size
            }
            baseSize()
        }
    }

    fun snapshottableGet(key: Any, epoch: Long): T? {
        var result: T? = baseGet(key)
        if (result != null && result.startEpoch() <= epoch) return result
        if (epoch == LATEST_EPOCH) return null

        val iterator = snapshotRegistry.iterator(epoch)
        while (iterator.hasNext()) {
            val snapshot = iterator.next()
            val tier: HashTier<T>? = snapshot.getDelta(this@SnapshottableHashTable)
            tier?.deltaTable?.let { deltaTable ->
                result = deltaTable.baseGet(key)

                if (result != null) {
                    return if (result!!.startEpoch() <= epoch) result
                    else null
                }
            }
        }
        return null
    }

    fun snapshottableAddUnlessPresent(obj: T): Boolean {
        val prev = baseGet(obj as Any)
        if (prev != null) return false

        obj.setStartEpoch(snapshotRegistry.latestEpoch() + 1)
        val prevSize = baseSize()
        baseAddOrReplace(obj)
        updateTierData(prevSize)

        return true
    }

    fun snapshottableAddOrReplace(obj: T): T? {
        obj!!.setStartEpoch(snapshotRegistry.latestEpoch() + 1)
        val prevSize = baseSize()
        val prev = baseAddOrReplace(obj)

        if (prev == null) updateTierData(prevSize)
        else updateTierData(prev, prevSize)

        return prev
    }

    fun snapshottableRemove(obj: Any): T? {
        val prev = baseRemove(obj)

        return if (prev == null) null
        else {
            updateTierData(prev, baseSize() + 1)
            prev
        }
    }

    private fun updateTierData(prevSize: Int) {
        val iterator = snapshotRegistry.reverseIterator()
        if (iterator.hasNext()) {
            val snapshot = iterator.next()
            var tier: HashTier<T>? = snapshot.getDelta(this@SnapshottableHashTable)
            if (tier == null) {
                tier = HashTier(prevSize)
                snapshot.setDelta(this@SnapshottableHashTable, tier)
            }
        }
    }

    private fun updateTierData(prev: T, prevSize: Int) {
        val iterator = snapshotRegistry.reverseIterator()
        if (iterator.hasNext()) {
            val snapshot = iterator.next()
            // If the previous element was present in the most recent snapshot, add it to that snapshot's hash tier.
            if (prev!!.startEpoch() <= snapshot.epoch) {
                var tier: HashTier<T>? = snapshot.getDelta(this@SnapshottableHashTable)
                if (tier == null) {
                    tier = HashTier(prevSize)
                    snapshot.setDelta(this@SnapshottableHashTable, tier)
                }
                if (tier.deltaTable == null) {
                    tier.deltaTable = BaseHashTable(1)
                }
                tier.deltaTable!!.baseAddOrReplace(prev)
            }
        }
    }

    fun snapshottableIterator(epoch: Long): MutableIterator<T> {
        return if (epoch == LATEST_EPOCH) CurrentIterator(baseElements())
        else HistoricalIterator(baseElements(), snapshotRegistry.getSnapshot(epoch))
    }

    fun snapshottableToDebugString(): String {
        val bld = StringBuilder()
        bld.append(String.format("SnapshottableHashTable{%n"))
        bld.append("top tier: ")
        bld.append(baseToDebugString())
        bld.append(String.format(",%nsnapshot tiers: [%n"))
        val prefix = ""

        val iter: Iterator<Snapshot> = snapshotRegistry.iterator()
        while (iter.hasNext()) {
            val snapshot = iter.next()
            bld.append(prefix)
            bld.append("epoch ").append(snapshot.epoch).append(": ")

            val tier: HashTier<T>? = snapshot.getDelta(this)
            if (tier == null) bld.append("null")
            else {
                bld.append("HashTier{")
                bld.append("size=").append(tier.size)
                bld.append(", deltaTable=")

                if (tier.deltaTable == null) bld.append("null")
                else bld.append(tier.deltaTable!!.baseToDebugString())

                bld.append("}")
            }
            bld.append(String.format("%n"))
        }
        bld.append(String.format("]}%n"))
        return bld.toString()
    }

    override fun executeRevert(targetEpoch: Long, delta: Delta) {
        val tier = delta as HashTier<T>
        val iter = snapshottableIterator(LATEST_EPOCH)
        while (iter.hasNext()) {
            val element = iter.next()
            if (element!!.startEpoch() > targetEpoch) iter.remove()
        }
        val deltaTable = tier.deltaTable
        if (deltaTable != null) {
            val out: MutableList<T> = ArrayList()
            for (i in deltaTable.baseElements().indices) {
                unpackSlot(out, deltaTable.baseElements(), i)

                for (value in out) baseAddOrReplace(value)

                out.clear()
            }
        }
    }

    override fun reset() {
        val iter = snapshottableIterator(LATEST_EPOCH)
        while (iter.hasNext()) {
            iter.next()
            iter.remove()
        }
    }

    interface ElementWithStartEpoch {

        fun setStartEpoch(startEpoch: Long)

        fun startEpoch(): Long
    }

    internal class HashTier<T : ElementWithStartEpoch?>(internal val size: Int) : Delta {

        internal var deltaTable: BaseHashTable<T>? = null

        override fun mergeFrom(epoch: Long, source: Delta) {
            val other = source as HashTier<T>
            // As an optimization, the deltaTable might not exist for a new key as there is no previous value
            if (other.deltaTable != null) {
                val list: MutableList<T> = ArrayList()
                val otherElements = other.deltaTable!!.baseElements()

                for (slot in otherElements.indices) {
                    unpackSlot(list, otherElements, slot)
                    for (element in list) {
                        // When merging in a later hash tier, we want to keep only the elements that were
                        // present at our epoch.
                        if (element!!.startEpoch() <= epoch) {
                            if (deltaTable == null) {
                                deltaTable = BaseHashTable(1)
                            }
                            deltaTable!!.baseAddOrReplace(element)
                        }
                    }
                }
            }
        }
    }

    /**
     * Iterate over the values that currently exist in the hash table.
     *
     * You can use this iterator even if you are making changes to the map.
     * The changes may or may not be visible while you are iterating.
     */
    internal inner class CurrentIterator(private val topTier: Array<Any?>) : MutableIterator<T> {

        private val ready: MutableList<T> = ArrayList()

        private var slot = 0

        private var lastReturned: T? = null

        override fun hasNext(): Boolean {
            while (ready.isEmpty()) {
                if (slot == topTier.size) return false

                unpackSlot(ready, topTier, slot)
                slot++
            }
            return true
        }

        override fun next(): T {
            if (!hasNext()) throw NoSuchElementException()

            lastReturned = ready.removeAt(ready.size - 1)
            return lastReturned!!
        }

        override fun remove() {
            val lastReturned = this.lastReturned ?: throw UnsupportedOperationException("remove")
            snapshottableRemove(lastReturned)
            this.lastReturned = null
        }
    }

    /**
     * Iterate over the values that existed in the hash table during a specific snapshot.
     *
     * You can use this iterator even if you are making changes to the map. The snapshot is immutable and
     * will always show up the same.
     */
    internal inner class HistoricalIterator(
        private val topTier: Array<Any?>,
        private val snapshot: Snapshot,
    ) : MutableIterator<T> {

        private val temp: MutableList<T> = ArrayList()

        private val ready: MutableList<T> = ArrayList()

        private var slot = 0

        override fun hasNext(): Boolean {
            while (ready.isEmpty()) {
                if (slot == topTier.size) return false

                unpackSlot(temp, topTier, slot)
                for (obj in temp) {
                    if (obj!!.startEpoch() <= snapshot.epoch)
                        ready.add(obj)
                }
                temp.clear()

                /*
                 * As we iterate over the SnapshottableHashTable, elements may move from
                 * the top tier into the snapshot tiers. This would happen if something
                 * were deleted in the top tier, for example, but still retained in the
                 * snapshot.
                 *
                 * We don't want to return any elements twice, though. Therefore, we
                 * iterate over the top tier and the snapshot tier at the
                 * same time. The key to understanding how this works is realizing that
                 * both hash tables use the same hash function, but simply choose a
                 * different number of significant bits based on their size.
                 * So if the top tier has size 4 and the snapshot tier has size 2, we have
                 * the following mapping:
                 *
                 * Elements that would be in slot 0 or 1 in the top tier can only be in
                 * slot 0 in the snapshot tier.
                 * Elements that would be in slot 2 or 3 in the top tier can only be in
                 * slot 1 in the snapshot tier.
                 *
                 * Therefore, we can do something like this:
                 * 1. check slot 0 in the top tier and slot 0 in the snapshot tier.
                 * 2. check slot 1 in the top tier and slot 0 in the snapshot tier.
                 * 3. check slot 2 in the top tier and slot 1 in the snapshot tier.
                 * 4. check slot 3 in the top tier and slot 1 in the snapshot tier.
                 *
                 * If elements move from the top tier to the snapshot tier, then
                 * we'll still find them and report them exactly once.
                 *
                 * Note that while I used 4 and 2 as example sizes here, the same pattern
                 * holds for different powers of two. The "snapshot slot" of an element
                 * will be the top few bits of the top tier slot of that element.
                 */
                val iterator: Iterator<Snapshot> = snapshotRegistry.iterator(snapshot)
                while (iterator.hasNext()) {
                    val curSnapshot = iterator.next()
                    val tier: HashTier<T>? = curSnapshot.getDelta(this@SnapshottableHashTable)
                    tier?.deltaTable?.let { deltaTable ->
                        val shift = Integer.numberOfLeadingZeros(deltaTable.baseElements().size) -
                                Integer.numberOfLeadingZeros(topTier.size)
                        val tierSlot = slot ushr shift
                        unpackSlot(temp, deltaTable.baseElements(), tierSlot)
                        for (obj in temp) {
                            if (obj!!.startEpoch() <= snapshot.epoch) {
                                if (findSlot(obj, topTier.size) == slot) ready.add(obj)
                            }
                        }
                        temp.clear()
                    }
                }
                slot++
            }
            return true
        }

        override fun next(): T {
            if (!hasNext()) throw NoSuchElementException()
            return ready.removeAt(ready.size - 1)
        }

        override fun remove() = throw UnsupportedOperationException("remove")
    }

    companion object {

        /**
         * A special epoch value that represents the latest data.
         */
        const val LATEST_EPOCH = Long.MAX_VALUE
    }
}
