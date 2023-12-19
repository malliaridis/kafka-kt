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

/**
 * This is a hash map which can be snapshotted.
 *
 * See [SnapshottableHashTable] for more details about the implementation.
 *
 * This class requires external synchronization.  Null keys and values are not supported.
 *
 * @param K The key type of the set.
 * @param V The value type of the set.
 */
class TimelineHashMap<K : Any, V : Any>(
    snapshotRegistry: SnapshotRegistry,
    expectedSize: Int,
) : SnapshottableHashTable<TimelineHashMap.TimelineHashMapEntry<K, V>>(
    snapshotRegistry = snapshotRegistry,
    expectedSize = expectedSize,
), MutableMap<K, V> {

    override val size: Int
        get() = size(LATEST_EPOCH)

    fun size(epoch: Long): Int = snapshottableSize(epoch)

    override fun isEmpty(): Boolean = isEmpty(LATEST_EPOCH)

    fun isEmpty(epoch: Long): Boolean = snapshottableSize(epoch) == 0

    override fun containsKey(key: K): Boolean {
        return containsKey(key, LATEST_EPOCH)
    }

    fun containsKey(key: K, epoch: Long): Boolean {
        return snapshottableGet(TimelineHashMapEntry(key, null), epoch) != null
    }

    override fun containsValue(value: V): Boolean {
        val iter: Iterator<Map.Entry<K, V>> = entries.iterator()
        while (iter.hasNext()) {
            val (_, value1) = iter.next()
            if (value == value1) return true
        }
        return false
    }

    override operator fun get(key: K): V? = get(key, LATEST_EPOCH)

    operator fun get(key: K, epoch: Long): V? {
        val (_, value) = snapshottableGet(TimelineHashMapEntry(key, null), epoch) ?: return null
        return value
    }

    override fun put(key: K, value: V): V? {
        val entry = TimelineHashMapEntry(key, value)
        val (_, value1) = snapshottableAddOrReplace(entry) ?: return null
        return value1
    }

    override fun remove(key: K): V? {
        val result = snapshottableRemove(TimelineHashMapEntry(key, null))
        return result?.value
    }

    override fun putAll(from: Map<out K, V>) {
        for ((key, value) in from) {
            put(key, value)
        }
    }

    override fun clear() = reset()

    override val keys: MutableSet<K>
        get() = keys(LATEST_EPOCH)

    fun keys(epoch: Long): MutableSet<K> = KeySet(epoch)

    override val values: MutableCollection<V>
        get() = values(LATEST_EPOCH)

    fun values(epoch: Long): MutableCollection<V> = Values(epoch)

    override val entries: MutableSet<MutableMap.MutableEntry<K, V>>
        get() = entries(LATEST_EPOCH)

    fun entries(epoch: Long): MutableSet<MutableMap.MutableEntry<K, V>> = EntrySet(epoch)

    override fun hashCode(): Int {
        var hash = 0
        val iter: Iterator<Map.Entry<K, V>> = entries.iterator()
        while (iter.hasNext()) {
            hash += iter.next().hashCode()
        }
        return hash
    }

    override fun equals(other: Any?): Boolean {
        if (other === this) return true
        if (other !is Map<*, *>) return false
        if (other.size != size) return false
        try {
            val iter = entries.iterator()
            while (iter.hasNext()) {
                val (key, value) = iter.next()
                if (other[key] != value) return false
            }
        } catch (_: ClassCastException) {
            return false
        }
        return true
    }

    class TimelineHashMapEntry<K, V> internal constructor(
        override val key: K,
        override val value: V,
    ) : ElementWithStartEpoch, MutableMap.MutableEntry<K, V> {

        private var startEpoch: Long = LATEST_EPOCH

        override fun setValue(newValue: V): V {
            // This would be inefficient to support since we'd need a back-reference
            // to the enclosing map in each Entry object.  There would also be
            // complications if this entry object was sourced from a historical iterator;
            // we don't support modifying the past.  Since we don't really need this API,
            // let's just not support it.
            throw UnsupportedOperationException()
        }

        override fun setStartEpoch(startEpoch: Long) {
            this.startEpoch = startEpoch
        }

        override fun startEpoch(): Long = startEpoch

        override fun equals(other: Any?): Boolean {
            if (other !is TimelineHashMapEntry<*, *>) return false
            val (key1) = other as TimelineHashMapEntry<K, V>
            return key == key1
        }

        override fun hashCode(): Int = key.hashCode()
    }

    internal inner class KeySet(private val epoch: Long) : AbstractMutableSet<K>() {

        override val size: Int
            get() = this@TimelineHashMap.size(epoch)

        override fun clear() {
            if (epoch != LATEST_EPOCH) throw RuntimeException("can't modify snapshot")
            this@TimelineHashMap.clear()
        }

        override fun iterator(): MutableIterator<K> = KeyIterator(epoch)

        override operator fun contains(element: K): Boolean {
            return this@TimelineHashMap.containsKey(element, epoch)
        }

        override fun remove(element: K): Boolean {
            if (epoch != LATEST_EPOCH) throw RuntimeException("can't modify snapshot")
            return this@TimelineHashMap.remove(element) != null
        }

        override fun add(element: K): Boolean = throw UnsupportedOperationException()
    }

    internal inner class KeyIterator(epoch: Long) : MutableIterator<K> {

        private val iter: MutableIterator<TimelineHashMapEntry<K, V>> = snapshottableIterator(epoch)

        override fun hasNext(): Boolean = iter.hasNext()

        override fun next(): K {
            val (key) = iter.next()
            return key
        }

        override fun remove() = iter.remove()
    }

    internal inner class Values(private val epoch: Long) : AbstractMutableCollection<V>() {

        override val size: Int
            get() = this@TimelineHashMap.size(epoch)

        override fun clear() {
            if (epoch != LATEST_EPOCH) throw RuntimeException("can't modify snapshot")
            this@TimelineHashMap.clear()
        }

        override fun iterator(): MutableIterator<V> = ValueIterator(epoch)

        override operator fun contains(element: V): Boolean {
            return this@TimelineHashMap.containsKey(element as K, epoch)
        }

        override fun add(element: V): Boolean = throw UnsupportedOperationException()
    }

    internal inner class ValueIterator(epoch: Long) : MutableIterator<V> {

        private val iter: MutableIterator<TimelineHashMapEntry<K, V>> = snapshottableIterator(epoch)

        override fun hasNext(): Boolean = iter.hasNext()

        override fun next(): V {
            val (_, value) = iter.next()
            return value
        }

        override fun remove() = iter.remove()
    }

    internal inner class EntrySet(private val epoch: Long) : AbstractMutableSet<MutableMap.MutableEntry<K, V>>() {

        override val size: Int
            get() = this@TimelineHashMap.size(epoch)

        override fun clear() {
            if (epoch != LATEST_EPOCH) {
                throw RuntimeException("can't modify snapshot")
            }
            this@TimelineHashMap.clear()
        }

        override fun iterator(): MutableIterator<MutableMap.MutableEntry<K, V>> = EntryIterator(epoch)

        override fun contains(element: MutableMap.MutableEntry<K, V>): Boolean {
            return snapshottableGet(element, epoch) != null
        }

        override fun remove(element: MutableMap.MutableEntry<K, V>): Boolean {
            if (epoch != LATEST_EPOCH) throw RuntimeException("can't modify snapshot")
            return snapshottableRemove(TimelineHashMapEntry(element, null)) != null
        }

        override fun add(element: MutableMap.MutableEntry<K, V>): Boolean =
            throw UnsupportedOperationException()
    }

    internal inner class EntryIterator(epoch: Long) : MutableIterator<MutableMap.MutableEntry<K, V>> {

        private val iter: MutableIterator<TimelineHashMapEntry<K, V>> = snapshottableIterator(epoch)

        override fun hasNext(): Boolean = iter.hasNext()

        override fun next(): MutableMap.MutableEntry<K, V> = iter.next()

        override fun remove() = iter.remove()
    }
}
