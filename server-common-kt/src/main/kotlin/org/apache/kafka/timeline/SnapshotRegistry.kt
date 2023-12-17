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

import org.apache.kafka.common.utils.LogContext
import org.slf4j.Logger

class SnapshotRegistry(logContext: LogContext) {

    private val log: Logger = logContext.logger(javaClass)

    /**
     * A map from snapshot epochs to snapshot data structures.
     */
    private val snapshots = HashMap<Long, Snapshot>()

    /**
     * The head of a list of snapshots, sorted by epoch.
     */
    private val head = Snapshot(Long.MIN_VALUE)

    /**
     * Collection of all Revertable registered with this registry
     */
    private val revertables: MutableList<Revertable> = ArrayList()

    /**
     * Returns a snapshot iterator that iterates from the snapshots with the lowest epoch to those with the highest,
     * starting at the snapshot with the given epoch.
     */
    fun iterator(epoch: Long): Iterator<Snapshot> = iterator(getSnapshot(epoch))

    /**
     * Returns a snapshot iterator that iterates from the snapshots with the lowest epoch to those with the highest,
     * starting at the given snapshot.
     */
    fun iterator(snapshot: Snapshot = head.next()): MutableIterator<Snapshot> = SnapshotIterator(snapshot)

    /**
     * Returns a reverse snapshot iterator that iterates from the snapshots with the
     * highest epoch to those with the lowest.
     */
    fun reverseIterator(): Iterator<Snapshot> = ReverseSnapshotIterator()

    /**
     * Returns a sorted list of snapshot epochs.
     */
    fun epochsList(): List<Long> = iterator().asSequence()
        .map { it.epoch }
        .toList()

    fun hasSnapshot(epoch: Long): Boolean = snapshots.containsKey(epoch)

    private fun epochsToString(): String = epochsList().joinToString { it.toString() }

    /**
     * Gets the snapshot for a specific epoch.
     */
    fun getSnapshot(epoch: Long): Snapshot {
        return snapshots[epoch] ?: throw RuntimeException(
            "No in-memory snapshot for epoch $epoch. Snapshot epochs are: ${epochsToString()}"
        )
    }

    /**
     * Creates a new snapshot at the given epoch.
     *
     * If `epoch` already exists, and it is the last snapshot, then just return that snapshot.
     *
     * @param epoch The epoch to create the snapshot at.  The current epoch
     * will be advanced to one past this epoch.
     */
    fun getOrCreateSnapshot(epoch: Long): Snapshot {
        val last = head.prev()
        if (last.epoch > epoch) throw RuntimeException(
            "Can't create a new in-memory snapshot at epoch $epoch because there is already " +
                    "a snapshot with epoch ${last.epoch}. Snapshot epochs are ${epochsToString()}"
        )
        else if (last.epoch == epoch) return last

        val snapshot = Snapshot(epoch)
        last.appendNext(snapshot)
        snapshots[epoch] = snapshot
        log.debug("Creating in-memory snapshot {}", epoch)

        return snapshot
    }

    /**
     * Reverts the state of all data structures to the state at the given epoch.
     *
     * @param targetEpoch The epoch of the snapshot to revert to.
     */
    fun revertToSnapshot(targetEpoch: Long) {
        log.debug("Reverting to in-memory snapshot {}", targetEpoch)
        val target = getSnapshot(targetEpoch)
        val iterator = iterator(target)

        iterator.next()
        while (iterator.hasNext()) {
            val snapshot = iterator.next()
            log.debug(
                "Deleting in-memory snapshot {} because we are reverting to {}",
                snapshot.epoch, targetEpoch
            )
            iterator.remove()
        }
        target.handleRevert()
    }

    /**
     * Deletes the snapshot with the given epoch.
     *
     * @param targetEpoch The epoch of the snapshot to delete.
     */
    fun deleteSnapshot(targetEpoch: Long) = deleteSnapshot(getSnapshot(targetEpoch))

    /**
     * Deletes the given snapshot.
     *
     * @param snapshot The snapshot to delete.
     */
    fun deleteSnapshot(snapshot: Snapshot) {
        val prev = snapshot.prev()
        if (prev != head) prev.mergeFrom(snapshot)
        else snapshot.erase()

        log.debug("Deleting in-memory snapshot {}", snapshot.epoch)
        snapshots.remove(snapshot.epoch, snapshot)
    }

    /**
     * Deletes all the snapshots up to the given epoch
     *
     * @param targetEpoch The epoch to delete up to.
     */
    fun deleteSnapshotsUpTo(targetEpoch: Long) {
        val iterator = iterator()
        while (iterator.hasNext()) {
            val snapshot = iterator.next()

            if (snapshot.epoch >= targetEpoch) return

            iterator.remove()
        }
    }

    /**
     * Return the latest epoch.
     */
    fun latestEpoch(): Long = head.prev().epoch

    /**
     * Associate a revertable with this registry.
     */
    fun register(revertable: Revertable) = revertables.add(revertable)

    /**
     * Delete all snapshots and resets all of the Revertable object registered.
     */
    fun reset() {
        deleteSnapshotsUpTo(LATEST_EPOCH)
        for (revertable in revertables) revertable.reset()

    }

    /**
     * Iterate through the list of snapshots in order of creation, such that older
     * snapshots come first.
     */
    internal inner class SnapshotIterator(var cur: Snapshot) : MutableIterator<Snapshot> {

        var result: Snapshot? = null

        override fun hasNext(): Boolean {
            return cur != head
        }

        override fun next(): Snapshot {
            val current = cur
            result = current
            cur = current.next()
            return current
        }

        override fun remove() {
            val result = checkNotNull(this.result)
            deleteSnapshot(result)
            this.result = null
        }
    }

    /**
     * Iterate through the list of snapshots in reverse order of creation, such that the newest snapshot is first.
     */
    internal inner class ReverseSnapshotIterator : Iterator<Snapshot> {

        var cur: Snapshot = head.prev()

        override fun hasNext(): Boolean = cur != head

        override fun next(): Snapshot {
            val result = cur
            cur = cur.prev()

            return result
        }
    }

    companion object {
        val LATEST_EPOCH = Long.MAX_VALUE
    }
}
