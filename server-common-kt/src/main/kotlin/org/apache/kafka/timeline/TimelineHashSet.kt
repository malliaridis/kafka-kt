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

import java.util.Objects

/**
 * This is a hash set which can be snapshotted.
 *
 * See [SnapshottableHashTable] for more details about the implementation.
 *
 * This class requires external synchronization. `null` values are not supported.
 *
 * @param T The value type of the set.
 */
class TimelineHashSet<T>(
    snapshotRegistry: SnapshotRegistry,
    expectedSize: Int,
) : SnapshottableHashTable<TimelineHashSet.TimelineHashSetEntry<T>>(
    snapshotRegistry = snapshotRegistry,
    expectedSize = expectedSize,
), MutableSet<T> {

    override val size: Int
        get() = size(LATEST_EPOCH)

    fun size(epoch: Long): Int = snapshottableSize(epoch)

    override fun isEmpty(): Boolean = isEmpty(LATEST_EPOCH)

    fun isEmpty(epoch: Long): Boolean = snapshottableSize(epoch) == 0

    override operator fun contains(element: T): Boolean = contains(element, LATEST_EPOCH)

    fun contains(key: T, epoch: Long): Boolean {
        return snapshottableGet(TimelineHashSetEntry(key), epoch) != null
    }

    override fun iterator(): MutableIterator<T> = iterator(LATEST_EPOCH)

    fun iterator(epoch: Long): MutableIterator<T> = ValueIterator(epoch)

    override fun add(element: T): Boolean {
        Objects.requireNonNull(element)
        return snapshottableAddUnlessPresent(TimelineHashSetEntry(element))
    }

    override fun remove(element: T): Boolean {
        return snapshottableRemove(TimelineHashSetEntry(element)) != null
    }

    override fun containsAll(elements: Collection<T>): Boolean {
        for (value in elements) {
            if (!contains(value)) return false
        }
        return true
    }

    override fun addAll(elements: Collection<T>): Boolean {
        var modified = false
        for (value in elements) {
            if (add(value)) {
                modified = true
            }
        }
        return modified
    }

    override fun retainAll(elements: Collection<T>): Boolean {
        Objects.requireNonNull(elements)
        var modified = false
        val it = iterator()
        while (it.hasNext()) {
            if (!elements.contains(it.next())) {
                it.remove()
                modified = true
            }
        }
        return modified
    }

    override fun removeAll(elements: Collection<T>): Boolean {
        Objects.requireNonNull(elements)
        var modified = false
        val it = iterator()
        while (it.hasNext()) {
            if (elements.contains(it.next())) {
                it.remove()
                modified = true
            }
        }
        return modified
    }

    override fun clear() = reset()

    override fun hashCode(): Int {
        var hash = 0
        val iter: Iterator<T> = iterator()
        while (iter.hasNext()) {
            hash += iter.next().hashCode()
        }
        return hash
    }

    override fun equals(other: Any?): Boolean {
        if (other === this) return true
        if (other !is Set<*>) return false

        return if (other.size != size) false else try {
            containsAll(other)
        } catch (_: ClassCastException) {
            false
        }
    }

    class TimelineHashSetEntry<T>(val value: T) : ElementWithStartEpoch {

        private var startEpoch: Long = LATEST_EPOCH

        override fun setStartEpoch(startEpoch: Long) {
            this.startEpoch = startEpoch
        }

        override fun startEpoch(): Long = startEpoch

        override fun equals(other: Any?): Boolean {
            if (other !is TimelineHashSetEntry<*>) return false
            return value == other.value
        }

        override fun hashCode(): Int = value.hashCode()
    }

    internal inner class ValueIterator(epoch: Long) : MutableIterator<T> {

        private val iter: MutableIterator<TimelineHashSetEntry<T>> = snapshottableIterator(epoch)

        override fun hasNext(): Boolean = iter.hasNext()

        override fun next(): T = iter.next().value

        override fun remove() = iter.remove()
    }
}
