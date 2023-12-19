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
 * This is a mutable long which can be snapshotted.
 *
 * This class requires external synchronization.
 */
class TimelineLong(private val snapshotRegistry: SnapshotRegistry) : Revertable {

    private var value: Long = INIT

    init {
        snapshotRegistry.register(this)
    }

    fun get(): Long = value

    operator fun get(epoch: Long): Long {
        if (epoch == SnapshotRegistry.LATEST_EPOCH) return value
        val iterator = snapshotRegistry.iterator(epoch)
        while (iterator.hasNext()) {
            val snapshot = iterator.next()
            val container: LongContainer? = snapshot.getDelta(this@TimelineLong)
            if (container != null) return container.value()
        }
        return value
    }

    fun set(newValue: Long) {
        val iterator = snapshotRegistry.reverseIterator()
        if (iterator.hasNext()) {
            val snapshot = iterator.next()
            var prevContainer: LongContainer? = snapshot.getDelta(this@TimelineLong)
            if (prevContainer == null) {
                prevContainer = LongContainer()
                snapshot.setDelta(this@TimelineLong, prevContainer)
                prevContainer.setValue(value)
            }
        }
        value = newValue
    }

    fun increment() = set(get() + 1L)

    fun decrement() = set(get() - 1L)

    override fun executeRevert(targetEpoch: Long, delta: Delta) {
        val container = delta as LongContainer
        value = container.value()
    }

    override fun reset() = set(INIT)

    override fun hashCode(): Int =  value.toInt() xor (value ushr 32).toInt()

    override fun equals(other: Any?): Boolean {
        if (other !is TimelineLong) return false
        return value == other.value
    }

    override fun toString(): String = value.toString()

    internal class LongContainer : Delta {

        private var value = INIT

        fun value(): Long = value

        fun setValue(value: Long) {
            this.value = value
        }

        override fun mergeFrom(destinationEpoch: Long, source: Delta) = Unit // Nothing to do
    }

    companion object {
        const val INIT: Long = 0
    }
}
