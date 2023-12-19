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
 * This is a mutable integer which can be snapshotted.
 *
 * This class requires external synchronization.
 */
class TimelineInteger(private val snapshotRegistry: SnapshotRegistry) : Revertable {

    internal class IntegerContainer : Delta {

        internal var value = INIT

        fun value(): Int = value

        fun setValue(value: Int) {
            this.value = value
        }

        override fun mergeFrom(destinationEpoch: Long, source: Delta) = Unit // Nothing to do
    }

    private var value: Int = INIT

    init {
        snapshotRegistry.register(this)
    }

    fun get(): Int = value

    operator fun get(epoch: Long): Int {
        if (epoch == SnapshotRegistry.LATEST_EPOCH) return value

        val iterator = snapshotRegistry.iterator(epoch)
        while (iterator.hasNext()) {
            val snapshot = iterator.next()
            val container: IntegerContainer? = snapshot.getDelta(this@TimelineInteger)
            if (container != null) return container.value()
        }

        return value
    }

    fun set(newValue: Int) {
        val iterator = snapshotRegistry.reverseIterator()
        if (iterator.hasNext()) {
            val snapshot = iterator.next()
            var container: IntegerContainer? = snapshot.getDelta(this@TimelineInteger)
            if (container == null) {
                container = IntegerContainer()
                snapshot.setDelta(this@TimelineInteger, container)
                container.setValue(value)
            }
        }
        value = newValue
    }

    fun increment() = set(get() + 1)

    fun decrement() = set(get() - 1)

    override fun executeRevert(targetEpoch: Long, delta: Delta) {
        val container = delta as IntegerContainer
        value = container.value
    }

    override fun reset() = set(INIT)

    override fun hashCode(): Int = value

    override fun equals(other: Any?): Boolean {
        if (other !is TimelineInteger) return false
        return value == other.value
    }

    override fun toString(): String = value.toString()

    companion object {
        const val INIT = 0
    }
}
