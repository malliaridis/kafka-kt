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
 * This is a mutable reference to an immutable object. It can be snapshotted.
 *
 * This class requires external synchronization.
 */
class TimelineObject<T: Any>(
    private val snapshotRegistry: SnapshotRegistry,
    private val initialValue: T,
) : Revertable {

    private var value: T = initialValue

    init {
        snapshotRegistry.register(this)
    }

    fun get(): T = value

    operator fun get(epoch: Long): T {
        if (epoch == SnapshotRegistry.LATEST_EPOCH) return value
        val iterator = snapshotRegistry.iterator(epoch)
        while (iterator.hasNext()) {
            val snapshot = iterator.next()
            val container: ObjectContainer<T>? = snapshot.getDelta(this@TimelineObject)
            if (container != null) return container.value()
        }
        return value
    }

    fun set(newValue: T) {
        Objects.requireNonNull(newValue)
        val iterator = snapshotRegistry.reverseIterator()
        if (iterator.hasNext()) {
            val snapshot = iterator.next()
            var prevContainer: ObjectContainer<T>? = snapshot.getDelta(this@TimelineObject)
            if (prevContainer == null) {
                prevContainer = ObjectContainer(initialValue)
                snapshot.setDelta(this@TimelineObject, prevContainer)
                prevContainer.setValue(value)
            }
        }
        value = newValue
    }

    override fun executeRevert(targetEpoch: Long, delta: Delta) {
        val container = delta as ObjectContainer<T>
        value = container.value()
    }

    override fun reset() = set(initialValue)

    override fun hashCode(): Int = value.hashCode()

    override fun equals(other: Any?): Boolean {
        if (other !is TimelineObject<*>) return false
        return value == other.value
    }

    override fun toString(): String = value.toString()

    internal class ObjectContainer<T>(private var value: T) : Delta {

        fun value(): T = value

        fun setValue(value: T) {
            this.value = value
        }

        override fun mergeFrom(destinationEpoch: Long, source: Delta) = Unit // Nothing to do
    }
}
