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

package org.apache.kafka.common.internals

import java.util.*
import org.apache.kafka.common.TopicPartition

/**
 * This class is a useful building block for doing fetch requests where topic partitions have to be
 * rotated via round-robin to ensure fairness and some level of determinism given the existence of a
 * limit on the fetch response size. Because the serialization of fetch requests is more efficient
 * if all partitions for the same topic are grouped together, we do such grouping in the method
 * `set`.
 *
 * As partitions are moved to the end, the same topic may be repeated more than once. In the optimal
 * case, a single topic would "wrap around" and appear twice. However, as partitions are fetched in
 * different orders and partition leadership changes, we will deviate from the optimal. If this
 * turns out to be an issue in practice, we can improve it by tracking the partitions per node or
 * calling `set` every so often.
 *
 * Note that this class is not thread-safe except for [size] which returns the number of partitions
 * currently tracked.
 */
class PartitionStates<S> {

    private val map = LinkedHashMap<TopicPartition, S>()

    private val partitionSetView = Collections.unmodifiableSet(map.keys)

    /* the number of partitions that are currently assigned available in a thread safe manner */
    @Volatile
    private var size = 0

    fun moveToEnd(topicPartition: TopicPartition) {
        val state = map.remove(topicPartition)
        if (state != null) map[topicPartition] = state
    }

    fun updateAndMoveToEnd(topicPartition: TopicPartition, state: S) {
        map.remove(topicPartition)
        map[topicPartition] = state
        updateSize()
    }

    fun update(topicPartition: TopicPartition, state: S) {
        map[topicPartition] = state
        updateSize()
    }

    fun remove(topicPartition: TopicPartition) {
        map.remove(topicPartition)
        updateSize()
    }

    /**
     * Returns an unmodifiable view of the partitions in random order.
     * changes to this PartitionStates instance will be reflected in this view.
     */
    fun partitionSet(): Set<TopicPartition> {
        return partitionSetView
    }

    fun clear() {
        map.clear()
        updateSize()
    }

    operator fun contains(topicPartition: TopicPartition): Boolean {
        return map.containsKey(topicPartition)
    }

    fun stateIterator(): Iterator<S?> {
        return map.values.iterator()
    }

    fun forEach(block: (TopicPartition, S) -> Unit) = map.forEach { (key, value) -> block(key, value) }

    fun partitionStateMap(): Map<TopicPartition, S> = map

    /**
     * Returns the partition state values in order.
     */
    fun partitionStateValues(): List<S> = ArrayList(map.values)

    fun stateValue(topicPartition: TopicPartition): S? = map[topicPartition]

    /**
     * Get the number of partitions that are currently being tracked. This is thread-safe.
     */
    fun size(): Int = size

    /**
     * Update the builder to have the received map as its state (i.e. the previous state is cleared). The builder will
     * "batch by topic", so if we have a, b and c, each with two partitions, we may end up with something like the
     * following (the order of topics and partitions within topics is dependent on the iteration order of the received
     * map): a0, a1, b1, b0, c0, c1.
     */
    fun set(partitionToState: Map<TopicPartition, S>) {
        map.clear()
        update(partitionToState)
        updateSize()
    }

    private fun updateSize() {
        size = map.size
    }

    private fun update(partitionToState: Map<TopicPartition, S>) {
        val topicToPartitions = LinkedHashMap<String, MutableList<TopicPartition>>()

        partitionToState.keys.forEach { tp ->
            val partitions = topicToPartitions.computeIfAbsent(tp.topic) { ArrayList() }
            partitions.add(tp)
        }

        topicToPartitions.values.forEach { value ->
            value.forEach { tp ->
                val state = partitionToState[tp]
                state?.let { map[tp] = it }
            }
        }
    }

    data class PartitionState<S>(
        val topicPartition: TopicPartition,
        val state: S,
    ) {

        @Deprecated(
            message = "Use property instead",
            replaceWith = ReplaceWith("state"),
        )
        fun value(): S = state

        @Deprecated(
            message = "Use property instead",
            replaceWith = ReplaceWith("topicPartition"),
        )
        fun topicPartition(): TopicPartition = topicPartition

        override fun toString(): String {
            return "PartitionState($topicPartition=${state})"
        }
    }
}
