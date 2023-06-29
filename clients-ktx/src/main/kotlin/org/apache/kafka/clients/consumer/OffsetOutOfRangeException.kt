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

package org.apache.kafka.clients.consumer

import org.apache.kafka.common.TopicPartition

/**
 * No reset policy has been defined, and the offsets for these partitions are either larger or smaller
 * than the range of offsets the server has for the given partition.
 */
class OffsetOutOfRangeException(
    message: String? = null,
    val offsetOutOfRangePartitions: Map<TopicPartition, Long>,
) : InvalidOffsetException(message) {

    constructor(offsetOutOfRangePartitions: Map<TopicPartition, Long>) : this(
        message = "Offsets out of range with no configured reset policy for partitions: " +
                offsetOutOfRangePartitions,
        offsetOutOfRangePartitions = offsetOutOfRangePartitions,
    )

    /**
     * Get a map of the topic partitions and the respective out-of-range fetch offsets.
     */
    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("offsetOutOfRangePartitions"),
    )
    fun offsetOutOfRangePartitions(): Map<TopicPartition, Long> = offsetOutOfRangePartitions

    override fun partitions(): Set<TopicPartition> = offsetOutOfRangePartitions.keys

    companion object {
        private const val serialVersionUID = 1L
    }
}
