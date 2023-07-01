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
 * In the event of an unclean leader election, the log will be truncated, previously committed data
 * will be lost, and new data will be written over these offsets. When this happens, the consumer
 * will detect the truncation and raise this exception (if no automatic reset policy has been
 * defined) with the first offset known to diverge from what the consumer previously read.
 */
class LogTruncationException(
    message: String?,
    fetchOffsets: Map<TopicPartition, Long>,
    divergentOffsets: Map<TopicPartition, OffsetAndMetadata>,
) : OffsetOutOfRangeException(message, fetchOffsets) {

    val divergentOffsets: Map<TopicPartition, OffsetAndMetadata>

    init {
        this.divergentOffsets = divergentOffsets.toMap()
    }

    /**
     * Get the divergent offsets for the partitions which were truncated. For each partition, this
     * is the first offset which is known to diverge from what the consumer read.
     *
     * Note that there is no guarantee that this offset will be known. It is necessary to use
     * [partitions] to see the set of partitions that were truncated and then check for the
     * presence of a divergent offset in this map.
     */
    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("divergentOffsets"),
    )
    fun divergentOffsets(): Map<TopicPartition, OffsetAndMetadata> = divergentOffsets
}
