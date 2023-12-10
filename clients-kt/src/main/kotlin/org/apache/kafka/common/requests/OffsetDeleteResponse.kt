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

package org.apache.kafka.common.requests

import java.nio.ByteBuffer
import java.util.function.Consumer
import java.util.function.Function
import org.apache.kafka.common.message.OffsetDeleteResponseData
import org.apache.kafka.common.message.OffsetDeleteResponseData.OffsetDeleteResponsePartition
import org.apache.kafka.common.message.OffsetDeleteResponseData.OffsetDeleteResponseTopic
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.ByteBufferAccessor
import org.apache.kafka.common.protocol.Errors

/**
 * Possible error codes:
 *
 * - Partition errors:
 * - [Errors.GROUP_SUBSCRIBED_TO_TOPIC]
 * - [Errors.TOPIC_AUTHORIZATION_FAILED]
 * - [Errors.UNKNOWN_TOPIC_OR_PARTITION]
 *
 * - Group or coordinator errors:
 * - [Errors.COORDINATOR_LOAD_IN_PROGRESS]
 * - [Errors.COORDINATOR_NOT_AVAILABLE]
 * - [Errors.NOT_COORDINATOR]
 * - [Errors.GROUP_AUTHORIZATION_FAILED]
 * - [Errors.INVALID_GROUP_ID]
 * - [Errors.GROUP_ID_NOT_FOUND]
 * - [Errors.NON_EMPTY_GROUP]
 */
class OffsetDeleteResponse(
    private val data: OffsetDeleteResponseData,
) : AbstractResponse(ApiKeys.OFFSET_DELETE) {

    override fun data(): OffsetDeleteResponseData = data

    override fun errorCounts(): Map<Errors, Int> {
        val counts = mutableMapOf<Errors, Int>()
        updateErrorCounts(counts, Errors.forCode(data.errorCode))

        data.topics.forEach { topic ->
            topic.partitions.forEach { partition ->
                updateErrorCounts(counts, Errors.forCode(partition.errorCode))
            }
        }
        return counts
    }

    override fun throttleTimeMs(): Int = data.throttleTimeMs

    override fun maybeSetThrottleTimeMs(throttleTimeMs: Int) {
        data.setThrottleTimeMs(throttleTimeMs)
    }

    override fun shouldClientThrottle(version: Short): Boolean = version >= 0

    class Builder {
        var data = OffsetDeleteResponseData()
        private fun getOrCreateTopic(
            topicName: String,
        ): OffsetDeleteResponseTopic {
            var topic = data.topics.find(topicName)
            if (topic == null) {
                topic = OffsetDeleteResponseTopic().setName(topicName)
                data.topics.add(topic)
            }
            return topic
        }

        fun addPartition(
            topicName: String,
            partitionIndex: Int,
            error: Errors,
        ): Builder {
            val topicResponse = getOrCreateTopic(topicName)
            topicResponse.partitions.add(
                OffsetDeleteResponsePartition()
                    .setPartitionIndex(partitionIndex)
                    .setErrorCode(error.code)
            )
            return this
        }

        fun <P> addPartitions(
            topicName: String,
            partitions: List<P>,
            partitionIndex: Function<P, Int?>,
            error: Errors,
        ): Builder {
            val topicResponse = getOrCreateTopic(topicName)
            partitions.forEach(Consumer { partition: P ->
                topicResponse.partitions.add(
                    OffsetDeleteResponsePartition()
                        .setPartitionIndex(partitionIndex.apply(partition)!!)
                        .setErrorCode(error.code)
                )
            })
            return this
        }

        fun merge(
            newData: OffsetDeleteResponseData,
        ): Builder {
            if (data.topics.isEmpty()) {
                // If the current data is empty, we can discard it and use the new data.
                data = newData
            } else {
                // Otherwise, we have to merge them together.
                newData.topics.forEach { newTopic ->
                    val existingTopic = data.topics.find(newTopic.name)
                    if (existingTopic == null) {
                        // If no topic exists, we can directly copy the new topic data.
                        data.topics.add(newTopic.duplicate())
                    } else {
                        // Otherwise, we add the partitions to the existing one. Note we
                        // expect non-overlapping partitions here as we don't verify
                        // if the partition is already in the list before adding it.
                        newTopic.partitions.forEach { partition ->
                            existingTopic.partitions.add(partition.duplicate())
                        }
                    }
                }
            }
            return this
        }

        fun build(): OffsetDeleteResponse = OffsetDeleteResponse(data)
    }

    companion object {

        fun parse(buffer: ByteBuffer, version: Short): OffsetDeleteResponse =
            OffsetDeleteResponse(OffsetDeleteResponseData(ByteBufferAccessor(buffer), version))
    }
}
