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
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.message.OffsetCommitResponseData
import org.apache.kafka.common.message.OffsetCommitResponseData.OffsetCommitResponsePartition
import org.apache.kafka.common.message.OffsetCommitResponseData.OffsetCommitResponseTopic
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.ByteBufferAccessor
import org.apache.kafka.common.protocol.Errors

/**
 * Possible error codes:
 *
 * - [Errors.UNKNOWN_TOPIC_OR_PARTITION]
 * - [Errors.REQUEST_TIMED_OUT]
 * - [Errors.OFFSET_METADATA_TOO_LARGE]
 * - [Errors.COORDINATOR_LOAD_IN_PROGRESS]
 * - [Errors.COORDINATOR_NOT_AVAILABLE]
 * - [Errors.NOT_COORDINATOR]
 * - [Errors.ILLEGAL_GENERATION]
 * - [Errors.UNKNOWN_MEMBER_ID]
 * - [Errors.REBALANCE_IN_PROGRESS]
 * - [Errors.INVALID_COMMIT_OFFSET_SIZE]
 * - [Errors.TOPIC_AUTHORIZATION_FAILED]
 * - [Errors.GROUP_AUTHORIZATION_FAILED]
 */
class OffsetCommitResponse : AbstractResponse {

    private val data: OffsetCommitResponseData

    constructor(data: OffsetCommitResponseData) : super(ApiKeys.OFFSET_COMMIT) {
        this.data = data
    }

    constructor(
        requestThrottleMs: Int,
        responseData: Map<TopicPartition, Errors>,
    ) : super(ApiKeys.OFFSET_COMMIT) {
        val responseTopicDataMap = mutableMapOf<String, OffsetCommitResponseTopic>()

        responseData.forEach { (topicPartition, value) ->
            val topicName = topicPartition.topic
            val topic = responseTopicDataMap.getOrDefault(
                topicName,
                OffsetCommitResponseTopic().setName(topicName)
            )
            topic.partitions += OffsetCommitResponsePartition()
                .setErrorCode(value.code)
                .setPartitionIndex(topicPartition.partition)

            responseTopicDataMap[topicName] = topic
        }

        data = OffsetCommitResponseData()
            .setTopics(responseTopicDataMap.values.toList())
            .setThrottleTimeMs(requestThrottleMs)
    }

    constructor(responseData: Map<TopicPartition, Errors>) : this(
        DEFAULT_THROTTLE_TIME,
        responseData
    )

    override fun data(): OffsetCommitResponseData = data

    override fun errorCounts(): Map<Errors, Int> {
        return errorCounts(
            data.topics.flatMap { topicResult ->
                topicResult.partitions.map { partitionResult ->
                    Errors.forCode(partitionResult.errorCode)
                }
            }
        )
    }

    override fun toString(): String = data.toString()

    override fun throttleTimeMs(): Int = data.throttleTimeMs

    override fun maybeSetThrottleTimeMs(throttleTimeMs: Int) {
        data.setThrottleTimeMs(throttleTimeMs)
    }

    override fun shouldClientThrottle(version: Short): Boolean = version >= 4

    class Builder {

        var data = OffsetCommitResponseData()

        var byTopicName = mutableMapOf<String, OffsetCommitResponseTopic>()

        private fun getOrCreateTopic(
            topicName: String,
        ): OffsetCommitResponseTopic {
            var topic = byTopicName[topicName]
            if (topic == null) {
                topic = OffsetCommitResponseTopic().setName(topicName)
                data.topics += topic
                byTopicName[topicName] = topic
            }
            return topic
        }

        fun addPartition(
            topicName: String,
            partitionIndex: Int,
            error: Errors,
        ): Builder {
            val topicResponse = getOrCreateTopic(topicName)
            topicResponse.partitions += OffsetCommitResponsePartition()
                .setPartitionIndex(partitionIndex)
                .setErrorCode(error.code)
            return this
        }

        fun <P> addPartitions(
            topicName: String,
            partitions: List<P>,
            partitionIndex: (P) -> Int,
            error: Errors,
        ): Builder {
            val topicResponse = getOrCreateTopic(topicName)
            partitions.forEach { partition ->
                topicResponse.partitions += OffsetCommitResponsePartition()
                    .setPartitionIndex(partitionIndex(partition))
                    .setErrorCode(error.code)
            }
            return this
        }

        fun merge(
            newData: OffsetCommitResponseData,
        ): Builder {
            if (data.topics.isEmpty()) {
                // If the current data is empty, we can discard it and use the new data.
                data = newData
            } else {
                // Otherwise, we have to merge them together.
                newData.topics.forEach { newTopic ->
                    val existingTopic = byTopicName[newTopic.name]
                    if (existingTopic == null) {
                        // If no topic exists, we can directly copy the new topic data.
                        data.topics += newTopic
                        byTopicName[newTopic.name] = newTopic
                    } else {
                        // Otherwise, we add the partitions to the existing one. Note we
                        // expect non-overlapping partitions here as we don't verify
                        // if the partition is already in the list before adding it.
                        existingTopic.partitions += newTopic.partitions
                    }
                }
            }
            return this
        }

        fun build(): OffsetCommitResponse = OffsetCommitResponse(data)
    }

    companion object {

        fun parse(buffer: ByteBuffer, version: Short): OffsetCommitResponse =
            OffsetCommitResponse(OffsetCommitResponseData(ByteBufferAccessor(buffer), version))
    }
}
