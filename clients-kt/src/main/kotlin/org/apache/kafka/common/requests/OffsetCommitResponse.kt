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
import java.util.function.Function
import java.util.stream.Stream
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
            .setTopics(ArrayList(responseTopicDataMap.values))
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

    companion object {

        fun parse(buffer: ByteBuffer, version: Short): OffsetCommitResponse =
            OffsetCommitResponse(OffsetCommitResponseData(ByteBufferAccessor(buffer), version))
    }
}
