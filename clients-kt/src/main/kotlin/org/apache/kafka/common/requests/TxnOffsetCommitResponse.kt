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
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.message.TxnOffsetCommitResponseData
import org.apache.kafka.common.message.TxnOffsetCommitResponseData.TxnOffsetCommitResponsePartition
import org.apache.kafka.common.message.TxnOffsetCommitResponseData.TxnOffsetCommitResponseTopic
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.ByteBufferAccessor
import org.apache.kafka.common.protocol.Errors

/**
 * Possible error codes:
 *
 * - [Errors.INVALID_PRODUCER_EPOCH]
 * - [Errors.NOT_COORDINATOR]
 * - [Errors.COORDINATOR_NOT_AVAILABLE]
 * - [Errors.COORDINATOR_LOAD_IN_PROGRESS]
 * - [Errors.OFFSET_METADATA_TOO_LARGE]
 * - [Errors.GROUP_AUTHORIZATION_FAILED]
 * - [Errors.INVALID_COMMIT_OFFSET_SIZE]
 * - [Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED]
 * - [Errors.REQUEST_TIMED_OUT]
 * - [Errors.UNKNOWN_MEMBER_ID]
 * - [Errors.FENCED_INSTANCE_ID]
 * - [Errors.ILLEGAL_GENERATION]
 */
class TxnOffsetCommitResponse : AbstractResponse {

    private val data: TxnOffsetCommitResponseData

    constructor(data: TxnOffsetCommitResponseData) : super(ApiKeys.TXN_OFFSET_COMMIT) {
        this.data = data
    }

    constructor(
        requestThrottleMs: Int,
        responseData: Map<TopicPartition, Errors>,
    ) : super(ApiKeys.TXN_OFFSET_COMMIT) {
        val responseTopicDataMap = mutableMapOf<String, TxnOffsetCommitResponseTopic>()

        for ((topicPartition, value) in responseData) {
            val topicName = topicPartition.topic
            val topic = responseTopicDataMap.getOrDefault(
                topicName, TxnOffsetCommitResponseTopic().setName(topicName)
            )
            topic.partitions += TxnOffsetCommitResponsePartition()
                .setErrorCode(value.code)
                .setPartitionIndex(topicPartition.partition)

            responseTopicDataMap[topicName] = topic
        }

        data = TxnOffsetCommitResponseData()
            .setTopics(responseTopicDataMap.values.toList())
            .setThrottleTimeMs(requestThrottleMs)
    }

    override fun data(): TxnOffsetCommitResponseData = data

    override fun throttleTimeMs(): Int = data.throttleTimeMs

    override fun maybeSetThrottleTimeMs(throttleTimeMs: Int) {
        data.setThrottleTimeMs(throttleTimeMs)
    }

    override fun errorCounts(): Map<Errors, Int> = errorCounts(
        data.topics.flatMap { topic ->
            topic.partitions.map { partition -> Errors.forCode(partition.errorCode) }
        }
    )

    fun errors(): Map<TopicPartition, Errors> {
        val errorMap = mutableMapOf<TopicPartition, Errors>()

        for (topic in data.topics)
            for (partition in topic.partitions)
                errorMap[TopicPartition(topic.name, partition.partitionIndex)] =
                    Errors.forCode(partition.errorCode)

        return errorMap
    }

    override fun toString(): String = data.toString()

    override fun shouldClientThrottle(version: Short): Boolean = version >= 1

    class Builder {

        var data = TxnOffsetCommitResponseData()

        var byTopicName = HashMap<String, TxnOffsetCommitResponseTopic>()

        private fun getOrCreateTopic(topicName: String): TxnOffsetCommitResponseTopic {
            var topic = byTopicName[topicName]
            if (topic == null) {
                topic = TxnOffsetCommitResponseTopic().setName(topicName)
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
            topicResponse.partitions += TxnOffsetCommitResponsePartition()
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
                topicResponse.partitions += TxnOffsetCommitResponsePartition()
                    .setPartitionIndex(partitionIndex(partition))
                    .setErrorCode(error.code)
            }
            return this
        }

        fun merge(newData: TxnOffsetCommitResponseData): Builder {
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

        fun build(): TxnOffsetCommitResponse = TxnOffsetCommitResponse(data)
    }

    companion object {

        fun parse(buffer: ByteBuffer, version: Short): TxnOffsetCommitResponse =
            TxnOffsetCommitResponse(
                TxnOffsetCommitResponseData(ByteBufferAccessor(buffer), version)
            )
    }
}
