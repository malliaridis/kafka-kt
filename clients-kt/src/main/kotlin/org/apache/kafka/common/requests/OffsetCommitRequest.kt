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
import org.apache.kafka.common.errors.UnsupportedVersionException
import org.apache.kafka.common.message.OffsetCommitRequestData
import org.apache.kafka.common.message.OffsetCommitRequestData.OffsetCommitRequestPartition
import org.apache.kafka.common.message.OffsetCommitRequestData.OffsetCommitRequestTopic
import org.apache.kafka.common.message.OffsetCommitResponseData
import org.apache.kafka.common.message.OffsetCommitResponseData.OffsetCommitResponsePartition
import org.apache.kafka.common.message.OffsetCommitResponseData.OffsetCommitResponseTopic
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.ByteBufferAccessor
import org.apache.kafka.common.protocol.Errors

class OffsetCommitRequest(
    private val data: OffsetCommitRequestData,
    version: Short,
) : AbstractRequest(ApiKeys.OFFSET_COMMIT, version) {

    override fun data(): OffsetCommitRequestData = data

    fun offsets(): Map<TopicPartition, Long> {
        val offsets = mutableMapOf<TopicPartition, Long>()

        for (topic: OffsetCommitRequestTopic in data.topics)
            for (partition: OffsetCommitRequestPartition in topic.partitions)
                offsets[TopicPartition(topic.name, partition.partitionIndex)] =
                    partition.committedOffset

        return offsets
    }

    override fun getErrorResponse(throttleTimeMs: Int, e: Throwable): OffsetCommitResponse {
        val responseTopicData = getErrorResponseTopics(data.topics, Errors.forException(e))

        return OffsetCommitResponse(
            OffsetCommitResponseData()
                .setTopics(responseTopicData)
                .setThrottleTimeMs(throttleTimeMs)
        )
    }

    class Builder(
        private val data: OffsetCommitRequestData,
    ) : AbstractRequest.Builder<OffsetCommitRequest>(ApiKeys.OFFSET_COMMIT) {

        override fun build(version: Short): OffsetCommitRequest {
            if (data.groupInstanceId != null && version < 7)
                throw UnsupportedVersionException(
                    "The broker offset commit protocol version $version " +
                            "does not support usage of config group.instance.id."
                )

            return OffsetCommitRequest(data, version)
        }

        override fun toString(): String = data.toString()
    }

    companion object {

        // default values for the current version
        val DEFAULT_GENERATION_ID = -1

        val DEFAULT_MEMBER_ID = ""

        val DEFAULT_RETENTION_TIME = -1L

        // default values for old versions, will be removed after these versions are no longer
        // supported
        val DEFAULT_TIMESTAMP = -1L // for V0, V1

        fun getErrorResponseTopics(
            requestTopics: List<OffsetCommitRequestTopic>,
            e: Errors,
        ): List<OffsetCommitResponseTopic> {
            val responseTopicData = mutableListOf<OffsetCommitResponseTopic>()

            for (entry in requestTopics) {
                val responsePartitions: MutableList<OffsetCommitResponsePartition> = ArrayList()
                for (requestPartition in entry.partitions) {
                    responsePartitions.add(
                        OffsetCommitResponsePartition()
                            .setPartitionIndex(requestPartition.partitionIndex)
                            .setErrorCode(e.code)
                    )
                }

                responseTopicData.add(
                    OffsetCommitResponseTopic()
                        .setName(entry.name)
                        .setPartitions(responsePartitions)
                )
            }
            return responseTopicData
        }

        fun parse(buffer: ByteBuffer, version: Short): OffsetCommitRequest =
            OffsetCommitRequest(
                OffsetCommitRequestData(ByteBufferAccessor(buffer), version),
                version,
            )
    }
}
