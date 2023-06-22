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

import org.apache.kafka.common.IsolationLevel
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.message.ListOffsetsRequestData
import org.apache.kafka.common.message.ListOffsetsRequestData.ListOffsetsPartition
import org.apache.kafka.common.message.ListOffsetsRequestData.ListOffsetsTopic
import org.apache.kafka.common.message.ListOffsetsResponseData
import org.apache.kafka.common.message.ListOffsetsResponseData.ListOffsetsPartitionResponse
import org.apache.kafka.common.message.ListOffsetsResponseData.ListOffsetsTopicResponse
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.ByteBufferAccessor
import org.apache.kafka.common.protocol.Errors
import java.nio.ByteBuffer

class ListOffsetsRequest private constructor(
    private val data: ListOffsetsRequestData,
    version: Short,
) : AbstractRequest(ApiKeys.LIST_OFFSETS, version) {

    val duplicatePartitions: Set<TopicPartition>

    /**
     * Private constructor with a specified version.
     */
    init {
        val duplicates = mutableSetOf<TopicPartition>()

        val partitions: MutableSet<TopicPartition> = HashSet()
        data.topics().forEach { topic ->
            topic.partitions().forEach { partition ->
                val tp = TopicPartition(topic.name(), partition.partitionIndex())
                if (!partitions.add(tp)) duplicates.add(tp)
            }
        }
        duplicatePartitions = duplicates
    }

    override fun getErrorResponse(throttleTimeMs: Int, e: Throwable): AbstractResponse? {
        val versionId = version
        val errorCode = Errors.forException(e).code
        val responses: MutableList<ListOffsetsTopicResponse> = ArrayList()
        for (topic in data.topics()) {
            val topicResponse = ListOffsetsTopicResponse().setName(topic.name())
            val partitions: MutableList<ListOffsetsPartitionResponse> = ArrayList()
            for (partition in topic.partitions()) {
                val partitionResponse = ListOffsetsPartitionResponse()
                    .setErrorCode(errorCode)
                    .setPartitionIndex(partition.partitionIndex())
                if (versionId.toInt() == 0) {
                    partitionResponse.setOldStyleOffsets(emptyList())
                } else {
                    partitionResponse.setOffset(ListOffsetsResponse.UNKNOWN_OFFSET)
                        .setTimestamp(ListOffsetsResponse.UNKNOWN_TIMESTAMP)
                }
                partitions.add(partitionResponse)
            }
            topicResponse.setPartitions(partitions)
            responses.add(topicResponse)
        }
        val responseData = ListOffsetsResponseData()
            .setThrottleTimeMs(throttleTimeMs)
            .setTopics(responses)
        return ListOffsetsResponse(responseData)
    }

    override fun data(): ListOffsetsRequestData = data

    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("replicaId"),
    )
    fun replicaId(): Int = data.replicaId()

    val replicaId: Int
        get() = data.replicaId()

    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("isolationLevel"),
    )
    fun isolationLevel(): IsolationLevel {
        return IsolationLevel.forId(data.isolationLevel())
    }

    val isolationLevel: IsolationLevel
        get() = IsolationLevel.forId(data.isolationLevel())

    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("topics"),
    )
    fun topics(): List<ListOffsetsTopic> = data.topics()

    val topics: List<ListOffsetsTopic>
        get() = data.topics()

    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("duplicatePartitions"),
    )
    fun duplicatePartitions(): Set<TopicPartition> = duplicatePartitions

    class Builder private constructor(
        oldestAllowedVersion: Short,
        latestAllowedVersion: Short,
        replicaId: Int,
        isolationLevel: IsolationLevel,
    ) : AbstractRequest.Builder<ListOffsetsRequest>(
        apiKey = ApiKeys.LIST_OFFSETS,
        oldestAllowedVersion = oldestAllowedVersion,
        latestAllowedVersion = latestAllowedVersion
    ) {

        private val data: ListOffsetsRequestData = ListOffsetsRequestData()
            .setIsolationLevel(isolationLevel.id())
            .setReplicaId(replicaId)

        fun setTargetTimes(topics: List<ListOffsetsTopic?>?): Builder {
            data.setTopics(topics)
            return this
        }

        override fun build(version: Short): ListOffsetsRequest = ListOffsetsRequest(data, version)

        override fun toString(): String = data.toString()

        companion object {

            fun forReplica(allowedVersion: Short, replicaId: Int): Builder = Builder(
                0.toShort(),
                allowedVersion,
                replicaId,
                IsolationLevel.READ_UNCOMMITTED
            )

            fun forConsumer(
                requireTimestamp: Boolean,
                isolationLevel: IsolationLevel,
                requireMaxTimestamp: Boolean
            ): Builder {
                val minVersion: Short =
                    if (requireMaxTimestamp) 7
                    else if (isolationLevel === IsolationLevel.READ_COMMITTED) 2
                    else if (requireTimestamp) 1
                    else 0

                return Builder(
                    minVersion,
                    ApiKeys.LIST_OFFSETS.latestVersion(),
                    CONSUMER_REPLICA_ID,
                    isolationLevel
                )
            }
        }
    }

    companion object {

        const val EARLIEST_TIMESTAMP = -2L

        const val LATEST_TIMESTAMP = -1L

        const val MAX_TIMESTAMP = -3L

        const val CONSUMER_REPLICA_ID = -1

        const val DEBUGGING_REPLICA_ID = -2

        fun parse(buffer: ByteBuffer, version: Short): ListOffsetsRequest =
            ListOffsetsRequest(
                ListOffsetsRequestData(ByteBufferAccessor(buffer), version),
                version
            )

        fun toListOffsetsTopics(
            timestampsToSearch: Map<TopicPartition, ListOffsetsPartition?>,
        ): List<ListOffsetsTopic> {
            val topics: MutableMap<String, ListOffsetsTopic> = HashMap()

            for ((tp, value) in timestampsToSearch) {
                val topic = topics.computeIfAbsent(tp.topic) { ListOffsetsTopic().setName(tp.topic) }
                topic.partitions().add(value)
            }
            return ArrayList(topics.values)
        }

        fun singletonRequestData(
            topic: String?,
            partitionIndex: Int,
            timestamp: Long,
            maxNumOffsets: Int,
        ): ListOffsetsTopic = ListOffsetsTopic()
            .setName(topic)
            .setPartitions(
                listOf(
                    ListOffsetsPartition()
                        .setPartitionIndex(partitionIndex)
                        .setTimestamp(timestamp)
                        .setMaxNumOffsets(maxNumOffsets)
                )
            )
    }
}
