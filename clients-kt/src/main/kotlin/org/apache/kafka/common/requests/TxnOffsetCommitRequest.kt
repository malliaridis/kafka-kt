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
import java.util.*
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.UnsupportedVersionException
import org.apache.kafka.common.message.TxnOffsetCommitRequestData
import org.apache.kafka.common.message.TxnOffsetCommitRequestData.TxnOffsetCommitRequestPartition
import org.apache.kafka.common.message.TxnOffsetCommitRequestData.TxnOffsetCommitRequestTopic
import org.apache.kafka.common.message.TxnOffsetCommitResponseData
import org.apache.kafka.common.message.TxnOffsetCommitResponseData.TxnOffsetCommitResponsePartition
import org.apache.kafka.common.message.TxnOffsetCommitResponseData.TxnOffsetCommitResponseTopic
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.ByteBufferAccessor
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.record.RecordBatch

class TxnOffsetCommitRequest(
    private val data: TxnOffsetCommitRequestData,
    version: Short,
) : AbstractRequest(ApiKeys.TXN_OFFSET_COMMIT, version) {

    fun offsets(): Map<TopicPartition, CommittedOffset> {
        val topics = data.topics
        val offsetMap: MutableMap<TopicPartition, CommittedOffset> = HashMap()
        for (topic: TxnOffsetCommitRequestTopic in topics) {
            for (partition in topic.partitions) {
                offsetMap[TopicPartition(topic.name, partition.partitionIndex)] =
                    CommittedOffset(
                        offset = partition.committedOffset,
                        metadata = partition.committedMetadata,
                        leaderEpoch = RequestUtils.getLeaderEpoch(partition.committedLeaderEpoch),
                    )
            }
        }
        return offsetMap
    }

    override fun data(): TxnOffsetCommitRequestData {
        return data
    }

    override fun getErrorResponse(throttleTimeMs: Int, e: Throwable): TxnOffsetCommitResponse {
        val responseTopicData = getErrorResponseTopics(
            data.topics, Errors.forException(e)
        )
        return TxnOffsetCommitResponse(
            TxnOffsetCommitResponseData()
                .setThrottleTimeMs(throttleTimeMs)
                .setTopics(responseTopicData)
        )
    }

    data class CommittedOffset(
        val offset: Long,
        val metadata: String?,
        val leaderEpoch: Int?,
    ) {

        override fun toString(): String = "CommittedOffset(" +
                "offset=$offset" +
                ", leaderEpoch=$leaderEpoch" +
                ", metadata='$metadata" +
                "')"

    }

    class Builder(
        transactionalId: String,
        consumerGroupId: String,
        producerId: Long,
        producerEpoch: Short,
        pendingTxnOffsetCommits: Map<TopicPartition, CommittedOffset>,
        memberId: String = JoinGroupRequest.UNKNOWN_MEMBER_ID,
        generationId: Int = JoinGroupRequest.UNKNOWN_GENERATION_ID,
        groupInstanceId: String? = null,
    ) : AbstractRequest.Builder<TxnOffsetCommitRequest>(ApiKeys.TXN_OFFSET_COMMIT) {

        val data: TxnOffsetCommitRequestData

        init {
            data = TxnOffsetCommitRequestData()
                .setTransactionalId(transactionalId)
                .setGroupId(consumerGroupId)
                .setProducerId(producerId)
                .setProducerEpoch(producerEpoch)
                .setTopics(getTopics(pendingTxnOffsetCommits))
                .setMemberId(memberId)
                .setGenerationId(generationId)
                .setGroupInstanceId(groupInstanceId)
        }

        override fun build(version: Short): TxnOffsetCommitRequest {
            if (version < 3 && groupMetadataSet()) throw UnsupportedVersionException(
                "Broker doesn't support group metadata commit API on version $version" +
                        ", minimum supported request version is 3 which requires brokers to " +
                        "be on version 2.5 or above."
            )

            return TxnOffsetCommitRequest(data, version)
        }

        private fun groupMetadataSet(): Boolean {
            return data.memberId != JoinGroupRequest.UNKNOWN_MEMBER_ID || (
                    data.generationId != JoinGroupRequest.UNKNOWN_GENERATION_ID) || (
                    data.groupInstanceId != null)
        }

        override fun toString(): String = data.toString()
    }

    companion object {

        fun getTopics(
            pendingTxnOffsetCommits: Map<TopicPartition, CommittedOffset>,
        ): List<TxnOffsetCommitRequestTopic> {
            val topicPartitionMap =
                mutableMapOf<String, MutableList<TxnOffsetCommitRequestPartition>>()

            for ((topicPartition, offset) in pendingTxnOffsetCommits) {
                val partitions = topicPartitionMap.getOrDefault(topicPartition.topic, ArrayList())
                partitions.add(
                    TxnOffsetCommitRequestPartition()
                        .setPartitionIndex(topicPartition.partition)
                        .setCommittedOffset(offset.offset)
                        .setCommittedLeaderEpoch(
                            offset.leaderEpoch ?: RecordBatch.NO_PARTITION_LEADER_EPOCH
                        )
                        .setCommittedMetadata(offset.metadata)
                )
                topicPartitionMap[topicPartition.topic] = partitions
            }

            return topicPartitionMap.map { (key, value) ->
                TxnOffsetCommitRequestTopic()
                    .setName(key)
                    .setPartitions(value)
            }
        }

        fun getErrorResponseTopics(
            requestTopics: List<TxnOffsetCommitRequestTopic>,
            e: Errors,
        ): List<TxnOffsetCommitResponseTopic> {
            val responseTopicData = mutableListOf<TxnOffsetCommitResponseTopic>()

            for (entry: TxnOffsetCommitRequestTopic in requestTopics) {
                val responsePartitions = mutableListOf<TxnOffsetCommitResponsePartition>()

                for (requestPartition: TxnOffsetCommitRequestPartition in entry.partitions)
                    responsePartitions.add(
                        TxnOffsetCommitResponsePartition()
                            .setPartitionIndex(requestPartition.partitionIndex)
                            .setErrorCode(e.code)
                    )

                responseTopicData.add(
                    TxnOffsetCommitResponseTopic()
                        .setName(entry.name)
                        .setPartitions(responsePartitions)
                )
            }
            return responseTopicData
        }

        fun parse(buffer: ByteBuffer, version: Short): TxnOffsetCommitRequest =
            TxnOffsetCommitRequest(
                TxnOffsetCommitRequestData(ByteBufferAccessor(buffer), version),
                version
            )
    }
}
