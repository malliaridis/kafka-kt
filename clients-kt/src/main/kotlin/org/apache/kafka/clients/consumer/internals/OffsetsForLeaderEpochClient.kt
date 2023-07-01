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

package org.apache.kafka.clients.consumer.internals

import org.apache.kafka.clients.consumer.internals.OffsetsForLeaderEpochClient.OffsetForEpochResult
import org.apache.kafka.clients.consumer.internals.SubscriptionState.FetchPosition
import org.apache.kafka.common.Node
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.TopicAuthorizationException
import org.apache.kafka.common.message.OffsetForLeaderEpochRequestData.OffsetForLeaderPartition
import org.apache.kafka.common.message.OffsetForLeaderEpochRequestData.OffsetForLeaderTopic
import org.apache.kafka.common.message.OffsetForLeaderEpochRequestData.OffsetForLeaderTopicCollection
import org.apache.kafka.common.message.OffsetForLeaderEpochResponseData
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.record.RecordBatch
import org.apache.kafka.common.requests.AbstractRequest
import org.apache.kafka.common.requests.OffsetsForLeaderEpochRequest
import org.apache.kafka.common.requests.OffsetsForLeaderEpochResponse
import org.apache.kafka.common.utils.LogContext

/**
 * Convenience class for making asynchronous requests to the OffsetsForLeaderEpoch API
 */
class OffsetsForLeaderEpochClient internal constructor(
    client: ConsumerNetworkClient,
    logContext: LogContext,
) : AsyncClient<
        Map<TopicPartition, FetchPosition>,
        OffsetsForLeaderEpochRequest,
        OffsetsForLeaderEpochResponse,
        OffsetForEpochResult
        >(
    client = client,
    logContext = logContext,
) {

    override fun prepareRequest(
        node: Node,
        requestData: Map<TopicPartition, FetchPosition>,
    ): AbstractRequest.Builder<OffsetsForLeaderEpochRequest> {
        val topics = OffsetForLeaderTopicCollection(requestData.size)

        requestData.forEach { (topicPartition, fetchPosition) ->
            fetchPosition.offsetEpoch?.let { fetchEpoch ->
                var topic = topics.find(topicPartition.topic)
                if (topic == null) {
                    topic = OffsetForLeaderTopic().setTopic(topicPartition.topic)
                    topics.add(topic)
                }
                topic.partitions().add(
                    OffsetForLeaderPartition()
                        .setPartition(topicPartition.partition)
                        .setLeaderEpoch(fetchEpoch)
                        .setCurrentLeaderEpoch(
                            fetchPosition.currentLeader.epoch
                                ?: RecordBatch.NO_PARTITION_LEADER_EPOCH
                        )
                )
            }
        }
        return OffsetsForLeaderEpochRequest.Builder.forConsumer(topics)
    }

    override fun handleResponse(
        node: Node,
        requestData: Map<TopicPartition, FetchPosition>,
        response: OffsetsForLeaderEpochResponse?,
    ): OffsetForEpochResult {
        val partitionsToRetry = requestData.keys.toMutableList()
        val unauthorizedTopics = mutableSetOf<String>()
        val endOffsets =
            mutableMapOf<TopicPartition, OffsetForLeaderEpochResponseData.EpochEndOffset>()

        for (topic in response!!.data().topics()) {
            for (partition in topic.partitions()) {
                val topicPartition = TopicPartition(topic.topic(), partition.partition())
                if (!requestData.containsKey(topicPartition)) {
                    logger().warn(
                        "Received unrequested topic or partition {} from response, ignoring.",
                        topicPartition
                    )
                    continue
                }
                when (val error = Errors.forCode(partition.errorCode())) {
                    Errors.NONE -> {
                        logger().debug(
                            "Handling OffsetsForLeaderEpoch response for {}. Got offset {} for epoch {}.",
                            topicPartition,
                            partition.endOffset(),
                            partition.leaderEpoch(),
                        )
                        endOffsets[topicPartition] = partition
                        partitionsToRetry.remove(topicPartition)
                    }

                    Errors.NOT_LEADER_OR_FOLLOWER,
                    Errors.REPLICA_NOT_AVAILABLE,
                    Errors.KAFKA_STORAGE_ERROR,
                    Errors.OFFSET_NOT_AVAILABLE,
                    Errors.LEADER_NOT_AVAILABLE,
                    Errors.FENCED_LEADER_EPOCH,
                    Errors.UNKNOWN_LEADER_EPOCH -> logger().debug(
                        "Attempt to fetch offsets for partition {} failed due to {}, retrying.",
                        topicPartition,
                        error,
                    )

                    Errors.UNKNOWN_TOPIC_OR_PARTITION -> logger().warn(
                        "Received unknown topic or partition error in OffsetsForLeaderEpoch " +
                                "request for partition {}.",
                        topicPartition,
                    )

                    Errors.TOPIC_AUTHORIZATION_FAILED -> {
                        unauthorizedTopics.add(topicPartition.topic)
                        partitionsToRetry.remove(topicPartition)
                    }

                    else -> logger().warn(
                        "Attempt to fetch offsets for partition {} failed due to: {}, retrying.",
                        topicPartition,
                        error.message,
                    )
                }
            }
        }

        return if (unauthorizedTopics.isNotEmpty())
            throw TopicAuthorizationException(unauthorizedTopics)
        else OffsetForEpochResult(endOffsets, partitionsToRetry.toSet())
    }

    data class OffsetForEpochResult internal constructor(
        val endOffsets: Map<TopicPartition, OffsetForLeaderEpochResponseData.EpochEndOffset>,
        val partitionsToRetry: Set<TopicPartition>,
    ) {

        @Deprecated(
            message = "User property instead",
            replaceWith = ReplaceWith("endOffsets"),
        )
        fun endOffsets(): Map<TopicPartition, OffsetForLeaderEpochResponseData.EpochEndOffset> =
            endOffsets

        @Deprecated(
            message = "User property instead",
            replaceWith = ReplaceWith("partitionsToRetry"),
        )
        fun partitionsToRetry(): Set<TopicPartition> = partitionsToRetry
    }
}
