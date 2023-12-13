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

package org.apache.kafka.clients.admin.internals

import org.apache.kafka.clients.admin.internals.AdminApiFuture.SimpleAdminApiFuture
import org.apache.kafka.clients.admin.internals.AdminApiHandler.ApiResult
import org.apache.kafka.clients.admin.internals.AdminApiHandler.Batched
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.Node
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.message.OffsetCommitRequestData
import org.apache.kafka.common.message.OffsetCommitRequestData.OffsetCommitRequestPartition
import org.apache.kafka.common.message.OffsetCommitRequestData.OffsetCommitRequestTopic
import org.apache.kafka.common.message.OffsetCommitResponseData.OffsetCommitResponsePartition
import org.apache.kafka.common.message.OffsetCommitResponseData.OffsetCommitResponseTopic
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.AbstractResponse
import org.apache.kafka.common.requests.FindCoordinatorRequest.CoordinatorType
import org.apache.kafka.common.requests.OffsetCommitRequest
import org.apache.kafka.common.requests.OffsetCommitResponse
import org.apache.kafka.common.utils.LogContext
import org.slf4j.Logger

class AlterConsumerGroupOffsetsHandler(
    groupId: String,
    private val offsets: Map<TopicPartition, OffsetAndMetadata>,
    logContext: LogContext
) : Batched<CoordinatorKey, Map<TopicPartition, Errors>>() {

    private val groupId: CoordinatorKey = CoordinatorKey.byGroupId(groupId)

    private val log: Logger = logContext.logger(AlterConsumerGroupOffsetsHandler::class.java)

    private val lookupStrategy: AdminApiLookupStrategy<CoordinatorKey> =
        CoordinatorStrategy(CoordinatorType.GROUP, logContext)

    override fun apiName(): String = "offsetCommit"

    override fun lookupStrategy(): AdminApiLookupStrategy<CoordinatorKey> = lookupStrategy

    private fun validateKeys(groupIds: Set<CoordinatorKey>) {
        require(groupIds == setOf(groupId)) {
            "Received unexpected group ids $groupIds (expected only ${setOf(groupId)})"
        }
    }

    override fun buildBatchedRequest(
        brokerId: Int,
        keys: Set<CoordinatorKey>,
    ): OffsetCommitRequest.Builder {
        validateKeys(keys)
        val offsetData: MutableMap<String, OffsetCommitRequestTopic> = HashMap()
        offsets.forEach { (topicPartition, offsetAndMetadata) ->
            offsetData.computeIfAbsent(topicPartition.topic) {
                OffsetCommitRequestTopic().setName(topicPartition.topic)
            }.partitions += OffsetCommitRequestPartition()
                .setCommittedOffset(offsetAndMetadata.offset)
                .setCommittedLeaderEpoch(offsetAndMetadata.leaderEpoch() ?: -1)
                .setCommittedMetadata(offsetAndMetadata.metadata)
                .setPartitionIndex(topicPartition.partition)
        }
        val data = OffsetCommitRequestData()
            .setGroupId(groupId.idValue)
            .setTopics(offsetData.values.toList())

        return OffsetCommitRequest.Builder(data)
    }

    override fun handleResponse(
        broker: Node,
        keys: Set<CoordinatorKey>,
        response: AbstractResponse
    ): ApiResult<CoordinatorKey, Map<TopicPartition, Errors>> {
        validateKeys(keys)
        response as OffsetCommitResponse
        val groupsToUnmap: MutableSet<CoordinatorKey> = hashSetOf()
        val groupsToRetry: MutableSet<CoordinatorKey> = hashSetOf()
        val partitionResults: MutableMap<TopicPartition, Errors> = hashMapOf()
        for (topic: OffsetCommitResponseTopic in response.data().topics) {
            for (partition: OffsetCommitResponsePartition in topic.partitions) {
                val topicPartition = TopicPartition(topic.name, partition.partitionIndex)
                val error = Errors.forCode(partition.errorCode)
                if (error !== Errors.NONE) {
                    handleError(
                        groupId = groupId,
                        topicPartition = topicPartition,
                        error = error,
                        partitionResults = partitionResults,
                        groupsToUnmap = groupsToUnmap,
                        groupsToRetry = groupsToRetry,
                    )
                } else partitionResults[topicPartition] = error
            }
        }
        return if (groupsToUnmap.isEmpty() && groupsToRetry.isEmpty())
            ApiResult.completed(groupId, partitionResults)
        else ApiResult.unmapped(groupsToUnmap.toList())
    }

    private fun handleError(
        groupId: CoordinatorKey,
        topicPartition: TopicPartition,
        error: Errors,
        partitionResults: MutableMap<TopicPartition, Errors>,
        groupsToUnmap: MutableSet<CoordinatorKey>,
        groupsToRetry: MutableSet<CoordinatorKey>
    ) {
        when (error) {
            Errors.COORDINATOR_LOAD_IN_PROGRESS, Errors.REBALANCE_IN_PROGRESS -> {
                log.debug(
                    "OffsetCommit request for group id {} returned error {}. Will retry.",
                    groupId.idValue, error
                )
                groupsToRetry.add(groupId)
            }

            Errors.COORDINATOR_NOT_AVAILABLE, Errors.NOT_COORDINATOR -> {
                log.debug(
                    "OffsetCommit request for group id {} returned error {}. Will rediscover the " +
                            "coordinator and retry.",
                    groupId.idValue, error
                )
                groupsToUnmap.add(groupId)
            }

            Errors.INVALID_GROUP_ID,
            Errors.INVALID_COMMIT_OFFSET_SIZE,
            Errors.GROUP_AUTHORIZATION_FAILED,
            Errors.UNKNOWN_MEMBER_ID -> {
                log.debug(
                    "OffsetCommit request for group id {} failed due to error {}.",
                    groupId.idValue, error
                )
                partitionResults[topicPartition] = error
            }

            Errors.UNKNOWN_TOPIC_OR_PARTITION,
            Errors.OFFSET_METADATA_TOO_LARGE,
            Errors.TOPIC_AUTHORIZATION_FAILED -> {
                log.debug(
                    "OffsetCommit request for group id {} and partition {} failed due" +
                            " to error {}.", groupId.idValue, topicPartition, error
                )
                partitionResults[topicPartition] = error
            }

            else -> {
                log.error(
                    "OffsetCommit request for group id {} and partition {} failed due" +
                            " to unexpected error {}.", groupId.idValue, topicPartition, error
                )
                partitionResults[topicPartition] = error
            }
        }
    }

    companion object {
        fun newFuture(groupId: String): SimpleAdminApiFuture<CoordinatorKey, Map<TopicPartition, Errors>> {
            return AdminApiFuture.forKeys(setOf(CoordinatorKey.byGroupId(groupId)))
        }
    }
}
