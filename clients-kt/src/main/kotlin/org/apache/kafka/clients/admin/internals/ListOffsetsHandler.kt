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

import org.apache.kafka.clients.admin.ListOffsetsOptions
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo
import org.apache.kafka.clients.admin.internals.AdminApiFuture.SimpleAdminApiFuture
import org.apache.kafka.clients.admin.internals.AdminApiHandler.ApiResult
import org.apache.kafka.clients.admin.internals.AdminApiHandler.Batched
import org.apache.kafka.common.Node
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.ApiException
import org.apache.kafka.common.errors.RetriableException
import org.apache.kafka.common.errors.UnsupportedVersionException
import org.apache.kafka.common.message.ListOffsetsRequestData.ListOffsetsPartition
import org.apache.kafka.common.message.ListOffsetsRequestData.ListOffsetsTopic
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.AbstractResponse
import org.apache.kafka.common.requests.ListOffsetsRequest
import org.apache.kafka.common.requests.ListOffsetsResponse
import org.apache.kafka.common.utils.CollectionUtils.groupPartitionsByTopic
import org.apache.kafka.common.utils.LogContext
import org.slf4j.Logger

class ListOffsetsHandler(
    private val offsetTimestampsByPartition: Map<TopicPartition, Long>,
    private val options: ListOffsetsOptions,
    logContext: LogContext,
) : Batched<TopicPartition, ListOffsetsResultInfo>() {

    private val log: Logger

    private val lookupStrategy: AdminApiLookupStrategy<TopicPartition>

    init {
        log = logContext.logger(ListOffsetsHandler::class.java)
        lookupStrategy = PartitionLeaderStrategy(logContext, false)
    }

    override fun apiName(): String = "listOffsets"

    override fun lookupStrategy(): AdminApiLookupStrategy<TopicPartition> = lookupStrategy

    override fun buildBatchedRequest(brokerId: Int, keys: Set<TopicPartition>): ListOffsetsRequest.Builder {
        val topicsByName = groupPartitionsByTopic(
            partitions = keys,
            buildGroup = { topicName: String? ->
                ListOffsetsTopic().setName(
                    (topicName)!!
                )
            },
            addToGroup = { listOffsetsTopic: ListOffsetsTopic, partitionId ->
                val topicPartition = TopicPartition(listOffsetsTopic.name, partitionId)
                val offsetTimestamp: Long = offsetTimestampsByPartition[topicPartition]!!
                listOffsetsTopic.partitions += ListOffsetsPartition()
                    .setPartitionIndex((partitionId))
                    .setTimestamp(offsetTimestamp)
            },
        )
        val supportsMaxTimestamp = keys.any { key ->
            offsetTimestampsByPartition[key] == ListOffsetsRequest.MAX_TIMESTAMP
        }
        return ListOffsetsRequest.Builder
            .forConsumer(true, options.isolationLevel, supportsMaxTimestamp)
            .setTargetTimes(topicsByName.values.toList())
    }

    override fun handleResponse(
        broker: Node,
        keys: Set<TopicPartition>,
        response: AbstractResponse,
    ): ApiResult<TopicPartition, ListOffsetsResultInfo> {
        response as ListOffsetsResponse
        val completed = mutableMapOf<TopicPartition, ListOffsetsResultInfo>()
        val failed = mutableMapOf<TopicPartition, Throwable>()
        val unmapped = mutableListOf<TopicPartition>()
        val retriable = mutableSetOf<TopicPartition>()

        for (topic in response.topics) {
            for (partition in topic.partitions) {
                val topicPartition = TopicPartition(topic.name, partition.partitionIndex)
                val error = Errors.forCode(partition.errorCode)
                if (!offsetTimestampsByPartition.containsKey(topicPartition)) {
                    log.warn("ListOffsets response includes unknown topic partition {}", topicPartition)
                } else if (error == Errors.NONE) {
                    val leaderEpoch =
                        if (partition.leaderEpoch == ListOffsetsResponse.UNKNOWN_EPOCH) null
                        else partition.leaderEpoch
                    completed[topicPartition] = ListOffsetsResultInfo(
                        offset = partition.offset,
                        timestamp = partition.timestamp,
                        leaderEpoch = leaderEpoch,
                    )
                } else handlePartitionError(topicPartition, error, failed, unmapped, retriable)
            }
        }

        // Sanity-check if the current leader for these partitions returned results for all of them
        for (topicPartition in keys) {
            if (
                unmapped.isEmpty()
                && !completed.containsKey(topicPartition)
                && !failed.containsKey(topicPartition)
                && !retriable.contains(topicPartition)
            ) {
                val sanityCheckException = ApiException(
                    "The response from broker ${broker.id} did not contain a result for topic partition $topicPartition"
                )
                log.error(
                    "ListOffsets request for topic partition {} failed sanity check",
                    topicPartition,
                    sanityCheckException
                )
                failed[topicPartition] = sanityCheckException
            }
        }
        return ApiResult(completed, failed, unmapped)
    }

    private fun handlePartitionError(
        topicPartition: TopicPartition,
        error: Errors,
        failed: MutableMap<TopicPartition, Throwable>,
        unmapped: MutableList<TopicPartition>,
        retriable: MutableSet<TopicPartition>,
    ) {
        if (error == Errors.NOT_LEADER_OR_FOLLOWER || error == Errors.LEADER_NOT_AVAILABLE) {
            log.debug(
                "ListOffsets lookup request for topic partition {} will be retried due to invalid leader metadata {}",
                topicPartition,
                error
            )
            unmapped.add(topicPartition)
        } else if (error.exception is RetriableException) {
            log.debug(
                "ListOffsets fulfillment request for topic partition {} will be retried due to {}",
                topicPartition,
                error
            )
            retriable.add(topicPartition)
        } else {
            log.error(
                "ListOffsets request for topic partition {} failed due to an unexpected error {}",
                topicPartition,
                error
            )
            failed[topicPartition] = error.exception!!
        }
    }

    override fun handleUnsupportedVersionException(
        brokerId: Int,
        exception: UnsupportedVersionException,
        keys: Set<TopicPartition>,
    ): Map<TopicPartition, Throwable> {
        log.warn("Broker $brokerId does not support MAX_TIMESTAMP offset specs")
        val maxTimestampPartitions = mutableMapOf<TopicPartition, Throwable>()

        for (topicPartition in keys) {
            val offsetTimestamp = offsetTimestampsByPartition[topicPartition]
            if (offsetTimestamp == ListOffsetsRequest.MAX_TIMESTAMP) {
                maxTimestampPartitions[topicPartition] = exception
            }
        }
        // If there are no partitions with MAX_TIMESTAMP specs the UnsupportedVersionException cannot be handled
        // and all partitions should be failed here.
        // Otherwise, just the partitions with MAX_TIMESTAMP specs should be failed here and the fulfillment stage
        // will later be retried for the potentially empty set of partitions with non-MAX_TIMESTAMP specs.
        return if (maxTimestampPartitions.isEmpty()) keys.associateWith { exception }
        else maxTimestampPartitions
    }

    companion object {
        fun newFuture(
            topicPartitions: Collection<TopicPartition>,
        ): SimpleAdminApiFuture<TopicPartition, ListOffsetsResultInfo> {
            return AdminApiFuture.forKeys(topicPartitions.toSet())
        }
    }
}
