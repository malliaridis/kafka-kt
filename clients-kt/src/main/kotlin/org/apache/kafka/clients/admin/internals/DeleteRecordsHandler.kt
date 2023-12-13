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

import org.apache.kafka.clients.admin.DeletedRecords
import org.apache.kafka.clients.admin.RecordsToDelete
import org.apache.kafka.clients.admin.internals.AdminApiFuture.SimpleAdminApiFuture
import org.apache.kafka.clients.admin.internals.AdminApiHandler.ApiResult
import org.apache.kafka.clients.admin.internals.AdminApiHandler.Batched
import org.apache.kafka.common.Node
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.ApiException
import org.apache.kafka.common.errors.InvalidMetadataException
import org.apache.kafka.common.errors.RetriableException
import org.apache.kafka.common.errors.TopicAuthorizationException
import org.apache.kafka.common.message.DeleteRecordsRequestData
import org.apache.kafka.common.message.DeleteRecordsRequestData.DeleteRecordsPartition
import org.apache.kafka.common.message.DeleteRecordsRequestData.DeleteRecordsTopic
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.AbstractResponse
import org.apache.kafka.common.requests.DeleteRecordsRequest
import org.apache.kafka.common.requests.DeleteRecordsResponse
import org.apache.kafka.common.utils.LogContext
import org.slf4j.Logger

class DeleteRecordsHandler(
    private val recordsToDelete: Map<TopicPartition, RecordsToDelete>,
    logContext: LogContext,
    private val timeout: Int,
) : Batched<TopicPartition, DeletedRecords>() {

    private val log: Logger

    private val lookupStrategy: AdminApiLookupStrategy<TopicPartition>

    init {
        log = logContext.logger(DeleteRecordsHandler::class.java)
        lookupStrategy = PartitionLeaderStrategy(logContext)
    }

    override fun apiName(): String {
        return "deleteRecords"
    }

    override fun lookupStrategy(): AdminApiLookupStrategy<TopicPartition> = lookupStrategy

    override fun buildBatchedRequest(brokerId: Int, keys: Set<TopicPartition>): DeleteRecordsRequest.Builder {
        val deletionsForTopic = mutableMapOf<String, DeleteRecordsTopic>()
        for ((topicPartition, recordsToDelete) in recordsToDelete.entries) {
            val deleteRecords = deletionsForTopic.computeIfAbsent(
                topicPartition.topic
            ) { DeleteRecordsTopic().setName(topicPartition.topic) }

            deleteRecords.partitions += DeleteRecordsPartition()
                .setPartitionIndex(topicPartition.partition)
                .setOffset(recordsToDelete.beforeOffset)
        }
        val data = DeleteRecordsRequestData()
            .setTopics(deletionsForTopic.values.toList())
            .setTimeoutMs(timeout)
        return DeleteRecordsRequest.Builder(data)
    }

    override fun handleResponse(
        broker: Node,
        keys: Set<TopicPartition>,
        response: AbstractResponse,
    ): ApiResult<TopicPartition, DeletedRecords> {
        response as DeleteRecordsResponse
        val completed = mutableMapOf<TopicPartition, DeletedRecords>()
        val failed = mutableMapOf<TopicPartition, Throwable>()
        val unmapped = mutableListOf<TopicPartition>()
        val retriable = mutableSetOf<TopicPartition>()

        for (topicResult in response.data().topics) {
            for (partitionResult in topicResult.partitions) {
                val error = Errors.forCode(partitionResult.errorCode)
                val topicPartition = TopicPartition(topicResult.name, partitionResult.partitionIndex)

                if (error == Errors.NONE) completed[topicPartition] = DeletedRecords(partitionResult.lowWatermark)
                else handlePartitionError(topicPartition, error, failed, unmapped, retriable)
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
                    "DeleteRecords request for topic partition {} failed sanity check",
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
        when (error.exception) {
            is InvalidMetadataException -> {
                log.debug(
                    "DeleteRecords lookup request for topic partition {} will be retried due to invalid leader metadata {}",
                    topicPartition,
                    error
                )
                unmapped.add(topicPartition)
            }

            is RetriableException -> {
                log.debug(
                    "DeleteRecords fulfillment request for topic partition {} will be retried due to {}",
                    topicPartition,
                    error
                )
                retriable.add(topicPartition)
            }

            is TopicAuthorizationException -> {
                log.error(
                    "DeleteRecords request for topic partition {} failed due to an error {}",
                    topicPartition,
                    error
                )
                failed[topicPartition] = error.exception!!
            }

            else -> {
                log.error(
                    "DeleteRecords request for topic partition {} failed due to an unexpected error {}",
                    topicPartition,
                    error
                )
                failed[topicPartition] = error.exception!!
            }
        }
    }

    companion object {
        fun newFuture(
            topicPartitions: Collection<TopicPartition>,
        ): SimpleAdminApiFuture<TopicPartition, DeletedRecords> {
            return AdminApiFuture.forKeys(topicPartitions.toSet())
        }
    }
}
