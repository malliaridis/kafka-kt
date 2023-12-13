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

import org.apache.kafka.clients.admin.DescribeProducersOptions
import org.apache.kafka.clients.admin.DescribeProducersResult.PartitionProducerState
import org.apache.kafka.clients.admin.ProducerState
import org.apache.kafka.clients.admin.internals.AdminApiFuture.SimpleAdminApiFuture
import org.apache.kafka.clients.admin.internals.AdminApiHandler.ApiResult
import org.apache.kafka.clients.admin.internals.AdminApiHandler.Batched
import org.apache.kafka.common.Node
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.InvalidTopicException
import org.apache.kafka.common.errors.TopicAuthorizationException
import org.apache.kafka.common.message.DescribeProducersRequestData
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.AbstractResponse
import org.apache.kafka.common.requests.ApiError
import org.apache.kafka.common.requests.DescribeProducersRequest
import org.apache.kafka.common.requests.DescribeProducersResponse
import org.apache.kafka.common.utils.CollectionUtils
import org.apache.kafka.common.utils.LogContext
import org.slf4j.Logger

class DescribeProducersHandler(
    private val options: DescribeProducersOptions,
    logContext: LogContext
) : Batched<TopicPartition, PartitionProducerState>() {

    private val log: Logger = logContext.logger(DescribeProducersHandler::class.java)

    private var lookupStrategy: AdminApiLookupStrategy<TopicPartition> =
        options.brokerId?.let { StaticBrokerStrategy(it) }
            ?: PartitionLeaderStrategy(logContext)

    override fun apiName(): String {
        return "describeProducers"
    }

    override fun lookupStrategy(): AdminApiLookupStrategy<TopicPartition> {
        return lookupStrategy
    }

    override fun buildBatchedRequest(
        brokerId: Int,
        keys: Set<TopicPartition>
    ): DescribeProducersRequest.Builder {
        val request = DescribeProducersRequestData()
        val builder = DescribeProducersRequest.Builder(request)

        CollectionUtils.groupPartitionsByTopic(
            keys, { topic -> builder.addTopic(topic) }
        ) { topicRequest, partitionId ->
            topicRequest.partitionIndexes += partitionId
        }

        return builder
    }

    private fun handlePartitionError(
        topicPartition: TopicPartition,
        apiError: ApiError,
        failed: MutableMap<TopicPartition, Throwable>,
        unmapped: MutableList<TopicPartition>
    ) {
        when (apiError.error) {
            Errors.NOT_LEADER_OR_FOLLOWER -> options.brokerId?.let { brokerId ->
                // Typically these errors are retriable, but if the user specified the brokerId
                // explicitly, then they are fatal.
                log.error(
                    "Not leader error in `DescribeProducers` response for partition {} " +
                            "for brokerId {} set in options",
                    topicPartition,
                    brokerId,
                    apiError.exception()
                )
                failed[topicPartition] = apiError.error.exception(
                    "Failed to describe active producers for partition $topicPartition on brokerId $brokerId"
                )!!
            } ?: run {
                // Otherwise, we unmap the partition so that we can find the new leader
                log.debug(
                    "Not leader error in `DescribeProducers` response for partition {}. " +
                            "Will retry later.", topicPartition
                )
                unmapped.add(topicPartition)
            }

            Errors.UNKNOWN_TOPIC_OR_PARTITION -> log.debug(
                "Unknown topic/partition error in `DescribeProducers` response for partition {}. " +
                        "Will retry later.", topicPartition
            )

            Errors.INVALID_TOPIC_EXCEPTION -> {
                log.error(
                    "Invalid topic in `DescribeProducers` response for partition {}",
                    topicPartition,
                    apiError.exception(),
                )
                failed[topicPartition] = InvalidTopicException(
                    message = ("Failed to fetch metadata for partition $topicPartition due to invalid" +
                            " topic error: ${apiError.messageWithFallback()}"),
                    invalidTopics = setOf(topicPartition.topic),
                )
            }

            Errors.TOPIC_AUTHORIZATION_FAILED -> {
                log.error(
                    "Authorization failed in `DescribeProducers` response for partition {}",
                    topicPartition, apiError.exception()
                )
                failed[topicPartition] = TopicAuthorizationException(
                    ("Failed to describe " +
                            "active producers for partition $topicPartition due to authorization" +
                            " failure on topic `${topicPartition.topic}`"),
                    setOf(topicPartition.topic),
                )
            }

            else -> {
                log.error(
                    "Unexpected error in `DescribeProducers` response for partition {}",
                    topicPartition,
                    apiError.exception(),
                )
                failed[topicPartition] = apiError.error.exception(
                    "Failed to describe active producers for partition $topicPartition due to unexpected error"
                )!!
            }
        }
    }

    override fun handleResponse(
        broker: Node,
        keys: Set<TopicPartition>,
        response: AbstractResponse,
    ): ApiResult<TopicPartition, PartitionProducerState> {

        response as DescribeProducersResponse
        val completed: MutableMap<TopicPartition, PartitionProducerState> = HashMap()
        val failed: MutableMap<TopicPartition, Throwable> = HashMap()
        val unmapped: MutableList<TopicPartition> = ArrayList()

        response.data().topics.forEach { topicResponse ->

            topicResponse.partitions.forEach innerLoop@{ partitionResponse ->

                val topicPartition = TopicPartition(
                    topic = topicResponse.name,
                    partition = partitionResponse.partitionIndex,
                )

                val error = Errors.forCode(partitionResponse.errorCode)
                if (error !== Errors.NONE) {
                    val apiError = ApiError(error, partitionResponse.errorMessage)
                    handlePartitionError(topicPartition, apiError, failed, unmapped)
                    return@innerLoop
                }

                val activeProducers = partitionResponse.activeProducers.map { activeProducer ->

                    val currentTransactionFirstOffset: Long? =
                        if (activeProducer.currentTxnStartOffset < 0) null
                        else activeProducer.currentTxnStartOffset

                    val coordinatorEpoch: Int? =
                        if (activeProducer.coordinatorEpoch < 0) null
                        else activeProducer.coordinatorEpoch

                    ProducerState(
                        producerId = activeProducer.producerId,
                        producerEpoch = activeProducer.producerEpoch,
                        lastSequence = activeProducer.lastSequence,
                        lastTimestamp = activeProducer.lastTimestamp,
                        coordinatorEpoch = coordinatorEpoch,
                        currentTransactionStartOffset = currentTransactionFirstOffset
                    )
                }

                completed[topicPartition] = PartitionProducerState(activeProducers)
            }
        }
        return ApiResult(completed, failed, unmapped)
    }

    companion object {
        fun newFuture(
            topicPartitions: Collection<TopicPartition>
        ): SimpleAdminApiFuture<TopicPartition, PartitionProducerState> {
            return AdminApiFuture.forKeys(topicPartitions.toHashSet())
        }
    }
}
