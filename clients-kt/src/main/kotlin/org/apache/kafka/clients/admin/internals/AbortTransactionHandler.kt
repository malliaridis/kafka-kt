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

import org.apache.kafka.clients.admin.AbortTransactionSpec
import org.apache.kafka.clients.admin.internals.AdminApiFuture.SimpleAdminApiFuture
import org.apache.kafka.clients.admin.internals.AdminApiHandler.ApiResult
import org.apache.kafka.clients.admin.internals.AdminApiHandler.Batched
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.Node
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.ClusterAuthorizationException
import org.apache.kafka.common.errors.InvalidProducerEpochException
import org.apache.kafka.common.errors.TransactionCoordinatorFencedException
import org.apache.kafka.common.message.WriteTxnMarkersRequestData
import org.apache.kafka.common.message.WriteTxnMarkersRequestData.WritableTxnMarker
import org.apache.kafka.common.message.WriteTxnMarkersRequestData.WritableTxnMarkerTopic
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.AbstractResponse
import org.apache.kafka.common.requests.WriteTxnMarkersRequest
import org.apache.kafka.common.requests.WriteTxnMarkersResponse
import org.apache.kafka.common.utils.LogContext
import org.slf4j.Logger

class AbortTransactionHandler(
    private val abortSpec: AbortTransactionSpec,
    logContext: LogContext
) : Batched<TopicPartition, Unit>() {
    private val log: Logger = logContext.logger(AbortTransactionHandler::class.java)
    private val lookupStrategy: PartitionLeaderStrategy = PartitionLeaderStrategy(logContext)

    override fun apiName(): String {
        return "abortTransaction"
    }

    override fun lookupStrategy(): AdminApiLookupStrategy<TopicPartition> {
        return lookupStrategy
    }

    override fun buildBatchedRequest(
        brokerId: Int,
        keys: Set<TopicPartition>
    ): WriteTxnMarkersRequest.Builder {
        validateTopicPartitions(keys)
        val marker = WritableTxnMarker()
            .setCoordinatorEpoch(abortSpec.coordinatorEpoch)
            .setProducerEpoch(abortSpec.producerEpoch)
            .setProducerId(abortSpec.producerId)
            .setTransactionResult(false)
        marker.topics().add(
            WritableTxnMarkerTopic()
                .setName(abortSpec.topicPartition.topic)
                .setPartitionIndexes(listOf(abortSpec.topicPartition.partition))
        )
        val request = WriteTxnMarkersRequestData()
        request.markers().add(marker)
        return WriteTxnMarkersRequest.Builder(request)
    }

    override fun handleResponse(
        broker: Node,
        keys: Set<TopicPartition>,
        response: AbstractResponse
    ): ApiResult<TopicPartition, Unit> {
        validateTopicPartitions(keys)
        response as WriteTxnMarkersResponse
        val markerResponses = response.data().markers()
        if (markerResponses.size != 1 || markerResponses[0].producerId() != abortSpec.producerId) {
            return ApiResult.failed(
                abortSpec.topicPartition, KafkaException(
                    "WriteTxnMarkers response " +
                            "included unexpected marker entries: $markerResponses(expected to find exactly one " +
                            "entry with producerId ${abortSpec.producerId})"
                )
            )
        }
        val markerResponse = markerResponses[0]
        val topicResponses = markerResponse.topics()
        if (topicResponses.size != 1
            || topicResponses[0].name() != abortSpec.topicPartition.topic) {
            return ApiResult.failed(
                abortSpec.topicPartition, KafkaException(
                    ("WriteTxnMarkers response " +
                            "included unexpected topic entries: $markerResponses(expected to find exactly one " +
                            "entry with topic partition ${abortSpec.topicPartition})")
                )
            )
        }
        val topicResponse = topicResponses[0]
        val partitionResponses = topicResponse.partitions()
        if (partitionResponses.size != 1
            || partitionResponses[0].partitionIndex() != abortSpec.topicPartition.partition
        ) {
            return ApiResult.failed(
                abortSpec.topicPartition, KafkaException(
                    "WriteTxnMarkers response included unexpected partition entries for topic" +
                            " ${abortSpec.topicPartition.topic}:$markerResponses(expected to find " +
                            "exactly one entry with partition ${abortSpec.topicPartition.partition})"
                )
            )
        }
        val partitionResponse = partitionResponses[0]
        val error = Errors.forCode(partitionResponse.errorCode())
        return if (error != Errors.NONE) handleError(error)
        else ApiResult.completed(abortSpec.topicPartition, Unit)
    }

    private fun handleError(error: Errors): ApiResult<TopicPartition, Unit> = when (error) {
        Errors.CLUSTER_AUTHORIZATION_FAILED -> {
            log.error(
                "WriteTxnMarkers request for abort spec {} failed cluster authorization",
                abortSpec
            )
            ApiResult.failed(
                abortSpec.topicPartition, ClusterAuthorizationException(
                    ("WriteTxnMarkers request with $abortSpec failed due to cluster " +
                            "authorization error")
                )
            )
        }

        Errors.INVALID_PRODUCER_EPOCH -> {
            log.error(
                "WriteTxnMarkers request for abort spec {} failed due to an invalid producer epoch",
                abortSpec
            )
            ApiResult.failed(
                abortSpec.topicPartition, InvalidProducerEpochException(
                    "WriteTxnMarkers request with $abortSpec failed due an invalid producer epoch"
                )
            )
        }

        Errors.TRANSACTION_COORDINATOR_FENCED -> {
            log.error(
                "WriteTxnMarkers request for abort spec {} failed because the coordinator epoch is fenced",
                abortSpec
            )
            ApiResult.failed(
                abortSpec.topicPartition, TransactionCoordinatorFencedException(
                    ("WriteTxnMarkers request with $abortSpec failed since the provided " +
                            "coordinator epoch ${abortSpec.coordinatorEpoch} has been fenced " +
                            "by the active coordinator")
                )
            )
        }

        Errors.NOT_LEADER_OR_FOLLOWER, Errors.REPLICA_NOT_AVAILABLE, Errors.BROKER_NOT_AVAILABLE, Errors.UNKNOWN_TOPIC_OR_PARTITION -> {
            log.debug(
                "WriteTxnMarkers request for abort spec {} failed due to {}. Will retry after attempting to " +
                        "find the leader again", abortSpec, error
            )
            ApiResult.unmapped(listOf(abortSpec.topicPartition))
        }

        else -> {
            log.error(
                "WriteTxnMarkers request for abort spec {} failed due to an unexpected error {}",
                abortSpec, error
            )
            ApiResult.failed(
                abortSpec.topicPartition, error.exception(
                    "WriteTxnMarkers request with $abortSpec failed due to unexpected error: " + error.message
                )
            )
        }
    }

    private fun validateTopicPartitions(topicPartitions: Set<TopicPartition>) {
        require(topicPartitions == setOf(abortSpec.topicPartition)) {
            "Received unexpected topic partitions $topicPartitions (expected only ${setOf(abortSpec.topicPartition)})"
        }
    }

    companion object {

        fun newFuture(
            topicPartitions: Set<TopicPartition>,
        ): SimpleAdminApiFuture<TopicPartition, Unit> {
            return AdminApiFuture.forKeys(topicPartitions)
        }
    }
}
