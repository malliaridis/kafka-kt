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
import org.apache.kafka.common.Node
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.message.OffsetDeleteRequestData
import org.apache.kafka.common.message.OffsetDeleteRequestData.OffsetDeleteRequestPartition
import org.apache.kafka.common.message.OffsetDeleteRequestData.OffsetDeleteRequestTopic
import org.apache.kafka.common.message.OffsetDeleteRequestData.OffsetDeleteRequestTopicCollection
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.AbstractResponse
import org.apache.kafka.common.requests.FindCoordinatorRequest.CoordinatorType
import org.apache.kafka.common.requests.OffsetDeleteRequest
import org.apache.kafka.common.requests.OffsetDeleteResponse
import org.apache.kafka.common.utils.LogContext
import org.slf4j.Logger

class DeleteConsumerGroupOffsetsHandler(
    groupId: String?,
    private val partitions: Set<TopicPartition>,
    logContext: LogContext
) : Batched<CoordinatorKey, Map<TopicPartition, Errors>>() {

    private val groupId: CoordinatorKey = CoordinatorKey.byGroupId(groupId!!)

    private val log: Logger = logContext.logger(DeleteConsumerGroupOffsetsHandler::class.java)

    private val lookupStrategy: AdminApiLookupStrategy<CoordinatorKey> =
        CoordinatorStrategy(CoordinatorType.GROUP, logContext)

    override fun apiName(): String = "offsetDelete"

    override fun lookupStrategy(): AdminApiLookupStrategy<CoordinatorKey> = lookupStrategy

    private fun validateKeys(groupIds: Set<CoordinatorKey>) {
        require(groupIds == setOf(groupId)) {
            "Received unexpected group ids $groupIds (expected only ${setOf(groupId)})"
        }
    }

    override fun buildBatchedRequest(
        brokerId: Int,
        keys: Set<CoordinatorKey>,
    ): OffsetDeleteRequest.Builder {
        validateKeys(keys)

        val topics = OffsetDeleteRequestTopicCollection()
        partitions.groupBy { it.topic }
            .forEach { (topic, topicPartitions) ->
                topics.add(
                    OffsetDeleteRequestTopic()
                        .setName(topic)
                        .setPartitions(
                            topicPartitions.map { tp: TopicPartition ->
                                OffsetDeleteRequestPartition().setPartitionIndex(tp.partition)
                            }
                        )
                )
            }

        return OffsetDeleteRequest.Builder(
            OffsetDeleteRequestData()
                .setGroupId(groupId.idValue)
                .setTopics(topics)
        )
    }

    override fun handleResponse(
        broker: Node,
        keys: Set<CoordinatorKey>,
        response: AbstractResponse
    ): ApiResult<CoordinatorKey, Map<TopicPartition, Errors>> {
        validateKeys(keys)
        response as OffsetDeleteResponse

        val error = Errors.forCode(response.data().errorCode)
        if (error !== Errors.NONE) {
            val failed: MutableMap<CoordinatorKey, Throwable> = HashMap()
            val groupsToUnmap: MutableSet<CoordinatorKey> = hashSetOf()

            handleGroupError(groupId, error, failed, groupsToUnmap)

            return ApiResult(emptyMap(), failed, groupsToUnmap.toList())
        } else {
            val partitionResults: MutableMap<TopicPartition, Errors> = HashMap()
            response.data().topics.forEach { topic ->
                topic.partitions.forEach { partition ->
                    partitionResults[
                        TopicPartition(topic.name, partition.partitionIndex)
                    ] = Errors.forCode(partition.errorCode)
                }
            }
            return ApiResult.completed(groupId, partitionResults)
        }
    }

    private fun handleGroupError(
        groupId: CoordinatorKey,
        error: Errors,
        failed: MutableMap<CoordinatorKey, Throwable>,
        groupsToUnmap: MutableSet<CoordinatorKey>
    ) {
        when (error) {
            Errors.GROUP_AUTHORIZATION_FAILED,
            Errors.GROUP_ID_NOT_FOUND,
            Errors.INVALID_GROUP_ID,
            Errors.NON_EMPTY_GROUP -> {
                log.debug(
                    "`OffsetDelete` request for group id {} failed due to error {}.",
                    groupId.idValue,
                    error,
                )
                failed[groupId] = error.exception!!
            }

            Errors.COORDINATOR_LOAD_IN_PROGRESS ->
                // If the coordinator is in the middle of loading, then we just need to retry
                log.debug(
                    "`OffsetDelete` request for group id {} failed because the coordinator" +
                            " is still in the process of loading state. Will retry.",
                    groupId.idValue
                )

            Errors.COORDINATOR_NOT_AVAILABLE,
            Errors.NOT_COORDINATOR -> {
                // If the coordinator is unavailable or there was a coordinator change, then we
                // unmap the key so that we retry the `FindCoordinator` request
                log.debug(
                    "`OffsetDelete` request for group id {} returned error {}. " +
                            "Will attempt to find the coordinator again and retry.",
                    groupId.idValue,
                    error
                )
                groupsToUnmap.add(groupId)
            }

            else -> {
                log.error(
                    "`OffsetDelete` request for group id {} failed due to unexpected error {}.",
                    groupId.idValue,
                    error
                )
                failed[groupId] = error.exception!!
            }
        }
    }

    companion object {
        fun newFuture(groupId: String): SimpleAdminApiFuture<CoordinatorKey, Map<TopicPartition, Errors>> {
            return AdminApiFuture.forKeys(setOf(CoordinatorKey.byGroupId((groupId))))
        }
    }
}
