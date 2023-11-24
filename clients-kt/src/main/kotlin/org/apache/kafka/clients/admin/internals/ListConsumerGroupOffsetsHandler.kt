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

import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsSpec
import org.apache.kafka.clients.admin.internals.AdminApiFuture.SimpleAdminApiFuture
import org.apache.kafka.clients.admin.internals.AdminApiHandler.ApiResult
import org.apache.kafka.clients.admin.internals.AdminApiHandler.RequestAndKeys
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.Node
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.AbstractResponse
import org.apache.kafka.common.requests.FindCoordinatorRequest.CoordinatorType
import org.apache.kafka.common.requests.OffsetFetchRequest
import org.apache.kafka.common.requests.OffsetFetchResponse
import org.apache.kafka.common.utils.LogContext
import org.slf4j.Logger

class ListConsumerGroupOffsetsHandler(
    private val groupSpecs: Map<String, ListConsumerGroupOffsetsSpec>,
    private val requireStable: Boolean,
    logContext: LogContext,
) : AdminApiHandler<CoordinatorKey, Map<TopicPartition, OffsetAndMetadata?>> {

    private val log: Logger = logContext.logger(ListConsumerGroupOffsetsHandler::class.java)

    private val lookupStrategy: CoordinatorStrategy =
        CoordinatorStrategy(CoordinatorType.GROUP, logContext)

    override fun apiName(): String = "offsetFetch"

    override fun lookupStrategy(): AdminApiLookupStrategy<CoordinatorKey> = lookupStrategy

    private fun validateKeys(groupIds: Set<CoordinatorKey>) {
        val keys = coordinatorKeys(groupSpecs.keys)
        require(keys.containsAll(groupIds)) {
            "Received unexpected group ids $groupIds (expected one of $keys)"
        }
    }

    fun buildBatchedRequest(groupIds: Set<CoordinatorKey>): OffsetFetchRequest.Builder {
        // Create a map that only contains the consumer groups owned by the coordinator.
        val coordinatorGroupIdToTopicPartitions: MutableMap<String, List<TopicPartition>> =
            HashMap(groupIds.size)

        groupIds.forEach { g: CoordinatorKey ->
            val spec = groupSpecs[g.idValue]

            val partitions: List<TopicPartition>? =
                if (spec!!.topicPartitions != null) ArrayList(spec.topicPartitions)
                else null

            coordinatorGroupIdToTopicPartitions[g.idValue] = partitions!!
        }

        return OffsetFetchRequest.Builder(coordinatorGroupIdToTopicPartitions, requireStable, false)
    }

    override fun buildRequest(
        brokerId: Int,
        keys: Set<CoordinatorKey>,
    ): Collection<RequestAndKeys<CoordinatorKey>> {
        validateKeys(keys)

        // When the OffsetFetchRequest fails with NoBatchedOffsetFetchRequestException, we completely disable
        // the batching end-to-end, including the FindCoordinatorRequest.
        return if (lookupStrategy.batch)
            listOf(RequestAndKeys(buildBatchedRequest(keys), keys))
        else keys.map { groupId: CoordinatorKey ->
            RequestAndKeys(buildBatchedRequest(setOf(groupId)), keys)
        }
    }

    override fun handleResponse(
        broker: Node,
        keys: Set<CoordinatorKey>,
        response: AbstractResponse,
    ): ApiResult<CoordinatorKey, Map<TopicPartition, OffsetAndMetadata?>> {
        validateKeys(keys)
        response as OffsetFetchResponse

        val completed = mutableMapOf<CoordinatorKey, Map<TopicPartition, OffsetAndMetadata?>>()
        val failed = mutableMapOf<CoordinatorKey, Throwable>()
        val unmapped = mutableListOf<CoordinatorKey>()

        keys.forEach { coordinatorKey ->

            val group = coordinatorKey.idValue

            if (response.groupHasError(group)) handleGroupError(
                groupId = CoordinatorKey.byGroupId(group),
                error = response.groupLevelError(group)!!,
                failed = failed,
                groupsToUnmap = unmapped
            )
            else {
                val groupOffsetsListing: MutableMap<TopicPartition, OffsetAndMetadata?> = HashMap()
                val responseData = response.partitionDataMap(group)

                responseData.forEach { (topicPartition, partitionData) ->

                    val error = partitionData.error
                    if (error === Errors.NONE) with(partitionData) {
                        // Negative offset indicates that the group has no committed offset for this partition
                        groupOffsetsListing[topicPartition] =
                            if (offset < 0) null
                            else OffsetAndMetadata(offset, leaderEpoch, metadata)
                    } else log.warn(
                        "Skipping return offset for {} due to error {}.",
                        topicPartition,
                        error
                    )
                }
                completed[CoordinatorKey.byGroupId(group)] = groupOffsetsListing
            }
        }
        return ApiResult(completed, failed, unmapped)
    }

    private fun handleGroupError(
        groupId: CoordinatorKey,
        error: Errors,
        failed: MutableMap<CoordinatorKey, Throwable>,
        groupsToUnmap: MutableList<CoordinatorKey>,
    ) {
        when (error) {
            Errors.GROUP_AUTHORIZATION_FAILED -> {
                log.debug(
                    "`OffsetFetch` request for group id {} failed due to error {}",
                    groupId.idValue,
                    error
                )
                failed[groupId] = error.exception!!
            }

            Errors.COORDINATOR_LOAD_IN_PROGRESS ->
                // If the coordinator is in the middle of loading, then we just need to retry
                log.debug(
                    "`OffsetFetch` request for group id {} failed because the coordinator " +
                            "is still in the process of loading state. Will retry",
                    groupId.idValue
                )

            Errors.COORDINATOR_NOT_AVAILABLE,
            Errors.NOT_COORDINATOR,
            -> {
                // If the coordinator is unavailable or there was a coordinator change, then we unmap
                // the key so that we retry the `FindCoordinator` request
                log.debug(
                    "`OffsetFetch` request for group id {} returned error {}. " +
                            "Will attempt to find the coordinator again and retry",
                    groupId.idValue,
                    error
                )
                groupsToUnmap.add(groupId)
            }

            else -> {
                log.error(
                    "`OffsetFetch` request for group id {} failed due to unexpected error {}",
                    groupId.idValue,
                    error
                )
                failed[groupId] = error.exception!!
            }
        }
    }

    companion object {

        fun newFuture(
            groupIds: Collection<String>,
        ): SimpleAdminApiFuture<CoordinatorKey, Map<TopicPartition, OffsetAndMetadata?>> {
            return AdminApiFuture.forKeys(coordinatorKeys(groupIds))
        }

        private fun coordinatorKeys(groupIds: Collection<String>): Set<CoordinatorKey> {
            return groupIds.map(CoordinatorKey::byGroupId).toSet()
        }
    }
}
