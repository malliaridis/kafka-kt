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

import java.util.stream.Collectors
import org.apache.kafka.clients.admin.internals.AdminApiFuture.SimpleAdminApiFuture
import org.apache.kafka.clients.admin.internals.AdminApiHandler.ApiResult
import org.apache.kafka.clients.admin.internals.AdminApiHandler.Batched
import org.apache.kafka.common.Node
import org.apache.kafka.common.message.DeleteGroupsRequestData
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.AbstractResponse
import org.apache.kafka.common.requests.DeleteGroupsRequest
import org.apache.kafka.common.requests.DeleteGroupsResponse
import org.apache.kafka.common.requests.FindCoordinatorRequest.CoordinatorType
import org.apache.kafka.common.utils.LogContext
import org.slf4j.Logger

class DeleteConsumerGroupsHandler(
    logContext: LogContext
) : Batched<CoordinatorKey, Unit>() {
    private val log: Logger = logContext.logger(DeleteConsumerGroupsHandler::class.java)
    val lookupStrategy = CoordinatorStrategy(CoordinatorType.GROUP, logContext)

    override fun apiName(): String {
        return "deleteConsumerGroups"
    }

    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("lookupStrategy"),
    )
    override fun lookupStrategy(): AdminApiLookupStrategy<CoordinatorKey> {
        return lookupStrategy
    }

    override fun buildBatchedRequest(
        brokerId: Int,
        keys: Set<CoordinatorKey>
    ): DeleteGroupsRequest.Builder {
        val groupIds = keys.map { key -> key.idValue }
        val data = DeleteGroupsRequestData().setGroupsNames(groupIds)
        return DeleteGroupsRequest.Builder(data)
    }

    override fun handleResponse(
        broker: Node,
        keys: Set<CoordinatorKey>,
        response: AbstractResponse
    ): ApiResult<CoordinatorKey, Unit> {
        val completed: MutableMap<CoordinatorKey, Unit> = HashMap()
        val failed: MutableMap<CoordinatorKey, Throwable> = HashMap()
        val groupsToUnmap: MutableSet<CoordinatorKey> = HashSet()
        (response as DeleteGroupsResponse).data().results()?.forEach { deletedGroup ->
            val groupIdKey = CoordinatorKey.byGroupId(deletedGroup.groupId())
            val error = Errors.forCode(deletedGroup.errorCode())
            if (error !== Errors.NONE) {
                handleError(groupIdKey, error, failed, groupsToUnmap)
                return@forEach
            }
            completed[groupIdKey] = Unit
        }
        return ApiResult(completed, failed, ArrayList(groupsToUnmap))
    }

    /**
     * Error handler that
     */
    private fun handleError(
        groupId: CoordinatorKey,
        error: Errors,
        failed: MutableMap<CoordinatorKey, Throwable>,
        groupsToUnmap: MutableSet<CoordinatorKey>
    ) {
        // error.exception is only `null` for Errors.NONE, which is not allowed here.
        when (error) {
            Errors.GROUP_AUTHORIZATION_FAILED,
            Errors.INVALID_GROUP_ID,
            Errors.NON_EMPTY_GROUP,
            Errors.GROUP_ID_NOT_FOUND -> {
                log.debug(
                    "`DeleteConsumerGroups` request for group id {} failed due to error {}",
                    groupId.idValue,
                    error
                )
                failed[groupId] = error.exception!!
            }

            Errors.COORDINATOR_LOAD_IN_PROGRESS ->
                // If the coordinator is in the middle of loading, then we just need to retry
                log.debug(
                    "`DeleteConsumerGroups` request for group id {} failed because the coordinator " +
                            "is still in the process of loading state. Will retry", groupId.idValue
                )

            Errors.COORDINATOR_NOT_AVAILABLE,
            Errors.NOT_COORDINATOR -> {
                // If the coordinator is unavailable or there was a coordinator change, then we unmap
                // the key so that we retry the `FindCoordinator` request
                log.debug(
                    "`DeleteConsumerGroups` request for group id {} returned error {}. " +
                            "Will attempt to find the coordinator again and retry",
                    groupId.idValue,
                    error
                )
                groupsToUnmap.add(groupId)
            }
            Errors.NONE -> {} // Should be caught before `else` to avoid NullPointerException
            else -> {
                log.error(
                    "`DeleteConsumerGroups` request for group id {} failed due to unexpected error {}",
                    groupId.idValue,
                    error
                )
                failed[groupId] = error.exception!!
            }
        }
    }

    companion object {
        fun newFuture(
            groupIds: Collection<String>
        ): SimpleAdminApiFuture<CoordinatorKey, Unit> {
            return AdminApiFuture.forKeys(buildKeySet(groupIds))
        }

        private fun buildKeySet(groupIds: Collection<String>): Set<CoordinatorKey> {
            return groupIds.map { groupId -> CoordinatorKey.byGroupId(groupId) }.toSet()
        }
    }
}
