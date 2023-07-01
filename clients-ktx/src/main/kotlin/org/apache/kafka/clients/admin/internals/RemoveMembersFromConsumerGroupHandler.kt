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
import org.apache.kafka.common.message.LeaveGroupRequestData.MemberIdentity
import org.apache.kafka.common.message.LeaveGroupResponseData.MemberResponse
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.AbstractResponse
import org.apache.kafka.common.requests.FindCoordinatorRequest.CoordinatorType
import org.apache.kafka.common.requests.LeaveGroupRequest
import org.apache.kafka.common.requests.LeaveGroupResponse
import org.apache.kafka.common.utils.LogContext
import org.slf4j.Logger

class RemoveMembersFromConsumerGroupHandler(
    groupId: String,
    private val members: List<MemberIdentity>,
    logContext: LogContext
) : Batched<CoordinatorKey, Map<MemberIdentity, Errors>>() {

    private val groupId: CoordinatorKey

    private val log: Logger

    private val lookupStrategy: AdminApiLookupStrategy<CoordinatorKey>

    init {
        this.groupId = CoordinatorKey.byGroupId(groupId)
        log = logContext.logger(RemoveMembersFromConsumerGroupHandler::class.java)
        lookupStrategy = CoordinatorStrategy(CoordinatorType.GROUP, logContext)
    }

    override fun apiName(): String = "leaveGroup"

    override fun lookupStrategy(): AdminApiLookupStrategy<CoordinatorKey> = lookupStrategy

    private fun validateKeys(groupIds: Set<CoordinatorKey>) {
        require(groupIds == setOf(groupId)) {
            "Received unexpected group ids $groupIds (expected only ${setOf(groupId)})"
        }
    }

    override fun buildBatchedRequest(
        brokerId: Int,
        keys: Set<CoordinatorKey>
    ): LeaveGroupRequest.Builder {
        validateKeys(keys)
        return LeaveGroupRequest.Builder(groupId.idValue, members)
    }

    override fun handleResponse(
        broker: Node,
        keys: Set<CoordinatorKey>,
        response: AbstractResponse
    ): ApiResult<CoordinatorKey, Map<MemberIdentity, Errors>> {
        validateKeys(keys)
        val res = response as LeaveGroupResponse
        val error = res.topLevelError()
        return if (error !== Errors.NONE) {
            val failed = mutableMapOf<CoordinatorKey, Throwable>()
            val groupsToUnmap = mutableSetOf<CoordinatorKey>()

            handleGroupError(groupId, error, failed, groupsToUnmap)
            ApiResult(emptyMap(), failed, ArrayList(groupsToUnmap))
        } else {
            val memberErrors = mutableMapOf<MemberIdentity, Errors>()
            for (memberResponse: MemberResponse in res.memberResponses()) {
                memberErrors[
                    MemberIdentity()
                        .setMemberId(memberResponse.memberId())
                        .setGroupInstanceId(memberResponse.groupInstanceId())
                ] = Errors.forCode(memberResponse.errorCode())
            }
            ApiResult.completed(groupId, memberErrors)
        }
    }

    private fun handleGroupError(
        groupId: CoordinatorKey,
        error: Errors,
        failed: MutableMap<CoordinatorKey, Throwable>,
        groupsToUnmap: MutableSet<CoordinatorKey>,
    ) {
        when (error) {
            Errors.GROUP_AUTHORIZATION_FAILED -> {
                log.debug(
                    "`LeaveGroup` request for group id {} failed due to error {}",
                    groupId.idValue,
                    error
                )
                failed[groupId] = error.exception!!
            }

            // If the coordinator is in the middle of loading, then we just need to retry
            Errors.COORDINATOR_LOAD_IN_PROGRESS -> log.debug(
                "`LeaveGroup` request for group id {} failed because the coordinator " +
                        "is still in the process of loading state. Will retry",
                groupId.idValue,
            )

            Errors.COORDINATOR_NOT_AVAILABLE, Errors.NOT_COORDINATOR -> {
                // If the coordinator is unavailable or there was a coordinator change, then we unmap
                // the key so that we retry the `FindCoordinator` request
                log.debug(
                    "`LeaveGroup` request for group id {} returned error {}. " +
                            "Will attempt to find the coordinator again and retry",
                    groupId.idValue,
                    error,
                )
                groupsToUnmap.add(groupId)
            }

            else -> {
                log.error(
                    "`LeaveGroup` request for group id {} failed due to unexpected error {}",
                    groupId.idValue,
                    error,
                )
                failed[groupId] = error.exception!!
            }
        }
    }

    companion object {

        fun newFuture(
            groupId: String,
        ): SimpleAdminApiFuture<CoordinatorKey, Map<MemberIdentity, Errors>> =
            AdminApiFuture.forKeys(setOf(CoordinatorKey.byGroupId(groupId)))
    }
}
