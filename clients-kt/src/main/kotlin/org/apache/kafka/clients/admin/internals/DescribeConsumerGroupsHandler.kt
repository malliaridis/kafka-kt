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

import java.nio.ByteBuffer
import org.apache.kafka.clients.admin.ConsumerGroupDescription
import org.apache.kafka.clients.admin.MemberAssignment
import org.apache.kafka.clients.admin.MemberDescription
import org.apache.kafka.clients.admin.internals.AdminApiFuture.SimpleAdminApiFuture
import org.apache.kafka.clients.admin.internals.AdminApiHandler.ApiResult
import org.apache.kafka.clients.admin.internals.AdminApiHandler.Batched
import org.apache.kafka.clients.consumer.internals.ConsumerProtocol
import org.apache.kafka.common.ConsumerGroupState
import org.apache.kafka.common.Node
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.acl.AclOperation
import org.apache.kafka.common.message.DescribeGroupsRequestData
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.AbstractResponse
import org.apache.kafka.common.requests.DescribeGroupsRequest
import org.apache.kafka.common.requests.DescribeGroupsResponse
import org.apache.kafka.common.requests.FindCoordinatorRequest.CoordinatorType
import org.apache.kafka.common.requests.MetadataResponse
import org.apache.kafka.common.utils.LogContext
import org.apache.kafka.common.utils.Utils
import org.slf4j.Logger

class DescribeConsumerGroupsHandler(
    private val includeAuthorizedOperations: Boolean,
    logContext: LogContext
) : Batched<CoordinatorKey, ConsumerGroupDescription>() {

    private val log: Logger = logContext.logger(DescribeConsumerGroupsHandler::class.java)

    private val lookupStrategy: AdminApiLookupStrategy<CoordinatorKey> =
        CoordinatorStrategy(CoordinatorType.GROUP, logContext)

    override fun apiName(): String = "describeGroups"

    override fun lookupStrategy(): AdminApiLookupStrategy<CoordinatorKey> = lookupStrategy

    override fun buildBatchedRequest(
        brokerId: Int,
        keys: Set<CoordinatorKey>,
    ): DescribeGroupsRequest.Builder {
        val groupIds = keys.map { key: CoordinatorKey ->
            require(key.type == CoordinatorType.GROUP) {
                "Invalid transaction coordinator key $key when building `DescribeGroups` request"
            }
            key.idValue
        }
        val data = DescribeGroupsRequestData()
            .setGroups(groupIds)
            .setIncludeAuthorizedOperations(includeAuthorizedOperations)
        return DescribeGroupsRequest.Builder(data)
    }

    override fun handleResponse(
        broker: Node,
        keys: Set<CoordinatorKey>,
        response: AbstractResponse
    ): ApiResult<CoordinatorKey, ConsumerGroupDescription> {
        response as DescribeGroupsResponse

        val completed: MutableMap<CoordinatorKey, ConsumerGroupDescription> = HashMap()
        val failed: MutableMap<CoordinatorKey, Throwable> = HashMap()
        val groupsToUnmap: MutableSet<CoordinatorKey> = hashSetOf()

        response.data().groups.forEach { describedGroup ->
            val groupIdKey = CoordinatorKey.byGroupId(describedGroup.groupId)
            val error = Errors.forCode(describedGroup.errorCode)

            if (error !== Errors.NONE) {
                handleError(groupIdKey, error, failed, groupsToUnmap)
                return@forEach
            }

            val protocolType = describedGroup.protocolType
            if (protocolType == ConsumerProtocol.PROTOCOL_TYPE || protocolType.isEmpty()) {
                val members = describedGroup.members
                val memberDescriptions: MutableList<MemberDescription> = ArrayList(members.size)
                val authorizedOperations = validAclOperations(describedGroup.authorizedOperations)

                members.forEach { groupMember ->
                    var partitions: Set<TopicPartition> = emptySet()

                    if (groupMember.memberAssignment.isNotEmpty()) {
                        val assignment = ConsumerProtocol.deserializeAssignment(
                            ByteBuffer.wrap(groupMember.memberAssignment)
                        )
                        partitions = assignment.partitions.toSet()
                    }

                    memberDescriptions.add(
                        MemberDescription(
                            memberId = groupMember.memberId,
                            groupInstanceId = groupMember.groupInstanceId,
                            clientId = groupMember.clientId,
                            host = groupMember.clientHost,
                            assignment = MemberAssignment(partitions)
                        )
                    )
                }

                val consumerGroupDescription = ConsumerGroupDescription(
                    groupId = groupIdKey.idValue,
                    isSimpleConsumerGroup = protocolType.isEmpty(),
                    members = memberDescriptions,
                    partitionAssignor = describedGroup.protocolData,
                    state = ConsumerGroupState.parse(describedGroup.groupState),
                    coordinator = broker,
                    authorizedOperations = authorizedOperations
                )
                completed[groupIdKey] = consumerGroupDescription
            } else {
                failed[groupIdKey] = IllegalArgumentException(
                    String.format(
                        "GroupId %s is not a consumer group (%s).",
                        groupIdKey.idValue,
                        protocolType
                    )
                )
            }
        }
        return ApiResult(completed, failed, ArrayList(groupsToUnmap))
    }

    private fun handleError(
        groupId: CoordinatorKey,
        error: Errors,
        failed: MutableMap<CoordinatorKey, Throwable>,
        groupsToUnmap: MutableSet<CoordinatorKey>
    ) {
        when (error) {
            Errors.GROUP_AUTHORIZATION_FAILED -> {
                log.debug(
                    "`DescribeGroups` request for group id {} failed due to error {}",
                    groupId.idValue,
                    error
                )
                failed[groupId] = error.exception!!
            }

            Errors.COORDINATOR_LOAD_IN_PROGRESS ->                 // If the coordinator is in the middle of loading, then we just need to retry
                log.debug(
                    "`DescribeGroups` request for group id {} failed because the coordinator " +
                            "is still in the process of loading state. Will retry",
                    groupId.idValue
                )

            Errors.COORDINATOR_NOT_AVAILABLE, Errors.NOT_COORDINATOR -> {
                // If the coordinator is unavailable or there was a coordinator change, then we unmap
                // the key so that we retry the `FindCoordinator` request
                log.debug(
                    "`DescribeGroups` request for group id {} returned error {}. " +
                            "Will attempt to find the coordinator again and retry",
                    groupId.idValue,
                    error
                )
                groupsToUnmap.add(groupId)
            }

            else -> {
                log.error(
                    "`DescribeGroups` request for group id {} failed due to unexpected error {}",
                    groupId.idValue,
                    error
                )
                failed[groupId] = error.exception!!
            }
        }
    }

    private fun validAclOperations(authorizedOperations: Int): Set<AclOperation> {
        return if (authorizedOperations == MetadataResponse.AUTHORIZED_OPERATIONS_OMITTED) emptySet()
        else Utils.from32BitField(authorizedOperations)
            .map { code: Byte -> AclOperation.fromCode(code) }
            .filter { operation: AclOperation ->
                operation != AclOperation.UNKNOWN && operation != AclOperation.ALL && operation != AclOperation.ANY
            }.toSet()
    }

    companion object {
        private fun buildKeySet(groupIds: Collection<String>): Set<CoordinatorKey> {
            return groupIds.map(CoordinatorKey::byGroupId).toSet()
        }

        fun newFuture(
            groupIds: Collection<String>,
        ): SimpleAdminApiFuture<CoordinatorKey, ConsumerGroupDescription> {
            return AdminApiFuture.forKeys(buildKeySet(groupIds))
        }
    }
}
