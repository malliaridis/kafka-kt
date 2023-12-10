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

package org.apache.kafka.common.requests

import java.nio.ByteBuffer
import org.apache.kafka.common.message.DescribeGroupsResponseData
import org.apache.kafka.common.message.DescribeGroupsResponseData.DescribedGroup
import org.apache.kafka.common.message.DescribeGroupsResponseData.DescribedGroupMember
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.ByteBufferAccessor
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.utils.Utils.to32BitField

/**
 * Possible per-group error codes:
 *
 * - COORDINATOR_LOAD_IN_PROGRESS (14)
 * - COORDINATOR_NOT_AVAILABLE (15)
 * - NOT_COORDINATOR (16)
 * - AUTHORIZATION_FAILED (29)
 */
class DescribeGroupsResponse(
    private val data: DescribeGroupsResponseData,
) : AbstractResponse(ApiKeys.DESCRIBE_GROUPS) {

    override fun data(): DescribeGroupsResponseData = data

    override fun throttleTimeMs(): Int = data.throttleTimeMs

    override fun maybeSetThrottleTimeMs(throttleTimeMs: Int) {
        data.setThrottleTimeMs(throttleTimeMs)
    }

    override fun errorCounts(): Map<Errors, Int> {
        val errorCounts = mutableMapOf<Errors, Int>()
        data.groups.forEach { describedGroup: DescribedGroup ->
            updateErrorCounts(errorCounts, Errors.forCode(describedGroup.errorCode))
        }
        return errorCounts
    }

    override fun shouldClientThrottle(version: Short): Boolean = version >= 2

    companion object {

        const val AUTHORIZED_OPERATIONS_OMITTED = Int.MIN_VALUE

        const val UNKNOWN_STATE = ""

        const val UNKNOWN_PROTOCOL_TYPE = ""

        const val UNKNOWN_PROTOCOL = ""

        fun groupMember(
            memberId: String,
            groupInstanceId: String?,
            clientId: String,
            clientHost: String,
            assignment: ByteArray,
            metadata: ByteArray,
        ): DescribedGroupMember = DescribedGroupMember()
            .setMemberId(memberId)
            .setGroupInstanceId(groupInstanceId)
            .setClientId(clientId)
            .setClientHost(clientHost)
            .setMemberAssignment(assignment)
            .setMemberMetadata(metadata)

        fun groupMetadata(
            groupId: String,
            error: Errors,
            state: String,
            protocolType: String,
            protocol: String,
            members: List<DescribedGroupMember>,
            authorizedOperations: Set<Byte>,
        ): DescribedGroup = DescribedGroup()
            .setGroupId(groupId)
            .setErrorCode(error.code)
            .setGroupState(state)
            .setProtocolType(protocolType)
            .setProtocolData(protocol)
            .setMembers(members)
            .setAuthorizedOperations(to32BitField(authorizedOperations))

        fun groupMetadata(
            groupId: String,
            error: Errors,
            state: String,
            protocolType: String,
            protocol: String,
            members: List<DescribedGroupMember>,
            authorizedOperations: Int,
        ): DescribedGroup = DescribedGroup()
            .setGroupId(groupId)
            .setErrorCode(error.code)
            .setGroupState(state)
            .setProtocolType(protocolType)
            .setProtocolData(protocol)
            .setMembers(members)
            .setAuthorizedOperations(authorizedOperations)

        fun groupError(groupId: String, error: Errors): DescribedGroup = groupMetadata(
            groupId = groupId,
            error = error,
            state = UNKNOWN_STATE,
            protocolType = UNKNOWN_PROTOCOL_TYPE,
            protocol = UNKNOWN_PROTOCOL,
            members = emptyList(),
            authorizedOperations = AUTHORIZED_OPERATIONS_OMITTED,
        )

        fun parse(buffer: ByteBuffer, version: Short): DescribeGroupsResponse =
            DescribeGroupsResponse(
                DescribeGroupsResponseData(ByteBufferAccessor(buffer), version)
            )
    }
}
