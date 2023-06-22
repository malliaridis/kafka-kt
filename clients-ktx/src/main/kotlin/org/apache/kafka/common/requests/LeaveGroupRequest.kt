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

import org.apache.kafka.common.errors.UnsupportedVersionException
import org.apache.kafka.common.message.LeaveGroupRequestData
import org.apache.kafka.common.message.LeaveGroupRequestData.MemberIdentity
import org.apache.kafka.common.message.LeaveGroupResponseData
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.ByteBufferAccessor
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.protocol.MessageUtil.deepToString
import java.nio.ByteBuffer

class LeaveGroupRequest private constructor(
    private val data: LeaveGroupRequestData,
    version: Short,
) : AbstractRequest(
    apiKey = ApiKeys.LEAVE_GROUP,
    version = version,
) {

    override fun data(): LeaveGroupRequestData = data

    fun normalizedData(): LeaveGroupRequestData {
        return if (version >= 3) data
        else LeaveGroupRequestData()
            .setGroupId(data.groupId())
            .setMembers(listOf(MemberIdentity().setMemberId(data.memberId())))
    }

    fun members(): List<MemberIdentity> {
        // Before version 3, leave group request is still in single mode
        return if (version <= 2) listOf(MemberIdentity().setMemberId(data.memberId()))
        else data.members()
    }

    override fun getErrorResponse(throttleTimeMs: Int, e: Throwable): AbstractResponse {
        val responseData = LeaveGroupResponseData().setErrorCode(Errors.forException(e).code)

        if (version >= 1) responseData.setThrottleTimeMs(throttleTimeMs)
        return LeaveGroupResponse(responseData)
    }

    class Builder internal constructor(
        private val groupId: String,
        private val members: List<MemberIdentity>,
        oldestVersion: Short,
        latestVersion: Short,
    ) : AbstractRequest.Builder<LeaveGroupRequest>(
        apiKey = ApiKeys.LEAVE_GROUP,
        oldestAllowedVersion = oldestVersion,
        latestAllowedVersion = latestVersion,
    ) {

        constructor(
            groupId: String,
            members: List<MemberIdentity>,
        ) : this(
            groupId = groupId,
            members = members,
            oldestVersion = ApiKeys.LEAVE_GROUP.oldestVersion(),
            latestVersion = ApiKeys.LEAVE_GROUP.latestVersion(),
        )

        init {
            require(members.isNotEmpty()) { "leaving members should not be empty" }
        }

        /**
         * Based on the request version to choose fields.
         */
        override fun build(version: Short): LeaveGroupRequest {
            // Starting from version 3, all the leave group request will be in batch.
            val data: LeaveGroupRequestData = if (version >= 3)
                LeaveGroupRequestData()
                    .setGroupId(groupId)
                    .setMembers(members)
            else {
                if (members.size != 1) throw UnsupportedVersionException(
                    "Version $version leave group request only supports single member " +
                        "instance than ${members.size} members"
                )

                LeaveGroupRequestData()
                    .setGroupId(groupId)
                    .setMemberId(members[0].memberId())
            }
            return LeaveGroupRequest(data, version)
        }

        override fun toString(): String {
            return "(type=LeaveGroupRequest" +
                ", groupId=$groupId" +
                ", members=${deepToString(members.iterator())}" +
                ")"
        }
    }

    companion object {

        fun parse(buffer: ByteBuffer, version: Short): LeaveGroupRequest =
            LeaveGroupRequest(
                LeaveGroupRequestData(ByteBufferAccessor(buffer), version),
                version,
            )
    }
}
