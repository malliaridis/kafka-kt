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
import org.apache.kafka.common.message.LeaveGroupResponseData
import org.apache.kafka.common.message.LeaveGroupResponseData.MemberResponse
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.ByteBufferAccessor
import org.apache.kafka.common.protocol.Errors
import java.nio.ByteBuffer
import java.util.*

/**
 * Possible error codes.
 *
 * Top level errors:
 * - [Errors.COORDINATOR_LOAD_IN_PROGRESS]
 * - [Errors.COORDINATOR_NOT_AVAILABLE]
 * - [Errors.NOT_COORDINATOR]
 * - [Errors.GROUP_AUTHORIZATION_FAILED]
 *
 * Member level errors:
 * - [Errors.FENCED_INSTANCE_ID]
 * - [Errors.UNKNOWN_MEMBER_ID]
 *
 * If the top level error code is set, normally this indicates that broker early stops the request
 * handling due to some severe global error, so it is expected to see the member level errors to be
 * empty. For older version response, we may populate member level error towards top level because
 * older client couldn't parse member level.
 */
class LeaveGroupResponse : AbstractResponse {

    private val data: LeaveGroupResponseData

    constructor(data: LeaveGroupResponseData) : super(ApiKeys.LEAVE_GROUP) {
        this.data = data
    }

    constructor(data: LeaveGroupResponseData, version: Short) : super(ApiKeys.LEAVE_GROUP) {
        if (version >= 3) this.data = data
        else {
            if (data.members().size != 1) throw UnsupportedVersionException(
                "LeaveGroup response version $version can only contain one member, " +
                        "got ${data.members().size} members."
            )

            val topLevelError = Errors.forCode(data.errorCode())
            val errorCode = getError(topLevelError, data.members()).code
            this.data = LeaveGroupResponseData().setErrorCode(errorCode)
        }
    }

    constructor(
        memberResponses: List<MemberResponse>,
        topLevelError: Errors,
        throttleTimeMs: Int,
        version: Short,
    ) : super(ApiKeys.LEAVE_GROUP) {

        data = if (version <= 2) {
            // Populate member level error.
            val errorCode = getError(topLevelError, memberResponses).code
            LeaveGroupResponseData().setErrorCode(errorCode)
        } else LeaveGroupResponseData()
            .setErrorCode(topLevelError.code)
            .setMembers(memberResponses)

        if (version >= 1) data.setThrottleTimeMs(throttleTimeMs)
    }

    override fun throttleTimeMs(): Int = data.throttleTimeMs()

    override fun maybeSetThrottleTimeMs(throttleTimeMs: Int) {
        data.setThrottleTimeMs(throttleTimeMs)
    }

    fun memberResponses(): List<MemberResponse> = data.members()

    fun error(): Errors = getError(Errors.forCode(data.errorCode()), data.members())

    fun topLevelError(): Errors = Errors.forCode(data.errorCode())

    override fun errorCounts(): Map<Errors, Int> {
        val combinedErrorCounts = mutableMapOf<Errors, Int>()
        // Top level error.
        updateErrorCounts(combinedErrorCounts, Errors.forCode(data.errorCode()))

        // Member level error.
        data.members().forEach { memberResponse ->
            updateErrorCounts(combinedErrorCounts, Errors.forCode(memberResponse.errorCode()))
        }

        return combinedErrorCounts
    }

    override fun data(): LeaveGroupResponseData = data

    override fun shouldClientThrottle(version: Short): Boolean = version >= 2

    override fun equals(other: Any?): Boolean {
        return other is LeaveGroupResponse
            && other.data == data
    }

    override fun hashCode(): Int = Objects.hashCode(data)

    override fun toString(): String = data.toString()

    companion object {

        private fun getError(topLevelError: Errors, memberResponses: List<MemberResponse>): Errors {
            return if (topLevelError !== Errors.NONE) topLevelError
            else {
                for (memberResponse in memberResponses) {
                    val memberError = Errors.forCode(memberResponse.errorCode())
                    if (memberError !== Errors.NONE) return memberError
                }
                Errors.NONE
            }
        }

        fun parse(buffer: ByteBuffer, version: Short): LeaveGroupResponse =
            LeaveGroupResponse(LeaveGroupResponseData(ByteBufferAccessor(buffer), version))
    }
}
