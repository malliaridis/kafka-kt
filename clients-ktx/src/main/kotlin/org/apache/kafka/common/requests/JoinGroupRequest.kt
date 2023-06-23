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

import org.apache.kafka.common.errors.InvalidConfigurationException
import org.apache.kafka.common.errors.UnsupportedVersionException
import org.apache.kafka.common.internals.Topic.validate
import org.apache.kafka.common.message.JoinGroupRequestData
import org.apache.kafka.common.message.JoinGroupResponseData
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.ByteBufferAccessor
import org.apache.kafka.common.protocol.Errors
import java.nio.ByteBuffer

class JoinGroupRequest(
    private val data: JoinGroupRequestData,
    version: Short,
) : AbstractRequest(ApiKeys.JOIN_GROUP, version) {

    init {
        maybeOverrideRebalanceTimeout(version)
    }

    override fun data(): JoinGroupRequestData = data

    override fun getErrorResponse(throttleTimeMs: Int, e: Throwable): AbstractResponse {
        val data = JoinGroupResponseData()
            .setThrottleTimeMs(throttleTimeMs)
            .setErrorCode(Errors.forException(e).code)
            .setGenerationId(UNKNOWN_GENERATION_ID)
            .setProtocolName(UNKNOWN_PROTOCOL_NAME)
            .setLeader(UNKNOWN_MEMBER_ID)
            .setMemberId(UNKNOWN_MEMBER_ID)
            .setMembers(emptyList())

        if (version >= 7) data.setProtocolName(null)
        else data.setProtocolName(UNKNOWN_PROTOCOL_NAME)
        return JoinGroupResponse(data, version)
    }

    private fun maybeOverrideRebalanceTimeout(version: Short) {
        if (version.toInt() == 0)
        // Version 0 has no rebalance timeout, so we use the session timeout
        // to be consistent with the original behavior of the API.
            data.setRebalanceTimeoutMs(data.sessionTimeoutMs())
    }

    class Builder(
        private val data: JoinGroupRequestData,
    ) : AbstractRequest.Builder<JoinGroupRequest>(ApiKeys.JOIN_GROUP) {

        override fun build(version: Short): JoinGroupRequest {
            if (data.groupInstanceId() != null && version < 5) throw UnsupportedVersionException(
                "The broker join group protocol version $version does not support " +
                    "usage of config group.instance.id."
            )
            return JoinGroupRequest(data, version)
        }

        override fun toString(): String = data.toString()
    }

    companion object {

        const val UNKNOWN_MEMBER_ID = ""

        const val UNKNOWN_GENERATION_ID = -1

        const val UNKNOWN_PROTOCOL_NAME = ""

        /**
         * Ported from class Topic in [org.apache.kafka.common.internals] to restrict the charset for
         * static member id.
         */
        fun validateGroupInstanceId(id: String) {
            validate(id, "Group instance id") { message ->
                throw InvalidConfigurationException(message)
            }
        }

        /**
         * Ensures that the provided `reason` remains within a range of 255 chars.
         *
         * @param reason This is the reason that is sent to the broker over the wire as a part of
         * `JoinGroupRequest` or `LeaveGroupRequest` messages.
         * @return a provided reason as is or truncated reason if it exceeds the 255 chars
         * threshold.
         */
        fun maybeTruncateReason(reason: String): String {
            return if (reason.length > 255) reason.substring(0, 255)
            else reason
        }

        fun parse(buffer: ByteBuffer, version: Short): JoinGroupRequest =
            JoinGroupRequest(JoinGroupRequestData(ByteBufferAccessor(buffer), version), version)
    }
}
