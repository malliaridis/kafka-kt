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
import org.apache.kafka.common.message.ConsumerGroupHeartbeatResponseData
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.ByteBufferAccessor
import org.apache.kafka.common.protocol.Errors

/**
 * Possible error codes.
 *
 * - [Errors.GROUP_AUTHORIZATION_FAILED]
 * - [Errors.NOT_COORDINATOR]
 * - [Errors.COORDINATOR_NOT_AVAILABLE]
 * - [Errors.COORDINATOR_LOAD_IN_PROGRESS]
 * - [Errors.INVALID_REQUEST]
 * - [Errors.UNKNOWN_MEMBER_ID]
 * - [Errors.FENCED_MEMBER_EPOCH]
 * - [Errors.UNSUPPORTED_ASSIGNOR]
 * - [Errors.UNRELEASED_INSTANCE_ID]
 * - [Errors.GROUP_MAX_SIZE_REACHED]
 */
class ConsumerGroupHeartbeatResponse(
    private val data: ConsumerGroupHeartbeatResponseData,
) : AbstractResponse(ApiKeys.CONSUMER_GROUP_HEARTBEAT) {

    override fun data(): ConsumerGroupHeartbeatResponseData = data

    override fun errorCounts(): Map<Errors, Int> = mapOf(Errors.forCode(data.errorCode) to 1)

    override fun throttleTimeMs(): Int = data.throttleTimeMs

    override fun maybeSetThrottleTimeMs(throttleTimeMs: Int) {
        data.setThrottleTimeMs(throttleTimeMs)
    }

    companion object {
        fun parse(buffer: ByteBuffer, version: Short): ConsumerGroupHeartbeatResponse =
            ConsumerGroupHeartbeatResponse(
                ConsumerGroupHeartbeatResponseData(
                    ByteBufferAccessor(buffer), version
                )
            )
    }
}
