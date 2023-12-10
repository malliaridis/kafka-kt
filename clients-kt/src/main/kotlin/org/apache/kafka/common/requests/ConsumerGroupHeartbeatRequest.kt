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
import org.apache.kafka.common.message.ConsumerGroupHeartbeatRequestData
import org.apache.kafka.common.message.ConsumerGroupHeartbeatResponseData
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.ByteBufferAccessor
import org.apache.kafka.common.protocol.Errors

class ConsumerGroupHeartbeatRequest(
    private val data: ConsumerGroupHeartbeatRequestData,
    version: Short,
) : AbstractRequest(ApiKeys.CONSUMER_GROUP_HEARTBEAT, version) {

    class Builder(
        private val data: ConsumerGroupHeartbeatRequestData,
        enableUnstableLastVersion: Boolean = false,
    ) : AbstractRequest.Builder<ConsumerGroupHeartbeatRequest>(
        apiKey = ApiKeys.CONSUMER_GROUP_HEARTBEAT,
        enableUnstableLastVersion = enableUnstableLastVersion
    ) {

        override fun build(version: Short): ConsumerGroupHeartbeatRequest =
            ConsumerGroupHeartbeatRequest(data, version)

        override fun toString(): String = data.toString()
    }

    override fun getErrorResponse(throttleTimeMs: Int, e: Throwable): AbstractResponse =
        ConsumerGroupHeartbeatResponse(
            ConsumerGroupHeartbeatResponseData()
                .setThrottleTimeMs(throttleTimeMs)
                .setErrorCode(Errors.forException(e).code)
        )

    override fun data(): ConsumerGroupHeartbeatRequestData = data

    companion object {

        fun parse(buffer: ByteBuffer, version: Short): ConsumerGroupHeartbeatRequest =
            ConsumerGroupHeartbeatRequest(
                data = ConsumerGroupHeartbeatRequestData(ByteBufferAccessor(buffer), version),
                version = version,
            )
    }
}
