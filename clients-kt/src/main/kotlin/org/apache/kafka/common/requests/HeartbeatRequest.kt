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
import org.apache.kafka.common.message.HeartbeatRequestData
import org.apache.kafka.common.message.HeartbeatResponseData
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.ByteBufferAccessor
import org.apache.kafka.common.protocol.Errors
import java.nio.ByteBuffer

class HeartbeatRequest private constructor(
    private val data: HeartbeatRequestData,
    version: Short,
) : AbstractRequest(ApiKeys.HEARTBEAT, version) {

    override fun getErrorResponse(throttleTimeMs: Int, e: Throwable): AbstractResponse {
        val responseData = HeartbeatResponseData().setErrorCode(Errors.forException(e).code)
        if (version >= 1) responseData.setThrottleTimeMs(throttleTimeMs)

        return HeartbeatResponse(responseData)
    }

    override fun data(): HeartbeatRequestData = data

    class Builder(
        private val data: HeartbeatRequestData,
    ) : AbstractRequest.Builder<HeartbeatRequest>(
        ApiKeys.HEARTBEAT
    ) {

        override fun build(version: Short): HeartbeatRequest {
            if (data.groupInstanceId != null && version < 3) throw UnsupportedVersionException(
                "The broker heartbeat protocol version $version does not support usage " +
                    "of config group.instance.id."
            )

            return HeartbeatRequest(data, version)
        }

        override fun toString(): String = data.toString()
    }

    companion object {

        fun parse(buffer: ByteBuffer, version: Short): HeartbeatRequest =
            HeartbeatRequest(
                HeartbeatRequestData(ByteBufferAccessor(buffer), version),
                version
            )
    }
}
