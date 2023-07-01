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
import org.apache.kafka.common.message.RenewDelegationTokenRequestData
import org.apache.kafka.common.message.RenewDelegationTokenResponseData
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.ByteBufferAccessor
import org.apache.kafka.common.protocol.Errors


class RenewDelegationTokenRequest(
    private val data: RenewDelegationTokenRequestData,
    version: Short,
) : AbstractRequest(ApiKeys.RENEW_DELEGATION_TOKEN, version) {

    override fun data(): RenewDelegationTokenRequestData = data

    override fun getErrorResponse(throttleTimeMs: Int, e: Throwable): AbstractResponse {
        return RenewDelegationTokenResponse(
            RenewDelegationTokenResponseData()
                .setThrottleTimeMs(throttleTimeMs)
                .setErrorCode(Errors.forException(e).code)
        )
    }

    class Builder(
        private val data: RenewDelegationTokenRequestData,
    ) : AbstractRequest.Builder<RenewDelegationTokenRequest>(ApiKeys.RENEW_DELEGATION_TOKEN) {

        override fun build(version: Short): RenewDelegationTokenRequest =
            RenewDelegationTokenRequest(data, version)

        override fun toString(): String = data.toString()
    }

    companion object {

        fun parse(buffer: ByteBuffer, version: Short): RenewDelegationTokenRequest =
            RenewDelegationTokenRequest(
                RenewDelegationTokenRequestData(ByteBufferAccessor(buffer), version),
                version
            )
    }
}
