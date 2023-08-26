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

import org.apache.kafka.common.message.ExpireDelegationTokenRequestData
import org.apache.kafka.common.message.ExpireDelegationTokenResponseData
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.ByteBufferAccessor
import org.apache.kafka.common.protocol.Errors
import java.nio.ByteBuffer

class ExpireDelegationTokenRequest private constructor(
    private val data: ExpireDelegationTokenRequestData,
    version: Short,
) : AbstractRequest(ApiKeys.EXPIRE_DELEGATION_TOKEN, version) {

    override fun data(): ExpireDelegationTokenRequestData = data

    override fun getErrorResponse(throttleTimeMs: Int, e: Throwable): AbstractResponse =
        ExpireDelegationTokenResponse(
            ExpireDelegationTokenResponseData()
                .setErrorCode(Errors.forException(e).code)
                .setThrottleTimeMs(throttleTimeMs)
        )

    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("expiryTimePeriod"),
    )
    fun hmac(): ByteBuffer = ByteBuffer.wrap(data.hmac)

    val hmac: ByteBuffer
        get() = ByteBuffer.wrap(data.hmac)

    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("expiryTimePeriod"),
    )
    fun expiryTimePeriod(): Long = data.expiryTimePeriodMs

    val expiryTimePeriod: Long
        get() = data.expiryTimePeriodMs

    class Builder(
        private val data: ExpireDelegationTokenRequestData,
    ) : AbstractRequest.Builder<ExpireDelegationTokenRequest>(ApiKeys.EXPIRE_DELEGATION_TOKEN) {

        override fun build(version: Short): ExpireDelegationTokenRequest =
            ExpireDelegationTokenRequest(data, version)

        override fun toString(): String = data.toString()
    }

    companion object {

        fun parse(buffer: ByteBuffer, version: Short): ExpireDelegationTokenRequest =
            ExpireDelegationTokenRequest(
                ExpireDelegationTokenRequestData(ByteBufferAccessor(buffer), version),
                version,
            )
    }
}
