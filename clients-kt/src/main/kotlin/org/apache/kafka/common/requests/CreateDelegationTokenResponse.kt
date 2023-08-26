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

import org.apache.kafka.common.message.CreateDelegationTokenResponseData
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.ByteBufferAccessor
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.security.auth.KafkaPrincipal
import java.nio.ByteBuffer

class CreateDelegationTokenResponse(
    private val data: CreateDelegationTokenResponseData,
) : AbstractResponse(ApiKeys.CREATE_DELEGATION_TOKEN) {

    override fun data(): CreateDelegationTokenResponseData = data

    override fun errorCounts(): Map<Errors, Int> = errorCounts(error())

    override fun throttleTimeMs(): Int = data.throttleTimeMs

    override fun maybeSetThrottleTimeMs(throttleTimeMs: Int) {
        data.setThrottleTimeMs(throttleTimeMs)
    }

    fun error(): Errors = Errors.forCode(data.errorCode)

    fun hasError(): Boolean = error() !== Errors.NONE

    override fun shouldClientThrottle(version: Short): Boolean = version >= 1

    companion object {

        fun parse(buffer: ByteBuffer, version: Short): CreateDelegationTokenResponse =
            CreateDelegationTokenResponse(
                CreateDelegationTokenResponseData(ByteBufferAccessor(buffer), version)
            )

        fun prepareResponse(
            version: Int,
            throttleTimeMs: Int,
            error: Errors,
            owner: KafkaPrincipal,
            tokenRequester: KafkaPrincipal,
            issueTimestamp: Long = -1,
            expiryTimestamp: Long = -1,
            maxTimestamp: Long = -1,
            tokenId: String = "",
            hmac: ByteBuffer = ByteBuffer.wrap(byteArrayOf()),
        ): CreateDelegationTokenResponse {
            val data = CreateDelegationTokenResponseData()
                .setThrottleTimeMs(throttleTimeMs)
                .setErrorCode(error.code)
                .setPrincipalType(owner.principalType)
                .setPrincipalName(owner.getName())
                .setIssueTimestampMs(issueTimestamp)
                .setExpiryTimestampMs(expiryTimestamp)
                .setMaxTimestampMs(maxTimestamp)
                .setTokenId(tokenId)
                .setHmac(hmac.array())

            if (version > 2) data.setTokenRequesterPrincipalType(tokenRequester.principalType)
                .setTokenRequesterPrincipalName(tokenRequester.getName())

            return CreateDelegationTokenResponse(data)
        }
    }
}
