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
import java.util.stream.Collectors
import org.apache.kafka.common.message.DescribeDelegationTokenResponseData
import org.apache.kafka.common.message.DescribeDelegationTokenResponseData.DescribedDelegationToken
import org.apache.kafka.common.message.DescribeDelegationTokenResponseData.DescribedDelegationTokenRenewer
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.ByteBufferAccessor
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.security.auth.KafkaPrincipal
import org.apache.kafka.common.security.token.delegation.DelegationToken
import org.apache.kafka.common.security.token.delegation.TokenInformation

class DescribeDelegationTokenResponse : AbstractResponse {

    private val data: DescribeDelegationTokenResponseData

    constructor(
        version: Int,
        throttleTimeMs: Int,
        error: Errors,
        tokens: List<DelegationToken> = ArrayList(),
    ) : super(ApiKeys.DESCRIBE_DELEGATION_TOKEN) {

        val describedDelegationTokenList = tokens.map { (tokenInfo, hmac): DelegationToken ->
            val ddt = DescribedDelegationToken()
                .setTokenId(tokenInfo.tokenId)
                .setPrincipalType(tokenInfo.owner.principalType)
                .setPrincipalName(tokenInfo.owner.name)
                .setIssueTimestamp(tokenInfo.issueTimestamp)
                .setMaxTimestamp(tokenInfo.maxTimestamp)
                .setExpiryTimestamp(tokenInfo.expiryTimestamp)
                .setHmac(hmac)
                .setRenewers(
                    tokenInfo.renewers.map { principal ->
                        DescribedDelegationTokenRenewer()
                            .setPrincipalName(principal.name)
                            .setPrincipalType(principal.principalType)
                    }
                )

            if (version > 2) {
                ddt.setTokenRequesterPrincipalType(tokenInfo.tokenRequester.principalType)
                    .setTokenRequesterPrincipalName(tokenInfo.tokenRequester.name)
            }
            ddt
        }

        data = DescribeDelegationTokenResponseData()
            .setThrottleTimeMs(throttleTimeMs)
            .setErrorCode(error.code)
            .setTokens(describedDelegationTokenList)
    }

    constructor(
        data: DescribeDelegationTokenResponseData,
    ) : super(ApiKeys.DESCRIBE_DELEGATION_TOKEN) {
        this.data = data
    }

    override fun errorCounts(): Map<Errors, Int> = errorCounts(error())

    override fun data(): DescribeDelegationTokenResponseData = data

    override fun throttleTimeMs(): Int = data.throttleTimeMs()

    override fun maybeSetThrottleTimeMs(throttleTimeMs: Int) {
        data.setThrottleTimeMs(throttleTimeMs)
    }

    fun error(): Errors = Errors.forCode(data.errorCode())

    fun tokens(): List<DelegationToken> {
        return data.tokens().map { ddt: DescribedDelegationToken ->
            DelegationToken(
                TokenInformation(
                    ddt.tokenId(),
                    KafkaPrincipal(ddt.principalType(), ddt.principalName()),
                    KafkaPrincipal(
                        principalType = ddt.tokenRequesterPrincipalType(),
                        name = ddt.tokenRequesterPrincipalName(),
                    ),
                    ddt.renewers().map { ddtr ->
                            KafkaPrincipal(
                                principalType = ddtr.principalType(),
                                name = ddtr.principalName(),
                            )
                        },
                    ddt.issueTimestamp(),
                    ddt.maxTimestamp(),
                    ddt.expiryTimestamp()
                ),
                ddt.hmac()
            )
        }
    }

    fun hasError(): Boolean = error() !== Errors.NONE

    override fun shouldClientThrottle(version: Short): Boolean = version >= 1

    companion object {

        fun parse(buffer: ByteBuffer, version: Short): DescribeDelegationTokenResponse =
            DescribeDelegationTokenResponse(
                DescribeDelegationTokenResponseData(ByteBufferAccessor(buffer), version)
            )
    }
}
