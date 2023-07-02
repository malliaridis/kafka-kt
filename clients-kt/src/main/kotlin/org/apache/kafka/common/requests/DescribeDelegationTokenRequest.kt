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

import org.apache.kafka.common.message.DescribeDelegationTokenRequestData
import org.apache.kafka.common.message.DescribeDelegationTokenRequestData.DescribeDelegationTokenOwner
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.ByteBufferAccessor
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.security.auth.KafkaPrincipal
import java.nio.ByteBuffer

class DescribeDelegationTokenRequest(
    private val data: DescribeDelegationTokenRequestData,
    version: Short,
) : AbstractRequest(ApiKeys.DESCRIBE_DELEGATION_TOKEN, version) {

    override fun data(): DescribeDelegationTokenRequestData = data

    fun ownersListEmpty(): Boolean = data.owners() != null && data.owners().isEmpty()

    override fun getErrorResponse(throttleTimeMs: Int, e: Throwable): AbstractResponse =
        DescribeDelegationTokenResponse(
            version = version.toInt(),
            throttleTimeMs = throttleTimeMs,
            error = Errors.forException(e)
        )

    class Builder(
        owners: List<KafkaPrincipal>?,
    ) : AbstractRequest.Builder<DescribeDelegationTokenRequest>(ApiKeys.DESCRIBE_DELEGATION_TOKEN) {
        private val data: DescribeDelegationTokenRequestData

        init {
            data = DescribeDelegationTokenRequestData().setOwners(
                owners?.map { (principalType, name) ->
                    DescribeDelegationTokenOwner()
                        .setPrincipalName(name)
                        .setPrincipalType(principalType)
                }
            )
        }

        override fun build(version: Short): DescribeDelegationTokenRequest =
            DescribeDelegationTokenRequest(data, version)

        override fun toString(): String = data.toString()
    }

    companion object {

        fun parse(buffer: ByteBuffer, version: Short): DescribeDelegationTokenRequest =
            DescribeDelegationTokenRequest(
                DescribeDelegationTokenRequestData(ByteBufferAccessor(buffer), version),
                version,
            )
    }
}
