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

import org.apache.kafka.common.message.CreateDelegationTokenRequestData
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.ByteBufferAccessor
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.security.auth.KafkaPrincipal
import java.nio.ByteBuffer

class CreateDelegationTokenRequest private constructor(
    private val data: CreateDelegationTokenRequestData,
    version: Short,
) : AbstractRequest(ApiKeys.CREATE_DELEGATION_TOKEN, version) {

    override fun data(): CreateDelegationTokenRequestData = data

    override fun getErrorResponse(throttleTimeMs: Int, e: Throwable): AbstractResponse =
        CreateDelegationTokenResponse.prepareResponse(
            version = version.toInt(),
            throttleTimeMs = throttleTimeMs,
            error = Errors.forException(e),
            owner = KafkaPrincipal.ANONYMOUS,
            tokenRequester = KafkaPrincipal.ANONYMOUS,
        )

    class Builder(
        private val data: CreateDelegationTokenRequestData,
    ) : AbstractRequest.Builder<CreateDelegationTokenRequest>(ApiKeys.CREATE_DELEGATION_TOKEN) {

        override fun build(version: Short): CreateDelegationTokenRequest =
            CreateDelegationTokenRequest(data, version)

        override fun toString(): String = data.toString()
    }

    companion object {

        fun parse(buffer: ByteBuffer, version: Short): CreateDelegationTokenRequest =
            CreateDelegationTokenRequest(
                CreateDelegationTokenRequestData(ByteBufferAccessor(buffer), version),
                version,
            )
    }
}
