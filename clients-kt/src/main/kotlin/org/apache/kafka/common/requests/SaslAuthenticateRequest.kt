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
import org.apache.kafka.common.message.SaslAuthenticateRequestData
import org.apache.kafka.common.message.SaslAuthenticateResponseData
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.ByteBufferAccessor

/**
 * Request from SASL client containing client SASL authentication token as defined by the SASL
 * protocol for the configured SASL mechanism.
 *
 * For interoperability with versions prior to Kafka 1.0.0, this request is used only with broker
 * version 1.0.0 and higher that support SaslHandshake request v1. Clients connecting to older
 * brokers will send SaslHandshake request v0 followed by SASL tokens without the Kafka request
 * headers.
 */
class SaslAuthenticateRequest(
    private val data: SaslAuthenticateRequestData,
    version: Short,
) : AbstractRequest(ApiKeys.SASL_AUTHENTICATE, version) {

    override fun data(): SaslAuthenticateRequestData = data

    override fun getErrorResponse(throttleTimeMs: Int, e: Throwable): AbstractResponse {
        val (error, message) = ApiError.fromThrowable(e)
        val response = SaslAuthenticateResponseData()
            .setErrorCode(error.code)
            .setErrorMessage(message)

        return SaslAuthenticateResponse(response)
    }

    class Builder(
        private val data: SaslAuthenticateRequestData,
    ) : AbstractRequest.Builder<SaslAuthenticateRequest>(ApiKeys.SASL_AUTHENTICATE) {

        override fun build(version: Short): SaslAuthenticateRequest =
            SaslAuthenticateRequest(data, version)

        override fun toString(): String = "(type=SaslAuthenticateRequest)"
    }

    companion object {

        fun parse(buffer: ByteBuffer, version: Short): SaslAuthenticateRequest =
            SaslAuthenticateRequest(
                SaslAuthenticateRequestData(ByteBufferAccessor(buffer), version),
                version,
            )
    }
}
