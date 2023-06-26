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
import org.apache.kafka.common.message.SaslHandshakeRequestData
import org.apache.kafka.common.message.SaslHandshakeResponseData
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.ByteBufferAccessor

/**
 * Request from SASL client containing client SASL mechanism.
 *
 * For interoperability with Kafka 0.9.0.x, the mechanism flow may be omitted when using GSSAPI.
 * Hence this request should not conflict with the first GSSAPI client packet. For GSSAPI, the first
 * context establishment packet starts with byte 0x60 (APPLICATION-0 tag) followed by a
 * variable-length encoded size. This handshake request starts with a request header two-byte API
 * key set to 17, followed by a mechanism name, making it easy to distinguish from a GSSAPI packet.
 */
class SaslHandshakeRequest(
    private val data: SaslHandshakeRequestData,
    version: Short,
) : AbstractRequest(ApiKeys.SASL_HANDSHAKE, version) {

    override fun data(): SaslHandshakeRequestData = data

    override fun getErrorResponse(throttleTimeMs: Int, e: Throwable): AbstractResponse {
        val response = SaslHandshakeResponseData()
        response.setErrorCode(ApiError.fromThrowable(e).error.code)
        return SaslHandshakeResponse(response)
    }

    class Builder(
        private val data: SaslHandshakeRequestData,
    ) : AbstractRequest.Builder<SaslHandshakeRequest>(ApiKeys.SASL_HANDSHAKE) {

        override fun build(version: Short): SaslHandshakeRequest =
            SaslHandshakeRequest(data, version)

        override fun toString(): String = data.toString()
    }

    companion object {

        fun parse(buffer: ByteBuffer, version: Short): SaslHandshakeRequest =
            SaslHandshakeRequest(
                SaslHandshakeRequestData(ByteBufferAccessor(buffer), version),
                version,
            )
    }
}
