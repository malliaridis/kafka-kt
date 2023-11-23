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
import org.apache.kafka.common.message.SaslHandshakeResponseData
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.ByteBufferAccessor
import org.apache.kafka.common.protocol.Errors

/**
 * Response from SASL server which indicates if the client-chosen mechanism is enabled in the
 * server. For error responses, the list of enabled mechanisms is included in the response.
 */
class SaslHandshakeResponse(
    private val data: SaslHandshakeResponseData,
) : AbstractResponse(ApiKeys.SASL_HANDSHAKE) {

    /*
     * Possible error codes:
     * - UNSUPPORTED_SASL_MECHANISM(33): Client mechanism not enabled in server
     * - ILLEGAL_SASL_STATE(34) : Invalid request during SASL handshake
     */
    fun error(): Errors = Errors.forCode(data.errorCode)

    override fun errorCounts(): Map<Errors, Int> = errorCounts(Errors.forCode(data.errorCode))

    override fun throttleTimeMs(): Int = DEFAULT_THROTTLE_TIME

    // Not supported by the response schema
    override fun maybeSetThrottleTimeMs(throttleTimeMs: Int) = Unit

    override fun data(): SaslHandshakeResponseData = data

    fun enabledMechanisms(): List<String> = data.mechanisms

    companion object {

        fun parse(buffer: ByteBuffer, version: Short): SaslHandshakeResponse =
            SaslHandshakeResponse(
                SaslHandshakeResponseData(ByteBufferAccessor(buffer), version)
            )
    }
}
