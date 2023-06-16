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
import org.apache.kafka.common.message.SaslAuthenticateResponseData
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.ByteBufferAccessor
import org.apache.kafka.common.protocol.Errors

/**
 * Response from SASL server which for a SASL challenge as defined by the SASL protocol for the
 * mechanism configured for the client.
 */
class SaslAuthenticateResponse(
    private val data: SaslAuthenticateResponseData,
) : AbstractResponse(ApiKeys.SASL_AUTHENTICATE) {

    /**
     * Possible error codes:
     * SASL_AUTHENTICATION_FAILED(57) : Authentication failed
     */
    fun error(): Errors = Errors.forCode(data.errorCode())

    override fun errorCounts(): Map<Errors, Int> = errorCounts(Errors.forCode(data.errorCode()))

    fun errorMessage(): String = data.errorMessage()

    fun sessionLifetimeMs(): Long = data.sessionLifetimeMs()

    fun saslAuthBytes(): ByteArray = data.authBytes()

    override fun throttleTimeMs(): Int = DEFAULT_THROTTLE_TIME

    // Not supported by the response schema
    override fun maybeSetThrottleTimeMs(throttleTimeMs: Int) = Unit

    override fun data(): SaslAuthenticateResponseData = data

    companion object {

        fun parse(buffer: ByteBuffer, version: Short): SaslAuthenticateResponse =
            SaslAuthenticateResponse(
                SaslAuthenticateResponseData(ByteBufferAccessor(buffer), version)
            )
    }
}
