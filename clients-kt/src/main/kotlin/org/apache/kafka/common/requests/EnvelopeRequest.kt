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

import org.apache.kafka.common.message.EnvelopeRequestData
import org.apache.kafka.common.message.EnvelopeResponseData
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.ByteBufferAccessor
import org.apache.kafka.common.protocol.Errors
import java.nio.ByteBuffer

class EnvelopeRequest(
    private val data: EnvelopeRequestData,
    version: Short,
) : AbstractRequest(ApiKeys.ENVELOPE, version) {

    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("requestData"),
    )
    fun requestData(): ByteBuffer = data.requestData

    val requestData: ByteBuffer
        get() = data.requestData

    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("clientAddress"),
    )
    fun clientAddress(): ByteArray = data.clientHostAddress

    val clientAddress: ByteArray
        get() = data.clientHostAddress

    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("requestPrincipal"),
    )
    fun requestPrincipal(): ByteArray? = data.requestPrincipal

    val requestPrincipal: ByteArray?
        get() = data.requestPrincipal

    override fun getErrorResponse(throttleTimeMs: Int, e: Throwable): AbstractResponse =
        EnvelopeResponse(EnvelopeResponseData().setErrorCode(Errors.forException(e).code))

    override fun data(): EnvelopeRequestData = data

    class Builder(
        requestData: ByteBuffer,
        serializedPrincipal: ByteArray?,
        clientAddress: ByteArray,
    ) :
        AbstractRequest.Builder<EnvelopeRequest>(ApiKeys.ENVELOPE) {
        private val data: EnvelopeRequestData

        init {
            data = EnvelopeRequestData()
                .setRequestData(requestData)
                .setRequestPrincipal(serializedPrincipal)
                .setClientHostAddress(clientAddress)
        }

        override fun build(version: Short): EnvelopeRequest = EnvelopeRequest(data, version)

        override fun toString(): String = data.toString()
    }

    companion object {

        fun parse(buffer: ByteBuffer, version: Short): EnvelopeRequest =
            EnvelopeRequest(EnvelopeRequestData(ByteBufferAccessor(buffer), version), version)
    }
}
