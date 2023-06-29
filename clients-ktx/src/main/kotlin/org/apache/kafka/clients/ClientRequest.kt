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

package org.apache.kafka.clients

import org.apache.kafka.common.message.RequestHeaderData
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.requests.AbstractRequest
import org.apache.kafka.common.requests.RequestHeader

/**
 * A request being sent to the server. This holds both the network send and the client-level
 * metadata.
 *
 * @property destination The brokerId to send the request to
 * @property requestBuilder The builder for the request to make
 * @property correlationId The correlation id for this client request
 * @property clientId The client ID to use for the header
 * @property createdTimeMs The unix timestamp in milliseconds for the time at which this request was
 * created.
 * @property expectResponse Should we expect a response message or is this request complete once it
 * is sent?
 * @property callback A callback to execute when the response has been received (or null if no
 * callback is necessary)
 */
data class ClientRequest(
    val destination: String,
    val requestBuilder: AbstractRequest.Builder<*>,
    val correlationId: Int,
    val clientId: String,
    val createdTimeMs: Long,
    val expectResponse: Boolean,
    val requestTimeoutMs: Int,
    val callback: RequestCompletionHandler,
) {
    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("expectResponse"),
    )
    fun expectResponse(): Boolean = expectResponse

    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("apiKey"),
    )
    fun apiKey(): ApiKeys = requestBuilder.apiKey

    val apiKey: ApiKeys
        get() =  requestBuilder.apiKey

    fun makeHeader(version: Short): RequestHeader {
        val requestApiKey = apiKey()
        return RequestHeader(
            RequestHeaderData()
                .setRequestApiKey(requestApiKey.id)
                .setRequestApiVersion(version)
                .setClientId(clientId)
                .setCorrelationId(correlationId),
            requestApiKey.requestHeaderVersion(version)
        )
    }

    fun requestBuilder(): AbstractRequest.Builder<*> {
        return requestBuilder
    }

    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith(" destination"),
    )
    fun destination(): String = destination

    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("callback"),
    )
    fun callback(): RequestCompletionHandler = callback

    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("createdTimeMs"),
    )
    fun createdTimeMs(): Long = createdTimeMs

    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("correlationId"),
    )
    fun correlationId(): Int = correlationId

    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("requestTimeoutMs"),
    )
    fun requestTimeoutMs(): Int = requestTimeoutMs

    override fun toString(): String {
        return "ClientRequest(expectResponse=$expectResponse" +
                ", callback=$callback" +
                ", destination=$destination" +
                ", correlationId=$correlationId" +
                ", clientId=$clientId" +
                ", createdTimeMs=$createdTimeMs" +
                ", requestBuilder=$requestBuilder" +
                ")"
    }
}
