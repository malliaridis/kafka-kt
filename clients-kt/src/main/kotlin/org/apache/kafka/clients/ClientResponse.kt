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

import org.apache.kafka.common.errors.AuthenticationException
import org.apache.kafka.common.errors.UnsupportedVersionException
import org.apache.kafka.common.requests.AbstractResponse
import org.apache.kafka.common.requests.RequestHeader

/**
 * A response from the server. Contains both the body of the response as well as the correlated request
 * metadata that was originally sent.
 *
 * @property requestHeader The header of the corresponding request
 * @property callback The callback to be invoked
 * @property createdTimeMs The unix timestamp when the corresponding request was created
 * @property destination The node the corresponding request was sent to
 * @property receivedTimeMs The unix timestamp when this response was received
 * @property disconnected Whether the client disconnected before fully reading a response
 * @property versionMismatch Whether there was a version mismatch that prevented sending the
 * request.
 * @property responseBody The response contents (or null) if we disconnected, no response was
 * expected, or if there was a version mismatch.
 */
class ClientResponse(
    val requestHeader: RequestHeader,
    private val callback: RequestCompletionHandler?,
    val destination: String,
    createdTimeMs: Long,
    val receivedTimeMs: Long,
    val disconnected: Boolean,
    val versionMismatch: UnsupportedVersionException? = null,
    val authenticationException: AuthenticationException? = null,
    val responseBody: AbstractResponse? = null,
) {
    val requestLatencyMs: Long = receivedTimeMs - createdTimeMs

    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("receivedTimeMs"),
    )
    fun receivedTimeMs(): Long = receivedTimeMs

    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("disconnected"),
    )
    fun wasDisconnected(): Boolean = disconnected

    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("versionMismatch"),
    )
    fun versionMismatch(): UnsupportedVersionException? = versionMismatch

    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("authenticationException"),
    )
    fun authenticationException(): AuthenticationException? = authenticationException

    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("requestHeader"),
    )
    fun requestHeader(): RequestHeader = requestHeader

    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("destination"),
    )
    fun destination(): String = destination

    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("responseBody"),
    )
    fun responseBody(): AbstractResponse? = responseBody

    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("hasResponse"),
    )
    fun hasResponse(): Boolean = responseBody != null

    val hasResponse: Boolean = responseBody != null

    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("requestLatencyMs"),
    )
    fun requestLatencyMs(): Long = requestLatencyMs

    fun onComplete() = callback?.onComplete(this)

    override fun toString(): String {
        return "ClientResponse(receivedTimeMs=$receivedTimeMs" +
                ", latencyMs=$requestLatencyMs" +
                ", disconnected=$disconnected" +
                ", requestHeader=$requestHeader" +
                ", responseBody=$responseBody" +
                ")"
    }
}
