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

import java.net.InetAddress
import java.nio.ByteBuffer
import java.util.*
import org.apache.kafka.common.errors.InvalidRequestException
import org.apache.kafka.common.message.ApiVersionsRequestData
import org.apache.kafka.common.network.ClientInformation
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.network.Send
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.security.auth.KafkaPrincipal
import org.apache.kafka.common.security.auth.KafkaPrincipalSerde
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.server.authorizer.AuthorizableRequestContext

class RequestContext(
    val header: RequestHeader,
    val connectionId: String,
    val clientAddress: InetAddress,
    val principal: KafkaPrincipal,
    val listenerName: ListenerName?,
    val securityProtocol: SecurityProtocol,
    val clientInformation: ClientInformation,
    val fromPrivilegedListener: Boolean,
    val principalSerde: KafkaPrincipalSerde? = null,
) : AuthorizableRequestContext {

    fun parseRequest(buffer: ByteBuffer?): RequestAndSize {
        if (isUnsupportedApiVersionsRequest) {
            // Unsupported ApiVersion requests are treated as v0 requests and are not parsed
            val apiVersionsRequest = ApiVersionsRequest(
                ApiVersionsRequestData(),
                0.toShort(),
                header.apiVersion
            )

            return RequestAndSize(apiVersionsRequest, 0)
        } else {
            val apiKey = header.apiKey
            try {
                val apiVersion = header.apiVersion
                return AbstractRequest.parseRequest(apiKey, apiVersion, buffer!!)
            } catch (ex: Throwable) {
                throw InvalidRequestException(
                    "Error getting request for apiKey: $apiKey" +
                            ", apiVersion: ${header.apiVersion}" +
                            ", connectionId: $connectionId" +
                            ", listenerName: $listenerName" +
                            ", principal: $principal",
                    ex
                )
            }
        }
    }

    /**
     * Build a [Send] for direct transmission of the provided response over the network.
     */
    fun buildResponseSend(body: AbstractResponse): Send {
        return body.toSend(header.toResponseHeader(), apiVersion())
    }

    /**
     * Serialize a response into a [ByteBuffer]. This is used when the response will be encapsulated
     * in an [EnvelopeResponse]. The buffer will contain both the serialized [ResponseHeader] as
     * well as the bytes from the response. There is no `size` prefix unlike the output from
     * [buildResponseSend].
     *
     * Note that envelope requests are reserved only for APIs which have set the
     * [ApiKeys.forwardable] flag. Notably the `Fetch` API cannot be forwarded, so we do not lose
     * the benefit of "zero copy" transfers from disk.
     */
    fun buildResponseEnvelopePayload(body: AbstractResponse): ByteBuffer {
        return body.serializeWithHeader(header.toResponseHeader(), apiVersion())
    }

    private val isUnsupportedApiVersionsRequest: Boolean
        get() = header.apiKey === ApiKeys.API_VERSIONS
                && !ApiKeys.API_VERSIONS.isVersionSupported(header.apiVersion)

    fun apiVersion(): Short {
        // Use v0 when serializing an unhandled ApiVersion response
        return if (isUnsupportedApiVersionsRequest) 0
        else header.apiVersion
    }

    override fun listenerName(): String? = listenerName?.value

    override fun securityProtocol(): SecurityProtocol = securityProtocol

    override fun principal(): KafkaPrincipal = principal

    override fun clientAddress(): InetAddress = clientAddress

    override fun requestType(): Int = header.apiKey.id.toInt()

    override fun requestVersion(): Int = header.apiVersion.toInt()

    override fun clientId(): String? = header.clientId

    override fun correlationId(): Int = header.correlationId

    override fun toString(): String {
        return "RequestContext(" +
                "header=$header" +
                ", connectionId='$connectionId'" +
                ", clientAddress=$clientAddress" +
                ", principal=$principal" +
                ", listenerName=$listenerName" +
                ", securityProtocol=$securityProtocol" +
                ", clientInformation=$clientInformation" +
                ", fromPrivilegedListener=$fromPrivilegedListener" +
                ", principalSerde=$principalSerde" +
                ')'
    }
}
