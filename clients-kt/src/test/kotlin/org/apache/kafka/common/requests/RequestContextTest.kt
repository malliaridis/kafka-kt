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
import java.net.UnknownHostException
import java.nio.ByteBuffer
import org.apache.kafka.common.errors.InvalidRequestException
import org.apache.kafka.common.message.ApiVersionsResponseData
import org.apache.kafka.common.message.ApiVersionsResponseData.ApiVersionCollection
import org.apache.kafka.common.message.CreateTopicsResponseData
import org.apache.kafka.common.message.CreateTopicsResponseData.CreatableTopicResult
import org.apache.kafka.common.message.CreateTopicsResponseData.CreatableTopicResultCollection
import org.apache.kafka.common.message.ProduceRequestData
import org.apache.kafka.common.message.ProduceRequestData.PartitionProduceData
import org.apache.kafka.common.message.ProduceRequestData.TopicProduceData
import org.apache.kafka.common.message.SaslAuthenticateRequestData
import org.apache.kafka.common.network.ClientInformation
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.ApiMessage
import org.apache.kafka.common.protocol.ByteBufferAccessor
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.protocol.ObjectSerializationCache
import org.apache.kafka.common.security.auth.KafkaPrincipal
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertIs
import kotlin.test.assertTrue

class RequestContextTest {

    @Test
    @Throws(Exception::class)
    fun testSerdeUnsupportedApiVersionRequest() {
        val correlationId = 23423
        val header = RequestHeader(
            requestApiKey = ApiKeys.API_VERSIONS,
            requestVersion = Short.MAX_VALUE,
            clientId = "",
            correlationId = correlationId,
        )
        val context = RequestContext(
            header = header,
            connectionId = "0",
            clientAddress = InetAddress.getLocalHost(),
            principal = KafkaPrincipal.ANONYMOUS,
            listenerName = ListenerName("ssl"),
            securityProtocol = SecurityProtocol.SASL_SSL,
            clientInformation = ClientInformation.EMPTY,
            fromPrivilegedListener = false,
        )
        assertEquals(0, context.apiVersion().toInt())

        // Write some garbage to the request buffer. This should be ignored since we will treat
        // the unknown version type as v0 which has an empty request body.
        val requestBuffer = ByteBuffer.allocate(8)
        requestBuffer.putInt(3709234)
        requestBuffer.putInt(29034)
        requestBuffer.flip()
        val requestAndSize = context.parseRequest(requestBuffer)
        val request = assertIs<ApiVersionsRequest>(requestAndSize.request)
        assertTrue(request.hasUnsupportedRequestVersion())
        val send = context.buildResponseSend(
            ApiVersionsResponse(
                ApiVersionsResponseData()
                    .setThrottleTimeMs(0)
                    .setErrorCode(Errors.UNSUPPORTED_VERSION.code)
                    .setApiKeys(ApiVersionCollection())
            )
        )
        val channel = ByteBufferChannel(256)
        send.writeTo(channel)
        val responseBuffer = channel.buffer()
        responseBuffer.flip()
        responseBuffer.getInt() // strip off the size
        val responseHeader = ResponseHeader.parse(
            buffer = responseBuffer,
            headerVersion = ApiKeys.API_VERSIONS.responseHeaderVersion(header.apiVersion)
        )
        assertEquals(correlationId, responseHeader.correlationId)
        val response = AbstractResponse.parseResponse(
            apiKey = ApiKeys.API_VERSIONS,
            responseBuffer = responseBuffer,
            version = 0,
        ) as ApiVersionsResponse
        assertEquals(Errors.UNSUPPORTED_VERSION.code, response.data().errorCode)
        assertTrue(response.data().apiKeys.isEmpty())
    }

    @Test
    @Throws(Exception::class)
    fun testEnvelopeResponseSerde() {
        val collection = CreatableTopicResultCollection()
        collection.add(
            CreatableTopicResult()
                .setTopicConfigErrorCode(Errors.CLUSTER_AUTHORIZATION_FAILED.code)
                .setNumPartitions(5)
        )
        val expectedResponse = CreateTopicsResponseData()
            .setThrottleTimeMs(10)
            .setTopics(collection)
        val correlationId = 15
        val clientId = "clientId"
        val header = RequestHeader(
            requestApiKey = ApiKeys.CREATE_TOPICS,
            requestVersion = ApiKeys.CREATE_TOPICS.latestVersion(),
            clientId = clientId,
            correlationId = correlationId,
        )
        val context = RequestContext(
            header = header,
            connectionId = "0",
            clientAddress = InetAddress.getLocalHost(),
            principal = KafkaPrincipal.ANONYMOUS,
            listenerName = ListenerName("ssl"),
            securityProtocol = SecurityProtocol.SASL_SSL,
            clientInformation = ClientInformation.EMPTY,
            fromPrivilegedListener = true,
        )
        val buffer = context.buildResponseEnvelopePayload(CreateTopicsResponse(expectedResponse))
        assertEquals(buffer.capacity(), buffer.limit(), "Buffer limit and capacity should be the same")
        val parsedResponse = AbstractResponse.parseResponse(buffer, header) as CreateTopicsResponse
        assertEquals(expectedResponse, parsedResponse.data())
    }

    @Test
    @Throws(UnknownHostException::class)
    fun testInvalidRequestForImplicitHashCollection() {
        val version = 5.toShort() // choose a version with fixed length encoding, for simplicity
        val corruptBuffer = produceRequest(version)
        // corrupt the length of the topics array
        corruptBuffer.putInt(8, (Int.MAX_VALUE - 1) / 2)
        val header = RequestHeader(
            requestApiKey = ApiKeys.PRODUCE,
            requestVersion = version,
            clientId = "console-producer",
            correlationId = 3,
        )
        val context = RequestContext(
            header = header,
            connectionId = "0",
            clientAddress = InetAddress.getLocalHost(),
            principal = KafkaPrincipal.ANONYMOUS,
            listenerName = ListenerName("ssl"),
            securityProtocol = SecurityProtocol.SASL_SSL,
            clientInformation = ClientInformation.EMPTY,
            fromPrivilegedListener = true,
        )
        val error = assertFailsWith<InvalidRequestException> { context.parseRequest(corruptBuffer) }
        assertEquals(
            expected = "Tried to allocate a collection of size 1073741823, but there are only 17 bytes remaining.",
            actual = error.cause!!.message,
        )
    }

    @Test
    @Throws(UnknownHostException::class)
    fun testInvalidRequestForArrayList() {
        val version = 5.toShort() // choose a version with fixed length encoding, for simplicity
        val corruptBuffer = produceRequest(version)
        // corrupt the length of the partitions array
        corruptBuffer.putInt(17, Int.MAX_VALUE)
        val header = RequestHeader(
            requestApiKey = ApiKeys.PRODUCE,
            requestVersion = version,
            clientId = "console-producer",
            correlationId = 3,
        )
        val context = RequestContext(
            header = header,
            connectionId = "0",
            clientAddress = InetAddress.getLocalHost(),
            principal = KafkaPrincipal.ANONYMOUS,
            listenerName = ListenerName("ssl"),
            securityProtocol = SecurityProtocol.SASL_SSL,
            clientInformation = ClientInformation.EMPTY,
            fromPrivilegedListener = true,
        )
        val error = assertFailsWith<InvalidRequestException> { context.parseRequest(corruptBuffer) }
        assertEquals(
            expected = "Tried to allocate a collection of size 2147483647, but there are only 8 bytes remaining.",
            actual = error.cause!!.message,
        )
    }

    private fun produceRequest(version: Short): ByteBuffer {
        val data = ProduceRequestData()
            .setAcks(-1)
            .setTimeoutMs(1)
        data.topicData.add(
            TopicProduceData()
                .setName("foo")
                .setPartitionData(
                    listOf(PartitionProduceData().setIndex(42))
                )
        )
        return serialize(version, data)
    }

    private fun serialize(version: Short, data: ApiMessage): ByteBuffer {
        val cache = ObjectSerializationCache()
        data.size(cache, version)
        val buffer = ByteBuffer.allocate(1024)
        data.write(ByteBufferAccessor(buffer), cache, version)
        buffer.flip()
        return buffer
    }

    @Test
    @Throws(UnknownHostException::class)
    fun testInvalidRequestForByteArray() {
        val version = 1.toShort() // choose a version with fixed length encoding, for simplicity
        val corruptBuffer = serialize(
            version = version,
            data = SaslAuthenticateRequestData().setAuthBytes(ByteArray(0)),
        )
        // corrupt the length of the bytes array
        corruptBuffer.putInt(0, Int.MAX_VALUE)
        val header = RequestHeader(
            requestApiKey = ApiKeys.SASL_AUTHENTICATE,
            requestVersion = version,
            clientId = "console-producer",
            correlationId = 1,
        )
        val context = RequestContext(
            header = header,
            connectionId = "0",
            clientAddress = InetAddress.getLocalHost(),
            principal = KafkaPrincipal.ANONYMOUS,
            listenerName = ListenerName("ssl"),
            securityProtocol = SecurityProtocol.SASL_SSL,
            clientInformation = ClientInformation.EMPTY,
            fromPrivilegedListener = true,
        )
        val error = assertFailsWith<InvalidRequestException> { context.parseRequest(corruptBuffer) }
        assertEquals(
            expected = "Error reading byte array of 2147483647 byte(s): only 0 byte(s) available",
            actual = error.cause!!.message,
        )
    }
}
