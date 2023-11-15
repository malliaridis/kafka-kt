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

import java.io.IOException
import java.net.InetAddress
import java.nio.ByteBuffer
import org.apache.kafka.common.message.EnvelopeRequestData
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.ByteBufferAccessor
import org.apache.kafka.common.security.auth.KafkaPrincipal
import org.apache.kafka.common.security.authenticator.DefaultKafkaPrincipalBuilder
import org.apache.kafka.test.TestUtils.toBuffer
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

class EnvelopeRequestTest {

    @Test
    fun testGetPrincipal() {
        val kafkaPrincipal = KafkaPrincipal(
            principalType = KafkaPrincipal.USER_TYPE,
            name = "principal",
            tokenAuthenticated = true,
        )
        val kafkaPrincipalBuilder = DefaultKafkaPrincipalBuilder(
            kerberosShortNamer = null,
            sslPrincipalMapper = null,
        )

        val requestBuilder = EnvelopeRequest.Builder(
            requestData = ByteBuffer.allocate(0),
            serializedPrincipal = kafkaPrincipalBuilder.serialize(kafkaPrincipal),
            clientAddress = "client-address".toByteArray()
        )
        val request = requestBuilder.build(EnvelopeRequestData.HIGHEST_SUPPORTED_VERSION)
        assertEquals(kafkaPrincipal, kafkaPrincipalBuilder.deserialize(request.requestPrincipal!!))
    }

    @Test
    @Throws(IOException::class)
    fun testToSend() {
        for (version in ApiKeys.ENVELOPE.allVersions()) {
            val requestData = ByteBuffer.wrap("foobar".toByteArray())
            val header = RequestHeader(
                requestApiKey = ApiKeys.ENVELOPE,
                requestVersion = version,
                clientId = "clientId",
                correlationId = 15,
            )
            val request = EnvelopeRequest.Builder(
                requestData = requestData,
                serializedPrincipal = "principal".toByteArray(),
                clientAddress = InetAddress.getLocalHost().address,
            ).build(version)

            val send = request.toSend(header)
            val buffer = toBuffer(send)
            assertEquals(send.size() - 4, buffer.getInt().toLong())
            val parsedHeader = RequestHeader.parse(buffer)
            assertEquals(header.size(), parsedHeader.size())
            assertEquals(header, parsedHeader)

            val parsedRequestData = EnvelopeRequestData()
            parsedRequestData.read(ByteBufferAccessor(buffer), version)
            assertEquals(request.data(), parsedRequestData)
        }
    }
}
