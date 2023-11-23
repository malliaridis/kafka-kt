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

package org.apache.kafka.common.security.oauthbearer.internals

import javax.security.sasl.SaslException
import org.apache.kafka.common.security.auth.SaslExtensions
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class OAuthBearerClientInitialResponseTest {

    // Test how a client would build a response
    @Test
    @Throws(Exception::class)
    fun testBuildClientResponseToBytes() {
        val expectedMesssage = "n,,\u0001auth=Bearer 123.345.567\u0001nineteen=42\u0001\u0001"
        val extensions = mapOf("nineteen" to "42")
        val response = OAuthBearerClientInitialResponse(
            tokenValue = "123.345.567",
            extensions = SaslExtensions(extensions),
        )
        val message = String(response.toBytes())
        assertEquals(expectedMesssage, message)
    }

    @Test
    @Throws(Exception::class)
    fun testBuildServerResponseToBytes() {
        val serverMessage = "n,,\u0001auth=Bearer 123.345.567\u0001nineteen=42\u0001\u0001"
        val response = OAuthBearerClientInitialResponse(serverMessage.toByteArray())
        val message = String(response.toBytes())
        assertEquals(serverMessage, message)
    }

    @Test
    @Throws(Exception::class)
    fun testThrowsSaslExceptionOnInvalidExtensionKey() {
        val extensions = mapOf("19" to "42") // keys can only be a-z
        assertFailsWith<SaslException> {
            OAuthBearerClientInitialResponse(
                tokenValue = "123.345.567",
                extensions = SaslExtensions(extensions),
            )
        }
    }

    @Test
    @Throws(Exception::class)
    fun testToken() {
        val message = "n,,\u0001auth=Bearer 123.345.567\u0001\u0001"
        val response = OAuthBearerClientInitialResponse(message.toByteArray())
        assertEquals("123.345.567", response.tokenValue)
        assertEquals("", response.authorizationId)
    }

    @Test
    @Throws(Exception::class)
    fun testAuthorizationId() {
        val message = "n,a=myuser,\u0001auth=Bearer 345\u0001\u0001"
        val response = OAuthBearerClientInitialResponse(message.toByteArray())
        assertEquals("345", response.tokenValue)
        assertEquals("myuser", response.authorizationId)
    }

    @Test
    @Throws(Exception::class)
    fun testExtensions() {
        val message = "n,,\u0001propA=valueA1, valueA2\u0001auth=Bearer 567\u0001propB=valueB\u0001\u0001"
        val response = OAuthBearerClientInitialResponse(message.toByteArray())
        assertEquals("567", response.tokenValue)
        assertEquals("", response.authorizationId)
        assertEquals("valueA1, valueA2", response.saslExtensions.map()["propA"])
        assertEquals("valueB", response.saslExtensions.map()["propB"])
    }

    // The example in the RFC uses `vF9dft4qmTc2Nvb3RlckBhbHRhdmlzdGEuY29tCg==` as the token
    // But since we use Base64Url encoding, padding is omitted. Hence, this test verifies without '='.
    @Test
    @Throws(Exception::class)
    fun testRfc7688Example() {
        val message = "n,a=user@example.com,\u0001host=server.example.com\u0001port=143\u0001" +
                "auth=Bearer vF9dft4qmTc2Nvb3RlckBhbHRhdmlzdGEuY29tCg\u0001\u0001"
        val response = OAuthBearerClientInitialResponse(message.toByteArray())
        assertEquals("vF9dft4qmTc2Nvb3RlckBhbHRhdmlzdGEuY29tCg", response.tokenValue)
        assertEquals("user@example.com", response.authorizationId)
        assertEquals("server.example.com", response.saslExtensions.map()["host"])
        assertEquals("143", response.saslExtensions.map()["port"])
    }

    @Test
    @Throws(Exception::class)
    fun testNoExtensionsFromByteArray() {
        val message = "n,a=user@example.com,\u0001" +
                "auth=Bearer vF9dft4qmTc2Nvb3RlckBhbHRhdmlzdGEuY29tCg\u0001\u0001"
        val response = OAuthBearerClientInitialResponse(message.toByteArray())
        assertEquals("vF9dft4qmTc2Nvb3RlckBhbHRhdmlzdGEuY29tCg", response.tokenValue)
        assertEquals("user@example.com", response.authorizationId)
        assertTrue(response.saslExtensions.map().isEmpty())
    }

    @Test
    @Throws(Exception::class)
    fun testNoExtensionsFromTokenAndNullExtensions() {
        val response = OAuthBearerClientInitialResponse(tokenValue = "token")
        assertTrue(response.saslExtensions.map().isEmpty())
    }

    @Test
    @Throws(Exception::class)
    fun testValidateNullExtensions() {
        OAuthBearerClientInitialResponse.validateExtensions(extensions = null)
    }
}
