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

package org.apache.kafka.common.security.oauthbearer.internals.secured

import com.fasterxml.jackson.databind.ObjectMapper
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.IOException
import java.io.InputStream
import java.io.OutputStream
import java.net.HttpURLConnection
import java.util.Base64
import org.junit.jupiter.api.Test
import org.mockito.kotlin.any
import org.mockito.kotlin.mock
import org.mockito.kotlin.whenever
import kotlin.random.Random
import kotlin.test.assertContains
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotNull

class HttpAccessTokenRetrieverTest : OAuthBearerTest() {

    @Test
    @Throws(IOException::class)
    fun test() {
        val expectedResponse = "Hiya, buddy"
        val mockedCon = createHttpURLConnection(expectedResponse)
        val response = HttpAccessTokenRetriever.post(
            connection = mockedCon,
            headers = null,
            requestBody = null,
            connectTimeoutMs = null,
            readTimeoutMs = null,
        )
        assertEquals(expectedResponse, response)
    }

    @Test
    @Throws(IOException::class)
    fun testEmptyResponse() {
        val mockedCon = createHttpURLConnection("")
        assertFailsWith<IOException> {
            HttpAccessTokenRetriever.post(
                connection = mockedCon,
                headers = null,
                requestBody = null,
                connectTimeoutMs = null,
                readTimeoutMs = null,
            )
        }
    }

    @Test
    @Throws(IOException::class)
    fun testErrorReadingResponse() {
        val mockedCon = createHttpURLConnection("dummy")
        whenever(mockedCon.inputStream).thenThrow(IOException("Can't read"))
        assertFailsWith<IOException> {
            HttpAccessTokenRetriever.post(
                connection = mockedCon,
                headers = null,
                requestBody = null,
                connectTimeoutMs = null,
                readTimeoutMs = null,
            )
        }
    }

    @Test
    @Throws(IOException::class)
    fun testErrorResponseUnretryableCode() {
        val mockedCon = createHttpURLConnection("dummy")
        whenever(mockedCon.inputStream).thenThrow(IOException("Can't read"))
        whenever(mockedCon.errorStream).thenReturn(
            ByteArrayInputStream(
                """{"error":"some_arg", "error_description":"some problem with arg"}""".toByteArray()
            )
        )
        whenever(mockedCon.getResponseCode()).thenReturn(HttpURLConnection.HTTP_BAD_REQUEST)
        val ioe = assertFailsWith<UnretryableException> {
            HttpAccessTokenRetriever.post(
                connection = mockedCon,
                headers = null,
                requestBody = null,
                connectTimeoutMs = null,
                readTimeoutMs = null,
            )
        }
        val message = assertNotNull(ioe.message)
        assertContains(message, """{"some_arg" - "some problem with arg"}""")
    }

    @Test
    @Throws(IOException::class)
    fun testErrorResponseRetryableCode() {
        val mockedCon = createHttpURLConnection("dummy")
        whenever(mockedCon.inputStream).thenThrow(IOException("Can't read"))
        whenever(mockedCon.errorStream).thenReturn(
            ByteArrayInputStream(
                """{"error":"some_arg", "error_description":"some problem with arg"}""".toByteArray()
            )
        )
        whenever(mockedCon.getResponseCode()).thenReturn(HttpURLConnection.HTTP_INTERNAL_ERROR)
        var ioe = assertFailsWith<IOException> {
            HttpAccessTokenRetriever.post(
                connection = mockedCon,
                headers = null,
                requestBody = null,
                connectTimeoutMs = null,
                readTimeoutMs = null,
            )
        }
        assertContains(ioe.message!!, """{"some_arg" - "some problem with arg"}""")

        // error response body has different keys
        whenever(mockedCon.errorStream).thenReturn(
            ByteArrayInputStream(
                """{"errorCode":"some_arg", "errorSummary":"some problem with arg"}""".toByteArray()
            )
        )
        ioe = assertFailsWith<IOException> {
            HttpAccessTokenRetriever.post(
                connection = mockedCon,
                headers = null,
                requestBody = null,
                connectTimeoutMs = null,
                readTimeoutMs = null,
            )
        }
        assertContains(ioe.message!!, """{"some_arg" - "some problem with arg"}""")

        // error response is valid json but unknown keys
        whenever(mockedCon.errorStream).thenReturn(
            ByteArrayInputStream(
                """{"err":"some_arg", "err_des":"some problem with arg"}""".toByteArray()
            )
        )
        ioe = assertFailsWith<IOException> {
            HttpAccessTokenRetriever.post(
                connection = mockedCon,
                headers = null,
                requestBody = null,
                connectTimeoutMs = null,
                readTimeoutMs = null,
            )
        }
        assertContains(ioe.message!!, """{"err":"some_arg", "err_des":"some problem with arg"}""")
    }

    @Test
    @Throws(IOException::class)
    fun testErrorResponseIsInvalidJson() {
        val mockedCon = createHttpURLConnection("dummy")
        whenever(mockedCon.inputStream).thenThrow(IOException("Can't read"))
        whenever(mockedCon.errorStream).thenReturn(
            ByteArrayInputStream("non json error output".toByteArray())
        )
        whenever(mockedCon.getResponseCode()).thenReturn(HttpURLConnection.HTTP_INTERNAL_ERROR)
        val ioe = assertFailsWith<IOException> {
            HttpAccessTokenRetriever.post(
                connection = mockedCon,
                headers = null,
                requestBody = null,
                connectTimeoutMs = null,
                readTimeoutMs = null,
            )
        }
        assertContains(ioe.message!!, "{non json error output}")
    }

    @Test
    @Throws(IOException::class)
    fun testCopy() {
        val expected = ByteArray(4096 + 1)
        Random.nextBytes(expected)
        val inputStream = ByteArrayInputStream(expected)
        val out = ByteArrayOutputStream()
        HttpAccessTokenRetriever.copy(inputStream, out)
        assertContentEquals(expected, out.toByteArray())
    }

    @Test
    @Throws(IOException::class)
    fun testCopyError() {
        val mockedIn = mock<InputStream>()
        val out: OutputStream = ByteArrayOutputStream()
        whenever(mockedIn.read(any<ByteArray>())).thenThrow(IOException())
        assertFailsWith<IOException> { HttpAccessTokenRetriever.copy(mockedIn, out) }
    }

    @Test
    @Throws(IOException::class)
    fun testParseAccessToken() {
        val expected = "abc"
        val mapper = ObjectMapper()
        val node = mapper.createObjectNode()
        node.put("access_token", expected)
        val actual = HttpAccessTokenRetriever.parseAccessToken(mapper.writeValueAsString(node))
        assertEquals(expected, actual)
    }

    @Test
    fun testParseAccessTokenEmptyAccessToken() {
        val mapper = ObjectMapper()
        val node = mapper.createObjectNode()
        node.put("access_token", "")
        assertFailsWith<IllegalArgumentException> {
            HttpAccessTokenRetriever.parseAccessToken(mapper.writeValueAsString(node))
        }
    }

    @Test
    fun testParseAccessTokenMissingAccessToken() {
        val mapper = ObjectMapper()
        val node = mapper.createObjectNode()
        node.put("sub", "jdoe")
        assertFailsWith<IllegalArgumentException> {
            HttpAccessTokenRetriever.parseAccessToken(mapper.writeValueAsString(node))
        }
    }

    @Test
    fun testParseAccessTokenInvalidJson() {
        assertFailsWith<IOException> {
            HttpAccessTokenRetriever.parseAccessToken("not valid JSON")
        }
    }

    @Test
    fun testFormatAuthorizationHeader() {
        assertAuthorizationHeader("id", "secret")
    }

    @Test
    fun testFormatAuthorizationHeaderEncoding() {
        // See KAFKA-14496
        assertAuthorizationHeader("SOME_RANDOM_LONG_USER_01234", "9Q|0`8i~ute-n9ksjLWb\\50\"AX@UUED5E")
    }

    private fun assertAuthorizationHeader(clientId: String, clientSecret: String) {
        val expected = "Basic " + Base64.getEncoder().encodeToString("$clientId:$clientSecret".toByteArray())
        val actual = HttpAccessTokenRetriever.formatAuthorizationHeader(clientId, clientSecret)
        assertEquals(
            expected = expected,
            actual = actual,
            message = "Expected the HTTP Authorization header generated for client ID \"$clientId\" and " +
                    "client secret \"$clientSecret\" to match"
        )
    }

    @Test
    fun testFormatAuthorizationHeaderMissingValues() {
        // Kotlin Migration: client ID and secret are non-nullable
//        assertFailsWith<IllegalArgumentException> {
//            HttpAccessTokenRetriever.formatAuthorizationHeader(clientId = null, clientSecret = "secret")
//        }
//        assertFailsWith<IllegalArgumentException> {
//            HttpAccessTokenRetriever.formatAuthorizationHeader(clientId = "id", clientSecret = null)
//        }
//        assertFailsWith<IllegalArgumentException> {
//            HttpAccessTokenRetriever.formatAuthorizationHeader(clientId = null, clientSecret = null)
//        }
        assertFailsWith<IllegalArgumentException> {
            HttpAccessTokenRetriever.formatAuthorizationHeader(clientId = "", clientSecret = "secret")
        }
        assertFailsWith<IllegalArgumentException> {
            HttpAccessTokenRetriever.formatAuthorizationHeader(clientId = "id", clientSecret = "")
        }
        assertFailsWith<IllegalArgumentException> {
            HttpAccessTokenRetriever.formatAuthorizationHeader(clientId = "", clientSecret = "")
        }
        assertFailsWith<IllegalArgumentException> {
            HttpAccessTokenRetriever.formatAuthorizationHeader(clientId = "  ", clientSecret = "secret")
        }
        assertFailsWith<IllegalArgumentException> {
            HttpAccessTokenRetriever.formatAuthorizationHeader(clientId = "id", clientSecret = "  ")
        }
        assertFailsWith<IllegalArgumentException> {
            HttpAccessTokenRetriever.formatAuthorizationHeader(clientId = "  ", clientSecret = "  ")
        }
    }

    @Test
    @Throws(IOException::class)
    fun testFormatRequestBody() {
        val expected = "grant_type=client_credentials&scope=scope"
        val actual = HttpAccessTokenRetriever.formatRequestBody(scope = "scope")
        assertEquals(expected, actual)
    }

    @Test
    @Throws(IOException::class)
    fun testFormatRequestBodyWithEscaped() {
        val questionMark = "%3F"
        val exclamationMark = "%21"
        var expected = "grant_type=client_credentials&scope=earth+is+great$exclamationMark"
        var actual = HttpAccessTokenRetriever.formatRequestBody(scope = "earth is great!")
        assertEquals(expected, actual)
        expected = "grant_type=client_credentials&scope=what+on+earth" +
                "$questionMark$exclamationMark$questionMark$exclamationMark$questionMark"
        actual = HttpAccessTokenRetriever.formatRequestBody(scope = "what on earth?!?!?")
        assertEquals(expected, actual)
    }

    @Test
    @Throws(IOException::class)
    fun testFormatRequestBodyMissingValues() {
        val expected = "grant_type=client_credentials"
        var actual = HttpAccessTokenRetriever.formatRequestBody(scope = null)
        assertEquals(expected, actual)
        actual = HttpAccessTokenRetriever.formatRequestBody(scope = "")
        assertEquals(expected, actual)
        actual = HttpAccessTokenRetriever.formatRequestBody(scope = "  ")
        assertEquals(expected, actual)
    }
}
