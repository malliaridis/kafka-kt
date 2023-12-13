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

package org.apache.kafka.common.security.oauthbearer

import java.io.IOException
import java.util.Base64
import java.util.Calendar
import java.util.TimeZone
import javax.security.auth.callback.Callback
import javax.security.auth.callback.UnsupportedCallbackException
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL
import org.apache.kafka.common.security.auth.SaslExtensionsCallback
import org.apache.kafka.common.security.oauthbearer.internals.OAuthBearerClientInitialResponse
import org.apache.kafka.common.security.oauthbearer.internals.secured.AccessTokenBuilder
import org.apache.kafka.common.security.oauthbearer.internals.secured.AccessTokenRetriever
import org.apache.kafka.common.security.oauthbearer.internals.secured.AccessTokenValidatorFactory
import org.apache.kafka.common.security.oauthbearer.internals.secured.FileTokenRetriever
import org.apache.kafka.common.security.oauthbearer.internals.secured.HttpAccessTokenRetriever
import org.apache.kafka.common.security.oauthbearer.internals.secured.OAuthBearerTest
import org.jose4j.jws.AlgorithmIdentifiers
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import kotlin.test.assertContains
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertIs
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.fail

class OAuthBearerLoginCallbackHandlerTest : OAuthBearerTest() {
    
    @Test
    @Throws(Exception::class)
    fun testHandleTokenCallback() {
        val configs = saslConfigs
        val builder = AccessTokenBuilder().apply {
            jwk = createRsaJwk()
            alg = AlgorithmIdentifiers.RSA_USING_SHA256
        }
        val accessToken = builder.build()
        val accessTokenRetriever = AccessTokenRetriever { accessToken }
        val handler = createHandler(accessTokenRetriever, configs)
        try {
            val callback = OAuthBearerTokenCallback()
            handler.handle(arrayOf(callback))
            val token = assertNotNull(callback.token)
            assertEquals(accessToken, token.value())
            assertEquals(builder.subject, token.principalName())
            assertEquals(builder.expirationSeconds!! * 1000, token.lifetimeMs())
            assertEquals(builder.issuedAtSeconds * 1000, token.startTimeMs())
        } finally {
            handler.close()
        }
    }

    @Test
    @Throws(Exception::class)
    fun testHandleSaslExtensionsCallback() {
        val handler = OAuthBearerLoginCallbackHandler()
        val configs = getSaslConfigs(
            name = SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL,
            value = "http://www.example.com",
        )
        val jaasConfig = mapOf(
            OAuthBearerLoginCallbackHandler.CLIENT_ID_CONFIG to "an ID",
            OAuthBearerLoginCallbackHandler.CLIENT_SECRET_CONFIG to "a secret",
            "extension_foo" to "1",
            "extension_bar" to 2,
            "EXTENSION_baz" to "3",
        )

        configureHandler(handler, configs, jaasConfig)
        try {
            val callback = SaslExtensionsCallback()
            handler.handle(arrayOf(callback))
            assertNotNull(callback.extensions)
            val extensions = callback.extensions.map()
            assertEquals("1", extensions["foo"])
            assertEquals("2", extensions["bar"])
            assertNull(extensions["baz"])
            assertEquals(2, extensions.size)
        } finally {
            handler.close()
        }
    }

    @Test
    fun testHandleSaslExtensionsCallbackWithInvalidExtension() {
        val illegalKey = "extension_${OAuthBearerClientInitialResponse.AUTH_KEY}"
        val handler = OAuthBearerLoginCallbackHandler()
        val configs = getSaslConfigs(
            name = SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL,
            value = "http://www.example.com",
        )
        val jaasConfig = mapOf(
            OAuthBearerLoginCallbackHandler.CLIENT_ID_CONFIG to "an ID",
            OAuthBearerLoginCallbackHandler.CLIENT_SECRET_CONFIG to "a secret",
            illegalKey to "this key isn't allowed per OAuthBearerClientInitialResponse.validateExtensions",
        )
        configureHandler(handler, configs, jaasConfig)
        try {
            val callback = SaslExtensionsCallback()
            assertThrowsWithMessage(
                clazz = ConfigException::class.java,
                executable = { handler.handle(arrayOf(callback)) },
                substring = "Extension name ${OAuthBearerClientInitialResponse.AUTH_KEY} is invalid",
            )
        } finally {
            handler.close()
        }
    }

    @Test
    fun testInvalidCallbackGeneratesUnsupportedCallbackException() {
        val configs = saslConfigs
        val handler = OAuthBearerLoginCallbackHandler()
        val accessTokenRetriever = AccessTokenRetriever { "foo" }
        val accessTokenValidator = AccessTokenValidatorFactory.create(configs)
        handler.init(accessTokenRetriever, accessTokenValidator)
        try {
            val unsupportedCallback = object : Callback {}
            assertFailsWith<UnsupportedCallbackException> { handler.handle(arrayOf(unsupportedCallback)) }
        } finally {
            handler.close()
        }
    }

    @Test
    @Throws(Exception::class)
    fun testInvalidAccessToken() {
        testInvalidAccessToken(
            accessToken = "this isn't valid",
            expectedMessageSubstring = "Malformed JWT provided",
        )
        testInvalidAccessToken(
            accessToken = "this.isn't.valid",
            expectedMessageSubstring = "malformed Base64 URL encoded value",
        )
        testInvalidAccessToken(
            accessToken = createAccessKey(header = "this", payload = "isn't", signature = "valid"),
            expectedMessageSubstring = "malformed JSON",
        )
        testInvalidAccessToken(
            accessToken = createAccessKey(header = "{}", payload = "{}", signature = "{}"),
            expectedMessageSubstring = "exp value must be non-null",
        )
    }

    @Test
    fun testMissingAccessToken() {
        val accessTokenRetriever = AccessTokenRetriever {
            throw IOException("The token endpoint response access_token value must be non-null")
        }
        val configs = saslConfigs
        val handler = createHandler(accessTokenRetriever, configs)
        try {
            val callback = OAuthBearerTokenCallback()
            assertThrowsWithMessage(
                clazz = IOException::class.java,
                executable = { handler.handle(arrayOf(callback)) },
                substring = "token endpoint response access_token value must be non-null",
            )
        } finally {
            handler.close()
        }
    }

    @Test
    @Throws(IOException::class)
    fun testFileTokenRetrieverHandlesNewline() {
        val cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"))
        val cur = cal.getTimeInMillis() / 1000
        val exp = "" + (cur + 60 * 60) // 1 hour in future
        val iat = "" + cur
        val expected = createAccessKey(
            header = "{}",
            payload = "{\"exp\":$exp, \"iat\":$iat, \"sub\":\"subj\"}",
            signature = "sign"
        )
        val withNewline = expected + "\n"
        val tmpDir = createTempDir("access-token")
        val accessTokenFile = createTempFile(
            tmpDir = tmpDir,
            prefix = "access-token-",
            suffix = ".json",
            contents = withNewline,
        )
        val configs = saslConfigs
        val handler = createHandler(
            accessTokenRetriever = FileTokenRetriever(accessTokenFile.toPath()),
            configs = configs,
        )
        val callback = OAuthBearerTokenCallback()
        try {
            handler.handle(arrayOf(callback))
            assertEquals(callback.token!!.value(), expected)
        } catch (e: Exception) {
            fail(cause = e)
        } finally {
            handler.close()
        }
    }

    @Test
    fun testNotConfigured() {
        val handler = OAuthBearerLoginCallbackHandler()
        assertThrowsWithMessage(
            clazz = IllegalStateException::class.java,
            executable = { handler.handle(arrayOf()) },
            substring = "first call the configure or init method",
        )
    }

    @Test
    @Throws(Exception::class)
    fun testConfigureWithAccessTokenFile() {
        val expected = "{}"
        val tmpDir = createTempDir("access-token")
        val accessTokenFile = createTempFile(
            tmpDir = tmpDir,
            prefix = "access-token-",
            suffix = ".json",
            contents = expected,
        )
        val handler = OAuthBearerLoginCallbackHandler()
        val configs = getSaslConfigs(
            name = SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL,
            value = accessTokenFile.toURI().toString(),
        )
        val jaasConfigs = emptyMap<String, Any>()
        configureHandler(handler, configs, jaasConfigs)
        assertIs<FileTokenRetriever>(handler.accessTokenRetriever)
    }

    @Test
    fun testConfigureWithAccessClientCredentials() {
        val handler = OAuthBearerLoginCallbackHandler()
        val configs = getSaslConfigs(
            name = SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL,
            value = "http://www.example.com",
        )
        val jaasConfigs = mapOf(
            OAuthBearerLoginCallbackHandler.CLIENT_ID_CONFIG to "an ID",
            OAuthBearerLoginCallbackHandler.CLIENT_SECRET_CONFIG to "a secret",
        )
        configureHandler(handler, configs, jaasConfigs)
        assertIs<HttpAccessTokenRetriever>(handler.accessTokenRetriever)
    }

    @Throws(Exception::class)
    private fun testInvalidAccessToken(accessToken: String, expectedMessageSubstring: String) {
        val configs = saslConfigs
        val handler = createHandler(
            accessTokenRetriever = { accessToken },
            configs = configs,
        )
        try {
            val callback = OAuthBearerTokenCallback()
            handler.handle(arrayOf(callback))
            assertNull(callback.token)
            val actualMessage = assertNotNull(callback.errorDescription)
            assertContains(
                charSequence = actualMessage,
                other = expectedMessageSubstring,
                message = "The error message \"$actualMessage\" didn't contain " +
                        "the expected substring \"$expectedMessageSubstring\"",
            )
        } finally {
            handler.close()
        }
    }

    private fun createAccessKey(header: String, payload: String, signature: String): String {
        var header: String = header
        var payload: String = payload
        var signature: String = signature
        val enc = Base64.getEncoder()
        header = enc.encodeToString(header.toByteArray())
        payload = enc.encodeToString(payload.toByteArray())
        signature = enc.encodeToString(signature.toByteArray())

        return "$header.$payload.$signature"
    }

    private fun createHandler(
        accessTokenRetriever: AccessTokenRetriever,
        configs: Map<String, *>,
    ): OAuthBearerLoginCallbackHandler {
        val handler = OAuthBearerLoginCallbackHandler()
        val accessTokenValidator = AccessTokenValidatorFactory.create(configs)
        handler.init(accessTokenRetriever, accessTokenValidator)
        return handler
    }
}
