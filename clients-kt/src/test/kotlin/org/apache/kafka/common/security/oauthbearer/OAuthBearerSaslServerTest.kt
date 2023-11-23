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
import javax.security.auth.callback.Callback
import javax.security.auth.callback.UnsupportedCallbackException
import org.apache.kafka.common.config.SaslConfigs
import org.apache.kafka.common.config.types.Password
import org.apache.kafka.common.errors.SaslAuthenticationException
import org.apache.kafka.common.security.JaasContext
import org.apache.kafka.common.security.auth.AuthenticateCallbackHandler
import org.apache.kafka.common.security.auth.SaslExtensions
import org.apache.kafka.common.security.authenticator.SaslInternalConfigs
import org.apache.kafka.common.security.oauthbearer.internals.OAuthBearerClientInitialResponse
import org.apache.kafka.common.security.oauthbearer.internals.OAuthBearerSaslServer
import org.apache.kafka.common.security.oauthbearer.internals.unsecured.OAuthBearerConfigException
import org.apache.kafka.common.security.oauthbearer.internals.unsecured.OAuthBearerUnsecuredLoginCallbackHandler
import org.apache.kafka.common.security.oauthbearer.internals.unsecured.OAuthBearerUnsecuredValidatorCallbackHandler
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

class OAuthBearerSaslServerTest {

    private lateinit var saslServer: OAuthBearerSaslServer

    @BeforeEach
    fun setUp() {
        saslServer = OAuthBearerSaslServer(VALIDATOR_CALLBACK_HANDLER)
    }

    @Test
    @Throws(Exception::class)
    fun noAuthorizationIdSpecified() {
        val nextChallenge = saslServer.evaluateResponse(clientInitialResponse(authorizationId = null))
        // also asserts that no authentication error is thrown if OAuthBearerExtensionsValidatorCallback is
        // not supported
        assertTrue(nextChallenge.isEmpty(), "Next challenge is not empty")
    }

    @Test
    @Throws(Exception::class)
    fun negotiatedProperty() {
        saslServer.evaluateResponse(clientInitialResponse(USER))
        val token = saslServer.getNegotiatedProperty("OAUTHBEARER.token") as OAuthBearerToken
        assertNotNull(token)
        assertEquals(
            token.lifetimeMs(),
            saslServer.getNegotiatedProperty(SaslInternalConfigs.CREDENTIAL_LIFETIME_MS_SASL_NEGOTIATED_PROPERTY_KEY),
        )
    }

    /**
     * SASL Extensions that are validated by the callback handler should be accessible through the
     * `#getNegotiatedProperty()` method
     */
    @Test
    @Throws(Exception::class)
    fun savesCustomExtensionAsNegotiatedProperty() {
        val customExtensions: MutableMap<String, String> = HashMap()
        customExtensions["firstKey"] = "value1"
        customExtensions["secondKey"] = "value2"
        val nextChallenge = saslServer.evaluateResponse(
            clientInitialResponse(
                authorizationId = null,
                illegalToken = false,
                customExtensions = customExtensions,
            )
        )
        assertTrue(nextChallenge.isEmpty(), "Next challenge is not empty")
        assertEquals("value1", saslServer.getNegotiatedProperty("firstKey"))
        assertEquals("value2", saslServer.getNegotiatedProperty("secondKey"))
    }

    /**
     * SASL Extensions that were not recognized (neither validated nor invalidated)
     * by the callback handler must not be accessible through the `#getNegotiatedProperty()` method
     */
    @Test
    @Throws(Exception::class)
    fun unrecognizedExtensionsAreNotSaved() {
        saslServer = OAuthBearerSaslServer(EXTENSIONS_VALIDATOR_CALLBACK_HANDLER)
        val customExtensions: MutableMap<String, String> = HashMap()
        customExtensions["firstKey"] = "value1"
        customExtensions["secondKey"] = "value1"
        customExtensions["thirdKey"] = "value1"
        val nextChallenge = saslServer.evaluateResponse(
            clientInitialResponse(
                authorizationId = null,
                illegalToken = false,
                customExtensions = customExtensions,
            )
        )
        assertTrue(nextChallenge.isEmpty(), "Next challenge is not empty")
        assertNull(
            actual = saslServer.getNegotiatedProperty("thirdKey"),
            message = "Extensions not recognized by the server must be ignored",
        )
    }

    /**
     * If the callback handler handles the `OAuthBearerExtensionsValidatorCallback`
     * and finds an invalid extension, SaslServer should throw an authentication exception
     */
    @Test
    fun throwsAuthenticationExceptionOnInvalidExtensions() {
        val invalidHandler: OAuthBearerUnsecuredValidatorCallbackHandler =
            object : OAuthBearerUnsecuredValidatorCallbackHandler() {

                @Throws(UnsupportedCallbackException::class)
                override fun handle(callbacks: Array<Callback>) {
                    for (callback in callbacks) {
                        when (callback) {
                            is OAuthBearerValidatorCallback -> callback.token = OAuthBearerTokenMock()
                            is OAuthBearerExtensionsValidatorCallback -> {
                                callback.error("firstKey", "is not valid")
                                callback.error("secondKey", "is not valid either")
                            }

                            else -> throw UnsupportedCallbackException(callback)
                        }
                    }
                }
            }
        saslServer = OAuthBearerSaslServer(invalidHandler)
        val customExtensions = mapOf("firstKey" to "value", "secondKey" to "value")
        assertFailsWith<SaslAuthenticationException> {
            saslServer.evaluateResponse(
                clientInitialResponse(
                    authorizationId = null,
                    illegalToken = false,
                    customExtensions = customExtensions,
                )
            )
        }
    }

    @Test
    @Throws(Exception::class)
    fun authorizatonIdEqualsAuthenticationId() {
        val nextChallenge = saslServer.evaluateResponse(clientInitialResponse(USER))
        assertTrue(nextChallenge.isEmpty(), "Next challenge is not empty")
    }

    @Test
    fun authorizatonIdNotEqualsAuthenticationId() {
        assertFailsWith<SaslAuthenticationException> {
            saslServer.evaluateResponse(clientInitialResponse(authorizationId = USER + "x"))
        }
    }

    @Test
    @Throws(Exception::class)
    fun illegalToken() {
        val bytes = saslServer.evaluateResponse(
            clientInitialResponse(
                authorizationId = null,
                illegalToken = true,
                customExtensions = emptyMap(),
            )
        )
        val challenge = String(bytes)
        assertEquals("{\"status\":\"invalid_token\"}", challenge)
    }

    @Throws(OAuthBearerConfigException::class, IOException::class, UnsupportedCallbackException::class)
    private fun clientInitialResponse(authorizationId: String?, illegalToken: Boolean = false): ByteArray {
        return clientInitialResponse(
            authorizationId = authorizationId,
            illegalToken = false,
            customExtensions = emptyMap(),
        )
    }

    @Throws(OAuthBearerConfigException::class, IOException::class, UnsupportedCallbackException::class)
    private fun clientInitialResponse(
        authorizationId: String?,
        illegalToken: Boolean,
        customExtensions: Map<String, String>,
    ): ByteArray {
        val callback = OAuthBearerTokenCallback()
        LOGIN_CALLBACK_HANDLER.handle(arrayOf(callback))
        val token = callback.token
        val compactSerialization = token!!.value()
        val tokenValue = compactSerialization + if (illegalToken) "AB" else ""
        return OAuthBearerClientInitialResponse(
            tokenValue = tokenValue,
            authorizationId = authorizationId,
            extensions = SaslExtensions(customExtensions),
        ).toBytes()
    }

    companion object {

        private const val USER = "user"

        private val CONFIGS: Map<String, *>

        private val LOGIN_CALLBACK_HANDLER = OAuthBearerUnsecuredLoginCallbackHandler()

        private val VALIDATOR_CALLBACK_HANDLER = OAuthBearerUnsecuredValidatorCallbackHandler()

        private val EXTENSIONS_VALIDATOR_CALLBACK_HANDLER: AuthenticateCallbackHandler

        init {
            val jaasConfigText = ("org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule Required"
                    + " unsecuredLoginStringClaim_sub=\"" + USER + "\";")
            val tmp = mapOf(SaslConfigs.SASL_JAAS_CONFIG to Password(jaasConfigText))
            CONFIGS = tmp

            LOGIN_CALLBACK_HANDLER.configure(
                configs = CONFIGS,
                saslMechanism = OAuthBearerLoginModule.OAUTHBEARER_MECHANISM,
                jaasConfigEntries = JaasContext.loadClientContext(CONFIGS).configurationEntries,
            )

            VALIDATOR_CALLBACK_HANDLER.configure(
                configs = CONFIGS,
                saslMechanism = OAuthBearerLoginModule.OAUTHBEARER_MECHANISM,
                jaasConfigEntries = JaasContext.loadClientContext(CONFIGS).configurationEntries,
            )

            // only validate extensions "firstKey" and "secondKey"
            EXTENSIONS_VALIDATOR_CALLBACK_HANDLER = object : OAuthBearerUnsecuredValidatorCallbackHandler() {

                @Throws(UnsupportedCallbackException::class)
                override fun handle(callbacks: Array<Callback>) {
                    for (callback in callbacks) {
                        when (callback) {
                            is OAuthBearerValidatorCallback -> callback.token = OAuthBearerTokenMock()
                            is OAuthBearerExtensionsValidatorCallback -> {
                                callback.valid("firstKey")
                                callback.valid("secondKey")
                            }

                            else -> throw UnsupportedCallbackException(callback)
                        }
                    }
                }
            }
        }
    }
}
