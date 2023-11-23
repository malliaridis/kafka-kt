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

import javax.security.auth.callback.Callback
import javax.security.auth.callback.UnsupportedCallbackException
import javax.security.auth.login.AppConfigurationEntry
import javax.security.sasl.SaslException
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.common.security.auth.AuthenticateCallbackHandler
import org.apache.kafka.common.security.auth.SaslExtensions
import org.apache.kafka.common.security.auth.SaslExtensionsCallback
import org.apache.kafka.common.security.oauthbearer.OAuthBearerToken
import org.apache.kafka.common.security.oauthbearer.OAuthBearerTokenCallback
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertIs

class OAuthBearerSaslClientTest {

    private var testExtensions = SaslExtensions(TEST_PROPERTIES)

    private val errorMessage = "Error as expected!"

    inner class ExtensionsCallbackHandler(private val toThrow: Boolean) : AuthenticateCallbackHandler {

        private var configured = false

        fun configured(): Boolean = configured

        override fun configure(
            configs: Map<String, *>,
            saslMechanism: String,
            jaasConfigEntries: List<AppConfigurationEntry>,
        ) {
            configured = true
        }

        @Throws(UnsupportedCallbackException::class)
        override fun handle(callbacks: Array<Callback>) {
            for (callback in callbacks) {
                if (callback is OAuthBearerTokenCallback) callback.token = object : OAuthBearerToken {
                    override fun value(): String = ""
                    override fun scope(): Set<String> = emptySet()
                    override fun lifetimeMs(): Long = 100
                    override fun principalName(): String = "principalName"
                    override fun startTimeMs(): Long? = null
                } else if (callback is SaslExtensionsCallback) {
                    if (toThrow) throw ConfigException(errorMessage)
                    else callback.extensions = testExtensions
                } else throw UnsupportedCallbackException(callback)
            }
        }

        override fun close() {}
    }

    @Test
    @Throws(Exception::class)
    fun testAttachesExtensionsToFirstClientMessage() {
        val expectedToken = String(OAuthBearerClientInitialResponse(
            tokenValue = "",
            extensions = testExtensions,
        ).toBytes())
        val client = OAuthBearerSaslClient(ExtensionsCallbackHandler(toThrow = false))
        val message = String(client.evaluateChallenge("".toByteArray())!!)
        assertEquals(expectedToken, message)
    }

    @Test
    @Throws(Exception::class)
    fun testNoExtensionsDoesNotAttachAnythingToFirstClientMessage() {
        TEST_PROPERTIES.clear()
        testExtensions = SaslExtensions(TEST_PROPERTIES)
        val expectedToken = String(
            OAuthBearerClientInitialResponse(
                tokenValue = "",
                extensions = SaslExtensions(TEST_PROPERTIES),
            ).toBytes(),
        )
        val client = OAuthBearerSaslClient(ExtensionsCallbackHandler(toThrow = false))
        val message = String(client.evaluateChallenge("".toByteArray())!!)
        assertEquals(expectedToken, message)
    }

    @Test
    fun testWrapsExtensionsCallbackHandlingErrorInSaslExceptionInFirstClientMessage() {
        val client = OAuthBearerSaslClient(ExtensionsCallbackHandler(toThrow = true))
        val error = assertFailsWith<SaslException>(
            message = "Should have failed with " + SaslException::class.java.getName()
        ) { client.evaluateChallenge("".toByteArray()) }

        // assert it has caught our expected exception
        assertIs<ConfigException>(error.cause)
        assertEquals(errorMessage, error.cause!!.message)
    }

    companion object {
        private val TEST_PROPERTIES = mutableMapOf(
            "One" to "1",
            "Two" to "2",
            "Three" to "3",
        )
    }
}
