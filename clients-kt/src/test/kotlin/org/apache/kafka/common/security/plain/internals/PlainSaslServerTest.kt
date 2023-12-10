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

package org.apache.kafka.common.security.plain.internals

import org.apache.kafka.common.errors.SaslAuthenticationException
import org.apache.kafka.common.security.JaasContext
import org.apache.kafka.common.security.authenticator.TestJaasConfig
import org.apache.kafka.common.security.plain.PlainLoginModule
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class PlainSaslServerTest {
    
    private lateinit var saslServer: PlainSaslServer
    
    @BeforeEach
    fun setUp() {
        val jaasConfig = TestJaasConfig()
        val options = mapOf(
            "user_$USER_A" to PASSWORD_A,
            "user_$USER_B" to PASSWORD_B,
        )
        jaasConfig.addEntry(
            name = "jaasContext",
            loginModule = PlainLoginModule::class.java.getName(),
            options = options,
        )
        val jaasContext = JaasContext(
            name = "jaasContext",
            type = JaasContext.Type.SERVER,
            configuration = jaasConfig,
            dynamicJaasConfig = null,
        )
        val callbackHandler = PlainServerCallbackHandler()
        callbackHandler.configure(
            configs = emptyMap<String, Any?>(),
            saslMechanism = "PLAIN",
            jaasConfigEntries = jaasContext.configurationEntries,
        )
        saslServer = PlainSaslServer(callbackHandler)
    }

    @Test
    fun noAuthorizationIdSpecified() {
        val nextChallenge = saslServer.evaluateResponse(
            saslMessage(
                authorizationId = "",
                userName = USER_A,
                password = PASSWORD_A,
            )
        )
        assertEquals(0, nextChallenge.size)
    }

    @Test
    fun authorizationIdEqualsAuthenticationId() {
        val nextChallenge = saslServer.evaluateResponse(
            saslMessage(
                authorizationId = USER_A,
                userName = USER_A,
                password = PASSWORD_A,
            )
        )
        assertEquals(0, nextChallenge.size)
    }

    @Test
    fun authorizationIdNotEqualsAuthenticationId() {
        assertFailsWith<SaslAuthenticationException> {
            saslServer.evaluateResponse(
                saslMessage(
                    authorizationId = USER_B,
                    userName = USER_A,
                    password = PASSWORD_A,
                )
            )
        }
    }

    @Test
    fun emptyTokens() {
        var e = assertFailsWith<SaslAuthenticationException> {
            saslServer.evaluateResponse(
                saslMessage(authorizationId = "", userName = "", password = ""),
            )
        }
        assertEquals("Authentication failed: username not specified", e.message)
        e = assertFailsWith<SaslAuthenticationException> {
            saslServer.evaluateResponse(
                saslMessage(authorizationId = "", userName = "", password = "p"),
            )
        }
        assertEquals("Authentication failed: username not specified", e.message)
        e = assertFailsWith<SaslAuthenticationException> {
            saslServer.evaluateResponse(
                saslMessage(authorizationId = "", userName = "u", password = ""),
            )
        }
        assertEquals("Authentication failed: password not specified", e.message)
        e = assertFailsWith<SaslAuthenticationException> {
            saslServer.evaluateResponse(
                saslMessage(authorizationId = "a", userName = "", password = ""),
            )
        }
        assertEquals("Authentication failed: username not specified", e.message)
        e = assertFailsWith<SaslAuthenticationException> {
            saslServer.evaluateResponse(
                saslMessage(authorizationId = "a", userName = "", password = "p"),
            )
        }
        assertEquals("Authentication failed: username not specified", e.message)
        e = assertFailsWith<SaslAuthenticationException> {
            saslServer.evaluateResponse(
                saslMessage(authorizationId = "a", userName = "u", password = ""),
            )
        }
        assertEquals("Authentication failed: password not specified", e.message)
        val nul = "\u0000"
        e = assertFailsWith<SaslAuthenticationException> {
            saslServer.evaluateResponse(
                "a${nul}u${nul}p$nul".toByteArray(),
            )
        }
        assertEquals("Invalid SASL/PLAIN response: expected 3 tokens, got 4", e.message)
        e = assertFailsWith<SaslAuthenticationException> {
            saslServer.evaluateResponse(
                "${nul}u".toByteArray()
            )
        }
        assertEquals("Invalid SASL/PLAIN response: expected 3 tokens, got 2", e.message)
    }

    private fun saslMessage(authorizationId: String, userName: String, password: String): ByteArray {
        val nul = "\u0000"
        val message = "$authorizationId$nul$userName$nul$password"
        return message.toByteArray()
    }

    companion object {
        
        private const val USER_A = "userA"
        
        private const val PASSWORD_A = "passwordA"
        
        private const val USER_B = "userB"
        
        private const val PASSWORD_B = "passwordB"
    }
}
