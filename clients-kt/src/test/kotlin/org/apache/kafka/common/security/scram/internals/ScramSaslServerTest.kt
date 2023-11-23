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

package org.apache.kafka.common.security.scram.internals

import org.apache.kafka.common.errors.SaslAuthenticationException
import org.apache.kafka.common.security.authenticator.CredentialCache
import org.apache.kafka.common.security.scram.ScramCredential
import org.apache.kafka.common.security.scram.internals.ScramMechanism.Companion.mechanismNames
import org.apache.kafka.common.security.token.delegation.internals.DelegationTokenCache
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class ScramSaslServerTest {
    
    private lateinit var mechanism: ScramMechanism
    
    private lateinit var formatter: ScramFormatter
    
    private lateinit var saslServer: ScramSaslServer
    
    @BeforeEach
    @Throws(Exception::class)
    fun setUp() {
        mechanism = ScramMechanism.SCRAM_SHA_256
        formatter = ScramFormatter(mechanism)
        val credentialCache = CredentialCache().createCache(
            mechanism = mechanism.mechanismName,
            credentialClass = ScramCredential::class.java,
        )
        credentialCache.put(
            username = USER_A,
            credential = formatter.generateCredential(password = "passwordA", iterations = 4096),
        )
        credentialCache.put(
            username = USER_B,
            credential = formatter.generateCredential(password = "passwordB", iterations = 4096),
        )
        val callbackHandler = ScramServerCallbackHandler(
            credentialCache = credentialCache,
            tokenCache = DelegationTokenCache(mechanismNames)
        )
        saslServer = ScramSaslServer(
            mechanism = mechanism,
            props = emptyMap<String, Any?>(),
            callbackHandler = callbackHandler
        )
    }

    @Test
    @Throws(Exception::class)
    fun noAuthorizationIdSpecified() {
        val nextChallenge = saslServer.evaluateResponse(
            clientFirstMessage(userName = USER_A, authorizationId = null)
        )
        assertTrue(nextChallenge.isNotEmpty(), "Next challenge is empty")
    }

    @Test
    @Throws(Exception::class)
    fun authorizatonIdEqualsAuthenticationId() {
        val nextChallenge = saslServer.evaluateResponse(
            clientFirstMessage(userName = USER_A, authorizationId = USER_A)
        )
        assertTrue(nextChallenge.isNotEmpty(), "Next challenge is empty")
    }

    @Test
    fun authorizatonIdNotEqualsAuthenticationId() {
        assertFailsWith<SaslAuthenticationException> {
            saslServer.evaluateResponse(
                clientFirstMessage(userName = USER_A, authorizationId = USER_B)
            )
        }
    }

    private fun clientFirstMessage(userName: String, authorizationId: String?): ByteArray {
        val nonce = formatter.secureRandomString()
        val authorizationField = if (authorizationId != null) "a=$authorizationId" else ""
        val firstMessage = "n,$authorizationField,n=$userName,r=$nonce"

        return firstMessage.toByteArray()
    }

    companion object {

        private const val USER_A = "userA"

        private const val USER_B = "userB"
    }
}
