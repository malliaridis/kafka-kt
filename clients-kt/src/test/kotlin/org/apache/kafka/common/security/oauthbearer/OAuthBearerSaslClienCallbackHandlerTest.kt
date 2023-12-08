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
import java.security.AccessController
import java.security.PrivilegedActionException
import java.security.PrivilegedExceptionAction
import javax.security.auth.Subject
import org.apache.kafka.common.security.oauthbearer.internals.OAuthBearerSaslClientCallbackHandler
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertIs

class OAuthBearerSaslClienCallbackHandlerTest {

    @Test
    fun testWithZeroTokens() {
        val handler = createCallbackHandler()
        val e = assertFailsWith<PrivilegedActionException> {
            Subject.doAs(
                Subject(),
                PrivilegedExceptionAction {
                    val callback = OAuthBearerTokenCallback()
                    handler.handle(arrayOf(callback))
                }
            )
        }
        assertIs<IOException>(e.cause!!)
    }

    @Test
    @Throws(Exception::class)
    fun testWithPotentiallyMultipleTokens() {
        val handler = createCallbackHandler()
        Subject.doAs(
            Subject(),
            PrivilegedExceptionAction {
                val maxTokens = 4
                val privateCredentials =
                    Subject.getSubject(AccessController.getContext()).privateCredentials
                privateCredentials.clear()

                for (num in 1..maxTokens) {
                    privateCredentials.add(createTokenWithLifetimeMillis(num.toLong()))
                    val callback = OAuthBearerTokenCallback()
                    handler.handle(arrayOf(callback))
                    assertEquals(num.toLong(), callback.token!!.lifetimeMs())
                }
            }
        )
    }

    companion object {

        private fun createTokenWithLifetimeMillis(lifetimeMillis: Long): OAuthBearerToken {
            return object : OAuthBearerToken {
                override fun value(): String? = null
                override fun startTimeMs(): Long? = null
                override fun scope(): Set<String> = emptySet()
                override fun principalName(): String = ""
                override fun lifetimeMs(): Long = lifetimeMillis
            }
        }

        private fun createCallbackHandler(): OAuthBearerSaslClientCallbackHandler {
            val handler = OAuthBearerSaslClientCallbackHandler()
            handler.configure(
                configs = emptyMap<String, Any>(),
                saslMechanism = OAuthBearerLoginModule.OAUTHBEARER_MECHANISM,
                jaasConfigEntries = emptyList(),
            )
            return handler
        }
    }
}
