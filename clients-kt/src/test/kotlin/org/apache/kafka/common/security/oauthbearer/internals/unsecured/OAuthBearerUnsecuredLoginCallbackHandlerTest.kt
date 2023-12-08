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

package org.apache.kafka.common.security.oauthbearer.internals.unsecured

import java.io.IOException
import javax.security.auth.callback.UnsupportedCallbackException
import org.apache.kafka.common.security.auth.SaslExtensionsCallback
import org.apache.kafka.common.security.authenticator.TestJaasConfig
import org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule
import org.apache.kafka.common.security.oauthbearer.OAuthBearerTokenCallback
import org.apache.kafka.common.utils.MockTime
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotNull

class OAuthBearerUnsecuredLoginCallbackHandlerTest {

    @Test
    @Throws(IOException::class, UnsupportedCallbackException::class)
    fun addsExtensions() {
        val options: MutableMap<String, String> = HashMap()
        options["unsecuredLoginExtension_testId"] = "1"
        val callbackHandler = createCallbackHandler(options, MockTime())
        val callback = SaslExtensionsCallback()
        callbackHandler.handle(arrayOf(callback))
        assertEquals("1", callback.extensions.map()["testId"])
    }

    @Test
    fun throwsErrorOnInvalidExtensionName() {
        val options: MutableMap<String, String> = HashMap()
        options["unsecuredLoginExtension_test.Id"] = "1"
        val callbackHandler = createCallbackHandler(options, MockTime())
        val callback = SaslExtensionsCallback()
        assertFailsWith<IOException> { callbackHandler.handle(arrayOf(callback)) }
    }

    @Test
    fun throwsErrorOnInvalidExtensionValue() {
        val options: MutableMap<String, String> = HashMap()
        options["unsecuredLoginExtension_testId"] = "Çalifornia"
        val callbackHandler = createCallbackHandler(options, MockTime())
        val callback = SaslExtensionsCallback()
        assertFailsWith<IOException> { callbackHandler.handle(arrayOf(callback)) }
    }

    @Test
    @Throws(IOException::class, UnsupportedCallbackException::class)
    fun minimalToken() {
        val user = "user"
        val options = mapOf("unsecuredLoginStringClaim_sub" to user)
        val mockTime = MockTime()
        val callbackHandler = createCallbackHandler(options, mockTime)
        val callback = OAuthBearerTokenCallback()
        callbackHandler.handle(arrayOf(callback))
        val jws = callback.token as OAuthBearerUnsecuredJws?
        assertNotNull(jws, "create token failed")
        val startMs = mockTime.milliseconds()
        confirmCorrectValues(jws, user, startMs, (1000 * 60 * 60).toLong())
        assertEquals(HashSet(mutableListOf("sub", "iat", "exp")), jws.claims.keys)
    }

    @Test
    @Throws(IOException::class, UnsupportedCallbackException::class)
    fun validOptionsWithExplicitOptionValues() {
        val explicitScope1 = "scope1"
        val explicitScope2 = "scope2"
        val explicitScopeClaimName = "putScopeInHere"
        val principalClaimName = "principal"
        val scopeClaimNameOptionValues = arrayOf(null, explicitScopeClaimName)
        for (scopeClaimNameOptionValue in scopeClaimNameOptionValues) {
            val options: MutableMap<String, String> = HashMap()
            val user = "user"
            options["unsecuredLoginStringClaim_$principalClaimName"] = user
            options["unsecuredLoginListClaim_" + "list"] = ",1,2,"
            options["unsecuredLoginListClaim_" + "emptyList1"] = ""
            options["unsecuredLoginListClaim_" + "emptyList2"] = ","
            options["unsecuredLoginNumberClaim_" + "number"] = "1"
            val lifetmeSeconds: Long = 10000
            options["unsecuredLoginLifetimeSeconds"] = lifetmeSeconds.toString()
            options["unsecuredLoginPrincipalClaimName"] = principalClaimName
            if (scopeClaimNameOptionValue != null) options["unsecuredLoginScopeClaimName"] = scopeClaimNameOptionValue
            val actualScopeClaimName = if (scopeClaimNameOptionValue == null) "scope" else explicitScopeClaimName
            options["unsecuredLoginListClaim_$actualScopeClaimName"] = "|$explicitScope1|$explicitScope2"
            val mockTime = MockTime()
            val callbackHandler = createCallbackHandler(options, mockTime)
            val callback = OAuthBearerTokenCallback()
            callbackHandler.handle(arrayOf(callback))
            val jws = callback.token as OAuthBearerUnsecuredJws?
            assertNotNull(jws, "create token failed")
            val startMs = mockTime.milliseconds()
            confirmCorrectValues(jws, user, startMs, lifetmeSeconds * 1000)
            val claims = jws.claims
            assertEquals(
                setOf(
                    actualScopeClaimName,
                    principalClaimName,
                    "iat",
                    "exp",
                    "number",
                    "list",
                    "emptyList1",
                    "emptyList2",
                ),
                claims.keys
            )
            assertEquals(
                setOf(explicitScope1, explicitScope2),
                (claims[actualScopeClaimName] as? List<*>)?.toSet()
            )
            assertEquals(setOf(explicitScope1, explicitScope2), jws.scope())
            assertEquals(1.0, jws.claim("number", Number::class.java))
            assertEquals(listOf("1", "2", ""), jws.claim("list", List::class.java))
            assertEquals(emptyList<String>(), jws.claim("emptyList1", List::class.java))
            assertEquals(emptyList<String>(), jws.claim("emptyList2", List::class.java))
        }
    }

    companion object {

        private fun createCallbackHandler(
            options: Map<String, String>,
            mockTime: MockTime,
        ): OAuthBearerUnsecuredLoginCallbackHandler {
            val config = TestJaasConfig()
            config.createOrUpdateEntry(
                name = "KafkaClient",
                loginModule = "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule",
                options = options,
            )
            val callbackHandler = OAuthBearerUnsecuredLoginCallbackHandler()
            callbackHandler.time(mockTime)
            callbackHandler.configure(
                configs = emptyMap<String, Any>(),
                saslMechanism = OAuthBearerLoginModule.OAUTHBEARER_MECHANISM,
                jaasConfigEntries = listOf(config.getAppConfigurationEntry("KafkaClient")!![0]!!)
            )
            return callbackHandler
        }

        @Throws(OAuthBearerIllegalTokenException::class)
        private fun confirmCorrectValues(
            jws: OAuthBearerUnsecuredJws,
            user: String?,
            startMs: Long,
            lifetimeSeconds: Long,
        ) {
            val header = jws.header
            assertEquals(header.size, 1)
            assertEquals("none", header["alg"])
            assertEquals(user ?: "<unknown>", jws.principalName())
            assertEquals(startMs, jws.startTimeMs())
            assertEquals(startMs, Math.round(jws.issuedAt!!.toDouble() * 1000))
            assertEquals(startMs + lifetimeSeconds, jws.lifetimeMs())
            assertEquals(jws.lifetimeMs(), Math.round(jws.expirationTime!!.toDouble() * 1000))
        }
    }
}
