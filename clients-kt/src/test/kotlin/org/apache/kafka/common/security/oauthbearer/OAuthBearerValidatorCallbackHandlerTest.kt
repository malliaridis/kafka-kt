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

import java.util.Base64
import org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_EXPECTED_AUDIENCE
import org.apache.kafka.common.security.oauthbearer.internals.secured.AccessTokenBuilder
import org.apache.kafka.common.security.oauthbearer.internals.secured.AccessTokenValidatorFactory.create
import org.apache.kafka.common.security.oauthbearer.internals.secured.CloseableVerificationKeyResolver
import org.apache.kafka.common.security.oauthbearer.internals.secured.OAuthBearerTest
import org.jose4j.jws.AlgorithmIdentifiers
import org.jose4j.jws.JsonWebSignature
import org.jose4j.jwx.JsonWebStructure
import org.junit.jupiter.api.Test
import kotlin.test.assertContains
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull

class OAuthBearerValidatorCallbackHandlerTest : OAuthBearerTest() {

    @Test
    @Throws(Exception::class)
    fun testBasic() {
        val expectedAudience = "a"
        val allAudiences = listOf(expectedAudience, "b", "c")
        val builder = AccessTokenBuilder().apply {
            audience = expectedAudience
            jwk = createRsaJwk()
            alg = AlgorithmIdentifiers.RSA_USING_SHA256
        }
        val accessToken = builder.build()
        val configs = getSaslConfigs(
            name = SASL_OAUTHBEARER_EXPECTED_AUDIENCE,
            value = allAudiences,
        )
        val handler = createHandler(configs, builder)
        try {
            val callback = OAuthBearerValidatorCallback(accessToken)
            handler.handle(arrayOf(callback))
            assertNotNull(callback.token)
            val token = callback.token
            assertEquals(accessToken, token!!.value())
            assertEquals(builder.subject, token.principalName())
            assertEquals(builder.expirationSeconds!! * 1000, token.lifetimeMs())
            assertEquals(builder.issuedAtSeconds * 1000, token.startTimeMs())
        } finally {
            handler.close()
        }
    }

    @Test
    @Throws(Exception::class)
    fun testInvalidAccessToken() {
        // There aren't different error messages for the validation step, so these are all the
        // same :(
        val substring = "invalid_token"
        assertInvalidAccessTokenFails(
            accessToken = "this isn't valid",
            expectedMessageSubstring = substring,
        )
        assertInvalidAccessTokenFails(
            accessToken = "this.isn't.valid",
            expectedMessageSubstring = substring,
        )
        assertInvalidAccessTokenFails(
            accessToken = createAccessKey(header = "this", payload = "isn't", signature = "valid"),
            expectedMessageSubstring = substring,
        )
        assertInvalidAccessTokenFails(
            accessToken = createAccessKey(header = "{}", payload = "{}", signature = "{}"),
            expectedMessageSubstring = substring,
        )
    }

    @Throws(Exception::class)
    private fun assertInvalidAccessTokenFails(accessToken: String, expectedMessageSubstring: String) {
        val configs = saslConfigs
        val handler = createHandler(configs, AccessTokenBuilder())
        try {
            val callback = OAuthBearerValidatorCallback(accessToken)
            handler.handle(arrayOf(callback))
            assertNull(callback.token)
            val actualMessage = callback.errorStatus
            assertNotNull(actualMessage)
            assertContains(
                charSequence = actualMessage,
                other = expectedMessageSubstring,
                message = "The error message \"$actualMessage\" didn't contain the expected " +
                        "substring \"$expectedMessageSubstring\"",
            )
        } finally {
            handler.close()
        }
    }

    private fun createHandler(
        options: Map<String, *>,
        builder: AccessTokenBuilder,
    ): OAuthBearerValidatorCallbackHandler {
        val handler = OAuthBearerValidatorCallbackHandler()
        val verificationKeyResolver = CloseableVerificationKeyResolver { _, _ -> builder.jwk!!.publicKey }
        val accessTokenValidator = create(
            configs = options,
            verificationKeyResolver = verificationKeyResolver,
        )
        handler.init(verificationKeyResolver, accessTokenValidator)
        return handler
    }

    private fun createAccessKey(header: String, payload: String, signature: String): String {
        var header = header
        var payload = payload
        var signature = signature
        val enc = Base64.getEncoder()
        header = enc.encodeToString(header.toByteArray())
        payload = enc.encodeToString(payload.toByteArray())
        signature = enc.encodeToString(signature.toByteArray())

        return "$header.$payload.$signature"
    }
}
