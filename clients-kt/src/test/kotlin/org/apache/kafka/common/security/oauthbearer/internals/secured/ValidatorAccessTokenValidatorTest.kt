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

import org.jose4j.jwk.PublicJsonWebKey
import org.jose4j.jws.AlgorithmIdentifiers
import org.jose4j.lang.InvalidAlgorithmException
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

class ValidatorAccessTokenValidatorTest : AccessTokenValidatorTest() {
    
    override fun createAccessTokenValidator(builder: AccessTokenBuilder): AccessTokenValidator {
        return ValidatorAccessTokenValidator(
            clockSkew = 30, expectedAudiences = emptySet(),
            expectedIssuer = null,
            verificationKeyResolver = { _, _ -> builder.jwk!!.key },
            scopeClaimName = builder.scopeClaimName,
            subClaimName = builder.subjectClaimName
        )
    }

    @Test
    @Throws(Exception::class)
    fun testRsaEncryptionAlgorithm() {
        val jwk = createRsaJwk()
        testEncryptionAlgorithm(jwk = jwk, alg = AlgorithmIdentifiers.RSA_USING_SHA256)
    }

    @Test
    @Throws(Exception::class)
    fun testEcdsaEncryptionAlgorithm() {
        val jwk = createEcJwk()
        testEncryptionAlgorithm(jwk = jwk, alg = AlgorithmIdentifiers.ECDSA_USING_P256_CURVE_AND_SHA256)
    }

    @Test
    @Throws(Exception::class)
    fun testInvalidEncryptionAlgorithm() {
        val jwk = createRsaJwk()
        assertThrowsWithMessage(
            clazz = InvalidAlgorithmException::class.java,
            executable = { testEncryptionAlgorithm(jwk, "fake") },
            substring = "fake is an unknown, unsupported or unavailable alg algorithm",
        )
    }

    @Test
    @Throws(Exception::class)
    fun testMissingSubShouldBeValid() {
        val subClaimName = "client_id"
        val subject = "otherSub"
        val jwk = createRsaJwk()
        val tokenBuilder = AccessTokenBuilder().apply {
            this.jwk = jwk
            this.alg = AlgorithmIdentifiers.RSA_USING_SHA256
            addCustomClaim(subClaimName, subject)
            this.subject = null
        }
        val validator = createAccessTokenValidator(tokenBuilder)

        // Validation should succeed (e.g. signature verification) even if sub-claim is missing
        val token = validator.validate(tokenBuilder.build())
        assertEquals(subject, token.principalName())
    }

    @Throws(Exception::class)
    private fun testEncryptionAlgorithm(jwk: PublicJsonWebKey, alg: String) {
        val builder = AccessTokenBuilder().apply {
            this.jwk = jwk
            this.alg = alg
        }
        val validator = createAccessTokenValidator(builder)
        val accessToken = builder.build()
        val token = validator.validate(accessToken)
        assertEquals(builder.subject, token.principalName())
        assertEquals(builder.issuedAtSeconds * 1000, token.startTimeMs())
        assertEquals(builder.expirationSeconds!! * 1000, token.lifetimeMs())
        assertEquals(1, token.scope().size)
    }
}
