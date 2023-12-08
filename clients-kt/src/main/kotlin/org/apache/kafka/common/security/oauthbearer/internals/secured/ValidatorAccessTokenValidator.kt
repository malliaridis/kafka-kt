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

import org.apache.kafka.common.security.oauthbearer.OAuthBearerToken
import org.jose4j.jwa.AlgorithmConstraints
import org.jose4j.jwt.MalformedClaimException
import org.jose4j.jwt.NumericDate
import org.jose4j.jwt.ReservedClaimNames
import org.jose4j.jwt.consumer.InvalidJwtException
import org.jose4j.jwt.consumer.JwtConsumer
import org.jose4j.jwt.consumer.JwtConsumerBuilder
import org.jose4j.jwt.consumer.JwtContext
import org.jose4j.keys.resolvers.VerificationKeyResolver
import org.slf4j.LoggerFactory

/**
 * ValidatorAccessTokenValidator is an implementation of [AccessTokenValidator] that is used by the
 * broker to perform more extensive validation of the JWT access token that is received from the
 * client, but ultimately from posting the client credentials to the OAuth/OIDC provider's token
 * endpoint.
 *
 * The validation steps performed (primary by the jose4j library) are:
 *
 * 1. Basic structural validation of the `b64token` value as defined in
 *    [RFC 6750 Section 2.1](https://tools.ietf.org/html/rfc6750#section-2.1)
 * 2. Basic conversion of the token into an in-memory data structure
 * 3. Presence of scope, `exp`, subject, `iss`, and `iat` claims
 * 4. Signature matching validation against the `kid` and those provided by the OAuth/OIDC
 * provider's JWKS
 *
 * @constructor Creates a new ValidatorAccessTokenValidator that will be used by the broker for more
 * thorough validation of the JWT.
 * @param clockSkew The optional value (in seconds) to allow for differences between the time of the
 * OAuth/OIDC identity provider and the broker. If `null` is provided, the broker and the OAUth/OIDC
 * identity provider are assumed to have very close clock settings.
 * @param expectedAudiences The (optional) collection the broker will use to verify that the JWT was
 * issued for one of the expected audiences. The JWT will be inspected for the standard OAuth `aud`
 * claim and if this value is set, the broker will match the value from JWT's `aud` claim to see if
 * there is an **exact** match. If there is no match, the broker will reject the JWT and
 * authentication will fail. May be `null` to not perform any check to verify the JWT's `aud` claim
 * matches any fixed set of known/expected audiences.
 * @param expectedIssuer The (optional) value for the broker to use to verify that the JWT was
 * created by the expected issuer. The JWT will be inspected for the standard OAuth `iss` claim and
 * if this value is set, the broker will match it **exactly** against what is in the JWT's `iss`
 * claim. If there is no match, the broker will reject the JWT and authentication will fail. May be
 * `null` to not perform any check to verify the JWT's `iss` claim matches a specific issuer.
 * @param verificationKeyResolver jose4j-based [VerificationKeyResolver] that is used to validate
 * the signature matches the contents of the header and payload
 * @property scopeClaimName Name of the scope claim to use
 * @property subClaimName Name of the subject claim to use
 *
 * @see JwtConsumerBuilder
 * @see JwtConsumer
 * @see VerificationKeyResolver
 */
class ValidatorAccessTokenValidator(
    clockSkew: Int?,
    expectedAudiences: Collection<String>?,
    expectedIssuer: String?,
    verificationKeyResolver: VerificationKeyResolver?,
    private val scopeClaimName: String,
    private val subClaimName: String
) : AccessTokenValidator {

    private val jwtConsumer: JwtConsumer

    init {
        val jwtConsumerBuilder = JwtConsumerBuilder()
        if (clockSkew != null) jwtConsumerBuilder.setAllowedClockSkewInSeconds(clockSkew)

        if (!expectedAudiences.isNullOrEmpty())
            jwtConsumerBuilder.setExpectedAudience(*expectedAudiences.toTypedArray())

        if (expectedIssuer != null) jwtConsumerBuilder.setExpectedIssuer(expectedIssuer)

        jwtConsumer = jwtConsumerBuilder
            .setJwsAlgorithmConstraints(AlgorithmConstraints.DISALLOW_NONE)
            .setRequireExpirationTime()
            .setRequireIssuedAt()
            .setVerificationKeyResolver(verificationKeyResolver)
            .build()
    }

    /**
     * Accepts an OAuth JWT access token in base-64 encoded format, validates, and returns an
     * OAuthBearerToken.
     *
     * @param accessToken Non-`null` JWT access token
     * @return [OAuthBearerToken]
     * @throws ValidateException Thrown on errors performing validation of given token
     */
    @Throws(ValidateException::class)
    override fun validate(accessToken: String): OAuthBearerToken {
        val serializedJwt = SerializedJwt(accessToken)

        val jwt: JwtContext = try {
            jwtConsumer.process(serializedJwt.token)
        } catch (e: InvalidJwtException) {
            throw ValidateException("Could not validate the access token: ${e.message}", e)
        }

        val claims = jwt.jwtClaims
        val scopeRaw = getClaim(
            supplier = { claims.getClaimValue(scopeClaimName) },
            claimName = scopeClaimName,
        )

        val scopeRawCollection: Collection<String> = when (scopeRaw) {
            is String -> listOf(scopeRaw)
            is Collection<*> -> scopeRaw as Collection<String>
            else -> emptySet()
        }

        val expirationRaw = getClaim<NumericDate?>(
            supplier = { claims.expirationTime },
            claimName = ReservedClaimNames.EXPIRATION_TIME,
        )

        val subRaw: String? = getClaim(
            supplier = { claims.getStringClaimValue(subClaimName) },
            claimName = subClaimName,
        )

        val issuedAtRaw = getClaim<NumericDate?>(
            supplier =  { claims.issuedAt },
            claimName = ReservedClaimNames.ISSUED_AT,
        )

        val scopes = ClaimValidationUtils.validateScopes(scopeClaimName, scopeRawCollection)
        val expiration = ClaimValidationUtils.validateExpiration(
            ReservedClaimNames.EXPIRATION_TIME,
            expirationRaw?.valueInMillis,
        )

        val sub = ClaimValidationUtils.validateSubject(subClaimName, subRaw)
        val issuedAt = ClaimValidationUtils.validateIssuedAt(
            ReservedClaimNames.ISSUED_AT,
            issuedAtRaw?.valueInMillis,
        )

        return BasicOAuthBearerToken(
            token = accessToken,
            scopes = scopes,
            lifetimeMs = expiration,
            principalName = sub,
            startTimeMs = issuedAt
        )
    }

    @Throws(ValidateException::class)
    private fun <T> getClaim(supplier: ClaimSupplier<T>, claimName: String): T {
        return try {
            val value = supplier.get()
            log.debug("getClaim - {}: {}", claimName, value)
            value
        } catch (e: MalformedClaimException) {
            throw ValidateException("Could not extract the '$claimName' claim from the access token", e)
        }
    }

    fun interface ClaimSupplier<T> {
        @Throws(MalformedClaimException::class)
        fun get(): T
    }

    companion object {
        private val log = LoggerFactory.getLogger(ValidatorAccessTokenValidator::class.java)
    }
}
