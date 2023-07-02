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

import org.apache.kafka.common.config.SaslConfigs
import org.apache.kafka.common.security.oauthbearer.OAuthBearerToken
import org.apache.kafka.common.security.oauthbearer.internals.unsecured.OAuthBearerIllegalTokenException
import org.apache.kafka.common.security.oauthbearer.internals.unsecured.OAuthBearerUnsecuredJws
import org.slf4j.LoggerFactory

/**
 * LoginAccessTokenValidator is an implementation of [AccessTokenValidator] that is used by the
 * client to perform some rudimentary validation of the JWT access token that is received as part of
 * the response from posting the client credentials to the OAuth/OIDC provider's token endpoint.
 *
 * The validation steps performed are:
 * 1. Basic structural validation of the `b64token` value as defined in
 *    [RFC 6750 Section 2.1](https://tools.ietf.org/html/rfc6750#section-2.1)
 * 2. Basic conversion of the token into an in-memory map
 * 3. Presence of scope, `exp`, subject, and `iat` claims
 *
 * @constructor Creates a new LoginAccessTokenValidator that will be used by the client for
 * lightweight validation of the JWT.
 * @param scopeClaimName Name of the scope claim to use; must be not blank
 * @param subClaimName Name of the subject claim to use; must be not blank
 */
class LoginAccessTokenValidator(
    scopeClaimName: String,
    subClaimName: String,
) : AccessTokenValidator {

    private val scopeClaimName: String

    private val subClaimName: String

    init {
        this.scopeClaimName = ClaimValidationUtils.validateClaimNameOverride(
            SaslConfigs.DEFAULT_SASL_OAUTHBEARER_SCOPE_CLAIM_NAME,
            scopeClaimName,
        )
        this.subClaimName = ClaimValidationUtils.validateClaimNameOverride(
            SaslConfigs.DEFAULT_SASL_OAUTHBEARER_SUB_CLAIM_NAME,
            subClaimName,
        )
    }

    /**
     * Accepts an OAuth JWT access token in base-64 encoded format, validates, and returns an
     * OAuthBearerToken.
     *
     * @param accessToken JWT access token
     * @return [OAuthBearerToken]
     * @throws ValidateException Thrown on errors performing validation of given token
     */
    @Throws(ValidateException::class)
    override fun validate(accessToken: String): OAuthBearerToken {
        val serializedJwt = SerializedJwt(accessToken)
        val payload: Map<String, Any> = try {
            OAuthBearerUnsecuredJws.toMap(serializedJwt.payload)
        } catch (exception: OAuthBearerIllegalTokenException) {
            throw ValidateException(
                String.format("Could not validate the access token: %s", exception.message),
                exception,
            )
        }

        val scopeRawCollection = when (val scopeRaw = getClaim(payload, scopeClaimName)) {
            is String -> listOf(scopeRaw)
            is Collection<*> -> scopeRaw as Collection<String>
            else -> emptySet()
        }

        val expirationRaw = getClaim(payload, EXPIRATION_CLAIM_NAME) as Number?
        val subRaw = getClaim(payload, subClaimName) as String?
        val issuedAtRaw = getClaim(payload, ISSUED_AT_CLAIM_NAME) as Number?

        val scopes = ClaimValidationUtils.validateScopes(scopeClaimName, scopeRawCollection)
        val expiration = ClaimValidationUtils.validateExpiration(
            EXPIRATION_CLAIM_NAME,
            expirationRaw?.toLong()?.times(1000L)
        )
        val subject = ClaimValidationUtils.validateSubject(subClaimName, subRaw)
        val issuedAt = ClaimValidationUtils.validateIssuedAt(
            ISSUED_AT_CLAIM_NAME,
            if (issuedAtRaw != null) issuedAtRaw.toLong() * 1000L
            else null
        )

        return BasicOAuthBearerToken(
            accessToken,
            scopes,
            expiration,
            subject,
            issuedAt
        )
    }

    private fun getClaim(payload: Map<String, Any>, claimName: String): Any? {
        val value = payload[claimName]
        log.debug("getClaim - {}: {}", claimName, value)
        return value
    }

    companion object {

        private val log = LoggerFactory.getLogger(LoginAccessTokenValidator::class.java)

        const val EXPIRATION_CLAIM_NAME = "exp"

        const val ISSUED_AT_CLAIM_NAME = "iat"
    }
}
