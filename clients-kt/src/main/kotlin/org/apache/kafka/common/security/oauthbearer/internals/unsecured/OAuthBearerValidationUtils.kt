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

import org.apache.kafka.common.security.oauthbearer.OAuthBearerToken

object OAuthBearerValidationUtils {

    /**
     * Validate the given claim for existence and type. It can be required to exist in the given
     * claims, and if it exists it must be one of the types indicated
     *
     * @param jwt The mandatory JWT to which the validation will be applied
     * @param required `true` if the claim is required to exist
     * @param claimName The required claim name identifying the claim to be checked
     * @param allowedTypes One or more of `String.class`, `Number.class`, and `List.class`
     * identifying the type(s) that the claim value is allowed to be if it exists
     * @return The result of the validation
     */
    fun validateClaimForExistenceAndType(
        jwt: OAuthBearerUnsecuredJws,
        required: Boolean,
        claimName: String,
        vararg allowedTypes: Class<*>,
    ): OAuthBearerValidationResult {
        val rawClaim = jwt.rawClaim(claimName)
            ?: return if (required) OAuthBearerValidationResult.newFailure(
                "Required claim missing: $claimName"
            ) else OAuthBearerValidationResult.newSuccess()

        allowedTypes.forEach { allowedType ->
            if (allowedType.isAssignableFrom(rawClaim.javaClass))
                return OAuthBearerValidationResult.newSuccess()
        }

        return OAuthBearerValidationResult.newFailure(
            "The $claimName claim had the incorrect type: ${rawClaim.javaClass.simpleName}"
        )
    }

    /**
     * Validate the 'iat' (Issued At) claim. It can be required to exist in the given claims, and if
     * it exists it must be a (potentially fractional) number of seconds since the epoch defining
     * when the JWT was issued; it is a validation error if the Issued At time is after the time at
     * which the check is being done (plus any allowable clock skew).
     *
     * @param jwt The mandatory JWT to which the validation will be applied
     * @param required `true` if the claim is required to exist
     * @param whenCheckTimeMs The time relative to which the validation is to occur
     * @param allowableClockSkewMs Non-negative number to take into account some potential clock
     * skew
     * @return The result of the validation
     * @throws OAuthBearerConfigException If the given allowable clock skew is negative
     */
    @Throws(OAuthBearerConfigException::class)
    fun validateIssuedAt(
        jwt: OAuthBearerUnsecuredJws,
        required: Boolean,
        whenCheckTimeMs: Long,
        allowableClockSkewMs: Int,
    ): OAuthBearerValidationResult {
        val value: Number = try {
            jwt.issuedAt
        } catch (e: OAuthBearerIllegalTokenException) {
            return e.reason
        } ?: return doesNotExistResult(required, "iat")

        val doubleValue = value.toDouble()
        return if (1000 * doubleValue > whenCheckTimeMs + confirmNonNegative(allowableClockSkewMs))
            OAuthBearerValidationResult.newFailure(
                String.format(
                    "The Issued At value (%f seconds) was after the indicated time (%d ms) plus " +
                            "allowable clock skew (%d ms)",
                    doubleValue,
                    whenCheckTimeMs,
                    allowableClockSkewMs
                )
            )
        else OAuthBearerValidationResult.newSuccess()
    }

    /**
     * Validate the 'exp' (Expiration Time) claim. It must exist and it must be a (potentially
     * fractional) number of seconds defining the point at which the JWT expires. It is a validation
     * error if the time at which the check is being done (minus any allowable clock skew) is on or
     * after the Expiration Time time.
     *
     * @param jwt The mandatory JWT to which the validation will be applied
     * @param whenCheckTimeMs The time relative to which the validation is to occur
     * @param allowableClockSkewMs Non-negative number to take into account some potential clock
     * skew
     * @return The result of the validation
     * @throws OAuthBearerConfigException -if the given allowable clock skew is negative
     */
    @Throws(OAuthBearerConfigException::class)
    fun validateExpirationTime(
        jwt: OAuthBearerUnsecuredJws,
        whenCheckTimeMs: Long,
        allowableClockSkewMs: Int
    ): OAuthBearerValidationResult {
        val value: Number = try {
            jwt.expirationTime
        } catch (e: OAuthBearerIllegalTokenException) {
            return e.reason
        } ?: return doesNotExistResult(true, "exp")

        val doubleValue = value.toDouble()
        return if (whenCheckTimeMs - confirmNonNegative(allowableClockSkewMs) >= 1000 * doubleValue)
            OAuthBearerValidationResult.newFailure(
                String.format(
                    "The indicated time (%d ms) minus allowable clock skew (%d ms) was on or " +
                            "after the Expiration Time value (%f seconds)",
                    whenCheckTimeMs, allowableClockSkewMs, doubleValue
                )
            )
        else OAuthBearerValidationResult.newSuccess()
    }

    /**
     * Validate the 'iat' (Issued At) and 'exp' (Expiration Time) claims for internal consistency.
     * The following must be true if both claims exist:
     *
     * ```plaintext
     * exp > iat
     * ```
     *
     * @param jwt The mandatory JWT to which the validation will be applied
     * @return The result of the validation
     */
    fun validateTimeConsistency(jwt: OAuthBearerUnsecuredJws): OAuthBearerValidationResult {
        val issuedAt: Number?
        val expirationTime: Number?
        try {
            issuedAt = jwt.issuedAt
            expirationTime = jwt.expirationTime
        } catch (e: OAuthBearerIllegalTokenException) {
            return e.reason
        }
        return if (
            expirationTime != null
            && issuedAt != null
            && expirationTime.toDouble() <= issuedAt.toDouble()
        ) OAuthBearerValidationResult.newFailure(
            String.format(
                "The Expiration Time time (%f seconds) was not after the Issued At time " +
                        "(%f seconds)",
                expirationTime.toDouble(),
                issuedAt.toDouble()
            )
        ) else OAuthBearerValidationResult.newSuccess()
    }

    /**
     * Validate the given token's scope against the required scope. Every required scope element
     * (if any) must exist in the provided token's scope for the validation to succeed.
     *
     * @param token The required token for which the scope will to validate
     * @param requiredScope The optional required scope against which the given token's scope will
     * be validated
     * @return The result of the validation
     */
    fun validateScope(
        token: OAuthBearerToken,
        requiredScope: List<String>?,
    ): OAuthBearerValidationResult {
        val tokenScope: Set<String> = token.scope()
        if (requiredScope.isNullOrEmpty()) return OAuthBearerValidationResult.newSuccess()

        val missingScope = requiredScope.firstOrNull { element -> !tokenScope.contains(element) }

        return missingScope?.let { missingScope ->
            OAuthBearerValidationResult.newFailure(
                failureDescription = String.format(
                    "The provided scope (%s) was missing a required scope (%s). All required " +
                            "scope elements: %s",
                    tokenScope.toString(),
                    missingScope,
                    requiredScope.toString()
                ),
                failureScope = requiredScope.toString(),
            )
        } ?: OAuthBearerValidationResult.newSuccess()
    }

    @Throws(OAuthBearerConfigException::class)
    private fun confirmNonNegative(allowableClockSkewMs: Int): Int {
        if (allowableClockSkewMs < 0) throw OAuthBearerConfigException(
            String.format("Allowable clock skew must not be negative: %d", allowableClockSkewMs)
        )
        return allowableClockSkewMs
    }

    private fun doesNotExistResult(
        required: Boolean,
        claimName: String,
    ): OAuthBearerValidationResult {
        return if (required) OAuthBearerValidationResult.newFailure(
            String.format("Required claim missing: %s", claimName)
        ) else OAuthBearerValidationResult.newSuccess()
    }
}
