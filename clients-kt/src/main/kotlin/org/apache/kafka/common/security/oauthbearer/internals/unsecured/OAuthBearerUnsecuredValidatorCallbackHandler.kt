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

import java.util.*
import javax.security.auth.callback.Callback
import javax.security.auth.callback.UnsupportedCallbackException
import javax.security.auth.login.AppConfigurationEntry
import org.apache.kafka.common.security.auth.AuthenticateCallbackHandler
import org.apache.kafka.common.security.oauthbearer.OAuthBearerExtensionsValidatorCallback
import org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule
import org.apache.kafka.common.security.oauthbearer.OAuthBearerValidatorCallback
import org.apache.kafka.common.security.oauthbearer.internals.unsecured.OAuthBearerScopeUtils.parseScope
import org.apache.kafka.common.security.oauthbearer.internals.unsecured.OAuthBearerValidationUtils.validateClaimForExistenceAndType
import org.apache.kafka.common.security.oauthbearer.internals.unsecured.OAuthBearerValidationUtils.validateExpirationTime
import org.apache.kafka.common.security.oauthbearer.internals.unsecured.OAuthBearerValidationUtils.validateIssuedAt
import org.apache.kafka.common.security.oauthbearer.internals.unsecured.OAuthBearerValidationUtils.validateScope
import org.apache.kafka.common.security.oauthbearer.internals.unsecured.OAuthBearerValidationUtils.validateTimeConsistency
import org.apache.kafka.common.utils.Time
import org.slf4j.LoggerFactory

/**
 * A `CallbackHandler` that recognizes [OAuthBearerValidatorCallback] and validates an unsecured
 * OAuth 2 bearer token. It requires there to be an `"exp" (Expiration Time)` claim of type Number.
 * If `"iat" (Issued At)` or `"nbf" (Not Before)` claims are present each must be a number that
 * precedes the Expiration Time claim, and if both are present the Not Before claim must not precede
 * the Issued At claim. It also accepts the following options, none of which are required:
 *
 * - `unsecuredValidatorPrincipalClaimName` set to a non-empty value if you wish a particular String
 * claim holding a principal name to be checked for existence; the default is to check for the
 * existence of the '`sub`' claim
 * - `unsecuredValidatorScopeClaimName` set to a custom claim name if you wish the name of the
 * String or String List claim holding any token scope to be something other than '`scope`'
 * - `unsecuredValidatorRequiredScope` set to a space-delimited list of scope values if you wish the
 * String/String List claim holding the token scope to be checked to make sure it contains certain
 * values
 * - `unsecuredValidatorAllowableClockSkewMs` set to a positive integer value if you wish to allow
 * up to some number of positive milliseconds of clock skew (the default is 0)
 *
 * For example:
 *
 * ```plaintext
 * KafkaServer {
 *   org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule Required
 *   unsecuredLoginStringClaim_sub="thePrincipalName"
 *   unsecuredLoginListClaim_scope=",KAFKA_BROKER,LOGIN_TO_KAFKA"
 *   unsecuredValidatorRequiredScope="LOGIN_TO_KAFKA"
 *   unsecuredValidatorAllowableClockSkewMs="3000";
 * };
 * ```
 *
 * It also recognizes [OAuthBearerExtensionsValidatorCallback] and validates every extension passed
 * to it.
 *
 * This class is the default when the SASL mechanism is OAUTHBEARER and no value is explicitly set
 * via the `listener.name.sasl_[plaintext|ssl].oauthbearer.sasl.server.callback.handler.class`
 * broker configuration property. It is worth noting that this class is not suitable for production
 * use due to the use of unsecured JWT tokens and validation of every given extension.
 */
open class OAuthBearerUnsecuredValidatorCallbackHandler : AuthenticateCallbackHandler {

    private var time = Time.SYSTEM

    private lateinit var moduleOptions: Map<String, String?>

    var configured = false
        private set

    /**
     * For testing
     *
     * @param time
     * the mandatory time to set
     */
    fun time(time: Time) {
        this.time = time
    }

    /**
     * Return true if this instance has been configured, otherwise false
     *
     * @return true if this instance has been configured, otherwise false
     */
    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("configured")
    )
    fun configured(): Boolean = configured

    override fun configure(
        configs: Map<String, *>,
        saslMechanism: String,
        jaasConfigEntries: List<AppConfigurationEntry>
    ) {
        require(OAuthBearerLoginModule.OAUTHBEARER_MECHANISM == saslMechanism) {
            String.format("Unexpected SASL mechanism: %s", saslMechanism)
        }
        require(jaasConfigEntries.size == 1) {
            String.format(
                "Must supply exactly 1 non-null JAAS mechanism configuration (size was %d)",
                jaasConfigEntries.size
            )
        }
        val unmodifiableModuleOptions = jaasConfigEntries[0].options as Map<String, String?>
        moduleOptions = unmodifiableModuleOptions
        configured = true
    }

    @Throws(UnsupportedCallbackException::class)
    override fun handle(callbacks: Array<Callback>) {
        check(configured) { "Callback handler not configured" }
        for (callback in callbacks) {
            when (callback) {
                is OAuthBearerValidatorCallback -> {
                    try {
                        handleCallback(callback)
                    } catch (e: OAuthBearerIllegalTokenException) {
                        val failureReason = e.reason
                        val failureScope = e.reason.failureScope
                        callback.error(
                            errorStatus = if (failureScope != null) "insufficient_scope"
                            else "invalid_token",
                            errorScope = failureScope,
                            errorOpenIDConfiguration = failureReason.failureOpenIdConfig,
                        )
                    }
                }

                is OAuthBearerExtensionsValidatorCallback -> callback.inputExtensions.map().keys
                    .forEach { extensionName -> callback.valid(extensionName) }

                else -> throw UnsupportedCallbackException(callback)
            }
        }
    }

    override fun close() = Unit

    private fun handleCallback(callback: OAuthBearerValidatorCallback) {
        val principalClaimName = principalClaimName()
        val scopeClaimName = scopeClaimName()
        val requiredScope = requiredScope()
        val allowableClockSkewMs = allowableClockSkewMs()
        val unsecuredJwt = OAuthBearerUnsecuredJws(
            callback.tokenValue,
            principalClaimName,
            scopeClaimName
        )
        val now = time.milliseconds()
        validateClaimForExistenceAndType(
            jwt = unsecuredJwt,
            required = true,
            claimName = principalClaimName,
            String::class.java,
        ).throwExceptionIfFailed()

        validateIssuedAt(
            jwt = unsecuredJwt,
            required = false,
            whenCheckTimeMs = now,
            allowableClockSkewMs = allowableClockSkewMs,
        ).throwExceptionIfFailed()

        validateExpirationTime(
            jwt = unsecuredJwt,
            whenCheckTimeMs = now,
            allowableClockSkewMs = allowableClockSkewMs,
        ).throwExceptionIfFailed()

        validateTimeConsistency(jwt = unsecuredJwt).throwExceptionIfFailed()

        validateScope(
            token = unsecuredJwt,
            requiredScope = requiredScope,
        ).throwExceptionIfFailed()

        log.info(
            "Successfully validated token with principal {}: {}", unsecuredJwt.principalName(),
            unsecuredJwt.claims
        )
        callback.setToken(unsecuredJwt)
    }

    private fun principalClaimName(): String {
        val principalClaimNameValue = option(PRINCIPAL_CLAIM_NAME_OPTION)
        return if (principalClaimNameValue.isNullOrBlank()) "sub"
        else principalClaimNameValue!!.trim { it <= ' ' }
    }

    private fun scopeClaimName(): String {
        val scopeClaimNameValue = option(SCOPE_CLAIM_NAME_OPTION)
        return if (scopeClaimNameValue.isNullOrBlank()) "scope"
        else scopeClaimNameValue!!.trim { it <= ' ' }
    }

    private fun requiredScope(): List<String> {
        val requiredSpaceDelimitedScope = option(REQUIRED_SCOPE_OPTION)
        return if (requiredSpaceDelimitedScope.isNullOrBlank()) emptyList()
        else parseScope(requiredSpaceDelimitedScope!!.trim { it <= ' ' })
    }

    private fun allowableClockSkewMs(): Int {
        val allowableClockSkewMsValue = option(ALLOWABLE_CLOCK_SKEW_MILLIS_OPTION)

        val allowableClockSkewMs = try {
            if (allowableClockSkewMsValue.isNullOrBlank()) 0
            else allowableClockSkewMsValue!!.trim { it <= ' ' }.toInt()
        } catch (e: NumberFormatException) {
            throw OAuthBearerConfigException(e.message, e)
        }

        if (allowableClockSkewMs < 0) throw OAuthBearerConfigException(
            String.format(
                "Allowable clock skew millis must not be negative: %s",
                allowableClockSkewMsValue
            )
        )

        return allowableClockSkewMs
    }

    private fun option(key: String): String? {
        check(configured) { "Callback handler not configured" }
        return moduleOptions[key]
    }

    companion object {

        private val log =
            LoggerFactory.getLogger(OAuthBearerUnsecuredValidatorCallbackHandler::class.java)

        private const val OPTION_PREFIX = "unsecuredValidator"

        private const val PRINCIPAL_CLAIM_NAME_OPTION = OPTION_PREFIX + "PrincipalClaimName"

        private const val SCOPE_CLAIM_NAME_OPTION = OPTION_PREFIX + "ScopeClaimName"

        private const val REQUIRED_SCOPE_OPTION = OPTION_PREFIX + "RequiredScope"

        private const val ALLOWABLE_CLOCK_SKEW_MILLIS_OPTION =
            OPTION_PREFIX + "AllowableClockSkewMs"
    }
}
