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
import java.util.Base64
import javax.security.auth.callback.UnsupportedCallbackException
import org.apache.kafka.common.security.authenticator.TestJaasConfig
import org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule
import org.apache.kafka.common.security.oauthbearer.OAuthBearerValidatorCallback
import org.apache.kafka.common.utils.MockTime
import org.apache.kafka.common.utils.Time
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertNull

class OAuthBearerUnsecuredValidatorCallbackHandlerTest {

    @Test
    fun validToken() {
        for (includeOptionalIssuedAtClaim in booleanArrayOf(true, false)) {
            val claimsJson = "{$PRINCIPAL_CLAIM_TEXT,$EXPIRATION_TIME_CLAIM_TEXT${
                if (includeOptionalIssuedAtClaim) ",$ISSUED_AT_CLAIM_TEXT" else ""
            }}"
            val validationResult = validationResult(
                headerJson = UNSECURED_JWT_HEADER_JSON,
                claimsJson = claimsJson,
                moduleOptionsMap = MODULE_OPTIONS_MAP_NO_SCOPE_REQUIRED,
            )
            assertIs<OAuthBearerValidatorCallback>(validationResult)
            assertIs<OAuthBearerUnsecuredJws>(validationResult.token)
        }
    }

    @Test
    @Throws(IOException::class, UnsupportedCallbackException::class)
    fun badOrMissingPrincipal() {
        for (exists in booleanArrayOf(true, false)) {
            val claimsJson = "{$EXPIRATION_TIME_CLAIM_TEXT${if (exists) ",$BAD_PRINCIPAL_CLAIM_TEXT" else ""}}"
            confirmFailsValidation(
                headerJson = UNSECURED_JWT_HEADER_JSON,
                claimsJson = claimsJson,
                moduleOptionsMap = MODULE_OPTIONS_MAP_NO_SCOPE_REQUIRED,
            )
        }
    }

    @Test
    @Throws(IOException::class, UnsupportedCallbackException::class)
    fun tooEarlyExpirationTime() {
        val claimsJson = "{" + PRINCIPAL_CLAIM_TEXT + comma(ISSUED_AT_CLAIM_TEXT) +
                comma(TOO_EARLY_EXPIRATION_TIME_CLAIM_TEXT) + "}"
        confirmFailsValidation(
            headerJson = UNSECURED_JWT_HEADER_JSON,
            claimsJson = claimsJson,
            moduleOptionsMap = MODULE_OPTIONS_MAP_NO_SCOPE_REQUIRED,
        )
    }

    @Test
    fun includesRequiredScope() {
        val claimsJson = "{" + SUB_CLAIM_TEXT + comma(EXPIRATION_TIME_CLAIM_TEXT) + comma(SCOPE_CLAIM_TEXT) + "}"
        val validationResult = validationResult(
            headerJson = UNSECURED_JWT_HEADER_JSON,
            claimsJson = claimsJson,
            moduleOptionsMap = MODULE_OPTIONS_MAP_REQUIRE_EXISTING_SCOPE
        )
        assertIs<OAuthBearerValidatorCallback>(validationResult)
        assertIs<OAuthBearerUnsecuredJws>((validationResult).token)
    }

    @Test
    @Throws(IOException::class, UnsupportedCallbackException::class)
    fun missingRequiredScope() {
        val claimsJson = "{" + SUB_CLAIM_TEXT + comma(EXPIRATION_TIME_CLAIM_TEXT) + comma(SCOPE_CLAIM_TEXT) + "}"
        confirmFailsValidation(
            headerJson = UNSECURED_JWT_HEADER_JSON,
            claimsJson = claimsJson,
            moduleOptionsMap = MODULE_OPTIONS_MAP_REQUIRE_ADDITIONAL_SCOPE,
            optionalFailureScope = "[scope1, scope2]",
        )
    }

    companion object {

        private val UNSECURED_JWT_HEADER_JSON = """{${claimOrHeaderText("alg", "none")}}"""

        private val MOCK_TIME: Time = MockTime()

        private const val QUOTE = "\""

        private const val PRINCIPAL_CLAIM_VALUE = "username"

        private val PRINCIPAL_CLAIM_TEXT = claimOrHeaderText("principal", PRINCIPAL_CLAIM_VALUE)

        private val SUB_CLAIM_TEXT = claimOrHeaderText("sub", PRINCIPAL_CLAIM_VALUE)

        private val BAD_PRINCIPAL_CLAIM_TEXT = claimOrHeaderText("principal", 1)

        private const val LIFETIME_SECONDS_TO_USE = (1000 * 60 * 60).toLong()

        private val EXPIRATION_TIME_CLAIM_TEXT = expClaimText(LIFETIME_SECONDS_TO_USE)

        private val TOO_EARLY_EXPIRATION_TIME_CLAIM_TEXT = expClaimText(0)

        private val ISSUED_AT_CLAIM_TEXT = claimOrHeaderText("iat", MOCK_TIME.milliseconds() / 1000.0)

        private val SCOPE_CLAIM_TEXT = claimOrHeaderText("scope", "scope1")

        private val MODULE_OPTIONS_MAP_NO_SCOPE_REQUIRED: Map<String, String> = mapOf(
            "unsecuredValidatorPrincipalClaimName" to "principal",
            "unsecuredValidatorAllowableClockSkewMs" to "1",
        )

        private val MODULE_OPTIONS_MAP_REQUIRE_EXISTING_SCOPE: Map<String, String> =
            mapOf("unsecuredValidatorRequiredScope" to "scope1")

        private val MODULE_OPTIONS_MAP_REQUIRE_ADDITIONAL_SCOPE: Map<String, String> =
            mapOf("unsecuredValidatorRequiredScope" to "scope1 scope2")

        @Throws(OAuthBearerConfigException::class, OAuthBearerIllegalTokenException::class)
        private fun confirmFailsValidation(
            headerJson: String, claimsJson: String,
            moduleOptionsMap: Map<String, String>?, optionalFailureScope: String? = null,
        ) {
            val validationResultObj = validationResult(headerJson, claimsJson, moduleOptionsMap)
            val callback = assertIs<OAuthBearerValidatorCallback>(validationResultObj)
            assertNull(callback.token)
            assertNull(callback.errorOpenIDConfiguration)
            if (optionalFailureScope == null) {
                assertEquals("invalid_token", callback.errorStatus)
                assertNull(callback.errorScope)
            } else {
                assertEquals("insufficient_scope", callback.errorStatus)
                assertEquals(optionalFailureScope, callback.errorScope)
            }
        }

        private fun validationResult(
            headerJson: String,
            claimsJson: String,
            moduleOptionsMap: Map<String, String>?,
        ): Any {
            val urlEncoderNoPadding = Base64.getUrlEncoder().withoutPadding()
            return try {
                val tokenValue = "${urlEncoderNoPadding.encodeToString(headerJson.toByteArray())}." +
                        "${urlEncoderNoPadding.encodeToString(claimsJson.toByteArray())}."
                val callback = OAuthBearerValidatorCallback(tokenValue)
                createCallbackHandler(moduleOptionsMap).handle(arrayOf(callback))
                callback
            } catch (e: Exception) {
                e
            }
        }

        private fun createCallbackHandler(options: Map<String, String>?): OAuthBearerUnsecuredValidatorCallbackHandler {
            val config = TestJaasConfig()
            config.createOrUpdateEntry(
                name = "KafkaClient",
                loginModule = "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule",
                options = options,
            )
            val callbackHandler = OAuthBearerUnsecuredValidatorCallbackHandler()
            callbackHandler.configure(
                configs = emptyMap<String, Any>(),
                saslMechanism = OAuthBearerLoginModule.OAUTHBEARER_MECHANISM,
                jaasConfigEntries = listOf(config.getAppConfigurationEntry("KafkaClient")!![0]!!),
            )
            return callbackHandler
        }

        private fun comma(value: String): String = ",$value"

        private fun claimOrHeaderText(claimName: String, claimValue: Number): String =
            "$QUOTE$claimName$QUOTE:$claimValue"

        private fun claimOrHeaderText(claimName: String, claimValue: String): String =
            "$QUOTE$claimName$QUOTE:$QUOTE$claimValue$QUOTE"

        private fun expClaimText(lifetimeSeconds: Long): String = claimOrHeaderText(
            claimName = "exp",
            claimValue = MOCK_TIME.milliseconds() / 1000.0 + lifetimeSeconds,
        )
    }
}
