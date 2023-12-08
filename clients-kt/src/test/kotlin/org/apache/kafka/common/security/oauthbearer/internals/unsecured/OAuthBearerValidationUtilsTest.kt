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

import java.util.Base64
import org.apache.kafka.common.security.oauthbearer.internals.unsecured.OAuthBearerValidationUtils.validateClaimForExistenceAndType
import org.apache.kafka.common.security.oauthbearer.internals.unsecured.OAuthBearerValidationUtils.validateExpirationTime
import org.apache.kafka.common.security.oauthbearer.internals.unsecured.OAuthBearerValidationUtils.validateIssuedAt
import org.apache.kafka.common.security.oauthbearer.internals.unsecured.OAuthBearerValidationUtils.validateScope
import org.apache.kafka.common.security.oauthbearer.internals.unsecured.OAuthBearerValidationUtils.validateTimeConsistency
import org.apache.kafka.common.utils.Time
import org.junit.jupiter.api.Test
import kotlin.test.assertTrue

class OAuthBearerValidationUtilsTest {

    @Test
    @Throws(OAuthBearerIllegalTokenException::class)
    fun validateClaimForExistenceAndType() {
        val claimName = "foo"
        for (exists in arrayOf(null, true, false)) {
            val useErrorValue = exists == null
            for (required in booleanArrayOf(true, false)) {
                val sb = StringBuilder("{")
                appendJsonText(sb, "exp", 100)
                appendCommaJsonText(sb, "sub", "principalName")
                if (useErrorValue) appendCommaJsonText(
                    sb = sb,
                    claimName = claimName,
                    claimValue = 1,
                ) else if (exists != null && exists) appendCommaJsonText(
                    sb = sb,
                    claimName = claimName,
                    claimValue = claimName,
                )
                sb.append("}")
                val compactSerialization = HEADER_COMPACT_SERIALIZATION +
                        Base64.getUrlEncoder().withoutPadding().encodeToString(sb.toString().toByteArray()) + "."
                val testJwt = OAuthBearerUnsecuredJws(
                    compactSerialization = compactSerialization,
                    principalClaimName = "sub",
                    scopeClaimName = "scope",
                )
                val result = validateClaimForExistenceAndType(
                    jwt = testJwt,
                    required = required,
                    claimName = claimName,
                    allowedTypes = arrayOf(String::class.java),
                )
                if (useErrorValue || required && !exists!!) assertTrue(isFailureWithMessageAndNoFailureScope(result))
                else assertTrue(isSuccess(result))
            }
        }
    }

    @Test
    fun validateIssuedAt() {
        val nowMs = TIME.milliseconds()
        val nowClaimValue = nowMs.toDouble() / 1000
        for (exists in booleanArrayOf(true, false)) {
            val sb = StringBuilder("{")
            appendJsonText(sb = sb, claimName = "exp", claimValue = nowClaimValue)
            appendCommaJsonText(sb = sb, claimName = "sub", claimValue = "principalName")
            if (exists) appendCommaJsonText(sb = sb, claimName = "iat", claimValue = nowClaimValue)
            sb.append("}")
            val compactSerialization = HEADER_COMPACT_SERIALIZATION +
                    Base64.getUrlEncoder().withoutPadding().encodeToString(sb.toString().toByteArray()) + "."
            val testJwt = OAuthBearerUnsecuredJws(
                compactSerialization = compactSerialization,
                principalClaimName = "sub",
                scopeClaimName = "scope",
            )
            for (required in booleanArrayOf(true, false)) {
                for (allowableClockSkewMs in intArrayOf(0, 5, 10, 20)) {
                    for (whenCheckOffsetMs in longArrayOf(-10, 0, 10)) {
                        val whenCheckMs = nowMs + whenCheckOffsetMs
                        val result = validateIssuedAt(
                            jwt = testJwt,
                            required = required,
                            whenCheckTimeMs = whenCheckMs,
                            allowableClockSkewMs = allowableClockSkewMs,
                        )
                        if (required && !exists) assertTrue(
                            actual = isFailureWithMessageAndNoFailureScope(result),
                            message = "useErrorValue || required && !exists",
                        )
                        else if (!required && !exists) assertTrue(isSuccess(result), "!required && !exists")
                        else if (nowClaimValue * 1000 > whenCheckMs + allowableClockSkewMs) // issued in future
                            assertTrue(
                                actual = isFailureWithMessageAndNoFailureScope(result),
                                message = assertionFailureMessage(nowClaimValue, allowableClockSkewMs, whenCheckMs),
                            )
                        else assertTrue(
                            actual = isSuccess(result),
                            message = assertionFailureMessage(nowClaimValue, allowableClockSkewMs, whenCheckMs),
                        )
                    }
                }
            }
        }
    }

    @Test
    fun validateExpirationTime() {
        val nowMs = TIME.milliseconds()
        val nowClaimValue = nowMs.toDouble() / 1000
        val sb = StringBuilder("{")
        appendJsonText(sb = sb, claimName = "exp", claimValue = nowClaimValue)
        appendCommaJsonText(sb = sb, claimName = "sub", claimValue = "principalName")
        sb.append("}")
        val compactSerialization = HEADER_COMPACT_SERIALIZATION +
                Base64.getUrlEncoder().withoutPadding().encodeToString(sb.toString().toByteArray()) + "."
        val testJwt = OAuthBearerUnsecuredJws(
            compactSerialization = compactSerialization,
            principalClaimName = "sub",
            scopeClaimName = "scope",
        )
        for (allowableClockSkewMs in intArrayOf(0, 5, 10, 20)) {
            for (whenCheckOffsetMs in longArrayOf(-10, 0, 10)) {
                val whenCheckMs = nowMs + whenCheckOffsetMs
                val result = validateExpirationTime(
                    jwt = testJwt,
                    whenCheckTimeMs = whenCheckMs,
                    allowableClockSkewMs = allowableClockSkewMs,
                )
                if (whenCheckMs - allowableClockSkewMs >= nowClaimValue * 1000) assertTrue(
                    // expired
                    actual = isFailureWithMessageAndNoFailureScope(result),
                    message = assertionFailureMessage(nowClaimValue, allowableClockSkewMs, whenCheckMs),
                )
                else assertTrue(
                    actual = isSuccess(result),
                    message = assertionFailureMessage(nowClaimValue, allowableClockSkewMs, whenCheckMs)
                )
            }
        }
    }

    @Test
    @Throws(OAuthBearerIllegalTokenException::class)
    fun validateExpirationTimeAndIssuedAtConsistency() {
        val nowMs = TIME.milliseconds()
        val nowClaimValue = nowMs.toDouble() / 1000
        for (issuedAtExists in booleanArrayOf(true, false)) {
            if (!issuedAtExists) {
                val sb = StringBuilder("{")
                appendJsonText(sb, "exp", nowClaimValue)
                appendCommaJsonText(sb, "sub", "principalName")
                sb.append("}")
                val compactSerialization = HEADER_COMPACT_SERIALIZATION +
                        Base64.getUrlEncoder().withoutPadding().encodeToString(sb.toString().toByteArray()) + "."
                val testJwt = OAuthBearerUnsecuredJws(
                    compactSerialization = compactSerialization,
                    principalClaimName = "sub",
                    scopeClaimName = "scope",
                )
                assertTrue(isSuccess(validateTimeConsistency(testJwt)))
            } else for (expirationTimeOffset in -1..1) {
                val sb = StringBuilder("{")
                appendJsonText(sb, "iat", nowClaimValue)
                appendCommaJsonText(sb = sb, claimName = "exp", claimValue = nowClaimValue + expirationTimeOffset)
                appendCommaJsonText(sb = sb, claimName = "sub", claimValue = "principalName")
                sb.append("}")
                val compactSerialization = HEADER_COMPACT_SERIALIZATION +
                        Base64.getUrlEncoder().withoutPadding().encodeToString(sb.toString().toByteArray()) + "."
                val testJwt = OAuthBearerUnsecuredJws(
                    compactSerialization = compactSerialization,
                    principalClaimName = "sub",
                    scopeClaimName = "scope",
                )
                val result = validateTimeConsistency(testJwt)
                if (expirationTimeOffset <= 0) assertTrue(isFailureWithMessageAndNoFailureScope(result))
                else assertTrue(isSuccess(result))
            }
        }
    }

    @Test
    fun validateScope() {
        val nowMs = TIME.milliseconds()
        val nowClaimValue = nowMs.toDouble() / 1000
        val noScope = emptyList<String>()
        val scope1: List<String> = mutableListOf("scope1")
        val scope1And2: List<String> = mutableListOf("scope1", "scope2")
        for (actualScopeExists in booleanArrayOf(true, false)) {
            val scopes =
                if (!actualScopeExists) listOf<List<String>?>(null)
                else listOf(noScope, scope1, scope1And2)

            for (actualScope in scopes) {
                for (requiredScopeExists in booleanArrayOf(true, false)) {
                    val requiredScopes =
                        if (!requiredScopeExists) listOf<List<String>?>(null)
                        else listOf(noScope, scope1, scope1And2)

                    for (requiredScope in requiredScopes) {
                        val sb = StringBuilder("{")
                        appendJsonText(sb, "exp", nowClaimValue)
                        appendCommaJsonText(sb, "sub", "principalName")
                        if (actualScope != null) sb.append(',').append(scopeJson(actualScope))
                        sb.append("}")
                        val compactSerialization = HEADER_COMPACT_SERIALIZATION +
                                Base64.getUrlEncoder().withoutPadding()
                                    .encodeToString(sb.toString().toByteArray()) + "."
                        val testJwt = OAuthBearerUnsecuredJws(
                            compactSerialization = compactSerialization,
                            principalClaimName = "sub",
                            scopeClaimName = "scope",
                        )
                        val result = validateScope(testJwt, requiredScope)
                        if (!requiredScopeExists || requiredScope!!.isEmpty()) assertTrue(isSuccess(result))
                        else if (!actualScopeExists || actualScope!!.size < requiredScope.size)
                            assertTrue(isFailureWithMessageAndFailureScope(result))
                        else assertTrue(isSuccess(result))
                    }
                }
            }
        }
    }

    companion object {

        private const val QUOTE = "\""

        private val HEADER_COMPACT_SERIALIZATION = Base64.getUrlEncoder().withoutPadding()
            .encodeToString("{\"alg\":\"none\"}".toByteArray()) + "."

        private val TIME = Time.SYSTEM

        private fun assertionFailureMessage(claimValue: Double, allowableClockSkewMs: Int, whenCheckMs: Long): String {
            return String.format(
                "time=%f seconds, whenCheck = %d ms, allowableClockSkew=%d ms",
                claimValue,
                whenCheckMs,
                allowableClockSkewMs,
            )
        }

        private fun isSuccess(result: OAuthBearerValidationResult): Boolean {
            return result.success
        }

        private fun isFailureWithMessageAndNoFailureScope(result: OAuthBearerValidationResult): Boolean {
            return !result.success
                    && result.failureDescription!!.isNotEmpty()
                    && result.failureScope == null
                    && result.failureOpenIdConfig == null
        }

        private fun isFailureWithMessageAndFailureScope(result: OAuthBearerValidationResult): Boolean {
            return !result.success
                    && result.failureDescription!!.isNotEmpty()
                    && result.failureScope!!.isNotEmpty()
                    && result.failureOpenIdConfig == null
        }

        private fun appendCommaJsonText(sb: StringBuilder, claimName: String, claimValue: Number) {
            sb.append(',')
                .append(QUOTE)
                .append(escape(claimName))
                .append(QUOTE)
                .append(":")
                .append(claimValue)
        }

        private fun appendCommaJsonText(sb: StringBuilder, claimName: String, claimValue: String) {
            sb.append(',')
                .append(QUOTE)
                .append(escape(claimName))
                .append(QUOTE)
                .append(":")
                .append(QUOTE)
                .append(escape(claimValue))
                .append(QUOTE)
        }

        private fun appendJsonText(sb: StringBuilder, claimName: String, claimValue: Number) {
            sb.append(QUOTE)
                .append(escape(claimName))
                .append(QUOTE)
                .append(":")
                .append(claimValue)
        }

        private fun escape(jsonStringValue: String): String {
            return jsonStringValue.replace("\"", "\\\"")
                .replace("\\", "\\\\")
        }

        private fun scopeJson(scope: List<String>): String {
            val scopeJsonBuilder = StringBuilder("\"scope\":[")
            val initialLength = scopeJsonBuilder.length
            for (scopeValue in scope) {
                if (scopeJsonBuilder.length > initialLength) scopeJsonBuilder.append(',')
                scopeJsonBuilder.append('"').append(scopeValue).append('"')
            }
            scopeJsonBuilder.append(']')
            return scopeJsonBuilder.toString()
        }
    }
}
