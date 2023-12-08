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
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class OAuthBearerUnsecuredJwsTest {

    @Test
    @Throws(OAuthBearerIllegalTokenException::class)
    fun validClaims() {
        val issuedAtSeconds = 100.1
        val expirationTimeSeconds = 300.3
        val sb = StringBuilder("{")
        appendJsonText(sb = sb, claimName = "sub", claimValue = "SUBJECT")
        appendCommaJsonText(sb = sb, claimName = "iat", claimValue = issuedAtSeconds)
        appendCommaJsonText(sb = sb, claimName = "exp", claimValue = expirationTimeSeconds)
        sb.append("}")
        val compactSerialization = HEADER_COMPACT_SERIALIZATION +
                Base64.getUrlEncoder().withoutPadding()
                    .encodeToString(sb.toString().toByteArray()) + "."
        val testJwt = OAuthBearerUnsecuredJws(
            compactSerialization = compactSerialization,
            principalClaimName = "sub",
            scopeClaimName = "scope",
        )
        assertEquals(compactSerialization, testJwt.value())
        assertEquals("sub", testJwt.principalClaimName)
        assertEquals(1, testJwt.header.size)
        assertEquals("none", testJwt.header["alg"])
        assertEquals("scope", testJwt.scopeClaimName)
        assertEquals(expirationTimeSeconds, testJwt.expirationTime)
        assertTrue(testJwt.isClaimType(claimName = "exp", type = Number::class.java))
        assertEquals(issuedAtSeconds, testJwt.issuedAt)
        assertEquals("SUBJECT", testJwt.subject)
    }

    @Test
    fun validCompactSerialization() {
        val subject = "foo"
        val issuedAt: Long = 100
        val expirationTime = issuedAt + 60 * 60
        val scope = listOf("scopeValue1", "scopeValue2")
        val validCompactSerialization = compactSerialization(
            subject = subject,
            issuedAt = issuedAt,
            expirationTime = expirationTime,
            scope = scope,
        )
        val jws = OAuthBearerUnsecuredJws(
            compactSerialization = validCompactSerialization,
            principalClaimName = "sub",
            scopeClaimName = "scope",
        )
        assertEquals(1, jws.header.size)
        assertEquals("none", jws.header["alg"])
        assertEquals(4, jws.claims.size)
        assertEquals(subject, jws.claims["sub"])
        assertEquals(subject, jws.principalName())
        assertEquals(issuedAt, Number::class.java.cast(jws.claims["iat"]).toLong())
        assertEquals(expirationTime, Number::class.java.cast(jws.claims["exp"]).toLong())
        assertEquals(expirationTime * 1000, jws.lifetimeMs())
        assertEquals(scope, jws.claims["scope"])
        assertEquals(HashSet(scope), jws.scope())
        assertEquals(3, jws.splits.size)
        assertEquals(
            expected = validCompactSerialization
                .split("\\.".toRegex())
                .dropLastWhile { it.isEmpty() }
                .toTypedArray()[0],
            actual = jws.splits[0],
        )
        assertEquals(
            expected = validCompactSerialization
                .split("\\.".toRegex())
                .dropLastWhile { it.isEmpty() }
                .toTypedArray()[1],
            actual = jws.splits[1],
        )
        assertEquals("", jws.splits[2])
    }

    @Test
    fun missingPrincipal() {
        val subject: String? = null
        val issuedAt: Long = 100
        val expirationTime: Long? = null
        val scope = listOf("scopeValue1", "scopeValue2")
        val validCompactSerialization = compactSerialization(
            subject = subject,
            issuedAt = issuedAt,
            expirationTime = expirationTime,
            scope = scope,
        )
        assertFailsWith<OAuthBearerIllegalTokenException> {
            OAuthBearerUnsecuredJws(
                compactSerialization = validCompactSerialization,
                principalClaimName = "sub",
                scopeClaimName = "scope",
            )
        }
    }

    @Test
    fun blankPrincipalName() {
        val subject = "   "
        val issuedAt: Long = 100
        val expirationTime = issuedAt + 60 * 60
        val scope = listOf("scopeValue1", "scopeValue2")
        val validCompactSerialization = compactSerialization(
            subject = subject,
            issuedAt = issuedAt,
            expirationTime = expirationTime,
            scope = scope,
        )
        assertFailsWith<OAuthBearerIllegalTokenException> {
            OAuthBearerUnsecuredJws(
                compactSerialization = validCompactSerialization,
                principalClaimName = "sub",
                scopeClaimName = "scope",
            )
        }
    }

    companion object {

        private const val QUOTE = "\""

        private val HEADER_COMPACT_SERIALIZATION = Base64.getUrlEncoder().withoutPadding()
            .encodeToString("{\"alg\":\"none\"}".toByteArray()) + "."

        private fun compactSerialization(
            subject: String?,
            issuedAt: Long?,
            expirationTime: Long?,
            scope: List<String>?,
        ): String {
            val encoder = Base64.getUrlEncoder().withoutPadding()
            val algorithm = "none"
            val headerJson = """{"alg":"$algorithm"}"""
            val encodedHeader = encoder.encodeToString(headerJson.toByteArray())
            val subjectJson = subject?.let { """"sub":"$it"""" }
            val issuedAtJson = issuedAt?.let { """"iat":$it""" }
            val expirationTimeJson = expirationTime?.let { """"exp":$it""" }
            val scopeJson = scope?.let { scopeJson(it) }
            val claimsJson = claimsJson(
                subjectJson,
                issuedAtJson,
                expirationTimeJson,
                scopeJson,
            )
            val encodedClaims = encoder.encodeToString(claimsJson.toByteArray())
            return "$encodedHeader.$encodedClaims."
        }

        private fun claimsJson(vararg jsonValues: String?): String {
            val claimsJsonBuilder = StringBuilder("{")
            val initialLength = claimsJsonBuilder.length
            for (jsonValue in jsonValues) {
                if (jsonValue != null) {
                    if (claimsJsonBuilder.length > initialLength) claimsJsonBuilder.append(',')
                    claimsJsonBuilder.append(jsonValue)
                }
            }
            claimsJsonBuilder.append('}')
            return claimsJsonBuilder.toString()
        }

        private fun scopeJson(scope: List<String>): String {
            val scopeJsonBuilder = StringBuilder("\"scope\":[")
            val initialLength = scopeJsonBuilder.length
            for (scopeValue in scope) {
                if (scopeJsonBuilder.length > initialLength) scopeJsonBuilder.append(',')
                scopeJsonBuilder.append('"')
                    .append(scopeValue)
                    .append('"')
            }
            scopeJsonBuilder.append(']')
            return scopeJsonBuilder.toString()
        }

        private fun appendCommaJsonText(sb: StringBuilder, claimName: String, claimValue: Number) {
            sb.append(',')
                .append(QUOTE)
                .append(escape(claimName))
                .append(QUOTE)
                .append(":")
                .append(claimValue)
        }

        private fun appendJsonText(sb: StringBuilder, claimName: String, claimValue: String) {
            sb.append(QUOTE)
                .append(escape(claimName))
                .append(QUOTE)
                .append(":")
                .append(QUOTE)
                .append(escape(claimValue))
                .append(QUOTE)
        }

        private fun escape(jsonStringValue: String): String {
            return jsonStringValue.replace("\"", "\\\"")
                .replace("\\", "\\\\")
        }
    }
}
