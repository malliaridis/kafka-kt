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

import java.util.SortedSet
import java.util.TreeSet
import org.apache.kafka.common.security.oauthbearer.internals.secured.ClaimValidationUtils.validateClaimNameOverride
import org.apache.kafka.common.security.oauthbearer.internals.secured.ClaimValidationUtils.validateExpiration
import org.apache.kafka.common.security.oauthbearer.internals.secured.ClaimValidationUtils.validateIssuedAt
import org.apache.kafka.common.security.oauthbearer.internals.secured.ClaimValidationUtils.validateScopes
import org.apache.kafka.common.security.oauthbearer.internals.secured.ClaimValidationUtils.validateSubject
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class ClaimValidationUtilsTest : OAuthBearerTest() {
    
    @Test
    fun testValidateScopes() {
        val scopes = validateScopes(
            scopeClaimName = "scope",
            scopes = listOf("  a  ", "    b    "),
        )
        assertEquals(2, scopes.size)
        assertTrue(scopes.contains("a"))
        assertTrue(scopes.contains("b"))
    }

    @Test
    fun testValidateScopesDisallowsDuplicates() {
        assertFailsWith<ValidateException> {
            validateScopes(
                scopeClaimName = "scope",
                scopes = listOf("a", "b", "a"),
            )
        }
        assertFailsWith<ValidateException> {
            validateScopes(
                scopeClaimName = "scope",
                scopes = listOf("a", "b", "  a  "),
            )
        }
    }

    @Test
    fun testValidateScopesDisallowsEmptyNullAndWhitespace() {
        assertFailsWith<ValidateException> {
            validateScopes(
                scopeClaimName = "scope",
                scopes = listOf("a", ""),
            )
        }
        // Kotlin Migration: null values are not allowed
//        assertFailsWith<ValidateException> {
//            validateScopes(
//                "scope",
//                listOf("a", null),
//            )
//        }
        assertFailsWith<ValidateException> {
            validateScopes(
                scopeClaimName = "scope",
                scopes = listOf("a", "  "),
            )
        }
    }

    @Test
    fun testValidateScopesResultIsImmutable() {
        val callerSet: SortedSet<String> = TreeSet(listOf("a", "b", "c"))
        val scopes = validateScopes(scopeClaimName = "scope", scopes = callerSet)
        assertEquals(3, scopes.size)
        callerSet.add("d")
        assertEquals(4, callerSet.size)
        assertTrue(callerSet.contains("d"))
        assertEquals(3, scopes.size)
        assertFalse(scopes.contains("d"))
        callerSet.remove("c")
        assertEquals(3, callerSet.size)
        assertFalse(callerSet.contains("c"))
        assertEquals(3, scopes.size)
        assertTrue(scopes.contains("c"))
        callerSet.clear()
        assertEquals(0, callerSet.size)
        assertEquals(3, scopes.size)
    }

    @Test
    @Disabled("Kotlin Migration: A read-only set is returned by validateScopes")
    fun testValidateScopesResultThrowsExceptionOnMutation() {
//        val callerSet: SortedSet<String> = TreeSet(listOf("a", "b", "c"))
//        val scopes = validateScopes(scopeClaimName = "scope", scopes = callerSet)
//        assertFailsWith<UnsupportedOperationException> { scopes.clear() }
    }

    @Test
    fun testValidateExpiration() {
        val expected = 1L
        val actual = validateExpiration(claimName = "exp", claimValue = expected)
        assertEquals(expected, actual)
    }

    @Test
    fun testValidateExpirationAllowsZero() {
        val expected = 0L
        val actual = validateExpiration(claimName = "exp", claimValue = expected)
        assertEquals(expected, actual)
    }

    @Test
    fun testValidateExpirationDisallowsNull() {
        assertFailsWith<ValidateException> { validateExpiration(claimName = "exp", claimValue = null) }
    }

    @Test
    fun testValidateExpirationDisallowsNegatives() {
        assertFailsWith<ValidateException> { validateExpiration(claimName = "exp", claimValue = -1L) }
    }

    @Test
    fun testValidateSubject() {
        val expected = "jdoe"
        val actual = validateSubject("sub", expected)
        assertEquals(expected, actual)
    }

    @Test
    fun testValidateSubjectDisallowsEmptyNullAndWhitespace() {
        assertFailsWith<ValidateException> { validateSubject(claimName = "sub", claimValue = "") }
        assertFailsWith<ValidateException> { validateSubject(claimName = "sub", claimValue = null) }
        assertFailsWith<ValidateException> { validateSubject(claimName = "sub", claimValue = "  ") }
    }

    @Test
    fun testValidateClaimNameOverride() {
        val expected = "email"
        val actual = validateClaimNameOverride("sub", "  $expected  ")
        assertEquals(expected, actual)
    }

    @Test
    fun testValidateClaimNameOverrideDisallowsEmptyNullAndWhitespace() {
        assertFailsWith<ValidateException> { validateSubject(claimName = "sub", claimValue = "") }
        assertFailsWith<ValidateException> { validateSubject(claimName = "sub", claimValue = null) }
        assertFailsWith<ValidateException> { validateSubject(claimName = "sub", claimValue = "  ") }
    }

    @Test
    fun testValidateIssuedAt() {
        val expected = 1L
        val actual = validateIssuedAt(claimName = "iat", claimValue = expected)
        assertEquals(expected, actual)
    }

    @Test
    fun testValidateIssuedAtAllowsZero() {
        val expected = 0L
        val actual = validateIssuedAt(claimName = "iat", claimValue = expected)
        assertEquals(expected, actual)
    }

    @Test
    fun testValidateIssuedAtAllowsNull() {
        val expected: Long? = null
        val actual = validateIssuedAt(claimName = "iat", claimValue = expected)
        assertEquals(expected, actual)
    }

    @Test
    fun testValidateIssuedAtDisallowsNegatives() {
        assertFailsWith<ValidateException> { validateIssuedAt(claimName = "iat", claimValue = -1L) }
    }
}
