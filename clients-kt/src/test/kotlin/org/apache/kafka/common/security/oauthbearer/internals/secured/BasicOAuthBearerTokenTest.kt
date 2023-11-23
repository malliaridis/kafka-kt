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
import org.apache.kafka.common.security.oauthbearer.OAuthBearerToken
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

class BasicOAuthBearerTokenTest {
    
    @Test
    fun basic() {
        val token: OAuthBearerToken = BasicOAuthBearerToken(
            token = "not.valid.token",
            scopes = emptySet(),
            lifetimeMs = 0L,
            principalName = "jdoe",
            startTimeMs = 0L,
        )
        assertEquals("not.valid.token", token.value())
        assertTrue(token.scope().isEmpty())
        assertEquals(0L, token.lifetimeMs())
        assertEquals("jdoe", token.principalName())
        assertEquals(0L, token.startTimeMs())
    }

    @Test
    fun negativeLifetime() {
        val token: OAuthBearerToken = BasicOAuthBearerToken(
            token = "not.valid.token",
            scopes = emptySet(),
            lifetimeMs = -1L,
            principalName = "jdoe",
            startTimeMs = 0L,
        )
        assertEquals("not.valid.token", token.value())
        assertTrue(token.scope().isEmpty())
        assertEquals(-1L, token.lifetimeMs())
        assertEquals("jdoe", token.principalName())
        assertEquals(0L, token.startTimeMs())
    }

    @Test
    fun noErrorIfModifyScope() {
        // Start with a basic set created by the caller.
        val callerSet: SortedSet<String> = TreeSet(mutableListOf("a", "b", "c"))
        val token: OAuthBearerToken = BasicOAuthBearerToken(
            token = "not.valid.token",
            scopes = callerSet,
            lifetimeMs = 0L,
            principalName = "jdoe",
            startTimeMs = 0L
        )

        // Make sure it all looks good
        assertNotNull(token.scope())
        assertEquals(3, token.scope().size)

        // Add a value to the caller's set and note that it changes the token's scope set.
        // Make sure to make it read-only when it's passed in.
        callerSet.add("d")
        assertTrue(token.scope().contains("d"))

        // Similarly, removing a value from the caller's will affect the token's scope set.
        // Make sure to make it read-only when it's passed in.
        callerSet.remove("c")
        assertFalse(token.scope().contains("c"))

        // Ensure that attempting to change the token's scope set directly will not throw any error.
        (token.scope() as MutableSet<*>).clear()
    }
}
