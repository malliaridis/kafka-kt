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

package org.apache.kafka.common.security.oauthbearer

import org.apache.kafka.common.security.auth.SaslExtensions
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class OAuthBearerExtensionsValidatorCallbackTest {
    
    @Test
    fun testValidatedExtensionsAreReturned() {
        val extensions = mapOf("hello" to "bye")
        val callback = OAuthBearerExtensionsValidatorCallback(
            token = TOKEN,
            inputExtensions = SaslExtensions(extensions),
        )
        assertTrue(callback.validatedExtensions().isEmpty())
        assertTrue(callback.invalidExtensions().isEmpty())
        callback.valid("hello")
        assertFalse(callback.validatedExtensions().isEmpty())
        assertEquals("bye", callback.validatedExtensions()["hello"])
        assertTrue(callback.invalidExtensions().isEmpty())
    }

    @Test
    fun testInvalidExtensionsAndErrorMessagesAreReturned() {
        val extensions = mapOf("hello" to "bye")
        val callback = OAuthBearerExtensionsValidatorCallback(
            token = TOKEN,
            inputExtensions = SaslExtensions(extensions),
        )
        assertTrue(callback.validatedExtensions().isEmpty())
        assertTrue(callback.invalidExtensions().isEmpty())
        callback.error("hello", "error")
        assertFalse(callback.invalidExtensions().isEmpty())
        assertEquals("error", callback.invalidExtensions()["hello"])
        assertTrue(callback.validatedExtensions().isEmpty())
    }

    /**
     * Extensions that are neither validated or invalidated must not be present in either maps
     */
    @Test
    fun testUnvalidatedExtensionsAreIgnored() {
        val extensions = mapOf(
            "valid" to "valid",
            "error" to "error",
            "nothing" to "nothing",
        )
        val callback = OAuthBearerExtensionsValidatorCallback(
            token = TOKEN,
            inputExtensions = SaslExtensions(extensions),
        )
        callback.error("error", "error")
        callback.valid("valid")
        assertFalse(callback.validatedExtensions().containsKey("nothing"))
        assertFalse(callback.invalidExtensions().containsKey("nothing"))
        assertEquals("nothing", callback.ignoredExtensions()["nothing"])
    }

    @Test
    fun testCannotValidateExtensionWhichWasNotGiven() {
        val extensions = mapOf("hello" to "bye")
        val callback = OAuthBearerExtensionsValidatorCallback(
            token = TOKEN,
            inputExtensions = SaslExtensions(extensions),
        )
        assertFailsWith<IllegalArgumentException> { callback.valid("???") }
    }

    companion object {
        private val TOKEN: OAuthBearerToken = OAuthBearerTokenMock()
    }
}
