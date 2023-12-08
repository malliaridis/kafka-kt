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

package org.apache.kafka.common.security

import java.util.Collections
import org.apache.kafka.common.security.auth.SaslExtensions
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import kotlin.test.assertFailsWith
import kotlin.test.assertNotEquals
import kotlin.test.assertNull

class SaslExtensionsTest {
    
    private lateinit var map: MutableMap<String, String>
    
    @BeforeEach
    fun setUp() {
        map = mutableMapOf(
            "what" to "42",
            "who" to "me",
        )
    }

    @Test
    @Disabled("Kotlin Migration: The extensions map() function returns an immutable list.")
    fun testReturnedMapIsImmutable() {
//        val extensions = SaslExtensions(map)
//        assertFailsWith<UnsupportedOperationException> { extensions.map().put("hello", "test") }
    }

    @Test
    fun testCannotAddValueToMapReferenceAndGetFromExtensions() {
        val extensions = SaslExtensions(map)
        assertNull(extensions.map()["hello"])
        map["hello"] = "42"
        assertNull(extensions.map()["hello"])
    }

    /**
     * Tests that even when using the same underlying values in the map, two [SaslExtensions]
     * are considered unique.
     *
     * @see SaslExtensions class-level documentation
     */
    @Test
    fun testExtensionsWithEqualValuesAreUnique() {
        // If the maps are distinct objects but have the same underlying values, the SaslExtension
        // objects should still be unique.
        assertNotEquals(
            illegal = SaslExtensions(Collections.singletonMap("key", "value")),
            actual = SaslExtensions(Collections.singletonMap("key", "value")),
            message = "SaslExtensions with unique maps should be unique",
        )

        // If the maps are the same object (with the same underlying values), the SaslExtension
        // objects should still be unique.
        assertNotEquals(
            illegal = SaslExtensions(map),
            actual = SaslExtensions(map),
            message = "SaslExtensions with duplicate maps should be unique",
        )

        // If the maps are empty, the SaslExtension objects should still be unique.
        assertNotEquals(
            illegal = SaslExtensions.empty(),
            actual = SaslExtensions.empty(),
            message = "SaslExtensions returned from SaslExtensions.empty() should be unique",
        )
    }
}
