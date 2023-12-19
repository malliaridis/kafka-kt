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

package org.apache.kafka.server.utils

import java.util.AbstractMap.SimpleImmutableEntry
import java.util.TreeMap
import org.apache.kafka.server.util.TranslatedValueMapView
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue

@Timeout(value = 60)
class TranslatedValueMapViewTest {

    @Test
    fun testContains() {
        val underlying = createTestMap()
        val view = TranslatedValueMapView(underlying) { it.toString() }
        assertTrue(view.containsKey("foo"))
        assertTrue(view.containsKey("bar"))
        assertTrue(view.containsKey("baz"))
        assertFalse(view.containsKey("quux"))
        underlying["quux"] = 101
        assertTrue(view.containsKey("quux"))
    }

    @Test
    fun testIsEmptyAndSize() {
        val underlying = mutableMapOf<String, Int>()
        val view = TranslatedValueMapView(underlying) { it.toString() }
        assertTrue(view.isEmpty())
        assertEquals(0, view.size)
        underlying["quux"] = 101
        assertFalse(view.isEmpty())
        assertEquals(1, view.size)
    }

    @Test
    fun testGet() {
        val underlying = createTestMap()
        val view = TranslatedValueMapView(underlying) { it.toString() }
        assertEquals("2", view["foo"])
        assertEquals("3", view["bar"])
        assertEquals("5", view["baz"])
        assertNull(view["quux"])
        underlying["quux"] = 101
        assertEquals("101", view["quux"])
    }

    @Test
    fun testEntrySet() {
        val underlying: Map<String, Int> = createTestMap()
        val view = TranslatedValueMapView(underlying) { it.toString() }
        assertEquals(3, view.entries.size)
        assertFalse(view.entries.isEmpty())
        assertTrue(view.entries.contains(SimpleImmutableEntry("foo", "2")))
        assertFalse(view.entries.contains(SimpleImmutableEntry("bar", "4")))
    }

    @Test
    fun testEntrySetIterator() {
        val underlying: Map<String, Int> = createTestMap()
        val view = TranslatedValueMapView(underlying) { it.toString() }
        val iterator = view.entries.iterator()
        assertTrue(iterator.hasNext())
        assertEquals(SimpleImmutableEntry("bar", "3"), iterator.next())
        assertTrue(iterator.hasNext())
        assertEquals(SimpleImmutableEntry("baz", "5"), iterator.next())
        assertTrue(iterator.hasNext())
        assertEquals(SimpleImmutableEntry("foo", "2"), iterator.next())
        assertFalse(iterator.hasNext())
    }

    companion object {

        private fun createTestMap(): MutableMap<String, Int> {
            val testMap: MutableMap<String, Int> = TreeMap()
            testMap["foo"] = 2
            testMap["bar"] = 3
            testMap["baz"] = 5
            return testMap
        }
    }
}
