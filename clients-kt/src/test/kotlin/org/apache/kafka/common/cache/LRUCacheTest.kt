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

package org.apache.kafka.common.cache

import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue

class LRUCacheTest {

    @Test
    fun testPutGet() {
        val cache: Cache<String, String> = LRUCache(4)
        cache.put("a", "b")
        cache.put("c", "d")
        cache.put("e", "f")
        cache.put("g", "h")
        assertEquals(4, cache.size())
        assertEquals("b", cache["a"])
        assertEquals("d", cache["c"])
        assertEquals("f", cache["e"])
        assertEquals("h", cache["g"])
    }

    @Test
    fun testRemove() {
        val cache: Cache<String, String> = LRUCache(4)
        cache.put("a", "b")
        cache.put("c", "d")
        cache.put("e", "f")
        assertEquals(3, cache.size())

        assertTrue(cache.remove("a"))
        assertEquals(2, cache.size())
        assertNull(cache["a"])
        assertEquals("d", cache["c"])
        assertEquals("f", cache["e"])

        assertFalse(cache.remove("key-does-not-exist"))

        assertTrue(cache.remove("c"))
        assertEquals(1, cache.size())
        assertNull(cache["c"])
        assertEquals("f", cache["e"])

        assertTrue(cache.remove("e"))
        assertEquals(0, cache.size())
        assertNull(cache["e"])
    }

    @Test
    fun testEviction() {
        val cache: Cache<String, String> = LRUCache(2)
        cache.put("a", "b")
        cache.put("c", "d")
        assertEquals(2, cache.size())
        cache.put("e", "f")
        assertEquals(2, cache.size())
        assertNull(cache["a"])
        assertEquals("d", cache["c"])
        assertEquals("f", cache["e"])

        // Validate correct access order eviction
        cache["c"]
        cache.put("g", "h")
        assertEquals(2, cache.size())
        assertNull(cache["e"])
        assertEquals("d", cache["c"])
        assertEquals("h", cache["g"])
    }
}
