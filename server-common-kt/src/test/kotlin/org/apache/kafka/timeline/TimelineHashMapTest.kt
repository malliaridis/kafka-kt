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

package org.apache.kafka.timeline

import java.util.Collections
import org.apache.kafka.common.utils.LogContext
import org.hamcrest.MatcherAssert
import org.hamcrest.Matchers
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNotEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

@Timeout(value = 40)
class TimelineHashMapTest {

    @Test
    fun testEmptyMap() {
        val registry = SnapshotRegistry(LogContext())
        val map = TimelineHashMap<Int, String>(registry, 1)
        assertTrue(map.isEmpty())
        assertEquals(0, map.size)
        map.clear()
        assertTrue(map.isEmpty())
    }

    @Test
    @Disabled("Kotlin Migration - Generic types cannot be null in Kotlin.")
    fun testNullsForbidden() {
//        val registry = SnapshotRegistry(LogContext())
//        val map = TimelineHashMap<String, Boolean>(registry, 1)
//        assertFailsWith<NullPointerException> { map[null] = true }
//        assertFailsWith<NullPointerException> { map["abc"] = null }
//        assertFailsWith<NullPointerException> { map[null] = null }
    }

    @Test
    fun testIteration() {
        val registry = SnapshotRegistry(LogContext())
        val map = TimelineHashMap<Int, String>(registry, 1)
        map[123] = "abc"
        map[456] = "def"
        MatcherAssert.assertThat(map.keys.iterator().asSequence().toList(), Matchers.containsInAnyOrder(123, 456))
        MatcherAssert.assertThat(map.values.iterator().asSequence().toList(), Matchers.containsInAnyOrder("abc", "def"))
        assertTrue(map.containsValue("abc"))
        assertTrue(map.containsKey(456))
        assertFalse(map.isEmpty())
        registry.getOrCreateSnapshot(2)
        val iter: Iterator<Map.Entry<Int, String>> = map.entries(2).iterator()
        map.clear()
        val snapshotValues: MutableList<String> = ArrayList()
        snapshotValues.add(iter.next().value)
        snapshotValues.add(iter.next().value)
        assertFalse(iter.hasNext())
        MatcherAssert.assertThat(snapshotValues, Matchers.containsInAnyOrder("abc", "def"))
        assertFalse(map.isEmpty(2))
        assertTrue(map.isEmpty())
    }

    @Test
    fun testMapMethods() {
        val registry = SnapshotRegistry(LogContext())
        val map = TimelineHashMap<Int, String>(registry, 1)
        assertNull(map.putIfAbsent(1, "xyz"))
        assertEquals("xyz", map.putIfAbsent(1, "123"))
        assertEquals("xyz", map.putIfAbsent(1, "ghi"))
        map.putAll(Collections.singletonMap(2, "b"))
        assertTrue(map.containsKey(2))
        assertEquals("xyz", map.remove(1))
        assertEquals("b", map.remove(2))
    }

    @Test
    fun testMapEquals() {
        val registry = SnapshotRegistry(LogContext())
        val map1 = TimelineHashMap<Int, String>(registry, 1)
        assertNull(map1.putIfAbsent(1, "xyz"))
        assertNull(map1.putIfAbsent(2, "abc"))
        val map2 = TimelineHashMap<Int, String>(registry, 1)
        assertNull(map2.putIfAbsent(1, "xyz"))
        assertNotEquals(map1, map2)
        assertNull(map2.putIfAbsent(2, "abc"))
        assertEquals(map1, map2)
    }

    companion object {

        @Deprecated(
            message = "Replace with Iterator.toList()",
            replaceWith = ReplaceWith("iter.asSequence().toList()"),
        )
        fun <T> iteratorToList(iter: Iterator<T>): List<T> {
            val list =  mutableListOf<T>()
            while (iter.hasNext()) list.add(iter.next())

            return list
        }
    }
}
