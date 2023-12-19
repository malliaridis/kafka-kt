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

import org.apache.kafka.common.utils.LogContext
import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.Matchers
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue

@Timeout(value = 40)
class TimelineHashSetTest {

    @Test
    fun testEmptySet() {
        val registry = SnapshotRegistry(LogContext())
        val set = TimelineHashSet<String>(registry, 1)
        assertTrue(set.isEmpty())
        assertEquals(0, set.size)
        set.clear()
        assertTrue(set.isEmpty())
    }

    @Test
    fun testNullsForbidden() {
        val registry = SnapshotRegistry(LogContext())
        val set = TimelineHashSet<String?>(registry, 1)
        assertFailsWith<NullPointerException> { set.add(null) }
    }

    @Test
    fun testIteration() {
        val registry = SnapshotRegistry(LogContext())
        val set = TimelineHashSet<String>(registry, 1)
        set.add("a")
        set.add("b")
        set.add("c")
        set.add("d")
        assertTrue(set.retainAll(mutableListOf("a", "b", "c")))
        assertFalse(set.retainAll(mutableListOf("a", "b", "c")))
        assertFalse(set.removeAll(mutableListOf("d")))
        registry.getOrCreateSnapshot(2)
        assertTrue(set.removeAll(mutableListOf("c")))
        assertThat(
            set.iterator(2).asSequence().toList(),
            Matchers.containsInAnyOrder("a", "b", "c")
        )
        assertThat(
            set.iterator().asSequence().toList(),
            Matchers.containsInAnyOrder("a", "b")
        )
        assertEquals(2, set.size)
        assertEquals(3, set.size(2))
        set.clear()
        assertTrue(set.isEmpty())
        assertFalse(set.isEmpty(2))
    }

    @Test
    @Disabled("Kotlin Migration - toArray not supported in Kotlin.")
    fun testToArray() {
//        val registry = SnapshotRegistry(LogContext())
//        val set = TimelineHashSet<String>(registry, 1)
//        set.add("z")
//        assertContentEquals(arrayOf("z"), set.toArray())
//        assertContentEquals(arrayOf("z", null), set.toArray(arrayOfNulls<String?>(2)))
//        assertContentEquals(arrayOf("z"), set.toArray())
    }

    @Test
    fun testSetMethods() {
        val registry = SnapshotRegistry(LogContext())
        val set = TimelineHashSet<String>(registry, 1)
        assertTrue(set.add("xyz"))
        assertFalse(set.add("xyz"))
        assertTrue(set.remove("xyz"))
        assertFalse(set.remove("xyz"))
        assertTrue(set.addAll(mutableListOf("abc", "def", "ghi")))
        assertFalse(set.addAll(mutableListOf("abc", "def", "ghi")))
        assertTrue(set.addAll(mutableListOf("abc", "def", "ghi", "jkl")))
        assertTrue(set.containsAll(mutableListOf("def", "jkl")))
        assertFalse(set.containsAll(mutableListOf("abc", "def", "xyz")))
        assertTrue(set.removeAll(mutableListOf("def", "ghi", "xyz")))
        registry.getOrCreateSnapshot(5)
        assertThat(
            set.iterator(5).asSequence().toList(),
            Matchers.containsInAnyOrder("abc", "jkl"),
        )
        assertThat(
            set.iterator().asSequence().toList(),
            Matchers.containsInAnyOrder("abc", "jkl")
        )
        set.removeIf { e: String -> e.startsWith("a") }
        assertThat(
            set.iterator().asSequence().toList(),
            Matchers.containsInAnyOrder("jkl")
        )
        assertThat(
            set.iterator(5).asSequence().toList(),
            Matchers.containsInAnyOrder("abc", "jkl")
        )
    }
}
