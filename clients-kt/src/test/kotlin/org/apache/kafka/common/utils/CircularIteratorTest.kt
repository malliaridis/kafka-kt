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

package org.apache.kafka.common.utils

import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull
import kotlin.test.assertTrue

class CircularIteratorTest {

    @Test
    @Disabled("Kotlin Migration: CircularIterator does not allow null collections.")
    fun testNullCollection() {
        // assertFailsWith<NullPointerException> { CircularIterator<Any>(null) }
    }

    @Test
    fun testEmptyCollection() {
        assertFailsWith<IllegalArgumentException> { CircularIterator(emptyList<Any>()) }
    }

    @Test
    fun testCycleCollection() {
        val it = CircularIterator(mutableListOf("A", "B", null, "C"))
        assertEquals("A", it.peek())
        assertTrue(it.hasNext())
        assertEquals("A", it.next())
        assertEquals("B", it.peek())
        assertTrue(it.hasNext())
        assertEquals("B", it.next())
        assertNull(it.peek())
        assertTrue(it.hasNext())
        assertNull(it.next())
        assertEquals("C", it.peek())
        assertTrue(it.hasNext())
        assertEquals("C", it.next())
        assertEquals("A", it.peek())
        assertTrue(it.hasNext())
        assertEquals("A", it.next())
        assertEquals("B", it.peek())

        // Check that peek does not have any side-effects
        assertEquals("B", it.peek())
    }
}
