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

package org.apache.kafka.server.mutable

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue

@Timeout(120)
class BoundedListTest {

    @Test
    fun testMaxLengthMustNotBeZero() {
        val message = assertFailsWith<IllegalArgumentException> {
            BoundedList(0, ArrayList<Int>())
        }.message
        assertEquals("Invalid non-positive maxLength of 0", message)
    }

    @Test
    fun testMaxLengthMustNotBeNegative() {
        val message = assertFailsWith<IllegalArgumentException> {
            BoundedList(maxLength = -123, underlying = mutableListOf<Int>())
        }.message
        assertEquals("Invalid non-positive maxLength of -123", message)
    }

    @Test
    fun testOwnedListMustNotBeTooLong() {
        val message = assertFailsWith<BoundedListTooLongException> {
            BoundedList(1, mutableListOf(1, 2))
        }.message
        assertEquals("Cannot wrap list, because it is longer than the maximum length 1", message)
    }

    @Test
    fun testAddingToBoundedList() {
        val list = BoundedList(2, ArrayList<Int>(3))
        assertEquals(0, list.size)
        assertTrue(list.isEmpty())
        assertTrue(list.add(456))
        assertTrue(list.contains(456))
        assertEquals(1, list.size)
        assertFalse(list.isEmpty())
        assertTrue(list.add(789))
        val message = assertFailsWith<BoundedListTooLongException> { list.add(912) }.message
        assertEquals(
            "Cannot add another element to the list because it would exceed the maximum length of 2",
            message,
        )
        val message2 = assertFailsWith<BoundedListTooLongException> { list.add(0, 912) }.message
        assertEquals(
            "Cannot add another element to the list because it would exceed the maximum length of 2",
            message2,
        )
    }

    private fun <E> testHashCodeAndEquals(list: MutableList<E>) {
        assertEquals(list, BoundedList(maxLength = 123, underlying = list))
        assertEquals(list.hashCode(), BoundedList(maxLength = 123, underlying = list).hashCode())
    }

    @Test
    fun testHashCodeAndEqualsForEmptyList() {
        testHashCodeAndEquals(mutableListOf<Any>())
    }

    @Test
    fun testHashCodeAndEqualsForNonEmptyList() {
        testHashCodeAndEquals(mutableListOf(1, 2, 3, 4, 5, 6, 7))
    }

    @Test
    fun testSet() {
        val underlying = mutableListOf(1, 2, 3)
        val list = BoundedList(maxLength = 3, underlying = underlying)
        list[1] = 200
        assertEquals(mutableListOf(1, 200, 3), list)
    }

    @Test
    fun testRemove() {
        val underlying = mutableListOf("a", "a", "c")
        val list = BoundedList(maxLength = 3, underlying = underlying)
        assertEquals(0, list.indexOf("a"))
        assertEquals(1, list.lastIndexOf("a"))
        list.remove("a")
        assertEquals(mutableListOf("a", "c"), list)
        list.removeAt(0)
        assertEquals(mutableListOf("c"), list)
    }

    @Test
    fun testClear() {
        val underlying = mutableListOf("a", "b", "c")
        val list = BoundedList(maxLength = 3, underlying = underlying)
        list.clear()
        assertEquals(mutableListOf(), list)
        assertTrue(list.isEmpty())
    }

    @Test
    fun testGet() {
        val list = BoundedList(3, mutableListOf(1, 2, 3))
        assertEquals(2, list[1])
    }

    @Test
    fun testToArray() {
        val list = BoundedList(maxLength = 3, underlying = mutableListOf(1, 2, 3))
        assertContentEquals(arrayOf(1, 2, 3), list.toTypedArray())
        assertContentEquals(arrayOf(1, 2, 3), list.toTypedArray())
    }

    @Test
    fun testAddAll() {
        val underlying = mutableListOf("a", "b", "c")
        val list = BoundedList(maxLength = 5, underlying = underlying)
        val message =  assertFailsWith<BoundedListTooLongException> { list.addAll(listOf("d", "e", "f")) }.message
        assertEquals(
            "Cannot add another 3 element(s) to the list because it would exceed the maximum length of 5",
            message,
        )
        val message2 = assertFailsWith<BoundedListTooLongException> {
            list.addAll(0, listOf("d", "e", "f"))
        }.message
        assertEquals(
            "Cannot add another 3 element(s) to the list because it would exceed the maximum length of 5",
            message2,
        )
        list.addAll(listOf("d", "e"))
        assertEquals(listOf("a", "b", "c", "d", "e"), list)
    }

    @Test
    fun testIterator() {
        val list = BoundedList(maxLength = 3, underlying = mutableListOf(1, 2, 3))
        assertEquals(1, list.iterator().next())
        assertEquals(1, list.listIterator().next())
        assertEquals(3, list.listIterator(2).next())
        assertFalse(list.listIterator(3).hasNext())
    }

    @Test
    fun testIteratorIsImmutable() {
        val list = BoundedList(maxLength = 3, underlying = mutableListOf(1, 2, 3))
        assertFailsWith<UnsupportedOperationException> { list.iterator().remove() }
        assertFailsWith<UnsupportedOperationException> { list.listIterator().remove() }
    }

    @Test
    fun testSubList() {
        val list = BoundedList(maxLength = 3, underlying = mutableListOf(1, 2, 3))
        assertEquals(mutableListOf(2), list.subList(1, 2))
        assertFailsWith<UnsupportedOperationException> { list.subList(1, 2).removeAt(2) }
    }
}
