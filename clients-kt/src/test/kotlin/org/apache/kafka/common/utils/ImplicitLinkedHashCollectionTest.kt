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
import org.junit.jupiter.api.Timeout
import kotlin.random.Random
import kotlin.test.assertContains
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNotEquals
import kotlin.test.assertNull
import kotlin.test.assertSame
import kotlin.test.assertTrue

/**
 * A unit test for ImplicitLinkedHashCollection.
 */
@Timeout(120)
class ImplicitLinkedHashCollectionTest {

    class TestElement : ImplicitLinkedHashCollection.Element {

        private var prev = ImplicitLinkedHashCollection.INVALID_INDEX

        private var next = ImplicitLinkedHashCollection.INVALID_INDEX

        internal val key: Int

        internal val value: Int

        constructor(key: Int) {
            this.key = key
            value = 0
        }

        constructor(key: Int, value: Int) {
            this.key = key
            this.value = value
        }

        override fun prev(): Int = prev

        override fun setPrev(prev: Int) {
            this.prev = prev
        }

        override fun next(): Int = next

        override fun setNext(next: Int) {
            this.next = next
        }

        override fun elementKeysAreEqual(other: Any?): Boolean {
            return if (this === other) true
            else if (other == null || other.javaClass != TestElement::class.java) false
            else key == (other as TestElement).key
        }

        override fun equals(other: Any?): Boolean {
            if (this === other) return true
            if (other == null || other.javaClass != TestElement::class.java) return false
            val that = other as TestElement
            return key == that.key && value == that.value
        }

        override fun toString(): String = "TestElement(key=$key, val=$value)"

        override fun hashCode(): Int {
            val hashCode = 2654435761L * key
            return (hashCode shr 32).toInt()
        }
    }

    @Test
    @Disabled("Kotlin Migration: Element is non-nullable and therefore forbidden by default in Kotlin.")
    fun testNullForbidden() {
//        val multiColl = ImplicitLinkedHashMultiCollection<TestElement>()
//        assertFalse(multiColl.add(null))
    }

    @Test
    fun testInsertDelete() {
        val coll = ImplicitLinkedHashCollection<TestElement>(100)
        assertTrue(coll.add(TestElement(1)))
        val second = TestElement(2)
        assertTrue(coll.add(second))
        assertTrue(coll.add(TestElement(3)))
        assertFalse(coll.add(TestElement(3)))
        assertEquals(3, coll.size)
        assertTrue(coll.contains(TestElement(1)))
        assertFalse(coll.contains(TestElement(4)))
        val secondAgain = coll.find(TestElement(2))
        assertSame(second, secondAgain)
        assertTrue(coll.remove(TestElement(1)))
        assertFalse(coll.remove(TestElement(1)))
        assertEquals(2, coll.size)
        coll.clear()
        assertEquals(0, coll.size)
    }

    @Test
    fun testTraversal() {
        val coll = ImplicitLinkedHashCollection<TestElement>()
        expectTraversal(coll.iterator())
        assertTrue(coll.add(TestElement(2)))
        expectTraversal(coll.iterator(), 2)
        assertTrue(coll.add(TestElement(1)))
        expectTraversal(coll.iterator(), 2, 1)
        assertTrue(coll.add(TestElement(100)))
        expectTraversal(coll.iterator(), 2, 1, 100)
        assertTrue(coll.remove(TestElement(1)))
        expectTraversal(coll.iterator(), 2, 100)
        assertTrue(coll.add(TestElement(1)))
        expectTraversal(coll.iterator(), 2, 100, 1)
        val iter = coll.iterator()
        iter.next()
        iter.next()
        iter.remove()
        iter.next()
        assertFalse(iter.hasNext())
        expectTraversal(coll.iterator(), 2, 1)
        val list: MutableList<TestElement?> = ArrayList()
        list.add(TestElement(1))
        list.add(TestElement(2))
        assertTrue(coll.removeAll(list.toSet()))
        assertFalse(coll.removeAll(list.toSet()))
        expectTraversal(coll.iterator())
        assertEquals(0, coll.size)
        assertTrue(coll.isEmpty())
    }

    @Test
    fun testSetViewGet() {
        val coll = ImplicitLinkedHashCollection<TestElement>()
        coll.add(TestElement(1))
        coll.add(TestElement(2))
        coll.add(TestElement(3))
        val set = coll.valuesSet()
        assertTrue(set.contains(TestElement(1)))
        assertTrue(set.contains(TestElement(2)))
        assertTrue(set.contains(TestElement(3)))
        assertEquals(3, set.size)
    }

    @Test
    fun testSetViewModification() {
        val coll = ImplicitLinkedHashCollection<TestElement>()
        coll.add(TestElement(1))
        coll.add(TestElement(2))
        coll.add(TestElement(3))

        // Removal from set is reflected in collection
        val set = coll.valuesSet()
        set.remove(TestElement(1))
        assertFalse(coll.contains(TestElement(1)))
        assertEquals(2, coll.size)

        // Addition to set is reflected in collection
        set.add(TestElement(4))
        assertTrue(coll.contains(TestElement(4)))
        assertEquals(3, coll.size)

        // Removal from collection is reflected in set
        coll.remove(TestElement(2))
        assertFalse(set.contains(TestElement(2)))
        assertEquals(2, set.size)

        // Addition to collection is reflected in set
        coll.add(TestElement(5))
        assertTrue(set.contains(TestElement(5)))
        assertEquals(3, set.size)

        // Ordering in the collection is maintained
        var key = 3
        for (e in coll) {
            assertEquals(key, e.key)
            ++key
        }
    }

    @Test
    fun testListViewGet() {
        val coll = ImplicitLinkedHashCollection<TestElement>()
        coll.add(TestElement(1))
        coll.add(TestElement(2))
        coll.add(TestElement(3))
        val list = coll.valuesList()
        assertEquals(1, list[0].key)
        assertEquals(2, list[1].key)
        assertEquals(3, list[2].key)
        assertEquals(3, list.size)
    }

    @Test
    fun testListViewModification() {
        val coll = ImplicitLinkedHashCollection<TestElement>()
        coll.add(TestElement(1))
        coll.add(TestElement(2))
        coll.add(TestElement(3))

        // Removal from list is reflected in collection
        val list = coll.valuesList()
        list.removeAt(1)
        assertTrue(coll.contains(TestElement(1)))
        assertFalse(coll.contains(TestElement(2)))
        assertTrue(coll.contains(TestElement(3)))
        assertEquals(2, coll.size)

        // Removal from collection is reflected in list
        coll.remove(TestElement(1))
        assertEquals(3, list[0].key)
        assertEquals(1, list.size)

        // Addition to collection is reflected in list
        coll.add(TestElement(4))
        assertEquals(3, list[0].key)
        assertEquals(4, list[1].key)
        assertEquals(2, list.size)
    }

    @Test
    fun testEmptyListIterator() {
        val coll = ImplicitLinkedHashCollection<TestElement>()
        val iter = coll.valuesList().listIterator()
        assertFalse(iter.hasNext())
        assertFalse(iter.hasPrevious())
        assertEquals(0, iter.nextIndex())
        assertEquals(-1, iter.previousIndex())
    }

    @Test
    fun testListIteratorCreation() {
        val coll = ImplicitLinkedHashCollection<TestElement>()
        coll.add(TestElement(1))
        coll.add(TestElement(2))
        coll.add(TestElement(3))

        // Iterator created at the start of the list should have a next but no prev
        var iter = coll.valuesList().listIterator()
        assertTrue(iter.hasNext())
        assertFalse(iter.hasPrevious())
        assertEquals(0, iter.nextIndex())
        assertEquals(-1, iter.previousIndex())

        // Iterator created in the middle of the list should have both a next and a prev
        iter = coll.valuesList().listIterator(2)
        assertTrue(iter.hasNext())
        assertTrue(iter.hasPrevious())
        assertEquals(2, iter.nextIndex())
        assertEquals(1, iter.previousIndex())

        // Iterator created at the end of the list should have a prev but no next
        iter = coll.valuesList().listIterator(3)
        assertFalse(iter.hasNext())
        assertTrue(iter.hasPrevious())
        assertEquals(3, iter.nextIndex())
        assertEquals(2, iter.previousIndex())
    }

    @Test
    fun testListIteratorTraversal() {
        val coll = ImplicitLinkedHashCollection<TestElement>()
        coll.add(TestElement(1))
        coll.add(TestElement(2))
        coll.add(TestElement(3))
        val iter = coll.valuesList().listIterator()

        // Step the iterator forward to the end of the list
        assertTrue(iter.hasNext())
        assertFalse(iter.hasPrevious())
        assertEquals(0, iter.nextIndex())
        assertEquals(-1, iter.previousIndex())
        assertEquals(1, iter.next().key)
        assertTrue(iter.hasNext())
        assertTrue(iter.hasPrevious())
        assertEquals(1, iter.nextIndex())
        assertEquals(0, iter.previousIndex())
        assertEquals(2, iter.next().key)
        assertTrue(iter.hasNext())
        assertTrue(iter.hasPrevious())
        assertEquals(2, iter.nextIndex())
        assertEquals(1, iter.previousIndex())
        assertEquals(3, iter.next().key)
        assertFalse(iter.hasNext())
        assertTrue(iter.hasPrevious())
        assertEquals(3, iter.nextIndex())
        assertEquals(2, iter.previousIndex())

        // Step back to the middle of the list
        assertEquals(3, iter.previous().key)
        assertTrue(iter.hasNext())
        assertTrue(iter.hasPrevious())
        assertEquals(2, iter.nextIndex())
        assertEquals(1, iter.previousIndex())
        assertEquals(2, iter.previous().key)
        assertTrue(iter.hasNext())
        assertTrue(iter.hasPrevious())
        assertEquals(1, iter.nextIndex())
        assertEquals(0, iter.previousIndex())

        // Step forward one and then back one, return value should remain the same
        assertEquals(2, iter.next().key)
        assertTrue(iter.hasNext())
        assertTrue(iter.hasPrevious())
        assertEquals(2, iter.nextIndex())
        assertEquals(1, iter.previousIndex())
        assertEquals(2, iter.previous().key)
        assertTrue(iter.hasNext())
        assertTrue(iter.hasPrevious())
        assertEquals(1, iter.nextIndex())
        assertEquals(0, iter.previousIndex())

        // Step back to the front of the list
        assertEquals(1, iter.previous().key)
        assertTrue(iter.hasNext())
        assertFalse(iter.hasPrevious())
        assertEquals(0, iter.nextIndex())
        assertEquals(-1, iter.previousIndex())
    }

    @Test
    fun testListIteratorRemove() {
        val coll = ImplicitLinkedHashCollection<TestElement>()
        coll.add(TestElement(1))
        coll.add(TestElement(2))
        coll.add(TestElement(3))
        coll.add(TestElement(4))
        coll.add(TestElement(5))
        val iter = coll.valuesList().toMutableList().listIterator()
        assertFailsWith<IllegalStateException>(
            message = "Calling remove() without calling next() or previous() should raise an exception",
        ) { iter.remove() }

        // Remove after next()
        iter.next()
        iter.next()
        iter.next()
        iter.remove()
        assertTrue(iter.hasNext())
        assertTrue(iter.hasPrevious())
        assertEquals(2, iter.nextIndex())
        assertEquals(1, iter.previousIndex())
        assertFailsWith<IllegalStateException>(
            "Calling remove() twice without calling next() or previous() in between should raise an exception",
        ) { iter.remove() }

        // Remove after previous()
        assertEquals(2, iter.previous().key)
        iter.remove()
        assertTrue(iter.hasNext())
        assertTrue(iter.hasPrevious())
        assertEquals(1, iter.nextIndex())
        assertEquals(0, iter.previousIndex())

        // Remove the first element of the list
        assertEquals(1, iter.previous().key)
        iter.remove()
        assertTrue(iter.hasNext())
        assertFalse(iter.hasPrevious())
        assertEquals(0, iter.nextIndex())
        assertEquals(-1, iter.previousIndex())

        // Remove the last element of the list
        assertEquals(4, iter.next().key)
        assertEquals(5, iter.next().key)
        iter.remove()
        assertFalse(iter.hasNext())
        assertTrue(iter.hasPrevious())
        assertEquals(1, iter.nextIndex())
        assertEquals(0, iter.previousIndex())

        // Remove the final remaining element of the list
        assertEquals(4, iter.previous().key)
        iter.remove()
        assertFalse(iter.hasNext())
        assertFalse(iter.hasPrevious())
        assertEquals(0, iter.nextIndex())
        assertEquals(-1, iter.previousIndex())
    }

    @Test
    fun testCollisions() {
        val coll = ImplicitLinkedHashCollection<TestElement>(5)
        assertEquals(11, coll.numSlots())
        assertTrue(coll.add(TestElement(11)))
        assertTrue(coll.add(TestElement(0)))
        assertTrue(coll.add(TestElement(22)))
        assertTrue(coll.add(TestElement(33)))
        assertEquals(11, coll.numSlots())
        expectTraversal(coll.iterator(), 11, 0, 22, 33)
        assertTrue(coll.remove(TestElement(22)))
        expectTraversal(coll.iterator(), 11, 0, 33)
        assertEquals(3, coll.size)
        assertFalse(coll.isEmpty())
    }

    @Test
    fun testEnlargement() {
        val coll = ImplicitLinkedHashCollection<TestElement>(5)
        assertEquals(11, coll.numSlots())
        for (i in 0..5) {
            assertTrue(coll.add(TestElement(i)))
        }
        assertEquals(23, coll.numSlots())
        assertEquals(6, coll.size)
        expectTraversal(coll.iterator(), 0, 1, 2, 3, 4, 5)
        for (i in 0..5) assertContains(coll, TestElement(i), "Failed to find element $i")
        coll.remove(TestElement(3))
        assertEquals(23, coll.numSlots())
        assertEquals(5, coll.size)
        expectTraversal(coll.iterator(), 0, 1, 2, 4, 5)
    }

    @Test
    fun testManyInsertsAndDeletes() {
        val random = Random(123)
        val existing = LinkedHashSet<Int>()
        val coll = ImplicitLinkedHashCollection<TestElement>()
        for (i in 0..99) {
            addRandomElement(random, existing, coll)
            addRandomElement(random, existing, coll)
            addRandomElement(random, existing, coll)
            removeRandomElement(random, existing)
            expectTraversal(coll.iterator(), existing.iterator())
        }
    }

    @Test
    fun testInsertingTheSameObjectMultipleTimes() {
        val coll = ImplicitLinkedHashCollection<TestElement>()
        val element = TestElement(123)
        assertTrue(coll.add(element))
        assertFalse(coll.add(element))
        assertFalse(coll.add(element))
        assertTrue(coll.remove(element))
        assertFalse(coll.remove(element))
        assertTrue(coll.add(element))
        assertFalse(coll.add(element))
    }

    @Test
    fun testEquals() {
        val coll1 = ImplicitLinkedHashCollection<TestElement>()
        coll1.add(TestElement(1))
        coll1.add(TestElement(2))
        coll1.add(TestElement(3))
        val coll2 = ImplicitLinkedHashCollection<TestElement>()
        coll2.add(TestElement(1))
        coll2.add(TestElement(2))
        coll2.add(TestElement(3))
        val coll3 = ImplicitLinkedHashCollection<TestElement>()
        coll3.add(TestElement(1))
        coll3.add(TestElement(3))
        coll3.add(TestElement(2))
        assertEquals(coll1, coll2)
        assertNotEquals(coll1, coll3)
        assertNotEquals(coll2, coll3)
    }

    @Test
    fun testFindContainsRemoveOnEmptyCollection() {
        val coll = ImplicitLinkedHashCollection<TestElement>()
        assertNull(coll.find(TestElement(2)))
        assertFalse(coll.contains(TestElement(2)))
        assertFalse(coll.remove(TestElement(2)))
    }

    private fun addRandomElement(
        random: Random,
        existing: LinkedHashSet<Int>,
        set: ImplicitLinkedHashCollection<TestElement>,
    ) {
        var next: Int
        do {
            next = random.nextInt()
        } while (existing.contains(next))

        existing.add(next)
        set.add(TestElement(next))
    }

    private fun removeRandomElement(random: Random, existing: MutableCollection<*>) {
        val removeIdx = random.nextInt(existing.size)
        val iter = existing.iterator()
        var element: Int? = null
        for (i in 0..removeIdx) {
            element = iter.next() as Int
        }
        existing.remove(TestElement(element!!))
    }

    @Test
    fun testSameKeysDifferentValues() {
        val coll = ImplicitLinkedHashCollection<TestElement>()
        assertTrue(coll.add(TestElement(1, 1)))
        assertFalse(coll.add(TestElement(1, 2)))
        val element2 = TestElement(1, 2)
        val element1 = coll.find(element2)
        assertNotEquals(element2, element1)
        assertTrue(element2.elementKeysAreEqual(element1))
    }

    @Test
    fun testMoveToEnd() {
        val coll = ImplicitLinkedHashCollection<TestElement>()
        val e1 = TestElement(1, 1)
        val e2 = TestElement(2, 2)
        val e3 = TestElement(3, 3)
        assertTrue(coll.add(e1))
        assertTrue(coll.add(e2))
        assertTrue(coll.add(e3))
        coll.moveToEnd(e1)
        expectTraversal(coll.iterator(), 2, 3, 1)
        assertFailsWith<RuntimeException> { coll.moveToEnd(TestElement(4, 4)) }
    }

    @Test
    fun testRemovals() {
        val coll = ImplicitLinkedHashCollection<TestElement>()
        val elements: MutableList<TestElement> = ArrayList()
        repeat(100) { i ->
            val element = TestElement(i, i)
            elements.add(element)
            coll.add(element)
        }
        assertEquals(100, coll.size)
        val iter = coll.iterator()
        repeat(50) {
            iter.next()
            iter.remove()
        }
        assertEquals(50, coll.size)
        for (i in 50..99) {
            assertEquals(
                TestElement(i, i), coll.find(
                    elements[i]
                )
            )
        }
    }

    internal class TestElementComparator : Comparator<TestElement> {

        override fun compare(a: TestElement, b: TestElement): Int {
            return if (a.key < b.key) -1
            else if (a.key > b.key) 1
            else if (a.value < b.value) -1
            else if (a.value > b.value) 1
            else 0
        }

        companion object {
            val INSTANCE = TestElementComparator()
        }
    }

    internal class ReverseTestElementComparator : Comparator<TestElement> {

        override fun compare(a: TestElement, b: TestElement): Int =
            TestElementComparator.INSTANCE.compare(b, a)

        companion object {
            val INSTANCE = ReverseTestElementComparator()
        }
    }

    @Test
    fun testSort() {
        val coll = ImplicitLinkedHashCollection<TestElement>()
        coll.add(TestElement(3, 3))
        coll.add(TestElement(1, 1))
        coll.add(TestElement(10, 10))
        coll.add(TestElement(9, 9))
        coll.add(TestElement(2, 2))
        coll.add(TestElement(4, 4))
        coll.add(TestElement(0, 0))
        coll.add(TestElement(30, 30))
        coll.add(TestElement(20, 20))
        coll.add(TestElement(11, 11))
        coll.add(TestElement(15, 15))
        coll.add(TestElement(5, 5))
        expectTraversal(coll.iterator(), 3, 1, 10, 9, 2, 4, 0, 30, 20, 11, 15, 5)
        coll.sort(TestElementComparator.INSTANCE)
        expectTraversal(coll.iterator(), 0, 1, 2, 3, 4, 5, 9, 10, 11, 15, 20, 30)
        coll.sort(TestElementComparator.INSTANCE)
        expectTraversal(coll.iterator(), 0, 1, 2, 3, 4, 5, 9, 10, 11, 15, 20, 30)
        coll.sort(ReverseTestElementComparator.INSTANCE)
        expectTraversal(coll.iterator(), 30, 20, 15, 11, 10, 9, 5, 4, 3, 2, 1, 0)
    }

    companion object {
        fun expectTraversal(iterator: Iterator<TestElement?>, vararg sequence: Int?) {
            var i = 0
            while (iterator.hasNext()) {
                val element = iterator.next()
                assertTrue(
                    actual = i < sequence.size,
                    message = "Iterator yieled ${i + 1} elements, but only ${sequence.size} were expected.",
                )
                assertEquals(
                    expected = sequence[i],
                    actual = element!!.key,
                    message = "Iterator value number ${i + 1} was incorrect.",
                )
                i += 1
            }
            assertEquals(
                expected = i,
                actual = sequence.size,
                message = "Iterator yieled ${i + 1} elements, but ${sequence.size} were expected.",
            )
        }

        fun expectTraversal(iter: Iterator<TestElement?>, expectedIter: Iterator<Int?>) {
            var i = 0
            while (iter.hasNext()) {
                val element = iter.next()
                assertTrue(
                    actual = expectedIter.hasNext(),
                    message = "Iterator yieled ${i + 1} elements, but only $i were expected."
                )
                val expected = expectedIter.next()
                assertEquals(expected, element!!.key, "Iterator value number ${i + 1} was incorrect.")
                i += 1
            }
            assertFalse(
                actual = expectedIter.hasNext(),
                message = "Iterator yieled $i elements, but at least ${i + 1} were expected.",
            )
        }
    }
}
