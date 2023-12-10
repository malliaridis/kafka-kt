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

import java.util.LinkedList
import org.apache.kafka.common.utils.ImplicitLinkedHashCollectionTest.TestElement
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import kotlin.random.Random
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertSame
import kotlin.test.assertTrue
import kotlin.test.fail

/**
 * A unit test for ImplicitLinkedHashMultiCollection.
 */
@Timeout(120)
class ImplicitLinkedHashMultiCollectionTest {

    @Test
    @Disabled("Kotlin Migration: Element is not nullable and therefore null is forbidden by default.")
    fun testNullForbidden() {
//        val multiSet = ImplicitLinkedHashMultiCollection<TestElement>()
//        assertFalse(multiSet.add(null))
    }

    @Test
    fun testFindFindAllContainsRemoveOnEmptyCollection() {
        val coll = ImplicitLinkedHashMultiCollection<TestElement>()
        assertNull(coll.find(TestElement(2)))
        assertFalse(coll.contains(TestElement(2)))
        assertFalse(coll.remove(TestElement(2)))
        assertTrue(coll.findAll(TestElement(2)).isEmpty())
    }

    @Test
    fun testInsertDelete() {
        val multiSet = ImplicitLinkedHashMultiCollection<TestElement>(100)
        val e1 = TestElement(1)
        val e2 = TestElement(1)
        val e3 = TestElement(2)
        multiSet.mustAdd(e1)
        multiSet.mustAdd(e2)
        multiSet.mustAdd(e3)
        assertFalse(multiSet.add(e3))
        assertEquals(3, multiSet.size)
        expectExactTraversal(multiSet.findAll(e1).iterator(), e1, e2)
        expectExactTraversal(multiSet.findAll(e3).iterator(), e3)
        multiSet.remove(e2)
        expectExactTraversal(multiSet.findAll(e1).iterator(), e1)
        assertTrue(multiSet.contains(e2))
    }

    @Test
    fun testTraversal() {
        val multiSet = ImplicitLinkedHashMultiCollection<TestElement>()
        expectExactTraversal(multiSet.iterator())
        val e1 = TestElement(1)
        val e2 = TestElement(1)
        val e3 = TestElement(2)
        assertTrue(multiSet.add(e1))
        assertTrue(multiSet.add(e2))
        assertTrue(multiSet.add(e3))
        expectExactTraversal(multiSet.iterator(), e1, e2, e3)
        assertTrue(multiSet.remove(e2))
        expectExactTraversal(multiSet.iterator(), e1, e3)
        assertTrue(multiSet.remove(e1))
        expectExactTraversal(multiSet.iterator(), e3)
    }

    @Test
    fun testEnlargement() {
        val multiSet = ImplicitLinkedHashMultiCollection<TestElement>(5)
        assertEquals(11, multiSet.numSlots())
        val testElements = arrayOf(
            TestElement(100),
            TestElement(101),
            TestElement(102),
            TestElement(100),
            TestElement(101),
            TestElement(105)
        )
        for (i in testElements.indices) {
            assertTrue(multiSet.add(testElements[i]))
        }
        for (i in testElements.indices) {
            assertFalse(multiSet.add(testElements[i]))
        }
        assertEquals(23, multiSet.numSlots())
        assertEquals(testElements.size, multiSet.size)
        expectExactTraversal(multiSet.iterator(), *testElements)
        multiSet.remove(testElements[1])
        assertEquals(23, multiSet.numSlots())
        assertEquals(5, multiSet.size)
        expectExactTraversal(
            multiSet.iterator(),
            testElements[0], testElements[2], testElements[3], testElements[4], testElements[5]
        )
    }

    @Test
    fun testManyInsertsAndDeletes() {
        val random = Random(123)
        val existing = LinkedList<TestElement>()
        val multiSet = ImplicitLinkedHashMultiCollection<TestElement>()
        repeat(100) {
            repeat(4) {
                val testElement = TestElement(random.nextInt())
                multiSet.mustAdd(testElement)
                existing.add(testElement)
            }
            val elementToRemove = random.nextInt(multiSet.size)
            val iter1 = multiSet.iterator()
            val iter2 = existing.iterator()
            for (j in 0..elementToRemove) {
                iter1.next()
                iter2.next()
            }
            iter1.remove()
            iter2.remove()
            expectTraversal(multiSet.iterator(), existing.iterator())
        }
    }

    fun expectTraversal(iter: Iterator<TestElement>, expectedIter: Iterator<TestElement>) {
        var i = 0
        while (iter.hasNext()) {
            val element = iter.next()
            assertTrue(
                actual = expectedIter.hasNext(),
                message = "Iterator yieled ${i + 1} elements, but only $i were expected.",
            )
            val expected = expectedIter.next()
            assertSame(expected, element, "Iterator value number ${i + 1} was incorrect.")
            i += 1
        }
        assertFalse(
            actual = expectedIter.hasNext(),
            message = "Iterator yieled $i elements, but at least ${i + 1} were expected.",
        )
    }

    companion object {

        fun expectExactTraversal(iterator: Iterator<TestElement?>, vararg sequence: TestElement) {
            var i = 0
            while (iterator.hasNext()) {
                val element = iterator.next()
                assertTrue(
                    actual = i < sequence.size,
                    message = "Iterator yieled ${i + 1} elements, but only ${sequence.size} were expected.",
                )
                if (sequence[i] !== element) {
                    fail("Iterator value number ${i + 1} was incorrect.")
                }
                i += 1
            }
            assertEquals(
                expected = sequence.size,
                actual = i,
                message = "Iterator yieled ${i + 1} elements, but ${sequence.size} were expected.",
            )
        }
    }
}
