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

import java.util.IdentityHashMap
import org.apache.kafka.common.utils.LogContext
import org.apache.kafka.timeline.SnapshottableHashTable.ElementWithStartEpoch
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertSame
import kotlin.test.assertTrue

@Timeout(value = 40)
class SnapshottableHashTableTest {

    /**
     * The class of test elements.
     *
     * This class is intended to help test how the table handles distinct objects which
     * are equal to each other.  Therefore, for the purpose of hashing and equality, we
     * only check i here, and ignore j.
     */
    internal class TestElement(private val i: Int, private val j: Char) : ElementWithStartEpoch {

        private var startEpoch = Long.MAX_VALUE

        override fun setStartEpoch(startEpoch: Long) {
            this.startEpoch = startEpoch
        }

        override fun startEpoch(): Long = startEpoch

        override fun hashCode(): Int = i

        override fun equals(other: Any?): Boolean {
            if (other !is TestElement) return false
            return other.i == i
        }

        override fun toString(): String {
            return String.format("E_%d%c(%s)", i, j, System.identityHashCode(this))
        }
    }

    @Test
    fun testEmptyTable() {
        val registry = SnapshotRegistry(LogContext())
        val table = SnapshottableHashTable<TestElement>(registry, 1)
        assertEquals(0, table.snapshottableSize(Long.MAX_VALUE))
    }

    @Test
    fun testDeleteOnEmptyDeltaTable() {
        // A simple test case to validate the behavior of the TimelineHashSet
        // when the deltaTable for a snapshot is null
        val registry = SnapshotRegistry(LogContext())
        val set = TimelineHashSet<String>(registry, 5)
        registry.getOrCreateSnapshot(100)
        set.add("bar")
        registry.getOrCreateSnapshot(200)
        set.add("baz")

        // The deltatable of epoch 200 is null, it should not throw exception while reverting (deltatable merge)
        registry.revertToSnapshot(100)
        assertTrue(set.isEmpty())
        set.add("foo")
        registry.getOrCreateSnapshot(300)
        // After reverting to epoch 100, "bar" is not existed anymore
        set.remove("bar")
        // No deltatable merging is needed because nothing change in snapshot epoch 300
        registry.revertToSnapshot(100)
        assertTrue(set.isEmpty())
        set.add("qux")
        registry.getOrCreateSnapshot(400)
        assertEquals(1, set.size)
        set.add("fred")
        set.add("thud")
        registry.getOrCreateSnapshot(500)
        assertEquals(3, set.size)

        // remove the value in epoch 101(after epoch 100), it'll create an entry in deltatable in the snapshot of epoch 500 for the deleted value in epoch 101
        set.remove("qux")
        assertEquals(2, set.size)
        // When reverting to snapshot of epoch 400, we'll merge the deltatable in epoch 500 with the one in epoch 400.
        // The deltatable in epoch 500 has an entry created above, but the deltatable in epoch 400 is null.
        // It should not throw exception while reverting (deltatable merge)
        registry.revertToSnapshot(400)
        // After reverting, the deltatable in epoch 500 should merge to the current epoch
        assertEquals(1, set.size)

        // When reverting to epoch 100, the deltatable in epoch 400 won't be merged because the entry change is epoch 101(after epoch 100)
        registry.revertToSnapshot(100)
        assertTrue(set.isEmpty())
    }

    @Test
    fun testAddAndRemove() {
        val registry = SnapshotRegistry(LogContext())
        val table = SnapshottableHashTable<TestElement>(registry, 1)
        assertNull(table.snapshottableAddOrReplace(E_1B))
        assertEquals(1, table.snapshottableSize(Long.MAX_VALUE))
        registry.getOrCreateSnapshot(0)
        assertSame(E_1B, table.snapshottableAddOrReplace(E_1A))
        assertSame(E_1B, table.snapshottableGet(E_1A, 0))
        assertSame(E_1A, table.snapshottableGet(E_1A, Long.MAX_VALUE))
        assertNull(table.snapshottableAddOrReplace(E_2A))
        assertNull(table.snapshottableAddOrReplace(E_3A))
        assertEquals(3, table.snapshottableSize(Long.MAX_VALUE))
        assertEquals(1, table.snapshottableSize(0))
        registry.getOrCreateSnapshot(1)
        assertEquals(E_1A, table.snapshottableRemove(E_1B))
        assertEquals(E_2A, table.snapshottableRemove(E_2A))
        assertEquals(E_3A, table.snapshottableRemove(E_3A))
        assertEquals(0, table.snapshottableSize(Long.MAX_VALUE))
        assertEquals(1, table.snapshottableSize(0))
        assertEquals(3, table.snapshottableSize(1))
        registry.deleteSnapshot(0)
        val message = assertFailsWith<RuntimeException> { table.snapshottableSize(0) }.message
        assertEquals("No in-memory snapshot for epoch 0. Snapshot epochs are: 1", message)
        registry.deleteSnapshot(1)
        assertEquals(0, table.snapshottableSize(Long.MAX_VALUE))
    }

    @Test
    fun testIterateOverSnapshot() {
        val registry = SnapshotRegistry(LogContext())
        val table = SnapshottableHashTable<TestElement>(registry, 1)
        assertTrue(table.snapshottableAddUnlessPresent(E_1B))
        assertFalse(table.snapshottableAddUnlessPresent(E_1A))
        assertTrue(table.snapshottableAddUnlessPresent(E_2A))
        assertTrue(table.snapshottableAddUnlessPresent(E_3A))
        registry.getOrCreateSnapshot(0)
        assertIteratorYields(table.snapshottableIterator(0), E_1B, E_2A, E_3A)
        assertEquals(E_1B, table.snapshottableRemove(E_1B))
        assertIteratorYields(table.snapshottableIterator(0), E_1B, E_2A, E_3A)
        assertNull(table.snapshottableRemove(E_1A))
        assertIteratorYields(table.snapshottableIterator(Long.MAX_VALUE), E_2A, E_3A)
        assertEquals(E_2A, table.snapshottableRemove(E_2A))
        assertEquals(E_3A, table.snapshottableRemove(E_3A))
        assertIteratorYields(table.snapshottableIterator(0), E_1B, E_2A, E_3A)
    }

    @Test
    fun testIterateOverSnapshotWhileExpandingTable() {
        val registry = SnapshotRegistry(LogContext())
        val table = SnapshottableHashTable<TestElement>(registry, 1)
        assertNull(table.snapshottableAddOrReplace(E_1A))
        registry.getOrCreateSnapshot(0)
        val iter: Iterator<TestElement> = table.snapshottableIterator(0)
        assertTrue(table.snapshottableAddUnlessPresent(E_2A))
        assertTrue(table.snapshottableAddUnlessPresent(E_3A))
        assertIteratorYields(iter, E_1A)
    }

    @Test
    fun testIterateOverSnapshotWhileDeletingAndReplacing() {
        val registry = SnapshotRegistry(LogContext())
        val table = SnapshottableHashTable<TestElement>(registry, 1)
        assertNull(table.snapshottableAddOrReplace(E_1A))
        assertNull(table.snapshottableAddOrReplace(E_2A))
        assertNull(table.snapshottableAddOrReplace(E_3A))
        assertEquals(E_1A, table.snapshottableRemove(E_1A))
        assertNull(table.snapshottableAddOrReplace(E_1B))
        registry.getOrCreateSnapshot(0)
        val iter: Iterator<TestElement> = table.snapshottableIterator(0)
        val iterElements: MutableList<TestElement> = ArrayList()
        iterElements.add(iter.next())
        assertEquals(E_2A, table.snapshottableRemove(E_2A))
        assertEquals(E_3A, table.snapshottableAddOrReplace(E_3B))
        iterElements.add(iter.next())
        assertEquals(E_1B, table.snapshottableRemove(E_1B))
        iterElements.add(iter.next())
        assertFalse(iter.hasNext())
        assertIteratorYields(iterElements.iterator(), E_1B, E_2A, E_3A)
    }

    @Test
    fun testRevert() {
        val registry = SnapshotRegistry(LogContext())
        val table = SnapshottableHashTable<TestElement>(registry, 1)
        assertNull(table.snapshottableAddOrReplace(E_1A))
        assertNull(table.snapshottableAddOrReplace(E_2A))
        assertNull(table.snapshottableAddOrReplace(E_3A))
        registry.getOrCreateSnapshot(0)
        assertEquals(E_1A, table.snapshottableAddOrReplace(E_1B))
        assertEquals(E_3A, table.snapshottableAddOrReplace(E_3B))
        registry.getOrCreateSnapshot(1)
        assertEquals(3, table.snapshottableSize(Long.MAX_VALUE))
        assertIteratorYields(table.snapshottableIterator(Long.MAX_VALUE), E_1B, E_2A, E_3B)
        table.snapshottableRemove(E_1B)
        table.snapshottableRemove(E_2A)
        table.snapshottableRemove(E_3B)
        assertEquals(0, table.snapshottableSize(Long.MAX_VALUE))
        assertEquals(3, table.snapshottableSize(0))
        assertEquals(3, table.snapshottableSize(1))
        registry.revertToSnapshot(0)
        assertIteratorYields(table.snapshottableIterator(Long.MAX_VALUE), E_1A, E_2A, E_3A)
    }

    @Test
    fun testReset() {
        val registry = SnapshotRegistry(LogContext())
        val table = SnapshottableHashTable<TestElement>(registry, 1)
        assertNull(table.snapshottableAddOrReplace(E_1A))
        assertNull(table.snapshottableAddOrReplace(E_2A))
        assertNull(table.snapshottableAddOrReplace(E_3A))
        registry.getOrCreateSnapshot(0)
        assertEquals(E_1A, table.snapshottableAddOrReplace(E_1B))
        assertEquals(E_3A, table.snapshottableAddOrReplace(E_3B))
        registry.getOrCreateSnapshot(1)
        registry.reset()
        assertEquals(emptyList<Any>(), registry.epochsList())
        // Check that the table is empty
        assertIteratorYields(table.snapshottableIterator(Long.MAX_VALUE))
    }

    @Test
    fun testIteratorAtOlderEpoch() {
        val registry = SnapshotRegistry(LogContext())
        val table = SnapshottableHashTable<TestElement>(registry, 4)
        assertNull(table.snapshottableAddOrReplace(E_3B))
        registry.getOrCreateSnapshot(0)
        assertNull(table.snapshottableAddOrReplace(E_1A))
        registry.getOrCreateSnapshot(1)
        assertEquals(E_1A, table.snapshottableAddOrReplace(E_1B))
        registry.getOrCreateSnapshot(2)
        assertEquals(E_1B, table.snapshottableRemove(E_1B))
        assertIteratorYields(table.snapshottableIterator(1), E_3B, E_1A)
    }

    companion object {

        private val E_1A = TestElement(1, 'A')

        private val E_1B = TestElement(1, 'B')

        private val E_2A = TestElement(2, 'A')

        private val E_3A = TestElement(3, 'A')

        private val E_3B = TestElement(3, 'B')

        /**
         * Assert that the given iterator contains the given elements, in any order.
         * We compare using reference equality here, rather than object equality.
         */
        private fun assertIteratorYields(
            iter: Iterator<*>,
            vararg expected: Any,
        ) {
            val remaining = IdentityHashMap<Any, Boolean?>()
            for (element: Any in expected) remaining[element] = true

            val extraObjects = mutableListOf<Any>()
            val i = 0
            while (iter.hasNext()) {
                val element = iter.next()!!
                assertNotNull(element)
                if (remaining.remove(element) == null) extraObjects.add(element)
            }
            if (extraObjects.isNotEmpty() || !remaining.isEmpty()) throw RuntimeException(
                "Found extra object(s): " +
                        extraObjects.joinToString(
                            prefix = "[",
                            postfix = "]",
                        ) { it.toString() } +
                        " and didn't find object(s): " +
                        remaining.keys.joinToString(
                            prefix = "[",
                            postfix = "]",
                        ) { it.toString() }
            )
        }
    }
}
