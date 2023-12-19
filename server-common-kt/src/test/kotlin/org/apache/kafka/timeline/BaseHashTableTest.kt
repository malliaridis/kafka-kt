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

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import kotlin.random.Random
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

@Timeout(value = 40)
class BaseHashTableTest {

    @Test
    fun testEmptyTable() {
        val table = BaseHashTable<Int>(expectedSize = 0)
        assertEquals(0, table.baseSize())
        assertNull(table.baseGet(1))
    }

    @Test
    fun testFindSlot() {
        val random = Random(123)
        for (i in 1..5) {
            val numSlots = 2 shl i
            val slotsReturned = HashSet<Int>()
            while (slotsReturned.size < numSlots) {
                val slot = BaseHashTable.findSlot(random.nextInt(), numSlots)
                assertTrue(slot >= 0)
                assertTrue(slot < numSlots)
                slotsReturned.add(slot)
            }
        }
    }

    @Test
    fun testInsertAndRemove() {
        val table = BaseHashTable<Int>(20)
        val one = 1
        val two = 2
        val three = 3
        val four = 4
        assertNull(table.baseAddOrReplace(one))
        assertNull(table.baseAddOrReplace(two))
        assertNull(table.baseAddOrReplace(three))
        assertEquals(3, table.baseSize())
        assertEquals(one, table.baseGet(one))
        assertEquals(two, table.baseGet(two))
        assertEquals(three, table.baseGet(three))
        assertNull(table.baseGet(four))
        assertEquals(one, table.baseRemove(one))
        assertEquals(2, table.baseSize())
        assertNull(table.baseGet(one))
        assertEquals(2, table.baseSize())
    }

    internal class Foo {

        override fun equals(other: Any?): Boolean = this === other

        override fun hashCode(): Int = 42
    }

    @Test
    fun testHashCollisions() {
        val one = Foo()
        val two = Foo()
        val three = Foo()
        val four = Foo()
        val table = BaseHashTable<Foo>(20)
        assertNull(table.baseAddOrReplace(one))
        assertNull(table.baseAddOrReplace(two))
        assertNull(table.baseAddOrReplace(three))
        assertEquals(3, table.baseSize())
        assertEquals(one, table.baseGet(one))
        assertEquals(two, table.baseGet(two))
        assertEquals(three, table.baseGet(three))
        assertNull(table.baseGet(four))
        assertEquals(one, table.baseRemove(one))
        assertEquals(three, table.baseRemove(three))
        assertEquals(1, table.baseSize())
        assertNull(table.baseGet(four))
        assertEquals(two, table.baseGet(two))
        assertEquals(two, table.baseRemove(two))
        assertEquals(0, table.baseSize())
    }

    @Test
    fun testExpansion() {
        val table = BaseHashTable<Int>(0)
        for (i in 0..4095) {
            assertEquals(i, table.baseSize())
            assertNull(table.baseAddOrReplace(i))
        }
        for (i in 0..4095) {
            assertEquals(4096 - i, table.baseSize())
            assertEquals(i, table.baseRemove(i))
        }
    }

    @Test
    fun testExpectedSizeToCapacity() {
        assertEquals(2, BaseHashTable.expectedSizeToCapacity(Int.MIN_VALUE))
        assertEquals(2, BaseHashTable.expectedSizeToCapacity(-123))
        assertEquals(2, BaseHashTable.expectedSizeToCapacity(0))
        assertEquals(2, BaseHashTable.expectedSizeToCapacity(1))
        assertEquals(4, BaseHashTable.expectedSizeToCapacity(2))
        assertEquals(4, BaseHashTable.expectedSizeToCapacity(3))
        assertEquals(8, BaseHashTable.expectedSizeToCapacity(4))
        assertEquals(16, BaseHashTable.expectedSizeToCapacity(12))
        assertEquals(32, BaseHashTable.expectedSizeToCapacity(13))
        assertEquals(0x2000000, BaseHashTable.expectedSizeToCapacity(0x1010400))
        assertEquals(0x4000000, BaseHashTable.expectedSizeToCapacity(0x2000000))
        assertEquals(0x4000000, BaseHashTable.expectedSizeToCapacity(0x2000001))
        assertEquals(BaseHashTable.MAX_CAPACITY, BaseHashTable.expectedSizeToCapacity(BaseHashTable.MAX_CAPACITY))
        assertEquals(BaseHashTable.MAX_CAPACITY, BaseHashTable.expectedSizeToCapacity(BaseHashTable.MAX_CAPACITY + 1))
        assertEquals(BaseHashTable.MAX_CAPACITY, BaseHashTable.expectedSizeToCapacity(Int.MAX_VALUE - 1))
        assertEquals(BaseHashTable.MAX_CAPACITY, BaseHashTable.expectedSizeToCapacity(Int.MAX_VALUE))
    }
}
