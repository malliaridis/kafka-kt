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

import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

class FlattenedIteratorTest {

    @Test
    fun testNestedLists() {
        val list = listOf<List<String>>(
            mutableListOf("foo", "a", "bc"),
            mutableListOf("ddddd"),
            mutableListOf("", "bar2", "baz45")
        )
        val flattenedIterable = Iterable { FlattenedIterator(list.iterator()) { it.iterator() } }
        val flattened = flattenedIterable.toList()
        assertEquals(list.flatten(), flattened)

        // Ensure we can iterate multiple times
        val flattened2 = flattenedIterable.toList()
        assertEquals(flattened, flattened2)
    }

    @Test
    fun testEmptyList() {
        val list = emptyList<List<String>>()
        val flattenedIterable = Iterable { FlattenedIterator(list.iterator()) { it.iterator() } }
        val flattened = flattenedIterable.toList()
        assertEquals(emptyList(), flattened)
    }

    @Test
    fun testNestedSingleEmptyList() {
        val list = listOf(emptyList<String>())
        val flattenedIterable = Iterable { FlattenedIterator(list.iterator()) { it.iterator() } }
        val flattened = flattenedIterable.toList()
        assertEquals(emptyList(), flattened)
    }

    @Test
    fun testEmptyListFollowedByNonEmpty() {
        val list = listOf(emptyList(), mutableListOf("boo", "b", "de"))
        val flattenedIterable = Iterable { FlattenedIterator(list.iterator()) { it.iterator() } }
        val flattened = flattenedIterable.toList()
        assertEquals(list.flatten(), flattened)
    }

    @Test
    fun testEmptyListInBetweenNonEmpty() {
        val list = listOf(
            mutableListOf("aadwdwdw"),
            emptyList(),
            mutableListOf("ee", "aa", "dd")
        )
        val flattenedIterable = Iterable { FlattenedIterator(list.iterator()) { it.iterator() } }
        val flattened = flattenedIterable.toList()
        assertEquals(list.flatten(), flattened)
    }

    @Test
    fun testEmptyListAtTheEnd() {
        val list = listOf(
            mutableListOf("ee", "dd"),
            mutableListOf("e"),
            emptyList(),
        )
        val flattenedIterable = Iterable { FlattenedIterator(list.iterator()) { it.iterator() } }
        val flattened = flattenedIterable.toList()
        assertEquals(list.flatten(), flattened)
    }
}
