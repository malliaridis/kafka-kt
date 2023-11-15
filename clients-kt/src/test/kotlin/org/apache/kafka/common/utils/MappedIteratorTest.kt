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

class MappedIteratorTest {

    @Test
    fun testStringToInteger() {
        val list = listOf("foo", "", "bar2", "baz45")
        val mapper: (String) -> Int = { s: String -> s.length }
        val mappedIterable = Iterable { MappedIterator(list.iterator(), mapper) }
        val mapped = mutableListOf<Int>()
        mappedIterable.forEach(mapped::add)
        assertEquals(list.map(mapper), mapped)

        // Ensure that we can iterate a second time
        val mapped2 = mutableListOf<Int>()
        mappedIterable.forEach(mapped2::add)
        assertEquals(mapped, mapped2)
    }

    @Test
    fun testEmptyList() {
        val list = emptyList<String>()
        val mapper: (String) -> Int = { s: String -> s.length }
        val mappedIterable = Iterable { MappedIterator(list.iterator(), mapper) }
        val mapped = mutableListOf<Int>()
        mappedIterable.forEach(mapped::add)
        assertEquals(emptyList(), mapped)
    }
}
