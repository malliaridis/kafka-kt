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
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse

class FixedOrderMapTest {

    @Test
    fun shouldMaintainOrderWhenAdding() {
        val map = FixedOrderMap<String, Int>()
        map["a"] = 0
        map["b"] = 1
        map["c"] = 2
        map["b"] = 3
        val iterator = map.iterator()
        assertEquals("a" to 0, iterator.next().toPair())
        assertEquals("b" to 3, iterator.next().toPair())
        assertEquals("c" to 2, iterator.next().toPair())
        assertFalse(iterator.hasNext())
    }

    @Suppress("Deprecation")
    @Test
    fun shouldForbidRemove() {
        val map = FixedOrderMap<String, Int>()
        map["a"] = 0
        assertFailsWith<UnsupportedOperationException> { map.remove("a") }
        assertEquals(0, map["a"])
    }

    @Suppress("Deprecation")
    @Test
    fun shouldForbidConditionalRemove() {
        val map = FixedOrderMap<String, Int>()
        map["a"] = 0
        assertFailsWith<UnsupportedOperationException> { map.remove("a", 0) }
        assertEquals(0, map["a"])
    }
}
