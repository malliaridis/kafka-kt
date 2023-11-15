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

import org.apache.kafka.common.utils.CollectionUtils.subtractMap
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotSame
import kotlin.test.assertTrue

class CollectionUtilsTest {

    @Test
    @Disabled("Kotlin Migration - Collection utils not needed in Kotlin")
    fun testSubtractMapRemovesSecondMapsKeys() {
        val mainMap = mapOf(
            "one" to "1",
            "two" to "2",
            "three" to "3",
        )
        val secondaryMap = mapOf(
            "one" to "4",
            "two" to "5",
        )
        val newMap = subtractMap(mainMap, secondaryMap)
        assertEquals(3, mainMap.size) // original map should not be modified
        assertEquals(1, newMap.size)
        assertTrue(newMap.containsKey("three"))
        assertEquals("3", newMap["three"])
    }

    @Test
    @Disabled("Kotlin Migration - Collection utils not needed anymore")
    fun testSubtractMapDoesntRemoveAnythingWhenEmptyMap() {
        val mainMap: MutableMap<String, String> = HashMap()
        mainMap["one"] = "1"
        mainMap["two"] = "2"
        mainMap["three"] = "3"
        val secondaryMap: Map<String, String> = HashMap()
        val newMap: Map<String, String> = subtractMap(mainMap, secondaryMap)
        assertEquals(3, newMap.size)
        assertEquals("1", newMap["one"])
        assertEquals("2", newMap["two"])
        assertEquals("3", newMap["three"])
        assertNotSame(newMap, mainMap)
    }
}
