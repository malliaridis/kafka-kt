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

import java.util.NavigableMap
import java.util.TreeMap
import org.junit.jupiter.api.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class BytesTest {

    @Test
    fun testIncrement() {
        val input = byteArrayOf(0xAB.toByte(), 0xCD.toByte(), 0xFF.toByte())
        val expected = byteArrayOf(0xAB.toByte(), 0xCE.toByte(), 0x00.toByte())
        val output = Bytes.increment(Bytes.wrap(input)!!)
        assertContentEquals(expected, output!!.get())
    }

    @Test
    fun testIncrementUpperBoundary() {
        val input = byteArrayOf(0xFF.toByte(), 0xFF.toByte(), 0xFF.toByte())
        assertFailsWith<IndexOutOfBoundsException> { Bytes.increment(Bytes.wrap(input)!!) }
    }

    @Test
    fun testIncrementWithSubmap() {
        val map: NavigableMap<Bytes?, ByteArray> = TreeMap()
        val key1 = Bytes.wrap(byteArrayOf(0xAA.toByte()))
        val value = byteArrayOf(0x00.toByte())
        map[key1] = value
        val key2 = Bytes.wrap(byteArrayOf(0xAA.toByte(), 0xAA.toByte()))
        map[key2] = value
        val key3 = Bytes.wrap(byteArrayOf(0xAA.toByte(), 0x00.toByte(), 0xFF.toByte(), 0xFF.toByte(), 0xFF.toByte()))
        map[key3] = value
        val key4 = Bytes.wrap(byteArrayOf(0xAB.toByte(), 0x00.toByte()))
        map[key4] = value
        val key5 = Bytes.wrap(byteArrayOf(0x00.toByte(), 0x00.toByte(), 0x00.toByte(), 0x01.toByte()))
        map[key5] = value
        val prefixEnd = Bytes.increment(key1!!)
        val comparator = map.comparator()
        val result = comparator?.compare(key1, prefixEnd) ?: key1.compareTo(prefixEnd!!)
        val subMapResults: NavigableMap<Bytes?, ByteArray> = if (result > 0)
        // Prefix increment would cause a wrap-around. Get the submap from toKey to the end of the map
            map.tailMap(key1, true)
        else map.subMap(key1, true, prefixEnd, false)
        val subMapExpected: NavigableMap<Bytes?, ByteArray> = TreeMap()
        subMapExpected[key1] = value
        subMapExpected[key2] = value
        subMapExpected[key3] = value
        assertEquals(subMapExpected.keys, subMapResults.keys)
    }
}
