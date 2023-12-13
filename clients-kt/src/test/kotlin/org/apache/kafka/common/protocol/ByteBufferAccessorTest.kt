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

package org.apache.kafka.common.protocol

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import org.junit.jupiter.api.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class ByteBufferAccessorTest {

    @Test
    fun testReadArray() {
        val buf = ByteBuffer.allocate(1024)
        val accessor = ByteBufferAccessor(buf)
        val testArray = byteArrayOf(0x4b, 0x61, 0x46)
        accessor.writeByteArray(testArray)
        accessor.writeInt(12345)
        accessor.flip()
        val testArray2 = accessor.readArray(3)
        assertContentEquals(testArray, testArray2)
        assertEquals(12345, accessor.readInt())
        assertEquals(
            expected = "Error reading byte array of 3 byte(s): only 0 byte(s) available",
            actual = assertFailsWith<RuntimeException> { accessor.readArray(3) }.message,
        )
    }

    @Test
    fun testReadString() {
        val buf = ByteBuffer.allocate(1024)
        val accessor = ByteBufferAccessor(buf)
        val testString = "ABC"
        val testArray = testString.toByteArray(StandardCharsets.UTF_8)
        accessor.writeByteArray(testArray)
        accessor.flip()
        assertEquals("ABC", accessor.readString(3))
        assertEquals(
            expected = "Error reading byte array of 2 byte(s): only 0 byte(s) available",
            actual = assertFailsWith<RuntimeException> { accessor.readString(2) }.message,
        )
    }
}
