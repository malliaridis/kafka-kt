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

import java.io.DataOutputStream
import java.nio.ByteBuffer
import org.apache.kafka.common.utils.ByteBufferOutputStream
import org.junit.jupiter.api.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals

class DataOutputStreamWritableTest {

    @Test
    fun testWritingSlicedByteBuffer() {
        val expectedArray = byteArrayOf(2, 3, 0, 0)
        val sourceBuffer = ByteBuffer.wrap(byteArrayOf(0, 1, 2, 3))
        val resultBuffer = ByteBuffer.allocate(4)

        // Move position forward to ensure slice is not whole buffer
        sourceBuffer.position(2)
        val slicedBuffer = sourceBuffer.slice()

        val writable = DataOutputStreamWritable(DataOutputStream(ByteBufferOutputStream(resultBuffer)))
        writable.writeByteBuffer(slicedBuffer)

        assertEquals(2, resultBuffer.position(), "Writing to the buffer moves the position forward")
        assertContentEquals(expectedArray, resultBuffer.array(), "Result buffer should have expected elements")
    }

    @Test
    fun testWritingSlicedByteBufferWithNonZeroPosition() {
        val expectedArray = byteArrayOf(3, 0, 0, 0)
        val originalBuffer = ByteBuffer.wrap(byteArrayOf(0, 1, 2, 3))
        val resultBuffer = ByteBuffer.allocate(4)

        // Move position forward to ensure slice is backed by heap buffer with non-zero offset
        originalBuffer.position(2)
        val slicedBuffer = originalBuffer.slice()
        // Move the slice's position forward to ensure the writer starts reading at that position
        slicedBuffer.position(1)

        val writable = DataOutputStreamWritable(DataOutputStream(ByteBufferOutputStream(resultBuffer)))
        writable.writeByteBuffer(slicedBuffer)

        assertEquals(1, resultBuffer.position(), "Writing to the buffer moves the position forward")
        assertContentEquals(expectedArray, resultBuffer.array(), "Result buffer should have expected elements")
    }
}