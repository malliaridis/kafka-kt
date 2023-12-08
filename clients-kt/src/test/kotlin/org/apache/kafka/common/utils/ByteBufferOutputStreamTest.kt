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

import java.io.IOException
import java.nio.ByteBuffer
import org.junit.jupiter.api.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals

class ByteBufferOutputStreamTest {

    @Test
    @Throws(Exception::class)
    fun testExpandByteBufferOnPositionIncrease() {
        testExpandByteBufferOnPositionIncrease(ByteBuffer.allocate(16))
    }

    @Test
    @Throws(Exception::class)
    fun testExpandDirectByteBufferOnPositionIncrease() {
        testExpandByteBufferOnPositionIncrease(ByteBuffer.allocateDirect(16))
    }

    @Throws(Exception::class)
    private fun testExpandByteBufferOnPositionIncrease(initialBuffer: ByteBuffer) {
        val output = ByteBufferOutputStream(initialBuffer)
        output.write("hello".toByteArray())
        output.position(32)
        assertEquals(32, output.position)
        assertEquals(0, initialBuffer.position())
        val buffer = output.buffer
        assertEquals(32, buffer.limit())
        buffer.position(0)
        buffer.limit(5)
        val bytes = ByteArray(5)
        buffer[bytes]
        assertContentEquals("hello".toByteArray(), bytes)
        output.close()
    }

    @Test
    @Throws(Exception::class)
    fun testExpandByteBufferOnWrite() {
        testExpandByteBufferOnWrite(ByteBuffer.allocate(16))
    }

    @Test
    @Throws(Exception::class)
    fun testExpandDirectByteBufferOnWrite() {
        testExpandByteBufferOnWrite(ByteBuffer.allocateDirect(16))
    }

    @Throws(Exception::class)
    private fun testExpandByteBufferOnWrite(initialBuffer: ByteBuffer) {
        val output = ByteBufferOutputStream(initialBuffer)
        output.write("hello".toByteArray())
        output.write(ByteArray(27))
        assertEquals(32, output.position)
        assertEquals(0, initialBuffer.position())
        val buffer = output.buffer
        assertEquals(32, buffer.limit())
        buffer.position(0)
        buffer.limit(5)
        val bytes = ByteArray(5)
        buffer[bytes]
        assertContentEquals("hello".toByteArray(), bytes)
        output.close()
    }

    @Test
    @Throws(IOException::class)
    fun testWriteByteBuffer() {
        testWriteByteBuffer(ByteBuffer.allocate(16))
    }

    @Test
    @Throws(IOException::class)
    fun testWriteDirectByteBuffer() {
        testWriteByteBuffer(ByteBuffer.allocateDirect(16))
    }

    @Throws(IOException::class)
    private fun testWriteByteBuffer(input: ByteBuffer) {
        val value = 234239230L
        input.putLong(value)
        input.flip()
        val output = ByteBufferOutputStream(ByteBuffer.allocate(32))
        output.write(input)
        assertEquals(8, input.position())
        assertEquals(8, output.position)
        assertEquals(value, output.buffer.getLong(0))
        output.close()
    }
}
