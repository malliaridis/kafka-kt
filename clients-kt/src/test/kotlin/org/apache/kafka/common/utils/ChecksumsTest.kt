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

import java.nio.ByteBuffer
import org.apache.kafka.common.utils.Checksums.update
import org.apache.kafka.common.utils.Checksums.updateInt
import org.apache.kafka.common.utils.Checksums.updateLong
import org.apache.kafka.common.utils.Crc32C.compute
import org.apache.kafka.common.utils.Crc32C.create
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

class ChecksumsTest {

    @Test
    fun testUpdateByteBuffer() {
        val bytes = byteArrayOf(0, 1, 2, 3, 4, 5)
        doTestUpdateByteBuffer(bytes, ByteBuffer.allocate(bytes.size))
        doTestUpdateByteBuffer(bytes, ByteBuffer.allocateDirect(bytes.size))
    }

    private fun doTestUpdateByteBuffer(bytes: ByteArray, buffer: ByteBuffer) {
        buffer.put(bytes)
        buffer.flip()
        val bufferCrc = create()
        update(bufferCrc, buffer, buffer.remaining())
        assertEquals(compute(bytes, 0, bytes.size), bufferCrc.value)
        assertEquals(0, buffer.position())
    }

    @Test
    fun testUpdateByteBufferWithOffsetPosition() {
        val bytes = byteArrayOf(-2, -1, 0, 1, 2, 3, 4, 5)
        doTestUpdateByteBufferWithOffsetPosition(bytes, ByteBuffer.allocate(bytes.size), 2)
        doTestUpdateByteBufferWithOffsetPosition(bytes, ByteBuffer.allocateDirect(bytes.size), 2)
    }

    @Test
    fun testUpdateInt() {
        val value = 1000
        val buffer = ByteBuffer.allocate(4)
        buffer.putInt(value)
        val crc1 = create()
        val crc2 = create()
        updateInt(crc1, value)
        crc2.update(buffer.array(), buffer.arrayOffset(), 4)
        assertEquals(crc1.value, crc2.value, "Crc values should be the same")
    }

    @Test
    fun testUpdateLong() {
        val value = Int.MAX_VALUE + 1L
        val buffer = ByteBuffer.allocate(8)
        buffer.putLong(value)
        val crc1 = create()
        val crc2 = create()
        updateLong(crc1, value)
        crc2.update(buffer.array(), buffer.arrayOffset(), 8)
        assertEquals(crc1.value, crc2.value, "Crc values should be the same")
    }

    private fun doTestUpdateByteBufferWithOffsetPosition(bytes: ByteArray, buffer: ByteBuffer, offset: Int) {
        buffer.put(bytes)
        buffer.flip()
        buffer.position(offset)
        val bufferCrc = create()
        update(bufferCrc, buffer, buffer.remaining())
        assertEquals(compute(bytes, offset, buffer.remaining()), bufferCrc.value)
        assertEquals(offset, buffer.position())
    }
}
