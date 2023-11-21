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
import org.apache.kafka.common.record.CompressionType
import org.apache.kafka.common.record.MemoryRecords
import org.apache.kafka.common.record.SimpleRecord
import org.apache.kafka.common.record.TimestampType
import org.apache.kafka.common.record.UnalignedMemoryRecords
import org.apache.kafka.common.utils.Utils.utf8
import org.apache.kafka.test.TestUtils.toBuffer
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

class SendBuilderTest {

    @Test
    fun testZeroCopyByteBuffer() {
        val data = "foo".toByteArray()
        val zeroCopyBuffer = ByteBuffer.wrap(data)
        val builder = SendBuilder(8)
        builder.writeInt(5)
        builder.writeByteBuffer(zeroCopyBuffer)
        builder.writeInt(15)
        val send = builder.build()

        // Overwrite the original buffer in order to prove the data was not copied
        val overwrittenData = "bar".toByteArray()
        assertEquals(data.size, overwrittenData.size)
        zeroCopyBuffer.rewind()
        zeroCopyBuffer.put(overwrittenData)
        zeroCopyBuffer.rewind()
        val buffer = toBuffer(send)
        assertEquals(8 + data.size, buffer.remaining())
        assertEquals(5, buffer.getInt())
        assertEquals("bar", getString(buffer, data.size))
        assertEquals(15, buffer.getInt())
    }

    @Test
    fun testWriteByteBufferRespectsPosition() {
        val data = "yolo".toByteArray()
        assertEquals(4, data.size)
        val buffer = ByteBuffer.wrap(data)
        val builder = SendBuilder(0)
        buffer.limit(2)
        builder.writeByteBuffer(buffer)
        assertEquals(0, buffer.position())
        buffer.position(2)
        buffer.limit(4)
        builder.writeByteBuffer(buffer)
        assertEquals(2, buffer.position())
        val send = builder.build()
        val readBuffer = toBuffer(send)
        assertEquals("yolo", getString(readBuffer, 4))
    }

    @Test
    fun testZeroCopyRecords() {
        val buffer = ByteBuffer.allocate(128)
        val records = createRecords(buffer, "foo")
        val builder = SendBuilder(8)
        builder.writeInt(5)
        builder.writeRecords(records)
        builder.writeInt(15)
        val send = builder.build()

        // Overwrite the original buffer in order to prove the data was not copied
        buffer.rewind()
        val overwrittenRecords = createRecords(buffer, "bar")
        val readBuffer = toBuffer(send)
        assertEquals(5, readBuffer.getInt())
        assertEquals(overwrittenRecords, getRecords(readBuffer, records.sizeInBytes()))
        assertEquals(15, readBuffer.getInt())
    }

    @Test
    fun testZeroCopyUnalignedRecords() {
        val buffer = ByteBuffer.allocate(128)
        val records = createRecords(buffer, "foo")
        val buffer1 = records.buffer().duplicate()
        buffer1.limit(buffer1.limit() / 2)
        val buffer2 = records.buffer().duplicate()
        buffer2.position(buffer2.limit() / 2)
        val records1 = UnalignedMemoryRecords(buffer1)
        val records2 = UnalignedMemoryRecords(buffer2)
        val builder = SendBuilder(8)
        builder.writeInt(5)
        builder.writeRecords(records1)
        builder.writeRecords(records2)
        builder.writeInt(15)
        val send = builder.build()

        // Overwrite the original buffer in order to prove the data was not copied
        buffer.rewind()
        val overwrittenRecords = createRecords(buffer, "bar")
        val readBuffer = toBuffer(send)
        assertEquals(5, readBuffer.getInt())
        assertEquals(overwrittenRecords, getRecords(readBuffer, records.sizeInBytes()))
        assertEquals(15, readBuffer.getInt())
    }

    private fun getString(buffer: ByteBuffer, size: Int): String {
        val readData = ByteArray(size)
        buffer[readData]
        return utf8(readData)
    }

    private fun getRecords(buffer: ByteBuffer, size: Int): MemoryRecords {
        val initialPosition = buffer.position()
        val initialLimit = buffer.limit()
        val recordsLimit = initialPosition + size
        buffer.limit(recordsLimit)
        val records = MemoryRecords.readableRecords(buffer.slice())
        buffer.position(recordsLimit)
        buffer.limit(initialLimit)
        return records
    }

    private fun createRecords(buffer: ByteBuffer, value: String): MemoryRecords {
        val recordsBuilder = MemoryRecords.builder(
            buffer = buffer,
            compressionType = CompressionType.NONE,
            timestampType = TimestampType.CREATE_TIME,
            baseOffset = 0L,
        )
        recordsBuilder.append(SimpleRecord(value = value.toByteArray()))
        return recordsBuilder.build()
    }
}
