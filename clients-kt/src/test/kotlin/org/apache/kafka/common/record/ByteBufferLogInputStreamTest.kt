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

package org.apache.kafka.common.record

import java.nio.ByteBuffer
import org.apache.kafka.common.errors.CorruptRecordException
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

class ByteBufferLogInputStreamTest {

    @Test
    fun iteratorIgnoresIncompleteEntries() {
        val buffer = ByteBuffer.allocate(1024)
        var builder = MemoryRecords.builder(
            buffer = buffer,
            compressionType = CompressionType.NONE,
            timestampType = TimestampType.CREATE_TIME,
            baseOffset = 0L,
        )
        builder.append(timestamp = 15L, key = "a".toByteArray(), value = "1".toByteArray())
        builder.append(timestamp = 20L, key = "b".toByteArray(), value = "2".toByteArray())
        builder.close()
        builder = MemoryRecords.builder(
            buffer = buffer,
            compressionType = CompressionType.NONE,
            timestampType = TimestampType.CREATE_TIME,
            baseOffset = 2L,
        )
        builder.append(timestamp = 30L, key = "c".toByteArray(), value = "3".toByteArray())
        builder.append(timestamp = 40L, key = "d".toByteArray(), value = "4".toByteArray())
        builder.close()
        buffer.flip()
        buffer.limit(buffer.limit() - 5)
        val records = MemoryRecords.readableRecords(buffer)
        val iterator = records.batches().iterator()
        assertTrue(iterator.hasNext())
        val first = iterator.next()
        assertEquals(1L, first.lastOffset())
        assertFalse(iterator.hasNext())
    }

    @Test
    fun iteratorRaisesOnTooSmallRecords() {
        val buffer = ByteBuffer.allocate(1024)
        var builder = MemoryRecords.builder(
            buffer = buffer,
            compressionType = CompressionType.NONE,
            timestampType = TimestampType.CREATE_TIME,
            baseOffset = 0L,
        )
        builder.append(timestamp = 15L, key = "a".toByteArray(), value = "1".toByteArray())
        builder.append(timestamp = 20L, key = "b".toByteArray(), value = "2".toByteArray())
        builder.close()
        val position = buffer.position()
        builder = MemoryRecords.builder(
            buffer = buffer,
            compressionType = CompressionType.NONE,
            timestampType = TimestampType.CREATE_TIME,
            baseOffset = 2L,
        )
        builder.append(timestamp = 30L, key = "c".toByteArray(), value = "3".toByteArray())
        builder.append(timestamp = 40L, key = "d".toByteArray(), value = "4".toByteArray())
        builder.close()
        buffer.flip()
        buffer.putInt(position + DefaultRecordBatch.LENGTH_OFFSET, 9)
        val logInputStream = ByteBufferLogInputStream(buffer, Int.MAX_VALUE)
        assertNotNull(logInputStream.nextBatch())
        assertFailsWith<CorruptRecordException> { logInputStream.nextBatch() }
    }

    @Test
    fun iteratorRaisesOnInvalidMagic() {
        val buffer = ByteBuffer.allocate(1024)
        var builder = MemoryRecords.builder(
            buffer = buffer,
            compressionType = CompressionType.NONE,
            timestampType = TimestampType.CREATE_TIME,
            baseOffset = 0L,
        )
        builder.append(timestamp = 15L, key = "a".toByteArray(), value = "1".toByteArray())
        builder.append(timestamp = 20L, key = "b".toByteArray(), value = "2".toByteArray())
        builder.close()
        val position = buffer.position()
        builder = MemoryRecords.builder(
            buffer = buffer,
            compressionType = CompressionType.NONE,
            timestampType = TimestampType.CREATE_TIME,
            baseOffset = 2L
        )
        builder.append(timestamp = 30L, key = "c".toByteArray(), value = "3".toByteArray())
        builder.append(timestamp = 40L, key = "d".toByteArray(), value = "4".toByteArray())
        builder.close()
        buffer.flip()
        buffer.put(position + DefaultRecordBatch.MAGIC_OFFSET, 37.toByte())
        val logInputStream = ByteBufferLogInputStream(buffer, Int.MAX_VALUE)
        assertNotNull(logInputStream.nextBatch())
        assertFailsWith<CorruptRecordException> { logInputStream.nextBatch() }
    }

    @Test
    fun iteratorRaisesOnTooLargeRecords() {
        val buffer = ByteBuffer.allocate(1024)
        var builder = MemoryRecords.builder(
            buffer = buffer,
            compressionType = CompressionType.NONE,
            timestampType = TimestampType.CREATE_TIME,
            baseOffset = 0L,
        )
        builder.append(15L, "a".toByteArray(), "1".toByteArray())
        builder.close()
        builder = MemoryRecords.builder(
            buffer = buffer,
            compressionType = CompressionType.NONE,
            timestampType = TimestampType.CREATE_TIME,
            baseOffset = 2L,
        )
        builder.append(30L, "c".toByteArray(), "3".toByteArray())
        builder.append(40L, "d".toByteArray(), "4".toByteArray())
        builder.close()
        buffer.flip()
        val logInputStream = ByteBufferLogInputStream(buffer = buffer, maxMessageSize = 60)
        assertNotNull(logInputStream.nextBatch())
        assertFailsWith<CorruptRecordException> { logInputStream.nextBatch() }
    }
}
