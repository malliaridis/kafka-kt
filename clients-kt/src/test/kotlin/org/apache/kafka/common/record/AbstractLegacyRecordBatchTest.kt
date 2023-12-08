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

import org.apache.kafka.common.InvalidRecordException
import org.apache.kafka.common.record.AbstractLegacyRecordBatch.ByteBufferLegacyRecordBatch
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue
import kotlin.test.fail

class AbstractLegacyRecordBatchTest {

    @Test
    fun testSetLastOffsetCompressed() {
        val simpleRecords = arrayOf(
            SimpleRecord(timestamp = 1L, key = "a".toByteArray(), value = "1".toByteArray()),
            SimpleRecord(timestamp = 2L, key = "b".toByteArray(), value = "2".toByteArray()),
            SimpleRecord(timestamp = 3L, key = "c".toByteArray(), value = "3".toByteArray()),
        )
        val records = MemoryRecords.withRecords(
            magic = RecordBatch.MAGIC_VALUE_V1,
            initialOffset = 0L,
            compressionType = CompressionType.GZIP,
            timestampType = TimestampType.CREATE_TIME,
            records = simpleRecords,
        )
        val lastOffset = 500L
        val firstOffset = lastOffset - simpleRecords.size + 1
        val batch = ByteBufferLegacyRecordBatch(records.buffer())
        batch.setLastOffset(lastOffset)
        assertEquals(lastOffset, batch.lastOffset())
        assertEquals(firstOffset, batch.baseOffset())
        assertTrue(batch.isValid)
        val recordBatches = records.batches().iterator().asSequence().toList()
        assertEquals(1, recordBatches.size)
        assertEquals(lastOffset, recordBatches[0].lastOffset())
        var offset = firstOffset
        for (record in records.records()) assertEquals(offset++, record.offset())
    }

    /**
     * The wrapper offset should be 0 in v0, but not in v1. However, the latter worked by accident and some versions of
     * librdkafka now depend on it. So we support 0 for compatibility reasons, but the recommendation is to set the
     * wrapper offset to the relative offset of the last record in the batch.
     */
    @Test
    fun testIterateCompressedRecordWithWrapperOffsetZero() {
        for (magic in listOf(RecordBatch.MAGIC_VALUE_V0, RecordBatch.MAGIC_VALUE_V1)) {
            val simpleRecords = arrayOf(
                SimpleRecord(timestamp = 1L, key = "a".toByteArray(), value = "1".toByteArray()),
                SimpleRecord(timestamp = 2L, key = "b".toByteArray(), value = "2".toByteArray()),
                SimpleRecord(timestamp = 3L, key = "c".toByteArray(), value = "3".toByteArray()),
            )
            val records = MemoryRecords.withRecords(
                magic = magic,
                initialOffset = 0L,
                compressionType = CompressionType.GZIP,
                timestampType = TimestampType.CREATE_TIME,
                records = simpleRecords,
            )
            val batch = ByteBufferLegacyRecordBatch(records.buffer())
            batch.setLastOffset(0L)
            var offset = 0L
            for (record in batch) assertEquals(offset++, record.offset())
        }
    }

    @Test
    fun testInvalidWrapperOffsetV1() {
        val simpleRecords = arrayOf(
            SimpleRecord(timestamp = 1L, key = "a".toByteArray(), value = "1".toByteArray()),
            SimpleRecord(timestamp = 2L, key = "b".toByteArray(), value = "2".toByteArray()),
            SimpleRecord(timestamp = 3L, key = "c".toByteArray(), value = "3".toByteArray()),
        )
        val records = MemoryRecords.withRecords(
            magic = RecordBatch.MAGIC_VALUE_V1,
            initialOffset = 0L,
            compressionType = CompressionType.GZIP,
            timestampType = TimestampType.CREATE_TIME,
            records = simpleRecords,
        )
        val batch = ByteBufferLegacyRecordBatch(records.buffer())
        batch.setLastOffset(1L)
        assertFailsWith<InvalidRecordException> { batch.iterator() }
    }

    @Test
    fun testSetNoTimestampTypeNotAllowed() {
        val records = MemoryRecords.withRecords(
            magic = RecordBatch.MAGIC_VALUE_V1,
            initialOffset = 0L,
            compressionType = CompressionType.GZIP,
            timestampType = TimestampType.CREATE_TIME,
            records = arrayOf(
                SimpleRecord(timestamp = 1L, key = "a".toByteArray(), value = "1".toByteArray()),
                SimpleRecord(timestamp = 2L, key = "b".toByteArray(), value = "2".toByteArray()),
                SimpleRecord(timestamp = 3L, key = "c".toByteArray(), value = "3".toByteArray()),
            ),
        )
        val batch = ByteBufferLegacyRecordBatch(records.buffer())
        assertFailsWith<IllegalArgumentException> {
            batch.setMaxTimestamp(
                timestampType = TimestampType.NO_TIMESTAMP_TYPE,
                maxTimestamp = RecordBatch.NO_TIMESTAMP,
            )
        }
    }

    @Test
    fun testSetLogAppendTimeNotAllowedV0() {
        val records = MemoryRecords.withRecords(
            magic = RecordBatch.MAGIC_VALUE_V0,
            initialOffset = 0L,
            compressionType = CompressionType.GZIP,
            timestampType = TimestampType.CREATE_TIME,
            records = arrayOf(
                SimpleRecord(timestamp = 1L, key = "a".toByteArray(), value = "1".toByteArray()),
                SimpleRecord(timestamp = 2L, key = "b".toByteArray(), value = "2".toByteArray()),
                SimpleRecord(timestamp = 3L, key = "c".toByteArray(), value = "3".toByteArray()),
            ),
        )
        val logAppendTime = 15L
        val batch = ByteBufferLegacyRecordBatch(records.buffer())
        assertFailsWith<UnsupportedOperationException> {
            batch.setMaxTimestamp(
                timestampType = TimestampType.LOG_APPEND_TIME,
                maxTimestamp = logAppendTime,
            )
        }
    }

    @Test
    fun testSetCreateTimeNotAllowedV0() {
        val records = MemoryRecords.withRecords(
            magic = RecordBatch.MAGIC_VALUE_V0,
            initialOffset = 0L,
            compressionType = CompressionType.GZIP,
            timestampType = TimestampType.CREATE_TIME,
            records = arrayOf(
                SimpleRecord(timestamp = 1L, key = "a".toByteArray(), value = "1".toByteArray()),
                SimpleRecord(timestamp = 2L, key = "b".toByteArray(), value = "2".toByteArray()),
                SimpleRecord(timestamp = 3L, key = "c".toByteArray(), value = "3".toByteArray()),
            ),
        )
        val createTime = 15L
        val batch = ByteBufferLegacyRecordBatch(records.buffer())
        assertFailsWith<UnsupportedOperationException> {
            batch.setMaxTimestamp(
                timestampType = TimestampType.CREATE_TIME,
                maxTimestamp = createTime
            )
        }
    }

    @Test
    fun testSetPartitionLeaderEpochNotAllowedV0() {
        val records = MemoryRecords.withRecords(
            magic = RecordBatch.MAGIC_VALUE_V0,
            initialOffset = 0L,
            compressionType = CompressionType.GZIP,
            timestampType = TimestampType.CREATE_TIME,
            records = arrayOf(
                SimpleRecord(timestamp = 1L, key = "a".toByteArray(), value = "1".toByteArray()),
                SimpleRecord(timestamp = 2L, key = "b".toByteArray(), value = "2".toByteArray()),
                SimpleRecord(timestamp = 3L, key = "c".toByteArray(), value = "3".toByteArray()),
            ),
        )
        val batch = ByteBufferLegacyRecordBatch(records.buffer())
        assertFailsWith<UnsupportedOperationException> { batch.setPartitionLeaderEpoch(15) }
    }

    @Test
    fun testSetPartitionLeaderEpochNotAllowedV1() {
        val records = MemoryRecords.withRecords(
            magic = RecordBatch.MAGIC_VALUE_V1,
            initialOffset = 0L,
            compressionType = CompressionType.GZIP,
            timestampType = TimestampType.CREATE_TIME,
            records = arrayOf(
                SimpleRecord(timestamp = 1L, key = "a".toByteArray(), value = "1".toByteArray()),
                SimpleRecord(timestamp = 2L, key = "b".toByteArray(), value = "2".toByteArray()),
                SimpleRecord(timestamp = 3L, key = "c".toByteArray(), value = "3".toByteArray()),
            ),
        )
        val batch = ByteBufferLegacyRecordBatch(records.buffer())
        assertFailsWith<UnsupportedOperationException> { batch.setPartitionLeaderEpoch(15) }
    }

    @Test
    fun testSetLogAppendTimeV1() {
        val records = MemoryRecords.withRecords(
            magic = RecordBatch.MAGIC_VALUE_V1,
            initialOffset = 0L,
            compressionType = CompressionType.GZIP,
            timestampType = TimestampType.CREATE_TIME,
            records = arrayOf(
                SimpleRecord(timestamp = 1L, key = "a".toByteArray(), value = "1".toByteArray()),
                SimpleRecord(timestamp = 2L, key = "b".toByteArray(), value = "2".toByteArray()),
                SimpleRecord(timestamp = 3L, key = "c".toByteArray(), value = "3".toByteArray()),
            ),
        )
        val logAppendTime = 15L
        val batch = ByteBufferLegacyRecordBatch(records.buffer())
        batch.setMaxTimestamp(TimestampType.LOG_APPEND_TIME, logAppendTime)
        assertEquals(TimestampType.LOG_APPEND_TIME, batch.timestampType())
        assertEquals(logAppendTime, batch.maxTimestamp())
        assertTrue(batch.isValid)
        val recordBatches = records.batches().iterator().asSequence().toList()
        assertEquals(1, recordBatches.size)
        assertEquals(TimestampType.LOG_APPEND_TIME, recordBatches[0].timestampType())
        assertEquals(logAppendTime, recordBatches[0].maxTimestamp())
        for (record in records.records()) assertEquals(logAppendTime, record.timestamp())
    }

    @Test
    fun testSetCreateTimeV1() {
        val records = MemoryRecords.withRecords(
            magic = RecordBatch.MAGIC_VALUE_V1,
            initialOffset = 0L,
            compressionType = CompressionType.GZIP,
            timestampType = TimestampType.CREATE_TIME,
            records = arrayOf(
                SimpleRecord(timestamp = 1L, key = "a".toByteArray(), value = "1".toByteArray()),
                SimpleRecord(timestamp = 2L, key = "b".toByteArray(), value = "2".toByteArray()),
                SimpleRecord(timestamp = 3L, key = "c".toByteArray(), value = "3".toByteArray()),
            ),
        )
        val createTime = 15L
        val batch = ByteBufferLegacyRecordBatch(records.buffer())
        batch.setMaxTimestamp(TimestampType.CREATE_TIME, createTime)
        assertEquals(TimestampType.CREATE_TIME, batch.timestampType())
        assertEquals(createTime, batch.maxTimestamp())
        assertTrue(batch.isValid)
        val recordBatches = records.batches().iterator().asSequence().toList()
        assertEquals(1, recordBatches.size)
        assertEquals(TimestampType.CREATE_TIME, recordBatches[0].timestampType())
        assertEquals(createTime, recordBatches[0].maxTimestamp())
        var expectedTimestamp = 1L
        for (record in records.records()) assertEquals(expectedTimestamp++, record.timestamp())
    }

    @Test
    fun testZStdCompressionTypeWithV0OrV1() {
        val simpleRecords = arrayOf(
            SimpleRecord(timestamp = 1L, key = "a".toByteArray(), value = "1".toByteArray()),
            SimpleRecord(timestamp = 2L, key = "b".toByteArray(), value = "2".toByteArray()),
            SimpleRecord(timestamp = 3L, key = "c".toByteArray(), value = "3".toByteArray())
        )

        // Check V0
        try {
            val records = MemoryRecords.withRecords(
                magic = RecordBatch.MAGIC_VALUE_V0,
                initialOffset = 0L,
                compressionType = CompressionType.ZSTD,
                timestampType = TimestampType.CREATE_TIME,
                records = simpleRecords,
            )
            val batch = ByteBufferLegacyRecordBatch(records.buffer())
            batch.setLastOffset(1L)
            batch.iterator()
            fail("Can't reach here")
        } catch (e: IllegalArgumentException) {
            assertEquals("ZStandard compression is not supported for magic 0", e.message)
        }

        // Check V1
        try {
            val records = MemoryRecords.withRecords(
                magic = RecordBatch.MAGIC_VALUE_V1,
                initialOffset = 0L,
                compressionType = CompressionType.ZSTD,
                timestampType = TimestampType.CREATE_TIME,
                records = simpleRecords,
            )
            val batch = ByteBufferLegacyRecordBatch(records.buffer())
            batch.setLastOffset(1L)
            batch.iterator()
            fail("Can't reach here")
        } catch (e: IllegalArgumentException) {
            assertEquals("ZStandard compression is not supported for magic 1", e.message)
        }
    }
}
