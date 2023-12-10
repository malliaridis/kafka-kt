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

import java.io.IOException
import java.io.InputStream
import java.nio.ByteBuffer
import java.security.NoSuchAlgorithmException
import java.security.SecureRandom
import java.util.Random
import java.util.stream.Stream
import org.apache.kafka.common.InvalidRecordException
import org.apache.kafka.common.compress.ZstdFactory.wrapForInput
import org.apache.kafka.common.errors.CorruptRecordException
import org.apache.kafka.common.header.Header
import org.apache.kafka.common.header.internals.RecordHeader
import org.apache.kafka.common.record.DefaultRecordBatch.StreamRecordIterator
import org.apache.kafka.common.utils.BufferSupplier
import org.apache.kafka.common.utils.ChunkedBytesStream
import org.apache.kafka.test.TestUtils.checkEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import org.mockito.ArgumentMatchers
import org.mockito.Mockito
import org.mockito.kotlin.any
import org.mockito.kotlin.doReturn
import org.mockito.kotlin.mock
import org.mockito.kotlin.never
import org.mockito.kotlin.spy
import org.mockito.kotlin.times
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

class DefaultRecordBatchTest {

    @Test
    fun testWriteEmptyHeader() {
        val producerId = 23423L
        val producerEpoch: Short = 145
        val baseSequence = 983
        val baseOffset = 15L
        val lastOffset: Long = 37
        val partitionLeaderEpoch = 15
        val timestamp = System.currentTimeMillis()
        for (timestampType in listOf(TimestampType.CREATE_TIME, TimestampType.LOG_APPEND_TIME)) {
            for (isTransactional in listOf(true, false)) {
                for (isControlBatch in listOf(true, false)) {
                    val buffer = ByteBuffer.allocate(2048)
                    DefaultRecordBatch.writeEmptyHeader(
                        buffer = buffer,
                        magic = RecordBatch.CURRENT_MAGIC_VALUE,
                        producerId = producerId,
                        producerEpoch = producerEpoch,
                        baseSequence = baseSequence,
                        baseOffset = baseOffset,
                        lastOffset = lastOffset,
                        partitionLeaderEpoch = partitionLeaderEpoch,
                        timestampType = timestampType,
                        timestamp = timestamp,
                        isTransactional = isTransactional,
                        isControlRecord = isControlBatch
                    )
                    buffer.flip()
                    val batch = DefaultRecordBatch(buffer)
                    assertEquals(producerId, batch.producerId())
                    assertEquals(producerEpoch, batch.producerEpoch())
                    assertEquals(baseSequence, batch.baseSequence())
                    assertEquals(baseSequence + (lastOffset - baseOffset).toInt(), batch.lastSequence())
                    assertEquals(baseOffset, batch.baseOffset())
                    assertEquals(lastOffset, batch.lastOffset())
                    assertEquals(partitionLeaderEpoch, batch.partitionLeaderEpoch())
                    assertEquals(isTransactional, batch.isTransactional)
                    assertEquals(timestampType, batch.timestampType())
                    assertEquals(timestamp, batch.maxTimestamp())
                    assertEquals(RecordBatch.NO_TIMESTAMP, batch.baseTimestamp())
                    assertEquals(isControlBatch, batch.isControlBatch)
                }
            }
        }
    }

    @Test
    fun buildDefaultRecordBatch() {
        val buffer = ByteBuffer.allocate(2048)
        val builder = MemoryRecords.builder(
            buffer = buffer,
            magic = RecordBatch.MAGIC_VALUE_V2,
            compressionType = CompressionType.NONE,
            timestampType = TimestampType.CREATE_TIME,
            baseOffset = 1234567L,
        )
        builder.appendWithOffset(
            offset = 1234567,
            timestamp = 1L,
            key = "a".toByteArray(),
            value = "v".toByteArray(),
        )
        builder.appendWithOffset(
            offset = 1234568,
            timestamp = 2L,
            key = "b".toByteArray(),
            value = "v".toByteArray(),
        )
        val records = builder.build()
        for (batch in records.batches()) {
            assertTrue(batch.isValid)
            assertEquals(1234567, batch.baseOffset())
            assertEquals(1234568, batch.lastOffset())
            assertEquals(2L, batch.maxTimestamp())
            assertEquals(RecordBatch.NO_PRODUCER_ID, batch.producerId())
            assertEquals(RecordBatch.NO_PRODUCER_EPOCH, batch.producerEpoch())
            assertEquals(RecordBatch.NO_SEQUENCE, batch.baseSequence())
            assertEquals(RecordBatch.NO_SEQUENCE, batch.lastSequence())
            for (record in batch) record.ensureValid()
        }
    }

    @Test
    fun buildDefaultRecordBatchWithProducerId() {
        val pid = 23423L
        val epoch: Short = 145
        val baseSequence = 983
        val buffer = ByteBuffer.allocate(2048)
        val builder = MemoryRecords.builder(
            buffer = buffer,
            magic = RecordBatch.MAGIC_VALUE_V2,
            compressionType = CompressionType.NONE,
            timestampType = TimestampType.CREATE_TIME,
            baseOffset = 1234567L,
            logAppendTime = RecordBatch.NO_TIMESTAMP,
            producerId = pid,
            producerEpoch = epoch,
            baseSequence = baseSequence,
        )
        builder.appendWithOffset(
            offset = 1234567,
            timestamp = 1L,
            key = "a".toByteArray(),
            value = "v".toByteArray(),
        )
        builder.appendWithOffset(
            offset = 1234568,
            timestamp = 2L,
            key = "b".toByteArray(),
            value = "v".toByteArray(),
        )
        val records = builder.build()
        for (batch in records.batches()) {
            assertTrue(batch.isValid)
            assertEquals(1234567, batch.baseOffset())
            assertEquals(1234568, batch.lastOffset())
            assertEquals(2L, batch.maxTimestamp())
            assertEquals(pid, batch.producerId())
            assertEquals(epoch, batch.producerEpoch())
            assertEquals(baseSequence, batch.baseSequence())
            assertEquals(baseSequence + 1, batch.lastSequence())
            for (record in batch) record.ensureValid()
        }
    }

    @Test
    fun buildDefaultRecordBatchWithSequenceWrapAround() {
        val pid = 23423L
        val epoch: Short = 145
        val baseSequence = Int.MAX_VALUE - 1
        val buffer = ByteBuffer.allocate(2048)
        val builder = MemoryRecords.builder(
            buffer = buffer,
            magic = RecordBatch.MAGIC_VALUE_V2,
            compressionType = CompressionType.NONE,
            timestampType = TimestampType.CREATE_TIME,
            baseOffset = 1234567L,
            logAppendTime = RecordBatch.NO_TIMESTAMP,
            producerId = pid,
            producerEpoch = epoch,
            baseSequence = baseSequence,
        )
        builder.appendWithOffset(
            offset = 1234567,
            timestamp = 1L,
            key = "a".toByteArray(),
            value = "v".toByteArray(),
        )
        builder.appendWithOffset(
            offset = 1234568,
            timestamp = 2L,
            key = "b".toByteArray(),
            value = "v".toByteArray(),
        )
        builder.appendWithOffset(
            offset = 1234569,
            timestamp = 3L,
            key = "c".toByteArray(),
            value = "v".toByteArray(),
        )
        val records = builder.build()
        val batches = records.batches().toList()
        assertEquals(1, batches.size)
        val batch: RecordBatch = batches[0]
        assertEquals(pid, batch.producerId())
        assertEquals(epoch, batch.producerEpoch())
        assertEquals(baseSequence, batch.baseSequence())
        assertEquals(0, batch.lastSequence())
        val allRecords = batch.toList()
        assertEquals(3, allRecords.size)
        assertEquals(Int.MAX_VALUE - 1, allRecords[0].sequence())
        assertEquals(Int.MAX_VALUE, allRecords[1].sequence())
        assertEquals(0, allRecords[2].sequence())
    }

    @Test
    fun testSizeInBytes() {
        val headers = arrayOf<Header>(
            RecordHeader("foo", "value".toByteArray()),
            RecordHeader("bar", null as ByteArray?)
        )
        val timestamp = System.currentTimeMillis()
        val records = arrayOf(
            SimpleRecord(timestamp, "key".toByteArray(), "value".toByteArray()),
            SimpleRecord(timestamp + 30000, null, "value".toByteArray()),
            SimpleRecord(timestamp + 60000, "key".toByteArray(), null),
            SimpleRecord(timestamp + 60000, "key".toByteArray(), "value".toByteArray(), headers),
        )
        val actualSize = MemoryRecords.withRecords(
            compressionType = CompressionType.NONE,
            records = records,
        ).sizeInBytes()
        assertEquals(actualSize, DefaultRecordBatch.sizeInBytes(listOf(*records)))
    }

    @Test
    fun testInvalidRecordSize() {
        val records = MemoryRecords.withRecords(
            magic = RecordBatch.MAGIC_VALUE_V2,
            initialOffset = 0L,
            compressionType = CompressionType.NONE,
            timestampType = TimestampType.CREATE_TIME,
            records = arrayOf(
                SimpleRecord(timestamp = 1L, key = "a".toByteArray(), value = "1".toByteArray()),
                SimpleRecord(timestamp = 2L, key = "b".toByteArray(), value = "2".toByteArray()),
                SimpleRecord(timestamp = 3L, key = "c".toByteArray(), value = "3".toByteArray()),
            )
        )
        val buffer = records.buffer()
        buffer.putInt(DefaultRecordBatch.LENGTH_OFFSET, 10)
        val batch = DefaultRecordBatch(buffer)
        assertFalse(batch.isValid)
        assertFailsWith<CorruptRecordException> { batch.ensureValid() }
    }

    @Test
    fun testInvalidRecordCountTooManyNonCompressedV2() {
        val now = System.currentTimeMillis()
        val batch = recordsWithInvalidRecordCount(RecordBatch.MAGIC_VALUE_V2, now, CompressionType.NONE, 5)
        // force iteration through the batch to execute validation
        // batch validation is a part of normal workflow for LogValidator.validateMessagesAndAssignOffsets
        assertFailsWith<InvalidRecordException> { batch.forEach(Record::ensureValid) }
    }

    @Test
    fun testInvalidRecordCountTooLittleNonCompressedV2() {
        val now = System.currentTimeMillis()
        val batch = recordsWithInvalidRecordCount(
            magicValue = RecordBatch.MAGIC_VALUE_V2,
            timestamp = now,
            codec = CompressionType.NONE,
            invalidCount = 2,
        )
        // force iteration through the batch to execute validation
        // batch validation is a part of normal workflow for LogValidator.validateMessagesAndAssignOffsets
        assertFailsWith<InvalidRecordException> { batch.forEach(Record::ensureValid) }
    }

    @Test
    fun testInvalidRecordCountTooManyCompressedV2() {
        val now = System.currentTimeMillis()
        val batch = recordsWithInvalidRecordCount(
            magicValue = RecordBatch.MAGIC_VALUE_V2,
            timestamp = now,
            codec = CompressionType.GZIP,
            invalidCount = 5,
        )
        // force iteration through the batch to execute validation
        // batch validation is a part of normal workflow for LogValidator.validateMessagesAndAssignOffsets
        assertFailsWith<InvalidRecordException> { batch.forEach(Record::ensureValid) }
    }

    @Test
    fun testInvalidRecordCountTooLittleCompressedV2() {
        val now = System.currentTimeMillis()
        val batch = recordsWithInvalidRecordCount(
            magicValue = RecordBatch.MAGIC_VALUE_V2,
            timestamp = now,
            codec = CompressionType.GZIP,
            invalidCount = 2,
        )
        // force iteration through the batch to execute validation
        // batch validation is a part of normal workflow for LogValidator.validateMessagesAndAssignOffsets
        assertFailsWith<InvalidRecordException> { batch.forEach(Record::ensureValid) }
    }

    @Test
    fun testInvalidCrc() {
        val records = MemoryRecords.withRecords(
            magic = RecordBatch.MAGIC_VALUE_V2,
            initialOffset = 0L,
            compressionType = CompressionType.NONE,
            timestampType = TimestampType.CREATE_TIME,
            records = arrayOf(
                SimpleRecord(timestamp = 1L, key = "a".toByteArray(), value = "1".toByteArray()),
                SimpleRecord(timestamp = 2L, key = "b".toByteArray(), value = "2".toByteArray()),
                SimpleRecord(timestamp = 3L, key = "c".toByteArray(), value = "3".toByteArray()),
            )
        )
        val buffer = records.buffer()
        buffer.putInt(DefaultRecordBatch.LAST_OFFSET_DELTA_OFFSET, 23)
        val batch = DefaultRecordBatch(buffer)
        assertFalse(batch.isValid)
        assertFailsWith<CorruptRecordException> { batch.ensureValid() }
    }

    @Test
    fun testSetLastOffset() {
        val simpleRecords = arrayOf(
            SimpleRecord(timestamp = 1L, key = "a".toByteArray(), value = "1".toByteArray()),
            SimpleRecord(timestamp = 2L, key = "b".toByteArray(), value = "2".toByteArray()),
            SimpleRecord(timestamp = 3L, key = "c".toByteArray(), value = "3".toByteArray()),
        )
        val records = MemoryRecords.withRecords(
            magic = RecordBatch.MAGIC_VALUE_V2,
            initialOffset = 0L,
            compressionType = CompressionType.NONE,
            timestampType = TimestampType.CREATE_TIME,
            records = simpleRecords,
        )
        val lastOffset = 500L
        val firstOffset = lastOffset - simpleRecords.size + 1
        val batch = DefaultRecordBatch(records.buffer())
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

    @Test
    fun testSetPartitionLeaderEpoch() {
        val records = MemoryRecords.withRecords(
            magic = RecordBatch.MAGIC_VALUE_V2,
            initialOffset = 0L,
            compressionType = CompressionType.NONE,
            timestampType = TimestampType.CREATE_TIME,
            records = arrayOf(
                SimpleRecord(timestamp = 1L, key = "a".toByteArray(), value = "1".toByteArray()),
                SimpleRecord(timestamp = 2L, key = "b".toByteArray(), value = "2".toByteArray()),
                SimpleRecord(timestamp = 3L, key = "c".toByteArray(), value = "3".toByteArray()),
            )
        )
        val leaderEpoch = 500
        val batch = DefaultRecordBatch(records.buffer())
        batch.setPartitionLeaderEpoch(leaderEpoch)
        assertEquals(leaderEpoch, batch.partitionLeaderEpoch())
        assertTrue(batch.isValid)
        val recordBatches = records.batches().iterator().asSequence().toList()
        assertEquals(1, recordBatches.size)
        assertEquals(leaderEpoch, recordBatches[0].partitionLeaderEpoch())
    }

    @Test
    fun testSetLogAppendTime() {
        val records = MemoryRecords.withRecords(
            magic = RecordBatch.MAGIC_VALUE_V2,
            initialOffset = 0L,
            compressionType = CompressionType.NONE,
            timestampType = TimestampType.CREATE_TIME,
            records = arrayOf(
                SimpleRecord(timestamp = 1L, key = "a".toByteArray(), value = "1".toByteArray()),
                SimpleRecord(timestamp = 2L, key = "b".toByteArray(), value = "2".toByteArray()),
                SimpleRecord(timestamp = 3L, key = "c".toByteArray(), value = "3".toByteArray()),
            )
        )
        val logAppendTime = 15L
        val batch = DefaultRecordBatch(records.buffer())
        batch.setMaxTimestamp(TimestampType.LOG_APPEND_TIME, logAppendTime)
        assertEquals(TimestampType.LOG_APPEND_TIME, batch.timestampType())
        assertEquals(logAppendTime, batch.maxTimestamp())
        assertTrue(batch.isValid)
        val recordBatches = records.batches().iterator().asSequence().toList()
        assertEquals(1, recordBatches.size)
        assertEquals(logAppendTime, recordBatches[0].maxTimestamp())
        assertEquals(TimestampType.LOG_APPEND_TIME, recordBatches[0].timestampType())
        for (record in records.records()) assertEquals(logAppendTime, record.timestamp())
    }

    @Test
    fun testSetNoTimestampTypeNotAllowed() {
        val records = MemoryRecords.withRecords(
            magic = RecordBatch.MAGIC_VALUE_V2,
            initialOffset = 0L,
            compressionType = CompressionType.NONE,
            timestampType = TimestampType.CREATE_TIME,
            records = arrayOf(
                SimpleRecord(timestamp = 1L, key = "a".toByteArray(), value = "1".toByteArray()),
                SimpleRecord(timestamp = 2L, key = "b".toByteArray(), value = "2".toByteArray()),
                SimpleRecord(timestamp = 3L, key = "c".toByteArray(), value = "3".toByteArray()),
            )
        )
        val batch = DefaultRecordBatch(records.buffer())
        assertFailsWith<IllegalArgumentException> {
            batch.setMaxTimestamp(
                timestampType = TimestampType.NO_TIMESTAMP_TYPE,
                maxTimestamp = RecordBatch.NO_TIMESTAMP,
            )
        }
    }

    @Test
    fun testReadAndWriteControlBatch() {
        val producerId = 1L
        val producerEpoch: Short = 0
        val coordinatorEpoch = 15
        val buffer = ByteBuffer.allocate(128)
        val builder = MemoryRecordsBuilder(
            buffer = buffer,
            magic = RecordBatch.CURRENT_MAGIC_VALUE,
            compressionType = CompressionType.NONE,
            timestampType = TimestampType.CREATE_TIME,
            baseOffset = 0L,
            logAppendTime = RecordBatch.NO_TIMESTAMP,
            producerId = producerId,
            producerEpoch = producerEpoch,
            baseSequence = RecordBatch.NO_SEQUENCE,
            isTransactional = true,
            isControlBatch = true,
            partitionLeaderEpoch = RecordBatch.NO_PARTITION_LEADER_EPOCH,
            writeLimit = buffer.remaining(),
        )
        val marker = EndTransactionMarker(ControlRecordType.COMMIT, coordinatorEpoch)
        builder.appendEndTxnMarker(System.currentTimeMillis(), marker)
        val records = builder.build()
        val batches = records.batches().toList()
        assertEquals(1, batches.size)
        val batch = batches[0]
        assertTrue(batch.isControlBatch)
        val logRecords = records.records().toList()
        assertEquals(1, logRecords.size)
        val commitRecord = logRecords[0]
        assertEquals(marker, EndTransactionMarker.deserialize(commitRecord))
    }

    @ParameterizedTest
    @Test
    fun testStreamingIteratorConsistency(compressionType: CompressionType) {
        val records = MemoryRecords.withRecords(
            magic = RecordBatch.MAGIC_VALUE_V2,
            initialOffset = 0L,
            compressionType = compressionType,
            timestampType = TimestampType.CREATE_TIME,
            records = arrayOf(
                SimpleRecord(timestamp = 1L, key = "a".toByteArray(), value = "1".toByteArray()),
                SimpleRecord(timestamp = 2L, key = "b".toByteArray(), value = "2".toByteArray()),
                SimpleRecord(timestamp = 3L, key = "c".toByteArray(), value = "3".toByteArray()),
            )
        )
        val batch = DefaultRecordBatch(records.buffer())
        batch.streamingIterator(BufferSupplier.create()).use { streamingIterator ->
            checkEquals(
                streamingIterator,
                batch.iterator()
            )
        }
    }

    @ParameterizedTest
    @Test
    fun testSkipKeyValueIteratorCorrectness(compressionType: CompressionType) {
        val headers = arrayOf<Header>(
            RecordHeader(key = "k1", value = "v1".toByteArray()),
            RecordHeader(key = "k2", value = "v2".toByteArray()),
        )
        val largeRecordValue = ByteArray(200 * 1024) // 200KB
        RANDOM.nextBytes(largeRecordValue)
        
        val records = MemoryRecords.withRecords(
            magic = RecordBatch.MAGIC_VALUE_V2,
            initialOffset = 0L,
            compressionType = compressionType,
            timestampType = TimestampType.CREATE_TIME,
            records = arrayOf(
                // one sample with small value size
                SimpleRecord(timestamp = 1L, key = "a".toByteArray(), value = "1".toByteArray()),
                // one sample with null value
                SimpleRecord(timestamp = 2L, key = "b".toByteArray(), value = null),
                // one sample with null key
                SimpleRecord(timestamp = 3L, key = null, value = "3".toByteArray()),
                // one sample with null key and null value
                SimpleRecord(timestamp = 4L, key = null, value = null as ByteArray?),
                // one sample with large value size
                SimpleRecord(timestamp = 1000L, key = "abc".toByteArray(), value = largeRecordValue),
                // one sample with headers, one of the header has null value
                SimpleRecord(timestamp = 9999L, key = "abc".toByteArray(), value = "0".toByteArray(), headers = headers)
            )
        )
        
        val batch = DefaultRecordBatch(records.buffer())
        
        BufferSupplier.create().use { bufferSupplier ->
            batch.skipKeyValueIterator(bufferSupplier).use { skipKeyValueIterator ->
                if (CompressionType.NONE === compressionType) {
                    // assert that for uncompressed data stream record iterator is not used
                    assertTrue(skipKeyValueIterator is DefaultRecordBatch.RecordIterator)
                    // superficial validation for correctness. Deep validation is already performed in other tests
                    assertEquals(
                        records.records().toList().size,
                        skipKeyValueIterator.asSequence().toList().size,
                    )
                } else {
                    // assert that a streaming iterator is used for compressed records
                    assertTrue(skipKeyValueIterator is StreamRecordIterator)
                    // assert correctness for compressed records
                    assertContentEquals(
                        listOf(
                            PartialDefaultRecord(
                                sizeInBytes = 9,
                                attributes = 0.toByte(),
                                offset = 0L,
                                timestamp = 1L,
                                sequence = -1,
                                keySize = 1,
                                valueSize = 1,
                            ),
                            PartialDefaultRecord(
                                sizeInBytes = 8,
                                attributes = 0.toByte(),
                                offset = 1L,
                                timestamp = 2L,
                                sequence = -1,
                                keySize = 1,
                                valueSize = -1,
                            ),
                            PartialDefaultRecord(
                                sizeInBytes = 8,
                                attributes = 0.toByte(),
                                offset = 2L,
                                timestamp = 3L,
                                sequence = -1,
                                keySize = -1,
                                valueSize = 1,
                            ),
                            PartialDefaultRecord(
                                sizeInBytes = 7,
                                attributes = 0.toByte(),
                                offset = 3L,
                                timestamp = 4L,
                                sequence = -1,
                                keySize = -1,
                                valueSize = -1,
                            ),
                            PartialDefaultRecord(
                                sizeInBytes = 15 + largeRecordValue.size,
                                attributes = 0.toByte(),
                                offset = 4L,
                                timestamp = 1000L,
                                sequence = -1,
                                keySize = 3,
                                valueSize = largeRecordValue.size,
                            ),
                            PartialDefaultRecord(
                                sizeInBytes = 23,
                                attributes = 0.toByte(),
                                offset = 5L,
                                timestamp = 9999L,
                                sequence = -1,
                                keySize = 3,
                                valueSize = 1,
                            )
                        ),
                        skipKeyValueIterator.asSequence().toList()
                    )
                }
            }
        }
    }

    @ParameterizedTest
    @MethodSource
    fun testBufferReuseInSkipKeyValueIterator(
        compressionType: CompressionType?,
        expectedNumBufferAllocations: Int,
        recordValue: ByteArray?,
    ) {
        val records = MemoryRecords.withRecords(
            magic = RecordBatch.MAGIC_VALUE_V2,
            initialOffset = 0L,
            compressionType = compressionType,
            timestampType = TimestampType.CREATE_TIME,
            records = arrayOf(
                SimpleRecord(timestamp = 1000L, key = "a".toByteArray(), value = "0".toByteArray()),
                SimpleRecord(timestamp = 9999L, key = "b".toByteArray(), value = recordValue),
            )
        )
        val batch = DefaultRecordBatch(records.buffer())
        spy(BufferSupplier.create()).use { bufferSupplier ->
            batch.skipKeyValueIterator(bufferSupplier).use { streamingIterator ->
                
                // Consume through the iterator
                streamingIterator.asSequence().toList()

                // Close the iterator to release any buffers
                streamingIterator.close()

                // assert number of buffer allocations
                verify(bufferSupplier, times(expectedNumBufferAllocations))[ArgumentMatchers.anyInt()]
                verify(bufferSupplier, times(expectedNumBufferAllocations)).release(any<ByteBuffer>())
            }
        }
    }

    @ParameterizedTest
    @MethodSource
    @Throws(IOException::class)
    fun testZstdJniForSkipKeyValueIterator(expectedJniCalls: Int, recordValue: ByteArray?) {
        val records = MemoryRecords.withRecords(
            magic = RecordBatch.MAGIC_VALUE_V2,
            initialOffset = 0L,
            compressionType = CompressionType.ZSTD,
            timestampType = TimestampType.CREATE_TIME,
            records = arrayOf(
                SimpleRecord(timestamp = 9L, key = "hakuna-matata".toByteArray(), value = recordValue),
            ),
        )

        // Buffer containing compressed data
        val compressedBuf = records.buffer()
        // Create a RecordBatch object
        val batch = spy(DefaultRecordBatch(compressedBuf.duplicate()))
        val mockCompression = mock<CompressionType>()
        doReturn(mockCompression).whenever(batch).compressionType()

        // Buffer containing compressed records to be used for creating zstd-jni stream
        val recordsBuffer = compressedBuf.duplicate()
        recordsBuffer.position(DefaultRecordBatch.RECORDS_OFFSET)
        
        BufferSupplier.create().use { bufferSupplier ->
            spy(
                wrapForInput(
                    buffer = recordsBuffer,
                    messageVersion = batch.magic(),
                    decompressionBufferSupplier = bufferSupplier
                )
            ).use { zstdStream ->
                ChunkedBytesStream(
                    inputStream = zstdStream,
                    bufferSupplier = bufferSupplier,
                    intermediateBufSize = 16 * 1024,
                    delegateSkipToSourceStream = false,
                ).use { chunkedStream ->

                    whenever(mockCompression.wrapForInput(any<ByteBuffer>(), any(), any<BufferSupplier>()))
                        .thenReturn(chunkedStream)

                    batch.skipKeyValueIterator(bufferSupplier).use { streamingIterator ->
                        assertNotNull(streamingIterator)
                        streamingIterator.asSequence().toList()
                        // verify the number of read() calls to zstd JNI stream. Each read() call is a JNI call.
                        verify(zstdStream, Mockito.times(expectedJniCalls)).read(any<ByteArray>(), any(), any())
                        // verify that we don't use the underlying skip() functionality. The underlying skip()
                        // allocates 1 buffer per skip call from he buffer pool whereas our implementation does not
                        // perform any allocation during skip.
                        verify<InputStream>(zstdStream, never()).skip(any())
                    }
                }
            }
        }
    }

    @Test
    fun testIncrementSequence() {
        assertEquals(10, DefaultRecordBatch.incrementSequence(5, 5))
        assertEquals(0, DefaultRecordBatch.incrementSequence(Int.MAX_VALUE, 1))
        assertEquals(4, DefaultRecordBatch.incrementSequence(Int.MAX_VALUE - 5, 10))
    }

    @Test
    fun testDecrementSequence() {
        assertEquals(0, DefaultRecordBatch.decrementSequence(5, 5))
        assertEquals(Int.MAX_VALUE, DefaultRecordBatch.decrementSequence(0, 1))
    }

    companion object {
        
        private val RANDOM: Random = try {
            SecureRandom.getInstanceStrong()
        } catch (exception: NoSuchAlgorithmException) {
            throw RuntimeException(exception)
        }

        @Throws(NoSuchAlgorithmException::class)
        @JvmStatic
        private fun testBufferReuseInSkipKeyValueIterator(): Stream<Arguments> {
            val smallRecordValue = "1".toByteArray()
            val largeRecordValue = ByteArray(512 * 1024) // 512KB
            RANDOM.nextBytes(largeRecordValue)
            return Stream.of(
                /*
                 * 1 allocation per batch (i.e. per iterator instance) for buffer holding uncompressed data
                 * = 1 buffer allocations
                 */
                Arguments.of(CompressionType.GZIP, 1, smallRecordValue),
                Arguments.of(CompressionType.GZIP, 1, largeRecordValue),
                Arguments.of(CompressionType.SNAPPY, 1, smallRecordValue),
                Arguments.of(CompressionType.SNAPPY, 1, largeRecordValue), 
                /*
                 * 1 allocation per batch (i.e. per iterator instance) for buffer holding compressed data
                 * 1 allocation per batch (i.e. per iterator instance) for buffer holding uncompressed data
                 * = 2 buffer allocations
                 */
                Arguments.of(CompressionType.LZ4, 2, smallRecordValue),
                Arguments.of(CompressionType.LZ4, 2, largeRecordValue),
                Arguments.of(CompressionType.ZSTD, 2, smallRecordValue),
                Arguments.of(CompressionType.ZSTD, 2, largeRecordValue),
            )
        }

        @Throws(NoSuchAlgorithmException::class)
        @JvmStatic
        private fun testZstdJniForSkipKeyValueIterator(): Stream<Arguments> {
            val smallRecordValue = "1".toByteArray()
            val largeRecordValue = ByteArray(40 * 1024) // 40KB
            RANDOM.nextBytes(largeRecordValue)
            return Stream.of(
                /*
                 * We expect exactly 2 read call to the JNI:
                 * 1 for fetching the full data (size < 16KB)
                 * 1 for detecting end of stream by trying to read more data
                 */
                Arguments.of(2, smallRecordValue),
                /*
                 * We expect exactly 4 read call to the JNI:
                 * 3 for fetching the full data (Math.ceil(40/16))
                 * 1 for detecting end of stream by trying to read more data
                 */
                Arguments.of(4, largeRecordValue),
            )
        }

        private fun recordsWithInvalidRecordCount(
            magicValue: Byte,
            timestamp: Long,
            codec: CompressionType,
            invalidCount: Int,
        ): DefaultRecordBatch {
            val buf = ByteBuffer.allocate(512)
            val builder = MemoryRecords.builder(
                buffer = buf,
                magic = magicValue,
                compressionType = codec,
                timestampType = TimestampType.CREATE_TIME,
                baseOffset = 0L,
            )
            builder.appendWithOffset(offset = 0, timestamp = timestamp, key = null, value = "hello".toByteArray())
            builder.appendWithOffset(offset = 1, timestamp = timestamp, key = null, value = "there".toByteArray())
            builder.appendWithOffset(offset = 2, timestamp = timestamp, key = null, value = "beautiful".toByteArray())
            val records = builder.build()
            val buffer = records.buffer()
            buffer.position(0)
            buffer.putInt(DefaultRecordBatch.RECORDS_COUNT_OFFSET, invalidCount)
            buffer.position(0)
            return DefaultRecordBatch(buffer)
        }
    }
}
