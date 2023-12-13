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

import java.io.File
import java.io.FileInputStream
import java.io.IOException
import java.util.stream.Stream
import org.apache.kafka.common.header.Header
import org.apache.kafka.common.header.internals.RecordHeader
import org.apache.kafka.test.TestUtils.tempFile
import org.junit.jupiter.api.extension.ExtensionContext
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.ArgumentsProvider
import org.junit.jupiter.params.provider.ArgumentsSource
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

class RemoteLogInputStreamTest {

    class Args(
        val magic: Byte,
        val compression: CompressionType
    ) {
        override fun toString(): String = "Args{magic=$magic, compression=$compression}"
    }

    private class RemoteLogInputStreamArgsProvider : ArgumentsProvider {

        override fun provideArguments(context: ExtensionContext): Stream<out Arguments> {
            val values = mutableListOf<Arguments>()
            val magicValues = listOf(
                RecordBatch.MAGIC_VALUE_V0,
                RecordBatch.MAGIC_VALUE_V1,
                RecordBatch.MAGIC_VALUE_V2
            )
            for (magic in magicValues) {
                for (type in CompressionType.entries) {
                    values.add(Arguments.of(Args(magic, type)))
                }
            }
            return values.stream()
        }
    }

    @ParameterizedTest
    @ArgumentsSource(RemoteLogInputStreamArgsProvider::class)
    @Throws(IOException::class)
    fun testSimpleBatchIteration(args: Args) {
        val magic = args.magic
        val compression = args.compression
        if (compression === CompressionType.ZSTD && magic < RecordBatch.MAGIC_VALUE_V2) return

        val firstBatchRecord = SimpleRecord(timestamp = 3241324L, key = "a".toByteArray(), value = "foo".toByteArray())
        val secondBatchRecord = SimpleRecord(timestamp = 234280L, key = "b".toByteArray(), value = "bar".toByteArray())

        val file = tempFile()
        FileRecords.open(file).use { fileRecords ->
            fileRecords.append(
                MemoryRecords.withRecords(
                    magic = magic,
                    initialOffset = 0L,
                    compressionType = compression,
                    timestampType = TimestampType.CREATE_TIME,
                    records = arrayOf(firstBatchRecord),
                )
            )
            fileRecords.append(
                MemoryRecords.withRecords(
                    magic = magic,
                    initialOffset = 1L,
                    compressionType = compression,
                    timestampType = TimestampType.CREATE_TIME,
                    records = arrayOf(secondBatchRecord),
                )
            )
            fileRecords.flush()
        }
        FileInputStream(file).use { inputStream ->
            val logInputStream = RemoteLogInputStream(inputStream)

            val firstBatch = assertNotNull(logInputStream.nextBatch())
            assertGenericRecordBatchData(
                args = args,
                batch = firstBatch,
                baseOffset = 0L,
                maxTimestamp = 3241324L,
                records = arrayOf(firstBatchRecord),
            )
            assertNoProducerData(firstBatch)

            val secondBatch = assertNotNull(logInputStream.nextBatch())
            assertGenericRecordBatchData(
                args = args,
                batch = secondBatch,
                baseOffset = 1L,
                maxTimestamp = 234280L,
                records = arrayOf(secondBatchRecord),
            )
            assertNoProducerData(secondBatch)

            assertNull(logInputStream.nextBatch())
        }
    }

    @ParameterizedTest
    @ArgumentsSource(RemoteLogInputStreamArgsProvider::class)
    @Throws(IOException::class)
    fun testBatchIterationWithMultipleRecordsPerBatch(args: Args) {
        val magic = args.magic
        val compression = args.compression
        if (magic < RecordBatch.MAGIC_VALUE_V2 && compression === CompressionType.NONE) return

        if (compression === CompressionType.ZSTD && magic < RecordBatch.MAGIC_VALUE_V2) return

        val firstBatchRecords = arrayOf(
            SimpleRecord(timestamp = 3241324L, key = "a".toByteArray(), value = "1".toByteArray()),
            SimpleRecord(timestamp = 234280L, key = "b".toByteArray(), value = "2".toByteArray())
        )
        val secondBatchRecords = arrayOf(
            SimpleRecord(timestamp = 238423489L, key = "c".toByteArray(), value = "3".toByteArray()),
            SimpleRecord(timestamp = 897839L, key = null, value = "4".toByteArray()),
            SimpleRecord(timestamp = 8234020L, key = "e".toByteArray(), value = null)
        )

        val file = tempFile()
        FileRecords.open(file).use { fileRecords ->
            fileRecords.append(
                MemoryRecords.withRecords(
                    magic = magic,
                    initialOffset = 0L,
                    compressionType = compression,
                    timestampType = TimestampType.CREATE_TIME,
                    records = firstBatchRecords,
                )
            )
            fileRecords.append(
                MemoryRecords.withRecords(
                    magic = magic,
                    initialOffset = 1L,
                    compressionType = compression,
                    timestampType = TimestampType.CREATE_TIME,
                    records = secondBatchRecords,
                )
            )
            fileRecords.flush()
        }
        FileInputStream(file).use { `is` ->
            val logInputStream = RemoteLogInputStream(`is`)

            val firstBatch = assertNotNull(logInputStream.nextBatch())
            assertNoProducerData(firstBatch)
            assertGenericRecordBatchData(
                args = args,
                batch = firstBatch,
                baseOffset = 0L,
                maxTimestamp = 3241324L,
                records = firstBatchRecords,
            )

            val secondBatch = assertNotNull(logInputStream.nextBatch())
            assertNoProducerData(secondBatch)
            assertGenericRecordBatchData(
                args = args,
                batch = secondBatch,
                baseOffset = 1L,
                maxTimestamp = 238423489L,
                records = secondBatchRecords,
            )

            assertNull(logInputStream.nextBatch())
        }
    }

    @ParameterizedTest
    @ArgumentsSource(RemoteLogInputStreamArgsProvider::class)
    @Throws(IOException::class)
    fun testBatchIterationV2(args: Args) {
        val magic = args.magic
        val compression = args.compression

        if (magic != RecordBatch.MAGIC_VALUE_V2) return

        val producerId = 83843L
        val producerEpoch: Short = 15
        val baseSequence = 234
        val partitionLeaderEpoch = 9832
        val headers = arrayOf<Header>(
            RecordHeader(key = "header-key", value = "header-value".toByteArray())
        )
        val firstBatchRecords = arrayOf(
            SimpleRecord(timestamp = 3241324L, key = "a".toByteArray(), value = "1".toByteArray()), // Add a record with headers.
            SimpleRecord(timestamp = 234280L, key = "b".toByteArray(), value = "2".toByteArray(), headers = headers)
        )
        val secondBatchRecords = arrayOf(
            SimpleRecord(timestamp = 238423489L, key = "c".toByteArray(), value = "3".toByteArray()),
            SimpleRecord(timestamp = 897839L, key = null, value = "4".toByteArray()),
            SimpleRecord(timestamp = 8234020L, key = "e".toByteArray(), value = null)
        )

        val file: File = tempFile()
        FileRecords.open(file).use { fileRecords ->
            fileRecords.append(
                MemoryRecords.withIdempotentRecords(
                    magic = magic,
                    initialOffset = 15L,
                    compressionType = compression,
                    producerId = producerId,
                    producerEpoch = producerEpoch,
                    baseSequence = baseSequence,
                    partitionLeaderEpoch = partitionLeaderEpoch,
                    records = firstBatchRecords,
                )
            )
            fileRecords.append(
                MemoryRecords.withTransactionalRecords(
                    magic = magic,
                    initialOffset = 27L,
                    compressionType = compression,
                    producerId = producerId,
                    producerEpoch = producerEpoch,
                    baseSequence = baseSequence + firstBatchRecords.size,
                    partitionLeaderEpoch = partitionLeaderEpoch,
                    records = secondBatchRecords
                )
            )
            fileRecords.flush()
        }
        FileInputStream(file).use { `is` ->
            val logInputStream = RemoteLogInputStream(`is`)

            val firstBatch = assertNotNull(logInputStream.nextBatch())
            assertProducerData(
                batch = firstBatch,
                producerId = producerId,
                producerEpoch = producerEpoch,
                baseSequence = baseSequence,
                isTransactional = false,
                records = firstBatchRecords,
            )
            assertGenericRecordBatchData(
                args = args,
                batch = firstBatch,
                baseOffset = 15L,
                maxTimestamp = 3241324L,
                records = firstBatchRecords,
            )
            assertEquals(partitionLeaderEpoch, firstBatch.partitionLeaderEpoch())

            val secondBatch = assertNotNull(logInputStream.nextBatch())
            assertProducerData(
                batch = secondBatch,
                producerId = producerId,
                producerEpoch = producerEpoch,
                baseSequence = baseSequence + firstBatchRecords.size,
                isTransactional = true,
                records = secondBatchRecords,
            )
            assertGenericRecordBatchData(
                args = args,
                batch = secondBatch,
                baseOffset = 27L,
                maxTimestamp = 238423489L,
                records = secondBatchRecords,
            )
            assertEquals(partitionLeaderEpoch, secondBatch.partitionLeaderEpoch())

            assertNull(logInputStream.nextBatch())
        }
    }

    @ParameterizedTest
    @ArgumentsSource(RemoteLogInputStreamArgsProvider::class)
    @Throws(IOException::class)
    fun testBatchIterationIncompleteBatch(args: Args) {
        val magic = args.magic
        val compression = args.compression
        if (compression === CompressionType.ZSTD && magic < RecordBatch.MAGIC_VALUE_V2) return
        FileRecords.open(tempFile()).use { fileRecords ->
            val firstBatchRecord = SimpleRecord(timestamp = 100L, value = "foo".toByteArray())
            val secondBatchRecord = SimpleRecord(timestamp = 200L, value = "bar".toByteArray())
            fileRecords.append(
                MemoryRecords.withRecords(
                    magic = magic,
                    initialOffset = 0L,
                    compressionType = compression,
                    timestampType = TimestampType.CREATE_TIME,
                    records = arrayOf(firstBatchRecord),
                )
            )
            fileRecords.append(
                MemoryRecords.withRecords(
                    magic = magic,
                    initialOffset = 1L,
                    compressionType = compression,
                    timestampType = TimestampType.CREATE_TIME,
                    records = arrayOf(secondBatchRecord),
                )
            )
            fileRecords.flush()
            fileRecords.truncateTo(fileRecords.sizeInBytes() - 13)
            val logInputStream = FileLogInputStream(
                records = fileRecords,
                start = 0,
                end = fileRecords.sizeInBytes(),
            )
            val firstBatch = assertNotNull(logInputStream.nextBatch())
            assertNoProducerData(firstBatch)
            assertGenericRecordBatchData(
                args = args,
                batch = firstBatch,
                baseOffset = 0L,
                maxTimestamp = 100L,
                records = arrayOf(firstBatchRecord),
            )
            assertNull(logInputStream.nextBatch())
        }
    }

    private fun assertProducerData(
        batch: RecordBatch,
        producerId: Long,
        producerEpoch: Short,
        baseSequence: Int,
        isTransactional: Boolean,
        vararg records: SimpleRecord,
    ) {
        assertEquals(producerId, batch.producerId())
        assertEquals(producerEpoch, batch.producerEpoch())
        assertEquals(baseSequence, batch.baseSequence())
        assertEquals(baseSequence + records.size - 1, batch.lastSequence())
        assertEquals(isTransactional, batch.isTransactional)
    }

    private fun assertNoProducerData(batch: RecordBatch) {
        assertEquals(RecordBatch.NO_PRODUCER_ID, batch.producerId())
        assertEquals(RecordBatch.NO_PRODUCER_EPOCH, batch.producerEpoch())
        assertEquals(RecordBatch.NO_SEQUENCE, batch.baseSequence())
        assertEquals(RecordBatch.NO_SEQUENCE, batch.lastSequence())
        assertFalse(batch.isTransactional)
    }

    private fun assertGenericRecordBatchData(
        args: Args,
        batch: RecordBatch,
        baseOffset: Long,
        maxTimestamp: Long,
        vararg records: SimpleRecord,
    ) {
        val magic = args.magic
        val compression = args.compression

        assertEquals(magic, batch.magic())
        assertEquals(compression, batch.compressionType())

        if (magic == RecordBatch.MAGIC_VALUE_V0) {
            assertEquals(TimestampType.NO_TIMESTAMP_TYPE, batch.timestampType())
        } else {
            assertEquals(TimestampType.CREATE_TIME, batch.timestampType())
            assertEquals(maxTimestamp, batch.maxTimestamp())
        }

        assertEquals(baseOffset + records.size - 1, batch.lastOffset())

        if (magic >= RecordBatch.MAGIC_VALUE_V2) assertEquals(records.size, batch.countOrNull())

        assertEquals(baseOffset, batch.baseOffset())
        assertTrue(batch.isValid)

        val batchRecords = batch.toList()
        for (i in records.indices) {
            assertEquals(baseOffset + i, batchRecords[i].offset())
            assertEquals(records[i].key, batchRecords[i].key())
            assertEquals(records[i].value, batchRecords[i].value())
            assertContentEquals(records[i].headers, batchRecords[i].headers())
            if (magic == RecordBatch.MAGIC_VALUE_V0) assertEquals(
                RecordBatch.NO_TIMESTAMP,
                batchRecords[i].timestamp()
            ) else assertEquals(
                records[i].timestamp, batchRecords[i].timestamp()
            )
        }
    }
}
