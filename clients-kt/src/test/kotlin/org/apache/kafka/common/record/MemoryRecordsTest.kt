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
import java.util.stream.Stream
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.CorruptRecordException
import org.apache.kafka.common.header.internals.RecordHeaders
import org.apache.kafka.common.message.LeaderChangeMessage
import org.apache.kafka.common.message.LeaderChangeMessage.Voter
import org.apache.kafka.common.record.ControlRecordUtils.deserializeLeaderChangeMessage
import org.apache.kafka.common.record.MemoryRecords.RecordFilter
import org.apache.kafka.common.record.MemoryRecords.RecordFilter.BatchRetention
import org.apache.kafka.common.utils.BufferSupplier
import org.apache.kafka.common.utils.Utils.utf8
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtensionContext
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.ArgumentsProvider
import org.junit.jupiter.params.provider.ArgumentsSource
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

class MemoryRecordsTest {

    private val logAppendTime = System.currentTimeMillis()

    @ParameterizedTest
    @ArgumentsSource(MemoryRecordsArgumentsProvider::class)
    fun testIterator(args: Args) {
        val compression = args.compression
        val magic = args.magic
        val pid = args.pid
        val epoch = args.epoch
        val firstSequence = args.firstSequence
        val firstOffset = args.firstOffset
        val buffer = ByteBuffer.allocate(1024)
        val partitionLeaderEpoch = 998
        val builder = MemoryRecordsBuilder(
            buffer = buffer,
            magic = magic,
            compressionType = compression,
            timestampType = TimestampType.CREATE_TIME,
            baseOffset = firstOffset,
            logAppendTime = logAppendTime,
            producerId = pid,
            producerEpoch = epoch,
            baseSequence = firstSequence,
            isTransactional = false,
            isControlBatch = false,
            partitionLeaderEpoch = partitionLeaderEpoch,
            writeLimit = buffer.limit(),
        )
        val records = arrayOf(
            SimpleRecord(1L, "a".toByteArray(), "1".toByteArray()),
            SimpleRecord(2L, "b".toByteArray(), "2".toByteArray()),
            SimpleRecord(3L, "c".toByteArray(), "3".toByteArray()),
            SimpleRecord(4L, null, "4".toByteArray()),
            SimpleRecord(5L, "d".toByteArray(), null),
            SimpleRecord(6L, null as ByteArray?, null)
        )
        for (record in records) builder.append(record)
        val memoryRecords = builder.build()
        for (iteration in 0..1) {
            var total = 0
            for (batch in memoryRecords.batches()) {
                assertTrue(batch.isValid)
                assertEquals(compression, batch.compressionType())
                assertEquals(firstOffset + total, batch.baseOffset())
                if (magic >= RecordBatch.MAGIC_VALUE_V2) {
                    assertEquals(pid, batch.producerId())
                    assertEquals(epoch, batch.producerEpoch())
                    assertEquals(firstSequence + total, batch.baseSequence())
                    assertEquals(partitionLeaderEpoch, batch.partitionLeaderEpoch())
                    assertEquals(records.size, batch.countOrNull())
                    assertEquals(TimestampType.CREATE_TIME, batch.timestampType())
                    assertEquals(records[records.size - 1].timestamp(), batch.maxTimestamp())
                } else {
                    assertEquals(RecordBatch.NO_PRODUCER_ID, batch.producerId())
                    assertEquals(RecordBatch.NO_PRODUCER_EPOCH, batch.producerEpoch())
                    assertEquals(RecordBatch.NO_SEQUENCE, batch.baseSequence())
                    assertEquals(RecordBatch.NO_PARTITION_LEADER_EPOCH, batch.partitionLeaderEpoch())
                    assertNull(batch.countOrNull())
                    if (magic == RecordBatch.MAGIC_VALUE_V0)
                        assertEquals(TimestampType.NO_TIMESTAMP_TYPE, batch.timestampType())
                    else assertEquals(TimestampType.CREATE_TIME, batch.timestampType())
                }
                var recordCount = 0
                for (record in batch) {
                    record.ensureValid()
                    assertTrue(record.hasMagic(batch.magic()))
                    assertFalse(record.isCompressed)
                    assertEquals(firstOffset + total, record.offset())
                    assertEquals(records[total].key, record.key())
                    assertEquals(records[total].value, record.value())
                    if (magic >= RecordBatch.MAGIC_VALUE_V2)
                        assertEquals(firstSequence + total, record.sequence())
                    assertFalse(record.hasTimestampType(TimestampType.LOG_APPEND_TIME))
                    if (magic == RecordBatch.MAGIC_VALUE_V0) {
                        assertEquals(RecordBatch.NO_TIMESTAMP, record.timestamp())
                        assertFalse(record.hasTimestampType(TimestampType.CREATE_TIME))
                        assertTrue(record.hasTimestampType(TimestampType.NO_TIMESTAMP_TYPE))
                    } else {
                        assertEquals(records[total].timestamp(), record.timestamp())
                        assertFalse(record.hasTimestampType(TimestampType.NO_TIMESTAMP_TYPE))
                        if (magic < RecordBatch.MAGIC_VALUE_V2)
                            assertTrue(record.hasTimestampType(TimestampType.CREATE_TIME))
                        else assertFalse(record.hasTimestampType(TimestampType.CREATE_TIME))
                    }
                    total++
                    recordCount++
                }
                assertEquals(batch.baseOffset() + recordCount - 1, batch.lastOffset())
            }
        }
    }

    @ParameterizedTest
    @ArgumentsSource(MemoryRecordsArgumentsProvider::class)
    fun testHasRoomForMethod(args: Args) {
        val builder = MemoryRecords.builder(
            buffer = ByteBuffer.allocate(1024),
            magic = args.magic,
            compressionType = args.compression,
            timestampType = TimestampType.CREATE_TIME,
            baseOffset = 0L,
        )
        builder.append(timestamp = 0L, key = "a".toByteArray(), value = "1".toByteArray())
        assertTrue(
            builder.hasRoomFor(
                timestamp = 1L,
                key = "b".toByteArray(),
                value = "2".toByteArray(),
                headers = Record.EMPTY_HEADERS,
            )
        )
        builder.close()
        assertFalse(
            builder.hasRoomFor(
                timestamp = 1L,
                key = "b".toByteArray(),
                value = "2".toByteArray(),
                headers = Record.EMPTY_HEADERS,
            )
        )
    }

    @ParameterizedTest
    @ArgumentsSource(MemoryRecordsArgumentsProvider::class)
    fun testHasRoomForMethodWithHeaders(args: Args) {
        val magic = args.magic
        val builder = MemoryRecords.builder(
            buffer = ByteBuffer.allocate(120),
            magic = magic,
            compressionType = args.compression,
            timestampType = TimestampType.CREATE_TIME,
            baseOffset = 0L,
        )
        builder.append(logAppendTime, "key".toByteArray(), "value".toByteArray())
        val headers = RecordHeaders()
        for (i in 0..9) headers.add("hello", "world.world".toByteArray())
        // Make sure that hasRoomFor accounts for header sizes by letting a record without headers pass, but stopping
        // a record with a large number of headers.
        assertTrue(
            builder.hasRoomFor(
                timestamp = logAppendTime,
                key = "key".toByteArray(),
                value = "value".toByteArray(),
                headers = Record.EMPTY_HEADERS,
            )
        )
        if (magic < RecordBatch.MAGIC_VALUE_V2) assertTrue(
            builder.hasRoomFor(
                timestamp = logAppendTime,
                key = "key".toByteArray(),
                value = "value".toByteArray(),
                headers = headers.toArray(),
            )
        ) else assertFalse(
            builder.hasRoomFor(
                timestamp = logAppendTime,
                key = "key".toByteArray(),
                value = "value".toByteArray(),
                headers = headers.toArray(),
            )
        )
    }

    /**
     * This test verifies that the checksum returned for various versions matches hardcoded values to catch unintentional
     * changes to how the checksum is computed.
     */
    @ParameterizedTest
    @ArgumentsSource(MemoryRecordsArgumentsProvider::class)
    fun testChecksum(args: Args) {
        val compression = args.compression
        val magic = args.magic
        // we get reasonable coverage with uncompressed and one compression type
        if (compression !== CompressionType.NONE && compression !== CompressionType.LZ4) return
        val records = arrayOf(
            SimpleRecord(283843L, "key1".toByteArray(), "value1".toByteArray()),
            SimpleRecord(1234L, "key2".toByteArray(), "value2".toByteArray()),
        )
        val batch = MemoryRecords.withRecords(
            magic = magic,
            compressionType = compression,
            records = records,
        ).batches().first()
        val expectedChecksum = if (magic == RecordBatch.MAGIC_VALUE_V0) {
            if (compression === CompressionType.NONE) 1978725405u
            else 66944826u
        } else if (magic == RecordBatch.MAGIC_VALUE_V1) {
            if (compression === CompressionType.NONE) 109425508u
            else 1407303399u
        } else {
            if (compression === CompressionType.NONE) 3851219455u
            else 2745969314u
        }
        assertEquals(
            expected = expectedChecksum,
            actual = batch.checksum(),
            message = "Unexpected checksum for magic $magic and compression type $compression",
        )
    }

    @ParameterizedTest
    @ArgumentsSource(MemoryRecordsArgumentsProvider::class)
    fun testFilterToPreservesPartitionLeaderEpoch(args: Args) {
        val magic = args.magic
        val partitionLeaderEpoch = 67
        val buffer = ByteBuffer.allocate(2048)
        val builder = MemoryRecords.builder(
            buffer = buffer,
            magic = magic,
            compressionType = args.compression,
            timestampType = TimestampType.CREATE_TIME,
            baseOffset = 0L,
            logAppendTime = RecordBatch.NO_TIMESTAMP,
            partitionLeaderEpoch = partitionLeaderEpoch,
        )
        builder.append(10L, null, "a".toByteArray())
        builder.append(11L, "1".toByteArray(), "b".toByteArray())
        builder.append(12L, null, "c".toByteArray())
        val filtered = ByteBuffer.allocate(2048)
        builder.build().filterTo(
            partition = TopicPartition(topic = "foo", partition = 0),
            filter = RetainNonNullKeysFilter(),
            destinationBuffer = filtered,
            maxRecordBatchSize = Int.MAX_VALUE,
            decompressionBufferSupplier = BufferSupplier.NO_CACHING,
        )
        filtered.flip()
        val filteredRecords = MemoryRecords.readableRecords(filtered)
        val batches = filteredRecords.batches().toList()
        assertEquals(1, batches.size)
        val firstBatch = batches[0]
        if (magic < RecordBatch.MAGIC_VALUE_V2)
            assertEquals(RecordBatch.NO_PARTITION_LEADER_EPOCH, firstBatch.partitionLeaderEpoch())
        else assertEquals(partitionLeaderEpoch, firstBatch.partitionLeaderEpoch())
    }

    @ParameterizedTest
    @ArgumentsSource(MemoryRecordsArgumentsProvider::class)
    fun testFilterToEmptyBatchRetention(args: Args) {
        val magic = args.magic
        for (isTransactional in mutableListOf(true, false)) {
            val buffer = ByteBuffer.allocate(2048)
            val producerId = 23L
            val producerEpoch: Short = 5
            val baseOffset = 3L
            val baseSequence = 10
            val partitionLeaderEpoch = 293
            val numRecords = 2
            val supplier = {
                MemoryRecords.builder(
                    buffer = buffer,
                    magic = magic,
                    compressionType = args.compression,
                    timestampType = TimestampType.CREATE_TIME,
                    baseOffset = baseOffset,
                    logAppendTime = RecordBatch.NO_TIMESTAMP,
                    producerId = producerId,
                    producerEpoch = producerEpoch,
                    baseSequence = baseSequence,
                    isTransactional = isTransactional,
                    partitionLeaderEpoch = partitionLeaderEpoch,
                )
            }
            if (isTransactional && magic < RecordBatch.MAGIC_VALUE_V2)
                assertFailsWith<IllegalArgumentException> { supplier() }
            else {
                val builder = supplier()
                builder.append(11L, "2".toByteArray(), "b".toByteArray())
                builder.append(12L, "3".toByteArray(), "c".toByteArray())
                if (magic < RecordBatch.MAGIC_VALUE_V2) assertFailsWith<IllegalArgumentException> { builder.close() }
                else {
                    builder.close()
                    val records = builder.build()
                    val filtered = ByteBuffer.allocate(2048)
                    val filterResult = records.filterTo(
                        partition = TopicPartition("foo", 0),
                        filter = object : RecordFilter(0, 0) {
                            // retain all batches
                            override fun checkBatchRetention(batch: RecordBatch): BatchRetentionResult =
                                BatchRetentionResult(BatchRetention.RETAIN_EMPTY, false)
                            
                            // delete the records
                            override fun shouldRetainRecord(recordBatch: RecordBatch, record: Record): Boolean = false
                        },
                        destinationBuffer = filtered,
                        maxRecordBatchSize = Int.MAX_VALUE,
                        decompressionBufferSupplier = BufferSupplier.NO_CACHING,
                    )

                    // Verify filter result
                    assertEquals(numRecords, filterResult.messagesRead)
                    assertEquals(records.sizeInBytes(), filterResult.bytesRead)
                    assertEquals(baseOffset + 1, filterResult.maxOffset)
                    assertEquals(0, filterResult.messagesRetained)
                    assertEquals(DefaultRecordBatch.RECORD_BATCH_OVERHEAD, filterResult.bytesRetained)
                    assertEquals(12, filterResult.maxTimestamp)
                    assertEquals(baseOffset + 1, filterResult.shallowOffsetOfMaxTimestamp)

                    // Verify filtered records
                    filtered.flip()
                    val filteredRecords = MemoryRecords.readableRecords(filtered)
                    val batches = filteredRecords.batches().toList()
                    assertEquals(1, batches.size)
                    val batch = batches[0]
                    assertEquals(0, batch.countOrNull())
                    assertEquals(12L, batch.maxTimestamp())
                    assertEquals(TimestampType.CREATE_TIME, batch.timestampType())
                    assertEquals(baseOffset, batch.baseOffset())
                    assertEquals(baseOffset + 1, batch.lastOffset())
                    assertEquals(baseSequence, batch.baseSequence())
                    assertEquals(baseSequence + 1, batch.lastSequence())
                    assertEquals(isTransactional, batch.isTransactional)
                }
            }
        }
    }

    @Test
    fun testEmptyBatchRetention() {
        val buffer = ByteBuffer.allocate(DefaultRecordBatch.RECORD_BATCH_OVERHEAD)
        val producerId = 23L
        val producerEpoch: Short = 5
        val baseOffset = 3L
        val baseSequence = 10
        val partitionLeaderEpoch = 293
        val timestamp = System.currentTimeMillis()
        DefaultRecordBatch.writeEmptyHeader(
            buffer = buffer,
            magic = RecordBatch.MAGIC_VALUE_V2,
            producerId = producerId,
            producerEpoch = producerEpoch,
            baseSequence = baseSequence,
            baseOffset = baseOffset,
            lastOffset = baseOffset,
            partitionLeaderEpoch = partitionLeaderEpoch,
            timestampType = TimestampType.CREATE_TIME,
            timestamp = timestamp,
            isTransactional = false,
            isControlRecord = false,
        )
        buffer.flip()
        val filtered = ByteBuffer.allocate(2048)
        val records = MemoryRecords.readableRecords(buffer)
        val filterResult = records.filterTo(
            partition = TopicPartition("foo", 0),
            filter = object : RecordFilter(0, 0) {
                // retain all batches
                override fun checkBatchRetention(batch: RecordBatch): BatchRetentionResult =
                    BatchRetentionResult(BatchRetention.RETAIN_EMPTY, false)

                override fun shouldRetainRecord(recordBatch: RecordBatch, record: Record): Boolean = false
            },
            destinationBuffer = filtered,
            maxRecordBatchSize = Int.MAX_VALUE,
            decompressionBufferSupplier = BufferSupplier.NO_CACHING,
        )

        // Verify filter result
        assertEquals(0, filterResult.messagesRead)
        assertEquals(records.sizeInBytes(), filterResult.bytesRead)
        assertEquals(baseOffset, filterResult.maxOffset)
        assertEquals(0, filterResult.messagesRetained)
        assertEquals(DefaultRecordBatch.RECORD_BATCH_OVERHEAD, filterResult.bytesRetained)
        assertEquals(timestamp, filterResult.maxTimestamp)
        assertEquals(baseOffset, filterResult.shallowOffsetOfMaxTimestamp)
        assertTrue(filterResult.outputBuffer().position() > 0)

        // Verify filtered records
        filtered.flip()
        val filteredRecords = MemoryRecords.readableRecords(filtered)
        assertEquals(DefaultRecordBatch.RECORD_BATCH_OVERHEAD, filteredRecords.sizeInBytes())
    }

    @Test
    fun testEmptyBatchDeletion() {
        for (deleteRetention in listOf(BatchRetention.DELETE, BatchRetention.DELETE_EMPTY)) {
            val buffer = ByteBuffer.allocate(DefaultRecordBatch.RECORD_BATCH_OVERHEAD)
            val producerId = 23L
            val producerEpoch: Short = 5
            val baseOffset = 3L
            val baseSequence = 10
            val partitionLeaderEpoch = 293
            val timestamp = System.currentTimeMillis()
            DefaultRecordBatch.writeEmptyHeader(
                buffer = buffer,
                magic = RecordBatch.MAGIC_VALUE_V2,
                producerId = producerId,
                producerEpoch = producerEpoch,
                baseSequence = baseSequence,
                baseOffset = baseOffset,
                lastOffset = baseOffset,
                partitionLeaderEpoch = partitionLeaderEpoch,
                timestampType = TimestampType.CREATE_TIME,
                timestamp = timestamp,
                isTransactional = false,
                isControlRecord = false,
            )
            buffer.flip()
            val filtered = ByteBuffer.allocate(2048)
            val records = MemoryRecords.readableRecords(buffer)
            val filterResult = records.filterTo(
                partition = TopicPartition("foo", 0),
                filter = object : RecordFilter(0, 0) {
                    override fun checkBatchRetention(batch: RecordBatch): BatchRetentionResult =
                        BatchRetentionResult(deleteRetention, false)

                    override fun shouldRetainRecord(recordBatch: RecordBatch, record: Record): Boolean = false
                },
                destinationBuffer = filtered,
                maxRecordBatchSize = Int.MAX_VALUE,
                decompressionBufferSupplier = BufferSupplier.NO_CACHING,
            )

            // Verify filter result
            assertEquals(0, filterResult.outputBuffer().position())

            // Verify filtered records
            filtered.flip()
            val filteredRecords = MemoryRecords.readableRecords(filtered)
            assertEquals(0, filteredRecords.sizeInBytes())
        }
    }

    @Test
    fun testBuildEndTxnMarker() {
        val producerId: Long = 73
        val producerEpoch: Short = 13
        val initialOffset = 983L
        val coordinatorEpoch = 347
        val partitionLeaderEpoch = 29
        val marker = EndTransactionMarker(ControlRecordType.COMMIT, coordinatorEpoch)
        val records = MemoryRecords.withEndTransactionMarker(
            initialOffset = initialOffset,
            timestamp = System.currentTimeMillis(),
            partitionLeaderEpoch = partitionLeaderEpoch,
            producerId = producerId,
            producerEpoch = producerEpoch,
            marker = marker,
        )
        // verify that buffer allocation was precise
        assertEquals(records.buffer().remaining(), records.buffer().capacity())
        val batches = records.batches().toList()
        assertEquals(1, batches.size)
        val batch: RecordBatch = batches[0]
        assertTrue(batch.isControlBatch)
        assertEquals(producerId, batch.producerId())
        assertEquals(producerEpoch, batch.producerEpoch())
        assertEquals(initialOffset, batch.baseOffset())
        assertEquals(partitionLeaderEpoch, batch.partitionLeaderEpoch())
        assertTrue(batch.isValid)
        val createdRecords = batch.toList()
        assertEquals(1, createdRecords.size)
        val record = createdRecords[0]
        record.ensureValid()
        val deserializedMarker = EndTransactionMarker.deserialize(record)
        assertEquals(ControlRecordType.COMMIT, deserializedMarker.controlType)
        assertEquals(coordinatorEpoch, deserializedMarker.coordinatorEpoch)
    }

    /**
     * This test is used to see if the base timestamp of the batch has been successfully
     * converted to a delete horizon for the tombstones / transaction markers of the batch.
     * It also verifies that the record timestamps remain correct as a delta relative to the delete horizon.
     */
    @ParameterizedTest
    @ArgumentsSource(V2MemoryRecordsArgumentsProvider::class)
    fun testBaseTimestampToDeleteHorizonConversion(args: Args) {
        val partitionLeaderEpoch = 998
        val buffer = ByteBuffer.allocate(2048)
        val builder = MemoryRecords.builder(
            buffer = buffer,
            magic = args.magic,
            compressionType = args.compression,
            timestampType = TimestampType.CREATE_TIME,
            baseOffset = 0L,
            logAppendTime = RecordBatch.NO_TIMESTAMP,
            partitionLeaderEpoch = partitionLeaderEpoch,
        )
        builder.append(5L, "0".toByteArray(), "0".toByteArray())
        builder.append(10L, "1".toByteArray(), null)
        builder.append(15L, "2".toByteArray(), "2".toByteArray())
        val filtered = ByteBuffer.allocate(2048)
        val deleteHorizon = (Int.MAX_VALUE / 2).toLong()
        val recordFilter = object : RecordFilter(deleteHorizon - 1, 1) {
            override fun shouldRetainRecord(recordBatch: RecordBatch, record: Record): Boolean = true

            override fun checkBatchRetention(batch: RecordBatch): BatchRetentionResult =
                BatchRetentionResult(BatchRetention.RETAIN_EMPTY, false)
        }
        builder.build()
            .filterTo(
                partition = TopicPartition("random", 0),
                filter = recordFilter,
                destinationBuffer = filtered,
                maxRecordBatchSize = Int.MAX_VALUE,
                decompressionBufferSupplier = BufferSupplier.NO_CACHING,
            )
        filtered.flip()
        val filteredRecords = MemoryRecords.readableRecords(filtered)
        val batches = filteredRecords.batches().toList()
        assertEquals(1, batches.size)
        assertEquals(deleteHorizon, batches[0].deleteHorizonMs())
        val recordIterator = batches[0].streamingIterator(BufferSupplier.create())
        var record = recordIterator.next()
        assertEquals(5L, record.timestamp())
        record = recordIterator.next()
        assertEquals(10L, record.timestamp())
        record = recordIterator.next()
        assertEquals(15L, record.timestamp())
        recordIterator.close()
    }

    @Test
    fun testBuildLeaderChangeMessage() {
        val leaderId = 5
        val leaderEpoch = 20
        val voterId = 6
        val initialOffset = 983L
        val leaderChangeMessage = LeaderChangeMessage()
            .setLeaderId(leaderId)
            .setVoters(listOf(Voter().setVoterId(voterId)))
        val buffer = ByteBuffer.allocate(256)
        val records = MemoryRecords.withLeaderChangeMessage(
            initialOffset,
            System.currentTimeMillis(),
            leaderEpoch,
            buffer,
            leaderChangeMessage
        )
        val batches = records.batches().toList()
        assertEquals(1, batches.size)
        val batch: RecordBatch = batches[0]
        assertTrue(batch.isControlBatch)
        assertEquals(initialOffset, batch.baseOffset())
        assertEquals(leaderEpoch, batch.partitionLeaderEpoch())
        assertTrue(batch.isValid)
        val createdRecords = batch.toList()
        assertEquals(1, createdRecords.size)
        val record = createdRecords[0]
        record.ensureValid()
        assertEquals(ControlRecordType.LEADER_CHANGE, ControlRecordType.parse(record.key()!!))
        val deserializedMessage = deserializeLeaderChangeMessage(record)
        assertEquals(leaderId, deserializedMessage.leaderId)
        assertEquals(1, deserializedMessage.voters.size)
        assertEquals(voterId, deserializedMessage.voters[0].voterId)
    }

    @ParameterizedTest
    @ArgumentsSource(MemoryRecordsArgumentsProvider::class)
    fun testFilterToBatchDiscard(args: Args) {
        val compression = args.compression
        val magic = args.magic
        val buffer = ByteBuffer.allocate(2048)
        var builder = MemoryRecords.builder(
            buffer = buffer,
            magic = magic,
            compressionType = compression,
            timestampType = TimestampType.CREATE_TIME,
            baseOffset = 0L,
        )
        builder.append(10L, "1".toByteArray(), "a".toByteArray())
        builder.close()
        builder = MemoryRecords.builder(
            buffer = buffer,
            magic = magic,
            compressionType = compression,
            timestampType = TimestampType.CREATE_TIME,
            baseOffset = 1L,
        )
        builder.append(11L, "2".toByteArray(), "b".toByteArray())
        builder.append(12L, "3".toByteArray(), "c".toByteArray())
        builder.close()
        builder = MemoryRecords.builder(
            buffer = buffer,
            magic = magic,
            compressionType = compression,
            timestampType = TimestampType.CREATE_TIME,
            baseOffset = 3L,
        )
        builder.append(13L, "4".toByteArray(), "d".toByteArray())
        builder.append(20L, "5".toByteArray(), "e".toByteArray())
        builder.append(15L, "6".toByteArray(), "f".toByteArray())
        builder.close()
        builder = MemoryRecords.builder(
            buffer = buffer,
            magic = magic,
            compressionType = compression,
            timestampType = TimestampType.CREATE_TIME,
            baseOffset = 6L,
        )
        builder.append(16L, "7".toByteArray(), "g".toByteArray())
        builder.close()
        buffer.flip()
        val filtered = ByteBuffer.allocate(2048)
        MemoryRecords.readableRecords(buffer).filterTo(
            partition = TopicPartition("foo", 0),
            filter = object : RecordFilter(0, 0) {
                override fun checkBatchRetention(batch: RecordBatch): BatchRetentionResult {
                    // discard the second and fourth batches
                    return if (batch.lastOffset() == 2L || batch.lastOffset() == 6L)
                        BatchRetentionResult(BatchRetention.DELETE, false)
                    else BatchRetentionResult(BatchRetention.DELETE_EMPTY, false)
                }

                override fun shouldRetainRecord(recordBatch: RecordBatch, record: Record): Boolean = true
            },
            destinationBuffer = filtered,
            maxRecordBatchSize = Int.MAX_VALUE,
            decompressionBufferSupplier = BufferSupplier.NO_CACHING,
        )
        filtered.flip()
        val filteredRecords = MemoryRecords.readableRecords(filtered)
        val batches = filteredRecords.batches().toList()
        if (compression !== CompressionType.NONE || magic >= RecordBatch.MAGIC_VALUE_V2) {
            assertEquals(2, batches.size)
            assertEquals(0, batches[0].lastOffset())
            assertEquals(5, batches[1].lastOffset())
        } else {
            assertEquals(5, batches.size)
            assertEquals(0, batches[0].lastOffset())
            assertEquals(1, batches[1].lastOffset())
        }
    }

    @ParameterizedTest
    @ArgumentsSource(MemoryRecordsArgumentsProvider::class)
    fun testFilterToAlreadyCompactedLog(args: Args) {
        val magic = args.magic
        val compression = args.compression
        val buffer = ByteBuffer.allocate(2048)

        // create a batch with some offset gaps to simulate a compacted batch
        val builder = MemoryRecords.builder(
            buffer, magic, compression,
            TimestampType.CREATE_TIME, 0L
        )
        builder.appendWithOffset(5L, 10L, null, "a".toByteArray())
        builder.appendWithOffset(8L, 11L, "1".toByteArray(), "b".toByteArray())
        builder.appendWithOffset(10L, 12L, null, "c".toByteArray())
        builder.close()
        buffer.flip()
        val filtered = ByteBuffer.allocate(2048)
        MemoryRecords.readableRecords(buffer).filterTo(
            partition = TopicPartition("foo", 0),
            filter = RetainNonNullKeysFilter(),
            destinationBuffer = filtered,
            maxRecordBatchSize = Int.MAX_VALUE,
            decompressionBufferSupplier = BufferSupplier.NO_CACHING,
        )
        filtered.flip()
        val filteredRecords = MemoryRecords.readableRecords(filtered)
        val batches = filteredRecords.batches().toList()
        assertEquals(1, batches.size)
        val batch = batches[0]
        val records = batch.toList()
        assertEquals(1, records.size)
        assertEquals(8L, records[0].offset())
        if (magic >= RecordBatch.MAGIC_VALUE_V1) assertEquals(
            expected = SimpleRecord(11L, "1".toByteArray(), "b".toByteArray()),
            actual = SimpleRecord(records[0]),
        ) else assertEquals(
            expected = SimpleRecord(RecordBatch.NO_TIMESTAMP, "1".toByteArray(), "b".toByteArray()),
            actual = SimpleRecord(records[0]),
        )
        if (magic >= RecordBatch.MAGIC_VALUE_V2) {
            // the new format preserves first and last offsets from the original batch
            assertEquals(0L, batch.baseOffset())
            assertEquals(10L, batch.lastOffset())
        } else {
            assertEquals(8L, batch.baseOffset())
            assertEquals(8L, batch.lastOffset())
        }
    }

    @ParameterizedTest
    @ArgumentsSource(MemoryRecordsArgumentsProvider::class)
    fun testFilterToPreservesProducerInfo(args: Args) {
        val magic = args.magic
        val compression = args.compression
        val buffer = ByteBuffer.allocate(2048)

        // non-idempotent, non-transactional
        var builder = MemoryRecords.builder(
            buffer = buffer,
            magic = magic,
            compressionType = compression,
            timestampType = TimestampType.CREATE_TIME,
            baseOffset = 0L,
        )
        builder.append(10L, null, "a".toByteArray())
        builder.append(11L, "1".toByteArray(), "b".toByteArray())
        builder.append(12L, null, "c".toByteArray())
        builder.close()

        // idempotent
        val pid1 = 23L
        val epoch1: Short = 5
        val baseSequence1 = 10
        val idempotentBuilder = MemoryRecords.builder(
            buffer = buffer,
            magic = magic,
            compressionType = compression,
            timestampType = TimestampType.CREATE_TIME,
            baseOffset = 3L,
            logAppendTime = RecordBatch.NO_TIMESTAMP,
            producerId = pid1,
            producerEpoch = epoch1,
            baseSequence = baseSequence1,
        )
        idempotentBuilder.append(13L, null, "d".toByteArray())
        idempotentBuilder.append(14L, "4".toByteArray(), "e".toByteArray())
        idempotentBuilder.append(15L, "5".toByteArray(), "f".toByteArray())
        if (magic < RecordBatch.MAGIC_VALUE_V2) assertFailsWith<IllegalArgumentException> { idempotentBuilder.close() }
        else idempotentBuilder.close()


        // transactional
        val pid2 = 99384L
        val epoch2: Short = 234
        val baseSequence2 = 15
        val transactionSupplier = {
            MemoryRecords.builder(
                buffer = buffer,
                magic = magic,
                compressionType = compression,
                timestampType = TimestampType.CREATE_TIME,
                baseOffset = 3L,
                logAppendTime = RecordBatch.NO_TIMESTAMP,
                producerId = pid2,
                producerEpoch = epoch2,
                baseSequence = baseSequence2,
                isTransactional = true,
                partitionLeaderEpoch = RecordBatch.NO_PARTITION_LEADER_EPOCH,
            )
        }
        if (magic < RecordBatch.MAGIC_VALUE_V2) assertFailsWith<IllegalArgumentException> { transactionSupplier() }
        else {
            builder = transactionSupplier()
            builder.append(16L, "6".toByteArray(), "g".toByteArray())
            builder.append(17L, "7".toByteArray(), "h".toByteArray())
            builder.append(18L, null, "i".toByteArray())
            builder.close()
            buffer.flip()
            val filtered = ByteBuffer.allocate(2048)
            MemoryRecords.readableRecords(buffer).filterTo(
                partition = TopicPartition("foo", 0),
                filter = RetainNonNullKeysFilter(),
                destinationBuffer = filtered,
                maxRecordBatchSize = Int.MAX_VALUE,
                decompressionBufferSupplier = BufferSupplier.NO_CACHING,
            )
            filtered.flip()
            val filteredRecords = MemoryRecords.readableRecords(filtered)
            val batches = filteredRecords.batches().toList()
            assertEquals(3, batches.size)
            val firstBatch = batches[0]
            assertEquals(1, firstBatch.countOrNull())
            assertEquals(0L, firstBatch.baseOffset())
            assertEquals(2L, firstBatch.lastOffset())
            assertEquals(RecordBatch.NO_PRODUCER_ID, firstBatch.producerId())
            assertEquals(RecordBatch.NO_PRODUCER_EPOCH, firstBatch.producerEpoch())
            assertEquals(RecordBatch.NO_SEQUENCE, firstBatch.baseSequence())
            assertEquals(RecordBatch.NO_SEQUENCE, firstBatch.lastSequence())
            assertFalse(firstBatch.isTransactional)
            val firstBatchRecords = firstBatch.toList()
            assertEquals(1, firstBatchRecords.size)
            assertEquals(RecordBatch.NO_SEQUENCE, firstBatchRecords[0].sequence())
            assertEquals(
                expected = SimpleRecord(11L, "1".toByteArray(), "b".toByteArray()),
                actual = SimpleRecord(firstBatchRecords[0]),
            )
            val secondBatch = batches[1]
            assertEquals(2, secondBatch.countOrNull())
            assertEquals(3L, secondBatch.baseOffset())
            assertEquals(5L, secondBatch.lastOffset())
            assertEquals(pid1, secondBatch.producerId())
            assertEquals(epoch1, secondBatch.producerEpoch())
            assertEquals(baseSequence1, secondBatch.baseSequence())
            assertEquals(baseSequence1 + 2, secondBatch.lastSequence())
            assertFalse(secondBatch.isTransactional)
            val secondBatchRecords = secondBatch.toList()
            assertEquals(2, secondBatchRecords.size)
            assertEquals(baseSequence1 + 1, secondBatchRecords[0].sequence())
            assertEquals(
                expected = SimpleRecord(14L, "4".toByteArray(), "e".toByteArray()),
                actual = SimpleRecord(secondBatchRecords[0]),
            )
            assertEquals(baseSequence1 + 2, secondBatchRecords[1].sequence())
            assertEquals(
                expected = SimpleRecord(15L, "5".toByteArray(), "f".toByteArray()),
                actual = SimpleRecord(secondBatchRecords[1]),
            )
            val thirdBatch = batches[2]
            assertEquals(2, thirdBatch.countOrNull())
            assertEquals(3L, thirdBatch.baseOffset())
            assertEquals(5L, thirdBatch.lastOffset())
            assertEquals(pid2, thirdBatch.producerId())
            assertEquals(epoch2, thirdBatch.producerEpoch())
            assertEquals(baseSequence2, thirdBatch.baseSequence())
            assertEquals(baseSequence2 + 2, thirdBatch.lastSequence())
            assertTrue(thirdBatch.isTransactional)
            val thirdBatchRecords = thirdBatch.toList()
            assertEquals(2, thirdBatchRecords.size)
            assertEquals(baseSequence2, thirdBatchRecords[0].sequence())
            assertEquals(
                expected = SimpleRecord(16L, "6".toByteArray(), "g".toByteArray()),
                actual = SimpleRecord(thirdBatchRecords[0]),
            )
            assertEquals(baseSequence2 + 1, thirdBatchRecords[1].sequence())
            assertEquals(
                expected = SimpleRecord(17L, "7".toByteArray(), "h".toByteArray()),
                actual = SimpleRecord(thirdBatchRecords[1]),
            )
        }
    }

    @ParameterizedTest
    @ArgumentsSource(MemoryRecordsArgumentsProvider::class)
    fun testFilterToWithUndersizedBuffer(args: Args) {
        val magic = args.magic
        val compression = args.compression
        val buffer = ByteBuffer.allocate(1024)
        var builder = MemoryRecords.builder(
            buffer = buffer,
            magic = magic,
            compressionType = compression,
            timestampType = TimestampType.CREATE_TIME,
            baseOffset = 0L,
        )
        builder.append(10L, null, "a".toByteArray())
        builder.close()
        builder = MemoryRecords.builder(
            buffer = buffer,
            magic = magic,
            compressionType = compression,
            timestampType = TimestampType.CREATE_TIME,
            baseOffset = 1L,
        )
        builder.append(11L, "1".toByteArray(), ByteArray(128))
        builder.append(12L, "2".toByteArray(), "c".toByteArray())
        builder.append(13L, null, "d".toByteArray())
        builder.close()
        builder = MemoryRecords.builder(
            buffer = buffer,
            magic = magic,
            compressionType = compression,
            timestampType = TimestampType.CREATE_TIME,
            baseOffset = 4L,
        )
        builder.append(14L, null, "e".toByteArray())
        builder.append(15L, "5".toByteArray(), "f".toByteArray())
        builder.append(16L, "6".toByteArray(), "g".toByteArray())
        builder.close()
        builder = MemoryRecords.builder(
            buffer = buffer,
            magic = magic,
            compressionType = compression,
            timestampType = TimestampType.CREATE_TIME,
            baseOffset = 7L,
        )
        builder.append(17L, "7".toByteArray(), ByteArray(128))
        builder.close()
        buffer.flip()
        val output = ByteBuffer.allocate(64)
        val records = mutableListOf<Record>()
        while (buffer.hasRemaining()) {
            output.rewind()
            val result = MemoryRecords.readableRecords(buffer)
                .filterTo(
                    partition = TopicPartition("foo", 0),
                    filter = RetainNonNullKeysFilter(),
                    destinationBuffer = output,
                    maxRecordBatchSize = Int.MAX_VALUE,
                    decompressionBufferSupplier = BufferSupplier.NO_CACHING,
                )
            buffer.position(buffer.position() + result.bytesRead)
            result.outputBuffer().flip()
            if (output !== result.outputBuffer()) assertEquals(0, output.position())
            val filtered = MemoryRecords.readableRecords(result.outputBuffer())
            records.addAll(filtered.records().toList())
        }
        assertEquals(5, records.size)
        for (record in records) assertNotNull(record.key())
    }

    @ParameterizedTest
    @ArgumentsSource(MemoryRecordsArgumentsProvider::class)
    fun testFilterTo(args: Args) {
        val magic = args.magic
        val compression = args.compression
        val buffer = ByteBuffer.allocate(2048)
        var builder = MemoryRecords.builder(
            buffer = buffer,
            magic = magic,
            compressionType = compression,
            timestampType = TimestampType.CREATE_TIME,
            baseOffset = 0L,
        )
        builder.append(10L, null, "a".toByteArray())
        builder.close()
        builder = MemoryRecords.builder(
            buffer = buffer,
            magic = magic,
            compressionType = compression,
            timestampType = TimestampType.CREATE_TIME,
            baseOffset = 1L,
        )
        builder.append(11L, "1".toByteArray(), "b".toByteArray())
        builder.append(12L, null, "c".toByteArray())
        builder.close()
        builder = MemoryRecords.builder(
            buffer = buffer,
            magic = magic,
            compressionType = compression,
            timestampType = TimestampType.CREATE_TIME,
            baseOffset = 3L,
        )
        builder.append(13L, null, "d".toByteArray())
        builder.append(20L, "4".toByteArray(), "e".toByteArray())
        builder.append(15L, "5".toByteArray(), "f".toByteArray())
        builder.close()
        builder = MemoryRecords.builder(
            buffer = buffer,
            magic = magic,
            compressionType = compression,
            timestampType = TimestampType.CREATE_TIME,
            baseOffset = 6L,
        )
        builder.append(16L, "6".toByteArray(), "g".toByteArray())
        builder.close()
        buffer.flip()
        val filtered = ByteBuffer.allocate(2048)
        val result = MemoryRecords.readableRecords(buffer).filterTo(
            partition = TopicPartition("foo", 0),
            filter = RetainNonNullKeysFilter(),
            destinationBuffer = filtered,
            maxRecordBatchSize = Int.MAX_VALUE,
            decompressionBufferSupplier = BufferSupplier.NO_CACHING,
        )
        filtered.flip()
        assertEquals(7, result.messagesRead)
        assertEquals(4, result.messagesRetained)
        assertEquals(buffer.limit(), result.bytesRead)
        assertEquals(filtered.limit(), result.bytesRetained)
        if (magic > RecordBatch.MAGIC_VALUE_V0) {
            assertEquals(20L, result.maxTimestamp)
            if (compression === CompressionType.NONE && magic < RecordBatch.MAGIC_VALUE_V2)
                assertEquals(4L, result.shallowOffsetOfMaxTimestamp)
            else assertEquals(5L, result.shallowOffsetOfMaxTimestamp)
        }
        val filteredRecords = MemoryRecords.readableRecords(filtered)
        val batches = filteredRecords.batches().toList()
        val expectedEndOffsets: List<Long>
        val expectedStartOffsets: List<Long>
        val expectedMaxTimestamps: List<Long>
        if (magic < RecordBatch.MAGIC_VALUE_V2 && compression === CompressionType.NONE) {
            expectedEndOffsets = mutableListOf(1L, 4L, 5L, 6L)
            expectedStartOffsets = mutableListOf(1L, 4L, 5L, 6L)
            expectedMaxTimestamps = mutableListOf(11L, 20L, 15L, 16L)
        } else if (magic < RecordBatch.MAGIC_VALUE_V2) {
            expectedEndOffsets = mutableListOf(1L, 5L, 6L)
            expectedStartOffsets = mutableListOf(1L, 4L, 6L)
            expectedMaxTimestamps = mutableListOf(11L, 20L, 16L)
        } else {
            expectedEndOffsets = mutableListOf(2L, 5L, 6L)
            expectedStartOffsets = mutableListOf(1L, 3L, 6L)
            expectedMaxTimestamps = mutableListOf(11L, 20L, 16L)
        }
        assertEquals(expectedEndOffsets.size, batches.size)
        for (i in expectedEndOffsets.indices) {
            val batch: RecordBatch = batches[i]
            assertEquals(expectedStartOffsets[i], batch.baseOffset())
            assertEquals(expectedEndOffsets[i], batch.lastOffset())
            assertEquals(magic, batch.magic())
            assertEquals(compression, batch.compressionType())
            if (magic >= RecordBatch.MAGIC_VALUE_V1) {
                assertEquals(expectedMaxTimestamps[i], batch.maxTimestamp())
                assertEquals(TimestampType.CREATE_TIME, batch.timestampType())
            } else {
                assertEquals(RecordBatch.NO_TIMESTAMP, batch.maxTimestamp())
                assertEquals(TimestampType.NO_TIMESTAMP_TYPE, batch.timestampType())
            }
        }
        val records = filteredRecords.records().toList()
        assertEquals(4, records.size)
        val first = records[0]
        assertEquals(1L, first.offset())
        if (magic > RecordBatch.MAGIC_VALUE_V0) assertEquals(11L, first.timestamp())
        assertEquals("1", utf8(first.key()!!, first.keySize()))
        assertEquals("b", utf8(first.value()!!, first.valueSize()))
        val second = records[1]
        assertEquals(4L, second.offset())
        if (magic > RecordBatch.MAGIC_VALUE_V0) assertEquals(20L, second.timestamp())
        assertEquals("4", utf8(second.key()!!, second.keySize()))
        assertEquals("e", utf8(second.value()!!, second.valueSize()))
        val third = records[2]
        assertEquals(5L, third.offset())
        if (magic > RecordBatch.MAGIC_VALUE_V0) assertEquals(15L, third.timestamp())
        assertEquals("5", utf8(third.key()!!, third.keySize()))
        assertEquals("f", utf8(third.value()!!, third.valueSize()))
        val fourth = records[3]
        assertEquals(6L, fourth.offset())
        if (magic > RecordBatch.MAGIC_VALUE_V0) assertEquals(16L, fourth.timestamp())
        assertEquals("6", utf8(fourth.key()!!, fourth.keySize()))
        assertEquals("g", utf8(fourth.value()!!, fourth.valueSize()))
    }

    @ParameterizedTest
    @ArgumentsSource(MemoryRecordsArgumentsProvider::class)
    fun testFilterToPreservesLogAppendTime(args: Args) {
        val magic = args.magic
        val compression = args.compression
        val pid = args.pid
        val epoch = args.epoch
        val firstSequence = args.firstSequence
        val logAppendTime = System.currentTimeMillis()
        val buffer = ByteBuffer.allocate(2048)
        var builder = MemoryRecords.builder(
            buffer = buffer,
            magic = magic,
            compressionType = compression,
            timestampType = TimestampType.LOG_APPEND_TIME,
            baseOffset = 0L,
            logAppendTime = logAppendTime,
            producerId = pid,
            producerEpoch = epoch,
            baseSequence = firstSequence,
        )
        builder.append(10L, null, "a".toByteArray())
        builder.close()
        builder = MemoryRecords.builder(
            buffer = buffer,
            magic = magic,
            compressionType = compression,
            timestampType = TimestampType.LOG_APPEND_TIME,
            baseOffset = 1L,
            logAppendTime = logAppendTime,
            producerId = pid,
            producerEpoch = epoch,
            baseSequence = firstSequence,
        )
        builder.append(11L, "1".toByteArray(), "b".toByteArray())
        builder.append(12L, null, "c".toByteArray())
        builder.close()
        builder = MemoryRecords.builder(
            buffer = buffer,
            magic = magic,
            compressionType = compression,
            timestampType = TimestampType.LOG_APPEND_TIME,
            baseOffset = 3L,
            logAppendTime = logAppendTime,
            producerId = pid,
            producerEpoch = epoch,
            baseSequence = firstSequence,
        )
        builder.append(13L, null, "d".toByteArray())
        builder.append(14L, "4".toByteArray(), "e".toByteArray())
        builder.append(15L, "5".toByteArray(), "f".toByteArray())
        builder.close()
        buffer.flip()
        val filtered = ByteBuffer.allocate(2048)
        MemoryRecords.readableRecords(buffer).filterTo(
            partition = TopicPartition("foo", 0),
            filter = RetainNonNullKeysFilter(),
            destinationBuffer = filtered,
            maxRecordBatchSize = Int.MAX_VALUE,
            decompressionBufferSupplier = BufferSupplier.NO_CACHING,
        )
        filtered.flip()
        val filteredRecords = MemoryRecords.readableRecords(filtered)
        val batches = filteredRecords.batches().toList()
        assertEquals(
            expected = if (magic < RecordBatch.MAGIC_VALUE_V2 && compression === CompressionType.NONE) 3 else 2,
            actual = batches.size,
        )
        for (batch in batches) {
            assertEquals(compression, batch.compressionType())
            if (magic > RecordBatch.MAGIC_VALUE_V0) {
                assertEquals(TimestampType.LOG_APPEND_TIME, batch.timestampType())
                assertEquals(logAppendTime, batch.maxTimestamp())
            }
        }
    }

    @ParameterizedTest
    @ArgumentsSource(MemoryRecordsArgumentsProvider::class)
    fun testNextBatchSize(args: Args) {
        val buffer = ByteBuffer.allocate(2048)
        val builder = MemoryRecords.builder(
            buffer = buffer,
            magic = args.magic,
            compressionType = args.compression,
            timestampType = TimestampType.LOG_APPEND_TIME,
            baseOffset = 0L,
            logAppendTime = logAppendTime,
            producerId = args.pid,
            producerEpoch = args.epoch,
            baseSequence = args.firstSequence,
        )
        builder.append(10L, null, "abc".toByteArray())
        builder.close()
        buffer.flip()
        val size = buffer.remaining()
        val records = MemoryRecords.readableRecords(buffer)
        assertEquals(size, records.firstBatchSize())
        assertEquals(0, buffer.position())
        buffer.limit(1) // size not in buffer
        assertNull(records.firstBatchSize())
        buffer.limit(Records.LOG_OVERHEAD) // magic not in buffer
        assertNull(records.firstBatchSize())
        buffer.limit(Records.HEADER_SIZE_UP_TO_MAGIC) // payload not in buffer
        assertEquals(size, records.firstBatchSize())
        buffer.limit(size)
        val magic = buffer[Records.MAGIC_OFFSET]
        buffer.put(Records.MAGIC_OFFSET, 10.toByte())
        assertFailsWith<CorruptRecordException> { records.firstBatchSize() }
        buffer.put(Records.MAGIC_OFFSET, magic)
        buffer.put(Records.SIZE_OFFSET + 3, 0.toByte())
        assertFailsWith<CorruptRecordException> { records.firstBatchSize() }
    }

    @ParameterizedTest
    @ArgumentsSource(MemoryRecordsArgumentsProvider::class)
    fun testWithRecords(args: Args) {
        val compression = args.compression
        val magic = args.magic
        val memoryRecords = MemoryRecords.withRecords(
            magic = magic,
            compressionType = compression,
            records = arrayOf(SimpleRecord(10L, "key1".toByteArray(), "value1".toByteArray())),
        )
        val key = utf8(memoryRecords.batches().first().first().key()!!)
        assertEquals("key1", key)
    }

    @Test
    fun testUnsupportedCompress() {
        val builderBiFunction = { magic: Byte, compressionType: CompressionType ->
                MemoryRecords.withRecords(
                    magic = magic,
                    compressionType = compressionType,
                    records = arrayOf(SimpleRecord(10L, "key1".toByteArray(), "value1".toByteArray())),
                )
            }
        listOf(RecordBatch.MAGIC_VALUE_V0, RecordBatch.MAGIC_VALUE_V1).forEach { magic: Byte ->
            val e = assertFailsWith<IllegalArgumentException> { builderBiFunction(magic, CompressionType.ZSTD) }
            assertEquals(e.message, "ZStandard compression is not supported for magic $magic")
        }
    }
    
    private class RetainNonNullKeysFilter : RecordFilter(0, 0) {
        override fun checkBatchRetention(batch: RecordBatch) = BatchRetentionResult(
            batchRetention = BatchRetention.DELETE_EMPTY,
            containsMarkerForEmptyTxn = false,
        )

        override fun shouldRetainRecord(batch: RecordBatch, record: Record): Boolean = record.hasKey()
    }

    class Args(
        val magic: Byte,
        val firstOffset: Long,
        val compression: CompressionType,
    ) {
        var pid: Long = 0
        var epoch: Short = 0
        var firstSequence = 0

        init {
            if (magic >= RecordBatch.MAGIC_VALUE_V2) {
                pid = 134234L
                epoch = 28
                firstSequence = 777
            } else {
                pid = RecordBatch.NO_PRODUCER_ID
                epoch = RecordBatch.NO_PRODUCER_EPOCH
                firstSequence = RecordBatch.NO_SEQUENCE
            }
        }

        override fun toString(): String = "magic=$magic, firstOffset=$firstOffset, compressionType=$compression"
    }
    
    class MemoryRecordsArgumentsProvider : ArgumentsProvider {

        override fun provideArguments(context: ExtensionContext): Stream<out Arguments> {
            val arguments = mutableListOf<Arguments>()
            for (firstOffset in listOf(0L, 57L))
                for (type in CompressionType.entries) {
                    val magics =
                        if (type === CompressionType.ZSTD) listOf(RecordBatch.MAGIC_VALUE_V2)
                        else listOf(
                            RecordBatch.MAGIC_VALUE_V0,
                            RecordBatch.MAGIC_VALUE_V1,
                            RecordBatch.MAGIC_VALUE_V2
                        )
                    for (magic in magics) arguments.add(Arguments.of(Args(magic, firstOffset, type)))
                }
            return arguments.stream()
        }
    }

    class V2MemoryRecordsArgumentsProvider : ArgumentsProvider {
        override fun provideArguments(context: ExtensionContext): Stream<out Arguments> {
            val arguments: MutableList<Arguments> = ArrayList()
            for (firstOffset in listOf(0L, 57L))
                for (type in CompressionType.entries)
                    arguments.add(Arguments.of(Args(RecordBatch.MAGIC_VALUE_V2, firstOffset, type)))

            return arguments.stream()
        }
    }
}
