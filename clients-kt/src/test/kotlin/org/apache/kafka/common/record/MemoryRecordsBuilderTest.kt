package org.apache.kafka.common.record

import java.nio.ByteBuffer
import java.util.stream.Stream
import org.apache.kafka.common.errors.UnsupportedCompressionTypeException
import org.apache.kafka.common.message.LeaderChangeMessage
import org.apache.kafka.common.message.LeaderChangeMessage.Voter
import org.apache.kafka.common.record.ControlRecordUtils.deserializeLeaderChangeMessage
import org.apache.kafka.common.utils.BufferSupplier
import org.apache.kafka.common.utils.ByteBufferOutputStream
import org.apache.kafka.common.utils.Time
import org.apache.kafka.common.utils.Utils.utf8
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtensionContext
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.ArgumentsProvider
import org.junit.jupiter.params.provider.ArgumentsSource
import org.junit.jupiter.params.provider.EnumSource
import kotlin.random.Random
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

class MemoryRecordsBuilderTest {

    private val time = Time.SYSTEM

    @Test
    fun testUnsupportedCompress() {
        val builderBiFunction: (Byte, CompressionType) -> Unit = { magic, compressionType ->
            MemoryRecordsBuilder(
                buffer = ByteBuffer.allocate(128),
                magic = magic,
                compressionType = compressionType,
                timestampType = TimestampType.CREATE_TIME,
                baseOffset = 0L,
                logAppendTime = 0L,
                producerId = RecordBatch.NO_PRODUCER_ID,
                producerEpoch = RecordBatch.NO_PRODUCER_EPOCH,
                baseSequence = RecordBatch.NO_SEQUENCE,
                isTransactional = false,
                isControlBatch = false,
                partitionLeaderEpoch = RecordBatch.NO_PARTITION_LEADER_EPOCH,
                writeLimit = 128,
            )
        }
        listOf(RecordBatch.MAGIC_VALUE_V0, RecordBatch.MAGIC_VALUE_V1).forEach { magic ->
            val e = assertFailsWith<IllegalArgumentException> {
                builderBiFunction(magic, CompressionType.ZSTD)
            }
            assertEquals(e.message, "ZStandard compression is not supported for magic $magic")
        }
    }

    @ParameterizedTest
    @ArgumentsSource(MemoryRecordsBuilderArgumentsProvider::class)
    fun testWriteEmptyRecordSet(args: Args) {
        val magic = args.magic
        val buffer = allocateBuffer(128, args)
        val records = MemoryRecordsBuilder(
            buffer = buffer,
            magic = magic,
            compressionType = args.compressionType,
            timestampType = TimestampType.CREATE_TIME,
            baseOffset = 0L,
            logAppendTime = 0L,
            producerId = RecordBatch.NO_PRODUCER_ID,
            producerEpoch = RecordBatch.NO_PRODUCER_EPOCH,
            baseSequence = RecordBatch.NO_SEQUENCE,
            isTransactional = false,
            isControlBatch = false,
            partitionLeaderEpoch = RecordBatch.NO_PARTITION_LEADER_EPOCH,
            writeLimit = buffer.capacity(),
        ).build()
        assertEquals(0, records.sizeInBytes())
        assertEquals(args.bufferOffset, buffer.position())
    }

    @ParameterizedTest
    @ArgumentsSource(MemoryRecordsBuilderArgumentsProvider::class)
    fun testWriteTransactionalRecordSet(args: Args) {
        val buffer = allocateBuffer(128, args)
        val pid: Long = 9809
        val epoch: Short = 15
        val sequence = 2342
        val supplier = {
            MemoryRecordsBuilder(
                buffer = buffer,
                magic = args.magic,
                compressionType = args.compressionType,
                timestampType = TimestampType.CREATE_TIME,
                baseOffset = 0L,
                logAppendTime = 0L,
                producerId = pid,
                producerEpoch = epoch,
                baseSequence = sequence,
                isTransactional = true,
                isControlBatch = false,
                partitionLeaderEpoch = RecordBatch.NO_PARTITION_LEADER_EPOCH,
                writeLimit = buffer.capacity(),
            )
        }
        if (args.magic < RecordBatch.MAGIC_VALUE_V2) {
            assertFailsWith<IllegalArgumentException> { supplier() }
        } else {
            val builder = supplier()
            builder.append(
                timestamp = System.currentTimeMillis(),
                key = "foo".toByteArray(),
                value = "bar".toByteArray(),
            )
            val records = builder.build()
            val batches = records.batches().iterator().asSequence().toList()
            assertEquals(1, batches.size)
            assertTrue(batches[0].isTransactional)
        }
    }

    @ParameterizedTest
    @ArgumentsSource(MemoryRecordsBuilderArgumentsProvider::class)
    fun testWriteTransactionalWithInvalidPID(args: Args) {
        val buffer: ByteBuffer = allocateBuffer(128, args)
        val pid = RecordBatch.NO_PRODUCER_ID
        val epoch: Short = 15
        val sequence = 2342
        val supplier = {
            MemoryRecordsBuilder(
                buffer = buffer,
                magic = args.magic,
                compressionType = args.compressionType,
                timestampType = TimestampType.CREATE_TIME,
                baseOffset = 0L,
                logAppendTime = 0L,
                producerId = pid,
                producerEpoch = epoch,
                baseSequence = sequence,
                isTransactional = true,
                isControlBatch = false,
                partitionLeaderEpoch = RecordBatch.NO_PARTITION_LEADER_EPOCH,
                writeLimit = buffer.capacity(),
            )
        }
        if (args.magic < RecordBatch.MAGIC_VALUE_V2) {
            assertFailsWith<IllegalArgumentException> { supplier() }
        } else {
            val builder = supplier()
            assertFailsWith<IllegalArgumentException> { builder.close() }
        }
    }

    @ParameterizedTest
    @ArgumentsSource(MemoryRecordsBuilderArgumentsProvider::class)
    fun testWriteIdempotentWithInvalidEpoch(args: Args) {
        val buffer: ByteBuffer = allocateBuffer(128, args)
        val pid: Long = 9809
        val epoch = RecordBatch.NO_PRODUCER_EPOCH
        val sequence = 2342
        val supplier = {
            MemoryRecordsBuilder(
                buffer = buffer,
                magic = args.magic,
                compressionType = args.compressionType,
                timestampType = TimestampType.CREATE_TIME,
                baseOffset = 0L,
                logAppendTime = 0L,
                producerId = pid,
                producerEpoch = epoch,
                baseSequence = sequence,
                isTransactional = true,
                isControlBatch = false,
                partitionLeaderEpoch = RecordBatch.NO_PARTITION_LEADER_EPOCH,
                writeLimit = buffer.capacity(),
            )
        }
        if (args.magic < RecordBatch.MAGIC_VALUE_V2) {
            assertFailsWith<IllegalArgumentException> { supplier() }
        } else {
            val builder = supplier()
            assertFailsWith<IllegalArgumentException> { builder.close() }
        }
    }

    @ParameterizedTest
    @ArgumentsSource(MemoryRecordsBuilderArgumentsProvider::class)
    fun testWriteIdempotentWithInvalidBaseSequence(args: Args) {
        val buffer: ByteBuffer = allocateBuffer(128, args)
        val pid: Long = 9809
        val epoch: Short = 15
        val sequence = RecordBatch.NO_SEQUENCE
        val supplier = {
            MemoryRecordsBuilder(
                buffer = buffer,
                magic = args.magic,
                compressionType = args.compressionType,
                timestampType = TimestampType.CREATE_TIME,
                baseOffset = 0L,
                logAppendTime = 0L,
                producerId = pid,
                producerEpoch = epoch,
                baseSequence = sequence,
                isTransactional = true,
                isControlBatch = false,
                partitionLeaderEpoch = RecordBatch.NO_PARTITION_LEADER_EPOCH,
                writeLimit = buffer.capacity(),
            )
        }
        if (args.magic < RecordBatch.MAGIC_VALUE_V2) {
            assertFailsWith<IllegalArgumentException> { supplier() }
        } else {
            val builder = supplier()
            assertFailsWith<IllegalArgumentException> { builder.close() }
        }
    }

    @ParameterizedTest
    @ArgumentsSource(MemoryRecordsBuilderArgumentsProvider::class)
    fun testWriteEndTxnMarkerNonTransactionalBatch(args: Args) {
        val buffer: ByteBuffer = allocateBuffer(128, args)
        val pid: Long = 9809
        val epoch: Short = 15
        val sequence = RecordBatch.NO_SEQUENCE
        val supplier = {
                MemoryRecordsBuilder(
                    buffer = buffer,
                    magic = args.magic,
                    compressionType = args.compressionType,
                    timestampType = TimestampType.CREATE_TIME,
                    baseOffset = 0L,
                    logAppendTime = 0L,
                    producerId = pid,
                    producerEpoch = epoch,
                    baseSequence = sequence,
                    isTransactional = false,
                    isControlBatch = true,
                    partitionLeaderEpoch = RecordBatch.NO_PARTITION_LEADER_EPOCH,
                    writeLimit = buffer.capacity(),
                )
            }
        if (args.magic < RecordBatch.MAGIC_VALUE_V2) {
            assertFailsWith<IllegalArgumentException> { supplier() }
        } else {
            val builder = supplier()
            assertFailsWith<IllegalArgumentException> {
                builder.appendEndTxnMarker(
                    timestamp = RecordBatch.NO_TIMESTAMP,
                    marker = EndTransactionMarker(
                        controlType = ControlRecordType.ABORT,
                        coordinatorEpoch = 0,
                    ),
                )
            }
        }
    }

    @ParameterizedTest
    @ArgumentsSource(MemoryRecordsBuilderArgumentsProvider::class)
    fun testWriteEndTxnMarkerNonControlBatch(args: Args) {
        val buffer: ByteBuffer = allocateBuffer(128, args)
        val pid: Long = 9809
        val epoch: Short = 15
        val sequence = RecordBatch.NO_SEQUENCE
        val supplier = {
            MemoryRecordsBuilder(
                buffer = buffer,
                magic = args.magic,
                compressionType = args.compressionType,
                timestampType = TimestampType.CREATE_TIME,
                baseOffset = 0L,
                logAppendTime = 0L,
                producerId = pid,
                producerEpoch = epoch,
                baseSequence = sequence,
                isTransactional = true,
                isControlBatch = false,
                partitionLeaderEpoch = RecordBatch.NO_PARTITION_LEADER_EPOCH,
                writeLimit = buffer.capacity(),
            )
        }
        if (args.magic < RecordBatch.MAGIC_VALUE_V2) {
            assertFailsWith<IllegalArgumentException> { supplier() }
        } else {
            val builder = supplier()
            assertFailsWith<IllegalArgumentException> {
                builder.appendEndTxnMarker(
                    timestamp = RecordBatch.NO_TIMESTAMP,
                    marker = EndTransactionMarker(
                        controlType = ControlRecordType.ABORT,
                        coordinatorEpoch = 0,
                    )
                )
            }
        }
    }

    @ParameterizedTest
    @ArgumentsSource(MemoryRecordsBuilderArgumentsProvider::class)
    fun testWriteLeaderChangeControlBatchWithoutLeaderEpoch(args: Args) {
        val buffer: ByteBuffer = allocateBuffer(128, args)
        val supplier = {
                MemoryRecordsBuilder(
                    buffer = buffer,
                    magic = args.magic,
                    compressionType = args.compressionType,
                    timestampType = TimestampType.CREATE_TIME,
                    baseOffset = 0L,
                    logAppendTime = 0L,
                    producerId = RecordBatch.NO_PRODUCER_ID,
                    producerEpoch = RecordBatch.NO_PRODUCER_EPOCH,
                    baseSequence = RecordBatch.NO_SEQUENCE,
                    isTransactional = false,
                    isControlBatch = true,
                    partitionLeaderEpoch = RecordBatch.NO_PARTITION_LEADER_EPOCH,
                    writeLimit = buffer.capacity(),
                )
            }
        if (args.magic < RecordBatch.MAGIC_VALUE_V2) {
            assertFailsWith<IllegalArgumentException> { supplier() }
        } else {
            val leaderId = 1
            val builder = supplier()
            assertFailsWith<IllegalArgumentException> {
                builder.appendLeaderChangeMessage(
                    timestamp = RecordBatch.NO_TIMESTAMP,
                    leaderChangeMessage = LeaderChangeMessage()
                        .setLeaderId(leaderId)
                        .setVoters(emptyList())
                )
            }
        }
    }

    @ParameterizedTest
    @ArgumentsSource(MemoryRecordsBuilderArgumentsProvider::class)
    fun testWriteLeaderChangeControlBatch(args: Args) {
        val buffer: ByteBuffer = allocateBuffer(128, args)
        val leaderId = 1
        val leaderEpoch = 5
        val voters: List<Int> = mutableListOf(2, 3)
        val supplier = {
            MemoryRecordsBuilder(
                buffer = buffer,
                magic = args.magic,
                compressionType = args.compressionType,
                timestampType = TimestampType.CREATE_TIME,
                baseOffset = 0L,
                logAppendTime = 0L,
                producerId = RecordBatch.NO_PRODUCER_ID,
                producerEpoch = RecordBatch.NO_PRODUCER_EPOCH,
                baseSequence = RecordBatch.NO_SEQUENCE,
                isTransactional = false,
                isControlBatch = true,
                partitionLeaderEpoch = leaderEpoch,
                writeLimit = buffer.capacity(),
            )
        }
        if (args.magic < RecordBatch.MAGIC_VALUE_V2) {
            assertFailsWith<IllegalArgumentException> { supplier() }
        } else {
            val builder = supplier()
            builder.appendLeaderChangeMessage(
                timestamp = RecordBatch.NO_TIMESTAMP,
                leaderChangeMessage = LeaderChangeMessage()
                    .setLeaderId(leaderId)
                    .setVoters(voters.map { voterId -> Voter().setVoterId(voterId) })
            )
            val built = builder.build()
            val records = built.records().toList()
            assertEquals(1, records.size)
            val leaderChangeMessage = deserializeLeaderChangeMessage(records[0])
            assertEquals(leaderId, leaderChangeMessage.leaderId)
            assertEquals(voters, leaderChangeMessage.voters.map(Voter::voterId))
        }
    }

    @ParameterizedTest
    @ArgumentsSource(MemoryRecordsBuilderArgumentsProvider::class)
    fun testLegacyCompressionRate(args: Args) {
        val magic = args.magic
        val buffer: ByteBuffer = allocateBuffer(1024, args)
        val supplier = {
                arrayOf(
                    LegacyRecord.create(magic, 0L, "a".toByteArray(), "1".toByteArray()),
                    LegacyRecord.create(magic, 1L, "b".toByteArray(), "2".toByteArray()),
                    LegacyRecord.create(magic, 2L, "c".toByteArray(), "3".toByteArray()),
                )
            }
        if (magic >= RecordBatch.MAGIC_VALUE_V2) {
            assertFailsWith<IllegalArgumentException> { supplier() }
        } else {
            val records = supplier()
            val builder = MemoryRecordsBuilder(
                buffer = buffer,
                magic = magic,
                compressionType = args.compressionType,
                timestampType = TimestampType.CREATE_TIME,
                baseOffset = 0L,
                logAppendTime = 0L,
                producerId = RecordBatch.NO_PRODUCER_ID,
                producerEpoch = RecordBatch.NO_PRODUCER_EPOCH,
                baseSequence = RecordBatch.NO_SEQUENCE,
                isTransactional = false,
                isControlBatch = false,
                partitionLeaderEpoch = RecordBatch.NO_PARTITION_LEADER_EPOCH,
                writeLimit = buffer.capacity(),
            )
            var uncompressedSize = 0
            for (record in records) {
                uncompressedSize += record.sizeInBytes() + Records.LOG_OVERHEAD
                builder.append(record)
            }
            val built = builder.build()
            if (args.compressionType === CompressionType.NONE) {
                assertEquals(1.0, builder.compressionRatio, 0.00001)
            } else {
                val recordHeaad =
                    if (magic == RecordBatch.MAGIC_VALUE_V0) LegacyRecord.RECORD_OVERHEAD_V0
                    else LegacyRecord.RECORD_OVERHEAD_V1
                val compressedSize = built.sizeInBytes() - Records.LOG_OVERHEAD - recordHeaad
                val computedCompressionRate = compressedSize.toDouble() / uncompressedSize
                assertEquals(computedCompressionRate, builder.compressionRatio, 0.00001)
            }
        }
    }

    @ParameterizedTest
    @ArgumentsSource(MemoryRecordsBuilderArgumentsProvider::class)
    fun testEstimatedSizeInBytes(args: Args) {
        val buffer: ByteBuffer = allocateBuffer(1024, args)
        val builder = MemoryRecordsBuilder(
            buffer = buffer,
            magic = args.magic,
            compressionType = args.compressionType,
            timestampType = TimestampType.CREATE_TIME,
            baseOffset = 0L,
            logAppendTime = 0L,
            producerId = RecordBatch.NO_PRODUCER_ID,
            producerEpoch = RecordBatch.NO_PRODUCER_EPOCH,
            baseSequence = RecordBatch.NO_SEQUENCE,
            isTransactional = false,
            isControlBatch = false,
            partitionLeaderEpoch = RecordBatch.NO_PARTITION_LEADER_EPOCH,
            writeLimit = buffer.capacity(),
        )
        var previousEstimate = 0
        repeat(10) { i ->
            builder.append(SimpleRecord(timestamp = i.toLong(), value = ("" + i).toByteArray()))
            val currentEstimate = builder.estimatedSizeInBytes()
            assertTrue(currentEstimate > previousEstimate)
            previousEstimate = currentEstimate
        }
        val bytesWrittenBeforeClose = builder.estimatedSizeInBytes()
        val records = builder.build()
        assertEquals(records.sizeInBytes(), builder.estimatedSizeInBytes())
        if (args.compressionType === CompressionType.NONE)
            assertEquals(records.sizeInBytes(), bytesWrittenBeforeClose)
    }

    @ParameterizedTest
    @ArgumentsSource(MemoryRecordsBuilderArgumentsProvider::class)
    fun buildUsingLogAppendTime(args: Args) {
        val magic = args.magic
        val buffer = allocateBuffer(1024, args)
        val logAppendTime = System.currentTimeMillis()
        val builder = MemoryRecordsBuilder(
            buffer = buffer,
            magic = magic,
            compressionType = args.compressionType,
            timestampType = TimestampType.LOG_APPEND_TIME,
            baseOffset = 0L,
            logAppendTime = logAppendTime,
            producerId = RecordBatch.NO_PRODUCER_ID,
            producerEpoch = RecordBatch.NO_PRODUCER_EPOCH,
            baseSequence = RecordBatch.NO_SEQUENCE,
            isTransactional = false,
            isControlBatch = false,
            partitionLeaderEpoch = RecordBatch.NO_PARTITION_LEADER_EPOCH,
            writeLimit = buffer.capacity(),
        )
        builder.append(timestamp = 0L, key = "a".toByteArray(), value = "1".toByteArray())
        builder.append(timestamp = 0L, key = "b".toByteArray(), value = "2".toByteArray())
        builder.append(timestamp = 0L, key = "c".toByteArray(), value = "3".toByteArray())
        val records = builder.build()
        val info = builder.info()
        assertEquals(logAppendTime, info.maxTimestamp)
        if (args.compressionType === CompressionType.NONE && magic <= RecordBatch.MAGIC_VALUE_V1)
            assertEquals(0L, info.shallowOffsetOfMaxTimestamp)
        else assertEquals(2L, info.shallowOffsetOfMaxTimestamp)

        for (batch in records.batches()) {
            if (magic == RecordBatch.MAGIC_VALUE_V0) {
                assertEquals(TimestampType.NO_TIMESTAMP_TYPE, batch.timestampType())
            } else {
                assertEquals(TimestampType.LOG_APPEND_TIME, batch.timestampType())
                for (record in batch) assertEquals(logAppendTime, record.timestamp())
            }
        }
    }

    @ParameterizedTest
    @ArgumentsSource(MemoryRecordsBuilderArgumentsProvider::class)
    fun buildUsingCreateTime(args: Args) {
        val magic = args.magic
        val buffer: ByteBuffer = allocateBuffer(1024, args)
        val logAppendTime = System.currentTimeMillis()
        val builder = MemoryRecordsBuilder(
            buffer = buffer,
            magic = magic,
            compressionType = args.compressionType,
            timestampType = TimestampType.CREATE_TIME,
            baseOffset = 0L,
            logAppendTime = logAppendTime,
            producerId = RecordBatch.NO_PRODUCER_ID,
            producerEpoch = RecordBatch.NO_PRODUCER_EPOCH,
            baseSequence = RecordBatch.NO_SEQUENCE,
            isTransactional = false,
            isControlBatch = false,
            partitionLeaderEpoch = RecordBatch.NO_PARTITION_LEADER_EPOCH,
            writeLimit = buffer.capacity(),
        )
        builder.append(0L, "a".toByteArray(), "1".toByteArray())
        builder.append(2L, "b".toByteArray(), "2".toByteArray())
        builder.append(1L, "c".toByteArray(), "3".toByteArray())
        val records = builder.build()
        val info = builder.info()
        if (magic == RecordBatch.MAGIC_VALUE_V0) assertEquals(-1, info.maxTimestamp)
        else assertEquals(2L, info.maxTimestamp)
        if (args.compressionType === CompressionType.NONE && magic == RecordBatch.MAGIC_VALUE_V1)
            assertEquals(1L, info.shallowOffsetOfMaxTimestamp)
        else assertEquals(2L, info.shallowOffsetOfMaxTimestamp)
        var i = 0
        val expectedTimestamps = longArrayOf(0L, 2L, 1L)
        for (batch in records.batches()) {
            if (magic == RecordBatch.MAGIC_VALUE_V0) {
                assertEquals(TimestampType.NO_TIMESTAMP_TYPE, batch.timestampType())
            } else {
                assertEquals(TimestampType.CREATE_TIME, batch.timestampType())
                for (record in batch) assertEquals(expectedTimestamps[i++], record.timestamp())
            }
        }
    }

    @ParameterizedTest
    @ArgumentsSource(MemoryRecordsBuilderArgumentsProvider::class)
    fun testAppendedChecksumConsistency(args: Args) {
        val buffer = ByteBuffer.allocate(512)
        val builder = MemoryRecordsBuilder(
            buffer = buffer,
            magic = args.magic,
            compressionType = args.compressionType,
            timestampType = TimestampType.CREATE_TIME,
            baseOffset = 0L,
            logAppendTime = LegacyRecord.NO_TIMESTAMP,
            producerId = RecordBatch.NO_PRODUCER_ID,
            producerEpoch = RecordBatch.NO_PRODUCER_EPOCH,
            baseSequence = RecordBatch.NO_SEQUENCE,
            isTransactional = false,
            isControlBatch = false,
            partitionLeaderEpoch = RecordBatch.NO_PARTITION_LEADER_EPOCH,
            writeLimit = buffer.capacity(),
        )
        builder.append(1L, "key".toByteArray(), "value".toByteArray())
        val memoryRecords = builder.build()
        val records = memoryRecords.records().toList()
        assertEquals(1, records.size)
    }

    @ParameterizedTest
    @ArgumentsSource(MemoryRecordsBuilderArgumentsProvider::class)
    fun testSmallWriteLimit(args: Args) {
        // with a small write limit, we always allow at least one record to be added
        val key = "foo".toByteArray()
        val value = "bar".toByteArray()
        val writeLimit = 0
        val buffer = ByteBuffer.allocate(512)
        val builder = MemoryRecordsBuilder(
            buffer = buffer,
            magic = args.magic,
            compressionType = args.compressionType,
            timestampType = TimestampType.CREATE_TIME,
            baseOffset = 0L,
            logAppendTime = LegacyRecord.NO_TIMESTAMP,
            producerId = RecordBatch.NO_PRODUCER_ID,
            producerEpoch = RecordBatch.NO_PRODUCER_EPOCH,
            baseSequence = RecordBatch.NO_SEQUENCE,
            isTransactional = false,
            isControlBatch = false,
            partitionLeaderEpoch = RecordBatch.NO_PARTITION_LEADER_EPOCH,
            writeLimit = writeLimit,
        )
        assertFalse(builder.isFull)
        assertTrue(
            builder.hasRoomFor(
                timestamp = 0L,
                key = key,
                value = value,
                headers = Record.EMPTY_HEADERS
            ),
        )
        builder.append(0L, key, value)
        assertTrue(builder.isFull)
        assertFalse(
            builder.hasRoomFor(
                timestamp = 0L,
                key = key,
                value = value,
                headers = Record.EMPTY_HEADERS,
            )
        )
        val memRecords = builder.build()
        val records = memRecords.records().toList()
        assertEquals(1, records.size)
        val record = records[0]
        assertEquals(ByteBuffer.wrap(key), record.key())
        assertEquals(ByteBuffer.wrap(value), record.value())
    }

    @ParameterizedTest
    @ArgumentsSource(MemoryRecordsBuilderArgumentsProvider::class)
    fun writePastLimit(args: Args) {
        val magic = args.magic
        val buffer: ByteBuffer = allocateBuffer(64, args)
        val logAppendTime = System.currentTimeMillis()
        val builder = MemoryRecordsBuilder(
            buffer = buffer,
            magic = magic,
            compressionType = args.compressionType,
            timestampType = TimestampType.CREATE_TIME,
            baseOffset = 0L,
            logAppendTime = logAppendTime,
            producerId = RecordBatch.NO_PRODUCER_ID,
            producerEpoch = RecordBatch.NO_PRODUCER_EPOCH,
            baseSequence = RecordBatch.NO_SEQUENCE,
            isTransactional = false,
            isControlBatch = false,
            partitionLeaderEpoch = RecordBatch.NO_PARTITION_LEADER_EPOCH,
            writeLimit = buffer.capacity(),
        )
        builder.setEstimatedCompressionRatio(0.5f)
        builder.append(timestamp = 0L, key = "a".toByteArray(), value = "1".toByteArray())
        builder.append(timestamp = 1L, key = "b".toByteArray(), value = "2".toByteArray())
        assertFalse(
            builder.hasRoomFor(
                timestamp = 2L,
                key = "c".toByteArray(),
                value = "3".toByteArray(),
                headers = Record.EMPTY_HEADERS,
            )
        )
        builder.append(timestamp = 2L, key = "c".toByteArray(), value = "3".toByteArray())
        val records = builder.build()
        val info = builder.info()
        if (magic == RecordBatch.MAGIC_VALUE_V0) assertEquals(-1, info.maxTimestamp)
        else assertEquals(2L, info.maxTimestamp)
        assertEquals(2L, info.shallowOffsetOfMaxTimestamp)
        var i = 0L
        for (batch in records.batches()) {
            if (magic == RecordBatch.MAGIC_VALUE_V0) {
                assertEquals(TimestampType.NO_TIMESTAMP_TYPE, batch.timestampType())
            } else {
                assertEquals(TimestampType.CREATE_TIME, batch.timestampType())
                for (record in batch) assertEquals(i++, record.timestamp())
            }
        }
    }

    @ParameterizedTest
    @ArgumentsSource(MemoryRecordsBuilderArgumentsProvider::class)
    fun testAppendAtInvalidOffset(args: Args) {
        val buffer: ByteBuffer = allocateBuffer(1024, args)
        val logAppendTime = System.currentTimeMillis()
        val builder = MemoryRecordsBuilder(
            buffer = buffer,
            magic = args.magic,
            compressionType = args.compressionType,
            timestampType = TimestampType.CREATE_TIME,
            baseOffset = 0L,
            logAppendTime = logAppendTime,
            producerId = RecordBatch.NO_PRODUCER_ID,
            producerEpoch = RecordBatch.NO_PRODUCER_EPOCH,
            baseSequence = RecordBatch.NO_SEQUENCE,
            isTransactional = false,
            isControlBatch = false,
            partitionLeaderEpoch = RecordBatch.NO_PARTITION_LEADER_EPOCH,
            writeLimit = buffer.capacity(),
        )
        builder.appendWithOffset(0L, System.currentTimeMillis(), "a".toByteArray(), null)

        // offsets must increase monotonically
        assertFailsWith<IllegalArgumentException> {
            builder.appendWithOffset(
                offset = 0L,
                timestamp = System.currentTimeMillis(),
                key = "b".toByteArray(),
                value = null,
            )
        }
    }

    @ParameterizedTest
    @EnumSource(CompressionType::class)
    fun convertV2ToV1UsingMixedCreateAndLogAppendTime(compressionType: CompressionType) {
        val buffer = ByteBuffer.allocate(512)
        var builder = MemoryRecords.builder(
            buffer = buffer,
            magic = RecordBatch.MAGIC_VALUE_V2,
            compressionType = compressionType,
            timestampType = TimestampType.LOG_APPEND_TIME,
            baseOffset = 0L,
        )
        builder.append(timestamp = 10L, key = "1".toByteArray(), value = "a".toByteArray())
        builder.close()
        var sizeExcludingTxnMarkers = buffer.position()
        MemoryRecords.writeEndTransactionalMarker(
            buffer = buffer,
            initialOffset = 1L,
            timestamp = System.currentTimeMillis(),
            partitionLeaderEpoch = 0,
            producerId = 15L,
            producerEpoch = 0.toShort(),
            marker = EndTransactionMarker(
                controlType = ControlRecordType.ABORT,
                coordinatorEpoch = 0,
            ),
        )
        val position = buffer.position()
        builder = MemoryRecords.builder(
            buffer = buffer,
            magic = RecordBatch.MAGIC_VALUE_V2,
            compressionType = compressionType,
            timestampType = TimestampType.CREATE_TIME,
            baseOffset = 1L,
        )
        builder.append(timestamp = 12L, key = "2".toByteArray(), value = "b".toByteArray())
        builder.append(timestamp = 13L, key = "3".toByteArray(), value = "c".toByteArray())
        builder.close()
        sizeExcludingTxnMarkers += buffer.position() - position
        MemoryRecords.writeEndTransactionalMarker(
            buffer = buffer,
            initialOffset = 14L,
            timestamp = System.currentTimeMillis(),
            partitionLeaderEpoch = 0,
            producerId = 1L,
            producerEpoch = 0.toShort(),
            marker = EndTransactionMarker(
                controlType = ControlRecordType.COMMIT,
                coordinatorEpoch = 0,
            ),
        )
        buffer.flip()
        val convertedRecordsSupplier = {
            MemoryRecords.readableRecords(buffer).downConvert(
                toMagic = RecordBatch.MAGIC_VALUE_V1,
                firstOffset = 0,
                time = time,
            )
        }
        if (compressionType !== CompressionType.ZSTD) {
            val convertedRecords = convertedRecordsSupplier()
            val records = convertedRecords.records

            // Transactional markers are skipped when down converting to V1, so exclude them from size
            verifyRecordsProcessingStats(
                compressionType = compressionType,
                processingStats = convertedRecords.recordConversionStats,
                numRecords = 3,
                numRecordsConverted = 3,
                finalBytes = records.sizeInBytes().toLong(),
                preConvertedBytes = sizeExcludingTxnMarkers.toLong(),
            )
            val batches = records.batches().iterator().asSequence().toList()
            if (compressionType !== CompressionType.NONE) {
                assertEquals(2, batches.size)
                assertEquals(TimestampType.LOG_APPEND_TIME, batches[0].timestampType())
                assertEquals(TimestampType.CREATE_TIME, batches[1].timestampType())
            } else {
                assertEquals(3, batches.size)
                assertEquals(TimestampType.LOG_APPEND_TIME, batches[0].timestampType())
                assertEquals(TimestampType.CREATE_TIME, batches[1].timestampType())
                assertEquals(TimestampType.CREATE_TIME, batches[2].timestampType())
            }
            val logRecords = records.records().iterator().asSequence().toList()
            assertEquals(3, logRecords.size)
            assertEquals(ByteBuffer.wrap("1".toByteArray()), logRecords[0].key())
            assertEquals(ByteBuffer.wrap("2".toByteArray()), logRecords[1].key())
            assertEquals(ByteBuffer.wrap("3".toByteArray()), logRecords[2].key())
        } else {
            val e = assertFailsWith<UnsupportedCompressionTypeException> { convertedRecordsSupplier() }
            assertEquals("Down-conversion of zstandard-compressed batches is not supported", e.message)
        }
    }

    @ParameterizedTest
    @EnumSource(CompressionType::class)
    fun convertToV1WithMixedV0AndV2Data(compressionType: CompressionType) {
        val buffer = ByteBuffer.allocate(512)
        val supplier = {
            MemoryRecords.builder(
                buffer = buffer,
                magic = RecordBatch.MAGIC_VALUE_V0,
                compressionType = compressionType,
                timestampType = TimestampType.NO_TIMESTAMP_TYPE,
                baseOffset = 0L,
            )
        }
        if (compressionType === CompressionType.ZSTD) {
            assertFailsWith<IllegalArgumentException> { supplier() }
        } else {
            var builder = supplier()
            builder.append(timestamp = RecordBatch.NO_TIMESTAMP, key = "1".toByteArray(), value = "a".toByteArray())
            builder.close()
            builder = MemoryRecords.builder(
                buffer = buffer,
                magic = RecordBatch.MAGIC_VALUE_V2,
                compressionType = compressionType,
                timestampType = TimestampType.CREATE_TIME,
                baseOffset = 1L,
            )
            builder.append(timestamp = 11L, key = "2".toByteArray(), value = "b".toByteArray())
            builder.append(timestamp = 12L, key = "3".toByteArray(), value = "c".toByteArray())
            builder.close()
            buffer.flip()
            var convertedRecords = MemoryRecords.readableRecords(buffer).downConvert(
                toMagic = RecordBatch.MAGIC_VALUE_V1,
                firstOffset = 0,
                time = time,
            )
            var records = convertedRecords.records
            verifyRecordsProcessingStats(
                compressionType = compressionType,
                processingStats = convertedRecords.recordConversionStats,
                numRecords = 3,
                numRecordsConverted = 2,
                finalBytes = records.sizeInBytes().toLong(),
                preConvertedBytes = buffer.limit().toLong(),
            )
            var batches = records.batches().iterator().asSequence().toList()
            if (compressionType !== CompressionType.NONE) {
                assertEquals(2, batches.size)
                assertEquals(RecordBatch.MAGIC_VALUE_V0, batches[0].magic())
                assertEquals(0, batches[0].baseOffset())
                assertEquals(RecordBatch.MAGIC_VALUE_V1, batches[1].magic())
                assertEquals(1, batches[1].baseOffset())
            } else {
                assertEquals(3, batches.size)
                assertEquals(RecordBatch.MAGIC_VALUE_V0, batches[0].magic())
                assertEquals(0, batches[0].baseOffset())
                assertEquals(RecordBatch.MAGIC_VALUE_V1, batches[1].magic())
                assertEquals(1, batches[1].baseOffset())
                assertEquals(RecordBatch.MAGIC_VALUE_V1, batches[2].magic())
                assertEquals(2, batches[2].baseOffset())
            }
            var logRecords = records.records().iterator().asSequence().toList()
            assertEquals("1", utf8(logRecords[0].key()!!))
            assertEquals("2", utf8(logRecords[1].key()!!))
            assertEquals("3", utf8(logRecords[2].key()!!))
            convertedRecords = MemoryRecords.readableRecords(buffer).downConvert(
                toMagic = RecordBatch.MAGIC_VALUE_V1,
                firstOffset = 2L,
                time = time,
            )
            records = convertedRecords.records
            batches = records.batches().iterator().asSequence().toList()
            logRecords = records.records().iterator().asSequence().toList()
            if (compressionType !== CompressionType.NONE) {
                assertEquals(2, batches.size)
                assertEquals(RecordBatch.MAGIC_VALUE_V0, batches[0].magic())
                assertEquals(0, batches[0].baseOffset())
                assertEquals(RecordBatch.MAGIC_VALUE_V1, batches[1].magic())
                assertEquals(1, batches[1].baseOffset())
                assertEquals("1", utf8(logRecords[0].key()!!))
                assertEquals("2", utf8(logRecords[1].key()!!))
                assertEquals("3", utf8(logRecords[2].key()!!))
                verifyRecordsProcessingStats(
                    compressionType = compressionType,
                    processingStats = convertedRecords.recordConversionStats,
                    numRecords = 3,
                    numRecordsConverted = 2,
                    finalBytes = records.sizeInBytes().toLong(),
                    preConvertedBytes = buffer.limit().toLong(),
                )
            } else {
                assertEquals(2, batches.size)
                assertEquals(RecordBatch.MAGIC_VALUE_V0, batches[0].magic())
                assertEquals(0, batches[0].baseOffset())
                assertEquals(RecordBatch.MAGIC_VALUE_V1, batches[1].magic())
                assertEquals(2, batches[1].baseOffset())
                assertEquals("1", utf8(logRecords[0].key()!!))
                assertEquals("3", utf8(logRecords[1].key()!!))
                verifyRecordsProcessingStats(
                    compressionType = compressionType,
                    processingStats = convertedRecords.recordConversionStats,
                    numRecords = 3,
                    numRecordsConverted = 1,
                    finalBytes = records.sizeInBytes().toLong(),
                    preConvertedBytes = buffer.limit().toLong(),
                )
            }
        }
    }

    @ParameterizedTest
    @ArgumentsSource(MemoryRecordsBuilderArgumentsProvider::class)
    fun shouldThrowIllegalStateExceptionOnBuildWhenAborted(args: Args) {
        val buffer: ByteBuffer = allocateBuffer(128, args)
        val builder = MemoryRecordsBuilder(
            buffer = buffer,
            magic = args.magic,
            compressionType = args.compressionType,
            timestampType = TimestampType.CREATE_TIME,
            baseOffset = 0L,
            logAppendTime = 0L,
            producerId = RecordBatch.NO_PRODUCER_ID,
            producerEpoch = RecordBatch.NO_PRODUCER_EPOCH,
            baseSequence = RecordBatch.NO_SEQUENCE,
            isTransactional = false,
            isControlBatch = false,
            partitionLeaderEpoch = RecordBatch.NO_PARTITION_LEADER_EPOCH,
            writeLimit = buffer.capacity(),
        )
        builder.abort()
        assertFailsWith<IllegalStateException> { builder.build() }
    }

    @ParameterizedTest
    @ArgumentsSource(MemoryRecordsBuilderArgumentsProvider::class)
    fun shouldResetBufferToInitialPositionOnAbort(args: Args) {
        val buffer: ByteBuffer = allocateBuffer(128, args)
        val builder = MemoryRecordsBuilder(
            buffer = buffer,
            magic = args.magic,
            compressionType = args.compressionType,
            timestampType = TimestampType.CREATE_TIME,
            baseOffset = 0L,
            logAppendTime = 0L,
            producerId = RecordBatch.NO_PRODUCER_ID,
            producerEpoch = RecordBatch.NO_PRODUCER_EPOCH,
            baseSequence = RecordBatch.NO_SEQUENCE,
            isTransactional = false,
            isControlBatch = false,
            partitionLeaderEpoch = RecordBatch.NO_PARTITION_LEADER_EPOCH,
            writeLimit = buffer.capacity(),
        )
        builder.append(timestamp = 0L, key = "a".toByteArray(), value = "1".toByteArray())
        builder.abort()
        assertEquals(args.bufferOffset, builder.buffer.position())
    }

    @ParameterizedTest
    @ArgumentsSource(MemoryRecordsBuilderArgumentsProvider::class)
    fun shouldThrowIllegalStateExceptionOnCloseWhenAborted(args: Args) {
        val buffer: ByteBuffer = allocateBuffer(128, args)
        val builder = MemoryRecordsBuilder(
            buffer = buffer,
            magic = args.magic,
            compressionType = args.compressionType,
            timestampType = TimestampType.CREATE_TIME,
            baseOffset = 0L,
            logAppendTime = 0L,
            producerId = RecordBatch.NO_PRODUCER_ID,
            producerEpoch = RecordBatch.NO_PRODUCER_EPOCH,
            baseSequence = RecordBatch.NO_SEQUENCE,
            isTransactional = false,
            isControlBatch = false,
            partitionLeaderEpoch = RecordBatch.NO_PARTITION_LEADER_EPOCH,
            writeLimit = buffer.capacity(),
        )
        builder.abort()
        assertFailsWith<IllegalStateException>(
            message = "Should have thrown IllegalStateException",
        ) { builder.close() }
    }

    @ParameterizedTest
    @ArgumentsSource(MemoryRecordsBuilderArgumentsProvider::class)
    fun shouldThrowIllegalStateExceptionOnAppendWhenAborted(args: Args) {
        val buffer: ByteBuffer = allocateBuffer(128, args)
        val builder = MemoryRecordsBuilder(
            buffer = buffer,
            magic = args.magic,
            compressionType = args.compressionType,
            timestampType = TimestampType.CREATE_TIME,
            baseOffset = 0L,
            logAppendTime = 0L,
            producerId = RecordBatch.NO_PRODUCER_ID,
            producerEpoch = RecordBatch.NO_PRODUCER_EPOCH,
            baseSequence = RecordBatch.NO_SEQUENCE,
            isTransactional = false,
            isControlBatch = false,
            partitionLeaderEpoch = RecordBatch.NO_PARTITION_LEADER_EPOCH,
            writeLimit = buffer.capacity(),
        )
        builder.abort()
        assertFailsWith<IllegalStateException>(
            message = "Should have thrown IllegalStateException",
        ) { builder.append(0L, "a".toByteArray(), "1".toByteArray()) }
    }

    @ParameterizedTest
    @ArgumentsSource(MemoryRecordsBuilderArgumentsProvider::class)
    fun testBuffersDereferencedOnClose(args: Args) {
        val runtime = Runtime.getRuntime()
        val payloadLen = 1024 * 1024
        val buffer = ByteBuffer.allocate(payloadLen * 2)
        val key = ByteArray(0)
        val value = ByteArray(payloadLen)
        Random.nextBytes(value) // Use random payload so that compressed buffer is large
        val builders: MutableList<MemoryRecordsBuilder> = java.util.ArrayList(100)
        var startMem: Long = 0
        var memUsed: Long = 0
        var iterations = 0
        while (iterations++ < 100) {
            buffer.rewind()
            val builder = MemoryRecordsBuilder(
                buffer = buffer,
                magic = args.magic,
                compressionType = args.compressionType,
                timestampType = TimestampType.CREATE_TIME,
                baseOffset = 0L,
                logAppendTime = 0L,
                producerId = RecordBatch.NO_PRODUCER_ID,
                producerEpoch = RecordBatch.NO_PRODUCER_EPOCH,
                baseSequence = RecordBatch.NO_SEQUENCE,
                isTransactional = false,
                isControlBatch = false,
                partitionLeaderEpoch = RecordBatch.NO_PARTITION_LEADER_EPOCH,
                writeLimit = 0,
            )
            builder.append(1L, key, value)
            builder.build()
            builders.add(builder)
            System.gc()
            memUsed = runtime.totalMemory() - runtime.freeMemory() - startMem
            // Ignore memory usage during initialization
            if (iterations == 2) startMem = memUsed
            else if (iterations > 2 && memUsed < (iterations - 2) * 1024) break
        }
        assertTrue(iterations < 100, "Memory usage too high: $memUsed")
    }

    @ParameterizedTest
    @ArgumentsSource(V2MemoryRecordsBuilderArgumentsProvider::class)
    fun testRecordTimestampsWithDeleteHorizon(args: Args) {
        val deleteHorizon: Long = 100
        val payloadLen = 1024 * 1024
        val buffer = ByteBuffer.allocate(payloadLen * 2)
        val byteBufferOutputStream = ByteBufferOutputStream(buffer)
        val builder = MemoryRecordsBuilder(
            bufferStream = byteBufferOutputStream,
            magic = args.magic,
            compressionType = args.compressionType,
            timestampType = TimestampType.CREATE_TIME,
            baseOffset = 0L,
            logAppendTime = 0L,
            producerId = RecordBatch.NO_PRODUCER_ID,
            producerEpoch = RecordBatch.NO_PRODUCER_EPOCH,
            baseSequence = RecordBatch.NO_SEQUENCE,
            isTransactional = false,
            isControlBatch = false,
            partitionLeaderEpoch = RecordBatch.NO_PARTITION_LEADER_EPOCH,
            writeLimit = 0,
            deleteHorizonMs = deleteHorizon,
        )
        builder.append(50L, "0".toByteArray(), "0".toByteArray())
        builder.append(100L, "1".toByteArray(), null)
        builder.append(150L, "2".toByteArray(), "2".toByteArray())
        val records = builder.build()
        val batches = records.batches().toList()
        assertEquals(deleteHorizon, batches[0].deleteHorizonMs())
        val recordIterator = batches[0].streamingIterator(BufferSupplier.create())
        var record = recordIterator.next()
        assertEquals(50L, record.timestamp())
        record = recordIterator.next()
        assertEquals(100L, record.timestamp())
        record = recordIterator.next()
        assertEquals(150L, record.timestamp())
        recordIterator.close()
    }


    private class V2MemoryRecordsBuilderArgumentsProvider : ArgumentsProvider {
        override fun provideArguments(context: ExtensionContext): Stream<out Arguments> {
            val values = mutableListOf<Arguments>()
            for (bufferOffset in mutableListOf(0, 15))
                for (type in CompressionType.values())
                    values.add(Arguments.of(Args(bufferOffset, type, RecordBatch.MAGIC_VALUE_V2)))

            return values.stream()
        }
    }


    private fun verifyRecordsProcessingStats(
        compressionType: CompressionType,
        processingStats: RecordConversionStats,
        numRecords: Int,
        numRecordsConverted: Int,
        finalBytes: Long,
        preConvertedBytes: Long,
    ) {
        assertNotNull(processingStats, "Records processing info is null")
        assertEquals(numRecordsConverted, processingStats.numRecordsConverted)
        // Since nanoTime accuracy on build machines may not be sufficient to measure small conversion times,
        // only check if the value >= 0. Default is -1, so this checks if time has been recorded.
        assertTrue(
            actual = processingStats.conversionTimeNanos >= 0,
            message = "Processing time not recorded: $processingStats"
        )
        val tempBytes = processingStats.temporaryMemoryBytes
        if (compressionType === CompressionType.NONE) {
            when (numRecordsConverted) {
                0 -> assertEquals(finalBytes, tempBytes)
                numRecords -> assertEquals(preConvertedBytes + finalBytes, tempBytes)
                else -> assertTrue(
                    actual = tempBytes > finalBytes && tempBytes < finalBytes + preConvertedBytes,
                    message = "Unexpected temp bytes $tempBytes final $finalBytes pre $preConvertedBytes",
                )
            }
        } else {
            val compressedBytes = finalBytes - Records.LOG_OVERHEAD - LegacyRecord.RECORD_OVERHEAD_V0
            assertTrue(
                actual = tempBytes > compressedBytes,
                message = "Uncompressed size expected temp=$tempBytes, compressed=$compressedBytes",
            )
        }
    }

    private fun allocateBuffer(size: Int, args: Args): ByteBuffer {
        val buffer = ByteBuffer.allocate(size)
        buffer.position(args.bufferOffset)

        return buffer
    }

    class Args(
        val bufferOffset: Int,
        val compressionType: CompressionType,
        val magic: Byte,
    ) {
        override fun toString(): String = "magic=$magic, bufferOffset=$bufferOffset, compressionType=$compressionType"
    }

    class MemoryRecordsBuilderArgumentsProvider : ArgumentsProvider {

        override fun provideArguments(context: ExtensionContext): Stream<out Arguments> {
            val values: MutableList<Arguments> = ArrayList()
            for (bufferOffset in mutableListOf(0, 15))
                for (type in CompressionType.values()) {
                    val magics = if (type === CompressionType.ZSTD) listOf(RecordBatch.MAGIC_VALUE_V2)
                    else listOf(RecordBatch.MAGIC_VALUE_V0, RecordBatch.MAGIC_VALUE_V1, RecordBatch.MAGIC_VALUE_V2)

                    for (magic in magics) values.add(Arguments.of(Args(bufferOffset, type, magic)))
                }
            return values.stream()
        }
    }
}
