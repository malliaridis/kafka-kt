package org.apache.kafka.common.record

import java.nio.ByteBuffer
import java.util.stream.Stream
import org.apache.kafka.common.errors.CorruptRecordException
import org.junit.jupiter.api.extension.ExtensionContext
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.ArgumentsProvider
import org.junit.jupiter.params.provider.ArgumentsSource
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class LegacyRecordTest {

    private class LegacyRecordArgumentsProvider : ArgumentsProvider {
        override fun provideArguments(context: ExtensionContext): Stream<out Arguments> {
            val payload = ByteArray(1000) { 1 }
            val arguments: MutableList<Arguments> = ArrayList()
            for (magic in listOf(RecordBatch.MAGIC_VALUE_V0, RecordBatch.MAGIC_VALUE_V1))
                for (timestamp in listOf(RecordBatch.NO_TIMESTAMP, 0L, 1L))
                    for (key in listOf(null, "".toByteArray(), "key".toByteArray(), payload))
                        for (value in listOf(null, "".toByteArray(), "value".toByteArray(), payload))
                            for (compression in CompressionType.entries)
                                arguments.add(Arguments.of(Args(magic, timestamp, key, value, compression))
            )
            return arguments.stream()
        }
    }

    @ParameterizedTest
    @ArgumentsSource(LegacyRecordArgumentsProvider::class)
    fun testFields(args: Args) {
        val record = args.record
        val key = args.key
        assertEquals(args.compression, record.compressionType())
        assertEquals(key != null, record.hasKey())
        assertEquals(key, record.key())
        if (key != null) assertEquals(key.limit(), record.keySize())
        assertEquals(args.magic, record.magic())
        assertEquals(args.value, record.value())
        if (args.value != null) assertEquals(args.value.limit(), record.valueSize())
        if (args.magic > 0) {
            assertEquals(args.timestamp, record.timestamp())
            assertEquals(args.timestampType, record.timestampType())
        } else {
            assertEquals(RecordBatch.NO_TIMESTAMP, record.timestamp())
            assertEquals(TimestampType.NO_TIMESTAMP_TYPE, record.timestampType())
        }
    }

    @ParameterizedTest
    @ArgumentsSource(LegacyRecordArgumentsProvider::class)
    fun testChecksum(args: Args) {
        val record = args.record
        assertEquals(record.checksum(), record.computeChecksum())
        val attributes = LegacyRecord.computeAttributes(
            magic = args.magic,
            type = args.compression,
            timestampType = TimestampType.CREATE_TIME,
        )
        assertEquals(
            expected = record.checksum(),
            actual = LegacyRecord.computeChecksum(
                magic = args.magic,
                attributes = attributes,
                timestamp = args.timestamp,
                key = args.key?.array(),
                value = args.value?.array(),
            ),
        )
        assertTrue(record.isValid)
        for (i in LegacyRecord.CRC_OFFSET + LegacyRecord.CRC_LENGTH until record.sizeInBytes()) {
            val copy = copyOf(record)
            copy.buffer().put(i, 69.toByte())
            assertFalse(copy.isValid)
            assertFailsWith<CorruptRecordException> { copy.ensureValid() }
        }
    }

    private fun copyOf(record: LegacyRecord): LegacyRecord {
        val buffer = ByteBuffer.allocate(record.sizeInBytes())
        record.buffer().put(buffer)
        buffer.rewind()
        record.buffer().rewind()
        return LegacyRecord(buffer)
    }

    @ParameterizedTest
    @ArgumentsSource(LegacyRecordArgumentsProvider::class)
    fun testEquality(args: Args) {
        assertEquals(args.record, copyOf(args.record))
    }
    
    class Args(
        val magic: Byte,
        val timestamp: Long,
        key: ByteArray?,
        value: ByteArray?,
        val compression: CompressionType,
    ) {
        val key = key?.let { ByteBuffer.wrap(it) }
        val value = value?.let { ByteBuffer.wrap(it) }
        val timestampType: TimestampType = TimestampType.CREATE_TIME
        val record = LegacyRecord.create(
            magic = magic,
            timestamp = timestamp,
            key = key,
            value = value,
            compressionType = compression,
            timestampType = timestampType
        )

        override fun toString(): String = "magic=$magic, compression=$compression, timestamp=$timestamp"
    }
}
