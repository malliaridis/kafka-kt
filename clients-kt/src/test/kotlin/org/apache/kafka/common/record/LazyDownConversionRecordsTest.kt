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
import java.io.IOException
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.StandardOpenOption
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.header.Header
import org.apache.kafka.common.header.internals.RecordHeader
import org.apache.kafka.common.network.TransferableChannel
import org.apache.kafka.common.utils.Time
import org.apache.kafka.common.utils.Utils.utf8
import org.apache.kafka.test.TestUtils.tempFile
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class LazyDownConversionRecordsTest {

    /**
     * Test the lazy down-conversion path in the presence of commit markers. When converting to V0 or V1, these batches
     * are dropped. If there happen to be no more batches left to convert, we must get an overflow message batch after
     * conversion.
     */
    @Test
    @Throws(IOException::class)
    fun testConversionOfCommitMarker() {
        val recordsToConvert = MemoryRecords.withEndTransactionMarker(
            initialOffset = 0,
            timestamp = Time.SYSTEM.milliseconds(),
            partitionLeaderEpoch = RecordBatch.NO_PARTITION_LEADER_EPOCH,
            producerId = 1,
            producerEpoch = 1.toShort(),
            marker = EndTransactionMarker(
                controlType = ControlRecordType.COMMIT,
                coordinatorEpoch = 0,
            ),
        )
        val convertedRecords = convertRecords(
            recordsToConvert = recordsToConvert,
            toMagic = 1,
            bytesToConvert = recordsToConvert.sizeInBytes(),
        )
        val buffer = convertedRecords.buffer()

        // read the offset and the batch length
        buffer.getLong()
        val sizeOfConvertedRecords = buffer.getInt()

        // assert we got an overflow message batch
        assertTrue(sizeOfConvertedRecords > buffer.limit())
        assertFalse(convertedRecords.batchIterator().hasNext())
    }

    /**
     * Test the lazy down-conversion path.
     *
     * If `overflow` is true, the number of bytes we want to convert is much larger
     * than the number of bytes we get after conversion. This causes overflow message batch(es) to be appended towards the
     * end of the converted output.
     */
    @ParameterizedTest(name = "compressionType={0}, toMagic={1}, overflow={2}")
    @MethodSource("parameters")
    @Throws(IOException::class)
    fun testConversion(compressionType: CompressionType, toMagic: Byte, overflow: Boolean) {
        doTestConversion(compressionType, toMagic, overflow)
    }

    @Throws(IOException::class)
    private fun doTestConversion(compressionType: CompressionType, toMagic: Byte, testConversionOverflow: Boolean) {
        val offsets = listOf(0L, 2L, 3L, 9L, 11L, 15L, 16L, 17L, 22L, 24L)
        val headers = arrayOf<Header>(
            RecordHeader("headerKey1", "headerValue1".toByteArray()),
            RecordHeader("headerKey2", "headerValue2".toByteArray()),
            RecordHeader("headerKey3", "headerValue3".toByteArray()),
        )
        val records = listOf(
            SimpleRecord(1L, "k1".toByteArray(), "hello".toByteArray()),
            SimpleRecord(2L, "k2".toByteArray(), "goodbye".toByteArray()),
            SimpleRecord(3L, "k3".toByteArray(), "hello again".toByteArray()),
            SimpleRecord(4L, "k4".toByteArray(), "goodbye for now".toByteArray()),
            SimpleRecord(5L, "k5".toByteArray(), "hello again".toByteArray()),
            SimpleRecord(6L, "k6".toByteArray(), "I sense indecision".toByteArray()),
            SimpleRecord(7L, "k7".toByteArray(), "what now".toByteArray()),
            SimpleRecord(8L, "k8".toByteArray(), "running out".toByteArray(), headers),
            SimpleRecord(9L, "k9".toByteArray(), "ok, almost done".toByteArray()),
            SimpleRecord(10L, "k10".toByteArray(), "finally".toByteArray(), headers)
        )
        assertEquals(offsets.size, records.size, "incorrect test setup")
        val buffer = ByteBuffer.allocate(1024)
        var builder = MemoryRecords.builder(
            buffer = buffer,
            magic = RecordBatch.CURRENT_MAGIC_VALUE,
            compressionType = compressionType,
            timestampType = TimestampType.CREATE_TIME,
            baseOffset = 0L,
        )
        for (i in 0..2) builder.appendWithOffset(offsets[i], records[i])
        builder.close()
        builder = MemoryRecords.builder(
            buffer = buffer,
            magic = RecordBatch.CURRENT_MAGIC_VALUE,
            compressionType = compressionType,
            timestampType = TimestampType.CREATE_TIME,
            baseOffset = 0L,
        )
        for (i in 3..5) builder.appendWithOffset(offsets[i], records[i])
        builder.close()
        builder = MemoryRecords.builder(
            buffer = buffer,
            magic = RecordBatch.CURRENT_MAGIC_VALUE,
            compressionType = compressionType,
            timestampType = TimestampType.CREATE_TIME,
            baseOffset = 0L,
        )
        for (i in 6..9) builder.appendWithOffset(offsets[i], records[i])
        builder.close()
        buffer.flip()
        val recordsToConvert = MemoryRecords.readableRecords(buffer)
        var numBytesToConvert = recordsToConvert.sizeInBytes()
        if (testConversionOverflow) numBytesToConvert *= 2
        val convertedRecords = convertRecords(
            recordsToConvert = recordsToConvert,
            toMagic = toMagic,
            bytesToConvert = numBytesToConvert,
        )
        verifyDownConvertedRecords(
            initialRecords = records,
            initialOffsets = offsets,
            downConvertedRecords = convertedRecords,
            compressionType = compressionType,
            toMagic = toMagic,
        )
    }

    companion object {
        private fun parameters(): Collection<Arguments> {
            val arguments: MutableList<Arguments> = ArrayList()
            for (toMagic in RecordBatch.MAGIC_VALUE_V0..RecordBatch.CURRENT_MAGIC_VALUE) {
                listOf(true, false).forEach { overflow ->
                    arguments.add(Arguments.of(CompressionType.NONE, toMagic, overflow))
                    arguments.add(Arguments.of(CompressionType.GZIP, toMagic, overflow))
                }
            }
            return arguments
        }

        @Throws(IOException::class)
        private fun convertRecords(
            recordsToConvert: MemoryRecords,
            toMagic: Byte,
            bytesToConvert: Int,
        ): MemoryRecords {
            FileRecords.open(tempFile()).use { inputRecords ->
                inputRecords.append(recordsToConvert)
                inputRecords.flush()
                val lazyRecords = LazyDownConversionRecords(
                    TopicPartition("test", 1),
                    inputRecords, toMagic, 0L, Time.SYSTEM
                )
                val lazySend = lazyRecords.toSend()
                val outputFile: File = tempFile()
                lateinit var convertedRecordsBuffer: ByteBuffer
                toTransferableChannel(
                    FileChannel.open(
                        outputFile.toPath(),
                        StandardOpenOption.READ,
                        StandardOpenOption.WRITE
                    )
                ).use { channel ->
                    var written = 0
                    while (written < bytesToConvert) written += lazySend.writeTo(
                        channel = channel,
                        previouslyWritten = written.toLong(),
                        remaining = bytesToConvert - written,
                    ).toInt()

                    FileRecords.open(
                        file = outputFile,
                        fileAlreadyExists = true,
                        initFileSize = written,
                        preallocate = false,
                    ).use { convertedRecords ->
                        convertedRecordsBuffer = ByteBuffer.allocate(convertedRecords.sizeInBytes())
                        convertedRecords.readInto(convertedRecordsBuffer, 0)
                    }
                }
                return MemoryRecords.readableRecords(convertedRecordsBuffer)
            }
        }

        private fun toTransferableChannel(channel: FileChannel): TransferableChannel {
            return object : TransferableChannel {
                override fun hasPendingWrites(): Boolean = false

                @Throws(IOException::class)
                override fun transferFrom(
                    fileChannel: FileChannel,
                    position: Long,
                    count: Long,
                ): Long = fileChannel.transferTo(position, count, channel)

                override fun isOpen(): Boolean = channel.isOpen

                @Throws(IOException::class)
                override fun close() = channel.close()

                @Throws(IOException::class)
                override fun write(src: ByteBuffer): Int = channel.write(src)

                @Throws(IOException::class)
                override fun write(srcs: Array<ByteBuffer>, offset: Int, length: Int): Long =
                    channel.write(srcs, offset, length)

                @Throws(IOException::class)
                override fun write(srcs: Array<ByteBuffer>): Long = channel.write(srcs)
            }
        }

        private fun verifyDownConvertedRecords(
            initialRecords: List<SimpleRecord>,
            initialOffsets: List<Long>,
            downConvertedRecords: MemoryRecords,
            compressionType: CompressionType,
            toMagic: Byte,
        ) {
            var i = 0
            for (batch in downConvertedRecords.batches()) {
                assertTrue(
                    batch.magic() <= toMagic,
                    "Magic byte should be lower than or equal to $toMagic"
                )
                if (batch.magic() == RecordBatch.MAGIC_VALUE_V0)
                    assertEquals(TimestampType.NO_TIMESTAMP_TYPE, batch.timestampType())
                else assertEquals(TimestampType.CREATE_TIME, batch.timestampType())
                assertEquals(
                    expected = compressionType,
                    actual = batch.compressionType(),
                    message = "Compression type should not be affected by conversion",
                )
                for (record in batch) {
                    assertTrue(record.hasMagic(batch.magic()), "Inner record should have magic $toMagic")
                    assertEquals(initialOffsets[i], record.offset(), "Offset should not change")
                    assertEquals(utf8(initialRecords[i].key!!), utf8(record.key()!!), "Key should not change")
                    assertEquals(utf8(initialRecords[i].value!!), utf8(record.value()!!), "Value should not change")
                    assertFalse(record.hasTimestampType(TimestampType.LOG_APPEND_TIME))
                    if (batch.magic() == RecordBatch.MAGIC_VALUE_V0) {
                        assertEquals(RecordBatch.NO_TIMESTAMP, record.timestamp())
                        assertFalse(record.hasTimestampType(TimestampType.CREATE_TIME))
                        assertTrue(record.hasTimestampType(TimestampType.NO_TIMESTAMP_TYPE))
                    } else if (batch.magic() == RecordBatch.MAGIC_VALUE_V1) {
                        assertEquals(
                            expected = initialRecords[i].timestamp,
                            actual = record.timestamp(),
                            message = "Timestamp should not change",
                        )
                        assertTrue(record.hasTimestampType(TimestampType.CREATE_TIME))
                        assertFalse(record.hasTimestampType(TimestampType.NO_TIMESTAMP_TYPE))
                    } else {
                        assertEquals(
                            expected = initialRecords[i].timestamp,
                            actual = record.timestamp(),
                            message = "Timestamp should not change",
                        )
                        assertFalse(record.hasTimestampType(TimestampType.CREATE_TIME))
                        assertFalse(record.hasTimestampType(TimestampType.NO_TIMESTAMP_TYPE))
                        assertContentEquals(
                            expected = initialRecords[i].headers,
                            actual = record.headers(),
                            message = "Headers should not change",
                        )
                    }
                    i += 1
                }
            }
            assertEquals(initialOffsets.size, i)
        }
    }
}
