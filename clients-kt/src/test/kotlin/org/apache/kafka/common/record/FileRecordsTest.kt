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
import java.util.concurrent.Executors
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.header.Header
import org.apache.kafka.common.header.internals.RecordHeader
import org.apache.kafka.common.network.TransferableChannel
import org.apache.kafka.common.record.FileRecords.LogOffsetPosition
import org.apache.kafka.common.record.FileRecords.TimestampAndOffset
import org.apache.kafka.common.utils.MockTime
import org.apache.kafka.common.utils.Time
import org.apache.kafka.common.utils.Utils.utf8
import org.apache.kafka.test.TestUtils.checkEquals
import org.apache.kafka.test.TestUtils.tempFile
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.ArgumentMatchers.any
import org.mockito.ArgumentMatchers.anyLong
import org.mockito.ArgumentMatchers.eq
import org.mockito.Mockito.`when`
import org.mockito.kotlin.atLeastOnce
import org.mockito.kotlin.mock
import org.mockito.kotlin.times
import org.mockito.kotlin.verify
import kotlin.math.max
import kotlin.math.min
import kotlin.random.Random
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

class FileRecordsTest {
    
    private val values = arrayOf(
        "abcd".toByteArray(),
        "efgh".toByteArray(),
        "ijkl".toByteArray(),
    )
    
    private lateinit var fileRecords: FileRecords
    
    private lateinit var time: Time
    
    @BeforeEach
    @Throws(IOException::class)
    fun setup() {
        fileRecords = createFileRecords(values)
        time = MockTime()
    }

    @AfterEach
    @Throws(IOException::class)
    fun cleanup() {
        fileRecords.close()
    }

    @Test
    @Throws(Exception::class)
    fun testAppendProtectsFromOverflow() {
        val fileMock = mock<File>()
        val fileChannelMock = mock<FileChannel>()
        `when`(fileChannelMock.size()).thenReturn(Int.MAX_VALUE.toLong())
        val records = FileRecords(
            file = fileMock,
            channel = fileChannelMock,
            start = 0,
            end = Int.MAX_VALUE,
            isSlice = false,
        )
        assertFailsWith<IllegalArgumentException> { append(records, values) }
    }

    @Test
    @Throws(Exception::class)
    fun testOpenOversizeFile() {
        val fileMock = mock<File>()
        val fileChannelMock = mock<FileChannel>()
        `when`(fileChannelMock.size()).thenReturn(Int.MAX_VALUE + 5L)
        assertFailsWith<KafkaException> {
            FileRecords(
                file = fileMock,
                channel = fileChannelMock,
                start = 0,
                end = Int.MAX_VALUE,
                isSlice = false,
            )
        }
    }

    @Test
    fun testOutOfRangeSlice() {
        assertFailsWith<IllegalArgumentException> {
            fileRecords.slice(fileRecords.sizeInBytes() + 1, 15).sizeInBytes()
        }
    }

    /**
     * Test that the cached size variable matches the actual file size as we append messages
     */
    @Test
    @Throws(IOException::class)
    fun testFileSize() {
        assertEquals(fileRecords.channel.size(), fileRecords.sizeInBytes().toLong())
        repeat(20) {
            fileRecords.append(
                MemoryRecords.withRecords(
                    compressionType = CompressionType.NONE,
                    records = arrayOf(SimpleRecord(value = "abcd".toByteArray())),
                )
            )
            assertEquals(fileRecords.channel.size(), fileRecords.sizeInBytes().toLong())
        }
    }

    /**
     * Test that adding invalid bytes to the end of the log doesn't break iteration
     */
    @Test
    @Throws(IOException::class)
    fun testIterationOverPartialAndTruncation() {
        testPartialWrite(size = 0, fileRecords = fileRecords)
        testPartialWrite(size = 2, fileRecords = fileRecords)
        testPartialWrite(size = 4, fileRecords = fileRecords)
        testPartialWrite(size = 5, fileRecords = fileRecords)
        testPartialWrite(size = 6, fileRecords = fileRecords)
    }

    @Test
    @Throws(Exception::class)
    fun testSliceSizeLimitWithConcurrentWrite() {
        val log = FileRecords.open(tempFile())
        val executor = Executors.newFixedThreadPool(2)
        val maxSizeInBytes = 16384
        try {
            val readerCompletion = executor.submit<Unit> {
                while (log.sizeInBytes() < maxSizeInBytes) {
                    val currentSize = log.sizeInBytes()
                    val slice = log.slice(0, currentSize)
                    assertEquals(currentSize, slice.sizeInBytes())
                }
            }
            val writerCompletion = executor.submit<Unit> {
                while (log.sizeInBytes() < maxSizeInBytes) append(log, values)
            }
            writerCompletion.get()
            readerCompletion.get()
        } finally {
            executor.shutdownNow()
        }
    }

    @Throws(IOException::class)
    private fun testPartialWrite(size: Int, fileRecords: FileRecords) {
        val buffer = ByteBuffer.allocate(size)
        for (i in 0 until size) buffer.put(0.toByte())
        buffer.rewind()
        fileRecords.channel.write(buffer)

        // appending those bytes should not change the contents
        val records = fileRecords.records().iterator()
        for (value in values) {
            assertTrue(records.hasNext())
            assertEquals(records.next().value(), ByteBuffer.wrap(value))
        }
    }

    /**
     * Iterating over the file does file reads but shouldn't change the position of the underlying FileChannel.
     */
    @Test
    @Throws(IOException::class)
    fun testIterationDoesntChangePosition() {
        val position = fileRecords.channel.position()
        val records = fileRecords.records().iterator()
        for (value in values) {
            assertTrue(records.hasNext())
            assertEquals(records.next().value(), ByteBuffer.wrap(value))
        }
        assertEquals(position, fileRecords.channel.position())
    }

    /**
     * Test a simple append and read.
     */
    @Test
    @Throws(IOException::class)
    fun testRead() {
        var read = fileRecords.slice(0, fileRecords.sizeInBytes())
        assertEquals(fileRecords.sizeInBytes(), read.sizeInBytes())
        checkEquals(fileRecords.batches(), read.batches())
        val items = batches(read)
        val first = items[0]

        // read from second message until the end
        read = fileRecords.slice(first.sizeInBytes(), fileRecords.sizeInBytes() - first.sizeInBytes())
        assertEquals(fileRecords.sizeInBytes() - first.sizeInBytes(), read.sizeInBytes())
        assertEquals(items.subList(1, items.size), batches(read), "Read starting from the second message")

        // read from second message and size is past the end of the file
        read = fileRecords.slice(first.sizeInBytes(), fileRecords.sizeInBytes())
        assertEquals(fileRecords.sizeInBytes() - first.sizeInBytes(), read.sizeInBytes())
        assertEquals(items.subList(1, items.size), batches(read), "Read starting from the second message")

        // read from second message and position + size overflows
        read = fileRecords.slice(first.sizeInBytes(), Int.MAX_VALUE)
        assertEquals(fileRecords.sizeInBytes() - first.sizeInBytes(), read.sizeInBytes())
        assertEquals(items.subList(1, items.size), batches(read), "Read starting from the second message")

        // read from second message and size is past the end of the file on a view/slice
        read = fileRecords.slice(1, fileRecords.sizeInBytes() - 1)
            .slice(first.sizeInBytes() - 1, fileRecords.sizeInBytes())
        assertEquals(fileRecords.sizeInBytes() - first.sizeInBytes(), read.sizeInBytes())
        assertEquals(items.subList(1, items.size), batches(read), "Read starting from the second message")

        // read from second message and position + size overflows on a view/slice
        read = fileRecords.slice(1, fileRecords.sizeInBytes() - 1)
            .slice(first.sizeInBytes() - 1, Int.MAX_VALUE)
        assertEquals(fileRecords.sizeInBytes() - first.sizeInBytes(), read.sizeInBytes())
        assertEquals(items.subList(1, items.size), batches(read), "Read starting from the second message")

        // read a single message starting from second message
        val second = items[1]
        read = fileRecords.slice(first.sizeInBytes(), second.sizeInBytes())
        assertEquals(second.sizeInBytes(), read.sizeInBytes())
        assertEquals(listOf(second), batches(read), "Read a single message starting from the second message")
    }

    /**
     * Test the MessageSet.searchFor API.
     */
    @Test
    @Throws(IOException::class)
    fun testSearch() {
        // append a new message with a high offset
        val lastMessage = SimpleRecord(value = "test".toByteArray())
        fileRecords.append(
            MemoryRecords.withRecords(
                initialOffset = 50L,
                compressionType = CompressionType.NONE,
                records = arrayOf(lastMessage),
            ),
        )
        val batches = batches(fileRecords)
        var position = 0
        val message1Size = batches[0].sizeInBytes()
        assertEquals(
            expected = LogOffsetPosition(0L, position, message1Size),
            actual = fileRecords.searchForOffsetWithSize(0, 0),
            message = "Should be able to find the first message by its offset",
        )
        position += message1Size
        val message2Size = batches[1].sizeInBytes()
        assertEquals(
            expected = LogOffsetPosition(1L, position, message2Size),
            actual = fileRecords.searchForOffsetWithSize(1, 0),
            message = "Should be able to find second message when starting from 0",
        )
        assertEquals(
            expected = LogOffsetPosition(1L, position, message2Size),
            actual = fileRecords.searchForOffsetWithSize(1, position),
            message = "Should be able to find second message starting from its offset",
        )
        position += message2Size + batches[2].sizeInBytes()
        val message4Size = batches[3].sizeInBytes()
        assertEquals(
            expected = LogOffsetPosition(50L, position, message4Size),
            actual = fileRecords.searchForOffsetWithSize(3, position),
            message = "Should be able to find fourth message from a non-existent offset",
        )
        assertEquals(
            expected = LogOffsetPosition(50L, position, message4Size),
            actual = fileRecords.searchForOffsetWithSize(50, position),
            message = "Should be able to find fourth message by correct offset",
        )
    }

    /**
     * Test that the message set iterator obeys start and end slicing
     */
    @Test
    @Throws(IOException::class)
    fun testIteratorWithLimits() {
        val batch = batches(fileRecords)[1]
        val start = fileRecords.searchForOffsetWithSize(1, 0)!!.position
        val size = batch.sizeInBytes()
        val slice = fileRecords.slice(start, size)
        assertEquals(listOf(batch), batches(slice))
        val slice2 = fileRecords.slice(start, size - 1)
        assertEquals(emptyList<Any>(), batches(slice2))
    }

    /**
     * Test the truncateTo method lops off messages and appropriately updates the size
     */
    @Test
    @Throws(IOException::class)
    fun testTruncate() {
        val batch = batches(fileRecords)[0]
        val end = fileRecords.searchForOffsetWithSize(1, 0)!!.position
        fileRecords.truncateTo(end)
        assertEquals(listOf(batch), batches(fileRecords))
        assertEquals(batch.sizeInBytes(), fileRecords.sizeInBytes())
    }

    /**
     * Test that truncateTo only calls truncate on the FileChannel if the size of the
     * FileChannel is bigger than the target size. This is important because some JVMs
     * change the mtime of the file, even if truncate should do nothing.
     */
    @Test
    @Throws(IOException::class)
    fun testTruncateNotCalledIfSizeIsSameAsTargetSize() {
        val channelMock = mock<FileChannel>()
        `when`(channelMock.size()).thenReturn(42L)
        `when`(channelMock.position(42L)).thenReturn(null)
        val fileRecords = FileRecords(
            file = tempFile(),
            channel = channelMock,
            start = 0,
            end = Int.MAX_VALUE,
            isSlice = false,
        )
        fileRecords.truncateTo(42)
        verify(channelMock, atLeastOnce()).size()
        verify(channelMock, times(0)).truncate(anyLong())
    }

    /**
     * Expect a KafkaException if targetSize is bigger than the size of
     * the FileRecords.
     */
    @Test
    @Throws(IOException::class)
    fun testTruncateNotCalledIfSizeIsBiggerThanTargetSize() {
        val channelMock = mock<FileChannel>()
        `when`(channelMock.size()).thenReturn(42L)
        val fileRecords = FileRecords(
            file = tempFile(),
            channel = channelMock,
            start = 0,
            end = Int.MAX_VALUE,
            isSlice = false,
        )
        assertFailsWith<KafkaException>("Should throw KafkaException") {
            fileRecords.truncateTo(43)
        }
        verify(channelMock, atLeastOnce()).size()
    }

    /**
     * see #testTruncateNotCalledIfSizeIsSameAsTargetSize
     */
    @Test
    @Throws(IOException::class)
    fun testTruncateIfSizeIsDifferentToTargetSize() {
        val channelMock = mock<FileChannel>()
        `when`(channelMock.size()).thenReturn(42L)
        `when`(channelMock.truncate(anyLong())).thenReturn(channelMock)
        val fileRecords = FileRecords(
            file = tempFile(),
            channel = channelMock,
            start = 0,
            end = Int.MAX_VALUE,
            isSlice = false,
        )
        fileRecords.truncateTo(23)
        verify(channelMock, atLeastOnce()).size()
        verify(channelMock).truncate(23)
    }

    /**
     * Test the new FileRecords with pre allocate as true
     */
    @Test
    @Throws(IOException::class)
    fun testPreallocateTrue() {
        val temp: File = tempFile()
        val fileRecords = FileRecords.open(
            file = temp,
            fileAlreadyExists = false,
            initFileSize = 1024 * 1024,
            preallocate = true,
        )
        val position = fileRecords.channel.position()
        val size = fileRecords.sizeInBytes()
        assertEquals(0, position)
        assertEquals(0, size)
        assertEquals((1024 * 1024).toLong(), temp.length())
    }

    /**
     * Test the new FileRecords with pre allocate as false
     */
    @Test
    @Throws(IOException::class)
    fun testPreallocateFalse() {
        val temp: File = tempFile()
        val set = FileRecords.open(
            file = temp,
            fileAlreadyExists = false,
            initFileSize = 1024 * 1024,
            preallocate = false,
        )
        val position = set.channel.position()
        val size = set.sizeInBytes()
        assertEquals(0, position)
        assertEquals(0, size)
        assertEquals(0, temp.length())
    }

    /**
     * Test the new FileRecords with pre allocate as true and file has been clearly shut down, the file will be truncate to end of valid data.
     */
    @Test
    @Throws(IOException::class)
    fun testPreallocateClearShutdown() {
        val temp: File = tempFile()
        val fileRecords = FileRecords.open(
            file = temp,
            fileAlreadyExists = false,
            initFileSize = 1024 * 1024,
            preallocate = true,
        )
        append(fileRecords, values)
        val oldPosition = fileRecords.channel.position().toInt()
        val oldSize = fileRecords.sizeInBytes()
        assertEquals(this.fileRecords.sizeInBytes(), oldPosition)
        assertEquals(this.fileRecords.sizeInBytes(), oldSize)
        fileRecords.close()
        val tempReopen = File(temp.absolutePath)
        val setReopen = FileRecords.open(
            file = tempReopen,
            fileAlreadyExists = true,
            initFileSize = 1024 * 1024,
            preallocate = true,
        )
        val position = setReopen.channel.position().toInt()
        val size = setReopen.sizeInBytes()
        assertEquals(oldPosition, position)
        assertEquals(oldPosition, size)
        assertEquals(oldPosition.toLong(), tempReopen.length())
    }

    @Test
    @Throws(IOException::class)
    fun testFormatConversionWithPartialMessage() {
        val batch = batches(fileRecords)[1]
        val start = fileRecords.searchForOffsetWithSize(targetOffset = 1, startingPosition = 0)!!.position
        val size = batch.sizeInBytes()
        val slice = fileRecords.slice(start, size - 1)
        val messageV0 = slice.downConvert(
            toMagic = RecordBatch.MAGIC_VALUE_V0,
            firstOffset = 0,
            time = time,
        ).records
        assertTrue(batches(messageV0).isEmpty(), "No message should be there")
        assertEquals(size - 1, messageV0.sizeInBytes(), "There should be ${size - 1} bytes")

        // Lazy down-conversion will not return any messages for a partial input batch
        val tp = TopicPartition("topic-1", 0)
        val lazyRecords = LazyDownConversionRecords(
            topicPartition = tp,
            records = slice,
            toMagic = RecordBatch.MAGIC_VALUE_V0,
            firstOffset = 0,
            time = Time.SYSTEM,
        )
        val it = lazyRecords.iterator(16 * 1024L)
        assertFalse(it.hasNext(), "No messages should be returned")
    }

    @Test
    @Throws(IOException::class)
    fun testFormatConversionWithNoMessages() {
        val tp = TopicPartition("topic-1", 0)
        val lazyRecords = LazyDownConversionRecords(
            topicPartition = tp,
            records = MemoryRecords.EMPTY,
            toMagic = RecordBatch.MAGIC_VALUE_V0,
            firstOffset = 0,
            time = Time.SYSTEM,
        )
        assertEquals(0, lazyRecords.sizeInBytes())
        val it = lazyRecords.iterator(16 * 1024L)
        assertFalse(it.hasNext(), "No messages should be returned")
    }

    @Test
    @Throws(IOException::class)
    fun testSearchForTimestamp() {
        for (version in RecordVersion.values()) {
            testSearchForTimestamp(version)
        }
    }

    @Throws(IOException::class)
    private fun testSearchForTimestamp(version: RecordVersion) {
        val temp: File = tempFile()
        val fileRecords = FileRecords.open(
            file = temp,
            fileAlreadyExists = false,
            initFileSize = 1024 * 1024,
            preallocate = true,
        )
        appendWithOffsetAndTimestamp(
            fileRecords = fileRecords,
            recordVersion = version,
            timestamp = 10L,
            offset = 5,
            leaderEpoch = 0,
        )
        appendWithOffsetAndTimestamp(
            fileRecords = fileRecords,
            recordVersion = version,
            timestamp = 11L,
            offset = 6,
            leaderEpoch = 1,
        )
        assertFoundTimestamp(
            expected = TimestampAndOffset(timestamp = 10L, offset = 5, leaderEpoch = 0),
            actual = fileRecords.searchForTimestamp(
                targetTimestamp = 9L,
                startingPosition = 0,
                startingOffset = 0L,
            ),
            version = version
        )
        assertFoundTimestamp(
            expected = TimestampAndOffset(timestamp = 10L, offset = 5, leaderEpoch = 0),
            actual = fileRecords.searchForTimestamp(
                targetTimestamp = 10L,
                startingPosition = 0,
                startingOffset = 0L,
            ),
            version = version,
        )
        assertFoundTimestamp(
            TimestampAndOffset(timestamp = 11L, offset = 6, leaderEpoch = 1),
            fileRecords.searchForTimestamp(
                targetTimestamp = 11L,
                startingPosition = 0,
                startingOffset = 0L,
            ), version
        )
        assertNull(
            fileRecords.searchForTimestamp(
                targetTimestamp = 12L,
                startingPosition = 0,
                startingOffset = 0L,
            )
        )
    }

    private fun assertFoundTimestamp(
        expected: TimestampAndOffset,
        actual: TimestampAndOffset?,
        version: RecordVersion,
    ) {
        if (version == RecordVersion.V0) {
            assertNull(actual, "Expected no match for message format v0")
        } else {
            assertNotNull(actual, "Expected to find timestamp for message format $version")
            assertEquals(
                expected = expected.timestamp, actual = actual.timestamp,
                message = "Expected matching timestamps for message format$version",
            )
            assertEquals(
                expected.offset, actual.offset,
                "Expected matching offsets for message format $version"
            )
            val expectedLeaderEpoch =
                if (version.value >= RecordVersion.V2.value) expected.leaderEpoch
                else null
            assertEquals(
                expected = expectedLeaderEpoch, actual = actual.leaderEpoch,
                message = "Non-matching leader epoch for version $version"
            )
        }
    }

    @Throws(IOException::class)
    private fun appendWithOffsetAndTimestamp(
        fileRecords: FileRecords,
        recordVersion: RecordVersion,
        timestamp: Long,
        offset: Long,
        leaderEpoch: Int,
    ) {
        val buffer = ByteBuffer.allocate(128)
        val builder = MemoryRecords.builder(
            buffer = buffer,
            magic = recordVersion.value,
            compressionType = CompressionType.NONE,
            timestampType = TimestampType.CREATE_TIME,
            baseOffset = offset,
            logAppendTime = timestamp,
            partitionLeaderEpoch = leaderEpoch,
        )
        builder.append(SimpleRecord(timestamp, ByteArray(0), ByteArray(0)))
        fileRecords.append(builder.build())
    }

    @Test
    @Throws(IOException::class)
    fun testDownconversionAfterMessageFormatDowngrade() {
        // random bytes
        val bytes = ByteArray(3000)
        Random.nextBytes(bytes)

        // records
        val compressionType = CompressionType.GZIP
        val offsets: List<Long> = mutableListOf(0L, 1L)
        val magic = listOf(
            RecordBatch.MAGIC_VALUE_V2,
            RecordBatch.MAGIC_VALUE_V1,
        ) // downgrade message format from v2 to v1
        val records = listOf(
            SimpleRecord(timestamp = 1L, key = "k1".toByteArray(), value = bytes),
            SimpleRecord(timestamp = 2L, key = "k2".toByteArray(), value = bytes),
        )
        val toMagic = 1.toByte()

        // create MemoryRecords
        val buffer = ByteBuffer.allocate(8000)
        for (i in records.indices) {
            val builder = MemoryRecords.builder(
                buffer = buffer,
                magic = magic[i],
                compressionType = compressionType,
                timestampType = TimestampType.CREATE_TIME,
                baseOffset = 0L,
            )
            builder.appendWithOffset(offsets[i], records[i])
            builder.close()
        }
        buffer.flip()
        FileRecords.open(tempFile()).use { fileRecords ->
            fileRecords.append(MemoryRecords.readableRecords(buffer))
            fileRecords.flush()
            downConvertAndVerifyRecords(
                initialRecords = records,
                initialOffsets = offsets,
                fileRecords = fileRecords,
                compressionType = compressionType,
                toMagic = toMagic,
                firstOffset = 0L,
                time = time,
            )
        }
    }

    @Test
    @Throws(IOException::class)
    fun testConversion() {
        doTestConversion(CompressionType.NONE, RecordBatch.MAGIC_VALUE_V0)
        doTestConversion(CompressionType.GZIP, RecordBatch.MAGIC_VALUE_V0)
        doTestConversion(CompressionType.NONE, RecordBatch.MAGIC_VALUE_V1)
        doTestConversion(CompressionType.GZIP, RecordBatch.MAGIC_VALUE_V1)
        doTestConversion(CompressionType.NONE, RecordBatch.MAGIC_VALUE_V2)
        doTestConversion(CompressionType.GZIP, RecordBatch.MAGIC_VALUE_V2)
    }

    @Test
    @Throws(IOException::class)
    fun testBytesLengthOfWriteTo() {
        val size = fileRecords.sizeInBytes()
        val firstWritten = (size / 3).toLong()
        val channel = mock<TransferableChannel>()

        // Firstly we wrote some of the data
        fileRecords.writeTo(channel, 0, firstWritten.toInt())
        verify(channel).transferFrom(any(), anyLong(), eq(firstWritten))

        // Ensure (length > size - firstWritten)
        val secondWrittenLength = size - firstWritten.toInt() + 1
        fileRecords.writeTo(channel, firstWritten, secondWrittenLength)
        // But we still only write (size - firstWritten), which is not fulfilled in the old version
        verify(channel).transferFrom(any(), anyLong(), eq(size - firstWritten))
    }

    @Throws(IOException::class)
    private fun doTestConversion(compressionType: CompressionType, toMagic: Byte) {
        val offsets: List<Long> = mutableListOf(0L, 2L, 3L, 9L, 11L, 15L, 16L, 17L, 22L, 24L)
        val headers = arrayOf<Header>(
            RecordHeader("headerKey1", "headerValue1".toByteArray()),
            RecordHeader("headerKey2", "headerValue2".toByteArray()),
            RecordHeader("headerKey3", "headerValue3".toByteArray())
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
            SimpleRecord(10L, "k10".toByteArray(), "finally".toByteArray(), headers),
        )
        assertEquals(offsets.size, records.size, "incorrect test setup")
        val buffer = ByteBuffer.allocate(1024)
        var builder = MemoryRecords.builder(
            buffer = buffer,
            magic = RecordBatch.MAGIC_VALUE_V0,
            compressionType = compressionType,
            timestampType = TimestampType.CREATE_TIME,
            baseOffset = 0L,
        )
        for (i in 0..2) builder.appendWithOffset(offsets[i], records[i])
        builder.close()
        builder = MemoryRecords.builder(
            buffer = buffer,
            magic = RecordBatch.MAGIC_VALUE_V1,
            compressionType = compressionType,
            timestampType = TimestampType.CREATE_TIME,
            baseOffset = 0L,
        )
        for (i in 3..5) builder.appendWithOffset(offsets[i], records[i])
        builder.close()
        builder =
            MemoryRecords.builder(
                buffer = buffer,
                magic = RecordBatch.MAGIC_VALUE_V2,
                compressionType = compressionType,
                timestampType = TimestampType.CREATE_TIME,
                baseOffset = 0L,
            )
        for (i in 6..9) builder.appendWithOffset(offsets[i], records[i])
        builder.close()
        buffer.flip()
        FileRecords.open(tempFile()).use { fileRecords ->
            fileRecords.append(MemoryRecords.readableRecords(buffer))
            fileRecords.flush()
            downConvertAndVerifyRecords(
                initialRecords = records,
                initialOffsets = offsets,
                fileRecords = fileRecords,
                compressionType = compressionType,
                toMagic = toMagic,
                firstOffset = 0L,
                time = time,
            )
            if (toMagic <= RecordBatch.MAGIC_VALUE_V1 && compressionType === CompressionType.NONE) {
                val firstOffset = if (toMagic == RecordBatch.MAGIC_VALUE_V0) 11L // v1 record
                else 17 // v2 record
                val filteredOffsets = offsets.toMutableList()
                val filteredRecords = records.toMutableList()
                val index = filteredOffsets.indexOf(firstOffset) - 1
                filteredRecords.removeAt(index)
                filteredOffsets.removeAt(index)
                downConvertAndVerifyRecords(
                    initialRecords = filteredRecords,
                    initialOffsets = filteredOffsets,
                    fileRecords = fileRecords,
                    compressionType = compressionType,
                    toMagic = toMagic,
                    firstOffset = firstOffset,
                    time = time,
                )
            } else {
                // firstOffset doesn't have any effect in this case
                downConvertAndVerifyRecords(
                    initialRecords = records,
                    initialOffsets = offsets,
                    fileRecords = fileRecords,
                    compressionType = compressionType,
                    toMagic = toMagic,
                    firstOffset = 10L,
                    time = time,
                )
            }
        }
    }

    private fun downConvertAndVerifyRecords(
        initialRecords: List<SimpleRecord>,
        initialOffsets: List<Long>,
        fileRecords: FileRecords,
        compressionType: CompressionType,
        toMagic: Byte,
        firstOffset: Long,
        time: Time?,
    ) {
        var minBatchSize = Long.MAX_VALUE
        var maxBatchSize = Long.MIN_VALUE
        for (batch in fileRecords.batches()) {
            minBatchSize = min(minBatchSize.toDouble(), batch.sizeInBytes().toDouble()).toLong()
            maxBatchSize = max(maxBatchSize.toDouble(), batch.sizeInBytes().toDouble()).toLong()
        }

        // Test the normal down-conversion path
        val convertedRecords: MutableList<Records> = ArrayList()
        convertedRecords.add(fileRecords.downConvert(toMagic, firstOffset, time!!).records)
        verifyConvertedRecords(initialRecords, initialOffsets, convertedRecords, compressionType, toMagic)
        convertedRecords.clear()

        // Test the lazy down-conversion path
        val maximumReadSize = listOf(
            16L * 1024L,
            fileRecords.sizeInBytes().toLong(),
            fileRecords.sizeInBytes().toLong() - 1,
            fileRecords.sizeInBytes().toLong() / 4,
            maxBatchSize + 1,
            1L,
        )
        for (readSize in maximumReadSize) {
            val tp = TopicPartition("topic-1", 0)
            val lazyRecords = LazyDownConversionRecords(
                topicPartition = tp,
                records = fileRecords,
                toMagic = toMagic,
                firstOffset = firstOffset,
                time = Time.SYSTEM,
            )
            val it = lazyRecords.iterator(readSize)
            while (it.hasNext()) convertedRecords.add(it.next().records)
            verifyConvertedRecords(
                initialRecords = initialRecords,
                initialOffsets = initialOffsets,
                convertedRecordsList = convertedRecords,
                compressionType = compressionType,
                magicByte = toMagic,
            )
            convertedRecords.clear()
        }
    }

    private fun verifyConvertedRecords(
        initialRecords: List<SimpleRecord>,
        initialOffsets: List<Long>,
        convertedRecordsList: List<Records>,
        compressionType: CompressionType,
        magicByte: Byte,
    ) {
        var i = 0
        for (convertedRecords in convertedRecordsList) {
            for (batch in convertedRecords.batches()) {
                assertTrue(
                    batch.magic() <= magicByte,
                    "Magic byte should be lower than or equal to $magicByte"
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
                    assertTrue(record.hasMagic(batch.magic()), "Inner record should have magic $magicByte")
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
                            message = "Timestamp should not change"
                        )
                        assertTrue(record.hasTimestampType(TimestampType.CREATE_TIME))
                        assertFalse(record.hasTimestampType(TimestampType.NO_TIMESTAMP_TYPE))
                    } else {
                        assertEquals(
                            expected = initialRecords[i].timestamp,
                            actual = record.timestamp(),
                            message = "Timestamp should not change"
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
        }
        assertEquals(initialOffsets.size, i)
    }

    @Throws(IOException::class)
    private fun createFileRecords(values: Array<ByteArray>): FileRecords {
        val fileRecords = FileRecords.open(tempFile())
        append(fileRecords, values)
        return fileRecords
    }

    @Throws(IOException::class)
    private fun append(fileRecords: FileRecords, values: Array<ByteArray>) {
        var offset = 0L
        for (value in values) {
            val buffer = ByteBuffer.allocate(128)
            val builder = MemoryRecords.builder(
                buffer = buffer,
                magic = RecordBatch.CURRENT_MAGIC_VALUE,
                compressionType = CompressionType.NONE,
                timestampType = TimestampType.CREATE_TIME,
                baseOffset = offset,
            )
            builder.appendWithOffset(offset++, System.currentTimeMillis(), null, value)
            fileRecords.append(builder.build())
        }
        fileRecords.flush()
    }

    companion object {
        private fun batches(buffer: Records): List<RecordBatch> {
            return buffer.batches().toList()
        }
    }
}
