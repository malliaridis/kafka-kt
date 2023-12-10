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

import java.io.DataOutputStream
import java.io.IOException
import java.nio.ByteBuffer
import org.apache.kafka.common.InvalidRecordException
import org.apache.kafka.common.header.Header
import org.apache.kafka.common.header.internals.RecordHeader
import org.apache.kafka.common.utils.ByteBufferInputStream
import org.apache.kafka.common.utils.ByteBufferOutputStream
import org.apache.kafka.common.utils.ByteUtils.sizeOfVarint
import org.apache.kafka.common.utils.ByteUtils.writeVarint
import org.apache.kafka.common.utils.ByteUtils.writeVarlong
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotNull

class DefaultRecordTest {

    @Test
    @Throws(IOException::class)
    fun testBasicSerde() {
        val headers = arrayOf<Header>(
            RecordHeader("foo", "value".toByteArray()),
            RecordHeader("bar", null as ByteArray?),
            RecordHeader("\"A\\u00ea\\u00f1\\u00fcC\"", "value".toByteArray())
        )
        val records = arrayOf(
            SimpleRecord(key = "hi".toByteArray(), value = "there".toByteArray()),
            SimpleRecord(key = null, value = "there".toByteArray()),
            SimpleRecord(key = "hi".toByteArray(), value = null),
            SimpleRecord(key = null as ByteArray?, value = null),
            SimpleRecord(timestamp = 15L, key = "hi".toByteArray(), value = "there".toByteArray(), headers = headers)
        )
        for (record in records) {
            val baseSequence = 723
            val baseOffset: Long = 37
            val offsetDelta = 10
            val baseTimestamp = System.currentTimeMillis()
            val timestampDelta: Long = 323
            val out = ByteBufferOutputStream(1024)
            DefaultRecord.writeTo(
                out = DataOutputStream(out),
                offsetDelta = offsetDelta,
                timestampDelta = timestampDelta,
                key = record.key,
                value = record.value,
                headers = record.headers,
            )
            val buffer = out.buffer
            buffer.flip()
            val logRecord = DefaultRecord.readFrom(
                buffer = buffer,
                baseOffset = baseOffset,
                baseTimestamp = baseTimestamp,
                baseSequence = baseSequence,
                logAppendTime = null,
            )
            assertNotNull(logRecord)
            assertEquals(baseOffset + offsetDelta, logRecord.offset())
            assertEquals(baseSequence + offsetDelta, logRecord.sequence())
            assertEquals(baseTimestamp + timestampDelta, logRecord.timestamp())
            assertEquals(record.key, logRecord.key())
            assertEquals(record.value, logRecord.value())
            assertContentEquals(record.headers, logRecord.headers())
            assertEquals(
                expected = DefaultRecord.sizeInBytes(
                    offsetDelta = offsetDelta,
                    timestampDelta = timestampDelta,
                    key = record.key,
                    value = record.value,
                    headers = record.headers,
                ),
                actual = logRecord.sizeInBytes(),
            )
        }
    }

    @Test
    @Throws(IOException::class)
    fun testBasicSerdeInvalidHeaderCountTooHigh() {
        val headers = arrayOf<Header>(
            RecordHeader(key = "foo", value = "value".toByteArray()),
            RecordHeader(key = "bar", value = null),
            RecordHeader(key = "\"A\\u00ea\\u00f1\\u00fcC\"", value = "value".toByteArray())
        )
        val record = SimpleRecord(
            timestamp = 15L,
            key = "hi".toByteArray(),
            value = "there".toByteArray(),
            headers = headers
        )
        val baseSequence = 723
        val baseOffset: Long = 37
        val offsetDelta = 10
        val baseTimestamp = System.currentTimeMillis()
        val timestampDelta: Long = 323
        val out = ByteBufferOutputStream(1024)
        DefaultRecord.writeTo(
            out = DataOutputStream(out),
            offsetDelta = offsetDelta,
            timestampDelta = timestampDelta,
            key = record.key,
            value = record.value,
            headers = record.headers,
        )
        val buffer = out.buffer
        buffer.flip()
        buffer.put(14, 8.toByte())
        // test for input stream input
        ByteBufferInputStream(buffer.asReadOnlyBuffer()).use { inpStream ->
            assertFailsWith<InvalidRecordException> {
                DefaultRecord.readFrom(
                    input = inpStream,
                    baseOffset = baseOffset,
                    baseTimestamp = baseTimestamp,
                    baseSequence = baseSequence,
                    logAppendTime = null,
                )
            }
        }
        // test for buffer input
        assertFailsWith<InvalidRecordException> {
            DefaultRecord.readFrom(
                buffer = buffer,
                baseOffset = baseOffset,
                baseTimestamp = baseTimestamp,
                baseSequence = baseSequence,
                logAppendTime = null,
            )
        }
    }

    @Test
    @Throws(IOException::class)
    fun testBasicSerdeInvalidHeaderCountTooLow() {
        val headers = arrayOf<Header>(
            RecordHeader("foo", "value".toByteArray()),
            RecordHeader("bar", null),
            RecordHeader("\"A\\u00ea\\u00f1\\u00fcC\"", "value".toByteArray())
        )
        val record = SimpleRecord(
            timestamp = 15L,
            key = "hi".toByteArray(),
            value = "there".toByteArray(),
            headers = headers,
        )
        val baseSequence = 723
        val baseOffset: Long = 37
        val offsetDelta = 10
        val baseTimestamp = System.currentTimeMillis()
        val timestampDelta: Long = 323
        val out = ByteBufferOutputStream(1024)
        DefaultRecord.writeTo(
            out = DataOutputStream(out),
            offsetDelta = offsetDelta,
            timestampDelta = timestampDelta,
            key = record.key,
            value = record.value,
            headers = record.headers,
        )
        val buffer = out.buffer
        buffer.flip()
        buffer.put(14, 4.toByte())
        assertFailsWith<InvalidRecordException> {
            DefaultRecord.readFrom(
                buffer = buffer,
                baseOffset = baseOffset,
                baseTimestamp = baseTimestamp,
                baseSequence = baseSequence,
                logAppendTime = null,
            )
        }
    }

    @Test
    @Throws(IOException::class)
    fun testInvalidKeySize() {
        val attributes: Byte = 0
        val timestampDelta: Long = 2
        val offsetDelta = 1
        val sizeOfBodyInBytes = 100
        val keySize = 105 // use a key size larger than the full message
        val buf = ByteBuffer.allocate(sizeOfBodyInBytes + sizeOfVarint(sizeOfBodyInBytes))
        writeVarint(sizeOfBodyInBytes, buf)
        buf.put(attributes)
        writeVarlong(timestampDelta, buf)
        writeVarint(offsetDelta, buf)
        writeVarint(keySize, buf)
        buf.position(buf.limit())
        buf.flip()
        assertDecodingRecordFromBufferThrowsInvalidRecordException(buf)
    }

    @Test
    @Throws(IOException::class)
    fun testInvalidKeySizePartial() {
        val attributes: Byte = 0
        val timestampDelta: Long = 2
        val offsetDelta = 1
        val sizeOfBodyInBytes = 100
        val keySize = 105 // use a key size larger than the full message

        val buf = ByteBuffer.allocate(sizeOfBodyInBytes + sizeOfVarint(sizeOfBodyInBytes))
        writeVarint(sizeOfBodyInBytes, buf)
        buf.put(attributes)
        writeVarlong(timestampDelta, buf)
        writeVarint(offsetDelta, buf)
        writeVarint(keySize, buf)
        buf.position(buf.limit())

        buf.flip()
        assertPartiallyDecodingRecordsFromBufferThrowsInvalidRecordException(buf)
    }

    @Test
    @Throws(IOException::class)
    fun testInvalidValueSize() {
        val attributes: Byte = 0
        val timestampDelta: Long = 2
        val offsetDelta = 1
        val sizeOfBodyInBytes = 100
        val valueSize = 105 // use a value size larger than the full message

        val buf = ByteBuffer.allocate(sizeOfBodyInBytes + sizeOfVarint(sizeOfBodyInBytes))
        writeVarint(sizeOfBodyInBytes, buf)
        buf.put(attributes)
        writeVarlong(timestampDelta, buf)
        writeVarint(offsetDelta, buf)
        writeVarint(-1, buf) // null key
        writeVarint(valueSize, buf)
        buf.position(buf.limit())

        buf.flip()
        assertDecodingRecordFromBufferThrowsInvalidRecordException(buf)
    }

    @Test
    @Throws(IOException::class)
    fun testInvalidValueSizePartial() {
        val attributes: Byte = 0
        val timestampDelta: Long = 2
        val offsetDelta = 1
        val sizeOfBodyInBytes = 100
        val valueSize = 105 // use a value size larger than the full message

        val buf = ByteBuffer.allocate(sizeOfBodyInBytes + sizeOfVarint(sizeOfBodyInBytes))
        writeVarint(sizeOfBodyInBytes, buf)
        buf.put(attributes)
        writeVarlong(timestampDelta, buf)
        writeVarint(offsetDelta, buf)
        writeVarint(-1, buf) // null key
        writeVarint(valueSize, buf)
        buf.position(buf.limit())

        buf.flip()
        assertPartiallyDecodingRecordsFromBufferThrowsInvalidRecordException(buf)
    }

    @Test
    @Throws(IOException::class)
    fun testInvalidNumHeaders() {
        val attributes: Byte = 0
        val timestampDelta: Long = 2
        val offsetDelta = 1
        val sizeOfBodyInBytes = 100

        val buf = ByteBuffer.allocate(sizeOfBodyInBytes + sizeOfVarint(sizeOfBodyInBytes))
        writeVarint(sizeOfBodyInBytes, buf)
        buf.put(attributes)
        writeVarlong(timestampDelta, buf)
        writeVarint(offsetDelta, buf)
        writeVarint(-1, buf) // null key
        writeVarint(-1, buf) // null value
        writeVarint(-1, buf) // -1 num.headers, not allowed
        buf.position(buf.limit())

        buf.flip()
        assertDecodingRecordFromBufferThrowsInvalidRecordException(buf)

        val buf2 = ByteBuffer.allocate(sizeOfBodyInBytes + sizeOfVarint(sizeOfBodyInBytes))
        writeVarint(sizeOfBodyInBytes, buf2)
        buf2.put(attributes)
        writeVarlong(timestampDelta, buf2)
        writeVarint(offsetDelta, buf2)
        writeVarint(-1, buf2) // null key
        writeVarint(-1, buf2) // null value
        writeVarint(sizeOfBodyInBytes, buf2) // more headers than remaining buffer size, not allowed
        buf2.position(buf2.limit())

        buf2.flip()
        assertDecodingRecordFromBufferThrowsInvalidRecordException(buf2)
    }

    @Test
    @Throws(IOException::class)
    fun testInvalidNumHeadersPartial() {
        val attributes: Byte = 0
        val timestampDelta: Long = 2
        val offsetDelta = 1
        val sizeOfBodyInBytes = 100

        val buf = ByteBuffer.allocate(sizeOfBodyInBytes + sizeOfVarint(sizeOfBodyInBytes))
        writeVarint(sizeOfBodyInBytes, buf)
        buf.put(attributes)
        writeVarlong(timestampDelta, buf)
        writeVarint(offsetDelta, buf)
        writeVarint(-1, buf) // null key
        writeVarint(-1, buf) // null value
        writeVarint(-1, buf) // -1 num.headers, not allowed
        buf.position(buf.limit())

        buf.flip()
        assertPartiallyDecodingRecordsFromBufferThrowsInvalidRecordException(buf)
    }

    @Test
    @Throws(IOException::class)
    fun testInvalidHeaderKey() {
        val attributes: Byte = 0
        val timestampDelta: Long = 2
        val offsetDelta = 1
        val sizeOfBodyInBytes = 100

        val buf = ByteBuffer.allocate(sizeOfBodyInBytes + sizeOfVarint(sizeOfBodyInBytes))
        writeVarint(sizeOfBodyInBytes, buf)
        buf.put(attributes)
        writeVarlong(timestampDelta, buf)
        writeVarint(offsetDelta, buf)
        writeVarint(-1, buf) // null key
        writeVarint(-1, buf) // null value
        writeVarint(1, buf)
        writeVarint(105, buf) // header key too long
        buf.position(buf.limit())

        buf.flip()
        assertDecodingRecordFromBufferThrowsInvalidRecordException(buf)
    }

    @Test
    @Throws(IOException::class)
    fun testInvalidHeaderKeyPartial() {
        val attributes: Byte = 0
        val timestampDelta: Long = 2
        val offsetDelta = 1
        val sizeOfBodyInBytes = 100

        val buf = ByteBuffer.allocate(sizeOfBodyInBytes + sizeOfVarint(sizeOfBodyInBytes))
        writeVarint(sizeOfBodyInBytes, buf)
        buf.put(attributes)
        writeVarlong(timestampDelta, buf)
        writeVarint(offsetDelta, buf)
        writeVarint(-1, buf) // null key
        writeVarint(-1, buf) // null value
        writeVarint(1, buf)
        writeVarint(105, buf) // header key too long
        buf.position(buf.limit())

        buf.flip()
        assertPartiallyDecodingRecordsFromBufferThrowsInvalidRecordException(buf)
    }

    @Test
    @Throws(IOException::class)
    fun testNullHeaderKey() {
        val attributes: Byte = 0
        val timestampDelta: Long = 2
        val offsetDelta = 1
        val sizeOfBodyInBytes = 100

        val buf = ByteBuffer.allocate(sizeOfBodyInBytes + sizeOfVarint(sizeOfBodyInBytes))
        writeVarint(sizeOfBodyInBytes, buf)
        buf.put(attributes)
        writeVarlong(timestampDelta, buf)
        writeVarint(offsetDelta, buf)
        writeVarint(-1, buf) // null key
        writeVarint(-1, buf) // null value
        writeVarint(1, buf)
        writeVarint(-1, buf) // null header key not allowed
        buf.position(buf.limit())

        buf.flip()
        assertDecodingRecordFromBufferThrowsInvalidRecordException(buf)
    }

    @Test
    @Throws(IOException::class)
    fun testNullHeaderKeyPartial() {
        val attributes: Byte = 0
        val timestampDelta: Long = 2
        val offsetDelta = 1
        val sizeOfBodyInBytes = 100

        val buf = ByteBuffer.allocate(sizeOfBodyInBytes + sizeOfVarint(sizeOfBodyInBytes))
        writeVarint(sizeOfBodyInBytes, buf)
        buf.put(attributes)
        writeVarlong(timestampDelta, buf)
        writeVarint(offsetDelta, buf)
        writeVarint(-1, buf) // null key
        writeVarint(-1, buf) // null value
        writeVarint(1, buf)
        writeVarint(-1, buf) // null header key not allowed
        buf.position(buf.limit())

        buf.flip()
        assertPartiallyDecodingRecordsFromBufferThrowsInvalidRecordException(buf)
    }

    @Test
    @Throws(IOException::class)
    fun testInvalidHeaderValue() {
        val attributes: Byte = 0
        val timestampDelta: Long = 2
        val offsetDelta = 1
        val sizeOfBodyInBytes = 100

        val buf = ByteBuffer.allocate(sizeOfBodyInBytes + sizeOfVarint(sizeOfBodyInBytes))
        writeVarint(sizeOfBodyInBytes, buf)
        buf.put(attributes)
        writeVarlong(timestampDelta, buf)
        writeVarint(offsetDelta, buf)
        writeVarint(-1, buf) // null key
        writeVarint(-1, buf) // null value
        writeVarint(1, buf)
        writeVarint(1, buf)
        buf.put(1.toByte())
        writeVarint(105, buf) // header value too long
        buf.position(buf.limit())

        buf.flip()
        assertDecodingRecordFromBufferThrowsInvalidRecordException(buf)
    }

    @Test
    @Throws(IOException::class)
    fun testInvalidHeaderValuePartial() {
        val attributes: Byte = 0
        val timestampDelta: Long = 2
        val offsetDelta = 1
        val sizeOfBodyInBytes = 100

        val buf = ByteBuffer.allocate(sizeOfBodyInBytes + sizeOfVarint(sizeOfBodyInBytes))
        writeVarint(sizeOfBodyInBytes, buf)
        buf.put(attributes)
        writeVarlong(timestampDelta, buf)
        writeVarint(offsetDelta, buf)
        writeVarint(-1, buf) // null key
        writeVarint(-1, buf) // null value
        writeVarint(1, buf)
        writeVarint(1, buf)
        buf.put(1.toByte())
        writeVarint(105, buf) // header value too long
        buf.position(buf.limit())

        buf.flip()
        assertPartiallyDecodingRecordsFromBufferThrowsInvalidRecordException(buf)
    }

    @Test
    @Throws(IOException::class)
    fun testUnderflowReadingTimestamp() {
        val attributes: Byte = 0
        val sizeOfBodyInBytes = 1
        val buf = ByteBuffer.allocate(sizeOfBodyInBytes + sizeOfVarint(sizeOfBodyInBytes))
        writeVarint(sizeOfBodyInBytes, buf)
        buf.put(attributes)
        buf.flip()
        assertDecodingRecordFromBufferThrowsInvalidRecordException(buf)
    }

    @Test
    @Throws(IOException::class)
    fun testUnderflowReadingVarlong() {
        val attributes: Byte = 0
        val sizeOfBodyInBytes = 2 // one byte for attributes, one byte for partial timestamp
        val buf = ByteBuffer.allocate(sizeOfBodyInBytes + sizeOfVarint(sizeOfBodyInBytes) + 1)
        writeVarint(sizeOfBodyInBytes, buf)
        buf.put(attributes)
        writeVarlong(156, buf) // needs 2 bytes to represent
        buf.position(buf.limit() - 1)
        buf.flip()
        assertDecodingRecordFromBufferThrowsInvalidRecordException(buf)
    }

    @Test
    @Throws(IOException::class)
    fun testInvalidVarlong() {
        val attributes: Byte = 0
        val sizeOfBodyInBytes = 11 // one byte for attributes, 10 bytes for max timestamp
        val buf = ByteBuffer.allocate(sizeOfBodyInBytes + sizeOfVarint(sizeOfBodyInBytes) + 1)
        writeVarint(sizeOfBodyInBytes, buf)
        val recordStartPosition = buf.position()

        buf.put(attributes)
        writeVarlong(Long.MAX_VALUE, buf) // takes 10 bytes
        buf.put(recordStartPosition + 10, Byte.MIN_VALUE) // use an invalid final byte

        buf.flip()
        assertDecodingRecordFromBufferThrowsInvalidRecordException(buf)
    }

    @Test
    @Throws(IOException::class)
    fun testSerdeNoSequence() {
        val key = ByteBuffer.wrap("hi".toByteArray())
        val value = ByteBuffer.wrap("there".toByteArray())
        val baseOffset: Long = 37
        val offsetDelta = 10
        val baseTimestamp = System.currentTimeMillis()
        val timestampDelta: Long = 323

        val out = ByteBufferOutputStream(1024)
        DefaultRecord.writeTo(
            out = DataOutputStream(out),
            offsetDelta = offsetDelta,
            timestampDelta = timestampDelta,
            key = key,
            value = value,
            headers = emptyArray(),
        )
        val buffer = out.buffer
        buffer.flip()

        // test for input stream input
        ByteBufferInputStream(buffer.asReadOnlyBuffer()).use { inpStream ->
            val record = DefaultRecord.readFrom(
                input = inpStream,
                baseOffset = baseOffset,
                baseTimestamp = baseTimestamp,
                baseSequence = RecordBatch.NO_SEQUENCE,
                logAppendTime = null,
            )
            assertNotNull(record)
            assertEquals(RecordBatch.NO_SEQUENCE, record.sequence())
        }

        // test for buffer input
        val record = DefaultRecord.readFrom(
            buffer = buffer,
            baseOffset = baseOffset,
            baseTimestamp = baseTimestamp,
            baseSequence = RecordBatch.NO_SEQUENCE,
            logAppendTime = null,
        )
        assertNotNull(record)
        assertEquals(RecordBatch.NO_SEQUENCE, record.sequence())
    }

    @Test
    @Throws(IOException::class)
    fun testInvalidSizeOfBodyInBytes() {
        val sizeOfBodyInBytes = 10
        val buf = ByteBuffer.allocate(5)
        writeVarint(sizeOfBodyInBytes, buf)
        buf.flip()

        // test for input stream input
        assertDecodingRecordFromBufferThrowsInvalidRecordException(buf)
    }

    companion object {

        @Throws(IOException::class)
        private fun assertPartiallyDecodingRecordsFromBufferThrowsInvalidRecordException(buf: ByteBuffer) {
            ByteBufferInputStream(buf).use { inputStream ->
                assertFailsWith<InvalidRecordException> {
                    DefaultRecord.readPartiallyFrom(
                        input = inputStream,
                        baseOffset = 0L,
                        baseTimestamp = 0L,
                        baseSequence = RecordBatch.NO_SEQUENCE,
                        logAppendTime = null
                    )
                }
            }
        }

        @Throws(IOException::class)
        private fun assertDecodingRecordFromBufferThrowsInvalidRecordException(buf: ByteBuffer) {
            // test for input stream input
            ByteBufferInputStream(buf.asReadOnlyBuffer()).use { inpStream ->
                assertFailsWith<InvalidRecordException> {
                    DefaultRecord.readFrom(
                        input = inpStream,
                        baseOffset = 0L,
                        baseTimestamp = 0L,
                        baseSequence = RecordBatch.NO_SEQUENCE,
                        logAppendTime = null
                    )
                }
            }
            // test for buffer input
            assertFailsWith<InvalidRecordException> {
                DefaultRecord.readFrom(
                    buffer = buf,
                    baseOffset = 0L,
                    baseTimestamp = 0L,
                    baseSequence = RecordBatch.NO_SEQUENCE,
                    logAppendTime = null
                )
            }
        }
    }
}
