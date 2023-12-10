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
import java.io.InputStream
import java.nio.BufferUnderflowException
import java.nio.ByteBuffer
import org.apache.kafka.common.InvalidRecordException
import org.apache.kafka.common.header.Header
import org.apache.kafka.common.header.internals.RecordHeader
import org.apache.kafka.common.utils.ByteUtils
import org.apache.kafka.common.utils.ByteUtils.readVarint
import org.apache.kafka.common.utils.ByteUtils.readVarlong
import org.apache.kafka.common.utils.ByteUtils.sizeOfVarint
import org.apache.kafka.common.utils.Utils.readBytes
import org.apache.kafka.common.utils.Utils.readFully
import org.apache.kafka.common.utils.Utils.utf8Length
import org.apache.kafka.common.utils.Utils.writeTo

/**
 * This class implements the inner record format for magic 2 and above. The schema is as follows:
 *
 * Record =>
 * Length => Varint
 * Attributes => Int8
 * TimestampDelta => Varlong
 * OffsetDelta => Varint
 * Key => Bytes
 * Value => Bytes
 * Headers => [HeaderKey HeaderValue]
 * HeaderKey => String
 * HeaderValue => Bytes
 *
 * Note that in this schema, the Bytes and String types use a variable length integer to represent
 * the length of the field. The array type used for the headers also uses a Varint for the number of
 * headers.
 *
 * The current record attributes are depicted below:
 *
 * ----------------
 * | Unused (0-7) |
 * ----------------
 *
 * The offset and timestamp deltas compute the difference relative to the base offset and
 * base timestamp of the batch that this record is contained in.
 */
open class DefaultRecord internal constructor(
    private val sizeInBytes: Int,
    private val attributes: Byte,
    private val offset: Long,
    private val timestamp: Long,
    private val sequence: Int,
    private val key: ByteBuffer?,
    private val value: ByteBuffer?,
    private val headers: Array<Header>,
) : Record {

    override fun offset(): Long = offset

    override fun sequence(): Int = sequence

    override fun sizeInBytes(): Int = sizeInBytes

    override fun timestamp(): Long = timestamp

    fun attributes(): Byte = attributes

    override fun ensureValid() = Unit

    override fun keySize(): Int = key?.remaining() ?: -1

    override fun valueSize(): Int = value?.remaining() ?: -1

    override fun hasKey(): Boolean = key != null

    override fun key(): ByteBuffer? = key?.duplicate()

    override fun hasValue(): Boolean = value != null

    override fun value(): ByteBuffer? = value?.duplicate()

    override fun headers(): Array<Header> = headers

    override fun hasMagic(magic: Byte): Boolean {
        return magic >= RecordBatch.MAGIC_VALUE_V2
    }

    override val isCompressed: Boolean = false

    override fun hasTimestampType(timestampType: TimestampType): Boolean = false

    override fun toString(): String {
        return String.format(
            "DefaultRecord(offset=%d, timestamp=%d, key=%d bytes, value=%d bytes)",
            offset,
            timestamp,
            key?.limit() ?: 0,
            value?.limit() ?: 0
        )
    }

    override fun equals(o: Any?): Boolean {
        if (this === o) return true
        if (o == null || javaClass != o.javaClass) return false
        val that = o as DefaultRecord
        return sizeInBytes == that.sizeInBytes
                && attributes == that.attributes
                && offset == that.offset
                && timestamp == that.timestamp
                && sequence == that.sequence
                && (key == that.key)
                && (value == that.value)
                && headers.contentEquals(that.headers)
    }

    override fun hashCode(): Int {
        var result = sizeInBytes
        result = 31 * result + attributes.toInt()
        result = 31 * result + java.lang.Long.hashCode(offset)
        result = 31 * result + java.lang.Long.hashCode(timestamp)
        result = 31 * result + sequence
        result = 31 * result + (key?.hashCode() ?: 0)
        result = 31 * result + (value?.hashCode() ?: 0)
        result = 31 * result + headers.contentHashCode()
        return result
    }

    @Suppress("TooManyFunctions")
    companion object {

        // excluding key, value and headers: 5 bytes length + 10 bytes timestamp + 5 bytes offset + 1 byte attributes
        val MAX_RECORD_OVERHEAD = 21

        private val NULL_VARINT_SIZE_BYTES = ByteUtils.sizeOfVarint(-1)

        /**
         * Write the record to `out` and return its size.
         */
        @Throws(IOException::class)
        fun writeTo(
            out: DataOutputStream,
            offsetDelta: Int,
            timestampDelta: Long,
            key: ByteBuffer?,
            value: ByteBuffer?,
            headers: Array<Header>,
        ): Int {
            val sizeInBytes = sizeOfBodyInBytes(offsetDelta, timestampDelta, key, value, headers)
            ByteUtils.writeVarint(sizeInBytes, out)

            val attributes: Byte = 0 // there are no used record attributes at the moment
            out.write(attributes.toInt())
            ByteUtils.writeVarlong(timestampDelta, out)
            ByteUtils.writeVarint(offsetDelta, out)

            key?.let {
                val keySize = it.remaining()
                ByteUtils.writeVarint(keySize, out)
                writeTo(out, it, keySize)
            } ?: ByteUtils.writeVarint(-1, out)

            value?.let {
                val valueSize = it.remaining()
                ByteUtils.writeVarint(valueSize, out)
                writeTo(out, it, valueSize)
            } ?: ByteUtils.writeVarint(-1, out)

            ByteUtils.writeVarint(headers.size, out)

            headers.forEach { header: Header ->
                val headerKey: String = header.key

                val utf8Bytes = headerKey.toByteArray()
                ByteUtils.writeVarint(utf8Bytes.size, out)
                out.write(utf8Bytes)

                header.value?.let {
                    ByteUtils.writeVarint(it.size, out)
                    out.write(it)
                } ?: ByteUtils.writeVarint(-1, out)
            }
            return ByteUtils.sizeOfVarint(sizeInBytes) + sizeInBytes
        }

        @Throws(IOException::class)
        fun readFrom(
            input: InputStream,
            baseOffset: Long,
            baseTimestamp: Long,
            baseSequence: Int,
            logAppendTime: Long?,
        ): DefaultRecord {
            val sizeOfBodyInBytes = ByteUtils.readVarint(input)
            val recordBuffer = ByteBuffer.allocate(sizeOfBodyInBytes)

            val bytesRead = readFully(input, recordBuffer)
            if (bytesRead != sizeOfBodyInBytes) throw InvalidRecordException(
                "Invalid record size: expected $sizeOfBodyInBytes bytes in record payload, but the record payload reached EOF."
            )
            recordBuffer.flip() // prepare for reading

            return readFrom(
                buffer = recordBuffer,
                sizeOfBodyInBytes = sizeOfBodyInBytes,
                baseOffset = baseOffset,
                baseTimestamp = baseTimestamp,
                baseSequence = baseSequence,
                logAppendTime = logAppendTime,
            )
        }

        fun readFrom(
            buffer: ByteBuffer,
            baseOffset: Long,
            baseTimestamp: Long,
            baseSequence: Int,
            logAppendTime: Long?,
        ): DefaultRecord {
            val sizeOfBodyInBytes = readVarint(buffer)
            return readFrom(
                buffer = buffer,
                sizeOfBodyInBytes = sizeOfBodyInBytes,
                baseOffset = baseOffset,
                baseTimestamp = baseTimestamp,
                baseSequence = baseSequence,
                logAppendTime = logAppendTime,
            )
        }

        private fun readFrom(
            buffer: ByteBuffer,
            sizeOfBodyInBytes: Int,
            baseOffset: Long,
            baseTimestamp: Long,
            baseSequence: Int,
            logAppendTime: Long?,
        ): DefaultRecord {
            if (buffer.remaining() < sizeOfBodyInBytes) throw InvalidRecordException(
                "Invalid record size: expected $sizeOfBodyInBytes bytes in record payload, but instead the " +
                        "buffer has only ${buffer.remaining()} remaining bytes."
            )
            try {
                val recordStart = buffer.position()
                val attributes = buffer.get()
                val timestampDelta = ByteUtils.readVarlong(buffer)
                var timestamp = baseTimestamp + timestampDelta

                logAppendTime?.let { timestamp = it }

                val offsetDelta = ByteUtils.readVarint(buffer)
                val offset = baseOffset + offsetDelta

                val sequence =
                    if (baseSequence >= 0) DefaultRecordBatch.incrementSequence(
                        baseSequence,
                        offsetDelta
                    )
                    else RecordBatch.NO_SEQUENCE

                // read key
                val keySize = readVarint(buffer)
                val key = readBytes(buffer, keySize)

                // read value
                val valueSize = readVarint(buffer)
                val value = readBytes(buffer, valueSize)

                val numHeaders = readVarint(buffer)

                if (numHeaders < 0) throw InvalidRecordException(
                    "Found invalid number of record headers $numHeaders"
                )
                if (numHeaders > buffer.remaining()) throw InvalidRecordException(
                    "Found invalid number of record headers. $numHeaders is larger than the " +
                            "remaining size of the buffer"
                )

                val headers: Array<Header> = if (numHeaders == 0) Record.EMPTY_HEADERS
                else readHeaders(buffer, numHeaders)

                // validate whether we have read all header bytes in the current record
                if (buffer.position() - recordStart != sizeOfBodyInBytes) throw InvalidRecordException(
                    "Invalid record size: expected to read $sizeOfBodyInBytes bytes in " +
                            "record payload, but instead read ${buffer.position() - recordStart}"
                )

                val totalSizeInBytes = sizeOfVarint(sizeOfBodyInBytes) + sizeOfBodyInBytes
                return DefaultRecord(
                    sizeInBytes = totalSizeInBytes,
                    attributes = attributes,
                    offset = offset,
                    timestamp = timestamp,
                    sequence = sequence,
                    key = key,
                    value = value,
                    headers = headers,
                )
            } catch (e: BufferUnderflowException) {
                throw InvalidRecordException("Found invalid record structure", e)
            } catch (e: IllegalArgumentException) {
                throw InvalidRecordException("Found invalid record structure", e)
            }
        }

        @Throws(IOException::class)
        fun readPartiallyFrom(
            input: InputStream,
            baseOffset: Long,
            baseTimestamp: Long,
            baseSequence: Int,
            logAppendTime: Long?,
        ): PartialDefaultRecord {
            val sizeOfBodyInBytes = readVarint(input)
            val totalSizeInBytes = sizeOfVarint(sizeOfBodyInBytes) + sizeOfBodyInBytes

            return readPartiallyFrom(
                input = input,
                sizeInBytes = totalSizeInBytes,
                baseOffset = baseOffset,
                baseTimestamp = baseTimestamp,
                baseSequence = baseSequence,
                logAppendTime = logAppendTime
            )
        }

        @Throws(IOException::class)
        private fun readPartiallyFrom(
            input: InputStream,
            sizeInBytes: Int,
            baseOffset: Long,
            baseTimestamp: Long,
            baseSequence: Int,
            logAppendTime: Long?,
        ): PartialDefaultRecord {
            try {
                val attributes = input.read().toByte()
                val timestampDelta = readVarlong(input)
                var timestamp = baseTimestamp + timestampDelta
                if (logAppendTime != null) timestamp = logAppendTime

                val offsetDelta = readVarint(input)
                val offset = baseOffset + offsetDelta
                val sequence =
                    if (baseSequence >= 0) DefaultRecordBatch.incrementSequence(baseSequence, offsetDelta)
                    else RecordBatch.NO_SEQUENCE

                // skip key
                val keySize = readVarint(input)
                skipBytes(input, keySize)

                // skip value
                val valueSize = readVarint(input)
                skipBytes(input, valueSize)

                // skip header
                val numHeaders = readVarint(input)
                if (numHeaders < 0) throw InvalidRecordException("Found invalid number of record headers $numHeaders")
                for (i in 0..<numHeaders) {
                    val headerKeySize = readVarint(input)
                    if (headerKeySize < 0) throw InvalidRecordException("Invalid negative header key size $headerKeySize")
                    skipBytes(input, headerKeySize)

                    // headerValueSize
                    val headerValueSize = readVarint(input)
                    skipBytes(input, headerValueSize)
                }

                return PartialDefaultRecord(
                    sizeInBytes = sizeInBytes,
                    attributes = attributes,
                    offset = offset,
                    timestamp = timestamp,
                    sequence = sequence,
                    keySize = keySize,
                    valueSize = valueSize,
                )
            } catch (e: BufferUnderflowException) {
                throw InvalidRecordException("Found invalid record structure", e)
            } catch (e: IllegalArgumentException) {
                throw InvalidRecordException("Found invalid record structure", e)
            }
        }

        /**
         * Skips over and discards exactly `bytesToSkip` bytes from the input stream.
         *
         * We require a loop over [InputStream.skip] because it is possible for InputStream to skip smaller
         * number of bytes than expected (see javadoc for InputStream#skip).
         *
         * No-op for case where bytesToSkip <= 0. This could occur for cases where field is expected to be null.
         * @throws InvalidRecordException if end of stream is encountered before we could skip required bytes.
         * @throws IOException is an I/O error occurs while trying to skip from InputStream.
         *
         * @see java.io.InputStream.skip
         */
        @Throws(IOException::class)
        private fun skipBytes(input: InputStream, bytesToSkip: Int) {
            var toSkip = bytesToSkip
            if (toSkip <= 0) return

            // Starting JDK 12, this implementation could be replaced by InputStream#skipNBytes
            while (toSkip > 0) {
                val ns = input.skip(toSkip.toLong()).toInt()
                when (ns) {
                    in 1..toSkip -> toSkip -= ns // adjust number to skip
                    0 -> { // no bytes skipped
                        // read one byte to check for EOS
                        if (input.read() == -1) throw InvalidRecordException(
                            "Reached end of input stream before skipping all bytes. Remaining bytes:$toSkip"
                        )

                        // one byte read so decrement number to skip
                        toSkip--
                    }

                    else -> throw IOException("Unable to skip exactly") // skipped negative or too many bytes
                }
            }
        }

        private fun readHeaders(buffer: ByteBuffer, numHeaders: Int): Array<Header> {
            val headers = arrayOfNulls<Header>(numHeaders)
            for (i in 0..<numHeaders) {
                val headerKeySize = readVarint(buffer)

                if (headerKeySize < 0) throw InvalidRecordException(
                    "Invalid negative header key size $headerKeySize"
                )

                val headerKeyBuffer = readBytes(buffer, headerKeySize)!!

                val headerValueSize = readVarint(buffer)
                val headerValue = readBytes(buffer, headerValueSize)

                headers[i] = RecordHeader(headerKeyBuffer, headerValue)
            }
            return headers.filterNotNull().toTypedArray()
        }

        fun sizeInBytes(
            offsetDelta: Int,
            timestampDelta: Long,
            key: ByteBuffer?,
            value: ByteBuffer?,
            headers: Array<Header>,
        ): Int {
            val bodySize = sizeOfBodyInBytes(offsetDelta, timestampDelta, key, value, headers)
            return bodySize + ByteUtils.sizeOfVarint(bodySize)
        }

        fun sizeInBytes(
            offsetDelta: Int,
            timestampDelta: Long,
            keySize: Int,
            valueSize: Int,
            headers: Array<Header>,
        ): Int {
            val bodySize =
                sizeOfBodyInBytes(offsetDelta, timestampDelta, keySize, valueSize, headers)
            return bodySize + ByteUtils.sizeOfVarint(bodySize)
        }

        private fun sizeOfBodyInBytes(
            offsetDelta: Int,
            timestampDelta: Long,
            key: ByteBuffer?,
            value: ByteBuffer?,
            headers: Array<Header>,
        ): Int {
            val keySize = key?.remaining() ?: -1
            val valueSize = value?.remaining() ?: -1
            return sizeOfBodyInBytes(offsetDelta, timestampDelta, keySize, valueSize, headers)
        }

        fun sizeOfBodyInBytes(
            offsetDelta: Int,
            timestampDelta: Long,
            keySize: Int,
            valueSize: Int,
            headers: Array<Header>,
        ): Int {
            var size = 1 // always one byte for attributes
            size += ByteUtils.sizeOfVarint(offsetDelta)
            size += ByteUtils.sizeOfVarlong(timestampDelta)
            size += sizeOf(keySize, valueSize, headers)
            return size
        }

        private fun sizeOf(keySize: Int, valueSize: Int, headers: Array<Header>): Int {
            var size = 0

            size += if (keySize < 0) NULL_VARINT_SIZE_BYTES
            else ByteUtils.sizeOfVarint(keySize) + keySize

            size += if (valueSize < 0) NULL_VARINT_SIZE_BYTES
            else ByteUtils.sizeOfVarint(valueSize) + valueSize

            size += ByteUtils.sizeOfVarint(headers.size)
            for (header: Header in headers) {
                val headerKey: String = header.key
                val headerKeySize = utf8Length(headerKey)

                size += ByteUtils.sizeOfVarint(headerKeySize) + headerKeySize
                val headerValue = header.value

                size += if (headerValue == null) NULL_VARINT_SIZE_BYTES
                else ByteUtils.sizeOfVarint(headerValue.size) + headerValue.size
            }
            return size
        }

        fun recordSizeUpperBound(
            key: ByteBuffer?,
            value: ByteBuffer?,
            headers: Array<Header>,
        ): Int {
            val keySize = key?.remaining() ?: -1
            val valueSize = value?.remaining() ?: -1
            return MAX_RECORD_OVERHEAD + sizeOf(keySize, valueSize, headers)
        }
    }
}
