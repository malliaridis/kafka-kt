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

import java.io.DataInput
import java.io.DataOutputStream
import java.io.IOException
import java.nio.BufferUnderflowException
import java.nio.ByteBuffer
import org.apache.kafka.common.InvalidRecordException
import org.apache.kafka.common.header.Header
import org.apache.kafka.common.header.internals.RecordHeader
import org.apache.kafka.common.utils.ByteUtils
import org.apache.kafka.common.utils.PrimitiveRef
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
    private val headers: Array<Header>
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

    override val isCompressed: Boolean= false

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
            headers: Array<Header>
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
            input: DataInput,
            baseOffset: Long,
            baseTimestamp: Long,
            baseSequence: Int,
            logAppendTime: Long?
        ): DefaultRecord {
            val sizeOfBodyInBytes = ByteUtils.readVarint(input)
            val recordBuffer = ByteBuffer.allocate(sizeOfBodyInBytes)

            input.readFully(recordBuffer.array(), 0, sizeOfBodyInBytes)
            val totalSizeInBytes = ByteUtils.sizeOfVarint(sizeOfBodyInBytes) + sizeOfBodyInBytes

            return readFrom(
                buffer = recordBuffer,
                sizeInBytes = totalSizeInBytes,
                sizeOfBodyInBytes = sizeOfBodyInBytes,
                baseOffset = baseOffset,
                baseTimestamp = baseTimestamp,
                baseSequence = baseSequence,
                logAppendTime = logAppendTime
            )
        }

        fun readFrom(
            buffer: ByteBuffer,
            baseOffset: Long,
            baseTimestamp: Long,
            baseSequence: Int,
            logAppendTime: Long?
        ): DefaultRecord {
            val sizeOfBodyInBytes = ByteUtils.readVarint(buffer)
            if (buffer.remaining() < sizeOfBodyInBytes) throw InvalidRecordException(
                "Invalid record size: expected $sizeOfBodyInBytes bytes in record " +
                        "payload, but instead the buffer has only ${buffer.remaining()} " +
                        "remaining bytes."
            )
            val totalSizeInBytes = ByteUtils.sizeOfVarint(sizeOfBodyInBytes) + sizeOfBodyInBytes

            return readFrom(
                buffer = buffer,
                sizeInBytes = totalSizeInBytes,
                sizeOfBodyInBytes = sizeOfBodyInBytes,
                baseOffset = baseOffset,
                baseTimestamp = baseTimestamp,
                baseSequence = baseSequence,
                logAppendTime = logAppendTime
            )
        }

        private fun readFrom(
            buffer: ByteBuffer,
            sizeInBytes: Int,
            sizeOfBodyInBytes: Int,
            baseOffset: Long,
            baseTimestamp: Long,
            baseSequence: Int,
            logAppendTime: Long?
        ): DefaultRecord {
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

                var key: ByteBuffer? = null
                val keySize = ByteUtils.readVarint(buffer)

                if (keySize >= 0) {
                    key = buffer.slice()
                    key.limit(keySize)
                    buffer.position(buffer.position() + keySize)
                }

                var value: ByteBuffer? = null
                val valueSize = ByteUtils.readVarint(buffer)

                if (valueSize >= 0) {
                    value = buffer.slice()
                    value.limit(valueSize)
                    buffer.position(buffer.position() + valueSize)
                }
                val numHeaders = ByteUtils.readVarint(buffer)

                if (numHeaders < 0) throw InvalidRecordException(
                    "Found invalid number of record headers $numHeaders"
                )
                if (numHeaders > buffer.remaining())throw InvalidRecordException(
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

                return DefaultRecord(
                    sizeInBytes = sizeInBytes,
                    attributes = attributes,
                    offset = offset,
                    timestamp = timestamp,
                    sequence = sequence,
                    key = key,
                    value = value,
                    headers = headers
                )
            } catch (e: BufferUnderflowException) {
                throw InvalidRecordException("Found invalid record structure", e)
            } catch (e: IllegalArgumentException) {
                throw InvalidRecordException("Found invalid record structure", e)
            }
        }

        @Throws(IOException::class)
        fun readPartiallyFrom(
            input: DataInput,
            skipArray: ByteArray,
            baseOffset: Long,
            baseTimestamp: Long,
            baseSequence: Int,
            logAppendTime: Long?
        ): PartialDefaultRecord {
            val sizeOfBodyInBytes = ByteUtils.readVarint(input)
            val totalSizeInBytes = ByteUtils.sizeOfVarint(sizeOfBodyInBytes) + sizeOfBodyInBytes

            return readPartiallyFrom(
                input = input,
                skipArray = skipArray,
                sizeInBytes = totalSizeInBytes,
                sizeOfBodyInBytes = sizeOfBodyInBytes,
                baseOffset = baseOffset,
                baseTimestamp = baseTimestamp,
                baseSequence = baseSequence,
                logAppendTime = logAppendTime
            )
        }

        @Throws(IOException::class)
        private fun readPartiallyFrom(
            input: DataInput,
            skipArray: ByteArray,
            sizeInBytes: Int,
            sizeOfBodyInBytes: Int,
            baseOffset: Long,
            baseTimestamp: Long,
            baseSequence: Int,
            logAppendTime: Long?
        ): PartialDefaultRecord {
            val skipBuffer = ByteBuffer.wrap(skipArray)

            // set its limit to 0 to indicate no bytes readable yet
            skipBuffer.limit(0)

            try {
                // reading the attributes / timestamp / offset and key-size does not require
                // any byte array allocation and therefore we can just read them straight-forwardly
                val bytesRemaining = PrimitiveRef.ofInt(sizeOfBodyInBytes)
                val attributes = readByte(skipBuffer, input, bytesRemaining)
                val timestampDelta = readVarLong(skipBuffer, input, bytesRemaining)
                var timestamp = baseTimestamp + timestampDelta

                if (logAppendTime != null) timestamp = logAppendTime

                val offsetDelta = readVarInt(skipBuffer, input, bytesRemaining)
                val offset = baseOffset + offsetDelta

                val sequence =
                    if (baseSequence >= 0) DefaultRecordBatch.incrementSequence(
                        baseSequence,
                        offsetDelta
                    )
                    else RecordBatch.NO_SEQUENCE

                // first skip key
                val keySize = skipLengthDelimitedField(skipBuffer, input, bytesRemaining)

                // then skip value
                val valueSize = skipLengthDelimitedField(skipBuffer, input, bytesRemaining)

                // then skip header
                val numHeaders = readVarInt(skipBuffer, input, bytesRemaining)
                if (numHeaders < 0) throw InvalidRecordException("Found invalid number of record headers $numHeaders")
                for (i in 0 until numHeaders) {
                    val headerKeySize = skipLengthDelimitedField(skipBuffer, input, bytesRemaining)
                    if (headerKeySize < 0) throw InvalidRecordException("Invalid negative header key size $headerKeySize")

                    // headerValueSize
                    skipLengthDelimitedField(skipBuffer, input, bytesRemaining)
                }
                if (bytesRemaining.value > 0 || skipBuffer.remaining() > 0) throw InvalidRecordException(
                    "Invalid record size: expected to read $sizeOfBodyInBytes" +
                            " bytes in record payload, but there are still bytes remaining"
                )

                return PartialDefaultRecord(
                    sizeInBytes,
                    attributes,
                    offset,
                    timestamp,
                    sequence,
                    keySize,
                    valueSize
                )
            } catch (e: BufferUnderflowException) {
                throw InvalidRecordException("Found invalid record structure", e)
            } catch (e: IllegalArgumentException) {
                throw InvalidRecordException("Found invalid record structure", e)
            }
        }

        @Throws(IOException::class)
        private fun readByte(
            buffer: ByteBuffer,
            input: DataInput,
            bytesRemaining: PrimitiveRef.IntRef
        ): Byte {
            if (buffer.remaining() < 1 && bytesRemaining.value > 0) {
                readMore(buffer, input, bytesRemaining)
            }
            return buffer.get()
        }

        @Throws(IOException::class)
        private fun readVarLong(
            buffer: ByteBuffer,
            input: DataInput,
            bytesRemaining: PrimitiveRef.IntRef
        ): Long {
            if (buffer.remaining() < 10 && bytesRemaining.value > 0) {
                readMore(buffer, input, bytesRemaining)
            }
            return ByteUtils.readVarlong(buffer)
        }

        @Throws(IOException::class)
        private fun readVarInt(
            buffer: ByteBuffer,
            input: DataInput,
            bytesRemaining: PrimitiveRef.IntRef
        ): Int {
            if (buffer.remaining() < 5 && bytesRemaining.value > 0) {
                readMore(buffer, input, bytesRemaining)
            }
            return ByteUtils.readVarint(buffer)
        }

        @Throws(IOException::class)
        private fun skipLengthDelimitedField(
            buffer: ByteBuffer,
            input: DataInput,
            bytesRemaining: PrimitiveRef.IntRef
        ): Int {
            var needMore = false
            var sizeInBytes = -1
            var bytesToSkip = -1

            while (true) {
                if (needMore) {
                    readMore(buffer, input, bytesRemaining)
                    needMore = false
                }

                if (bytesToSkip < 0) {
                    if (buffer.remaining() < 5 && bytesRemaining.value > 0) needMore = true
                    else {
                        sizeInBytes = ByteUtils.readVarint(buffer)
                        if (sizeInBytes <= 0) return sizeInBytes else bytesToSkip = sizeInBytes
                    }
                } else {
                    if (bytesToSkip > buffer.remaining()) {
                        bytesToSkip -= buffer.remaining()
                        buffer.position(buffer.limit())
                        needMore = true
                    } else {
                        buffer.position(buffer.position() + bytesToSkip)
                        return sizeInBytes
                    }
                }
            }
        }

        @Throws(IOException::class)
        private fun readMore(
            buffer: ByteBuffer,
            input: DataInput,
            bytesRemaining: PrimitiveRef.IntRef
        ) {
            if (bytesRemaining.value > 0) {
                val array = buffer.array()

                // first copy the remaining bytes to the beginning of the array;
                // at most 4 bytes would be shifted here
                val stepsToLeftShift = buffer.position()
                val bytesToLeftShift = buffer.remaining()
                for (i in 0 until bytesToLeftShift) {
                    array[i] = array[i + stepsToLeftShift]
                }

                // then try to read more bytes to the remaining of the array
                val bytesRead = bytesRemaining.value.coerceAtMost(array.size - bytesToLeftShift)
                input.readFully(array, bytesToLeftShift, bytesRead)
                buffer.rewind()
                // only those many bytes are readable
                buffer.limit(bytesToLeftShift + bytesRead)
                bytesRemaining.value -= bytesRead
            } else throw InvalidRecordException(
                "Invalid record size: expected to read more bytes in record payload"
            )
        }

        private fun readHeaders(buffer: ByteBuffer, numHeaders: Int): Array<Header> {
            val headers = arrayOfNulls<Header>(numHeaders)
            for (i in 0 until numHeaders) {
                val headerKeySize = ByteUtils.readVarint(buffer)

                if (headerKeySize < 0) throw InvalidRecordException(
                    "Invalid negative header key size $headerKeySize"
                )

                val headerKeyBuffer = buffer.slice()
                headerKeyBuffer.limit(headerKeySize)
                buffer.position(buffer.position() + headerKeySize)
                var headerValue: ByteBuffer? = null
                val headerValueSize = ByteUtils.readVarint(buffer)

                if (headerValueSize >= 0) {
                    headerValue = buffer.slice()
                    headerValue.limit(headerValueSize)
                    buffer.position(buffer.position() + headerValueSize)
                }
                headers[i] = RecordHeader(headerKeyBuffer, headerValue)
            }
            return headers.filterNotNull().toTypedArray()
        }

        fun sizeInBytes(
            offsetDelta: Int,
            timestampDelta: Long,
            key: ByteBuffer?,
            value: ByteBuffer?,
            headers: Array<Header>
        ): Int {
            val bodySize = sizeOfBodyInBytes(offsetDelta, timestampDelta, key, value, headers)
            return bodySize + ByteUtils.sizeOfVarint(bodySize)
        }

        fun sizeInBytes(
            offsetDelta: Int,
            timestampDelta: Long,
            keySize: Int,
            valueSize: Int,
            headers: Array<Header>
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
            headers: Array<Header>
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
            headers: Array<Header>
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
            headers: Array<Header>
        ): Int {
            val keySize = key?.remaining() ?: -1
            val valueSize = value?.remaining() ?: -1
            return MAX_RECORD_OVERHEAD + sizeOf(keySize, valueSize, headers)
        }
    }
}
