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

import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.errors.CorruptRecordException
import org.apache.kafka.common.utils.ByteBufferOutputStream
import org.apache.kafka.common.utils.ByteUtils.readUnsignedInt
import org.apache.kafka.common.utils.ByteUtils.writeUnsignedInt
import org.apache.kafka.common.utils.Checksums.update
import org.apache.kafka.common.utils.Checksums.updateInt
import org.apache.kafka.common.utils.Checksums.updateLong
import org.apache.kafka.common.utils.Utils.sizeDelimited
import org.apache.kafka.common.utils.Utils.wrapNullable
import org.apache.kafka.common.utils.Utils.writeTo
import java.io.DataOutputStream
import java.io.IOException
import java.nio.ByteBuffer
import java.util.zip.CRC32

/**
 * This class represents the serialized key and value along with the associated CRC and other fields
 * of message format versions 0 and 1. Note that it is uncommon to need to access this class
 * directly. Usually it should be accessed indirectly through the [Record] interface which is
 * exposed through the [Records] object.
 */
class LegacyRecord(
    private val buffer: ByteBuffer,
    private val wrapperRecordTimestamp: Long? = null,
    private val wrapperRecordTimestampType: TimestampType? = null
) {

    /**
     * Compute the checksum of the record from the record contents
     */
    fun computeChecksum(): UInt = crc32(
        buffer = buffer,
        offset = MAGIC_OFFSET,
        size = buffer.limit() - MAGIC_OFFSET
    )

    /**
     * Retrieve the previously computed CRC for this record
     */
    fun checksum(): UInt = readUnsignedInt(buffer, CRC_OFFSET)

    /**
     * `true` if the crc stored with the record matches the crc computed off the record contents
     */
    val isValid: Boolean
        get() = sizeInBytes() >= RECORD_OVERHEAD_V0 && checksum() == computeChecksum()

    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("wrapperRecordTimestamp"),
    )
    fun wrapperRecordTimestamp(): Long? = wrapperRecordTimestamp

    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("wrapperRecordTimestampType"),
    )
    fun wrapperRecordTimestampType(): TimestampType? = wrapperRecordTimestampType

    /**
     * Throw an InvalidRecordException if isValid is false for this record
     */
    fun ensureValid() {
        if (sizeInBytes() < RECORD_OVERHEAD_V0) throw CorruptRecordException(
            "Record is corrupt (crc could not be retrieved as the record is too "
                    + "small, size = ${sizeInBytes()})"
        )
        if (!isValid) throw CorruptRecordException(
            "Record is corrupt (stored crc = ${checksum()}, computed crc = ${computeChecksum()})"
        )
    }

    /**
     * The complete serialized size of this record in bytes (including crc, header attributes, etc),
     * but excluding the log overhead (offset and record size).
     *
     * @return the size in bytes
     */
    fun sizeInBytes(): Int =  buffer.limit()

    /**
     * The length of the key in bytes
     *
     * @return the size in bytes of the key (0 if the key is null)
     */
    fun keySize(): Int =
        if (magic() == RecordBatch.MAGIC_VALUE_V0) buffer.getInt(KEY_SIZE_OFFSET_V0)
        else buffer.getInt(KEY_SIZE_OFFSET_V1)

    /**
     * Does the record have a key?
     *
     * @return true if so, false otherwise
     */
    fun hasKey(): Boolean = keySize() >= 0

    /**
     * The position where the value size is stored
     */
    private fun valueSizeOffset(): Int =
        if (magic() == RecordBatch.MAGIC_VALUE_V0) KEY_OFFSET_V0 + keySize().coerceAtLeast(0)
        else KEY_OFFSET_V1 + keySize().coerceAtLeast(0)

    /**
     * The length of the value in bytes
     *
     * @return the size in bytes of the value (0 if the value is null)
     */
    fun valueSize(): Int = buffer.getInt(valueSizeOffset())

    /**
     * Check whether the value field of this record is null.
     *
     * @return `true` if the value is `null`, `false` otherwise
     */
    fun hasNullValue(): Boolean = valueSize() < 0

    /**
     * The magic value (i.e. message format version) of this record
     *
     * @return the magic value
     */
    fun magic(): Byte = buffer[MAGIC_OFFSET]

    /**
     * The attributes stored with this record
     *
     * @return the attributes
     */
    fun attributes(): Byte = buffer[ATTRIBUTES_OFFSET]

    /**
     * When magic value is greater than 0, the timestamp of a record is determined in the following
     * way:
     * 1. wrapperRecordTimestampType = null and wrapperRecordTimestamp is null - Uncompressed
     *    message, timestamp is in the message.
     * 2. wrapperRecordTimestampType = LOG_APPEND_TIME and WrapperRecordTimestamp is not null -
     *    Compressed message using LOG_APPEND_TIME
     * 3. wrapperRecordTimestampType = CREATE_TIME and wrapperRecordTimestamp is not null -
     *    Compressed message using CREATE_TIME
     *
     * @return the timestamp as determined above
     */
    fun timestamp(): Long =
        if (magic() == RecordBatch.MAGIC_VALUE_V0) RecordBatch.NO_TIMESTAMP
        // case 2
        else if (
            wrapperRecordTimestampType == TimestampType.LOG_APPEND_TIME
            && wrapperRecordTimestamp != null
        ) wrapperRecordTimestamp
        else buffer.getLong(TIMESTAMP_OFFSET)

    /**
     * The compression type used with this record
     */
    fun compressionType(): CompressionType =
        CompressionType.forId(buffer[ATTRIBUTES_OFFSET].toInt() and COMPRESSION_CODEC_MASK)

    /**
     * A ByteBuffer containing the value of this record
     *
     * @return the value or null if the value for this record is null
     */
    fun value(): ByteBuffer? = sizeDelimited(buffer, valueSizeOffset())

    /**
     * A ByteBuffer containing the message key
     *
     * @return the buffer or `null` if the key for this record is `null`
     */
    fun key(): ByteBuffer? =
        if (magic() == RecordBatch.MAGIC_VALUE_V0)
            sizeDelimited(buffer, KEY_SIZE_OFFSET_V0)
        else sizeDelimited(buffer, KEY_SIZE_OFFSET_V1)

    /**
     * Get the underlying buffer backing this record instance.
     *
     * @return the buffer
     */
    fun buffer(): ByteBuffer = buffer

    override fun toString(): String {
        return if (magic() > 0) String.format(
            "Record(magic=%d, attributes=%d, compression=%s, crc=%d, %s=%d, key=%d bytes, value=%d bytes)",
            magic(),
            attributes(),
            compressionType(),
            checksum(),
            timestampType(),
            timestamp(),
            key()?.limit() ?: 0,
            value()?.limit() ?: 0,
        ) else String.format(
            "Record(magic=%d, attributes=%d, compression=%s, crc=%d, key=%d bytes, value=%d bytes)",
            magic(),
            attributes(),
            compressionType(),
            checksum(),
            key()?.limit() ?: 0,
            value()?.limit() ?: 0,
        )
    }

    /**
     * Get the timestamp type of the record.
     *
     * @return The timestamp type or [TimestampType.NO_TIMESTAMP_TYPE] if the magic is 0.
     */
    fun timestampType(): TimestampType = timestampType(
        magic = magic(),
        wrapperRecordTimestampType = wrapperRecordTimestampType,
        attributes = attributes(),
    )

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other == null) return false
        if (other.javaClass != LegacyRecord::class.java) return false
        val record = other as LegacyRecord
        return (buffer == record.buffer)
    }

    override fun hashCode(): Int = buffer.hashCode()

    companion object {

        /**
         * The current offset and size for all the fixed-length fields
         */
        val CRC_OFFSET = 0

        val CRC_LENGTH = 4

        val MAGIC_OFFSET = CRC_OFFSET + CRC_LENGTH

        val MAGIC_LENGTH = 1

        val ATTRIBUTES_OFFSET = MAGIC_OFFSET + MAGIC_LENGTH

        val ATTRIBUTES_LENGTH = 1

        val TIMESTAMP_OFFSET = ATTRIBUTES_OFFSET + ATTRIBUTES_LENGTH

        val TIMESTAMP_LENGTH = 8

        val KEY_SIZE_OFFSET_V0 = ATTRIBUTES_OFFSET + ATTRIBUTES_LENGTH

        val KEY_SIZE_OFFSET_V1 = TIMESTAMP_OFFSET + TIMESTAMP_LENGTH

        val KEY_SIZE_LENGTH = 4

        val KEY_OFFSET_V0 = KEY_SIZE_OFFSET_V0 + KEY_SIZE_LENGTH

        val KEY_OFFSET_V1 = KEY_SIZE_OFFSET_V1 + KEY_SIZE_LENGTH

        val VALUE_SIZE_LENGTH = 4

        /**
         * The size for the record header
         */
        val HEADER_SIZE_V0 = CRC_LENGTH + MAGIC_LENGTH + ATTRIBUTES_LENGTH

        val HEADER_SIZE_V1 = CRC_LENGTH + MAGIC_LENGTH + ATTRIBUTES_LENGTH + TIMESTAMP_LENGTH

        /**
         * The amount of overhead bytes in a record
         */
        val RECORD_OVERHEAD_V0 = HEADER_SIZE_V0 + KEY_SIZE_LENGTH + VALUE_SIZE_LENGTH

        /**
         * The amount of overhead bytes in a record
         */
        val RECORD_OVERHEAD_V1 = HEADER_SIZE_V1 + KEY_SIZE_LENGTH + VALUE_SIZE_LENGTH

        /**
         * Specifies the mask for the compression code. 3 bits to hold the compression codec. 0 is reserved to indicate no
         * compression
         */
        private val COMPRESSION_CODEC_MASK = 0x07

        /**
         * Specify the mask of timestamp type: 0 for CreateTime, 1 for LogAppendTime.
         */
        private val TIMESTAMP_TYPE_MASK: Byte = 0x08

        /**
         * Timestamp value for records without a timestamp
         */
        val NO_TIMESTAMP = -1L

        /**
         * Create a new record instance. If the record's compression type is not none, then its
         * value payload should be already compressed with the specified type; the constructor would
         * always write the value payload as is and will not do the compression itself.
         *
         * @param magic The magic value to use
         * @param timestamp The timestamp of the record
         * @param key The key of the record (`null`, if none)
         * @param value The record value
         * @param compressionType The compression type used on the contents of the record (if any)
         * @param timestampType The timestamp type to be used for this record
         */
        fun create(
            magic: Byte,
            timestamp: Long,
            key: ByteArray?,
            value: ByteArray?,
            compressionType: CompressionType = CompressionType.NONE,
            timestampType: TimestampType = TimestampType.CREATE_TIME
        ): LegacyRecord {
            val keySize = key?.size ?: 0
            val valueSize = value?.size ?: 0
            val buffer = ByteBuffer.allocate(recordSize(magic, keySize, valueSize))
            write(
                buffer = buffer,
                magic = magic,
                timestamp = timestamp,
                key = wrapNullable(key),
                value = wrapNullable(value),
                compressionType = compressionType,
                timestampType = timestampType
            )
            buffer.rewind()
            return LegacyRecord(buffer)
        }

        /**
         * Write the header for a compressed record set in-place (i.e. assuming the compressed
         * record data has already been written at the value offset in a wrapped record). This lets
         * you dynamically create a compressed message set, and then go back later and fill in its
         * size and CRC, which saves the need for copying to another buffer.
         *
         * @param buffer The buffer containing the compressed record data positioned at the first
         * offset of the
         * @param magic The magic value of the record set
         * @param recordSize The size of the record (including record overhead)
         * @param timestamp The timestamp of the wrapper record
         * @param compressionType The compression type used
         * @param timestampType The timestamp type of the wrapper record
         */
        fun writeCompressedRecordHeader(
            buffer: ByteBuffer,
            magic: Byte,
            recordSize: Int,
            timestamp: Long,
            compressionType: CompressionType,
            timestampType: TimestampType
        ) {
            val recordPosition = buffer.position()
            val valueSize = recordSize - recordOverhead(magic)

            // write the record header with a null value (the key is always null for the wrapper)
            write(
                buffer = buffer,
                magic = magic,
                timestamp = timestamp,
                compressionType = compressionType,
                timestampType = timestampType
            )
            buffer.position(recordPosition)

            // now fill in the value size
            buffer.putInt(recordPosition + keyOffset(magic), valueSize)

            // compute and fill the crc from the beginning of the message
            val crc = crc32(buffer, MAGIC_OFFSET, recordSize - MAGIC_OFFSET)
            writeUnsignedInt(buffer, recordPosition + CRC_OFFSET, crc)
        }

        private fun write(
            buffer: ByteBuffer,
            magic: Byte,
            timestamp: Long,
            key: ByteBuffer? = null,
            value: ByteBuffer? = null,
            compressionType: CompressionType,
            timestampType: TimestampType
        ) {
            try {
                val out = DataOutputStream(ByteBufferOutputStream(buffer))
                write(
                    out = out,
                    magic = magic,
                    timestamp = timestamp,
                    key = key,
                    value = value,
                    compressionType = compressionType,
                    timestampType = timestampType
                )
            } catch (e: IOException) {
                throw KafkaException(cause = e)
            }
        }

        /**
         * Write the record data with the given compression type and return the computed crc.
         *
         * @param out The output stream to write to
         * @param magic The magic value to be used
         * @param timestamp The timestamp of the record
         * @param key The record key
         * @param value The record value
         * @param compressionType The compression type
         * @param timestampType The timestamp type
         * @return the computed CRC for this record.
         * @throws IOException for any IO errors writing to the output stream.
         */
        @Throws(IOException::class)
        fun write(
            out: DataOutputStream,
            magic: Byte,
            timestamp: Long,
            key: ByteArray?,
            value: ByteArray?,
            compressionType: CompressionType,
            timestampType: TimestampType
        ): UInt {
            return write(
                out = out,
                magic = magic,
                timestamp = timestamp,
                key = wrapNullable(key),
                value = wrapNullable(value),
                compressionType = compressionType,
                timestampType = timestampType
            )
        }

        @Throws(IOException::class)
        fun write(
            out: DataOutputStream,
            magic: Byte,
            timestamp: Long,
            key: ByteBuffer?,
            value: ByteBuffer?,
            compressionType: CompressionType,
            timestampType: TimestampType
        ): UInt {
            val attributes = computeAttributes(
                magic = magic,
                type = compressionType,
                timestampType = timestampType
            )
            val crc = computeChecksum(magic, attributes, timestamp, key, value)
            write(out, magic, crc, attributes, timestamp, key, value)
            return crc
        }

        /**
         * Write a record using raw fields (without validation). This should only be used in testing.
         */
        @Throws(IOException::class)
        fun write(
            out: DataOutputStream,
            magic: Byte,
            crc: UInt,
            attributes: Byte,
            timestamp: Long,
            key: ByteArray?,
            value: ByteArray?
        ) = write(out, magic, crc, attributes, timestamp, wrapNullable(key), wrapNullable(value))

        // Write a record to the buffer, if the record's compression type is none, then
        // its value payload should be already compressed with the specified type
        @Throws(IOException::class)
        private fun write(
            out: DataOutputStream,
            magic: Byte,
            crc: UInt,
            attributes: Byte,
            timestamp: Long,
            key: ByteBuffer?,
            value: ByteBuffer?,
        ) {
            require(
                magic == RecordBatch.MAGIC_VALUE_V0
                        || magic == RecordBatch.MAGIC_VALUE_V1
            ) { "Invalid magic value $magic" }

            require(timestamp >= 0 || timestamp == RecordBatch.NO_TIMESTAMP) {
                "Invalid message timestamp $timestamp"
            }

            // write crc
            out.writeInt(crc.toInt())
            // write magic value
            out.writeByte(magic.toInt())
            // write attributes
            out.writeByte(attributes.toInt())

            // maybe write timestamp
            if (magic > RecordBatch.MAGIC_VALUE_V0) out.writeLong(timestamp)

            // write the key
            if (key == null) out.writeInt(-1)
            else {
                val size = key.remaining()
                out.writeInt(size)
                writeTo(out, key, size)
            }
            // write the value
            if (value == null) out.writeInt(-1)
            else {
                val size = value.remaining()
                out.writeInt(size)
                writeTo(out, value, size)
            }
        }

        fun recordSize(magic: Byte, key: ByteBuffer?, value: ByteBuffer?): Int =
            recordSize(magic, key?.limit() ?: 0, value?.limit() ?: 0)

        fun recordSize(magic: Byte, keySize: Int, valueSize: Int): Int =
            recordOverhead(magic) + keySize + valueSize

        // visible only for testing
        fun computeAttributes(
            magic: Byte,
            type: CompressionType,
            timestampType: TimestampType,
        ): Byte {
            var attributes: Byte = 0
            if (type.id > 0) attributes = (0 or (COMPRESSION_CODEC_MASK and type.id)).toByte()

            if (magic > RecordBatch.MAGIC_VALUE_V0) {
                require(timestampType != TimestampType.NO_TIMESTAMP_TYPE) {
                    "Timestamp type must be provided to compute attributes for message format v1"
                }

                if (timestampType == TimestampType.LOG_APPEND_TIME)
                    attributes = (attributes.toInt() or TIMESTAMP_TYPE_MASK.toInt()).toByte()
            }

            return attributes
        }

        // visible only for testing
        fun computeChecksum(
            magic: Byte,
            attributes: Byte,
            timestamp: Long,
            key: ByteArray?,
            value: ByteArray?
        ): UInt {
            return computeChecksum(
                magic,
                attributes,
                timestamp,
                wrapNullable(key),
                wrapNullable(value)
            )
        }

        private fun crc32(buffer: ByteBuffer, offset: Int, size: Int): UInt {
            val crc = CRC32()
            update(crc, buffer, offset, size)
            return crc.value.toUInt()
        }

        /**
         * Compute the checksum of the record from the attributes, key and value payloads
         */
        private fun computeChecksum(
            magic: Byte,
            attributes: Byte,
            timestamp: Long,
            key: ByteBuffer?,
            value: ByteBuffer?
        ): UInt {
            val crc = CRC32()
            crc.update(magic.toInt())
            crc.update(attributes.toInt())
            if (magic > RecordBatch.MAGIC_VALUE_V0) updateLong(crc, timestamp)
            // update for the key
            if (key == null) updateInt(crc, -1)
            else {
                val size = key.remaining()
                updateInt(crc, size)
                update(crc, key, size)
            }
            // update for the value
            if (value == null) updateInt(crc, -1)
            else {
                val size = value.remaining()
                updateInt(crc, size)
                update(crc, value, size)
            }
            return crc.value.toUInt()
        }

        fun recordOverhead(magic: Byte): Int {
            return if (magic.toInt() == 0) RECORD_OVERHEAD_V0
            else if (magic.toInt() == 1) RECORD_OVERHEAD_V1
            else throw IllegalArgumentException("Invalid magic used in LegacyRecord: $magic")
        }

        fun headerSize(magic: Byte): Int {
            return if (magic.toInt() == 0) HEADER_SIZE_V0
            else if (magic.toInt() == 1) HEADER_SIZE_V1
            else throw IllegalArgumentException("Invalid magic used in LegacyRecord: $magic")
        }

        private fun keyOffset(magic: Byte): Int {
            return if (magic.toInt() == 0) KEY_OFFSET_V0
            else if (magic.toInt() == 1) KEY_OFFSET_V1
            else throw IllegalArgumentException("Invalid magic used in LegacyRecord: $magic")
        }

        fun timestampType(
            magic: Byte,
            wrapperRecordTimestampType: TimestampType?,
            attributes: Byte,
        ): TimestampType {
            return if (magic.toInt() == 0) TimestampType.NO_TIMESTAMP_TYPE
            else wrapperRecordTimestampType
                ?: if ((attributes.toInt() and TIMESTAMP_TYPE_MASK.toInt()) == 0) TimestampType.CREATE_TIME
                else TimestampType.LOG_APPEND_TIME
        }
    }
}
