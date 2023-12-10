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

import org.apache.kafka.common.InvalidRecordException
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.errors.CorruptRecordException
import org.apache.kafka.common.header.Header
import org.apache.kafka.common.utils.AbstractIterator
import org.apache.kafka.common.utils.BufferSupplier
import org.apache.kafka.common.utils.ByteBufferOutputStream
import org.apache.kafka.common.utils.ByteUtils.writeUnsignedInt
import org.apache.kafka.common.utils.CloseableIterator
import org.apache.kafka.common.utils.Utils.closeQuietly
import org.apache.kafka.common.utils.Utils.readFully
import java.io.DataOutputStream
import java.io.IOException
import java.io.InputStream
import java.nio.ByteBuffer
import java.util.*

/**
 * This [RecordBatch] implementation is for magic versions 0 and 1. In addition to implementing
 * [RecordBatch], it also implements [Record], which exposes the duality of the old message format
 * in its handling of compressed messages. The wrapper record is considered the record batch in this
 * interface, while the inner records are considered the log records (though they both share the
 * same schema).
 *
 * In general, this class should not be used directly. Instances of [Records] provides access to
 * this class indirectly through the [RecordBatch] interface.
 */
abstract class AbstractLegacyRecordBatch : AbstractRecordBatch(), Record {

    abstract fun outerRecord(): LegacyRecord

    override fun lastOffset(): Long = offset()

    override val isValid: Boolean
        get() = outerRecord().isValid

    override fun ensureValid() = outerRecord().ensureValid()

    override fun keySize(): Int = outerRecord().keySize()

    override fun hasKey(): Boolean = outerRecord().hasKey()

    override fun key(): ByteBuffer? = outerRecord().key()

    override fun valueSize(): Int = outerRecord().valueSize()

    override fun hasValue(): Boolean = !outerRecord().hasNullValue()

    override fun value(): ByteBuffer? = outerRecord().value()

    override fun headers(): Array<Header> = Record.EMPTY_HEADERS

    override fun hasMagic(magic: Byte): Boolean = magic == outerRecord().magic()

    override fun hasTimestampType(timestampType: TimestampType): Boolean =
        outerRecord().timestampType() == timestampType

    override fun checksum(): UInt = outerRecord().checksum()

    override fun maxTimestamp(): Long = timestamp()

    override fun timestamp(): Long = outerRecord().timestamp()

    override fun timestampType(): TimestampType = outerRecord().timestampType()

    override fun baseOffset(): Long = first().offset()

    override fun magic(): Byte = outerRecord().magic()

    override fun compressionType(): CompressionType = outerRecord().compressionType()

    override fun sizeInBytes(): Int = outerRecord().sizeInBytes() + Records.LOG_OVERHEAD

    override fun countOrNull(): Int? = null

    override fun toString(): String =
        "LegacyRecordBatch(offset=${offset()}, ${outerRecord()})"

    override fun writeTo(buffer: ByteBuffer) {
        writeHeader(buffer, offset(), outerRecord().sizeInBytes())
        buffer.put(outerRecord().buffer().duplicate())
    }

    override fun producerId(): Long = RecordBatch.NO_PRODUCER_ID

    override fun producerEpoch(): Short = RecordBatch.NO_PRODUCER_EPOCH

    override fun hasProducerId(): Boolean = false

    override fun sequence(): Int = RecordBatch.NO_SEQUENCE

    override fun baseSequence(): Int = RecordBatch.NO_SEQUENCE

    override fun lastSequence(): Int = RecordBatch.NO_SEQUENCE

    override val isTransactional: Boolean = false

    override fun partitionLeaderEpoch(): Int = RecordBatch.NO_PARTITION_LEADER_EPOCH

    override val isControlBatch: Boolean = false

    override fun deleteHorizonMs(): Long? = null

    /**
     * Get an iterator for the nested entries contained within this batch. Note that if the batch is
     * not compressed, then this method will return an iterator over the shallow record only (i.e.
     * this object).
     *
     * @return An iterator over the records contained within this batch
     */
    override fun iterator(): Iterator<Record> = iterator(BufferSupplier.NO_CACHING)

    fun iterator(bufferSupplier: BufferSupplier): CloseableIterator<Record> {
        return if (isCompressed) DeepRecordsIterator(
            wrapperEntry = this,
            ensureMatchingMagic = false,
            maxMessageSize = Int.MAX_VALUE,
            bufferSupplier = bufferSupplier
        ) else object : CloseableIterator<Record> {

            private var hasNext = true

            override fun close() = Unit

            override fun hasNext(): Boolean = hasNext

            override fun next(): Record {
                if (!hasNext) throw NoSuchElementException()
                hasNext = false
                return this@AbstractLegacyRecordBatch
            }

            override fun remove() = throw UnsupportedOperationException()
        }
    }

    // the older message format versions do not support streaming, so we return the normal iterator
    override fun streamingIterator(
        decompressionBufferSupplier: BufferSupplier,
    ): CloseableIterator<Record> = iterator(decompressionBufferSupplier)

    private class DataLogInputStream(
        private val stream: InputStream,
        val maxMessageSize: Int
    ) : LogInputStream<AbstractLegacyRecordBatch> {

        private val offsetAndSizeBuffer: ByteBuffer = ByteBuffer.allocate(Records.LOG_OVERHEAD)

        @Throws(IOException::class)
        override fun nextBatch(): AbstractLegacyRecordBatch? {
            offsetAndSizeBuffer.clear()
            readFully(stream, offsetAndSizeBuffer)
            if (offsetAndSizeBuffer.hasRemaining()) return null

            val offset = offsetAndSizeBuffer.getLong(Records.OFFSET_OFFSET)
            val size = offsetAndSizeBuffer.getInt(Records.SIZE_OFFSET)

            if (size < LegacyRecord.RECORD_OVERHEAD_V0) throw CorruptRecordException(
                String.format(
                    "Record size is less than the minimum record overhead (%d)",
                    LegacyRecord.RECORD_OVERHEAD_V0
                )
            )

            if (size > maxMessageSize) throw CorruptRecordException(
                String.format(
                    "Record size exceeds the largest allowable message size (%d).",
                    maxMessageSize
                )
            )

            val batchBuffer = ByteBuffer.allocate(size)
            readFully(stream, batchBuffer)
            if (batchBuffer.hasRemaining()) return null

            batchBuffer.flip()
            return BasicLegacyRecordBatch(offset, LegacyRecord(batchBuffer))
        }
    }

    private class DeepRecordsIterator(
        wrapperEntry: AbstractLegacyRecordBatch,
        ensureMatchingMagic: Boolean,
        maxMessageSize: Int,
        bufferSupplier: BufferSupplier
    ) : AbstractIterator<Record>(), CloseableIterator<Record> {

        private val innerEntries: ArrayDeque<AbstractLegacyRecordBatch>

        private var absoluteBaseOffset: Long = 0

        private val wrapperMagic: Byte

        init {
            val wrapperRecord = wrapperEntry.outerRecord()
            wrapperMagic = wrapperRecord.magic()
            if (
                wrapperMagic != RecordBatch.MAGIC_VALUE_V0
                && wrapperMagic != RecordBatch.MAGIC_VALUE_V1
            ) throw InvalidRecordException(
                "Invalid wrapper magic found in legacy deep record iterator $wrapperMagic"
            )

            val compressionType = wrapperRecord.compressionType()
            if (compressionType === CompressionType.ZSTD) throw InvalidRecordException(
                "Invalid wrapper compressionType found in legacy deep record iterator $wrapperMagic"
            )

            val wrapperValue = wrapperRecord.value() ?: throw InvalidRecordException(
                "Found invalid compressed record set with null value (magic = $wrapperMagic)"
            )

            val stream = compressionType.wrapForInput(
                buffer = wrapperValue,
                messageVersion = wrapperRecord.magic(),
                decompressionBufferSupplier = bufferSupplier
            )

            val logStream: LogInputStream<AbstractLegacyRecordBatch> =
                DataLogInputStream(stream, maxMessageSize)
            val lastOffsetFromWrapper = wrapperEntry.lastOffset()
            val timestampFromWrapper = wrapperRecord.timestamp()
            innerEntries = ArrayDeque()

            // If relative offset is used, we need to decompress the entire message first to compute
            // the absolute offset. For simplicity and because it's a format that is on its way out,
            // we do the same for message format version 0
            try {
                while (true) {
                    var innerEntry = logStream.nextBatch() ?: break
                    val record = innerEntry.outerRecord()
                    val magic = record.magic()
                    if (ensureMatchingMagic && magic != wrapperMagic) throw InvalidRecordException(
                        "Compressed message magic " + magic +
                                " does not match wrapper magic " + wrapperMagic
                    )
                    if (magic == RecordBatch.MAGIC_VALUE_V1) {
                        val recordWithTimestamp = LegacyRecord(
                            record.buffer(),
                            timestampFromWrapper,
                            wrapperRecord.timestampType()
                        )
                        innerEntry =
                            BasicLegacyRecordBatch(innerEntry.lastOffset(), recordWithTimestamp)
                    }
                    innerEntries.addLast(innerEntry)
                }
                if (innerEntries.isEmpty()) throw InvalidRecordException(
                    "Found invalid compressed record set with no inner records"
                )
                absoluteBaseOffset = if (wrapperMagic == RecordBatch.MAGIC_VALUE_V1) {
                    // The outer offset may be 0 if this is produce data from certain versions of
                    // librdkafka.
                    if (lastOffsetFromWrapper == 0L) 0
                    else {
                        val lastInnerOffset = innerEntries.last.offset()
                        if (lastOffsetFromWrapper < lastInnerOffset) throw InvalidRecordException(
                            "Found invalid wrapper offset in compressed v1 message set, wrapper " +
                                    "offset '$lastOffsetFromWrapper' is less than the last inner " +
                                    "message offset '$lastInnerOffset' and it is not zero."
                        )
                        lastOffsetFromWrapper - lastInnerOffset
                    }
                } else -1
            } catch (e: IOException) {
                throw KafkaException(cause = e)
            } finally {
                closeQuietly(stream, "records iterator stream")
            }
        }

        override fun makeNext(): Record? {
            if (innerEntries.isEmpty()) return allDone()
            var entry = innerEntries.remove()

            // Convert offset to absolute offset if needed.
            if (wrapperMagic == RecordBatch.MAGIC_VALUE_V1) {
                val absoluteOffset = absoluteBaseOffset + entry.offset()
                entry = BasicLegacyRecordBatch(absoluteOffset, entry.outerRecord())
            }

            if (entry.isCompressed)
                throw InvalidRecordException("Inner messages must not be compressed")

            return entry
        }

        override fun close() = Unit
    }

    private data class BasicLegacyRecordBatch(
        private val offset: Long,
        private val record: LegacyRecord,
    ) : AbstractLegacyRecordBatch() {

        override fun offset(): Long = offset

        override fun outerRecord(): LegacyRecord = record
    }

    internal data class ByteBufferLegacyRecordBatch(
        private val buffer: ByteBuffer,
    ) : AbstractLegacyRecordBatch(), MutableRecordBatch {

        private val record: LegacyRecord

        init {
            buffer.position(Records.LOG_OVERHEAD)
            record = LegacyRecord(buffer.slice())
            buffer.position(Records.OFFSET_OFFSET)
        }

        override fun offset(): Long = buffer.getLong(Records.OFFSET_OFFSET)

        override fun outerRecord(): LegacyRecord = record

        override fun setLastOffset(offset: Long) {
            buffer.putLong(Records.OFFSET_OFFSET, offset)
        }

        override fun setMaxTimestamp(timestampType: TimestampType, maxTimestamp: Long) {
            if (record.magic() == RecordBatch.MAGIC_VALUE_V0) throw UnsupportedOperationException(
                "Cannot set timestamp for a record with magic = 0"
            )

            val currentTimestamp = record.timestamp()
            // We don't need to recompute crc if the timestamp is not updated.
            if (record.timestampType() == timestampType && currentTimestamp == maxTimestamp) return
            setTimestampAndUpdateCrc(timestampType, maxTimestamp)
        }

        override fun setPartitionLeaderEpoch(epoch: Int) = throw UnsupportedOperationException(
            "Magic versions prior to 2 do not support partition leader epoch"
        )

        private fun setTimestampAndUpdateCrc(timestampType: TimestampType, timestamp: Long) {
            val attributes =
                LegacyRecord.computeAttributes(magic(), compressionType(), timestampType)
            buffer.put(Records.LOG_OVERHEAD + LegacyRecord.ATTRIBUTES_OFFSET, attributes)
            buffer.putLong(Records.LOG_OVERHEAD + LegacyRecord.TIMESTAMP_OFFSET, timestamp)

            val crc = record.computeChecksum()
            writeUnsignedInt((buffer), Records.LOG_OVERHEAD + LegacyRecord.CRC_OFFSET, crc)
        }

        /**
         * LegacyRecordBatch does not implement this iterator and would hence fallback to the normal
         * iterator.
         *
         * @return An iterator over the records contained within this batch
         */
        override fun skipKeyValueIterator(bufferSupplier: BufferSupplier): CloseableIterator<Record> {
            return CloseableIterator.wrap(iterator(bufferSupplier))
        }

        override fun writeTo(outputStream: ByteBufferOutputStream) =
            outputStream.write(buffer.duplicate())
    }

    internal class LegacyFileChannelRecordBatch(
        offset: Long,
        magic: Byte,
        fileRecords: FileRecords,
        position: Int,
        batchSize: Int
    ) : FileLogInputStream.FileChannelRecordBatch(
        offset = offset,
        magic = magic,
        fileRecords = fileRecords,
        position = position,
        batchSize = batchSize
    ) {

        override fun toMemoryRecordBatch(buffer: ByteBuffer): RecordBatch =
            ByteBufferLegacyRecordBatch(buffer)

        override fun baseOffset(): Long = loadFullBatch().baseOffset()

        override fun deleteHorizonMs(): Long? = null

        override fun lastOffset(): Long = offset

        override fun producerId(): Long = RecordBatch.NO_PRODUCER_ID

        override fun producerEpoch(): Short = RecordBatch.NO_PRODUCER_EPOCH

        override fun baseSequence(): Int = RecordBatch.NO_SEQUENCE

        override fun lastSequence(): Int = RecordBatch.NO_SEQUENCE

        override fun countOrNull(): Int? = null

        override val isTransactional: Boolean = false

        override val isControlBatch: Boolean = false

        override fun partitionLeaderEpoch(): Int =  RecordBatch.NO_PARTITION_LEADER_EPOCH

        override fun headerSize(): Int = Records.LOG_OVERHEAD + LegacyRecord.headerSize(magic)
    }

    companion object {

        fun writeHeader(buffer: ByteBuffer, offset: Long, size: Int) {
            buffer.putLong(offset)
            buffer.putInt(size)
        }

        @Throws(IOException::class)
        fun writeHeader(out: DataOutputStream, offset: Long, size: Int) {
            out.writeLong(offset)
            out.writeInt(size)
        }
    }
}
