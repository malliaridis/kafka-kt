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

import java.io.DataInputStream
import java.io.EOFException
import java.io.IOException
import java.nio.BufferUnderflowException
import java.nio.ByteBuffer
import java.util.*
import org.apache.kafka.common.InvalidRecordException
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.errors.CorruptRecordException
import org.apache.kafka.common.header.Header
import org.apache.kafka.common.record.FileLogInputStream.FileChannelRecordBatch
import org.apache.kafka.common.utils.BufferSupplier
import org.apache.kafka.common.utils.ByteBufferOutputStream
import org.apache.kafka.common.utils.ByteUtils
import org.apache.kafka.common.utils.CloseableIterator
import org.apache.kafka.common.utils.Crc32C

/**
 * RecordBatch implementation for magic 2 and above. The schema is given below:
 *
 * RecordBatch =>
 * BaseOffset => Int64
 * Length => Int32
 * PartitionLeaderEpoch => Int32
 * Magic => Int8
 * CRC => Uint32
 * Attributes => Int16
 * LastOffsetDelta => Int32 // also serves as LastSequenceDelta
 * BaseTimestamp => Int64
 * MaxTimestamp => Int64
 * ProducerId => Int64
 * ProducerEpoch => Int16
 * BaseSequence => Int32
 * Records => [Record]
 *
 * Note that when compression is enabled (see attributes below), the compressed record data is
 * serialized directly following the count of the number of records.
 *
 * The CRC covers the data from the attributes to the end of the batch (i.e. all the bytes that
 * follow the CRC). It is located after the magic byte, which means that clients must parse the
 * magic byte before deciding how to interpret the bytes between the batch length and the magic
 * byte. The partition leader epoch field is not included in the CRC computation to avoid the need
 * to recompute the CRC when this field is assigned for every batch that is received by the broker.
 * The CRC-32C (Castagnoli) polynomial is used for the computation.
 *
 * On Compaction: Unlike the older message formats, magic v2 and above preserves the first and last
 * offset/sequence numbers from the original batch when the log is cleaned. This is required in
 * order to be able to restore the producer's state when the log is reloaded. If we did not retain
 * the last sequence number, then following a partition leader failure, once the new leader has
 * rebuilt the producer state from the log, the next sequence expected number would no longer be in
 * sync with what was written by the client. This would cause an unexpected OutOfOrderSequence
 * error, which is typically fatal. The base sequence number must be preserved for duplicate
 * checking: the broker checks incoming Produce requests for duplicates by verifying that the first
 * and last sequence numbers of the incoming batch match the last from that producer.
 *
 * Note that if all of the records in a batch are removed during compaction, the broker may still
 * retain an empty batch header in order to preserve the producer sequence information as described
 * above. These empty batches are retained only until either a new sequence number is written by the
 * corresponding producer or the producerId is expired from lack of activity.
 *
 * There is no similar need to preserve the timestamp from the original batch after compaction. The
 * BaseTimestamp field therefore reflects the timestamp of the first record in the batch in most
 * cases. If the batch is empty, the BaseTimestamp will be set to -1 (NO_TIMESTAMP). If the delete
 * horizon flag is set to 1, the BaseTimestamp will be set to the time at which tombstone records
 * and aborted transaction markers in the batch should be removed.
 *
 * Similarly, the MaxTimestamp field reflects the maximum timestamp of the current records if the
 * timestamp type is CREATE_TIME. For LOG_APPEND_TIME, on the other hand, the MaxTimestamp field
 * reflects the timestamp set by the broker and is preserved after compaction. Additionally, the
 * MaxTimestamp of an empty batch always retains the previous value prior to becoming empty.
 *
 * The current attributes are given below:
 *
 * ---------------------------------------------------------------------------------------------------------------------------
 * | Unused (7-15) | Delete Horizon Flag (6) | Control (5) | Transactional (4) | Timestamp Type (3) | Compression Type (0-2) |
 * ---------------------------------------------------------------------------------------------------------------------------
 */
class DefaultRecordBatch internal constructor(
    private val buffer: ByteBuffer,
) : AbstractRecordBatch(), MutableRecordBatch {

    override fun magic(): Byte = buffer[MAGIC_OFFSET]

    override fun ensureValid() {

        if (sizeInBytes() < RECORD_BATCH_OVERHEAD) throw CorruptRecordException(
            "Record batch is corrupt (the size ${sizeInBytes()}" +
                    " is smaller than the minimum allowed overhead $RECORD_BATCH_OVERHEAD)"
        )

        if (!isValid) throw CorruptRecordException(
            "Record is corrupt (stored crc = ${checksum()}, computed crc = ${computeChecksum()})"
        )
    }

    /**
     * Gets the base timestamp of the batch which is used to calculate the record timestamps from
     * the deltas.
     *
     * @return The base timestamp
     */
    fun baseTimestamp(): Long {
        return buffer.getLong(BASE_TIMESTAMP_OFFSET)
    }

    override fun maxTimestamp(): Long {
        return buffer.getLong(MAX_TIMESTAMP_OFFSET)
    }

    override fun timestampType(): TimestampType =
        if ((attributes().toInt() and TIMESTAMP_TYPE_MASK.toInt()) == 0) TimestampType.CREATE_TIME
        else TimestampType.LOG_APPEND_TIME

    override fun baseOffset(): Long {
        return buffer.getLong(BASE_OFFSET_OFFSET)
    }

    override fun lastOffset(): Long {
        return baseOffset() + lastOffsetDelta()
    }

    override fun producerId(): Long {
        return buffer.getLong(PRODUCER_ID_OFFSET)
    }

    override fun producerEpoch(): Short {
        return buffer.getShort(PRODUCER_EPOCH_OFFSET)
    }

    override fun baseSequence(): Int {
        return buffer.getInt(BASE_SEQUENCE_OFFSET)
    }

    private fun lastOffsetDelta(): Int {
        return buffer.getInt(LAST_OFFSET_DELTA_OFFSET)
    }

    override fun lastSequence(): Int {
        val baseSequence = baseSequence()
        return if (baseSequence == RecordBatch.NO_SEQUENCE) RecordBatch.NO_SEQUENCE
        else incrementSequence(baseSequence, lastOffsetDelta())
    }

    override fun compressionType(): CompressionType {
        return CompressionType.forId(attributes().toInt() and COMPRESSION_CODEC_MASK.toInt())
    }

    override fun sizeInBytes(): Int {
        return Records.LOG_OVERHEAD + buffer.getInt(LENGTH_OFFSET)
    }

    private fun count(): Int {
        return buffer.getInt(RECORDS_COUNT_OFFSET)
    }

    override fun countOrNull(): Int {
        return count()
    }

    override fun writeTo(buffer: ByteBuffer) {
        buffer.put(this.buffer.duplicate())
    }

    override fun writeTo(outputStream: ByteBufferOutputStream) {
        outputStream.write(buffer.duplicate())
    }

    override val isTransactional: Boolean
        get() = (attributes().toInt() and TRANSACTIONAL_FLAG_MASK.toInt()) > 0

    private fun hasDeleteHorizonMs(): Boolean {
        return (attributes().toInt() and DELETE_HORIZON_FLAG_MASK.toInt()) > 0
    }

    override fun deleteHorizonMs(): Long? {
        return if (hasDeleteHorizonMs()) buffer.getLong(BASE_TIMESTAMP_OFFSET)
        else null
    }

    override val isControlBatch: Boolean
        get() = (attributes().toInt() and CONTROL_FLAG_MASK) > 0

    override fun partitionLeaderEpoch(): Int {
        return buffer.getInt(PARTITION_LEADER_EPOCH_OFFSET)
    }

    fun recordInputStream(bufferSupplier: BufferSupplier): DataInputStream {
        val buffer = buffer.duplicate()
        buffer.position(RECORDS_OFFSET)
        return DataInputStream(compressionType().wrapForInput(buffer, magic(), bufferSupplier))
    }

    private fun compressedIterator(
        bufferSupplier: BufferSupplier,
        skipKeyValue: Boolean
    ): CloseableIterator<Record> {
        val inputStream = recordInputStream(bufferSupplier)
        return if (skipKeyValue) {
            // this buffer is used to skip length delimited fields like key, value, headers
            val skipArray = ByteArray(MAX_SKIP_BUFFER_SIZE)
            object : StreamRecordIterator(inputStream) {

                @Throws(IOException::class)
                override fun doReadRecord(
                    baseOffset: Long,
                    baseTimestamp: Long,
                    baseSequence: Int,
                    logAppendTime: Long?
                ): Record {
                    return DefaultRecord.readPartiallyFrom(
                        inputStream,
                        skipArray,
                        baseOffset,
                        baseTimestamp,
                        baseSequence,
                        logAppendTime
                    )
                }
            }
        } else object : StreamRecordIterator(inputStream) {

            @Throws(IOException::class)
            override fun doReadRecord(
                baseOffset: Long,
                baseTimestamp: Long,
                baseSequence: Int,
                logAppendTime: Long?
            ): Record {
                return DefaultRecord.readFrom(
                    inputStream,
                    baseOffset,
                    baseTimestamp,
                    baseSequence,
                    logAppendTime
                )
            }
        }
    }

    private fun uncompressedIterator(): CloseableIterator<Record> {
        val buffer = buffer.duplicate()
        buffer.position(RECORDS_OFFSET)

        return object : RecordIterator() {

            override fun readNext(
                baseOffset: Long,
                baseTimestamp: Long,
                baseSequence: Int,
                logAppendTime: Long?
            ): Record {
                try {
                    return DefaultRecord.readFrom(
                        buffer,
                        baseOffset,
                        baseTimestamp,
                        baseSequence,
                        logAppendTime
                    )
                } catch (e: BufferUnderflowException) {
                    throw InvalidRecordException("Incorrect declared batch size, premature EOF reached")
                }
            }

            override fun ensureNoneRemaining(): Boolean = !buffer.hasRemaining()

            override fun close() = Unit
        }
    }

    override fun iterator(): Iterator<Record> {
        if (count() == 0) return Collections.emptyIterator()
        if (!isCompressed) return uncompressedIterator()
        compressedIterator(BufferSupplier.NO_CACHING, false).use { iterator ->
            val records: MutableList<Record> =
                ArrayList(count())
            while (iterator.hasNext()) records.add(iterator.next())
            return records.iterator()
        }
    }

    override fun skipKeyValueIterator(bufferSupplier: BufferSupplier): CloseableIterator<Record> {
        if (count() == 0) {
            return CloseableIterator.wrap(Collections.emptyIterator())
        }

        /*
         * For uncompressed iterator, it is actually not worth skipping key / value / headers at all since
         * its ByteBufferInputStream's skip() function is less efficient compared with just reading it actually
         * as it will allocate new byte array.
         */return if (!isCompressed) uncompressedIterator() else compressedIterator(
            bufferSupplier,
            true
        )

        // we define this to be a closable iterator so that caller (i.e. the log validator) needs to close it
        // while we can save memory footprint of not decompressing the full record set ahead of time
    }

    override fun streamingIterator(bufferSupplier: BufferSupplier): CloseableIterator<Record> {
        return if (isCompressed) compressedIterator(bufferSupplier, false)
        else uncompressedIterator()
    }

    override fun setLastOffset(offset: Long) {
        buffer.putLong(BASE_OFFSET_OFFSET, offset - lastOffsetDelta())
    }

    override fun setMaxTimestamp(timestampType: TimestampType, maxTimestamp: Long) {
        val currentMaxTimestamp = maxTimestamp()
        // We don't need to recompute crc if the timestamp is not updated.
        if (timestampType() == timestampType && currentMaxTimestamp == maxTimestamp) return
        val attributes = computeAttributes(
            compressionType(),
            timestampType,
            isTransactional,
            isControlBatch,
            hasDeleteHorizonMs()
        )
        buffer.putShort(ATTRIBUTES_OFFSET, attributes.toShort())
        buffer.putLong(MAX_TIMESTAMP_OFFSET, maxTimestamp)
        val crc = computeChecksum()
        ByteUtils.writeUnsignedInt(buffer, CRC_OFFSET, crc)
    }

    override fun setPartitionLeaderEpoch(epoch: Int) {
        buffer.putInt(PARTITION_LEADER_EPOCH_OFFSET, epoch)
    }

    override fun checksum(): Long {
        return ByteUtils.readUnsignedInt(buffer, CRC_OFFSET)
    }

    override val isValid: Boolean
        get() = sizeInBytes() >= RECORD_BATCH_OVERHEAD && checksum() == computeChecksum()

    private fun computeChecksum(): Long {
        return Crc32C.compute(buffer, ATTRIBUTES_OFFSET, buffer.limit() - ATTRIBUTES_OFFSET)
    }

    private fun attributes(): Byte {
        // note we're not using the second byte of attributes
        return buffer.getShort(ATTRIBUTES_OFFSET).toByte()
    }

    override fun equals(o: Any?): Boolean {
        if (this === o) return true
        if (o == null || javaClass != o.javaClass) return false
        val that = o as DefaultRecordBatch
        return (buffer == that.buffer)
    }

    override fun hashCode(): Int {
        return buffer.hashCode()
    }

    override fun toString(): String {
        return "RecordBatch(magic=${magic()}" +
                ", offsets=[${baseOffset()}" +
                ", ${lastOffset()}]" +
                ", sequence=[${baseSequence()}" +
                ", ${lastSequence()}]" +
                ", isTransactional=$isTransactional" +
                ", isControlBatch=$isControlBatch" +
                ", compression=${compressionType()}" +
                ", timestampType=${timestampType()}" +
                ", crc=${checksum()})"
    }

    private abstract inner class RecordIterator : CloseableIterator<Record> {

        private val logAppendTime: Long?

        private val baseOffset: Long = baseOffset()

        private val baseTimestamp: Long = baseTimestamp()

        private val baseSequence: Int = baseSequence()

        private val numRecords: Int = count()

        private var readRecords = 0

        init {
            logAppendTime =
                if (timestampType() == TimestampType.LOG_APPEND_TIME) maxTimestamp()
                else null

            if (numRecords < 0) throw InvalidRecordException(
                "Found invalid record count $numRecords in magic v${magic()} batch"
            )
        }

        override fun hasNext(): Boolean = readRecords < numRecords

        override fun next(): Record {

            if (readRecords >= numRecords) throw NoSuchElementException()
            readRecords++

            val rec = readNext(baseOffset, baseTimestamp, baseSequence, logAppendTime)

            if (readRecords == numRecords) {
                // Validate that the actual size of the batch is equal to declared size
                // by checking that after reading declared number of items, there no items left
                // (overflow case, i.e. reading past buffer end is checked elsewhere).
                if (!ensureNoneRemaining())throw InvalidRecordException(
                    "Incorrect declared batch size, records still remaining in file"
                )
            }
            return rec
        }

        protected abstract fun readNext(
            baseOffset: Long,
            baseTimestamp: Long,
            baseSequence: Int,
            logAppendTime: Long?
        ): Record

        protected abstract fun ensureNoneRemaining(): Boolean

        override fun remove() = throw UnsupportedOperationException()
    }

    private abstract inner class StreamRecordIterator(
        private val inputStream: DataInputStream,
    ) : RecordIterator() {

        @Throws(IOException::class)
        abstract fun doReadRecord(
            baseOffset: Long,
            baseTimestamp: Long,
            baseSequence: Int,
            logAppendTime: Long?
        ): Record

        override fun readNext(
            baseOffset: Long,
            baseTimestamp: Long,
            baseSequence: Int,
            logAppendTime: Long?
        ): Record {
            try {
                return doReadRecord(baseOffset, baseTimestamp, baseSequence, logAppendTime)
            } catch (e: EOFException) {
                throw InvalidRecordException("Incorrect declared batch size, premature EOF reached")
            } catch (e: IOException) {
                throw KafkaException("Failed to decompress record stream", e)
            }
        }

        override fun ensureNoneRemaining(): Boolean {
            try {
                return inputStream.read() == -1
            } catch (e: IOException) {
                throw KafkaException("Error checking for remaining bytes after reading batch", e)
            }
        }

        override fun close() {
            try {
                inputStream.close()
            } catch (e: IOException) {
                throw KafkaException("Failed to close record stream", e)
            }
        }
    }

    internal class DefaultFileChannelRecordBatch(
        offset: Long,
        magic: Byte,
        fileRecords: FileRecords?,
        position: Int,
        batchSize: Int
    ) : FileChannelRecordBatch(offset, magic, fileRecords, position, batchSize) {

        override fun toMemoryRecordBatch(
            buffer: ByteBuffer,
        ): RecordBatch = DefaultRecordBatch(buffer)

        override fun baseOffset(): Long = offset

        override fun lastOffset(): Long = loadBatchHeader().lastOffset()

        override fun producerId(): Long = loadBatchHeader().producerId()

        override fun producerEpoch(): Short = loadBatchHeader().producerEpoch()

        override fun baseSequence(): Int = loadBatchHeader().baseSequence()

        override fun lastSequence(): Int = loadBatchHeader().lastSequence()

        override fun checksum(): Long = loadBatchHeader().checksum()

        override fun countOrNull(): Int? = loadBatchHeader().countOrNull()

        override val isTransactional: Boolean
            get() = loadBatchHeader().isTransactional

        override fun deleteHorizonMs(): Long? = loadBatchHeader().deleteHorizonMs()

        override val isControlBatch: Boolean
            get() = loadBatchHeader().isControlBatch

        override fun partitionLeaderEpoch(): Int = loadBatchHeader().partitionLeaderEpoch()

        override fun headerSize(): Int = RECORD_BATCH_OVERHEAD
    }

    companion object {

        const val BASE_OFFSET_OFFSET = 0

        const val BASE_OFFSET_LENGTH = 8

        const val LENGTH_OFFSET = BASE_OFFSET_OFFSET + BASE_OFFSET_LENGTH

        const val LENGTH_LENGTH = 4

        const val PARTITION_LEADER_EPOCH_OFFSET = LENGTH_OFFSET + LENGTH_LENGTH

        const val PARTITION_LEADER_EPOCH_LENGTH = 4

        const val MAGIC_OFFSET = PARTITION_LEADER_EPOCH_OFFSET + PARTITION_LEADER_EPOCH_LENGTH

        const val MAGIC_LENGTH = 1

        const val CRC_OFFSET = MAGIC_OFFSET + MAGIC_LENGTH

        const val CRC_LENGTH = 4

        const val ATTRIBUTES_OFFSET = CRC_OFFSET + CRC_LENGTH

        const val ATTRIBUTE_LENGTH = 2

        const val LAST_OFFSET_DELTA_OFFSET = ATTRIBUTES_OFFSET + ATTRIBUTE_LENGTH

        const val LAST_OFFSET_DELTA_LENGTH = 4

        const val BASE_TIMESTAMP_OFFSET = LAST_OFFSET_DELTA_OFFSET + LAST_OFFSET_DELTA_LENGTH

        const val BASE_TIMESTAMP_LENGTH = 8

        const val MAX_TIMESTAMP_OFFSET = BASE_TIMESTAMP_OFFSET + BASE_TIMESTAMP_LENGTH

        const val MAX_TIMESTAMP_LENGTH = 8

        const val PRODUCER_ID_OFFSET = MAX_TIMESTAMP_OFFSET + MAX_TIMESTAMP_LENGTH

        const val PRODUCER_ID_LENGTH = 8

        const val PRODUCER_EPOCH_OFFSET = PRODUCER_ID_OFFSET + PRODUCER_ID_LENGTH

        const val PRODUCER_EPOCH_LENGTH = 2

        const val BASE_SEQUENCE_OFFSET = PRODUCER_EPOCH_OFFSET + PRODUCER_EPOCH_LENGTH

        const val BASE_SEQUENCE_LENGTH = 4

        const val RECORDS_COUNT_OFFSET = BASE_SEQUENCE_OFFSET + BASE_SEQUENCE_LENGTH

        const val RECORDS_COUNT_LENGTH = 4

        const val RECORDS_OFFSET = RECORDS_COUNT_OFFSET + RECORDS_COUNT_LENGTH

        const val RECORD_BATCH_OVERHEAD = RECORDS_OFFSET

        private const val COMPRESSION_CODEC_MASK: Byte = 0x07

        private const val TRANSACTIONAL_FLAG_MASK: Byte = 0x10

        private const val CONTROL_FLAG_MASK = 0x20

        private const val DELETE_HORIZON_FLAG_MASK: Byte = 0x40

        private const val TIMESTAMP_TYPE_MASK: Byte = 0x08

        private const val MAX_SKIP_BUFFER_SIZE = 2048

        private fun computeAttributes(
            type: CompressionType?,
            timestampType: TimestampType,
            isTransactional: Boolean,
            isControl: Boolean,
            isDeleteHorizonSet: Boolean,
        ): Byte {
            if (timestampType == TimestampType.NO_TIMESTAMP_TYPE) throw IllegalArgumentException(
                "Timestamp type must be provided to compute attributes for message " +
                        "format v2 and above"
            )

            var attributes = if (isTransactional) TRANSACTIONAL_FLAG_MASK else 0

            if (isControl) attributes = (attributes.toInt() or CONTROL_FLAG_MASK).toByte()

            if (type!!.id > 0) attributes =
                (attributes.toInt() or (COMPRESSION_CODEC_MASK.toInt() and type.id)).toByte()

            if (timestampType == TimestampType.LOG_APPEND_TIME) attributes =
                (attributes.toInt() or TIMESTAMP_TYPE_MASK.toInt()).toByte()

            if (isDeleteHorizonSet) attributes =
                (attributes.toInt() or DELETE_HORIZON_FLAG_MASK.toInt()).toByte()

            return attributes
        }

        fun writeEmptyHeader(
            buffer: ByteBuffer,
            magic: Byte,
            producerId: Long,
            producerEpoch: Short,
            baseSequence: Int,
            baseOffset: Long,
            lastOffset: Long,
            partitionLeaderEpoch: Int,
            timestampType: TimestampType,
            timestamp: Long,
            isTransactional: Boolean,
            isControlRecord: Boolean
        ) {
            val offsetDelta = (lastOffset - baseOffset).toInt()
            writeHeader(
                buffer,
                baseOffset,
                offsetDelta,
                RECORD_BATCH_OVERHEAD,
                magic,
                CompressionType.NONE,
                timestampType,
                RecordBatch.NO_TIMESTAMP,
                timestamp,
                producerId,
                producerEpoch,
                baseSequence,
                isTransactional,
                isControlRecord,
                false,
                partitionLeaderEpoch,
                0
            )
        }

        fun writeHeader(
            buffer: ByteBuffer,
            baseOffset: Long,
            lastOffsetDelta: Int,
            sizeInBytes: Int,
            magic: Byte,
            compressionType: CompressionType?,
            timestampType: TimestampType,
            baseTimestamp: Long,
            maxTimestamp: Long,
            producerId: Long,
            epoch: Short,
            sequence: Int,
            isTransactional: Boolean,
            isControlBatch: Boolean,
            isDeleteHorizonSet: Boolean,
            partitionLeaderEpoch: Int,
            numRecords: Int
        ) {
            if (magic < RecordBatch.CURRENT_MAGIC_VALUE)
                throw IllegalArgumentException("Invalid magic value $magic")

            if (baseTimestamp < 0 && baseTimestamp != RecordBatch.NO_TIMESTAMP)
                throw IllegalArgumentException("Invalid message timestamp $baseTimestamp")

            val attributes = computeAttributes(
                compressionType,
                timestampType,
                isTransactional,
                isControlBatch,
                isDeleteHorizonSet
            ).toShort()

            val position = buffer.position()
            buffer.putLong(position + BASE_OFFSET_OFFSET, baseOffset)
            buffer.putInt(position + LENGTH_OFFSET, sizeInBytes - Records.LOG_OVERHEAD)
            buffer.putInt(position + PARTITION_LEADER_EPOCH_OFFSET, partitionLeaderEpoch)
            buffer.put(position + MAGIC_OFFSET, magic)
            buffer.putShort(position + ATTRIBUTES_OFFSET, attributes)
            buffer.putLong(position + BASE_TIMESTAMP_OFFSET, baseTimestamp)
            buffer.putLong(position + MAX_TIMESTAMP_OFFSET, maxTimestamp)
            buffer.putInt(position + LAST_OFFSET_DELTA_OFFSET, lastOffsetDelta)
            buffer.putLong(position + PRODUCER_ID_OFFSET, producerId)
            buffer.putShort(position + PRODUCER_EPOCH_OFFSET, epoch)
            buffer.putInt(position + BASE_SEQUENCE_OFFSET, sequence)
            buffer.putInt(position + RECORDS_COUNT_OFFSET, numRecords)
            val crc = Crc32C.compute(buffer, ATTRIBUTES_OFFSET, sizeInBytes - ATTRIBUTES_OFFSET)
            buffer.putInt(position + CRC_OFFSET, crc.toInt())
            buffer.position(position + RECORD_BATCH_OVERHEAD)
        }

        fun sizeInBytes(baseOffset: Long, records: Iterable<Record>): Int {
            val iterator = records.iterator()

            if (!iterator.hasNext()) return 0

            var size = RECORD_BATCH_OVERHEAD
            var baseTimestamp: Long? = null

            while (iterator.hasNext()) {
                val record = iterator.next()
                val offsetDelta = (record.offset() - baseOffset).toInt()
                if (baseTimestamp == null) baseTimestamp = record.timestamp()
                val timestampDelta = record.timestamp() - baseTimestamp
                size += DefaultRecord.sizeInBytes(
                    offsetDelta, timestampDelta, record.key(), record.value(),
                    record.headers()
                )
            }

            return size
        }

        fun sizeInBytes(records: Iterable<SimpleRecord>): Int {
            val iterator = records.iterator()

            if (!iterator.hasNext()) return 0

            var size = RECORD_BATCH_OVERHEAD
            var offsetDelta = 0
            var baseTimestamp: Long? = null

            while (iterator.hasNext()) {
                val record = iterator.next()
                if (baseTimestamp == null) baseTimestamp = record.timestamp()
                val timestampDelta = record.timestamp() - baseTimestamp
                size += DefaultRecord.sizeInBytes(
                    offsetDelta++, timestampDelta, record.key(), record.value(),
                    record.headers()
                )
            }

            return size
        }

        /**
         * Get an upper bound on the size of a batch with only a single record using a given key and
         * value. This is only an estimate because it does not take into account additional overhead
         * from the compression algorithm used.
         */
        fun estimateBatchSizeUpperBound(
            key: ByteBuffer,
            value: ByteBuffer,
            headers: Array<Header>
        ): Int = RECORD_BATCH_OVERHEAD + DefaultRecord.recordSizeUpperBound(key, value, headers)

        fun incrementSequence(sequence: Int, increment: Int): Int =
            if (sequence > Int.MAX_VALUE - increment) increment - (Int.MAX_VALUE - sequence) - 1
            else sequence + increment

        fun decrementSequence(sequence: Int, decrement: Int): Int =
            if (sequence < decrement) Int.MAX_VALUE - (decrement - sequence) + 1
            else sequence - decrement
    }
}
