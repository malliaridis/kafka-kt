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

import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.message.LeaderChangeMessage
import org.apache.kafka.common.message.SnapshotFooterRecord
import org.apache.kafka.common.message.SnapshotHeaderRecord
import org.apache.kafka.common.network.TransferableChannel
import org.apache.kafka.common.record.MemoryRecords.RecordFilter.BatchRetention
import org.apache.kafka.common.utils.AbstractIterator
import org.apache.kafka.common.utils.BufferSupplier
import org.apache.kafka.common.utils.ByteBufferOutputStream
import org.apache.kafka.common.utils.Time
import org.apache.kafka.common.utils.Utils.tryWriteTo
import org.slf4j.LoggerFactory
import java.io.IOException
import java.nio.ByteBuffer
import java.nio.channels.GatheringByteChannel
import java.util.*
import kotlin.math.max

/**
 * A [Records] implementation backed by a ByteBuffer. This is used only for reading or modifying
 * in-place an existing buffer of record batches. To create a new buffer see [MemoryRecordsBuilder],
 * or one of the [builder] variants.
 */
class MemoryRecords private constructor(private val buffer: ByteBuffer) : AbstractRecords() {

    private val batches = Iterable { batchIterator() }

    private var validBytes = -1

    // Construct a writable memory records

    override fun sizeInBytes(): Int = buffer.limit()

    @Throws(IOException::class)
    override fun writeTo(channel: TransferableChannel, position: Long, length: Int): Long {
        require(position <= Int.MAX_VALUE) {
            "position should not be greater than Integer.MAX_VALUE: $position"
        }
        require(position + length <= buffer.limit()) {
            "position+length should not be greater than buffer.limit(), position: " +
                    "$position, length: $length, buffer.limit(): ${buffer.limit()}"
        }

        return tryWriteTo(channel, position.toInt(), length, buffer)
    }

    /**
     * Write all records to the given channel (including partial records).
     *
     * @param channel The channel to write to
     * @return The number of bytes written
     * @throws IOException For any IO errors writing to the channel
     */
    @Throws(IOException::class)
    fun writeFullyTo(channel: GatheringByteChannel): Int {
        buffer.mark()
        var written = 0
        while (written < sizeInBytes()) written += channel.write(buffer)
        buffer.reset()
        return written
    }

    /**
     * The total number of bytes in this message set not including any partial, trailing messages.
     * This may be smaller than what is returned by [sizeInBytes].
     * @return The number of valid bytes
     */
    fun validBytes(): Int {
        if (validBytes >= 0) return validBytes
        var bytes = 0
        for (batch: RecordBatch in batches()) bytes += batch.sizeInBytes()
        validBytes = bytes
        return bytes
    }

    override fun downConvert(
        toMagic: Byte,
        firstOffset: Long,
        time: Time
    ): ConvertedRecords<MemoryRecords> = RecordsUtil.downConvert(
        batches = batches(),
        toMagic = toMagic,
        firstOffset = firstOffset,
        time = time,
    )

    override fun batchIterator(): AbstractIterator<MutableRecordBatch> =
        RecordBatchIterator(ByteBufferLogInputStream(buffer.duplicate(), Int.MAX_VALUE))

    /**
     * Validates the header of the first batch and returns batch size.
     *
     * @return first batch size including LOG_OVERHEAD if buffer contains header up to magic byte,
     * `null` otherwise
     * @throws CorruptRecordException if record size or magic is invalid
     */
    fun firstBatchSize(): Int? =
        if (buffer.remaining() < Records.HEADER_SIZE_UP_TO_MAGIC) null
        else ByteBufferLogInputStream(buffer, Int.MAX_VALUE).nextBatchSize()

    /**
     * Filter the records into the provided ByteBuffer.
     *
     * @param partition The partition that is filtered (used only for logging)
     * @param filter The filter function
     * @param destinationBuffer The byte buffer to write the filtered records to
     * @param maxRecordBatchSize The maximum record batch size. Note this is not a hard limit: if a
     * batch exceeds this after filtering, we log a warning, but the batch will still be created.
     * @param decompressionBufferSupplier The supplier of ByteBuffer(s) used for decompression if
     * supported. For small record batches, allocating a potentially large buffer (64 KB for LZ4)
     * will dominate the cost of decompressing and iterating over the records in the batch. As such,
     * a supplier that reuses buffers will have a significant performance impact.
     * @return A FilterResult with a summary of the output (for metrics) and potentially an overflow
     * buffer
     */
    fun filterTo(
        partition: TopicPartition, filter: RecordFilter, destinationBuffer: ByteBuffer,
        maxRecordBatchSize: Int, decompressionBufferSupplier: BufferSupplier
    ): FilterResult {
        return filterTo(
            partition,
            batches(),
            filter,
            destinationBuffer,
            maxRecordBatchSize,
            decompressionBufferSupplier
        )
    }

    private class BatchFilterResult(
        val writeOriginalBatch: Boolean,
        val containsTombstones: Boolean,
        val maxOffset: Long
    )

    /**
     * Get the byte buffer that backs this instance for reading.
     */
    fun buffer(): ByteBuffer = buffer.duplicate()

    override fun batches(): Iterable<MutableRecordBatch> = batches

    override fun toString(): String =
        "MemoryRecords(size=${sizeInBytes()}, buffer=$buffer)"

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other == null || javaClass != other.javaClass) return false
        val that = other as MemoryRecords
        return buffer == that.buffer
    }

    override fun hashCode(): Int = buffer.hashCode()

    abstract class RecordFilter(val currentTime: Long, val deleteRetentionMs: Long) {

        class BatchRetentionResult(
            val batchRetention: BatchRetention,
            val containsMarkerForEmptyTxn: Boolean
        )

        enum class BatchRetention {
            DELETE,

            // Delete the batch without inspecting records
            RETAIN_EMPTY,

            // Retain the batch even if it is empty
            DELETE_EMPTY // Delete the batch if it is empty
        }

        /**
         * Check whether the full batch can be discarded (i.e. whether we even need to check the
         * records individually).
         */
        abstract fun checkBatchRetention(batch: RecordBatch): BatchRetentionResult

        /**
         * Check whether a record should be retained in the log. Note that [checkBatchRetention]
         * is used prior to checking individual record retention. Only records from batches which
         * were not explicitly discarded with [BatchRetention.DELETE] will be considered.
         */
        abstract fun shouldRetainRecord(recordBatch: RecordBatch, record: Record): Boolean
    }

    class FilterResult internal constructor(var outputBuffer: ByteBuffer) {
        var messagesRead = 0
            internal set

        // Note that `bytesRead` should contain only bytes from batches that have been processed,
        // i.e. bytes from `messagesRead` and any discarded batches.
        var bytesRead = 0
            internal set

        var messagesRetained = 0
            private set

        var bytesRetained = 0
            private set

        var maxOffset = -1L
            private set

        var maxTimestamp = RecordBatch.NO_TIMESTAMP
            private set

        var shallowOffsetOfMaxTimestamp = -1L
            private set

        fun updateRetainedBatchMetadata(
            retainedBatch: MutableRecordBatch,
            numMessagesInBatch: Int,
            headerOnly: Boolean
        ) {
            val bytesRetained =
                if (headerOnly) DefaultRecordBatch.RECORD_BATCH_OVERHEAD
                else retainedBatch.sizeInBytes()

            updateRetainedBatchMetadata(
                maxTimestamp = retainedBatch.maxTimestamp(),
                shallowOffsetOfMaxTimestamp = retainedBatch.lastOffset(),
                maxOffset = retainedBatch.lastOffset(),
                messagesRetained = numMessagesInBatch,
                bytesRetained = bytesRetained,
            )
        }

        fun updateRetainedBatchMetadata(
            maxTimestamp: Long,
            shallowOffsetOfMaxTimestamp: Long,
            maxOffset: Long,
            messagesRetained: Int,
            bytesRetained: Int
        ) {
            validateBatchMetadata(maxTimestamp, shallowOffsetOfMaxTimestamp, maxOffset)
            if (maxTimestamp > this.maxTimestamp) {
                this.maxTimestamp = maxTimestamp
                this.shallowOffsetOfMaxTimestamp = shallowOffsetOfMaxTimestamp
            }
            this.maxOffset = max(maxOffset, this.maxOffset)
            this.messagesRetained += messagesRetained
            this.bytesRetained += bytesRetained
        }

        private fun validateBatchMetadata(
            maxTimestamp: Long,
            shallowOffsetOfMaxTimestamp: Long,
            maxOffset: Long
        ) {
            require(maxTimestamp == RecordBatch.NO_TIMESTAMP || shallowOffsetOfMaxTimestamp >= 0) {
                "shallowOffset undefined for maximum timestamp $maxTimestamp"
            }
            require(maxOffset >= 0) { "maxOffset undefined" }
        }

        fun outputBuffer(): ByteBuffer = outputBuffer

        @Deprecated(
            message = "User property instead",
            replaceWith = ReplaceWith("messagesRead"),
        )
        fun messagesRead(): Int = messagesRead

        @Deprecated(
            message = "User property instead",
            replaceWith = ReplaceWith("bytesRead"),
        )
        fun bytesRead(): Int = bytesRead

        @Deprecated(
            message = "User property instead",
            replaceWith = ReplaceWith("messagesRetained"),
        )
        fun messagesRetained(): Int = messagesRetained

        @Deprecated(
            message = "User property instead",
            replaceWith = ReplaceWith("bytesRetained"),
        )
        fun bytesRetained(): Int = bytesRetained

        @Deprecated(
            message = "User property instead",
            replaceWith = ReplaceWith("maxOffset"),
        )
        fun maxOffset(): Long = maxOffset

        @Deprecated(
            message = "User property instead",
            replaceWith = ReplaceWith("maxTimestamp"),
        )
        fun maxTimestamp(): Long = maxTimestamp

        @Deprecated(
            message = "User property instead",
            replaceWith = ReplaceWith("shallowOffsetOfMaxTimestamp"),
        )
        fun shallowOffsetOfMaxTimestamp(): Long = shallowOffsetOfMaxTimestamp
    }

    @Suppress("TooManyFunctions")
    companion object {

        private val log = LoggerFactory.getLogger(MemoryRecords::class.java)

        val EMPTY = readableRecords(ByteBuffer.allocate(0))

        /**
         * Note: This method is also used to convert the first timestamp of the batch (which is
         * usually the timestamp of the first record) to the delete horizon of the tombstones or txn
         * markers which are present in the batch.
         */
        private fun filterTo(
            partition: TopicPartition,
            batches: Iterable<MutableRecordBatch>,
            filter: RecordFilter,
            destinationBuffer: ByteBuffer,
            maxRecordBatchSize: Int,
            decompressionBufferSupplier: BufferSupplier,
        ): FilterResult {
            val filterResult = FilterResult(destinationBuffer)
            val bufferOutputStream = ByteBufferOutputStream(destinationBuffer)

            for (batch: MutableRecordBatch in batches) {
                val batchRetentionResult = filter.checkBatchRetention(batch)
                val containsMarkerForEmptyTxn = batchRetentionResult.containsMarkerForEmptyTxn
                val batchRetention = batchRetentionResult.batchRetention
                filterResult.bytesRead += batch.sizeInBytes()
                if (batchRetention == BatchRetention.DELETE) continue

                // We use the absolute offset to decide whether to retain the message or not. Due to
                // KAFKA-4298, we have to allow for the possibility that a previous version
                // corrupted the log by writing a compressed record batch with a magic value not
                // matching the magic of the records (magic < 2). This will be fixed as we recopy
                // the messages to the destination buffer.
                val batchMagic = batch.magic()
                val retainedRecords: MutableList<Record> = ArrayList()
                val iterationResult = filterBatch(
                    batch = batch,
                    decompressionBufferSupplier = decompressionBufferSupplier,
                    filterResult = filterResult,
                    filter = filter,
                    batchMagic = batchMagic,
                    writeOriginalBatch = true,
                    retainedRecords = retainedRecords,
                )
                val containsTombstones = iterationResult.containsTombstones
                val writeOriginalBatch = iterationResult.writeOriginalBatch
                val maxOffset = iterationResult.maxOffset
                if (retainedRecords.isNotEmpty()) {
                    // we check if the delete horizon should be set to a new value in which case, we
                    // need to reset the base timestamp and overwrite the timestamp deltas if the
                    // batch does not contain tombstones, then we don't need to overwrite batch
                    val needToSetDeleteHorizon =
                        batch.magic() >= RecordBatch.MAGIC_VALUE_V2
                                && (containsTombstones || containsMarkerForEmptyTxn)
                                && batch.deleteHorizonMs() == null

                    if (writeOriginalBatch && !needToSetDeleteHorizon) {
                        batch.writeTo(bufferOutputStream)
                        filterResult.updateRetainedBatchMetadata(batch, retainedRecords.size, false)
                    } else {
                        val builder: MemoryRecordsBuilder
                        val deleteHorizonMs: Long =
                            if (needToSetDeleteHorizon) filter.currentTime + filter.deleteRetentionMs
                            else batch.deleteHorizonMs() ?: RecordBatch.NO_TIMESTAMP

                        builder = buildRetainedRecordsInto(
                            originalBatch = batch,
                            retainedRecords = retainedRecords,
                            bufferOutputStream = bufferOutputStream,
                            deleteHorizonMs = deleteHorizonMs
                        )
                        val records = builder.build()
                        val filteredBatchSize = records.sizeInBytes()
                        if (
                            filteredBatchSize > batch.sizeInBytes()
                            && filteredBatchSize > maxRecordBatchSize
                        ) log.warn(
                            "Record batch from {} with last offset {} exceeded max record batch " +
                                    "size {} after cleaning (new size is {}). Consumers with " +
                                    "version earlier than 0.10.1.0 may need to increase their " +
                                    "fetch sizes.",
                            partition,
                            batch.lastOffset(),
                            maxRecordBatchSize,
                            filteredBatchSize,
                        )
                        val info = builder.info()
                        filterResult.updateRetainedBatchMetadata(
                            maxTimestamp = info.maxTimestamp,
                            shallowOffsetOfMaxTimestamp = info.shallowOffsetOfMaxTimestamp,
                            maxOffset = maxOffset,
                            messagesRetained = retainedRecords.size,
                            bytesRetained = filteredBatchSize,
                        )
                    }
                } else if (batchRetention == BatchRetention.RETAIN_EMPTY) {
                    check(batchMagic >= RecordBatch.MAGIC_VALUE_V2) {
                        "Empty batches are only supported for magic v2 and above"
                    }
                    bufferOutputStream.ensureRemaining(DefaultRecordBatch.RECORD_BATCH_OVERHEAD)
                    DefaultRecordBatch.writeEmptyHeader(
                        buffer = bufferOutputStream.buffer,
                        magic = batchMagic,
                        producerId = batch.producerId(),
                        producerEpoch = batch.producerEpoch(),
                        baseSequence = batch.baseSequence(),
                        baseOffset = batch.baseOffset(),
                        lastOffset = batch.lastOffset(),
                        partitionLeaderEpoch = batch.partitionLeaderEpoch(),
                        timestampType = batch.timestampType(),
                        timestamp = batch.maxTimestamp(),
                        isTransactional = batch.isTransactional,
                        isControlRecord = batch.isControlBatch,
                    )
                    filterResult.updateRetainedBatchMetadata(batch, 0, true)
                }

                // If we had to allocate a new buffer to fit the filtered buffer (see KAFKA-5316), return early to
                // avoid the need for additional allocations.
                val outputBuffer = bufferOutputStream.buffer()
                if (outputBuffer !== destinationBuffer) {
                    filterResult.outputBuffer = outputBuffer
                    return filterResult
                }
            }
            return filterResult
        }

        private fun filterBatch(
            batch: RecordBatch,
            decompressionBufferSupplier: BufferSupplier,
            filterResult: FilterResult,
            filter: RecordFilter,
            batchMagic: Byte,
            writeOriginalBatch: Boolean,
            retainedRecords: MutableList<Record>,
        ): BatchFilterResult {
            var writeOriginalBatch = writeOriginalBatch
            var maxOffset: Long = -1
            var containsTombstones = false

            batch.streamingIterator(decompressionBufferSupplier).use { iterator ->
                while (iterator.hasNext()) {
                    val record: Record = iterator.next()
                    filterResult.messagesRead += 1
                    if (filter.shouldRetainRecord(batch, record)) {
                        // Check for log corruption due to KAFKA-4298. If we find it, make sure that
                        // we overwrite the corrupted batch with correct data.
                        if (!record.hasMagic(batchMagic)) writeOriginalBatch = false
                        if (record.offset() > maxOffset) maxOffset = record.offset()
                        retainedRecords.add(record)
                        if (!record.hasValue()) {
                            containsTombstones = true
                        }
                    } else writeOriginalBatch = false
                }

                return BatchFilterResult(writeOriginalBatch, containsTombstones, maxOffset)
            }
        }

        private fun buildRetainedRecordsInto(
            originalBatch: RecordBatch,
            retainedRecords: List<Record>,
            bufferOutputStream: ByteBufferOutputStream,
            deleteHorizonMs: Long,
        ): MemoryRecordsBuilder {
            val magic = originalBatch.magic()
            val timestampType = originalBatch.timestampType()

            val logAppendTime =
                if (timestampType == TimestampType.LOG_APPEND_TIME) originalBatch.maxTimestamp()
                else RecordBatch.NO_TIMESTAMP
            val baseOffset =
                if (magic >= RecordBatch.MAGIC_VALUE_V2) originalBatch.baseOffset()
                else retainedRecords[0].offset()

            val builder = MemoryRecordsBuilder(
                bufferStream = bufferOutputStream,
                magic = magic,
                compressionType = originalBatch.compressionType(),
                timestampType = timestampType,
                baseOffset = baseOffset,
                logAppendTime = logAppendTime,
                producerId = originalBatch.producerId(),
                producerEpoch = originalBatch.producerEpoch(),
                baseSequence = originalBatch.baseSequence(),
                isTransactional = originalBatch.isTransactional,
                isControlBatch = originalBatch.isControlBatch,
                partitionLeaderEpoch = originalBatch.partitionLeaderEpoch(),
                writeLimit = bufferOutputStream.limit,
                deleteHorizonMs = deleteHorizonMs,
            )

            for (record: Record in retainedRecords) builder.append(record)
            if (magic >= RecordBatch.MAGIC_VALUE_V2)
                // we must preserve the last offset from the initial batch in order to ensure that
                // the last sequence number from the batch remains even after compaction. Otherwise,
                // the producer could incorrectly see an out of sequence error.
                builder.overrideLastOffset(originalBatch.lastOffset())

            return builder
        }

        fun readableRecords(buffer: ByteBuffer): MemoryRecords = MemoryRecords(buffer)

        fun builder(
            buffer: ByteBuffer,
            compressionType: CompressionType,
            timestampType: TimestampType,
            baseOffset: Long,
            maxSize: Int
        ): MemoryRecordsBuilder {
            val logAppendTime =
                if (timestampType == TimestampType.LOG_APPEND_TIME) System.currentTimeMillis()
                else RecordBatch.NO_TIMESTAMP

            return MemoryRecordsBuilder(
                buffer = buffer,
                magic = RecordBatch.CURRENT_MAGIC_VALUE,
                compressionType = compressionType,
                timestampType = timestampType,
                baseOffset = baseOffset,
                logAppendTime = logAppendTime,
                producerId = RecordBatch.NO_PRODUCER_ID,
                producerEpoch = RecordBatch.NO_PRODUCER_EPOCH,
                baseSequence = RecordBatch.NO_SEQUENCE,
                isTransactional = false,
                isControlBatch = false,
                partitionLeaderEpoch = RecordBatch.NO_PARTITION_LEADER_EPOCH,
                writeLimit = maxSize,
            )
        }

        fun idempotentBuilder(
            buffer: ByteBuffer,
            compressionType: CompressionType,
            baseOffset: Long,
            producerId: Long,
            producerEpoch: Short,
            baseSequence: Int
        ): MemoryRecordsBuilder = builder(
            buffer = buffer,
            magic = RecordBatch.CURRENT_MAGIC_VALUE,
            compressionType = compressionType,
            timestampType = TimestampType.CREATE_TIME,
            baseOffset = baseOffset,
            logAppendTime = System.currentTimeMillis(),
            producerId = producerId,
            producerEpoch = producerEpoch,
            baseSequence = baseSequence,
        )

        fun builder(
            buffer: ByteBuffer,
            magic: Byte = RecordBatch.CURRENT_MAGIC_VALUE,
            compressionType: CompressionType,
            timestampType: TimestampType,
            baseOffset: Long
        ): MemoryRecordsBuilder {
            val logAppendTime =
                if (timestampType == TimestampType.LOG_APPEND_TIME) System.currentTimeMillis()
                else RecordBatch.NO_TIMESTAMP

            return builder(
                buffer = buffer,
                magic = magic,
                compressionType = compressionType,
                timestampType = timestampType,
                baseOffset = baseOffset,
                logAppendTime = logAppendTime,
                producerId = RecordBatch.NO_PRODUCER_ID,
                producerEpoch = RecordBatch.NO_PRODUCER_EPOCH,
                baseSequence = RecordBatch.NO_SEQUENCE,
                isTransactional = false,
                partitionLeaderEpoch = RecordBatch.NO_PARTITION_LEADER_EPOCH,
            )
        }

        fun builder(
            buffer: ByteBuffer,
            magic: Byte = RecordBatch.CURRENT_MAGIC_VALUE,
            compressionType: CompressionType,
            timestampType: TimestampType = TimestampType.CREATE_TIME,
            baseOffset: Long,
            logAppendTime: Long = RecordBatch.NO_TIMESTAMP,
            producerId: Long = RecordBatch.NO_PRODUCER_ID,
            producerEpoch: Short = RecordBatch.NO_PRODUCER_EPOCH,
            baseSequence: Int = RecordBatch.NO_SEQUENCE,
            isTransactional: Boolean = false,
            isControlBatch: Boolean = false,
            partitionLeaderEpoch: Int = RecordBatch.NO_PARTITION_LEADER_EPOCH,
        ): MemoryRecordsBuilder = MemoryRecordsBuilder(
            buffer = buffer,
            magic = magic,
            compressionType = compressionType,
            timestampType = timestampType,
            baseOffset = baseOffset,
            logAppendTime = logAppendTime,
            producerId = producerId,
            producerEpoch = producerEpoch,
            baseSequence = baseSequence,
            isTransactional = isTransactional,
            isControlBatch = isControlBatch,
            partitionLeaderEpoch = partitionLeaderEpoch,
            writeLimit = buffer.remaining()
        )

        fun withIdempotentRecords(
            magic: Byte = RecordBatch.CURRENT_MAGIC_VALUE,
            initialOffset: Long = 0L,
            compressionType: CompressionType?,
            producerId: Long,
            producerEpoch: Short,
            baseSequence: Int,
            partitionLeaderEpoch: Int = RecordBatch.NO_PARTITION_LEADER_EPOCH,
            vararg records: SimpleRecord,
        ): MemoryRecords = withRecords(
            magic = magic,
            initialOffset = initialOffset,
            compressionType = compressionType,
            timestampType = TimestampType.CREATE_TIME,
            producerId = producerId,
            producerEpoch = producerEpoch,
            baseSequence = baseSequence,
            partitionLeaderEpoch = partitionLeaderEpoch,
            isTransactional = false,
            records = records,
        )

        fun withTransactionalRecords(
            magic: Byte = RecordBatch.CURRENT_MAGIC_VALUE,
            initialOffset: Long = 0L,
            compressionType: CompressionType?,
            producerId: Long,
            producerEpoch: Short,
            baseSequence: Int,
            partitionLeaderEpoch: Int = RecordBatch.NO_PARTITION_LEADER_EPOCH,
            vararg records: SimpleRecord,
        ): MemoryRecords = withRecords(
            magic = magic,
            initialOffset = initialOffset,
            compressionType = compressionType,
            timestampType = TimestampType.CREATE_TIME,
            producerId = producerId,
            producerEpoch = producerEpoch,
            baseSequence = baseSequence,
            partitionLeaderEpoch = partitionLeaderEpoch,
            isTransactional = true,
            records = records
        )

        fun withRecords(
            magic: Byte = RecordBatch.CURRENT_MAGIC_VALUE,
            initialOffset: Long = 0L,
            compressionType: CompressionType?,
            timestampType: TimestampType = TimestampType.CREATE_TIME,
            producerId: Long = RecordBatch.NO_PRODUCER_ID,
            producerEpoch: Short = RecordBatch.NO_PRODUCER_EPOCH,
            baseSequence: Int = RecordBatch.NO_SEQUENCE,
            partitionLeaderEpoch: Int,
            isTransactional: Boolean = false,
            vararg records: SimpleRecord
        ): MemoryRecords {
            if (records.isEmpty()) return EMPTY
            val sizeEstimate = estimateSizeInBytes(
                magic,
                (compressionType)!!,
                listOf(*records),
            )
            val bufferStream = ByteBufferOutputStream(sizeEstimate)
            var logAppendTime = RecordBatch.NO_TIMESTAMP
            if (timestampType == TimestampType.LOG_APPEND_TIME) logAppendTime =
                System.currentTimeMillis()
            val builder = MemoryRecordsBuilder(
                bufferStream = bufferStream,
                magic = magic,
                compressionType = compressionType,
                timestampType = timestampType,
                baseOffset = initialOffset,
                logAppendTime = logAppendTime,
                producerId = producerId,
                producerEpoch = producerEpoch,
                baseSequence = baseSequence,
                isTransactional = isTransactional,
                isControlBatch = false,
                partitionLeaderEpoch = partitionLeaderEpoch,
                writeLimit = sizeEstimate,
            )
            for (record: SimpleRecord in records) builder.append(record)
            return builder.build()
        }

        fun withEndTransactionMarker(
            producerId: Long,
            producerEpoch: Short,
            marker: EndTransactionMarker,
        ): MemoryRecords = withEndTransactionMarker(
            initialOffset = 0L,
            timestamp = System.currentTimeMillis(),
            partitionLeaderEpoch = RecordBatch.NO_PARTITION_LEADER_EPOCH,
            producerId = producerId,
            producerEpoch = producerEpoch,
            marker = marker,
        )

        fun withEndTransactionMarker(
            initialOffset: Long = 0L,
            timestamp: Long,
            partitionLeaderEpoch: Int = RecordBatch.NO_PARTITION_LEADER_EPOCH,
            producerId: Long,
            producerEpoch: Short,
            marker: EndTransactionMarker,
        ): MemoryRecords {
            val endTxnMarkerBatchSize = DefaultRecordBatch.RECORD_BATCH_OVERHEAD +
                    EndTransactionMarker.CURRENT_END_TXN_SCHEMA_RECORD_SIZE
            val buffer = ByteBuffer.allocate(endTxnMarkerBatchSize)

            writeEndTransactionalMarker(
                buffer = buffer,
                initialOffset = initialOffset,
                timestamp = timestamp,
                partitionLeaderEpoch = partitionLeaderEpoch,
                producerId = producerId,
                producerEpoch = producerEpoch,
                marker = marker,
            )
            buffer.flip()
            return readableRecords(buffer)
        }

        fun writeEndTransactionalMarker(
            buffer: ByteBuffer,
            initialOffset: Long,
            timestamp: Long,
            partitionLeaderEpoch: Int,
            producerId: Long,
            producerEpoch: Short,
            marker: EndTransactionMarker,
        ) {
            val isTransactional = true
            MemoryRecordsBuilder(
                buffer = buffer,
                magic = RecordBatch.CURRENT_MAGIC_VALUE,
                compressionType = CompressionType.NONE,
                timestampType = TimestampType.CREATE_TIME,
                baseOffset = initialOffset,
                logAppendTime = timestamp,
                producerId = producerId,
                producerEpoch = producerEpoch,
                baseSequence = RecordBatch.NO_SEQUENCE,
                isTransactional = isTransactional,
                isControlBatch = true,
                partitionLeaderEpoch = partitionLeaderEpoch,
                writeLimit = buffer.capacity(),
            ).use { builder -> builder.appendEndTxnMarker(timestamp, marker) }
        }

        fun withLeaderChangeMessage(
            initialOffset: Long,
            timestamp: Long,
            leaderEpoch: Int,
            buffer: ByteBuffer,
            leaderChangeMessage: LeaderChangeMessage,
        ): MemoryRecords {
            writeLeaderChangeMessage(
                buffer = buffer,
                initialOffset = initialOffset,
                timestamp = timestamp,
                leaderEpoch = leaderEpoch,
                leaderChangeMessage = leaderChangeMessage,
            )
            buffer.flip()
            return readableRecords(buffer)
        }

        private fun writeLeaderChangeMessage(
            buffer: ByteBuffer,
            initialOffset: Long,
            timestamp: Long,
            leaderEpoch: Int,
            leaderChangeMessage: LeaderChangeMessage
        ) = MemoryRecordsBuilder(
            buffer = buffer,
            magic = RecordBatch.CURRENT_MAGIC_VALUE,
            compressionType = CompressionType.NONE,
            timestampType = TimestampType.CREATE_TIME,
            baseOffset = initialOffset,
            logAppendTime = timestamp,
            producerId = RecordBatch.NO_PRODUCER_ID,
            producerEpoch = RecordBatch.NO_PRODUCER_EPOCH,
            baseSequence = RecordBatch.NO_SEQUENCE,
            isTransactional = false,
            isControlBatch = true,
            partitionLeaderEpoch = leaderEpoch,
            writeLimit = buffer.capacity()
        ).use { builder -> builder.appendLeaderChangeMessage(timestamp, leaderChangeMessage) }

        fun withSnapshotHeaderRecord(
            initialOffset: Long,
            timestamp: Long,
            leaderEpoch: Int,
            buffer: ByteBuffer,
            snapshotHeaderRecord: SnapshotHeaderRecord,
        ): MemoryRecords {
            writeSnapshotHeaderRecord(
                buffer = buffer,
                initialOffset = initialOffset,
                timestamp = timestamp,
                leaderEpoch = leaderEpoch,
                snapshotHeaderRecord = snapshotHeaderRecord,
            )
            buffer.flip()
            return readableRecords(buffer)
        }

        private fun writeSnapshotHeaderRecord(
            buffer: ByteBuffer,
            initialOffset: Long,
            timestamp: Long,
            leaderEpoch: Int,
            snapshotHeaderRecord: SnapshotHeaderRecord,
        ) = MemoryRecordsBuilder(
            buffer = buffer,
            magic = RecordBatch.CURRENT_MAGIC_VALUE,
            compressionType = CompressionType.NONE,
            timestampType = TimestampType.CREATE_TIME,
            baseOffset = initialOffset,
            logAppendTime = timestamp,
            producerId = RecordBatch.NO_PRODUCER_ID,
            producerEpoch = RecordBatch.NO_PRODUCER_EPOCH,
            baseSequence = RecordBatch.NO_SEQUENCE,
            isTransactional = false,
            isControlBatch = true,
            partitionLeaderEpoch = leaderEpoch,
            writeLimit = buffer.capacity(),
        ).use { builder -> builder.appendSnapshotHeaderMessage(timestamp, snapshotHeaderRecord) }

        fun withSnapshotFooterRecord(
            initialOffset: Long,
            timestamp: Long,
            leaderEpoch: Int,
            buffer: ByteBuffer,
            snapshotFooterRecord: SnapshotFooterRecord
        ): MemoryRecords {
            writeSnapshotFooterRecord(
                buffer = buffer,
                initialOffset = initialOffset,
                timestamp = timestamp,
                leaderEpoch = leaderEpoch,
                snapshotFooterRecord = snapshotFooterRecord,
            )
            buffer.flip()
            return readableRecords(buffer)
        }

        private fun writeSnapshotFooterRecord(
            buffer: ByteBuffer,
            initialOffset: Long,
            timestamp: Long,
            leaderEpoch: Int,
            snapshotFooterRecord: SnapshotFooterRecord,
        ) = MemoryRecordsBuilder(
            buffer = buffer,
            magic = RecordBatch.CURRENT_MAGIC_VALUE,
            compressionType = CompressionType.NONE,
            timestampType = TimestampType.CREATE_TIME,
            baseOffset = initialOffset,
            logAppendTime = timestamp,
            producerId = RecordBatch.NO_PRODUCER_ID,
            producerEpoch = RecordBatch.NO_PRODUCER_EPOCH,
            baseSequence = RecordBatch.NO_SEQUENCE,
            isTransactional = false,
            isControlBatch = true,
            partitionLeaderEpoch = leaderEpoch,
            writeLimit = buffer.capacity(),
        ).use { builder -> builder.appendSnapshotFooterMessage(timestamp, snapshotFooterRecord) }
    }
}
