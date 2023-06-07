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

import java.io.IOException
import java.nio.ByteBuffer
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.errors.CorruptRecordException
import org.apache.kafka.common.record.AbstractLegacyRecordBatch.LegacyFileChannelRecordBatch
import org.apache.kafka.common.record.DefaultRecordBatch.DefaultFileChannelRecordBatch
import org.apache.kafka.common.record.FileLogInputStream.FileChannelRecordBatch
import org.apache.kafka.common.utils.BufferSupplier
import org.apache.kafka.common.utils.CloseableIterator
import org.apache.kafka.common.utils.Utils.readFully
import org.apache.kafka.common.utils.Utils.readFullyOrFail

/**
 * A log input stream which is backed by a [java.nio.channels.FileChannel].
 *
 * @property records Underlying FileRecords instance
 * @property start Position in the file channel to start from
 * @property end Position in the file channel not to read past
 */
class FileLogInputStream internal constructor(
    private val records: FileRecords,
    private var start: Int,
    private val end: Int
) : LogInputStream<FileChannelRecordBatch> {

    private val logHeaderBuffer = ByteBuffer.allocate(Records.HEADER_SIZE_UP_TO_MAGIC)

    @Throws(IOException::class)
    override fun nextBatch(): FileChannelRecordBatch? {
        val channel = records.channel()

        if (start >= end - Records.HEADER_SIZE_UP_TO_MAGIC) return null

        logHeaderBuffer.rewind()
        readFullyOrFail(channel, logHeaderBuffer, start.toLong(), "log header")
        logHeaderBuffer.rewind()

        val offset = logHeaderBuffer.getLong(Records.OFFSET_OFFSET)
        val size = logHeaderBuffer.getInt(Records.SIZE_OFFSET)

        // V0 has the smallest overhead, stricter checking is done later
        if (size < LegacyRecord.RECORD_OVERHEAD_V0) throw CorruptRecordException(
                "Found record size $size smaller than minimum record overhead (" +
                        "${LegacyRecord.RECORD_OVERHEAD_V0}) in file ${records.file()}."
        )

        if (start > end - Records.LOG_OVERHEAD - size) return null

        val magic = logHeaderBuffer[Records.MAGIC_OFFSET]
        val batch: FileChannelRecordBatch =
            if (magic < RecordBatch.MAGIC_VALUE_V2)
                LegacyFileChannelRecordBatch(offset, magic, records, start, size)
            else DefaultFileChannelRecordBatch(offset, magic, records, start, size)

        start += batch.sizeInBytes()
        return batch
    }

    /**
     * Log entry backed by an underlying FileChannel. This allows iteration over the record batches
     * without needing to read the record data into memory until it is needed. The downside
     * is that entries will generally no longer be readable when the underlying channel is closed.
     */
    abstract class FileChannelRecordBatch internal constructor(
        protected val offset: Long,
        protected val magic: Byte,
        protected val fileRecords: FileRecords?,
        val position: Int,
        protected val batchSize: Int
    ) : AbstractRecordBatch() {

        private var fullBatch: RecordBatch? = null

        private var batchHeader: RecordBatch? = null

        override fun compressionType(): CompressionType = loadBatchHeader().compressionType()

        override fun timestampType(): TimestampType? = loadBatchHeader().timestampType()

        override fun checksum(): Long = loadBatchHeader().checksum()

        override fun maxTimestamp(): Long = loadBatchHeader().maxTimestamp()

        @Deprecated(
            message = "Use property instead.",
            replaceWith = ReplaceWith("position")
        )
        fun position(): Int = position

        override fun magic(): Byte = magic

        override fun iterator(): Iterator<Record> = loadFullBatch().iterator()

        override fun streamingIterator(
            decompressionBufferSupplier: BufferSupplier,
        ): CloseableIterator<Record> {
            return loadFullBatch().streamingIterator(decompressionBufferSupplier)
        }

        override val isValid: Boolean
            get() = loadFullBatch().isValid

        override fun ensureValid() = loadFullBatch().ensureValid()

        override fun sizeInBytes(): Int {
            return Records.LOG_OVERHEAD + batchSize
        }

        override fun writeTo(buffer: ByteBuffer) {
            val channel = fileRecords!!.channel()
            try {
                val limit = buffer.limit()
                buffer.limit(buffer.position() + sizeInBytes())
                readFully(channel, buffer, position.toLong())
                buffer.limit(limit)
            } catch (e: IOException) {
                throw KafkaException(
                    "Failed to read record batch at position $position from $fileRecords",
                    e
                )
            }
        }

        protected abstract fun toMemoryRecordBatch(buffer: ByteBuffer?): RecordBatch

        protected abstract fun headerSize(): Int

        protected fun loadFullBatch(): RecordBatch {
            return  fullBatch ?: run {
                batchHeader = null
                loadBatchWithSize(sizeInBytes(), "full record batch")
                    .also { fullBatch = it }
            }
        }

        protected fun loadBatchHeader(): RecordBatch {
            return fullBatch ?: batchHeader ?: run {
                loadBatchWithSize(headerSize(), "record batch header")
                    .also { batchHeader = it }
            }
        }

        private fun loadBatchWithSize(size: Int, description: String): RecordBatch {
            val channel = fileRecords!!.channel()
            return try {
                val buffer = ByteBuffer.allocate(size)
                readFullyOrFail(channel, buffer, position.toLong(), description)
                buffer.rewind()
                toMemoryRecordBatch(buffer)
            } catch (e: IOException) {
                throw KafkaException(
                    "Failed to load record batch at position $position from $fileRecords",
                    e
                )
            }
        }

        override fun equals(other: Any?): Boolean {
            if (this === other) return true
            if (other == null || javaClass != other.javaClass) return false
            val that = other as FileChannelRecordBatch
            val channel = fileRecords?.channel()
            val thatChannel = if (that.fileRecords == null) null else that.fileRecords.channel()
            return offset == that.offset
                    && position == that.position
                    && batchSize == that.batchSize
                    && channel == thatChannel
        }

        override fun hashCode(): Int {
            val channel = fileRecords?.channel()
            var result = java.lang.Long.hashCode(offset)
            result = 31 * result + (channel?.hashCode() ?: 0)
            result = 31 * result + position
            result = 31 * result + batchSize
            return result
        }

        override fun toString(): String {
            return "FileChannelRecordBatch(magic: $magic" +
                    ", offset: $offset" +
                    ", size: $batchSize)"
        }
    }
}
