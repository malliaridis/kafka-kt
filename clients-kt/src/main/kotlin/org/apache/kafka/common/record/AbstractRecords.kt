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

import java.nio.ByteBuffer
import org.apache.kafka.common.header.Header
import org.apache.kafka.common.utils.AbstractIterator
import org.apache.kafka.common.utils.Utils.wrapNullable

abstract class AbstractRecords : Records {

    private val records = Iterable { recordsIterator() }

    override fun hasMatchingMagic(magic: Byte): Boolean {
        return batches().none { batch -> batch.magic() != magic }
    }

    fun firstBatch(): RecordBatch? {
        val iterator = batches().iterator()
        return if (!iterator.hasNext()) null
        else iterator.next()
    }

    /**
     * Get an iterator over the deep records.
     * @return An iterator over the records
     */
    override fun records(): Iterable<Record> = records

    override fun toSend(): DefaultRecordsSend<Records> = DefaultRecordsSend(this)

    private fun recordsIterator(): Iterator<Record> {

        return object : AbstractIterator<Record>() {

            private val batches = batches().iterator()

            private var records: Iterator<Record>? = null

            override fun makeNext(): Record? {
                records?.let { if (it.hasNext()) return it.next() }

                return if (batches.hasNext()) {
                    records = batches.next().iterator()
                    makeNext()
                } else allDone()
            }
        }
    }

    companion object {

        fun estimateSizeInBytes(
            magic: Byte,
            baseOffset: Long,
            compressionType: CompressionType,
            records: Iterable<Record>,
        ): Int {
            var size = 0

            if (magic <= RecordBatch.MAGIC_VALUE_V1) {
                records.forEach { record ->
                    size += Records.LOG_OVERHEAD +
                            LegacyRecord.recordSize(magic, record.key(), record.value())
                }
            } else size = DefaultRecordBatch.sizeInBytes(baseOffset, records)

            return estimateCompressedSizeInBytes(size, compressionType)
        }

        fun estimateSizeInBytes(
            magic: Byte,
            compressionType: CompressionType,
            records: Iterable<SimpleRecord>
        ): Int {
            var size = 0

            if (magic <= RecordBatch.MAGIC_VALUE_V1) {

                records.forEach { record ->
                    size += Records.LOG_OVERHEAD + LegacyRecord.recordSize(
                        magic,
                        record.key(),
                        record.value()
                    )
                }
            } else size = DefaultRecordBatch.sizeInBytes(records)

            return estimateCompressedSizeInBytes(size, compressionType)
        }

        private fun estimateCompressedSizeInBytes(
            size: Int,
            compressionType: CompressionType
        ): Int {
            return if (compressionType === CompressionType.NONE) size
            else (size / 2).coerceAtLeast(1024)
                .coerceAtMost(1 shl 16)
        }

        /**
         * Get an upper bound estimate on the batch size needed to hold a record with the given
         * fields. This is only an estimate because it does not take into account overhead from the
         * compression algorithm.
         */
        fun estimateSizeInBytesUpperBound(
            magic: Byte,
            compressionType: CompressionType,
            key: ByteArray?,
            value: ByteArray?,
            headers: Array<Header>
        ): Int {
            return estimateSizeInBytesUpperBound(
                magic,
                compressionType,
                wrapNullable(key),
                wrapNullable(value),
                headers
            )
        }

        /**
         * Get an upper bound estimate on the batch size needed to hold a record with the given
         * fields. This is only an estimate because it does not take into account overhead from the
         * compression algorithm.
         */
        fun estimateSizeInBytesUpperBound(
            magic: Byte,
            compressionType: CompressionType,
            key: ByteBuffer?,
            value: ByteBuffer?,
            headers: Array<Header>
        ): Int {
            return if (magic >= RecordBatch.MAGIC_VALUE_V2)
                DefaultRecordBatch.estimateBatchSizeUpperBound(key, value, headers)
            else if (compressionType !== CompressionType.NONE) Records.LOG_OVERHEAD +
                    LegacyRecord.recordOverhead(magic) +
                    LegacyRecord.recordSize(magic, key, value)
            else Records.LOG_OVERHEAD + LegacyRecord.recordSize(magic, key, value)
        }

        /**
         * Return the size of the record batch header.
         *
         * For V0 and V1 with no compression, it's unclear if Records.LOG_OVERHEAD or 0 should be
         * chosen. There is no header per batch, but a sequence of batches is preceded by the offset
         * and size. This method returns `0` as it's what `MemoryRecordsBuilder` requires.
         */
        fun recordBatchHeaderSizeInBytes(magic: Byte, compressionType: CompressionType): Int {
            return if (magic > RecordBatch.MAGIC_VALUE_V1) DefaultRecordBatch.RECORD_BATCH_OVERHEAD
            else if (compressionType !== CompressionType.NONE)
                Records.LOG_OVERHEAD + LegacyRecord.recordOverhead(magic)
            else 0
        }
    }
}
