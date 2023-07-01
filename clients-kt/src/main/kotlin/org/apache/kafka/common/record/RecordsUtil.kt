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
import org.apache.kafka.common.errors.UnsupportedCompressionTypeException
import org.apache.kafka.common.utils.Time
import org.apache.kafka.common.utils.Utils.ensureCapacity

object RecordsUtil {

    /**
     * Down convert batches to the provided message format version. The first offset parameter is
     * only relevant in the conversion from uncompressed v2 or higher to v1 or lower. The reason is
     * that uncompressed records in v0 and v1 are not batched (put another way, each batch always
     * has 1 record).
     *
     * If a client requests records in v1 format starting from the middle of an uncompressed batch
     * in v2 format, we need to drop records from the batch during the conversion. Some versions of
     * librdkafka rely on this for correctness.
     *
     * The temporaryMemoryBytes computation assumes that the batches are not loaded into the heap
     * (via classes like FileChannelRecordBatch) before this method is called. This is the case in
     * the broker (we only load records into the heap when down converting), but it's not for the
     * producer. However, down converting in the producer is very uncommon and the extra complexity
     * to handle that case is not worth it.
     */
    internal fun downConvert(
        batches: Iterable<RecordBatch>,
        toMagic: Byte,
        firstOffset: Long,
        time: Time,
    ): ConvertedRecords<MemoryRecords> {
        // maintain the batch along with the decompressed records to avoid the need to decompress again
        val recordBatchAndRecordsList = mutableListOf<RecordBatchAndRecords>()
        var totalSizeEstimate = 0
        val startNanos = time.nanoseconds()

        batches.forEach { batch ->
            if (toMagic < RecordBatch.MAGIC_VALUE_V2) {
                if (batch.isControlBatch) return@forEach
                if (batch.compressionType() === CompressionType.ZSTD)
                    throw UnsupportedCompressionTypeException(
                        "Down-conversion of zstandard-compressed batches is not supported"
                    )
            }
            if (batch.magic() <= toMagic) {
                totalSizeEstimate += batch.sizeInBytes()
                recordBatchAndRecordsList.add(RecordBatchAndRecords(batch))
            } else {
                val records: MutableList<Record> = ArrayList()
                batch.forEach { record ->
                    // See the method javadoc for an explanation
                    if (toMagic > RecordBatch.MAGIC_VALUE_V1
                        || batch.isCompressed
                        || record.offset() >= firstOffset
                    ) records.add(record)
                }
                if (records.isEmpty()) return@forEach

                val baseOffset: Long =
                    if (batch.magic() >= RecordBatch.MAGIC_VALUE_V2
                        && toMagic >= RecordBatch.MAGIC_VALUE_V2
                    ) batch.baseOffset()
                    else records[0].offset()

                totalSizeEstimate += AbstractRecords.estimateSizeInBytes(
                    toMagic,
                    baseOffset,
                    batch.compressionType(),
                    records
                )
                recordBatchAndRecordsList.add(RecordBatchAndRecords(batch, records, baseOffset))
            }
        }

        var buffer = ByteBuffer.allocate(totalSizeEstimate)
        var temporaryMemoryBytes: Long = 0
        var numRecordsConverted = 0

        recordBatchAndRecordsList.forEach { recordBatchAndRecords ->

            temporaryMemoryBytes += recordBatchAndRecords.batch.sizeInBytes().toLong()
            if (recordBatchAndRecords.batch.magic() <= toMagic) {
                buffer = ensureCapacity(
                    buffer,
                    buffer.position() + recordBatchAndRecords.batch.sizeInBytes()
                )
                recordBatchAndRecords.batch.writeTo(buffer)
            } else {
                val builder = convertRecordBatch(toMagic, buffer, recordBatchAndRecords)
                buffer = builder.buffer()
                temporaryMemoryBytes += builder.uncompressedBytesWritten().toLong()
                numRecordsConverted += builder.numRecords()
            }
        }
        buffer.flip()
        val stats = RecordConversionStats(
            temporaryMemoryBytes, numRecordsConverted,
            time.nanoseconds() - startNanos
        )

        return ConvertedRecords(MemoryRecords.readableRecords(buffer), stats)
    }

    /**
     * Return a buffer containing the converted record batches. The returned buffer may not be the same as the received
     * one (e.g. it may require expansion).
     */
    private fun convertRecordBatch(
        magic: Byte,
        buffer: ByteBuffer,
        recordBatchAndRecords: RecordBatchAndRecords
    ): MemoryRecordsBuilder {
        val batch = recordBatchAndRecords.batch
        val timestampType = batch.timestampType()

        val logAppendTime =
            if (timestampType == TimestampType.LOG_APPEND_TIME) batch.maxTimestamp()
            else RecordBatch.NO_TIMESTAMP

        val builder = MemoryRecords.builder(
            buffer, magic, batch.compressionType(),
            timestampType, recordBatchAndRecords.baseOffset!!, logAppendTime
        )

        recordBatchAndRecords.records.forEach { record ->
            // Down-convert this record. Ignore headers when down-converting to V0 and V1 since they are not supported
            if (magic > RecordBatch.MAGIC_VALUE_V1) builder.append(record)
            else builder.appendWithOffset(
                record.offset(),
                record.timestamp(),
                record.key(),
                record.value()
            )
        }

        builder.close()
        return builder
    }

    private data class RecordBatchAndRecords(
        val batch: RecordBatch,
        val records: List<Record> = emptyList(),
        val baseOffset: Long? = null
    )
}
