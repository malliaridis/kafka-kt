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

import org.apache.kafka.common.errors.CorruptRecordException
import org.apache.kafka.common.record.AbstractLegacyRecordBatch.ByteBufferLegacyRecordBatch
import java.nio.ByteBuffer

/**
 * A byte buffer backed log input stream. This class avoids the need to copy records by returning
 * slices from the underlying byte buffer.
 */
internal class ByteBufferLogInputStream(
    private val buffer: ByteBuffer,
    private val maxMessageSize: Int,
) : LogInputStream<MutableRecordBatch> {

    override fun nextBatch(): MutableRecordBatch? {
        val remaining = buffer.remaining()
        val batchSize = nextBatchSize()
        if (batchSize == null || remaining < batchSize) return null

        val magic = buffer[buffer.position() + Records.MAGIC_OFFSET]
        val batchSlice = buffer.slice()
        batchSlice.limit(batchSize)
        buffer.position(buffer.position() + batchSize)

        return if (magic > RecordBatch.MAGIC_VALUE_V1) DefaultRecordBatch(batchSlice)
        else ByteBufferLegacyRecordBatch(batchSlice)
    }

    /**
     * Validates the header of the next batch and returns batch size.
     *
     * @return next batch size including LOG_OVERHEAD if buffer contains header up to
     * magic byte, null otherwise
     * @throws CorruptRecordException if record size or magic is invalid
     */
    @Throws(CorruptRecordException::class)
    fun nextBatchSize(): Int? {
        val remaining = buffer.remaining()
        if (remaining < Records.LOG_OVERHEAD) return null
        val recordSize = buffer.getInt(buffer.position() + Records.SIZE_OFFSET)
        // V0 has the smallest overhead, stricter checking is done later
        if (recordSize < LegacyRecord.RECORD_OVERHEAD_V0) throw CorruptRecordException(
            String.format(
                "Record size %d is less than the minimum record overhead (%d)",
                recordSize,
                LegacyRecord.RECORD_OVERHEAD_V0
            )
        )
        if (recordSize > maxMessageSize) throw CorruptRecordException(
            String.format(
                "Record size %d exceeds the largest allowable message size (%d).",
                recordSize,
                maxMessageSize
            )
        )
        if (remaining < Records.HEADER_SIZE_UP_TO_MAGIC) return null
        val magic = buffer[buffer.position() + Records.MAGIC_OFFSET]
        if (magic < 0 || magic > RecordBatch.CURRENT_MAGIC_VALUE)
            throw CorruptRecordException("Invalid magic found in record: $magic")

        return recordSize + Records.LOG_OVERHEAD
    }
}
