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
import java.nio.ByteBuffer
import org.apache.kafka.common.InvalidRecordException
import org.apache.kafka.common.errors.CorruptRecordException
import org.apache.kafka.common.utils.ByteBufferOutputStream
import org.apache.kafka.common.utils.Utils
import org.junit.jupiter.api.Test
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse

class SimpleLegacyRecordTest {

    @Test
    @Throws(Exception::class)
    fun testCompressedIterationWithNullValue() {
        val buffer = ByteBuffer.allocate(128)
        val out = DataOutputStream(ByteBufferOutputStream(buffer))
        AbstractLegacyRecordBatch.writeHeader(
            out = out,
            offset = 0L,
            size = LegacyRecord.RECORD_OVERHEAD_V1,
        )
        LegacyRecord.write(
            out = out,
            magic = RecordBatch.MAGIC_VALUE_V1,
            timestamp = 1L,
            key = null as ByteArray?,
            value = null,
            compressionType = CompressionType.GZIP,
            timestampType = TimestampType.CREATE_TIME,
        )
        buffer.flip()
        val records = MemoryRecords.readableRecords(buffer)
        assertFailsWith<InvalidRecordException> { records.records().iterator().hasNext() }
    }

    @Test
    @Throws(Exception::class)
    fun testCompressedIterationWithEmptyRecords() {
        val emptyCompressedValue = ByteBuffer.allocate(64)
        val gzipOutput = CompressionType.GZIP.wrapForOutput(
            bufferStream = ByteBufferOutputStream(emptyCompressedValue),
            messageVersion = RecordBatch.MAGIC_VALUE_V1,
        )
        gzipOutput.close()
        emptyCompressedValue.flip()
        val buffer = ByteBuffer.allocate(128)
        val out = DataOutputStream(ByteBufferOutputStream(buffer))
        AbstractLegacyRecordBatch.writeHeader(
            out = out,
            offset = 0L,
            size = LegacyRecord.RECORD_OVERHEAD_V1 + emptyCompressedValue.remaining(),
        )
        LegacyRecord.write(
            out = out,
            magic = RecordBatch.MAGIC_VALUE_V1,
            timestamp = 1L,
            key = null,
            value = Utils.toArray(emptyCompressedValue),
            compressionType = CompressionType.GZIP,
            timestampType = TimestampType.CREATE_TIME,
        )
        buffer.flip()
        val records = MemoryRecords.readableRecords(buffer)
        assertFailsWith<InvalidRecordException> { records.records().iterator().hasNext() }
    }

    /**
     * This scenario can happen if the record size field is corrupt and we end up allocating a buffer that is too small
     */
    @Test
    fun testIsValidWithTooSmallBuffer() {
        val buffer = ByteBuffer.allocate(2)
        val record = LegacyRecord(buffer)
        assertFalse(record.isValid)
        assertFailsWith<CorruptRecordException> { record.ensureValid() }
    }

    @Test
    fun testIsValidWithChecksumMismatch() {
        val buffer = ByteBuffer.allocate(4)
        // set checksum
        buffer.putInt(2)
        val record = LegacyRecord(buffer)
        assertFalse(record.isValid)
        assertFailsWith<CorruptRecordException> { record.ensureValid() }
    }
}
