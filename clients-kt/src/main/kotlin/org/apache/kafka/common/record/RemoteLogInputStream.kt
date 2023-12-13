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
import java.io.InputStream
import java.nio.ByteBuffer
import org.apache.kafka.common.errors.CorruptRecordException
import org.apache.kafka.common.record.AbstractLegacyRecordBatch.ByteBufferLegacyRecordBatch
import org.apache.kafka.common.utils.Utils.readFully

class RemoteLogInputStream(
    private val inputStream: InputStream,
) : LogInputStream<RecordBatch> {

    // LogHeader buffer up to magic.
    private val logHeaderBuffer = ByteBuffer.allocate(Records.HEADER_SIZE_UP_TO_MAGIC)

    @Throws(IOException::class)
    override fun nextBatch(): RecordBatch? {
        logHeaderBuffer.clear()
        readFully(inputStream, logHeaderBuffer)
        if (logHeaderBuffer.position() < Records.HEADER_SIZE_UP_TO_MAGIC) return null
        logHeaderBuffer.rewind()
        val size = logHeaderBuffer.getInt(Records.SIZE_OFFSET)

        // V0 has the smallest overhead, stricter checking is done later
        if (size < LegacyRecord.RECORD_OVERHEAD_V0) throw CorruptRecordException(
            String.format(
                "Found record size %d smaller than minimum record overhead (%d).",
                size,
                LegacyRecord.RECORD_OVERHEAD_V0,
            )
        )

        // Total size is: "LOG_OVERHEAD + the size of the rest of the content"
        val bufferSize = Records.LOG_OVERHEAD + size

        // buffer contains the complete payload including header and records.
        val buffer = ByteBuffer.allocate(bufferSize)

        // write log header into buffer
        buffer.put(logHeaderBuffer)

        // write the records payload into the buffer
        readFully(inputStream, buffer)
        if (buffer.position() != bufferSize) return null
        buffer.rewind()

        val magic = logHeaderBuffer[Records.MAGIC_OFFSET]

        return if (magic > RecordBatch.MAGIC_VALUE_V1) DefaultRecordBatch(buffer)
        else ByteBufferLegacyRecordBatch(buffer)
    }
}
