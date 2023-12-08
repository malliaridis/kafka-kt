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
import org.apache.kafka.common.requests.ByteBufferChannel
import org.apache.kafka.test.TestUtils.tempFile
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class UnalignedFileRecordsTest {
    
    private val values = arrayOf("foo".toByteArray(), "bar".toByteArray())
    
    private lateinit var fileRecords: FileRecords
    
    @BeforeEach
    @Throws(IOException::class)
    fun setup() {
        fileRecords = createFileRecords(values)
    }

    @AfterEach
    @Throws(IOException::class)
    fun cleanup() {
        fileRecords.close()
    }

    @Test
    @Throws(IOException::class)
    fun testWriteTo() {
        val channel = ByteBufferChannel(fileRecords.sizeInBytes().toLong())
        val size = fileRecords.sizeInBytes()
        val records1 = fileRecords.sliceUnaligned(0, size / 2)
        val records2 = fileRecords.sliceUnaligned(size / 2, size - size / 2)
        records1.writeTo(
            destChannel = channel,
            previouslyWritten = 0,
            remaining = records1.sizeInBytes(),
        )
        records2.writeTo(
            destChannel = channel,
            previouslyWritten = 0,
            remaining = records2.sizeInBytes(),
        )
        channel.close()
        val records = MemoryRecords.readableRecords(channel.buffer()).records().iterator()
        for (value in values) {
            assertTrue(records.hasNext())
            assertEquals(records.next().value(), ByteBuffer.wrap(value))
        }
    }

    @Throws(IOException::class)
    private fun createFileRecords(values: Array<ByteArray>): FileRecords {
        val fileRecords = FileRecords.open(tempFile())
        for (value in values) {
            fileRecords.append(
                MemoryRecords.withRecords(
                    compressionType = CompressionType.NONE,
                    records = arrayOf(SimpleRecord(value = value)),
                )
            )
        }
        return fileRecords
    }
}
