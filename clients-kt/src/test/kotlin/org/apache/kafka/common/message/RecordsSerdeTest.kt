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

package org.apache.kafka.common.message

import org.apache.kafka.common.protocol.ByteBufferAccessor
import org.apache.kafka.common.protocol.MessageUtil.toByteBuffer
import org.apache.kafka.common.record.CompressionType
import org.apache.kafka.common.record.MemoryRecords
import org.apache.kafka.common.record.SimpleRecord
import org.junit.jupiter.api.Test
import java.nio.ByteBuffer
import kotlin.test.assertEquals
import kotlin.test.assertNull

class RecordsSerdeTest {

    @Test
    @Throws(Exception::class)
    fun testSerdeRecords() {
        val records = MemoryRecords.withRecords(
            compressionType = CompressionType.NONE,
            records = arrayOf(
                SimpleRecord(value = "foo".toByteArray()),
                SimpleRecord(value = "bar".toByteArray()),
            ),
        )
        val message = SimpleRecordsMessageData()
            .setTopic("foo")
            .setRecordSet(records)
        testAllRoundTrips(message)
    }

    @Test
    @Throws(Exception::class)
    fun testSerdeNullRecords() {
        val message = SimpleRecordsMessageData().setTopic("foo")
        assertNull(message.recordSet)
        testAllRoundTrips(message)
    }

    @Test
    @Throws(Exception::class)
    fun testSerdeEmptyRecords() {
        val message = SimpleRecordsMessageData()
            .setTopic("foo")
            .setRecordSet(MemoryRecords.EMPTY)
        testAllRoundTrips(message)
    }

    @Throws(Exception::class)
    private fun testAllRoundTrips(message: SimpleRecordsMessageData) {
        with(SimpleRecordsMessageData) {
            for (version in LOWEST_SUPPORTED_VERSION..HIGHEST_SUPPORTED_VERSION)
                testRoundTrip(message, version.toShort())
        }
    }

    private fun testRoundTrip(message: SimpleRecordsMessageData, version: Short) {
        val buf = toByteBuffer(message, version)
        val message2 = deserialize(buf.duplicate(), version)
        assertEquals(message, message2)
        assertEquals(message.hashCode(), message2.hashCode())
    }

    private fun deserialize(buffer: ByteBuffer, version: Short): SimpleRecordsMessageData {
        val readable = ByteBufferAccessor(buffer)
        return SimpleRecordsMessageData(readable, version)
    }
}
