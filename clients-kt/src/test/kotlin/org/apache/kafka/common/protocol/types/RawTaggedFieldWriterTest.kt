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

package org.apache.kafka.common.protocol.types

import java.nio.ByteBuffer
import org.apache.kafka.common.protocol.ByteBufferAccessor
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.fail

@Timeout(120)
class RawTaggedFieldWriterTest {

    @Test
    fun testWritingZeroRawTaggedFields() {
        val writer = RawTaggedFieldWriter.forFields(null)
        assertEquals(0, writer.numFields)
        val accessor = ByteBufferAccessor(ByteBuffer.allocate(0))
        writer.writeRawTags(accessor, Int.MAX_VALUE)
    }

    @Test
    fun testWritingSeveralRawTaggedFields() {
        val tags = listOf(
            RawTaggedField(tag = 2, data = byteArrayOf(0x1, 0x2, 0x3)),
            RawTaggedField(tag = 5, data = byteArrayOf(0x4, 0x5)),
        )
        val writer = RawTaggedFieldWriter.forFields(tags)
        assertEquals(2, writer.numFields)
        val arr = ByteArray(9)
        val accessor = ByteBufferAccessor(ByteBuffer.wrap(arr))
        writer.writeRawTags(accessor, 1)
        assertContentEquals(byteArrayOf(0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0), arr)
        writer.writeRawTags(accessor, 3)
        assertContentEquals(byteArrayOf(0x2, 0x3, 0x1, 0x2, 0x3, 0x0, 0x0, 0x0, 0x0), arr)
        writer.writeRawTags(accessor, 7)
        assertContentEquals(byteArrayOf(0x2, 0x3, 0x1, 0x2, 0x3, 0x5, 0x2, 0x4, 0x5), arr)
        writer.writeRawTags(accessor, Int.MAX_VALUE)
        assertContentEquals(byteArrayOf(0x2, 0x3, 0x1, 0x2, 0x3, 0x5, 0x2, 0x4, 0x5), arr)
    }

    @Test
    fun testInvalidNextDefinedTag() {
        val tags = listOf(
            RawTaggedField(tag = 2, data = byteArrayOf(0x1, 0x2, 0x3)),
            RawTaggedField(tag = 5, data = byteArrayOf(0x4, 0x5, 0x6)),
            RawTaggedField(tag = 7, data = byteArrayOf(0x0)),
        )
        val writer = RawTaggedFieldWriter.forFields(tags)
        assertEquals(3, writer.numFields)
        try {
            writer.writeRawTags(ByteBufferAccessor(ByteBuffer.allocate(1024)), 2)
            fail("expected to get RuntimeException")
        } catch (e: RuntimeException) {
            assertEquals("Attempted to use tag 2 as an undefined tag.", e.message)
        }
    }

    @Test
    fun testOutOfOrderTags() {
        val tags = listOf(
            RawTaggedField(tag = 5, data = byteArrayOf(0x4, 0x5, 0x6)),
            RawTaggedField(tag = 2, data = byteArrayOf(0x1, 0x2, 0x3)),
            RawTaggedField(tag = 7, data = byteArrayOf(0x0)),
        )
        val writer = RawTaggedFieldWriter.forFields(tags)
        assertEquals(3, writer.numFields)
        try {
            writer.writeRawTags(ByteBufferAccessor(ByteBuffer.allocate(1024)), 8)
            fail("expected to get RuntimeException")
        } catch (e: RuntimeException) {
            assertEquals(
                expected = "Invalid raw tag field list: tag 2 comes after tag 5, but is not higher than it.",
                actual = e.message,
            )
        }
    }
}
