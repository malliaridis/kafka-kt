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

import java.nio.ByteBuffer
import org.apache.kafka.common.message.NullableStructMessageData.MyStruct
import org.apache.kafka.common.message.NullableStructMessageData.MyStruct2
import org.apache.kafka.common.message.NullableStructMessageData.MyStruct3
import org.apache.kafka.common.message.NullableStructMessageData.MyStruct4
import org.apache.kafka.common.protocol.ByteBufferAccessor
import org.apache.kafka.common.protocol.MessageUtil.toByteBuffer
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull

class NullableStructMessageTest {

    @Test
    fun testDefaultValues() {
        var message = NullableStructMessageData()
        assertNull(message.nullableStruct)
        assertEquals(MyStruct2(), message.nullableStruct2)
        assertNull(message.nullableStruct3)
        assertEquals(MyStruct4(), message.nullableStruct4)

        message = roundTrip(message, 2.toShort())
        assertNull(message.nullableStruct)
        assertEquals(MyStruct2(), message.nullableStruct2)
        assertNull(message.nullableStruct3)
        assertEquals(MyStruct4(), message.nullableStruct4)
    }

    @Test
    fun testRoundTrip() {
        val message = NullableStructMessageData()
            .setNullableStruct(
                MyStruct()
                    .setMyInt(1)
                    .setMyString("1")
            )
            .setNullableStruct2(
                MyStruct2()
                    .setMyInt(2)
                    .setMyString("2")
            )
            .setNullableStruct3(
                MyStruct3()
                    .setMyInt(3)
                    .setMyString("3")
            )
            .setNullableStruct4(
                MyStruct4()
                    .setMyInt(4)
                    .setMyString("4")
            )

        val newMessage = roundTrip(message = message, version = 2)
        assertEquals(message, newMessage)
    }

    @Test
    fun testNullForAllFields() {
        var message = NullableStructMessageData()
            .setNullableStruct(null)
            .setNullableStruct2(null)
            .setNullableStruct3(null)
            .setNullableStruct4(null)
        message = roundTrip(message, 2.toShort())
        assertNull(message.nullableStruct)
        assertNull(message.nullableStruct2)
        assertNull(message.nullableStruct3)
        assertNull(message.nullableStruct4)
    }

    @Test
    fun testNullableStruct2CanNotBeNullInVersion0() {
        val message = NullableStructMessageData().setNullableStruct2(null)
        assertFailsWith<NullPointerException> { roundTrip(message = message, version = 0) }
    }

    @Test
    fun testToStringWithNullStructs() {
        val message = NullableStructMessageData()
            .setNullableStruct(null)
            .setNullableStruct2(null)
            .setNullableStruct3(null)
            .setNullableStruct4(null)
        message.toString()
    }

    private fun deserialize(buf: ByteBuffer, version: Short): NullableStructMessageData {
        val message = NullableStructMessageData()
        message.read(ByteBufferAccessor(buf.duplicate()), version)
        return message
    }

    private fun serialize(message: NullableStructMessageData, version: Short): ByteBuffer {
        return toByteBuffer(message, version)
    }

    private fun roundTrip(message: NullableStructMessageData, version: Short): NullableStructMessageData {
        val buffer = serialize(message, version)
        return deserialize(buffer.duplicate(), version)
    }
}
