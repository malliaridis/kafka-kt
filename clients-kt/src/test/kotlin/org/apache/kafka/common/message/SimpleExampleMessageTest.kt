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
import java.util.function.Consumer
import org.apache.kafka.common.Uuid
import org.apache.kafka.common.errors.UnsupportedVersionException
import org.apache.kafka.common.message.SimpleExampleMessageData.MyStruct
import org.apache.kafka.common.message.SimpleExampleMessageData.TaggedStruct
import org.apache.kafka.common.message.SimpleExampleMessageData.TestCommonStruct
import org.apache.kafka.common.message.SimpleExampleMessageDataJsonConverter.read
import org.apache.kafka.common.message.SimpleExampleMessageDataJsonConverter.write
import org.apache.kafka.common.protocol.ByteBufferAccessor
import org.apache.kafka.common.protocol.MessageUtil.toByteBuffer
import org.apache.kafka.common.protocol.ObjectSerializationCache
import org.apache.kafka.common.utils.ByteUtils
import org.junit.jupiter.api.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull

class SimpleExampleMessageTest {
    @Test
    fun shouldStoreField() {
        val uuid = Uuid.randomUuid()
        val buf = ByteBuffer.wrap(byteArrayOf(1, 2, 3))
        
        val out = SimpleExampleMessageData()
        out.setProcessId(uuid)
        out.setZeroCopyByteBuffer(buf)
        
        assertEquals(uuid, out.processId)
        assertEquals(buf, out.zeroCopyByteBuffer)
        
        out.setNullableZeroCopyByteBuffer(null)
        assertNull(out.nullableZeroCopyByteBuffer)
        out.setNullableZeroCopyByteBuffer(buf)
        assertEquals(buf, out.nullableZeroCopyByteBuffer)
    }

    @Test
    fun shouldThrowIfCannotWriteNonIgnorableField() {
        // processId is not supported in v0 and is not marked as ignorable
        val out = SimpleExampleMessageData().setProcessId(Uuid.randomUuid())
        assertFailsWith<UnsupportedVersionException> {
            out.write(
                writable = ByteBufferAccessor(ByteBuffer.allocate(64)),
                cache = ObjectSerializationCache(),
                version = 0,
            )
        }
    }

    @Test
    fun shouldDefaultField() {
        val out = SimpleExampleMessageData()
        assertEquals(Uuid.fromString("AAAAAAAAAAAAAAAAAAAAAA"), out.processId)
        assertEquals(ByteUtils.EMPTY_BUF, out.zeroCopyByteBuffer)
        assertEquals(ByteUtils.EMPTY_BUF, out.nullableZeroCopyByteBuffer)
    }

    @Test
    fun shouldRoundTripFieldThroughBuffer() {
        val uuid = Uuid.randomUuid()
        val buf = ByteBuffer.wrap(byteArrayOf(1, 2, 3))
        val outputData = SimpleExampleMessageData()
        outputData.setProcessId(uuid)
        outputData.setZeroCopyByteBuffer(buf)
        
        val buffer = toByteBuffer(outputData, 1.toShort())
        
        val inputData = SimpleExampleMessageData()
        inputData.read(ByteBufferAccessor(buffer), 1.toShort())
        
        buf.rewind()
        
        assertEquals(uuid, inputData.processId)
        assertEquals(buf, inputData.zeroCopyByteBuffer)
        assertEquals(ByteUtils.EMPTY_BUF, inputData.nullableZeroCopyByteBuffer)
    }

    @Test
    fun shouldRoundTripFieldThroughBufferWithNullable() {
        val uuid = Uuid.randomUuid()
        val buf1 = ByteBuffer.wrap(byteArrayOf(1, 2, 3))
        val buf2 = ByteBuffer.wrap(byteArrayOf(4, 5, 6))
        val outputData = SimpleExampleMessageData()
        outputData.setProcessId(uuid)
        outputData.setZeroCopyByteBuffer(buf1)
        outputData.setNullableZeroCopyByteBuffer(buf2)
        val buffer = toByteBuffer(outputData, 1.toShort())
        val inputData = SimpleExampleMessageData()
        inputData.read(ByteBufferAccessor(buffer), 1.toShort())
        buf1.rewind()
        buf2.rewind()
        assertEquals(uuid, inputData.processId)
        assertEquals(buf1, inputData.zeroCopyByteBuffer)
        assertEquals(buf2, inputData.nullableZeroCopyByteBuffer)
    }

    @Test
    fun shouldImplementEqualsAndHashCode() {
        val uuid = Uuid.randomUuid()
        val buf = ByteBuffer.wrap(byteArrayOf(1, 2, 3))
        val a = SimpleExampleMessageData()
        a.setProcessId(uuid)
        a.setZeroCopyByteBuffer(buf)
        
        val b = SimpleExampleMessageData()
        b.setProcessId(uuid)
        b.setZeroCopyByteBuffer(buf)
        
        assertEquals(a, b)
        assertEquals(a.hashCode(), b.hashCode())
        // just tagging this on here
        assertEquals(a.toString(), b.toString())
        
        a.setNullableZeroCopyByteBuffer(buf)
        b.setNullableZeroCopyByteBuffer(buf)
        
        assertEquals(a, b)
        assertEquals(a.hashCode(), b.hashCode())
        assertEquals(a.toString(), b.toString())
        
        a.setNullableZeroCopyByteBuffer(null)
        b.setNullableZeroCopyByteBuffer(null)
        
        assertEquals(a, b)
        assertEquals(a.hashCode(), b.hashCode())
        assertEquals(a.toString(), b.toString())
    }

    @Test
    fun testMyTaggedIntArray() {
        // Verify that the tagged int array reads as empty when not set.
        testRoundTrip(
            message = SimpleExampleMessageData(),
            validator = { message -> assertContentEquals(intArrayOf(), message.myTaggedIntArray) },
        )

        // Verify that we can set a tagged array of ints.
        testRoundTrip(
            message = SimpleExampleMessageData().setMyTaggedIntArray(intArrayOf(1, 2, 3)),
            validator = { message -> assertContentEquals(intArrayOf(1, 2, 3), message.myTaggedIntArray) },
        )
    }

    @Test
    fun testMyNullableString() {
        // Verify that the tagged field reads as null when not set.
        testRoundTrip(
            message = SimpleExampleMessageData(),
            validator = { message -> assertNull(message.myNullableString) },
        )

        // Verify that we can set and retrieve a string for the tagged field.
        testRoundTrip(
            message = SimpleExampleMessageData().setMyNullableString("foobar"),
            validator = { message -> assertEquals("foobar", message.myNullableString) },
        )
    }

    @Test
    fun testMyInt16() {
        // Verify that the tagged field reads as 123 when not set.
        testRoundTrip(
            message = SimpleExampleMessageData(),
            validator = { message -> assertEquals(123, message.myInt16) },
        )
        testRoundTrip(
            message = SimpleExampleMessageData().setMyInt16(456),
            validator = { message -> assertEquals(456, message.myInt16) },
        )
    }

    @Test
    fun testMyUint32() {
        // Verify that the uint16 field reads as 33000 when not set.
        testRoundTrip(
            message = SimpleExampleMessageData(),
            validator = { message -> assertEquals(1234567u, message.myUint32) },
        )
        testRoundTrip(
            message = SimpleExampleMessageData().setMyUint32(123u),
            validator = { message -> assertEquals(123u, message.myUint32) },
        )
        testRoundTrip(
            message = SimpleExampleMessageData().setMyUint32(60000u),
            validator = { message -> assertEquals(60000u, message.myUint32) },
        )
    }

    @Test
    fun testMyUint16() {
        // Verify that the uint16 field reads as 33000 when not set.
        testRoundTrip(
            message = SimpleExampleMessageData(),
            validator = { message -> assertEquals(33000u, message.myUint16) },
        )
        testRoundTrip(
            message = SimpleExampleMessageData().setMyUint16(123u),
            validator = { message -> assertEquals(123u, message.myUint16) },
        )
        testRoundTrip(
            message = SimpleExampleMessageData().setMyUint16(60000u),
            validator = { message -> assertEquals(60000u, message.myUint16) },
        )
    }

    @Test
    fun testMyString() {
        // Verify that the tagged field reads as empty when not set.
        testRoundTrip(
            message = SimpleExampleMessageData(),
            validator = { message -> assertEquals("", message.myString) },
        )
        testRoundTrip(
            message = SimpleExampleMessageData().setMyString("abc"),
            validator = { message -> assertEquals("abc", message.myString) },
        )
    }

    @Test
    fun testMyBytes() {
        // Kotlin Migration: Unsigned numerical values are well-defined in Kotlin and cannot be passed as negative
        // values without parsing
        // assertFailsWith<RuntimeException> { SimpleExampleMessageData().setMyUint16(-1) }
        // assertFailsWith<RuntimeException> { SimpleExampleMessageData().setMyUint16(UNSIGNED_SHORT_MAX + 1) }
        // assertFailsWith<RuntimeException> { SimpleExampleMessageData().setMyUint32(-1) }
        // assertFailsWith<RuntimeException> { SimpleExampleMessageData().setMyUint32(UInt.MAX_VALUE + 1) }

        // Verify that the tagged field reads as empty when not set.
        testRoundTrip(
            message = SimpleExampleMessageData(),
            validator = { message -> assertContentEquals(byteArrayOf(), message.myBytes) },
        )
        testRoundTrip(
            message = SimpleExampleMessageData().setMyBytes(byteArrayOf(0x43, 0x66)),
            validator = { message -> assertContentEquals(byteArrayOf(0x43, 0x66), message.myBytes) },
        )
        testRoundTrip(
            message = SimpleExampleMessageData().setMyBytes(null),
            validator = { message -> assertNull(message.myBytes) },
        )
    }

    @Test
    fun testTaggedUuid() {
        testRoundTrip(
            message = SimpleExampleMessageData(),
            validator = { message -> assertEquals(Uuid.fromString("H3KKO4NTRPaCWtEmm3vW7A"), message.taggedUuid) },
        )
        val randomUuid = Uuid.randomUuid()
        testRoundTrip(
            message = SimpleExampleMessageData().setTaggedUuid(randomUuid),
            validator = { message -> assertEquals(randomUuid, message.taggedUuid) },
        )
    }

    @Test
    fun testTaggedLong() {
        testRoundTrip(
            message = SimpleExampleMessageData(),
            validator = { message -> assertEquals(0xcafcacafcacafcaL, message.taggedLong) },
        )
        testRoundTrip(
            message = SimpleExampleMessageData()
                .setMyString("blah")
                .setMyTaggedIntArray(intArrayOf(4))
                .setTaggedLong(0x123443211234432L),
            validator = { message -> assertEquals(0x123443211234432L, message.taggedLong) },
        )
    }

    @Test
    fun testMyStruct() {
        // Verify that we can set and retrieve a nullable struct object.
        val myStruct = MyStruct()
            .setStructId(10)
            .setArrayInStruct(listOf(SimpleExampleMessageData.StructArray().setArrayFieldId(20)))
        testRoundTrip(
            message = SimpleExampleMessageData().setMyStruct(myStruct),
            validator = { message -> assertEquals(myStruct, message.myStruct)},
            version = 2,
        )
    }

    @Test
    fun testMyStructUnsupportedVersion() {
        val myStruct = MyStruct().setStructId(10)
        // Check serialization throws exception for unsupported version
        assertFailsWith<UnsupportedVersionException> {
            testRoundTrip(
                message = SimpleExampleMessageData().setMyStruct(myStruct),
                version = 1
            )
        }
    }

    /**
     * Check following cases:
     * 1. Tagged struct can be serialized/deserialized for version it is supported
     * 2. Tagged struct doesn't matter for versions it is not declared.
     */
    @Test
    fun testMyTaggedStruct() {
        // Verify that we can set and retrieve a nullable struct object.
        val myStruct = TaggedStruct().setStructId("abc")
        testRoundTrip(
            message = SimpleExampleMessageData().setMyTaggedStruct(myStruct),
            validator = { message -> assertEquals(myStruct, message.myTaggedStruct) },
            version = 2,
        )

        // Not setting field works for both version 1 and version 2 protocol
        testRoundTrip(
            message = SimpleExampleMessageData().setMyString("abc"),
            validator = { message -> assertEquals("abc", message.myString)},
            version = 1,
        )
        testRoundTrip(
            message = SimpleExampleMessageData().setMyString("abc"),
            validator = { message -> assertEquals("abc", message.myString) },
            version = 2,
        )
    }

    @Test
    fun testCommonStruct() {
        val message = SimpleExampleMessageData()
        message.setMyCommonStruct(
            TestCommonStruct()
                .setFoo(1)
                .setBar(2)
        )
        message.setMyOtherCommonStruct(
            TestCommonStruct()
                .setFoo(3)
                .setBar(4)
        )
        testRoundTrip(message, 2)
    }

    private fun deserialize(buf: ByteBuffer, version: Short): SimpleExampleMessageData {
        val message = SimpleExampleMessageData()
        message.read(ByteBufferAccessor(buf.duplicate()), version)
        return message
    }

    private fun testRoundTrip(message: SimpleExampleMessageData, version: Short) {
        testRoundTrip(
            message = message,
            validator = { },
            version = version,
        )
    }

    private fun testRoundTrip(
        message: SimpleExampleMessageData,
        validator: Consumer<SimpleExampleMessageData>,
        version: Short = 1.toShort(),
    ) {
        validator.accept(message)
        val message2 = roundTripSerde(message, version)
        validator.accept(message2)
        assertEquals(message, message2)
        assertEquals(message.hashCode(), message2.hashCode())

        // Check JSON serialization
        val serializedJson = write(message, version)
        val messageFromJson = read(serializedJson, version)
        validator.accept(messageFromJson)
        assertEquals(message, messageFromJson)
        assertEquals(message.hashCode(), messageFromJson.hashCode())
    }

    private fun roundTripSerde(
        message: SimpleExampleMessageData,
        version: Short,
    ): SimpleExampleMessageData {
        val buf = toByteBuffer(message, version)
        return deserialize(buf.duplicate(), version)
    }

    @Test
    fun testTaggedFieldsShouldSupportFlexibleVersionSubset() {
        val message = SimpleExampleMessageData().setTaggedLongFlexibleVersionSubset(15L)
        testRoundTrip(
            message = message,
            validator = { msg -> assertEquals(15, msg.taggedLongFlexibleVersionSubset) },
            version = 2,
        )
        val deserialized = roundTripSerde(message, 1.toShort())
        assertEquals(SimpleExampleMessageData(), deserialized)
        assertEquals(0, deserialized.taggedLongFlexibleVersionSubset)
    }

    @Test
    fun testToString() {
        val message = SimpleExampleMessageData()
        message.setMyUint16(65535u)
        message.setTaggedUuid(Uuid.fromString("x7D3Ck_ZRA22-dzIvu_pnQ"))
        message.setMyFloat64(1.0)
        assertEquals(
            "SimpleExampleMessageData(processId=AAAAAAAAAAAAAAAAAAAAAA, " +
                    "myTaggedIntArray=[], " +
                    "myNullableString=null, " +
                    "myInt16=123, myFloat64=1.0, " +
                    "myString='', " +
                    "myBytes=[], " +
                    "taggedUuid=x7D3Ck_ZRA22-dzIvu_pnQ, " +
                    "taggedLong=914172222550880202, " +
                    "zeroCopyByteBuffer=java.nio.HeapByteBuffer[pos=0 lim=0 cap=0], " +
                    "nullableZeroCopyByteBuffer=java.nio.HeapByteBuffer[pos=0 lim=0 cap=0], " +
                    "myStruct=MyStruct(structId=0, arrayInStruct=[]), " +
                    "myTaggedStruct=TaggedStruct(structId=''), " +
                    "taggedLongFlexibleVersionSubset=0, " +
                    "myCommonStruct=TestCommonStruct(foo=123, bar=123), " +
                    "myOtherCommonStruct=TestCommonStruct(foo=123, bar=123), " +
                    "myUint16=65535, " +
                    "myUint32=1234567)",
            message.toString(),
        )
    }
}
