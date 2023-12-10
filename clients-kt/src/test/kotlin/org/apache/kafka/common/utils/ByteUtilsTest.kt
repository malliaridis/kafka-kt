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

package org.apache.kafka.common.utils

import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.DataInputStream
import java.io.DataOutputStream
import java.io.IOException
import java.nio.ByteBuffer
import java.util.function.Function
import java.util.function.IntFunction
import java.util.function.LongFunction
import org.apache.kafka.common.utils.ByteUtils.readDouble
import org.apache.kafka.common.utils.ByteUtils.readIntBE
import org.apache.kafka.common.utils.ByteUtils.readUnsignedInt
import org.apache.kafka.common.utils.ByteUtils.readUnsignedIntLE
import org.apache.kafka.common.utils.ByteUtils.readUnsignedVarint
import org.apache.kafka.common.utils.ByteUtils.readVarint
import org.apache.kafka.common.utils.ByteUtils.readVarlong
import org.apache.kafka.common.utils.ByteUtils.sizeOfUnsignedVarint
import org.apache.kafka.common.utils.ByteUtils.sizeOfVarlong
import org.apache.kafka.common.utils.ByteUtils.writeDouble
import org.apache.kafka.common.utils.ByteUtils.writeUnsignedInt
import org.apache.kafka.common.utils.ByteUtils.writeUnsignedIntLE
import org.apache.kafka.common.utils.ByteUtils.writeUnsignedVarint
import org.apache.kafka.common.utils.ByteUtils.writeVarint
import org.apache.kafka.common.utils.ByteUtils.writeVarlong
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class ByteUtilsTest {

    private val x00 = 0x00.toByte()

    private val x01 = 0x01.toByte()

    private val x02 = 0x02.toByte()

    private val x0F = 0x0f.toByte()

    private val x07 = 0x07.toByte()

    private val x08 = 0x08.toByte()

    private val x3F = 0x3f.toByte()

    private val x40 = 0x40.toByte()

    private val x7E = 0x7E.toByte()

    private val x7F = 0x7F.toByte()

    private val xFF = 0xff.toByte()

    private val x80 = 0x80.toByte()

    private val x81 = 0x81.toByte()

    private val xBF = 0xbf.toByte()

    private val xC0 = 0xc0.toByte()

    private val xFE = 0xfe.toByte()

    @Test
    fun testReadUnsignedIntLEFromArray() {
        val array1 = byteArrayOf(0x01, 0x02, 0x03, 0x04, 0x05)
        assertEquals(0x04030201, readUnsignedIntLE(array1, 0))
        assertEquals(0x05040302, readUnsignedIntLE(array1, 1))
        val array2 = byteArrayOf(
            0xf1.toByte(),
            0xf2.toByte(),
            0xf3.toByte(),
            0xf4.toByte(),
            0xf5.toByte(),
            0xf6.toByte(),
        )
        assertEquals(-0xb0c0d0f, readUnsignedIntLE(array2, 0))
        assertEquals(-0x90a0b0d, readUnsignedIntLE(array2, 2))
    }

    @Test
    @Throws(IOException::class)
    fun testReadUnsignedIntLEFromInputStream() {
        val array1 = byteArrayOf(0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09)
        val is1 = ByteArrayInputStream(array1)
        assertEquals(0x04030201, readUnsignedIntLE(is1))
        assertEquals(0x08070605, readUnsignedIntLE(is1))
        val array2 = byteArrayOf(
            0xf1.toByte(),
            0xf2.toByte(),
            0xf3.toByte(),
            0xf4.toByte(),
            0xf5.toByte(),
            0xf6.toByte(),
            0xf7.toByte(),
            0xf8.toByte(),
        )
        val is2 = ByteArrayInputStream(array2)
        assertEquals(-0xb0c0d0f, readUnsignedIntLE(is2))
        assertEquals(-0x708090b, readUnsignedIntLE(is2))
    }

    @Test
    fun testReadUnsignedInt() {
        val buffer = ByteBuffer.allocate(4)
        val writeValue = 133444u
        writeUnsignedInt(buffer, writeValue)
        buffer.flip()
        val readValue = readUnsignedInt(buffer)
        assertEquals(writeValue, readValue)
    }

    @Test
    fun testWriteUnsignedIntLEToArray() {
        val value1 = 0x04030201
        var array1 = ByteArray(4)
        writeUnsignedIntLE(array1, 0, value1)
        assertContentEquals(byteArrayOf(0x01, 0x02, 0x03, 0x04), array1)
        array1 = ByteArray(8)
        writeUnsignedIntLE(array1, 2, value1)
        assertContentEquals(byteArrayOf(0, 0, 0x01, 0x02, 0x03, 0x04, 0, 0), array1)
        val value2 = -0xb0c0d0f
        var array2 = ByteArray(4)
        writeUnsignedIntLE(array2, 0, value2)
        assertContentEquals(byteArrayOf(0xf1.toByte(), 0xf2.toByte(), 0xf3.toByte(), 0xf4.toByte()), array2)
        array2 = ByteArray(8)
        writeUnsignedIntLE(array2, 2, value2)
        assertContentEquals(
            byteArrayOf(
                0.toByte(),
                0.toByte(),
                0xf1.toByte(),
                0xf2.toByte(),
                0xf3.toByte(),
                0xf4.toByte(),
                0.toByte(),
                0.toByte(),
            ), array2
        )
    }

    @Test
    @Throws(IOException::class)
    fun testWriteUnsignedIntLEToOutputStream() {
        val value1 = 0x04030201
        val os1 = ByteArrayOutputStream()
        writeUnsignedIntLE(os1, value1)
        writeUnsignedIntLE(os1, value1)
        assertContentEquals(byteArrayOf(0x01, 0x02, 0x03, 0x04, 0x01, 0x02, 0x03, 0x04), os1.toByteArray())
        val value2 = -0xb0c0d0f
        val os2 = ByteArrayOutputStream()
        writeUnsignedIntLE(os2, value2)
        assertContentEquals(
            expected = byteArrayOf(0xf1.toByte(), 0xf2.toByte(), 0xf3.toByte(), 0xf4.toByte()),
            actual = os2.toByteArray()
        )
    }

    @Test
    @Throws(Exception::class)
    fun testUnsignedVarintSerde() {
        assertUnsignedVarintSerde(0, byteArrayOf(x00))
        assertUnsignedVarintSerde(-1, byteArrayOf(xFF, xFF, xFF, xFF, x0F))
        assertUnsignedVarintSerde(1, byteArrayOf(x01))
        assertUnsignedVarintSerde(63, byteArrayOf(x3F))
        assertUnsignedVarintSerde(-64, byteArrayOf(xC0, xFF, xFF, xFF, x0F))
        assertUnsignedVarintSerde(64, byteArrayOf(x40))
        assertUnsignedVarintSerde(8191, byteArrayOf(xFF, x3F))
        assertUnsignedVarintSerde(-8192, byteArrayOf(x80, xC0, xFF, xFF, x0F))
        assertUnsignedVarintSerde(8192, byteArrayOf(x80, x40))
        assertUnsignedVarintSerde(-8193, byteArrayOf(xFF, xBF, xFF, xFF, x0F))
        assertUnsignedVarintSerde(1048575, byteArrayOf(xFF, xFF, x3F))
        assertUnsignedVarintSerde(1048576, byteArrayOf(x80, x80, x40))
        assertUnsignedVarintSerde(Int.MAX_VALUE, byteArrayOf(xFF, xFF, xFF, xFF, x07))
        assertUnsignedVarintSerde(Int.MIN_VALUE, byteArrayOf(x80, x80, x80, x80, x08))
    }

    @Test
    @Throws(Exception::class)
    fun testVarintSerde() {
        assertVarintSerde(0, byteArrayOf(x00))
        assertVarintSerde(-1, byteArrayOf(x01))
        assertVarintSerde(1, byteArrayOf(x02))
        assertVarintSerde(63, byteArrayOf(x7E))
        assertVarintSerde(-64, byteArrayOf(x7F))
        assertVarintSerde(64, byteArrayOf(x80, x01))
        assertVarintSerde(-65, byteArrayOf(x81, x01))
        assertVarintSerde(8191, byteArrayOf(xFE, x7F))
        assertVarintSerde(-8192, byteArrayOf(xFF, x7F))
        assertVarintSerde(8192, byteArrayOf(x80, x80, x01))
        assertVarintSerde(-8193, byteArrayOf(x81, x80, x01))
        assertVarintSerde(1048575, byteArrayOf(xFE, xFF, x7F))
        assertVarintSerde(-1048576, byteArrayOf(xFF, xFF, x7F))
        assertVarintSerde(1048576, byteArrayOf(x80, x80, x80, x01))
        assertVarintSerde(-1048577, byteArrayOf(x81, x80, x80, x01))
        assertVarintSerde(134217727, byteArrayOf(xFE, xFF, xFF, x7F))
        assertVarintSerde(-134217728, byteArrayOf(xFF, xFF, xFF, x7F))
        assertVarintSerde(134217728, byteArrayOf(x80, x80, x80, x80, x01))
        assertVarintSerde(-134217729, byteArrayOf(x81, x80, x80, x80, x01))
        assertVarintSerde(Int.MAX_VALUE, byteArrayOf(xFE, xFF, xFF, xFF, x0F))
        assertVarintSerde(Int.MIN_VALUE, byteArrayOf(xFF, xFF, xFF, xFF, x0F))
    }

    @Test
    @Throws(Exception::class)
    fun testVarlongSerde() {
        assertVarlongSerde(0, byteArrayOf(x00))
        assertVarlongSerde(-1, byteArrayOf(x01))
        assertVarlongSerde(1, byteArrayOf(x02))
        assertVarlongSerde(63, byteArrayOf(x7E))
        assertVarlongSerde(-64, byteArrayOf(x7F))
        assertVarlongSerde(64, byteArrayOf(x80, x01))
        assertVarlongSerde(-65, byteArrayOf(x81, x01))
        assertVarlongSerde(8191, byteArrayOf(xFE, x7F))
        assertVarlongSerde(-8192, byteArrayOf(xFF, x7F))
        assertVarlongSerde(8192, byteArrayOf(x80, x80, x01))
        assertVarlongSerde(-8193, byteArrayOf(x81, x80, x01))
        assertVarlongSerde(1048575, byteArrayOf(xFE, xFF, x7F))
        assertVarlongSerde(-1048576, byteArrayOf(xFF, xFF, x7F))
        assertVarlongSerde(1048576, byteArrayOf(x80, x80, x80, x01))
        assertVarlongSerde(-1048577, byteArrayOf(x81, x80, x80, x01))
        assertVarlongSerde(134217727, byteArrayOf(xFE, xFF, xFF, x7F))
        assertVarlongSerde(-134217728, byteArrayOf(xFF, xFF, xFF, x7F))
        assertVarlongSerde(134217728, byteArrayOf(x80, x80, x80, x80, x01))
        assertVarlongSerde(-134217729, byteArrayOf(x81, x80, x80, x80, x01))
        assertVarlongSerde(Int.MAX_VALUE.toLong(), byteArrayOf(xFE, xFF, xFF, xFF, x0F))
        assertVarlongSerde(Int.MIN_VALUE.toLong(), byteArrayOf(xFF, xFF, xFF, xFF, x0F))
        assertVarlongSerde(17179869183L, byteArrayOf(xFE, xFF, xFF, xFF, x7F))
        assertVarlongSerde(-17179869184L, byteArrayOf(xFF, xFF, xFF, xFF, x7F))
        assertVarlongSerde(17179869184L, byteArrayOf(x80, x80, x80, x80, x80, x01))
        assertVarlongSerde(-17179869185L, byteArrayOf(x81, x80, x80, x80, x80, x01))
        assertVarlongSerde(2199023255551L, byteArrayOf(xFE, xFF, xFF, xFF, xFF, x7F))
        assertVarlongSerde(-2199023255552L, byteArrayOf(xFF, xFF, xFF, xFF, xFF, x7F))
        assertVarlongSerde(2199023255552L, byteArrayOf(x80, x80, x80, x80, x80, x80, x01))
        assertVarlongSerde(-2199023255553L, byteArrayOf(x81, x80, x80, x80, x80, x80, x01))
        assertVarlongSerde(281474976710655L, byteArrayOf(xFE, xFF, xFF, xFF, xFF, xFF, x7F))
        assertVarlongSerde(-281474976710656L, byteArrayOf(xFF, xFF, xFF, xFF, xFF, xFF, x7F))
        assertVarlongSerde(281474976710656L, byteArrayOf(x80, x80, x80, x80, x80, x80, x80, x01))
        assertVarlongSerde(-281474976710657L, byteArrayOf(x81, x80, x80, x80, x80, x80, x80, 1))
        assertVarlongSerde(36028797018963967L, byteArrayOf(xFE, xFF, xFF, xFF, xFF, xFF, xFF, x7F))
        assertVarlongSerde(-36028797018963968L, byteArrayOf(xFF, xFF, xFF, xFF, xFF, xFF, xFF, x7F))
        assertVarlongSerde(36028797018963968L, byteArrayOf(x80, x80, x80, x80, x80, x80, x80, x80, x01))
        assertVarlongSerde(-36028797018963969L, byteArrayOf(x81, x80, x80, x80, x80, x80, x80, x80, x01))
        assertVarlongSerde(4611686018427387903L, byteArrayOf(xFE, xFF, xFF, xFF, xFF, xFF, xFF, xFF, x7F))
        assertVarlongSerde(-4611686018427387904L, byteArrayOf(xFF, xFF, xFF, xFF, xFF, xFF, xFF, xFF, x7F))
        assertVarlongSerde(4611686018427387904L, byteArrayOf(x80, x80, x80, x80, x80, x80, x80, x80, x80, x01))
        assertVarlongSerde(-4611686018427387905L, byteArrayOf(x81, x80, x80, x80, x80, x80, x80, x80, x80, x01))
        assertVarlongSerde(Long.MAX_VALUE, byteArrayOf(xFE, xFF, xFF, xFF, xFF, xFF, xFF, xFF, xFF, x01))
        assertVarlongSerde(Long.MIN_VALUE, byteArrayOf(xFF, xFF, xFF, xFF, xFF, xFF, xFF, xFF, xFF, x01))
    }

    @Test
    fun testInvalidVarint() {
        // varint encoding has one overflow byte
        val buf = ByteBuffer.wrap(byteArrayOf(xFF, xFF, xFF, xFF, xFF, x01))
        assertFailsWith<IllegalArgumentException> { readVarint(buf) }
    }

    @Test
    fun testInvalidVarlong() {
        // varlong encoding has one overflow byte
        val buf = ByteBuffer.wrap(byteArrayOf(xFF, xFF, xFF, xFF, xFF, xFF, xFF, xFF, xFF, xFF, x01))
        assertFailsWith<IllegalArgumentException> { readVarlong(buf) }
    }

    @Test
    @Throws(IOException::class)
    fun testDouble() {
        assertDoubleSerde(0.0, 0x0L)
        assertDoubleSerde(-0.0, Long.MIN_VALUE)
        assertDoubleSerde(1.0, 0x3FF0000000000000L)
        assertDoubleSerde(-1.0, -0x4010000000000000L)
        assertDoubleSerde(123e45, 0x49B58B82C0E0BB00L)
        assertDoubleSerde(-123e45, -0x364a747d3f1f4500L)
        assertDoubleSerde(Double.MIN_VALUE, 0x1L)
        assertDoubleSerde(-Double.MIN_VALUE, -0x7fffffffffffffffL)
        assertDoubleSerde(Double.MAX_VALUE, 0x7FEFFFFFFFFFFFFFL)
        assertDoubleSerde(-Double.MAX_VALUE, -0x10000000000001L)
        assertDoubleSerde(Double.NaN, 0x7FF8000000000000L)
        assertDoubleSerde(Double.POSITIVE_INFINITY, 0x7FF0000000000000L)
        assertDoubleSerde(Double.NEGATIVE_INFINITY, -0x10000000000000L)
    }

    @Test
    @Disabled("TODO Enable this when we change the implementation of UnsignedVarlong")
    fun testCorrectnessWriteUnsignedVarlong() {
        // The old well-known implementation for writeVarlong.
        val simpleImplementation = LongFunction { value: Long ->
            var value = value
            val buffer = ByteBuffer.allocate(MAX_LENGTH_VARLONG)
            while (value and -0x80L != 0L) {
                val b = (value and 0x7fL or 0x80L).toByte()
                buffer.put(b)
                value = value ushr 7
            }
            buffer.put(value.toByte())
            buffer
        }

        // compare the full range of values
        val actual = ByteBuffer.allocate(MAX_LENGTH_VARLONG)
        var i: Long = 1
        while (i < Long.MAX_VALUE && i >= 0) {
            ByteUtils.writeUnsignedVarlong(i, actual)
            val expected = simpleImplementation.apply(i)
            assertContentEquals(
                expected = expected.array(),
                actual = actual.array(),
                message = "Implementations do not match for number=$i",
            )
            actual.clear()
            i = i shl 1
        }
    }

    @Test
    fun testCorrectnessWriteUnsignedVarint() {
        // The old well-known implementation for writeUnsignedVarint.
        val simpleImplementation = IntFunction { value: Int ->
            var value = value
            val buffer = ByteBuffer.allocate(MAX_LENGTH_VARINT)
            while (true) {
                value = if (value and 0x7F.inv() == 0) {
                    buffer.put(value.toByte())
                    break
                } else {
                    buffer.put((value and 0x7F or 0x80).toByte())
                    value ushr 7
                }
            }
            buffer
        }

        // compare the full range of values
        val actual = ByteBuffer.allocate(MAX_LENGTH_VARINT)
        var i = 0
        while (i < Int.MAX_VALUE && i >= 0) {
            writeUnsignedVarint(i, actual)
            val expected = simpleImplementation.apply(i)
            assertContentEquals(
                expected = expected.array(),
                actual = actual.array(),
                message = "Implementations do not match for integer=$i"
            )
            actual.clear()
            i += 13
        }
    }

    @Test
    fun testCorrectnessReadUnsignedVarint() {
        // The old well-known implementation for readUnsignedVarint
        val simpleImplementation = { buffer: ByteBuffer ->
            var value = 0
            var i = 0
            var b: Int
            while (buffer.get().toInt().also { b = it } and 0x80 != 0) {
                value = value or (b and 0x7f shl i)
                i += 7
                require(i <= 28) { "Invalid varint" }
            }
            value = value or (b shl i)
            value
        }

        // compare the full range of values
        val testData = ByteBuffer.allocate(MAX_LENGTH_VARINT)
        var i = 0
        while (i < Int.MAX_VALUE && i >= 0) {
            writeUnsignedVarint(i, testData)
            // prepare buffer for reading
            testData.flip()
            val actual = readUnsignedVarint(testData.duplicate())
            val expected = simpleImplementation(testData)
            assertEquals(expected, actual)
            testData.clear()
            i += 13
        }
    }

    @Test
    @Disabled // Enable this when we change the implementation of UnsignedVarlong
    fun testCorrectnessReadUnsignedVarlong() {
        // The old well-known implementation for readUnsignedVarlong
        val simpleImplementation =
            Function { buffer: ByteBuffer ->
                var value = 0L
                var i = 0
                var b: Long
                while (buffer.get().toLong().also { b = it } and 0x80L != 0L) {
                    value = value or (b and 0x7fL shl i)
                    i += 7
                    if (i > 63) throw IllegalArgumentException()
                }
                value = value or (b shl i)
                value
            }

        // compare the full range of values
        val testData = ByteBuffer.allocate(MAX_LENGTH_VARLONG)
        var i: Long = 1
        while (i < Long.MAX_VALUE && i >= 0) {
            ByteUtils.writeUnsignedVarlong(i, testData)
            // prepare buffer for reading
            testData.flip()
            val actual = ByteUtils.readUnsignedVarlong(testData.duplicate())
            val expected = simpleImplementation.apply(testData)
            assertEquals(expected, actual)
            testData.clear()
            i = i shl 1
        }
    }

    @Test
    fun testSizeOfUnsignedVarint() {
        // The old well-known implementation for sizeOfUnsignedVarint
        val simpleImplementation = IntFunction {
            var value = it
            var bytes = 1
            while ((value and -0x80).toLong() != 0L) {
                bytes += 1
                value = value ushr 7
            }
            bytes
        }

        // compare the full range of values
        var i = 0
        while (i < Int.MAX_VALUE && i >= 0) {
            val actual = sizeOfUnsignedVarint(i)
            val expected = simpleImplementation.apply(i)
            assertEquals(expected, actual)
            i += 13
        }
    }

    @Test
    fun testSizeOfVarlong() {
        // The old well-known implementation for sizeOfVarlong
        val simpleImplementation = LongFunction { value: Long ->
            var v = value shl 1 xor (value shr 63)
            var bytes = 1
            while (v and -0x80L != 0L) {
                bytes += 1
                v = v ushr 7
            }
            bytes
        }
        var l: Long = 1
        while (l < Long.MAX_VALUE && l >= 0) {
            val expected = simpleImplementation.apply(l)
            val actual = sizeOfVarlong(l)
            assertEquals(expected, actual)
            l = l shl 1
        }

        // check zero as well
        assertEquals(simpleImplementation.apply(0), sizeOfVarlong(0))
    }

    @Test
    fun testReadInt() {
        val values = intArrayOf(
            0,
            1,
            -1,
            Byte.MAX_VALUE.toInt(),
            Short.MAX_VALUE.toInt(),
            2 * Short.MAX_VALUE,
            Int.MAX_VALUE / 2,
            Int.MIN_VALUE / 2,
            Int.MAX_VALUE,
            Int.MIN_VALUE,
            Int.MAX_VALUE,
        )
        val buffer = ByteBuffer.allocate(4 * values.size)
        for (i in values.indices) {
            buffer.putInt(i * 4, values[i])
            assertEquals(
                expected = values[i],
                actual = readIntBE(buffer.array(), i * 4),
                message = "Written value should match read value.",
            )
        }
    }

    @Throws(IOException::class)
    private fun assertUnsignedVarintSerde(value: Int, expectedEncoding: ByteArray) {
        val buf = ByteBuffer.allocate(MAX_LENGTH_VARINT)
        writeUnsignedVarint(value, buf)
        buf.flip()
        assertContentEquals(expectedEncoding, Utils.toArray(buf))
        assertEquals(value, readUnsignedVarint(buf.duplicate()))
        buf.rewind()
        val out = DataOutputStream(ByteBufferOutputStream(buf))
        writeUnsignedVarint(value, out)
        buf.flip()
        assertContentEquals(expectedEncoding, Utils.toArray(buf))
        val inputStream = ByteBufferInputStream(buf)
        assertEquals(value, readUnsignedVarint(inputStream))
    }

    @Throws(IOException::class)
    private fun assertVarintSerde(value: Int, expectedEncoding: ByteArray) {
        val buf = ByteBuffer.allocate(MAX_LENGTH_VARINT)
        writeVarint(value, buf)
        buf.flip()
        assertContentEquals(expectedEncoding, Utils.toArray(buf))
        assertEquals(value, readVarint(buf.duplicate()))
        buf.rewind()
        val out = DataOutputStream(ByteBufferOutputStream(buf))
        writeVarint(value, out)
        buf.flip()
        assertContentEquals(expectedEncoding, Utils.toArray(buf))
        val inputStream = ByteBufferInputStream(buf)
        assertEquals(value, readVarint(inputStream))
    }

    @Throws(IOException::class)
    private fun assertVarlongSerde(value: Long, expectedEncoding: ByteArray) {
        val buf = ByteBuffer.allocate(MAX_LENGTH_VARLONG)
        writeVarlong(value, buf)
        buf.flip()
        assertEquals(value, readVarlong(buf.duplicate()))
        assertContentEquals(expectedEncoding, Utils.toArray(buf))
        buf.rewind()
        val out = DataOutputStream(ByteBufferOutputStream(buf))
        writeVarlong(value, out)
        buf.flip()
        assertContentEquals(expectedEncoding, Utils.toArray(buf))
        val inputStream = ByteBufferInputStream(buf)
        assertEquals(value, readVarlong(inputStream))
    }

    @Throws(IOException::class)
    private fun assertDoubleSerde(value: Double, expectedLongValue: Long) {
        var expectedLongValue = expectedLongValue
        val expectedEncoding = ByteArray(8)
        for (i in 0..7) {
            expectedEncoding[7 - i] = (expectedLongValue and 0xFFL).toByte()
            expectedLongValue = expectedLongValue shr 8
        }
        val buf = ByteBuffer.allocate(8)
        writeDouble(value, buf)
        buf.flip()
        assertEquals(value, readDouble(buf.duplicate()), 0.0)
        assertContentEquals(expectedEncoding, Utils.toArray(buf))
        buf.rewind()
        val out = DataOutputStream(ByteBufferOutputStream(buf))
        writeDouble(value, out)
        buf.flip()
        assertContentEquals(expectedEncoding, Utils.toArray(buf))
        val inputStream = DataInputStream(ByteBufferInputStream(buf))
        assertEquals(value, readDouble(inputStream), 0.0)
    }
    
    companion object {
        
        private const val MAX_LENGTH_VARINT = 5
        
        private const val MAX_LENGTH_VARLONG = 10
    }
}
