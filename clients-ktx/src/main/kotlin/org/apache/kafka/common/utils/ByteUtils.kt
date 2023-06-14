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

import java.io.DataInput
import java.io.DataOutput
import java.io.IOException
import java.io.InputStream
import java.io.OutputStream
import java.nio.ByteBuffer

/**
 * This classes exposes low-level methods for reading/writing from byte streams or buffers.
 */
object ByteUtils {

    val EMPTY_BUF = ByteBuffer.wrap(ByteArray(0))

    /**
     * Read an unsigned integer from the current position in the buffer, incrementing the position
     * by 4 bytes.
     *
     * @param buffer The buffer to read from
     * @return The integer read, as a long to avoid signedness
     */
    fun readUnsignedInt(buffer: ByteBuffer): Long =
        buffer.getInt().toLong() and 0xffffffffL

    /**
     * Read an unsigned integer from the given position without modifying the buffers position
     *
     * @param buffer the buffer to read from
     * @param index the index from which to read the integer
     * @return The integer read, as a long to avoid signedness
     */
    fun readUnsignedInt(buffer: ByteBuffer, index: Int): Long =
        buffer.getInt(index).toLong() and 0xffffffffL

    /**
     * Read an unsigned integer stored in little-endian format from the [InputStream].
     *
     * @param input The stream to read from
     * @return The integer read (MUST BE TREATED WITH SPECIAL CARE TO AVOID SIGNEDNESS)
     */
    @Throws(IOException::class)
    fun readUnsignedIntLE(input: InputStream): Int =
        (input.read()
                or (input.read() shl 8)
                or (input.read() shl 16)
                or (input.read() shl 24))

    /**
     * Read an unsigned integer stored in little-endian format from a byte array at a given offset.
     *
     * @param buffer The byte array to read from
     * @param offset The position in buffer to read from
     * @return The integer read (MUST BE TREATED WITH SPECIAL CARE TO AVOID SIGNEDNESS)
     */
    fun readUnsignedIntLE(buffer: ByteArray, offset: Int): Int =
        (buffer[offset].toInt() shl 0 and 0xff
                or (buffer[offset + 1].toInt() and 0xff shl 8)
                or (buffer[offset + 2].toInt() and 0xff shl 16)
                or (buffer[offset + 3].toInt() and 0xff shl 24))

    /**
     * Write the given long value as a 4 byte unsigned integer. Overflow is ignored.
     *
     * @param buffer The buffer to write to
     * @param index The position in the buffer at which to begin writing
     * @param value The value to write
     */
    fun writeUnsignedInt(buffer: ByteBuffer, index: Int, value: Long) {
        buffer.putInt(index, (value and 0xffffffffL).toInt())
    }

    /**
     * Write the given long value as a 4 byte unsigned integer. Overflow is ignored.
     *
     * @param buffer The buffer to write to
     * @param value The value to write
     */
    fun writeUnsignedInt(buffer: ByteBuffer, value: Long) {
        buffer.putInt((value and 0xffffffffL).toInt())
    }

    /**
     * Write an unsigned integer in little-endian format to the [OutputStream].
     *
     * @param out The stream to write to
     * @param value The value to write
     */
    @Throws(IOException::class)
    fun writeUnsignedIntLE(out: OutputStream, value: Int) {
        out.write(value)
        out.write(value ushr 8)
        out.write(value ushr 16)
        out.write(value ushr 24)
    }

    /**
     * Write an unsigned integer in little-endian format to a byte array at a given offset.
     *
     * @param buffer The byte array to write to
     * @param offset The position in buffer to write to
     * @param value The value to write
     */
    fun writeUnsignedIntLE(buffer: ByteArray, offset: Int, value: Int) {
        buffer[offset] = value.toByte()
        buffer[offset + 1] = (value ushr 8).toByte()
        buffer[offset + 2] = (value ushr 16).toByte()
        buffer[offset + 3] = (value ushr 24).toByte()
    }

    /**
     * Read an integer stored in variable-length format using unsigned decoding from
     * [Google Protocol Buffers](http://code.google.com/apis/protocolbuffers/docs/encoding.html).
     *
     * @param buffer The buffer to read from
     * @return The integer read
     * @throws IllegalArgumentException if variable-length value does not terminate after 5 bytes
     * have been read
     */
    fun readUnsignedVarint(buffer: ByteBuffer): Int {
        var value = 0
        var i = 0
        var b: Int
        while ((buffer.get().toInt().also { b = it } and 0x80) != 0) {
            value = value or (b and 0x7f shl i)
            i += 7
            if (i > 28) throw illegalVarintException(value)
        }
        value = value or (b shl i)
        return value
    }

    /**
     * Read an integer stored in variable-length format using unsigned decoding from
     * [Google Protocol Buffers](http://code.google.com/apis/protocolbuffers/docs/encoding.html).
     *
     * @param input The input to read from
     * @return The integer read
     *
     * @throws IllegalArgumentException if variable-length value does not terminate after 5 bytes
     * have been read
     * @throws IOException if [DataInput] throws [IOException]
     */
    @Throws(IOException::class)
    fun readUnsignedVarint(input: DataInput): Int {
        var value = 0
        var i = 0
        var b: Int
        while ((input.readByte().toInt().also { b = it } and 0x80) != 0) {
            value = value or (b and 0x7f shl i)
            i += 7
            if (i > 28) throw illegalVarintException(value)
        }
        value = value or (b shl i)
        return value
    }

    /**
     * Read an integer stored in variable-length format using zig-zag decoding from
     * [Google Protocol Buffers](http://code.google.com/apis/protocolbuffers/docs/encoding.html).
     *
     * @param buffer The buffer to read from
     * @return The integer read
     *
     * @throws IllegalArgumentException if variable-length value does not terminate after 5 bytes
     * have been read
     */
    fun readVarint(buffer: ByteBuffer): Int {
        val value = readUnsignedVarint(buffer)
        return value ushr 1 xor -(value and 1)
    }

    /**
     * Read an integer stored in variable-length format using zig-zag decoding from
     * [Google Protocol Buffers](http://code.google.com/apis/protocolbuffers/docs/encoding.html).
     *
     * @param input The input to read from
     * @return The integer read
     *
     * @throws IllegalArgumentException if variable-length value does not terminate after 5 bytes
     * have been read
     * @throws IOException if [DataInput] throws [IOException]
     */
    @Throws(IOException::class)
    fun readVarint(input: DataInput): Int {
        val value = readUnsignedVarint(input)
        return value ushr 1 xor -(value and 1)
    }

    /**
     * Read a long stored in variable-length format using zig-zag decoding from
     * [Google Protocol Buffers](http://code.google.com/apis/protocolbuffers/docs/encoding.html).
     *
     * @param input The input to read from
     * @return The long value read
     *
     * @throws IllegalArgumentException if variable-length value does not terminate after 10 bytes
     * have been read
     * @throws IOException if [DataInput] throws [IOException]
     */
    @Throws(IOException::class)
    fun readVarlong(input: DataInput): Long {
        var value = 0L
        var i = 0
        var b: Long
        while ((input.readByte().toLong().also { b = it } and 0x80L) != 0L) {
            value = value or (b and 0x7fL shl i)
            i += 7
            if (i > 63) throw illegalVarlongException(value)
        }
        value = value or (b shl i)
        return value ushr 1 xor -(value and 1L)
    }

    /**
     * Read a long stored in variable-length format using zig-zag decoding from
     * [Google Protocol Buffers](http://code.google.com/apis/protocolbuffers/docs/encoding.html).
     *
     * @param buffer The buffer to read from
     * @return The long value read
     *
     * @throws IllegalArgumentException if variable-length value does not terminate after 10 bytes
     * have been read
     */
    fun readVarlong(buffer: ByteBuffer): Long {
        var value = 0L
        var i = 0
        var b: Long
        while (buffer.get().toLong().also { b = it } and 0x80L != 0L) {
            value = value or (b and 0x7fL shl i)
            i += 7
            if (i > 63) throw illegalVarlongException(value)
        }
        value = value or (b shl i)
        return value ushr 1 xor -(value and 1L)
    }

    /**
     * Read a double-precision 64-bit format IEEE 754 value.
     *
     * @param input The input to read from
     * @return The double value read
     */
    @Throws(IOException::class)
    fun readDouble(input: DataInput): Double = input.readDouble()

    /**
     * Read a double-precision 64-bit format IEEE 754 value.
     *
     * @param buffer The buffer to read from
     * @return The long value read
     */
    fun readDouble(buffer: ByteBuffer): Double = buffer.getDouble()

    /**
     * Write the given integer following the variable-length unsigned encoding from
     * [Google Protocol Buffers](http://code.google.com/apis/protocolbuffers/docs/encoding.html)
     * into the buffer.
     *
     * @param value The value to write
     * @param buffer The output to write to
     */
    fun writeUnsignedVarint(value: Int, buffer: ByteBuffer) {
        var value = value
        while ((value and -0x80).toLong() != 0L) {
            val b = (value and 0x7f or 0x80).toByte()
            buffer.put(b)
            value = value ushr 7
        }
        buffer.put(value.toByte())
    }

    /**
     * Write the given integer following the variable-length unsigned encoding from
     * [Google Protocol Buffers](http://code.google.com/apis/protocolbuffers/docs/encoding.html)
     * into the buffer.
     *
     * @param value The value to write
     * @param out The output to write to
     */
    @Throws(IOException::class)
    fun writeUnsignedVarint(value: Int, out: DataOutput) {
        var value = value
        while ((value and -0x80).toLong() != 0L) {
            val b = (value and 0x7f or 0x80).toByte()
            out.writeByte(b.toInt())
            value = value ushr 7
        }
        out.writeByte(value.toByte().toInt())
    }

    /**
     * Write the given integer following the variable-length zig-zag encoding from
     * [Google Protocol Buffers](http://code.google.com/apis/protocolbuffers/docs/encoding.html)
     * into the output.
     *
     * @param value The value to write
     * @param out The output to write to
     */
    @Throws(IOException::class)
    fun writeVarint(value: Int, out: DataOutput) {
        writeUnsignedVarint(value shl 1 xor (value shr 31), out)
    }

    /**
     * Write the given integer following the variable-length zig-zag encoding from
     * [Google Protocol Buffers](http://code.google.com/apis/protocolbuffers/docs/encoding.html)
     * into the buffer.
     *
     * @param value The value to write
     * @param buffer The output to write to
     */
    fun writeVarint(value: Int, buffer: ByteBuffer) {
        writeUnsignedVarint(value shl 1 xor (value shr 31), buffer)
    }

    /**
     * Write the given integer following the variable-length zig-zag encoding from
     * [Google Protocol Buffers](http://code.google.com/apis/protocolbuffers/docs/encoding.html)
     * into the output.
     *
     * @param value The value to write
     * @param out The output to write to
     */
    @Throws(IOException::class)
    fun writeVarlong(value: Long, out: DataOutput) {
        var v = value shl 1 xor (value shr 63)
        while (v and -0x80L != 0L) {
            out.writeByte(v.toInt() and 0x7f or 0x80)
            v = v ushr 7
        }
        out.writeByte(v.toByte().toInt())
    }

    /**
     * Write the given integer following the variable-length zig-zag encoding from
     * [Google Protocol Buffers](http://code.google.com/apis/protocolbuffers/docs/encoding.html)
     * into the buffer.
     *
     * @param value The value to write
     * @param buffer The buffer to write to
     */
    fun writeVarlong(value: Long, buffer: ByteBuffer) {
        var v = value shl 1 xor (value shr 63)
        while (v and -0x80L != 0L) {
            val b = (v and 0x7fL or 0x80L).toByte()
            buffer.put(b)
            v = v ushr 7
        }
        buffer.put(v.toByte())
    }

    /**
     * Write the given double following the double-precision 64-bit format IEEE 754 value into the
     * output.
     *
     * @param value The value to write
     * @param out The output to write to
     */
    @Throws(IOException::class)
    fun writeDouble(value: Double, out: DataOutput) = out.writeDouble(value)

    /**
     * Write the given double following the double-precision 64-bit format IEEE 754 value into the
     * buffer.
     *
     * @param value The value to write
     * @param buffer The buffer to write to
     */
    fun writeDouble(value: Double, buffer: ByteBuffer) = buffer.putDouble(value)

    /**
     * Number of bytes needed to encode an integer in unsigned variable-length format.
     *
     * @param value The signed value
     *
     * @see .writeUnsignedVarint
     */
    fun sizeOfUnsignedVarint(value: Int): Int {
        // Protocol buffers varint encoding is variable length, with a minimum of 1 byte
        // (for zero). The values themselves are not important. What's important here is
        // any leading zero bits are dropped from output. We can use this leading zero
        // count w/ fast intrinsic to calc the output length directly.

        // Test cases verify this matches the output for loop logic exactly.

        // return (38 - leadingZeros) / 7 + leadingZeros / 32;

        // The above formula provides the implementation, but the Java encoding is suboptimal
        // when we have a narrow range of integers, so we can do better manually
        val leadingZeros = Integer.numberOfLeadingZeros(value)
        val leadingZerosBelow38DividedBy7 = (38 - leadingZeros) * 74899 ushr 19
        return leadingZerosBelow38DividedBy7 + (leadingZeros ushr 5)
    }

    /**
     * Number of bytes needed to encode an integer in variable-length format.
     *
     * @param value The signed value
     */
    fun sizeOfVarint(value: Int): Int = sizeOfUnsignedVarint(value shl 1 xor (value shr 31))

    /**
     * Number of bytes needed to encode a long in variable-length format.
     *
     * @param value The signed value
     * @see .sizeOfUnsignedVarint
     */
    fun sizeOfVarlong(value: Long): Int {
        val v = value shl 1 xor (value shr 63)

        // For implementation notes @see #sizeOfUnsignedVarint(int)
        // Similar logic is applied to allow for 64bit input -> 1-9byte output.
        // return (70 - leadingZeros) / 7 + leadingZeros / 64;
        val leadingZeros = java.lang.Long.numberOfLeadingZeros(v)
        val leadingZerosBelow70DividedBy7 = (70 - leadingZeros) * 74899 ushr 19
        return leadingZerosBelow70DividedBy7 + (leadingZeros ushr 6)
    }

    private fun illegalVarintException(value: Int): IllegalArgumentException {
        throw IllegalArgumentException(
            "Varint is too long, the most significant bit in the 5th byte is set, " +
                    "converted value: " + Integer.toHexString(value)
        )
    }

    private fun illegalVarlongException(value: Long): IllegalArgumentException {
        throw IllegalArgumentException(
            ("Varlong is too long, most significant bit in the 10th byte is set, " +
                    "converted value: " + java.lang.Long.toHexString(value))
        )
    }
}
