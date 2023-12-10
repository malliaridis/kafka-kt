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
 *
 * The implementation of these methods has been tuned for JVM and the empirical calculations could be found
 * using ByteUtilsBenchmark.java
 */
object ByteUtils {

    val EMPTY_BUF = ByteBuffer.wrap(ByteArray(0))

    /**
     * Read an unsigned short from the current position in the buffer, incrementing the position
     * by 2 bytes.
     *
     * @param buffer The buffer to read from
     * @return The integer read, as a long to avoid signedness
     */
    fun readUnsignedShort(buffer: ByteBuffer): UShort = buffer.getInt().toUShort()

    /**
     * Read an unsigned integer from the current position in the buffer, incrementing the position
     * by 4 bytes.
     *
     * @param buffer The buffer to read from
     * @return The integer read, as a long to avoid signedness
     */
    fun readUnsignedInt(buffer: ByteBuffer): UInt = buffer.getInt().toUInt()

    /**
     * Read an unsigned integer from the given position without modifying the buffers position
     *
     * @param buffer the buffer to read from
     * @param index the index from which to read the integer
     * @return The integer read, as a long to avoid signedness
     */
    fun readUnsignedInt(buffer: ByteBuffer, index: Int): UInt = buffer.getInt(index).toUInt()

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
     * Read a big-endian integer from a byte array
     */
    fun readIntBE(buffer: ByteArray, offset: Int): Int {
        return (buffer[offset].toInt() and 0xFF shl 24
                or (buffer[offset + 1].toInt() and 0xFF shl 16)
                or (buffer[offset + 2].toInt() and 0xFF shl 8)
                or (buffer[offset + 3].toInt() and 0xFF))
    }

    /**
     * Write the given long value as a 4 byte unsigned integer. Overflow is ignored.
     *
     * @param buffer The buffer to write to
     * @param index The position in the buffer at which to begin writing
     * @param value The value to write
     */
    fun writeUnsignedInt(buffer: ByteBuffer, index: Int, value: UInt) {
        buffer.putInt(index, value.toInt())
    }

    /**
     * Write the given long value as a 4 byte unsigned integer. Overflow is ignored.
     *
     * @param buffer The buffer to write to
     * @param value The value to write
     */
    fun writeUnsignedInt(buffer: ByteBuffer, value: UInt) {
        buffer.putInt(value.toInt())
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
     * The implementation is based on Netty's decoding of varint.
     *
     * See also [Netty's varint decoding](https://github.com/netty/netty/blob/59aa6e635b9996cf21cd946e64353270679adc73/codec/src/main/java/io/netty/handler/codec/protobuf/ProtobufVarint32FrameDecoder.java#L73)
     *
     * @param buffer The buffer to read from
     * @return The integer read
     * @throws IllegalArgumentException if variable-length value does not terminate after 5 bytes
     * have been read
     */
    fun readUnsignedVarint(buffer: ByteBuffer): Int {
        var tmp = buffer.get()
        return if (tmp >= 0) tmp.toInt()
        else {
            var result = tmp.toInt() and 127
            if (buffer.get().also { tmp = it } >= 0) {
                result = result or (tmp.toInt() shl 7)
            } else {
                result = result or (tmp.toInt() and 127 shl 7)
                if (buffer.get().also { tmp = it } >= 0) {
                    result = result or (tmp.toInt() shl 14)
                } else {
                    result = result or (tmp.toInt() and 127 shl 14)
                    if (buffer.get().also { tmp = it } >= 0) {
                        result = result or (tmp.toInt() shl 21)
                    } else {
                        result = result or (tmp.toInt() and 127 shl 21)
                        result = result or (buffer.get().also { tmp = it }.toInt() shl 28)
                        if (tmp < 0) throw illegalVarintException(result)
                    }
                }
            }
            result
        }
    }

    /**
     * Read an integer stored in variable-length format using unsigned decoding from
     * [Google Protocol Buffers](http://code.google.com/apis/protocolbuffers/docs/encoding.html).
     *
     * The implementation is based on Netty's decoding of varint.
     * See also [Netty's varint decoding](https://github.com/netty/netty/blob/59aa6e635b9996cf21cd946e64353270679adc73/codec/src/main/java/io/netty/handler/codec/protobuf/ProtobufVarint32FrameDecoder.java#L73)
     *
     * @param input The input to read from
     * @return The integer read
     *
     * @throws IllegalArgumentException if variable-length value does not terminate after 5 bytes
     * have been read
     * @throws IOException if [InputStream] throws [IOException]
     * @throws EOFException if [InputStream] throws [EOFException]
     */
    @Throws(IOException::class)
    fun readUnsignedVarint(`in`: InputStream): Int {
        var tmp = `in`.read().toByte()
        return if (tmp >= 0) tmp.toInt()
        else {
            var result = tmp.toInt() and 127
            if (`in`.read().toByte().also { tmp = it } >= 0) {
                result = result or (tmp.toInt() shl 7)
            } else {
                result = result or (tmp.toInt() and 127 shl 7)
                if (`in`.read().toByte().also { tmp = it } >= 0) {
                    result = result or (tmp.toInt() shl 14)
                } else {
                    result = result or (tmp.toInt() and 127 shl 14)
                    if (`in`.read().toByte().also { tmp = it } >= 0) {
                        result = result or (tmp.toInt() shl 21)
                    } else {
                        result = result or (tmp.toInt() and 127 shl 21)
                        result = result or (`in`.read().toByte().also { tmp = it }.toInt() shl 28)
                        if (tmp < 0) throw illegalVarintException(result)
                    }
                }
            }
            result
        }
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
    fun readVarint(input: InputStream): Int {
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
    fun readVarlong(input: InputStream): Long {
        var value = 0L
        var i = 0
        var b: Long
        while ((input.read().toLong().also { b = it } and 0x80L) != 0L) {
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
        val raw: Long = readUnsignedVarlong(buffer)
        return raw ushr 1 xor -(raw and 1L)
    }

    // visible for testing
    internal fun readUnsignedVarlong(buffer: ByteBuffer): Long {
        var value = 0L
        var i = 0
        var b: Long
        while ((buffer.get().toLong().also { b = it } and 0x80L) != 0L) {
            value = value or (b and 0x7fL shl i)
            i += 7
            if (i > 63) throw illegalVarlongException(value)
        }
        value = value or (b shl i)
        return value
    }

    /**
     * Read a single-precision 32-bit format IEEE 754 value.
     *
     * @param input The input to read from
     * @return The float value read
     */
    @Throws(IOException::class)
    fun readFloat(input: DataInput): Float = input.readFloat()

    /**
     * Read a single-precision 32-bit format IEEE 754 value.
     *
     * @param buffer The buffer to read from
     * @return The float value read
     */
    fun readFloat(buffer: ByteBuffer): Float = buffer.getFloat()

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
     * @return The double value read
     */
    fun readDouble(buffer: ByteBuffer): Double = buffer.getDouble()

    /**
     * Write the given integer following the variable-length unsigned encoding from
     * [Google Protocol Buffers](http://code.google.com/apis/protocolbuffers/docs/encoding.html)
     * into the buffer.
     *
     * Implementation copied from https://github.com/astei/varint-writing-showdown/tree/dev (MIT License)
     * See also [Sample implementation](https://github.com/astei/varint-writing-showdown/blob/6b1a4baec4b1f0ce65fa40cf0b282ec775fdf43e/src/jmh/java/me/steinborn/varintshowdown/res/SmartNoDataDependencyUnrolledVarIntWriter.java#L8)
     *
     * @param value The value to write
     * @param buffer The output to write to
     */
    fun writeUnsignedVarint(value: Int, buffer: ByteBuffer) {
        if (value and (-0x1 shl 7) == 0) {
            buffer.put(value.toByte())
        } else {
            buffer.put((value and 0x7F or 0x80).toByte())
            if (value and (-0x1 shl 14) == 0) {
                buffer.put((value ushr 7 and 0xFF).toByte())
            } else {
                buffer.put((value ushr 7 and 0x7F or 0x80).toByte())
                if (value and (-0x1 shl 21) == 0) {
                    buffer.put((value ushr 14 and 0xFF).toByte())
                } else {
                    buffer.put((value ushr 14 and 0x7F or 0x80).toByte())
                    if (value and (-0x1 shl 28) == 0) {
                        buffer.put((value ushr 21 and 0xFF).toByte())
                    } else {
                        buffer.put((value ushr 21 and 0x7F or 0x80).toByte())
                        buffer.put((value ushr 28 and 0xFF).toByte())
                    }
                }
            }
        }
    }

    /**
     * Write the given integer following the variable-length unsigned encoding from
     * [Google Protocol Buffers](http://code.google.com/apis/protocolbuffers/docs/encoding.html)
     * into the buffer.
     *
     * For implementation notes, see [writeUnsignedVarint].
     *
     * @param value The value to write
     * @param out The output to write to
     */
    @Throws(IOException::class)
    fun writeUnsignedVarint(value: Int, out: DataOutput) {
        if (value and (-0x1 shl 7) == 0) out.writeByte(value)
        else {
            out.writeByte(value and 0x7F or 0x80)
            if (value and (-0x1 shl 14) == 0) out.writeByte(value ushr 7)
            else {
                out.writeByte(value ushr 7 and 0x7F or 0x80)
                if (value and (-0x1 shl 21) == 0) out.writeByte(value ushr 14)
                else {
                    out.writeByte((value ushr 14 and 0x7F or 0x80).toByte().toInt())
                    if (value and (-0x1 shl 28) == 0) out.writeByte(value ushr 21)
                    else {
                        out.writeByte(value ushr 21 and 0x7F or 0x80)
                        out.writeByte(value ushr 28)
                    }
                }
            }
        }
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
        val v = value shl 1 xor (value shr 63)
        writeUnsignedVarlong(v, buffer)
    }

    // visible for testing and benchmarking
    internal fun writeUnsignedVarlong(v: Long, buffer: ByteBuffer) {
        var v = v
        while (v and -0x80L != 0L) {
            val b = (v and 0x7fL or 0x80L).toByte()
            buffer.put(b)
            v = v ushr 7
        }
        buffer.put(v.toByte())
    }


    /**
     * Write the given float following the single-precision 32-bit format IEEE 754 value into the
     * output.
     *
     * @param value The value to write
     * @param out The output to write to
     */
    @Throws(IOException::class)
    fun writeFloat(value: Float, out: DataOutput) = out.writeFloat(value)

    /**
     * Write the given float following the single-precision 32-bit format IEEE 754 value into the
     * buffer.
     *
     * @param value The value to write
     * @param buffer The buffer to write to
     */
    fun writeFloat(value: Float, buffer: ByteBuffer) = buffer.putFloat(value)

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
        return sizeOfUnsignedVarlong(value shl 1 xor (value shr 63))
    }

    // visible for benchmarking
    internal fun sizeOfUnsignedVarlong(v: Long): Int {
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
