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

package org.apache.kafka.common.compress

import net.jpountz.xxhash.XXHashFactory
import org.apache.kafka.common.utils.BufferSupplier
import org.junit.jupiter.api.extension.ExtensionContext
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.ArgumentsProvider
import org.junit.jupiter.params.provider.ArgumentsSource
import java.io.ByteArrayOutputStream
import java.io.IOException
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.util.stream.Stream
import kotlin.math.min
import kotlin.random.Random
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

class KafkaLZ4Test {

    data class Payload(var name: String, var payload: ByteArray) {

        override fun toString(): String {
            return "Payload{size=${payload.size}, name='$name'}"
        }

        override fun equals(other: Any?): Boolean {
            if (this === other) return true
            if (javaClass != other?.javaClass) return false

            other as Payload

            if (name != other.name) return false
            if (!payload.contentEquals(other.payload)) return false

            return true
        }

        override fun hashCode(): Int {
            var result = name.hashCode()
            result = 31 * result + payload.contentHashCode()
            return result
        }
    }

    class Args(
        val useBrokenFlagDescriptorChecksum: Boolean,
        val ignoreFlagDescriptorChecksum: Boolean,
        val blockChecksum: Boolean,
        val close: Boolean,
        payload: Payload,
    ) {

        val payload: ByteArray

        init {
            this.payload = payload.payload
        }

        override fun toString(): String {
            return "useBrokenFlagDescriptorChecksum=$useBrokenFlagDescriptorChecksum, " +
                    "ignoreFlagDescriptorChecksum=$ignoreFlagDescriptorChecksum, " +
                    "blockChecksum=$blockChecksum, " +
                    "close=$close, " +
                    "payload=${payload.contentToString()}"
        }
    }

    private class Lz4ArgumentsProvider : ArgumentsProvider {

        override fun provideArguments(context: ExtensionContext): Stream<out Arguments> {
            val payloads: MutableList<Payload> = ArrayList()
            payloads.add(Payload("empty", ByteArray(0)))
            payloads.add(Payload("onebyte", byteArrayOf(1)))
            for (size in listOf(1000, 1 shl 16, (1 shl 10) * 96)) {
                val random = ByteArray(size)
                RANDOM.nextBytes(random)
                payloads.add(Payload("random", random))
                val ones = ByteArray(size)
                ones.fill(1.toByte())
                payloads.add(Payload("ones", ones))
            }
            val arguments = mutableListOf<Arguments>()
            for (payload in payloads)
                for (broken in listOf(false, true))
                    for (ignore in listOf(false, true))
                        for (blockChecksum in listOf(false, true))
                            for (close in listOf(false, true))
                                arguments.add(
                                    Arguments.of(
                                        Args(broken, ignore, blockChecksum, close, payload)
                                    )
                                )
            return arguments.stream()
        }
    }

    @ParameterizedTest
    @ArgumentsSource(Lz4ArgumentsProvider::class)
    fun testHeaderPrematureEnd(args: Args) {
        val buffer = ByteBuffer.allocate(2)
        val e = assertFailsWith<IOException> { makeInputStream(buffer, args.ignoreFlagDescriptorChecksum) }
        assertEquals(KafkaLZ4BlockInputStream.PREMATURE_EOS, e.message)
    }

    @Throws(IOException::class)
    private fun makeInputStream(buffer: ByteBuffer, ignoreFlagDescriptorChecksum: Boolean): KafkaLZ4BlockInputStream {
        return KafkaLZ4BlockInputStream(buffer, BufferSupplier.create(), ignoreFlagDescriptorChecksum)
    }

    @ParameterizedTest
    @ArgumentsSource(Lz4ArgumentsProvider::class)
    @Throws(Exception::class)
    fun testNotSupported(args: Args) {
        val compressed = compressedBytes(args)
        compressed[0] = 0x00
        val buffer = ByteBuffer.wrap(compressed)
        val e = assertFailsWith<IOException> { makeInputStream(buffer, args.ignoreFlagDescriptorChecksum) }
        assertEquals(KafkaLZ4BlockInputStream.NOT_SUPPORTED, e.message)
    }

    @ParameterizedTest
    @ArgumentsSource(Lz4ArgumentsProvider::class)
    @Throws(Exception::class)
    fun testBadFrameChecksum(args: Args) {
        val compressed = compressedBytes(args)
        compressed[6] = 0xFF.toByte()
        val buffer = ByteBuffer.wrap(compressed)
        if (args.ignoreFlagDescriptorChecksum) makeInputStream(buffer, args.ignoreFlagDescriptorChecksum)
        else {
            val e = assertFailsWith<IOException> {
                makeInputStream(
                    buffer = buffer,
                    ignoreFlagDescriptorChecksum = args.ignoreFlagDescriptorChecksum,
                )
            }
            assertEquals(KafkaLZ4BlockInputStream.DESCRIPTOR_HASH_MISMATCH, e.message)
        }
    }

    @ParameterizedTest
    @ArgumentsSource(Lz4ArgumentsProvider::class)
    @Throws(Exception::class)
    fun testBadBlockSize(args: Args) {
        if (!args.close || args.useBrokenFlagDescriptorChecksum && !args.ignoreFlagDescriptorChecksum) return
        val compressed = compressedBytes(args)
        val buffer = ByteBuffer.wrap(compressed).order(ByteOrder.LITTLE_ENDIAN)
        var blockSize = buffer.getInt(7)
        blockSize = blockSize and
                KafkaLZ4BlockOutputStream.LZ4_FRAME_INCOMPRESSIBLE_MASK or
                (1 shl 24 and KafkaLZ4BlockOutputStream.LZ4_FRAME_INCOMPRESSIBLE_MASK.inv())
        buffer.putInt(7, blockSize)
        val e = assertFailsWith<IOException> { testDecompression(buffer, args) }
        assertTrue(e.message!!.contains("exceeded max"))
    }

    @ParameterizedTest
    @ArgumentsSource(Lz4ArgumentsProvider::class)
    @Throws(Exception::class)
    fun testCompression(args: Args) {
        val compressed = compressedBytes(args)

        // Check magic bytes stored as little-endian
        var offset = 0
        assertEquals(0x04, compressed[offset++].toInt())
        assertEquals(0x22, compressed[offset++].toInt())
        assertEquals(0x4D, compressed[offset++].toInt())
        assertEquals(0x18, compressed[offset++].toInt())

        // Check flg descriptor
        val flg = compressed[offset++]

        // 2-bit version must be 01
        val version = flg.toInt() ushr 6 and 3
        assertEquals(1, version)

        // Reserved bits should always be 0
        var reserved = flg.toInt() and 3
        assertEquals(0, reserved)

        // Check block descriptor
        val bd = compressed[offset++]

        // Block max-size
        val blockMaxSize = bd.toInt() ushr 4 and 7
        // Only supported values are 4 (64KB), 5 (256KB), 6 (1MB), 7 (4MB)
        assertTrue(blockMaxSize >= 4)
        assertTrue(blockMaxSize <= 7)

        // Multiple reserved bit ranges in block descriptor
        reserved = bd.toInt() and 15
        assertEquals(0, reserved)
        reserved = bd.toInt() ushr 7 and 1
        assertEquals(0, reserved)

        // If flg descriptor sets content size flag
        // there are 8 additional bytes before checksum
        val contentSize = flg.toInt() ushr 3 and 1 != 0
        if (contentSize) offset += 8

        // Checksum applies to frame descriptor: flg, bd, and optional contentsize
        // so initial offset should be 4 (for magic bytes)
        var off = 4
        var len = offset - 4

        // Initial implementation of checksum incorrectly applied to full header
        // including magic bytes
        if (args.useBrokenFlagDescriptorChecksum) {
            off = 0
            len = offset
        }
        val hash = XXHashFactory.fastestInstance().hash32().hash(compressed, off, len, 0)
        val hc = compressed[offset++]
        assertEquals((hash shr 8 and 0xFF).toByte(), hc)

        // Check EndMark, data block with size `0` expressed as a 32-bits value
        if (args.close) {
            offset = compressed.size - 4
            assertEquals(0, compressed[offset++].toInt())
            assertEquals(0, compressed[offset++].toInt())
            assertEquals(0, compressed[offset++].toInt())
            assertEquals(0, compressed[offset++].toInt())
        }
    }

    @ParameterizedTest
    @ArgumentsSource(Lz4ArgumentsProvider::class)
    @Throws(IOException::class)
    fun testArrayBackedBuffer(args: Args) {
        val compressed = compressedBytes(args)
        testDecompression(ByteBuffer.wrap(compressed), args)
    }

    @ParameterizedTest
    @ArgumentsSource(Lz4ArgumentsProvider::class)
    @Throws(IOException::class)
    fun testArrayBackedBufferSlice(args: Args) {
        val compressed = compressedBytes(args)
        val sliceOffset = 12
        var buffer = ByteBuffer.allocate(compressed.size + sliceOffset + 123)
        buffer.position(sliceOffset)
        buffer.put(compressed).flip()
        buffer.position(sliceOffset)
        var slice = buffer.slice()
        testDecompression(slice, args)
        val offset = 42
        buffer = ByteBuffer.allocate(compressed.size + sliceOffset + offset)
        buffer.position(sliceOffset + offset)
        buffer.put(compressed).flip()
        buffer.position(sliceOffset)
        slice = buffer.slice()
        slice.position(offset)
        testDecompression(slice, args)
    }

    @ParameterizedTest
    @ArgumentsSource(Lz4ArgumentsProvider::class)
    @Throws(IOException::class)
    fun testDirectBuffer(args: Args) {
        val compressed = compressedBytes(args)
        var buffer: ByteBuffer = ByteBuffer.allocateDirect(compressed.size)
        buffer.put(compressed).flip()
        testDecompression(buffer, args)
        val offset = 42
        buffer = ByteBuffer.allocateDirect(compressed.size + offset + 123)
        buffer.position(offset)
        buffer.put(compressed).flip()
        buffer.position(offset)
        testDecompression(buffer, args)
    }

    @ParameterizedTest
    @ArgumentsSource(Lz4ArgumentsProvider::class)
    @Throws(Exception::class)
    fun testSkip(args: Args) {
        if (!args.close || args.useBrokenFlagDescriptorChecksum && !args.ignoreFlagDescriptorChecksum) return
        val inputStream = makeInputStream(
            ByteBuffer.wrap(compressedBytes(args)),
            args.ignoreFlagDescriptorChecksum
        )
        var n = 100
        var remaining = args.payload.size.toLong()
        var skipped = inputStream.skip(n.toLong())
        assertEquals(remaining.coerceAtMost(n.toLong()), skipped)
        n = 10000
        remaining -= skipped
        skipped = inputStream.skip(n.toLong())
        assertEquals(remaining.coerceAtMost(n.toLong()), skipped)
    }

    @Throws(IOException::class)
    private fun testDecompression(buffer: ByteBuffer, args: Args) {
        var error: IOException? = null
        try {
            val decompressed = makeInputStream(buffer, args.ignoreFlagDescriptorChecksum)
            val testPayload = ByteArray(args.payload.size)
            val tmp = ByteArray(1024)
            var n: Int
            var pos = 0
            var i = 0
            while (decompressed.read(tmp, i, tmp.size - i).also { n = it } != -1) {
                i += n
                if (i == tmp.size) {
                    System.arraycopy(tmp, 0, testPayload, pos, i)
                    pos += i
                    i = 0
                }
            }
            System.arraycopy(tmp, 0, testPayload, pos, i)
            pos += i
            assertEquals(-1, decompressed.read(tmp, 0, tmp.size))
            assertEquals(args.payload.size, pos)
            assertContentEquals(args.payload, testPayload)
        } catch (e: IOException) {
            error = if (!args.ignoreFlagDescriptorChecksum && args.useBrokenFlagDescriptorChecksum) {
                assertEquals(KafkaLZ4BlockInputStream.DESCRIPTOR_HASH_MISMATCH, e.message)
                e
            } else if (!args.close) {
                assertEquals(KafkaLZ4BlockInputStream.PREMATURE_EOS, e.message)
                e
            } else throw e
        }
        if (!args.ignoreFlagDescriptorChecksum && args.useBrokenFlagDescriptorChecksum) assertNotNull(error)
        if (!args.close) assertNotNull(error)
    }

    @Throws(IOException::class)
    private fun compressedBytes(args: Args): ByteArray {
        val output = ByteArrayOutputStream()
        val lz4 = KafkaLZ4BlockOutputStream(
            out = output,
            blockSize = KafkaLZ4BlockOutputStream.BLOCKSIZE_64KB,
            blockChecksum = args.blockChecksum,
            useBrokenFlagDescriptorChecksum = args.useBrokenFlagDescriptorChecksum,
        )
        lz4.write(args.payload, 0, args.payload.size)
        if (args.close) lz4.close()
        else lz4.flush()

        return output.toByteArray()
    }

    companion object {
        private val RANDOM = Random(0)
    }
}
