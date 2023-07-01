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

import java.io.IOException
import java.io.InputStream
import java.nio.ByteBuffer
import java.nio.ByteOrder
import net.jpountz.lz4.LZ4Exception
import net.jpountz.lz4.LZ4Factory
import net.jpountz.util.SafeUtils
import net.jpountz.xxhash.XXHashFactory
import org.apache.kafka.common.utils.BufferSupplier

/**
 * A partial implementation of the v1.5.1 LZ4 Frame format.
 *
 * @see [LZ4 Frame Format](https://github.com/lz4/lz4/wiki/lz4_Frame_format.md)
 *
 * This class is not thread-safe.
 */
class KafkaLZ4BlockInputStream(
    buffer: ByteBuffer,
    bufferSupplier: BufferSupplier,
    ignoreFlagDescriptorChecksum: Boolean
) : InputStream() {

    private val buffer: ByteBuffer

    private val ignoreFlagDescriptorChecksum: Boolean

    private val bufferSupplier: BufferSupplier

    private val decompressionBuffer: ByteBuffer

    // `flg` and `maxBlockSize` are effectively final, they are initialised in the `readHeader`
    // method that is only invoked from the constructor
    private var _flg: KafkaLZ4BlockOutputStream.FLG? = null
    private val flg: KafkaLZ4BlockOutputStream.FLG
        get() = _flg!!

    private var maxBlockSize = 0

    // If a block is compressed, this is the same as `decompressionBuffer`. If a block is not
    // compressed, this is a slice of `buffer` to avoid unnecessary copies.
    private var _decompressedBuffer: ByteBuffer? = null
    private val decompressedBuffer: ByteBuffer
        get() = _decompressedBuffer!!

    private var finished: Boolean

    /**
     * Create a new [InputStream] that will decompress data using the LZ4 algorithm.
     *
     * @param in The byte buffer to decompress
     * @param ignoreFlagDescriptorChecksum for compatibility with old kafka clients, ignore
     * incorrect HC byte
     * @throws IOException
     */
    init {
        if (BROKEN_LZ4_EXCEPTION != null) throw BROKEN_LZ4_EXCEPTION

        this.ignoreFlagDescriptorChecksum = ignoreFlagDescriptorChecksum
        this.buffer = buffer.duplicate().order(ByteOrder.LITTLE_ENDIAN)
        this.bufferSupplier = bufferSupplier

        readHeader()

        decompressionBuffer = bufferSupplier[maxBlockSize]!!
        finished = false
    }

    /**
     * Check whether KafkaLZ4BlockInputStream is configured to ignore the
     * Frame Descriptor checksum, which is useful for compatibility with
     * old client implementations that use incorrect checksum calculations.
     */
    fun ignoreFlagDescriptorChecksum(): Boolean {
        return ignoreFlagDescriptorChecksum
    }

    /**
     * Reads the magic number and frame descriptor from input buffer.
     *
     * @throws IOException
     */
    @Throws(IOException::class)
    private fun readHeader() {
        // read first 6 bytes into buffer to check magic and FLG/BD descriptor flags
        if (buffer.remaining() < 6) {
            throw IOException(PREMATURE_EOS)
        }
        if (KafkaLZ4BlockOutputStream.MAGIC != buffer.getInt()) {
            throw IOException(NOT_SUPPORTED)
        }
        // mark start of data to checksum
        buffer.mark()
        _flg = KafkaLZ4BlockOutputStream.FLG.fromByte(buffer.get())
        maxBlockSize = KafkaLZ4BlockOutputStream.BD.fromByte(buffer.get()).blockMaximumSize
        if (flg.isContentSizeSet) {
            if (buffer.remaining() < 8) {
                throw IOException(PREMATURE_EOS)
            }
            buffer.position(buffer.position() + 8)
        }

        // Final byte of Frame Descriptor is HC checksum

        // Old implementations produced incorrect HC checksums
        if (ignoreFlagDescriptorChecksum) {
            buffer.position(buffer.position() + 1)
            return
        }
        val len = buffer.position() - buffer.reset().position()
        val hash = CHECKSUM.hash(buffer, buffer.position(), len, 0)
        buffer.position(buffer.position() + len)
        if (buffer.get() != (hash shr 8 and 0xFF).toByte()) {
            throw IOException(DESCRIPTOR_HASH_MISMATCH)
        }
    }

    /**
     * Decompresses (if necessary) buffered data, optionally computes and validates a XXHash32 checksum, and writes the
     * result to a buffer.
     *
     * @throws IOException
     */
    @Throws(IOException::class)
    private fun readBlock() {
        if (buffer.remaining() < 4) throw IOException(PREMATURE_EOS)

        var blockSize = buffer.getInt()
        val compressed = blockSize and KafkaLZ4BlockOutputStream.LZ4_FRAME_INCOMPRESSIBLE_MASK == 0
        blockSize = blockSize and KafkaLZ4BlockOutputStream.LZ4_FRAME_INCOMPRESSIBLE_MASK.inv()

        // Check for EndMark
        if (blockSize == 0) {
            finished = true
            if (flg.isContentChecksumSet) buffer.getInt() // TODO: verify this content checksum
            return
        } else if (blockSize > maxBlockSize)
            throw IOException("Block size $blockSize exceeded max: $maxBlockSize")

        if (buffer.remaining() < blockSize) {
            throw IOException(PREMATURE_EOS)
        }

        _decompressedBuffer = if (compressed) {
            try {
                val bufferSize = DECOMPRESSOR.decompress(
                    buffer, buffer.position(), blockSize, decompressionBuffer, 0,
                    maxBlockSize
                )
                decompressionBuffer.position(0)
                decompressionBuffer.limit(bufferSize)
                decompressionBuffer
            } catch (e: LZ4Exception) {
                throw IOException(e)
            }
        } else buffer.slice().also { it.limit(blockSize) }

        // verify checksum
        if (flg.isBlockChecksumSet) {
            val hash = CHECKSUM.hash(buffer, buffer.position(), blockSize, 0)
            buffer.position(buffer.position() + blockSize)

            if (hash != buffer.getInt()) throw IOException(BLOCK_HASH_MISMATCH)

        } else buffer.position(buffer.position() + blockSize)
    }

    @Throws(IOException::class)
    override fun read(): Int {
        if (finished) {
            return -1
        }
        if (available() == 0) {
            readBlock()
        }
        return if (finished) {
            -1
        } else decompressedBuffer!!.get().toInt() and 0xFF
    }

    @Throws(IOException::class)
    override fun read(bytes: ByteArray, offset: Int, length: Int): Int {
        var len = length
        SafeUtils.checkRange(bytes, offset, len)
        if (finished) {
            return -1
        }
        if (available() == 0) {
            readBlock()
        }
        if (finished) {
            return -1
        }
        len = len.coerceAtMost(available())
        decompressedBuffer[bytes, offset, len]
        return len
    }

    @Throws(IOException::class)
    override fun skip(n: Long): Long {
        if (finished) return 0
        if (available() == 0) readBlock()
        if (finished) return 0

        val skipped = n.coerceAtMost(available().toLong()).toInt()
        decompressedBuffer.position(decompressedBuffer.position() + skipped)

        return skipped.toLong()
    }

    override fun available(): Int =
        _decompressedBuffer?.remaining() ?: 0

    override fun close() = bufferSupplier.release(decompressionBuffer)

    override fun mark(readlimit: Int) = throw RuntimeException("mark not supported")

    override fun reset() = throw RuntimeException("reset not supported")

    override fun markSupported(): Boolean = false

    companion object {

        const val PREMATURE_EOS = "Stream ended prematurely"

        const val NOT_SUPPORTED = "Stream unsupported (invalid magic bytes)"

        const val BLOCK_HASH_MISMATCH = "Block checksum mismatch"

        const val DESCRIPTOR_HASH_MISMATCH = "Stream frame descriptor corrupted"

        private val DECOMPRESSOR = LZ4Factory.fastestInstance().safeDecompressor()

        private val CHECKSUM = XXHashFactory.fastestInstance().hash32()

        private val BROKEN_LZ4_EXCEPTION: RuntimeException?

        // https://issues.apache.org/jira/browse/KAFKA-9203
        // detect buggy lz4 libraries on the classpath
        init {
            var exception: RuntimeException? = null
            try {
                detectBrokenLz4Version()
            } catch (e: RuntimeException) {
                exception = e
            }
            BROKEN_LZ4_EXCEPTION = exception
        }

        /**
         * Checks whether the version of lz4 on the classpath has the fix for reading from ByteBuffers with
         * non-zero array offsets (see https://github.com/lz4/lz4-java/pull/65)
         */
        fun detectBrokenLz4Version() {
            val source = byteArrayOf(1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 3, 3, 3, 3, 3)
            val compressor = LZ4Factory.fastestInstance().fastCompressor()
            val compressed = ByteArray(compressor.maxCompressedLength(source.size))
            val compressedLength = compressor.compress(
                source, 0, source.size, compressed, 0,
                compressed.size
            )

            // allocate an array-backed ByteBuffer with non-zero array-offset containing the compressed data
            // a buggy decompressor will read the data from the beginning of the underlying array instead of
            // the beginning of the ByteBuffer, failing to decompress the invalid data.
            val zeroes = byteArrayOf(0, 0, 0, 0, 0)
            val nonZeroOffsetBuffer = ByteBuffer
                // allocate the backing array with extra space to offset the data
                .allocate(zeroes.size + compressed.size)
                // prepend invalid bytes (zeros) before the compressed data in the array
                .put(zeroes)
                // create a new ByteBuffer sharing the underlying array, offset to start on the compressed data
                .slice()
                // write the compressed data at the beginning of this new buffer
                .put(compressed)

            val dest = ByteBuffer.allocate(source.size)
            try {
                DECOMPRESSOR.decompress(
                    nonZeroOffsetBuffer,
                    0,
                    compressedLength,
                    dest,
                    0,
                    source.size
                )
            } catch (exception: Exception) {
                throw RuntimeException(
                    "Kafka has detected detected a buggy lz4-java library (< 1.4.x) on the classpath."
                            + " If you are using Kafka client libraries, make sure your application does not"
                            + " accidentally override the version provided by Kafka or include multiple versions"
                            + " of the library on the classpath. The lz4-java version on the classpath should"
                            + " match the version the Kafka client libraries depend on. Adding -verbose:class"
                            + " to your JVM arguments may help understand which lz4-java version is getting loaded.",
                    exception
                )
            }
        }
    }
}
