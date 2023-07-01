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
import java.io.OutputStream
import net.jpountz.lz4.LZ4Compressor
import net.jpountz.lz4.LZ4Factory
import net.jpountz.util.SafeUtils
import net.jpountz.xxhash.XXHash32
import net.jpountz.xxhash.XXHashFactory
import org.apache.kafka.common.utils.ByteUtils

/**
 * A partial implementation of the v1.5.1 LZ4 Frame format.
 *
 * @see [LZ4 Frame Format](https://github.com/lz4/lz4/wiki/lz4_Frame_format.md)
 *
 * This class is not thread-safe.
 *
 * @constructor Creates a new [OutputStream] that will compress data using the LZ4 algorithm.
 * @param out The output stream to compress
 * @param blockSize Default: 4. The block size used during compression. 4=64kb, 5=256kb, 6=1mb,
 * 7=4mb. All other values will generate an exception
 * @param blockChecksum Default: false. When true, a XXHash32 checksum is computed and appended to
 * the stream for every block of data
 * @param useBrokenFlagDescriptorChecksum Default: false. When true, writes an incorrect
 * FrameDescriptor checksum compatible with older kafka clients.
 * @throws IOException
 */
class KafkaLZ4BlockOutputStream(
    out: OutputStream?,
    blockSize: Int = BLOCKSIZE_64KB,
    blockChecksum: Boolean = false,
    private val useBrokenFlagDescriptorChecksum: Boolean = false
) : OutputStream() {

    private var _out: OutputStream? = out
    private val out: OutputStream
        get() = _out!!

    private val compressor: LZ4Compressor = LZ4Factory.fastestInstance().fastCompressor()

    private val checksum: XXHash32 = XXHashFactory.fastestInstance().hash32()

    private val flg: FLG = FLG(blockChecksum)

    private val bd: BD = BD(blockSize = blockSize)

    private val maxBlockSize: Int = bd.blockMaximumSize

    private var _buffer: ByteArray? = ByteArray(maxBlockSize)
    private val buffer: ByteArray
        get() = _buffer!!

    private var _compressedBuffer: ByteArray? =
        ByteArray(compressor.maxCompressedLength(maxBlockSize))

    private val compressedBuffer: ByteArray
        get() = _compressedBuffer!!

    private var bufferOffset = 0

    private var finished = false

    init {
        writeHeader()
    }

    constructor(out: OutputStream?, useBrokenHC: Boolean) : this(
        out,
        BLOCKSIZE_64KB,
        false,
        useBrokenHC
    )

    /**
     * Check whether KafkaLZ4BlockInputStream is configured to write an
     * incorrect Frame Descriptor checksum, which is useful for
     * compatibility with old client implementations.
     */
    fun useBrokenFlagDescriptorChecksum(): Boolean {
        return useBrokenFlagDescriptorChecksum
    }

    /**
     * Writes the magic number and frame descriptor to the underlying [OutputStream].
     *
     * @throws IOException
     */
    @Throws(IOException::class)
    private fun writeHeader() {
        ByteUtils.writeUnsignedIntLE(buffer, 0, MAGIC)
        bufferOffset = 4
        buffer[bufferOffset++] = flg.toByte()
        buffer[bufferOffset++] = bd.toByte()
        // TODO write uncompressed content size, update flg.validate()

        // compute checksum on all descriptor fields
        var offset = 4
        var len = bufferOffset - offset
        if (useBrokenFlagDescriptorChecksum) {
            len += offset
            offset = 0
        }
        val hash = (checksum.hash(buffer, offset, len, 0) shr 8 and 0xFF).toByte()
        buffer[bufferOffset++] = hash

        // write out frame descriptor
        out.write(buffer, 0, bufferOffset)
        bufferOffset = 0
    }

    /**
     * Compresses buffered data, optionally computes an XXHash32 checksum, and writes the result to the underlying
     * [OutputStream].
     *
     * @throws IOException
     */
    @Throws(IOException::class)
    private fun writeBlock() {
        if (bufferOffset == 0) {
            return
        }
        var compressedLength = compressor.compress(buffer, 0, bufferOffset, compressedBuffer, 0)
        var bufferToWrite = compressedBuffer
        var compressMethod = 0

        // Store block uncompressed if compressed length is greater (incompressible)
        if (compressedLength >= bufferOffset) {
            bufferToWrite = buffer
            compressedLength = bufferOffset
            compressMethod = LZ4_FRAME_INCOMPRESSIBLE_MASK
        }

        // Write content
        ByteUtils.writeUnsignedIntLE(out, compressedLength or compressMethod)
        out.write(bufferToWrite, 0, compressedLength)

        // Calculate and write block checksum
        if (flg.isBlockChecksumSet) {
            val hash = checksum.hash(bufferToWrite, 0, compressedLength, 0)
            ByteUtils.writeUnsignedIntLE(out, hash)
        }
        bufferOffset = 0
    }

    /**
     * Similar to the [.writeBlock] method. Writes a 0-length block (without block checksum) to signal the end
     * of the block stream.
     *
     * @throws IOException
     */
    @Throws(IOException::class)
    private fun writeEndMark() {
        ByteUtils.writeUnsignedIntLE(out, 0)
        // TODO implement content checksum, update flg.validate()
    }

    @Throws(IOException::class)
    override fun write(b: Int) {
        ensureNotFinished()
        if (bufferOffset == maxBlockSize) {
            writeBlock()
        }
        buffer[bufferOffset++] = b.toByte()
    }

    @Throws(IOException::class)
    override fun write(b: ByteArray, offset: Int, length: Int) {
        var off = offset
        var len = length

        SafeUtils.checkRange(b, off, len)
        ensureNotFinished()
        var bufferRemainingLength = maxBlockSize - bufferOffset

        // while b will fill the buffer
        while (len > bufferRemainingLength) {
            // fill remaining space in buffer
            System.arraycopy(b, off, buffer, bufferOffset, bufferRemainingLength)
            bufferOffset = maxBlockSize
            writeBlock()
            // compute new offset and length
            off += bufferRemainingLength
            len -= bufferRemainingLength
            bufferRemainingLength = maxBlockSize
        }

        System.arraycopy(b, off, buffer, bufferOffset, len)
        bufferOffset += len
    }

    @Throws(IOException::class)
    override fun flush() {
        if (!finished) writeBlock()
        _out?.flush()
    }

    /**
     * A simple state check to ensure the stream is still open.
     */
    private fun ensureNotFinished() {
        check(!finished) { CLOSED_STREAM }
    }

    @Throws(IOException::class)
    override fun close() {
        try {
            if (!finished) {
                // basically flush the buffer writing the last block
                writeBlock()
                // write the end block
                writeEndMark()
            }
        } finally {
            try {
                _out?.use { outputStream -> outputStream.flush() }
            } finally {
                _out = null
                _buffer = null
                _compressedBuffer = null
                finished = true
            }
        }
    }

    class FLG private constructor(
        private val reserved: Int,
        private val contentChecksum: Int,
        private val contentSize: Int,
        private val blockChecksum: Int,
        private val blockIndependence: Int,
        val version: Int
    ) {

        constructor(blockChecksum: Boolean = false) : this(
            reserved = 0,
            contentChecksum = 0,
            contentSize = 0,
            blockChecksum = if (blockChecksum) 1 else 0,
            blockIndependence = 1,
            version = VERSION
        )

        init {
            if (reserved != 0) throw RuntimeException("Reserved bits must be 0")
            if (blockIndependence != 1) throw RuntimeException("Dependent block stream is unsupported")
            if (version != VERSION) throw RuntimeException("Version $version is unsupported")
        }

        fun toByte(): Byte {
            return (reserved and 3 shl 0
                    or (contentChecksum and 1 shl 2)
                    or (contentSize and 1 shl 3)
                    or (blockChecksum and 1 shl 4)
                    or (blockIndependence and 1 shl 5)
                    or (version and 3 shl 6)
                    ).toByte()
        }

        val isContentChecksumSet: Boolean
            get() = contentChecksum == 1

        val isContentSizeSet: Boolean
            get() = contentSize == 1

        val isBlockChecksumSet: Boolean
            get() = blockChecksum == 1

        val isBlockIndependenceSet: Boolean
            get() = blockIndependence == 1

        companion object {

            private const val VERSION = 1

            fun fromByte(flg: Byte): FLG {
                val reserved = flg.toInt() ushr 0 and 3
                val contentChecksum = flg.toInt() ushr 2 and 1
                val contentSize = flg.toInt() ushr 3 and 1
                val blockChecksum = flg.toInt() ushr 4 and 1
                val blockIndependence = flg.toInt() ushr 5 and 1
                val version = flg.toInt() ushr 6 and 3
                return FLG(
                    reserved,
                    contentChecksum,
                    contentSize,
                    blockChecksum,
                    blockIndependence,
                    version
                )
            }
        }
    }

    class BD private constructor(
        private val reserved2: Int = 0,
        private val blockSizeValue: Int = BLOCKSIZE_64KB,
        private val reserved3: Int = 0,
    ) {

        constructor(blockSize: Int = BLOCKSIZE_64KB) : this(blockSizeValue = blockSize)

        init {
            if (reserved2 != 0) throw RuntimeException("Reserved2 field must be 0")
            if (blockSizeValue < MIN_BLOCK_SIZE || blockSizeValue > MAX_BLOCK_SIZE)
                throw RuntimeException("Block size value must be between 4 and 7")
            if (reserved3 != 0) throw RuntimeException("Reserved3 field must be 0")
        }

        @Deprecated(
            message = "Use property instead",
            replaceWith = ReplaceWith("blockMaximumSize"),
        )
        fun getBlockMaximumSize(): Int = blockMaximumSize

        val blockMaximumSize: Int
            // 2^(2n+8)
            get() = 1 shl 2 * blockSizeValue + 8

        fun toByte(): Byte {
            return (reserved2 and 15 shl 0 or (blockSizeValue and 7 shl 4) or (reserved3 and 1 shl 7)).toByte()
        }

        companion object {
            fun fromByte(bd: Byte): BD {
                val reserved2 = bd.toInt() ushr 0 and 15
                val blockMaximumSize = bd.toInt() ushr 4 and 7
                val reserved3 = bd.toInt() ushr 7 and 1
                return BD(reserved2, blockMaximumSize, reserved3)
            }
        }
    }

    companion object {

        const val MAGIC = 0x184D2204

        const val LZ4_MAX_HEADER_LENGTH = 19

        const val LZ4_FRAME_INCOMPRESSIBLE_MASK = -0x80000000

        const val CLOSED_STREAM = "The stream is already closed"

        const val BLOCKSIZE_64KB = 4

        const val BLOCKSIZE_256KB = 5

        const val BLOCKSIZE_1MB = 6

        const val BLOCKSIZE_4MB = 7

        private const val MIN_BLOCK_SIZE = BLOCKSIZE_64KB

        private const val MAX_BLOCK_SIZE = BLOCKSIZE_4MB
    }
}
