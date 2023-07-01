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

package org.apache.kafka.common.record

import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.compress.KafkaLZ4BlockInputStream
import org.apache.kafka.common.compress.KafkaLZ4BlockOutputStream
import org.apache.kafka.common.compress.SnappyFactory.wrapForInput
import org.apache.kafka.common.compress.SnappyFactory.wrapForOutput
import org.apache.kafka.common.compress.ZstdFactory
import org.apache.kafka.common.compress.ZstdFactory.wrapForOutput
import org.apache.kafka.common.utils.BufferSupplier
import org.apache.kafka.common.utils.ByteBufferInputStream
import org.apache.kafka.common.utils.ByteBufferOutputStream
import java.io.BufferedInputStream
import java.io.BufferedOutputStream
import java.io.InputStream
import java.io.OutputStream
import java.nio.ByteBuffer
import java.util.zip.GZIPInputStream
import java.util.zip.GZIPOutputStream

/**
 * The compression type to use
 */
enum class CompressionType(
    val id: Int,
    val _name: String,
    val rate: Float,
) {
    NONE(
        id = 0,
        _name = "none",
        rate = 1.0f
    ) {
        override fun wrapForOutput(
            bufferStream: ByteBufferOutputStream,
            messageVersion: Byte
        ): OutputStream = bufferStream

        override fun wrapForInput(
            buffer: ByteBuffer,
            messageVersion: Byte,
            decompressionBufferSupplier: BufferSupplier,
        ): InputStream = ByteBufferInputStream(buffer)
    },

    // Shipped with the JDK
    GZIP(
        id = 1,
        _name = "gzip",
        rate = 1.0f
    ) {
        override fun wrapForOutput(
            bufferStream: ByteBufferOutputStream,
            messageVersion: Byte,
        ): OutputStream {
            return try {
                // Set input buffer (uncompressed) to 16 KB (none by default) and output buffer
                // (compressed) to 8 KB (0.5 KB by default) to ensure reasonable performance in
                // cases where the caller passes a small number of bytes to write (potentially a
                // single byte)
                BufferedOutputStream(GZIPOutputStream(bufferStream, 8 * 1024), 16 * 1024)
            } catch (e: Exception) {
                throw KafkaException(cause = e)
            }
        }

        override fun wrapForInput(
            buffer: ByteBuffer,
            messageVersion: Byte,
            decompressionBufferSupplier: BufferSupplier,
        ): InputStream {
            return try {
                // Set output buffer (uncompressed) to 16 KB (none by default) and input buffer
                // (compressed) to 8 KB (0.5 KB by default) to ensure reasonable performance in
                // cases where the caller reads a small number of bytes (potentially a single byte)
                BufferedInputStream(
                    GZIPInputStream(ByteBufferInputStream(buffer), 8 * 1024),
                    16 * 1024,
                )
            } catch (e: Exception) {
                throw KafkaException(cause = e)
            }
        }
    },

    // We should only load classes from a given compression library when we actually use said
    // compression library. This is because compression libraries include native code for a set of
    // platforms and we want to avoid errors in case the platform is not supported and the
    // compression library is not actually used. To ensure this, we only reference compression
    // library code from classes that are only invoked when actual usage happens.
    SNAPPY(
        id = 2,
        _name = "snappy",
        rate = 1.0f
    ) {
        override fun wrapForOutput(
            bufferStream: ByteBufferOutputStream,
            messageVersion: Byte
        ): OutputStream = wrapForOutput(bufferStream)

        override fun wrapForInput(
            buffer: ByteBuffer,
            messageVersion: Byte,
            decompressionBufferSupplier: BufferSupplier,
        ): InputStream = wrapForInput(buffer)
    },
    LZ4(
        id = 3,
        _name = "lz4",
        rate = 1.0f
    ) {
        override fun wrapForOutput(
            bufferStream: ByteBufferOutputStream,
            messageVersion: Byte
        ): OutputStream {
            return try {
                KafkaLZ4BlockOutputStream(
                    out = bufferStream,
                    useBrokenHC = messageVersion == RecordBatch.MAGIC_VALUE_V0,
                )
            } catch (e: Throwable) {
                throw KafkaException(cause = e)
            }
        }

        override fun wrapForInput(
            buffer: ByteBuffer,
            messageVersion: Byte,
            decompressionBufferSupplier: BufferSupplier
        ): InputStream {
            return try {
                KafkaLZ4BlockInputStream(
                    buffer = buffer,
                    bufferSupplier = decompressionBufferSupplier,
                    ignoreFlagDescriptorChecksum = messageVersion == RecordBatch.MAGIC_VALUE_V0,
                )
            } catch (e: Throwable) {
                throw KafkaException(cause = e)
            }
        }
    },
    ZSTD(
        id = 4,
        _name = "zstd",
        rate = 1.0f
    ) {
        override fun wrapForOutput(
            bufferStream: ByteBufferOutputStream,
            messageVersion: Byte
        ): OutputStream = wrapForOutput(bufferStream)

        override fun wrapForInput(
            buffer: ByteBuffer,
            messageVersion: Byte,
            decompressionBufferSupplier: BufferSupplier
        ): InputStream = ZstdFactory.wrapForInput(
            buffer, messageVersion,
            decompressionBufferSupplier
        )
    };

    /**
     * Wrap bufferStream with an OutputStream that will compress data with this CompressionType.
     *
     * Note: Unlike [wrapForInput], [wrapForOutput] cannot take [ByteBuffer]s directly. Currently,
     * [MemoryRecordsBuilder.writeDefaultBatchHeader] and
     * [MemoryRecordsBuilder.writeLegacyCompressedWrapperHeader] write to the underlying buffer in
     * the given [ByteBufferOutputStream] after the compressed data has been written. In the event
     * that the buffer needs to be expanded while writing the data, access to the underlying buffer
     * needs to be preserved.
     */
    abstract fun wrapForOutput(
        bufferStream: ByteBufferOutputStream,
        messageVersion: Byte
    ): OutputStream

    /**
     * Wrap buffer with an InputStream that will decompress data with this CompressionType.
     *
     * @param decompressionBufferSupplier The supplier of ByteBuffer(s) used for decompression if
     * supported. For small record batches, allocating a potentially large buffer (64 KB for LZ4)
     * will dominate the cost of decompressing and iterating over the records in the batch. As such,
     * a supplier that reuses buffers will have a significant performance impact.
     */
    abstract fun wrapForInput(
        buffer: ByteBuffer,
        messageVersion: Byte,
        decompressionBufferSupplier: BufferSupplier
    ): InputStream

    override fun toString(): String = _name

    companion object {
        
        fun forId(id: Int): CompressionType {
            return when (id) {
                0 -> NONE
                1 -> GZIP
                2 -> SNAPPY
                3 -> LZ4
                4 -> ZSTD
                else -> throw IllegalArgumentException("Unknown compression type id: $id")
            }
        }

        fun forName(name: String): CompressionType {
            return requireNotNull(
                if (NONE._name == name) NONE
                else if (GZIP._name == name) GZIP
                else if (SNAPPY._name == name) SNAPPY
                else if (LZ4._name == name) LZ4
                else if (ZSTD._name == name) ZSTD
                else null
            ) { "Unknown compression name: $name" }
        }
    }
}
