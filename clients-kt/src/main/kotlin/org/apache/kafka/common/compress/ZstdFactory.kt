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

import com.github.luben.zstd.BufferPool
import com.github.luben.zstd.RecyclingBufferPool
import com.github.luben.zstd.ZstdInputStreamNoFinalizer
import com.github.luben.zstd.ZstdOutputStreamNoFinalizer
import java.io.BufferedInputStream
import java.io.BufferedOutputStream
import java.io.InputStream
import java.io.OutputStream
import java.nio.ByteBuffer
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.utils.BufferSupplier
import org.apache.kafka.common.utils.ByteBufferInputStream
import org.apache.kafka.common.utils.ByteBufferOutputStream

object ZstdFactory {

    private const val INPUT_BUFFER_SIZE = 16 * 1024

    fun wrapForOutput(buffer: ByteBufferOutputStream?): OutputStream {
        return try {
            // Set input buffer (uncompressed) to 16 KB (none by default) to ensure reasonable
            // performance in cases where the caller passes a small number of bytes to write
            // (potentially a single byte).
            BufferedOutputStream(
                ZstdOutputStreamNoFinalizer(buffer, RecyclingBufferPool.INSTANCE),
                INPUT_BUFFER_SIZE
            )
        } catch (exception: Throwable) {
            throw KafkaException(cause = exception)
        }
    }

    fun wrapForInput(
        buffer: ByteBuffer,
        messageVersion: Byte,
        decompressionBufferSupplier: BufferSupplier
    ): InputStream {

        return try {
            // We use our own BufferSupplier instead of com.github.luben.zstd.RecyclingBufferPool
            // since our implementation doesn't require locking or soft references.
            val bufferPool: BufferPool = object : BufferPool {

                override fun get(capacity: Int): ByteBuffer =
                    decompressionBufferSupplier[capacity]!!

                override fun release(buffer: ByteBuffer) =
                    decompressionBufferSupplier.release(buffer)
            }

            // Set output buffer (uncompressed) to 16 KB (none by default) to ensure reasonable
            // performance in cases where the caller reads a small number of bytes (potentially a
            // single byte).
            BufferedInputStream(
                ZstdInputStreamNoFinalizer(
                    ByteBufferInputStream(buffer),
                    bufferPool
                ), INPUT_BUFFER_SIZE
            )
        } catch (exception: Throwable) {
            throw KafkaException(cause = exception)
        }
    }
}
