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

import java.nio.ByteBuffer
import java.util.ArrayDeque
import java.util.Deque

/**
 * Simple non-threadsafe interface for caching byte buffers. This is suitable for simple cases like
 * ensuring that a given KafkaConsumer reuses the same decompression buffer when iterating over
 * fetched records. For small record batches, allocating a potentially large buffer (64 KB for LZ4)
 * will dominate the cost of decompressing and iterating over the records in the batch.
 */
abstract class BufferSupplier : AutoCloseable {

    /**
     * Supply a buffer with the required capacity. This may return a cached buffer or allocate a new
     * instance.
     */
    abstract operator fun get(capacity: Int): ByteBuffer?

    /**
     * Return the provided buffer to be reused by a subsequent call to `get`.
     */
    abstract fun release(buffer: ByteBuffer)

    /**
     * Release all resources associated with this supplier.
     */
    abstract override fun close()

    private class DefaultSupplier : BufferSupplier() {

        // We currently use a single block size, so optimise for that case
        private val bufferMap: MutableMap<Int, Deque<ByteBuffer>> = HashMap(1)

        override fun get(size: Int): ByteBuffer {
            val bufferQueue = bufferMap[size]

            return if (bufferQueue.isNullOrEmpty()) ByteBuffer.allocate(size)
            else bufferQueue.pollFirst()
        }

        override fun release(buffer: ByteBuffer) {
            buffer.clear()
            var bufferQueue = bufferMap[buffer.capacity()]

            if (bufferQueue == null) {
                // We currently keep a single buffer in flight, so optimise for that case
                bufferQueue = ArrayDeque(1)
                bufferMap[buffer.capacity()] = bufferQueue
            }

            bufferQueue.addLast(buffer)
        }

        override fun close() = bufferMap.clear()
    }

    /**
     * Simple buffer supplier for single-threaded usage. It caches a single buffer, which grows
     * monotonically as needed to fulfill the allocation request.
     */
    class GrowableBufferSupplier : BufferSupplier() {

        private var cachedBuffer: ByteBuffer? = null

        override fun get(minCapacity: Int): ByteBuffer {
            val res: ByteBuffer = cachedBuffer ?: return ByteBuffer.allocate(minCapacity)

            return if (res.capacity() >= minCapacity) {
                cachedBuffer = null
                res
            } else ByteBuffer.allocate(minCapacity)
        }

        override fun release(buffer: ByteBuffer) {
            buffer.clear()
            cachedBuffer = buffer
        }

        override fun close() {
            cachedBuffer = null
        }
    }

    companion object {

        val NO_CACHING: BufferSupplier = object : BufferSupplier() {

            override fun get(capacity: Int): ByteBuffer = ByteBuffer.allocate(capacity)

            override fun release(buffer: ByteBuffer) = Unit

            override fun close() = Unit
        }

        fun create(): BufferSupplier = DefaultSupplier()
    }
}
