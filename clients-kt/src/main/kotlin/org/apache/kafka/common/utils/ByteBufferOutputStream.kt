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

import java.io.OutputStream
import java.nio.ByteBuffer
import kotlin.math.max

/**
 * A ByteBuffer-backed OutputStream that expands the internal ByteBuffer as required. Given this, the caller should
 * always access the underlying ByteBuffer via the [buffer] method until all writes are completed.
 *
 * This class is typically used for 2 purposes:
 *
 * 1. Write to a ByteBuffer when there is a chance that we may need to expand it in order to fit all the desired data
 * 2. Write to a ByteBuffer via methods that expect an OutputStream interface
 *
 * Hard to track bugs can happen when this class is used for the second reason and unexpected buffer expansion happens.
 * So, it's best to assume that buffer expansion can always happen. An improvement would be to create a separate class
 * that throws an error if buffer expansion is required to avoid the issue altogether.
 *
 * @constructor Creates an instance of this class that will write to the received `buffer` up to its
 * `limit`. If necessary to satisfy `write` or `position` calls, larger buffers will be allocated so
 * the [buffer] method may return a different buffer than the received `buffer` parameter.
 *
 * Prefer one of the constructors that allocate the internal buffer for clearer semantics.
 */
class ByteBufferOutputStream(buffer: ByteBuffer) : OutputStream() {

    var buffer: ByteBuffer = buffer
        private set

    /**
     * The capacity of the first internal ByteBuffer used by this class. This is useful in cases
     * where a pooled ByteBuffer was passed via the constructor and it needs to be returned to the
     * pool.
     */
    val initialCapacity: Int = buffer.position()
    
    private val initialPosition: Int = buffer.capacity()

    constructor(
        initialCapacity: Int,
        directBuffer: Boolean = false
    ) : this(
        buffer = if (directBuffer) ByteBuffer.allocateDirect(initialCapacity)
        else ByteBuffer.allocate(initialCapacity)
    )

    override fun write(b: Int) {
        ensureRemaining(1)
        buffer.put(b.toByte())
    }

    override fun write(bytes: ByteArray, off: Int, len: Int) {
        ensureRemaining(len)
        buffer.put(bytes, off, len)
    }

    fun write(sourceBuffer: ByteBuffer) {
        ensureRemaining(sourceBuffer.remaining())
        buffer.put(sourceBuffer)
    }

    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("buffer")
    )
    fun buffer(): ByteBuffer = buffer

    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("position")
    )
    fun position(): Int = buffer.position()

    val position: Int
        get() = buffer.position()

    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("remaining")
    )
    fun remaining(): Int = buffer.remaining()

    val remaining: Int
        get() = buffer.remaining()

    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("limit")
    )
    fun limit(): Int = buffer.limit()

    val limit: Int
        get() = buffer.limit()

    fun position(position: Int) {
        ensureRemaining(position - buffer.position())
        buffer.position(position)
    }

    /**
     * The capacity of the first internal ByteBuffer used by this class. This is useful in cases
     * where a pooled ByteBuffer was passed via the constructor and it needs to be returned to the
     * pool.
     */
    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("initialCapacity")
    )
    fun initialCapacity(): Int = initialCapacity

    /**
     * Ensure there is enough space to write some number of bytes, expanding the underlying buffer
     * if necessary. This can be used to avoid incremental expansions through calls to [write] when
     * you know how many total bytes are needed.
     *
     * @param remainingBytesRequired The number of bytes required
     */
    fun ensureRemaining(remainingBytesRequired: Int) {
        if (remainingBytesRequired > buffer.remaining()) expandBuffer(remainingBytesRequired)
    }

    private fun expandBuffer(remainingRequired: Int) {
        val expandSize = max(
            (buffer.limit() * REALLOCATION_FACTOR).toInt(),
            buffer.position() + remainingRequired
        )

        val temp = ByteBuffer.allocate(expandSize)
        val limit = limit

        buffer.flip()
        temp.put(buffer)
        buffer.limit(limit)

        // reset the old buffer's position so that the partial data in the new buffer cannot be
        // mistakenly consumed we should ideally only do this for the original buffer, but the
        // additional complexity doesn't seem worth it
        buffer.position(initialPosition)
        buffer = temp
    }

    companion object {
        private const val REALLOCATION_FACTOR = 1.1f
    }
}
