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

package org.apache.kafka.common.network

import java.io.EOFException
import java.io.IOException
import java.nio.ByteBuffer

/**
 * A send backed by an array of byte buffers
 */
class ByteBufferSend : Send {

    private val size: Long

    private val buffers: Array<ByteBuffer>

    var remaining: Long = 0
        private set

    private var pending = false

    constructor(vararg buffers: ByteBuffer) {
        this.buffers = arrayOf(*buffers)
        for (buffer in buffers) remaining += buffer.remaining().toLong()
        size = remaining
    }

    constructor(buffers: Array<ByteBuffer>, size: Long) {
        this.buffers = buffers
        this.size = size
        remaining = size
    }

    override fun completed(): Boolean {
        return remaining <= 0 && !pending
    }

    override fun size(): Long = size

    @Throws(IOException::class)
    override fun writeTo(channel: TransferableChannel): Long {
        val written = channel.write(buffers)
        if (written < 0)
            throw EOFException("Wrote negative bytes to channel. This shouldn't happen.")

        remaining -= written
        pending = channel.hasPendingWrites()
        return written
    }

    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("remaining")
    )
    fun remaining(): Long = remaining

    override fun toString(): String {
        return "ByteBufferSend(" +
                ", size=$size" +
                ", remaining=$remaining" +
                ", pending=$pending" +
                ')'
    }

    companion object {

        fun sizePrefixed(buffer: ByteBuffer): ByteBufferSend {
            val sizeBuffer = ByteBuffer.allocate(4)
            sizeBuffer.putInt(0, buffer.remaining())
            return ByteBufferSend(sizeBuffer, buffer)
        }
    }
}
