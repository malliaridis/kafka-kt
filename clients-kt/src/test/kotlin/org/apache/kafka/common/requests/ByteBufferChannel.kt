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

package org.apache.kafka.common.requests

import java.io.IOException
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import org.apache.kafka.common.network.TransferableChannel

open class ByteBufferChannel(size: Long) : TransferableChannel {

    private val buf: ByteBuffer

    private var closed = false

    init {
        require(size <= Int.MAX_VALUE) { "size should be not be greater than Integer.MAX_VALUE" }
        buf = ByteBuffer.allocate(size.toInt())
    }

    override fun write(srcs: Array<ByteBuffer>, offset: Int, length: Int): Long {
        if (offset < 0 || length < 0 || offset > srcs.size - length) throw IndexOutOfBoundsException()
        val position = buf.position()
        val count = offset + length
        for (i in offset until count) buf.put(srcs[i].duplicate())
        return (buf.position() - position).toLong()
    }

    override fun write(srcs: Array<ByteBuffer>): Long = write(srcs, 0, srcs.size)

    override fun write(src: ByteBuffer): Int = write(arrayOf(src)).toInt()

    override fun isOpen(): Boolean = !closed

    override fun close() {
        buf.flip()
        closed = true
    }

    fun buffer(): ByteBuffer = buf

    override fun hasPendingWrites(): Boolean = false

    @Throws(IOException::class)
    override fun transferFrom(fileChannel: FileChannel, position: Long, count: Long): Long {
        return fileChannel.transferTo(position, count, this)
    }
}
