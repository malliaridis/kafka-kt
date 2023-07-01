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

package org.apache.kafka.common.protocol

import java.nio.ByteBuffer
import java.util.*
import org.apache.kafka.common.network.ByteBufferSend
import org.apache.kafka.common.network.Send
import org.apache.kafka.common.record.BaseRecords
import org.apache.kafka.common.record.MemoryRecords
import org.apache.kafka.common.record.MultiRecordsSend
import org.apache.kafka.common.record.UnalignedMemoryRecords
import org.apache.kafka.common.requests.RequestHeader
import org.apache.kafka.common.requests.ResponseHeader
import org.apache.kafka.common.utils.ByteUtils.writeUnsignedVarint
import org.apache.kafka.common.utils.ByteUtils.writeVarint
import org.apache.kafka.common.utils.ByteUtils.writeVarlong

/**
 * This class provides a way to build [Send] objects for network transmission from generated
 * [org.apache.kafka.common.protocol.ApiMessage] types without allocating new space for "zero-copy"
 * fields (see [writeByteBuffer] and [writeRecords]).
 *
 * See [org.apache.kafka.common.requests.EnvelopeRequest.toSend] for example usage.
 */
class SendBuilder internal constructor(size: Int) : Writable {

    private val buffer: ByteBuffer = ByteBuffer.allocate(size)

    private val sends: Queue<Send> = ArrayDeque(1)

    private var sizeOfSends: Long = 0

    private val buffers: MutableList<ByteBuffer> = ArrayList()

    private var sizeOfBuffers: Long = 0

    init {
        buffer.mark()
    }

    override fun writeByte(value: Byte) {
        buffer.put(value)
    }

    override fun writeShort(value: Short) {
        buffer.putShort(value)
    }

    override fun writeInt(value: Int) {
        buffer.putInt(value)
    }

    override fun writeLong(value: Long) {
        buffer.putLong(value)
    }

    override fun writeDouble(value: Double) {
        buffer.putDouble(value)
    }

    override fun writeByteArray(arr: ByteArray) {
        buffer.put(arr)
    }

    override fun writeUnsignedVarint(i: Int) {
        writeUnsignedVarint(i, buffer)
    }

    /**
     * Write a byte buffer. The reference to the underlying buffer will
     * be retained in the result of [.build].
     *
     * @param buf the buffer to write
     */
    override fun writeByteBuffer(buf: ByteBuffer) {
        flushPendingBuffer()
        addBuffer(buf.duplicate())
    }

    override fun writeVarint(i: Int) {
        writeVarint(i, buffer)
    }

    override fun writeVarlong(i: Long) {
        writeVarlong(i, buffer)
    }

    private fun addBuffer(buffer: ByteBuffer) {
        buffers.add(buffer)
        sizeOfBuffers += buffer.remaining().toLong()
    }

    private fun addSend(send: Send) {
        sends.add(send)
        sizeOfSends += send.size()
    }

    private fun clearBuffers() {
        buffers.clear()
        sizeOfBuffers = 0
    }

    /**
     * Write a record set. The underlying record data will be retained
     * in the result of [.build]. See [BaseRecords.toSend].
     *
     * @param records the records to write
     */
    override fun writeRecords(records: BaseRecords) {
        when (records) {
            is MemoryRecords -> {
                flushPendingBuffer()
                addBuffer(records.buffer())
            }

            is UnalignedMemoryRecords -> {
                flushPendingBuffer()
                addBuffer(records.buffer())
            }

            else -> {
                flushPendingSend()
                addSend(records.toSend())
            }
        }
    }

    private fun flushPendingSend() {
        flushPendingBuffer()
        if (buffers.isNotEmpty()) {
            addSend(ByteBufferSend(buffers.toTypedArray(), sizeOfBuffers))
            clearBuffers()
        }
    }

    private fun flushPendingBuffer() {
        val latestPosition = buffer.position()
        buffer.reset()

        if (latestPosition <= buffer.position()) return

        buffer.limit(latestPosition)
        addBuffer(buffer.slice())
        buffer.position(latestPosition)
        buffer.limit(buffer.capacity())
        buffer.mark()
    }

    fun build(): Send {
        flushPendingSend()
        return if (sends.size == 1) sends.poll()
        else MultiRecordsSend(sends, sizeOfSends)
    }

    companion object {

        fun buildRequestSend(
            header: RequestHeader,
            apiRequest: Message
        ): Send = buildSend(
            header.data(),
            header.headerVersion(),
            apiRequest,
            header.apiVersion()
        )

        fun buildResponseSend(
            header: ResponseHeader,
            apiResponse: Message,
            apiVersion: Short
        ): Send = buildSend(
            header.data,
            header.headerVersion,
            apiResponse,
            apiVersion
        )

        private fun buildSend(
            header: Message,
            headerVersion: Short,
            apiMessage: Message,
            apiVersion: Short
        ): Send {
            val serializationCache = ObjectSerializationCache()
            val messageSize = MessageSizeAccumulator()
            header.addSize(messageSize, serializationCache, headerVersion)
            apiMessage.addSize(messageSize, serializationCache, apiVersion)
            val builder = SendBuilder(messageSize.sizeExcludingZeroCopy() + 4)
            builder.writeInt(messageSize.totalSize())
            header.write(builder, serializationCache, headerVersion)
            apiMessage.write(builder, serializationCache, apiVersion)
            return builder.build()
        }
    }
}
