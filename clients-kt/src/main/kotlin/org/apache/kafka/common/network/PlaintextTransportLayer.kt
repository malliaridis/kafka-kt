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

import java.io.IOException
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.channels.SelectionKey
import java.nio.channels.SocketChannel
import java.security.Principal
import org.apache.kafka.common.security.auth.KafkaPrincipal

class PlaintextTransportLayer(private val key: SelectionKey) : TransportLayer {

    private val socketChannel: SocketChannel = key.channel() as SocketChannel

    private val principal: Principal = KafkaPrincipal.ANONYMOUS

    override fun ready(): Boolean = true

    @Throws(IOException::class)
    override fun finishConnect(): Boolean {
        val connected = socketChannel.finishConnect()

        if (connected) key.interestOps(
            key.interestOps()
                    and SelectionKey.OP_CONNECT.inv()
                    or SelectionKey.OP_READ
        )

        return connected
    }

    override fun disconnect() = key.cancel()

    override fun socketChannel(): SocketChannel = socketChannel

    override fun selectionKey(): SelectionKey = key

    override fun isOpen(): Boolean = socketChannel.isOpen

    override val isConnected: Boolean
        get() = socketChannel.isConnected

    @Throws(IOException::class)
    override fun close() {
        socketChannel.socket().close()
        socketChannel.close()
    }

    /**
     * Performs SSL handshake hence is a no-op for the non-secure
     * implementation
     */
    override fun handshake() = Unit

    /**
     * Reads a sequence of bytes from this channel into the given buffer.
     *
     * @param dst The buffer into which bytes are to be transferred
     * @return The number of bytes read, possible zero or -1 if the channel has reached
     * end-of-stream
     * @throws IOException if some other I/O error occurs
     */
    @Throws(IOException::class)
    override fun read(dst: ByteBuffer): Int {
        return socketChannel.read(dst)
    }

    /**
     * Reads a sequence of bytes from this channel into the given buffers.
     *
     * @param dsts - The buffers into which bytes are to be transferred.
     * @return The number of bytes read, possibly zero, or -1 if the channel has reached
     * end-of-stream.
     * @throws IOException if some other I/O error occurs
     */
    @Throws(IOException::class)
    override fun read(dsts: Array<ByteBuffer>): Long {
        return socketChannel.read(dsts)
    }

    /**
     * Reads a sequence of bytes from this channel into a subsequence of the given buffers.
     * @param dsts - The buffers into which bytes are to be transferred
     * @param offset - The offset within the buffer array of the first buffer into which bytes are
     * to be transferred; must be non-negative and no larger than dsts.length.
     * @param length - The maximum number of buffers to be accessed; must be non-negative and no
     * larger than dsts.length - offset
     * @return The number of bytes read, possibly zero, or -1 if the channel has reached end-of-stream.
     * @throws IOException if some other I/O error occurs
     */
    @Throws(IOException::class)
    override fun read(dsts: Array<ByteBuffer>, offset: Int, length: Int): Long =
        socketChannel.read(dsts, offset, length)

    /**
     * Writes a sequence of bytes to this channel from the given buffer.
     *
     * @param src The buffer from which bytes are to be retrieved
     * @return The number of bytes read, possibly zero, or -1 if the channel has reached
     * end-of-stream
     * @throws IOException If some other I/O error occurs
     */
    @Throws(IOException::class)
    override fun write(src: ByteBuffer): Int {
        return socketChannel.write(src)
    }

    /**
     * Writes a sequence of bytes to this channel from the given buffer.
     *
     * @param srcs The buffer from which bytes are to be retrieved
     * @return The number of bytes read, possibly zero, or -1 if the channel has reached
     * end-of-stream
     * @throws IOException If some other I/O error occurs
     */
    @Throws(IOException::class)
    override fun write(srcs: Array<ByteBuffer>): Long = socketChannel.write(srcs)

    /**
     * Writes a sequence of bytes to this channel from the subsequence of the given buffers.
     *
     * @param srcs The buffers from which bytes are to be retrieved
     * @param offset The offset within the buffer array of the first buffer from which bytes are to
     * be retrieved; must be non-negative and no larger than srcs.length.
     * @param length - The maximum number of buffers to be accessed; must be non-negative and no
     * larger than srcs.length - offset.
     * @return returns no.of bytes written , possibly zero.
     * @throws IOException If some other I/O error occurs
     */
    @Throws(IOException::class)
    override fun write(srcs: Array<ByteBuffer>, offset: Int, length: Int): Long =
        socketChannel.write(srcs, offset, length)

    /**
     * always returns false as there will be not be any
     * pending writes since we directly write to socketChannel.
     */
    override fun hasPendingWrites(): Boolean = false

    /**
     * Returns ANONYMOUS as Principal.
     */
    override fun peerPrincipal(): Principal = principal

    /**
     * Adds the interestOps to selectionKey.
     */
    override fun addInterestOps(ops: Int) {
        key.interestOps(key.interestOps() or ops)
    }

    /**
     * Removes the interestOps from selectionKey.
     */
    override fun removeInterestOps(ops: Int) {
        key.interestOps(key.interestOps() and ops.inv())
    }

    override val isMute: Boolean
        get() = key.isValid && key.interestOps() and SelectionKey.OP_READ == 0

    override fun hasBytesBuffered(): Boolean = false

    @Throws(IOException::class)
    override fun transferFrom(fileChannel: FileChannel, position: Long, count: Long): Long {
        return fileChannel.transferTo(position, count, socketChannel)
    }
}
