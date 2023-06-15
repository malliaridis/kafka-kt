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
import java.nio.channels.CancelledKeyException
import java.nio.channels.FileChannel
import java.nio.channels.SelectionKey
import java.nio.channels.SocketChannel
import java.security.Principal
import javax.net.ssl.SSLEngine
import javax.net.ssl.SSLEngineResult
import javax.net.ssl.SSLEngineResult.HandshakeStatus
import javax.net.ssl.SSLException
import javax.net.ssl.SSLHandshakeException
import javax.net.ssl.SSLKeyException
import javax.net.ssl.SSLPeerUnverifiedException
import javax.net.ssl.SSLProtocolException
import javax.net.ssl.SSLSession
import org.apache.kafka.common.errors.SslAuthenticationException
import org.apache.kafka.common.security.auth.KafkaPrincipal
import org.apache.kafka.common.utils.ByteBufferUnmapper.unmap
import org.apache.kafka.common.utils.ByteUtils
import org.apache.kafka.common.utils.LogContext
import org.apache.kafka.common.utils.Utils.ensureCapacity
import org.slf4j.Logger
import kotlin.math.min

/**
 * Transport layer for SSL communication.
 *
 * TLS v1.3 notes:
 *
 *   [Section 4.6](https://tools.ietf.org/html/rfc8446#section-4.6) : Post-Handshake Messages
 * > "TLS also allows other messages to be sent after the main handshake. These messages use a
 * > handshake content type and are encrypted under the appropriate application traffic key."
 *
 * @constructor Prefer `create`, only use this in tests
 */
open class SslTransportLayer internal constructor(
    private val channelId: String,
    private val key: SelectionKey,
    private val sslEngine: SSLEngine,
    private val metadataRegistry: ChannelMetadataRegistry,
) : TransportLayer {

    private val socketChannel: SocketChannel = key.channel() as SocketChannel

    private val log: Logger = LogContext(
        String.format("[SslTransportLayer channelId=%s key=%s] ", channelId, key)
    ).logger(javaClass)

    private var handshakeStatus: HandshakeStatus? = null

    private lateinit var handshakeResult: SSLEngineResult

    private var state: State = State.NOT_INITIALIZED

    private var handshakeException: SslAuthenticationException? = null

    private var _netReadBuffer: ByteBuffer? = null
    private val netReadBuffer: ByteBuffer
        get() = _netReadBuffer!!

    private var _netWriteBuffer: ByteBuffer? = null
    private val netWriteBuffer: ByteBuffer
        get() = _netWriteBuffer!!

    private var _appReadBuffer: ByteBuffer? = null
    private val appReadBuffer: ByteBuffer
        get() = _appReadBuffer!!

    private var _fileChannelBuffer: ByteBuffer? = null
    private val fileChannelBuffer: ByteBuffer
        get() = _fileChannelBuffer!!

    private var hasBytesBuffered = false

    // Visible for testing
    @Throws(IOException::class)
    protected open fun startHandshake() {
        if (state != State.NOT_INITIALIZED)
            error("startHandshake() can only be called once, state $state")

        _netReadBuffer = ByteBuffer.allocate(netReadBufferSize())
        _netWriteBuffer = ByteBuffer.allocate(netWriteBufferSize())
        _appReadBuffer = ByteBuffer.allocate(applicationBufferSize())

        netWriteBuffer.limit(0)
        netReadBuffer.limit(0)
        state = State.HANDSHAKE

        //initiate handshake
        sslEngine.beginHandshake()
        handshakeStatus = sslEngine.handshakeStatus
    }

    override fun ready(): Boolean {
        return state == State.POST_HANDSHAKE || state == State.READY
    }

    /**
     * does socketChannel.finishConnect()
     */
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

    /**
     * disconnects selectionKey.
     */
    override fun disconnect() = key.cancel()

    override fun socketChannel(): SocketChannel? = socketChannel

    override fun selectionKey(): SelectionKey = key

    override fun isOpen(): Boolean = socketChannel.isOpen

    override val isConnected: Boolean
        get() = socketChannel.isConnected

    /**
     * Sends an SSL close message and closes socketChannel.
     */
    @Throws(IOException::class)
    override fun close() {
        val prevState = state
        if (state == State.CLOSING) return
        state = State.CLOSING
        sslEngine.closeOutbound()
        try {
            if (prevState != State.NOT_INITIALIZED && isConnected) {
                if (!flush(netWriteBuffer)) {
                    throw IOException("Remaining data in the network buffer, can't send SSL close message.")
                }

                //prep the buffer for the close message
                netWriteBuffer.clear()

                //perform the close, since we called sslEngine.closeOutbound
                val wrapResult = sslEngine.wrap(ByteUtils.EMPTY_BUF, netWriteBuffer)

                //we should be in a close state
                if (wrapResult.status != SSLEngineResult.Status.CLOSED) {
                    throw IOException(
                        "Unexpected status returned by SSLEngine.wrap, expected CLOSED, received " +
                                "${wrapResult.status}. Will not send close message to peer."
                    )
                }
                netWriteBuffer.flip()
                flush(netWriteBuffer)
            }
        } catch (ie: IOException) {
            log.debug("Failed to send SSL Close message", ie)
        } finally {
            socketChannel.socket().close()
            socketChannel.close()

            _netReadBuffer = null
            _netWriteBuffer = null
            _appReadBuffer = null

            _fileChannelBuffer?.let {
                unmap("fileChannelBuffer", it)
                _fileChannelBuffer = null
            }
        }
    }

    /**
     * returns true if there are any pending contents in netWriteBuffer
     */
    override fun hasPendingWrites(): Boolean = netWriteBuffer.hasRemaining()

    /**
     * Reads available bytes from socket channel to `netReadBuffer`.
     * Visible for testing.
     * @return  number of bytes read
     */
    @Throws(IOException::class)
    protected open fun readFromSocketChannel(): Int = socketChannel.read(netReadBuffer)

    /**
     * Flushes the buffer to the network, non blocking.
     * Visible for testing.
     * @param buf ByteBuffer
     * @return boolean true if the buffer has been emptied out, false otherwise
     * @throws IOException
     */
    @Throws(IOException::class)
    protected open fun flush(buf: ByteBuffer): Boolean {
        val remaining = buf.remaining()

        if (remaining > 0) {
            val written = socketChannel.write(buf)
            return written >= remaining
        }

        return true
    }

    /**
     * Performs SSL handshake, non-blocking.
     *
     * Before application data (kafka protocols) can be sent client & kafka broker must perform ssl
     * handshake.
     *
     * During the handshake SSLEngine generates encrypted data that will be transported over
     * socketChannel. Each SSLEngine operation generates SSLEngineResult, of which
     * SSLEngineResult.handshakeStatus field is used to determine what operation needs to occur to
     * move handshake along.
     *
     * A typical handshake might look like this.
     *
     * |  client     |  SSL/TLS message                 | HSStatus    |
     * |-------------|----------------------------------|-------------|
     * | wrap()      | ClientHello                      | NEED_UNWRAP |
     * | unwrap()    | ServerHello/Cert/ServerHelloDone | NEED_WRAP   |
     * | wrap()      | ClientKeyExchange                | NEED_WRAP   |
     * | wrap()      | ChangeCipherSpec                 | NEED_WRAP   |
     * | wrap()      | Finished                         | NEED_UNWRAP |
     * | unwrap()    | ChangeCipherSpec                 | NEED_UNWRAP |
     * | unwrap()    | Finished                         | FINISHED    |
     *
     * @throws IOException if read/write fails
     * @throws SslAuthenticationException if handshake fails with an [SSLException]
     */
    @Throws(IOException::class)
    override fun handshake() {
        if (state == State.NOT_INITIALIZED) {
            try {
                startHandshake()
            } catch (e: SSLException) {
                maybeProcessHandshakeFailure(e, false, null)
            }
        }

        if (ready()) throw renegotiationException()
        if (state == State.CLOSING) throw closingException()

        var read = 0
        val readable = key.isReadable
        try {
            // Read any available bytes before attempting any writes to ensure that handshake
            // failures reported by the peer are processed even if writes fail (since peer closes
            // connection if handshake fails)
            if (readable) read = readFromSocketChannel()
            doHandshake()
            if (ready()) updateBytesBuffered(true)
        } catch (e: SSLException) {
            maybeProcessHandshakeFailure(e, true, null)
        } catch (e: IOException) {
            maybeThrowSslAuthenticationException()

            // This exception could be due to a write. If there is data available to unwrap in the
            // buffer, or data available in the socket channel to read and unwrap, process the data
            // so that any SSL handshake exceptions are reported.
            try {
                do {
                    log.trace(
                        "Process any available bytes from peer, netReadBuffer {} netWriterBuffer " +
                                "{} handshakeStatus {} readable? {}",
                        netReadBuffer,
                        netWriteBuffer,
                        handshakeStatus,
                        readable,
                    )

                    handshakeWrapAfterFailure(doWrite = false)
                    handshakeUnwrap(
                        doRead = false,
                        ignoreHandshakeStatus = true,
                    )
                } while (readable && readFromSocketChannel() > 0)
            } catch (e1: SSLException) {
                maybeProcessHandshakeFailure(e1, false, e)
            }

            // If we get here, this is not a handshake failure, throw the original IOException
            throw e
        }

        // Read from socket failed, so throw any pending handshake exception or EOF exception.
        if (read == -1) {
            maybeThrowSslAuthenticationException()
            throw EOFException("EOF during handshake, handshake status is $handshakeStatus")
        }
    }

    @Throws(IOException::class)
    private fun doHandshake() {
        val read = key.isReadable
        val write = key.isWritable
        handshakeStatus = sslEngine.handshakeStatus

        if (!flush(netWriteBuffer)) {
            key.interestOps(key.interestOps() or SelectionKey.OP_WRITE)
            return
        }

        // Throw any pending handshake exception since `netWriteBuffer` has been flushed
        maybeThrowSslAuthenticationException()

        when (handshakeStatus) {
            HandshakeStatus.NEED_TASK -> {
                log.trace(
                    "SSLHandshake NEED_TASK channelId {}, appReadBuffer pos {}, netReadBuffer " +
                            "pos {}, netWriteBuffer pos {}",
                    channelId,
                    appReadBuffer.position(),
                    netReadBuffer.position(),
                    netWriteBuffer.position(),
                )
                handshakeStatus = runDelegatedTasks()
            }

            HandshakeStatus.NEED_WRAP ->
                if (handleNeedWrap(write) && handleNeedUnwrap(read)) handshakeFinished()

            HandshakeStatus.NEED_UNWRAP ->
                if (handleNeedUnwrap(read)) handshakeFinished()

            HandshakeStatus.FINISHED -> handshakeFinished()
            HandshakeStatus.NOT_HANDSHAKING -> handshakeFinished()
            else -> error(String.format("Unexpected status [%s]", handshakeStatus))
        }
    }

    private fun handleNeedWrap(write: Boolean): Boolean {
        log.trace(
            "SSLHandshake NEED_WRAP channelId {}, appReadBuffer pos {}, netReadBuffer " +
                    "pos {}, netWriteBuffer pos {}",
            channelId,
            appReadBuffer.position(),
            netReadBuffer.position(),
            netWriteBuffer.position()
        )

        val handshakeResult = handshakeWrap(write).also { this.handshakeResult = it }

        when (handshakeResult.status) {
            SSLEngineResult.Status.BUFFER_OVERFLOW -> {
                val currentNetWriteBufferSize = netWriteBufferSize()
                netWriteBuffer.compact()
                _netWriteBuffer = ensureCapacity((netWriteBuffer), currentNetWriteBufferSize)
                netWriteBuffer.flip()
                if (netWriteBuffer.limit() >= currentNetWriteBufferSize) error(
                    "Buffer overflow when available data size (${netWriteBuffer.limit()}" +
                            ") >= network buffer size ($currentNetWriteBufferSize)"
                )
            }

            SSLEngineResult.Status.BUFFER_UNDERFLOW ->
                error("Should not have received BUFFER_UNDERFLOW during handshake WRAP.")

            SSLEngineResult.Status.CLOSED -> throw EOFException()
            else -> Unit
        }

        log.trace(
            "SSLHandshake NEED_WRAP channelId {}, handshakeResult {}, appReadBuffer pos " +
                    "{}, netReadBuffer pos {}, netWriteBuffer pos {}",
            channelId,
            handshakeResult,
            appReadBuffer.position(),
            netReadBuffer.position(),
            netWriteBuffer.position(),
        )

        //if handshake status is not NEED_UNWRAP or unable to flush netWriteBuffer contents
        //we will break here otherwise we can do need_unwrap in the same call.
        if (
            handshakeStatus != HandshakeStatus.NEED_UNWRAP
            || !flush(netWriteBuffer)
        ) {
            key.interestOps(key.interestOps() or SelectionKey.OP_WRITE)
            return false
        }
        return true
    }

    private fun handleNeedUnwrap(read: Boolean): Boolean {
        log.trace(
            "SSLHandshake NEED_UNWRAP channelId {}, appReadBuffer pos {}, netReadBuffer pos {}, " +
                    "netWriteBuffer pos {}",
            channelId,
            appReadBuffer.position(),
            netReadBuffer.position(),
            netWriteBuffer.position(),
        )
        do {
            val handshakeResult = handshakeUnwrap(read, false).also {
                this.handshakeResult = it
            }
            if (handshakeResult.status == SSLEngineResult.Status.BUFFER_OVERFLOW) {
                val currentAppBufferSize = applicationBufferSize()
                _appReadBuffer = ensureCapacity(appReadBuffer, currentAppBufferSize)
                check(appReadBuffer.position() <= currentAppBufferSize) {
                    "Buffer underflow when available data size (${appReadBuffer.position()}) " +
                            "> packet buffer size ($currentAppBufferSize)"
                }
            }
        } while (handshakeResult.status == SSLEngineResult.Status.BUFFER_OVERFLOW)

        if (handshakeResult.status == SSLEngineResult.Status.BUFFER_UNDERFLOW) {
            val currentNetReadBufferSize = netReadBufferSize()
            _netReadBuffer = ensureCapacity(netReadBuffer, currentNetReadBufferSize)
            check(netReadBuffer.position() < currentNetReadBufferSize) {
                "Buffer underflow when there is available data"
            }
        } else if (handshakeResult.status == SSLEngineResult.Status.CLOSED)
            throw EOFException("SSL handshake status CLOSED during handshake UNWRAP")

        log.trace(
            "SSLHandshake NEED_UNWRAP channelId {}, handshakeResult {}, appReadBuffer pos {}, " +
                    "netReadBuffer pos {}, netWriteBuffer pos {}",
            channelId,
            handshakeResult,
            appReadBuffer.position(),
            netReadBuffer.position(),
            netWriteBuffer.position()
        )

        //if handshakeStatus completed than fall-through to finished status.
        //after handshake is finished there is no data left to read/write in socketChannel.
        //so the selector won't invoke this channel if we don't go through the handshakeFinished here.

        //if handshakeStatus completed than fall-through to finished status.
        //after handshake is finished there is no data left to read/write in socketChannel.
        //so the selector won't invoke this channel if we don't go through the handshakeFinished here.
        if (handshakeStatus != HandshakeStatus.FINISHED) {
            if (handshakeStatus == HandshakeStatus.NEED_WRAP)
                key.interestOps(key.interestOps() or SelectionKey.OP_WRITE)
            else if (handshakeStatus == HandshakeStatus.NEED_UNWRAP)
                key.interestOps(key.interestOps() and SelectionKey.OP_WRITE.inv())

            return false
        }
        return true
    }

    private fun renegotiationException(): SSLHandshakeException {
        return SSLHandshakeException("Renegotiation is not supported")
    }

    private fun closingException(): IllegalStateException {
        throw IllegalStateException("Channel is in closing state")
    }

    /**
     * Executes the SSLEngine tasks needed.
     * @return HandshakeStatus
     */
    private fun runDelegatedTasks(): HandshakeStatus {
        while (true) {
            val task: Runnable = delegatedTask() ?: break
            task.run()
        }
        return sslEngine.handshakeStatus
    }

    /**
     * Checks if the handshake status is finished. Sets the interestOps for the selectionKey.
     */
    @Throws(IOException::class)
    private fun handshakeFinished() {
        // SSLEngine.getHandshakeStatus is transient and it doesn't record FINISHED status properly.
        // It can move from FINISHED status to NOT_HANDSHAKING after the handshake is completed.
        // Hence we also need to check handshakeResult.getHandshakeStatus() if the handshake
        // finished or not.
        if (handshakeResult.handshakeStatus == HandshakeStatus.FINISHED) {
            //we are complete if we have delivered the last packet
            //remove OP_WRITE if we are complete, otherwise we still have data to write
            if (netWriteBuffer.hasRemaining()) key.interestOps(
                key.interestOps()
                        or SelectionKey.OP_WRITE
            ) else {
                val session = sslEngine.session
                state = if ((session.protocol == TLS13)) State.POST_HANDSHAKE else State.READY
                key.interestOps(key.interestOps() and SelectionKey.OP_WRITE.inv())
                log.debug(
                    "SSL handshake completed successfully with peerHost '{}' peerPort {} " +
                            "peerPrincipal '{}' protocol '{}' cipherSuite '{}'",
                    session.peerHost,
                    session.peerPort,
                    peerPrincipal(),
                    session.protocol,
                    session.cipherSuite,
                )
                metadataRegistry.registerCipherInformation(
                    CipherInformation(session.cipherSuite, session.protocol)
                )
            }

            log.trace(
                "SSLHandshake FINISHED channelId {}, appReadBuffer pos {}, netReadBuffer pos {}, " +
                        "netWriteBuffer pos {} ",
                channelId,
                appReadBuffer.position(),
                netReadBuffer.position(),
                netWriteBuffer.position(),
            )
        } else throw IOException("NOT_HANDSHAKING during handshake")
    }

    /**
     * Performs the WRAP function
     *
     * @param doWrite boolean
     * @return SSLEngineResult
     * @throws IOException
     */
    @Throws(IOException::class)
    private fun handshakeWrap(doWrite: Boolean): SSLEngineResult {
        log.trace("SSLHandshake handshakeWrap {}", channelId)
        if (netWriteBuffer.hasRemaining())
            error("handshakeWrap called with netWriteBuffer not empty")

        // this should never be called with a network buffer that contains data, so we can clear it
        // here.
        netWriteBuffer.clear()
        val result: SSLEngineResult
        try {
            result = sslEngine.wrap(ByteUtils.EMPTY_BUF, netWriteBuffer)
        } finally {
            //prepare the results to be written
            netWriteBuffer.flip()
        }

        handshakeStatus = result.handshakeStatus
        if (result.status == SSLEngineResult.Status.OK &&
            result.handshakeStatus == HandshakeStatus.NEED_TASK
        ) handshakeStatus = runDelegatedTasks()

        if (doWrite) flush(netWriteBuffer)
        return result
    }

    /**
     * Perform handshake unwrap
     *
     * @param doRead boolean If true, read more from the socket channel
     * @param ignoreHandshakeStatus If true, continue to unwrap if data available regardless of
     * handshake status
     * @return SSLEngineResult
     * @throws IOException
     */
    @Throws(IOException::class)
    private fun handshakeUnwrap(doRead: Boolean, ignoreHandshakeStatus: Boolean): SSLEngineResult {
        log.trace("SSLHandshake handshakeUnwrap {}", channelId)
        var result: SSLEngineResult
        var read = 0
        if (doRead) read = readFromSocketChannel()
        var cont: Boolean
        do {
            //prepare the buffer with the incoming data
            val position = netReadBuffer.position()
            netReadBuffer.flip()
            result = sslEngine.unwrap(netReadBuffer, appReadBuffer)
            netReadBuffer.compact()
            handshakeStatus = result.handshakeStatus
            if (result.status == SSLEngineResult.Status.OK &&
                result.handshakeStatus == HandshakeStatus.NEED_TASK
            ) {
                handshakeStatus = runDelegatedTasks()
            }
            cont = (result.status == SSLEngineResult.Status.OK
                    && handshakeStatus == HandshakeStatus.NEED_UNWRAP
                    ) || (ignoreHandshakeStatus && netReadBuffer.position() != position)
            log.trace(
                "SSLHandshake handshakeUnwrap: handshakeStatus {} status {}",
                handshakeStatus,
                result.status
            )
        } while (netReadBuffer.position() != 0 && cont)

        // Throw EOF exception for failed read after processing already received data
        // so that handshake failures are reported correctly
        if (read == -1) throw EOFException("EOF during handshake, handshake status is $handshakeStatus")
        return result
    }

    /**
     * Reads a sequence of bytes from this channel into the given buffer. Reads as much as possible
     * until either the dst buffer is full or there is no more data in the socket.
     *
     * @param dst The buffer into which bytes are to be transferred
     * @return The number of bytes read, possible zero or -1 if the channel has reached
     * end-of-stream and no more data is available
     * @throws IOException if some other I/O error occurs
     */
    @Throws(IOException::class)
    override fun read(dst: ByteBuffer): Int {
        if (state == State.CLOSING) return -1 else if (!ready()) return 0

        //if we have unread decrypted data in appReadBuffer read that into dst buffer.
        var read = 0
        if (appReadBuffer.position() > 0) {
            read = readFromAppBuffer(dst)
        }
        var readFromNetwork = false
        var isClosed = false
        // Each loop reads at most once from the socket.
        while (dst.remaining() > 0) {
            var netread = 0
            _netReadBuffer = ensureCapacity((netReadBuffer), netReadBufferSize())
            if (netReadBuffer.remaining() > 0) {
                netread = readFromSocketChannel()
                if (netread > 0) readFromNetwork = true
            }

            while (netReadBuffer.position() > 0) {
                netReadBuffer.flip()
                var unwrapResult: SSLEngineResult
                try {
                    unwrapResult = sslEngine.unwrap(netReadBuffer, appReadBuffer)
                    if (state == State.POST_HANDSHAKE && appReadBuffer.position() != 0) {
                        // For TLSv1.3, we have finished processing post-handshake messages since we are now processing data
                        state = State.READY
                    }
                } catch (e: SSLException) {
                    // For TLSv1.3, handle SSL exceptions while processing post-handshake messages as authentication exceptions
                    if (state == State.POST_HANDSHAKE) {
                        state = State.HANDSHAKE_FAILED
                        throw SslAuthenticationException(
                            "Failed to process post-handshake messages",
                            e,
                        )
                    } else throw e
                }
                netReadBuffer.compact()

                // reject renegotiation if TLS < 1.3, key updates for TLS 1.3 are allowed
                if (
                    unwrapResult.handshakeStatus != HandshakeStatus.NOT_HANDSHAKING
                    && unwrapResult.handshakeStatus != HandshakeStatus.FINISHED
                    && unwrapResult.status == SSLEngineResult.Status.OK
                    && sslEngine.session.protocol != TLS13
                ) {
                    log.error(
                        "Renegotiation requested, but it is not supported, channelId {}, " +
                                "appReadBuffer pos {}, netReadBuffer pos {}, netWriteBuffer pos " +
                                "{} handshakeStatus {}",
                        channelId,
                        appReadBuffer.position(),
                        netReadBuffer.position(),
                        netWriteBuffer.position(),
                        unwrapResult.handshakeStatus
                    )
                    throw renegotiationException()
                }
                if (unwrapResult.status == SSLEngineResult.Status.OK) read += readFromAppBuffer(dst)
                else if (unwrapResult.status == SSLEngineResult.Status.BUFFER_OVERFLOW) {
                    val currentApplicationBufferSize = applicationBufferSize()
                    _appReadBuffer = ensureCapacity((appReadBuffer), currentApplicationBufferSize)
                    if (appReadBuffer.position() >= currentApplicationBufferSize) {
                        throw IllegalStateException(
                            ("Buffer overflow when available data size (" + appReadBuffer.position() +
                                    ") >= application buffer size (" + currentApplicationBufferSize + ")")
                        )
                    }

                    // appReadBuffer will extended upto currentApplicationBufferSize
                    // we need to read the existing content into dst before we can do unwrap again. If there are no space in dst
                    // we can break here.
                    if (dst.hasRemaining()) read += readFromAppBuffer(dst)
                    else break
                } else if (unwrapResult.status == SSLEngineResult.Status.BUFFER_UNDERFLOW) {
                    val currentNetReadBufferSize = netReadBufferSize()
                    _netReadBuffer = ensureCapacity(netReadBuffer, currentNetReadBufferSize)
                    if (netReadBuffer.position() >= currentNetReadBufferSize) error(
                        "Buffer underflow when available data size (${netReadBuffer.position()}) " +
                                "> packet buffer size (" + currentNetReadBufferSize + ")"
                    )
                    break
                } else if (unwrapResult.status == SSLEngineResult.Status.CLOSED) {
                    // If data has been read and unwrapped, return the data. Close will be handled on the next poll.
                    if (appReadBuffer.position() == 0 && read == 0) throw EOFException()
                    else {
                        isClosed = true
                        break
                    }
                }
            }
            if (read == 0 && netread < 0) throw EOFException("EOF during read")
            if (netread <= 0 || isClosed) break
        }
        updateBytesBuffered(readFromNetwork || read > 0)
        // If data has been read and unwrapped, return the data even if end-of-stream, channel will be closed
        // on a subsequent poll.
        return read
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
        return read(dsts, 0, dsts.size)
    }

    /**
     * Reads a sequence of bytes from this channel into a subsequence of the given buffers.
     *
     * @param dsts - The buffers into which bytes are to be transferred
     * @param offset - The offset within the buffer array of the first buffer into which bytes are
     * to be transferred; must be non-negative and no larger than dsts.length.
     * @param length - The maximum number of buffers to be accessed; must be non-negative and no
     * larger than dsts.length - offset
     * @return The number of bytes read, possibly zero, or -1 if the channel has reached
     * end-of-stream.
     * @throws IOException if some other I/O error occurs
     */
    @Throws(IOException::class)
    override fun read(dsts: Array<ByteBuffer>, offset: Int, length: Int): Long {
        if ((offset < 0) || (length < 0) || (offset > dsts.size - length))
            throw IndexOutOfBoundsException()

        var totalRead = 0
        var i = offset

        while (i < length) {
            if (dsts[i].hasRemaining()) {
                val read = read(dsts[i])
                if (read > 0) totalRead += read else break
            }
            if (!dsts[i].hasRemaining()) i++
        }

        return totalRead.toLong()
    }

    /**
     * Writes a sequence of bytes to this channel from the given buffer.
     *
     * @param src The buffer from which bytes are to be retrieved
     * @return The number of bytes read from src, possibly zero, or -1 if the channel has reached
     * end-of-stream
     * @throws IOException If some other I/O error occurs
     */
    @Throws(IOException::class)
    override fun write(src: ByteBuffer): Int {
        if (state == State.CLOSING) throw closingException()
        if (!ready()) return 0
        var written = 0

        while (flush(netWriteBuffer) && src.hasRemaining()) {
            netWriteBuffer.clear()
            val wrapResult = sslEngine.wrap(src, netWriteBuffer)
            netWriteBuffer.flip()

            // reject renegotiation if TLS < 1.3, key updates for TLS 1.3 are allowed
            if (((wrapResult.handshakeStatus != HandshakeStatus.NOT_HANDSHAKING) && (
                        wrapResult.status == SSLEngineResult.Status.OK) &&
                        sslEngine.session.protocol != TLS13)
            ) {
                throw renegotiationException()
            }
            when (wrapResult.status) {
                SSLEngineResult.Status.OK -> written += wrapResult.bytesConsumed()
                SSLEngineResult.Status.BUFFER_OVERFLOW -> {
                    // BUFFER_OVERFLOW means that the last `wrap` call had no effect, so we expand the buffer and try again
                    _netWriteBuffer = ensureCapacity((netWriteBuffer), netWriteBufferSize())
                    netWriteBuffer.position(netWriteBuffer.limit())
                }

                SSLEngineResult.Status.BUFFER_UNDERFLOW ->
                    error("SSL BUFFER_UNDERFLOW during write")

                SSLEngineResult.Status.CLOSED -> throw EOFException()
                else -> Unit // case should not occur in Kotlin
            }
        }
        return written
    }

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
    override fun write(srcs: Array<ByteBuffer>, offset: Int, length: Int): Long {
        if ((offset < 0) || (length < 0) || (offset > srcs.size - length))
            throw IndexOutOfBoundsException()

        var totalWritten = 0
        var i = offset

        while (i < length) {
            if (srcs[i].hasRemaining() || hasPendingWrites()) {
                val written = write(srcs[i])
                if (written > 0) totalWritten += written
            }
            if (!srcs[i].hasRemaining() && !hasPendingWrites()) i++
            // if we are unable to write the current buffer to socketChannel we should break,
            // as we might have reached max socket send buffer size.
            else break
        }
        return totalWritten.toLong()
    }

    /**
     * Writes a sequence of bytes to this channel from the given buffers.
     *
     * @param srcs The buffers from which bytes are to be retrieved
     * @return returns no.of bytes consumed by SSLEngine.wrap , possibly zero.
     * @throws IOException If some other I/O error occurs
     */
    @Throws(IOException::class)
    override fun write(srcs: Array<ByteBuffer>): Long = write(srcs, 0, srcs.size)

    /**
     * SSLSession's peerPrincipal for the remote host.
     *
     * @return Principal
     */
    override fun peerPrincipal(): Principal? {
        return try {
            sslEngine.session.peerPrincipal
        } catch (se: SSLPeerUnverifiedException) {
            log.debug("SSL peer is not authenticated, returning ANONYMOUS instead")
            KafkaPrincipal.ANONYMOUS
        }
    }

    /**
     * @return an SSL Session after the handshake is established
     * @throws IllegalStateException if the handshake is not established
     */
    @Throws(IllegalStateException::class)
    fun sslSession(): SSLSession = sslEngine.session

    /**
     * Adds interestOps to SelectionKey of the TransportLayer.
     *
     * @param ops SelectionKey interestOps
     */
    override fun addInterestOps(ops: Int) {
        if (!key.isValid) throw CancelledKeyException()
        else if (!ready()) error("handshake is not completed")

        key.interestOps(key.interestOps() or ops)
    }

    /**
     * Removes interestOps to SelectionKey of the TransportLayer.
     *
     * @param ops SelectionKey interestOps
     */
    override fun removeInterestOps(ops: Int) {
        if (!key.isValid) throw CancelledKeyException()
        else if (!ready()) error("handshake is not completed")

        key.interestOps(key.interestOps() and ops.inv())
    }

    /**
     * Returns delegatedTask for the SSLEngine.
     */
    protected fun delegatedTask(): Runnable = sslEngine.delegatedTask

    /**
     * transfers appReadBuffer contents (decrypted data) into dst bytebuffer
     * @param dst ByteBuffer
     */
    private fun readFromAppBuffer(dst: ByteBuffer): Int {
        appReadBuffer.flip()
        val remaining = min(appReadBuffer.remaining(), dst.remaining())
        if (remaining > 0) {
            val limit = appReadBuffer.limit()
            appReadBuffer.limit(appReadBuffer.position() + remaining)
            dst.put(appReadBuffer)
            appReadBuffer.limit(limit)
        }
        appReadBuffer.compact()
        return remaining
    }

    protected open fun netReadBufferSize(): Int = sslEngine.session.packetBufferSize

    protected open fun netWriteBufferSize(): Int = sslEngine.session.packetBufferSize

    protected open fun applicationBufferSize(): Int = sslEngine.session.applicationBufferSize

    protected fun netReadBuffer(): ByteBuffer? = _netReadBuffer

    // Visibility for testing
    protected fun appReadBuffer(): ByteBuffer? = _appReadBuffer

    /**
     * SSL exceptions are propagated as authentication failures so that clients can avoid
     * retries and report the failure. If `flush` is true, exceptions are propagated after
     * any pending outgoing bytes are flushed to ensure that the peer is notified of the failure.
     */
    @Throws(IOException::class)
    private fun handshakeFailure(sslException: SSLException, flush: Boolean) {
        //Release all resources such as internal buffers that SSLEngine is managing
        log.debug("SSL Handshake failed", sslException)
        sslEngine.closeOutbound()
        try {
            sslEngine.closeInbound()
        } catch (e: SSLException) {
            log.debug("SSLEngine.closeInBound() raised an exception.", e)
        }
        state = State.HANDSHAKE_FAILED
        handshakeException = SslAuthenticationException("SSL handshake failed", sslException).also {
            // Attempt to flush any outgoing bytes. If flush doesn't complete, delay exception
            // handling until outgoing bytes are flushed. If write fails because remote end has
            // closed the channel, log the I/O exception and  continue to handle the handshake
            // failure as an authentication exception.
            if (!flush || handshakeWrapAfterFailure(flush)) throw it
            else log.debug(
                "Delay propagation of handshake exception till {} bytes remaining are flushed",
                netWriteBuffer.remaining()
            )
        }
    }

    // SSL handshake failures are typically thrown as SSLHandshakeException, SSLProtocolException,
    // SSLPeerUnverifiedException or SSLKeyException if the cause is known. These exceptions
    // indicate authentication failures (e.g. configuration errors) which should not be retried. But
    // the SSL engine may also throw exceptions using the base class SSLException in a few cases:
    // a) If there are no matching ciphers or TLS version or the private key is invalid, client will
    //    be unable to process the server message and an SSLException is thrown:
    //    javax.net.ssl.SSLException: Unrecognized SSL message, plaintext connection?
    // b) If server closes the connection gracefully during handshake, client may receive
    //    close_notify and and an SSLException is thrown:
    //    javax.net.ssl.SSLException: Received close_notify during handshake
    // We want to handle a) as a non-retriable SslAuthenticationException and b) as a retriable
    // IOException. To do this we need to rely on the exception string. Since it is safer to throw a
    // retriable exception when we are not sure, we will treat only the first exception string as a
    // handshake exception.
    @Throws(IOException::class)
    private fun maybeProcessHandshakeFailure(
        sslException: SSLException,
        flush: Boolean,
        ioException: IOException?,
    ) {
        if (
            sslException is SSLHandshakeException
            || sslException is SSLProtocolException
            || sslException is SSLPeerUnverifiedException
            || sslException is SSLKeyException
            || sslException.message!!.contains("Unrecognized SSL message")
            || sslException.message!!.contains("Received fatal alert: ")
        ) handshakeFailure(sslException, flush)
        else if (ioException == null) throw sslException
        else {
            log.debug(
                "SSLException while unwrapping data after IOException, original IOException will " +
                        "be propagated",
                sslException
            )
            throw ioException
        }
    }

    // If handshake has already failed, throw the authentication exception.
    private fun maybeThrowSslAuthenticationException() = handshakeException?.let { throw it }

    /**
     * Perform handshake wrap after an SSLException or any IOException.
     *
     * If `doWrite=false`, we are processing IOException after peer has disconnected, so we
     * cannot send any more data. We perform any pending wraps so that we can unwrap any
     * peer data that is already available.
     *
     * If `doWrite=true`, we are processing SSLException, we perform wrap and flush
     * any data to notify the peer of the handshake failure.
     *
     * Returns true if no more wrap is required and any data is flushed or discarded.
     */
    private fun handshakeWrapAfterFailure(doWrite: Boolean): Boolean {
        try {
            log.trace("handshakeWrapAfterFailure status {} doWrite {}", handshakeStatus, doWrite)
            while (
                handshakeStatus == HandshakeStatus.NEED_WRAP
                && (!doWrite || flush(netWriteBuffer))
            ) {
                if (!doWrite) clearWriteBuffer()
                handshakeWrap(doWrite)
            }
        } catch (e: Exception) {
            log.debug("Failed to wrap and flush all bytes before closing channel", e)
            clearWriteBuffer()
        }
        if (!doWrite) clearWriteBuffer()
        return !netWriteBuffer.hasRemaining()
    }

    private fun clearWriteBuffer() {
        if (netWriteBuffer.hasRemaining())
            log.debug("Discarding write buffer {} since peer has disconnected", netWriteBuffer)

        netWriteBuffer.position(0)
        netWriteBuffer.limit(0)
    }

    override val isMute: Boolean
        get() = key.isValid && (key.interestOps() and SelectionKey.OP_READ) == 0

    override fun hasBytesBuffered(): Boolean {
        return hasBytesBuffered
    }

    // Update `hasBytesBuffered` status. If any bytes were read from the network or
    // if data was returned from read, `hasBytesBuffered` is set to true if any buffered
    // data is still remaining. If not, `hasBytesBuffered` is set to false since no progress
    // can be made until more data is available to read from the network.
    private fun updateBytesBuffered(madeProgress: Boolean) {
        hasBytesBuffered =
            if (madeProgress) netReadBuffer.position() != 0 || appReadBuffer.position() != 0
            else false
    }

    @Throws(IOException::class)
    override fun transferFrom(fileChannel: FileChannel, position: Long, count: Long): Long {
        if (state == State.CLOSING) throw closingException()
        if (state != State.READY) return 0
        if (!flush(netWriteBuffer)) return 0
        val channelSize = fileChannel.size()
        if (position > channelSize) return 0

        val totalBytesToWrite: Int =
            min(count, channelSize - position).coerceAtMost(Int.MAX_VALUE.toLong()).toInt()

        if (_fileChannelBuffer == null) {

            // Pick a size that allows for reasonably efficient disk reads, keeps the memory
            // overhead per connection manageable and can typically be drained in a single `write`
            // call. The `netWriteBuffer` is typically 16k and the socket send buffer is 100k by
            // default, so 32k is a good number given the mentioned trade-offs.
            val transferSize = 32768

            // Allocate a direct buffer to avoid one heap to heap buffer copy. SSLEngine copies the
            // source buffer (fileChannelBuffer) to the destination buffer (netWriteBuffer) and then
            // encrypts in-place. FileChannel.read() to a heap buffer requires a copy from a direct
            // buffer to a heap buffer, which is not useful here.
            _fileChannelBuffer = ByteBuffer.allocateDirect(transferSize)

            // The loop below drains any remaining bytes from the buffer before reading from
            // disk, so we ensure there are no remaining bytes in the empty buffer
        fileChannelBuffer.position(fileChannelBuffer.limit())
        }

        var totalBytesWritten = 0
        var pos = position

        try {
            while (totalBytesWritten < totalBytesToWrite) {
                if (!fileChannelBuffer.hasRemaining()) {
                    fileChannelBuffer.clear()
                    val bytesRemaining = totalBytesToWrite - totalBytesWritten
                    if (bytesRemaining < fileChannelBuffer.limit())
                        fileChannelBuffer.limit(bytesRemaining)
                    val bytesRead = fileChannel.read(fileChannelBuffer, pos)
                    if (bytesRead <= 0) break
                    fileChannelBuffer.flip()
                }

                val networkBytesWritten = write(fileChannelBuffer)
                totalBytesWritten += networkBytesWritten
                // In the case of a partial write we only return the written bytes to the caller. As a result, the
                // `position` passed in the next `transferFrom` call won't include the bytes remaining in
                // `fileChannelBuffer`. By draining `fileChannelBuffer` first, we ensure we update `pos` before
                // we invoke `fileChannel.read`.
                if (fileChannelBuffer.hasRemaining()) break
                pos += networkBytesWritten.toLong()
            }
            return totalBytesWritten.toLong()

        } catch (e: IOException) {
            if (totalBytesWritten > 0) return totalBytesWritten.toLong()
            throw e
        }
    }

    private enum class State {

        /**
         * Initial state.
         */
        NOT_INITIALIZED,

        /**
         * SSLEngine is in handshake mode.
         */
        HANDSHAKE,

        /**
         * SSL handshake failed, connection will be terminated.
         */
        HANDSHAKE_FAILED,

        /**
         * SSLEngine has completed handshake, post-handshake messages may be pending for TLSv1.3.
         */
        POST_HANDSHAKE,

        /**
         * SSLEngine has completed handshake, any post-handshake messages have been processed for
         * TLSv1.3. For TLSv1.3, we move the channel to READY state when incoming data is processed
         * after handshake.
         */
        READY,

        /**
         * Channel is being closed.
         */
        CLOSING
    }

    companion object {

        private val TLS13 = "TLSv1.3"

        @Throws(IOException::class)
        fun create(
            channelId: String,
            key: SelectionKey,
            sslEngine: SSLEngine,
            metadataRegistry: ChannelMetadataRegistry,
        ): SslTransportLayer = SslTransportLayer(
            channelId = channelId,
            key = key,
            sslEngine = sslEngine,
            metadataRegistry = metadataRegistry,
        )
    }
}
