package org.apache.kafka.common.network

import java.io.IOException
import java.nio.channels.ScatteringByteChannel
import java.nio.channels.SelectionKey
import java.nio.channels.SocketChannel
import java.security.Principal
import org.apache.kafka.common.errors.AuthenticationException

interface TransportLayer : ScatteringByteChannel, TransferableChannel {

    /**
     * Returns true if the channel has handshake and authentication done.
     */
    fun ready(): Boolean

    /**
     * Finishes the process of connecting a socket channel.
     */
    @Throws(IOException::class)
    fun finishConnect(): Boolean

    /**
     * disconnect socketChannel
     */
    fun disconnect()

    /**
     * Tells whether or not this channel's network socket is connected.
     */
    val isConnected: Boolean

    /**
     * returns underlying socketChannel
     */
    fun socketChannel(): SocketChannel?

    /**
     * Get the underlying selection key
     */
    fun selectionKey(): SelectionKey

    /**
     * This a no-op for the non-secure PLAINTEXT implementation. For SSL, this performs
     * SSL handshake. The SSL handshake includes client authentication if configured using
     * [org.apache.kafka.common.config.internals.BrokerSecurityConfigs].
     * @throws AuthenticationException if handshake fails due to an [javax.net.ssl.SSLException].
     * @throws IOException if read or write fails with an I/O error.
     */
    @Throws(AuthenticationException::class, IOException::class)
    fun handshake()

    /**
     * Returns `SSLSession.getPeerPrincipal()` if this is an SslTransportLayer and there is an authenticated peer,
     * `KafkaPrincipal.ANONYMOUS` is returned otherwise.
     */
    @Throws(IOException::class)
    fun peerPrincipal(): Principal?

    fun addInterestOps(ops: Int)

    fun removeInterestOps(ops: Int)

    val isMute: Boolean

    /**
     * @return true if channel has bytes to be read in any intermediate buffers
     * which may be processed without reading additional data from the network.
     */
    fun hasBytesBuffered(): Boolean
}
