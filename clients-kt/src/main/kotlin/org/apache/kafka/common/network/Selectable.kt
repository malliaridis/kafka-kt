package org.apache.kafka.common.network

import java.io.IOException
import java.net.InetSocketAddress

/**
 * An interface for asynchronous, multi-channel network I/O
 */
interface Selectable {

    /**
     * Begin establishing a socket connection to the given address identified by the given address
     * @param id The id for this connection
     * @param address The address to connect to
     * @param sendBufferSize The send buffer for the socket
     * @param receiveBufferSize The receive buffer for the socket
     * @throws IOException If we cannot begin connecting
     */
    @Throws(IOException::class)
    fun connect(id: String, address: InetSocketAddress, sendBufferSize: Int, receiveBufferSize: Int)

    /**
     * Wakeup this selector if it is blocked on I/O
     */
    fun wakeup()

    /**
     * Close this selector
     */
    fun close()

    /**
     * Close the connection identified by the given id
     */
    fun close(id: String)

    /**
     * Queue the given request for sending in the subsequent [poll()][poll] calls
     * @param send The request to send
     */
    fun send(send: NetworkSend)

    /**
     * Do I/O. Reads, writes, connection establishment, etc.
     * @param timeout The amount of time to block if there is nothing to do
     * @throws IOException
     */
    @Throws(IOException::class)
    fun poll(timeout: Long)

    /**
     * The list of sends that completed on the last [poll()][poll] call.
     */
    fun completedSends(): List<NetworkSend>

    /**
     * The collection of receives that completed on the last [poll()][poll] call.
     */
    fun completedReceives(): Collection<NetworkReceive>

    /**
     * The connections that finished disconnecting on the last [poll()][poll]
     * call. Channel state indicates the local channel state at the time of disconnection.
     */
    fun disconnected(): Map<String, ChannelState>

    /**
     * The list of connections that completed their connection on the last [poll()][poll]
     * call.
     */
    fun connected(): List<String>

    /**
     * Disable reads from the given connection
     * @param id The id for the connection
     */
    fun mute(id: String)

    /**
     * Re-enable reads from the given connection
     * @param id The id for the connection
     */
    fun unmute(id: String)

    /**
     * Disable reads from all connections
     */
    fun muteAll()

    /**
     * Re-enable reads from all connections
     */
    fun unmuteAll()

    /**
     * returns true  if a channel is ready
     * @param id The id for the connection
     */
    fun isChannelReady(id: String): Boolean

    companion object {
        /**
         * See [connect()][connect]
         */
        const val USE_DEFAULT_BUFFER_SIZE = -1
    }
}
