package org.apache.kafka.common.network

import java.io.IOException
import java.net.InetAddress
import java.net.SocketAddress
import java.nio.channels.SelectionKey
import java.util.*
import java.util.function.Supplier
import org.apache.kafka.common.errors.AuthenticationException
import org.apache.kafka.common.errors.SslAuthenticationException
import org.apache.kafka.common.memory.MemoryPool
import org.apache.kafka.common.network.KafkaChannel.ChannelMuteState
import org.apache.kafka.common.security.auth.KafkaPrincipal
import org.apache.kafka.common.security.auth.KafkaPrincipalSerde
import org.apache.kafka.common.utils.Utils

/**
 * A Kafka connection either existing on a client (which could be a broker in an
 * inter-broker scenario) and representing the channel to a remote broker or the
 * reverse (existing on a broker and representing the channel to a remote
 * client, which could be a broker in an inter-broker scenario).
 *
 *
 * Each instance has the following:
 *
 *  * a unique ID identifying it in the `KafkaClient` instance via which
 * the connection was made on the client-side or in the instance where it was
 * accepted on the server-side
 *  * a reference to the underlying [TransportLayer] to allow reading and
 * writing
 *  * an [Authenticator] that performs the authentication (or
 * re-authentication, if that feature is enabled and it applies to this
 * connection) by reading and writing directly from/to the same
 * [TransportLayer].
 *  * a [MemoryPool] into which responses are read (typically the JVM
 * heap for clients, though smaller pools can be used for brokers and for
 * testing out-of-memory scenarios)
 *  * a [NetworkReceive] representing the current incomplete/in-progress
 * request (from the server-side perspective) or response (from the client-side
 * perspective) being read, if applicable; or a non-null value that has had no
 * data read into it yet or a null value if there is no in-progress
 * request/response (either could be the case)
 *  * a [Send] representing the current request (from the client-side
 * perspective) or response (from the server-side perspective) that is either
 * waiting to be sent or partially sent, if applicable, or null
 *  * a [ChannelMuteState] to document if the channel has been muted due
 * to memory pressure or other reasons
 *
 */
open class KafkaChannel(
    private val id: String,
    private val transportLayer: TransportLayer,
    private val authenticatorCreator: Supplier<Authenticator>,
    private val maxReceiveSize: Int,
    private val memoryPool: MemoryPool,
    private val metadataRegistry: ChannelMetadataRegistry
) : AutoCloseable {

    /**
     * Mute States for KafkaChannel:
     *
     *  *  NOT_MUTED: Channel is not muted. This is the default state.
     *  *  MUTED: Channel is muted. Channel must be in this state to be unmuted.
     *  *  MUTED_AND_RESPONSE_PENDING: (SocketServer only) Channel is muted and SocketServer has not sent a response
     * back to the client yet (acks != 0) or is currently waiting to receive a
     * response from the API layer (acks == 0).
     *  *  MUTED_AND_THROTTLED: (SocketServer only) Channel is muted and throttling is in progress due to quota
     * violation.
     *  *  MUTED_AND_THROTTLED_AND_RESPONSE_PENDING: (SocketServer only) Channel is muted, throttling is in progress,
     * and a response is currently pending.
     *
     */
    enum class ChannelMuteState {
        NOT_MUTED,
        MUTED,
        MUTED_AND_RESPONSE_PENDING,
        MUTED_AND_THROTTLED,
        MUTED_AND_THROTTLED_AND_RESPONSE_PENDING
    }

    /** Socket server events that will change the mute state:
     *
     *  *  REQUEST_RECEIVED: A request has been received from the client.
     *  *  RESPONSE_SENT: A response has been sent out to the client (ack != 0) or SocketServer has heard back from
     * the API layer (acks = 0)
     *  *  THROTTLE_STARTED: Throttling started due to quota violation.
     *  *  THROTTLE_ENDED: Throttling ended.
     *
     *
     * Valid transitions on each event are:
     *
     *  *  REQUEST_RECEIVED: MUTED => MUTED_AND_RESPONSE_PENDING
     *  *  RESPONSE_SENT:    MUTED_AND_RESPONSE_PENDING => MUTED, MUTED_AND_THROTTLED_AND_RESPONSE_PENDING =>
     *  MUTED_AND_THROTTLED
     *  *  THROTTLE_STARTED: MUTED_AND_RESPONSE_PENDING => MUTED_AND_THROTTLED_AND_RESPONSE_PENDING
     *  *  THROTTLE_ENDED:   MUTED_AND_THROTTLED => MUTED, MUTED_AND_THROTTLED_AND_RESPONSE_PENDING =>
     *  MUTED_AND_RESPONSE_PENDING
     *
     */
    enum class ChannelMuteEvent {
        REQUEST_RECEIVED,
        RESPONSE_SENT,
        THROTTLE_STARTED,
        THROTTLE_ENDED
    }

    private var authenticator: Authenticator

    // Tracks accumulated network thread time. This is updated on the network thread.
    // The values are read and reset after each response is sent.
    private var networkThreadTimeNanos = 0L
    private var receive: NetworkReceive? = null
    private var send: NetworkSend? = null

    // Track connection and mute state of channels to enable outstanding requests on channels to be
    // processed after the channel is disconnected.
    private var disconnected = false
    private var muteState: ChannelMuteState
    private var state: ChannelState
    private var remoteAddress: SocketAddress? = null
    private var successfulAuthentications = 0
    private var midWrite = false
    private var lastReauthenticationStartNanos: Long = 0

    init {
        authenticator = authenticatorCreator.get()
        muteState = ChannelMuteState.NOT_MUTED
        state = ChannelState.NOT_CONNECTED
    }

    @Throws(IOException::class)
    override fun close() {
        disconnected = true
        Utils.closeAll(transportLayer, authenticator, receive, metadataRegistry)
    }

    /**
     * Returns the principal returned by `authenticator.principal()`.
     */
    fun principal(): KafkaPrincipal? = authenticator.principal()

    fun principalSerde(): KafkaPrincipalSerde? = authenticator.principalSerde()

    /**
     * Does handshake of transportLayer and authentication using configured authenticator.
     * For SSL with client authentication enabled, [TransportLayer.handshake] performs
     * authentication. For SASL, authentication is performed by [Authenticator.authenticate].
     */
    @Throws(AuthenticationException::class, IOException::class)
    fun prepare() {
        var authenticating = false
        try {
            if (!transportLayer.ready()) transportLayer.handshake()
            if (transportLayer.ready() && !authenticator.complete()) {
                authenticating = true
                authenticator.authenticate()
            }
        } catch (exception: AuthenticationException) {
            // Clients are notified of authentication exceptions to enable operations to be terminated
            // without retries. Other errors are handled as network exceptions in Selector.
            val remoteDesc = if (remoteAddress != null) remoteAddress.toString() else null
            state = ChannelState(
                state = ChannelState.State.AUTHENTICATION_FAILED,
                exception = exception,
                remoteAddress = remoteDesc,
            )
            if (authenticating) {
                delayCloseOnAuthenticationFailure()
                throw DelayedResponseAuthenticationException(exception)
            }
            throw exception
        }
        if (ready()) {
            ++successfulAuthentications
            state = ChannelState.READY
        }
    }

    fun disconnect() {
        disconnected = true
        if (state == ChannelState.NOT_CONNECTED && remoteAddress != null) {
            //if we captured the remote address we can provide more information
            state = ChannelState(
                state = ChannelState.State.NOT_CONNECTED,
                remoteAddress = remoteAddress.toString(),
            )
        }
        transportLayer.disconnect()
    }

    fun state(state: ChannelState) {
        this.state = state
    }

    fun state(): ChannelState {
        return state
    }

    @Throws(IOException::class)
    fun finishConnect(): Boolean {
        //we need to grab remoteAddr before finishConnect() is called otherwise
        //it becomes inaccessible if the connection was refused.
        val socketChannel = transportLayer.socketChannel()
        if (socketChannel != null) {
            remoteAddress = socketChannel.remoteAddress
        }
        val connected = transportLayer.finishConnect()
        if (connected) {
            state = if (ready()) {
                ChannelState.READY
            } else if (remoteAddress != null) {
                ChannelState(ChannelState.State.AUTHENTICATE, remoteAddress.toString())
            } else {
                ChannelState.AUTHENTICATE
            }
        }
        return connected
    }

    val isConnected: Boolean
        get() = transportLayer.isConnected

    fun id(): String {
        return id
    }

    fun selectionKey(): SelectionKey {
        return transportLayer.selectionKey()
    }

    /**
     * externally muting a channel should be done via selector to ensure proper state handling
     */
    fun mute() {
        if (muteState == ChannelMuteState.NOT_MUTED) {
            if (!disconnected) transportLayer.removeInterestOps(SelectionKey.OP_READ)
            muteState = ChannelMuteState.MUTED
        }
    }

    /**
     * Unmute the channel. The channel can be unmuted only if it is in the MUTED state. For other muted states
     * (MUTED_AND_*), this is a no-op.
     *
     * @return Whether or not the channel is in the NOT_MUTED state after the call
     */
    fun maybeUnmute(): Boolean {
        if (muteState == ChannelMuteState.MUTED) {
            if (!disconnected) transportLayer.addInterestOps(SelectionKey.OP_READ)
            muteState = ChannelMuteState.NOT_MUTED
        }
        return muteState == ChannelMuteState.NOT_MUTED
    }

    // Handle the specified channel mute-related event and transition the mute state according to the state machine.
    fun handleChannelMuteEvent(event: ChannelMuteEvent) {
        var stateChanged = false
        when (event) {
            ChannelMuteEvent.REQUEST_RECEIVED -> if (muteState == ChannelMuteState.MUTED) {
                muteState = ChannelMuteState.MUTED_AND_RESPONSE_PENDING
                stateChanged = true
            }

            ChannelMuteEvent.RESPONSE_SENT -> {
                if (muteState == ChannelMuteState.MUTED_AND_RESPONSE_PENDING) {
                    muteState = ChannelMuteState.MUTED
                    stateChanged = true
                }
                if (muteState == ChannelMuteState.MUTED_AND_THROTTLED_AND_RESPONSE_PENDING) {
                    muteState = ChannelMuteState.MUTED_AND_THROTTLED
                    stateChanged = true
                }
            }

            ChannelMuteEvent.THROTTLE_STARTED -> if (muteState == ChannelMuteState.MUTED_AND_RESPONSE_PENDING) {
                muteState = ChannelMuteState.MUTED_AND_THROTTLED_AND_RESPONSE_PENDING
                stateChanged = true
            }

            ChannelMuteEvent.THROTTLE_ENDED -> {
                if (muteState == ChannelMuteState.MUTED_AND_THROTTLED) {
                    muteState = ChannelMuteState.MUTED
                    stateChanged = true
                }
                if (muteState == ChannelMuteState.MUTED_AND_THROTTLED_AND_RESPONSE_PENDING) {
                    muteState = ChannelMuteState.MUTED_AND_RESPONSE_PENDING
                    stateChanged = true
                }
            }
        }
        check(stateChanged) { "Cannot transition from " + muteState.name + " for " + event.name }
    }

    fun muteState(): ChannelMuteState {
        return muteState
    }

    /**
     * Delay channel close on authentication failure. This will remove all read/write operations from the channel until
     * [completeCloseOnAuthenticationFailure] is called to finish up the channel close.
     */
    private fun delayCloseOnAuthenticationFailure() {
        transportLayer.removeInterestOps(SelectionKey.OP_WRITE)
    }

    /**
     * Finish up any processing on [prepare] failure.
     * @throws IOException
     */
    @Throws(IOException::class)
    fun completeCloseOnAuthenticationFailure() {
        transportLayer.addInterestOps(SelectionKey.OP_WRITE)
        // Invoke the underlying handler to finish up any processing on authentication failure
        authenticator.handleAuthenticationFailure()
    }

    val isMuted: Boolean
        /**
         * Returns true if this channel has been explicitly muted using [KafkaChannel.mute]
         */
        get() = muteState != ChannelMuteState.NOT_MUTED
    val isInMutableState: Boolean
        get() =//some requests do not require memory, so if we do not know what the current (or future) request is
        //(receive == null) we dont mute. we also dont mute if whatever memory required has already been
        //successfully allocated (if none is required for the currently-being-read request
            //receive.memoryAllocated() is expected to return true)
            if (receive == null || receive!!.memoryAllocated()) false else transportLayer.ready()
    //also cannot mute if underlying transport is not in the ready state

    fun ready(): Boolean {
        return transportLayer.ready() && authenticator.complete()
    }

    fun hasSend(): Boolean {
        return send != null
    }

    /**
     * Returns the address to which this channel's socket is connected or `null` if the socket has never been connected.
     *
     * If the socket was connected prior to being closed, then this method will continue to return the
     * connected address after the socket is closed.
     */
    fun socketAddress(): InetAddress? {
        return transportLayer.socketChannel()?.socket()?.inetAddress
    }

    fun socketDescription(): String {
        val socket = transportLayer.socketChannel()?.socket()
        return (socket?.inetAddress ?: socket?.localAddress).toString()
    }

    fun setSend(send: NetworkSend?) {
        check(this.send == null) {
            "Attempt to begin a send operation with prior send operation still in progress, connection id is $id"
        }
        this.send = send
        transportLayer.addInterestOps(SelectionKey.OP_WRITE)
    }

    fun maybeCompleteSend(): NetworkSend? {
        send?.let {
            if (it.completed()) {
                midWrite = false
                transportLayer.removeInterestOps(SelectionKey.OP_WRITE)
                send = null
                return it
            }
        }
        return null
    }

    @Throws(IOException::class)
    fun read(): Long {
        if (receive == null) {
            receive = NetworkReceive(
                source = id,
                maxSize = maxReceiveSize,
                memoryPool = memoryPool,
            )
        }
        val bytesReceived = receive(receive)
        if (receive!!.requiredMemoryAmountKnown() && !receive!!.memoryAllocated() && isInMutableState) {
            //pool must be out of memory, mute ourselves.
            mute()
        }
        return bytesReceived
    }

    fun currentReceive(): NetworkReceive? {
        return receive
    }

    fun maybeCompleteReceive(): NetworkReceive? {
        receive?.let {
            if (it.complete()) {
                it.payload()?.rewind()
                receive = null
                return it
            }
        }
        return null
    }

    @Throws(IOException::class)
    fun write(): Long {
        if (send == null) return 0
        midWrite = true
        return send!!.writeTo(transportLayer)
    }

    /**
     * Accumulates network thread time for this channel.
     */
    fun addNetworkThreadTimeNanos(nanos: Long) {
        networkThreadTimeNanos += nanos
    }

    val andResetNetworkThreadTimeNanos: Long
        /**
         * Returns accumulated network thread time for this channel and resets
         * the value to zero.
         */
        get() {
            val current = networkThreadTimeNanos
            networkThreadTimeNanos = 0
            return current
        }

    @Throws(IOException::class)
    private fun receive(receive: NetworkReceive?): Long {
        return try {
            receive!!.readFrom(transportLayer)
        } catch (exception: SslAuthenticationException) {
            // With TLSv1.3, post-handshake messages may throw SSLExceptions, which are
            // handled as authentication failures
            val remoteDesc = if (remoteAddress != null) remoteAddress.toString() else null
            state = ChannelState(
                state = ChannelState.State.AUTHENTICATION_FAILED,
                remoteAddress = remoteDesc,
                exception = exception,
            )
            throw exception
        }
    }

    /**
     * @return true if underlying transport has bytes remaining to be read from any underlying intermediate buffers.
     */
    fun hasBytesBuffered(): Boolean {
        return transportLayer.hasBytesBuffered()
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) {
            return true
        }
        if (other == null || javaClass != other.javaClass) {
            return false
        }
        val that = other as KafkaChannel
        return id == that.id
    }

    override fun hashCode(): Int {
        return id.hashCode()
    }

    override fun toString(): String {
        return super.toString() + " id=" + id
    }

    /**
     * Return the number of times this instance has successfully authenticated. This
     * value can only exceed 1 when re-authentication is enabled and it has
     * succeeded at least once.
     *
     * @return the number of times this instance has successfully authenticated
     */
    fun successfulAuthentications(): Int {
        return successfulAuthentications
    }

    /**
     * If this is a server-side connection that has an expiration time and at least
     * 1 second has passed since the prior re-authentication (if any) started then
     * begin the process of re-authenticating the connection and return true,
     * otherwise return false
     *
     * @param saslHandshakeNetworkReceive
     * the mandatory [NetworkReceive] containing the
     * `SaslHandshakeRequest` that has been received on the server
     * and that initiates re-authentication.
     * @param nowNanosSupplier
     * `Supplier` of the current time. The value must be in
     * nanoseconds as per `System.nanoTime()` and is therefore only
     * useful when compared to such a value -- it's absolute value is
     * meaningless.
     *
     * @return true if this is a server-side connection that has an expiration time
     * and at least 1 second has passed since the prior re-authentication
     * (if any) started to indicate that the re-authentication process has
     * begun, otherwise false
     * @throws AuthenticationException
     * if re-authentication fails due to invalid credentials or other
     * security configuration errors
     * @throws IOException
     * if read/write fails due to an I/O error
     * @throws IllegalStateException
     * if this channel is not "ready"
     */
    @Throws(AuthenticationException::class, IOException::class)
    fun maybeBeginServerReauthentication(
        saslHandshakeNetworkReceive: NetworkReceive?,
        nowNanosSupplier: Supplier<Long>
    ): Boolean {
        check(ready()) {
            "KafkaChannel should be \"ready\" when processing SASL Handshake for potential re-authentication"
        }

        /*
         * Re-authentication is disabled if there is no session expiration time, in
         * which case the SASL handshake network receive will be processed normally,
         * which results in a failure result being sent to the client. Also, no need to
         * check if we are muted since we are processing a received packet when we invoke
         * this.
         */
        if (authenticator.serverSessionExpirationTimeNanos() == null) return false

        /*
         * We've delayed getting the time as long as possible in case we don't need it,
         * but at this point we need it -- so get it now.
         */
        val nowNanos = nowNanosSupplier.get()

        /*
         * Cannot re-authenticate more than once every second; an attempt to do so will
         * result in the SASL handshake network receive being processed normally, which
         * results in a failure result being sent to the client.
         */if (lastReauthenticationStartNanos != 0L
            && nowNanos - lastReauthenticationStartNanos < MIN_REAUTH_INTERVAL_ONE_SECOND_NANOS
        ) return false

        lastReauthenticationStartNanos = nowNanos
        swapAuthenticatorsAndBeginReauthentication(
            ReauthenticationContext(authenticator, saslHandshakeNetworkReceive, nowNanos)
        )
        return true
    }

    /**
     * If this is a client-side connection that is not muted, there is no
     * in-progress write, and there is a session expiration time defined that has
     * past then begin the process of re-authenticating the connection and return
     * true, otherwise return false
     *
     * @param nowNanosSupplier
     * `Supplier` of the current time. The value must be in
     * nanoseconds as per `System.nanoTime()` and is therefore only
     * useful when compared to such a value -- it's absolute value is
     * meaningless.
     *
     * @return true if this is a client-side connection that is not muted, there is
     * no in-progress write, and there is a session expiration time defined
     * that has past to indicate that the re-authentication process has
     * begun, otherwise false
     * @throws AuthenticationException
     * if re-authentication fails due to invalid credentials or other
     * security configuration errors
     * @throws IOException
     * if read/write fails due to an I/O error
     * @throws IllegalStateException
     * if this channel is not "ready"
     */
    @Throws(AuthenticationException::class, IOException::class)
    fun maybeBeginClientReauthentication(nowNanosSupplier: Supplier<Long>): Boolean {
        check(ready()) { "KafkaChannel should always be \"ready\" when it is checked for possible re-authentication" }
        val reAuthenticationNanos = authenticator.clientSessionReauthenticationTimeNanos()
        if (
            muteState != ChannelMuteState.NOT_MUTED
            || midWrite
            || reAuthenticationNanos == null
        ) return false

        /*
         * We've delayed getting the time as long as possible in case we don't need it,
         * but at this point we need it -- so get it now.
         */
        val nowNanos = nowNanosSupplier.get()
        if (nowNanos < reAuthenticationNanos) return false
        swapAuthenticatorsAndBeginReauthentication(ReauthenticationContext(authenticator, receive, nowNanos))
        receive = null
        return true
    }

    /**
     * Return the number of milliseconds that elapsed while re-authenticating this
     * session from the perspective of this instance, if applicable, otherwise null.
     * The server-side perspective will yield a lower value than the client-side
     * perspective of the same re-authentication because the client-side observes an
     * additional network round-trip.
     *
     * @return the number of milliseconds that elapsed while re-authenticating this
     * session from the perspective of this instance, if applicable,
     * otherwise null
     */
    fun reauthenticationLatencyMs(): Long? = authenticator.reauthenticationLatencyMs()

    /**
     * Return true if this is a server-side channel and the given time is past the
     * session expiration time, if any, otherwise false
     *
     * @param nowNanos
     * the current time in nanoseconds as per `System.nanoTime()`
     * @return true if this is a server-side channel and the given time is past the
     * session expiration time, if any, otherwise false
     */
    fun serverAuthenticationSessionExpired(nowNanos: Long): Boolean {
        val serverSessionExpirationTimeNanos = authenticator.serverSessionExpirationTimeNanos()
        return serverSessionExpirationTimeNanos != null && nowNanos - serverSessionExpirationTimeNanos > 0
    }

    /**
     * Return the (always non-null but possibly empty) client-side
     * [NetworkReceive] response that arrived during re-authentication but
     * is unrelated to re-authentication. This corresponds to a request sent
     * prior to the beginning of re-authentication; the request was made when the
     * channel was successfully authenticated, and the response arrived during the
     * re-authentication process.
     *
     * @return client-side [NetworkReceive] response that arrived during
     * re-authentication that is unrelated to re-authentication. This may
     * be empty.
     */
    fun pollResponseReceivedDuringReauthentication(): NetworkReceive? {
        return authenticator.pollResponseReceivedDuringReauthentication()
    }

    /**
     * Return true if this is a server-side channel and the connected client has
     * indicated that it supports re-authentication, otherwise false
     *
     * @return true if this is a server-side channel and the connected client has
     * indicated that it supports re-authentication, otherwise false
     */
    fun connectedClientSupportsReauthentication(): Boolean {
        return authenticator.connectedClientSupportsReauthentication()
    }

    @Throws(IOException::class)
    private fun swapAuthenticatorsAndBeginReauthentication(reauthenticationContext: ReauthenticationContext) {
        // it is up to the new authenticator to close the old one
        // replace with a new one and begin the process of re-authenticating
        authenticator = authenticatorCreator.get()
        authenticator.reauthenticate(reauthenticationContext)
    }

    fun channelMetadataRegistry(): ChannelMetadataRegistry {
        return metadataRegistry
    }

    companion object {
        private const val MIN_REAUTH_INTERVAL_ONE_SECOND_NANOS = (1000 * 1000 * 1000).toLong()
    }
}
