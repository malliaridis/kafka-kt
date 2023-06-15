package org.apache.kafka.common.network

import java.io.IOException
import java.net.InetSocketAddress
import java.nio.channels.CancelledKeyException
import java.nio.channels.SelectionKey
import java.nio.channels.SocketChannel
import java.nio.channels.UnresolvedAddressException
import java.util.*
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.MetricName
import org.apache.kafka.common.errors.AuthenticationException
import org.apache.kafka.common.memory.MemoryPool
import org.apache.kafka.common.metrics.Measurable
import org.apache.kafka.common.metrics.MetricConfig
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.metrics.Sensor
import org.apache.kafka.common.metrics.internals.IntGaugeSuite
import org.apache.kafka.common.metrics.stats.Avg
import org.apache.kafka.common.metrics.stats.CumulativeSum
import org.apache.kafka.common.metrics.stats.Max
import org.apache.kafka.common.metrics.stats.Meter
import org.apache.kafka.common.metrics.stats.SampledStat
import org.apache.kafka.common.metrics.stats.WindowedCount
import org.apache.kafka.common.requests.DeleteAclsResponse.log
import org.apache.kafka.common.utils.LogContext
import org.apache.kafka.common.utils.Time
import org.apache.kafka.common.utils.Utils
import org.slf4j.Logger
import kotlin.collections.ArrayList
import kotlin.collections.HashMap
import kotlin.collections.HashSet

/**
 * A nioSelector interface for doing non-blocking multi-connection network I/O.
 *
 * This class works with [NetworkSend] and [NetworkReceive] to transmit size-delimited network
 * requests and responses.
 *
 * A connection can be added to the nioSelector associated with an integer id by doing
 *
 * ```java
 * nioSelector.connect("42", new InetSocketAddress("google.com", server.port), 64000, 64000);
 * ```
 *
 * The connect call does not block on the creation of the TCP connection, so the connect method only
 * begins initiating the connection. The successful invocation of this method does not mean a valid
 * connection has been established.
 *
 * Sending requests, receiving responses, processing connection completions, and disconnections on
 * the existing connections are all done using the `poll()` call.
 *
 * ```java
 * nioSelector.send(new NetworkSend(myDestination, myBytes));
 * nioSelector.send(new NetworkSend(myOtherDestination, myOtherBytes));
 * nioSelector.poll(TIMEOUT_MS);
 * ```
 *
 * The nioSelector maintains several lists that are reset by each call to `poll()` which are
 * available via various getters. These are reset by each call to `poll()`.
 *
 * This class is not thread safe!
 */
class Selector private constructor(
    private val maxReceiveSize: Int = NetworkReceive.UNLIMITED,
    private val failedAuthenticationDelayMs: Int = NO_FAILED_AUTHENTICATION_DELAY,
    private val time: Time,
    private val recordTimePerConnection: Boolean = false,
    private val channelBuilder: ChannelBuilder,
    private val memoryPool: MemoryPool = MemoryPool.NONE,
    private val idleExpiryManager: IdleExpiryManager?,
    private val lowMemThreshold: Long,
    private val delayedClosingChannels: LinkedHashMap<String, DelayedAuthenticationFailureClose>?,
    private val log: Logger,
) : Selectable, AutoCloseable {

    private var nioSelector: java.nio.channels.Selector? = null

    private val channels: MutableMap<String, KafkaChannel> = HashMap()

    private val explicitlyMutedChannels: MutableSet<KafkaChannel?> = HashSet()

    //package-private for testing
    internal var isOutOfMemory: Boolean = false
        private set

    private val completedSends: MutableList<NetworkSend> = ArrayList()

    private val completedReceives: LinkedHashMap<String, NetworkReceive> = LinkedHashMap()

    private val immediatelyConnectedKeys: MutableSet<SelectionKey?> = HashSet()

    private val closingChannels: MutableMap<String?, KafkaChannel> = HashMap()

    private var keysWithBufferedRead: MutableSet<SelectionKey?> = HashSet()

    private val disconnected: MutableMap<String, ChannelState> = HashMap()

    private val connected: MutableList<String> = ArrayList()

    private val failedSends: MutableList<String> = ArrayList()

    private lateinit var sensors: SelectorMetrics

    //package-private for testing
    //indicates if the previous call to poll was able to make progress in reading already-buffered data.
    //this is used to prevent tight loops when memory is not available to read any more data
    internal var isMadeReadProgressLastPoll = true
        private set

    /**
     * Create a new nioSelector
     *
     * @param maxReceiveSize Max size in bytes of a single network receive (use
     * [NetworkReceive.UNLIMITED] for no limit)
     * @param connectionMaxIdleMs Max idle connection time (use [NO_IDLE_TIMEOUT_MS] to disable idle
     * timeout)
     * @param failedAuthenticationDelayMs Minimum time by which failed authentication response and
     * channel close should be delayed by. Use [NO_FAILED_AUTHENTICATION_DELAY] to disable this
     * delay.
     * @param metrics Registry for Selector metrics
     * @param time Time implementation
     * @param metricGrpPrefix Prefix for the group of metrics registered by Selector
     * @param metricTags Additional tags to add to metrics registered by Selector
     * @param metricsPerConnection Whether or not to enable per-connection metrics
     * @param channelBuilder Channel builder for every new connection
     * @param logContext Context for logging with additional info
     */
    constructor(
        maxReceiveSize: Int = NetworkReceive.UNLIMITED,
        connectionMaxIdleMs: Long,
        failedAuthenticationDelayMs: Int = NO_FAILED_AUTHENTICATION_DELAY,
        metrics: Metrics,
        time: Time,
        metricGrpPrefix: String,
        metricTags: MutableMap<String, String> = mutableMapOf(),
        metricsPerConnection: Boolean = true,
        recordTimePerConnection: Boolean = false,
        channelBuilder: ChannelBuilder,
        memoryPool: MemoryPool = MemoryPool.NONE,
        logContext: LogContext
    ) : this(
        maxReceiveSize = maxReceiveSize,
        failedAuthenticationDelayMs = failedAuthenticationDelayMs,
        time = time,
        recordTimePerConnection = recordTimePerConnection,
        channelBuilder = channelBuilder,
        memoryPool = memoryPool,
        idleExpiryManager = if (connectionMaxIdleMs < 0) null
        else IdleExpiryManager(time, connectionMaxIdleMs),
        lowMemThreshold = (0.1 * memoryPool.size()).toLong(),
        delayedClosingChannels =
        if (failedAuthenticationDelayMs > NO_FAILED_AUTHENTICATION_DELAY) LinkedHashMap()
        else null,
        log = logContext.logger(Selector::class.java),
    ) {
        try {
            nioSelector = java.nio.channels.Selector.open()
        } catch (e: IOException) {
            throw KafkaException(cause = e)
        }
        this.sensors = SelectorMetrics(metrics, metricGrpPrefix, metricTags, metricsPerConnection)
    }

    /**
     * Begin connecting to the given address and add the connection to this nioSelector associated
     * with the given id number.
     *
     * Note that this call only initiates the connection, which will be completed on a future [poll]
     * call. Check [connected] to see which (if any) connections have completed after a given poll call.
     * @param id The id for the new connection
     * @param address The address to connect to
     * @param sendBufferSize The send buffer for the new connection
     * @param receiveBufferSize The receive buffer for the new connection
     * @throws IllegalStateException if there is already a connection for that id
     * @throws IOException if DNS resolution fails on the hostname or if the broker is down
     */
    @Throws(IOException::class)
    override fun connect(
        id: String,
        address: InetSocketAddress,
        sendBufferSize: Int,
        receiveBufferSize: Int,
    ) {
        ensureNotRegistered(id)
        val socketChannel = SocketChannel.open()
        var key: SelectionKey? = null
        try {
            configureSocketChannel(socketChannel, sendBufferSize, receiveBufferSize)
            val connected = doConnect(socketChannel, address)
            key = registerChannel(id, socketChannel, SelectionKey.OP_CONNECT)
            if (connected) {
                // OP_CONNECT won't trigger for immediately connected channels
                log.debug("Immediately connected to node {}", id)
                immediatelyConnectedKeys.add(key)
                key.interestOps(0)
            }
        } catch (e: IOException) {
            if (key != null) immediatelyConnectedKeys.remove(key)
            channels.remove(id)
            socketChannel.close()
            throw e
        } catch (e: RuntimeException) {
            if (key != null) immediatelyConnectedKeys.remove(key)
            channels.remove(id)
            socketChannel.close()
            throw e
        }
    }

    // Visible to allow test cases to override. In particular, we use this to implement a blocking connect
    // in order to simulate "immediately connected" sockets.
    @Throws(IOException::class)
    internal fun doConnect(channel: SocketChannel, address: InetSocketAddress?): Boolean {
        return try {
            channel.connect(address)
        } catch (e: UnresolvedAddressException) {
            throw IOException("Can't resolve address: $address", e)
        }
    }

    @Throws(IOException::class)
    private fun configureSocketChannel(socketChannel: SocketChannel, sendBufferSize: Int, receiveBufferSize: Int) {
        socketChannel.configureBlocking(false)
        val socket = socketChannel.socket()
        socket.keepAlive = true
        if (sendBufferSize != Selectable.USE_DEFAULT_BUFFER_SIZE) socket.sendBufferSize = sendBufferSize
        if (receiveBufferSize != Selectable.USE_DEFAULT_BUFFER_SIZE) socket.receiveBufferSize = receiveBufferSize
        socket.tcpNoDelay = true
    }

    /**
     * Register the nioSelector with an existing channel.
     *
     * Use this on server-side, when a connection is accepted by a different thread but processed by
     * the Selector.
     *
     * If a connection already exists with the same connection id in `channels` or
     * `closingChannels`, an exception is thrown. Connection ids must be chosen to avoid conflict
     * when remote ports are reused. Kafka brokers add an incrementing index to the connection id to
     * avoid reuse in the timing window where an existing connection may not yet have been closed by
     * the broker when a new connection with the same remote host:port is processed.
     *
     * If a `KafkaChannel` cannot be created for this connection, the `socketChannel` is closed and
     * its selection key cancelled.
     */
    @Throws(IOException::class)
    fun register(id: String, socketChannel: SocketChannel) {
        ensureNotRegistered(id)
        registerChannel(id, socketChannel, SelectionKey.OP_READ)
        sensors.connectionCreated.record()
        // Default to empty client information as the ApiVersionsRequest is not
        // mandatory. In this case, we still want to account for the connection.
        val metadataRegistry = this.channel(id)!!.channelMetadataRegistry()
        if (metadataRegistry.clientInformation() == null)
            metadataRegistry.registerClientInformation(ClientInformation.EMPTY)
    }

    private fun ensureNotRegistered(id: String) {
        check(!channels.containsKey(id)) { "There is already a connection for id $id" }
        check(!closingChannels.containsKey(id)) {
            "There is already a connection for id $id that is still being closed"
        }
    }

    @Throws(IOException::class)
    internal fun registerChannel(
        id: String,
        socketChannel: SocketChannel,
        interestedOps: Int,
    ): SelectionKey {
        val key = socketChannel.register(nioSelector, interestedOps)
        val channel = buildAndAttachKafkaChannel(socketChannel, id, key)

        channels[id] = channel
        idleExpiryManager?.update(channel.id(), time.nanoseconds())

        return key
    }

    @Throws(IOException::class)
    private fun buildAndAttachKafkaChannel(
        socketChannel: SocketChannel,
        id: String,
        key: SelectionKey
    ): KafkaChannel {
        return try {
            val channel = channelBuilder.buildChannel(
                id = id,
                key = key,
                maxReceiveSize = maxReceiveSize,
                memoryPool = memoryPool,
                metadataRegistry = SelectorChannelMetadataRegistry(),
            )
            key.attach(channel)
            channel
        } catch (e: Exception) {
            try {
                socketChannel.close()
            } finally {
                key.cancel()
            }
            throw IOException("Channel could not be created for socket $socketChannel", e)
        }
    }

    /**
     * Interrupt the nioSelector if it is blocked waiting to do I/O.
     */
    override fun wakeup() {
        nioSelector!!.wakeup()
    }

    /**
     * Close this selector and all associated connections
     */
    override fun close() {
        val connections: MutableList<String> = ArrayList(channels.keys)
        val firstException = AtomicReference<Throwable?>()
        Utils.closeAllQuietly(
            firstException = firstException,
            name = "release connections",
            closeables = connections.map { AutoCloseable { close(it) }},
        )

        // If there is any exception thrown in close(id), we should still be able
        // to close the remaining objects, especially the sensors because keeping
        // the sensors may lead to failure to start up the ReplicaFetcherThread if
        // the old sensors with the same names has not yet been cleaned up.
        Utils.closeQuietly(nioSelector, "nioSelector", firstException)
        Utils.closeQuietly(sensors, "sensors", firstException)
        Utils.closeQuietly(channelBuilder, "channelBuilder", firstException)
        val exception = firstException.get()
        if (exception is RuntimeException && exception !is SecurityException) {
            throw exception
        }
    }

    /**
     * Queue the given request for sending in the subsequent [poll] calls
     * @param send The request to send
     */
    override fun send(send: NetworkSend) {
        val connectionId = send.destinationId()
        val channel = openOrClosingChannelOrFail(connectionId)
        if (closingChannels.containsKey(connectionId)) {
            // ensure notification via `disconnected`, leave channel in the state in which closing was triggered
            failedSends.add(connectionId)
        } else {
            try {
                channel.setSend(send)
            } catch (exception: Exception) {
                // update the state for consistency, the channel will be discarded after `close`
                channel.state(ChannelState.FAILED_SEND)
                // ensure notification via `disconnected` when `failedSends` are processed in the next poll
                failedSends.add(connectionId)
                close(channel, CloseMode.DISCARD_NO_NOTIFY)
                if (exception !is CancelledKeyException) {
                    log.error(
                        "Unexpected exception during send, closing connection {} and rethrowing exception {}",
                        connectionId,
                        exception
                    )
                    throw exception
                }
            }
        }
    }

    /**
     * Do whatever I/O can be done on each connection without blocking. This includes completing
     * connections, completing disconnections, initiating new sends, or making progress on
     * in-progress sends or receives.
     *
     * When this call is completed the user can check for completed sends, receives, connections or
     * disconnects using [completedSends], [completedReceives], [connected], [disconnected]. These
     * lists will be cleared at the beginning of each `poll` call and repopulated by the call if
     * there is any completed I/O.
     *
     * In the "Plaintext" setting, we are using socketChannel to read & write to the network. But
     * for the "SSL" setting, we encrypt the data before we use socketChannel to write data to the
     * network, and decrypt before we return the responses. This requires additional buffers to be
     * maintained as we are reading from network, since the data on the wire is encrypted we won't
     * be able to read exact no.of bytes as kafka protocol requires. We read as many bytes as we
     * can, up to SSLEngine's application buffer size. This means we might be reading additional
     * bytes than the requested size. If there is no further data to read from socketChannel
     * selector won't invoke that channel and we have additional bytes in the buffer. To overcome
     * this issue we added "keysWithBufferedRead" map which tracks channels which have data in the
     * SSL buffers. If there are channels with buffered data that can by processed, we set "timeout"
     * to 0 and process the data even if there is no more data to read from the socket.
     *
     * At most one entry is added to "completedReceives" for a channel in each poll. This is
     * necessary to guarantee that requests from a channel are processed on the broker in the order
     * they are sent. Since outstanding requests added by SocketServer to the request queue may be
     * processed by different request handler threads, requests on each channel must be processed
     * one-at-a-time to guarantee ordering.
     *
     * @param timeout The amount of time to wait, in milliseconds, which must be non-negative
     * @throws IllegalArgumentException If `timeout` is negative
     * @throws IllegalStateException If a send is given for which we have no existing connection or
     * for which there is already an in-progress send
     */
    @Throws(IOException::class)
    override fun poll(timeout: Long) {
        var timeoutVariable = timeout
        require(timeoutVariable >= 0) { "timeout should be >= 0" }
        val madeReadProgressLastCall = isMadeReadProgressLastPoll
        clear()
        val dataInBuffers = keysWithBufferedRead.isNotEmpty()

        if (immediatelyConnectedKeys.isNotEmpty() || madeReadProgressLastCall && dataInBuffers)
            timeoutVariable = 0

        if (!memoryPool.isOutOfMemory && isOutOfMemory) {
            //we have recovered from memory pressure. unmute any channel not explicitly muted for other reasons
            log.trace("Broker no longer low on memory - unmuting incoming sockets")
            for (channel in channels.values) {
                if (channel.isInMutableState && !explicitlyMutedChannels.contains(channel)) {
                    channel.maybeUnmute()
                }
            }
            isOutOfMemory = false
        }

        /* check ready keys */
        val startSelect = time.nanoseconds()
        val numReadyKeys = select(timeoutVariable)
        val endSelect = time.nanoseconds()
        sensors.selectTime.record((endSelect - startSelect).toDouble(), time.milliseconds(), false)
        if (numReadyKeys > 0 || !immediatelyConnectedKeys.isEmpty() || dataInBuffers) {
            val readyKeys = nioSelector!!.selectedKeys()

            // Poll from channels that have buffered data (but nothing more from the underlying socket)
            if (dataInBuffers) {
                keysWithBufferedRead.removeAll(readyKeys) //so no channel gets polled twice
                val toPoll = keysWithBufferedRead
                keysWithBufferedRead = HashSet() //poll() calls will repopulate if needed
                pollSelectionKeys(toPoll, false, endSelect)
            }

            // Poll from channels where the underlying socket has more data
            pollSelectionKeys(readyKeys, false, endSelect)
            // Clear all selected keys so that they are included in the ready count for the next select
            readyKeys.clear()
            pollSelectionKeys(immediatelyConnectedKeys, true, endSelect)
            immediatelyConnectedKeys.clear()
        } else {
            isMadeReadProgressLastPoll = true //no work is also "progress"
        }
        val endIo = time.nanoseconds()
        sensors.ioTime.record((endIo - endSelect).toDouble(), time.milliseconds(), false)

        // Close channels that were delayed and are now ready to be closed
        completeDelayedChannelClose(endIo)

        // we use the time at the end of select to ensure that we don't close any connections that
        // have just been processed in pollSelectionKeys
        maybeCloseOldestConnection(endSelect)
    }

    /**
     * handle any ready I/O on a set of selection keys
     * @param selectionKeys set of keys to handle
     * @param isImmediatelyConnected true if running over a set of keys for just-connected sockets
     * @param currentTimeNanos time at which set of keys was determined
     */
    // package-private for testing
    fun pollSelectionKeys(
        selectionKeys: MutableSet<SelectionKey?>,
        isImmediatelyConnected: Boolean,
        currentTimeNanos: Long
    ) {
        for (key in determineHandlingOrder(selectionKeys)) {
            val channel = channel(key)
            val channelStartTimeNanos = if (recordTimePerConnection) time.nanoseconds() else 0
            var sendFailed = false
            val nodeId = channel.id()

            // register all per-connection metrics at once
            sensors.maybeRegisterConnectionMetrics(nodeId)
            idleExpiryManager?.update(nodeId, currentTimeNanos)
            try {
                // complete any connections that have finished their handshake (either normally or
                // immediately)
                if (isImmediatelyConnected || key!!.isConnectable) {
                    if (channel.finishConnect()) {
                        connected.add(nodeId)
                        sensors.connectionCreated.record()
                        val socketChannel = key!!.channel() as SocketChannel
                        log.debug(
                            "Created socket with SO_RCVBUF = {}, SO_SNDBUF = {}, SO_TIMEOUT = {} to node {}",
                            socketChannel.socket().receiveBufferSize,
                            socketChannel.socket().sendBufferSize,
                            socketChannel.socket().soTimeout,
                            nodeId
                        )
                    } else {
                        continue
                    }
                }

                /* if channel is not ready finish prepare */if (channel.isConnected && !channel.ready()) {
                    channel.prepare()
                    if (channel.ready()) {
                        val readyTimeMs = time.milliseconds()
                        val isReauthentication = channel.successfulAuthentications() > 1
                        if (isReauthentication) {
                            sensors.successfulReauthentication.record(1.0, readyTimeMs)
                            if (channel.reauthenticationLatencyMs() == null) log.warn(
                                "Should never happen: re-authentication latency for a re-authenticated channel was null; continuing..."
                            ) else sensors.reauthenticationLatency
                                .record(channel.reauthenticationLatencyMs()!!.toDouble(), readyTimeMs)
                        } else {
                            sensors.successfulAuthentication.record(1.0, readyTimeMs)
                            if (!channel.connectedClientSupportsReauthentication()) sensors.successfulAuthenticationNoReauth.record(
                                1.0,
                                readyTimeMs
                            )
                        }
                        log.debug(
                            "Successfully {}authenticated with {}",
                            if (isReauthentication) "re-" else "",
                            channel.socketDescription()
                        )
                    }
                }
                if (channel.ready() && channel.state() == ChannelState.NOT_CONNECTED) channel.state(ChannelState.READY)
                val responseReceivedDuringReauthentication: NetworkReceive? =
                    channel.pollResponseReceivedDuringReauthentication()
                responseReceivedDuringReauthentication?.let { receive: NetworkReceive ->
                    val currentTimeMs = time.milliseconds()
                    addToCompletedReceives(channel, receive, currentTimeMs)
                }

                //if channel is ready and has bytes to read from socket or buffer, and has no
                //previous completed receive then read from it
                if (channel.ready() && (key.isReadable || channel.hasBytesBuffered()) && !hasCompletedReceive(channel)
                    && !explicitlyMutedChannels.contains(channel)
                ) {
                    attemptRead(channel)
                }
                if (channel.hasBytesBuffered() && !explicitlyMutedChannels.contains(channel)) {
                    //this channel has bytes enqueued in intermediary buffers that we could not read
                    //(possibly because no memory). it may be the case that the underlying socket will
                    //not come up in the next poll() and so we need to remember this channel for the
                    //next poll call otherwise data may be stuck in said buffers forever. If we attempt
                    //to process buffered data and no progress is made, the channel buffered status is
                    //cleared to avoid the overhead of checking every time.
                    keysWithBufferedRead.add(key)
                }

                /* if channel is ready write to any sockets that have space in their buffer and for which we have data */
                val nowNanos = if (channelStartTimeNanos != 0L) channelStartTimeNanos else currentTimeNanos
                try {
                    attemptWrite(key, channel, nowNanos)
                } catch (e: Exception) {
                    sendFailed = true
                    throw e
                }

                /* cancel any defunct sockets */if (!key.isValid) close(channel, CloseMode.GRACEFUL)
            } catch (e: Exception) {
                val desc = String.format("%s (channelId=%s)", channel.socketDescription(), channel.id())
                if (e is IOException) {
                    log.debug("Connection with {} disconnected", desc, e)
                } else if (e is AuthenticationException) {
                    val isReauthentication = channel.successfulAuthentications() > 0
                    if (isReauthentication) sensors.failedReauthentication.record() else sensors.failedAuthentication.record()
                    var exceptionMessage = e.message
                    if (e is DelayedResponseAuthenticationException) exceptionMessage = e.cause!!.message
                    log.info(
                        "Failed {}authentication with {} ({})",
                        if (isReauthentication) "re-" else "",
                        desc,
                        exceptionMessage
                    )
                } else {
                    log.warn("Unexpected error from {}; closing connection", desc, e)
                }
                if (e is DelayedResponseAuthenticationException) maybeDelayCloseOnAuthenticationFailure(channel)
                else close(
                    channel,
                    if (sendFailed) CloseMode.NOTIFY_ONLY else CloseMode.GRACEFUL
                )
            } finally {
                maybeRecordTimePerConnection(channel, channelStartTimeNanos)
            }
        }
    }

    @Throws(IOException::class)
    private fun attemptWrite(key: SelectionKey?, channel: KafkaChannel, nowNanos: Long) {
        if (channel.hasSend()
            && channel.ready()
            && key!!.isWritable
            && !channel.maybeBeginClientReauthentication { nowNanos }
        ) {
            write(channel)
        }
    }

    // package-private for testing
    @Throws(IOException::class)
    fun write(channel: KafkaChannel) {
        val nodeId = channel.id()
        val bytesSent = channel.write()
        val send = channel.maybeCompleteSend()
        // We may complete the send with bytesSent < 1 if `TransportLayer.hasPendingWrites` was true and `channel.write()`
        // caused the pending writes to be written to the socket channel buffer
        if (bytesSent > 0 || send != null) {
            val currentTimeMs = time.milliseconds()
            if (bytesSent > 0) sensors.recordBytesSent(nodeId, bytesSent, currentTimeMs)
            if (send != null) {
                completedSends.add(send)
                sensors.recordCompletedSend(nodeId, send.size(), currentTimeMs)
            }
        }
    }

    private fun determineHandlingOrder(selectionKeys: MutableSet<SelectionKey?>): MutableCollection<SelectionKey?> {
        //it is possible that the iteration order over selectionKeys is the same every invocation.
        //this may cause starvation of reads when memory is low. to address this we shuffle the keys if memory is low.
        return if (!isOutOfMemory && memoryPool.availableMemory() < lowMemThreshold) {
            val shuffledKeys: MutableList<SelectionKey?> = ArrayList(selectionKeys)
            shuffledKeys.shuffle()
            shuffledKeys
        } else selectionKeys
    }

    @Throws(IOException::class)
    private fun attemptRead(channel: KafkaChannel) {
        val nodeId = channel.id()
        val bytesReceived = channel.read()
        if (bytesReceived != 0L) {
            val currentTimeMs = time.milliseconds()
            sensors.recordBytesReceived(nodeId, bytesReceived, currentTimeMs)
            isMadeReadProgressLastPoll = true
            val receive = channel.maybeCompleteReceive()
            receive?.let { addToCompletedReceives(channel, it, currentTimeMs) }
        }
        if (channel.isMuted) {
            isOutOfMemory = true //channel has muted itself due to memory pressure.
        } else {
            isMadeReadProgressLastPoll = true
        }
    }

    private fun maybeReadFromClosingChannel(channel: KafkaChannel): Boolean {
        val hasPending: Boolean
        hasPending =
            if (channel.state().state() !== ChannelState.State.READY) false else if (explicitlyMutedChannels.contains(
                    channel
                ) || hasCompletedReceive(channel)
            ) true else {
                try {
                    attemptRead(channel)
                    hasCompletedReceive(channel)
                } catch (e: Exception) {
                    log.trace("Read from closing channel failed, ignoring exception", e)
                    false
                }
            }
        return hasPending
    }

    // Record time spent in pollSelectionKeys for channel (moved into a method to keep checkstyle happy)
    private fun maybeRecordTimePerConnection(channel: KafkaChannel, startTimeNanos: Long) {
        if (recordTimePerConnection) channel.addNetworkThreadTimeNanos(time.nanoseconds() - startTimeNanos)
    }

    override fun completedSends(): MutableList<NetworkSend> {
        return completedSends
    }

    override fun completedReceives(): MutableCollection<NetworkReceive> {
        return completedReceives.values
    }

    override fun disconnected(): MutableMap<String, ChannelState> {
        return disconnected
    }

    override fun connected(): MutableList<String> {
        return connected
    }

    override fun mute(id: String) {
        val channel = openOrClosingChannelOrFail(id)
        mute(channel)
    }

    private fun mute(channel: KafkaChannel?) {
        channel!!.mute()
        explicitlyMutedChannels.add(channel)
        keysWithBufferedRead.remove(channel.selectionKey())
    }

    override fun unmute(id: String) {
        val channel = openOrClosingChannelOrFail(id)
        unmute(channel)
    }

    private fun unmute(channel: KafkaChannel?) {
        // Remove the channel from explicitlyMutedChannels only if the channel has been actually unmuted.
        if (channel!!.maybeUnmute()) {
            explicitlyMutedChannels.remove(channel)
            if (channel.hasBytesBuffered()) {
                keysWithBufferedRead.add(channel.selectionKey())
                isMadeReadProgressLastPoll = true
            }
        }
    }

    override fun muteAll() {
        for (channel in channels.values) mute(channel)
    }

    override fun unmuteAll() {
        for (channel in channels.values) unmute(channel)
    }

    // package-private for testing
    fun completeDelayedChannelClose(currentTimeNanos: Long) {
        if (delayedClosingChannels == null) return
        while (!delayedClosingChannels.isEmpty()) {
            val delayedClose = delayedClosingChannels.values.iterator().next()
            if (!delayedClose.tryClose(currentTimeNanos)) break
        }
    }

    private fun maybeCloseOldestConnection(currentTimeNanos: Long) {
        if (idleExpiryManager == null) return
        val expiredConnection = idleExpiryManager.pollExpiredConnection(currentTimeNanos)
        if (expiredConnection != null) {
            val connectionId = expiredConnection.key
            val channel = channels[connectionId]
            if (channel != null) {
                if (log.isTraceEnabled) log.trace(
                    "About to close the idle connection from {} due to being idle for {} millis",
                    connectionId, (currentTimeNanos - expiredConnection.value) / 1000 / 1000
                )
                channel.state(ChannelState.EXPIRED)
                close(channel, CloseMode.GRACEFUL)
            }
        }
    }

    /**
     * Clears completed receives. This is used by SocketServer to remove references to
     * receive buffers after processing completed receives, without waiting for the next
     * poll().
     */
    fun clearCompletedReceives() {
        completedReceives.clear()
    }

    /**
     * Clears completed sends. This is used by SocketServer to remove references to
     * send buffers after processing completed sends, without waiting for the next
     * poll().
     */
    fun clearCompletedSends() {
        completedSends.clear()
    }

    /**
     * Clears all the results from the previous poll. This is invoked by Selector at the start of
     * a poll() when all the results from the previous poll are expected to have been handled.
     *
     *
     * SocketServer uses [clearCompletedSends] and [clearCompletedReceives] to
     * clear `completedSends` and `completedReceives` as soon as they are processed to avoid
     * holding onto large request/response buffers from multiple connections longer than necessary.
     * Clients rely on Selector invoking [clear] at the start of each poll() since memory usage
     * is less critical and clearing once-per-poll provides the flexibility to process these results in
     * any order before the next poll.
     */
    private fun clear() {
        completedSends.clear()
        completedReceives.clear()
        connected.clear()
        disconnected.clear()

        // Remove closed channels after all their buffered receives have been processed or if a send was requested
        val it = closingChannels.entries.iterator()
        while (it.hasNext()) {
            val channel = it.next().value
            val sendFailed = failedSends.remove(channel.id())
            var hasPending = false
            if (!sendFailed) hasPending = maybeReadFromClosingChannel(channel)
            if (!hasPending) {
                doClose(channel, true)
                it.remove()
            }
        }
        for (channel in failedSends) disconnected[channel] = ChannelState.FAILED_SEND
        failedSends.clear()
        isMadeReadProgressLastPoll = false
    }

    /**
     * Check for data, waiting up to the given timeout.
     *
     * @param timeoutMs Length of time to wait, in milliseconds, which must be non-negative
     * @return The number of keys ready
     */
    @Throws(IOException::class)
    private fun select(timeoutMs: Long): Int {
        require(timeoutMs >= 0L) { "timeout should be >= 0" }
        return if (timeoutMs == 0L) nioSelector!!.selectNow() else nioSelector!!.select(timeoutMs)
    }

    /**
     * Close the connection identified by the given id
     */
    override fun close(id: String) {
        val channel = channels[id]
        if (channel != null) {
            // There is no disconnect notification for local close, but updating
            // channel state here anyway to avoid confusion.
            channel.state(ChannelState.LOCAL_CLOSE)
            close(channel, CloseMode.DISCARD_NO_NOTIFY)
        } else {
            val closingChannel = closingChannels.remove(id)
            // Close any closing channel, leave the channel in the state in which closing was triggered
            if (closingChannel != null) doClose(closingChannel, false)
        }
    }

    private fun maybeDelayCloseOnAuthenticationFailure(channel: KafkaChannel) {
        val delayedClose = DelayedAuthenticationFailureClose(channel, failedAuthenticationDelayMs)
        if (delayedClosingChannels != null) delayedClosingChannels[channel.id()] =
            delayedClose else delayedClose.closeNow()
    }

    private fun handleCloseOnAuthenticationFailure(channel: KafkaChannel) {
        try {
            channel.completeCloseOnAuthenticationFailure()
        } catch (e: Exception) {
            log.error("Exception handling close on authentication failure node {}", channel.id(), e)
        } finally {
            close(channel, CloseMode.GRACEFUL)
        }
    }

    /**
     * Begin closing this connection.
     * If 'closeMode' is `CloseMode.GRACEFUL`, the channel is disconnected here, but outstanding receives
     * are processed. The channel is closed when there are no outstanding receives or if a send is
     * requested. For other values of `closeMode`, outstanding receives are discarded and the channel
     * is closed immediately.
     *
     * The channel will be added to disconnect list when it is actually closed if `closeMode.notifyDisconnect`
     * is true.
     */
    private fun close(channel: KafkaChannel, closeMode: CloseMode) {
        channel.disconnect()

        // Ensure that `connected` does not have closed channels. This could happen if `prepare` throws an exception
        // in the `poll` invocation when `finishConnect` succeeds
        connected.remove(channel.id())

        // Keep track of closed channels with pending receives so that all received records
        // may be processed. For example, when producer with acks=0 sends some records and
        // closes its connections, a single poll() in the broker may receive records and
        // handle close(). When the remote end closes its connection, the channel is retained until
        // a send fails or all outstanding receives are processed. Mute state of disconnected channels
        // are tracked to ensure that requests are processed one-by-one by the broker to preserve ordering.
        if (closeMode == CloseMode.GRACEFUL && maybeReadFromClosingChannel(channel)) {
            closingChannels[channel.id()] = channel
            log.debug("Tracking closing connection {} to process outstanding requests", channel.id())
        } else {
            doClose(channel, closeMode.notifyDisconnect)
        }
        channels.remove(channel.id())
        delayedClosingChannels?.remove(channel.id())
        idleExpiryManager?.remove(channel.id())
    }

    private fun doClose(channel: KafkaChannel, notifyDisconnect: Boolean) {
        val key = channel.selectionKey()
        try {
            immediatelyConnectedKeys.remove(key)
            keysWithBufferedRead.remove(key)
            channel.close()
        } catch (e: IOException) {
            log.error("Exception closing connection to node {}:", channel.id(), e)
        } finally {
            key.cancel()
            key.attach(null)
        }
        sensors.connectionClosed.record()
        explicitlyMutedChannels.remove(channel)
        if (notifyDisconnect) disconnected[channel.id()] = channel.state()
    }

    /**
     * check if channel is ready
     */
    override fun isChannelReady(id: String): Boolean {
        val channel = channels[id]
        return channel != null && channel.ready()
    }

    private fun openOrClosingChannelOrFail(id: String): KafkaChannel {
        var channel = channels[id]
        if (channel == null) channel = closingChannels[id]

        checkNotNull(channel) {
            "Attempt to retrieve channel for which there is no connection. Connection id $id " +
                    "existing connections ${channels.keys}"
        }

        return channel
    }

    /**
     * Return the selector channels.
     */
    fun channels(): MutableList<KafkaChannel?> {
        return ArrayList(channels.values)
    }

    /**
     * Return the channel associated with this connection or `null` if there is no channel
     * associated with the connection.
     */
    fun channel(id: String?): KafkaChannel? {
        return channels[id]
    }

    /**
     * Return the channel with the specified id if it was disconnected, but not yet closed since
     * there are outstanding messages to be processed.
     */
    fun closingChannel(id: String?): KafkaChannel? {
        return closingChannels[id]
    }

    /**
     * Returns the lowest priority channel chosen using the following sequence:
     * 1) If one or more channels are in closing state, return any one of them
     * 2) If idle expiry manager is enabled, return the least recently updated channel
     * 3) Otherwise return any of the channels
     *
     * This method is used to close a channel to accommodate a new channel on the inter-broker
     * listener when broker-wide `max.connections` limit is enabled.
     */
    fun lowestPriorityChannel(): KafkaChannel? {
        var channel: KafkaChannel? = null
        if (closingChannels.isNotEmpty()) {
            channel = closingChannels.values.iterator().next()
        } else if (idleExpiryManager != null && idleExpiryManager.lruConnections.isNotEmpty()) {
            val channelId = idleExpiryManager.lruConnections.keys.iterator().next()
            channel = channel(channelId)
        } else if (channels.isNotEmpty()) {
            channel = channels.values.iterator().next()
        }
        return channel
    }

    /**
     * Get the channel associated with selectionKey
     */
    private fun channel(key: SelectionKey?): KafkaChannel {
        return key!!.attachment() as KafkaChannel
    }

    /**
     * Check if given channel has a completed receive
     */
    private fun hasCompletedReceive(channel: KafkaChannel): Boolean {
        return completedReceives.containsKey(channel.id())
    }

    /**
     * adds a receive to completed receives
     */
    private fun addToCompletedReceives(
        channel: KafkaChannel,
        networkReceive: NetworkReceive,
        currentTimeMs: Long,
    ) {
        check(!hasCompletedReceive(channel)) {
            "Attempting to add second completed receive to channel " + channel.id()
        }
        completedReceives[channel.id()] = networkReceive
        sensors.recordCompletedReceive(channel.id(), networkReceive.size().toLong(), currentTimeMs)
    }

    // only for testing
    fun keys(): MutableSet<SelectionKey> {
        return HashSet(nioSelector!!.keys())
    }

    internal inner class SelectorChannelMetadataRegistry : ChannelMetadataRegistry {
        private var cipherInformation: CipherInformation? = null
        private var clientInformation: ClientInformation? = null
        override fun registerCipherInformation(cipherInformation: CipherInformation?) {
            if (this.cipherInformation != null) {
                if (this.cipherInformation == cipherInformation) return
                sensors.connectionsByCipher.decrement(this.cipherInformation)
            }
            this.cipherInformation = cipherInformation
            sensors.connectionsByCipher.increment(cipherInformation)
        }

        override fun cipherInformation(): CipherInformation? {
            return cipherInformation
        }

        override fun registerClientInformation(clientInformation: ClientInformation?) {
            if (this.clientInformation != null) {
                if (this.clientInformation == clientInformation) return
                sensors.connectionsByClient.decrement(this.clientInformation)
            }
            this.clientInformation = clientInformation
            sensors.connectionsByClient.increment(clientInformation)
        }

        override fun clientInformation(): ClientInformation? {
            return clientInformation
        }

        override fun close() {
            if (cipherInformation != null) {
                sensors.connectionsByCipher.decrement(cipherInformation)
                cipherInformation = null
            }
            if (clientInformation != null) {
                sensors.connectionsByClient.decrement(clientInformation)
                clientInformation = null
            }
        }
    }

    inner class SelectorMetrics(
        private val metrics: Metrics,
        metricGrpPrefix: String,
        private val metricTags: MutableMap<String, String>,
        private val metricsPerConnection: Boolean
    ) : AutoCloseable {

        private val metricGrpName: String

        private val perConnectionMetricGrpName: String

        val connectionClosed: Sensor

        val connectionCreated: Sensor

        val successfulAuthentication: Sensor

        val successfulReauthentication: Sensor

        val successfulAuthenticationNoReauth: Sensor

        val reauthenticationLatency: Sensor

        val failedAuthentication: Sensor

        val failedReauthentication: Sensor

        val bytesTransferred: Sensor

        val bytesSent: Sensor

        val requestsSent: Sensor

        val bytesReceived: Sensor

        val responsesReceived: Sensor

        val selectTime: Sensor

        val ioTime: Sensor

        val connectionsByCipher: IntGaugeSuite<CipherInformation?>

        val connectionsByClient: IntGaugeSuite<ClientInformation?>

        /**
         * Names of metrics that are not registered through sensors
         */
        private val topLevelMetricNames: MutableList<MetricName> = ArrayList()

        private val sensors: MutableList<Sensor> = ArrayList()

        init {
            metricGrpName = "$metricGrpPrefix-metrics"
            perConnectionMetricGrpName = "$metricGrpPrefix-node-metrics"
            val tagsSuffix = StringBuilder()
            for ((key, value) in metricTags) {
                tagsSuffix.append(key)
                tagsSuffix.append("-")
                tagsSuffix.append(value)
            }
            connectionClosed = sensor("connections-closed:$tagsSuffix")
            connectionClosed.add(
                createMeter(
                    metrics = metrics,
                    groupName = metricGrpName,
                    metricTags = metricTags,
                    baseName = "connection-close",
                    descriptiveName = "connections closed",
                )
            )
            connectionCreated = sensor("connections-created:$tagsSuffix")
            connectionCreated.add(
                createMeter(
                    metrics = metrics,
                    groupName = metricGrpName,
                    metricTags = metricTags,
                    baseName = "connection-creation",
                    descriptiveName = "new connections established",
                )
            )
            successfulAuthentication = sensor("successful-authentication:$tagsSuffix")
            successfulAuthentication.add(
                createMeter(
                    metrics = metrics,
                    groupName = metricGrpName,
                    metricTags = metricTags,
                    baseName = "successful-authentication",
                    descriptiveName = "connections with successful authentication",
                )
            )
            successfulReauthentication = sensor("successful-reauthentication:$tagsSuffix")
            successfulReauthentication.add(
                createMeter(
                    metrics = metrics,
                    groupName = metricGrpName,
                    metricTags = metricTags,
                    baseName = "successful-reauthentication",
                    descriptiveName = "successful re-authentication of connections",
                )
            )
            successfulAuthenticationNoReauth =
                sensor("successful-authentication-no-reauth:$tagsSuffix")

            val successfulAuthenticationNoReauthMetricName = metrics.metricName(
                "successful-authentication-no-reauth-total",
                metricGrpName,
                "The total number of connections with successful authentication where " +
                        "the client does not support re-authentication",
                metricTags,
            )
            successfulAuthenticationNoReauth.add(successfulAuthenticationNoReauthMetricName, CumulativeSum())
            failedAuthentication = sensor("failed-authentication:$tagsSuffix")
            failedAuthentication.add(
                createMeter(
                    metrics = metrics,
                    groupName = metricGrpName,
                    metricTags = metricTags,
                    baseName = "failed-authentication",
                    descriptiveName = "connections with failed authentication",
                )
            )
            failedReauthentication = sensor("failed-reauthentication:$tagsSuffix")
            failedReauthentication.add(
                createMeter(
                    metrics = metrics,
                    groupName = metricGrpName,
                    metricTags = metricTags,
                    baseName = "failed-reauthentication",
                    descriptiveName = "failed re-authentication of connections",
                )
            )
            reauthenticationLatency = sensor("reauthentication-latency:$tagsSuffix")
            val reauthenticationLatencyMaxMetricName = metrics.metricName(
                "reauthentication-latency-max",
                metricGrpName,
                "The max latency observed due to re-authentication",
                metricTags
            )
            reauthenticationLatency.add(reauthenticationLatencyMaxMetricName, Max())
            val reauthenticationLatencyAvgMetricName = metrics.metricName(
                "reauthentication-latency-avg",
                metricGrpName,
                "The average latency observed due to re-authentication",
                metricTags
            )
            reauthenticationLatency.add(reauthenticationLatencyAvgMetricName, Avg())
            bytesTransferred = sensor("bytes-sent-received:$tagsSuffix")
            bytesTransferred.add(
                createMeter(
                    metrics = metrics,
                    groupName = metricGrpName,
                    metricTags = metricTags,
                    stat = WindowedCount(),
                    baseName = "network-io",
                    descriptiveName = "network operations (reads or writes) on all connections",
                )
            )
            bytesSent = sensor("bytes-sent:$tagsSuffix", bytesTransferred)
            bytesSent.add(
                createMeter(
                    metrics = metrics,
                    groupName = metricGrpName,
                    metricTags = metricTags,
                    baseName = "outgoing-byte",
                    descriptiveName = "outgoing bytes sent to all servers",
                )
            )
            requestsSent = sensor("requests-sent:$tagsSuffix")
            requestsSent.add(
                createMeter(
                    metrics = metrics,
                    groupName = metricGrpName,
                    metricTags = metricTags,
                    stat = WindowedCount(),
                    baseName = "request",
                    descriptiveName = "requests sent"
                )
            )
            var metricName = metrics.metricName(
                "request-size-avg",
                metricGrpName,
                "The average size of requests sent.",
                metricTags
            )
            requestsSent.add(metricName, Avg())
            metricName = metrics.metricName(
                "request-size-max",
                metricGrpName,
                "The maximum size of any request sent.",
                metricTags
            )
            requestsSent.add(metricName, Max())
            bytesReceived = sensor("bytes-received:$tagsSuffix", bytesTransferred)
            bytesReceived.add(
                createMeter(
                    metrics = metrics,
                    groupName = metricGrpName,
                    metricTags = metricTags,
                    baseName = "incoming-byte",
                    descriptiveName = "bytes read off all sockets",
                )
            )
            responsesReceived = sensor("responses-received:$tagsSuffix")
            responsesReceived.add(
                createMeter(
                    metrics = metrics,
                    groupName = metricGrpName,
                    metricTags = metricTags,
                    stat = WindowedCount(),
                    baseName = "response",
                    descriptiveName = "responses received",
                )
            )
            selectTime = sensor("select-time:$tagsSuffix")
            selectTime.add(
                createMeter(
                    metrics = metrics,
                    groupName = metricGrpName,
                    metricTags = metricTags,
                    stat = WindowedCount(),
                    baseName = "select",
                    descriptiveName = "times the I/O layer checked for new I/O to perform",
                )
            )
            metricName = metrics.metricName(
                name = "io-wait-time-ns-avg",
                group = metricGrpName,
                description = "The average length of time the I/O thread spent waiting for a socket " +
                        "ready for reads or writes in nanoseconds.",
                tags = metricTags,
            )
            selectTime.add(metricName, Avg())
            selectTime.add(createIOThreadRatioMeterLegacy(metrics, metricGrpName, metricTags, "io-wait", "waiting"))
            selectTime.add(createIOThreadRatioMeter(metrics, metricGrpName, metricTags, "io-wait", "waiting"))
            ioTime = sensor("io-time:$tagsSuffix")
            metricName = metrics.metricName(
                "io-time-ns-avg",
                metricGrpName,
                "The average length of time for I/O per select call in nanoseconds.",
                metricTags
            )
            ioTime.add(metricName, Avg())
            ioTime.add(createIOThreadRatioMeterLegacy(metrics, metricGrpName, metricTags, "io", "doing I/O"))
            ioTime.add(createIOThreadRatioMeter(metrics, metricGrpName, metricTags, "io", "doing I/O"))
            connectionsByCipher = IntGaugeSuite(
                log = log,
                suiteName = "sslCiphers",
                metrics = metrics,
                metricNameCalculator = { cipherInformation ->
                    metrics.metricName(
                        name = "connections",
                        group = metricGrpName,
                        description = "The number of connections with this SSL cipher and protocol.",
                        tags = mapOf(
                            "cipher" to cipherInformation!!.cipher(),
                            "protocol" to cipherInformation.protocol(),
                        ) + metricTags
                    )
                },
                maxEntries = 100,
            )
            connectionsByClient = IntGaugeSuite(
                log = log,
                suiteName = "clients",
                metrics = metrics,
                metricNameCalculator = { clientInformation ->
                    metrics.metricName(
                        name = "connections",
                        group = metricGrpName,
                        description = "The number of connections with this client and version.",
                        tags = mapOf(
                            "clientSoftwareName" to clientInformation!!.softwareName(),
                            "clientSoftwareVersion" to clientInformation.softwareVersion(),
                        ) + metricTags,
                    )
                },
                maxEntries = 100,
            )
            metricName = metrics.metricName(
                "connection-count", metricGrpName, "The current number of active connections.",
                metricTags
            )
            topLevelMetricNames.add(metricName)
            metrics.addMetric(
                metricName = metricName,
                metricValueProvider = Measurable { _, _ -> channels.size.toDouble() },
            )
        }

        private fun createMeter(
            metrics: Metrics,
            groupName: String,
            metricTags: MutableMap<String, String>,
            stat: SampledStat?,
            baseName: String,
            descriptiveName: String,
        ): Meter {
            val rateMetricName = metrics.metricName(
                "$baseName-rate",
                groupName,
                String.format("The number of %s per second", descriptiveName),
                metricTags
            )
            val totalMetricName = metrics.metricName(
                "$baseName-total",
                groupName,
                String.format("The total number of %s", descriptiveName),
                metricTags
            )
            return stat?.let {
                Meter(
                    rateMetricName = rateMetricName,
                    totalMetricName = totalMetricName,
                    rateStat = it,
                )
            } ?: Meter(
                rateMetricName = rateMetricName,
                totalMetricName = totalMetricName,
            )
        }

        private fun createMeter(
            metrics: Metrics,
            groupName: String,
            metricTags: MutableMap<String, String>,
            baseName: String,
            descriptiveName: String,
        ): Meter {
            return createMeter(metrics, groupName, metricTags, null, baseName, descriptiveName)
        }

        /**
         * This method generates `time-total` metrics but has a couple of deficiencies: no `-ns`
         * suffix and no dash between basename and `time-toal` suffix.
         */
        @Deprecated(
            message = "use {{@link #createIOThreadRatioMeter(Metrics, String, Map, String, " +
                    "String)}} for new metrics instead",
        )
        private fun createIOThreadRatioMeterLegacy(
            metrics: Metrics,
            groupName: String,
            metricTags: MutableMap<String, String>,
            baseName: String,
            action: String
        ): Meter {
            val rateMetricName = metrics.metricName(
                "$baseName-ratio",
                groupName,
                String.format("*Deprecated* The fraction of time the I/O thread spent %s", action),
                metricTags
            )
            val totalMetricName = metrics.metricName(
                baseName + "time-total",
                groupName,
                String.format("*Deprecated* The total time the I/O thread spent %s", action),
                metricTags
            )
            return Meter(
                rateMetricName = rateMetricName,
                totalMetricName = totalMetricName,
                unit = TimeUnit.NANOSECONDS,
            )
        }

        private fun createIOThreadRatioMeter(
            metrics: Metrics,
            groupName: String,
            metricTags: MutableMap<String, String>,
            baseName: String, action: String
        ): Meter {
            val rateMetricName = metrics.metricName(
                "$baseName-ratio",
                groupName,
                String.format("The fraction of time the I/O thread spent %s", action),
                metricTags
            )
            val totalMetricName = metrics.metricName(
                "$baseName-time-ns-total",
                groupName,
                String.format("The total time the I/O thread spent %s", action),
                metricTags
            )
            return Meter(
                rateMetricName = rateMetricName,
                totalMetricName = totalMetricName,
                unit = TimeUnit.NANOSECONDS,
            )
        }

        private fun sensor(name: String, vararg parents: Sensor): Sensor {
            val sensor = metrics.sensor(
                name = name,
                parents = parents
            )

            sensors.add(sensor)
            return sensor
        }

        fun maybeRegisterConnectionMetrics(connectionId: String) {
            if (connectionId.isNotEmpty() && metricsPerConnection) {
                // if one sensor of the metrics has been registered for the connection,
                // then all other sensors should have been registered; and vice versa
                val nodeRequestName = "node-$connectionId.requests-sent"
                var nodeRequest = metrics.getSensor(nodeRequestName)
                if (nodeRequest == null) {
                    val tags: MutableMap<String, String> = LinkedHashMap(metricTags)
                    tags["node-id"] = "node-$connectionId"
                    nodeRequest = sensor(nodeRequestName)
                    nodeRequest.add(
                        createMeter(
                            metrics,
                            perConnectionMetricGrpName,
                            tags,
                            WindowedCount(),
                            "request",
                            "requests sent"
                        )
                    )
                    var metricName = metrics.metricName(
                        "request-size-avg",
                        perConnectionMetricGrpName,
                        "The average size of requests sent.",
                        tags
                    )
                    nodeRequest.add(metricName, Avg())
                    metricName = metrics.metricName(
                        "request-size-max",
                        perConnectionMetricGrpName,
                        "The maximum size of any request sent.",
                        tags
                    )
                    nodeRequest.add(metricName, Max())
                    val bytesSentName = "node-$connectionId.bytes-sent"
                    val bytesSent = sensor(bytesSentName)
                    bytesSent.add(
                        createMeter(
                            metrics,
                            perConnectionMetricGrpName,
                            tags,
                            "outgoing-byte",
                            "outgoing bytes"
                        )
                    )
                    val nodeResponseName = "node-$connectionId.responses-received"
                    val nodeResponse = sensor(nodeResponseName)
                    nodeResponse.add(
                        createMeter(
                            metrics,
                            perConnectionMetricGrpName,
                            tags,
                            WindowedCount(),
                            "response",
                            "responses received"
                        )
                    )
                    val bytesReceivedName = "node-$connectionId.bytes-received"
                    val bytesReceive = sensor(bytesReceivedName)
                    bytesReceive.add(
                        createMeter(
                            metrics,
                            perConnectionMetricGrpName,
                            tags,
                            "incoming-byte",
                            "incoming bytes"
                        )
                    )
                    val nodeTimeName = "node-$connectionId.latency"
                    val nodeRequestTime = sensor(nodeTimeName)

                    metricName = metrics.metricName(
                        name = "request-latency-avg",
                        group = perConnectionMetricGrpName,
                        tags = tags
                    )
                    nodeRequestTime.add(metricName, Avg())

                    metricName = metrics.metricName(
                        name = "request-latency-max",
                        group = perConnectionMetricGrpName,
                        tags = tags,
                    )
                    nodeRequestTime.add(metricName, Max())
                }
            }
        }

        fun recordBytesSent(connectionId: String, bytes: Long, currentTimeMs: Long) {
            bytesSent.record(bytes.toDouble(), currentTimeMs, false)
            if (connectionId.isNotEmpty()) {
                val bytesSentName = "node-$connectionId.bytes-sent"
                val bytesSent = metrics.getSensor(bytesSentName)
                bytesSent?.record(bytes.toDouble(), currentTimeMs)
            }
        }

        fun recordCompletedSend(connectionId: String, totalBytes: Long, currentTimeMs: Long) {
            requestsSent.record(totalBytes.toDouble(), currentTimeMs, false)
            if (connectionId.isNotEmpty()) {
                val nodeRequestName = "node-$connectionId.requests-sent"
                val nodeRequest = metrics.getSensor(nodeRequestName)
                nodeRequest?.record(totalBytes.toDouble(), currentTimeMs)
            }
        }

        fun recordBytesReceived(connectionId: String, bytes: Long, currentTimeMs: Long) {
            bytesReceived.record(bytes.toDouble(), currentTimeMs, false)
            if (connectionId.isNotEmpty()) {
                val bytesReceivedName = "node-$connectionId.bytes-received"
                val bytesReceived = metrics.getSensor(bytesReceivedName)
                bytesReceived?.record(bytes.toDouble(), currentTimeMs)
            }
        }

        fun recordCompletedReceive(connectionId: String, totalBytes: Long, currentTimeMs: Long) {
            responsesReceived.record(totalBytes.toDouble(), currentTimeMs, false)
            if (connectionId.isNotEmpty()) {
                val nodeRequestName = "node-$connectionId.responses-received"
                val nodeRequest = metrics.getSensor(nodeRequestName)
                nodeRequest?.record(totalBytes.toDouble(), currentTimeMs)
            }
        }

        override fun close() {
            for (metricName in topLevelMetricNames) metrics.removeMetric(metricName)
            for (sensor in sensors) metrics.removeSensor(sensor.name())
            connectionsByCipher.close()
            connectionsByClient.close()
        }
    }

    /**
     * Encapsulate a channel that must be closed after a specific delay has elapsed due to authentication failure.
     *
     * @param channel The channel whose close is being delayed
     * @param delayMs The amount of time by which the operation should be delayed
     */
    private inner class DelayedAuthenticationFailureClose(private val channel: KafkaChannel, delayMs: Int) {
        private val endTimeNanos: Long = time.nanoseconds() + delayMs * 1000L * 1000L
        private var closed = false

        /**
         * Try to close this channel if the delay has expired.
         * @param currentTimeNanos The current time
         * @return True if the delay has expired and the channel was closed; false otherwise
         */
        fun tryClose(currentTimeNanos: Long): Boolean {
            if (endTimeNanos <= currentTimeNanos) closeNow()
            return closed
        }

        /**
         * Close the channel now, regardless of whether the delay has expired or not.
         */
        fun closeNow() {
            check(!closed) { "Attempt to close a channel that has already been closed" }
            handleCloseOnAuthenticationFailure(channel)
            closed = true
        }
    }

    // helper class for tracking least recently used connections to enable idle connection closing
    private class IdleExpiryManager(time: Time, connectionsMaxIdleMs: Long) {
        val lruConnections: MutableMap<String, Long>
        private val connectionsMaxIdleNanos: Long
        private var nextIdleCloseCheckTime: Long

        init {
            connectionsMaxIdleNanos = connectionsMaxIdleMs * 1000 * 1000
            // initial capacity and load factor are default, we set them explicitly because we want
            // to set accessOrder = true
            lruConnections = LinkedHashMap(16, .75f, true)
            nextIdleCloseCheckTime = time.nanoseconds() + connectionsMaxIdleNanos
        }

        fun update(connectionId: String, currentTimeNanos: Long) {
            lruConnections[connectionId] = currentTimeNanos
        }

        fun pollExpiredConnection(currentTimeNanos: Long): MutableMap.MutableEntry<String, Long>? {
            if (currentTimeNanos <= nextIdleCloseCheckTime) return null
            if (lruConnections.isEmpty()) {
                nextIdleCloseCheckTime = currentTimeNanos + connectionsMaxIdleNanos
                return null
            }
            val oldestConnectionEntry = lruConnections.entries.iterator().next()
            val connectionLastActiveTime = oldestConnectionEntry.value
            nextIdleCloseCheckTime = connectionLastActiveTime + connectionsMaxIdleNanos
            return if (currentTimeNanos > nextIdleCloseCheckTime) oldestConnectionEntry else null
        }

        fun remove(connectionId: String) {
            lruConnections.remove(connectionId)
        }
    }

    // package-private for testing
    fun delayedClosingChannels(): MutableMap<*, *>? {
        return delayedClosingChannels
    }
    private enum class CloseMode(// discard any outstanding receives, no disconnect notification
        var notifyDisconnect: Boolean
    ) {
        GRACEFUL(true),

        // process outstanding buffered receives, notify disconnect
        NOTIFY_ONLY(true),

        // discard any outstanding receives, notify disconnect
        DISCARD_NO_NOTIFY(false)

    }

    companion object {
        const val NO_IDLE_TIMEOUT_MS: Long = -1
        const val NO_FAILED_AUTHENTICATION_DELAY = 0
    }
}
