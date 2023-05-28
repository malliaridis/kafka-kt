package org.apache.kafka.clients

import java.io.IOException
import java.net.InetSocketAddress
import java.nio.BufferUnderflowException
import java.nio.ByteBuffer
import java.util.*
import java.util.concurrent.atomic.AtomicReference
import java.util.stream.Collectors
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.Node
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.AuthenticationException
import org.apache.kafka.common.errors.DisconnectException
import org.apache.kafka.common.errors.UnsupportedVersionException
import org.apache.kafka.common.metrics.Sensor
import org.apache.kafka.common.network.ChannelState
import org.apache.kafka.common.network.NetworkReceive
import org.apache.kafka.common.network.NetworkSend
import org.apache.kafka.common.network.Selectable
import org.apache.kafka.common.network.Send
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.protocol.types.SchemaException
import org.apache.kafka.common.requests.AbstractRequest
import org.apache.kafka.common.requests.AbstractResponse
import org.apache.kafka.common.requests.ApiVersionsRequest
import org.apache.kafka.common.requests.ApiVersionsResponse
import org.apache.kafka.common.requests.CorrelationIdMismatchException
import org.apache.kafka.common.requests.MetadataRequest
import org.apache.kafka.common.requests.MetadataResponse
import org.apache.kafka.common.requests.MetadataResponse.PartitionMetadata
import org.apache.kafka.common.requests.RequestHeader
import org.apache.kafka.common.security.authenticator.SaslClientAuthenticator
import org.apache.kafka.common.utils.LogContext
import org.apache.kafka.common.utils.Time
import org.apache.kafka.common.utils.Utils
import org.slf4j.Logger

/**
 * A network client for asynchronous request/response network i/o. This is an internal class used to implement the
 * user-facing producer and consumer clients.
 *
 *
 * This class is not thread-safe!
 */
class NetworkClient(
    metadataUpdater: MetadataUpdater?,
    metadata: Metadata?,
    selector: Selectable,
    clientId: String?,
    maxInFlightRequestsPerConnection: Int,
    reconnectBackoffMs: Long,
    reconnectBackoffMax: Long,
    socketSendBuffer: Int,
    socketReceiveBuffer: Int,
    defaultRequestTimeoutMs: Int,
    connectionSetupTimeoutMs: Long,
    connectionSetupTimeoutMaxMs: Long,
    time: Time,
    discoverBrokerVersions: Boolean,
    apiVersions: ApiVersions,
    throttleTimeSensor: Sensor?,
    logContext: LogContext,
    hostResolver: HostResolver?
) : KafkaClient {

    private enum class State {
        ACTIVE,
        CLOSING,
        CLOSED
    }

    private val log: Logger

    /* the selector used to perform network i/o */
    private val selector: Selectable
    private val randOffset: Random
    private val metadataUpdater: MetadataUpdater

    /* the state of each node's connection */
    private val connectionStates: ClusterConnectionStates

    /* the set of requests currently being sent or awaiting a response */
    private val inFlightRequests: InFlightRequests

    /* the socket send buffer size in bytes */
    private val socketSendBuffer: Int

    /* the socket receive size buffer in bytes */
    private val socketReceiveBuffer: Int

    /* the client id used to identify this client in requests to the server */
    private val clientId: String?

    /* the current correlation id to use when sending requests to servers */
    private var correlation: Int

    /* default timeout for individual requests to await acknowledgement from servers */
    private val defaultRequestTimeoutMs: Int

    /* time in ms to wait before retrying to create connection to a server */
    private val reconnectBackoffMs: Long
    private val time: Time

    /**
     * True if we should send an ApiVersionRequest when first connecting to a broker.
     */
    private val discoverBrokerVersions: Boolean
    private val apiVersions: ApiVersions
    private val nodesNeedingApiVersionsFetch: MutableMap<String, ApiVersionsRequest.Builder> = HashMap()
    private val abortedSends: MutableList<ClientResponse> = LinkedList()
    private val throttleTimeSensor: Sensor?
    private val state: AtomicReference<State>

    constructor(
        selector: Selectable,
        metadata: Metadata?,
        clientId: String?,
        maxInFlightRequestsPerConnection: Int,
        reconnectBackoffMs: Long,
        reconnectBackoffMax: Long,
        socketSendBuffer: Int,
        socketReceiveBuffer: Int,
        defaultRequestTimeoutMs: Int,
        connectionSetupTimeoutMs: Long,
        connectionSetupTimeoutMaxMs: Long,
        time: Time,
        discoverBrokerVersions: Boolean,
        apiVersions: ApiVersions,
        logContext: LogContext
    ) : this(
        selector,
        metadata,
        clientId,
        maxInFlightRequestsPerConnection,
        reconnectBackoffMs,
        reconnectBackoffMax,
        socketSendBuffer,
        socketReceiveBuffer,
        defaultRequestTimeoutMs,
        connectionSetupTimeoutMs,
        connectionSetupTimeoutMaxMs,
        time,
        discoverBrokerVersions,
        apiVersions,
        null,
        logContext
    )

    constructor(
        selector: Selectable,
        metadata: Metadata?,
        clientId: String?,
        maxInFlightRequestsPerConnection: Int,
        reconnectBackoffMs: Long,
        reconnectBackoffMax: Long,
        socketSendBuffer: Int,
        socketReceiveBuffer: Int,
        defaultRequestTimeoutMs: Int,
        connectionSetupTimeoutMs: Long,
        connectionSetupTimeoutMaxMs: Long,
        time: Time,
        discoverBrokerVersions: Boolean,
        apiVersions: ApiVersions,
        throttleTimeSensor: Sensor?,
        logContext: LogContext
    ) : this(
        null,
        metadata,
        selector,
        clientId,
        maxInFlightRequestsPerConnection,
        reconnectBackoffMs,
        reconnectBackoffMax,
        socketSendBuffer,
        socketReceiveBuffer,
        defaultRequestTimeoutMs,
        connectionSetupTimeoutMs,
        connectionSetupTimeoutMaxMs,
        time,
        discoverBrokerVersions,
        apiVersions,
        throttleTimeSensor,
        logContext,
        DefaultHostResolver()
    )

    constructor(
        selector: Selectable,
        metadataUpdater: MetadataUpdater?,
        clientId: String?,
        maxInFlightRequestsPerConnection: Int,
        reconnectBackoffMs: Long,
        reconnectBackoffMax: Long,
        socketSendBuffer: Int,
        socketReceiveBuffer: Int,
        defaultRequestTimeoutMs: Int,
        connectionSetupTimeoutMs: Long,
        connectionSetupTimeoutMaxMs: Long,
        time: Time,
        discoverBrokerVersions: Boolean,
        apiVersions: ApiVersions,
        logContext: LogContext
    ) : this(
        metadataUpdater,
        null,
        selector,
        clientId,
        maxInFlightRequestsPerConnection,
        reconnectBackoffMs,
        reconnectBackoffMax,
        socketSendBuffer,
        socketReceiveBuffer,
        defaultRequestTimeoutMs,
        connectionSetupTimeoutMs,
        connectionSetupTimeoutMaxMs,
        time,
        discoverBrokerVersions,
        apiVersions,
        null,
        logContext,
        DefaultHostResolver()
    )

    init {
        /* It would be better if we could pass `DefaultMetadataUpdater` from the public constructor, but it's not
         * possible because `DefaultMetadataUpdater` is an inner class and it can only be instantiated after the
         * super constructor is invoked.
         */
        if (metadataUpdater == null) {
            require(metadata != null) { "`metadata` must not be null" }
            this.metadataUpdater = DefaultMetadataUpdater(metadata)
        } else {
            this.metadataUpdater = metadataUpdater
        }
        this.selector = selector
        this.clientId = clientId
        inFlightRequests = InFlightRequests(maxInFlightRequestsPerConnection)
        connectionStates = ClusterConnectionStates(
            reconnectBackoffMs, reconnectBackoffMax,
            connectionSetupTimeoutMs, connectionSetupTimeoutMaxMs, logContext, hostResolver
        )
        this.socketSendBuffer = socketSendBuffer
        this.socketReceiveBuffer = socketReceiveBuffer
        correlation = 0
        randOffset = Random()
        this.defaultRequestTimeoutMs = defaultRequestTimeoutMs
        this.reconnectBackoffMs = reconnectBackoffMs
        this.time = time
        this.discoverBrokerVersions = discoverBrokerVersions
        this.apiVersions = apiVersions
        this.throttleTimeSensor = throttleTimeSensor
        log = logContext.logger(NetworkClient::class.java)
        state = AtomicReference(State.ACTIVE)
    }

    /**
     * Begin connecting to the given node, return true if we are already connected and ready to send to that node.
     *
     * @param node The node to check
     * @param now The current timestamp
     * @return True if we are ready to send to the given node
     */
    override fun ready(node: Node, now: Long): Boolean {
        require(!node.isEmpty) { "Cannot connect to empty node $node" }
        if (isReady(node, now)) return true
        if (connectionStates.canConnect(
                node.idString(),
                now
            )
        ) // if we are interested in sending to a node and we don't have a connection to it, initiate one
            initiateConnect(node, now)
        return false
    }

    // Visible for testing
    fun canConnect(node: Node?, now: Long): Boolean {
        return connectionStates.canConnect(node!!.idString(), now)
    }

    /**
     * Disconnects the connection to a particular node, if there is one.
     * Any pending ClientRequests for this connection will receive disconnections.
     *
     * @param nodeId The id of the node
     */
    override fun disconnect(nodeId: String) {
        if (connectionStates.isDisconnected(nodeId)) {
            log.debug("Client requested disconnect from node {}, which is already disconnected", nodeId)
            return
        }
        log.info("Client requested disconnect from node {}", nodeId)
        selector.close(nodeId)
        val now = time.milliseconds()
        cancelInFlightRequests(nodeId, now, abortedSends)
        connectionStates.disconnected(nodeId, now)
    }

    private fun cancelInFlightRequests(nodeId: String, now: Long, responses: MutableCollection<ClientResponse>?) {
        val inFlightRequests = inFlightRequests.clearAll(nodeId)
        for (request: InFlightRequest in inFlightRequests) {
            if (log.isDebugEnabled) {
                log.debug(
                    "Cancelled in-flight {} request with correlation id {} due to node {} being disconnected " +
                    "(elapsed time since creation: {}ms, elapsed time since send: {}ms, request timeout: {}ms): {}",
                    request.header.apiKey(), request.header.correlationId(), nodeId,
                    request.timeElapsedSinceCreateMs(now), request.timeElapsedSinceSendMs(now),
                    request.requestTimeoutMs, request.request
                )
            } else {
                log.info(
                    "Cancelled in-flight {} request with correlation id {} due to node {} being disconnected " +
                            "(elapsed time since creation: {}ms, elapsed time since send: {}ms, request timeout: {}ms)",
                    request.header.apiKey(), request.header.correlationId(), nodeId,
                    request.timeElapsedSinceCreateMs(now), request.timeElapsedSinceSendMs(now),
                    request.requestTimeoutMs
                )
            }
            if (!request.isInternalRequest) {
                responses?.add(request.disconnected(now, null))
            } else if (request.header.apiKey() == ApiKeys.METADATA) {
                metadataUpdater.handleFailedRequest(now, null)
            }
        }
    }

    /**
     * Closes the connection to a particular node (if there is one).
     * All requests on the connection will be cleared.  ClientRequest callbacks will not be invoked
     * for the cleared requests, nor will they be returned from poll().
     *
     * @param nodeId The id of the node
     */
    override fun close(nodeId: String) {
        log.info("Client requested connection close from node {}", nodeId)
        selector.close(nodeId)
        val now = time.milliseconds()
        cancelInFlightRequests(nodeId, now, null)
        connectionStates.remove(nodeId)
    }

    /**
     * Returns the number of milliseconds to wait, based on the connection state, before attempting to send data. When
     * disconnected, this respects the reconnect backoff time. When connecting or connected, this handles slow/stalled
     * connections.
     *
     * @param node The node to check
     * @param now The current timestamp
     * @return The number of milliseconds to wait.
     */
    override fun connectionDelay(node: Node, now: Long): Long {
        return connectionStates.connectionDelay(node.idString(), now)
    }

    // Return the remaining throttling delay in milliseconds if throttling is in progress. Return 0, otherwise.
    // This is for testing.
    fun throttleDelayMs(node: Node, now: Long): Long {
        return connectionStates.throttleDelayMs(node.idString(), now)
    }

    /**
     * Return the poll delay in milliseconds based on both connection and throttle delay.
     * @param node the connection to check
     * @param now the current time in ms
     */
    override fun pollDelayMs(node: Node, now: Long): Long {
        return connectionStates.pollDelayMs(node.idString(), now)
    }

    /**
     * Check if the connection of the node has failed, based on the connection state. Such connection failure are
     * usually transient and can be resumed in the next [.ready] }
     * call, but there are cases where transient failures needs to be caught and re-acted upon.
     *
     * @param node the node to check
     * @return true iff the connection has failed and the node is disconnected
     */
    override fun connectionFailed(node: Node): Boolean {
        return connectionStates.isDisconnected(node.idString())
    }

    /**
     * Check if authentication to this node has failed, based on the connection state. Authentication failures are
     * propagated without any retries.
     *
     * @param node the node to check
     * @return an AuthenticationException iff authentication has failed, null otherwise
     */
    override fun authenticationException(node: Node): AuthenticationException {
        return connectionStates.authenticationException(node.idString())
    }

    /**
     * Check if the node with the given id is ready to send more requests.
     *
     * @param node The node
     * @param now The current time in ms
     * @return true if the node is ready
     */
    override fun isReady(node: Node, now: Long): Boolean {
        // if we need to update our metadata now declare all requests unready to make metadata requests first
        // priority
        return !metadataUpdater.isUpdateDue(now) && canSendRequest(node.idString(), now)
    }

    /**
     * Are we connected and ready and able to send more requests to the given connection?
     *
     * @param node The node
     * @param now the current timestamp
     */
    private fun canSendRequest(node: String, now: Long): Boolean {
        return connectionStates.isReady(node, now) && selector.isChannelReady(node) &&
                inFlightRequests.canSendMore(node)
    }

    /**
     * Queue up the given request for sending. Requests can only be sent out to ready nodes.
     * @param request The request
     * @param now The current timestamp
     */
    override fun send(request: ClientRequest, now: Long) {
        doSend(request, false, now)
    }

    // package-private for testing
    fun sendInternalMetadataRequest(builder: MetadataRequest.Builder?, nodeConnectionId: String, now: Long) {
        val clientRequest = newClientRequest(nodeConnectionId, builder, now, true)
        doSend(clientRequest, true, now)
    }

    private fun doSend(clientRequest: ClientRequest, isInternalRequest: Boolean, now: Long) {
        ensureActive()
        val nodeId = clientRequest.destination()
        if (!isInternalRequest) {
            // If this request came from outside the NetworkClient, validate
            // that we can send data.  If the request is internal, we trust
            // that internal code has done this validation.  Validation
            // will be slightly different for some internal requests (for
            // example, ApiVersionsRequests can be sent prior to being in
            // READY state.)
            check(canSendRequest(nodeId, now)) { "Attempt to send a request to node $nodeId which is not ready." }
        }
        val builder = clientRequest.requestBuilder()
        try {
            val versionInfo = apiVersions[nodeId]
            val version: Short
            // Note: if versionInfo is null, we have no server version information. This would be
            // the case when sending the initial ApiVersionRequest which fetches the version
            // information itself.  It is also the case when discoverBrokerVersions is set to false.
            if (versionInfo == null) {
                version = builder.latestAllowedVersion()
                if (discoverBrokerVersions && log.isTraceEnabled) log.trace(
                    "No version information found when sending {} with correlation id {} to node {}. " +
                            "Assuming version {}.",
                    clientRequest.apiKey(),
                    clientRequest.correlationId(),
                    nodeId,
                    version
                )
            } else {
                version = versionInfo.latestUsableVersion(
                    clientRequest.apiKey(), builder.oldestAllowedVersion(),
                    builder.latestAllowedVersion()
                )
            }
            // The call to build may also throw UnsupportedVersionException, if there are essential
            // fields that cannot be represented in the chosen version.
            doSend(clientRequest, isInternalRequest, now, builder.build(version))
        } catch (unsupportedVersionException: UnsupportedVersionException) {
            // If the version is not supported, skip sending the request over the wire.
            // Instead, simply add it to the local queue of aborted requests.
            log.debug(
                "Version mismatch when attempting to send {} with correlation id {} to {}", builder,
                clientRequest.correlationId(), clientRequest.destination(), unsupportedVersionException
            )
            val clientResponse = ClientResponse(
                clientRequest.makeHeader(builder.latestAllowedVersion()),
                clientRequest.callback(), clientRequest.destination(), now, now,
                false, unsupportedVersionException, null, null
            )
            if (!isInternalRequest) abortedSends.add(clientResponse)
            else if (clientRequest.apiKey() == ApiKeys.METADATA)
                metadataUpdater.handleFailedRequest(now, unsupportedVersionException)
        }
    }

    private fun doSend(clientRequest: ClientRequest, isInternalRequest: Boolean, now: Long, request: AbstractRequest) {
        val destination = clientRequest.destination()
        val header = clientRequest.makeHeader(request.version())
        if (log.isDebugEnabled) {
            log.debug(
                "Sending {} request with header {} and timeout {} to node {}: {}",
                clientRequest.apiKey(), header, clientRequest.requestTimeoutMs(), destination, request
            )
        }
        val send = request.toSend(header)
        val inFlightRequest = InFlightRequest(
            clientRequest,
            header,
            isInternalRequest,
            request,
            send,
            now
        )
        inFlightRequests.add(inFlightRequest)
        selector.send(NetworkSend(clientRequest.destination(), send))
    }

    /**
     * Do actual reads and writes to sockets.
     *
     * @param timeout The maximum amount of time to wait (in ms) for responses if there are none immediately,
     * must be non-negative. The actual timeout will be the minimum of timeout, request timeout and
     * metadata timeout
     * @param now The current time in milliseconds
     * @return The list of responses received
     */
    override fun poll(timeout: Long, now: Long): List<ClientResponse> {
        ensureActive()
        if (abortedSends.isNotEmpty()) {
            // If there are aborted sends because of unsupported version exceptions or disconnects,
            // handle them immediately without waiting for Selector#poll.
            val responses: MutableList<ClientResponse> = ArrayList()
            handleAbortedSends(responses)
            completeResponses(responses)
            return responses
        }
        val metadataTimeout = metadataUpdater.maybeUpdate(now)
        try {
            selector.poll(Utils.min(timeout, metadataTimeout, defaultRequestTimeoutMs.toLong()))
        } catch (e: IOException) {
            log.error("Unexpected error during I/O", e)
        }

        // process completed actions
        val updatedNow = time.milliseconds()
        val responses: MutableList<ClientResponse> = ArrayList()
        handleCompletedSends(responses, updatedNow)
        handleCompletedReceives(responses, updatedNow)
        handleDisconnections(responses, updatedNow)
        handleConnections()
        handleInitiateApiVersionRequests(updatedNow)
        handleTimedOutConnections(responses, updatedNow)
        handleTimedOutRequests(responses, updatedNow)
        completeResponses(responses)
        return responses
    }

    private fun completeResponses(responses: List<ClientResponse>) {
        for (response: ClientResponse in responses) {
            try {
                response.onComplete()
            } catch (e: Exception) {
                log.error("Uncaught error in request completion:", e)
            }
        }
    }

    /**
     * Get the number of in-flight requests
     */
    override fun inFlightRequestCount(): Int {
        return inFlightRequests.count()
    }

    override fun hasInFlightRequests(): Boolean {
        return !inFlightRequests.isEmpty
    }

    /**
     * Get the number of in-flight requests for a given node
     */
    override fun inFlightRequestCount(nodeId: String): Int {
        return inFlightRequests.count(nodeId)
    }

    override fun hasInFlightRequests(nodeId: String): Boolean {
        return !inFlightRequests.isEmpty(nodeId)
    }

    override fun hasReadyNodes(now: Long): Boolean {
        return connectionStates.hasReadyNodes(now)
    }

    /**
     * Interrupt the client if it is blocked waiting on I/O.
     */
    override fun wakeup() {
        selector.wakeup()
    }

    override fun initiateClose() {
        if (state.compareAndSet(State.ACTIVE, State.CLOSING)) {
            wakeup()
        }
    }

    override fun active(): Boolean {
        return state.get() == State.ACTIVE
    }

    private fun ensureActive() {
        if (!active()) throw DisconnectException("NetworkClient is no longer active, state is $state")
    }

    /**
     * Close the network client
     */
    override fun close() {
        state.compareAndSet(State.ACTIVE, State.CLOSING)
        if (state.compareAndSet(State.CLOSING, State.CLOSED)) {
            selector.close()
            metadataUpdater.close()
        } else {
            log.warn("Attempting to close NetworkClient that has already been closed.")
        }
    }

    /**
     * Choose the node with the fewest outstanding requests which is at least eligible for connection. This method will
     * prefer a node with an existing connection, but will potentially choose a node for which we don't yet have a
     * connection if all existing connections are in use. If no connection exists, this method will prefer a node
     * with least recent connection attempts. This method will never choose a node for which there is no
     * existing connection and from which we have disconnected within the reconnect backoff period, or an active
     * connection which is being throttled.
     *
     * @return The node with the fewest in-flight requests.
     */
    override fun leastLoadedNode(now: Long): Node? {
        val nodes = metadataUpdater.fetchNodes()
        check(nodes.isNotEmpty()) { "There are no nodes in the Kafka cluster" }
        var inflight = Int.MAX_VALUE
        var foundConnecting: Node? = null
        var foundCanConnect: Node? = null
        var foundReady: Node? = null
        val offset = randOffset.nextInt(nodes.size)

        for (i in nodes.indices) {
            val idx = (offset + i) % nodes.size
            val node = nodes[idx]
            if (canSendRequest(node!!.idString(), now)) {
                val currInflight = inFlightRequests.count(node.idString())
                if (currInflight == 0) {
                    // if we find an established connection with no in-flight requests we can stop right away
                    log.trace("Found least loaded node {} connected with no in-flight requests", node)
                    return node
                } else if (currInflight < inflight) {
                    // otherwise if this is the best we have found so far, record that
                    inflight = currInflight
                    foundReady = node
                }
            } else if (connectionStates.isPreparingConnection(node.idString())) {
                foundConnecting = node
            } else if (canConnect(node, now)) {
                if (foundCanConnect == null ||
                    connectionStates.lastConnectAttemptMs(foundCanConnect.idString()) >
                    connectionStates.lastConnectAttemptMs(node.idString())
                ) {
                    foundCanConnect = node
                }
            } else {
                log.trace(
                    "Removing node {} from least loaded node selection since it is neither ready " +
                            "for sending or connecting", node
                )
            }
        }

        // We prefer established connections if possible. Otherwise, we will wait for connections
        // which are being established before connecting to new nodes.
        return if (foundReady != null) {
            log.trace("Found least loaded node {} with {} inflight requests", foundReady, inflight)
            foundReady
        } else if (foundConnecting != null) {
            log.trace("Found least loaded connecting node {}", foundConnecting)
            foundConnecting
        } else if (foundCanConnect != null) {
            log.trace("Found least loaded node {} with no active connection", foundCanConnect)
            foundCanConnect
        } else {
            log.trace("Least loaded node selection failed to find an available node")
            null
        }
    }

    /**
     * Post process disconnection of a node
     *
     * @param responses The list of responses to update
     * @param nodeId ID of the node to be disconnected
     * @param now The current time
     * @param disconnectState The state of the disconnected channel
     */
    private fun processDisconnection(
        responses: MutableList<ClientResponse>,
        nodeId: String,
        now: Long,
        disconnectState: ChannelState
    ) {
        connectionStates.disconnected(nodeId, now)
        apiVersions.remove(nodeId)
        nodesNeedingApiVersionsFetch.remove(nodeId)
        when (disconnectState.state()) {
            ChannelState.State.AUTHENTICATION_FAILED -> {
                val exception = disconnectState.exception()
                connectionStates.authenticationFailed(nodeId, now, exception)
                log.error(
                    "Connection to node {} ({}) failed authentication due to: {}", nodeId,
                    disconnectState.remoteAddress(),
                    exception?.message
                )
            }

            ChannelState.State.AUTHENTICATE -> log.warn(
                "Connection to node {} ({}) terminated during authentication. This may happen " +
                        "due to any of the following reasons: (1) Authentication failed due to invalid " +
                        "credentials with brokers older than 1.0.0, (2) Firewall blocking Kafka TLS " +
                        "traffic (eg it may only allow HTTPS traffic), (3) Transient network issue.",
                nodeId,
                disconnectState.remoteAddress()
            )

            ChannelState.State.NOT_CONNECTED -> log.warn(
                "Connection to node {} ({}) could not be established. Broker may not be available.",
                nodeId,
                disconnectState.remoteAddress()
            )

            else -> {}
        }
        cancelInFlightRequests(nodeId, now, responses)
        metadataUpdater.handleServerDisconnect(now, nodeId, disconnectState.exception())
    }

    /**
     * Iterate over all the inflight requests and expire any requests that have exceeded the configured requestTimeout.
     * The connection to the node associated with the request will be terminated and will be treated as a disconnection.
     *
     * @param responses The list of responses to update
     * @param now The current time
     */
    private fun handleTimedOutRequests(responses: MutableList<ClientResponse>, now: Long) {
        val nodeIds = inFlightRequests.nodesWithTimedOutRequests(now)
        for (nodeId: String in nodeIds) {
            // close connection to the node
            selector.close(nodeId)
            log.info("Disconnecting from node {} due to request timeout.", nodeId)
            processDisconnection(responses, nodeId, now, ChannelState.LOCAL_CLOSE)
        }
    }

    private fun handleAbortedSends(responses: MutableList<ClientResponse>) {
        responses.addAll(abortedSends)
        abortedSends.clear()
    }

    /**
     * Handle socket channel connection timeout. The timeout will hit iff a connection
     * stays at the ConnectionState.CONNECTING state longer than the timeout value,
     * as indicated by ClusterConnectionStates.NodeConnectionState.
     *
     * @param responses The list of responses to update
     * @param now The current time
     */
    private fun handleTimedOutConnections(responses: MutableList<ClientResponse>, now: Long) {
        val nodes = connectionStates.nodesWithConnectionSetupTimeout(now)
        for (nodeId: String in nodes) {
            selector.close(nodeId)
            log.info(
                "Disconnecting from node {} due to socket connection setup timeout. " +
                        "The timeout value is {} ms.",
                nodeId,
                connectionStates.connectionSetupTimeoutMs(nodeId)
            )
            processDisconnection(responses, nodeId, now, ChannelState.LOCAL_CLOSE)
        }
    }

    /**
     * Handle any completed request send. In particular if no response is expected consider the request complete.
     *
     * @param responses The list of responses to update
     * @param now The current time
     */
    private fun handleCompletedSends(responses: MutableList<ClientResponse>, now: Long) {
        // if no response is expected then when the send is completed, return it
        for (send: NetworkSend in selector.completedSends()) {
            val request = inFlightRequests.lastSent(send.destinationId())
            if (!request.expectResponse) {
                inFlightRequests.completeLastSent(send.destinationId())
                responses.add(request.completed(null, now))
            }
        }
    }

    /**
     * If a response from a node includes a non-zero throttle delay and client-side throttling has been enabled for
     * the connection to the node, throttle the connection for the specified delay.
     *
     * @param response the response
     * @param apiVersion the API version of the response
     * @param nodeId the id of the node
     * @param now The current time
     */
    private fun maybeThrottle(response: AbstractResponse, apiVersion: Short, nodeId: String, now: Long) {
        val throttleTimeMs = response.throttleTimeMs()
        if (throttleTimeMs > 0 && response.shouldClientThrottle(apiVersion)) {
            connectionStates.throttle(nodeId, now + throttleTimeMs)
            log.trace(
                "Connection to node {} is throttled for {} ms until timestamp {}", nodeId, throttleTimeMs,
                now + throttleTimeMs
            )
        }
    }

    /**
     * Handle any completed receives and update the response list with the responses received.
     *
     * @param responses The list of responses to update
     * @param now The current time
     */
    private fun handleCompletedReceives(responses: MutableList<ClientResponse>, now: Long) {
        for (receive: NetworkReceive in selector.completedReceives()) {
            val source = receive.source()
            val req = inFlightRequests.completeNext(source)
            val response = parseResponse(receive.payload(), req.header)
            throttleTimeSensor?.record(response.throttleTimeMs().toDouble(), now)
            if (log.isDebugEnabled) {
                log.debug(
                    "Received {} response from node {} for request with header {}: {}",
                    req.header.apiKey(), req.destination, req.header, response
                )
            }

            // If the received response includes a throttle delay, throttle the connection.
            maybeThrottle(response, req.header.apiVersion(), req.destination, now)
            if (req.isInternalRequest && response is MetadataResponse) metadataUpdater.handleSuccessfulResponse(
                req.header, now,
                response
            ) else if (req.isInternalRequest && response is ApiVersionsResponse) handleApiVersionsResponse(
                responses, req, now,
                response
            ) else responses.add(req.completed(response, now))
        }
    }

    private fun handleApiVersionsResponse(
        responses: MutableList<ClientResponse>,
        req: InFlightRequest, now: Long, apiVersionsResponse: ApiVersionsResponse
    ) {
        val node = req.destination
        if (apiVersionsResponse.data().errorCode() != Errors.NONE.code()) {
            if (req.request.version().toInt() == 0 || apiVersionsResponse.data()
                    .errorCode() != Errors.UNSUPPORTED_VERSION.code()
            ) {
                log.warn(
                    "Received error {} from node {} when making an ApiVersionsRequest with correlation id {}." +
                            "Disconnecting.",
                    Errors.forCode(apiVersionsResponse.data().errorCode()), node, req.header.correlationId()
                )
                selector.close(node)
                processDisconnection(responses, node, now, ChannelState.LOCAL_CLOSE)
            } else {
                // Starting from Apache Kafka 2.4, ApiKeys field is populated with the supported versions of
                // the ApiVersionsRequest when an UNSUPPORTED_VERSION error is returned.
                // If not provided, the client falls back to version 0.
                var maxApiVersion: Short = 0
                if (apiVersionsResponse.data().apiKeys().size > 0) {
                    val apiVersion = apiVersionsResponse.data().apiKeys().find(ApiKeys.API_VERSIONS.id)
                    if (apiVersion != null) {
                        maxApiVersion = apiVersion.maxVersion()
                    }
                }
                nodesNeedingApiVersionsFetch[node] = ApiVersionsRequest.Builder(maxApiVersion)
            }
            return
        }
        val nodeVersionInfo = NodeApiVersions(
            apiVersionsResponse.data().apiKeys(),
            apiVersionsResponse.data().supportedFeatures()
        )
        apiVersions.update(node, nodeVersionInfo)
        connectionStates.ready(node)
        log.debug(
            "Node {} has finalized features epoch: {}, finalized features: {}, supported features: {}," +
                    "API versions: {}.",
            node, apiVersionsResponse.data().finalizedFeaturesEpoch(), apiVersionsResponse.data().finalizedFeatures(),
            apiVersionsResponse.data().supportedFeatures(), nodeVersionInfo
        )
    }

    /**
     * Handle any disconnected connections
     *
     * @param responses The list of responses that completed with the disconnection
     * @param now The current time
     */
    private fun handleDisconnections(responses: MutableList<ClientResponse>, now: Long) {
        for (entry: Map.Entry<String, ChannelState> in selector.disconnected().entries) {
            val node = entry.key
            log.info("Node {} disconnected.", node)
            processDisconnection(responses, node, now, entry.value)
        }
    }

    /**
     * Record any newly completed connections
     */
    private fun handleConnections() {
        for (node: String in selector.connected()) {
            // We are now connected.  Note that we might not still be able to send requests. For instance,
            // if SSL is enabled, the SSL handshake happens after the connection is established.
            // Therefore, it is still necessary to check isChannelReady before attempting to send on this
            // connection.
            if (discoverBrokerVersions) {
                nodesNeedingApiVersionsFetch[node] = ApiVersionsRequest.Builder()
                log.debug("Completed connection to node {}. Fetching API versions.", node)
            } else {
                connectionStates.ready(node)
                log.debug("Completed connection to node {}. Ready.", node)
            }
        }
    }

    private fun handleInitiateApiVersionRequests(now: Long) {
        val iter: MutableIterator<Map.Entry<String, ApiVersionsRequest.Builder>> =
            nodesNeedingApiVersionsFetch.entries.iterator()
        while (iter.hasNext()) {
            val entry = iter.next()
            val node = entry.key
            if (selector.isChannelReady(node) && inFlightRequests.canSendMore(node)) {
                log.debug("Initiating API versions fetch from node {}.", node)
                // We transition the connection to the CHECKING_API_VERSIONS state only when
                // the ApiVersionsRequest is queued up to be sent out. Without this, the client
                // could remain in the CHECKING_API_VERSIONS state forever if the channel does
                // not before ready.
                connectionStates.checkingApiVersions(node)
                val apiVersionRequestBuilder = entry.value
                val clientRequest = newClientRequest(node, apiVersionRequestBuilder, now, true)
                doSend(clientRequest, true, now)
                iter.remove()
            }
        }
    }

    /**
     * Initiate a connection to the given node
     * @param node the node to connect to
     * @param now current time in epoch milliseconds
     */
    private fun initiateConnect(node: Node, now: Long) {
        val nodeConnectionId = node.idString()
        try {
            connectionStates.connecting(nodeConnectionId, now, node.host())
            val address = connectionStates.currentAddress(nodeConnectionId)
            log.debug("Initiating connection to node {} using address {}", node, address)
            selector.connect(
                nodeConnectionId,
                InetSocketAddress(address, node.port()),
                socketSendBuffer,
                socketReceiveBuffer
            )
        } catch (e: IOException) {
            log.warn("Error connecting to node {}", node, e)
            // Attempt failed, we'll try again after the backoff
            connectionStates.disconnected(nodeConnectionId, now)
            // Notify metadata updater of the connection failure
            metadataUpdater.handleServerDisconnect(now, nodeConnectionId, null)
        }
    }

    internal inner class DefaultMetadataUpdater(
        /* the current cluster metadata */
        private val metadata: Metadata
    ) : MetadataUpdater {
        // Defined if there is a request in progress, null otherwise
        private var inProgress: InProgressData? = null
        override fun fetchNodes(): List<Node?> {
            return metadata.fetch().nodes()
        }

        override fun isUpdateDue(now: Long): Boolean {
            return !hasFetchInProgress() && metadata.timeToNextUpdate(now) == 0L
        }

        private fun hasFetchInProgress(): Boolean {
            return inProgress != null
        }

        override fun maybeUpdate(now: Long): Long {
            // should we update our metadata?
            val timeToNextMetadataUpdate = metadata.timeToNextUpdate(now)
            val waitForMetadataFetch = (if (hasFetchInProgress()) defaultRequestTimeoutMs else 0).toLong()
            val metadataTimeout = timeToNextMetadataUpdate.coerceAtLeast(waitForMetadataFetch)
            if (metadataTimeout > 0) {
                return metadataTimeout
            }

            // Beware that the behavior of this method and the computation of timeouts for poll() are
            // highly dependent on the behavior of leastLoadedNode.
            return leastLoadedNode(now)?.let { maybeUpdate(now, it) }
                ?: run {
                    log.debug("Give up sending metadata request since no node is available")
                    reconnectBackoffMs
                }
        }

        override fun handleServerDisconnect(
            now: Long,
            nodeId: String?,
            maybeAuthException: AuthenticationException?
        ) {
            val cluster = metadata.fetch()
            // 'processDisconnection' generates warnings for misconfigured bootstrap server configuration
            // resulting in 'Connection Refused' and misconfigured security resulting in authentication failures.
            // The warning below handles the case where a connection to a broker was established, but was disconnected
            // before metadata could be obtained.
            if (cluster.isBootstrapConfigured) {
                val destinationId = nodeId!!.toInt()
                val node = cluster.nodeById(destinationId)
                if (node != null) log.warn("Bootstrap broker {} disconnected", node)
            }

            // If we have a disconnect while an update is due, we treat it as a failed update
            // so that we can back-off properly
            if (isUpdateDue(now)) handleFailedRequest(now, null)
            maybeAuthException?.let { metadata.fatalError(it) }

            // The disconnect may be the result of stale metadata, so request an update
            metadata.requestUpdate()
        }

        override fun handleFailedRequest(now: Long, maybeFatalException: KafkaException?) {
            maybeFatalException?.let { metadata.fatalError(it) }
            metadata.failedUpdate(now)
            inProgress = null
        }

        override fun handleSuccessfulResponse(
            requestHeader: RequestHeader?,
            now: Long,
            response: MetadataResponse
        ) {
            // If any partition has leader with missing listeners, log up to ten of these partitions
            // for diagnosing broker configuration issues.
            // This could be a transient issue if listeners were added dynamically to brokers.
            val missingListenerPartitions = response.topicMetadata()
                .stream()
                .flatMap { topicMetadata: MetadataResponse.TopicMetadata ->
                    topicMetadata.partitionMetadata()
                        .stream()
                        .filter { partitionMetadata: PartitionMetadata ->
                            partitionMetadata.error == Errors.LISTENER_NOT_FOUND
                        }
                        .map { partitionMetadata: PartitionMetadata ->
                            TopicPartition(
                                topicMetadata.topic(),
                                partitionMetadata.partition()
                            )
                    }
            }
                .collect(Collectors.toList())
            if (missingListenerPartitions.isNotEmpty()) {
                val count = missingListenerPartitions.size
                log.warn(
                    "{} partitions have leader brokers without a matching listener, including {}",
                    count, missingListenerPartitions.subList(0, count.coerceAtMost(10))
                )
            }

            // Check if any topic's metadata failed to get updated
            val errors = response.errors()
            if (errors.isNotEmpty()) log.warn(
                "Error while fetching metadata with correlation id {} : {}",
                requestHeader!!.correlationId(),
                errors
            )

            // When talking to the startup phase of a broker, it is possible to receive an empty metadata set, which
            // we should retry later.
            if (response.brokers().isEmpty()) {
                log.trace("Ignoring empty metadata response with correlation id {}.", requestHeader!!.correlationId())
                metadata.failedUpdate(now)
            } else {
                metadata.update(inProgress!!.requestVersion, response, inProgress!!.isPartialUpdate, now)
            }
            inProgress = null
        }

        override fun close() {
            metadata.close()
        }

        private val isAnyNodeConnecting: Boolean
            /**
             * Return true if there's at least one connection establishment is currently underway
             */
            get() {
                for (node: Node? in fetchNodes()) {
                    if (connectionStates.isConnecting(node!!.idString())) {
                        return true
                    }
                }
                return false
            }

        /**
         * Add a metadata request to the list of sends if we can make one
         */
        private fun maybeUpdate(now: Long, node: Node): Long {
            val nodeConnectionId = node.idString()
            return when {
                canSendRequest(nodeConnectionId, now) -> {
                    val requestAndVersion = metadata.newMetadataRequestAndVersion(now)
                    val metadataRequest = requestAndVersion.requestBuilder
                    log.debug("Sending metadata request {} to node {}", metadataRequest, node)
                    sendInternalMetadataRequest(metadataRequest, nodeConnectionId, now)
                    inProgress = InProgressData(requestAndVersion.requestVersion, requestAndVersion.isPartialUpdate)
                    defaultRequestTimeoutMs.toLong()
                }
                // If there's any connection establishment underway, wait until it completes. This prevents
                // the client from unnecessarily connecting to additional nodes while a previous connection
                // attempt has not been completed.
                isAnyNodeConnecting -> {
                    // Strictly the timeout we should return here is "connect timeout", but as we don't
                    // have such application level configuration, using reconnect backoff instead.
                    reconnectBackoffMs
                }
                connectionStates.canConnect(nodeConnectionId, now) -> {
                    // We don't have a connection to this node right now, make one
                    log.debug("Initialize connection to node {} for sending metadata request", node)
                    initiateConnect(node, now)
                    reconnectBackoffMs
                }
                // connected, but can't send more OR connecting
                // In either case, we just need to wait for a network event to let us know the selected
                // connection might be usable again.
                else -> Long.MAX_VALUE
            }
        }

        private inner class InProgressData(
            val requestVersion: Int,
            val isPartialUpdate: Boolean,
        )
    }

    override fun newClientRequest(
        nodeId: String,
        requestBuilder: AbstractRequest.Builder<*>?,
        createdTimeMs: Long,
        expectResponse: Boolean
    ): ClientRequest {
        return newClientRequest(
            nodeId,
            requestBuilder,
            createdTimeMs,
            expectResponse,
            defaultRequestTimeoutMs,
            null
        )
    }

    // visible for testing
    fun nextCorrelationId(): Int {
        if (SaslClientAuthenticator.isReserved(correlation)) {
            // the numeric overflow is fine as negative values is acceptable
            @Suppress("INTEGER_OVERFLOW")
            correlation = SaslClientAuthenticator.MAX_RESERVED_CORRELATION_ID + 1
        }
        return correlation++
    }

    override fun newClientRequest(
        nodeId: String,
        requestBuilder: AbstractRequest.Builder<*>?,
        createdTimeMs: Long,
        expectResponse: Boolean,
        requestTimeoutMs: Int,
        callback: RequestCompletionHandler?
    ): ClientRequest {
        return ClientRequest(
            nodeId, requestBuilder, nextCorrelationId(), clientId, createdTimeMs, expectResponse,
            requestTimeoutMs, callback
        )
    }

    fun discoverBrokerVersions(): Boolean {
        return discoverBrokerVersions
    }

    internal class InFlightRequest(
        val header: RequestHeader,
        requestTimeoutMs: Int,
        val createdTimeMs: Long,
        val destination: String,
        val callback: RequestCompletionHandler,
        val expectResponse: Boolean,
        // used to flag requests which are initiated internally by NetworkClient
        val isInternalRequest: Boolean,
        val request: AbstractRequest,
        val send: Send,
        val sendTimeMs: Long
    ) {
        val requestTimeoutMs: Long

        constructor(
            clientRequest: ClientRequest,
            header: RequestHeader,
            isInternalRequest: Boolean,
            request: AbstractRequest,
            send: Send,
            sendTimeMs: Long
        ) : this(
            header,
            clientRequest.requestTimeoutMs(),
            clientRequest.createdTimeMs(),
            clientRequest.destination(),
            clientRequest.callback(),
            clientRequest.expectResponse(),
            isInternalRequest,
            request,
            send,
            sendTimeMs
        )

        init {
            this.requestTimeoutMs = requestTimeoutMs.toLong()
        }

        fun timeElapsedSinceSendMs(currentTimeMs: Long): Long {
            return Math.max(0, currentTimeMs - sendTimeMs)
        }

        fun timeElapsedSinceCreateMs(currentTimeMs: Long): Long {
            return Math.max(0, currentTimeMs - createdTimeMs)
        }

        fun completed(response: AbstractResponse?, timeMs: Long): ClientResponse {
            return ClientResponse(
                header, callback, destination, createdTimeMs, timeMs,
                false, null, null, response
            )
        }

        fun disconnected(timeMs: Long, authenticationException: AuthenticationException?): ClientResponse {
            return ClientResponse(
                header, callback, destination, createdTimeMs, timeMs,
                true, null, authenticationException, null
            )
        }

        override fun toString(): String {
            return ("InFlightRequest(header=" + header +
                    ", destination=" + destination +
                    ", expectResponse=" + expectResponse +
                    ", createdTimeMs=" + createdTimeMs +
                    ", sendTimeMs=" + sendTimeMs +
                    ", isInternalRequest=" + isInternalRequest +
                    ", request=" + request +
                    ", callback=" + callback +
                    ", send=" + send + ")")
        }
    }

    companion object {
        fun parseResponse(responseBuffer: ByteBuffer?, requestHeader: RequestHeader): AbstractResponse {
            try {
                return AbstractResponse.parseResponse(responseBuffer, requestHeader)
            } catch (exception: BufferUnderflowException) {
                throw SchemaException(
                    "Buffer underflow while parsing response for request with header $requestHeader",
                    exception
                )
            } catch (exception: CorrelationIdMismatchException) {
                throw if ((SaslClientAuthenticator.isReserved(requestHeader.correlationId())
                            && !SaslClientAuthenticator.isReserved(exception.responseCorrelationId()))
                ) SchemaException(
                    ("The response is unrelated to Sasl request since its correlation id is "
                            + exception.responseCorrelationId() + " and the reserved range for Sasl request is [ "
                            + SaslClientAuthenticator.MIN_RESERVED_CORRELATION_ID + ","
                            + SaslClientAuthenticator.MAX_RESERVED_CORRELATION_ID + "]")
                ) else exception
            }
        }
    }
}
