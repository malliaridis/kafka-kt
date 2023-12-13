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

package org.apache.kafka.clients

import java.util.Queue
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentLinkedDeque
import java.util.function.Consumer
import org.apache.kafka.clients.MockClient.RequestMatcher
import org.apache.kafka.common.Node
import org.apache.kafka.common.errors.AuthenticationException
import org.apache.kafka.common.errors.InterruptException
import org.apache.kafka.common.errors.UnsupportedVersionException
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.AbstractRequest
import org.apache.kafka.common.requests.AbstractResponse
import org.apache.kafka.common.requests.MetadataRequest
import org.apache.kafka.common.requests.MetadataResponse
import org.apache.kafka.common.utils.Time
import org.apache.kafka.test.TestUtils.waitForCondition

/**
 * A mock network client for use testing code
 */
open class MockClient(
    private val time: Time,
    private val metadataUpdater: MockMetadataUpdater = NoOpMetadataUpdater(),
) : KafkaClient {
    
    private var correlation = 0
    
    private var wakeupHook: Runnable? = null
    
    private val connections: MutableMap<String, ConnectionState> = HashMap()
    
    private val pendingAuthenticationErrors: MutableMap<Node, Long> = HashMap()
    
    private val authenticationErrors: MutableMap<Node, AuthenticationException?> = HashMap()

    // Use concurrent queue for requests so that requests may be queried from a different thread
    private val requests: Queue<ClientRequest> = ConcurrentLinkedDeque()

    // Use concurrent queue for responses so that responses may be updated during poll() from a
    // different thread.
    private val responses: Queue<ClientResponse> = ConcurrentLinkedDeque()
    
    private val futureResponses: Queue<FutureResponse> = ConcurrentLinkedDeque()
    
    private val metadataUpdates: Queue<MetadataUpdate> = ConcurrentLinkedDeque()

    @Volatile
    private var nodeApiVersions = NodeApiVersions.create()

    @Volatile
    private var numBlockingWakeups = 0

    @Volatile
    private var active = true

    @Volatile
    private var disconnectFuture: CompletableFuture<String>? = null

    @Volatile
    private var readyCallback: Consumer<Node>? = null

    private val obj = Object()

    constructor(time: Time, metadata: Metadata) : this(time, DefaultMockMetadataUpdater(metadata))

    constructor(time: Time, staticNodes: List<Node>) : this(time, StaticMetadataUpdater(staticNodes))

    fun isConnected(idString: String): Boolean {
        return connectionState(idString).state == ConnectionState.State.CONNECTED
    }

    private fun connectionState(idString: String): ConnectionState {
        var connectionState = connections[idString]
        if (connectionState == null) {
            connectionState = ConnectionState()
            connections[idString] = connectionState
        }
        return connectionState
    }

    override fun isReady(node: Node, now: Long): Boolean {
        return connectionState(node.idString()).isReady(now)
    }

    override fun ready(node: Node, now: Long): Boolean {
        if (readyCallback != null) {
            readyCallback!!.accept(node)
        }
        return connectionState(node.idString()).ready(now)
    }

    override fun connectionDelay(node: Node, now: Long): Long =
        connectionState(node.idString()).connectionDelay(now)

    override fun pollDelayMs(node: Node, now: Long): Long =
        connectionDelay(node, now)

    fun backoff(node: Node, durationMs: Long) =
        connectionState(node.idString()).backoff(time.milliseconds() + durationMs)

    fun setUnreachable(node: Node, durationMs: Long) {
        disconnect(node.idString())
        connectionState(node.idString()).setUnreachable(time.milliseconds() + durationMs)
    }

    fun throttle(node: Node, durationMs: Long) =
        connectionState(node.idString()).throttle(time.milliseconds() + durationMs)

    fun delayReady(node: Node, durationMs: Long) =
        connectionState(node.idString()).setReadyDelayed(time.milliseconds() + durationMs)

    fun authenticationFailed(node: Node, backoffMs: Long) {
        pendingAuthenticationErrors.remove(node)
        authenticationErrors[node] =
            Errors.SASL_AUTHENTICATION_FAILED.exception as AuthenticationException?
        disconnect(node.idString())
        backoff(node, backoffMs)
    }

    fun createPendingAuthenticationError(node: Node, backoffMs: Long) {
        pendingAuthenticationErrors[node] = backoffMs
    }

    override fun connectionFailed(node: Node): Boolean =
        connectionState(node.idString()).isBackingOff(time.milliseconds())

    override fun authenticationException(node: Node): AuthenticationException? =
        authenticationErrors[node]

    fun setReadyCallback(onReadyCall: Consumer<Node>?) {
        readyCallback = onReadyCall
    }

    fun setDisconnectFuture(disconnectFuture: CompletableFuture<String>?) {
        this.disconnectFuture = disconnectFuture
    }

    override fun disconnect(nodeId: String) = disconnect(nodeId, false)

    fun disconnect(nodeId: String, allowLateResponses: Boolean) {
        val now = time.milliseconds()
        val iter = requests.iterator()
        while (iter.hasNext()) {
            val request = iter.next()
            if (request.destination == nodeId) {
                val version = request.requestBuilder().latestAllowedVersion
                responses.add(
                    ClientResponse(
                        requestHeader = request.makeHeader(version),
                        callback = request.callback,
                        destination = request.destination,
                        createdTimeMs = request.createdTimeMs,
                        receivedTimeMs = now,
                        disconnected = true,
                    )
                )
                if (!allowLateResponses) iter.remove()
            }
        }
        val curDisconnectFuture = disconnectFuture
        curDisconnectFuture?.complete(nodeId)
        connectionState(nodeId).disconnect()
    }

    override fun send(request: ClientRequest, now: Long) {
        check(connectionState(request.destination).isReady(now)) {
            "Cannot send $request since the destination is not ready"
        }

        // Check if the request is directed to a node with a pending authentication error.
        val authErrorIter = pendingAuthenticationErrors.iterator()

        while (authErrorIter.hasNext()) {
            val (node, backoffMs) = authErrorIter.next()
            if (node.idString() == request.destination) {
                authErrorIter.remove()
                // Set up a disconnected ClientResponse and create an authentication error
                // for the affected node.
                authenticationFailed(node, backoffMs)
                val builder = request.requestBuilder()
                val version = nodeApiVersions.latestUsableVersion(
                    apiKey = request.apiKey,
                    oldestAllowedVersion = builder.oldestAllowedVersion,
                    latestAllowedVersion = builder.latestAllowedVersion,
                )
                val resp = ClientResponse(
                    requestHeader = request.makeHeader(version),
                    callback = request.callback,
                    destination = request.destination,
                    createdTimeMs = request.createdTimeMs,
                    receivedTimeMs = time.milliseconds(),
                    disconnected = true,
                    versionMismatch = null,
                    authenticationException = AuthenticationException("Authentication failed"),
                    responseBody = null
                )
                responses.add(resp)
                return
            }
        }
        val iterator = futureResponses.iterator()
        while (iterator.hasNext()) {
            val futureResp = iterator.next()
            if (
                futureResp.node != null
                && request.destination != futureResp.node.idString()
            ) continue

            val builder = request.requestBuilder()
            try {
                val version = nodeApiVersions.latestUsableVersion(
                    apiKey = request.apiKey,
                    oldestAllowedVersion = builder.oldestAllowedVersion,
                    latestAllowedVersion = builder.latestAllowedVersion
                )
                var unsupportedVersionException: UnsupportedVersionException? = null
                if (futureResp.isUnsupportedRequest) {
                    unsupportedVersionException =
                        UnsupportedVersionException("Api ${request.apiKey} with version $version")
                } else {
                    val abstractRequest = request.requestBuilder().build(version)
                    check(futureResp.requestMatcher.matches(abstractRequest)) {
                        "Request matcher did not match next-in-line request $abstractRequest " +
                                "with prepared response ${futureResp.responseBody}"
                    }
                }
                val resp = ClientResponse(
                    requestHeader = request.makeHeader(version),
                    callback = request.callback,
                    destination = request.destination,
                    createdTimeMs = request.createdTimeMs,
                    receivedTimeMs = time.milliseconds(),
                    disconnected = futureResp.disconnected,
                    versionMismatch = unsupportedVersionException,
                    responseBody = futureResp.responseBody
                )
                responses.add(resp)
            } catch (unsupportedVersionException: UnsupportedVersionException) {
                val resp = ClientResponse(
                    requestHeader = request.makeHeader(builder.latestAllowedVersion),
                    callback = request.callback,
                    destination = request.destination,
                    createdTimeMs = request.createdTimeMs,
                    receivedTimeMs = time.milliseconds(),
                    disconnected = false,
                    versionMismatch = unsupportedVersionException,
                )
                responses.add(resp)
            }
            iterator.remove()
            return
        }
        requests.add(request)
    }

    /**
     * Simulate a blocking poll in order to test wakeup behavior.
     *
     * @param numBlockingWakeups The number of polls which will block until woken up
     */
    fun enableBlockingUntilWakeup(numBlockingWakeups: Int) = synchronized(obj) {
        this.numBlockingWakeups = numBlockingWakeups
    }

    @Synchronized
    override fun wakeup() {
        if (numBlockingWakeups > 0) {
            numBlockingWakeups--
            (this as Object).notify()
        }
        wakeupHook?.run()
    }

    private fun maybeAwaitWakeup() = synchronized(obj) {
        try {
            val remainingBlockingWakeups = numBlockingWakeups
            if (remainingBlockingWakeups <= 0) return
            waitForCondition(
                testCondition = {
                    if (numBlockingWakeups == remainingBlockingWakeups) obj.wait(500)
                    numBlockingWakeups < remainingBlockingWakeups
                },
                maxWaitMs = 5000,
                conditionDetails = "Failed to receive expected wakeup"
            )
        } catch (e: InterruptedException) {
            throw InterruptException(cause = e)
        }
    }

    override fun poll(timeout: Long, now: Long): List<ClientResponse> {
        maybeAwaitWakeup()
        checkTimeoutOfPendingRequests(now)

        // We skip metadata updates if all nodes are currently blacked out
        if (metadataUpdater.isUpdateNeeded && leastLoadedNode(now) != null) {
            val metadataUpdate = metadataUpdates.poll()
            if (metadataUpdate != null) metadataUpdater.update(time, metadataUpdate)
            else metadataUpdater.updateWithCurrentMetadata(time)
        }

        val copy = mutableListOf<ClientResponse>()
        var response: ClientResponse?
        while (responses.poll().also { response = it } != null) {
            response!!.onComplete()
            copy.add(response!!)
        }
        return copy
    }

    private fun elapsedTimeMs(currentTimeMs: Long, startTimeMs: Long): Long =
        (currentTimeMs - startTimeMs).coerceAtLeast(0)

    private fun checkTimeoutOfPendingRequests(nowMs: Long) {
        var request = requests.peek()
        while (
            request != null
            && elapsedTimeMs(nowMs, request.createdTimeMs) >= request.requestTimeoutMs
        ) {
            disconnect(request.destination)
            requests.poll()
            request = requests.peek()
        }
    }

    fun requests(): Queue<ClientRequest> = requests

    fun responses(): Queue<ClientResponse> = responses

    fun futureResponses(): Queue<FutureResponse> = futureResponses

    fun respond(matcher: RequestMatcher, response: AbstractResponse) {
        val nextRequest = checkNotNull(requests.peek()) { "No current requests queued" }
        val request = nextRequest.requestBuilder().build()
        check(matcher.matches(request)) {
            "Request matcher did not match next-in-line request $request"
        }
        respond(response)
    }

    // Utility method to enable out of order responses
    fun respondToRequest(clientRequest: ClientRequest, response: AbstractResponse?) {
        requests.remove(clientRequest)
        val version = clientRequest.requestBuilder().latestAllowedVersion
        responses.add(
            ClientResponse(
                requestHeader = clientRequest.makeHeader(version),
                callback = clientRequest.callback,
                destination = clientRequest.destination,
                createdTimeMs = clientRequest.createdTimeMs,
                receivedTimeMs = time.milliseconds(),
                disconnected = false,
                responseBody = response
            )
        )
    }

    @JvmOverloads
    fun respond(response: AbstractResponse, disconnected: Boolean = false) {
        check(requests.isNotEmpty()) { "No requests pending for inbound response $response" }
        val request = requests.poll()
        val version = request.requestBuilder().latestAllowedVersion
        responses.add(
            ClientResponse(
                requestHeader = request.makeHeader(version),
                callback = request.callback,
                destination = request.destination,
                createdTimeMs = request.createdTimeMs,
                receivedTimeMs = time.milliseconds(),
                disconnected = disconnected,
                responseBody = response,
            )
        )
    }

    @JvmOverloads
    fun respondFrom(response: AbstractResponse?, node: Node, disconnected: Boolean = false) {
        val iterator = requests.iterator()
        while (iterator.hasNext()) {
            val request = iterator.next()
            if ((request.destination == node.idString())) {
                iterator.remove()
                val version = request.requestBuilder().latestAllowedVersion
                responses.add(
                    ClientResponse(
                        requestHeader = request.makeHeader(version),
                        callback = request.callback,
                        destination = request.destination,
                        createdTimeMs = request.createdTimeMs,
                        receivedTimeMs = time.milliseconds(),
                        disconnected = disconnected,
                        responseBody = response
                    )
                )
                return
            }
        }
        throw IllegalArgumentException("No requests available to node $node")
    }

    /**
     * Prepare a response for a request matching the provided matcher. If the matcher does not
     * match, [KafkaClient.send] will throw IllegalStateException.
     *
     * @param matcher The request matcher to apply
     * @param response The response body
     * @param disconnected Whether the request was disconnected. Defaults to `false`.
     */
    fun prepareResponse(
        response: AbstractResponse?,
        matcher: RequestMatcher = ALWAYS_TRUE,
        disconnected: Boolean = false,
    ) = prepareResponseFrom(
        matcher = matcher,
        response = response,
        node = null,
        disconnected = disconnected,
        isUnsupportedVersion = false,
    )

    /**
     * Raise an unsupported version error on the next request if it matches the given matcher. If
     * the matcher does not match, [KafkaClient.send] will throw IllegalStateException.
     *
     * @param matcher The request matcher to apply
     */
    fun prepareUnsupportedVersionResponse(matcher: RequestMatcher) = prepareResponseFrom(
        matcher = matcher,
        response = null,
        node = null,
        disconnected = false,
        isUnsupportedVersion = true,
    )

    fun prepareResponseFrom(
        matcher: RequestMatcher = ALWAYS_TRUE,
        response: AbstractResponse? = null,
        node: Node? = null,
        disconnected: Boolean = false,
        isUnsupportedVersion: Boolean = false,
    ) = futureResponses.add(
        FutureResponse(
            node = node,
            requestMatcher = matcher,
            responseBody = response,
            disconnected = disconnected,
            isUnsupportedRequest = isUnsupportedVersion
        )
    )

    @Throws(InterruptedException::class)
    fun waitForRequests(minRequests: Int, maxWaitMs: Long) = waitForCondition(
        testCondition = { requests.size >= minRequests },
        maxWaitMs = maxWaitMs,
        conditionDetails = "Expected requests have not been sent",
    )

    fun reset() {
        connections.clear()
        requests.clear()
        responses.clear()
        futureResponses.clear()
        metadataUpdates.clear()
        authenticationErrors.clear()
    }

    fun hasPendingMetadataUpdates(): Boolean = !metadataUpdates.isEmpty()

    fun numAwaitingResponses(): Int = futureResponses.size

    fun prepareMetadataUpdate(
        updateResponse: MetadataResponse,
        expectMatchMetadataTopics: Boolean = false,
    ) = metadataUpdates.add(MetadataUpdate(updateResponse, expectMatchMetadataTopics))

    fun updateMetadata(updateResponse: MetadataResponse) {
        metadataUpdater.update(time, MetadataUpdate(updateResponse, false))
    }

    override fun inFlightRequestCount(): Int = requests.size

    override fun hasInFlightRequests(): Boolean = !requests.isEmpty()

    fun hasPendingResponses(): Boolean = !responses.isEmpty() || !futureResponses.isEmpty()

    override fun inFlightRequestCount(nodeId: String): Int {
        var result = 0
        for (req: ClientRequest in requests) if ((req.destination == nodeId)) ++result
        return result
    }

    override fun hasInFlightRequests(nodeId: String): Boolean = inFlightRequestCount(nodeId) > 0

    override fun hasReadyNodes(now: Long): Boolean =
        connections.values.any { cxn -> cxn.isReady(now) }

    override fun newClientRequest(
        nodeId: String,
        requestBuilder: AbstractRequest.Builder<*>,
        createdTimeMs: Long,
        expectResponse: Boolean,
    ): ClientRequest? = newClientRequest(
        nodeId = nodeId,
        requestBuilder = requestBuilder,
        createdTimeMs = createdTimeMs,
        expectResponse = expectResponse,
        requestTimeoutMs = 5000,
        callback = null,
    )

    override fun newClientRequest(
        nodeId: String,
        requestBuilder: AbstractRequest.Builder<*>,
        createdTimeMs: Long,
        expectResponse: Boolean,
        requestTimeoutMs: Int,
        callback: RequestCompletionHandler?,
    ): ClientRequest = ClientRequest(
        destination = nodeId,
        requestBuilder = requestBuilder,
        correlationId = correlation++,
        clientId = "mockClientId",
        createdTimeMs = createdTimeMs,
        expectResponse = expectResponse,
        requestTimeoutMs = requestTimeoutMs,
        callback = callback,
    )

    override fun initiateClose() = close()

    override fun active(): Boolean = active

    override fun close() {
        active = false
        metadataUpdater.close()
    }

    override fun close(nodeId: String) {
        connections.remove(nodeId)
    }

    override fun leastLoadedNode(now: Long): Node? {
        // Consistent with NetworkClient, we do not return nodes awaiting reconnect backoff
        return metadataUpdater.fetchNodes().firstOrNull { node ->
            !connectionState(node.idString()).isBackingOff(now)
        }
    }

    fun setWakeupHook(wakeupHook: Runnable?) {
        this.wakeupHook = wakeupHook
    }

    /**
     * The RequestMatcher provides a way to match a particular request to a response prepared
     * through [prepareResponse]. Basically this allows testers to inspect the request body for the
     * type of the request or for specific fields that should be set, and to fail the test if it
     * doesn't match.
     */
    fun interface RequestMatcher {
        fun matches(body: AbstractRequest?): Boolean
    }

    fun setNodeApiVersions(nodeApiVersions: NodeApiVersions) {
        this.nodeApiVersions = nodeApiVersions
    }

    class MetadataUpdate internal constructor(
        val updateResponse: MetadataResponse,
        val expectMatchRefreshTopics: Boolean,
    ) {
        fun topics(): Set<String> = updateResponse.topicMetadata()
            .map { it.topic }
            .toHashSet()
    }

    /**
     * This is a dumbed down version of [MetadataUpdater] which is used to facilitate metadata
     * tracking primarily in order to serve [KafkaClient.leastLoadedNode] and bookkeeping through
     * [Metadata]. The extensibility allows AdminClient, which does not rely on [Metadata] to do its
     * own thing.
     */
    interface MockMetadataUpdater {

        val isUpdateNeeded: Boolean

        fun fetchNodes(): List<Node>

        fun update(time: Time, update: MetadataUpdate)

        fun updateWithCurrentMetadata(time: Time) = Unit

        fun close() = Unit
    }

    private open class NoOpMetadataUpdater : MockMetadataUpdater {

        override fun fetchNodes(): List<Node> = emptyList()

        override val isUpdateNeeded: Boolean = false

        override fun update(time: Time, update: MetadataUpdate) =
            throw UnsupportedOperationException()
    }

    private class StaticMetadataUpdater(private val nodes: List<Node>) : NoOpMetadataUpdater() {
        override fun fetchNodes(): List<Node> = nodes
    }

    private class DefaultMockMetadataUpdater(private val metadata: Metadata) : MockMetadataUpdater {

        private var lastUpdate: MetadataUpdate? = null

        override val isUpdateNeeded: Boolean
            get() = metadata.updateRequested()

        override fun fetchNodes(): List<Node> = metadata.fetch().nodes

        override fun updateWithCurrentMetadata(time: Time) {
            val lastUpdate = checkNotNull(lastUpdate) { "No previous metadata update to use" }
            update(time, lastUpdate)
        }

        override fun update(time: Time, update: MetadataUpdate) {
            val builder = metadata.newMetadataRequestBuilder()
            maybeCheckExpectedTopics(update, builder)
            metadata.updateWithCurrentRequestVersion(
                response = update.updateResponse,
                isPartialUpdate = false,
                nowMs = time.milliseconds(),
            )
            lastUpdate = update
        }

        override fun close() = metadata.close()

        private fun maybeCheckExpectedTopics(
            update: MetadataUpdate,
            builder: MetadataRequest.Builder,
        ) {
            if (update.expectMatchRefreshTopics) {
                check(!builder.isAllTopics) {
                    "The metadata topics does not match expectation. Expected topics: " +
                            "${update.topics()}, asked topics: ALL"
                }
                val requestedTopics = builder.topics().toSet()
                check(requestedTopics == update.topics()) {
                    "The metadata topics does not match expectation. Expected topics: " +
                            "${update.topics()}, asked topics: $requestedTopics"
                }
            }
        }
    }

    private class ConnectionState {

        private var throttledUntilMs = 0L

        private var readyDelayedUntilMs = 0L

        private var backingOffUntilMs = 0L

        private var unreachableUntilMs = 0L

        var state = State.DISCONNECTED

        fun backoff(untilMs: Long) {
            backingOffUntilMs = untilMs
        }

        fun throttle(untilMs: Long) {
            throttledUntilMs = untilMs
        }

        fun setUnreachable(untilMs: Long) {
            unreachableUntilMs = untilMs
        }

        fun setReadyDelayed(untilMs: Long) {
            readyDelayedUntilMs = untilMs
        }

        fun isReady(now: Long): Boolean {
            return state == State.CONNECTED && notThrottled(now)
        }

        fun isReadyDelayed(now: Long): Boolean {
            return now < readyDelayedUntilMs
        }

        fun notThrottled(now: Long): Boolean = now > throttledUntilMs

        fun isBackingOff(now: Long): Boolean = now < backingOffUntilMs

        fun isUnreachable(now: Long): Boolean = now < unreachableUntilMs

        fun disconnect() {
            state = State.DISCONNECTED
        }

        fun connectionDelay(now: Long): Long {
            if (state != State.DISCONNECTED) return Long.MAX_VALUE
            return if (backingOffUntilMs > now) backingOffUntilMs - now else 0
        }

        fun ready(now: Long): Boolean {
            when (state) {
                State.CONNECTED -> return notThrottled(now)
                State.CONNECTING -> {
                    if (isReadyDelayed(now)) return false
                    state = State.CONNECTED
                    return ready(now)
                }

                State.DISCONNECTED -> {
                    if (isBackingOff(now)) return false
                    else if (isUnreachable(now)) {
                        backingOffUntilMs = now + 100
                        return false
                    }
                    state = State.CONNECTING
                    return ready(now)
                }

                else -> throw IllegalArgumentException("Invalid state: $state")
            }
        }

        enum class State {
            CONNECTING,
            CONNECTED,
            DISCONNECTED
        }
    }

    data class FutureResponse(
        val node: Node?,
        val requestMatcher: RequestMatcher,
        val responseBody: AbstractResponse?,
        val disconnected: Boolean,
        val isUnsupportedRequest: Boolean,
    )

    companion object {
        val ALWAYS_TRUE = RequestMatcher { true }
    }
}
