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

import org.apache.kafka.common.errors.AuthenticationException
import org.apache.kafka.common.utils.ExponentialBackoff
import org.apache.kafka.common.utils.LogContext
import org.slf4j.Logger
import java.net.InetAddress
import java.net.UnknownHostException
import java.util.stream.Collectors

/**
 * The state of our connection to each node in the cluster.
 */
internal class ClusterConnectionStates(
    reconnectBackoffMs: Long,
    reconnectBackoffMaxMs: Long,
    connectionSetupTimeoutMs: Long,
    connectionSetupTimeoutMaxMs: Long,
    logContext: LogContext,
    private val hostResolver: HostResolver,
) {
    private val nodeState: MutableMap<String, NodeConnectionState>
    private val log: Logger
    val connectingNodes: MutableSet<String>
    private val reconnectBackoff: ExponentialBackoff
    private val connectionSetupTimeout: ExponentialBackoff

    init {
        log = logContext.logger(ClusterConnectionStates::class.java)
        reconnectBackoff = ExponentialBackoff(
            reconnectBackoffMs,
            RECONNECT_BACKOFF_EXP_BASE,
            reconnectBackoffMaxMs,
            RECONNECT_BACKOFF_JITTER
        )
        connectionSetupTimeout = ExponentialBackoff(
            connectionSetupTimeoutMs,
            CONNECTION_SETUP_TIMEOUT_EXP_BASE,
            connectionSetupTimeoutMaxMs,
            CONNECTION_SETUP_TIMEOUT_JITTER
        )
        nodeState = HashMap()
        connectingNodes = HashSet()
    }

    /**
     * Return true iff we can currently initiate a new connection. This will be the case if we are
     * not connected and haven't been connected for at least the minimum reconnection backoff
     * period.
     *
     * @param id the connection id to check
     * @param now the current time in ms
     * @return true if we can initiate a new connection
     */
    fun canConnect(id: String, now: Long): Boolean {
        val state = nodeState[id]
        return state == null
                || (state.state.isDisconnected
                && now - state.lastConnectAttemptMs >= state.reconnectBackoffMs)
    }

    /**
     * Return true if we are disconnected from the given node and can't re-establish a connection
     * yet.
     *
     * @param id the connection to check
     * @param now the current time in ms
     */
    fun isBlackedOut(id: String, now: Long): Boolean {
        val state = nodeState[id]
        return state != null
                && state.state.isDisconnected
                && now - state.lastConnectAttemptMs < state.reconnectBackoffMs
    }

    /**
     * Returns the number of milliseconds to wait, based on the connection state, before attempting
     * to send data. When disconnected, this respects the reconnect backoff time. When connecting,
     * return a delay based on the connection timeout. When connected, wait indefinitely (i.e. until
     * a wakeup).
     *
     * @param id the connection to check
     * @param now the current time in ms
     */
    fun connectionDelay(id: String, now: Long): Long {
        val state = nodeState[id] ?: return 0
        return if (state.state == ConnectionState.CONNECTING) connectionSetupTimeoutMs(id)
        else if (state.state.isDisconnected) {
            val timeWaited = now - state.lastConnectAttemptMs
            (state.reconnectBackoffMs - timeWaited).coerceAtLeast(0)
        } else {
            // When connected, we should be able to delay indefinitely since other events
            // (connection or data acked) will cause a wakeup once data can be sent.
            Long.MAX_VALUE
        }
    }

    /**
     * Return true if a specific connection establishment is currently underway.
     *
     * @param id The id of the node to check
     */
    fun isConnecting(id: String): Boolean {
        val state = nodeState[id]
        return state != null && state.state == ConnectionState.CONNECTING
    }

    /**
     * Check whether a connection is either being established or awaiting API version information.
     *
     * @param id The id of the node to check
     * @return true if the node is either connecting or has connected and is awaiting API versions,
     * false otherwise
     */
    fun isPreparingConnection(id: String): Boolean {
        val state = nodeState[id]
        return state != null
                && (state.state == ConnectionState.CONNECTING
                || state.state == ConnectionState.CHECKING_API_VERSIONS)
    }

    /**
     * Enter the connecting state for the given connection, moving to a new resolved address if
     * necessary.
     *
     * @param id the id of the connection
     * @param now the current time in ms
     * @param host the host of the connection, to be resolved internally if needed
     */
    fun connecting(id: String, now: Long, host: String) {
        val connectionState = nodeState[id]
        if (connectionState != null && connectionState.host == host) {
            connectionState.lastConnectAttemptMs = now
            connectionState.state = ConnectionState.CONNECTING
            // Move to next resolved address, or if addresses are exhausted, mark node to be
            // re-resolved
            connectionState.moveToNextAddress()
            connectingNodes.add(id)
            return
        } else if (connectionState != null) log.info(
            "Hostname for node {} changed from {} to {}.",
            id,
            connectionState.host,
            host
        )

        // Create a new NodeConnectionState if nodeState does not already contain one for the
        // specified id or if the hostname associated with the node id changed.
        nodeState[id] = NodeConnectionState(
            state = ConnectionState.CONNECTING,
            lastConnectAttemptMs = now,
            reconnectBackoffMs = reconnectBackoff.backoff(0),
            connectionSetupTimeoutMs = connectionSetupTimeout.backoff(0),
            host = host,
            hostResolver = hostResolver,
        )
        connectingNodes.add(id)
    }

    /**
     * Returns a resolved address for the given connection, resolving it if necessary.
     *
     * @param id the id of the connection
     * @throws UnknownHostException if the address was not resolvable
     */
    @Throws(UnknownHostException::class)
    fun currentAddress(id: String): InetAddress = nodeState(id).currentAddress()

    /**
     * Enter the disconnected state for the given node.
     *
     * @param id the connection we have disconnected
     * @param now the current time in ms
     */
    fun disconnected(id: String, now: Long) {
        val nodeState = nodeState(id)
        nodeState.lastConnectAttemptMs = now
        updateReconnectBackoff(nodeState)
        if (nodeState.state == ConnectionState.CONNECTING) {
            updateConnectionSetupTimeout(nodeState)
            connectingNodes.remove(id)
        } else {
            resetConnectionSetupTimeout(nodeState)
            if (nodeState.state.isConnected) {
                // If a connection had previously been established, clear the addresses to trigger a
                // new DNS resolution because the node IPs may have changed
                nodeState.clearAddresses()
            }
        }
        nodeState.state = ConnectionState.DISCONNECTED
    }

    /**
     * Indicate that the connection is throttled until the specified deadline.
     *
     * @param id the connection to be throttled
     * @param throttleUntilTimeMs the throttle deadline in milliseconds
     */
    fun throttle(id: String, throttleUntilTimeMs: Long) {
        val state = nodeState[id]
        // The throttle deadline should never regress.
        if (state != null && state.throttleUntilTimeMs < throttleUntilTimeMs)
            state.throttleUntilTimeMs = throttleUntilTimeMs
    }

    /**
     * Return the remaining throttling delay in milliseconds if throttling is in progress. Return 0,
     * otherwise.
     *
     * @param id the connection to check
     * @param now the current time in ms
     */
    fun throttleDelayMs(id: String, now: Long): Long {
        val state = nodeState[id]
        return if (state != null && state.throttleUntilTimeMs > now) state.throttleUntilTimeMs - now
        else 0
    }

    /**
     * Return the number of milliseconds to wait, based on the connection state and the throttle
     * time, before attempting to send data. If the connection has been established but being
     * throttled, return throttle delay. Otherwise, return connection delay.
     *
     * @param id the connection to check
     * @param now the current time in ms
     */
    fun pollDelayMs(id: String, now: Long): Long {
        val throttleDelayMs = throttleDelayMs(id, now)
        return if (isConnected(id) && throttleDelayMs > 0) throttleDelayMs
        else connectionDelay(id, now)
    }

    /**
     * Enter the checking_api_versions state for the given node.
     *
     * @param id the connection identifier
     */
    fun checkingApiVersions(id: String) {
        val nodeState = nodeState(id)
        nodeState.state = ConnectionState.CHECKING_API_VERSIONS
        resetConnectionSetupTimeout(nodeState)
        connectingNodes.remove(id)
    }

    /**
     * Enter the ready state for the given node.
     *
     * @param id the connection identifier
     */
    fun ready(id: String) {
        val nodeState = nodeState(id)
        nodeState.state = ConnectionState.READY
        nodeState.authenticationException = null
        resetReconnectBackoff(nodeState)
        resetConnectionSetupTimeout(nodeState)
        connectingNodes.remove(id)
    }

    /**
     * Enter the authentication failed state for the given node.
     *
     * @param id the connection identifier
     * @param now the current time in ms
     * @param exception the authentication exception
     */
    fun authenticationFailed(id: String, now: Long, exception: AuthenticationException?) {
        val nodeState = nodeState(id)
        nodeState.authenticationException = exception
        nodeState.state = ConnectionState.AUTHENTICATION_FAILED
        nodeState.lastConnectAttemptMs = now
        updateReconnectBackoff(nodeState)
    }

    /**
     * Return true if the connection is in the READY state and currently not throttled.
     *
     * @param id the connection identifier
     * @param now the current time in ms
     */
    fun isReady(id: String, now: Long): Boolean = isReady(nodeState[id], now)

    private fun isReady(state: NodeConnectionState?, now: Long): Boolean =
        state != null && state.state == ConnectionState.READY && state.throttleUntilTimeMs <= now

    /**
     * Return `true` if there is at least one node with connection in the `READY` state and not
     * throttled. Returns `false` otherwise.
     *
     * @param now the current time in ms
     */
    fun hasReadyNodes(now: Long): Boolean = nodeState.values.any { isReady(it, now) }

    /**
     * Return true if the connection has been established
     *
     * @param id The id of the node to check
     */
    fun isConnected(id: String): Boolean {
        val state = nodeState[id]
        return state != null && state.state.isConnected
    }

    /**
     * Return true if the connection has been disconnected
     *
     * @param id The id of the node to check
     */
    fun isDisconnected(id: String): Boolean {
        val state = nodeState[id]
        return state != null && state.state.isDisconnected
    }

    /**
     * Return authentication exception if an authentication error occurred
     *
     * @param id The id of the node to check
     */
    fun authenticationException(id: String): AuthenticationException? {
        val state = nodeState[id]
        return state?.authenticationException
    }

    /**
     * Resets the failure count for a node and sets the reconnect backoff to the base value
     * configured via `reconnect.backoff.ms`
     *
     * @param nodeState The node state object to update
     */
    private fun resetReconnectBackoff(nodeState: NodeConnectionState) {
        nodeState.failedAttempts = 0
        nodeState.reconnectBackoffMs = reconnectBackoff.backoff(0)
    }

    /**
     * Resets the failure count for a node and sets the connection setup timeout to the base value
     * configured via socket.connection.setup.timeout.ms
     *
     * @param nodeState The node state object to update
     */
    private fun resetConnectionSetupTimeout(nodeState: NodeConnectionState) {
        nodeState.failedConnectAttempts = 0
        nodeState.connectionSetupTimeoutMs = connectionSetupTimeout.backoff(0)
    }

    /**
     * Increment the failure counter, update the node reconnect backoff exponentially, and record
     * the current timestamp. The delay is
     * `reconnect.backoff.ms * 2**(failures - 1) * (+/- 20% random jitter)`,
     * up to a (pre-jitter) maximum of `reconnect.backoff.max.ms`
     *
     * @param nodeState The node state object to update
     */
    private fun updateReconnectBackoff(nodeState: NodeConnectionState) {
        nodeState.reconnectBackoffMs = reconnectBackoff.backoff(nodeState.failedAttempts)
        nodeState.failedAttempts++
    }

    /**
     * Increment the failure counter and update the node connection setup timeout exponentially.
     * The delay is socket.connection.setup.timeout.ms * 2**(failures) * (+/- 20% random jitter)
     * Up to a (pre-jitter) maximum of reconnect.backoff.max.ms
     *
     * @param nodeState The node state object to update
     */
    private fun updateConnectionSetupTimeout(nodeState: NodeConnectionState) {
        nodeState.failedConnectAttempts++
        nodeState.connectionSetupTimeoutMs =
            connectionSetupTimeout.backoff(nodeState.failedConnectAttempts)
    }

    /**
     * Remove the given node from the tracked connection states. The main difference between this
     * and `disconnected` is the impact on `connectionDelay`: it will be 0 after this call whereas
     * `reconnectBackoffMs` will be taken into account after `disconnected` is called.
     *
     * @param id the connection to remove
     */
    fun remove(id: String) {
        nodeState.remove(id)
        connectingNodes.remove(id)
    }

    /**
     * Get the state of a given connection.
     *
     * @param id the id of the connection
     * @return the state of our connection
     */
    fun connectionState(id: String): ConnectionState = nodeState(id).state

    /**
     * Get the state of a given node.
     *
     * @param id the connection to fetch the state for
     */
    private fun nodeState(id: String): NodeConnectionState =
        checkNotNull(nodeState[id]) { "No entry found for connection $id" }

    /**
     * Get the id set of nodes which are in CONNECTING state
     */
    // package private for testing only
    fun connectingNodes(): Set<String> = connectingNodes

    /**
     * Get the timestamp of the latest connection attempt of a given node.
     *
     * @param id the connection to fetch the state for
     */
    fun lastConnectAttemptMs(id: String): Long = nodeState[id]?.lastConnectAttemptMs ?: 0

    /**
     * Get the current socket connection setup timeout of the given node. The base value is defined
     * via socket.connection.setup.timeout.
     *
     * @param id the connection to fetch the state for
     */
    fun connectionSetupTimeoutMs(id: String): Long = nodeState(id).connectionSetupTimeoutMs

    /**
     * Test if the connection to the given node has reached its timeout
     *
     * @param id the connection to fetch the state for
     * @param now the current time in ms
     */
    fun isConnectionSetupTimeout(id: String, now: Long): Boolean {
        val nodeState = nodeState(id)
        check(nodeState.state == ConnectionState.CONNECTING) {
            "Node $id is not in connecting state"
        }

        return now - lastConnectAttemptMs(id) > connectionSetupTimeoutMs(id)
    }

    /**
     * Return the List of nodes whose connection setup has timed out.#
     *
     * @param now the current time in ms
     */
    fun nodesWithConnectionSetupTimeout(now: Long): List<String> =
        connectingNodes.filter { id -> isConnectionSetupTimeout(id, now) }

    /**
     * The state of our connection to a node.
     */
    private class NodeConnectionState(
        var state: ConnectionState,
        var lastConnectAttemptMs: Long,
        var reconnectBackoffMs: Long,
        var connectionSetupTimeoutMs: Long,
        val host: String,
        private val hostResolver: HostResolver,
    ) {
        var authenticationException: AuthenticationException? = null

        var failedAttempts: Long = 0

        var failedConnectAttempts: Long = 0

        // Connection is being throttled if current time < throttleUntilTimeMs.
        var throttleUntilTimeMs: Long = 0

        private var addresses: List<InetAddress> = emptyList()

        private var addressIndex: Int = -1

        @Deprecated(
            message = "User property instead",
            replaceWith = ReplaceWith("host"),
        )
        fun host(): String = host

        /**
         * Fetches the current selected IP address for this node, resolving [host] if necessary.
         *
         * @return the selected address
         * @throws UnknownHostException if resolving [host] fails
         */
        @Throws(UnknownHostException::class)
        fun currentAddress(): InetAddress {
            if (addresses.isEmpty()) {
                // (Re-)initialize list
                addresses = ClientUtils.resolve(host, hostResolver)
                addressIndex = 0
            }
            return addresses[addressIndex]
        }

        /**
         * Jumps to the next available resolved address for this node. If no other addresses are
         * available, marks the list to be refreshed on the next [currentAddress] call.
         */
        fun moveToNextAddress() {
            if (addresses.isEmpty()) return // Avoid div0. List will initialize on next currentAddress() call
            addressIndex = (addressIndex + 1) % addresses.size
            if (addressIndex == 0) addresses =
                emptyList() // Exhausted list. Re-resolve on next currentAddress() call
        }

        /**
         * Clears the resolved addresses in order to trigger re-resolving on the next
         * [currentAddress] call.
         */
        fun clearAddresses() {
            addresses = emptyList()
        }

        override fun toString(): String =
            "NodeState($state, $lastConnectAttemptMs, $failedAttempts, $throttleUntilTimeMs)"
    }

    companion object {

        const val RECONNECT_BACKOFF_EXP_BASE = 2

        const val RECONNECT_BACKOFF_JITTER = 0.2

        const val CONNECTION_SETUP_TIMEOUT_EXP_BASE = 2

        const val CONNECTION_SETUP_TIMEOUT_JITTER = 0.2
    }
}
