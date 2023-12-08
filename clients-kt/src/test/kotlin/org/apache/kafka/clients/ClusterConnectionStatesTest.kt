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

import java.net.InetAddress
import java.net.UnknownHostException
import org.apache.kafka.clients.ClientUtils.resolve
import org.apache.kafka.common.errors.AuthenticationException
import org.apache.kafka.common.utils.LogContext
import org.apache.kafka.common.utils.MockTime
import org.junit.jupiter.api.BeforeEach
import kotlin.math.ln
import kotlin.math.max
import kotlin.math.min
import kotlin.math.pow
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotSame
import kotlin.test.assertNull
import kotlin.test.assertSame
import kotlin.test.assertTrue
import kotlin.test.fail

class ClusterConnectionStatesTest {

    private val time = MockTime()

    private val reconnectBackoffMs = (10 * 1000).toLong()

    private val reconnectBackoffMax = (60 * 1000).toLong()

    private val connectionSetupTimeoutMs = (10 * 1000).toLong()

    private val connectionSetupTimeoutMaxMs = (127 * 1000).toLong()

    private val reconnectBackoffExpBase =
        ClusterConnectionStates.RECONNECT_BACKOFF_EXP_BASE.toDouble()

    private val reconnectBackoffJitter = ClusterConnectionStates.RECONNECT_BACKOFF_JITTER

    private val connectionSetupTimeoutExpBase =
        ClusterConnectionStates.CONNECTION_SETUP_TIMEOUT_EXP_BASE.toDouble()

    private val connectionSetupTimeoutJitter =
        ClusterConnectionStates.CONNECTION_SETUP_TIMEOUT_JITTER

    private val nodeId1 = "1001"

    private val nodeId2 = "2002"

    private val nodeId3 = "3003"

    private val hostTwoIps = "multiple.ip.address"

    private lateinit var connectionStates: ClusterConnectionStates

    // For testing nodes with a single IP address, use localhost and default DNS resolution
    private val singleIPHostResolver = DefaultHostResolver()

    // For testing nodes with multiple IP addresses, mock DNS resolution to get consistent results
    private val multipleIPHostResolver = AddressChangeHostResolver(initialAddresses, newAddresses)

    @BeforeEach
    fun setup() {
        connectionStates = ClusterConnectionStates(
            reconnectBackoffMs = reconnectBackoffMs,
            reconnectBackoffMaxMs = reconnectBackoffMax,
            connectionSetupTimeoutMs = connectionSetupTimeoutMs,
            connectionSetupTimeoutMaxMs = connectionSetupTimeoutMaxMs,
            logContext = LogContext(),
            hostResolver = singleIPHostResolver,
        )
    }

    @Test
    fun testClusterConnectionStateChanges() {
        assertTrue(connectionStates.canConnect(nodeId1, time.milliseconds()))
        assertEquals(0, connectionStates.connectionDelay(nodeId1, time.milliseconds()))

        // Start connecting to Node and check state
        connectionStates.connecting(nodeId1, time.milliseconds(), "localhost")
        assertEquals(
            ConnectionState.CONNECTING, connectionStates.connectionState(nodeId1)
        )
        assertTrue(connectionStates.isConnecting(nodeId1))
        assertFalse(connectionStates.isReady(nodeId1, time.milliseconds()))
        assertFalse(connectionStates.isBlackedOut(nodeId1, time.milliseconds()))
        assertFalse(connectionStates.hasReadyNodes(time.milliseconds()))
        val connectionDelay = connectionStates.connectionDelay(nodeId1, time.milliseconds())
        val connectionDelayDelta = connectionSetupTimeoutMs * connectionSetupTimeoutJitter
        assertEquals(
            connectionSetupTimeoutMs.toDouble(), connectionDelay.toDouble(), connectionDelayDelta
        )
        time.sleep(100)

        // Successful connection
        connectionStates.ready(nodeId1)
        assertEquals(ConnectionState.READY, connectionStates.connectionState(nodeId1))
        assertTrue(connectionStates.isReady(nodeId1, time.milliseconds()))
        assertTrue(connectionStates.hasReadyNodes(time.milliseconds()))
        assertFalse(connectionStates.isConnecting(nodeId1))
        assertFalse(connectionStates.isBlackedOut(nodeId1, time.milliseconds()))
        assertEquals(
            Long.MAX_VALUE, connectionStates.connectionDelay(nodeId1, time.milliseconds())
        )
        time.sleep(15000)

        // Disconnected from broker
        connectionStates.disconnected(nodeId1, time.milliseconds())
        assertEquals(
            ConnectionState.DISCONNECTED, connectionStates.connectionState(nodeId1)
        )
        assertTrue(connectionStates.isDisconnected(nodeId1))
        assertTrue(connectionStates.isBlackedOut(nodeId1, time.milliseconds()))
        assertFalse(connectionStates.isConnecting(nodeId1))
        assertFalse(connectionStates.hasReadyNodes(time.milliseconds()))
        assertFalse(connectionStates.canConnect(nodeId1, time.milliseconds()))

        // After disconnecting we expect a backoff value equal to the reconnect.backoff.ms
        // setting (plus minus 20% jitter)
        val backoffTolerance = reconnectBackoffMs * reconnectBackoffJitter
        val currentBackoff = connectionStates.connectionDelay(nodeId1, time.milliseconds())
        assertEquals(reconnectBackoffMs.toDouble(), currentBackoff.toDouble(), backoffTolerance)
        time.sleep(currentBackoff + 1)
        // after waiting for the current backoff value we should be allowed to connect again
        assertTrue(connectionStates.canConnect(nodeId1, time.milliseconds()))
    }

    @Test
    fun testMultipleNodeConnectionStates() {
        // Check initial state, allowed to connect to all nodes, but no nodes shown as ready
        assertTrue(connectionStates.canConnect(nodeId1, time.milliseconds()))
        assertTrue(connectionStates.canConnect(nodeId2, time.milliseconds()))
        assertFalse(connectionStates.hasReadyNodes(time.milliseconds()))

        // Start connecting one node and check that the pool only shows ready nodes after
        // successful connect
        connectionStates.connecting(nodeId2, time.milliseconds(), "localhost")
        assertFalse(connectionStates.hasReadyNodes(time.milliseconds()))
        time.sleep(1000)
        connectionStates.ready(nodeId2)
        assertTrue(connectionStates.hasReadyNodes(time.milliseconds()))

        // Connect second node and check that both are shown as ready, pool should immediately
        // show ready nodes, since node2 is already connected
        connectionStates.connecting(nodeId1, time.milliseconds(), "localhost")
        assertTrue(connectionStates.hasReadyNodes(time.milliseconds()))
        time.sleep(1000)
        connectionStates.ready(nodeId1)
        assertTrue(connectionStates.hasReadyNodes(time.milliseconds()))
        time.sleep(12000)

        // disconnect nodes and check proper state of pool throughout
        connectionStates.disconnected(nodeId2, time.milliseconds())
        assertTrue(connectionStates.hasReadyNodes(time.milliseconds()))
        assertTrue(connectionStates.isBlackedOut(nodeId2, time.milliseconds()))
        assertFalse(connectionStates.isBlackedOut(nodeId1, time.milliseconds()))
        time.sleep(connectionStates.connectionDelay(nodeId2, time.milliseconds()))
        // by the time node1 disconnects node2 should have been unblocked again
        connectionStates.disconnected(nodeId1, time.milliseconds() + 1)
        assertTrue(connectionStates.isBlackedOut(nodeId1, time.milliseconds()))
        assertFalse(connectionStates.isBlackedOut(nodeId2, time.milliseconds()))
        assertFalse(connectionStates.hasReadyNodes(time.milliseconds()))
    }

    @Test
    fun testAuthorizationFailed() {
        // Try connecting
        connectionStates.connecting(nodeId1, time.milliseconds(), "localhost")
        time.sleep(100)
        connectionStates.authenticationFailed(
            nodeId1, time.milliseconds(), AuthenticationException("No path to CA for certificate!")
        )
        time.sleep(1000)
        assertEquals(
            connectionStates.connectionState(nodeId1), ConnectionState.AUTHENTICATION_FAILED
        )
        assertTrue(connectionStates.authenticationException(nodeId1) is AuthenticationException)
        assertFalse(connectionStates.hasReadyNodes(time.milliseconds()))
        assertFalse(connectionStates.canConnect(nodeId1, time.milliseconds()))
        time.sleep(connectionStates.connectionDelay(nodeId1, time.milliseconds()) + 1)
        assertTrue(connectionStates.canConnect(nodeId1, time.milliseconds()))
        connectionStates.ready(nodeId1)
        assertNull(connectionStates.authenticationException(nodeId1))
    }

    @Test
    fun testRemoveNode() {
        connectionStates.connecting(nodeId1, time.milliseconds(), "localhost")
        time.sleep(1000)
        connectionStates.ready(nodeId1)
        time.sleep(10000)
        connectionStates.disconnected(nodeId1, time.milliseconds())
        // Node is disconnected and blocked, removing it from the list should reset all blocks
        connectionStates.remove(nodeId1)
        assertTrue(connectionStates.canConnect(nodeId1, time.milliseconds()))
        assertFalse(connectionStates.isBlackedOut(nodeId1, time.milliseconds()))
        assertEquals(
            connectionStates.connectionDelay(nodeId1, time.milliseconds()), 0L
        )
    }

    @Test
    fun testMaxReconnectBackoff() {
        val effectiveMaxReconnectBackoff =
            Math.round(reconnectBackoffMax * (1 + reconnectBackoffJitter))
        connectionStates.connecting(nodeId1, time.milliseconds(), "localhost")
        time.sleep(1000)
        connectionStates.disconnected(nodeId1, time.milliseconds())

        // Do 100 reconnect attempts and check that MaxReconnectBackoff (plus jitter) is not exceeded
        for (i in 0..99) {
            val reconnectBackoff = connectionStates.connectionDelay(nodeId1, time.milliseconds())
            assertTrue(reconnectBackoff <= effectiveMaxReconnectBackoff)
            assertFalse(connectionStates.canConnect(nodeId1, time.milliseconds()))
            time.sleep(reconnectBackoff + 1)
            assertTrue(connectionStates.canConnect(nodeId1, time.milliseconds()))
            connectionStates.connecting(nodeId1, time.milliseconds(), "localhost")
            time.sleep(10)
            connectionStates.disconnected(nodeId1, time.milliseconds())
        }
    }

    @Test
    fun testExponentialReconnectBackoff() {
        verifyReconnectExponentialBackoff(false)
        verifyReconnectExponentialBackoff(true)
    }

    @Test
    fun testThrottled() {
        connectionStates.connecting(nodeId1, time.milliseconds(), "localhost")
        time.sleep(1000)
        connectionStates.ready(nodeId1)
        time.sleep(10000)

        // Initially not throttled.
        assertEquals(0, connectionStates.throttleDelayMs(nodeId1, time.milliseconds()))

        // Throttle for 100ms from now.
        connectionStates.throttle(nodeId1, time.milliseconds() + 100)
        assertEquals(
            100, connectionStates.throttleDelayMs(nodeId1, time.milliseconds())
        )

        // Still throttled after 50ms. The remaining delay is 50ms. The poll delay should be same as throttling delay.
        time.sleep(50)
        assertEquals(
            50, connectionStates.throttleDelayMs(nodeId1, time.milliseconds())
        )
        assertEquals(50, connectionStates.pollDelayMs(nodeId1, time.milliseconds()))

        // Not throttled anymore when the deadline is reached. The poll delay should be same as connection delay.
        time.sleep(50)
        assertEquals(0, connectionStates.throttleDelayMs(nodeId1, time.milliseconds()))
        assertEquals(
            connectionStates.connectionDelay(nodeId1, time.milliseconds()),
            connectionStates.pollDelayMs(nodeId1, time.milliseconds())
        )
    }

    @Test
    @Throws(UnknownHostException::class)
    fun testSingleIP() {
        assertEquals(1, resolve("localhost", singleIPHostResolver).size)
        connectionStates.connecting(nodeId1, time.milliseconds(), "localhost")
        val currAddress = connectionStates.currentAddress(nodeId1)
        connectionStates.connecting(nodeId1, time.milliseconds(), "localhost")
        assertSame(currAddress, connectionStates.currentAddress(nodeId1))
    }

    @Test
    @Throws(UnknownHostException::class)
    fun testMultipleIPs() {
        setupMultipleIPs()
        assertTrue(resolve(hostTwoIps, multipleIPHostResolver).size > 1)
        connectionStates.connecting(nodeId1, time.milliseconds(), hostTwoIps)
        val addr1 = connectionStates.currentAddress(nodeId1)
        connectionStates.connecting(nodeId1, time.milliseconds(), hostTwoIps)
        val addr2 = connectionStates.currentAddress(nodeId1)
        assertNotSame(addr1, addr2)
        connectionStates.connecting(nodeId1, time.milliseconds(), hostTwoIps)
        val addr3 = connectionStates.currentAddress(nodeId1)
        assertNotSame(addr1, addr3)
    }

    @Test
    @Throws(UnknownHostException::class)
    fun testHostResolveChange() {
        setupMultipleIPs()
        assertTrue(resolve(hostTwoIps, multipleIPHostResolver).size > 1)
        connectionStates.connecting(nodeId1, time.milliseconds(), hostTwoIps)
        val addr1 = connectionStates.currentAddress(nodeId1)
        multipleIPHostResolver.changeAddresses()
        connectionStates.connecting(nodeId1, time.milliseconds(), "localhost")
        val addr2 = connectionStates.currentAddress(nodeId1)
        assertNotSame(addr1, addr2)
    }

    @Test
    @Throws(UnknownHostException::class)
    fun testNodeWithNewHostname() {
        setupMultipleIPs()
        connectionStates.connecting(nodeId1, time.milliseconds(), "localhost")
        val addr1 = connectionStates.currentAddress(nodeId1)
        multipleIPHostResolver.changeAddresses()
        connectionStates.connecting(nodeId1, time.milliseconds(), hostTwoIps)
        val addr2 = connectionStates.currentAddress(nodeId1)
        assertNotSame(addr1, addr2)
    }

    @Test
    fun testIsPreparingConnection() {
        assertFalse(connectionStates.isPreparingConnection(nodeId1))
        connectionStates.connecting(nodeId1, time.milliseconds(), "localhost")
        assertTrue(connectionStates.isPreparingConnection(nodeId1))
        connectionStates.checkingApiVersions(nodeId1)
        assertTrue(connectionStates.isPreparingConnection(nodeId1))
        connectionStates.disconnected(nodeId1, time.milliseconds())
        assertFalse(connectionStates.isPreparingConnection(nodeId1))
    }

    @Test
    fun testExponentialConnectionSetupTimeout() {
        assertTrue(connectionStates.canConnect(nodeId1, time.milliseconds()))

        // Check the exponential timeout growth
        var n = 0
        val fraction = connectionSetupTimeoutMaxMs.toDouble() / connectionSetupTimeoutMs
        val logValue = ln(connectionSetupTimeoutExpBase)

        while (n <= ln(fraction) / logValue) {
            connectionStates.connecting(nodeId1, time.milliseconds(), "localhost")
            assertTrue(connectionStates.connectingNodes().contains(nodeId1))
            assertEquals(
                expected = connectionSetupTimeoutMs * connectionSetupTimeoutExpBase.pow(n.toDouble()),
                actual = connectionStates.connectionSetupTimeoutMs(nodeId1).toDouble(),
                absoluteTolerance =
                connectionSetupTimeoutMs
                * connectionSetupTimeoutExpBase.pow(n.toDouble())
                * connectionSetupTimeoutJitter,
            )
            connectionStates.disconnected(nodeId1, time.milliseconds())
            assertFalse(connectionStates.connectingNodes().contains(nodeId1))
            n++
        }

        // Check the timeout value upper bound
        connectionStates.connecting(nodeId1, time.milliseconds(), "localhost")
        assertEquals(
            connectionSetupTimeoutMaxMs.toDouble(),
            connectionStates.connectionSetupTimeoutMs(nodeId1).toDouble(),
            connectionSetupTimeoutMaxMs * connectionSetupTimeoutJitter
        )
        assertTrue(connectionStates.connectingNodes().contains(nodeId1))

        // Should reset the timeout value to the init value
        connectionStates.ready(nodeId1)
        assertEquals(
            connectionSetupTimeoutMs.toDouble(),
            connectionStates.connectionSetupTimeoutMs(nodeId1).toDouble(),
            connectionSetupTimeoutMs * connectionSetupTimeoutJitter
        )
        assertFalse(connectionStates.connectingNodes().contains(nodeId1))
        connectionStates.disconnected(nodeId1, time.milliseconds())

        // Check if the connection state transition from ready to disconnected
        // won't increase the timeout value
        connectionStates.connecting(nodeId1, time.milliseconds(), "localhost")
        assertEquals(
            connectionSetupTimeoutMs.toDouble(),
            connectionStates.connectionSetupTimeoutMs(nodeId1).toDouble(),
            connectionSetupTimeoutMs * connectionSetupTimeoutJitter
        )
        assertTrue(connectionStates.connectingNodes().contains(nodeId1))
    }

    @Test
    fun testTimedOutConnections() {
        // Initiate two connections
        connectionStates.connecting(nodeId1, time.milliseconds(), "localhost")
        connectionStates.connecting(nodeId2, time.milliseconds(), "localhost")

        // Expect no timed out connections
        assertEquals(
            0, connectionStates.nodesWithConnectionSetupTimeout(time.milliseconds()).size
        )

        // Advance time by half of the connection setup timeout
        time.sleep(connectionSetupTimeoutMs / 2)

        // Initiate a third connection
        connectionStates.connecting(nodeId3, time.milliseconds(), "localhost")

        // Advance time beyond the connection setup timeout (+ max jitter) for the first two connections
        time.sleep((connectionSetupTimeoutMs / 2 + connectionSetupTimeoutMs * connectionSetupTimeoutJitter).toLong())

        // Expect two timed out connections.
        var timedOutConnections: List<String?> =
            connectionStates.nodesWithConnectionSetupTimeout(time.milliseconds())
        assertEquals(2, timedOutConnections.size)
        assertTrue(timedOutConnections.contains(nodeId1))
        assertTrue(timedOutConnections.contains(nodeId2))

        // Disconnect the first two connections
        connectionStates.disconnected(nodeId1, time.milliseconds())
        connectionStates.disconnected(nodeId2, time.milliseconds())

        // Advance time beyond the connection setup timeout (+ max jitter) for the third connections
        time.sleep((connectionSetupTimeoutMs / 2 + connectionSetupTimeoutMs * connectionSetupTimeoutJitter).toLong())

        // Expect one timed out connection
        timedOutConnections =
            connectionStates.nodesWithConnectionSetupTimeout(time.milliseconds())
        assertEquals(1, timedOutConnections.size)
        assertTrue(timedOutConnections.contains(nodeId3))

        // Disconnect the third connection
        connectionStates.disconnected(nodeId3, time.milliseconds())

        // Expect no timed out connections
        assertEquals(
            0, connectionStates.nodesWithConnectionSetupTimeout(time.milliseconds()).size
        )
    }

    private fun setupMultipleIPs() {
        connectionStates = ClusterConnectionStates(
            reconnectBackoffMs,
            reconnectBackoffMax,
            connectionSetupTimeoutMs,
            connectionSetupTimeoutMaxMs,
            LogContext(),
            multipleIPHostResolver
        )
    }

    private fun verifyReconnectExponentialBackoff(enterCheckingApiVersionState: Boolean) {
        val reconnectBackoffMaxExp =
            ln(reconnectBackoffMax / max(reconnectBackoffMs.toDouble(), 1.0)) / ln(
                reconnectBackoffExpBase.toDouble()
            )
        connectionStates.remove(nodeId1)
        // Run through 10 disconnects and check that reconnect backoff value is within
        // expected range for every attempt
        for (i in 0..9) {
            connectionStates.connecting(nodeId1, time.milliseconds(), "localhost")
            if (enterCheckingApiVersionState) {
                connectionStates.checkingApiVersions(nodeId1)
            }
            connectionStates.disconnected(nodeId1, time.milliseconds())
            // Calculate expected backoff value without jitter
            val expectedBackoff = Math.round(
                reconnectBackoffExpBase.pow(
                    min(
                        i.toDouble(), reconnectBackoffMaxExp
                    )
                ) * reconnectBackoffMs
            )
            val currentBackoff = connectionStates.connectionDelay(nodeId1, time.milliseconds())
            assertEquals(
                expectedBackoff.toDouble(),
                currentBackoff.toDouble(),
                reconnectBackoffJitter * expectedBackoff
            )
            time.sleep(connectionStates.connectionDelay(nodeId1, time.milliseconds()) + 1)
        }
    }

    companion object {

        private val initialAddresses: Array<InetAddress>

        private val newAddresses: Array<InetAddress>

        init {
            try {
                initialAddresses = listOf(
                    InetAddress.getByName("10.200.20.100"),
                    InetAddress.getByName("10.200.20.101"),
                    InetAddress.getByName("10.200.20.102")
                ).toTypedArray()
                newAddresses = listOf(
                    InetAddress.getByName("10.200.20.103"),
                    InetAddress.getByName("10.200.20.104"),
                    InetAddress.getByName("10.200.20.105")
                ).toTypedArray()
            } catch (e: UnknownHostException) {
                fail("Attempted to create an invalid InetAddress, this should not happen")
            }
        }
    }
}

