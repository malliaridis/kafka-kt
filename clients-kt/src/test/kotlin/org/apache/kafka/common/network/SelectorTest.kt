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

import java.io.ByteArrayOutputStream
import java.io.DataOutputStream
import java.io.IOException
import java.lang.reflect.Field
import java.net.InetSocketAddress
import java.net.ServerSocket
import java.nio.ByteBuffer
import java.nio.channels.SelectionKey
import java.nio.channels.ServerSocketChannel
import java.nio.channels.SocketChannel
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.function.Supplier
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.MetricName
import org.apache.kafka.common.memory.MemoryPool
import org.apache.kafka.common.memory.SimpleMemoryPool
import org.apache.kafka.common.metrics.KafkaMetric
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.network.NetworkTestUtils.waitForChannelConnected
import org.apache.kafka.common.network.NetworkTestUtils.waitForChannelReady
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.utils.LogContext
import org.apache.kafka.common.utils.MockTime
import org.apache.kafka.common.utils.Time
import org.apache.kafka.common.utils.Utils
import org.apache.kafka.test.TestCondition
import org.apache.kafka.test.TestUtils.fieldValue
import org.apache.kafka.test.TestUtils.randomString
import org.apache.kafka.test.TestUtils.setFieldValue
import org.apache.kafka.test.TestUtils.waitForCondition
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import org.mockito.Mockito.atLeastOnce
import org.mockito.Mockito.doThrow
import org.mockito.Mockito.mock
import org.mockito.Mockito.verify
import org.mockito.Mockito.`when`
import kotlin.math.max
import kotlin.random.Random
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * A set of tests for the selector. These use a test harness that runs a simple socket server that echos back responses.
 */
@Timeout(240)
open class SelectorTest {
    
    protected lateinit  var server: EchoServer
    
    protected lateinit  var time: Time
    
    protected lateinit var selector: Selector
    
    protected lateinit var channelBuilder: ChannelBuilder
    
    protected lateinit var metrics: Metrics
    
    @BeforeEach
    @Throws(Exception::class)
    open fun setUp() {
        val configs = emptyMap<String, Any?>()
        server = EchoServer(SecurityProtocol.PLAINTEXT, configs)
        server.start()
        time = MockTime()
        channelBuilder = PlaintextChannelBuilder(ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT))
        channelBuilder.configure(clientConfigs())
        metrics = Metrics()
        selector = Selector(
            connectionMaxIdleMs = CONNECTION_MAX_IDLE_MS,
            metrics = metrics,
            time = time,
            metricGrpPrefix = METRIC_GROUP,
            channelBuilder = channelBuilder,
            logContext = LogContext(),
        )
    }

    @AfterEach
    @Throws(Exception::class)
    open fun tearDown() {
        try {
            verifySelectorEmpty()
        } finally {
            selector.close()
            server.close()
            metrics.close()
        }
    }

    protected open fun clientConfigs(): Map<String, Any?> = emptyMap()

    /**
     * Validate that when the server disconnects, a client send ends up with that node in the disconnected list.
     */
    @Test
    @Throws(Exception::class)
    fun testServerDisconnect() {
        val node = "0"

        // connect and do a simple request
        blockingConnect(node)
        assertEquals("hello", blockingRequest(node, "hello"))
        val channel = selector.channel(node)

        // disconnect
        server.closeConnections()
        waitForCondition(
            testCondition = TestCondition {
                try {
                    selector.poll(1000L)
                    return@TestCondition selector.disconnected().containsKey(node)
                } catch (e: IOException) {
                    throw RuntimeException(e)
                }
            },
            maxWaitMs = 5000,
            conditionDetails = "Failed to observe disconnected node in disconnected set",
        )
        assertNull(channel!!.selectionKey().attachment())

        // reconnect and do another request
        blockingConnect(node)
        assertEquals("hello", blockingRequest(node, "hello"))
    }

    /**
     * Sending a request with one already in flight should result in an exception
     */
    @Test
    @Throws(Exception::class)
    fun testCantSendWithInProgress() {
        val node = "0"
        blockingConnect(node)
        selector.send(createSend(node, "test1"))
        assertFailsWith<IllegalStateException>(
            message = "IllegalStateException not thrown when sending a request with one in flight",
        ) { selector.send(createSend(node, "test2")) }
        selector.poll(0)
        assertTrue(selector.disconnected().containsKey(node), "Channel not closed")
        assertEquals(ChannelState.FAILED_SEND, selector.disconnected()[node])
    }

    /**
     * Sending a request to a node without an existing connection should result in an exception
     */
    @Test
    fun testSendWithoutConnecting() {
        assertFailsWith<IllegalStateException> { selector.send(createSend("0", "test")) }
    }

    /**
     * Sending a request to a node with a bad hostname should result in an exception during connect
     */
    @Test
    fun testNoRouteToHost() {
        assertFailsWith<IOException> {
            selector.connect(
                id = "0",
                address = InetSocketAddress("some.invalid.hostname.foo.bar.local", server.port),
                sendBufferSize = BUFFER_SIZE,
                receiveBufferSize = BUFFER_SIZE
            )
        }
    }

    /**
     * Sending a request to a node not listening on that port should result in disconnection
     */
    @Test
    @Throws(Exception::class)
    fun testConnectionRefused() {
        val node = "0"
        val nonListeningSocket = ServerSocket(0)
        val nonListeningPort = nonListeningSocket.getLocalPort()
        selector.connect(node, InetSocketAddress("localhost", nonListeningPort), BUFFER_SIZE, BUFFER_SIZE)
        while (selector.disconnected().containsKey(node)) {
            assertEquals(ChannelState.NOT_CONNECTED, selector.disconnected()[node])
            selector.poll(1000L)
        }
        nonListeningSocket.close()
    }

    /**
     * Send multiple requests to several connections in parallel. Validate that responses are received in
     * the order that requests were sent.
     */
    @Test
    @Throws(Exception::class)
    fun testNormalOperation() {
        val conns = 5
        val reqs = 500

        // create connections
        val addr = InetSocketAddress("localhost", server.port)
        for (i in 0 until conns) connect(i.toString(), addr)
        // send echo requests and receive responses
        val requests = mutableMapOf<String, Int>()
        val responses = mutableMapOf<String, Int>()
        var responseCount = 0
        for (i in 0 until conns) {
            val node = i.toString()
            selector.send(createSend(node, "$node-0"))
        }

        // loop until we complete all requests
        while (responseCount < conns * reqs) {
            // do the i/o
            selector.poll(0L)
            assertEquals(0, selector.disconnected().size, "No disconnects should have occurred.")

            // handle any responses we may have gotten
            for (receive in selector.completedReceives()) {
                val pieces = asString(receive).split("-".toRegex())
                    .dropLastWhile { it.isEmpty() }
                    .toTypedArray()

                assertEquals(2, pieces.size, "Should be in the form 'conn-counter'")
                assertEquals(receive.source(), pieces[0], "Check the source")
                assertEquals(0, receive.payload()!!.position(), "Check that the receive has kindly been rewound")

                if (responses.containsKey(receive.source())) {
                    assertEquals(responses[receive.source()], pieces[1].toInt(), "Check the request counter")
                    responses[receive.source()] = responses[receive.source()]!! + 1
                } else {
                    assertEquals(0, pieces[1].toInt(), "Check the request counter")
                    responses[receive.source()] = 1
                }
                responseCount++
            }

            // prepare new sends for the next round
            for (send in selector.completedSends()) {
                val dest = send.destinationId
                requests[dest] = if (requests.containsKey(dest)) requests[dest]!! + 1 else 1
                if (requests[dest]!! < reqs) selector.send(createSend(dest, dest + "-" + requests[dest]))
            }
        }
        if (channelBuilder is PlaintextChannelBuilder) assertEquals(0, cipherMetrics(metrics).size)
        else {
            waitForCondition(
                testCondition = { cipherMetrics(metrics).size == 1 },
                conditionDetails = "Waiting for cipher metrics to be created.",
            )
            assertEquals(5, cipherMetrics(metrics)[0].metricValue())
        }
    }

    /**
     * Validate that we can send and receive a message larger than the receive and send buffer size
     */
    @Test
    @Throws(Exception::class)
    fun testSendLargeRequest() {
        val node = "0"
        blockingConnect(node)
        val big = randomString(10 * BUFFER_SIZE)
        assertEquals(big, blockingRequest(node, big))
    }

    @Test
    @Throws(Exception::class)
    fun testPartialSendAndReceiveReflectedInMetrics() {
        // We use a large payload to attempt to trigger the partial send and receive logic.
        val payloadSize = 20 * BUFFER_SIZE
        val payload = randomString(payloadSize)
        val nodeId = "0"
        blockingConnect(nodeId)
        val send = ByteBufferSend.sizePrefixed(ByteBuffer.wrap(payload.toByteArray()))
        val networkSend = NetworkSend(nodeId, send)
        selector.send(networkSend)
        val channel = selector.channel(nodeId)!!
        val outgoingByteTotal = findUntaggedMetricByName("outgoing-byte-total")
        val incomingByteTotal = findUntaggedMetricByName("incoming-byte-total")
        waitForCondition(
            testCondition = {
                val bytesSent = send.size() - send.remaining
                assertEquals(bytesSent, (outgoingByteTotal.metricValue() as Double).toLong())
                val currentReceive = channel.currentReceive()
                if (currentReceive != null) {
                    assertEquals(currentReceive.bytesRead(), (incomingByteTotal.metricValue() as Double).toInt())
                }
                selector.poll(50)
                !selector.completedReceives().isEmpty()
            },
            conditionDetails = "Failed to receive expected response",
        )
        val requestTotal = findUntaggedMetricByName("request-total")
        assertEquals(1, (requestTotal.metricValue() as Double).toInt())

        val responseTotal = findUntaggedMetricByName("response-total")
        assertEquals(1, (responseTotal.metricValue() as Double).toInt())
    }

    @Test
    @Throws(Exception::class)
    fun testLargeMessageSequence() {
        val bufferSize = 512 * 1024
        val node = "0"
        val reqs = 50
        val addr = InetSocketAddress("localhost", server.port)
        connect(node, addr)
        val requestPrefix = randomString(bufferSize)
        sendAndReceive(
            node = node,
            requestPrefix = requestPrefix,
            startIndex = 0,
            endIndex = reqs,
        )
    }

    @Test
    @Throws(Exception::class)
    fun testEmptyRequest() {
        val node = "0"
        blockingConnect(node)
        assertEquals("", blockingRequest(node, ""))
    }

    @Test
    @Throws(Exception::class)
    fun testClearCompletedSendsAndReceives() {
        val bufferSize = 1024
        val node = "0"
        val addr = InetSocketAddress("localhost", server.port)
        connect(node, addr)
        val request = randomString(bufferSize)
        selector.send(createSend(node, request))
        var sent = false
        var received = false
        while (!sent || !received) {
            selector.poll(1000L)
            assertEquals(0, selector.disconnected().size, "No disconnects should have occurred.")
            if (selector.completedSends().isNotEmpty()) {
                assertEquals(1, selector.completedSends().size)
                selector.clearCompletedSends()
                assertEquals(0, selector.completedSends().size)
                sent = true
            }
            if (!selector.completedReceives().isEmpty()) {
                assertEquals(1, selector.completedReceives().size)
                assertEquals(request, asString(selector.completedReceives().first()))
                selector.clearCompletedReceives()
                assertEquals(0, selector.completedReceives().size)
                received = true
            }
        }
    }

    @Test
    @Throws(IOException::class)
    fun testExistingConnectionId() {
        blockingConnect("0")
        assertFailsWith<IllegalStateException> { blockingConnect("0") }
    }

    @Test
    @Throws(Exception::class)
    fun testMute() {
        blockingConnect("0")
        blockingConnect("1")
        selector.send(createSend("0", "hello"))
        selector.send(createSend("1", "hi"))
        selector.mute("1")
        while (selector.completedReceives().isEmpty()) selector.poll(5)
        assertEquals(
            expected = 1,
            actual = selector.completedReceives().size,
            message = "We should have only one response",
        )
        assertEquals(
            expected = "0",
            actual = selector.completedReceives().first().source(),
            message = "The response should not be from the muted node",
        )
        selector.unmute("1")
        do { selector.poll(5) } while (selector.completedReceives().isEmpty())
        assertEquals(
            expected = 1,
            actual = selector.completedReceives().size,
            message = "We should have only one response",
        )
        assertEquals(
            expected = "1",
            actual = selector.completedReceives().first().source(),
            message = "The response should be from the previously muted node",
        )
    }

    @Test
    @Throws(Exception::class)
    fun testCloseAllChannels() {
        val closedChannelsCount = AtomicInteger(0)
        val channelBuilder = object : PlaintextChannelBuilder(null) {

            private var channelIndex = 0

            override fun buildChannel(
                id: String,
                transportLayer: TransportLayer,
                authenticatorCreator: Supplier<Authenticator>,
                maxReceiveSize: Int,
                memoryPool: MemoryPool,
                metadataRegistry: ChannelMetadataRegistry?,
            ): KafkaChannel {
                return object : KafkaChannel(
                    id = id,
                    transportLayer = transportLayer,
                    authenticatorCreator = authenticatorCreator,
                    maxReceiveSize = maxReceiveSize,
                    memoryPool = memoryPool,
                    metadataRegistry = metadataRegistry,
                ) {
                    private val index = channelIndex++

                    @Throws(IOException::class)
                    override fun close() {
                        closedChannelsCount.getAndIncrement()
                        if (index == 0) throw RuntimeException("you should fail") else super.close()
                    }
                }
            }
        }
        channelBuilder.configure(clientConfigs())
        val selector = Selector(
            connectionMaxIdleMs = CONNECTION_MAX_IDLE_MS,
            metrics = Metrics(),
            time = MockTime(),
            metricGrpPrefix = "MetricGroup",
            channelBuilder = channelBuilder,
            logContext = LogContext(),
        )
        selector.connect("0", InetSocketAddress("localhost", server.port), BUFFER_SIZE, BUFFER_SIZE)
        selector.connect("1", InetSocketAddress("localhost", server.port), BUFFER_SIZE, BUFFER_SIZE)
        assertFailsWith<RuntimeException> { selector.close() }
        assertEquals(2, closedChannelsCount.get())
    }

    @Test
    @Throws(Exception::class)
    fun registerFailure() {
        val channelBuilder = object : PlaintextChannelBuilder(null) {

            @Throws(KafkaException::class)
            override fun buildChannel(
                id: String,
                key: SelectionKey,
                maxReceiveSize: Int,
                memoryPool: MemoryPool?,
                metadataRegistry: ChannelMetadataRegistry,
            ): KafkaChannel = throw RuntimeException("Test exception")
        }
        val selector = Selector(
            connectionMaxIdleMs = CONNECTION_MAX_IDLE_MS,
            metrics = Metrics(),
            time = MockTime(),
            metricGrpPrefix = "MetricGroup",
            channelBuilder = channelBuilder,
            logContext = LogContext(),
        )
        val socketChannel = SocketChannel.open()
        socketChannel.configureBlocking(false)

        val e = assertFailsWith<IOException> { selector.register("1", socketChannel) }
        assertTrue(e.cause!!.message!!.contains("Test exception"), "Unexpected exception: $e")
        assertFalse(socketChannel.isOpen, "Socket not closed")
        selector.close()
    }

    @Test
    @Throws(Exception::class)
    fun testCloseOldestConnection() {
        val id = "0"
        selector.connect(
            id = id,
            address = InetSocketAddress("localhost", server.port),
            sendBufferSize = BUFFER_SIZE,
            receiveBufferSize = BUFFER_SIZE,
        )
        waitForChannelConnected(selector, id)
        time.sleep(CONNECTION_MAX_IDLE_MS + 1000)
        selector.poll(0)
        assertTrue(selector.disconnected().containsKey(id), "The idle connection should have been closed")
        assertEquals(ChannelState.EXPIRED, selector.disconnected()[id])
    }

    @Test
    @Throws(IOException::class)
    fun testIdleExpiryWithoutReadyKeys() {
        val id = "0"
        selector.connect(
            id = id,
            address = InetSocketAddress("localhost", server.port),
            sendBufferSize = BUFFER_SIZE,
            receiveBufferSize = BUFFER_SIZE,
        )
        val channel = selector.channel(id)
        channel!!.selectionKey().interestOps(0)
        time.sleep(CONNECTION_MAX_IDLE_MS + 1000)
        selector.poll(0)
        assertTrue(selector.disconnected().containsKey(id), "The idle connection should have been closed")
        assertEquals(ChannelState.EXPIRED, selector.disconnected()[id])
    }

    @Test
    @Throws(Exception::class)
    fun testImmediatelyConnectedCleaned() {
        val metrics = Metrics() // new metrics object to avoid metric registration conflicts
        val selector = ImmediatelyConnectingSelector(
            connectionMaxIdleMS = CONNECTION_MAX_IDLE_MS,
            metrics = metrics,
            time = time,
            metricGrpPrefix = "MetricGroup",
            channelBuilder = channelBuilder,
            logContext = LogContext(),
        )
        try {
            testImmediatelyConnectedCleaned(selector, true)
            testImmediatelyConnectedCleaned(selector, false)
        } finally {
            selector.close()
            metrics.close()
        }
    }

    private open class ImmediatelyConnectingSelector(
        connectionMaxIdleMS: Long,
        metrics: Metrics,
        time: Time,
        metricGrpPrefix: String,
        channelBuilder: ChannelBuilder,
        logContext: LogContext,
    ) : Selector(
        connectionMaxIdleMs = connectionMaxIdleMS,
        metrics = metrics,
        time = time,
        metricGrpPrefix = metricGrpPrefix,
        channelBuilder = channelBuilder,
        logContext = logContext,
    ) {
        @Throws(IOException::class)
        override fun doConnect(channel: SocketChannel, address: InetSocketAddress?): Boolean {
            // Use a blocking connect to trigger the immediately connected path
            channel.configureBlocking(true)
            val connected = super.doConnect(channel, address)
            channel.configureBlocking(false)

            return connected
        }
    }

    @Throws(Exception::class)
    private fun testImmediatelyConnectedCleaned(selector: Selector, closeAfterFirstPoll: Boolean) {
        val id = "0"
        selector.connect(
            id = id,
            address = InetSocketAddress("localhost", server.port),
            sendBufferSize = BUFFER_SIZE,
            receiveBufferSize = BUFFER_SIZE,
        )
        verifyNonEmptyImmediatelyConnectedKeys(selector)
        if (closeAfterFirstPoll) {
            selector.poll(0)
            verifyEmptyImmediatelyConnectedKeys(selector)
        }
        selector.close(id)
        verifySelectorEmpty(selector)
    }

    /**
     * Verify that if Selector#connect fails and throws an Exception, all related objects
     * are cleared immediately before the exception is propagated.
     */
    @Test
    @Throws(Exception::class)
    fun testConnectException() {
        val metrics = Metrics()
        val throwIOException = AtomicBoolean()
        val selector = object : ImmediatelyConnectingSelector(
            connectionMaxIdleMS = CONNECTION_MAX_IDLE_MS,
            metrics = metrics,
            time = time,
            metricGrpPrefix = "MetricGroup",
            channelBuilder = channelBuilder,
            logContext = LogContext(),
        ) {

            @Throws(IOException::class)
            override fun registerChannel(
                id: String,
                socketChannel: SocketChannel,
                interestedOps: Int,
            ): SelectionKey {
                val key: SelectionKey = super.registerChannel(id, socketChannel, interestedOps)
                key.cancel()
                if (throwIOException.get()) throw IOException("Test exception")
                return key
            }
        }
        try {
            verifyImmediatelyConnectedException(selector, "0")
            throwIOException.set(true)
            verifyImmediatelyConnectedException(selector, "1")
        } finally {
            selector.close()
            metrics.close()
        }
    }

    @Throws(Exception::class)
    private fun verifyImmediatelyConnectedException(selector: Selector, id: String) {
        assertFailsWith<Exception>("Expected exception not thrown") {
            selector.connect(
                id = id,
                address = InetSocketAddress("localhost", server.port),
                sendBufferSize = BUFFER_SIZE,
                receiveBufferSize = BUFFER_SIZE,
            )
        }
        verifyEmptyImmediatelyConnectedKeys(selector)
        assertNull(selector.channel(id), "Channel not removed")
        ensureEmptySelectorFields(selector)
    }

    /*
     * Verifies that a muted connection is expired on idle timeout even if there are pending
     * receives on the socket.
     */
    @Test
    @Throws(Exception::class)
    fun testExpireConnectionWithPendingReceives() {
        val channel = createConnectionWithPendingReceives(5)
        verifyChannelExpiry(channel)
    }

    /**
     * Verifies that a muted connection closed by peer is expired on idle timeout even if there are pending
     * receives on the socket.
     */
    @Test
    @Throws(Exception::class)
    fun testExpireClosedConnectionWithPendingReceives() {
        val channel = createConnectionWithPendingReceives(5)
        server.closeConnections()
        verifyChannelExpiry(channel)
    }

    @Throws(Exception::class)
    private fun verifyChannelExpiry(channel: KafkaChannel?) {
        val id = channel!!.id
        selector.mute(id) // Mute to allow channel to be expired even if more data is available for read
        time.sleep(CONNECTION_MAX_IDLE_MS + 1000)
        selector.poll(0)
        assertNull(selector.channel(id), "Channel not expired")
        assertNull(selector.closingChannel(id), "Channel not removed from closingChannels")
        assertEquals(ChannelState.EXPIRED, channel.state)
        assertNull(channel.selectionKey().attachment())
        assertTrue(selector.disconnected().containsKey(id), "Disconnect not notified")
        assertEquals(ChannelState.EXPIRED, selector.disconnected()[id])
        verifySelectorEmpty()
    }

    /**
     * Verifies that sockets with incoming data available are not expired.
     * For PLAINTEXT, pending receives are always read from socket without any buffering, so this
     * test is only verifying that channels are not expired while there is data to read from socket.
     * For SSL, pending receives may also be in SSL netReadBuffer or appReadBuffer. So the test verifies
     * that connection is not expired when data is available from buffers or network.
     */
    @Test
    @Throws(Exception::class)
    fun testCloseOldestConnectionWithMultiplePendingReceives() {
        val expectedReceives = 5
        val channel = createConnectionWithPendingReceives(expectedReceives)!!
        var completedReceives = selector.completedReceives().size
        while (selector.disconnected().isEmpty()) {
            time.sleep(CONNECTION_MAX_IDLE_MS + 1000)
            selector.poll((if (completedReceives == expectedReceives) 0 else 1000))
            completedReceives += selector.completedReceives().size
        }
        assertEquals(expectedReceives, completedReceives)
        assertNull(selector.channel(channel.id), "Channel not expired")
        assertNull(selector.closingChannel(channel.id), "Channel not expired")
        assertTrue(selector.disconnected().containsKey(channel.id), "Disconnect not notified")
        assertTrue(selector.completedReceives().isEmpty(), "Unexpected receive")
    }

    /**
     * Tests that graceful close of channel processes remaining data from socket read buffers.
     * Since we cannot determine how much data is available in the buffers, this test verifies that
     * multiple receives are completed after server shuts down connections, with retries to tolerate
     * cases where data may not be available in the socket buffer.
     */
    @Test
    @Throws(Exception::class)
    fun testGracefulClose() {
        var maxReceiveCountAfterClose = 0
        var i = 6
        while (i <= 100 && maxReceiveCountAfterClose < 5) {
            var receiveCount = 0
            val channel = createConnectionWithPendingReceives(i)
            // Poll until one or more receives complete and then close the server-side connection
            waitForCondition(
                testCondition = {
                    selector.poll(1000)
                    selector.completedReceives().isNotEmpty()
                },
                maxWaitMs = 5000,
                conditionDetails = "Receive not completed",
            )
            server.closeConnections()
            while (selector.disconnected().isEmpty()) {
                selector.poll(1)
                receiveCount += selector.completedReceives().size
                assertTrue(
                    actual = selector.completedReceives().size <= 1,
                    message = "Too many completed receives in one poll",
                )
            }
            assertEquals(channel!!.id, selector.disconnected().keys.first())
            maxReceiveCountAfterClose = max(maxReceiveCountAfterClose.toDouble(), receiveCount.toDouble()).toInt()
            i++
        }
        assertTrue(
            actual = maxReceiveCountAfterClose >= 5,
            message = "Too few receives after close: $maxReceiveCountAfterClose",
        )
    }

    /**
     * Tests that graceful close is not delayed if only part of an incoming receive is
     * available in the socket buffer.
     */
    @Test
    @Throws(Exception::class)
    fun testPartialReceiveGracefulClose() {
        val id = "0"
        blockingConnect(id)
        val channel = selector.channel(id)!!
        // Inject a NetworkReceive into Kafka channel with a large size
        injectNetworkReceive(channel, 100000)
        sendNoReceive(channel, 2) // Send some data that gets received as part of injected receive
        selector.poll(1000) // Wait until some data arrives, but not a completed receive
        assertEquals(0, selector.completedReceives().size)
        server.closeConnections()
        waitForCondition(
            testCondition = {
                try {
                    selector.poll(100)
                    return@waitForCondition selector.disconnected().isNotEmpty()
                } catch (e: IOException) {
                    throw RuntimeException(e)
                }
            },
            maxWaitMs = 10000,
            conditionDetails = "Channel not disconnected",
        )
        assertEquals(1, selector.disconnected().size)
        assertEquals(channel.id, selector.disconnected().keys.first())
        assertEquals(0, selector.completedReceives().size)
    }

    @Test
    @Throws(Exception::class)
    open fun testMuteOnOOM() {
        //clean up default selector, replace it with one that uses a finite mem pool
        selector.close()
        val pool = SimpleMemoryPool(
            sizeInBytes = 900,
            maxSingleAllocationBytes = 900,
            strict = false,
            oomPeriodSensor = null,
        )
        selector = Selector(
            maxReceiveSize = NetworkReceive.UNLIMITED,
            connectionMaxIdleMs = CONNECTION_MAX_IDLE_MS,
            metrics = metrics,
            time = time,
            metricGrpPrefix = "MetricGroup",
            metricTags = mutableMapOf(),
            metricsPerConnection = true,
            recordTimePerConnection = false,
            channelBuilder = channelBuilder,
            memoryPool = pool,
            logContext = LogContext(),
        )
        ServerSocketChannel.open().use { ss ->
            ss.bind(InetSocketAddress(0))
            val serverAddress = ss.localAddress as InetSocketAddress
            val sender1 = createSender(serverAddress, randomPayload(900))
            val sender2 = createSender(serverAddress, randomPayload(900))
            sender1.start()
            sender2.start()

            //wait until everything has been flushed out to network (assuming payload size is smaller than OS buffer size)
            //this is important because we assume both requests' prefixes (1st 4 bytes) have made it.
            sender1.join(5000)
            sender2.join(5000)
            val channelX = ss.accept() //not defined if its 1 or 2
            channelX.configureBlocking(false)
            val channelY = ss.accept()
            channelY.configureBlocking(false)
            selector.register("clientX", channelX)
            selector.register("clientY", channelY)

            var completed: Collection<NetworkReceive> = emptyList()
            var deadline = System.currentTimeMillis() + 5000
            while (System.currentTimeMillis() < deadline && completed.isEmpty()) {
                selector.poll(1000)
                completed = selector.completedReceives()
            }
            assertEquals(1, completed.size, "could not read a single request within timeout")
            val firstReceive = completed.first()
            assertEquals(0, pool.availableMemory())
            assertTrue(selector.isOutOfMemory)
            selector.poll(10)
            assertTrue(selector.completedReceives().isEmpty())
            assertEquals(0, pool.availableMemory())
            assertTrue(selector.isOutOfMemory)
            firstReceive.close()
            assertEquals(900, pool.availableMemory()) //memory has been released back to pool
            completed = emptyList()
            deadline = System.currentTimeMillis() + 5000
            while (System.currentTimeMillis() < deadline && completed.isEmpty()) {
                selector.poll(1000)
                completed = selector.completedReceives()
            }
            assertEquals(
                expected = 1,
                actual = selector.completedReceives().size,
                message = "could not read a single request within timeout",
            )
            assertEquals(0, pool.availableMemory())
            assertFalse(selector.isOutOfMemory)
        }
    }

    private fun createSender(serverAddress: InetSocketAddress, payload: ByteArray): Thread =
        PlaintextSender(serverAddress, payload)

    @Throws(Exception::class)
    protected fun randomPayload(sizeBytes: Int): ByteArray {
        val payload = ByteArray(sizeBytes + 4)
        Random.nextBytes(payload)

        val prefixOs = ByteArrayOutputStream()
        val prefixDos = DataOutputStream(prefixOs)
        prefixDos.writeInt(sizeBytes)

        prefixDos.flush()
        prefixDos.close()
        prefixOs.flush()
        prefixOs.close()

        val prefix = prefixOs.toByteArray()
        System.arraycopy(prefix, 0, payload, 0, prefix.size)
        return payload
    }

    /**
     * Tests that a connect and disconnect in a single poll invocation results in the channel id being
     * in `disconnected`, but not `connected`.
     */
    @Test
    @Throws(Exception::class)
    fun testConnectDisconnectDuringInSinglePoll() {
        // channel is connected, not ready and it throws an exception during prepare
        val kafkaChannel = mock<KafkaChannel>()
        `when`(kafkaChannel.id).thenReturn("1")
        `when`(kafkaChannel.socketDescription()).thenReturn("")
        `when`(kafkaChannel.state).thenReturn(ChannelState.NOT_CONNECTED)
        `when`(kafkaChannel.finishConnect()).thenReturn(true)
        `when`(kafkaChannel.isConnected).thenReturn(true)
        `when`(kafkaChannel.ready()).thenReturn(false)
        doThrow(IOException()).`when`(kafkaChannel).prepare()
        val selectionKey = mock<SelectionKey>()
        `when`(kafkaChannel.selectionKey()).thenReturn(selectionKey)
        `when`(selectionKey.channel()).thenReturn(SocketChannel.open())
        `when`(selectionKey.readyOps()).thenReturn(SelectionKey.OP_CONNECT)
        `when`(selectionKey.attachment()).thenReturn(kafkaChannel)
        val selectionKeys = setOf(selectionKey)
        selector.pollSelectionKeys(selectionKeys, false, System.nanoTime())
        assertFalse(selector.connected().contains(kafkaChannel.id))
        assertTrue(selector.disconnected().containsKey(kafkaChannel.id))
        verify(kafkaChannel, atLeastOnce()).ready()
        verify(kafkaChannel).disconnect()
        verify(kafkaChannel).close()
        verify(selectionKey).cancel()
    }

    @Test
    @Throws(Exception::class)
    fun testOutboundConnectionsCountInConnectionCreationMetric() {
        // create connections
        val expectedConnections = 5
        val addr = InetSocketAddress("localhost", server.port)
        for (i in 0 until expectedConnections) connect(i.toString(), addr)

        // Poll continuously, as we cannot guarantee that the first call will see all connections
        var seenConnections = 0
        for (i in 0..9) {
            selector.poll(100L)
            seenConnections += selector.connected().size
            if (seenConnections == expectedConnections) break
        }
        assertEquals(expectedConnections.toDouble(), getMetric("connection-creation-total").metricValue())
        assertEquals(expectedConnections.toDouble(), getMetric("connection-count").metricValue())
    }

    @Test
    @Throws(Exception::class)
    fun testInboundConnectionsCountInConnectionCreationMetric() {
        val conns = 5
        ServerSocketChannel.open().use { ss ->
            ss.bind(InetSocketAddress(0))
            val serverAddress = ss.localAddress as InetSocketAddress
            for (i in 0 until conns) {
                val sender = createSender(serverAddress, randomPayload(1))
                sender.start()
                val channel = ss.accept()
                channel.configureBlocking(false)
                selector.register(i.toString(), channel)
            }
        }
        assertEquals(conns.toDouble(), getMetric("connection-creation-total").metricValue())
        assertEquals(conns.toDouble(), getMetric("connection-count").metricValue())
    }

    @Test
    @Throws(Exception::class)
    fun testConnectionsByClientMetric() {
        val node = "0"
        val unknownNameAndVersion = softwareNameAndVersionTags(
            clientSoftwareName = ClientInformation.UNKNOWN_NAME_OR_VERSION,
            clientSoftwareVersion = ClientInformation.UNKNOWN_NAME_OR_VERSION,
        )
        val knownNameAndVersion = softwareNameAndVersionTags(
            clientSoftwareName = "A",
            clientSoftwareVersion = "B",
        )
        ServerSocketChannel.open().use { ss ->
            ss.bind(InetSocketAddress(0))
            val serverAddress = ss.localAddress as InetSocketAddress
            val sender = createSender(serverAddress, randomPayload(1))
            sender.start()
            val channel = ss.accept()
            channel.configureBlocking(false)

            // Metric with unknown / unknown should be there
            selector.register(node, channel)
            assertEquals(1, getMetric("connections", unknownNameAndVersion).metricValue())
            assertEquals(
                expected = ClientInformation.EMPTY,
                actual = selector.channel(node)!!.channelMetadataRegistry()!!.clientInformation(),
            )

            // Metric with unknown / unknown should not be there, metric with A / B should be there
            val clientInformation = ClientInformation("A", "B")
            selector.channel(node)!!.channelMetadataRegistry()!!.registerClientInformation(clientInformation)
            assertEquals(
                expected = clientInformation,
                actual = selector.channel(node)!!.channelMetadataRegistry()!!.clientInformation(),
            )
            assertEquals(0, getMetric("connections", unknownNameAndVersion).metricValue())
            assertEquals(1, getMetric("connections", knownNameAndVersion).metricValue())

            // Metric with A / B should not be there,
            selector.close(node)
            assertEquals(0, getMetric("connections", knownNameAndVersion).metricValue())
        }
    }

    private fun softwareNameAndVersionTags(
        clientSoftwareName: String,
        clientSoftwareVersion: String,
    ): Map<String, String> {
        return mapOf(
            "clientSoftwareName" to clientSoftwareName,
            "clientSoftwareVersion" to clientSoftwareVersion,
        )
    }

    @Throws(Exception::class)
    private fun getMetric(name: String, tags: Map<String, String>): KafkaMetric {
        val metric = metrics.metrics.entries.firstOrNull { (key) ->
            key.name == name && key.tags == tags
        } ?: throw Exception("Could not find metric called $name with tags $tags")
        return metric.value
    }

    @Test
    @Throws(Exception::class)
    fun testLowestPriorityChannel() {
        val conns = 5
        val addr = InetSocketAddress("localhost", server.port)
        for (i in 0 until conns) connect(i.toString(), addr)
        assertNotNull(selector.lowestPriorityChannel())
        for (i in conns - 1 downTo 0) {
            if (i != 2) assertEquals("", blockingRequest(i.toString(), ""))
            time.sleep(10)
        }
        assertEquals("2", selector.lowestPriorityChannel()!!.id)
        val field = Selector::class.java.getDeclaredField("closingChannels")
        field.setAccessible(true)
        val closingChannels = assertIs<MutableMap<String, KafkaChannel?>>(field[selector])
        closingChannels["3"] = selector.channel("3")
        assertEquals("3", selector.lowestPriorityChannel()!!.id)
        closingChannels.remove("3")
        for (i in 0 until conns) selector.close(i.toString())
        assertNull(selector.lowestPriorityChannel())
    }

    @Test
    @Throws(Exception::class)
    fun testMetricsCleanupOnSelectorClose() {
        val metrics = Metrics()
        val selector = object : ImmediatelyConnectingSelector(
            connectionMaxIdleMS = CONNECTION_MAX_IDLE_MS,
            metrics = metrics,
            time = time,
            metricGrpPrefix = "MetricGroup",
            channelBuilder = channelBuilder,
            logContext = LogContext(),
        ) {
            override fun close(id: String) = throw RuntimeException()
        }
        assertTrue(metrics.metrics.size > 1)
        val id = "0"
        selector.connect(
            id = id,
            address = InetSocketAddress("localhost", server.port),
            sendBufferSize = BUFFER_SIZE,
            receiveBufferSize = BUFFER_SIZE,
        )

        // Close the selector and ensure a RuntimeException has been throw
        assertFailsWith<RuntimeException> { selector.close() }

        // We should only have one remaining metric for kafka-metrics-count, which is a global metric
        assertEquals(1, metrics.metrics.size)
    }

    @Test
    @Throws(IOException::class)
    fun testWriteCompletesSendWithNoBytesWritten() {
        val channel = mock(KafkaChannel::class.java)
        `when`(channel.id).thenReturn("1")
        `when`(channel.write()).thenReturn(0L)
        val send = NetworkSend(
            destinationId = "destination",
            send = ByteBufferSend(ByteBuffer.allocate(0)),
        )
        `when`(channel.maybeCompleteSend()).thenReturn(send)
        selector.write(channel)
        assertEquals(listOf(send), selector.completedSends())
    }

    /**
     * Ensure that no errors are thrown if channels are closed while processing multiple completed receives
     */
    @Test
    @Throws(Exception::class)
    fun testChannelCloseWhileProcessingReceives() {
        val numChannels = 4
        val channels = fieldValue<MutableMap<String, KafkaChannel>>(
            o = selector,
            clazz = Selector::class.java,
            fieldName = "channels",
        )
        val selectionKeys = mutableSetOf<SelectionKey>()
        for (i in 0 until numChannels) {
            val id = i.toString()

            val channel = mock<KafkaChannel>()
            channels[id] = channel
            `when`(channel.id).thenReturn(id)
            `when`(channel.state).thenReturn(ChannelState.READY)
            `when`(channel.isConnected).thenReturn(true)
            `when`(channel.ready()).thenReturn(true)
            `when`(channel.read()).thenReturn(1L)

            val selectionKey = mock<SelectionKey>()
            `when`(channel.selectionKey()).thenReturn(selectionKey)
            `when`(selectionKey.isValid).thenReturn(true)
            `when`(selectionKey.isReadable).thenReturn(true)
            `when`(selectionKey.readyOps()).thenReturn(SelectionKey.OP_READ)
            `when`(selectionKey.attachment())
                .thenReturn(channel)
                .thenReturn(null)
            selectionKeys.add(selectionKey)

            val receive = mock<NetworkReceive>()
            `when`(receive.source()).thenReturn(id)
            `when`(receive.size()).thenReturn(10)
            `when`(receive.bytesRead()).thenReturn(1)
            `when`(receive.payload()).thenReturn(ByteBuffer.allocate(10))
            `when`(channel.maybeCompleteReceive()).thenReturn(receive)
        }
        selector.pollSelectionKeys(
            selectionKeys = selectionKeys,
            isImmediatelyConnected = false,
            currentTimeNanos = System.nanoTime()
        )
        assertEquals(numChannels, selector.completedReceives().size)
        val closed = mutableSetOf<KafkaChannel>()
        val notClosed = mutableSetOf<KafkaChannel>()

        for (receive in selector.completedReceives()) {
            val channel = selector.channel(receive.source())
            assertNotNull(channel)
            if (closed.size < 2) {
                selector.close(channel.id)
                closed.add(channel)
            } else notClosed.add(channel)
        }
        assertEquals(notClosed, selector.channels().toSet())
        closed.forEach { channel -> assertNull(selector.channel(channel.id)) }
        selector.poll(0)
        assertEquals(0, selector.completedReceives().size)
    }

    @Throws(IOException::class)
    private fun blockingRequest(node: String, s: String): String {
        selector.send(createSend(node, s))
        while (true) {
            selector.poll(1000L)
            for (receive in selector.completedReceives())
                if (receive.source() == node) return asString(receive)
        }
    }

    @Throws(IOException::class)
    protected open fun connect(node: String, serverAddr: InetSocketAddress) = selector.connect(
        id = node,
        address = serverAddr,
        sendBufferSize = BUFFER_SIZE,
        receiveBufferSize = BUFFER_SIZE
    )

    /* connect and wait for the connection to complete */
    @Throws(IOException::class)
    private fun blockingConnect(node: String) = blockingConnect(
        node = node,
        serverAddr = InetSocketAddress("localhost", server.port),
    )

    @Throws(IOException::class)
    protected fun blockingConnect(node: String, serverAddr: InetSocketAddress) {
        selector.connect(
            id = node,
            address = serverAddr,
            sendBufferSize = BUFFER_SIZE,
            receiveBufferSize = BUFFER_SIZE,
        )
        waitForChannelReady(selector, node)
    }

    protected fun createSend(node: String, payload: String): NetworkSend = NetworkSend(
        destinationId = node,
        send = ByteBufferSend.sizePrefixed(ByteBuffer.wrap(payload.toByteArray())),
    )

    protected fun asString(receive: NetworkReceive): String = String(Utils.toArray(receive.payload()!!))

    @Throws(Exception::class)
    private fun sendAndReceive(node: String, requestPrefix: String, startIndex: Int, endIndex: Int) {
        var requests = startIndex
        var responses = startIndex
        selector.send(createSend(node, "$requestPrefix-$startIndex"))
        requests++
        while (responses < endIndex) {
            // do the i/o
            selector.poll(0L)
            assertEquals(0, selector.disconnected().size, "No disconnects should have occurred.")
            // handle requests and responses of the fast node
            for (receive in selector.completedReceives()) {
                assertEquals("$requestPrefix-$responses", asString(receive))
                responses++
            }
            var i = 0
            while (i < selector.completedSends().size && requests < endIndex) {
                selector.send(createSend(node, "$requestPrefix-$requests"))
                i++
                requests++
            }
        }
    }

    @Throws(Exception::class)
    private fun verifyNonEmptyImmediatelyConnectedKeys(selector: Selector) {
        val field = Selector::class.java.getDeclaredField("immediatelyConnectedKeys")
        field.setAccessible(true)
        val immediatelyConnectedKeys = field[selector] as Collection<*>
        assertFalse(immediatelyConnectedKeys.isEmpty())
    }

    @Throws(Exception::class)
    private fun verifyEmptyImmediatelyConnectedKeys(selector: Selector) {
        val field = Selector::class.java.getDeclaredField("immediatelyConnectedKeys")
        ensureEmptySelectorField(selector, field)
    }

    @Throws(Exception::class)
    protected fun verifySelectorEmpty() {
        verifySelectorEmpty(selector)
    }

    @Throws(Exception::class)
    fun verifySelectorEmpty(selector: Selector) {
        for (channel in selector.channels()) {
            selector.close(channel.id)
            assertNull(channel.selectionKey().attachment())
        }
        selector.poll(0)
        selector.poll(0) // Poll a second time to clear everything
        ensureEmptySelectorFields(selector)
    }

    @Throws(Exception::class)
    private fun ensureEmptySelectorFields(selector: Selector?) {
        for (field in Selector::class.java.getDeclaredFields()) {
            ensureEmptySelectorField(selector, field)
        }
    }

    @Throws(Exception::class)
    private fun ensureEmptySelectorField(selector: Selector?, field: Field) {
        field.setAccessible(true)
        val obj = field[selector]
        if (obj is Collection<*>) assertTrue(obj.isEmpty(), "Field not empty: $field $obj")
        else if (obj is Map<*, *>) assertTrue(obj.isEmpty(), "Field not empty: $field $obj")
    }

    @Throws(Exception::class)
    private fun getMetric(name: String): KafkaMetric {
        val metric = metrics.metrics.entries
            .firstOrNull { (key) -> key.name == name }
            ?: throw Exception(String.format("Could not find metric called %s", name))

        return metric.value
    }

    private fun findUntaggedMetricByName(name: String): KafkaMetric {
        val metricName = MetricName(
            name = name,
            group = "$METRIC_GROUP-metrics",
            description = "",
            tags = emptyMap(),
        )
        val metric = metrics.metrics[metricName]
        assertNotNull(metric)
        return metric
    }

    /**
     * Creates a connection, sends the specified number of requests and returns without reading
     * any incoming data. Some of the incoming data may be in the socket buffers when this method
     * returns, but there is no guarantee that all the data from the server will be available
     * immediately.
     */
    @Throws(Exception::class)
    private fun createConnectionWithPendingReceives(pendingReceives: Int): KafkaChannel? {
        val id = "0"
        blockingConnect(id)
        val channel = selector.channel(id)!!
        sendNoReceive(channel, pendingReceives)
        return channel
    }

    /**
     * Sends the specified number of requests and waits for the requests to be sent.
     * The channel is muted during polling to ensure that incoming data is not received.
     */
    @Throws(Exception::class)
    private fun sendNoReceive(channel: KafkaChannel, numRequests: Int) {
        selector.mute(channel.id)
        for (i in 0 until numRequests) {
            selector.send(createSend(channel.id, i.toString()))
            do { selector.poll(10) } while (selector.completedSends().isEmpty())
        }
        selector.unmute(channel.id)
    }

    /**
     * Injects a NetworkReceive for channel with size buffer filled in with the provided size
     * and a payload buffer allocated with that size, but no data in the payload buffer.
     */
    @Throws(Exception::class)
    private fun injectNetworkReceive(channel: KafkaChannel, size: Int) {
        val receive = NetworkReceive()
        setFieldValue(channel, "receive", receive)
        val sizeBuffer = fieldValue<ByteBuffer>(receive, NetworkReceive::class.java, "size")
        sizeBuffer.putInt(size)
        setFieldValue(receive, "buffer", ByteBuffer.allocate(size))
    }

    companion object {

        internal const val BUFFER_SIZE = 4 * 1024

        private const val METRIC_GROUP = "MetricGroup"

        private const val CONNECTION_MAX_IDLE_MS: Long = 5000

        fun cipherMetrics(metrics: Metrics): List<KafkaMetric> {
            return metrics.metrics.entries.filter { (key) ->
                key.description.contains("The number of connections with this SSL cipher and protocol.")
            }.map { (_, value) -> value }
        }
    }
}
