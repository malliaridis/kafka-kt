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

import java.io.File
import java.io.IOException
import java.net.InetSocketAddress
import java.nio.channels.SelectionKey
import java.nio.channels.ServerSocketChannel
import java.nio.channels.SocketChannel
import java.security.GeneralSecurityException
import java.security.Security
import java.util.concurrent.atomic.AtomicInteger
import javax.net.ssl.SSLEngine
import org.apache.kafka.common.config.SecurityConfig
import org.apache.kafka.common.memory.SimpleMemoryPool
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.network.NetworkTestUtils.waitForChannelReady
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.security.ssl.SslFactory
import org.apache.kafka.common.security.ssl.mock.TestKeyManagerFactory
import org.apache.kafka.common.security.ssl.mock.TestProviderCreator
import org.apache.kafka.common.security.ssl.mock.TestTrustManagerFactory
import org.apache.kafka.common.utils.LogContext
import org.apache.kafka.common.utils.MockTime
import org.apache.kafka.common.utils.Time
import org.apache.kafka.test.TestSslUtils
import org.apache.kafka.test.TestSslUtils.SslConfigsBuilder
import org.apache.kafka.test.TestSslUtils.createSslConfig
import org.apache.kafka.test.TestUtils.randomString
import org.apache.kafka.test.TestUtils.tempFile
import org.apache.kafka.test.TestUtils.waitForCondition
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

/**
 * A set of tests for the selector. These use a test harness that runs a simple socket server that echos back responses.
 */
abstract class SslSelectorTest : SelectorTest() {
    
    private lateinit var sslClientConfigs: Map<String, Any?>

    @BeforeEach
    @Throws(Exception::class)
    override fun setUp() {
        val trustStoreFile = tempFile(prefix = "truststore", suffix = ".jks")
        val sslServerConfigs = createSslConfig(
            useClientCert = false,
            trustStore = true,
            mode = Mode.SERVER,
            trustStoreFile = trustStoreFile,
            certAlias = "server",
        )
        server = EchoServer(securityProtocol = SecurityProtocol.SSL, configs = sslServerConfigs)
        server.start()
        time = MockTime()
        sslClientConfigs = createSslClientConfigs(trustStoreFile)
        val logContext = LogContext()
        channelBuilder = SslChannelBuilder(
            mode = Mode.CLIENT,
            listenerName = null,
            isInterBrokerListener = false,
            logContext = logContext,
        )
        channelBuilder.configure(sslClientConfigs)
        metrics = Metrics()
        selector = Selector(
            connectionMaxIdleMs = 5000,
            metrics = metrics,
            time = time,
            metricGrpPrefix = "MetricGroup",
            channelBuilder = channelBuilder,
            logContext = logContext,
        )
    }

    @Throws(GeneralSecurityException::class, IOException::class)
    protected abstract fun createSslClientConfigs(trustStoreFile: File?): Map<String, Any?>

    @AfterEach
    @Throws(Exception::class)
    override fun tearDown() {
        selector.close()
        server.close()
        metrics.close()
    }

    override fun clientConfigs(): Map<String, Any?> = sslClientConfigs

    @Test
    @Throws(Exception::class)
    fun testConnectionWithCustomKeyManager() {
        val testProviderCreator = TestProviderCreator()
        val requestSize = 100 * 1024
        val node = "0"
        val request = randomString(requestSize)
        val sslServerConfigs = createSslConfig(
            keyManagerAlgorithm = TestKeyManagerFactory.ALGORITHM,
            trustManagerAlgorithm = TestTrustManagerFactory.ALGORITHM,
            tlsProtocol = TestSslUtils.DEFAULT_TLS_PROTOCOL_FOR_TESTS,
        ).toMutableMap()
        sslServerConfigs[SecurityConfig.SECURITY_PROVIDERS_CONFIG] = testProviderCreator.javaClass.getName()
        val server = EchoServer(securityProtocol = SecurityProtocol.SSL, configs = sslServerConfigs)
        server.start()
        val time: Time = MockTime()
        val trustStoreFile = File(TestKeyManagerFactory.TestKeyManager.mockTrustStoreFile)
        val sslClientConfigs = createSslConfig(
            useClientCert = true,
            trustStore = true,
            mode = Mode.CLIENT,
            trustStoreFile = trustStoreFile,
            certAlias = "client",
        )
        val channelBuilder = TestSslChannelBuilder(Mode.CLIENT)
        channelBuilder.configure(sslClientConfigs)
        val metrics = Metrics()
        val selector = Selector(
            connectionMaxIdleMs = 5000,
            metrics = metrics,
            time = time,
            metricGrpPrefix = "MetricGroup",
            channelBuilder = channelBuilder,
            logContext = LogContext()
        )
        selector.connect(
            id = node,
            address = InetSocketAddress("localhost", server.port),
            sendBufferSize = BUFFER_SIZE,
            receiveBufferSize = BUFFER_SIZE,
        )
        waitForChannelReady(selector, node)
        selector.send(createSend(node, request))
        waitForBytesBuffered(selector, node)
        waitForCondition(
            testCondition = { cipherMetrics(metrics).size == 1 },
            conditionDetails = "Waiting for cipher metrics to be created.",
        )
        assertEquals(1, cipherMetrics(metrics)[0].metricValue())
        assertNotNull(selector.channel(node)!!.channelMetadataRegistry()!!.cipherInformation())

        selector.close(node)
        super.verifySelectorEmpty(selector)

        assertEquals(1, cipherMetrics(metrics).size)
        assertEquals(0, cipherMetrics(metrics)[0].metricValue())
        Security.removeProvider(testProviderCreator.provider.name)

        selector.close()
        server.close()
        metrics.close()
    }

    @Test
    @Throws(Exception::class)
    fun testDisconnectWithIntermediateBufferedBytes() {
        val requestSize = 100 * 1024
        val node = "0"
        val request = randomString(requestSize)
        selector.close()
        channelBuilder = TestSslChannelBuilder(Mode.CLIENT)
        channelBuilder.configure(sslClientConfigs)
        selector = Selector(
            connectionMaxIdleMs = 5000,
            metrics = metrics,
            time = time,
            metricGrpPrefix = "MetricGroup",
            channelBuilder = channelBuilder,
            logContext = LogContext(),
        )
        connect(node = node, serverAddr = InetSocketAddress("localhost", server.port))
        selector.send(createSend(node, request))
        waitForBytesBuffered(selector, node)
        selector.close(node)
        verifySelectorEmpty()
    }

    @Throws(Exception::class)
    private fun waitForBytesBuffered(selector: Selector, node: String) {
        waitForCondition(
            testCondition = {
                try {
                    selector.poll(0L)
                    return@waitForCondition selector.channel(node)!!.hasBytesBuffered()
                } catch (e: IOException) {
                    throw RuntimeException(e)
                }
            },
            maxWaitMs = 2000L,
            conditionDetails = "Failed to reach socket state with bytes buffered",
        )
    }

    @Test
    @Throws(Exception::class)
    fun testBytesBufferedChannelWithNoIncomingBytes() {
        verifyNoUnnecessaryPollWithBytesBuffered { key ->
            key.interestOps(key.interestOps() and SelectionKey.OP_READ.inv())
        }
    }

    @Test
    @Throws(Exception::class)
    fun testBytesBufferedChannelAfterMute() {
        verifyNoUnnecessaryPollWithBytesBuffered { key: SelectionKey -> (key.attachment() as KafkaChannel).mute() }
    }

    @Throws(Exception::class)
    private fun verifyNoUnnecessaryPollWithBytesBuffered(disableRead: (SelectionKey) -> Unit) {
        selector.close()
        val node1 = "1"
        val node2 = "2"
        val node1Polls = AtomicInteger()
        channelBuilder = TestSslChannelBuilder(Mode.CLIENT)
        channelBuilder.configure(sslClientConfigs)
        selector = object : Selector(
            connectionMaxIdleMs = 5000,
            metrics = metrics,
            time = time,
            metricGrpPrefix = "MetricGroup",
            channelBuilder = channelBuilder,
            logContext = LogContext(),
        ) {

            override fun pollSelectionKeys(
                selectionKeys: Set<SelectionKey?>,
                isImmediatelyConnected: Boolean,
                currentTimeNanos: Long,
            ) {
                for (key in selectionKeys) {
                    val channel = key!!.attachment() as KafkaChannel
                    if (channel.id == node1) node1Polls.incrementAndGet()
                }
                super.pollSelectionKeys(selectionKeys, isImmediatelyConnected, currentTimeNanos)
            }
        }

        // Get node1 into bytes buffered state and then disable read on the socket.
        // Truncate the read buffers to ensure that there is buffered data, but not enough to make progress.
        val largeRequestSize = 100 * 1024
        connect(node1, InetSocketAddress("localhost", server.port))
        selector.send(createSend(node1, randomString(largeRequestSize)))
        waitForBytesBuffered(selector, node1)
        TestSslChannelBuilder.TestSslTransportLayer.transportLayers[node1]!!.truncateReadBuffer()
        disableRead(selector.channel(node1)!!.selectionKey())

        // Clear poll count and count the polls from now on
        node1Polls.set(0)

        // Process sends and receives on node2. Test verifies that we don't process node1
        // unnecessarily on each of these polls.
        connect(node2, InetSocketAddress("localhost", server.port))
        var received = 0
        val request = randomString(10)
        selector.send(createSend(node2, request))

        while (received < 100) {
            received += selector.completedReceives().size
            if (selector.completedSends().isNotEmpty()) selector.send(createSend(node2, request))
            selector.poll(5)
        }

        // Verify that pollSelectionKeys was invoked once to process buffered data
        // but not again since there isn't sufficient data to process.
        assertEquals(1, node1Polls.get())
        selector.close(node1)
        selector.close(node2)
        verifySelectorEmpty()
    }

    @Test
    @Throws(Exception::class)
    override fun testMuteOnOOM() {
        //clean up default selector, replace it with one that uses a finite mem pool
        selector.close()
        val pool = SimpleMemoryPool(
            sizeInBytes = 900,
            maxSingleAllocationBytes = 900,
            strict = false,
            oomPeriodSensor = null,
        )
        //the initial channel builder is for clients, we need a server one
        val tlsProtocol = "TLSv1.2"
        val trustStoreFile = tempFile("truststore", ".jks")
        val sslServerConfigs: Map<String, Any?> = SslConfigsBuilder(Mode.SERVER)
            .tlsProtocol(tlsProtocol)
            .createNewTrustStore(trustStoreFile)
            .build()
        channelBuilder = SslChannelBuilder(Mode.SERVER, null, false, LogContext())
        channelBuilder.configure(sslServerConfigs)
        selector = Selector(
            maxReceiveSize = NetworkReceive.UNLIMITED,
            connectionMaxIdleMs = 5000,
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
            val sender1 = createSender(tlsProtocol, serverAddress, randomPayload(900))
            val sender2 = createSender(tlsProtocol, serverAddress, randomPayload(900))
            sender1.start()
            sender2.start()
            val channelX = ss.accept() //not defined if its 1 or 2
            channelX.configureBlocking(false)
            val channelY = ss.accept()
            channelY.configureBlocking(false)
            selector.register("clientX", channelX)
            selector.register("clientY", channelY)
            var handshaked = false
            var firstReceive: NetworkReceive? = null
            var deadline = System.currentTimeMillis() + 5000
            //keep calling poll until:
            //1. both senders have completed the handshakes (so server selector has tried reading both payloads)
            //2. a single payload is actually read out completely (the other is too big to fit)
            while (System.currentTimeMillis() < deadline) {
                selector.poll(10)
                val completed: Collection<NetworkReceive> = selector.completedReceives()
                if (firstReceive == null) {
                    if (!completed.isEmpty()) {
                        assertEquals(1, completed.size, "expecting a single request")
                        firstReceive = completed.iterator().next()
                        assertTrue(selector.isMadeReadProgressLastPoll)
                        assertEquals(0, pool.availableMemory())
                    }
                } else assertTrue(completed.isEmpty(), "only expecting single request")

                handshaked = sender1.waitForHandshake(1) && sender2.waitForHandshake(1)
                if (handshaked && firstReceive != null && selector.isOutOfMemory) break
            }
            assertTrue(handshaked, "could not initiate connections within timeout")
            selector.poll(10)
            assertTrue(selector.completedReceives().isEmpty())
            assertEquals(0, pool.availableMemory())
            assertNotNull(firstReceive, "First receive not complete")
            assertTrue(selector.isOutOfMemory, "Selector not out of memory")
            firstReceive.close()
            assertEquals(900, pool.availableMemory()) //memory has been released back to pool

            var completed: Collection<NetworkReceive?> = emptyList<NetworkReceive>()
            deadline = System.currentTimeMillis() + 5000
            while (System.currentTimeMillis() < deadline && completed.isEmpty()) {
                selector.poll(1000)
                completed = selector.completedReceives()
            }
            assertEquals(1, completed.size, "could not read remaining request within timeout")
            assertEquals(0, pool.availableMemory())
            assertFalse(selector.isOutOfMemory)
        }
    }

    /**
     * Connects and waits for handshake to complete. This is required since SslTransportLayer
     * implementation requires the channel to be ready before send is invoked (unlike plaintext
     * where send can be invoked straight after connect)
     */
    @Throws(IOException::class)
    override fun connect(node: String, serverAddr: InetSocketAddress) {
        blockingConnect(node, serverAddr)
    }

    private fun createSender(tlsProtocol: String, serverAddress: InetSocketAddress, payload: ByteArray): SslSender {
        return SslSender(tlsProtocol, serverAddress, payload)
    }

    private class TestSslChannelBuilder(mode: Mode) : SslChannelBuilder(
        mode = mode,
        listenerName = null,
        isInterBrokerListener = false,
        logContext = LogContext(),
    ) {

        @Throws(IOException::class)
        override fun buildTransportLayer(
            sslFactory: SslFactory,
            id: String,
            key: SelectionKey,
            metadataRegistry: ChannelMetadataRegistry?,
        ): SslTransportLayer {
            val socketChannel = key.channel() as SocketChannel
            val sslEngine = sslFactory.createSslEngine(socketChannel.socket())
            return TestSslTransportLayer(id, key, sslEngine, metadataRegistry!!)
        }

        /*
         * TestSslTransportLayer will read from socket once every two tries. This increases
         * the chance that there will be bytes buffered in the transport layer after read().
         */
        internal class TestSslTransportLayer(
            channelId: String,
            key: SelectionKey,
            sslEngine: SSLEngine,
            metadataRegistry: ChannelMetadataRegistry,
        ) :
            SslTransportLayer(channelId, key, sslEngine, metadataRegistry) {
            var muteSocket = false

            init {
                transportLayers[channelId] = this
            }

            @Throws(IOException::class)
            override fun readFromSocketChannel(): Int {
                if (muteSocket) {
                    if (selectionKey().interestOps() and SelectionKey.OP_READ != 0) muteSocket = false
                    return 0
                }
                muteSocket = true
                return super.readFromSocketChannel()
            }

            // Leave one byte in network read buffer so that some buffered bytes are present,
            // but not enough to make progress on a read.
            @Throws(Exception::class)
            fun truncateReadBuffer() {
                netReadBuffer()!!.position(1)
                appReadBuffer()!!.position(0)
                muteSocket = true
            }

            companion object {
                var transportLayers = mutableMapOf<String, TestSslTransportLayer>()
            }
        }
    }
}
