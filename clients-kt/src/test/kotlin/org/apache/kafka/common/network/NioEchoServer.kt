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

import java.io.IOException
import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.channels.SelectionKey
import java.nio.channels.ServerSocketChannel
import java.nio.channels.SocketChannel
import java.nio.channels.WritableByteChannel
import java.util.Collections
import org.apache.kafka.common.config.AbstractConfig
import org.apache.kafka.common.message.ApiMessageType
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.network.ChannelBuilders.serverChannelBuilder
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.security.authenticator.CredentialCache
import org.apache.kafka.common.security.scram.ScramCredential
import org.apache.kafka.common.security.scram.internals.ScramMechanism
import org.apache.kafka.common.security.token.delegation.internals.DelegationTokenCache
import org.apache.kafka.common.utils.LogContext
import org.apache.kafka.common.utils.MockTime
import org.apache.kafka.common.utils.Time
import org.apache.kafka.common.utils.Utils.closeQuietly
import org.apache.kafka.test.TestUtils
import org.apache.kafka.test.TestUtils.apiKeyFrom
import org.apache.kafka.test.TestUtils.waitForCondition
import org.slf4j.LoggerFactory
import kotlin.math.abs
import kotlin.math.sign
import kotlin.test.assertEquals

/**
 * Non-blocking EchoServer implementation that uses ChannelBuilder to create channels
 * with the configured security protocol.
 *
 */
class NioEchoServer(
    listenerName: ListenerName?,
    securityProtocol: SecurityProtocol,
    config: AbstractConfig?,
    serverHost: String?,
    channelBuilder: ChannelBuilder?,
    credentialCache: CredentialCache?,
    failedAuthenticationDelayMs: Int = 100,
    time: Time,
    tokenCache: DelegationTokenCache = DelegationTokenCache(ScramMechanism.mechanismNames),
) : Thread("echoserver") {

    val port: Int

    private val serverSocketChannel: ServerSocketChannel

    private val newChannels: MutableList<SocketChannel>

    private val socketChannels: MutableList<SocketChannel>

    private val acceptorThread: AcceptorThread

    val selector: Selector

    @Volatile
    private var outputChannel: TransferableChannel? = null

    val credentialCache: CredentialCache?

    private val metrics: Metrics

    @Volatile
    var numSent = 0

    @Volatile
    private var closeKafkaChannels = false

    val tokenCache: DelegationTokenCache

    private val time: Time

    constructor(
        listenerName: ListenerName?,
        securityProtocol: SecurityProtocol,
        config: AbstractConfig?,
        serverHost: String?,
        channelBuilder: ChannelBuilder?,
        credentialCache: CredentialCache,
        time: Time,
    ) : this(
        listenerName = listenerName,
        securityProtocol = securityProtocol,
        config = config,
        serverHost = serverHost,
        channelBuilder = channelBuilder,
        credentialCache = credentialCache,
        failedAuthenticationDelayMs = 100,
        time = time
    )

    init {
        setDaemon(true)
        serverSocketChannel = ServerSocketChannel.open()
        serverSocketChannel.configureBlocking(false)
        serverSocketChannel.socket().bind(InetSocketAddress(serverHost, 0))
        port = serverSocketChannel.socket().getLocalPort()
        socketChannels = Collections.synchronizedList(ArrayList())
        newChannels = Collections.synchronizedList(ArrayList())
        this.credentialCache = credentialCache
        this.tokenCache = tokenCache
        if (securityProtocol == SecurityProtocol.SASL_PLAINTEXT || securityProtocol == SecurityProtocol.SASL_SSL) {
            for (mechanism in ScramMechanism.mechanismNames) {
                if (credentialCache!!.cache(mechanism, ScramCredential::class.java) == null)
                    credentialCache.createCache(mechanism, ScramCredential::class.java)
            }
        }
        val logContext = LogContext()

        val actualChannelBuilder = channelBuilder ?: serverChannelBuilder(
            listenerName = listenerName,
            isInterBrokerListener = false,
            securityProtocol = securityProtocol,
            config = config!!,
            credentialCache = credentialCache,
            tokenCache = tokenCache,
            time = time,
            logContext = logContext,
        ) { TestUtils.defaultApiVersionsResponse(listenerType = ApiMessageType.ListenerType.ZK_BROKER) }
        metrics = Metrics()
        selector = Selector(
            connectionMaxIdleMs = 10000,
            failedAuthenticationDelayMs = failedAuthenticationDelayMs,
            metrics = metrics,
            time = time,
            metricGrpPrefix = "MetricGroup",
            channelBuilder = actualChannelBuilder,
            logContext = logContext,
        )
        acceptorThread = AcceptorThread()
        this.time = time
    }

    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("port"),
    )
    fun port(): Int = port

    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("credentialCache"),
    )
    fun credentialCache(): CredentialCache? = credentialCache

    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("tokenCache"),
    )
    fun tokenCache(): DelegationTokenCache = tokenCache

    fun metricValue(name: String): Double {
        val value = metrics.metrics.firstNotNullOfOrNull { (key, value) -> if (key.name == name) value else null }
        return value?.metricValue() as Double? ?: error("Metric not found, $name, found=${metrics.metrics.keys}")
    }

    @Throws(InterruptedException::class)
    fun verifyAuthenticationMetrics(successfulAuthentications: Int, failedAuthentications: Int) {
        waitForMetrics(
            namePrefix = "successful-authentication",
            expectedValue = successfulAuthentications.toDouble(),
            metricTypes = setOf(MetricType.TOTAL, MetricType.RATE),
        )
        waitForMetrics(
            namePrefix = "failed-authentication",
            expectedValue = failedAuthentications.toDouble(),
            metricTypes =setOf(MetricType.TOTAL, MetricType.RATE),
        )
    }

    @Throws(InterruptedException::class)
    fun verifyReauthenticationMetrics(successfulReauthentications: Int, failedReauthentications: Int) {
        waitForMetrics(
            namePrefix = "successful-reauthentication",
            expectedValue = successfulReauthentications.toDouble(),
            metricTypes = setOf(MetricType.TOTAL, MetricType.RATE),
        )
        waitForMetrics(
            namePrefix = "failed-reauthentication",
            expectedValue = failedReauthentications.toDouble(),
            metricTypes = setOf(MetricType.TOTAL, MetricType.RATE),
        )
        waitForMetrics(
            namePrefix = "successful-authentication-no-reauth",
            expectedValue = 0.0,
            metricTypes = setOf(MetricType.TOTAL),
        )
        if (time !is MockTime) waitForMetrics(
            namePrefix = "reauthentication-latency",
            expectedValue = sign(successfulReauthentications.toDouble()),
            metricTypes = setOf(MetricType.MAX, MetricType.AVG)
        )
    }

    @Throws(InterruptedException::class)
    fun verifyAuthenticationNoReauthMetric(successfulAuthenticationNoReauths: Int) {
        waitForMetrics(
            namePrefix = "successful-authentication-no-reauth",
            expectedValue = successfulAuthenticationNoReauths.toDouble(),
            metricTypes = setOf(MetricType.TOTAL),
        )
    }

    @Throws(InterruptedException::class)
    fun waitForMetric(name: String, expectedValue: Double) {
        waitForMetrics(
            namePrefix = name,
            expectedValue = expectedValue,
            metricTypes = setOf(MetricType.TOTAL, MetricType.RATE),
        )
    }

    @Throws(InterruptedException::class)
    fun waitForMetrics(namePrefix: String, expectedValue: Double, metricTypes: Set<MetricType>) {
        val maxAggregateWaitMs = 15000L
        val startMs = time.milliseconds()
        for (metricType in metricTypes) {
            val currentElapsedMs = time.milliseconds() - startMs
            val thisMaxWaitMs = maxAggregateWaitMs - currentElapsedMs
            val metricName = namePrefix + metricType.metricNameSuffix()
            if (expectedValue == 0.0) {
                var expected = expectedValue
                if (metricType == MetricType.MAX || metricType == MetricType.AVG) expected = Double.NaN
                assertEquals(
                    expected = expected,
                    actual = metricValue(metricName),
                    absoluteTolerance = EPS,
                    message =
                    "Metric not updated $metricName expected:<$expectedValue> but was:<${metricValue(metricName)}>",
                )
            } else if (metricType == MetricType.TOTAL) waitForCondition(
                testCondition = { abs(metricValue(metricName) - expectedValue) <= EPS },
                maxWaitMs = thisMaxWaitMs,
                conditionDetails =
                "Metric not updated $metricName expected:<$expectedValue> but was:<${metricValue(metricName)}>",
            ) else waitForCondition(
                testCondition = { metricValue(metricName) > 0.0 },
                maxWaitMs = thisMaxWaitMs,
                conditionDetails =
                "Metric not updated $metricName expected:<a positive number> but was:<${metricValue(metricName)}>"
            )
        }
    }

    override fun run() {
        try {
            acceptorThread.start()
            while (serverSocketChannel.isOpen) {
                selector.poll(100)
                synchronized(newChannels) {
                    for (socketChannel in newChannels) {
                        val id = id(socketChannel)
                        selector.register(id, socketChannel)
                        socketChannels.add(socketChannel)
                    }
                    newChannels.clear()
                }
                if (closeKafkaChannels) {
                    for (channel in selector.channels()) selector.close(channel.id)
                }
                val completedReceives = selector.completedReceives()
                for (rcv in completedReceives) {
                    val channel = channel(rcv.source())
                    if (!maybeBeginServerReauthentication(channel, rcv, time)) {
                        val channelId = channel!!.id
                        selector.mute(channelId)
                        val send = NetworkSend(rcv.source(), ByteBufferSend.sizePrefixed(rcv.payload()!!))
                        if (outputChannel == null) selector.send(send) else {
                            send.writeTo(outputChannel!!)
                            selector.unmute(channelId)
                        }
                    }
                }
                for (send in selector.completedSends()) {
                    selector.unmute(send.destinationId)
                    numSent += 1
                }
            }
        } catch (e: IOException) {
            LOG.warn(e.message, e)
        }
    }

    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("numSent"),
    )
    fun numSent(): Int = numSent

    private fun id(channel: SocketChannel): String {
        return channel.socket().getLocalAddress().hostAddress + ":" + channel.socket().getLocalPort() + "-" +
                channel.socket().getInetAddress().hostAddress + ":" + channel.socket().getPort()
    }

    private fun channel(id: String): KafkaChannel? {
        return selector.channel(id) ?: selector.closingChannel(id)
    }

    /**
     * Sets the output channel to which messages received on this server are echoed.
     * This is useful in tests where the clients sending the messages don't receive
     * the responses (eg. testing graceful close).
     */
    fun outputChannel(channel: WritableByteChannel) {
        outputChannel = object : TransferableChannel {
            override fun hasPendingWrites(): Boolean = false

            @Throws(IOException::class)
            override fun transferFrom(
                fileChannel: FileChannel,
                position: Long,
                count: Long,
            ): Long = fileChannel.transferTo(position, count, channel)

            override fun isOpen(): Boolean = channel.isOpen

            @Throws(IOException::class)
            override fun close() = channel.close()

            @Throws(IOException::class)
            override fun write(src: ByteBuffer): Int = channel.write(src)

            @Throws(IOException::class)
            override fun write(srcs: Array<ByteBuffer>, offset: Int, length: Int): Long {
                var result: Long = 0
                for (i in offset until offset + length) result += write(srcs[i]).toLong()
                return result
            }

            @Throws(IOException::class)
            override fun write(srcs: Array<ByteBuffer>): Long {
                return write(srcs, 0, srcs.size)
            }
        }
    }

    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("selector"),
    )
    fun selector(): Selector = selector

    fun closeKafkaChannels() {
        closeKafkaChannels = true
        selector.wakeup()
        try {
            waitForCondition(
                testCondition = { selector.channels().isEmpty() },
                conditionDetails = "Channels not closed",
            )
        } catch (e: InterruptedException) {
            throw RuntimeException(e)
        } finally {
            closeKafkaChannels = false
        }
    }

    @Throws(IOException::class)
    fun closeSocketChannels() {
        socketChannels.forEach(SocketChannel::close)
        socketChannels.clear()
    }

    @Throws(IOException::class, InterruptedException::class)
    fun close() {
        serverSocketChannel.close()
        closeSocketChannels()
        closeQuietly(selector, "selector")
        acceptorThread.interrupt()
        acceptorThread.join()
        interrupt()
        join()
    }

    private inner class AcceptorThread : Thread() {

        init {
            setName("acceptor")
        }

        override fun run() {
            var acceptSelector: java.nio.channels.Selector? = null
            try {
                acceptSelector = java.nio.channels.Selector.open()
                serverSocketChannel.register(acceptSelector, SelectionKey.OP_ACCEPT)
                while (serverSocketChannel.isOpen) {
                    if (acceptSelector.select(1000) > 0) {
                        val it = acceptSelector.selectedKeys().iterator()
                        while (it.hasNext()) {
                            val key = it.next()
                            if (key.isAcceptable) {
                                val socketChannel = (key.channel() as ServerSocketChannel).accept()
                                socketChannel.configureBlocking(false)
                                newChannels.add(socketChannel)
                                selector.wakeup()
                            }
                            it.remove()
                        }
                    }
                }
            } catch (e: IOException) {
                LOG.warn(e.message, e)
            } finally {
                closeQuietly(acceptSelector, "acceptSelector")
            }
        }
    }

    enum class MetricType {
        TOTAL,
        RATE,
        AVG,
        MAX;

        private val metricNameSuffix = "-${name.lowercase()}"
        fun metricNameSuffix(): String = metricNameSuffix
    }

    companion object {

        private val LOG = LoggerFactory.getLogger(NioEchoServer::class.java)

        private const val EPS = 0.0001

        private fun maybeBeginServerReauthentication(
            channel: KafkaChannel?,
            networkReceive: NetworkReceive,
            time: Time,
        ): Boolean {
            try {
                if (apiKeyFrom(networkReceive) == ApiKeys.SASL_HANDSHAKE) {
                    return channel!!.maybeBeginServerReauthentication(networkReceive) { time.nanoseconds() }
                }
            } catch (_: Exception) {
                // ignore
            }
            return false
        }
    }
}
