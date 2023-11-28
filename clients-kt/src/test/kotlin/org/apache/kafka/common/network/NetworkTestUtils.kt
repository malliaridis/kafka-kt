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
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import org.apache.kafka.common.config.AbstractConfig
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.security.authenticator.CredentialCache
import org.apache.kafka.common.security.token.delegation.internals.DelegationTokenCache
import org.apache.kafka.common.utils.LogContext
import org.apache.kafka.common.utils.Time
import org.apache.kafka.common.utils.Utils
import org.apache.kafka.common.utils.Utils.sleep
import org.apache.kafka.test.TestUtils.randomString
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

/**
 * Common utility functions used by transport layer and authenticator tests.
 */
object NetworkTestUtils {
    
    @Throws(Exception::class)
    fun createEchoServer(
        listenerName: ListenerName?,
        securityProtocol: SecurityProtocol,
        serverConfig: AbstractConfig?,
        credentialCache: CredentialCache?,
        time: Time,
    ): NioEchoServer = createEchoServer(
        listenerName = listenerName,
        securityProtocol = securityProtocol,
        serverConfig = serverConfig,
        credentialCache = credentialCache,
        failedAuthenticationDelayMs = 100,
        time = time,
    )

    @Throws(Exception::class)
    fun createEchoServer(
        listenerName: ListenerName?,
        securityProtocol: SecurityProtocol,
        serverConfig: AbstractConfig?,
        credentialCache: CredentialCache?,
        failedAuthenticationDelayMs: Int,
        time: Time,
    ): NioEchoServer {
        val server = NioEchoServer(
            listenerName = listenerName,
            securityProtocol = securityProtocol,
            config = serverConfig,
            serverHost = "localhost",
            channelBuilder = null,
            credentialCache = credentialCache,
            failedAuthenticationDelayMs = failedAuthenticationDelayMs,
            time = time,
        )
        server.start()
        return server
    }

    @Throws(Exception::class)
    fun createEchoServer(
        listenerName: ListenerName?,
        securityProtocol: SecurityProtocol,
        serverConfig: AbstractConfig?,
        credentialCache: CredentialCache,
        failedAuthenticationDelayMs: Int,
        time: Time,
        tokenCache: DelegationTokenCache,
    ): NioEchoServer {
        val server = NioEchoServer(
            listenerName = listenerName,
            securityProtocol = securityProtocol,
            config = serverConfig,
            serverHost = "localhost",
            channelBuilder = null,
            credentialCache = credentialCache,
            failedAuthenticationDelayMs = failedAuthenticationDelayMs,
            time = time,
            tokenCache = tokenCache,
        )
        server.start()
        return server
    }

    fun createSelector(channelBuilder: ChannelBuilder, time: Time): Selector {
        return Selector(
            connectionMaxIdleMs = 5000,
            metrics = Metrics(),
            time = time,
            metricGrpPrefix = "MetricGroup",
            channelBuilder = channelBuilder,
            logContext = LogContext(),
        )
    }

    @Throws(Exception::class)
    fun checkClientConnection(
        selector: Selector,
        node: String,
        minMessageSize: Int,
        messageCount: Int,
    ) {
        waitForChannelReady(selector, node)
        val prefix = randomString(minMessageSize)
        var requests = 0
        var responses = 0
        selector.send(
            NetworkSend(
                destinationId = node,
                send = ByteBufferSend.sizePrefixed(ByteBuffer.wrap("$prefix-0".toByteArray()))
            )
        )
        requests++
        while (responses < messageCount) {
            selector.poll(0L)
            assertEquals(
                expected = 0,
                actual = selector.disconnected().size,
                message = "No disconnects should have occurred .${selector.disconnected()}",
            )
            for (receive in selector.completedReceives()) {
                assertEquals(
                    expected = "$prefix-$responses",
                    actual = String(Utils.toArray(receive.payload()!!), StandardCharsets.UTF_8)
                )
                responses++
            }
            var i = 0
            while (
                i < selector.completedSends().size
                && requests < messageCount
                && selector.isChannelReady(node)
            ) {
                selector.send(
                    NetworkSend(
                        destinationId = node,
                        send = ByteBufferSend.sizePrefixed(ByteBuffer.wrap("$prefix-$requests".toByteArray())),
                    )
                )
                i++
                requests++
            }
        }
    }

    @Throws(IOException::class)
    fun waitForChannelConnected(selector: Selector, node: String) {
        var secondsLeft = 30
        while (selector.channel(node) != null && !selector.channel(node)!!.isConnected && secondsLeft-- > 0)
            selector.poll(1000L)

        assertNotNull(selector.channel(node))
        assertTrue(
            actual = selector.channel(node)!!.isConnected,
            message = "Channel $node was not connected after 30 seconds",
        )
    }

    @Throws(IOException::class)
    fun waitForChannelReady(selector: Selector, node: String) {
        // wait for handshake to finish
        var secondsLeft = 30
        while (!selector.isChannelReady(node) && secondsLeft-- > 0) {
            selector.poll(1000L)
        }

        assertTrue(
            actual = selector.isChannelReady(node),
            message = "Channel $node was not ready after 30 seconds",
        )
    }

    @Throws(IOException::class)
    fun waitForChannelClose(
        selector: Selector,
        node: String?,
        channelState: ChannelState.State?,
        delayBetweenPollMs: Int = 0,
    ): ChannelState {
        var closed = false
        for (i in 0..299) {
            if (delayBetweenPollMs > 0) sleep(delayBetweenPollMs.toLong())
            selector.poll(100L)
            if (selector.channel(node!!) == null && selector.closingChannel(node) == null) {
                closed = true
                break
            }
        }
        assertTrue(closed, "Channel was not closed by timeout")
        val finalState = selector.disconnected()[node]
        assertEquals(channelState, finalState!!.state())
        return finalState
    }

    fun completeDelayedChannelClose(selector: Selector, currentTimeNanos: Long) {
        selector.completeDelayedChannelClose(currentTimeNanos)
    }

    fun delayedClosingChannels(selector: Selector): Map<*, *>? {
        return selector.delayedClosingChannels()
    }
}
