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
import org.apache.kafka.common.config.SslConfigs
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.network.NetworkTestUtils.createEchoServer
import org.apache.kafka.common.network.NetworkTestUtils.waitForChannelClose
import org.apache.kafka.common.network.NetworkTestUtils.waitForChannelReady
import org.apache.kafka.common.security.TestSecurityConfig
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.utils.LogContext
import org.apache.kafka.common.utils.Time
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.condition.EnabledForJreRange
import org.junit.jupiter.api.condition.JRE

class SslTransportTls12Tls13Test {

    private var server: NioEchoServer? = null

    private lateinit var selector: Selector

    private lateinit var sslClientConfigs: MutableMap<String, Any?>

    private lateinit var sslServerConfigs: MutableMap<String, Any?>

    @BeforeEach
    @Throws(Exception::class)
    fun setup() {
        // Create certificates for use by client and server. Add server cert to client truststore and vice versa.
        val serverCertStores = CertStores(
            server = true,
            commonName = "server",
            sanHostName = "localhost",
        )
        val clientCertStores = CertStores(
            server = false,
            commonName = "client",
            sanHostName = "localhost",
        )
        sslServerConfigs = serverCertStores.getTrustingConfig(clientCertStores).toMutableMap()
        sslClientConfigs = clientCertStores.getTrustingConfig(serverCertStores).toMutableMap()
        val logContext = LogContext()
        val channelBuilder: ChannelBuilder = SslChannelBuilder(
            mode = Mode.CLIENT,
            listenerName = null,
            isInterBrokerListener = false,
            logContext = logContext,
        )
        channelBuilder.configure(sslClientConfigs)
        selector = Selector(
            connectionMaxIdleMs = 5000L,
            metrics = Metrics(),
            time = TIME,
            metricGrpPrefix = "MetricGroup",
            channelBuilder = channelBuilder,
            logContext = logContext,
        )
    }

    @AfterEach
    @Throws(Exception::class)
    fun teardown() {
        if (::selector.isInitialized) selector.close()
        server?.close()
    }

    /**
     * Tests that connections fails if TLSv1.3 enabled but cipher suite suitable only for TLSv1.2 used.
     */
    @Test
    @EnabledForJreRange(min = JRE.JAVA_11)
    @Throws(Exception::class)
    fun testCiphersSuiteForTls12FailsForTls13() {
        val cipherSuite = "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384"
        sslServerConfigs[SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG] = listOf("TLSv1.3")
        sslServerConfigs[SslConfigs.SSL_CIPHER_SUITES_CONFIG] = listOf(cipherSuite)
        server = createEchoServer(
            listenerName = ListenerName.forSecurityProtocol(SecurityProtocol.SSL),
            securityProtocol = SecurityProtocol.SSL,
            serverConfig = TestSecurityConfig(sslServerConfigs),
            credentialCache = null,
            time = TIME,
        )
        sslClientConfigs[SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG] = listOf("TLSv1.3")
        sslClientConfigs[SslConfigs.SSL_CIPHER_SUITES_CONFIG] = listOf(cipherSuite)
        checkAuthentiationFailed()
    }

    /**
     * Tests that connections can't be made if server uses TLSv1.2 with custom cipher suite and client uses TLSv1.3.
     */
    @Test
    @EnabledForJreRange(min = JRE.JAVA_11)
    @Throws(Exception::class)
    fun testCiphersSuiteFailForServerTls12ClientTls13() {
        val tls12CipherSuite = "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384"
        val tls13CipherSuite = "TLS_AES_128_GCM_SHA256"
        sslServerConfigs[SslConfigs.SSL_PROTOCOL_CONFIG] = "TLSv1.2"
        sslServerConfigs[SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG] = listOf("TLSv1.2")
        sslServerConfigs[SslConfigs.SSL_CIPHER_SUITES_CONFIG] = listOf(tls12CipherSuite)
        server = createEchoServer(
            listenerName = ListenerName.forSecurityProtocol(SecurityProtocol.SSL),
            securityProtocol = SecurityProtocol.SSL,
            serverConfig = TestSecurityConfig(sslServerConfigs),
            credentialCache = null,
            time = TIME,
        )
        sslClientConfigs[SslConfigs.SSL_PROTOCOL_CONFIG] = "TLSv1.3"
        sslClientConfigs[SslConfigs.SSL_CIPHER_SUITES_CONFIG] = listOf(tls13CipherSuite)
        checkAuthentiationFailed()
    }

    /**
     * Tests that connections can be made with TLSv1.3 cipher suite.
     */
    @Test
    @EnabledForJreRange(min = JRE.JAVA_11)
    @Throws(Exception::class)
    fun testCiphersSuiteForTls13() {
        val cipherSuite = "TLS_AES_128_GCM_SHA256"
        sslServerConfigs[SslConfigs.SSL_CIPHER_SUITES_CONFIG] = listOf(cipherSuite)
        server = createEchoServer(
            listenerName = ListenerName.forSecurityProtocol(SecurityProtocol.SSL),
            securityProtocol = SecurityProtocol.SSL,
            serverConfig = TestSecurityConfig(sslServerConfigs),
            credentialCache = null,
            time = TIME
        )
        sslClientConfigs[SslConfigs.SSL_CIPHER_SUITES_CONFIG] = listOf(cipherSuite)
        checkAuthenticationSucceed()
    }

    /**
     * Tests that connections can be made with TLSv1.2 cipher suite.
     */
    @Test
    @Throws(Exception::class)
    fun testCiphersSuiteForTls12() {
        val cipherSuite = "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384"
        sslServerConfigs[SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG] =
            SslConfigs.DEFAULT_SSL_ENABLED_PROTOCOLS
                .split(",".toRegex())
                .dropLastWhile { it.isEmpty() }
        sslServerConfigs[SslConfigs.SSL_CIPHER_SUITES_CONFIG] = listOf(cipherSuite)
        server = createEchoServer(
            listenerName = ListenerName.forSecurityProtocol(SecurityProtocol.SSL),
            securityProtocol = SecurityProtocol.SSL,
            serverConfig = TestSecurityConfig(sslServerConfigs),
            credentialCache = null,
            time = TIME
        )
        sslClientConfigs[SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG] =
            SslConfigs.DEFAULT_SSL_ENABLED_PROTOCOLS
                .split(",".toRegex())
                .dropLastWhile { it.isEmpty() }
        sslClientConfigs[SslConfigs.SSL_CIPHER_SUITES_CONFIG] = listOf(cipherSuite)
        checkAuthenticationSucceed()
    }

    /** Checks connection failed using the specified `tlsVersion`.  */
    @Throws(IOException::class, InterruptedException::class)
    private fun checkAuthentiationFailed() {
        sslClientConfigs[SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG] = mutableListOf("TLSv1.3")
        createSelector(sslClientConfigs)
        val addr = InetSocketAddress("localhost", server!!.port)
        selector.connect(
            id = "0",
            address = addr,
            sendBufferSize = BUFFER_SIZE,
            receiveBufferSize = BUFFER_SIZE,
        )
        waitForChannelClose(
            selector = selector,
            node = "0",
            channelState = ChannelState.State.AUTHENTICATION_FAILED,
        )
        server!!.verifyAuthenticationMetrics(successfulAuthentications = 0, failedAuthentications = 1)
    }

    @Throws(IOException::class, InterruptedException::class)
    private fun checkAuthenticationSucceed() {
        createSelector(sslClientConfigs)
        val addr = InetSocketAddress("localhost", server!!.port)
        selector.connect(
            id = "0",
            address = addr,
            sendBufferSize = BUFFER_SIZE,
            receiveBufferSize = BUFFER_SIZE,
        )
        waitForChannelReady(selector = selector, node = "0")
        server!!.verifyAuthenticationMetrics(successfulAuthentications = 1, failedAuthentications = 0)
    }

    private fun createSelector(sslClientConfigs: Map<String, Any?>) {
        val channelBuilder = SslTransportLayerTest.TestSslChannelBuilder(Mode.CLIENT)
        channelBuilder.configureBufferSizes(
            netReadBufSize = null,
            netWriteBufSize = null,
            appBufSize = null,
        )
        channelBuilder.configure(sslClientConfigs)
        selector = Selector(
            connectionMaxIdleMs = (100 * 5000).toLong(),
            metrics = Metrics(),
            time = TIME,
            metricGrpPrefix = "MetricGroup",
            channelBuilder = channelBuilder,
            logContext = LogContext(),
        )
    }

    companion object {

        private const val BUFFER_SIZE = 4 * 1024

        private val TIME = Time.SYSTEM
    }
}
