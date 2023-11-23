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

import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.util.Collections
import java.util.stream.Stream
import org.apache.kafka.common.config.SslConfigs
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.network.NetworkTestUtils.createEchoServer
import org.apache.kafka.common.network.NetworkTestUtils.waitForChannelClose
import org.apache.kafka.common.network.NetworkTestUtils.waitForChannelReady
import org.apache.kafka.common.security.TestSecurityConfig
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.utils.Java
import org.apache.kafka.common.utils.LogContext
import org.apache.kafka.common.utils.Time
import org.apache.kafka.test.TestUtils.randomString
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import kotlin.test.assertFalse
import kotlin.test.assertNotNull

/**
 * Tests for the SSL transport layer.
 * Checks different versions of the protocol usage on the server and client.
 */
class SslVersionsTransportLayerTest {
    
    /**
     * Tests that connection success with the default TLS version.
     * Note that debug mode for javax.net.ssl can be enabled via
     * `System.setProperty("javax.net.debug", "ssl:handshake");`
     */
    @ParameterizedTest(name = "tlsServerProtocol = {0}, tlsClientProtocol = {1}")
    @MethodSource("Parameters")
    @Throws(Exception::class)
    fun testTlsDefaults(serverProtocols: List<String?>, clientProtocols: List<String?>) {
        // Create certificates for use by client and server. Add server cert to client truststore and vice versa.
        val serverCertStores = CertStores(server = true, commonName = "server", sanHostName = "localhost")
        val clientCertStores = CertStores(server = false, commonName = "client", sanHostName = "localhost")
        val sslClientConfigs = getTrustingConfig(
            certStores = clientCertStores,
            peerCertStores = serverCertStores,
            tlsProtocols = clientProtocols,
        )
        val sslServerConfigs = getTrustingConfig(
            certStores = serverCertStores,
            peerCertStores = clientCertStores,
            tlsProtocols = serverProtocols,
        )
        val server = createEchoServer(
            listenerName = ListenerName.forSecurityProtocol(SecurityProtocol.SSL),
            securityProtocol = SecurityProtocol.SSL,
            serverConfig = TestSecurityConfig(sslServerConfigs),
            credentialCache = null,
            time = TIME,
        )
        val selector = createClientSelector(sslClientConfigs)
        val node = "0"
        selector.connect(
            id = node,
            address = InetSocketAddress("localhost", server.port),
            sendBufferSize = BUFFER_SIZE,
            receiveBufferSize = BUFFER_SIZE,
        )
        if (isCompatible(serverProtocols, clientProtocols)) {
            waitForChannelReady(selector, node)
            val msgSz = 1024 * 1024
            val message = randomString(msgSz)
            selector.send(
                NetworkSend(
                    destinationId = node,
                    send = ByteBufferSend.sizePrefixed(ByteBuffer.wrap(message.toByteArray())),
                )
            )
            while (selector.completedReceives().isEmpty()) selector.poll(100L)
            val totalBytes = msgSz + 4 // including 4-byte size
            server.waitForMetric(name = "incoming-byte", expectedValue = totalBytes.toDouble())
            server.waitForMetric(name = "outgoing-byte", expectedValue = totalBytes.toDouble())
            server.waitForMetric(name = "request", expectedValue = 1.0)
            server.waitForMetric(name = "response", expectedValue = 1.0)
        } else {
            waitForChannelClose(
                selector = selector,
                node = node,
                channelState = ChannelState.State.AUTHENTICATION_FAILED,
            )
            server.verifyAuthenticationMetrics(successfulAuthentications = 0, failedAuthentications = 1)
        }
    }

    /**
     * The explanation of this check in the structure of the ClientHello SSL message.
     * Please, take a look at the [Guide](https://docs.oracle.com/en/java/javase/11/security/java-secure-socket-extension-jsse-reference-guide.html#GUID-4D421910-C36D-40A2-8BA2-7D42CCBED3C6),
     * "Send ClientHello Message" section.
     *
     * > Client version: For TLS 1.3, this has a fixed value, TLSv1.2; TLS 1.3 uses the extension supported_versions and not this field to negotiate protocol version
     * ...
     * > supported_versions: Lists which versions of TLS the client supports. In particular, if the client
     * > requests TLS 1.3, then the client version field has the value TLSv1.2 and this extension
     * > contains the value TLSv1.3; if the client requests TLS 1.2, then the client version field has the
     * > value TLSv1.2 and this extension either doesn’t exist or contains the value TLSv1.2 but not the value TLSv1.3.
     *
     * This mean that TLSv1.3 client can fallback to TLSv1.2 but TLSv1.2 client can't change protocol to TLSv1.3.
     *
     * @param serverProtocols Server protocols. Expected to be non empty.
     * @param clientProtocols Client protocols. Expected to be non empty.
     * @return `true` if client should be able to connect to the server.
     */
    private fun isCompatible(serverProtocols: List<String?>, clientProtocols: List<String?>): Boolean {
        assertNotNull(serverProtocols)
        assertFalse(serverProtocols.isEmpty())
        assertNotNull(clientProtocols)
        assertFalse(clientProtocols.isEmpty())
        return serverProtocols.contains(clientProtocols[0])
                || clientProtocols[0] == "TLSv1.3"
                && !Collections.disjoint(serverProtocols, clientProtocols)
    }

    private fun createClientSelector(sslClientConfigs: Map<String, Any?>): Selector {
        val channelBuilder = SslTransportLayerTest.TestSslChannelBuilder(Mode.CLIENT)
        channelBuilder.configureBufferSizes(netReadBufSize = null, netWriteBufSize = null, appBufSize = null)
        channelBuilder.configure(sslClientConfigs)
        return Selector(
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

        fun parameters(): Stream<Arguments> {
            val parameters = mutableListOf<Arguments>()
            parameters.add(Arguments.of(listOf("TLSv1.2"), listOf("TLSv1.2")))
            if (Java.IS_JAVA11_COMPATIBLE) {
                parameters.add(Arguments.of(listOf("TLSv1.2"), listOf("TLSv1.3")))
                parameters.add(Arguments.of(listOf("TLSv1.3"), listOf("TLSv1.2")))
                parameters.add(Arguments.of(listOf("TLSv1.3"), listOf("TLSv1.3")))
                parameters.add(Arguments.of(listOf("TLSv1.2"), listOf("TLSv1.2", "TLSv1.3")))
                parameters.add(Arguments.of(listOf("TLSv1.2"), listOf("TLSv1.3", "TLSv1.2")))
                parameters.add(Arguments.of(listOf("TLSv1.3"), listOf("TLSv1.2", "TLSv1.3")))
                parameters.add(Arguments.of(listOf("TLSv1.3"), listOf("TLSv1.3", "TLSv1.2")))
                parameters.add(Arguments.of(listOf("TLSv1.3", "TLSv1.2"), listOf("TLSv1.3")))
                parameters.add(Arguments.of(listOf("TLSv1.3", "TLSv1.2"), listOf("TLSv1.2")))
                parameters.add(Arguments.of(listOf("TLSv1.3", "TLSv1.2"), listOf("TLSv1.2", "TLSv1.3")))
                parameters.add(Arguments.of(listOf("TLSv1.3", "TLSv1.2"), listOf("TLSv1.3", "TLSv1.2")))
                parameters.add(Arguments.of(listOf("TLSv1.2", "TLSv1.3"), listOf("TLSv1.3")))
                parameters.add(Arguments.of(listOf("TLSv1.2", "TLSv1.3"), listOf("TLSv1.2")))
                parameters.add(Arguments.of(listOf("TLSv1.2", "TLSv1.3"), listOf("TLSv1.2", "TLSv1.3")))
                parameters.add(Arguments.of(listOf("TLSv1.2", "TLSv1.3"), listOf("TLSv1.3", "TLSv1.2")))
            }
            return parameters.stream()
        }

        private fun getTrustingConfig(
            certStores: CertStores,
            peerCertStores: CertStores,
            tlsProtocols: List<String?>,
        ): Map<String, Any?> {
            val configs = certStores.getTrustingConfig(peerCertStores)
            return configs + sslConfig(tlsProtocols)
        }

        private fun sslConfig(tlsProtocols: List<String?>): Map<String, Any?> {
            return mapOf(
                SslConfigs.SSL_PROTOCOL_CONFIG to tlsProtocols[0],
                SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG to tlsProtocols,
            )
        }
    }
}
