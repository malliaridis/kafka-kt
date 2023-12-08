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
import java.io.File
import java.io.IOException
import java.net.InetAddress
import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.nio.channels.Channels
import java.nio.channels.SelectionKey
import java.nio.channels.SocketChannel
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import java.util.function.Supplier
import java.util.stream.Stream
import javax.net.ssl.SSLContext
import javax.net.ssl.SSLEngine
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.config.SslConfigs
import org.apache.kafka.common.config.internals.BrokerSecurityConfigs
import org.apache.kafka.common.config.types.Password
import org.apache.kafka.common.errors.InvalidConfigurationException
import org.apache.kafka.common.memory.MemoryPool
import org.apache.kafka.common.message.ApiMessageType
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.network.ChannelBuilders.serverChannelBuilder
import org.apache.kafka.common.network.NetworkTestUtils.checkClientConnection
import org.apache.kafka.common.network.NetworkTestUtils.createEchoServer
import org.apache.kafka.common.network.NetworkTestUtils.waitForChannelClose
import org.apache.kafka.common.network.NetworkTestUtils.waitForChannelReady
import org.apache.kafka.common.network.SslTransportLayerTest.FailureAction
import org.apache.kafka.common.requests.ApiVersionsResponse
import org.apache.kafka.common.security.TestSecurityConfig
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.security.ssl.DefaultSslEngineFactory
import org.apache.kafka.common.security.ssl.SslFactory
import org.apache.kafka.common.utils.Java
import org.apache.kafka.common.utils.LogContext
import org.apache.kafka.common.utils.Time
import org.apache.kafka.common.utils.Utils
import org.apache.kafka.test.TestSslUtils.CertificateBuilder
import org.apache.kafka.test.TestSslUtils.SslConfigsBuilder
import org.apache.kafka.test.TestSslUtils.TestSslEngineFactory
import org.apache.kafka.test.TestSslUtils.convertToPem
import org.apache.kafka.test.TestUtils.randomString
import org.apache.kafka.test.TestUtils.waitForCondition
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assumptions
import org.junit.jupiter.api.extension.ExtensionContext
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.ArgumentsProvider
import org.junit.jupiter.params.provider.ArgumentsSource
import kotlin.math.min
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

class SslTransportLayerTest {

    private lateinit var server: NioEchoServer

    private lateinit var selector: Selector

    @AfterEach
    @Throws(Exception::class)
    fun teardown() {
        if (::selector.isInitialized) selector.close()
        if (::server.isInitialized) server.close()
    }

    /**
     * Tests that server certificate with SubjectAltName containing the valid hostname
     * is accepted by a client that connects using the hostname and validates server endpoint.
     */
    @ParameterizedTest
    @ArgumentsSource(SslTransportLayerArgumentsProvider::class)
    @Throws(Exception::class)
    fun testValidEndpointIdentificationSanDns(args: Args) {
        createSelector(args)
        val node = "0"
        server = createEchoServer(args, SecurityProtocol.SSL)
        args.sslClientConfigs[SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG] = "HTTPS"
        createSelector(args.sslClientConfigs)
        val addr = InetSocketAddress("localhost", server.port)
        selector.connect(
            id = node,
            address = addr,
            sendBufferSize = BUFFER_SIZE,
            receiveBufferSize = BUFFER_SIZE,
        )
        checkClientConnection(
            selector = selector,
            node = node,
            minMessageSize = 100,
            messageCount = 10,
        )
        server.verifyAuthenticationMetrics(successfulAuthentications = 1, failedAuthentications = 0)
    }

    /**
     * Tests that server certificate with SubjectAltName containing valid IP address
     * is accepted by a client that connects using IP address and validates server endpoint.
     */
    @ParameterizedTest
    @ArgumentsSource(SslTransportLayerArgumentsProvider::class)
    @Throws(Exception::class)
    fun testValidEndpointIdentificationSanIp(args: Args) {
        val node = "0"
        args.serverCertStores = certBuilder(true, "server", args.useInlinePem)
            .hostAddress(InetAddress.getByName("127.0.0.1"))
            .build()
        args.clientCertStores = certBuilder(false, "client", args.useInlinePem)
            .hostAddress(InetAddress.getByName("127.0.0.1"))
            .build()
        args.sslServerConfigs = args.getTrustingConfig(
            certStores = args.serverCertStores,
            peerCertStores = args.clientCertStores,
        ).toMutableMap()
        args.sslClientConfigs = args.getTrustingConfig(
            certStores = args.clientCertStores,
            peerCertStores = args.serverCertStores,
        ).toMutableMap()
        server = createEchoServer(args, SecurityProtocol.SSL)
        
        args.sslClientConfigs[SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG] = "HTTPS"
        createSelector(args.sslClientConfigs)
        
        val addr = InetSocketAddress("127.0.0.1", server.port)
        selector.connect(
            id = node,
            address = addr,
            sendBufferSize = BUFFER_SIZE,
            receiveBufferSize = BUFFER_SIZE,
        )
        checkClientConnection(
            selector = selector,
            node = node,
            minMessageSize = 100,
            messageCount = 10,
        )
    }

    /**
     * Tests that server certificate with CN containing valid hostname
     * is accepted by a client that connects using hostname and validates server endpoint.
     */
    @ParameterizedTest
    @ArgumentsSource(SslTransportLayerArgumentsProvider::class)
    @Throws(Exception::class)
    fun testValidEndpointIdentificationCN(args: Args) {
        args.serverCertStores = certBuilder(
            isServer = true,
            cn = "localhost",
            useInlinePem = args.useInlinePem,
        ).build()
        args.clientCertStores = certBuilder(
            isServer = false,
            cn = "localhost",
            useInlinePem = args.useInlinePem,
        ).build()
        args.sslServerConfigs = args.getTrustingConfig(
            certStores = args.serverCertStores,
            peerCertStores = args.clientCertStores,
        ).toMutableMap()
        args.sslClientConfigs = args.getTrustingConfig(
            certStores = args.clientCertStores,
            peerCertStores = args.serverCertStores,
        ).toMutableMap()
        args.sslClientConfigs[SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG] = "HTTPS"
        verifySslConfigs(args)
    }

    /**
     * Tests that hostname verification is performed on the host name or address
     * specified by the client without using reverse DNS lookup. Certificate is
     * created with hostname, client connection uses IP address. Endpoint validation
     * must fail.
     */
    @ParameterizedTest
    @ArgumentsSource(SslTransportLayerArgumentsProvider::class)
    @Throws(Exception::class)
    fun testEndpointIdentificationNoReverseLookup(args: Args) {
        val node = "0"
        server = createEchoServer(args, SecurityProtocol.SSL)
        args.sslClientConfigs[SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG] = "HTTPS"
        createSelector(args.sslClientConfigs)
        val addr = InetSocketAddress("127.0.0.1", server.port)
        selector.connect(
            id = node,
            address = addr,
            sendBufferSize = BUFFER_SIZE,
            receiveBufferSize = BUFFER_SIZE,
        )
        waitForChannelClose(
            selector = selector,
            node = node,
            channelState = ChannelState.State.AUTHENTICATION_FAILED,
        )
    }

    /**
     * According to RFC 2818:
     * > Typically, the server has no external knowledge of what the client's identity ought to be and
     * so checks (other than that the client has a certificate chain rooted in an appropriate CA) are not possible.
     * If a server has such knowledge (typically from some source external to HTTP or TLS) it SHOULD check
     * the identity as described above.
     *
     * However, Java SSL engine does not perform any endpoint validation for client IP address. Hence it is safe
     * to avoid reverse DNS lookup while creating the SSL engine. This test checks that client validation does not fail
     * even if the client certificate has an invalid hostname. This test is to ensure that if client endpoint validation
     * is added to Java in future, we can detect and update Kafka SSL code to enable validation on the server-side
     * and provide hostname if required.
     */
    @ParameterizedTest
    @ArgumentsSource(SslTransportLayerArgumentsProvider::class)
    @Throws(Exception::class)
    fun testClientEndpointNotValidated(args: Args) {
        val node = "0"

        // Create client certificate with an invalid hostname
        args.clientCertStores = certBuilder(
            isServer = false,
            cn = "non-existent.com",
            useInlinePem = args.useInlinePem,
        ).build()
        args.serverCertStores = certBuilder(
            isServer = true,
            cn = "localhost",
            useInlinePem = args.useInlinePem,
        ).build()
        args.sslServerConfigs = args.getTrustingConfig(
            certStores = args.serverCertStores,
            peerCertStores = args.clientCertStores,
        ).toMutableMap()
        args.sslClientConfigs = args.getTrustingConfig(
            certStores = args.clientCertStores,
            peerCertStores = args.serverCertStores,
        ).toMutableMap()

        // Create a server with endpoint validation enabled on the server SSL engine
        val serverChannelBuilder = object : TestSslChannelBuilder(Mode.SERVER) {
            @Throws(IOException::class)
            override fun newTransportLayer(
                id: String,
                key: SelectionKey,
                sslEngine: SSLEngine,
            ): TestSslTransportLayer {
                val sslParams = sslEngine.getSSLParameters()
                sslParams.endpointIdentificationAlgorithm = "HTTPS"
                sslEngine.setSSLParameters(sslParams)
                return super.newTransportLayer(id, key, sslEngine)
            }
        }
        serverChannelBuilder.configure(args.sslServerConfigs)
        server = NioEchoServer(
            listenerName = ListenerName.forSecurityProtocol(SecurityProtocol.SSL),
            securityProtocol = SecurityProtocol.SSL,
            config = TestSecurityConfig(args.sslServerConfigs),
            serverHost = "localhost",
            channelBuilder = serverChannelBuilder,
            credentialCache = null,
            time = time,
        )
        server.start()
        createSelector(args.sslClientConfigs)
        val addr = InetSocketAddress("localhost", server.port)
        selector.connect(node, addr, BUFFER_SIZE, BUFFER_SIZE)
        checkClientConnection(selector, node, 100, 10)
    }

    /**
     * Tests that server certificate with invalid host name is not accepted by
     * a client that validates server endpoint. Server certificate uses
     * wrong hostname as common name to trigger endpoint validation failure.
     */
    @ParameterizedTest
    @ArgumentsSource(SslTransportLayerArgumentsProvider::class)
    @Throws(Exception::class)
    fun testInvalidEndpointIdentification(args: Args) {
        args.serverCertStores = certBuilder(
            isServer = true,
            cn = "server",
            useInlinePem = args.useInlinePem,
        ).addHostName("notahost")
            .build()
        args.clientCertStores = certBuilder(
            isServer = false,
            cn = "client",
            useInlinePem = args.useInlinePem
        ).addHostName("localhost")
            .build()
        args.sslServerConfigs = args.getTrustingConfig(
            certStores = args.serverCertStores,
            peerCertStores = args.clientCertStores,
        ).toMutableMap()
        args.sslClientConfigs = args.getTrustingConfig(
            certStores = args.clientCertStores,
            peerCertStores = args.serverCertStores,
        ).toMutableMap()
        args.sslClientConfigs[SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG] = "HTTPS"
        verifySslConfigsWithHandshakeFailure(args)
    }

    /**
     * Tests that server certificate with invalid host name is accepted by a client that has disabled
     * endpoint validation.
     */
    @ParameterizedTest
    @ArgumentsSource(SslTransportLayerArgumentsProvider::class)
    @Throws(Exception::class)
    fun testEndpointIdentificationDisabled(args: Args) {
        args.serverCertStores = certBuilder(
            isServer = true,
            cn = "server",
            useInlinePem = args.useInlinePem,
        ).addHostName("notahost")
            .build()
        args.clientCertStores = certBuilder(
            isServer = false,
            cn = "client",
            useInlinePem = args.useInlinePem
        ).addHostName("localhost")
            .build()
        args.sslServerConfigs = args.getTrustingConfig(
            certStores = args.serverCertStores,
            peerCertStores = args.clientCertStores,
        ).toMutableMap()
        args.sslClientConfigs = args.getTrustingConfig(
            certStores = args.clientCertStores,
            peerCertStores = args.serverCertStores,
        ).toMutableMap()
        server = createEchoServer(args, SecurityProtocol.SSL)
        val addr = InetSocketAddress("localhost", server.port)

        // Disable endpoint validation, connection should succeed
        val node = "1"
        args.sslClientConfigs[SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG] = ""
        createSelector(args.sslClientConfigs)
        selector.connect(
            id = node,
            address = addr,
            sendBufferSize = BUFFER_SIZE,
            receiveBufferSize = BUFFER_SIZE,
        )
        checkClientConnection(
            selector = selector,
            node = node,
            minMessageSize = 100,
            messageCount = 10,
        )

        // Disable endpoint validation using null value, connection should succeed
        val node2 = "2"
        args.sslClientConfigs[SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG] = null
        createSelector(args.sslClientConfigs)
        selector.connect(
            id = node2,
            address = addr,
            sendBufferSize = BUFFER_SIZE,
            receiveBufferSize = BUFFER_SIZE,
        )
        checkClientConnection(
            selector = selector,
            node = node2,
            minMessageSize = 100,
            messageCount = 10,
        )

        // Connection should fail with endpoint validation enabled
        val node3 = "3"
        args.sslClientConfigs[SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG] = "HTTPS"
        createSelector(args.sslClientConfigs)
        selector.connect(
            id = node3,
            address = addr,
            sendBufferSize = BUFFER_SIZE,
            receiveBufferSize = BUFFER_SIZE,
        )
        waitForChannelClose(
            selector = selector,
            node = node3,
            channelState = ChannelState.State.AUTHENTICATION_FAILED,
        )
        selector.close()
    }

    /**
     * Tests that server accepts connections from clients with a trusted certificate when client authentication
     * is required.
     */
    @ParameterizedTest
    @ArgumentsSource(SslTransportLayerArgumentsProvider::class)
    @Throws(Exception::class)
    fun testClientAuthenticationRequiredValidProvided(args: Args) {
        args.sslServerConfigs[BrokerSecurityConfigs.SSL_CLIENT_AUTH_CONFIG] = "required"
        verifySslConfigs(args)
    }

    /**
     * Tests that disabling client authentication as a listener override has the desired effect.
     */
    @ParameterizedTest
    @ArgumentsSource(SslTransportLayerArgumentsProvider::class)
    @Throws(Exception::class)
    fun testListenerConfigOverride(args: Args) {
        val node = "0"
        val clientListenerName = ListenerName("client")
        args.sslServerConfigs[BrokerSecurityConfigs.SSL_CLIENT_AUTH_CONFIG] = "required"
        args.sslServerConfigs[clientListenerName.configPrefix() + BrokerSecurityConfigs.SSL_CLIENT_AUTH_CONFIG] = "none"

        // `client` listener is not configured at this point, so client auth should be required
        server = createEchoServer(args, SecurityProtocol.SSL)
        var addr = InetSocketAddress("localhost", server.port)

        // Connect with client auth should work fine
        createSelector(args.sslClientConfigs)
        selector.connect(
            id = node,
            address = addr,
            sendBufferSize = BUFFER_SIZE,
            receiveBufferSize = BUFFER_SIZE,
        )
        checkClientConnection(
            selector = selector,
            node = node,
            minMessageSize = 100,
            messageCount = 10,
        )
        selector.close()

        // Remove client auth, so connection should fail
        CertStores.KEYSTORE_PROPS.forEach { key -> args.sslClientConfigs.remove(key) }
        createSelector(args.sslClientConfigs)
        selector.connect(
            id = node,
            address = addr,
            sendBufferSize = BUFFER_SIZE,
            receiveBufferSize = BUFFER_SIZE,
        )
        waitForChannelClose(
            selector = selector,
            node = node,
            channelState = ChannelState.State.AUTHENTICATION_FAILED,
        )
        selector.close()
        server.close()

        // Listener-specific config should be used and client auth should be disabled
        server = createEchoServer(
            args = args,
            listenerName = clientListenerName,
            securityProtocol = SecurityProtocol.SSL,
        )
        addr = InetSocketAddress("localhost", server.port)

        // Connect without client auth should work fine now
        createSelector(args.sslClientConfigs)
        selector.connect(
            id = node,
            address = addr,
            sendBufferSize = BUFFER_SIZE,
            receiveBufferSize = BUFFER_SIZE,
        )
        checkClientConnection(
            selector = selector,
            node = node,
            minMessageSize = 100,
            messageCount = 10,
        )
    }

    /**
     * Tests that server does not accept connections from clients with an untrusted certificate
     * when client authentication is required.
     */
    @ParameterizedTest
    @ArgumentsSource(SslTransportLayerArgumentsProvider::class)
    @Throws(Exception::class)
    fun testClientAuthenticationRequiredUntrustedProvided(args: Args) {
        args.sslServerConfigs = args.serverCertStores.untrustingConfig.toMutableMap()
        args.sslServerConfigs.putAll(args.sslConfigOverrides)
        args.sslServerConfigs[BrokerSecurityConfigs.SSL_CLIENT_AUTH_CONFIG] = "required"
        verifySslConfigsWithHandshakeFailure(args)
    }

    /**
     * Tests that server does not accept connections from clients which don't provide a certificate
     * when client authentication is required.
     */
    @ParameterizedTest
    @ArgumentsSource(SslTransportLayerArgumentsProvider::class)
    @Throws(Exception::class)
    fun testClientAuthenticationRequiredNotProvided(args: Args) {
        args.sslServerConfigs[BrokerSecurityConfigs.SSL_CLIENT_AUTH_CONFIG] = "required"
        CertStores.KEYSTORE_PROPS.forEach { key -> args.sslClientConfigs.remove(key) }
        verifySslConfigsWithHandshakeFailure(args)
    }

    /**
     * Tests that server accepts connections from a client configured with an untrusted certificate
     * if client authentication is disabled
     */
    @ParameterizedTest
    @ArgumentsSource(SslTransportLayerArgumentsProvider::class)
    @Throws(Exception::class)
    fun testClientAuthenticationDisabledUntrustedProvided(args: Args) {
        args.sslServerConfigs = args.serverCertStores.untrustingConfig.toMutableMap()
        args.sslServerConfigs.putAll(args.sslConfigOverrides)
        args.sslServerConfigs[BrokerSecurityConfigs.SSL_CLIENT_AUTH_CONFIG] = "none"
        verifySslConfigs(args)
    }

    /**
     * Tests that server accepts connections from a client that does not provide a certificate if client authentication
     * is disabled
     */
    @ParameterizedTest
    @ArgumentsSource(SslTransportLayerArgumentsProvider::class)
    @Throws(Exception::class)
    fun testClientAuthenticationDisabledNotProvided(args: Args) {
        args.sslServerConfigs[BrokerSecurityConfigs.SSL_CLIENT_AUTH_CONFIG] = "none"
        CertStores.KEYSTORE_PROPS.forEach { key -> args.sslClientConfigs.remove(key) }
        verifySslConfigs(args)
    }

    /**
     * Tests that server accepts connections from a client configured with a valid certificate if client authentication
     * is requested.
     */
    @ParameterizedTest
    @ArgumentsSource(SslTransportLayerArgumentsProvider::class)
    @Throws(Exception::class)
    fun testClientAuthenticationRequestedValidProvided(args: Args) {
        args.sslServerConfigs[BrokerSecurityConfigs.SSL_CLIENT_AUTH_CONFIG] = "requested"
        verifySslConfigs(args)
    }

    /**
     * Tests that server accepts connections from a client that does not provide a certificate if client authentication
     * is requested but not required.
     */
    @ParameterizedTest
    @ArgumentsSource(SslTransportLayerArgumentsProvider::class)
    @Throws(Exception::class)
    fun testClientAuthenticationRequestedNotProvided(args: Args) {
        args.sslServerConfigs[BrokerSecurityConfigs.SSL_CLIENT_AUTH_CONFIG] = "requested"
        CertStores.KEYSTORE_PROPS.forEach { key -> args.sslClientConfigs.remove(key) }
        verifySslConfigs(args)
    }

    /**
     * Tests key-pair created using DSA.
     */
    @ParameterizedTest
    @ArgumentsSource(SslTransportLayerArgumentsProvider::class)
    @Throws(Exception::class)
    fun testDsaKeyPair(args: Args) {
        // DSA algorithms are not supported for TLSv1.3.
        Assumptions.assumeTrue(args.tlsProtocol == "TLSv1.2")
        args.serverCertStores = certBuilder(
            isServer = true,
            cn = "server",
            useInlinePem = args.useInlinePem,
        ).keyAlgorithm("DSA")
            .build()
        args.clientCertStores = certBuilder(
            isServer = false,
            cn = "client",
            useInlinePem = args.useInlinePem,
        ).keyAlgorithm("DSA")
            .build()
        args.sslServerConfigs = args.getTrustingConfig(
            certStores = args.serverCertStores,
            peerCertStores = args.clientCertStores,
        ).toMutableMap()
        args.sslClientConfigs = args.getTrustingConfig(
            certStores = args.clientCertStores,
            peerCertStores = args.serverCertStores,
        ).toMutableMap()
        args.sslServerConfigs[BrokerSecurityConfigs.SSL_CLIENT_AUTH_CONFIG] = "required"
        verifySslConfigs(args)
    }

    /**
     * Tests key-pair created using EC.
     */
    @ParameterizedTest
    @ArgumentsSource(SslTransportLayerArgumentsProvider::class)
    @Throws(Exception::class)
    fun testECKeyPair(args: Args) {
        args.serverCertStores = certBuilder(
            isServer = true,
            cn = "server",
            useInlinePem = args.useInlinePem,
        ).keyAlgorithm("EC")
            .build()
        args.clientCertStores = certBuilder(
            isServer = false,
            cn = "client",
            useInlinePem = args.useInlinePem,
        ).keyAlgorithm("EC")
            .build()
        args.sslServerConfigs = args.getTrustingConfig(
            certStores = args.serverCertStores,
            peerCertStores = args.clientCertStores,
        ).toMutableMap()
        args.sslClientConfigs = args.getTrustingConfig(
            certStores = args.clientCertStores,
            peerCertStores = args.serverCertStores,
        ).toMutableMap()
        args.sslServerConfigs[BrokerSecurityConfigs.SSL_CLIENT_AUTH_CONFIG] = "required"
        verifySslConfigs(args)
    }

    /**
     * Tests PEM key store and trust store files which don't have store passwords.
     */
    @ParameterizedTest
    @ArgumentsSource(SslTransportLayerArgumentsProvider::class)
    @Throws(Exception::class)
    fun testPemFiles(args: Args) {
        convertToPem(
            sslProps = args.sslServerConfigs,
            writeToFile = true,
            encryptPrivateKey = true,
        )
        convertToPem(
            sslProps = args.sslClientConfigs,
            writeToFile = true,
            encryptPrivateKey = true,
        )
        args.sslServerConfigs[BrokerSecurityConfigs.SSL_CLIENT_AUTH_CONFIG] = "required"
        verifySslConfigs(args)
    }

    /**
     * Test with PEM key store files without key password for client key store.
     */
    @ParameterizedTest
    @ArgumentsSource(SslTransportLayerArgumentsProvider::class)
    @Throws(Exception::class)
    fun testPemFilesWithoutClientKeyPassword(args: Args) {
        val useInlinePem = args.useInlinePem
        convertToPem(
            sslProps = args.sslServerConfigs,
            writeToFile = !useInlinePem,
            encryptPrivateKey = true,
        )
        convertToPem(
            sslProps = args.sslClientConfigs,
            writeToFile = !useInlinePem,
            encryptPrivateKey = false,
        )
        args.sslServerConfigs[BrokerSecurityConfigs.SSL_CLIENT_AUTH_CONFIG] = "required"
        server = createEchoServer(args, SecurityProtocol.SSL)
        verifySslConfigs(args)
    }

    /**
     * Test with PEM key store files without key password for server key store.We don't allow this
     * with PEM files since unprotected private key on disk is not safe.
     */
    @ParameterizedTest
    @ArgumentsSource(SslTransportLayerArgumentsProvider::class)
    @Throws(Exception::class)
    fun testPemFilesWithoutServerKeyPassword(args: Args) {
        convertToPem(
            sslProps = args.sslServerConfigs,
            writeToFile = !args.useInlinePem,
            encryptPrivateKey = false,
        )
        convertToPem(
            sslProps = args.sslClientConfigs,
            writeToFile = !args.useInlinePem,
            encryptPrivateKey = true,
        )
        verifySslConfigs(args)
    }

    /**
     * Tests that an invalid SecureRandom implementation cannot be configured
     */
    @ParameterizedTest
    @ArgumentsSource(SslTransportLayerArgumentsProvider::class)
    fun testInvalidSecureRandomImplementation(args: Args) {
        newClientChannelBuilder().use { channelBuilder ->
            args.sslClientConfigs[SslConfigs.SSL_SECURE_RANDOM_IMPLEMENTATION_CONFIG] = "invalid"
            assertFailsWith<KafkaException> { channelBuilder.configure(args.sslClientConfigs) }
        }
    }

    /**
     * Tests that channels cannot be created if truststore cannot be loaded
     */
    @ParameterizedTest
    @ArgumentsSource(SslTransportLayerArgumentsProvider::class)
    fun testInvalidTruststorePassword(args: Args) {
        newClientChannelBuilder().use { channelBuilder ->
            args.sslClientConfigs[SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG] = "invalid"
            assertFailsWith<KafkaException> { channelBuilder.configure(args.sslClientConfigs) }
        }
    }

    /**
     * Tests that channels cannot be created if keystore cannot be loaded
     */
    @ParameterizedTest
    @ArgumentsSource(SslTransportLayerArgumentsProvider::class)
    fun testInvalidKeystorePassword(args: Args) {
        newClientChannelBuilder().use { channelBuilder ->
            args.sslClientConfigs[SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG] = "invalid"
            assertFailsWith<KafkaException> { channelBuilder.configure(args.sslClientConfigs) }
        }
    }

    /**
     * Tests that client connections can be created to a server
     * if null truststore password is used
     */
    @ParameterizedTest
    @ArgumentsSource(SslTransportLayerArgumentsProvider::class)
    @Throws(Exception::class)
    fun testNullTruststorePassword(args: Args) {
        args.sslClientConfigs.remove(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG)
        args.sslServerConfigs.remove(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG)
        verifySslConfigs(args)
    }

    /**
     * Tests that client connections cannot be created to a server
     * if key password is invalid
     */
    @ParameterizedTest
    @ArgumentsSource(SslTransportLayerArgumentsProvider::class)
    @Throws(Exception::class)
    fun testInvalidKeyPassword(args: Args) {
        args.sslServerConfigs[SslConfigs.SSL_KEY_PASSWORD_CONFIG] = Password("invalid")
        if (args.useInlinePem) {
            // We fail fast for PEM
            assertFailsWith<InvalidConfigurationException> { createEchoServer(args, SecurityProtocol.SSL) }
            return
        }
        verifySslConfigsWithHandshakeFailure(args)
    }

    /**
     * Tests that connection succeeds with the default TLS version.
     */
    @ParameterizedTest
    @ArgumentsSource(SslTransportLayerArgumentsProvider::class)
    @Throws(Exception::class)
    fun testTlsDefaults(args: Args) {
        args.sslServerConfigs = args.serverCertStores.getTrustingConfig(args.clientCertStores).toMutableMap()
        args.sslClientConfigs = args.clientCertStores.getTrustingConfig(args.serverCertStores).toMutableMap()
        assertEquals(SslConfigs.DEFAULT_SSL_PROTOCOL, args.sslServerConfigs[SslConfigs.SSL_PROTOCOL_CONFIG])
        assertEquals(SslConfigs.DEFAULT_SSL_PROTOCOL, args.sslClientConfigs[SslConfigs.SSL_PROTOCOL_CONFIG])
        
        server = createEchoServer(args, SecurityProtocol.SSL)
        createSelector(args.sslClientConfigs)
        val addr = InetSocketAddress("localhost", server.port)
        selector.connect(
            id = "0",
            address = addr,
            sendBufferSize = BUFFER_SIZE,
            receiveBufferSize = BUFFER_SIZE,
        )
        checkClientConnection(
            selector = selector,
            node = "0",
            minMessageSize = 10,
            messageCount = 100,
        )
        server.verifyAuthenticationMetrics(successfulAuthentications = 1, failedAuthentications = 0)
        selector.close()
    }

    /** Checks connection failed using the specified `tlsVersion`.  */
    @Throws(IOException::class)
    private fun checkAuthenticationFailed(args: Args, node: String, tlsVersion: String) {
        args.sslClientConfigs[SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG] = listOf(tlsVersion)
        createSelector(args.sslClientConfigs)
        val addr = InetSocketAddress("localhost", server.port)
        selector.connect(
            id = node,
            address = addr,
            sendBufferSize = BUFFER_SIZE,
            receiveBufferSize = BUFFER_SIZE
        )
        waitForChannelClose(
            selector = selector,
            node = node,
            channelState = ChannelState.State.AUTHENTICATION_FAILED,
        )
        selector.close()
    }

    /**
     * Tests that connections cannot be made with unsupported TLS cipher suites
     */
    @ParameterizedTest
    @ArgumentsSource(SslTransportLayerArgumentsProvider::class)
    @Throws(Exception::class)
    fun testUnsupportedCiphers(args: Args) {
        val context = SSLContext.getInstance(args.tlsProtocol)
        context.init(null, null, null)
        val cipherSuites = context.defaultSSLParameters.cipherSuites
        args.sslServerConfigs[SslConfigs.SSL_CIPHER_SUITES_CONFIG] = listOf(cipherSuites[0])
        server = createEchoServer(args, SecurityProtocol.SSL)
        args.sslClientConfigs[SslConfigs.SSL_CIPHER_SUITES_CONFIG] = listOf(cipherSuites[1])
        createSelector(args.sslClientConfigs)
        checkAuthenticationFailed(
            args = args,
            node = "1",
            tlsVersion = args.tlsProtocol,
        )
        server.verifyAuthenticationMetrics(successfulAuthentications = 0, failedAuthentications = 1)
    }

    @ParameterizedTest
    @ArgumentsSource(SslTransportLayerArgumentsProvider::class)
    @Throws(Exception::class)
    fun testServerRequestMetrics(args: Args) {
        val node = "0"
        server = createEchoServer(args, SecurityProtocol.SSL)
        createSelector(
            sslClientConfigs = args.sslClientConfigs,
            netReadBufSize = 16384,
            netWriteBufSize = 16384,
            appBufSize = 16384,
        )
        val addr = InetSocketAddress("localhost", server.port)
        selector.connect(
            id = node,
            address = addr,
            sendBufferSize = 102400,
            receiveBufferSize = 102400,
        )
        waitForChannelReady(selector, node)
        val messageSize = 1024 * 1024
        val message = randomString(messageSize)
        selector.send(
            NetworkSend(
                destinationId = node,
                send = ByteBufferSend.sizePrefixed(ByteBuffer.wrap(message.toByteArray())),
            )
        )
        while (selector.completedReceives().isEmpty()) selector.poll(100L)
        val totalBytes = messageSize + 4 // including 4-byte size
        server.waitForMetric(name = "incoming-byte", expectedValue = totalBytes.toDouble())
        server.waitForMetric(name = "outgoing-byte", expectedValue = totalBytes.toDouble())
        server.waitForMetric(name = "request", expectedValue = 1.0)
        server.waitForMetric(name = "response", expectedValue = 1.0)
    }

    /**
     * selector.poll() should be able to fetch more data than netReadBuffer from the socket.
     */
    @ParameterizedTest
    @ArgumentsSource(SslTransportLayerArgumentsProvider::class)
    @Throws(Exception::class)
    fun testSelectorPollReadSize(args: Args) {
        val node = "0"
        server = createEchoServer(args, SecurityProtocol.SSL)
        createSelector(
            sslClientConfigs = args.sslClientConfigs,
            netReadBufSize = 16384,
            netWriteBufSize = 16384,
            appBufSize = 16384,
        )
        val addr = InetSocketAddress("localhost", server.port)
        selector.connect(
            id = node,
            address = addr,
            sendBufferSize = 102400,
            receiveBufferSize = 102400,
        )
        checkClientConnection(
            selector = selector,
            node = node,
            minMessageSize = 81920,
            messageCount = 1,
        )

        // Send a message of 80K. This is 5X as large as the socket buffer. It should take at least three
        // selector.poll() to read this message from socket if the SslTransportLayer.read() does not read
        // all data from socket buffer.
        val message = randomString(81920)
        selector.send(
            NetworkSend(
                destinationId = node,
                send = ByteBufferSend.sizePrefixed(ByteBuffer.wrap(message.toByteArray())),
            )
        )

        // Send the message to echo server
        waitForCondition(
            testCondition = {
                try {
                    selector.poll(100L)
                } catch (e: IOException) {
                    return@waitForCondition false
                }
                selector.completedSends().size > 0
            },
            conditionDetails = "Timed out waiting for message to be sent",
        )

        // Wait for echo server to send the message back
        waitForCondition(
            testCondition = { server.numSent >= 2 },
            conditionDetails = "Timed out waiting for echo server to send message",
        )

        // Read the message from socket with only one poll()
        selector.poll(1000L)
        val receiveList = selector.completedReceives()
        assertEquals(1, receiveList.size)
        assertEquals(message, String(Utils.toArray(receiveList.first().payload()!!)))
    }

    /**
     * Tests that IOExceptions from read during SSL handshake are not treated as authentication failures.
     */
    @ParameterizedTest
    @ArgumentsSource(SslTransportLayerArgumentsProvider::class)
    @Throws(Exception::class)
    fun testIOExceptionsDuringHandshakeRead(args: Args) {
        server = createEchoServer(args, SecurityProtocol.SSL)
        testIOExceptionsDuringHandshake(
            args = args,
            readFailureAction = FailureAction.THROW_IO_EXCEPTION,
            flushFailureAction = FailureAction.NO_OP,
        )
    }

    /**
     * Tests that IOExceptions from write during SSL handshake are not treated as authentication failures.
     */
    @ParameterizedTest
    @ArgumentsSource(SslTransportLayerArgumentsProvider::class)
    @Throws(Exception::class)
    fun testIOExceptionsDuringHandshakeWrite(args: Args) {
        server = createEchoServer(args, SecurityProtocol.SSL)
        testIOExceptionsDuringHandshake(
            args = args,
            readFailureAction = FailureAction.NO_OP,
            flushFailureAction = FailureAction.THROW_IO_EXCEPTION,
        )
    }

    /**
     * Tests that if the remote end closes connection ungracefully  during SSL handshake while reading data,
     * the disconnection is not treated as an authentication failure.
     */
    @ParameterizedTest
    @ArgumentsSource(SslTransportLayerArgumentsProvider::class)
    @Throws(Exception::class)
    fun testUngracefulRemoteCloseDuringHandshakeRead(args: Args) {
        server = createEchoServer(args, SecurityProtocol.SSL)
        testIOExceptionsDuringHandshake(
            args = args,
            readFailureAction = { server.closeSocketChannels() },
            flushFailureAction = FailureAction.NO_OP,
        )
    }

    /**
     * Tests that if the remote end closes connection ungracefully during SSL handshake while writing data,
     * the disconnection is not treated as an authentication failure.
     */
    @ParameterizedTest
    @ArgumentsSource(SslTransportLayerArgumentsProvider::class)
    @Throws(Exception::class)
    fun testUngracefulRemoteCloseDuringHandshakeWrite(args: Args) {
        server = createEchoServer(args, SecurityProtocol.SSL)
        testIOExceptionsDuringHandshake(
            args = args,
            readFailureAction = FailureAction.NO_OP
        ) { server.closeSocketChannels() }
    }

    /**
     * Tests handling of BUFFER_UNDERFLOW during unwrap when network read buffer is smaller than
     * SSL session packet buffer size.
     */
    @ParameterizedTest
    @ArgumentsSource(SslTransportLayerArgumentsProvider::class)
    @Throws(Exception::class)
    fun testNetReadBufferResize(args: Args) {
        val node = "0"
        server = createEchoServer(args, SecurityProtocol.SSL)
        createSelector(
            sslClientConfigs = args.sslClientConfigs,
            netReadBufSize = 10,
            netWriteBufSize = null,
            appBufSize = null,
        )
        val addr = InetSocketAddress("localhost", server.port)
        selector.connect(
            id = node,
            address = addr,
            sendBufferSize = BUFFER_SIZE,
            receiveBufferSize = BUFFER_SIZE,
        )
        checkClientConnection(
            selector = selector,
            node = node,
            minMessageSize = 64000,
            messageCount = 10,
        )
    }

    /**
     * Tests handling of BUFFER_OVERFLOW during wrap when network write buffer is smaller than
     * SSL session packet buffer size.
     */
    @ParameterizedTest
    @ArgumentsSource(SslTransportLayerArgumentsProvider::class)
    @Throws(Exception::class)
    fun testNetWriteBufferResize(args: Args) {
        val node = "0"
        server = createEchoServer(args, SecurityProtocol.SSL)
        createSelector(
            sslClientConfigs = args.sslClientConfigs,
            netReadBufSize = null,
            netWriteBufSize = 10,
            appBufSize = null,
        )
        val addr = InetSocketAddress("localhost", server.port)
        selector.connect(
            id = node,
            address = addr,
            sendBufferSize = BUFFER_SIZE,
            receiveBufferSize = BUFFER_SIZE,
        )
        checkClientConnection(
            selector = selector,
            node = node,
            minMessageSize = 64000,
            messageCount = 10,
        )
    }

    /**
     * Tests handling of BUFFER_OVERFLOW during unwrap when application read buffer is smaller than
     * SSL session application buffer size.
     */
    @ParameterizedTest
    @ArgumentsSource(SslTransportLayerArgumentsProvider::class)
    @Throws(Exception::class)
    fun testApplicationBufferResize(args: Args) {
        val node = "0"
        server = createEchoServer(args, SecurityProtocol.SSL)
        createSelector(
            sslClientConfigs = args.sslClientConfigs,
            netReadBufSize = null,
            netWriteBufSize = null,
            appBufSize = 10,
        )
        val addr = InetSocketAddress("localhost", server.port)
        selector.connect(
            id = node,
            address = addr,
            sendBufferSize = BUFFER_SIZE,
            receiveBufferSize = BUFFER_SIZE,
        )
        checkClientConnection(
            selector = selector,
            node = node,
            minMessageSize = 64000,
            messageCount = 10,
        )
    }

    /**
     * Tests that time spent on the network thread is accumulated on each channel
     */
    @ParameterizedTest
    @ArgumentsSource(SslTransportLayerArgumentsProvider::class)
    @Throws(Exception::class)
    fun testNetworkThreadTimeRecorded(args: Args) {
        val logContext = LogContext()
        val channelBuilder = SslChannelBuilder(
            mode = Mode.CLIENT,
            listenerName = null,
            isInterBrokerListener = false,
            logContext = logContext,
        )
        channelBuilder.configure(args.sslClientConfigs)
        Selector(
            maxReceiveSize = NetworkReceive.UNLIMITED,
            connectionMaxIdleMs = Selector.NO_IDLE_TIMEOUT_MS,
            metrics = Metrics(),
            time = Time.SYSTEM,
            metricGrpPrefix = "MetricGroup",
            metricTags = mutableMapOf(),
            metricsPerConnection = false,
            recordTimePerConnection = true,
            channelBuilder = channelBuilder,
            memoryPool = MemoryPool.NONE,
            logContext = logContext,
        ).use { selector ->
            val node = "0"
            server = createEchoServer(args, SecurityProtocol.SSL)
            val addr = InetSocketAddress("localhost", server.port)
            selector.connect(
                id = node,
                address = addr,
                sendBufferSize = BUFFER_SIZE,
                receiveBufferSize = BUFFER_SIZE,
            )
            val message = randomString(1024 * 1024)
            waitForChannelReady(selector, node)
            val channel = assertNotNull(selector.channel(node))
            assertTrue(channel.getAndResetNetworkThreadTimeNanos() > 0, "SSL handshake time not recorded")
            assertEquals(0, channel.getAndResetNetworkThreadTimeNanos(), "Time not reset")
            selector.mute(node)
            selector.send(
                NetworkSend(
                    destinationId = node,
                    send = ByteBufferSend.sizePrefixed(ByteBuffer.wrap(message.toByteArray())),
                )
            )
            while (selector.completedSends().isEmpty()) selector.poll(100L)
            val sendTimeNanos = channel.getAndResetNetworkThreadTimeNanos()
            
            assertTrue(sendTimeNanos > 0, "Send time not recorded: $sendTimeNanos")
            assertEquals(0, channel.getAndResetNetworkThreadTimeNanos(), "Time not reset")
            assertFalse(channel.hasBytesBuffered(), "Unexpected bytes buffered")
            assertEquals(0, selector.completedReceives().size)
            
            selector.unmute(node)
            // Wait for echo server to send the message back
            waitForCondition(
                testCondition = {
                    try {
                        selector.poll(100L)
                    } catch (_: IOException) {
                        return@waitForCondition false
                    }
                    !selector.completedReceives().isEmpty()
                },
                conditionDetails = "Timed out waiting for a message to receive from echo server",
            )
            val receiveTimeNanos = channel.getAndResetNetworkThreadTimeNanos()
            assertTrue(receiveTimeNanos > 0, "Receive time not recorded: $receiveTimeNanos")
        }
    }

    /**
     * Tests that if the remote end closes the connection during SSL handshake while reading data,
     * the disconnection is not treated as an authentication failure.
     */
    @ParameterizedTest
    @ArgumentsSource(SslTransportLayerArgumentsProvider::class)
    @Throws(Exception::class)
    fun testGracefulRemoteCloseDuringHandshakeRead(args: Args) {
        server = createEchoServer(args, SecurityProtocol.SSL)
        testIOExceptionsDuringHandshake(
            args = args,
            readFailureAction = FailureAction.NO_OP
        ) { server.closeKafkaChannels() }
    }

    /**
     * Tests that if the remote end closes the connection during SSL handshake while writing data,
     * the disconnection is not treated as an authentication failure.
     */
    @ParameterizedTest
    @ArgumentsSource(SslTransportLayerArgumentsProvider::class)
    @Throws(Exception::class)
    fun testGracefulRemoteCloseDuringHandshakeWrite(args: Args) {
        server = createEchoServer(args, SecurityProtocol.SSL)
        testIOExceptionsDuringHandshake(
            args = args,
            readFailureAction = { server.closeKafkaChannels() },
            flushFailureAction = FailureAction.NO_OP,
        )
    }

    @Throws(java.lang.Exception::class)
    private fun testIOExceptionsDuringHandshake(
        args: Args,
        readFailureAction: FailureAction,
        flushFailureAction: FailureAction,
    ) {
        val channelBuilder = TestSslChannelBuilder(Mode.CLIENT)
        var done = false
        for (i in 1..100) {
            val node = i.toString()
            channelBuilder.readFailureAction = readFailureAction
            channelBuilder.flushFailureAction = flushFailureAction
            channelBuilder.failureIndex = i.toLong()
            channelBuilder.configure(args.sslClientConfigs)
            selector = Selector(
                connectionMaxIdleMs = 5000L,
                metrics = Metrics(),
                time = time,
                metricGrpPrefix = "MetricGroup",
                channelBuilder = channelBuilder,
                logContext = LogContext(),
            )
            val addr = InetSocketAddress("localhost", server.port)
            selector.connect(
                id = node,
                address = addr,
                sendBufferSize = BUFFER_SIZE,
                receiveBufferSize = BUFFER_SIZE,
            )
            for (j in 0..29) {
                selector.poll(1000L)
                val channel = selector.channel(node)
                if (channel != null && channel.ready()) {
                    done = true
                    break
                }
                if (selector.disconnected().containsKey(node)) {
                    val state = selector.disconnected()[node]!!.state
                    assertTrue(
                        actual = state == ChannelState.State.AUTHENTICATE || state == ChannelState.State.READY,
                        message = "Unexpected channel state $state",
                    )
                    break
                }
            }
            val channel = selector.channel(node)
            if (channel != null) assertTrue(
                actual = channel.ready(),
                message = "Channel not ready or disconnected:" + channel.state.state,
            )
            selector.close()
        }
        assertTrue(done, "Too many invocations of read/write during SslTransportLayer.handshake()")
    }

    /**
     * Tests that handshake failures are propagated only after writes complete, even when
     * there are delays in writes to ensure that clients see an authentication exception
     * rather than a connection failure.
     */
    @ParameterizedTest
    @ArgumentsSource(SslTransportLayerArgumentsProvider::class)
    @Throws(Exception::class)
    fun testPeerNotifiedOfHandshakeFailure(args: Args) {
        args.sslServerConfigs = args.serverCertStores.untrustingConfig.toMutableMap()
        args.sslServerConfigs.putAll(args.sslConfigOverrides)
        args.sslServerConfigs[BrokerSecurityConfigs.SSL_CLIENT_AUTH_CONFIG] = "required"

        // Test without delay and a couple of delay counts to ensure delay applies to handshake failure
        for (i in 0..2) {
            val node = i.toString()
            val serverChannelBuilder = TestSslChannelBuilder(Mode.SERVER)
            serverChannelBuilder.configure(args.sslServerConfigs)
            serverChannelBuilder.flushDelayCount = i
            server = NioEchoServer(
                listenerName = ListenerName.forSecurityProtocol(SecurityProtocol.SSL),
                securityProtocol = SecurityProtocol.SSL,
                config = TestSecurityConfig(args.sslServerConfigs),
                serverHost = "localhost",
                channelBuilder = serverChannelBuilder,
                credentialCache = null,
                time = time,
            )
            server.start()
            createSelector(args.sslClientConfigs)
            val addr = InetSocketAddress("localhost", server.port)
            selector.connect(
                id = node,
                address = addr,
                sendBufferSize = BUFFER_SIZE,
                receiveBufferSize = BUFFER_SIZE,
            )
            waitForChannelClose(
                selector = selector,
                node = node,
                channelState = ChannelState.State.AUTHENTICATION_FAILED,
            )
            server.close()
            selector.close()
            serverChannelBuilder.close()
        }
    }

    @ParameterizedTest
    @ArgumentsSource(SslTransportLayerArgumentsProvider::class)
    @Throws(Exception::class)
    fun testPeerNotifiedOfHandshakeFailureWithClientSideDelay(args: Args) {
        args.sslServerConfigs[BrokerSecurityConfigs.SSL_CLIENT_AUTH_CONFIG] = "required"
        CertStores.KEYSTORE_PROPS.forEach { key -> args.sslClientConfigs.remove(key) }
        verifySslConfigsWithHandshakeFailure(args, 1)
    }

    @ParameterizedTest
    @ArgumentsSource(SslTransportLayerArgumentsProvider::class)
    @Throws(Exception::class)
    fun testCloseSsl(args: Args) {
        testClose(
            args = args,
            securityProtocol = SecurityProtocol.SSL,
            clientChannelBuilder = newClientChannelBuilder(),
        )
    }

    @ParameterizedTest
    @ArgumentsSource(SslTransportLayerArgumentsProvider::class)
    @Throws(Exception::class)
    fun testClosePlaintext(args: Args) {
        testClose(
            args = args,
            securityProtocol = SecurityProtocol.PLAINTEXT,
            clientChannelBuilder = PlaintextChannelBuilder(null),
        )
    }

    private fun newClientChannelBuilder(): SslChannelBuilder = SslChannelBuilder(
        mode = Mode.CLIENT,
        listenerName = null,
        isInterBrokerListener = false,
        logContext = LogContext()
    )

    @Throws(java.lang.Exception::class)
    private fun testClose(args: Args, securityProtocol: SecurityProtocol, clientChannelBuilder: ChannelBuilder) {
        val node = "0"
        server = createEchoServer(args, securityProtocol)
        clientChannelBuilder.configure(args.sslClientConfigs)
        selector = Selector(
            connectionMaxIdleMs = 5000L,
            metrics = Metrics(),
            time = time,
            metricGrpPrefix = "MetricGroup",
            channelBuilder = clientChannelBuilder,
            logContext = LogContext(),
        )
        val addr = InetSocketAddress("localhost", server.port)
        selector.connect(
            id = node,
            address = addr,
            sendBufferSize = BUFFER_SIZE,
            receiveBufferSize = BUFFER_SIZE,
        )
        waitForChannelReady(selector, node)
        // `waitForChannelReady` waits for client-side channel to be ready. This is sufficient for other tests
        // operating on the client-side channel. But here, we are muting the server-side channel below, so we
        // need to wait for the server-side channel to be ready as well.
        waitForCondition(
            testCondition = { server.selector.channels().all { it.ready() } },
            conditionDetails = "Channel not ready",
        )
        val bytesOut = ByteArrayOutputStream()
        server.outputChannel(Channels.newChannel(bytesOut))
        server.selector.muteAll()
        val message = randomString(100).toByteArray()
        val count = 20
        val totalSendSize = count * (message.size + 4)
        for (i in 0 until count) {
            selector.send(NetworkSend(node, ByteBufferSend.sizePrefixed(ByteBuffer.wrap(message))))
            do { selector.poll(0L) } while (selector.completedSends().isEmpty())
        }
        server.selector.unmuteAll()
        selector.close(node)
        waitForCondition(
            testCondition = { bytesOut.toByteArray().size == totalSendSize },
            maxWaitMs = 5000,
            conditionDetails = "All requests sent were not processed",
        )
    }

    /**
     * Verifies that inter-broker listener with validation of truststore against keystore works
     * with configs including mutual authentication and hostname verification.
     */
    @ParameterizedTest
    @ArgumentsSource(SslTransportLayerArgumentsProvider::class)
    @Throws(Exception::class)
    fun testInterBrokerSslConfigValidation(args: Args) {
        val securityProtocol = SecurityProtocol.SSL
        args.sslServerConfigs[BrokerSecurityConfigs.SSL_CLIENT_AUTH_CONFIG] = "required"
        args.sslServerConfigs[SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG] = "HTTPS"
        args.sslServerConfigs.putAll(args.serverCertStores.keyStoreProps())
        args.sslServerConfigs.putAll(args.serverCertStores.trustStoreProps())
        args.sslClientConfigs.putAll(args.serverCertStores.keyStoreProps())
        args.sslClientConfigs.putAll(args.serverCertStores.trustStoreProps())
        val config = TestSecurityConfig(args.sslServerConfigs)
        val listenerName = ListenerName.forSecurityProtocol(securityProtocol)
        val serverChannelBuilder = serverChannelBuilder(
            listenerName = listenerName,
            isInterBrokerListener = true,
            securityProtocol = securityProtocol,
            config = config,
            credentialCache = null,
            tokenCache = null,
            time = time,
            logContext = LogContext(),
            apiVersionSupplier = defaultApiVersionsSupplier(),
        )
        server = NioEchoServer(
            listenerName = listenerName,
            securityProtocol = securityProtocol,
            config = config,
            serverHost = "localhost",
            channelBuilder = serverChannelBuilder,
            credentialCache = null,
            time = time,
        )
        server.start()
        selector = createSelector(
            sslClientConfigs = args.sslClientConfigs,
            netReadBufSize = null,
            netWriteBufSize = null,
            appBufSize = null,
        )
        val addr = InetSocketAddress("localhost", server.port)
        selector.connect(
            id = "0",
            address = addr,
            sendBufferSize = BUFFER_SIZE,
            receiveBufferSize = BUFFER_SIZE,
        )
        checkClientConnection(
            selector = selector,
            node = "0",
            minMessageSize = 100,
            messageCount = 10,
        )
    }

    /**
     * Verifies that inter-broker listener with validation of truststore against keystore
     * fails if certs from keystore are not trusted.
     */
    @ParameterizedTest
    @ArgumentsSource(SslTransportLayerArgumentsProvider::class)
    fun testInterBrokerSslConfigValidationFailure(args: Args) {
        val securityProtocol = SecurityProtocol.SSL
        args.sslServerConfigs.put(BrokerSecurityConfigs.SSL_CLIENT_AUTH_CONFIG, "required")
        val config = TestSecurityConfig(args.sslServerConfigs)
        val listenerName = ListenerName.forSecurityProtocol(securityProtocol)
        assertFailsWith<KafkaException> {
            serverChannelBuilder(
                listenerName = listenerName,
                isInterBrokerListener = true,
                securityProtocol = securityProtocol,
                config = config,
                credentialCache = null,
                tokenCache = null,
                time = time,
                logContext = LogContext(),
                apiVersionSupplier = defaultApiVersionsSupplier(),
            )
        }
    }

    /**
     * Tests reconfiguration of server keystore. Verifies that existing connections continue
     * to work with old keystore and new connections work with new keystore.
     */
    @ParameterizedTest
    @ArgumentsSource(SslTransportLayerArgumentsProvider::class)
    @Throws(Exception::class)
    fun testServerKeystoreDynamicUpdate(args: Args) {
        val securityProtocol = SecurityProtocol.SSL
        val config = TestSecurityConfig(args.sslServerConfigs)
        val listenerName = ListenerName.forSecurityProtocol(securityProtocol)
        val serverChannelBuilder = serverChannelBuilder(
            listenerName = listenerName,
            isInterBrokerListener = false,
            securityProtocol = securityProtocol,
            config = config,
            credentialCache = null,
            tokenCache = null,
            time = time,
            logContext = LogContext(),
            apiVersionSupplier = defaultApiVersionsSupplier(),
        )
        server = NioEchoServer(
            listenerName = listenerName,
            securityProtocol = securityProtocol,
            config = config,
            serverHost = "localhost",
            channelBuilder = serverChannelBuilder,
            credentialCache = null,
            time = time,
        )
        server.start()
        val addr = InetSocketAddress("localhost", server.port)

        // Verify that client with matching truststore can authenticate, send and receive
        val oldNode = "0"
        val oldClientSelector = createSelector(args.sslClientConfigs)
        oldClientSelector.connect(
            id = oldNode,
            address = addr,
            sendBufferSize = BUFFER_SIZE,
            receiveBufferSize = BUFFER_SIZE,
        )
        checkClientConnection(
            selector = selector,
            node = oldNode,
            minMessageSize = 100,
            messageCount = 10,
        )
        val newServerCertStores = certBuilder(
            isServer = true,
            cn = "server",
            useInlinePem = args.useInlinePem
        ).addHostName("localhost")
            .build()
        val newKeystoreConfigs = newServerCertStores.keyStoreProps()
        assertIs<ListenerReconfigurable>(
            value = serverChannelBuilder,
            message = "SslChannelBuilder not reconfigurable",
        )
        val reconfigurableBuilder = serverChannelBuilder as ListenerReconfigurable
        assertEquals(listenerName, reconfigurableBuilder.listenerName())
        reconfigurableBuilder.validateReconfiguration(newKeystoreConfigs)
        reconfigurableBuilder.reconfigure(newKeystoreConfigs)

        // Verify that new client with old truststore fails
        oldClientSelector.connect(
            id = "1",
            address = addr,
            sendBufferSize = BUFFER_SIZE,
            receiveBufferSize = BUFFER_SIZE,
        )
        waitForChannelClose(
            selector = oldClientSelector,
            node = "1",
            channelState = ChannelState.State.AUTHENTICATION_FAILED,
        )

        // Verify that new client with new truststore can authenticate, send and receive
        args.sslClientConfigs = args.getTrustingConfig(args.clientCertStores, newServerCertStores).toMutableMap()
        val newClientSelector = createSelector(args.sslClientConfigs)
        newClientSelector.connect(
            id = "2",
            address = addr,
            sendBufferSize = BUFFER_SIZE,
            receiveBufferSize = BUFFER_SIZE,
        )
        checkClientConnection(
            selector = newClientSelector,
            node = "2",
            minMessageSize = 100,
            messageCount = 10,
        )

        // Verify that old client continues to work
        checkClientConnection(
            selector = oldClientSelector,
            node = oldNode,
            minMessageSize = 100,
            messageCount = 10,
        )
        val invalidCertStores = certBuilder(
            isServer = true,
            cn = "server",
            useInlinePem = args.useInlinePem,
        ).addHostName("127.0.0.1")
            .build()
        val invalidConfigs = args.getTrustingConfig(invalidCertStores, args.clientCertStores)
        verifyInvalidReconfigure(
            reconfigurable = reconfigurableBuilder,
            invalidConfigs = invalidConfigs,
            errorMessage = "keystore with different SubjectAltName",
        )
        val missingStoreConfigs = mapOf(
            SslConfigs.SSL_KEYSTORE_TYPE_CONFIG to "PKCS12",
            SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG to "some.keystore.path",
            SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG to Password("some.keystore.password"),
            SslConfigs.SSL_KEY_PASSWORD_CONFIG to Password("some.key.password"),
        )
        verifyInvalidReconfigure(
            reconfigurable = reconfigurableBuilder,
            invalidConfigs = missingStoreConfigs,
            errorMessage = "keystore not found",
        )

        // Verify that new connections continue to work with the server with previously configured keystore
        // after failed reconfiguration
        newClientSelector.connect(
            id = "3",
            address = addr,
            sendBufferSize = BUFFER_SIZE,
            receiveBufferSize = BUFFER_SIZE,
        )
        checkClientConnection(
            selector = newClientSelector,
            node = "3",
            minMessageSize = 100,
            messageCount = 10,
        )
    }

    @ParameterizedTest
    @ArgumentsSource(SslTransportLayerArgumentsProvider::class)
    @Throws(Exception::class)
    fun testServerKeystoreDynamicUpdateWithNewSubjectAltName(args: Args) {
        val securityProtocol = SecurityProtocol.SSL
        val config = TestSecurityConfig(args.sslServerConfigs)
        val listenerName = ListenerName.forSecurityProtocol(securityProtocol)
        val serverChannelBuilder = serverChannelBuilder(
            listenerName = listenerName,
            isInterBrokerListener = false,
            securityProtocol = securityProtocol,
            config = config,
            credentialCache = null,
            tokenCache = null,
            time = time,
            logContext = LogContext(),
            apiVersionSupplier = defaultApiVersionsSupplier(),
        )
        server = NioEchoServer(
            listenerName = listenerName,
            securityProtocol = securityProtocol,
            config = config,
            serverHost = "localhost",
            channelBuilder = serverChannelBuilder,
            credentialCache = null,
            time = time,
        )
        server.start()
        val addr = InetSocketAddress("localhost", server.port)
        var selector = createSelector(args.sslClientConfigs)
        val node1 = "1"
        selector.connect(
            id = node1,
            address = addr,
            sendBufferSize = BUFFER_SIZE,
            receiveBufferSize = BUFFER_SIZE,
        )
        checkClientConnection(
            selector = selector,
            node = node1,
            minMessageSize = 100,
            messageCount = 10,
        )
        selector.close()
        val certBuilder = CertificateBuilder().sanDnsNames("localhost", "*.example.com")
        val truststorePath = args.sslClientConfigs[SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG] as String?
        val truststoreFile = if (truststorePath != null) File(truststorePath) else null
        val builder = SslConfigsBuilder(Mode.SERVER)
            .useClientCert(false)
            .certAlias("server")
            .cn("server")
            .certBuilder(certBuilder)
            .createNewTrustStore(truststoreFile)
            .usePem(args.useInlinePem)
        val newConfigs = builder.build()
        val newKeystoreConfigs = CertStores.KEYSTORE_PROPS.associateWith { propName -> newConfigs[propName] }
        val reconfigurableBuilder = serverChannelBuilder as ListenerReconfigurable
        reconfigurableBuilder.validateReconfiguration(newKeystoreConfigs)
        reconfigurableBuilder.reconfigure(newKeystoreConfigs)
        for (propName in CertStores.TRUSTSTORE_PROPS) {
            args.sslClientConfigs[propName] = newConfigs[propName]
        }
        selector = createSelector(args.sslClientConfigs)
        val node2 = "2"
        selector.connect(
            id = node2,
            address = addr,
            sendBufferSize = BUFFER_SIZE,
            receiveBufferSize = BUFFER_SIZE,
        )
        checkClientConnection(
            selector = selector,
            node = node2,
            minMessageSize = 100,
            messageCount = 10,
        )
        val invalidBuilder = CertificateBuilder().sanDnsNames("localhost")
        if (!args.useInlinePem) builder.useExistingTrustStore(truststoreFile!!)
        val invalidConfig = builder.certBuilder(invalidBuilder).build()
        val invalidKeystoreConfigs = CertStores.KEYSTORE_PROPS.associateWith { propName -> invalidConfig[propName] }
        verifyInvalidReconfigure(
            reconfigurable = reconfigurableBuilder,
            invalidConfigs = invalidKeystoreConfigs,
            errorMessage = "keystore without existing SubjectAltName",
        )
        val node3 = "3"
        selector.connect(
            id = node3,
            address = addr,
            sendBufferSize = BUFFER_SIZE,
            receiveBufferSize = BUFFER_SIZE,
        )
        checkClientConnection(
            selector = selector,
            node = node3,
            minMessageSize = 100,
            messageCount = 10,
        )
    }

    /**
     * Tests reconfiguration of server truststore. Verifies that existing connections continue
     * to work with old truststore and new connections work with new truststore.
     */
    @ParameterizedTest
    @ArgumentsSource(SslTransportLayerArgumentsProvider::class)
    @Throws(Exception::class)
    fun testServerTruststoreDynamicUpdate(args: Args) {
        val securityProtocol = SecurityProtocol.SSL
        args.sslServerConfigs[BrokerSecurityConfigs.SSL_CLIENT_AUTH_CONFIG] = "required"
        val config = TestSecurityConfig(args.sslServerConfigs)
        val listenerName = ListenerName.forSecurityProtocol(securityProtocol)
        val serverChannelBuilder = serverChannelBuilder(
            listenerName = listenerName,
            isInterBrokerListener = false,
            securityProtocol = securityProtocol,
            config = config,
            credentialCache = null,
            tokenCache = null,
            time = time,
            logContext = LogContext(),
            apiVersionSupplier = defaultApiVersionsSupplier(),
        )
        server = NioEchoServer(
            listenerName = listenerName,
            securityProtocol = securityProtocol,
            config = config,
            serverHost = "localhost",
            channelBuilder = serverChannelBuilder,
            credentialCache = null,
            time = time,
        )
        server.start()
        val addr = InetSocketAddress("localhost", server.port)

        // Verify that client with matching keystore can authenticate, send and receive
        val oldNode = "0"
        val oldClientSelector = createSelector(args.sslClientConfigs)
        oldClientSelector.connect(
            id = oldNode,
            address = addr,
            sendBufferSize = BUFFER_SIZE,
            receiveBufferSize = BUFFER_SIZE,
        )
        checkClientConnection(
            selector = selector,
            node = oldNode,
            minMessageSize = 100,
            messageCount = 10,
        )
        val newClientCertStores = certBuilder(
            isServer = true,
            cn = "client",
            useInlinePem = args.useInlinePem,
        ).addHostName("localhost")
            .build()
        args.sslClientConfigs = args.getTrustingConfig(newClientCertStores, args.serverCertStores).toMutableMap()
        val newTruststoreConfigs = newClientCertStores.trustStoreProps()
        val reconfigurableBuilder = assertIs<ListenerReconfigurable>(
            value = serverChannelBuilder,
            message = "SslChannelBuilder not reconfigurable",
        )
        assertEquals(listenerName, reconfigurableBuilder.listenerName())
        reconfigurableBuilder.validateReconfiguration(newTruststoreConfigs)
        reconfigurableBuilder.reconfigure(newTruststoreConfigs)

        // Verify that new client with old truststore fails
        oldClientSelector.connect(
            id = "1",
            address = addr,
            sendBufferSize = BUFFER_SIZE,
            receiveBufferSize = BUFFER_SIZE,
        )
        waitForChannelClose(
            selector = oldClientSelector,
            node = "1",
            channelState = ChannelState.State.AUTHENTICATION_FAILED,
        )

        // Verify that new client with new truststore can authenticate, send and receive
        val newClientSelector = createSelector(args.sslClientConfigs)
        newClientSelector.connect(
            id = "2",
            address = addr,
            sendBufferSize = BUFFER_SIZE,
            receiveBufferSize = BUFFER_SIZE,
        )
        checkClientConnection(
            selector = newClientSelector,
            node = "2",
            minMessageSize = 100,
            messageCount = 10,
        )

        // Verify that old client continues to work
        checkClientConnection(
            selector = oldClientSelector,
            node = oldNode,
            minMessageSize = 100,
            messageCount = 10
        )
        val invalidConfigs = newTruststoreConfigs + (SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG to "INVALID_TYPE")
        verifyInvalidReconfigure(
            reconfigurable = reconfigurableBuilder,
            invalidConfigs = invalidConfigs,
            errorMessage = "invalid truststore type",
        )
        val missingStoreConfigs = mapOf(
            SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG to "PKCS12",
            SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG to "some.truststore.path",
            SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG to Password("some.truststore.password"),
        )
        verifyInvalidReconfigure(
            reconfigurable = reconfigurableBuilder,
            invalidConfigs = missingStoreConfigs,
            errorMessage = "truststore not found",
        )

        // Verify that new connections continue to work with the server with previously configured keystore
        // after failed reconfiguration
        newClientSelector.connect(
            id = "3",
            address = addr,
            sendBufferSize = BUFFER_SIZE,
            receiveBufferSize = BUFFER_SIZE,
        )
        checkClientConnection(
            selector = newClientSelector,
            node = "3",
            minMessageSize = 100,
            messageCount = 10,
        )
    }

    /**
     * Tests if client can plugin customize ssl.engine.factory
     */
    @ParameterizedTest
    @ArgumentsSource(SslTransportLayerArgumentsProvider::class)
    @Throws(Exception::class)
    fun testCustomClientSslEngineFactory(args: Args) {
        args.sslClientConfigs[SslConfigs.SSL_ENGINE_FACTORY_CLASS_CONFIG] = TestSslEngineFactory::class.java
        verifySslConfigs(args)
    }

    /**
     * Tests if server can plugin customize ssl.engine.factory
     */
    @ParameterizedTest
    @ArgumentsSource(SslTransportLayerArgumentsProvider::class)
    @Throws(Exception::class)
    fun testCustomServerSslEngineFactory(args: Args) {
        args.sslServerConfigs[SslConfigs.SSL_ENGINE_FACTORY_CLASS_CONFIG] = TestSslEngineFactory::class.java
        verifySslConfigs(args)
    }

    /**
     * Tests if client and server both can plugin customize ssl.engine.factory and talk to each other!
     */
    @ParameterizedTest
    @ArgumentsSource(SslTransportLayerArgumentsProvider::class)
    @Throws(Exception::class)
    fun testCustomClientAndServerSslEngineFactory(args: Args) {
        args.sslClientConfigs[SslConfigs.SSL_ENGINE_FACTORY_CLASS_CONFIG] = TestSslEngineFactory::class.java
        args.sslServerConfigs[SslConfigs.SSL_ENGINE_FACTORY_CLASS_CONFIG] = TestSslEngineFactory::class.java
        verifySslConfigs(args)
    }

    /**
     * Tests invalid ssl.engine.factory plugin class
     */
    @ParameterizedTest
    @ArgumentsSource(SslTransportLayerArgumentsProvider::class)
    fun testInvalidSslEngineFactory(args: Args) {
        args.sslClientConfigs[SslConfigs.SSL_ENGINE_FACTORY_CLASS_CONFIG] = String::class.java
        assertFailsWith<KafkaException> { createSelector(args.sslClientConfigs) }
    }

    private fun verifyInvalidReconfigure(
        reconfigurable: ListenerReconfigurable,
        invalidConfigs: Map<String, Any?>,
        errorMessage: String,
    ) {
        assertFailsWith<KafkaException> { reconfigurable.validateReconfiguration(invalidConfigs) }
        assertFailsWith<KafkaException> { reconfigurable.reconfigure(invalidConfigs) }
    }

    private fun createSelector(
        sslClientConfigs: Map<String, Any?>,
        netReadBufSize: Int? = null,
        netWriteBufSize: Int? = null,
        appBufSize: Int? = null,
    ): Selector {
        val channelBuilder = TestSslChannelBuilder(Mode.CLIENT)
        channelBuilder.configureBufferSizes(
            netReadBufSize = netReadBufSize,
            netWriteBufSize = netWriteBufSize,
            appBufSize = appBufSize,
        )
        channelBuilder.configure(sslClientConfigs)
        selector = Selector(
            connectionMaxIdleMs = (100 * 5000).toLong(),
            metrics = Metrics(),
            time = time,
            metricGrpPrefix = "MetricGroup",
            channelBuilder = channelBuilder,
            logContext = LogContext(),
        )
        return selector
    }

    @Throws(java.lang.Exception::class)
    private fun createEchoServer(
        args: Args,
        listenerName: ListenerName,
        securityProtocol: SecurityProtocol,
    ): NioEchoServer {
        return createEchoServer(
            listenerName = listenerName,
            securityProtocol = securityProtocol,
            serverConfig = TestSecurityConfig(args.sslServerConfigs),
            credentialCache = null,
            time = time,
        )
    }

    @Throws(java.lang.Exception::class)
    private fun createEchoServer(args: Args, securityProtocol: SecurityProtocol): NioEchoServer = createEchoServer(
        args = args,
        listenerName = ListenerName.forSecurityProtocol(securityProtocol),
        securityProtocol = securityProtocol
    )

    private fun createSelector(args: Args): Selector {
        val logContext = LogContext()
        val channelBuilder = SslChannelBuilder(
            mode = Mode.CLIENT,
            listenerName = null,
            isInterBrokerListener = false,
            logContext = logContext,
        )
        channelBuilder.configure(args.sslClientConfigs)
        selector = Selector(
            connectionMaxIdleMs = 5000L,
            metrics = Metrics(),
            time = time,
            metricGrpPrefix = "MetricGroup",
            channelBuilder = channelBuilder,
            logContext = logContext,
        )
        return selector
    }

    @Throws(java.lang.Exception::class)
    private fun verifySslConfigs(args: Args) {
        server = createEchoServer(args, SecurityProtocol.SSL)
        createSelector(args.sslClientConfigs)
        val addr = InetSocketAddress("localhost", server.port)
        val node = "0"
        selector.connect(
            id = node,
            address = addr,
            sendBufferSize = BUFFER_SIZE,
            receiveBufferSize = BUFFER_SIZE,
        )
        checkClientConnection(
            selector = selector,
            node = node,
            minMessageSize = 100,
            messageCount = 10,
        )
    }

    @Throws(java.lang.Exception::class)
    private fun verifySslConfigsWithHandshakeFailure(args: Args, pollDelayMs: Int = 0) {
        server = createEchoServer(args, SecurityProtocol.SSL)
        createSelector(args.sslClientConfigs)
        val addr = InetSocketAddress("localhost", server.port)
        val node = "0"
        selector.connect(
            id = node,
            address = addr,
            sendBufferSize = BUFFER_SIZE,
            receiveBufferSize = BUFFER_SIZE,
        )
        waitForChannelClose(
            selector = selector,
            node = node,
            channelState = ChannelState.State.AUTHENTICATION_FAILED,
            delayBetweenPollMs = pollDelayMs,
        )
        server.verifyAuthenticationMetrics(successfulAuthentications = 0, failedAuthentications = 1)
    }

    internal fun interface FailureAction {

        @Throws(IOException::class)
        fun run()

        companion object {

            val NO_OP = FailureAction {}

            val THROW_IO_EXCEPTION = FailureAction { throw IOException("Test IO exception") }
        }
    }

    private fun defaultApiVersionsSupplier(): Supplier<ApiVersionsResponse> {
        return Supplier {
            ApiVersionsResponse.defaultApiVersionsResponse(listenerType = ApiMessageType.ListenerType.ZK_BROKER)
        }
    }

    class Args(internal val tlsProtocol: String, internal val useInlinePem: Boolean) {

        internal var serverCertStores: CertStores

        internal var clientCertStores: CertStores

        var sslClientConfigs = mutableMapOf<String, Any?>()

        var sslServerConfigs = mutableMapOf<String, Any?>()

        val sslConfigOverrides = mutableMapOf<String, Any?>()

        init {
            sslConfigOverrides[SslConfigs.SSL_PROTOCOL_CONFIG] = tlsProtocol
            sslConfigOverrides[SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG] = listOf(tlsProtocol)
            // Create certificates for use by client and server. Add server cert to client truststore and vice versa.
            serverCertStores = certBuilder(true, "server", useInlinePem).addHostName("localhost").build()
            clientCertStores = certBuilder(false, "client", useInlinePem).addHostName("localhost").build()
            sslServerConfigs = getTrustingConfig(serverCertStores, clientCertStores).toMutableMap()
            sslClientConfigs = getTrustingConfig(clientCertStores, serverCertStores).toMutableMap()
            sslServerConfigs[SslConfigs.SSL_ENGINE_FACTORY_CLASS_CONFIG] = DefaultSslEngineFactory::class.java
            sslClientConfigs[SslConfigs.SSL_ENGINE_FACTORY_CLASS_CONFIG] = DefaultSslEngineFactory::class.java
        }

        fun getTrustingConfig(certStores: CertStores, peerCertStores: CertStores): Map<String, Any?> {
            val configs = certStores.getTrustingConfig(peerCertStores)
            return configs + sslConfigOverrides
        }

        override fun toString(): String = "tlsProtocol=$tlsProtocol, useInlinePem=$useInlinePem"
    }

    private class SslTransportLayerArgumentsProvider : ArgumentsProvider {

        @Throws(Exception::class)
        override fun provideArguments(context: ExtensionContext): Stream<out Arguments> {
            val parameters: MutableList<Arguments> = ArrayList()
            parameters.add(Arguments.of(Args("TLSv1.2", false)))
            parameters.add(Arguments.of(Args("TLSv1.2", true)))
            if (Java.IS_JAVA11_COMPATIBLE) {
                parameters.add(Arguments.of(Args("TLSv1.3", false)))
            }
            return parameters.stream()
        }
    }

    internal open class TestSslChannelBuilder(mode: Mode) : SslChannelBuilder(
        mode = mode,
        listenerName = null,
        isInterBrokerListener = false,
        logContext = LogContext()
    ) {

        private var netReadBufSizeOverride: Int? = null

        private var netWriteBufSizeOverride: Int? = null

        private var appBufSizeOverride: Int? = null

        internal var failureIndex = Long.MAX_VALUE

        var readFailureAction = FailureAction.NO_OP

        var flushFailureAction = FailureAction.NO_OP

        var flushDelayCount = 0

        fun configureBufferSizes(netReadBufSize: Int?, netWriteBufSize: Int?, appBufSize: Int?) {
            netReadBufSizeOverride = netReadBufSize
            netWriteBufSizeOverride = netWriteBufSize
            appBufSizeOverride = appBufSize
        }

        @Throws(IOException::class)
        override fun buildTransportLayer(
            sslFactory: SslFactory,
            id: String,
            key: SelectionKey,
            metadataRegistry: ChannelMetadataRegistry?,
        ): SslTransportLayer {
            val socketChannel = key.channel() as SocketChannel
            val sslEngine = sslFactory.createSslEngine(socketChannel.socket())
            return newTransportLayer(id, key, sslEngine)
        }

        @Throws(IOException::class)
        internal open fun newTransportLayer(
            id: String,
            key: SelectionKey,
            sslEngine: SSLEngine,
        ): TestSslTransportLayer = TestSslTransportLayer(id, key, sslEngine)

        /**
         * SSLTransportLayer with overrides for testing including:
         *
         *  * Overrides for packet and application buffer size to test buffer resize code path.
         * The overridden buffer size starts with a small value and increases in size when the buffer size
         * is retrieved to handle overflow/underflow, until the actual session buffer size is reached.
         * - IOException injection for reads and writes for testing exception handling during handshakes.
         * - Delayed writes to test handshake failure notifications to peer
         */
        internal inner class TestSslTransportLayer(
            channelId: String,
            key: SelectionKey,
            sslEngine: SSLEngine,
        ) : SslTransportLayer(
            channelId = channelId,
            key = key,
            sslEngine = sslEngine,
            metadataRegistry = DefaultChannelMetadataRegistry(),
        ) {
            
            private val netReadBufSize = ResizeableBufferSize(netReadBufSizeOverride)

            private val netWriteBufSize = ResizeableBufferSize(netWriteBufSizeOverride)

            private val appBufSize = ResizeableBufferSize(appBufSizeOverride)

            private val numReadsRemaining = AtomicLong(failureIndex)

            private val numFlushesRemaining = AtomicLong(failureIndex)

            private val numDelayedFlushesRemaining = AtomicInteger(flushDelayCount)

            override fun netReadBufferSize(): Int {
                val netReadBuffer = netReadBuffer()
                // netReadBufferSize() is invoked in SSLTransportLayer.read() prior to the read
                // operation. To avoid the read buffer being expanded too early, increase buffer size
                // only when read buffer is full. This ensures that BUFFER_UNDERFLOW is always
                // triggered in testNetReadBufferResize().
                val updateBufSize = netReadBuffer != null && !netReadBuffer.hasRemaining()
                return netReadBufSize.updateAndGet(super.netReadBufferSize(), updateBufSize)
            }

            override fun netWriteBufferSize(): Int {
                return netWriteBufSize.updateAndGet(super.netWriteBufferSize(), true)
            }

            override fun applicationBufferSize(): Int {
                return appBufSize.updateAndGet(super.applicationBufferSize(), true)
            }

            @Throws(IOException::class)
            override fun readFromSocketChannel(): Int {
                if (numReadsRemaining.decrementAndGet() == 0L && !ready()) readFailureAction.run()
                return super.readFromSocketChannel()
            }

            @Throws(IOException::class)
            override fun flush(buf: ByteBuffer): Boolean {
                if (!buf.hasRemaining()) return super.flush(buf)
                if (numFlushesRemaining.decrementAndGet() == 0L && !ready()) flushFailureAction.run()
                else if (numDelayedFlushesRemaining.getAndDecrement() != 0) return false

                resetDelayedFlush()
                return super.flush(buf)
            }

            @Throws(IOException::class)
            override fun startHandshake() {
                assertTrue(socketChannel()!!.isConnected, "SSL handshake initialized too early")
                super.startHandshake()
            }

            private fun resetDelayedFlush() {
                numDelayedFlushesRemaining.set(flushDelayCount)
            }
        }

        internal class ResizeableBufferSize(private var bufSizeOverride: Int?) {
            fun updateAndGet(actualSize: Int, update: Boolean): Int {
                var size = actualSize
                bufSizeOverride?.let {
                    if (update) bufSizeOverride = (it * 2).coerceAtMost(size)
                    size = bufSizeOverride!!
                }
                return size
            }
        }
    }
    
    companion object {

        private const val BUFFER_SIZE = 4 * 1024

        private val time = Time.SYSTEM

        private fun certBuilder(
            isServer: Boolean,
            cn: String,
            useInlinePem: Boolean,
        ): CertStores.Builder = CertStores.Builder(isServer)
            .cn(cn)
            .usePem(useInlinePem)
    }
}
