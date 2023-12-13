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

package org.apache.kafka.common.security.authenticator

import java.io.IOException
import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.security.NoSuchAlgorithmException
import java.util.Base64
import java.util.concurrent.atomic.AtomicInteger
import javax.net.ssl.SSLPeerUnverifiedException
import javax.security.auth.Subject
import javax.security.auth.callback.Callback
import javax.security.auth.callback.CallbackHandler
import javax.security.auth.callback.NameCallback
import javax.security.auth.callback.PasswordCallback
import javax.security.auth.callback.UnsupportedCallbackException
import javax.security.auth.login.AppConfigurationEntry
import javax.security.auth.login.Configuration
import javax.security.auth.login.LoginContext
import javax.security.auth.login.LoginException
import javax.security.sasl.SaslClient
import javax.security.sasl.SaslException
import org.apache.kafka.clients.NetworkClient
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.config.SaslConfigs
import org.apache.kafka.common.config.SslClientAuth
import org.apache.kafka.common.config.SslConfigs
import org.apache.kafka.common.config.internals.BrokerSecurityConfigs
import org.apache.kafka.common.config.types.Password
import org.apache.kafka.common.errors.SaslAuthenticationException
import org.apache.kafka.common.errors.SslAuthenticationException
import org.apache.kafka.common.message.ApiMessageType
import org.apache.kafka.common.message.ApiVersionsRequestData
import org.apache.kafka.common.message.ApiVersionsResponseData
import org.apache.kafka.common.message.ApiVersionsResponseData.ApiVersionCollection
import org.apache.kafka.common.message.ListOffsetsResponseData
import org.apache.kafka.common.message.ListOffsetsResponseData.ListOffsetsPartitionResponse
import org.apache.kafka.common.message.ListOffsetsResponseData.ListOffsetsTopicResponse
import org.apache.kafka.common.message.RequestHeaderData
import org.apache.kafka.common.message.SaslAuthenticateRequestData
import org.apache.kafka.common.message.SaslHandshakeRequestData
import org.apache.kafka.common.network.ByteBufferSend
import org.apache.kafka.common.network.CertStores
import org.apache.kafka.common.network.ChannelBuilder
import org.apache.kafka.common.network.ChannelBuilders
import org.apache.kafka.common.network.ChannelMetadataRegistry
import org.apache.kafka.common.network.ChannelState
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.network.Mode
import org.apache.kafka.common.network.NetworkSend
import org.apache.kafka.common.network.NetworkTestUtils
import org.apache.kafka.common.network.NetworkTestUtils.waitForChannelClose
import org.apache.kafka.common.network.NioEchoServer
import org.apache.kafka.common.network.SaslChannelBuilder
import org.apache.kafka.common.network.Selector
import org.apache.kafka.common.network.TransportLayer
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.protocol.types.SchemaException
import org.apache.kafka.common.requests.AbstractRequest
import org.apache.kafka.common.requests.AbstractResponse
import org.apache.kafka.common.requests.ApiVersionsRequest
import org.apache.kafka.common.requests.ApiVersionsResponse
import org.apache.kafka.common.requests.ListOffsetsResponse
import org.apache.kafka.common.requests.MetadataRequest
import org.apache.kafka.common.requests.RequestHeader
import org.apache.kafka.common.requests.RequestTestUtils.serializeResponseWithHeader
import org.apache.kafka.common.requests.ResponseHeader
import org.apache.kafka.common.requests.SaslAuthenticateRequest
import org.apache.kafka.common.requests.SaslHandshakeRequest
import org.apache.kafka.common.requests.SaslHandshakeResponse
import org.apache.kafka.common.security.JaasContext
import org.apache.kafka.common.security.TestSecurityConfig
import org.apache.kafka.common.security.auth.AuthenticateCallbackHandler
import org.apache.kafka.common.security.auth.AuthenticationContext
import org.apache.kafka.common.security.auth.KafkaPrincipal
import org.apache.kafka.common.security.auth.KafkaPrincipalBuilder
import org.apache.kafka.common.security.auth.Login
import org.apache.kafka.common.security.auth.SaslAuthenticationContext
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.security.authenticator.AbstractLogin.DefaultLoginCallbackHandler
import org.apache.kafka.common.security.authenticator.TestDigestLoginModule.DigestServerCallbackHandler
import org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule
import org.apache.kafka.common.security.oauthbearer.OAuthBearerTokenCallback
import org.apache.kafka.common.security.oauthbearer.internals.unsecured.OAuthBearerConfigException
import org.apache.kafka.common.security.oauthbearer.internals.unsecured.OAuthBearerIllegalTokenException
import org.apache.kafka.common.security.oauthbearer.internals.unsecured.OAuthBearerUnsecuredJws
import org.apache.kafka.common.security.oauthbearer.internals.unsecured.OAuthBearerUnsecuredLoginCallbackHandler
import org.apache.kafka.common.security.plain.PlainLoginModule
import org.apache.kafka.common.security.plain.internals.PlainServerCallbackHandler
import org.apache.kafka.common.security.scram.ScramCredential
import org.apache.kafka.common.security.scram.ScramLoginModule
import org.apache.kafka.common.security.scram.internals.ScramCredentialUtils
import org.apache.kafka.common.security.scram.internals.ScramFormatter
import org.apache.kafka.common.security.scram.internals.ScramMechanism
import org.apache.kafka.common.security.token.delegation.TokenInformation
import org.apache.kafka.common.security.token.delegation.internals.DelegationTokenCache
import org.apache.kafka.common.utils.LogContext
import org.apache.kafka.common.utils.MockTime
import org.apache.kafka.common.utils.SecurityUtils
import org.apache.kafka.common.utils.Time
import org.apache.kafka.common.utils.Utils
import org.apache.kafka.test.TestUtils
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.opentest4j.AssertionFailedError
import kotlin.random.Random
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertNotNull
import kotlin.test.assertTrue
import kotlin.test.fail

/**
 * Tests for the Sasl authenticator. These use a test harness that runs a simple socket server that echos back responses.
 */
class SaslAuthenticatorTest {

    private lateinit var server: NioEchoServer

    private var selector: Selector? = null

    private lateinit var channelBuilder: ChannelBuilder

    private lateinit var serverCertStores: CertStores

    private lateinit var clientCertStores: CertStores

    private lateinit var saslServerConfigs: MutableMap<String, Any?>

    private lateinit var saslClientConfigs: MutableMap<String, Any?>

    private lateinit var credentialCache: CredentialCache

    private var nextCorrelationId = 0

    @BeforeEach
    @Throws(Exception::class)
    fun setup() {
        LoginManager.closeAll()
        time = Time.SYSTEM
        serverCertStores = CertStores(true, "localhost")
        clientCertStores = CertStores(false, "localhost")
        saslServerConfigs = serverCertStores.getTrustingConfig(clientCertStores).toMutableMap()
        saslClientConfigs = clientCertStores.getTrustingConfig(serverCertStores).toMutableMap()
        credentialCache = CredentialCache()
        TestLogin.loginCount.set(0)
    }

    @AfterEach
    @Throws(Exception::class)
    fun teardown() {
        if (::server.isInitialized) server.close()
        selector?.close()
    }

    /**
     * Tests good path SASL/PLAIN client and server channels using SSL transport layer.
     * Also tests successful re-authentication.
     */
    @Test
    @Throws(Exception::class)
    fun testValidSaslPlainOverSsl() {
        val node = "0"
        val securityProtocol = SecurityProtocol.SASL_SSL
        configureMechanisms(clientMechanism = "PLAIN", serverMechanisms = listOf("PLAIN"))
        server = createEchoServer(securityProtocol)
        checkAuthenticationAndReauthentication(securityProtocol = securityProtocol, node = node)
    }

    /**
     * Tests good path SASL/PLAIN client and server channels using PLAINTEXT transport layer.
     * Also tests successful re-authentication.
     */
    @Test
    @Throws(Exception::class)
    fun testValidSaslPlainOverPlaintext() {
        val node = "0"
        val securityProtocol = SecurityProtocol.SASL_PLAINTEXT
        configureMechanisms(clientMechanism = "PLAIN", serverMechanisms = listOf("PLAIN"))
        server = createEchoServer(securityProtocol)
        checkAuthenticationAndReauthentication(securityProtocol = securityProtocol, node = node)
    }

    /**
     * Test SASL/PLAIN with sasl.authentication.max.receive.size config
     */
    @Test
    @Throws(Exception::class)
    fun testSaslAuthenticationMaxReceiveSize() {
        val securityProtocol = SecurityProtocol.SASL_PLAINTEXT
        configureMechanisms("PLAIN", listOf("PLAIN"))

        // test auth with 1KB receive size
        saslServerConfigs[BrokerSecurityConfigs.SASL_SERVER_MAX_RECEIVE_SIZE_CONFIG] = "1024"
        server = createEchoServer(securityProtocol)

        // test valid sasl authentication
        val node1 = "valid"
        checkAuthenticationAndReauthentication(securityProtocol = securityProtocol, node = node1)

        // test with handshake request with large mechanism string
        val bytes = ByteArray(1024)
        Random.nextBytes(bytes)
        val mechanism = String(bytes)
        val node2 = "invalid1"
        createClientConnection(SecurityProtocol.PLAINTEXT, node2)
        val handshakeRequest = buildSaslHandshakeRequest(
            mechanism = mechanism,
            version = ApiKeys.SASL_HANDSHAKE.latestVersion(),
        )
        var header = RequestHeader(
            requestApiKey = ApiKeys.SASL_HANDSHAKE,
            requestVersion = handshakeRequest.version,
            clientId = "someclient",
            correlationId = nextCorrelationId++,
        )
        var send = NetworkSend(node2, handshakeRequest.toSend(header))
        selector!!.send(send)
        //we will get exception in server and connection gets closed.
        waitForChannelClose(
            selector = selector!!,
            node = node2,
            channelState = ChannelState.READY.state,
        )
        selector!!.close()
        val node3 = "invalid2"
        createClientConnection(securityProtocol = SecurityProtocol.PLAINTEXT, node = node3)
        sendHandshakeRequestReceiveResponse(node = node3, version = ApiKeys.SASL_HANDSHAKE.latestVersion())

        // test with sasl authenticate request with large auth_byes string
        val authString = "\u0000" + TestJaasConfig.USERNAME + "\u0000" + String(bytes, StandardCharsets.UTF_8)
        val authBuf = ByteBuffer.wrap(authString.toByteArray())
        val data = SaslAuthenticateRequestData().setAuthBytes(authBuf.array())
        val request = SaslAuthenticateRequest.Builder(data).build()
        header = RequestHeader(
            requestApiKey = ApiKeys.SASL_AUTHENTICATE,
            requestVersion = request.version,
            clientId = "someclient",
            correlationId = nextCorrelationId++,
        )
        send = NetworkSend(destinationId = node3, send = request.toSend(header))
        selector!!.send(send)
        waitForChannelClose(
            selector = selector!!,
            node = node3,
            channelState = ChannelState.READY.state,
        )
        server.verifyAuthenticationMetrics(successfulAuthentications = 1, failedAuthentications = 2)
    }

    /**
     * Tests that SASL/PLAIN clients with invalid password fail authentication.
     */
    @Test
    @Throws(Exception::class)
    fun testInvalidPasswordSaslPlain() {
        val node = "0"
        val securityProtocol = SecurityProtocol.SASL_SSL
        val jaasConfig = configureMechanisms(clientMechanism = "PLAIN", serverMechanisms = listOf("PLAIN"))
        jaasConfig.setClientOptions(
            saslMechanism = "PLAIN",
            clientUsername = TestJaasConfig.USERNAME,
            clientPassword = "invalidpassword",
        )
        server = createEchoServer(securityProtocol)
        createAndCheckClientAuthenticationFailure(
            securityProtocol = securityProtocol,
            node = node,
            mechanism = "PLAIN",
            expectedErrorMessage = "Authentication failed: Invalid username or password",
        )
        server.verifyAuthenticationMetrics(successfulAuthentications = 0, failedAuthentications = 1)
        server.verifyReauthenticationMetrics(successfulReauthentications = 0, failedReauthentications = 0)
    }

    /**
     * Tests that SASL/PLAIN clients with invalid username fail authentication.
     */
    @Test
    @Throws(Exception::class)
    fun testInvalidUsernameSaslPlain() {
        val node = "0"
        val securityProtocol = SecurityProtocol.SASL_SSL
        val jaasConfig = configureMechanisms(clientMechanism = "PLAIN", serverMechanisms = listOf("PLAIN"))
        jaasConfig.setClientOptions(
            saslMechanism = "PLAIN",
            clientUsername = "invaliduser",
            clientPassword = TestJaasConfig.PASSWORD,
        )
        server = createEchoServer(securityProtocol)
        createAndCheckClientAuthenticationFailure(
            securityProtocol = securityProtocol,
            node = node,
            mechanism = "PLAIN",
            expectedErrorMessage = "Authentication failed: Invalid username or password",
        )
        server.verifyAuthenticationMetrics(successfulAuthentications = 0, failedAuthentications = 1)
        server.verifyReauthenticationMetrics(successfulReauthentications = 0, failedReauthentications = 0)
    }

    /**
     * Tests that SASL/PLAIN clients without valid username fail authentication.
     */
    @Test
    @Throws(Exception::class)
    fun testMissingUsernameSaslPlain() {
        val node = "0"
        val jaasConfig = configureMechanisms("PLAIN", listOf("PLAIN"))
        jaasConfig.setClientOptions("PLAIN", null, "mypassword")
        val securityProtocol = SecurityProtocol.SASL_SSL
        server = createEchoServer(securityProtocol)
        createSelector(securityProtocol, saslClientConfigs)
        val addr = InetSocketAddress("localhost", server.port)
        assertFailsWith<IOException>("SASL/PLAIN channel created without username") {
            selector!!.connect(
                id = node,
                address = addr,
                sendBufferSize = BUFFER_SIZE,
                receiveBufferSize = BUFFER_SIZE,
            )
        }
        assertTrue(selector!!.channels().isEmpty(), "Channels not closed")
        for (key in selector!!.keys()) assertFalse(key.isValid, "Key not cancelled")
    }

    /**
     * Tests that SASL/PLAIN clients with missing password in JAAS configuration fail authentication.
     */
    @Test
    @Throws(Exception::class)
    fun testMissingPasswordSaslPlain() {
        val node = "0"
        val jaasConfig = configureMechanisms(clientMechanism = "PLAIN", serverMechanisms = listOf("PLAIN"))
        jaasConfig.setClientOptions(
            saslMechanism = "PLAIN",
            clientUsername = "myuser",
            clientPassword = null,
        )
        val securityProtocol = SecurityProtocol.SASL_SSL
        server = createEchoServer(securityProtocol)
        createSelector(securityProtocol, saslClientConfigs)
        val addr = InetSocketAddress("localhost", server.port)
        assertFailsWith<IOException>("SASL/PLAIN channel created without password") {
            selector!!.connect(
                id = node,
                address = addr,
                sendBufferSize = BUFFER_SIZE,
                receiveBufferSize = BUFFER_SIZE,
            )
        }
    }

    /**
     * Verify that messages from SaslExceptions thrown in the server during authentication are not
     * propagated to the client since these may contain sensitive data.
     */
    @Test
    @Throws(Exception::class)
    fun testClientExceptionDoesNotContainSensitiveData() {
        InvalidScramServerCallbackHandler.reset()
        val securityProtocol = SecurityProtocol.SASL_PLAINTEXT
        val jaasConfig = configureMechanisms(
            clientMechanism = "SCRAM-SHA-256",
            serverMechanisms = listOf("SCRAM-SHA-256"),
        )
        jaasConfig.createOrUpdateEntry(
            name = TestJaasConfig.LOGIN_CONTEXT_SERVER,
            loginModule = PlainLoginModule::class.java.getName(),
            options = emptyMap(),
        )
        val callbackPrefix = ListenerName.forSecurityProtocol(securityProtocol)
            .saslMechanismConfigPrefix("SCRAM-SHA-256")
        saslServerConfigs[callbackPrefix + BrokerSecurityConfigs.SASL_SERVER_CALLBACK_HANDLER_CLASS] =
            InvalidScramServerCallbackHandler::class.java.getName()
        server = createEchoServer(securityProtocol)
        try {
            with(InvalidScramServerCallbackHandler) {
                sensitiveException =
                    IOException("Could not connect to password database localhost:8000")
                createAndCheckClientAuthenticationFailure(
                    securityProtocol = securityProtocol,
                    node = "1",
                    mechanism = "SCRAM-SHA-256",
                    expectedErrorMessage = null,
                )
                sensitiveException =
                    SaslException("Password for existing user ${TestServerCallbackHandler.USERNAME} is invalid")
                createAndCheckClientAuthenticationFailure(
                    securityProtocol = securityProtocol,
                    node = "1",
                    mechanism = "SCRAM-SHA-256",
                    expectedErrorMessage = null,
                )
                reset()
                clientFriendlyException = SaslAuthenticationException("Credential verification failed")
                createAndCheckClientAuthenticationFailure(
                    securityProtocol = securityProtocol,
                    node = "1",
                    mechanism = "SCRAM-SHA-256",
                    expectedErrorMessage = clientFriendlyException!!.message,
                )
            }
        } finally {
            InvalidScramServerCallbackHandler.reset()
        }
    }

    class InvalidScramServerCallbackHandler : AuthenticateCallbackHandler {

        override fun configure(
            configs: Map<String, *>,
            saslMechanism: String,
            jaasConfigEntries: List<AppConfigurationEntry>,
        ) = Unit

        @Throws(IOException::class)
        override fun handle(callbacks: Array<Callback>) {
            sensitiveException?.let { throw it }
            clientFriendlyException?.let { throw it }
        }

        override fun close() = reset()

        companion object {
            // We want to test three types of exceptions:
            //   1) IOException since we can throw this from callback handlers. This may be sensitive.
            //   2) SaslException (also an IOException) which may contain data from external (or JRE) servers and callbacks and may be sensitive
            //   3) SaslAuthenticationException which is from our own code and is used only for client-friendly exceptions
            // We use two different exceptions here since the only checked exception CallbackHandler can throw is IOException,
            // covering case 1) and 2). For case 3), SaslAuthenticationException is a RuntimeExceptiom.
            @Volatile
            var sensitiveException: IOException? = null

            @Volatile
            var clientFriendlyException: SaslAuthenticationException? = null

            fun reset() {
                sensitiveException = null
                clientFriendlyException = null
            }
        }
    }

    /**
     * Tests that mechanisms that are not supported in Kafka can be plugged in without modifying
     * Kafka code if Sasl client and server providers are available.
     */
    @Test
    @Throws(Exception::class)
    fun testMechanismPluggability() {
        val node = "0"
        val securityProtocol = SecurityProtocol.SASL_SSL
        configureMechanisms(clientMechanism = "DIGEST-MD5", serverMechanisms = listOf("DIGEST-MD5"))
        configureDigestMd5ServerCallback(securityProtocol)
        server = createEchoServer(securityProtocol)
        createAndCheckClientConnection(securityProtocol, node)
    }

    /**
     * Tests that servers supporting multiple SASL mechanisms work with clients using
     * any of the enabled mechanisms.
     * Also tests successful re-authentication over multiple mechanisms.
     */
    @Test
    @Throws(Exception::class)
    fun testMultipleServerMechanisms() {
        val securityProtocol = SecurityProtocol.SASL_SSL
        configureMechanisms(
            clientMechanism = "DIGEST-MD5",
            serverMechanisms = listOf("DIGEST-MD5", "PLAIN", "SCRAM-SHA-256"),
        )
        configureDigestMd5ServerCallback(securityProtocol)
        server = createEchoServer(securityProtocol)
        updateScramCredentialCache(TestJaasConfig.USERNAME, TestJaasConfig.PASSWORD)
        val node1 = "1"
        saslClientConfigs[SaslConfigs.SASL_MECHANISM] = "PLAIN"
        createAndCheckClientConnection(securityProtocol, node1)
        server.verifyAuthenticationMetrics(
            successfulAuthentications = 1,
            failedAuthentications = 0,
        )
        var selector2: Selector? = null
        var selector3: Selector? = null
        try {
            val node2 = "2"
            saslClientConfigs[SaslConfigs.SASL_MECHANISM] = "DIGEST-MD5"
            createSelector(securityProtocol, saslClientConfigs)
            selector2 = selector
            val addr = InetSocketAddress("localhost", server.port)
            selector!!.connect(
                id = node2,
                address = addr,
                sendBufferSize = BUFFER_SIZE,
                receiveBufferSize = BUFFER_SIZE,
            )
            NetworkTestUtils.checkClientConnection(
                selector = selector!!,
                node = node2,
                minMessageSize = 100,
                messageCount = 10,
            )
            selector = null // keeps it from being closed when next one is created
            server.verifyAuthenticationMetrics(
                successfulAuthentications = 2,
                failedAuthentications = 0,
            )
            val node3 = "3"
            saslClientConfigs[SaslConfigs.SASL_MECHANISM] = "SCRAM-SHA-256"
            createSelector(securityProtocol, saslClientConfigs)
            selector3 = selector
            selector!!.connect(
                id = node3,
                address = InetSocketAddress("localhost", server.port),
                sendBufferSize = BUFFER_SIZE,
                receiveBufferSize = BUFFER_SIZE,
            )
            NetworkTestUtils.checkClientConnection(
                selector = selector!!,
                node = node3,
                minMessageSize = 100,
                messageCount = 10,
            )
            server.verifyAuthenticationMetrics(
                successfulAuthentications = 3,
                failedAuthentications = 0,
            )

            /*
             * Now re-authenticate the connections. First we have to sleep long enough so
             * that the next write will cause re-authentication, which we expect to succeed.
             */
            delay((CONNECTIONS_MAX_REAUTH_MS_VALUE * 1.1).toLong())
            server.verifyReauthenticationMetrics(
                successfulReauthentications = 0,
                failedReauthentications = 0,
            )
            NetworkTestUtils.checkClientConnection(
                selector = selector2!!,
                node = node2,
                minMessageSize = 100,
                messageCount = 10,
            )
            server.verifyReauthenticationMetrics(
                successfulReauthentications = 1,
                failedReauthentications = 0,
            )
            NetworkTestUtils.checkClientConnection(
                selector = selector3!!,
                node = node3,
                minMessageSize = 100,
                messageCount = 10,
            )
            server.verifyReauthenticationMetrics(
                successfulReauthentications = 2,
                failedReauthentications = 0,
            )
        } finally {
            selector2?.close()
            selector3?.close()
        }
    }

    /**
     * Tests good path SASL/SCRAM-SHA-256 client and server channels.
     * Also tests successful re-authentication.
     */
    @Test
    @Throws(Exception::class)
    fun testValidSaslScramSha256() {
        val securityProtocol = SecurityProtocol.SASL_SSL
        configureMechanisms(
            clientMechanism = "SCRAM-SHA-256",
            serverMechanisms = listOf("SCRAM-SHA-256"),
        )
        server = createEchoServer(securityProtocol)
        updateScramCredentialCache(TestJaasConfig.USERNAME, TestJaasConfig.PASSWORD)
        checkAuthenticationAndReauthentication(securityProtocol, "0")
    }

    /**
     * Tests all supported SCRAM client and server channels. Also tests that all
     * supported SCRAM mechanisms can be supported simultaneously on a server.
     */
    @Test
    @Throws(Exception::class)
    fun testValidSaslScramMechanisms() {
        val securityProtocol = SecurityProtocol.SASL_SSL
        configureMechanisms(
            clientMechanism = "SCRAM-SHA-256",
            serverMechanisms = ScramMechanism.mechanismNames.toList(),
        )
        server = createEchoServer(securityProtocol)
        updateScramCredentialCache(TestJaasConfig.USERNAME, TestJaasConfig.PASSWORD)
        for (mechanism in ScramMechanism.mechanismNames) {
            saslClientConfigs[SaslConfigs.SASL_MECHANISM] = mechanism
            createAndCheckClientConnection(
                securityProtocol = securityProtocol,
                node = "node-$mechanism",
            )
        }
    }

    /**
     * Tests that SASL/SCRAM clients fail authentication if password is invalid.
     */
    @Test
    @Throws(Exception::class)
    fun testInvalidPasswordSaslScram() {
        val securityProtocol = SecurityProtocol.SASL_SSL
        val jaasConfig = configureMechanisms(
            clientMechanism = "SCRAM-SHA-256",
            serverMechanisms = listOf("SCRAM-SHA-256"),
        )
        val options = mapOf(
            "username" to TestJaasConfig.USERNAME,
            "password" to "invalidpassword",
        )
        jaasConfig.createOrUpdateEntry(
            name = TestJaasConfig.LOGIN_CONTEXT_CLIENT,
            loginModule = ScramLoginModule::class.java.getName(),
            options = options,
        )
        val node = "0"
        server = createEchoServer(securityProtocol)
        updateScramCredentialCache(
            username = TestJaasConfig.USERNAME,
            password = TestJaasConfig.PASSWORD,
        )
        createAndCheckClientAuthenticationFailure(
            securityProtocol = securityProtocol,
            node = node,
            mechanism = "SCRAM-SHA-256",
            expectedErrorMessage = null,
        )
        server.verifyAuthenticationMetrics(
            successfulAuthentications = 0,
            failedAuthentications = 1,
        )
        server.verifyReauthenticationMetrics(
            successfulReauthentications = 0,
            failedReauthentications = 0,
        )
    }

    /**
     * Tests that SASL/SCRAM clients without valid username fail authentication.
     */
    @Test
    @Throws(Exception::class)
    fun testUnknownUserSaslScram() {
        val securityProtocol = SecurityProtocol.SASL_SSL
        val jaasConfig = configureMechanisms(
            clientMechanism = "SCRAM-SHA-256",
            serverMechanisms = listOf("SCRAM-SHA-256"),
        )
        val options = mapOf(
            "username" to "unknownUser",
            "password" to TestJaasConfig.PASSWORD,
        )
        jaasConfig.createOrUpdateEntry(
            name = TestJaasConfig.LOGIN_CONTEXT_CLIENT,
            loginModule = ScramLoginModule::class.java.getName(),
            options = options,
        )
        val node = "0"
        server = createEchoServer(securityProtocol)
        updateScramCredentialCache(
            username = TestJaasConfig.USERNAME,
            password = TestJaasConfig.PASSWORD,
        )
        createAndCheckClientAuthenticationFailure(
            securityProtocol = securityProtocol,
            node = node,
            mechanism = "SCRAM-SHA-256",
            expectedErrorMessage = null,
        )
        server.verifyAuthenticationMetrics(
            successfulAuthentications = 0,
            failedAuthentications = 1,
        )
        server.verifyReauthenticationMetrics(
            successfulReauthentications = 0,
            failedReauthentications = 0,
        )
    }

    /**
     * Tests that SASL/SCRAM clients fail authentication if credentials are not available for
     * the specific SCRAM mechanism.
     */
    @Test
    @Throws(Exception::class)
    fun testUserCredentialsUnavailableForScramMechanism() {
        val securityProtocol = SecurityProtocol.SASL_SSL
        configureMechanisms(
            clientMechanism = "SCRAM-SHA-256",
            serverMechanisms = ScramMechanism.mechanismNames.toList(),
        )
        server = createEchoServer(securityProtocol)
        updateScramCredentialCache(
            username = TestJaasConfig.USERNAME,
            password = TestJaasConfig.PASSWORD,
        )
        server.credentialCache!!.cache(
            mechanism = ScramMechanism.SCRAM_SHA_256.mechanismName,
            credentialClass = ScramCredential::class.java,
        )!!.remove(TestJaasConfig.USERNAME)
        val node = "1"
        saslClientConfigs[SaslConfigs.SASL_MECHANISM] = "SCRAM-SHA-256"
        createAndCheckClientAuthenticationFailure(
            securityProtocol = securityProtocol,
            node = node,
            mechanism = "SCRAM-SHA-256",
            expectedErrorMessage = null,
        )
        server.verifyAuthenticationMetrics(
            successfulAuthentications = 0,
            failedAuthentications = 1,
        )
        saslClientConfigs[SaslConfigs.SASL_MECHANISM] = "SCRAM-SHA-512"
        createAndCheckClientConnection(securityProtocol, "2")
        server.verifyAuthenticationMetrics(
            successfulAuthentications = 1,
            failedAuthentications = 1,
        )
        server.verifyReauthenticationMetrics(
            successfulReauthentications = 0,
            failedReauthentications = 0,
        )
    }

    /**
     * Tests SASL/SCRAM with username containing characters that need
     * to be encoded.
     */
    @Test
    @Throws(Exception::class)
    fun testScramUsernameWithSpecialCharacters() {
        val securityProtocol = SecurityProtocol.SASL_SSL
        val username = "special user= test,scram"
        val password = "$username-password"
        val jaasConfig = configureMechanisms(
            clientMechanism = "SCRAM-SHA-256",
            serverMechanisms = listOf("SCRAM-SHA-256"),
        )
        val options = mapOf(
            "username" to username,
            "password" to password,
        )
        jaasConfig.createOrUpdateEntry(
            name = TestJaasConfig.LOGIN_CONTEXT_CLIENT,
            loginModule = ScramLoginModule::class.java.getName(),
            options = options,
        )
        server = createEchoServer(securityProtocol)
        updateScramCredentialCache(username, password)
        createAndCheckClientConnection(securityProtocol, "0")
    }

    @Test
    @Throws(Exception::class)
    fun testTokenAuthenticationOverSaslScram() {
        val securityProtocol = SecurityProtocol.SASL_SSL
        val jaasConfig = configureMechanisms(
            clientMechanism = "SCRAM-SHA-256",
            serverMechanisms = listOf("SCRAM-SHA-256"),
        )

        //create jaas config for token auth
        val tokenId = "token1"
        val tokenHmac = "abcdefghijkl"
        val options = mapOf(
            "username" to tokenId, //tokenId
            "password" to tokenHmac, //token hmac
            ScramLoginModule.TOKEN_AUTH_CONFIG to "true", //enable token authentication
        )
        jaasConfig.createOrUpdateEntry(
            name = TestJaasConfig.LOGIN_CONTEXT_CLIENT,
            loginModule = ScramLoginModule::class.java.getName(),
            options = options,
        )
        server = createEchoServer(securityProtocol)

        //Check invalid tokenId/tokenInfo in tokenCache
        createAndCheckClientConnectionFailure(
            securityProtocol = securityProtocol,
            node = "0",
        )
        server.verifyAuthenticationMetrics(
            successfulAuthentications = 0,
            failedAuthentications = 1,
        )

        //Check valid token Info and invalid credentials
        val owner = SecurityUtils.parseKafkaPrincipal("User:Owner")
        val renewer = SecurityUtils.parseKafkaPrincipal("User:Renewer1")
        val tokenInfo = TokenInformation(
            tokenId = tokenId,
            owner = owner,
            renewers = setOf(renewer),
            issueTimestamp = System.currentTimeMillis(),
            maxTimestamp = System.currentTimeMillis(),
            expiryTimestamp = System.currentTimeMillis(),
        )
        server.tokenCache.addToken(tokenId, tokenInfo)
        createAndCheckClientConnectionFailure(securityProtocol = securityProtocol, node = "0")
        server.verifyAuthenticationMetrics(
            successfulAuthentications = 0,
            failedAuthentications = 2,
        )

        //Check with valid token Info and credentials
        updateTokenCredentialCache(tokenId, tokenHmac)
        createAndCheckClientConnection(securityProtocol = securityProtocol, node = "0")
        server.verifyAuthenticationMetrics(
            successfulAuthentications = 1,
            failedAuthentications = 2,
        )
        server.verifyReauthenticationMetrics(
            successfulReauthentications = 0,
            failedReauthentications = 0,
        )
    }

    @Test
    @Throws(Exception::class)
    fun testTokenReauthenticationOverSaslScram() {
        val securityProtocol = SecurityProtocol.SASL_SSL
        val jaasConfig = configureMechanisms(
            clientMechanism = "SCRAM-SHA-256",
            serverMechanisms = listOf("SCRAM-SHA-256"),
        )

        // create jaas config for token auth
        val tokenId = "token1"
        val tokenHmac = "abcdefghijkl"
        val options = mapOf(
            "username" to tokenId, // tokenId
            "password" to tokenHmac, // token hmac
            ScramLoginModule.TOKEN_AUTH_CONFIG to "true", // enable token authentication
        )
        jaasConfig.createOrUpdateEntry(
            name = TestJaasConfig.LOGIN_CONTEXT_CLIENT,
            loginModule = ScramLoginModule::class.java.getName(),
            options = options,
        )

        // ensure re-authentication based on token expiry rather than a default value
        saslServerConfigs[BrokerSecurityConfigs.CONNECTIONS_MAX_REAUTH_MS] = Long.MAX_VALUE
        /*
         * create a token cache that adjusts the token expiration dynamically so that
         * the first time the expiry is read during authentication we use it to define a
         * session expiration time that we can then sleep through; then the second time
         * the value is read (during re-authentication) it will be in the future.
         */
        val tokenLifetime = { callNum: Int -> 10 * callNum * CONNECTIONS_MAX_REAUTH_MS_VALUE }
        val tokenCache = object : DelegationTokenCache(ScramMechanism.mechanismNames) {

            var callNum = 0

            override fun token(tokenId: String): TokenInformation {
                val baseTokenInfo = super.token(tokenId)
                val thisLifetimeMs = System.currentTimeMillis() + tokenLifetime(++callNum)
                return TokenInformation(
                    tokenId = baseTokenInfo!!.tokenId,
                    owner = baseTokenInfo.owner,
                    renewers = baseTokenInfo.renewers,
                    issueTimestamp = baseTokenInfo.issueTimestamp,
                    maxTimestamp = thisLifetimeMs,
                    expiryTimestamp = thisLifetimeMs,
                )
            }
        }
        server = createEchoServer(ListenerName.forSecurityProtocol(securityProtocol), securityProtocol, tokenCache)
        val owner = SecurityUtils.parseKafkaPrincipal("User:Owner")
        val renewer = SecurityUtils.parseKafkaPrincipal("User:Renewer1")
        val tokenInfo = TokenInformation(
            tokenId = tokenId,
            owner = owner,
            renewers = setOf(renewer),
            issueTimestamp = System.currentTimeMillis(),
            maxTimestamp = System.currentTimeMillis(),
            expiryTimestamp = System.currentTimeMillis(),
        )
        server.tokenCache.addToken(tokenId, tokenInfo)
        updateTokenCredentialCache(tokenId, tokenHmac)
        // initial authentication must succeed
        createClientConnection(securityProtocol = securityProtocol, node = "0")
        checkClientConnection("0")
        // ensure metrics are as expected before trying to re-authenticate
        server.verifyAuthenticationMetrics(
            successfulAuthentications = 1,
            failedAuthentications = 0,
        )
        server.verifyReauthenticationMetrics(
            successfulReauthentications = 0,
            failedReauthentications = 0,
        )
        /*
         * Now re-authenticate and ensure it succeeds. We have to sleep long enough so
         * that the current delegation token will be expired when the next write occurs;
         * this will trigger a re-authentication. Then the second time the delegation
         * token is read and transmitted to the server it will again have an expiration
         * date in the future.
         */
        delay(tokenLifetime(1))
        checkClientConnection("0")
        server.verifyReauthenticationMetrics(
            successfulReauthentications = 1,
            failedReauthentications = 0,
        )
    }

    /**
     * Tests that Kafka ApiVersionsRequests are handled by the SASL server authenticator
     * prior to SASL handshake flow and that subsequent authentication succeeds
     * when transport layer is PLAINTEXT. This test simulates SASL authentication using a
     * (non-SASL) PLAINTEXT client and sends ApiVersionsRequest straight after
     * connection to the server is established, before any SASL-related packets are sent.
     * This test is run with SaslHandshake version 0 and no SaslAuthenticate headers.
     */
    @Test
    @Throws(Exception::class)
    fun testUnauthenticatedApiVersionsRequestOverPlaintextHandshakeVersion0() {
        testUnauthenticatedApiVersionsRequest(
            securityProtocol = SecurityProtocol.SASL_PLAINTEXT,
            saslHandshakeVersion = 0,
        )
    }

    /**
     * See [.testUnauthenticatedApiVersionsRequestOverSslHandshakeVersion0] for test scenario.
     * This test is run with SaslHandshake version 1 and SaslAuthenticate headers.
     */
    @Test
    @Throws(Exception::class)
    fun testUnauthenticatedApiVersionsRequestOverPlaintextHandshakeVersion1() {
        testUnauthenticatedApiVersionsRequest(
            securityProtocol = SecurityProtocol.SASL_PLAINTEXT,
            saslHandshakeVersion = 1,
        )
    }

    /**
     * Tests that Kafka ApiVersionsRequests are handled by the SASL server authenticator
     * prior to SASL handshake flow and that subsequent authentication succeeds
     * when transport layer is SSL. This test simulates SASL authentication using a
     * (non-SASL) SSL client and sends ApiVersionsRequest straight after
     * SSL handshake, before any SASL-related packets are sent.
     * This test is run with SaslHandshake version 0 and no SaslAuthenticate headers.
     */
    @Test
    @Throws(Exception::class)
    fun testUnauthenticatedApiVersionsRequestOverSslHandshakeVersion0() {
        testUnauthenticatedApiVersionsRequest(
            securityProtocol = SecurityProtocol.SASL_SSL,
            saslHandshakeVersion = 0,
        )
    }

    /**
     * See [.testUnauthenticatedApiVersionsRequestOverPlaintextHandshakeVersion0] for test scenario.
     * This test is run with SaslHandshake version 1 and SaslAuthenticate headers.
     */
    @Test
    @Throws(Exception::class)
    fun testUnauthenticatedApiVersionsRequestOverSslHandshakeVersion1() {
        testUnauthenticatedApiVersionsRequest(
            securityProtocol = SecurityProtocol.SASL_SSL,
            saslHandshakeVersion = 1,
        )
    }

    /**
     * Tests that unsupported version of ApiVersionsRequest before SASL handshake request
     * returns error response and does not result in authentication failure. This test
     * is similar to [.testUnauthenticatedApiVersionsRequest]
     * where a non-SASL client is used to send requests that are processed by
     * [SaslServerAuthenticator] of the server prior to client authentication.
     */
    @Test
    @Throws(Exception::class)
    fun testApiVersionsRequestWithServerUnsupportedVersion() {
        val handshakeVersion = ApiKeys.SASL_HANDSHAKE.latestVersion()
        val securityProtocol = SecurityProtocol.SASL_PLAINTEXT
        configureMechanisms(clientMechanism = "PLAIN", serverMechanisms = listOf("PLAIN"))
        server = createEchoServer(securityProtocol)

        // Send ApiVersionsRequest with unsupported version and validate error response.
        val node = "1"
        createClientConnection(SecurityProtocol.PLAINTEXT, node)
        val header = RequestHeader(
            data = RequestHeaderData().setRequestApiKey(ApiKeys.API_VERSIONS.id)
                .setRequestApiVersion(Short.MAX_VALUE)
                .setClientId("someclient")
                .setCorrelationId(1),
            headerVersion = 2,
        )
        val request = ApiVersionsRequest.Builder().build()
        selector!!.send(NetworkSend(node, request.toSend(header)))
        val responseBuffer = waitForResponse()
        ResponseHeader.parse(
            buffer = responseBuffer!!,
            headerVersion = ApiKeys.API_VERSIONS.responseHeaderVersion(0),
        )
        val response = ApiVersionsResponse.parse(buffer = responseBuffer, version = 0)
        assertEquals(Errors.UNSUPPORTED_VERSION.code, response.data().errorCode)
        val apiVersion = response.data().apiKeys.find(ApiKeys.API_VERSIONS.id)
        assertNotNull(apiVersion)
        assertEquals(ApiKeys.API_VERSIONS.id, apiVersion.apiKey)
        assertEquals(ApiKeys.API_VERSIONS.oldestVersion(), apiVersion.minVersion)
        assertEquals(ApiKeys.API_VERSIONS.latestVersion(), apiVersion.maxVersion)

        // Send ApiVersionsRequest with a supported version. This should succeed.
        sendVersionRequestReceiveResponse(node)

        // Test that client can authenticate successfully
        sendHandshakeRequestReceiveResponse(node, handshakeVersion)
        authenticateUsingSaslPlainAndCheckConnection(
            node = node,
            enableSaslAuthenticateHeader = handshakeVersion > 0,
        )
    }

    /**
     * Tests correct negotiation of handshake and authenticate api versions by having the server
     * return a higher version than supported on the client.
     * Note, that due to KAFKA-9577 this will require a workaround to effectively bump
     * SASL_HANDSHAKE in the future.
     */
    @Test
    @Throws(Exception::class)
    fun testSaslUnsupportedClientVersions() {
        configureMechanisms(
            clientMechanism = "SCRAM-SHA-512",
            serverMechanisms = listOf("SCRAM-SHA-512"),
        )
        server = startServerApiVersionsUnsupportedByClient(
            securityProtocol = SecurityProtocol.SASL_SSL,
            saslMechanism = "SCRAM-SHA-512",
        )
        updateScramCredentialCache(
            username = TestJaasConfig.USERNAME,
            password = TestJaasConfig.PASSWORD,
        )
        val node = "0"
        createClientConnection(
            securityProtocol = SecurityProtocol.SASL_SSL,
            saslMechanism = "SCRAM-SHA-512",
            node = node,
            enableSaslAuthenticateHeader = true,
        )
        NetworkTestUtils.checkClientConnection(
            selector = selector!!,
            node = "0",
            minMessageSize = 100,
            messageCount = 10,
        )
    }

    /**
     * Tests that invalid ApiVersionRequest is handled by the server correctly and
     * returns an INVALID_REQUEST error.
     */
    @Test
    @Throws(Exception::class)
    fun testInvalidApiVersionsRequest() {
        val handshakeVersion = ApiKeys.SASL_HANDSHAKE.latestVersion()
        val securityProtocol = SecurityProtocol.SASL_PLAINTEXT
        configureMechanisms(clientMechanism = "PLAIN", serverMechanisms = listOf("PLAIN"))
        server = createEchoServer(securityProtocol)

        // Send ApiVersionsRequest with invalid version and validate error response.
        val node = "1"
        val version = ApiKeys.API_VERSIONS.latestVersion()
        createClientConnection(SecurityProtocol.PLAINTEXT, node)
        val header = RequestHeader(
            requestApiKey = ApiKeys.API_VERSIONS,
            requestVersion = version,
            clientId = "someclient",
            correlationId = 1,
        )
        val request = ApiVersionsRequest(
            data = ApiVersionsRequestData()
                .setClientSoftwareName("  ")
                .setClientSoftwareVersion("   "),
            version = version
        )
        selector!!.send(NetworkSend(node, request.toSend(header)))
        val responseBuffer = waitForResponse()
        ResponseHeader.parse(
            buffer = responseBuffer!!,
            headerVersion = ApiKeys.API_VERSIONS.responseHeaderVersion(version),
        )
        val response = ApiVersionsResponse.parse(responseBuffer, version)
        assertEquals(Errors.INVALID_REQUEST.code, response.data().errorCode)

        // Send ApiVersionsRequest with a supported version. This should succeed.
        sendVersionRequestReceiveResponse(node)

        // Test that client can authenticate successfully
        sendHandshakeRequestReceiveResponse(node, handshakeVersion)
        authenticateUsingSaslPlainAndCheckConnection(
            node = node,
            enableSaslAuthenticateHeader = handshakeVersion > 0,
        )
    }

    @Test
    fun testForBrokenSaslHandshakeVersionBump() {
        assertEquals(
            expected = 1,
            actual = ApiKeys.SASL_HANDSHAKE.latestVersion().toInt(),
            message = "It is not possible to easily bump SASL_HANDSHAKE schema due to improper version negotiation in " +
                    "clients < 2.5. Please see https://issues.apache.org/jira/browse/KAFKA-9577"
        )
    }

    /**
     * Tests that valid ApiVersionRequest is handled by the server correctly and
     * returns an NONE error.
     */
    @Test
    @Throws(Exception::class)
    fun testValidApiVersionsRequest() {
        val handshakeVersion = ApiKeys.SASL_HANDSHAKE.latestVersion()
        val securityProtocol = SecurityProtocol.SASL_PLAINTEXT
        configureMechanisms(clientMechanism = "PLAIN", serverMechanisms = listOf("PLAIN"))
        server = createEchoServer(securityProtocol)

        // Send ApiVersionsRequest with valid version and validate error response.
        val node = "1"
        val version = ApiKeys.API_VERSIONS.latestVersion()
        createClientConnection(SecurityProtocol.PLAINTEXT, node)
        val header = RequestHeader(
            requestApiKey = ApiKeys.API_VERSIONS,
            requestVersion = version,
            clientId = "someclient",
            correlationId = 1,
        )
        val request = ApiVersionsRequest.Builder().build(version)
        selector!!.send(NetworkSend(node, request.toSend(header)))
        val responseBuffer = waitForResponse()
        ResponseHeader.parse(
            buffer = responseBuffer!!,
            headerVersion = ApiKeys.API_VERSIONS.responseHeaderVersion(version),
        )
        val response = ApiVersionsResponse.parse(responseBuffer, version)
        assertEquals(Errors.NONE.code, response.data().errorCode)

        // Test that client can authenticate successfully
        sendHandshakeRequestReceiveResponse(node, handshakeVersion)
        authenticateUsingSaslPlainAndCheckConnection(
            node = node,
            enableSaslAuthenticateHeader = handshakeVersion > 0,
        )
    }

    /**
     * Tests that unsupported version of SASL handshake request returns error
     * response and fails authentication. This test is similar to
     * [.testUnauthenticatedApiVersionsRequest]
     * where a non-SASL client is used to send requests that are processed by
     * [SaslServerAuthenticator] of the server prior to client authentication.
     */
    @Test
    @Throws(Exception::class)
    fun testSaslHandshakeRequestWithUnsupportedVersion() {
        val securityProtocol = SecurityProtocol.SASL_PLAINTEXT
        configureMechanisms("PLAIN", listOf("PLAIN"))
        server = createEchoServer(securityProtocol)

        // Send SaslHandshakeRequest and validate that connection is closed by server.
        val node1 = "invalid1"
        createClientConnection(SecurityProtocol.PLAINTEXT, node1)
        val request = buildSaslHandshakeRequest(
            mechanism = "PLAIN",
            version = ApiKeys.SASL_HANDSHAKE.latestVersion(),
        )
        val header = RequestHeader(
            requestApiKey = ApiKeys.SASL_HANDSHAKE,
            requestVersion = Short.MAX_VALUE,
            clientId = "someclient",
            correlationId = 2,
        )
        selector!!.send(NetworkSend(node1, request.toSend(header)))
        // This test uses a non-SASL PLAINTEXT client in order to do manual handshake.
        // So the channel is in READY state.
        waitForChannelClose(
            selector = selector!!,
            node = node1,
            channelState = ChannelState.READY.state,
        )
        selector!!.close()

        // Test good connection still works
        createAndCheckClientConnection(securityProtocol = securityProtocol, node = "good1")
    }

    /**
     * Tests that any invalid data during Kafka SASL handshake request flow
     * or the actual SASL authentication flow result in authentication failure
     * and do not cause any failures in the server.
     */
    @Test
    @Throws(Exception::class)
    fun testInvalidSaslPacket() {
        val securityProtocol = SecurityProtocol.SASL_PLAINTEXT
        configureMechanisms(clientMechanism = "PLAIN", serverMechanisms = listOf("PLAIN"))
        server = createEchoServer(securityProtocol)

        // Send invalid SASL packet after valid handshake request
        val node1 = "invalid1"
        createClientConnection(SecurityProtocol.PLAINTEXT, node1)
        sendHandshakeRequestReceiveResponse(node = node1, version = 1)
        val bytes = ByteArray(1024)
        Random.nextBytes(bytes)
        selector!!.send(NetworkSend(node1, ByteBufferSend.sizePrefixed(ByteBuffer.wrap(bytes))))
        waitForChannelClose(
            selector = selector!!,
            node = node1,
            channelState = ChannelState.READY.state,
        )
        selector!!.close()

        // Test good connection still works
        createAndCheckClientConnection(securityProtocol = securityProtocol, node = "good1")

        // Send invalid SASL packet before handshake request
        val node2 = "invalid2"
        createClientConnection(SecurityProtocol.PLAINTEXT, node2)
        Random.nextBytes(bytes)
        selector!!.send(NetworkSend(node2, ByteBufferSend.sizePrefixed(ByteBuffer.wrap(bytes))))
        waitForChannelClose(
            selector = selector!!,
            node = node2,
            channelState = ChannelState.READY.state,
        )
        selector!!.close()

        // Test good connection still works
        createAndCheckClientConnection(securityProtocol = securityProtocol, node = "good2")
    }

    /**
     * Tests that ApiVersionsRequest after Kafka SASL handshake request flow,
     * but prior to actual SASL authentication, results in authentication failure.
     * This is similar to [.testUnauthenticatedApiVersionsRequest]
     * where a non-SASL client is used to send requests that are processed by
     * [SaslServerAuthenticator] of the server prior to client authentication.
     */
    @Test
    @Throws(Exception::class)
    fun testInvalidApiVersionsRequestSequence() {
        val securityProtocol = SecurityProtocol.SASL_PLAINTEXT
        configureMechanisms(clientMechanism = "PLAIN", serverMechanisms = listOf("PLAIN"))
        server = createEchoServer(securityProtocol)

        // Send handshake request followed by ApiVersionsRequest
        val node1 = "invalid1"
        createClientConnection(SecurityProtocol.PLAINTEXT, node1)
        sendHandshakeRequestReceiveResponse(node = node1, version = 1)
        val request = createApiVersionsRequestV0()
        val versionsHeader = RequestHeader(
            requestApiKey = ApiKeys.API_VERSIONS,
            requestVersion = request.version,
            clientId = "someclient",
            correlationId = 2,
        )
        selector!!.send(NetworkSend(node1, request.toSend(versionsHeader)))
        waitForChannelClose(
            selector = selector!!,
            node = node1,
            channelState = ChannelState.READY.state,
        )
        selector!!.close()

        // Test good connection still works
        createAndCheckClientConnection(securityProtocol = securityProtocol, node = "good1")
    }

    /**
     * Tests that packets that are too big during Kafka SASL handshake request flow
     * or the actual SASL authentication flow result in authentication failure
     * and do not cause any failures in the server.
     */
    @Test
    @Throws(Exception::class)
    fun testPacketSizeTooBig() {
        val securityProtocol = SecurityProtocol.SASL_PLAINTEXT
        configureMechanisms(clientMechanism = "PLAIN", serverMechanisms = listOf("PLAIN"))
        server = createEchoServer(securityProtocol)

        // Send SASL packet with large size after valid handshake request
        val node1 = "invalid1"
        createClientConnection(securityProtocol = SecurityProtocol.PLAINTEXT, node = node1)
        sendHandshakeRequestReceiveResponse(node = node1, version = 1)
        val buffer = ByteBuffer.allocate(1024)
        buffer.putInt(Int.MAX_VALUE)
        buffer.put(ByteArray(buffer.capacity() - 4))
        buffer.rewind()
        selector!!.send(NetworkSend(node1, ByteBufferSend.sizePrefixed(buffer)))
        waitForChannelClose(
            selector = selector!!,
            node = node1,
            channelState = ChannelState.READY.state,
        )
        selector!!.close()

        // Test good connection still works
        createAndCheckClientConnection(securityProtocol = securityProtocol, node = "good1")

        // Send packet with large size before handshake request
        val node2 = "invalid2"
        createClientConnection(securityProtocol = SecurityProtocol.PLAINTEXT, node = node2)
        buffer.clear()
        buffer.putInt(Int.MAX_VALUE)
        buffer.put(ByteArray(buffer.capacity() - 4))
        buffer.rewind()
        selector!!.send(NetworkSend(node2, ByteBufferSend.sizePrefixed(buffer)))
        waitForChannelClose(
            selector = selector!!,
            node = node2,
            channelState = ChannelState.READY.state,
        )
        selector!!.close()

        // Test good connection still works
        createAndCheckClientConnection(securityProtocol = securityProtocol, node = "good2")
    }

    /**
     * Tests that Kafka requests that are forbidden until successful authentication result
     * in authentication failure and do not cause any failures in the server.
     */
    @Test
    @Throws(Exception::class)
    fun testDisallowedKafkaRequestsBeforeAuthentication() {
        val securityProtocol = SecurityProtocol.SASL_PLAINTEXT
        configureMechanisms("PLAIN", listOf("PLAIN"))
        server = createEchoServer(securityProtocol)

        // Send metadata request before Kafka SASL handshake request
        val node1 = "invalid1"
        createClientConnection(SecurityProtocol.PLAINTEXT, node1)
        val metadataRequest1 = MetadataRequest.Builder(
            topics = listOf("sometopic"),
            allowAutoTopicCreation = true,
        ).build()
        val metadataRequestHeader1 = RequestHeader(
            requestApiKey = ApiKeys.METADATA,
            requestVersion = metadataRequest1.version,
            clientId = "someclient",
            correlationId = 1,
        )
        selector!!.send(NetworkSend(node1, metadataRequest1.toSend(metadataRequestHeader1)))
        waitForChannelClose(
            selector = selector!!,
            node = node1,
            channelState = ChannelState.READY.state,
        )
        selector!!.close()

        // Test good connection still works
        createAndCheckClientConnection(securityProtocol = securityProtocol, node = "good1")

        // Send metadata request after Kafka SASL handshake request
        val node2 = "invalid2"
        createClientConnection(securityProtocol = SecurityProtocol.PLAINTEXT, node = node2)
        sendHandshakeRequestReceiveResponse(node = node2, version = 1)
        val metadataRequest2 = MetadataRequest.Builder(
            topics = listOf("sometopic"),
            allowAutoTopicCreation = true,
        ).build()
        val metadataRequestHeader2 = RequestHeader(
            requestApiKey = ApiKeys.METADATA,
            requestVersion = metadataRequest2.version,
            clientId = "someclient",
            correlationId = 2,
        )
        selector!!.send(NetworkSend(node2, metadataRequest2.toSend(metadataRequestHeader2)))
        waitForChannelClose(
            selector = selector!!,
            node = node2,
            channelState = ChannelState.READY.state,
        )
        selector!!.close()

        // Test good connection still works
        createAndCheckClientConnection(securityProtocol = securityProtocol, node = "good2")
    }

    /**
     * Tests that connections cannot be created if the login module class is unavailable.
     */
    @Test
    @Throws(Exception::class)
    fun testInvalidLoginModule() {
        val jaasConfig = configureMechanisms(clientMechanism = "PLAIN", serverMechanisms = listOf("PLAIN"))
        jaasConfig.createOrUpdateEntry(
            name = TestJaasConfig.LOGIN_CONTEXT_CLIENT,
            loginModule = "InvalidLoginModule",
            options = TestJaasConfig.defaultClientOptions(),
        )
        val securityProtocol = SecurityProtocol.SASL_SSL
        server = createEchoServer(securityProtocol)
        assertFailsWith<KafkaException>("SASL/PLAIN channel created without valid login module") {
            createSelector(securityProtocol, saslClientConfigs)
        }
    }

    /**
     * Tests SASL client authentication callback handler override.
     */
    @Test
    @Throws(Exception::class)
    fun testClientAuthenticateCallbackHandler() {
        val securityProtocol = SecurityProtocol.SASL_PLAINTEXT
        val jaasConfig = configureMechanisms(clientMechanism = "PLAIN", serverMechanisms = listOf("PLAIN"))
        saslClientConfigs[SaslConfigs.SASL_CLIENT_CALLBACK_HANDLER_CLASS] =
            TestClientCallbackHandler::class.java.getName()
        jaasConfig.setClientOptions(
            saslMechanism = "PLAIN",
            clientUsername = "",
            clientPassword = "",
        ) // remove username, password in login context
        val options = mutableMapOf(
            "user_" + TestClientCallbackHandler.USERNAME to TestClientCallbackHandler.PASSWORD
        )

        jaasConfig.createOrUpdateEntry(
            name = TestJaasConfig.LOGIN_CONTEXT_SERVER,
            loginModule = PlainLoginModule::class.java.getName(),
            options = options,
        )
        server = createEchoServer(securityProtocol)
        createAndCheckClientConnection(securityProtocol = securityProtocol, node = "good")
        options.clear()
        options["user_" + TestClientCallbackHandler.USERNAME] = "invalid-password"
        jaasConfig.createOrUpdateEntry(
            name = TestJaasConfig.LOGIN_CONTEXT_SERVER,
            loginModule = PlainLoginModule::class.java.getName(),
            options = options,
        )
        createAndCheckClientConnectionFailure(securityProtocol = securityProtocol, node = "invalid")
    }

    /**
     * Tests SASL server authentication callback handler override.
     */
    @Test
    @Throws(Exception::class)
    fun testServerAuthenticateCallbackHandler() {
        val securityProtocol = SecurityProtocol.SASL_PLAINTEXT
        val jaasConfig = configureMechanisms(clientMechanism = "PLAIN", serverMechanisms = listOf("PLAIN"))
        jaasConfig.createOrUpdateEntry(
            name = TestJaasConfig.LOGIN_CONTEXT_SERVER,
            loginModule = PlainLoginModule::class.java.getName(),
            options = emptyMap(),
        )
        val callbackPrefix = ListenerName.forSecurityProtocol(securityProtocol).saslMechanismConfigPrefix("PLAIN")
        saslServerConfigs[callbackPrefix + BrokerSecurityConfigs.SASL_SERVER_CALLBACK_HANDLER_CLASS] =
            TestServerCallbackHandler::class.java.getName()
        server = createEchoServer(securityProtocol)

        // Set client username/password to the values used by `TestServerCallbackHandler`
        jaasConfig.setClientOptions(
            saslMechanism = "PLAIN",
            clientUsername = TestServerCallbackHandler.USERNAME,
            clientPassword = TestServerCallbackHandler.PASSWORD,
        )
        createAndCheckClientConnection(securityProtocol = securityProtocol, node = "good")

        // Set client username/password to the invalid values
        jaasConfig.setClientOptions(
            saslMechanism = "PLAIN",
            clientUsername = TestJaasConfig.USERNAME,
            clientPassword = "invalid-password",
        )
        createAndCheckClientConnectionFailure(securityProtocol = securityProtocol, node = "invalid")
    }

    /**
     * Test that callback handlers are only applied to connections for the mechanisms
     * configured for the handler. Test enables two mechanisms 'PLAIN` and `DIGEST-MD5`
     * on the servers with different callback handlers for the two mechanisms. Verifies
     * that clients using both mechanisms authenticate successfully.
     */
    @Test
    @Throws(Exception::class)
    fun testAuthenticateCallbackHandlerMechanisms() {
        val securityProtocol = SecurityProtocol.SASL_PLAINTEXT
        val jaasConfig = configureMechanisms(
            clientMechanism = "DIGEST-MD5",
            serverMechanisms = listOf("DIGEST-MD5", "PLAIN"),
        )

        // Connections should fail using the digest callback handler if listener.mechanism prefix not specified
        saslServerConfigs["plain." + BrokerSecurityConfigs.SASL_SERVER_CALLBACK_HANDLER_CLASS] =
            TestServerCallbackHandler::class.java
        saslServerConfigs["digest-md5." + BrokerSecurityConfigs.SASL_SERVER_CALLBACK_HANDLER_CLASS] =
            DigestServerCallbackHandler::class.java
        server = createEchoServer(securityProtocol)
        createAndCheckClientConnectionFailure(securityProtocol = securityProtocol, node = "invalid")

        // Connections should succeed using the server callback handler associated with the listener
        val listener = ListenerName.forSecurityProtocol(securityProtocol)
        saslServerConfigs.remove("plain." + BrokerSecurityConfigs.SASL_SERVER_CALLBACK_HANDLER_CLASS)
        saslServerConfigs.remove("digest-md5." + BrokerSecurityConfigs.SASL_SERVER_CALLBACK_HANDLER_CLASS)
        saslServerConfigs[listener.saslMechanismConfigPrefix("plain") + BrokerSecurityConfigs.SASL_SERVER_CALLBACK_HANDLER_CLASS] =
            TestServerCallbackHandler::class.java
        saslServerConfigs[listener.saslMechanismConfigPrefix("digest-md5") + BrokerSecurityConfigs.SASL_SERVER_CALLBACK_HANDLER_CLASS] =
            DigestServerCallbackHandler::class.java
        server = createEchoServer(securityProtocol)

        // Verify that DIGEST-MD5 (currently configured for client) works with `DigestServerCallbackHandler`
        createAndCheckClientConnection(securityProtocol = securityProtocol, node = "good-digest-md5")

        // Verify that PLAIN works with `TestServerCallbackHandler`
        jaasConfig.setClientOptions(
            saslMechanism = "PLAIN",
            clientUsername = TestServerCallbackHandler.USERNAME,
            clientPassword = TestServerCallbackHandler.PASSWORD,
        )
        saslClientConfigs[SaslConfigs.SASL_MECHANISM] = "PLAIN"
        createAndCheckClientConnection(securityProtocol = securityProtocol, node = "good-plain")
    }

    /**
     * Tests SASL login class override.
     */
    @Test
    @Throws(Exception::class)
    fun testClientLoginOverride() {
        val securityProtocol = SecurityProtocol.SASL_PLAINTEXT
        val jaasConfig = configureMechanisms(clientMechanism = "PLAIN", serverMechanisms = listOf("PLAIN"))
        jaasConfig.setClientOptions(
            saslMechanism = "PLAIN",
            clientUsername = "invaliduser",
            clientPassword = "invalidpassword",
        )
        server = createEchoServer(securityProtocol)

        // Connection should succeed using login override that sets correct username/password in Subject
        saslClientConfigs[SaslConfigs.SASL_LOGIN_CLASS] = TestLogin::class.java.getName()
        createAndCheckClientConnection(securityProtocol = securityProtocol, node = "1")
        assertEquals(1, TestLogin.loginCount.get())

        // Connection should fail without login override since username/password in jaas config is invalid
        saslClientConfigs.remove(SaslConfigs.SASL_LOGIN_CLASS)
        createAndCheckClientConnectionFailure(securityProtocol = securityProtocol, node = "invalid")
        assertEquals(1, TestLogin.loginCount.get())
    }

    /**
     * Tests SASL server login class override.
     */
    @Test
    @Throws(Exception::class)
    fun testServerLoginOverride() {
        val securityProtocol = SecurityProtocol.SASL_PLAINTEXT
        configureMechanisms("PLAIN", listOf("PLAIN"))
        val prefix = ListenerName.forSecurityProtocol(securityProtocol).saslMechanismConfigPrefix("PLAIN")
        saslServerConfigs[prefix + SaslConfigs.SASL_LOGIN_CLASS] = TestLogin::class.java.getName()
        server = createEchoServer(securityProtocol)

        // Login is performed when server channel builder is created (before any connections are made on the server)
        assertEquals(1, TestLogin.loginCount.get())
        createAndCheckClientConnection(securityProtocol, "1")
        assertEquals(1, TestLogin.loginCount.get())
    }

    /**
     * Tests SASL login callback class override.
     */
    @Test
    @Throws(Exception::class)
    fun testClientLoginCallbackOverride() {
        val securityProtocol = SecurityProtocol.SASL_PLAINTEXT
        val jaasConfig = configureMechanisms("PLAIN", listOf("PLAIN"))
        jaasConfig.createOrUpdateEntry(
            name = TestJaasConfig.LOGIN_CONTEXT_CLIENT,
            loginModule = TestPlainLoginModule::class.java.getName(),
            options = emptyMap(),
        )
        server = createEchoServer(securityProtocol)

        // Connection should succeed using login callback override that sets correct username/password
        saslClientConfigs[SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS] =
            TestLoginCallbackHandler::class.java.getName()
        createAndCheckClientConnection(securityProtocol, "1")

        // Connection should fail without login callback override since username/password in jaas config is invalid
        saslClientConfigs.remove(SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS)
        try {
            createClientConnection(securityProtocol, "invalid")
        } catch (e: Exception) {
            assertIs<LoginException>(e.cause, "Unexpected exception " + e.cause)
        }
    }

    /**
     * Tests SASL server login callback class override.
     */
    @Test
    @Throws(Exception::class)
    fun testServerLoginCallbackOverride() {
        val securityProtocol = SecurityProtocol.SASL_PLAINTEXT
        val jaasConfig = configureMechanisms("PLAIN", listOf("PLAIN"))
        jaasConfig.createOrUpdateEntry(
            name = TestJaasConfig.LOGIN_CONTEXT_SERVER,
            loginModule = TestPlainLoginModule::class.java.getName(),
            options = emptyMap(),
        )
        jaasConfig.setClientOptions(
            saslMechanism = "PLAIN",
            clientUsername = TestServerCallbackHandler.USERNAME,
            clientPassword = TestServerCallbackHandler.PASSWORD,
        )
        val listenerName = ListenerName.forSecurityProtocol(securityProtocol)
        val prefix = listenerName.saslMechanismConfigPrefix("PLAIN")
        saslServerConfigs[prefix + BrokerSecurityConfigs.SASL_SERVER_CALLBACK_HANDLER_CLASS] =
            TestServerCallbackHandler::class.java
        val loginCallback = TestLoginCallbackHandler::class.java

        assertFailsWith<KafkaException>("Should have failed to create server with default login handler") {
            createEchoServer(securityProtocol)
        }

        assertFailsWith<KafkaException>(
            message = "Should have failed to create server with login handler config without listener+mechanism prefix",
        ) {
            saslServerConfigs[SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS] = loginCallback
            createEchoServer(securityProtocol)
        }

        assertFailsWith<KafkaException>(
            message = "Should have failed to create server with login handler config without listener prefix",
        ) {
            saslServerConfigs.put("plain." + SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS, loginCallback)
            createEchoServer(securityProtocol)
        }
        saslServerConfigs.remove("plain." + SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS)

        assertFailsWith<KafkaException>(
            message = "Should have failed to create server with login handler config without mechanism prefix",
        ) {
            saslServerConfigs[listenerName.configPrefix() + SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS] =
                loginCallback
            createEchoServer(securityProtocol)
        }
        saslServerConfigs.remove("plain." + SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS)

        // Connection should succeed using login callback override for mechanism
        saslServerConfigs[prefix + SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS] = loginCallback
        server = createEchoServer(securityProtocol)
        createAndCheckClientConnection(securityProtocol = securityProtocol, node = "1")
    }

    /**
     * Tests that mechanisms with default implementation in Kafka may be disabled in
     * the Kafka server by removing from the enabled mechanism list.
     */
    @Test
    @Throws(Exception::class)
    fun testDisabledMechanism() {
        val node = "0"
        val securityProtocol = SecurityProtocol.SASL_SSL
        configureMechanisms(clientMechanism = "PLAIN", serverMechanisms = listOf("DIGEST-MD5"))
        server = createEchoServer(securityProtocol)
        createAndCheckClientConnectionFailure(securityProtocol, node)
        server.verifyAuthenticationMetrics(
            successfulAuthentications = 0,
            failedAuthentications = 1,
        )
        server.verifyReauthenticationMetrics(
            successfulReauthentications = 0,
            failedReauthentications = 0,
        )
    }

    /**
     * Tests that clients using invalid SASL mechanisms fail authentication.
     */
    @Test
    @Throws(Exception::class)
    fun testInvalidMechanism() {
        val node = "0"
        val securityProtocol = SecurityProtocol.SASL_SSL
        configureMechanisms("PLAIN", listOf("PLAIN"))
        saslClientConfigs[SaslConfigs.SASL_MECHANISM] = "INVALID"
        server = createEchoServer(securityProtocol)
        try {
            createAndCheckClientConnectionFailure(securityProtocol, node)
            fail("Did not generate exception prior to creating channel")
        } catch (expected: IOException) {
            server.verifyAuthenticationMetrics(
                successfulAuthentications = 0,
                failedAuthentications = 0,
            )
            server.verifyReauthenticationMetrics(
                successfulReauthentications = 0,
                failedReauthentications = 0,
            )
            val underlyingCause = expected.cause!!.cause!!.cause
            assertIs<SaslAuthenticationException>(underlyingCause)
            assertEquals("Failed to create SaslClient with mechanism INVALID", underlyingCause.message)
        } finally {
            closeClientConnectionIfNecessary()
        }
    }

    /**
     * Tests dynamic JAAS configuration property for SASL clients. Invalid client credentials
     * are set in the static JVM-wide configuration instance to ensure that the dynamic
     * property override is used during authentication.
     */
    @Test
    @Throws(Exception::class)
    fun testClientDynamicJaasConfiguration() {
        val securityProtocol = SecurityProtocol.SASL_SSL
        saslClientConfigs[SaslConfigs.SASL_MECHANISM] = "PLAIN"
        saslServerConfigs[BrokerSecurityConfigs.SASL_ENABLED_MECHANISMS_CONFIG] = mutableListOf<String>("PLAIN")
        val serverOptions = mapOf(
            "user_user1" to "user1-secret",
            "user_user2" to "user2-secret",
        )
        val staticJaasConfig = TestJaasConfig()
        staticJaasConfig.createOrUpdateEntry(
            name = TestJaasConfig.LOGIN_CONTEXT_SERVER,
            loginModule = PlainLoginModule::class.java.getName(),
            options = serverOptions,
        )
        staticJaasConfig.setClientOptions(
            saslMechanism = "PLAIN",
            clientUsername = "user1",
            clientPassword = "invalidpassword",
        )
        Configuration.setConfiguration(staticJaasConfig)
        server = createEchoServer(securityProtocol)

        // Check that client using static Jaas config does not connect since password is invalid
        createAndCheckClientConnectionFailure(securityProtocol, "1")

        // Check that 'user1' can connect with a Jaas config property override
        saslClientConfigs[SaslConfigs.SASL_JAAS_CONFIG] = TestJaasConfig.jaasConfigProperty(
            mechanism = "PLAIN",
            username = "user1",
            password = "user1-secret",
        )
        createAndCheckClientConnection(securityProtocol = securityProtocol, node = "2")

        // Check that invalid password specified as Jaas config property results in connection failure
        saslClientConfigs[SaslConfigs.SASL_JAAS_CONFIG] = TestJaasConfig.jaasConfigProperty(
            mechanism = "PLAIN",
            username = "user1",
            password = "user2-secret",
        )
        createAndCheckClientConnectionFailure(securityProtocol = securityProtocol, node = "3")

        // Check that another user 'user2' can also connect with a Jaas config override without any changes to static configuration
        saslClientConfigs[SaslConfigs.SASL_JAAS_CONFIG] = TestJaasConfig.jaasConfigProperty(
            mechanism = "PLAIN",
            username = "user2",
            password = "user2-secret",
        )
        createAndCheckClientConnection(securityProtocol = securityProtocol, node = "4")

        // Check that clients specifying multiple login modules fail even if the credentials are valid
        val module1 = TestJaasConfig.jaasConfigProperty(
            mechanism = "PLAIN",
            username = "user1",
            password = "user1-secret",
        ).value
        val module2 = TestJaasConfig.jaasConfigProperty(
            mechanism = "PLAIN",
            username = "user2",
            password = "user2-secret",
        ).value
        saslClientConfigs[SaslConfigs.SASL_JAAS_CONFIG] = Password("$module1 $module2")
        assertFailsWith<IllegalArgumentException>(
            message = "Connection created with multiple login modules in sasl.jaas.config",
        ) { createClientConnection(securityProtocol, "1") }
    }

    /**
     * Tests dynamic JAAS configuration property for SASL server. Invalid server credentials
     * are set in the static JVM-wide configuration instance to ensure that the dynamic
     * property override is used during authentication.
     */
    @Test
    @Throws(Exception::class)
    fun testServerDynamicJaasConfiguration() {
        val securityProtocol = SecurityProtocol.SASL_SSL
        saslClientConfigs[SaslConfigs.SASL_MECHANISM] = "PLAIN"
        saslServerConfigs[BrokerSecurityConfigs.SASL_ENABLED_MECHANISMS_CONFIG] = listOf("PLAIN")
        val serverOptions = mapOf<String?, String?>(
            "user_user1" to "user1-secret",
            "user_user2" to "user2-secret",
        )
        saslServerConfigs["listener.name.sasl_ssl.plain." + SaslConfigs.SASL_JAAS_CONFIG] =
            TestJaasConfig.jaasConfigProperty(mechanism = "PLAIN", options = serverOptions)
        val staticJaasConfig = TestJaasConfig()
        staticJaasConfig.createOrUpdateEntry(
            name = TestJaasConfig.LOGIN_CONTEXT_SERVER,
            loginModule = PlainLoginModule::class.java.getName(),
            options = emptyMap(),
        )
        staticJaasConfig.setClientOptions(
            saslMechanism = "PLAIN",
            clientUsername = "user1",
            clientPassword = "user1-secret",
        )
        Configuration.setConfiguration(staticJaasConfig)
        server = createEchoServer(securityProtocol)

        // Check that 'user1' can connect with static Jaas config
        createAndCheckClientConnection(securityProtocol = securityProtocol, node = "1")

        // Check that user 'user2' can also connect with a Jaas config override
        saslClientConfigs[SaslConfigs.SASL_JAAS_CONFIG] = TestJaasConfig.jaasConfigProperty(
            mechanism = "PLAIN",
            username = "user2",
            password = "user2-secret",
        )
        createAndCheckClientConnection(securityProtocol = securityProtocol, node = "2")
    }

    @Test
    @Throws(Exception::class)
    fun testJaasConfigurationForListener() {
        val securityProtocol = SecurityProtocol.SASL_PLAINTEXT
        saslClientConfigs[SaslConfigs.SASL_MECHANISM] = "PLAIN"
        saslServerConfigs[BrokerSecurityConfigs.SASL_ENABLED_MECHANISMS_CONFIG] = listOf("PLAIN")
        val staticJaasConfig = TestJaasConfig()
        val globalServerOptions = mapOf(
            "user_global1" to "gsecret1",
            "user_global2" to "gsecret2",
        )
        staticJaasConfig.createOrUpdateEntry(
            name = TestJaasConfig.LOGIN_CONTEXT_SERVER,
            loginModule = PlainLoginModule::class.java.getName(),
            options = globalServerOptions,
        )
        val clientListenerServerOptions = mapOf(
            "user_client1" to "csecret1",
            "user_client2" to "csecret2",
        )
        val clientJaasEntryName = "client." + TestJaasConfig.LOGIN_CONTEXT_SERVER
        staticJaasConfig.createOrUpdateEntry(
            name = clientJaasEntryName,
            loginModule = PlainLoginModule::class.java.getName(),
            options = clientListenerServerOptions,
        )
        Configuration.setConfiguration(staticJaasConfig)

        // Listener-specific credentials
        server = createEchoServer(ListenerName("client"), securityProtocol)
        saslClientConfigs[SaslConfigs.SASL_JAAS_CONFIG] = TestJaasConfig.jaasConfigProperty(
            mechanism = "PLAIN",
            username = "client1",
            password = "csecret1",
        )
        createAndCheckClientConnection(securityProtocol = securityProtocol, node = "1")
        saslClientConfigs[SaslConfigs.SASL_JAAS_CONFIG] = TestJaasConfig.jaasConfigProperty(
            mechanism = "PLAIN",
            username = "global1",
            password = "gsecret1",
        )
        createAndCheckClientConnectionFailure(securityProtocol = securityProtocol, node = "2")
        server.close()

        // Global credentials as there is no listener-specific JAAS entry
        server = createEchoServer(ListenerName("other"), securityProtocol)
        saslClientConfigs[SaslConfigs.SASL_JAAS_CONFIG] = TestJaasConfig.jaasConfigProperty(
            mechanism = "PLAIN",
            username = "global1",
            password = "gsecret1",
        )
        createAndCheckClientConnection(securityProtocol = securityProtocol, node = "3")
        saslClientConfigs[SaslConfigs.SASL_JAAS_CONFIG] = TestJaasConfig.jaasConfigProperty(
            mechanism = "PLAIN",
            username = "client1",
            password = "csecret1",
        )
        createAndCheckClientConnectionFailure(securityProtocol = securityProtocol, node = "4")
    }

    /**
     * Tests good path SASL/PLAIN authentication over PLAINTEXT with old version of server
     * that does not support SASL_AUTHENTICATE headers and new version of client.
     */
    @Test
    @Throws(Exception::class)
    fun oldSaslPlainPlaintextServerWithoutSaslAuthenticateHeader() {
        verifySaslAuthenticateHeaderInterop(
            enableHeaderOnServer = false,
            enableHeaderOnClient = true,
            securityProtocol = SecurityProtocol.SASL_PLAINTEXT,
            saslMechanism = "PLAIN",
        )
    }

    /**
     * Tests good path SASL/PLAIN authentication over PLAINTEXT with old version of client
     * that does not support SASL_AUTHENTICATE headers and new version of server.
     */
    @Test
    @Throws(Exception::class)
    fun oldSaslPlainPlaintextClientWithoutSaslAuthenticateHeader() {
        verifySaslAuthenticateHeaderInterop(
            enableHeaderOnServer = true,
            enableHeaderOnClient = false,
            securityProtocol = SecurityProtocol.SASL_PLAINTEXT,
            saslMechanism = "PLAIN",
        )
    }

    /**
     * Tests good path SASL/SCRAM authentication over PLAINTEXT with old version of server
     * that does not support SASL_AUTHENTICATE headers and new version of client.
     */
    @Test
    @Throws(Exception::class)
    fun oldSaslScramPlaintextServerWithoutSaslAuthenticateHeader() {
        verifySaslAuthenticateHeaderInterop(
            enableHeaderOnServer = false,
            enableHeaderOnClient = true,
            securityProtocol = SecurityProtocol.SASL_PLAINTEXT,
            saslMechanism = "SCRAM-SHA-256",
        )
    }

    /**
     * Tests good path SASL/SCRAM authentication over PLAINTEXT with old version of client
     * that does not support SASL_AUTHENTICATE headers and new version of server.
     */
    @Test
    @Throws(Exception::class)
    fun oldSaslScramPlaintextClientWithoutSaslAuthenticateHeader() {
        verifySaslAuthenticateHeaderInterop(
            enableHeaderOnServer = true,
            enableHeaderOnClient = false,
            securityProtocol = SecurityProtocol.SASL_PLAINTEXT,
            saslMechanism = "SCRAM-SHA-256",
        )
    }

    /**
     * Tests good path SASL/PLAIN authentication over SSL with old version of server
     * that does not support SASL_AUTHENTICATE headers and new version of client.
     */
    @Test
    @Throws(Exception::class)
    fun oldSaslPlainSslServerWithoutSaslAuthenticateHeader() {
        verifySaslAuthenticateHeaderInterop(
            enableHeaderOnServer = false,
            enableHeaderOnClient = true,
            securityProtocol = SecurityProtocol.SASL_SSL,
            saslMechanism = "PLAIN",
        )
    }

    /**
     * Tests good path SASL/PLAIN authentication over SSL with old version of client
     * that does not support SASL_AUTHENTICATE headers and new version of server.
     */
    @Test
    @Throws(Exception::class)
    fun oldSaslPlainSslClientWithoutSaslAuthenticateHeader() {
        verifySaslAuthenticateHeaderInterop(
            enableHeaderOnServer = true,
            enableHeaderOnClient = false,
            securityProtocol = SecurityProtocol.SASL_SSL,
            saslMechanism = "PLAIN",
        )
    }

    /**
     * Tests good path SASL/SCRAM authentication over SSL with old version of server
     * that does not support SASL_AUTHENTICATE headers and new version of client.
     */
    @Test
    @Throws(Exception::class)
    fun oldSaslScramSslServerWithoutSaslAuthenticateHeader() {
        verifySaslAuthenticateHeaderInterop(
            enableHeaderOnServer = false,
            enableHeaderOnClient = true,
            securityProtocol = SecurityProtocol.SASL_SSL,
            saslMechanism = "SCRAM-SHA-512",
        )
    }

    /**
     * Tests good path SASL/SCRAM authentication over SSL with old version of client
     * that does not support SASL_AUTHENTICATE headers and new version of server.
     */
    @Test
    @Throws(Exception::class)
    fun oldSaslScramSslClientWithoutSaslAuthenticateHeader() {
        verifySaslAuthenticateHeaderInterop(
            enableHeaderOnServer = true,
            enableHeaderOnClient = false,
            securityProtocol = SecurityProtocol.SASL_SSL,
            saslMechanism = "SCRAM-SHA-512",
        )
    }

    /**
     * Tests SASL/PLAIN authentication failure over PLAINTEXT with old version of server
     * that does not support SASL_AUTHENTICATE headers and new version of client.
     */
    @Test
    @Throws(Exception::class)
    fun oldSaslPlainPlaintextServerWithoutSaslAuthenticateHeaderFailure() {
        verifySaslAuthenticateHeaderInteropWithFailure(
            enableHeaderOnServer = false,
            enableHeaderOnClient = true,
            securityProtocol = SecurityProtocol.SASL_PLAINTEXT,
            saslMechanism = "PLAIN",
        )
    }

    /**
     * Tests SASL/PLAIN authentication failure over PLAINTEXT with old version of client
     * that does not support SASL_AUTHENTICATE headers and new version of server.
     */
    @Test
    @Throws(Exception::class)
    fun oldSaslPlainPlaintextClientWithoutSaslAuthenticateHeaderFailure() {
        verifySaslAuthenticateHeaderInteropWithFailure(
            enableHeaderOnServer = true,
            enableHeaderOnClient = false,
            securityProtocol = SecurityProtocol.SASL_PLAINTEXT,
            saslMechanism = "PLAIN",
        )
    }

    /**
     * Tests SASL/SCRAM authentication failure over PLAINTEXT with old version of server
     * that does not support SASL_AUTHENTICATE headers and new version of client.
     */
    @Test
    @Throws(Exception::class)
    fun oldSaslScramPlaintextServerWithoutSaslAuthenticateHeaderFailure() {
        verifySaslAuthenticateHeaderInteropWithFailure(
            enableHeaderOnServer = false,
            enableHeaderOnClient = true,
            securityProtocol = SecurityProtocol.SASL_PLAINTEXT,
            saslMechanism = "SCRAM-SHA-256",
        )
    }

    /**
     * Tests SASL/SCRAM authentication failure over PLAINTEXT with old version of client
     * that does not support SASL_AUTHENTICATE headers and new version of server.
     */
    @Test
    @Throws(Exception::class)
    fun oldSaslScramPlaintextClientWithoutSaslAuthenticateHeaderFailure() {
        verifySaslAuthenticateHeaderInteropWithFailure(
            enableHeaderOnServer = true,
            enableHeaderOnClient = false,
            securityProtocol = SecurityProtocol.SASL_PLAINTEXT,
            saslMechanism = "SCRAM-SHA-256",
        )
    }

    /**
     * Tests SASL/PLAIN authentication failure over SSL with old version of server
     * that does not support SASL_AUTHENTICATE headers and new version of client.
     */
    @Test
    @Throws(Exception::class)
    fun oldSaslPlainSslServerWithoutSaslAuthenticateHeaderFailure() {
        verifySaslAuthenticateHeaderInteropWithFailure(
            enableHeaderOnServer = false,
            enableHeaderOnClient = true,
            securityProtocol = SecurityProtocol.SASL_SSL,
            saslMechanism = "PLAIN",
        )
    }

    /**
     * Tests SASL/PLAIN authentication failure over SSL with old version of client
     * that does not support SASL_AUTHENTICATE headers and new version of server.
     */
    @Test
    @Throws(Exception::class)
    fun oldSaslPlainSslClientWithoutSaslAuthenticateHeaderFailure() {
        verifySaslAuthenticateHeaderInteropWithFailure(
            enableHeaderOnServer = true,
            enableHeaderOnClient = false,
            securityProtocol = SecurityProtocol.SASL_SSL,
            saslMechanism = "PLAIN",
        )
    }

    /**
     * Tests SASL/SCRAM authentication failure over SSL with old version of server
     * that does not support SASL_AUTHENTICATE headers and new version of client.
     */
    @Test
    @Throws(Exception::class)
    fun oldSaslScramSslServerWithoutSaslAuthenticateHeaderFailure() {
        verifySaslAuthenticateHeaderInteropWithFailure(
            enableHeaderOnServer = false,
            enableHeaderOnClient = true,
            securityProtocol = SecurityProtocol.SASL_SSL,
            saslMechanism = "SCRAM-SHA-512",
        )
    }

    /**
     * Tests SASL/SCRAM authentication failure over SSL with old version of client
     * that does not support SASL_AUTHENTICATE headers and new version of server.
     */
    @Test
    @Throws(Exception::class)
    fun oldSaslScramSslClientWithoutSaslAuthenticateHeaderFailure() {
        verifySaslAuthenticateHeaderInteropWithFailure(
            enableHeaderOnServer = true,
            enableHeaderOnClient = false,
            securityProtocol = SecurityProtocol.SASL_SSL,
            saslMechanism = "SCRAM-SHA-512",
        )
    }

    /**
     * Tests OAUTHBEARER client and server channels.
     */
    @Test
    @Throws(Exception::class)
    fun testValidSaslOauthBearerMechanism() {
        val node = "0"
        val securityProtocol = SecurityProtocol.SASL_SSL
        configureMechanisms(clientMechanism = "OAUTHBEARER", serverMechanisms = listOf("OAUTHBEARER"))
        server = createEchoServer(securityProtocol)
        createAndCheckClientConnection(securityProtocol = securityProtocol, node = node)
    }

    /**
     * Re-authentication must fail if principal changes
     */
    @Test
    @Throws(Exception::class)
    fun testCannotReauthenticateWithDifferentPrincipal() {
        val node = "0"
        val securityProtocol = SecurityProtocol.SASL_SSL
        saslClientConfigs[SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS] =
            AlternateLoginCallbackHandler::class.java.getName()
        configureMechanisms(
            clientMechanism = OAuthBearerLoginModule.OAUTHBEARER_MECHANISM,
            serverMechanisms = listOf(OAuthBearerLoginModule.OAUTHBEARER_MECHANISM)
        )
        server = createEchoServer(securityProtocol)
        // initial authentication must succeed
        createClientConnection(securityProtocol, node)
        checkClientConnection(node)
        // ensure metrics are as expected before trying to re-authenticate
        server.verifyAuthenticationMetrics(
            successfulAuthentications = 1,
            failedAuthentications = 0,
        )
        server.verifyReauthenticationMetrics(
            successfulReauthentications = 0,
            failedReauthentications = 0,
        )
        /*
         * Now re-authenticate with a different principal and ensure it fails. We first
         * have to sleep long enough for the background refresh thread to replace the
         * original token with a new one.
         */
        delay(1000L)
        assertFailsWith<AssertionFailedError> { checkClientConnection(node) }
        server.verifyReauthenticationMetrics(
            successfulReauthentications = 0,
            failedReauthentications = 1,
        )
    }

    @Test
    fun testCorrelationId() {
        val authenticator = object : SaslClientAuthenticator(
            configs = emptyMap<String, Any?>(),
            callbackHandler = null,
            node = "node",
            subject = null,
            servicePrincipal = null,
            host = null,
            mechanism = "plain",
            handshakeRequestEnable = false,
            transportLayer = null,
            time = null,
            logContext = LogContext(),
        ) {
            override fun createSaslClient(): SaslClient? = null
        }
        val count =
            (SaslClientAuthenticator.MAX_RESERVED_CORRELATION_ID - SaslClientAuthenticator.MIN_RESERVED_CORRELATION_ID) * 2
        val ids = (0 until count).asIterable()
            .map { authenticator.nextCorrelationId() }
            .toSet()
        assertEquals(
            expected = SaslClientAuthenticator.MAX_RESERVED_CORRELATION_ID - SaslClientAuthenticator.MIN_RESERVED_CORRELATION_ID + 1,
            actual = ids.size,
        )
        ids.forEach { id: Int ->
            assertTrue(id >= SaslClientAuthenticator.MIN_RESERVED_CORRELATION_ID)
            assertTrue(SaslClientAuthenticator.isReserved(id))
        }
    }

    @Test
    fun testConvertListOffsetResponseToSaslHandshakeResponse() {
        val data = ListOffsetsResponseData()
            .setThrottleTimeMs(0)
            .setTopics(
                listOf(
                    ListOffsetsTopicResponse()
                        .setName("topic")
                        .setPartitions(
                            listOf(
                                ListOffsetsPartitionResponse()
                                    .setErrorCode(Errors.NONE.code)
                                    .setLeaderEpoch(ListOffsetsResponse.UNKNOWN_EPOCH)
                                    .setPartitionIndex(0)
                                    .setOffset(0)
                                    .setTimestamp(0)
                            )
                        )
                )
            )
        val response = ListOffsetsResponse(data)
        val buffer = serializeResponseWithHeader(
            response = response,
            version = ApiKeys.LIST_OFFSETS.latestVersion(),
            correlationId = 0,
        )
        val header0 = RequestHeader(
            requestApiKey = ApiKeys.LIST_OFFSETS,
            requestVersion = ApiKeys.LIST_OFFSETS.latestVersion(),
            clientId = "id",
            correlationId = SaslClientAuthenticator.MIN_RESERVED_CORRELATION_ID,
        )
        assertFailsWith<SchemaException> {
            NetworkClient.parseResponse(
                responseBuffer = buffer.duplicate(),
                requestHeader = header0,
            )
        }
        val header1 = RequestHeader(
            requestApiKey = ApiKeys.LIST_OFFSETS,
            requestVersion = ApiKeys.LIST_OFFSETS.latestVersion(),
            clientId = "id",
            correlationId = 1,
        )
        assertFailsWith<IllegalStateException> {
            NetworkClient.parseResponse(
                responseBuffer = buffer.duplicate(),
                requestHeader = header1,
            )
        }
    }

    /**
     * Re-authentication must fail if mechanism changes
     */
    @Test
    @Throws(Exception::class)
    fun testCannotReauthenticateWithDifferentMechanism() {
        val node = "0"
        val securityProtocol = SecurityProtocol.SASL_SSL
        configureMechanisms(clientMechanism = "DIGEST-MD5", serverMechanisms = listOf("DIGEST-MD5", "PLAIN"))
        configureDigestMd5ServerCallback(securityProtocol)
        server = createEchoServer(securityProtocol)
        val saslMechanism = saslClientConfigs[SaslConfigs.SASL_MECHANISM] as String
        val configs = TestSecurityConfig(saslClientConfigs).values()
        channelBuilder = AlternateSaslChannelBuilder(
            mode = Mode.CLIENT,
            jaasContexts = mapOf(saslMechanism to JaasContext.loadClientContext(configs)),
            securityProtocol = securityProtocol,
            listenerName = null,
            isInterBrokerListener = false,
            clientSaslMechanism = saslMechanism,
            handshakeRequestEnable = true,
            credentialCache = credentialCache,
            tokenCache = null,
            time = time
        )
        channelBuilder.configure(configs)
        // initial authentication must succeed
        selector = NetworkTestUtils.createSelector(channelBuilder = channelBuilder, time = time)
        val addr = InetSocketAddress("localhost", server.port)
        selector!!.connect(
            id = node,
            address = addr,
            sendBufferSize = BUFFER_SIZE,
            receiveBufferSize = BUFFER_SIZE,
        )
        checkClientConnection(node)
        // ensure metrics are as expected before trying to re-authenticate
        server.verifyAuthenticationMetrics(
            successfulAuthentications = 1,
            failedAuthentications = 0,
        )
        server.verifyReauthenticationMetrics(
            successfulReauthentications = 0,
            failedReauthentications = 0,
        )
        /*
         * Now re-authenticate with a different mechanism and ensure it fails. We have
         * to sleep long enough so that the next write will trigger a re-authentication.
         */delay((CONNECTIONS_MAX_REAUTH_MS_VALUE * 1.1).toLong())
        assertFailsWith<AssertionFailedError> { checkClientConnection(node) }
        server.verifyAuthenticationMetrics(
            successfulAuthentications = 1,
            failedAuthentications = 0,
        )
        server.verifyReauthenticationMetrics(
            successfulReauthentications = 0,
            failedReauthentications = 1,
        )
    }

    /**
     * Second re-authentication must fail if it is sooner than one second after the first
     */
    @Test
    @Throws(Exception::class)
    fun testCannotReauthenticateAgainFasterThanOneSecond() {
        val node = "0"
        time = MockTime()
        val securityProtocol = SecurityProtocol.SASL_SSL
        configureMechanisms(
            clientMechanism = OAuthBearerLoginModule.OAUTHBEARER_MECHANISM,
            serverMechanisms = listOf(OAuthBearerLoginModule.OAUTHBEARER_MECHANISM),
        )
        server = createEchoServer(securityProtocol)
        try {
            createClientConnection(securityProtocol, node)
            checkClientConnection(node)
            server.verifyAuthenticationMetrics(
                successfulAuthentications = 1,
                failedAuthentications = 0,
            )
            server.verifyReauthenticationMetrics(
                successfulReauthentications = 0,
                failedReauthentications = 0,
            )
            /*
             * Now sleep long enough so that the next write will cause re-authentication,
             * which we expect to succeed.
             */time.sleep((CONNECTIONS_MAX_REAUTH_MS_VALUE * 1.1).toLong())
            checkClientConnection(node)
            server.verifyAuthenticationMetrics(
                successfulAuthentications = 1,
                failedAuthentications = 0,
            )
            server.verifyReauthenticationMetrics(
                successfulReauthentications = 1,
                failedReauthentications = 0,
            )
            /*
             * Now sleep long enough so that the next write will cause re-authentication,
             * but this time we expect re-authentication to not occur since it has been too
             * soon. The checkClientConnection() call should return an error saying it
             * expected the one byte-plus-node response but got the SaslHandshakeRequest
             * instead
             */time.sleep((CONNECTIONS_MAX_REAUTH_MS_VALUE * 1.1).toLong())
            val exception = assertFailsWith<AssertionFailedError> {
                NetworkTestUtils.checkClientConnection(
                    selector = selector!!,
                    node = node,
                    minMessageSize = 1,
                    messageCount = 1,
                )
            }
            val expectedResponseTextRegex = "\\w-$node"
            val receivedResponseTextRegex = ".*" + OAuthBearerLoginModule.OAUTHBEARER_MECHANISM
            assertTrue(
                exception.message!!.matches(".*<$expectedResponseTextRegex>.*<$receivedResponseTextRegex.*?>".toRegex()),
                "Should have received the SaslHandshakeRequest bytes back since we re-authenticated too quickly, " +
                        "but instead we got our generated message echoed back, implying re-auth succeeded when it should not have: " +
                        exception
            )
            server.verifyReauthenticationMetrics(
                successfulReauthentications = 1,
                failedReauthentications = 0,
            ) // unchanged
        } finally {
            selector!!.close()
            selector = null
        }
    }

    /**
     * Tests good path SASL/PLAIN client and server channels using SSL transport layer.
     * Repeatedly tests successful re-authentication over several seconds.
     */
    @Test
    @Throws(Exception::class)
    fun testRepeatedValidSaslPlainOverSsl() {
        val node = "0"
        val securityProtocol = SecurityProtocol.SASL_SSL
        configureMechanisms("PLAIN", listOf("PLAIN"))
        /*
         * Make sure 85% of this value is at least 1 second otherwise it is possible for
         * the client to start re-authenticating but the server does not start due to
         * the 1-second minimum. If this happens the SASL HANDSHAKE request that was
         * injected to start re-authentication will be echoed back to the client instead
         * of the data that the client explicitly sent, and then the client will not
         * recognize that data and will throw an assertion error.
         */
        saslServerConfigs[BrokerSecurityConfigs.CONNECTIONS_MAX_REAUTH_MS] = (1.1 * 1000L / 0.85).toLong()
        server = createEchoServer(securityProtocol)
        createClientConnection(securityProtocol, node)
        checkClientConnection(node)
        server.verifyAuthenticationMetrics(
            successfulAuthentications = 1,
            failedAuthentications = 0,
        )
        server.verifyReauthenticationMetrics(
            successfulReauthentications = 0,
            failedReauthentications = 0,
        )
        var successfulReauthentications = 0.0
        val desiredNumReauthentications = 5
        val startMs = Time.SYSTEM.milliseconds()
        val timeoutMs = startMs + 1000 * 15 // stop after 15 seconds
        while (
            successfulReauthentications < desiredNumReauthentications
            && Time.SYSTEM.milliseconds() < timeoutMs
        ) {
            checkClientConnection(node)
            successfulReauthentications = server.metricValue("successful-reauthentication-total")
        }
        server.verifyReauthenticationMetrics(
            successfulReauthentications = desiredNumReauthentications,
            failedReauthentications = 0,
        )
    }

    /**
     * Tests OAUTHBEARER client channels without tokens for the server.
     */
    @Test
    @Throws(Exception::class)
    fun testValidSaslOauthBearerMechanismWithoutServerTokens() {
        val node = "0"
        val securityProtocol = SecurityProtocol.SASL_SSL
        saslClientConfigs.put(SaslConfigs.SASL_MECHANISM, "OAUTHBEARER")
        saslServerConfigs.put(
            BrokerSecurityConfigs.SASL_ENABLED_MECHANISMS_CONFIG,
            mutableListOf<String>("OAUTHBEARER")
        )
        saslClientConfigs[SaslConfigs.SASL_JAAS_CONFIG] = TestJaasConfig.jaasConfigProperty(
            mechanism = "OAUTHBEARER",
            options = mapOf("unsecuredLoginStringClaim_sub" to TestJaasConfig.USERNAME),
        )
        saslServerConfigs["listener.name.sasl_ssl.oauthbearer." + SaslConfigs.SASL_JAAS_CONFIG] =
            TestJaasConfig.jaasConfigProperty("OAUTHBEARER", emptyMap())

        // Server without a token should start up successfully and authenticate clients.
        server = createEchoServer(securityProtocol)
        createAndCheckClientConnection(securityProtocol, node)

        // Client without a token should fail to connect
        saslClientConfigs[SaslConfigs.SASL_JAAS_CONFIG] =
            TestJaasConfig.jaasConfigProperty("OAUTHBEARER", emptyMap())
        createAndCheckClientConnectionFailure(securityProtocol, node)

        // Server with extensions, but without a token should fail to start up since it could indicate a configuration error
        saslServerConfigs["listener.name.sasl_ssl.oauthbearer." + SaslConfigs.SASL_JAAS_CONFIG] =
            TestJaasConfig.jaasConfigProperty(
                mechanism = "OAUTHBEARER",
                options = mapOf("unsecuredLoginExtension_test" to "something"),
            )
        val error = assertFailsWith<Throwable>(
            message = "Server created with invalid login config containing extensions without a token",
        ) { createEchoServer(securityProtocol) }
        assertIs<LoginException>(error.cause, "Unexpected exception " + Utils.stackTrace(error))
    }

    /**
     * Tests OAUTHBEARER fails the connection when the client presents a token with
     * insufficient scope .
     */
    @Test
    @Throws(Exception::class)
    fun testInsufficientScopeSaslOauthBearerMechanism() {
        val securityProtocol = SecurityProtocol.SASL_SSL
        val jaasConfig = configureMechanisms("OAUTHBEARER", listOf("OAUTHBEARER"))
        // now update the server side to require a scope the client does not provide
        val serverJaasConfigOptionsMap = TestJaasConfig.defaultServerOptions("OAUTHBEARER").toMutableMap()
        serverJaasConfigOptionsMap["unsecuredValidatorRequiredScope"] = "LOGIN_TO_KAFKA" // causes the failure
        jaasConfig.createOrUpdateEntry(
            name = "KafkaServer",
            loginModule = "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule",
            options = serverJaasConfigOptionsMap,
        )
        server = createEchoServer(securityProtocol)
        createAndCheckClientAuthenticationFailure(
            securityProtocol = securityProtocol,
            node = "node-${OAuthBearerLoginModule.OAUTHBEARER_MECHANISM}",
            mechanism = OAuthBearerLoginModule.OAUTHBEARER_MECHANISM,
            expectedErrorMessage = """{"status":"insufficient_scope", "scope":"[LOGIN_TO_KAFKA]"}""",
        )
    }

    @Test
    @Throws(Exception::class)
    fun testSslClientAuthDisabledForSaslSslListener() {
        verifySslClientAuthForSaslSslListener(useListenerPrefix = true, configuredClientAuth = SslClientAuth.NONE)
    }

    @Test
    @Throws(Exception::class)
    fun testSslClientAuthRequestedForSaslSslListener() {
        verifySslClientAuthForSaslSslListener(useListenerPrefix = true, configuredClientAuth = SslClientAuth.REQUESTED)
    }

    @Test
    @Throws(Exception::class)
    fun testSslClientAuthRequiredForSaslSslListener() {
        verifySslClientAuthForSaslSslListener(useListenerPrefix = true, configuredClientAuth = SslClientAuth.REQUIRED)
    }

    @Test
    @Throws(Exception::class)
    fun testSslClientAuthRequestedOverriddenForSaslSslListener() {
        verifySslClientAuthForSaslSslListener(useListenerPrefix = false, configuredClientAuth = SslClientAuth.REQUESTED)
    }

    @Test
    @Throws(Exception::class)
    fun testSslClientAuthRequiredOverriddenForSaslSslListener() {
        verifySslClientAuthForSaslSslListener(useListenerPrefix = false, configuredClientAuth = SslClientAuth.REQUIRED)
    }

    @Throws(Exception::class)
    private fun verifySslClientAuthForSaslSslListener(
        useListenerPrefix: Boolean,
        configuredClientAuth: SslClientAuth,
    ) {
        val securityProtocol = SecurityProtocol.SASL_SSL
        configureMechanisms("PLAIN", listOf("PLAIN"))
        val listenerPrefix =
            if (useListenerPrefix) ListenerName.forSecurityProtocol(securityProtocol).configPrefix() else ""
        saslServerConfigs[listenerPrefix + BrokerSecurityConfigs.SSL_CLIENT_AUTH_CONFIG] = configuredClientAuth.name
        saslServerConfigs[BrokerSecurityConfigs.PRINCIPAL_BUILDER_CLASS_CONFIG] = SaslSslPrincipalBuilder::class.java.getName()
        server = createEchoServer(securityProtocol)
        val expectedClientAuth = if (useListenerPrefix) configuredClientAuth else SslClientAuth.NONE
        val certDn = "O=A client,CN=localhost"
        val principalWithMutualTls = SaslSslPrincipalBuilder.saslSslPrincipal(
            saslPrincipal = TestJaasConfig.USERNAME,
            sslPrincipal = certDn,
        )
        val principalWithOneWayTls = SaslSslPrincipalBuilder.saslSslPrincipal(
            saslPrincipal = TestJaasConfig.USERNAME,
            sslPrincipal = "ANONYMOUS",
        )

        // Client configured with valid key store
        createAndCheckClientConnectionAndPrincipal(
            securityProtocol = securityProtocol, node = "0",
            expectedPrincipal =
            if (expectedClientAuth == SslClientAuth.NONE) principalWithOneWayTls
            else principalWithMutualTls
        )

        // Client does not configure key store
        removeClientSslKeystore()
        if (expectedClientAuth != SslClientAuth.REQUIRED) createAndCheckClientConnectionAndPrincipal(
            securityProtocol = securityProtocol,
            node = "1",
            expectedPrincipal = principalWithOneWayTls,
        )
        else createAndCheckSslAuthenticationFailure(securityProtocol = securityProtocol, node = "1")

        // Client configures untrusted key store
        val newStore = CertStores(false, "localhost")
        newStore.keyStoreProps().forEach { (k: String, v: Any?) -> saslClientConfigs[k] = v }
        if (expectedClientAuth == SslClientAuth.NONE) createAndCheckClientConnectionAndPrincipal(
            securityProtocol = securityProtocol,
            node = "2",
            expectedPrincipal = principalWithOneWayTls
        )
        else createAndCheckSslAuthenticationFailure(securityProtocol = securityProtocol, node = "2")
    }

    private fun removeClientSslKeystore() {
        saslClientConfigs.remove(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG)
        saslClientConfigs.remove(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG)
        saslClientConfigs.remove(SslConfigs.SSL_KEY_PASSWORD_CONFIG)
    }

    @Throws(Exception::class)
    private fun verifySaslAuthenticateHeaderInterop(
        enableHeaderOnServer: Boolean,
        enableHeaderOnClient: Boolean,
        securityProtocol: SecurityProtocol,
        saslMechanism: String,
    ) {
        configureMechanisms(clientMechanism = saslMechanism, serverMechanisms = listOf(saslMechanism))
        createServer(
            securityProtocol = securityProtocol,
            saslMechanism = saslMechanism,
            enableSaslAuthenticateHeader = enableHeaderOnServer
        )
        val node = "0"
        createClientConnection(
            securityProtocol = securityProtocol,
            saslMechanism = saslMechanism,
            node = node,
            enableSaslAuthenticateHeader = enableHeaderOnClient
        )
        NetworkTestUtils.checkClientConnection(
            selector = selector!!,
            node = "0",
            minMessageSize = 100,
            messageCount = 10
        )
    }

    @Throws(Exception::class)
    private fun verifySaslAuthenticateHeaderInteropWithFailure(
        enableHeaderOnServer: Boolean,
        enableHeaderOnClient: Boolean,
        securityProtocol: SecurityProtocol,
        saslMechanism: String,
    ) {
        val jaasConfig = configureMechanisms(saslMechanism, listOf(saslMechanism))
        jaasConfig.setClientOptions(
            saslMechanism = saslMechanism,
            clientUsername = TestJaasConfig.USERNAME,
            clientPassword = "invalidpassword",
        )
        createServer(
            securityProtocol = securityProtocol,
            saslMechanism = saslMechanism,
            enableSaslAuthenticateHeader = enableHeaderOnServer,
        )
        val node = "0"
        createClientConnection(
            securityProtocol = securityProtocol,
            saslMechanism = saslMechanism,
            node = node,
            enableSaslAuthenticateHeader = enableHeaderOnClient,
        )
        // Without SASL_AUTHENTICATE headers, disconnect state is ChannelState.AUTHENTICATE which is
        // a hint that channel was closed during authentication, unlike ChannelState.AUTHENTICATE_FAILED
        // which is an actual authentication failure reported by the broker.
        waitForChannelClose(
            selector = selector!!,
            node = node,
            channelState = ChannelState.State.AUTHENTICATE,
        )
    }

    @Throws(Exception::class)
    private fun createServer(
        securityProtocol: SecurityProtocol,
        saslMechanism: String,
        enableSaslAuthenticateHeader: Boolean,
    ) {
        server =
            if (enableSaslAuthenticateHeader) createEchoServer(securityProtocol)
            else startServerWithoutSaslAuthenticateHeader(
                securityProtocol = securityProtocol,
                saslMechanism = saslMechanism,
            )
        updateScramCredentialCache(username = TestJaasConfig.USERNAME, password = TestJaasConfig.PASSWORD)
    }

    @Throws(Exception::class)
    private fun createClientConnection(
        securityProtocol: SecurityProtocol,
        saslMechanism: String,
        node: String,
        enableSaslAuthenticateHeader: Boolean,
    ) {
        if (enableSaslAuthenticateHeader) createClientConnection(
            securityProtocol = securityProtocol,
            node = node,
        )
        else createClientConnectionWithoutSaslAuthenticateHeader(
            securityProtocol = securityProtocol,
            saslMechanism = saslMechanism,
            node = node,
        )
    }

    @Throws(Exception::class)
    private fun startServerApiVersionsUnsupportedByClient(
        securityProtocol: SecurityProtocol,
        saslMechanism: String,
    ): NioEchoServer {
        val listenerName = ListenerName.forSecurityProtocol(securityProtocol)
        val configs = emptyMap<String, Any>()
        val jaasContext = JaasContext.loadServerContext(
            listenerName = listenerName,
            mechanism = saslMechanism,
            configs = configs,
        )
        val jaasContexts = mapOf(saslMechanism to jaasContext)
        val isScram = ScramMechanism.isScram(saslMechanism)
        if (isScram) ScramCredentialUtils.createCache(credentialCache, listOf(saslMechanism))
        val apiVersionSupplier = {
                val versionCollection = ApiVersionCollection(expectedNumElements = 2)
                versionCollection.add(
                    ApiVersionsResponseData.ApiVersion()
                        .setApiKey(ApiKeys.SASL_HANDSHAKE.id)
                        .setMinVersion(0)
                        .setMaxVersion(100)
                )
                versionCollection.add(
                    ApiVersionsResponseData.ApiVersion()
                        .setApiKey(ApiKeys.SASL_AUTHENTICATE.id)
                        .setMinVersion(0)
                        .setMaxVersion(100)
                )
                ApiVersionsResponse(ApiVersionsResponseData().setApiKeys(versionCollection))
            }
        val serverChannelBuilder = SaslChannelBuilder(
            mode = Mode.SERVER,
            jaasContexts = jaasContexts,
            securityProtocol = securityProtocol,
            listenerName = listenerName,
            isInterBrokerListener = false,
            clientSaslMechanism = saslMechanism,
            handshakeRequestEnable = true,
            credentialCache = credentialCache,
            tokenCache = null,
            sslClientAuthOverride = null,
            time = time,
            logContext = LogContext(),
            apiVersionSupplier = apiVersionSupplier,
        )
        serverChannelBuilder.configure(saslServerConfigs)
        server = NioEchoServer(
            listenerName = listenerName,
            securityProtocol = securityProtocol,
            config = TestSecurityConfig(saslServerConfigs),
            serverHost = "localhost",
            channelBuilder = serverChannelBuilder,
            credentialCache = credentialCache,
            time = time,
        )
        server.start()
        return server
    }

    @Throws(Exception::class)
    private fun startServerWithoutSaslAuthenticateHeader(
        securityProtocol: SecurityProtocol,
        saslMechanism: String,
    ): NioEchoServer {
        val listenerName = ListenerName.forSecurityProtocol(securityProtocol)
        val configs: Map<String, *> = emptyMap<String, Any>()
        val jaasContext = JaasContext.loadServerContext(listenerName, saslMechanism, configs)
        val jaasContexts = mapOf(saslMechanism to jaasContext)
        val isScram = ScramMechanism.isScram(saslMechanism)
        if (isScram) ScramCredentialUtils.createCache(credentialCache, listOf(saslMechanism))
        val apiVersionSupplier = {
                val defaultApiVersionResponse =
                    TestUtils.defaultApiVersionsResponse(listenerType = ApiMessageType.ListenerType.ZK_BROKER)
                val apiVersions = ApiVersionCollection()
                for (apiVersion in defaultApiVersionResponse.data().apiKeys) {
                    if (apiVersion.apiKey != ApiKeys.SASL_AUTHENTICATE.id) {
                        // ApiVersion can NOT be reused in second ApiVersionCollection
                        // due to the internal pointers it contains.
                        apiVersions.add(apiVersion.duplicate())
                    }
                }
                val data = ApiVersionsResponseData()
                    .setErrorCode(Errors.NONE.code)
                    .setThrottleTimeMs(0)
                    .setApiKeys(apiVersions)
                ApiVersionsResponse(data)
            }
        val serverChannelBuilder = object : SaslChannelBuilder(
            mode = Mode.SERVER,
            jaasContexts = jaasContexts,
            securityProtocol = securityProtocol,
            listenerName = listenerName,
            isInterBrokerListener = false,
            clientSaslMechanism = saslMechanism,
            handshakeRequestEnable = true,
            credentialCache = credentialCache,
            tokenCache = null,
            sslClientAuthOverride = null,
            time = time,
            logContext = LogContext(),
            apiVersionSupplier = apiVersionSupplier,
        ) {

            override fun buildServerAuthenticator(
                configs: Map<String, *>,
                callbackHandlers: Map<String, AuthenticateCallbackHandler>,
                id: String,
                transportLayer: TransportLayer,
                subjects: Map<String, Subject>,
                connectionsMaxReauthMsByMechanism: Map<String, Long>,
                metadataRegistry: ChannelMetadataRegistry,
            ): SaslServerAuthenticator {
                return object : SaslServerAuthenticator(
                    configs = configs,
                    callbackHandlers = callbackHandlers,
                    connectionId = id,
                    subjects = subjects,
                    kerberosNameParser = null,
                    listenerName = listenerName,
                    securityProtocol = securityProtocol,
                    transportLayer = transportLayer,
                    connectionsMaxReauthMsByMechanism = connectionsMaxReauthMsByMechanism,
                    metadataRegistry = metadataRegistry,
                    time = time,
                    apiVersionSupplier = apiVersionSupplier,
                ) {
                    // Don't enable Kafka SASL_AUTHENTICATE headers
                    override fun enableKafkaSaslAuthenticateHeaders(flag: Boolean) = Unit
                }
            }
        }
        serverChannelBuilder.configure(saslServerConfigs)
        server = NioEchoServer(
            listenerName = listenerName,
            securityProtocol = securityProtocol,
            config = TestSecurityConfig(saslServerConfigs),
            serverHost = "localhost",
            channelBuilder = serverChannelBuilder,
            credentialCache = credentialCache,
            time = time,
        )
        server.start()
        return server
    }

    @Throws(Exception::class)
    private fun createClientConnectionWithoutSaslAuthenticateHeader(
        securityProtocol: SecurityProtocol,
        saslMechanism: String,
        node: String,
    ) {
        val listenerName = ListenerName.forSecurityProtocol(securityProtocol)
        val configs = emptyMap<String, Any>()
        val jaasContext = JaasContext.loadClientContext(configs)
        val jaasContexts = mapOf(saslMechanism to jaasContext)
        val clientChannelBuilder = object : SaslChannelBuilder(
            mode = Mode.CLIENT,
            jaasContexts = jaasContexts,
            securityProtocol = securityProtocol,
            listenerName = listenerName,
            isInterBrokerListener = false,
            clientSaslMechanism = saslMechanism,
            handshakeRequestEnable = true,
            credentialCache = null,
            tokenCache = null,
            sslClientAuthOverride = null,
            time = time,
            logContext = LogContext(),
            apiVersionSupplier = null
        ) {
            override fun buildClientAuthenticator(
                configs: Map<String, *>,
                callbackHandler: AuthenticateCallbackHandler,
                id: String,
                serverHost: String,
                servicePrincipal: String,
                transportLayer: TransportLayer,
                subject: Subject,
            ): SaslClientAuthenticator {
                return object : SaslClientAuthenticator(
                    configs = configs,
                    callbackHandler = callbackHandler,
                    node = id,
                    subject = subject,
                    servicePrincipal = servicePrincipal,
                    host = serverHost,
                    mechanism = saslMechanism,
                    handshakeRequestEnable = true,
                    transportLayer = transportLayer,
                    time = time,
                    logContext = LogContext()
                ) {
                    override fun createSaslHandshakeRequest(version: Short): SaslHandshakeRequest {
                        return buildSaslHandshakeRequest(mechanism = saslMechanism, version = 0)
                    }

                    // Don't set version so that headers are disabled
                    override fun setSaslAuthenticateAndHandshakeVersions(
                        apiVersionsResponse: ApiVersionsResponse,
                    ) = Unit
                }
            }
        }
        clientChannelBuilder.configure(saslClientConfigs)
        selector = NetworkTestUtils.createSelector(channelBuilder = clientChannelBuilder, time = time)
        val addr = InetSocketAddress("localhost", server.port)
        selector!!.connect(
            id = node,
            address = addr,
            sendBufferSize = BUFFER_SIZE,
            receiveBufferSize = BUFFER_SIZE,
        )
    }

    /**
     * Tests that Kafka ApiVersionsRequests are handled by the SASL server authenticator
     * prior to SASL handshake flow and that subsequent authentication succeeds
     * when transport layer is PLAINTEXT/SSL. This test uses a non-SASL client that simulates
     * SASL authentication after ApiVersionsRequest.
     *
     *
     * Test sequence (using <tt>securityProtocol=PLAINTEXT</tt> as an example):
     *
     *  1. Starts a SASL_PLAINTEXT test server that simply echoes back client requests after authentication.
     *  1. A (non-SASL) PLAINTEXT test client connects to the SASL server port. Client is now unauthenticated.<./li>
     *  1. The unauthenticated non-SASL client sends an ApiVersionsRequest and validates the response.
     * A valid response indicates that [SaslServerAuthenticator] of the test server responded to
     * the ApiVersionsRequest even though the client is not yet authenticated.
     *  1. The unauthenticated non-SASL client sends a SaslHandshakeRequest and validates the response. A valid response
     * indicates that [SaslServerAuthenticator] of the test server responded to the SaslHandshakeRequest
     * after processing ApiVersionsRequest.
     *  1. The unauthenticated non-SASL client sends the SASL/PLAIN packet containing username/password to authenticate
     * itself. The client is now authenticated by the server. At this point this test client is at the
     * same state as a regular SASL_PLAINTEXT client that is <tt>ready</tt>.
     *  1. The authenticated client sends random data to the server and checks that the data is echoed
     * back by the test server (ie, not Kafka request-response) to ensure that the client now
     * behaves exactly as a regular SASL_PLAINTEXT client that has completed authentication.
     */
    @Throws(Exception::class)
    private fun testUnauthenticatedApiVersionsRequest(securityProtocol: SecurityProtocol, saslHandshakeVersion: Short) {
        configureMechanisms("PLAIN", listOf("PLAIN"))
        server = createEchoServer(securityProtocol)

        // Create non-SASL connection to manually authenticate after ApiVersionsRequest
        val node = "1"
        val clientProtocol = when (securityProtocol) {
            SecurityProtocol.SASL_PLAINTEXT -> SecurityProtocol.PLAINTEXT
            SecurityProtocol.SASL_SSL -> SecurityProtocol.SSL
            else -> throw IllegalArgumentException("Server protocol $securityProtocol is not SASL")
        }
        createClientConnection(securityProtocol = clientProtocol, node = node)
        NetworkTestUtils.waitForChannelReady(selector = selector!!, node = node)

        // Send ApiVersionsRequest and check response
        val versionsResponse = sendVersionRequestReceiveResponse(node)
        assertEquals(
            expected = ApiKeys.SASL_HANDSHAKE.oldestVersion(),
            actual = versionsResponse.apiVersion(ApiKeys.SASL_HANDSHAKE.id)!!.minVersion,
        )
        assertEquals(
            expected = ApiKeys.SASL_HANDSHAKE.latestVersion(),
            actual = versionsResponse.apiVersion(ApiKeys.SASL_HANDSHAKE.id)!!.maxVersion,
        )
        assertEquals(
            expected = ApiKeys.SASL_AUTHENTICATE.oldestVersion(),
            actual = versionsResponse.apiVersion(ApiKeys.SASL_AUTHENTICATE.id)!!.minVersion,
        )
        assertEquals(
            expected = ApiKeys.SASL_AUTHENTICATE.latestVersion(),
            actual = versionsResponse.apiVersion(ApiKeys.SASL_AUTHENTICATE.id)!!.maxVersion,
        )

        // Send SaslHandshakeRequest and check response
        val handshakeResponse = sendHandshakeRequestReceiveResponse(node, saslHandshakeVersion)
        assertEquals(listOf("PLAIN"), handshakeResponse.enabledMechanisms())

        // Complete manual authentication and check send/receive succeed
        authenticateUsingSaslPlainAndCheckConnection(
            node = node,
            enableSaslAuthenticateHeader = saslHandshakeVersion > 0,
        )
    }

    @Throws(Exception::class)
    private fun authenticateUsingSaslPlainAndCheckConnection(node: String, enableSaslAuthenticateHeader: Boolean) {
        // Authenticate using PLAIN username/password
        val authString = "\u0000" + TestJaasConfig.USERNAME + "\u0000" + TestJaasConfig.PASSWORD
        val authBuf = ByteBuffer.wrap(authString.toByteArray())
        if (enableSaslAuthenticateHeader) {
            val data = SaslAuthenticateRequestData().setAuthBytes(authBuf.array())
            val request = SaslAuthenticateRequest.Builder(data).build()
            sendKafkaRequestReceiveResponse(
                node = node,
                apiKey = ApiKeys.SASL_AUTHENTICATE,
                request = request,
            )
        } else {
            selector!!.send(NetworkSend(node, ByteBufferSend.sizePrefixed(authBuf)))
            waitForResponse()
        }

        // Check send/receive on the manually authenticated connection
        NetworkTestUtils.checkClientConnection(
            selector = selector!!,
            node = node,
            minMessageSize = 100,
            messageCount = 10,
        )
    }

    private fun configureMechanisms(clientMechanism: String, serverMechanisms: List<String>): TestJaasConfig {
        saslClientConfigs[SaslConfigs.SASL_MECHANISM] = clientMechanism
        saslServerConfigs[BrokerSecurityConfigs.SASL_ENABLED_MECHANISMS_CONFIG] = serverMechanisms
        saslServerConfigs[BrokerSecurityConfigs.CONNECTIONS_MAX_REAUTH_MS] = CONNECTIONS_MAX_REAUTH_MS_VALUE
        if (serverMechanisms.contains("DIGEST-MD5")) {
            saslServerConfigs["digest-md5." + BrokerSecurityConfigs.SASL_SERVER_CALLBACK_HANDLER_CLASS] =
                DigestServerCallbackHandler::class.java.getName()
        }
        return TestJaasConfig.createConfiguration(clientMechanism, serverMechanisms)
    }

    private fun configureDigestMd5ServerCallback(securityProtocol: SecurityProtocol) {
        val callbackPrefix = ListenerName.forSecurityProtocol(securityProtocol).saslMechanismConfigPrefix("DIGEST-MD5")
        saslServerConfigs[callbackPrefix + BrokerSecurityConfigs.SASL_SERVER_CALLBACK_HANDLER_CLASS] =
            DigestServerCallbackHandler::class.java
    }

    private fun createSelector(securityProtocol: SecurityProtocol, clientConfigs: Map<String, Any?>) {
        if (selector != null) {
            selector!!.close()
            selector = null
        }
        val saslMechanism = saslClientConfigs[SaslConfigs.SASL_MECHANISM] as String?
        channelBuilder = ChannelBuilders.clientChannelBuilder(
            securityProtocol = securityProtocol,
            contextType = JaasContext.Type.CLIENT,
            config = TestSecurityConfig(clientConfigs),
            listenerName = null,
            clientSaslMechanism = saslMechanism,
            time = time,
            saslHandshakeRequestEnable = true,
            logContext = LogContext()
        )
        selector = NetworkTestUtils.createSelector(channelBuilder = channelBuilder, time = time)
    }

    @Throws(Exception::class)
    private fun createEchoServer(securityProtocol: SecurityProtocol): NioEchoServer = createEchoServer(
        listenerName = ListenerName.forSecurityProtocol(securityProtocol),
        securityProtocol = securityProtocol,
    )

    @Throws(Exception::class)
    private fun createEchoServer(
        listenerName: ListenerName,
        securityProtocol: SecurityProtocol,
    ): NioEchoServer = NetworkTestUtils.createEchoServer(
        listenerName = listenerName,
        securityProtocol = securityProtocol,
        serverConfig = TestSecurityConfig(saslServerConfigs),
        credentialCache = credentialCache,
        time = time
    )

    @Throws(Exception::class)
    private fun createEchoServer(
        listenerName: ListenerName,
        securityProtocol: SecurityProtocol,
        tokenCache: DelegationTokenCache,
    ): NioEchoServer = NetworkTestUtils.createEchoServer(
        listenerName = listenerName,
        securityProtocol = securityProtocol,
        serverConfig = TestSecurityConfig(saslServerConfigs),
        credentialCache = credentialCache,
        failedAuthenticationDelayMs = 100,
        time = time,
        tokenCache = tokenCache,
    )

    @Throws(Exception::class)
    private fun createClientConnection(securityProtocol: SecurityProtocol, node: String) {
        createSelector(securityProtocol = securityProtocol, clientConfigs = saslClientConfigs)
        val addr = InetSocketAddress("localhost", server.port)
        selector!!.connect(
            id = node,
            address = addr,
            sendBufferSize = BUFFER_SIZE,
            receiveBufferSize = BUFFER_SIZE,
        )
    }

    @Throws(Exception::class)
    private fun checkClientConnection(node: String) = NetworkTestUtils.checkClientConnection(
        selector = selector!!,
        node = node,
        minMessageSize = 100,
        messageCount = 10,
    )

    @Throws(Exception::class)
    private fun closeClientConnectionIfNecessary() {
        if (selector != null) {
            selector!!.close()
            selector = null
        }
    }

    /*
     * Also closes the connection after creating/checking it
     */
    @Throws(Exception::class)
    private fun createAndCheckClientConnection(securityProtocol: SecurityProtocol, node: String) {
        try {
            createClientConnection(securityProtocol, node)
            checkClientConnection(node)
        } finally {
            closeClientConnectionIfNecessary()
        }
    }

    @Throws(Exception::class)
    private fun createAndCheckClientAuthenticationFailure(
        securityProtocol: SecurityProtocol,
        node: String,
        mechanism: String,
        expectedErrorMessage: String?,
    ) {
        val finalState = createAndCheckClientConnectionFailure(securityProtocol, node)
        val exception: Exception? = finalState.exception
        assertIs<SaslAuthenticationException>(exception, "Invalid exception class ${exception?.javaClass}")
        val expectedExceptionMessage = expectedErrorMessage
            ?: "Authentication failed during authentication due to invalid credentials with SASL mechanism $mechanism"
        assertEquals(expectedExceptionMessage, exception.message)
    }

    @Throws(Exception::class)
    private fun createAndCheckClientConnectionFailure(securityProtocol: SecurityProtocol, node: String): ChannelState {
        return try {
            createClientConnection(securityProtocol, node)
            waitForChannelClose(
                selector = selector!!,
                node = node,
                channelState = ChannelState.State.AUTHENTICATION_FAILED,
            )
        } finally {
            closeClientConnectionIfNecessary()
        }
    }

    @Throws(Exception::class)
    private fun createAndCheckClientConnectionAndPrincipal(
        securityProtocol: SecurityProtocol,
        node: String,
        expectedPrincipal: KafkaPrincipal,
    ) {
        try {
            assertEquals(emptyList(), server.selector.channels())
            createClientConnection(securityProtocol, node)
            NetworkTestUtils.waitForChannelReady(selector!!, node)
            assertEquals(expectedPrincipal, server.selector.channels()[0].principal())
            checkClientConnection(node)
        } finally {
            closeClientConnectionIfNecessary()
            TestUtils.waitForCondition(
                testCondition = { server.selector.channels().isEmpty() },
                conditionDetails = "Channel not removed after disconnection",
            )
        }
    }

    @Throws(Exception::class)
    private fun createAndCheckSslAuthenticationFailure(securityProtocol: SecurityProtocol, node: String) {
        val finalState = createAndCheckClientConnectionFailure(securityProtocol, node)
        val exception = finalState.exception
        assertIs<SslAuthenticationException>(exception)
    }

    @Throws(Exception::class)
    private fun checkAuthenticationAndReauthentication(securityProtocol: SecurityProtocol, node: String) {
        try {
            createClientConnection(securityProtocol, node)
            checkClientConnection(node)
            server.verifyAuthenticationMetrics(
                successfulAuthentications = 1,
                failedAuthentications = 0,
            )
            /*
             * Now re-authenticate the connection. First we have to sleep long enough so
             * that the next write will cause re-authentication, which we expect to succeed.
             */
            delay((CONNECTIONS_MAX_REAUTH_MS_VALUE * 1.1).toLong())
            server.verifyReauthenticationMetrics(
                successfulReauthentications = 0,
                failedReauthentications = 0,
            )
            checkClientConnection(node)
            server.verifyReauthenticationMetrics(
                successfulReauthentications = 1,
                failedReauthentications = 0,
            )
        } finally {
            closeClientConnectionIfNecessary()
        }
    }

    @Throws(IOException::class)
    private fun sendKafkaRequestReceiveResponse(
        node: String,
        apiKey: ApiKeys,
        request: AbstractRequest,
    ): AbstractResponse {
        val header = RequestHeader(
            requestApiKey = apiKey,
            requestVersion = request.version,
            clientId = "someclient",
            correlationId = nextCorrelationId++,
        )
        val send = NetworkSend(node, request.toSend(header))
        selector!!.send(send)
        val responseBuffer = waitForResponse()
        return NetworkClient.parseResponse(responseBuffer!!, header)
    }

    @Throws(Exception::class)
    private fun sendHandshakeRequestReceiveResponse(node: String, version: Short): SaslHandshakeResponse {
        val handshakeRequest = buildSaslHandshakeRequest(mechanism = "PLAIN", version = version)
        val response = sendKafkaRequestReceiveResponse(
            node = node,
            apiKey = ApiKeys.SASL_HANDSHAKE,
            request = handshakeRequest,
        ) as SaslHandshakeResponse
        assertEquals(Errors.NONE, response.error())
        return response
    }

    @Throws(Exception::class)
    private fun sendVersionRequestReceiveResponse(node: String): ApiVersionsResponse {
        val handshakeRequest = createApiVersionsRequestV0()
        val response = sendKafkaRequestReceiveResponse(
            node = node,
            apiKey = ApiKeys.API_VERSIONS,
            request = handshakeRequest,
        ) as ApiVersionsResponse

        assertEquals(Errors.NONE.code, response.data().errorCode)
        return response
    }

    @Throws(IOException::class)
    private fun waitForResponse(): ByteBuffer? {
        var waitSeconds = 10
        do { selector!!.poll(1000) } while (selector!!.completedReceives().isEmpty() && waitSeconds-- > 0)
        assertEquals(1, selector!!.completedReceives().size)
        return selector!!.completedReceives().first().payload()
    }

    class TestServerCallbackHandler : PlainServerCallbackHandler() {

        @Volatile
        private var configured = false

        override fun configure(
            configs: Map<String, *>,
            saslMechanism: String,
            jaasConfigEntries: List<AppConfigurationEntry>,
        ) {
            check(!configured) { "Server callback handler configured twice" }
            configured = true
            super.configure(configs, saslMechanism, jaasConfigEntries)
        }

        override fun authenticate(username: String?, password: CharArray?): Boolean {
            check(configured) { "Server callback handler not configured" }
            return USERNAME == username && String(password!!) == PASSWORD
        }

        companion object {

            const val USERNAME = "TestServerCallbackHandler-user"

            const val PASSWORD = "TestServerCallbackHandler-password"
        }
    }

    private fun buildSaslHandshakeRequest(mechanism: String, version: Short): SaslHandshakeRequest {
        return SaslHandshakeRequest.Builder(SaslHandshakeRequestData().setMechanism(mechanism)).build(version)
    }

    @Throws(NoSuchAlgorithmException::class)
    private fun updateScramCredentialCache(username: String, password: String) {
        for (mechanism in saslServerConfigs[BrokerSecurityConfigs.SASL_ENABLED_MECHANISMS_CONFIG] as List<*>) {
            val scramMechanism = ScramMechanism.forMechanismName(mechanism as String)
            if (scramMechanism != null) {
                val formatter = ScramFormatter(scramMechanism)
                val credential = formatter.generateCredential(password, 4096)
                credentialCache.cache(scramMechanism.mechanismName, ScramCredential::class.java)!!
                    .put(username, credential)
            }
        }
    }

    // Creates an ApiVersionsRequest with version 0. Using v0 in tests since
    // SaslClientAuthenticator always uses version 0
    private fun createApiVersionsRequestV0(): ApiVersionsRequest {
        return ApiVersionsRequest.Builder(0.toShort()).build()
    }

    @Throws(NoSuchAlgorithmException::class)
    private fun updateTokenCredentialCache(username: String, password: String) {
        for (mechanism in saslServerConfigs[BrokerSecurityConfigs.SASL_ENABLED_MECHANISMS_CONFIG] as List<*>) {
            val scramMechanism = ScramMechanism.forMechanismName(mechanism as String)
            if (scramMechanism != null) {
                val formatter = ScramFormatter(scramMechanism)
                val credential = formatter.generateCredential(password = password, iterations = 4096)
                server.tokenCache.credentialCache(scramMechanism.mechanismName)!!.put(username, credential)
            }
        }
    }

    class TestClientCallbackHandler : AuthenticateCallbackHandler {

        @Volatile
        private var configured = false

        override fun configure(
            configs: Map<String, *>,
            saslMechanism: String,
            jaasConfigEntries: List<AppConfigurationEntry>,
        ) {
            check(!configured) { "Client callback handler configured twice" }
            configured = true
        }

        @Throws(UnsupportedCallbackException::class)
        override fun handle(callbacks: Array<Callback>) {
            check(configured) { "Client callback handler not configured" }
            for (callback in callbacks) {
                when (callback) {
                    is NameCallback -> callback.name = USERNAME
                    is PasswordCallback -> callback.password = PASSWORD.toCharArray()
                    else -> throw UnsupportedCallbackException(callback)
                }
            }
        }

        override fun close() = Unit

        companion object {

            const val USERNAME = "TestClientCallbackHandler-user"

            const val PASSWORD = "TestClientCallbackHandler-password"
        }
    }

    class TestLogin : Login {

        private var contextName: String? = null

        private var configuration: Configuration? = null

        private var subject: Subject? = null

        override fun configure(
            configs: Map<String, *>,
            contextName: String,
            jaasConfiguration: Configuration,
            loginCallbackHandler: AuthenticateCallbackHandler,
        ) {
            assertEquals(1, jaasConfiguration.getAppConfigurationEntry(contextName).size)
            this.contextName = contextName
            this.configuration = jaasConfiguration
        }

        @Throws(LoginException::class)
        override fun login(): LoginContext {
            val context = LoginContext(contextName, null, DefaultLoginCallbackHandler(), configuration)
            context.login()
            subject = context.getSubject().apply {
                publicCredentials.clear()
                privateCredentials.clear()
                publicCredentials.add(TestJaasConfig.USERNAME)
                privateCredentials.add(TestJaasConfig.PASSWORD)
            }
            loginCount.incrementAndGet()
            return context
        }

        override fun subject(): Subject? = subject

        override fun serviceName(): String = "kafka"

        override fun close() = Unit

        companion object {
            var loginCount = AtomicInteger()
        }
    }

    class TestLoginCallbackHandler : AuthenticateCallbackHandler {

        @Volatile
        private var configured = false

        override fun configure(
            configs: Map<String, *>,
            saslMechanism: String,
            jaasConfigEntries: List<AppConfigurationEntry>,
        ) {
            check(!configured) { "Login callback handler configured twice" }
            configured = true
        }

        override fun handle(callbacks: Array<Callback>) {
            check(configured) { "Login callback handler not configured" }
            for (callback in callbacks) {
                when (callback) {
                    is NameCallback -> callback.name = TestJaasConfig.USERNAME
                    is PasswordCallback -> callback.password = TestJaasConfig.PASSWORD.toCharArray()
                }
            }
        }

        override fun close() = Unit
    }

    class TestPlainLoginModule : PlainLoginModule() {

        override fun initialize(
            subject: Subject,
            callbackHandler: CallbackHandler,
            sharedState: Map<String, *>,
            options: Map<String, *>,
        ) {
            try {
                val nameCallback = NameCallback("name:")
                val passwordCallback = PasswordCallback("password:", false)
                callbackHandler.handle(arrayOf<Callback>(nameCallback, passwordCallback))
                subject.publicCredentials.add(nameCallback.name)
                subject.privateCredentials.add(String(passwordCallback.password))
            } catch (e: Exception) {
                throw SaslAuthenticationException("Login initialization failed", e)
            }
        }
    }

    /*
     * Create an alternate login callback handler that continually returns a
     * different principal
     */
    class AlternateLoginCallbackHandler : AuthenticateCallbackHandler {

        @Throws(IOException::class, UnsupportedCallbackException::class)
        override fun handle(callbacks: Array<Callback>) {
            DELEGATE.handle(callbacks)
            // now change any returned token to have a different principal name
            if (callbacks.isNotEmpty()) for (callback in callbacks) {
                if (callback is OAuthBearerTokenCallback) {
                    val token = callback.token
                    if (token != null) {
                        val changedPrincipalNameToUse = (token.principalName() + (++numInvocations).toString())
                        val headerJson = "{${claimOrHeaderJsonText(claimName = "alg", claimValue = "none")}}"
                        /*
                         * Use a short lifetime so the background refresh thread replaces it before we
                         * re-authenticate
                         */
                        val lifetimeSecondsValueToUse = "1"
                        val claimsJson = try {
                            String.format(
                                "{%s,%s,%s}",
                                expClaimText(lifetimeSecondsValueToUse.toLong()),
                                claimOrHeaderJsonText(claimName = "iat", claimValue = time.milliseconds() / 1000.0),
                                claimOrHeaderJsonText(claimName = "sub", claimValue = changedPrincipalNameToUse),
                            )
                        } catch (e: NumberFormatException) {
                            throw OAuthBearerConfigException(e.message)
                        }
                        try {
                            val urlEncoderNoPadding = Base64.getUrlEncoder().withoutPadding()
                            val jws = OAuthBearerUnsecuredJws(
                                compactSerialization = String.format(
                                    "%s.%s.",
                                    urlEncoderNoPadding.encodeToString(headerJson.toByteArray()),
                                    urlEncoderNoPadding.encodeToString(claimsJson.toByteArray()),
                                ),
                                principalClaimName = "sub",
                                scopeClaimName = "scope",
                            )
                            callback.token = jws
                        } catch (e: OAuthBearerIllegalTokenException) {
                            // occurs if the principal claim doesn't exist or has an empty value
                            throw OAuthBearerConfigException(e.message, e)
                        }
                    }
                }
            }
        }

        override fun configure(
            configs: Map<String, *>,
            saslMechanism: String,
            jaasConfigEntries: List<AppConfigurationEntry>,
        ) = DELEGATE.configure(configs, saslMechanism, jaasConfigEntries)

        override fun close() = DELEGATE.close()

        companion object {

            private val DELEGATE = OAuthBearerUnsecuredLoginCallbackHandler()

            private const val QUOTE = "\""

            private var numInvocations = 0

            private fun claimOrHeaderJsonText(claimName: String, claimValue: String): String =
                "$QUOTE$claimName$QUOTE:$QUOTE$claimValue$QUOTE"

            private fun claimOrHeaderJsonText(claimName: String, claimValue: Number): String =
                "$QUOTE$claimName$QUOTE:$claimValue"

            private fun expClaimText(lifetimeSeconds: Long): String = claimOrHeaderJsonText(
                claimName = "exp",
                claimValue = time.milliseconds() / 1000.0 + lifetimeSeconds,
            )
        }
    }

    /*
     * Define a channel builder that starts with the DIGEST-MD5 mechanism and then
     * switches to the PLAIN mechanism
     */
    private class AlternateSaslChannelBuilder(
        mode: Mode,
        jaasContexts: Map<String, JaasContext>,
        securityProtocol: SecurityProtocol,
        listenerName: ListenerName?,
        isInterBrokerListener: Boolean,
        clientSaslMechanism: String,
        handshakeRequestEnable: Boolean,
        credentialCache: CredentialCache?,
        tokenCache: DelegationTokenCache?,
        time: Time,
    ) : SaslChannelBuilder(
        mode = mode,
        jaasContexts = jaasContexts,
        securityProtocol = securityProtocol,
        listenerName = listenerName,
        isInterBrokerListener = isInterBrokerListener,
        clientSaslMechanism = clientSaslMechanism,
        handshakeRequestEnable = handshakeRequestEnable,
        credentialCache = credentialCache,
        tokenCache = tokenCache,
        sslClientAuthOverride = null,
        time = time,
        logContext = LogContext(),
        apiVersionSupplier = {
            TestUtils.defaultApiVersionsResponse(listenerType = ApiMessageType.ListenerType.ZK_BROKER)
        }
    ) {
        private var numInvocations = 0

        override fun buildClientAuthenticator(
            configs: Map<String, *>,
            callbackHandler: AuthenticateCallbackHandler,
            id: String,
            serverHost: String,
            servicePrincipal: String,
            transportLayer: TransportLayer,
            subject: Subject,
        ): SaslClientAuthenticator {
            return if (++numInvocations == 1) SaslClientAuthenticator(
                configs = configs,
                callbackHandler = callbackHandler,
                node = id,
                subject = subject,
                servicePrincipal = servicePrincipal,
                host = serverHost,
                mechanism = "DIGEST-MD5",
                handshakeRequestEnable = true,
                transportLayer = transportLayer,
                time = time,
                logContext = LogContext(),
            ) else object : SaslClientAuthenticator(
                configs = configs,
                callbackHandler = callbackHandler,
                node = id,
                subject = subject,
                servicePrincipal = servicePrincipal,
                host = serverHost,
                mechanism = "PLAIN",
                handshakeRequestEnable = true,
                transportLayer = transportLayer,
                time = time,
                logContext = LogContext(),
            ) {
                override fun createSaslHandshakeRequest(version: Short): SaslHandshakeRequest {
                    return SaslHandshakeRequest.Builder(SaslHandshakeRequestData().setMechanism("PLAIN"))
                        .build(version)
                }
            }
        }
    }

    class SaslSslPrincipalBuilder : KafkaPrincipalBuilder {
        override fun build(context: AuthenticationContext): KafkaPrincipal {
            val saslContext = context as SaslAuthenticationContext
            val sslSession = assertNotNull(saslContext.sslSession)
            val sslPrincipal: String = try {
                sslSession.peerPrincipal.name
            } catch (e: SSLPeerUnverifiedException) {
                KafkaPrincipal.ANONYMOUS.getName()
            }
            val saslPrincipal = saslContext.server.authorizationID
            return saslSslPrincipal(saslPrincipal, sslPrincipal)
        }

        companion object {
            fun saslSslPrincipal(saslPrincipal: String, sslPrincipal: String) = KafkaPrincipal(
                principalType = KafkaPrincipal.USER_TYPE,
                name = "$saslPrincipal:$sslPrincipal"
            )
        }
    }

    companion object {

        private const val CONNECTIONS_MAX_REAUTH_MS_VALUE = 100L

        private const val BUFFER_SIZE = 4 * 1024

        private var time = Time.SYSTEM

        @Throws(InterruptedException::class)
        private fun delay(delayMillis: Long) {
            val startTime = System.currentTimeMillis()
            while (System.currentTimeMillis() - startTime < delayMillis)
                Thread.sleep(CONNECTIONS_MAX_REAUTH_MS_VALUE / 5)
        }
    }
}
