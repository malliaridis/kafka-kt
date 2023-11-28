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
import org.apache.kafka.common.config.SaslConfigs
import org.apache.kafka.common.config.internals.BrokerSecurityConfigs
import org.apache.kafka.common.errors.SaslAuthenticationException
import org.apache.kafka.common.network.CertStores
import org.apache.kafka.common.network.ChannelBuilder
import org.apache.kafka.common.network.ChannelBuilders.clientChannelBuilder
import org.apache.kafka.common.network.ChannelState
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.network.NetworkTestUtils
import org.apache.kafka.common.network.NioEchoServer
import org.apache.kafka.common.network.Selector
import org.apache.kafka.common.security.JaasContext
import org.apache.kafka.common.security.TestSecurityConfig
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.security.authenticator.TestDigestLoginModule.DigestServerCallbackHandler
import org.apache.kafka.common.utils.LogContext
import org.apache.kafka.common.utils.MockTime
import org.apache.kafka.test.TestUtils.waitForCondition
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertTrue

abstract class SaslAuthenticatorFailureDelayTest(private val failedAuthenticationDelayMs: Int) {
    
    private val time = MockTime(1)
    
    private lateinit var server: NioEchoServer
    
    private var selector: Selector? = null
    
    private lateinit var channelBuilder: ChannelBuilder
    
    private lateinit var serverCertStores: CertStores
    
    private lateinit var clientCertStores: CertStores
    
    private lateinit var saslClientConfigs: MutableMap<String, Any?>
    
    private lateinit var saslServerConfigs: MutableMap<String, Any?>
    
    private lateinit var credentialCache: CredentialCache
    
    private var startTimeMs: Long = 0
    
    @BeforeEach
    @Throws(Exception::class)
    fun setup() {
        LoginManager.closeAll()
        serverCertStores = CertStores(true, "localhost")
        clientCertStores = CertStores(false, "localhost")
        saslServerConfigs = serverCertStores.getTrustingConfig(clientCertStores).toMutableMap()
        saslClientConfigs = clientCertStores.getTrustingConfig(serverCertStores).toMutableMap()
        credentialCache = CredentialCache()
        SaslAuthenticatorTest.TestLogin.loginCount.set(0)
        startTimeMs = time.milliseconds()
    }

    @AfterEach
    @Throws(Exception::class)
    fun teardown() {
        val now = time.milliseconds()
        if (::server.isInitialized) server.close()
        if (selector != null) selector!!.close()
        assertTrue(
            actual = now - startTimeMs >= failedAuthenticationDelayMs,
            message = "timeSpent: " + (now - startTimeMs),
        )
    }

    /**
     * Tests that SASL/PLAIN clients with invalid password fail authentication.
     */
    @Test
    @Throws(Exception::class)
    fun testInvalidPasswordSaslPlain() {
        val node = "0"
        val securityProtocol = SecurityProtocol.SASL_SSL
        val jaasConfig = configureMechanisms("PLAIN", mutableListOf("PLAIN"))
        jaasConfig.setClientOptions("PLAIN", TestJaasConfig.USERNAME, "invalidpassword")
        server = createEchoServer(securityProtocol = securityProtocol)
        createAndCheckClientAuthenticationFailure(
            securityProtocol, node, "PLAIN",
            "Authentication failed: Invalid username or password"
        )
        server.verifyAuthenticationMetrics(0, 1)
    }

    /**
     * Tests that SASL/SCRAM clients with invalid password fail authentication with
     * connection close delay if configured.
     */
    @Test
    @Throws(Exception::class)
    fun testInvalidPasswordSaslScram() {
        val node = "0"
        val securityProtocol = SecurityProtocol.SASL_SSL
        val jaasConfig = configureMechanisms(
            clientMechanism = "SCRAM-SHA-256",
            serverMechanisms = listOf("SCRAM-SHA-256"),
        )
        jaasConfig.setClientOptions("SCRAM-SHA-256", TestJaasConfig.USERNAME, "invalidpassword")
        server = createEchoServer(securityProtocol = securityProtocol)
        createAndCheckClientAuthenticationFailure(
            securityProtocol = securityProtocol,
            node = node,
            mechanism = "SCRAM-SHA-256",
            expectedErrorMessage = null,
        )
        server.verifyAuthenticationMetrics(0, 1)
    }

    /**
     * Tests that clients with disabled SASL mechanism fail authentication with
     * connection close delay if configured.
     */
    @Test
    @Throws(Exception::class)
    fun testDisabledSaslMechanism() {
        val node = "0"
        val securityProtocol = SecurityProtocol.SASL_SSL
        val jaasConfig = configureMechanisms("SCRAM-SHA-256", listOf("SCRAM-SHA-256"))
        jaasConfig.setClientOptions("PLAIN", TestJaasConfig.USERNAME, "invalidpassword")
        server = createEchoServer(securityProtocol = securityProtocol)
        createAndCheckClientAuthenticationFailure(
            securityProtocol = securityProtocol,
            node = node,
            mechanism = "SCRAM-SHA-256",
            expectedErrorMessage = null,
        )
        server.verifyAuthenticationMetrics(0, 1)
    }

    /**
     * Tests client connection close before response for authentication failure is sent.
     */
    @Test
    @Throws(Exception::class)
    fun testClientConnectionClose() {
        val node = "0"
        val securityProtocol = SecurityProtocol.SASL_SSL
        val jaasConfig = configureMechanisms("PLAIN", mutableListOf("PLAIN"))
        jaasConfig.setClientOptions("PLAIN", TestJaasConfig.USERNAME, "invalidpassword")
        server = createEchoServer(securityProtocol = securityProtocol)
        createClientConnection(securityProtocol, node)
        val delayedClosingChannels = NetworkTestUtils.delayedClosingChannels(server.selector)!!

        // Wait until server has established connection with client and has processed the auth failure
        waitForCondition(
            testCondition = {
                poll(selector)
                server.selector.channels().isNotEmpty()
            },
            conditionDetails = "Timeout waiting for connection",
        )
        waitForCondition(
            testCondition = {
                poll(selector)
                failedAuthenticationDelayMs == 0 || delayedClosingChannels.isNotEmpty()
            },
            conditionDetails = "Timeout waiting for auth failure",
        )
        selector!!.close()
        selector = null

        // Now that client connection is closed, wait until server notices the disconnection and removes it from the
        // list of connected channels and from delayed response for auth failure
        waitForCondition(
            testCondition = { failedAuthenticationDelayMs == 0 || delayedClosingChannels.isEmpty() },
            conditionDetails = "Timeout waiting for delayed response remove",
        )
        waitForCondition(
            testCondition = { server.selector.channels().isEmpty() },
            conditionDetails = "Timeout waiting for connection close",
        )

        // Try forcing completion of delayed channel close
        waitForCondition(
            testCondition = { time.milliseconds() > startTimeMs + failedAuthenticationDelayMs + 1 },
            conditionDetails = "Timeout when waiting for auth failure response timeout to elapse",
        )
        NetworkTestUtils.completeDelayedChannelClose(server.selector, time.nanoseconds())
    }

    private fun poll(selector: Selector?) {
        try {
            selector!!.poll(50)
        } catch (e: IOException) {
            throw RuntimeException("Unexpected failure during selector poll", e)
        }
    }

    private fun configureMechanisms(clientMechanism: String, serverMechanisms: List<String>): TestJaasConfig {
        saslClientConfigs[SaslConfigs.SASL_MECHANISM] = clientMechanism
        saslServerConfigs[BrokerSecurityConfigs.SASL_ENABLED_MECHANISMS_CONFIG] = serverMechanisms
        if (serverMechanisms.contains("DIGEST-MD5")) {
            saslServerConfigs["digest-md5." + BrokerSecurityConfigs.SASL_SERVER_CALLBACK_HANDLER_CLASS] =
                DigestServerCallbackHandler::class.java.getName()
        }
        return TestJaasConfig.createConfiguration(clientMechanism, serverMechanisms)
    }

    private fun createSelector(securityProtocol: SecurityProtocol, clientConfigs: Map<String, Any?>) {
        if (selector != null) {
            selector!!.close()
            selector = null
        }
        val saslMechanism = saslClientConfigs[SaslConfigs.SASL_MECHANISM] as String?
        channelBuilder = clientChannelBuilder(
            securityProtocol = securityProtocol,
            contextType = JaasContext.Type.CLIENT,
            config = TestSecurityConfig(clientConfigs),
            listenerName = null,
            clientSaslMechanism = saslMechanism,
            time = time,
            saslHandshakeRequestEnable = true,
            logContext = LogContext(),
        )
        selector = NetworkTestUtils.createSelector(channelBuilder, time)
    }

    @Throws(Exception::class)
    private fun createEchoServer(
        securityProtocol: SecurityProtocol,
        listenerName: ListenerName = ListenerName.forSecurityProtocol(securityProtocol),
    ): NioEchoServer {
        return NetworkTestUtils.createEchoServer(
            listenerName, securityProtocol,
            TestSecurityConfig(saslServerConfigs), credentialCache, time
        )
    }

    @Throws(Exception::class)
    private fun createClientConnection(securityProtocol: SecurityProtocol, node: String) {
        createSelector(
            securityProtocol = securityProtocol,
            clientConfigs = saslClientConfigs,
        )
        val addr = InetSocketAddress("localhost", server.port)
        selector!!.connect(
            id = node,
            address = addr,
            sendBufferSize = BUFFER_SIZE,
            receiveBufferSize = BUFFER_SIZE,
        )
    }

    @Throws(Exception::class)
    private fun createAndCheckClientAuthenticationFailure(
        securityProtocol: SecurityProtocol,
        node: String,
        mechanism: String,
        expectedErrorMessage: String?,
    ) {
        val finalState = createAndCheckClientConnectionFailure(securityProtocol, node)
        val exception = finalState.exception()
        assertIs<SaslAuthenticationException>(
            value = exception,
            message = "Invalid exception class ${exception!!.javaClass}",
        )
        assertEquals(
            expected = (expectedErrorMessage
                ?: ("Authentication failed during authentication due to invalid credentials " +
                        "with SASL mechanism $mechanism")),
            actual = exception.message,
        )
    }

    @Throws(Exception::class)
    private fun createAndCheckClientConnectionFailure(
        securityProtocol: SecurityProtocol,
        node: String,
    ): ChannelState {
        createClientConnection(securityProtocol, node)
        val finalState = NetworkTestUtils.waitForChannelClose(
            selector = selector!!,
            node = node,
            channelState = ChannelState.State.AUTHENTICATION_FAILED,
        )
        selector!!.close()
        selector = null
        return finalState
    }

    companion object {
        private const val BUFFER_SIZE = 4 * 1024
    }
}
