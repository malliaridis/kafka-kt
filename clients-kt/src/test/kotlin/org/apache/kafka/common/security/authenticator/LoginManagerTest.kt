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

import org.apache.kafka.common.config.SaslConfigs
import org.apache.kafka.common.config.types.Password
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.security.JaasContext
import org.apache.kafka.common.security.plain.PlainLoginModule
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotSame
import kotlin.test.assertSame

class LoginManagerTest {

    private lateinit var dynamicPlainContext: Password

    private lateinit var dynamicDigestContext: Password

    @BeforeEach
    fun setUp() {
        dynamicPlainContext = Password(
            """${PlainLoginModule::class.java.getName()} required user="plainuser" password="plain-secret";"""
        )
        dynamicDigestContext = Password(
            """${TestDigestLoginModule::class.java.getName()} required user="digestuser" password="digest-secret";"""
        )
        TestJaasConfig.createConfiguration(
            clientMechanism = "SCRAM-SHA-256",
            serverMechanisms = listOf("SCRAM-SHA-256"),
        )
    }

    @AfterEach
    fun tearDown() {
        LoginManager.closeAll()
    }

    @Test
    @Throws(Exception::class)
    fun testClientLoginManager() {
        val configs = mapOf("sasl.jaas.config" to dynamicPlainContext)
        val dynamicContext = JaasContext.loadClientContext(configs)
        val staticContext = JaasContext.loadClientContext(emptyMap<String, Any>())
        val dynamicLogin = LoginManager.acquireLoginManager(
            jaasContext = dynamicContext,
            saslMechanism = "PLAIN",
            defaultLoginClass = DefaultLogin::class.java,
            configs = configs,
        )
        assertEquals(dynamicPlainContext, dynamicLogin.cacheKey)
        val staticLogin = LoginManager.acquireLoginManager(
            jaasContext = staticContext,
            saslMechanism = "SCRAM-SHA-256",
            defaultLoginClass = DefaultLogin::class.java,
            configs = configs
        )
        assertNotSame(dynamicLogin, staticLogin)
        assertEquals("KafkaClient", staticLogin.cacheKey)
        assertSame(
            expected = dynamicLogin,
            actual = LoginManager.acquireLoginManager(
                jaasContext = dynamicContext,
                saslMechanism = "PLAIN",
                defaultLoginClass = DefaultLogin::class.java,
                configs = configs,
            )
        )
        assertSame(
            expected = staticLogin,
            actual = LoginManager.acquireLoginManager(
                jaasContext = staticContext,
                saslMechanism = "SCRAM-SHA-256",
                defaultLoginClass = DefaultLogin::class.java,
                configs = configs,
            )
        )
        verifyLoginManagerRelease(
            loginManager = dynamicLogin,
            acquireCount = 2,
            jaasContext = dynamicContext,
            configs = configs,
        )
        verifyLoginManagerRelease(
            loginManager = staticLogin,
            acquireCount = 2,
            jaasContext = staticContext,
            configs = configs,
        )
    }

    @Test
    @Throws(Exception::class)
    fun testServerLoginManager() {
        val configs = mapOf(
            "plain.sasl.jaas.config" to dynamicPlainContext,
            "digest-md5.sasl.jaas.config" to dynamicDigestContext,
        )
        val listenerName = ListenerName("listener1")
        val plainJaasContext = JaasContext.loadServerContext(
            listenerName = listenerName,
            mechanism = "PLAIN",
            configs = configs,
        )
        val digestJaasContext = JaasContext.loadServerContext(
            listenerName = listenerName,
            mechanism = "DIGEST-MD5",
            configs = configs,
        )
        val scramJaasContext = JaasContext.loadServerContext(
            listenerName = listenerName,
            mechanism = "SCRAM-SHA-256",
            configs = configs,
        )
        val dynamicPlainLogin = LoginManager.acquireLoginManager(
            jaasContext = plainJaasContext,
            saslMechanism = "PLAIN",
            defaultLoginClass = DefaultLogin::class.java,
            configs = configs,
        )
        assertEquals(dynamicPlainContext, dynamicPlainLogin.cacheKey)
        val dynamicDigestLogin = LoginManager.acquireLoginManager(
            jaasContext = digestJaasContext,
            saslMechanism = "DIGEST-MD5",
            defaultLoginClass = DefaultLogin::class.java,
            configs = configs,
        )
        assertNotSame(dynamicPlainLogin, dynamicDigestLogin)
        assertEquals(dynamicDigestContext, dynamicDigestLogin.cacheKey)
        val staticScramLogin = LoginManager.acquireLoginManager(
            jaasContext = scramJaasContext,
            saslMechanism = "SCRAM-SHA-256",
            defaultLoginClass = DefaultLogin::class.java,
            configs = configs,
        )
        assertNotSame(dynamicPlainLogin, staticScramLogin)
        assertEquals("KafkaServer", staticScramLogin.cacheKey)
        assertSame(
            expected = dynamicPlainLogin,
            actual = LoginManager.acquireLoginManager(
                jaasContext = plainJaasContext,
                saslMechanism = "PLAIN",
                defaultLoginClass = DefaultLogin::class.java,
                configs = configs,
            ),
        )
        assertSame(
            expected = dynamicDigestLogin,
            actual = LoginManager.acquireLoginManager(
                jaasContext = digestJaasContext,
                saslMechanism = "DIGEST-MD5",
                defaultLoginClass = DefaultLogin::class.java,
                configs = configs,
            ),
        )
        assertSame(
            expected = staticScramLogin,
            actual = LoginManager.acquireLoginManager(
                jaasContext = scramJaasContext,
                saslMechanism = "SCRAM-SHA-256",
                defaultLoginClass = DefaultLogin::class.java,
                configs = configs,
            ),
        )
        verifyLoginManagerRelease(
            loginManager = dynamicPlainLogin,
            acquireCount = 2,
            jaasContext = plainJaasContext,
            configs = configs,
        )
        verifyLoginManagerRelease(
            loginManager = dynamicDigestLogin,
            acquireCount = 2,
            jaasContext = digestJaasContext,
            configs = configs,
        )
        verifyLoginManagerRelease(
            loginManager = staticScramLogin,
            acquireCount = 2,
            jaasContext = scramJaasContext,
            configs = configs,
        )
    }

    @Test
    @Throws(Exception::class)
    fun testLoginManagerWithDifferentConfigs() {
        val configs1 = mapOf(
            "sasl.jaas.config" to dynamicPlainContext,
            "client.id" to "client",
            SaslConfigs.SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL to "http://host1:1234",
        )
        val configs2 = configs1 + mapOf(
            SaslConfigs.SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL to "http://host2:1234",
        )
        val dynamicContext = JaasContext.loadClientContext(configs1)
        val configs3 = configs1 + mapOf("client.id" to "client3")
        val dynamicLogin1 = LoginManager.acquireLoginManager(
            jaasContext = dynamicContext,
            saslMechanism = "PLAIN",
            defaultLoginClass = DefaultLogin::class.java,
            configs = configs1,
        )
        val dynamicLogin2 = LoginManager.acquireLoginManager(
            jaasContext = dynamicContext,
            saslMechanism = "PLAIN",
            defaultLoginClass = DefaultLogin::class.java,
            configs = configs2,
        )
        assertEquals(dynamicPlainContext, dynamicLogin1.cacheKey)
        assertEquals(dynamicPlainContext, dynamicLogin2.cacheKey)
        assertNotSame(dynamicLogin1, dynamicLogin2)
        assertSame(
            expected = dynamicLogin1,
            actual = LoginManager.acquireLoginManager(
                jaasContext = dynamicContext,
                saslMechanism = "PLAIN",
                defaultLoginClass = DefaultLogin::class.java,
                configs = configs1,
            ),
        )
        assertSame(
            expected = dynamicLogin2,
            actual = LoginManager.acquireLoginManager(
                jaasContext = dynamicContext,
                saslMechanism = "PLAIN",
                defaultLoginClass = DefaultLogin::class.java,
                configs = configs2,
            ),
        )
        assertSame(
            expected = dynamicLogin1,
            actual = LoginManager.acquireLoginManager(
                jaasContext = dynamicContext,
                saslMechanism = "PLAIN",
                defaultLoginClass = DefaultLogin::class.java,
                configs = configs1.toMap(),
            )
        )
        assertSame(
            expected = dynamicLogin1,
            actual = LoginManager.acquireLoginManager(
                jaasContext = dynamicContext,
                saslMechanism = "PLAIN",
                defaultLoginClass = DefaultLogin::class.java,
                configs = configs3,
            )
        )
        val staticContext = JaasContext.loadClientContext(emptyMap<String, Any>())
        val staticLogin1 = LoginManager.acquireLoginManager(
            jaasContext = staticContext,
            saslMechanism = "SCRAM-SHA-256",
            defaultLoginClass = DefaultLogin::class.java,
            configs = configs1,
        )
        val staticLogin2 = LoginManager.acquireLoginManager(
            jaasContext = staticContext,
            saslMechanism = "SCRAM-SHA-256",
            defaultLoginClass = DefaultLogin::class.java,
            configs = configs2,
        )
        assertNotSame(staticLogin1, dynamicLogin1)
        assertNotSame(staticLogin2, dynamicLogin2)
        assertNotSame(staticLogin1, staticLogin2)
        assertSame(
            expected = staticLogin1,
            actual = LoginManager.acquireLoginManager(
                jaasContext = staticContext,
                saslMechanism = "SCRAM-SHA-256",
                defaultLoginClass = DefaultLogin::class.java,
                configs = configs1,
            ),
        )
        assertSame(
            expected = staticLogin2,
            actual = LoginManager.acquireLoginManager(
                jaasContext = staticContext,
                saslMechanism = "SCRAM-SHA-256",
                defaultLoginClass = DefaultLogin::class.java,
                configs = configs2,
            ),
        )
        assertSame(
            expected = staticLogin1,
            actual = LoginManager.acquireLoginManager(
                jaasContext = staticContext,
                saslMechanism = "SCRAM-SHA-256",
                defaultLoginClass = DefaultLogin::class.java,
                configs = configs1.toMap(),
            ),
        )
        assertSame(
            expected = staticLogin1,
            actual = LoginManager.acquireLoginManager(
                jaasContext = staticContext,
                saslMechanism = "SCRAM-SHA-256",
                defaultLoginClass = DefaultLogin::class.java,
                configs = configs3,
            ),
        )
        verifyLoginManagerRelease(
            loginManager = dynamicLogin1,
            acquireCount = 4,
            jaasContext = dynamicContext,
            configs = configs1,
        )
        verifyLoginManagerRelease(
            loginManager = dynamicLogin2,
            acquireCount = 2,
            jaasContext = dynamicContext,
            configs = configs2,
        )
        verifyLoginManagerRelease(
            loginManager = staticLogin1,
            acquireCount = 4,
            jaasContext = staticContext,
            configs = configs1,
        )
        verifyLoginManagerRelease(
            loginManager = staticLogin2,
            acquireCount = 2,
            jaasContext = staticContext,
            configs = configs2,
        )
    }

    @Throws(Exception::class)
    private fun verifyLoginManagerRelease(
        loginManager: LoginManager,
        acquireCount: Int,
        jaasContext: JaasContext,
        configs: Map<String, *>,
    ) {

        // Release all except one reference and verify that the loginManager is still cached
        for (i in 0 until acquireCount - 1) loginManager.release()
        assertSame(
            expected = loginManager,
            actual = LoginManager.acquireLoginManager(
                jaasContext = jaasContext,
                saslMechanism = "PLAIN",
                defaultLoginClass = DefaultLogin::class.java,
                configs = configs,
            )
        )

        // Release all references and verify that new LoginManager is created on next acquire
        repeat(2) {
            // release all references
            loginManager.release()
        }
        val newLoginManager = LoginManager.acquireLoginManager(
            jaasContext = jaasContext,
            saslMechanism = "PLAIN",
            defaultLoginClass = DefaultLogin::class.java,
            configs = configs,
        )
        assertNotSame(loginManager, newLoginManager)
        newLoginManager.release()
    }
}
