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

import javax.security.auth.Subject
import javax.security.auth.callback.CallbackHandler
import javax.security.auth.login.LoginException
import javax.security.auth.spi.LoginModule
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.SaslConfigs
import org.apache.kafka.common.config.SslConfigs
import org.apache.kafka.common.config.internals.BrokerSecurityConfigs
import org.apache.kafka.common.message.ApiMessageType
import org.apache.kafka.common.requests.ApiVersionsResponse
import org.apache.kafka.common.security.JaasContext
import org.apache.kafka.common.security.TestSecurityConfig
import org.apache.kafka.common.security.auth.KafkaPrincipal
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.security.authenticator.TestJaasConfig
import org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule
import org.apache.kafka.common.security.plain.PlainLoginModule
import org.apache.kafka.common.security.scram.ScramLoginModule
import org.apache.kafka.common.utils.LogContext
import org.apache.kafka.common.utils.Time
import org.apache.kafka.test.TestUtils
import org.ietf.jgss.GSSContext
import org.ietf.jgss.GSSCredential
import org.ietf.jgss.GSSManager
import org.ietf.jgss.GSSName
import org.ietf.jgss.Oid
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.mockito.kotlin.any
import org.mockito.kotlin.mock
import org.mockito.kotlin.times
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotNull
import kotlin.test.assertSame
import kotlin.test.assertTrue

class SaslChannelBuilderTest {

    @AfterEach
    fun tearDown() {
        System.clearProperty(SaslChannelBuilder.GSS_NATIVE_PROP)
    }

    @Test
    fun testCloseBeforeConfigureIsIdempotent() {
        val builder = createChannelBuilder(
            securityProtocol = SecurityProtocol.SASL_PLAINTEXT,
            saslMechanism = "PLAIN",
        )
        builder.close()
        assertTrue(builder.loginManagers().isEmpty())
        builder.close()
        assertTrue(builder.loginManagers().isEmpty())
    }

    @Test
    fun testCloseAfterConfigIsIdempotent() {
        val builder = createChannelBuilder(
            securityProtocol = SecurityProtocol.SASL_PLAINTEXT,
            saslMechanism = "PLAIN",
        )
        builder.configure(emptyMap<String, Any?>())
        assertNotNull(builder.loginManagers()["PLAIN"])
        builder.close()
        assertTrue(builder.loginManagers().isEmpty())
        builder.close()
        assertTrue(builder.loginManagers().isEmpty())
    }

    @Test
    fun testLoginManagerReleasedIfConfigureThrowsException() {
        val builder = createChannelBuilder(SecurityProtocol.SASL_SSL, "PLAIN")
        assertFailsWith<KafkaException>("Exception should have been thrown") {
            // Use invalid config so that an exception is thrown
            builder.configure(mapOf(SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG to "1"))
        }
        assertTrue(builder.loginManagers().isEmpty())

        builder.close()
        assertTrue(builder.loginManagers().isEmpty())
    }

    @Test
    @Throws(Exception::class)
    fun testNativeGssapiCredentials() {
        System.setProperty(SaslChannelBuilder.GSS_NATIVE_PROP, "true")

        val jaasConfig = TestJaasConfig()
        jaasConfig.addEntry(
            name = "jaasContext",
            loginModule = TestGssapiLoginModule::class.java.getName(),
            options = emptyMap(),
        )
        val jaasContext = JaasContext("jaasContext", JaasContext.Type.SERVER, jaasConfig, null)
        val jaasContexts = mapOf("GSSAPI" to jaasContext)
        val gssManager = mock<GSSManager>()
        val gssName = mock<GSSName>()
        whenever(gssManager.createName(any<String>(), any())).thenAnswer { gssName }
        val oid = Oid("1.2.840.113554.1.2.2")
        whenever(
            gssManager.createCredential(
                gssName,
                GSSContext.INDEFINITE_LIFETIME,
                oid,
                GSSCredential.ACCEPT_ONLY
            )
        ).thenAnswer { mock<GSSCredential>() }

        val channelBuilder1 = createGssapiChannelBuilder(jaasContexts, gssManager)
        assertEquals(1, channelBuilder1.subject("GSSAPI")!!.principals.size)
        assertEquals(1, channelBuilder1.subject("GSSAPI")!!.privateCredentials.size)

        val channelBuilder2 = createGssapiChannelBuilder(jaasContexts, gssManager)
        assertEquals(1, channelBuilder2.subject("GSSAPI")!!.principals.size)
        assertEquals(1, channelBuilder2.subject("GSSAPI")!!.privateCredentials.size)
        assertSame(channelBuilder1.subject("GSSAPI"), channelBuilder2.subject("GSSAPI"))

        verify(gssManager, times(1))
            .createCredential(gssName, GSSContext.INDEFINITE_LIFETIME, oid, GSSCredential.ACCEPT_ONLY)
    }

    /**
     * Verify that unparsed broker configs don't break clients. This is to ensure that clients
     * created by brokers are not broken if broker configs are passed to clients.
     */
    @Test
    @Throws(Exception::class)
    fun testClientChannelBuilderWithBrokerConfigs() {
        val configs = mutableMapOf<String, Any?>()
        val certStores = CertStores(
            server = false,
            commonName = "client",
            sanHostName = "localhost",
        )
        configs.putAll(certStores.getTrustingConfig(certStores))
        configs[SaslConfigs.SASL_KERBEROS_SERVICE_NAME] = "kafka"
        configs.putAll(ConfigDef().withClientSaslSupport().parse(configs))

        for (field in BrokerSecurityConfigs::class.java.getFields()) {
            if (field.name.endsWith("_CONFIG"))
                configs[field[BrokerSecurityConfigs::class.java].toString()] = "somevalue"
        }
        val plainBuilder = createChannelBuilder(SecurityProtocol.SASL_PLAINTEXT, "PLAIN")
        plainBuilder.configure(configs)

        val gssapiBuilder = createChannelBuilder(SecurityProtocol.SASL_PLAINTEXT, "GSSAPI")
        gssapiBuilder.configure(configs)

        val oauthBearerBuilder = createChannelBuilder(SecurityProtocol.SASL_PLAINTEXT, "OAUTHBEARER")

        oauthBearerBuilder.configure(configs)

        val scramBuilder = createChannelBuilder(SecurityProtocol.SASL_PLAINTEXT, "SCRAM-SHA-256")
        scramBuilder.configure(configs)

        val saslSslBuilder = createChannelBuilder(SecurityProtocol.SASL_SSL, "PLAIN")
        saslSslBuilder.configure(configs)
    }

    private fun createGssapiChannelBuilder(
        jaasContexts: Map<String, JaasContext>,
        gssManager: GSSManager,
    ): SaslChannelBuilder {
        val channelBuilder = object : SaslChannelBuilder(
            mode = Mode.SERVER,
            jaasContexts = jaasContexts,
            securityProtocol = SecurityProtocol.SASL_PLAINTEXT,
            listenerName = ListenerName("GSSAPI"),
            isInterBrokerListener = false,
            clientSaslMechanism = "GSSAPI",
            handshakeRequestEnable = true,
            credentialCache = null,
            tokenCache = null,
            sslClientAuthOverride = null,
            time = Time.SYSTEM,
            logContext = LogContext(),
            apiVersionSupplier = defaultApiVersionsSupplier,
        ) {
            override fun gssManager(): GSSManager = gssManager
        }
        val props = mapOf(SaslConfigs.SASL_KERBEROS_SERVICE_NAME to "kafka")
        channelBuilder.configure(TestSecurityConfig(props).values())
        return channelBuilder
    }

    private val defaultApiVersionsSupplier = {
        TestUtils.defaultApiVersionsResponse(listenerType = ApiMessageType.ListenerType.ZK_BROKER)
    }

    private fun createChannelBuilder(securityProtocol: SecurityProtocol, saslMechanism: String): SaslChannelBuilder {
        val loginModule = when (saslMechanism) {
            "PLAIN" -> PlainLoginModule::class.java
            "SCRAM-SHA-256" -> ScramLoginModule::class.java
            "OAUTHBEARER" -> OAuthBearerLoginModule::class.java
            "GSSAPI" -> TestGssapiLoginModule::class.java
            else -> throw IllegalArgumentException("Unsupported SASL mechanism $saslMechanism")
        }
        val jaasConfig = TestJaasConfig()
        jaasConfig.addEntry(
            name = "jaasContext",
            loginModule = loginModule.getName(),
            options = emptyMap(),
        )

        val jaasContext = JaasContext(
            name = "jaasContext",
            type = JaasContext.Type.SERVER,
            configuration = jaasConfig,
            dynamicJaasConfig = null,
        )
        val jaasContexts = mapOf(saslMechanism to jaasContext)

        return SaslChannelBuilder(
            mode = Mode.CLIENT,
            jaasContexts = jaasContexts,
            securityProtocol = securityProtocol,
            listenerName = ListenerName(saslMechanism),
            isInterBrokerListener = false,
            clientSaslMechanism = saslMechanism,
            handshakeRequestEnable = true,
            credentialCache = null,
            tokenCache = null,
            sslClientAuthOverride = null,
            time = Time.SYSTEM,
            logContext = LogContext(),
            apiVersionSupplier = defaultApiVersionsSupplier,
        )
    }

    class TestGssapiLoginModule : LoginModule {

        private lateinit var subject: Subject

        override fun initialize(
            subject: Subject,
            callbackHandler: CallbackHandler,
            sharedState: Map<String?, *>?,
            options: Map<String?, *>?,
        ) {
            this.subject = subject
        }

        @Throws(LoginException::class)
        override fun login(): Boolean {
            subject.principals.add(KafkaPrincipal("User", "kafka@kafka1.example.com"))
            return true
        }

        @Throws(LoginException::class)
        override fun commit(): Boolean = true

        @Throws(LoginException::class)
        override fun abort(): Boolean = true

        @Throws(LoginException::class)
        override fun logout(): Boolean = true
    }
}
