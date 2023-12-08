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

import org.apache.kafka.common.Configurable
import org.apache.kafka.common.config.internals.BrokerSecurityConfigs
import org.apache.kafka.common.network.ChannelBuilders.channelBuilderConfigs
import org.apache.kafka.common.network.ChannelBuilders.createPrincipalBuilder
import org.apache.kafka.common.security.TestSecurityConfig
import org.apache.kafka.common.security.auth.AuthenticationContext
import org.apache.kafka.common.security.auth.KafkaPrincipal
import org.apache.kafka.common.security.auth.KafkaPrincipalBuilder
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertNull
import kotlin.test.assertTrue

class ChannelBuildersTest {

    @Test
    fun testCreateConfigurableKafkaPrincipalBuilder() {
        val configs: MutableMap<String, Any?> = HashMap()
        configs[BrokerSecurityConfigs.PRINCIPAL_BUILDER_CLASS_CONFIG] = ConfigurableKafkaPrincipalBuilder::class.java
        val builder = createPrincipalBuilder(
            configs = configs,
            kerberosShortNamer = null,
            sslPrincipalMapper = null,
        )
        assertIs<ConfigurableKafkaPrincipalBuilder>(builder)
        assertTrue(builder.configured)
    }

    @Test
    fun testChannelBuilderConfigs() {
        val props = mapOf(
            "listener.name.listener1.gssapi.sasl.kerberos.service.name" to "testkafka",
            "listener.name.listener1.sasl.kerberos.service.name" to "testkafkaglobal",
            "plain.sasl.server.callback.handler.class" to "callback",
            "listener.name.listener1.gssapi.config1.key" to "custom.config1",
            "custom.config2.key" to "custom.config2",
        )
        var securityConfig = TestSecurityConfig(props)

        // test configs with listener prefix
        var configs = channelBuilderConfigs(
            config = securityConfig,
            listenerName = ListenerName("listener1"),
        )
        assertNull(configs["listener.name.listener1.gssapi.sasl.kerberos.service.name"])
        assertFalse(securityConfig.unused().contains("listener.name.listener1.gssapi.sasl.kerberos.service.name"))
        assertEquals(configs["gssapi.sasl.kerberos.service.name"], "testkafka")
        assertFalse(securityConfig.unused().contains("gssapi.sasl.kerberos.service.name"))
        assertEquals(configs["sasl.kerberos.service.name"], "testkafkaglobal")
        assertFalse(securityConfig.unused().contains("sasl.kerberos.service.name"))
        assertNull(configs["listener.name.listener1.sasl.kerberos.service.name"])
        assertFalse(securityConfig.unused().contains("listener.name.listener1.sasl.kerberos.service.name"))
        assertNull(configs["plain.sasl.server.callback.handler.class"])
        assertFalse(securityConfig.unused().contains("plain.sasl.server.callback.handler.class"))
        assertEquals(configs["listener.name.listener1.gssapi.config1.key"], "custom.config1")
        assertFalse(securityConfig.unused().contains("listener.name.listener1.gssapi.config1.key"))
        assertEquals(configs["custom.config2.key"], "custom.config2")
        assertFalse(securityConfig.unused().contains("custom.config2.key"))

        // test configs without listener prefix
        securityConfig = TestSecurityConfig(props)
        configs = channelBuilderConfigs(securityConfig, null)
        assertEquals(configs["listener.name.listener1.gssapi.sasl.kerberos.service.name"], "testkafka")
        assertFalse(securityConfig.unused().contains("listener.name.listener1.gssapi.sasl.kerberos.service.name"))
        assertNull(configs["gssapi.sasl.kerberos.service.name"])
        assertFalse(securityConfig.unused().contains("gssapi.sasl.kerberos.service.name"))
        assertEquals(configs["listener.name.listener1.sasl.kerberos.service.name"], "testkafkaglobal")
        assertFalse(securityConfig.unused().contains("listener.name.listener1.sasl.kerberos.service.name"))
        assertNull(configs["sasl.kerberos.service.name"])
        assertFalse(securityConfig.unused().contains("sasl.kerberos.service.name"))
        assertEquals(configs["plain.sasl.server.callback.handler.class"], "callback")
        assertFalse(securityConfig.unused().contains("plain.sasl.server.callback.handler.class"))
        assertEquals(configs["listener.name.listener1.gssapi.config1.key"], "custom.config1")
        assertFalse(securityConfig.unused().contains("listener.name.listener1.gssapi.config1.key"))
        assertEquals(configs["custom.config2.key"], "custom.config2")
        assertFalse(securityConfig.unused().contains("custom.config2.key"))
    }

    internal class ConfigurableKafkaPrincipalBuilder : KafkaPrincipalBuilder, Configurable {

        var configured = false

        override fun configure(configs: Map<String, *>) {
            configured = true
        }

        override fun build(context: AuthenticationContext): KafkaPrincipal {
            error("Should not be called during these tests.")
        }
    }
}
