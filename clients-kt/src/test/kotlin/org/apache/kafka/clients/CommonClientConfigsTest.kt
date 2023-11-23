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

package org.apache.kafka.clients

import org.apache.kafka.clients.CommonClientConfigs.metricsReporters
import org.apache.kafka.clients.CommonClientConfigs.postProcessReconnectBackoffConfigs
import org.apache.kafka.clients.CommonClientConfigs.postValidateSaslMechanismConfig
import org.apache.kafka.common.config.AbstractConfig
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigDef.NonNullValidator
import org.apache.kafka.common.config.ConfigDef.Range.Companion.atLeast
import org.apache.kafka.common.config.ConfigDef.ValidString.Companion.`in`
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.common.config.SaslConfigs
import org.apache.kafka.common.metrics.JmxReporter
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.utils.Utils.enumOptions
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertIs
import kotlin.test.assertTrue

class CommonClientConfigsTest {

    @Suppress("Deprecation")
    private class TestConfig(props: Map<String, Any?>) : AbstractConfig(CONFIG, props) {

        override fun postProcessParsedConfig(parsedValues: Map<String, Any?>): Map<String, Any?> {
            postValidateSaslMechanismConfig(this)
            return postProcessReconnectBackoffConfigs(this, parsedValues)
        }

        companion object {
            private val CONFIG: ConfigDef = ConfigDef()
                .define(
                    name = CommonClientConfigs.RECONNECT_BACKOFF_MS_CONFIG,
                    type = ConfigDef.Type.LONG,
                    defaultValue = 50L,
                    validator = atLeast(0L),
                    importance = ConfigDef.Importance.LOW,
                    documentation = "",
                )
                .define(
                    name = CommonClientConfigs.RECONNECT_BACKOFF_MAX_MS_CONFIG,
                    type = ConfigDef.Type.LONG,
                    defaultValue = 1000L,
                    validator = atLeast(0L),
                    importance = ConfigDef.Importance.LOW,
                    documentation = "",
                )
                .define(
                    name = CommonClientConfigs.SECURITY_PROTOCOL_CONFIG,
                    type = ConfigDef.Type.STRING,
                    defaultValue = CommonClientConfigs.DEFAULT_SECURITY_PROTOCOL,
                    validator = `in`(*enumOptions(SecurityProtocol::class.java)),
                    importance = ConfigDef.Importance.MEDIUM,
                    documentation = CommonClientConfigs.SECURITY_PROTOCOL_DOC,
                )
                .define(
                    name = SaslConfigs.SASL_MECHANISM,
                    type = ConfigDef.Type.STRING,
                    defaultValue = SaslConfigs.DEFAULT_SASL_MECHANISM,
                    importance = ConfigDef.Importance.MEDIUM,
                    documentation = SaslConfigs.SASL_MECHANISM_DOC,
                )
                .define(
                    name = CommonClientConfigs.METRIC_REPORTER_CLASSES_CONFIG,
                    type = ConfigDef.Type.LIST,
                    defaultValue = emptyList<Any>(),
                    validator = NonNullValidator(),
                    importance = ConfigDef.Importance.LOW,
                    documentation = CommonClientConfigs.METRIC_REPORTER_CLASSES_DOC,
                )
                .define(
                    name = CommonClientConfigs.AUTO_INCLUDE_JMX_REPORTER_CONFIG,
                    type = ConfigDef.Type.BOOLEAN,
                    defaultValue = true,
                    importance = ConfigDef.Importance.LOW,
                    documentation = CommonClientConfigs.AUTO_INCLUDE_JMX_REPORTER_DOC,
                )
        }
    }

    @Test
    fun testExponentialBackoffDefaults() {
        val defaultConf = TestConfig(emptyMap<String, Any>())
        assertEquals(
            expected = 50L,
            actual = defaultConf.getLong(CommonClientConfigs.RECONNECT_BACKOFF_MS_CONFIG)
        )
        assertEquals(
            expected = 1000L,
            actual = defaultConf.getLong(CommonClientConfigs.RECONNECT_BACKOFF_MAX_MS_CONFIG)
        )
        val bothSetConfig = TestConfig(
            props = hashMapOf(
                CommonClientConfigs.RECONNECT_BACKOFF_MS_CONFIG to "123",
                CommonClientConfigs.RECONNECT_BACKOFF_MAX_MS_CONFIG to "12345",
            ),
        )
        assertEquals(
            expected = 123L,
            actual = bothSetConfig.getLong(CommonClientConfigs.RECONNECT_BACKOFF_MS_CONFIG)
        )
        assertEquals(
            expected = 12345L,
            actual = bothSetConfig.getLong(CommonClientConfigs.RECONNECT_BACKOFF_MAX_MS_CONFIG)
        )
        val reconnectBackoffSetConf = TestConfig(
            props = hashMapOf(CommonClientConfigs.RECONNECT_BACKOFF_MS_CONFIG to "123"),
        )
        assertEquals(
            expected = 123L,
            actual = reconnectBackoffSetConf.getLong(CommonClientConfigs.RECONNECT_BACKOFF_MS_CONFIG)
        )
        assertEquals(
            expected = 123L,
            actual = reconnectBackoffSetConf.getLong(CommonClientConfigs.RECONNECT_BACKOFF_MAX_MS_CONFIG)
        )
    }

    @Test
    fun testInvalidSaslMechanism() {
        val configs = mutableMapOf<String, Any?>()
        configs[CommonClientConfigs.SECURITY_PROTOCOL_CONFIG] = SecurityProtocol.SASL_PLAINTEXT.name
        configs[SaslConfigs.SASL_MECHANISM] = null
        var ce = assertFailsWith<ConfigException> { TestConfig(configs) }
        assertTrue(ce.message!!.contains(SaslConfigs.SASL_MECHANISM))
        configs[SaslConfigs.SASL_MECHANISM] = ""
        ce = assertFailsWith<ConfigException> { TestConfig(configs) }
        assertTrue(ce.message!!.contains(SaslConfigs.SASL_MECHANISM))
    }

    @Test
    @Suppress("Deprecation")
    fun testMetricsReporters() {
        var config = TestConfig(emptyMap<String, Any>())
        var reporters = metricsReporters(
            clientId = "clientId",
            config = config,
        )
        assertEquals(expected = 1, actual = reporters.size)
        assertIs<JmxReporter>(reporters[0])

        config = TestConfig(
            props = mapOf(CommonClientConfigs.AUTO_INCLUDE_JMX_REPORTER_CONFIG to "false"),
        )
        reporters = metricsReporters("clientId", config)
        assertTrue(reporters.isEmpty())

        config = TestConfig(
            props = mapOf(
                CommonClientConfigs.METRIC_REPORTER_CLASSES_CONFIG to JmxReporter::class.java.name,
            )
        )
        reporters = metricsReporters("clientId", config)
        assertEquals(expected = 1, actual = reporters.size)
        assertIs<JmxReporter>(reporters[0])

        config = TestConfig(
            props = mapOf(
                CommonClientConfigs.METRIC_REPORTER_CLASSES_CONFIG to
                        "${JmxReporter::class.java.name},${MyJmxReporter::class.java.name}",
            )
        )
        reporters = metricsReporters("clientId", config)
        assertEquals(expected = 2, actual = reporters.size)
    }

    class MyJmxReporter : JmxReporter()
}
