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

package org.apache.kafka.clients.consumer

import java.util.Properties
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.common.errors.InvalidConfigurationException
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.StringDeserializer
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import kotlin.test.assertContains
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse

class ConsumerConfigTest {
    
    private val keyDeserializer: Deserializer<ByteArray?> = ByteArrayDeserializer()
    
    private val valueDeserializer: Deserializer<String?> = StringDeserializer()
    
    private val keyDeserializerClassName = keyDeserializer::class.java.name
    
    private val valueDeserializerClassName = valueDeserializer::class.java.name
    
    private val keyDeserializerClass: Any = keyDeserializer::class.java
    
    private val valueDeserializerClass: Any = valueDeserializer::class.java
    
    private val properties = Properties()
    
    @BeforeEach
    fun setUp() {
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializerClassName)
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializerClassName)
    }

    @Test
    fun testOverrideClientId() {
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "test-group")
        val config = ConsumerConfig(properties)
        assertFalse(config.getString(ConsumerConfig.CLIENT_ID_CONFIG)!!.isEmpty())
    }

    @Test
    fun testOverrideEnableAutoCommit() {
        var config = ConsumerConfig(properties)
        val overrideEnableAutoCommit = config.maybeOverrideEnableAutoCommit()
        assertFalse(overrideEnableAutoCommit)
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
        config = ConsumerConfig(properties)
        assertFailsWith<InvalidConfigurationException>(
            message = "Should have thrown an exception",
        ) { config.maybeOverrideEnableAutoCommit() }
    }

    @Test
    fun testAppendDeserializerToConfig() {
        val configs = mutableMapOf<String, Any?>()
        configs[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = keyDeserializerClass
        configs[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = valueDeserializerClass
        var newConfigs = ConsumerConfig.appendDeserializerToConfig(
            configs = configs,
            keyDeserializer = null,
            valueDeserializer = null,
        )
        assertEquals(newConfigs[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG], keyDeserializerClass)
        assertEquals(newConfigs[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG], valueDeserializerClass)
        configs.clear()

        configs[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = valueDeserializerClass
        newConfigs = ConsumerConfig.appendDeserializerToConfig(
            configs = configs,
            keyDeserializer = keyDeserializer,
            valueDeserializer = null,
        )
        assertEquals(newConfigs[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG], keyDeserializerClass)
        assertEquals(newConfigs[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG], valueDeserializerClass)
        configs.clear()

        configs[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = keyDeserializerClass
        newConfigs = ConsumerConfig.appendDeserializerToConfig(
            configs = configs,
            keyDeserializer = null,
            valueDeserializer = valueDeserializer,
        )
        assertEquals(newConfigs[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG], keyDeserializerClass)
        assertEquals(newConfigs[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG], valueDeserializerClass)
        configs.clear()

        newConfigs = ConsumerConfig.appendDeserializerToConfig(
            configs = configs,
            keyDeserializer = keyDeserializer,
            valueDeserializer = valueDeserializer,
        )
        assertEquals(newConfigs[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG], keyDeserializerClass)
        assertEquals(newConfigs[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG], valueDeserializerClass)
    }

    @Test
    fun testAppendDeserializerToConfigWithException() {
        val configs = mutableMapOf(
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to null,
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to valueDeserializerClass,
        )
        assertFailsWith<ConfigException> {
            ConsumerConfig.appendDeserializerToConfig(
                configs = configs,
                keyDeserializer = null,
                valueDeserializer = valueDeserializer
            )
        }
        configs.clear()

        configs[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = keyDeserializerClass
        configs[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = null
        assertFailsWith<ConfigException> {
            ConsumerConfig.appendDeserializerToConfig(
                configs = configs,
                keyDeserializer = keyDeserializer,
                valueDeserializer = null,
            )
        }
    }

    @Test
    fun ensureDefaultThrowOnUnsupportedStableFlagToFalse() {
        assertFalse(ConsumerConfig(properties).getBoolean(ConsumerConfig.THROW_ON_FETCH_STABLE_OFFSET_UNSUPPORTED)!!)
    }

    @Test
    fun testDefaultPartitionAssignor() {
        assertEquals(
            expected = listOf(RangeAssignor::class.java.name, CooperativeStickyAssignor::class.java.name),
            actual = ConsumerConfig(properties).getList(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG)
        )
    }

    @Test
    fun testInvalidGroupInstanceId() {
        val configs: MutableMap<String, Any> = HashMap()
        configs[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = keyDeserializerClass
        configs[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = valueDeserializerClass
        configs[ConsumerConfig.GROUP_INSTANCE_ID_CONFIG] = ""
        val ce = assertFailsWith<ConfigException> { ConsumerConfig(configs) }
        assertContains(ce.message!!, ConsumerConfig.GROUP_INSTANCE_ID_CONFIG)
    }

    @Test
    fun testInvalidSecurityProtocol() {
        val configs: MutableMap<String, Any> = HashMap()
        configs[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = keyDeserializerClass
        configs[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = valueDeserializerClass
        configs[CommonClientConfigs.SECURITY_PROTOCOL_CONFIG] = "abc"
        val ce = assertFailsWith<ConfigException> { ConsumerConfig(configs) }
        assertContains(ce.message!!, CommonClientConfigs.SECURITY_PROTOCOL_CONFIG)
    }

    @Test
    fun testCaseInsensitiveSecurityProtocol() {
        val saslSslLowerCase = SecurityProtocol.SASL_SSL.name.lowercase()
        val configs = mapOf(
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to keyDeserializerClass,
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to valueDeserializerClass,
            CommonClientConfigs.SECURITY_PROTOCOL_CONFIG to saslSslLowerCase,
        )
        val consumerConfig = ConsumerConfig(configs)
        assertEquals(
            expected = saslSslLowerCase,
            actual = consumerConfig.originals()[CommonClientConfigs.SECURITY_PROTOCOL_CONFIG],
        )
    }
}
