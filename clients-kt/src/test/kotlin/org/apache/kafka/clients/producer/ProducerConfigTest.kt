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

package org.apache.kafka.clients.producer

import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.kafka.common.serialization.Serializer
import org.apache.kafka.common.serialization.StringSerializer
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class ProducerConfigTest {
    
    private val keySerializer: Serializer<ByteArray?> = ByteArraySerializer()
    
    private val valueSerializer: Serializer<String?> = StringSerializer()
    
    private val keySerializerClass: Any = keySerializer.javaClass
    
    private val valueSerializerClass: Any = valueSerializer.javaClass
    
    @Test
    fun testAppendSerializerToConfig() {
        val configs = mutableMapOf(
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to keySerializerClass,
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to valueSerializerClass,
        )
        var newConfigs = ProducerConfig.appendSerializerToConfig(configs, null, null)

        assertEquals(newConfigs[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG], keySerializerClass)
        assertEquals(newConfigs[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG], valueSerializerClass)

        configs.clear()
        configs[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = valueSerializerClass
        newConfigs = ProducerConfig.appendSerializerToConfig(configs, keySerializer, null)

        assertEquals(newConfigs[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG], keySerializerClass)
        assertEquals(newConfigs[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG], valueSerializerClass)

        configs.clear()
        configs[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = keySerializerClass
        newConfigs = ProducerConfig.appendSerializerToConfig(configs, null, valueSerializer)

        assertEquals(newConfigs[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG], keySerializerClass)
        assertEquals(newConfigs[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG], valueSerializerClass)

        configs.clear()
        newConfigs = ProducerConfig.appendSerializerToConfig(configs, keySerializer, valueSerializer)

        assertEquals(newConfigs[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG], keySerializerClass)
        assertEquals(newConfigs[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG], valueSerializerClass)
    }

    @Test
    fun testAppendSerializerToConfigWithException() {
        val configs = mutableMapOf(
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to null,
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to valueSerializerClass,
        )

        assertFailsWith<ConfigException> {
            ProducerConfig.appendSerializerToConfig(
                configs = configs,
                keySerializer = null,
                valueSerializer = valueSerializer,
            )
        }
        configs.clear()
        configs[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = keySerializerClass
        configs[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = null

        assertFailsWith<ConfigException> {
            ProducerConfig.appendSerializerToConfig(
                configs = configs,
                keySerializer = keySerializer,
                valueSerializer = null,
            )
        }
    }

    @Test
    fun testInvalidCompressionType() {
        val configs = mapOf(
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to keySerializerClass,
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to valueSerializerClass,
            ProducerConfig.COMPRESSION_TYPE_CONFIG to "abc",
        )

        assertFailsWith<ConfigException> { ProducerConfig(configs) }
    }

    @Test
    fun testInvalidSecurityProtocol() {
        val configs = mapOf(
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to keySerializerClass,
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to valueSerializerClass,
            CommonClientConfigs.SECURITY_PROTOCOL_CONFIG to "abc",
        )

        val ce = assertFailsWith<ConfigException> { ProducerConfig(configs) }
        assertTrue(ce.message!!.contains(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG))
    }

    @Test
    fun testCaseInsensitiveSecurityProtocol() {
        val saslSslLowerCase = SecurityProtocol.SASL_SSL.name.lowercase()
        val configs = mapOf(
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to keySerializerClass,
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to valueSerializerClass,
            CommonClientConfigs.SECURITY_PROTOCOL_CONFIG to saslSslLowerCase,
        )
        val producerConfig = ProducerConfig(configs)
        assertEquals(
            expected = saslSslLowerCase,
            actual = producerConfig.originals()[CommonClientConfigs.SECURITY_PROTOCOL_CONFIG]
        )
    }
}
