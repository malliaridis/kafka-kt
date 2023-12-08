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

import java.time.Duration
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.admin.Admin
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.config.SaslConfigs
import org.apache.kafka.common.config.internals.BrokerSecurityConfigs
import org.apache.kafka.common.errors.SaslAuthenticationException
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.network.NetworkTestUtils.createEchoServer
import org.apache.kafka.common.network.NioEchoServer
import org.apache.kafka.common.security.TestSecurityConfig
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.common.utils.MockTime
import org.apache.kafka.test.TestUtils.assertFutureThrows
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import kotlin.test.assertFailsWith

class ClientAuthenticationFailureTest {
    
    private lateinit var server: NioEchoServer
    
    private lateinit var saslServerConfigs: Map<String, Any>
    
    private lateinit var saslClientConfigs: Map<String, Any>
    
    private val topic = "test"
    
    private lateinit var testJaasConfig: TestJaasConfig
    
    @BeforeEach
    @Throws(Exception::class)
    fun setup() {
        LoginManager.closeAll()
        val securityProtocol = SecurityProtocol.SASL_PLAINTEXT
        saslServerConfigs = mapOf(BrokerSecurityConfigs.SASL_ENABLED_MECHANISMS_CONFIG to listOf("PLAIN"))
        saslClientConfigs = mapOf(
            CommonClientConfigs.SECURITY_PROTOCOL_CONFIG to "SASL_PLAINTEXT",
            SaslConfigs.SASL_MECHANISM to "PLAIN",
        )
        testJaasConfig = TestJaasConfig.createConfiguration("PLAIN", mutableListOf("PLAIN"))
        testJaasConfig.setClientOptions("PLAIN", TestJaasConfig.USERNAME, "anotherpassword")
        server = createEchoServer(securityProtocol = securityProtocol)
    }

    @AfterEach
    @Throws(Exception::class)
    fun teardown() {
        if (::server.isInitialized) server.close()
    }

    @Test
    fun testConsumerWithInvalidCredentials() {
        val props = saslClientConfigs + mapOf(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to "localhost:${server.port}",
            ConsumerConfig.GROUP_ID_CONFIG to "",
        )
        val deserializer = StringDeserializer()
        KafkaConsumer(
            configs = props,
            keyDeserializer = deserializer,
            valueDeserializer = deserializer,
        ).use { consumer ->
            assertFailsWith<SaslAuthenticationException> {
                consumer.subscribe(setOf(topic))
                consumer.poll(Duration.ofSeconds(10))
            }
        }
    }

    @Test
    fun testProducerWithInvalidCredentials() {
        val props = saslClientConfigs + mapOf(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to "localhost:${server.port}",
        )
        val serializer = StringSerializer()
        KafkaProducer(
            configs = props,
            keySerializer = serializer,
            valueSerializer = serializer,
        ).use { producer ->
            val record = ProducerRecord<String, String>(topic = topic, value = "message")
            val future = producer.send(record)
            assertFutureThrows(future, SaslAuthenticationException::class.java)
        }
    }

    @Test
    fun testAdminClientWithInvalidCredentials() {
        val props = saslClientConfigs + mapOf(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to "localhost:${server.port}",
        )
        Admin.create(props).use { client ->
            val future = client.describeTopics(setOf("test")).allTopicNames()
            assertFutureThrows(future, SaslAuthenticationException::class.java)
        }
    }

    @Test
    fun testTransactionalProducerWithInvalidCredentials() {
        val props = saslClientConfigs + mapOf(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to "localhost:" + server.port,
            ProducerConfig.TRANSACTIONAL_ID_CONFIG to "txclient-1",
            ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG to "true",
        )
        val serializer = StringSerializer()
        KafkaProducer(
            configs = props,
            keySerializer = serializer,
            valueSerializer = serializer,
        ).use { producer ->
            assertFailsWith<SaslAuthenticationException> { producer.initTransactions() }
        }
    }

    @Throws(Exception::class)
    private fun createEchoServer(
        securityProtocol: SecurityProtocol,
        listenerName: ListenerName = ListenerName.forSecurityProtocol(securityProtocol),
    ): NioEchoServer = createEchoServer(
        listenerName = listenerName,
        securityProtocol = securityProtocol,
        serverConfig = TestSecurityConfig(saslServerConfigs),
        credentialCache = CredentialCache(),
        time = time,
    )

    companion object {
        private val time = MockTime(50)
    }
}
