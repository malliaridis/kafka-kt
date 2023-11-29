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
import org.apache.kafka.clients.KafkaClient
import org.apache.kafka.clients.MockClient
import org.apache.kafka.clients.NodeApiVersions
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.clients.producer.internals.FutureRecordMetadata
import org.apache.kafka.clients.producer.internals.ProduceRequestResult
import org.apache.kafka.clients.producer.internals.ProducerInterceptors
import org.apache.kafka.clients.producer.internals.ProducerMetadata
import org.apache.kafka.clients.producer.internals.RecordAccumulator
import org.apache.kafka.clients.producer.internals.RecordAccumulator.RecordAppendResult
import org.apache.kafka.clients.producer.internals.Sender
import org.apache.kafka.clients.producer.internals.TransactionManager
import org.apache.kafka.common.Cluster
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.Node
import org.apache.kafka.common.PartitionInfo
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.common.config.SslConfigs
import org.apache.kafka.common.errors.InterruptException
import org.apache.kafka.common.errors.InvalidTopicException
import org.apache.kafka.common.errors.RecordTooLargeException
import org.apache.kafka.common.header.internals.RecordHeader
import org.apache.kafka.common.internals.ClusterResourceListeners
import org.apache.kafka.common.message.AddOffsetsToTxnResponseData
import org.apache.kafka.common.message.EndTxnResponseData
import org.apache.kafka.common.message.InitProducerIdResponseData
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.metrics.Sensor
import org.apache.kafka.common.metrics.stats.Avg
import org.apache.kafka.common.network.Selectable
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.record.Record
import org.apache.kafka.common.record.RecordBatch
import org.apache.kafka.common.requests.AddOffsetsToTxnResponse
import org.apache.kafka.common.requests.EndTxnResponse
import org.apache.kafka.common.requests.FindCoordinatorRequest
import org.apache.kafka.common.requests.FindCoordinatorResponse
import org.apache.kafka.common.requests.InitProducerIdResponse
import org.apache.kafka.common.requests.JoinGroupRequest
import org.apache.kafka.common.requests.MetadataResponse
import org.apache.kafka.common.requests.ProduceResponse
import org.apache.kafka.common.requests.RequestTestUtils.metadataResponse
import org.apache.kafka.common.requests.RequestTestUtils.metadataUpdateWith
import org.apache.kafka.common.requests.TxnOffsetCommitRequest
import org.apache.kafka.common.requests.TxnOffsetCommitResponse
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.kafka.common.serialization.Serializer
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.common.utils.KafkaThread
import org.apache.kafka.common.utils.LogContext
import org.apache.kafka.common.utils.MockTime
import org.apache.kafka.common.utils.Time
import org.apache.kafka.test.MockMetricsReporter
import org.apache.kafka.test.MockPartitioner
import org.apache.kafka.test.MockProducerInterceptor
import org.apache.kafka.test.MockSerializer
import org.apache.kafka.test.TestUtils
import org.apache.kafka.test.TestUtils.assertFutureError
import org.apache.kafka.test.TestUtils.assertFutureThrows
import org.apache.kafka.test.TestUtils.singletonCluster
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInfo
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import org.mockito.Mockito.mock
import org.mockito.Mockito.never
import org.mockito.Mockito.times
import org.mockito.Mockito.verify
import org.mockito.Mockito.`when`
import java.lang.management.ManagementFactory
import java.time.Duration
import java.util.Properties
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Exchanger
import java.util.concurrent.ExecutionException
import java.util.concurrent.Executors
import java.util.concurrent.Future
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference
import javax.management.ObjectName
import org.mockito.ArgumentMatchers.eq
import org.mockito.ArgumentMatchers.notNull
import org.mockito.kotlin.any
import kotlin.collections.ArrayList
import kotlin.collections.HashMap
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertNotEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlin.test.fail

class KafkaProducerTest {

    private val topic = "topic"

    private val nodes: Collection<Node> = listOf(NODE)

    private val emptyCluster = Cluster(
        clusterId = null,
        nodes = nodes,
        partitions = emptySet(),
        unauthorizedTopics = emptySet(),
        invalidTopics = emptySet(),
    )

    private val onePartitionCluster = Cluster(
        clusterId = "dummy",
        nodes = nodes,
        partitions = listOf(
            PartitionInfo(
                topic = topic,
                partition = 0,
                leader = null,
                replicas = emptyList(),
                inSyncReplicas = emptyList(),
            ),
        ),
        unauthorizedTopics = emptySet(),
        invalidTopics = emptySet()
    )
    private val threePartitionCluster = Cluster(
        clusterId = "dummy",
        nodes = nodes,
        partitions = listOf(
            PartitionInfo(
                topic = topic,
                partition = 0,
                leader = null,
                replicas = emptyList(),
                inSyncReplicas = emptyList()
            ),
            PartitionInfo(
                topic = topic,
                partition = 1,
                leader = null,
                replicas = emptyList(),
                inSyncReplicas = emptyList(),
            ),
            PartitionInfo(
                topic = topic,
                partition = 2,
                leader = null,
                replicas = emptyList(),
                inSyncReplicas = emptyList(),
            )
        ),
        unauthorizedTopics = emptySet(),
        invalidTopics = emptySet(),
    )

    private lateinit var testInfo: TestInfo

    @BeforeEach
    fun setup(testInfo: TestInfo) {
        this.testInfo = testInfo
    }

    @Test
    fun testOverwriteAcksAndRetriesForIdempotentProducers() {
        val props = Properties()
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999")
        props.setProperty(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "transactionalId")
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.getName())
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.getName())
        val config = ProducerConfig(props)
        assertTrue(config.getBoolean(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG)!!)
        assertTrue(
            listOf("-1", "all").any { each ->
                each.equals(config.getString(ProducerConfig.ACKS_CONFIG), ignoreCase = true)
            }
        )
        assertEquals(config.getInt(ProducerConfig.RETRIES_CONFIG) as Int, Int.MAX_VALUE)
        assertTrue(
            config.getString(ProducerConfig.CLIENT_ID_CONFIG).equals(
                other = "producer-${config.getString(ProducerConfig.TRANSACTIONAL_ID_CONFIG)}",
                ignoreCase = true,
            )
        )
    }

    @Test
    fun testAcksAndIdempotenceForIdempotentProducers() {
        val baseProps = Properties().apply {
            setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999")
            setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.getName())
            setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.getName())
        }
        val validProps = Properties().apply {
            putAll(baseProps)
            setProperty(ProducerConfig.ACKS_CONFIG, "0")
            setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "false")
        }

        var config = ProducerConfig(validProps)

        assertFalse(
            actual = config.getBoolean(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG)!!,
            message = "idempotence should be overwritten",
        )
        assertEquals(
            expected = "0",
            actual = config.getString(ProducerConfig.ACKS_CONFIG),
            message = "acks should be overwritten",
        )

        val validProps2 = Properties().apply {
            putAll(baseProps)
            setProperty(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "transactionalId")
        }
        config = ProducerConfig(validProps2)

        assertTrue(
            actual = config.getBoolean(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG)!!,
            message = "idempotence should be set with the default value",
        )
        assertEquals(
            expected = "-1",
            actual = config.getString(ProducerConfig.ACKS_CONFIG),
            message = "acks should be set with the default value",
        )
        val validProps3 = Properties().apply {
            putAll(baseProps)
            setProperty(ProducerConfig.ACKS_CONFIG, "all")
            setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "false")
        }
        config = ProducerConfig(validProps3)

        assertFalse(
            actual = config.getBoolean(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG)!!,
            message = "idempotence should be overwritten"
        )
        assertEquals(
            expected = "-1",
            actual = config.getString(ProducerConfig.ACKS_CONFIG),
            message = "acks should be overwritten",
        )

        val validProps4 = Properties().apply {
            putAll(baseProps)
            setProperty(ProducerConfig.ACKS_CONFIG, "0")
        }
        config = ProducerConfig(validProps4)

        assertFalse(
            actual = config.getBoolean(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG)!!,
            message = "idempotence should be disabled when acks not set to all and `enable.idempotence` config is unset."
        )
        assertEquals(
            expected = "0",
            actual = config.getString(ProducerConfig.ACKS_CONFIG),
            message = "acks should be set with overridden value",
        )

        val validProps5 = Properties().apply {
            putAll(baseProps)
            setProperty(ProducerConfig.ACKS_CONFIG, "1")
        }
        config = ProducerConfig(validProps5)

        assertFalse(
            actual = config.getBoolean(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG)!!,
            message = "idempotence should be disabled when acks not set to all and `enable.idempotence` config is unset."
        )
        assertEquals(
            expected = "1",
            actual = config.getString(ProducerConfig.ACKS_CONFIG),
            message = "acks should be set with overridden value",
        )

        val invalidProps = Properties().apply {
            putAll(baseProps)
            setProperty(ProducerConfig.ACKS_CONFIG, "0")
            setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "false")
            setProperty(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "transactionalId")
        }

        assertFailsWith<ConfigException>(
            "Cannot set a transactional.id without also enabling idempotence"
        ) { ProducerConfig(invalidProps) }

        val invalidProps2 = Properties().apply {
            putAll(baseProps)
            setProperty(ProducerConfig.ACKS_CONFIG, "1")
            // explicitly enabling idempotence should still throw exception
            setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")
        }

        assertFailsWith<ConfigException>(
            "Must set acks to all in order to use the idempotent producer"
        ) { ProducerConfig(invalidProps2) }

        val invalidProps3 = Properties().apply {
            putAll(baseProps)
            setProperty(ProducerConfig.ACKS_CONFIG, "0")
            setProperty(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "transactionalId")
        }

        assertFailsWith<ConfigException>(
            "Must set acks to all when using the transactional producer."
        ) { ProducerConfig(invalidProps3) }
    }

    @Test
    fun testRetriesAndIdempotenceForIdempotentProducers() {
        val baseProps = Properties().apply {
            setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999")
            setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.getName())
            setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.getName())
        }
        val validProps = Properties().apply {
            putAll(baseProps)
            setProperty(ProducerConfig.RETRIES_CONFIG, "0")
            setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "false")
        }
        var config = ProducerConfig(validProps)

        assertFalse(
            actual = config.getBoolean(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG)!!,
            message = "idempotence should be overwritten",
        )
        assertEquals(
            expected = 0,
            actual = config.getInt(ProducerConfig.RETRIES_CONFIG),
            message = "retries should be overwritten",
        )
        val validProps2 = Properties().apply {
            putAll(baseProps)
            setProperty(ProducerConfig.RETRIES_CONFIG, "0")
        }
        config = ProducerConfig(validProps2)

        assertFalse(
            actual = config.getBoolean(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG)!!,
            message = "idempotence should be disabled when retries set to 0 and `enable.idempotence` config is unset.",
        )
        assertEquals(
            expected = 0,
            actual = config.getInt(ProducerConfig.RETRIES_CONFIG),
            message = "retries should be set with overridden value",
        )
        val invalidProps = Properties().apply {
            putAll(baseProps)
            setProperty(ProducerConfig.RETRIES_CONFIG, "0")
            setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "false")
            setProperty(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "transactionalId")
        }

        assertFailsWith<ConfigException>(
            "Cannot set a transactional.id without also enabling idempotence"
        ) { ProducerConfig(invalidProps) }

        val invalidProps2 = Properties().apply {
            putAll(baseProps)
            setProperty(ProducerConfig.RETRIES_CONFIG, "0")
            // explicitly enabling idempotence should still throw exception
            setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")
        }

        assertFailsWith<ConfigException>(
            "Must set retries to non-zero when using the idempotent producer.",
        ) { ProducerConfig(invalidProps2) }

        val invalidProps3 = Properties().apply {
            putAll(baseProps)
            setProperty(ProducerConfig.RETRIES_CONFIG, "0")
            setProperty(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "transactionalId")
        }

        assertFailsWith<ConfigException>(
            "Must set retries to non-zero when using the transactional producer.",
        ) { ProducerConfig(invalidProps3) }
    }

    @Test
    fun testInflightRequestsAndIdempotenceForIdempotentProducers() {
        val baseProps = Properties().apply {
            setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999")
            setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.getName())
            setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.getName())
        }

        val validProps = Properties().apply {
            putAll(baseProps)
            setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "6")
            setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "false")
        }
        var config = ProducerConfig(validProps)
        assertFalse(
            actual = config.getBoolean(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG)!!,
            message = "idempotence should be overwritten",
        )
        assertEquals(
            expected = 6,
            actual = config.getInt(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION),
            message = "max.in.flight.requests.per.connection should be overwritten",
        )

        val validProps2 = Properties().apply {
            putAll(baseProps)
            setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "6")
        }
        config = ProducerConfig(validProps2)

        assertFalse(
            actual = config.getBoolean(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG)!!,
            message = "idempotence should be disabled when `max.in.flight.requests.per.connection` is " +
                    "greater than 5 and `enable.idempotence` config is unset."
        )
        assertEquals(
            expected = 6,
            actual = config.getInt(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION),
            message = "`max.in.flight.requests.per.connection` should be set with overridden value"
        )

        val invalidProps = Properties().apply {
            putAll(baseProps)
            setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5")
            setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "false")
            setProperty(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "transactionalId")
        }
        assertFailsWith<ConfigException>(
            "Cannot set a transactional.id without also enabling idempotence"
        ) { ProducerConfig(invalidProps) }

        val invalidProps2 = Properties().apply {
            putAll(baseProps)
            setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "6")
            // explicitly enabling idempotence should still throw exception
            setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")
        }

        assertFailsWith<ConfigException>(
            "Must set max.in.flight.requests.per.connection to at most 5 when using the idempotent producer.",
        ) { ProducerConfig(invalidProps2) }

        val invalidProps3 = Properties().apply {
            putAll(baseProps)
            setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "6")
            setProperty(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "transactionalId")
        }
        assertFailsWith<ConfigException>(
            "Must set retries to non-zero when using the idempotent producer.",
        ) { ProducerConfig(invalidProps3) }
    }

    @Test
    fun testMetricsReporterAutoGeneratedClientId() {
        val props = Properties()
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999")
        props.setProperty(ProducerConfig.METRIC_REPORTER_CLASSES_CONFIG, MockMetricsReporter::class.java.getName())
        val producer = KafkaProducer(
            properties = props,
            keySerializer = StringSerializer(),
            valueSerializer = StringSerializer(),
        )
        val mockMetricsReporter = producer.metrics.reporters[0] as MockMetricsReporter

        assertEquals(producer.clientId, mockMetricsReporter.clientId)
        assertEquals(2, producer.metrics.reporters.size)

        producer.close()
    }

    @Test
    @Suppress("Deprecation")
    fun testDisableJmxReporter() {
        val props = Properties()
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999")
        props.setProperty(ProducerConfig.AUTO_INCLUDE_JMX_REPORTER_CONFIG, "false")
        val producer = KafkaProducer(props, StringSerializer(), StringSerializer())

        assertTrue(producer.metrics.reporters.isEmpty())

        producer.close()
    }

    @Test
    fun testExplicitlyEnableJmxReporter() {
        val props = Properties()
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999")
        props.setProperty(ProducerConfig.METRIC_REPORTER_CLASSES_CONFIG, "org.apache.kafka.common.metrics.JmxReporter")
        val producer = KafkaProducer(props, StringSerializer(), StringSerializer())

        assertEquals(1, producer.metrics.reporters.size)

        producer.close()
    }

    @Test
    fun testConstructorWithSerializers() {
        val producerProps = Properties()
        producerProps[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = "localhost:9000"
        KafkaProducer(producerProps, ByteArraySerializer(), ByteArraySerializer()).close()
    }

    @Test
    fun testNoSerializerProvided() {
        val producerProps = Properties()
        producerProps[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = "localhost:9000"
        assertFailsWith<ConfigException> { KafkaProducer<Any?, Any?>(producerProps) }
    }

    @Test
    fun testConstructorFailureCloseResource() {
        val props = Properties()
        props.setProperty(ProducerConfig.CLIENT_ID_CONFIG, "testConstructorClose")
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "some.invalid.hostname.foo.bar.local:9999")
        props.setProperty(ProducerConfig.METRIC_REPORTER_CLASSES_CONFIG, MockMetricsReporter::class.java.getName())
        val oldInitCount = MockMetricsReporter.INIT_COUNT.get()
        val oldCloseCount = MockMetricsReporter.CLOSE_COUNT.get()

        try {
            KafkaProducer(props, ByteArraySerializer(), ByteArraySerializer()).use {
                fail("should have caught an exception and returned")
            }
        } catch (e: KafkaException) {
            assertEquals(oldInitCount + 1, MockMetricsReporter.INIT_COUNT.get())
            assertEquals(oldCloseCount + 1, MockMetricsReporter.CLOSE_COUNT.get())
            assertEquals("Failed to construct kafka producer", e.message)
        }
    }

    @Test
    fun testConstructorWithNotStringKey() {
        val props = Properties()
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999")
        props[1] = "not string key"
        try {
            KafkaProducer(props, StringSerializer(), StringSerializer()).use {
                fail("Constructor should throw exception")
            }
        } catch (e: ConfigException) {
            assertTrue(e.message!!.contains("not string key"), "Unexpected exception message: ${e.message}")
        }
    }

    @Test
    fun testSerializerClose() {
        val configs: MutableMap<String, Any> = HashMap()
        configs[ProducerConfig.CLIENT_ID_CONFIG] = "testConstructorClose"
        configs[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = "localhost:9999"
        configs[ProducerConfig.METRIC_REPORTER_CLASSES_CONFIG] = MockMetricsReporter::class.java.getName()
        configs[CommonClientConfigs.SECURITY_PROTOCOL_CONFIG] = CommonClientConfigs.DEFAULT_SECURITY_PROTOCOL

        val oldInitCount = MockSerializer.INIT_COUNT.get()
        val oldCloseCount = MockSerializer.CLOSE_COUNT.get()
        val producer = KafkaProducer(configs, MockSerializer(), MockSerializer())

        assertEquals(oldInitCount + 2, MockSerializer.INIT_COUNT.get())
        assertEquals(oldCloseCount, MockSerializer.CLOSE_COUNT.get())

        producer.close()

        assertEquals(oldInitCount + 2, MockSerializer.INIT_COUNT.get())
        assertEquals(oldCloseCount + 2, MockSerializer.CLOSE_COUNT.get())
    }

    @Test
    fun testInterceptorConstructClose() {
        try {
            val props = Properties()
            // test with client ID assigned by KafkaProducer
            props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999")
            props.setProperty(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, MockProducerInterceptor::class.java.getName())
            props.setProperty(MockProducerInterceptor.APPEND_STRING_PROP, "something")

            val producer = KafkaProducer(
                properties = props,
                keySerializer = StringSerializer(),
                valueSerializer = StringSerializer(),
            )

            assertEquals(1, MockProducerInterceptor.INIT_COUNT.get())
            assertEquals(0, MockProducerInterceptor.CLOSE_COUNT.get())

            // Cluster metadata will only be updated on calling onSend.
            assertNull(MockProducerInterceptor.CLUSTER_META.get())

            producer.close()

            assertEquals(1, MockProducerInterceptor.INIT_COUNT.get())
            assertEquals(1, MockProducerInterceptor.CLOSE_COUNT.get())
        } finally {
            // cleanup since we are using mutable static variables in MockProducerInterceptor
            MockProducerInterceptor.resetCounters()
        }
    }

    @Test
    fun testInterceptorConstructorConfigurationWithExceptionShouldCloseRemainingInstances() {
        val targetInterceptor = 3
        try {
            val props = Properties()
            props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999")
            props.setProperty(
                ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, MockProducerInterceptor::class.java.getName() + ", "
                        + MockProducerInterceptor::class.java.getName() + ", "
                        + MockProducerInterceptor::class.java.getName()
            )
            props.setProperty(MockProducerInterceptor.APPEND_STRING_PROP, "something")
            MockProducerInterceptor.setThrowOnConfigExceptionThreshold(targetInterceptor)

            assertFailsWith<KafkaException> {
                KafkaProducer(
                    properties = props,
                    keySerializer = StringSerializer(),
                    valueSerializer = StringSerializer(),
                )
            }
            assertEquals(3, MockProducerInterceptor.CONFIG_COUNT.get())
            assertEquals(3, MockProducerInterceptor.CLOSE_COUNT.get())
        } finally {
            MockProducerInterceptor.resetCounters()
        }
    }

    @Test
    fun testPartitionerClose() {
        try {
            val props = Properties()
            props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999")
            MockPartitioner.resetCounters()
            props.setProperty(ProducerConfig.PARTITIONER_CLASS_CONFIG, MockPartitioner::class.java.getName())
            val producer = KafkaProducer(
                properties = props,
                keySerializer = StringSerializer(),
                valueSerializer = StringSerializer(),
            )

            assertEquals(1, MockPartitioner.INIT_COUNT.get())
            assertEquals(0, MockPartitioner.CLOSE_COUNT.get())

            producer.close()

            assertEquals(1, MockPartitioner.INIT_COUNT.get())
            assertEquals(1, MockPartitioner.CLOSE_COUNT.get())
        } finally {
            // cleanup since we are using mutable static variables in MockPartitioner
            MockPartitioner.resetCounters()
        }
    }

    @Test
    @Throws(Exception::class)
    fun shouldCloseProperlyAndThrowIfInterrupted() {
        val configs: MutableMap<String, Any?> = HashMap()
        configs[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = "localhost:9999"
        configs[ProducerConfig.PARTITIONER_CLASS_CONFIG] = MockPartitioner::class.java.getName()
        configs[ProducerConfig.BATCH_SIZE_CONFIG] = "1"
        val time = MockTime()
        val initialUpdateResponse = metadataUpdateWith(
            numNodes = 1,
            topicPartitionCounts = mapOf("topic" to 1),
        )
        val metadata = newMetadata(0, Long.MAX_VALUE)
        val client = MockClient(time, metadata)
        client.updateMetadata(initialUpdateResponse)
        val producer = kafkaProducer(
            configs = configs,
            keySerializer = StringSerializer(),
            valueSerializer = StringSerializer(),
            metadata = metadata,
            kafkaClient = client,
            interceptors = null,
            time = time,
        )
        val executor = Executors.newSingleThreadExecutor()
        val closeException = AtomicReference<Exception>()
        try {
            val future = executor.submit {
                producer.send(ProducerRecord(topic = "topic", key = "key", value = "value"))
                try {
                    producer.close()
                    fail("Close should block and throw.")
                } catch (e: Exception) {
                    closeException.set(e)
                }
            }

            // Close producer should not complete until send succeeds
            try {
                future[100, TimeUnit.MILLISECONDS]
                fail("Close completed without waiting for send")
            } catch (_: TimeoutException) { }

            // Ensure send has started
            client.waitForRequests(1, 1000)
            assertTrue(future.cancel(true), "Close terminated prematurely")
            TestUtils.waitForCondition(
                testCondition = { closeException.get() != null },
                conditionDetails = "InterruptException did not occur within timeout.",
            )
            assertIs<InterruptException>(
                value = closeException.get(),
                message = "Expected exception not thrown $closeException",
            )
        } finally {
            executor.shutdownNow()
        }
    }

    @Test
    fun testOsDefaultSocketBufferSizes() {
        val config = mapOf(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to "localhost:9999",
            ProducerConfig.SEND_BUFFER_CONFIG to Selectable.USE_DEFAULT_BUFFER_SIZE,
            ProducerConfig.RECEIVE_BUFFER_CONFIG to Selectable.USE_DEFAULT_BUFFER_SIZE,
        )
        KafkaProducer(config, ByteArraySerializer(), ByteArraySerializer()).close()
    }

    @Test
    fun testInvalidSocketSendBufferSize() {
        val config = mapOf(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to "localhost:9999",
            ProducerConfig.SEND_BUFFER_CONFIG to -2,
        )

        assertFailsWith<KafkaException> {
            KafkaProducer(
                configs = config,
                keySerializer = ByteArraySerializer(),
                valueSerializer = ByteArraySerializer(),
            )
        }
    }

    @Test
    fun testInvalidSocketReceiveBufferSize() {
        val config = mapOf(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to "localhost:9999",
            ProducerConfig.RECEIVE_BUFFER_CONFIG to -2,
        )

        assertFailsWith<KafkaException> {
            KafkaProducer(
                configs = config,
                keySerializer = ByteArraySerializer(),
                valueSerializer = ByteArraySerializer()
            )
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = [true, false])
    @Throws(InterruptedException::class)
    fun testMetadataFetch(isIdempotenceEnabled: Boolean) {
        val configs = mapOf(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to "localhost:9999",
            ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG to isIdempotenceEnabled,
        )
        val metadata = mock<ProducerMetadata>()

        // Return empty cluster 4 times and cluster from then on
        `when`(metadata.fetch())
            .thenReturn(emptyCluster, emptyCluster, emptyCluster, emptyCluster, onePartitionCluster)
        val producer = producerWithOverrideNewSender(configs, metadata)
        val record = ProducerRecord<String, String>(topic = topic, value = "value")
        producer.send(record)

        // One request update for each empty cluster returned
        verify(metadata, times(4)).requestUpdateForTopic(topic)
        verify(metadata, times(4)).awaitUpdate(any(), any())
        verify(metadata, times(5)).fetch()

        // Should not request update for subsequent `send`
        producer.send(record, null)
        verify(metadata, times(4)).requestUpdateForTopic(topic)
        verify(metadata, times(4)).awaitUpdate(any(), any())
        verify(metadata, times(6)).fetch()

        // Should not request update for subsequent `partitionsFor`
        producer.partitionsFor(topic)
        verify(metadata, times(4)).requestUpdateForTopic(topic)
        verify(metadata, times(4)).awaitUpdate(any(), any())
        verify(metadata, times(7)).fetch()
        producer.close(Duration.ofMillis(0))
    }

    @ParameterizedTest
    @ValueSource(booleans = [true, false])
    @Throws(InterruptedException::class)
    fun testMetadataExpiry(isIdempotenceEnabled: Boolean) {
        val configs = mapOf(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to "localhost:9999",
            ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG to isIdempotenceEnabled,
        )
        val metadata = mock<ProducerMetadata>()
        `when`(metadata.fetch())
            .thenReturn(onePartitionCluster, emptyCluster, onePartitionCluster)
        val producer = producerWithOverrideNewSender(configs, metadata)
        val record = ProducerRecord<String, String>(topic = topic, value = "value")
        producer.send(record)

        // Verify the topic's metadata isn't requested since it's already present.
        verify(metadata, times(0)).requestUpdateForTopic(topic)
        verify(metadata, times(0)).awaitUpdate(any(), any())
        verify(metadata, times(1)).fetch()

        // The metadata has been expired. Verify the producer requests the topic's metadata.
        producer.send(record, null)
        verify(metadata, times(1)).requestUpdateForTopic(topic)
        verify(metadata, times(1)).awaitUpdate(any(), any())
        verify(metadata, times(3)).fetch()
        producer.close(Duration.ofMillis(0))
    }

    @ParameterizedTest
    @ValueSource(booleans = [true, false])
    @Throws(Exception::class)
    fun testMetadataTimeoutWithMissingTopic(isIdempotenceEnabled: Boolean) {
        val configs = mapOf(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to "localhost:9999",
            ProducerConfig.MAX_BLOCK_MS_CONFIG to 60000,
            ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG to isIdempotenceEnabled,
        )

        // Create a record for a not-yet-created topic
        val record = ProducerRecord<String, String>(
            topic = topic,
            partition = 2,
            value = "value",
        )
        val metadata = mock<ProducerMetadata>()
        val mockTime = MockTime()
        val invocationCount = AtomicInteger(0)
        `when`(metadata.fetch()).then {
            invocationCount.incrementAndGet()
            if (invocationCount.get() == 5) mockTime.setCurrentTimeMs(mockTime.milliseconds() + 70000)
            emptyCluster
        }
        val producer = producerWithOverrideNewSender(configs, metadata, mockTime)

        // Four request updates where the topic isn't present, at which point the timeout expires and a
        // TimeoutException is thrown
        // For idempotence enabled case, the first metadata.fetch will be called in Sender#maybeSendAndPollTransactionalRequest
        val future = producer.send(record)
        verify(metadata, times(4)).requestUpdateForTopic(topic)
        verify(metadata, times(4)).awaitUpdate(any(), any())
        verify(metadata, times(5)).fetch()

        try {
            future.get()
        } catch (e: ExecutionException) {
            assertIs<org.apache.kafka.common.errors.TimeoutException>(e.cause)
        } finally {
            producer.close(Duration.ofMillis(0))
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = [true, false])
    @Throws(Exception::class)
    fun testMetadataWithPartitionOutOfRange(isIdempotenceEnabled: Boolean) {
        val configs = mapOf(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to "localhost:9999",
            ProducerConfig.MAX_BLOCK_MS_CONFIG to 60000,
            ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG to isIdempotenceEnabled,
        )

        // Create a record with a partition higher than the initial (outdated) partition range
        val record = ProducerRecord<String, String>(
            topic = topic,
            partition = 2,
            value = "value",
        )
        val metadata = mock<ProducerMetadata>()
        val mockTime = MockTime()
        `when`(metadata.fetch())
            .thenReturn(onePartitionCluster, onePartitionCluster, threePartitionCluster)
        val producer = producerWithOverrideNewSender(configs, metadata, mockTime)
        // One request update if metadata is available but outdated for the given record
        producer.send(record)
        verify(metadata, times(2)).requestUpdateForTopic(topic)
        verify(metadata, times(2)).awaitUpdate(any(), any())
        verify(metadata, times(3)).fetch()
        producer.close(Duration.ofMillis(0))
    }

    @ParameterizedTest
    @ValueSource(booleans = [true, false])
    @Throws(Exception::class)
    fun testMetadataTimeoutWithPartitionOutOfRange(isIdempotenceEnabled: Boolean) {
        val configs = mapOf(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to "localhost:9999",
            ProducerConfig.MAX_BLOCK_MS_CONFIG to 60000,
            ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG to isIdempotenceEnabled,
        )

        // Create a record with a partition higher than the initial (outdated) partition range
        val record = ProducerRecord<String, String>(
            topic = topic,
            partition = 2,
            value = "value",
        )
        val metadata = mock<ProducerMetadata>()
        val mockTime = MockTime()
        val invocationCount = AtomicInteger(0)
        `when`(metadata.fetch()).then {
            invocationCount.incrementAndGet()
            if (invocationCount.get() == 5) mockTime.setCurrentTimeMs(mockTime.milliseconds() + 70000)
            onePartitionCluster
        }
        val producer = producerWithOverrideNewSender(configs, metadata, mockTime)

        // Four request updates where the requested partition is out of range, at which point the timeout expires
        // and a TimeoutException is thrown
        // For idempotence enabled case, the first and last metadata.fetch will be called in Sender#maybeSendAndPollTransactionalRequest,
        // before the producer#send and after it finished
        val future = producer.send(record)
        verify(metadata, times(4)).requestUpdateForTopic(topic)
        verify(metadata, times(4)).awaitUpdate(any(), any())
        verify(metadata, times(5)).fetch()
        try {
            future.get()
        } catch (e: ExecutionException) {
            assertIs<org.apache.kafka.common.errors.TimeoutException>(e.cause)
        } finally {
            producer.close(Duration.ofMillis(0))
        }
    }

    @Test
    @Throws(InterruptedException::class)
    fun testTopicRefreshInMetadata() {
        val configs = mapOf(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to "localhost:9999",
            ProducerConfig.MAX_BLOCK_MS_CONFIG to "600000",
            // test under normal producer for simplicity
            ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG to false,
        )
        val refreshBackoffMs = 500L
        val metadataExpireMs = 60000L
        val metadataIdleMs = 60000L
        val time: Time = MockTime()
        val metadata = ProducerMetadata(
            refreshBackoffMs = refreshBackoffMs,
            metadataExpireMs = metadataExpireMs,
            metadataIdleMs = metadataIdleMs,
            logContext = LogContext(),
            clusterResourceListeners = ClusterResourceListeners(),
            time = time,
        )
        val topic = "topic"
        kafkaProducer(
            configs = configs,
            keySerializer = StringSerializer(),
            valueSerializer = StringSerializer(),
            metadata = metadata,
            kafkaClient = MockClient(time, metadata),
            interceptors = null,
            time = time,
        ).use { producer ->
            val running = AtomicBoolean(true)
            val t = Thread {
                val startTimeMs = System.currentTimeMillis()
                while (running.get()) {
                    while (!metadata.updateRequested() && System.currentTimeMillis() - startTimeMs < 100) Thread.yield()
                    val updateResponse = metadataUpdateWith(
                        clusterId = "kafka-cluster",
                        numNodes = 1,
                        topicErrors = mapOf(topic to Errors.UNKNOWN_TOPIC_OR_PARTITION),
                        topicPartitionCounts = emptyMap(),
                    )
                    metadata.updateWithCurrentRequestVersion(
                        response = updateResponse,
                        isPartialUpdate = false,
                        nowMs = time.milliseconds(),
                    )
                    time.sleep(60 * 1000L)
                }
            }
            t.start()
            assertFailsWith<org.apache.kafka.common.errors.TimeoutException> { producer.partitionsFor(topic) }
            running.set(false)
            t.join()
        }
    }

    @Test
    @Throws(InterruptedException::class)
    fun testTopicExpiryInMetadata() {
        val configs = mapOf(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to "localhost:9999",
            ProducerConfig.MAX_BLOCK_MS_CONFIG to "30000",
        )
        val refreshBackoffMs = 500L
        val metadataExpireMs = 60000L
        val metadataIdleMs = 60000L
        val time: Time = MockTime()
        val metadata = ProducerMetadata(
            refreshBackoffMs = refreshBackoffMs,
            metadataExpireMs = metadataExpireMs,
            metadataIdleMs = metadataIdleMs,
            logContext = LogContext(),
            clusterResourceListeners = ClusterResourceListeners(),
            time = time,
        )
        val topic = "topic"
        kafkaProducer(
            configs = configs,
            keySerializer = StringSerializer(),
            valueSerializer = StringSerializer(),
            metadata = metadata,
            kafkaClient = MockClient(time, metadata),
            interceptors = null,
            time = time,
        ).use { producer ->
            val exchanger = Exchanger<Unit>()
            val t = Thread {
                try {
                    exchanger.exchange(null) // 1
                    while (!metadata.updateRequested()) Thread.sleep(100)
                    var updateResponse = metadataUpdateWith(
                        numNodes = 1,
                        topicPartitionCounts = mapOf(topic to 1),
                    )
                    metadata.updateWithCurrentRequestVersion(
                        response = updateResponse,
                        isPartialUpdate = false,
                        nowMs = time.milliseconds(),
                    )
                    exchanger.exchange(null) // 2
                    time.sleep(120 * 1000L)

                    // Update the metadata again, but it should be expired at this point.
                    updateResponse = metadataUpdateWith(
                        numNodes = 1,
                        topicPartitionCounts = mapOf(topic to 1),
                    )
                    metadata.updateWithCurrentRequestVersion(
                        response = updateResponse,
                        isPartialUpdate = false,
                        nowMs = time.milliseconds(),
                    )
                    exchanger.exchange(null) // 3
                    while (!metadata.updateRequested()) Thread.sleep(100)
                    time.sleep(30 * 1000L)
                } catch (e: Exception) {
                    throw RuntimeException(e)
                }
            }
            t.start()
            exchanger.exchange(null) // 1
            assertNotNull(producer.partitionsFor(topic))
            exchanger.exchange(null) // 2
            exchanger.exchange(null) // 3
            assertFailsWith<org.apache.kafka.common.errors.TimeoutException> { producer.partitionsFor(topic) }
            t.join()
        }
    }

    @Test
    fun testHeaders() {
        doTestHeaders<Serializer<String?>>()
    }

    private inline fun <reified T : Serializer<String?>> doTestHeaders() {
        val configs = mapOf(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to "localhost:9999",
        )
        val keySerializer = mock<T>()
        val valueSerializer = mock<T>()
        val nowMs = Time.SYSTEM.milliseconds()
        val topic = "topic"
        val metadata = newMetadata(0, 90000)
        metadata.add(topic, nowMs)
        val initialUpdateResponse = metadataUpdateWith(
            numNodes = 1,
            topicPartitionCounts = mapOf(topic to 1),
        )
        metadata.updateWithCurrentRequestVersion(
            response = initialUpdateResponse,
            isPartialUpdate = false,
            nowMs = nowMs,
        )
        val producer = kafkaProducer(
            configs = configs,
            keySerializer = keySerializer,
            valueSerializer = valueSerializer,
            metadata = metadata,
            kafkaClient = null,
            interceptors = null,
            time = Time.SYSTEM,
        )
        `when`(keySerializer.serialize(any(), any(), any()))
            .then { invocation -> invocation.getArgument<String>(2).toByteArray() }

        `when`(valueSerializer.serialize(any(), any(), any()))
            .then { invocation -> invocation.getArgument<String>(2).toByteArray() }

        val value = "value"
        val key = "key"
        val record = ProducerRecord<String?, String?>(topic = topic, key = key, value = value)

        //ensure headers can be mutated pre send.
        record.headers.add(RecordHeader("test", "header2".toByteArray()))
        producer.send(record, null)

        //ensure headers are closed and cannot be mutated post send
        assertFailsWith<IllegalStateException> {
            record.headers.add(RecordHeader("test", "test".toByteArray()))
        }

        //ensure existing headers are not changed, and last header for key is still original value
        assertContentEquals(record.headers.lastHeader("test")!!.value, "header2".toByteArray())
        verify(valueSerializer).serialize(topic, record.headers, value)
        verify(keySerializer).serialize(topic, record.headers, key)
        producer.close(Duration.ofMillis(0))
    }

    @Test
    fun closeShouldBeIdempotent() {
        val producerProps = Properties().apply {
            put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9000")
        }
        val producer = KafkaProducer(
            properties = producerProps,
            keySerializer = ByteArraySerializer(),
            valueSerializer = ByteArraySerializer(),
        )
        producer.close()
        producer.close()
    }

    @Test
    fun closeWithNegativeTimestampShouldThrow() {
        val producerProps = Properties().apply {
            put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9000")
        }
        KafkaProducer(producerProps, ByteArraySerializer(), ByteArraySerializer()).use { producer ->
            assertFailsWith<IllegalArgumentException> { producer.close(Duration.ofMillis(-100)) }
        }
    }

    @Test
    fun testFlushCompleteSendOfInflightBatches() {
        val configs = mapOf(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to "localhost:9000",
            // only test in idempotence disabled producer for simplicity
            // flush operation acts the same for idempotence enabled and disabled cases
            ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG to false,
        )

        val time = MockTime(1)
        val initialUpdateResponse = metadataUpdateWith(
            numNodes = 1,
            topicPartitionCounts = mapOf("topic" to 1),
        )
        val metadata = newMetadata(0, Long.MAX_VALUE)
        val client = MockClient(time, metadata)
        client.updateMetadata(initialUpdateResponse)
        kafkaProducer(
            configs = configs,
            keySerializer = StringSerializer(),
            valueSerializer = StringSerializer(),
            metadata = metadata,
            kafkaClient = client,
            interceptors = null,
            time = time,
        ).use { producer ->
            val futureResponses = mutableListOf<Future<RecordMetadata>>()
            for (i in 0..49) {
                val response = producer.send(
                    ProducerRecord(
                        topic = "topic",
                        value = "value$i",
                    )
                )
                futureResponses.add(response)
            }
            futureResponses.forEach { res -> assertFalse(res.isDone) }
            producer.flush()
            futureResponses.forEach { res -> assertTrue(res.isDone) }
        }
    }

    @Test
    fun testFlushMeasureLatency() {
        val configs = mapOf(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to "localhost:9000",
        )
        val time = MockTime(1)
        val initialUpdateResponse = metadataUpdateWith(
            numNodes = 1,
            topicPartitionCounts = mapOf("topic" to 1),
        )
        val metadata = newMetadata(0, Long.MAX_VALUE)
        val client = MockClient(time, metadata)
        client.updateMetadata(initialUpdateResponse)
        kafkaProducer(
            configs = configs,
            keySerializer = StringSerializer(),
            valueSerializer = StringSerializer(),
            metadata = metadata,
            kafkaClient = client,
            interceptors = null,
            time = time
        ).use { producer ->
            producer.flush()
            val first: Double = getMetricValue(producer, "flush-time-ns-total")
            assertTrue(first > 0)
            producer.flush()
            assertTrue(getMetricValue(producer, "flush-time-ns-total") > first)
        }
    }

    @Test
    fun testMetricConfigRecordingLevel() {
        val props = Properties()
        props[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = "localhost:9000"
        KafkaProducer(
            properties = props,
            keySerializer = ByteArraySerializer(),
            valueSerializer = ByteArraySerializer(),
        ).use { producer ->
            assertEquals(
                expected = Sensor.RecordingLevel.INFO,
                actual = producer.metrics.config.recordingLevel,
            )
        }
        props[ProducerConfig.METRICS_RECORDING_LEVEL_CONFIG] = "DEBUG"
        KafkaProducer(
            properties = props,
            keySerializer = ByteArraySerializer(),
            valueSerializer = ByteArraySerializer(),
        ).use { producer ->
            assertEquals(
                expected = Sensor.RecordingLevel.DEBUG,
                actual = producer.metrics.config.recordingLevel,
            )
        }
    }

    @Test
    fun testInterceptorPartitionSetOnTooLargeRecord() {
        val configs = mapOf(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to "localhost:9999",
            ProducerConfig.MAX_REQUEST_SIZE_CONFIG to "1",
        )
        val topic = "topic"
        val record = ProducerRecord<String, String>(topic = topic, value = "value")
        val nowMs = Time.SYSTEM.milliseconds()
        val metadata = newMetadata(0, 90000)
        metadata.add(topic, nowMs)
        val initialUpdateResponse = metadataUpdateWith(
            numNodes = 1,
            topicPartitionCounts = mapOf(topic to 1)
        )
        metadata.updateWithCurrentRequestVersion(
            response = initialUpdateResponse,
            isPartialUpdate = false,
            nowMs = nowMs,
        )
        // it is safe to suppress, since this is a mock class
        val interceptors = mock<ProducerInterceptors<String, String>>()
        val producer = kafkaProducer(
            configs = configs,
            keySerializer = StringSerializer(),
            valueSerializer = StringSerializer(),
            metadata = metadata,
            kafkaClient = null,
            interceptors = interceptors,
            time = Time.SYSTEM,
        )
        `when`(interceptors.onSend(any()))
            .then { invocation -> invocation.getArgument(0) }
        producer.send(record)
        verify(interceptors).onSend(record)
        verify(interceptors)
            .onSendError(eq(record), notNull(), notNull())
        producer.close(Duration.ofMillis(0))
    }

    @Test
    @Disabled("Kotlin Migration: Field is not nullable anymore")
    fun testPartitionsForWithNullTopic() {
//        val props = Properties()
//        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9000")
//        KafkaProducer(props, ByteArraySerializer(), ByteArraySerializer()).use { producer ->
//            assertFailsWith<NullPointerException> { producer.partitionsFor(null) }
//        }
    }

    @Test
    @Throws(Exception::class)
    fun testInitTransactionsResponseAfterTimeout() {
        val maxBlockMs = 500
        val configs = mapOf(
            ProducerConfig.TRANSACTIONAL_ID_CONFIG to "bad-transaction",
            ProducerConfig.MAX_BLOCK_MS_CONFIG to maxBlockMs,
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to "localhost:9000",
        )
        val time: Time = MockTime()
        val initialUpdateResponse = metadataUpdateWith(
            numNodes = 1,
            topicPartitionCounts = mapOf("topic" to 1),
        )
        val metadata = newMetadata(refreshBackoffMs = 0, expirationMs = Long.MAX_VALUE)
        metadata.updateWithCurrentRequestVersion(
            response = initialUpdateResponse,
            isPartialUpdate = false,
            nowMs = time.milliseconds(),
        )
        val client = MockClient(time, metadata)
        val executor = Executors.newFixedThreadPool(1)
        val producer = kafkaProducer(
            configs = configs,
            keySerializer = StringSerializer(),
            valueSerializer = StringSerializer(),
            metadata = metadata,
            kafkaClient = client,
            interceptors = null,
            time = time,
        )
        try {
            client.prepareResponse(
                matcher = { request ->
                    request is FindCoordinatorRequest &&
                            request.data().keyType == FindCoordinatorRequest.CoordinatorType.TRANSACTION.id
                },
                response = FindCoordinatorResponse.prepareResponse(Errors.NONE, "bad-transaction", NODE)
            )
            val future = executor.submit(producer::initTransactions)
            TestUtils.waitForCondition(
                testCondition = client::hasInFlightRequests,
                conditionDetails = "Timed out while waiting for expected `InitProducerId` request to be sent"
            )
            time.sleep(maxBlockMs.toLong())
            assertFutureThrows(
                future = future,
                exceptionCauseClass = org.apache.kafka.common.errors.TimeoutException::class.java
            )
            client.respond(
                initProducerIdResponse(
                    producerId = 1L,
                    producerEpoch = 5,
                    error = Errors.NONE,
                )
            )
            Thread.sleep(1000)
            producer.initTransactions()
        } finally {
            producer.close(Duration.ZERO)
        }
    }

    @Test
    fun testInitTransactionTimeout() {
        val configs = mapOf(
            ProducerConfig.TRANSACTIONAL_ID_CONFIG to "bad-transaction",
            ProducerConfig.MAX_BLOCK_MS_CONFIG to 500,
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to "localhost:9000",
        )
        val time = MockTime(1)
        val initialUpdateResponse = metadataUpdateWith(
            numNodes = 1,
            topicPartitionCounts = mapOf("topic" to 1),
        )
        val metadata = newMetadata(refreshBackoffMs = 0, expirationMs = Long.MAX_VALUE)
        metadata.updateWithCurrentRequestVersion(
            response = initialUpdateResponse,
            isPartialUpdate = false,
            nowMs = time.milliseconds(),
        )
        val client = MockClient(time, metadata)
        kafkaProducer(
            configs = configs,
            keySerializer = StringSerializer(),
            valueSerializer = StringSerializer(),
            metadata = metadata,
            kafkaClient = client,
            interceptors = null,
            time = time,
        ).use { producer ->
            client.prepareResponse(
                matcher = { request ->
                    request is FindCoordinatorRequest
                            && request.data().keyType == FindCoordinatorRequest.CoordinatorType.TRANSACTION.id
                },
                response = FindCoordinatorResponse.prepareResponse(
                    error = Errors.NONE,
                    key = "bad-transaction",
                    node = NODE,
                ),
            )
            assertFailsWith<org.apache.kafka.common.errors.TimeoutException> { producer.initTransactions() }
            client.prepareResponse(
                matcher = { request ->
                    request is FindCoordinatorRequest
                            && request.data().keyType == FindCoordinatorRequest.CoordinatorType.TRANSACTION.id
                },
                response = FindCoordinatorResponse.prepareResponse(
                    error = Errors.NONE,
                    key = "bad-transaction",
                    node = NODE,
                ),
            )
            client.prepareResponse(
                initProducerIdResponse(
                    producerId = 1L,
                    producerEpoch = 5,
                    error = Errors.NONE,
                )
            )

            // retry initialization should work
            producer.initTransactions()
        }
    }

    @Test
    fun testInitTransactionWhileThrottled() {
        val configs = mapOf(
            ProducerConfig.TRANSACTIONAL_ID_CONFIG to "some.id",
            ProducerConfig.MAX_BLOCK_MS_CONFIG to 10000,
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to "localhost:9000",
        )
        val time = MockTime(1)
        val initialUpdateResponse = metadataUpdateWith(
            numNodes = 1,
            topicPartitionCounts = mapOf("topic" to 1),
        )
        val metadata = newMetadata(0, Long.MAX_VALUE)
        val client = MockClient(time, metadata)
        client.updateMetadata(initialUpdateResponse)
        val node = metadata.fetch().nodes[0]
        client.throttle(node, 5000)
        client.prepareResponse(
            FindCoordinatorResponse.prepareResponse(
                error = Errors.NONE,
                key = "some.id",
                node = NODE,
            )
        )
        client.prepareResponse(
            initProducerIdResponse(
                producerId = 1L,
                producerEpoch = 5,
                error = Errors.NONE
            )
        )
        kafkaProducer(
            configs = configs,
            keySerializer = StringSerializer(),
            valueSerializer = StringSerializer(),
            metadata = metadata,
            kafkaClient = client,
            interceptors = null,
            time = time,
        ).use { producer -> producer.initTransactions() }
    }

    @Test
    fun testAbortTransaction() {
        val configs = mapOf(
            ProducerConfig.TRANSACTIONAL_ID_CONFIG to "some.id",
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to "localhost:9000",
        )
        val time = MockTime(1)
        val initialUpdateResponse = metadataUpdateWith(
            numNodes = 1,
            topicPartitionCounts = mapOf("topic" to 1),
        )
        val metadata = newMetadata(0, Long.MAX_VALUE)
        val client = MockClient(time, metadata)
        client.updateMetadata(initialUpdateResponse)
        client.prepareResponse(
            FindCoordinatorResponse.prepareResponse(
                error = Errors.NONE,
                key = "some.id",
                node = NODE
            )
        )
        client.prepareResponse(
            initProducerIdResponse(
                producerId = 1L,
                producerEpoch = 5,
                error = Errors.NONE,
            )
        )
        client.prepareResponse(endTxnResponse(Errors.NONE))
        kafkaProducer(
            configs = configs,
            keySerializer = StringSerializer(),
            valueSerializer = StringSerializer(),
            metadata = metadata,
            kafkaClient = client,
            interceptors = null,
            time = time,
        ).use { producer ->
            producer.initTransactions()
            producer.beginTransaction()
            producer.abortTransaction()
        }
    }

    @Test
    fun testMeasureAbortTransactionDuration() {
        val configs = mapOf(
            ProducerConfig.TRANSACTIONAL_ID_CONFIG to "some.id",
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to "localhost:9000",
        )
        val time = MockTime(1)
        val initialUpdateResponse = metadataUpdateWith(
            numNodes = 1,
            topicPartitionCounts = mapOf("topic" to 1),
        )
        val metadata = newMetadata(refreshBackoffMs = 0, expirationMs = Long.MAX_VALUE)
        val client = MockClient(time, metadata)
        client.updateMetadata(initialUpdateResponse)
        client.prepareResponse(
            FindCoordinatorResponse.prepareResponse(
                error = Errors.NONE,
                key = "some.id",
                node = NODE,
            )
        )
        client.prepareResponse(
            initProducerIdResponse(
                producerId = 1L,
                producerEpoch = 5,
                error = Errors.NONE,
            )
        )
        kafkaProducer(
            configs = configs,
            keySerializer = StringSerializer(),
            valueSerializer = StringSerializer(),
            metadata = metadata,
            kafkaClient = client,
            interceptors = null,
            time = time,
        ).use { producer ->
            producer.initTransactions()
            client.prepareResponse(endTxnResponse(Errors.NONE))
            producer.beginTransaction()
            producer.abortTransaction()
            val first: Double = getMetricValue(producer, "txn-abort-time-ns-total")

            assertTrue(first > 0)

            client.prepareResponse(endTxnResponse(Errors.NONE))
            producer.beginTransaction()
            producer.abortTransaction()

            assertTrue(getMetricValue(producer, "txn-abort-time-ns-total") > first)
        }
    }

    @Test
    @Throws(Exception::class)
    fun testCommitTransactionWithRecordTooLargeException() {
        val configs = mapOf(
            ProducerConfig.TRANSACTIONAL_ID_CONFIG to "some.id",
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to "localhost:9000",
            ProducerConfig.MAX_REQUEST_SIZE_CONFIG to 1000,
        )
        val time = MockTime(1)
        val initialUpdateResponse = metadataUpdateWith(
            numNodes = 1,
            topicPartitionCounts = mapOf("topic" to 1),
        )
        val metadata = mock<ProducerMetadata>()
        val client = MockClient(time, metadata)
        client.updateMetadata(initialUpdateResponse)
        client.prepareResponse(
            FindCoordinatorResponse.prepareResponse(
                error = Errors.NONE,
                key = "some.id",
                node = NODE,
            )
        )
        client.prepareResponse(
            initProducerIdResponse(
                producerId = 1L,
                producerEpoch = 5,
                error = Errors.NONE,
            )
        )
        `when`(metadata.fetch()).thenReturn(onePartitionCluster)
        val largeString = "*".repeat(1000)
        val largeRecord = ProducerRecord(
            topic = topic,
            key = "large string",
            value = largeString,
        )
        kafkaProducer(
            configs = configs,
            keySerializer = StringSerializer(),
            valueSerializer = StringSerializer(),
            metadata = metadata,
            kafkaClient = client,
            interceptors = null,
            time = time,
        ).use { producer ->
            producer.initTransactions()
            client.prepareResponse(endTxnResponse(Errors.NONE))
            producer.beginTransaction()
            assertFutureError(
                future = producer.send(largeRecord),
                exceptionClass = RecordTooLargeException::class.java,
            )
            assertFailsWith<KafkaException> { producer.commitTransaction() }
        }
    }

    @Test
    @Throws(Exception::class)
    fun testCommitTransactionWithMetadataTimeoutForMissingTopic() {
        val configs = mapOf(
            ProducerConfig.TRANSACTIONAL_ID_CONFIG to "some.id",
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to "localhost:9999",
            ProducerConfig.MAX_BLOCK_MS_CONFIG to 60000,
        )

        // Create a record for a not-yet-created topic
        val record = ProducerRecord<String, String>(topic = topic, value = "value")
        val metadata = mock<ProducerMetadata>()
        val mockTime = MockTime()
        val client = MockClient(mockTime, metadata)
        client.prepareResponse(FindCoordinatorResponse.prepareResponse(Errors.NONE, "some.id", NODE))
        client.prepareResponse(initProducerIdResponse(1L, 5.toShort(), Errors.NONE))
        val invocationCount = AtomicInteger(0)
        `when`(metadata.fetch()).then {
            invocationCount.incrementAndGet()
            if (invocationCount.get() > 5) mockTime.setCurrentTimeMs(mockTime.milliseconds() + 70000)
            emptyCluster
        }
        kafkaProducer(
            configs = configs,
            keySerializer = StringSerializer(),
            valueSerializer = StringSerializer(),
            metadata = metadata,
            kafkaClient = client,
            interceptors = null,
            time = mockTime,
        ).use { producer ->
            producer.initTransactions()
            producer.beginTransaction()
            assertFutureError(
                future = producer.send(record),
                exceptionClass = org.apache.kafka.common.errors.TimeoutException::class.java,
            )
            assertFailsWith<KafkaException> { producer.commitTransaction() }
        }
    }

    @Test
    @Throws(Exception::class)
    fun testCommitTransactionWithMetadataTimeoutForPartitionOutOfRange() {
        val configs: MutableMap<String, Any?> = HashMap()
        configs[ProducerConfig.TRANSACTIONAL_ID_CONFIG] = "some.id"
        configs[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = "localhost:9999"
        configs[ProducerConfig.MAX_BLOCK_MS_CONFIG] = 60000

        // Create a record with a partition higher than the initial (outdated) partition range
        val record = ProducerRecord<String, String>(
            topic = topic,
            partition = 2,
            key = null,
            value = "value",
        )
        val metadata = mock<ProducerMetadata>()
        val mockTime = MockTime()
        val client = MockClient(mockTime, metadata)
        client.prepareResponse(
            FindCoordinatorResponse.prepareResponse(
                error = Errors.NONE,
                key = "some.id",
                node = NODE,
            )
        )
        client.prepareResponse(
            initProducerIdResponse(
                producerId = 1L,
                producerEpoch = 5,
                error = Errors.NONE,
            ),
        )
        val invocationCount = AtomicInteger(0)
        `when`(metadata.fetch()).then {
            invocationCount.incrementAndGet()
            if (invocationCount.get() > 5) mockTime.setCurrentTimeMs(mockTime.milliseconds() + 70000)
            onePartitionCluster
        }
        kafkaProducer(
            configs = configs,
            keySerializer = StringSerializer(),
            valueSerializer = StringSerializer(),
            metadata = metadata,
            kafkaClient = client,
            interceptors = null,
            time = mockTime,
        ).use { producer ->
            producer.initTransactions()
            producer.beginTransaction()
            assertFutureError(
                producer.send(record),
                org.apache.kafka.common.errors.TimeoutException::class.java
            )
            assertFailsWith<KafkaException> { producer.commitTransaction() }
        }
    }

    @Test
    @Throws(Exception::class)
    fun testCommitTransactionWithSendToInvalidTopic() {
        val configs = mapOf(
            ProducerConfig.TRANSACTIONAL_ID_CONFIG to "some.id",
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to "localhost:9000",
            ProducerConfig.MAX_BLOCK_MS_CONFIG to "15000",
        )
        val time = MockTime()
        val initialUpdateResponse = metadataUpdateWith(numNodes = 1, topicPartitionCounts = emptyMap())
        val metadata = newMetadata(refreshBackoffMs = 0, expirationMs = Long.MAX_VALUE)
        metadata.updateWithCurrentRequestVersion(initialUpdateResponse, false, time.milliseconds())
        val client = MockClient(time, metadata)
        client.prepareResponse(
            FindCoordinatorResponse.prepareResponse(
                error = Errors.NONE,
                key = "some.id",
                node = NODE,
            )
        )
        client.prepareResponse(initProducerIdResponse(1L, 5.toShort(), Errors.NONE))
        val invalidTopicName = "topic abc" // Invalid topic name due to space
        val record = ProducerRecord<String, String>(topic = invalidTopicName, value = "HelloKafka")
        val topicMetadata: MutableList<MetadataResponse.TopicMetadata> = ArrayList()
        topicMetadata.add(
            MetadataResponse.TopicMetadata(
                error = Errors.INVALID_TOPIC_EXCEPTION,
                topic = invalidTopicName,
                isInternal = false,
                partitionMetadata = emptyList(),
            )
        )
        val updateResponse = metadataResponse(
            brokers = initialUpdateResponse.brokers().toList(),
            clusterId = initialUpdateResponse.clusterId,
            controllerId = initialUpdateResponse.controller!!.id,
            topicMetadataList = topicMetadata,
        )
        client.prepareMetadataUpdate(updateResponse)
        kafkaProducer(
            configs = configs,
            keySerializer = StringSerializer(),
            valueSerializer = StringSerializer(),
            metadata = metadata,
            kafkaClient = client,
            interceptors = null,
            time = time,
        ).use { producer ->
            producer.initTransactions()
            producer.beginTransaction()
            assertFutureError(producer.send(record), InvalidTopicException::class.java)
            assertFailsWith<KafkaException> { producer.commitTransaction() }
        }
    }

    @Test
    fun testSendTxnOffsetsWithGroupId() {
        val configs = mapOf(
            ProducerConfig.TRANSACTIONAL_ID_CONFIG to "some.id",
            ProducerConfig.MAX_BLOCK_MS_CONFIG to 10000,
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to "localhost:9000",
        )
        val time = MockTime(1)
        val initialUpdateResponse = metadataUpdateWith(
            numNodes = 1,
            topicPartitionCounts = mapOf("topic" to 1),
        )
        val metadata = newMetadata(refreshBackoffMs = 0, expirationMs = Long.MAX_VALUE)
        val client = MockClient(time, metadata)
        client.updateMetadata(initialUpdateResponse)
        val node = metadata.fetch().nodes[0]
        client.throttle(node, 5000)
        client.prepareResponse(
            FindCoordinatorResponse.prepareResponse(
                error = Errors.NONE,
                key = "some.id",
                node = NODE,
            )
        )
        client.prepareResponse(
            initProducerIdResponse(
                producerId = 1L,
                producerEpoch = 5,
                error = Errors.NONE,
            )
        )
        client.prepareResponse(addOffsetsToTxnResponse(Errors.NONE))
        client.prepareResponse(
            FindCoordinatorResponse.prepareResponse(
                error = Errors.NONE,
                key = "some.id",
                node = NODE
            )
        )
        val groupId = "group"
        client.prepareResponse(
            matcher = { request -> (request as TxnOffsetCommitRequest).data().groupId == groupId },
            response = txnOffsetsCommitResponse(
                mapOf(TopicPartition("topic", 0) to Errors.NONE)
            )
        )
        client.prepareResponse(endTxnResponse(Errors.NONE))
        kafkaProducer(
            configs = configs,
            keySerializer = StringSerializer(),
            valueSerializer = StringSerializer(),
            metadata = metadata,
            kafkaClient = client,
            interceptors = null,
            time = time,
        ).use { producer ->
            producer.initTransactions()
            producer.beginTransaction()
            producer.sendOffsetsToTransaction(
                offsets = emptyMap(),
                groupMetadata = ConsumerGroupMetadata(groupId),
            )
            producer.commitTransaction()
        }
    }

    private fun assertDurationAtLeast(producer: KafkaProducer<*, *>, name: String, floor: Double) {
        getAndAssertDurationAtLeast(producer, name, floor)
    }

    private fun getAndAssertDurationAtLeast(producer: KafkaProducer<*, *>, name: String, floor: Double): Double {
        val value = getMetricValue(producer, name)
        assertTrue(value >= floor)
        return value
    }

    @Test
    fun testMeasureTransactionDurations() {
        val configs = mapOf(
            ProducerConfig.TRANSACTIONAL_ID_CONFIG to "some.id",
            ProducerConfig.MAX_BLOCK_MS_CONFIG to 10000,
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to "localhost:9000",
        )
        val tick = Duration.ofSeconds(1)
        val time = MockTime(tick.toMillis())
        val initialUpdateResponse = metadataUpdateWith(
            numNodes = 1,
            topicPartitionCounts = mapOf("topic" to 1),
        )
        val metadata = newMetadata(0, Long.MAX_VALUE)
        val client = MockClient(time, metadata)
        client.updateMetadata(initialUpdateResponse)
        client.prepareResponse(
            FindCoordinatorResponse.prepareResponse(
                error = Errors.NONE,
                key = "some.id",
                node = NODE,
            )
        )
        client.prepareResponse(
            initProducerIdResponse(
                producerId = 1L,
                producerEpoch = 5,
                error = Errors.NONE,
            )
        )
        kafkaProducer(
            configs = configs,
            keySerializer = StringSerializer(),
            valueSerializer = StringSerializer(),
            metadata = metadata,
            kafkaClient = client,
            interceptors = null,
            time = time,
        ).use { producer ->
            producer.initTransactions()
            assertDurationAtLeast(producer, "txn-init-time-ns-total", tick.toNanos().toDouble())
            client.prepareResponse(addOffsetsToTxnResponse(Errors.NONE))
            client.prepareResponse(
                FindCoordinatorResponse.prepareResponse(
                    error = Errors.NONE,
                    key = "some.id",
                    node = NODE,
                )
            )
            client.prepareResponse(
                txnOffsetsCommitResponse(
                    mapOf(TopicPartition("topic", 0) to Errors.NONE)
                )
            )
            client.prepareResponse(endTxnResponse(Errors.NONE))
            producer.beginTransaction()
            val beginFirst: Double = getAndAssertDurationAtLeast(
                producer = producer,
                name = "txn-begin-time-ns-total",
                floor = tick.toNanos().toDouble(),
            )
            producer.sendOffsetsToTransaction(
                offsets = mapOf(TopicPartition("topic", 0) to OffsetAndMetadata(5L)),
                groupMetadata = ConsumerGroupMetadata("group")
            )
            val sendOffFirst = getAndAssertDurationAtLeast(
                producer = producer,
                name = "txn-send-offsets-time-ns-total",
                floor = tick.toNanos().toDouble(),
            )
            producer.commitTransaction()
            val commitFirst = getAndAssertDurationAtLeast(
                producer = producer,
                name = "txn-commit-time-ns-total",
                floor = tick.toNanos().toDouble(),
            )
            client.prepareResponse(addOffsetsToTxnResponse(Errors.NONE))
            client.prepareResponse(
                txnOffsetsCommitResponse(
                    mapOf(TopicPartition(topic = "topic", partition = 0) to Errors.NONE)
                )
            )
            client.prepareResponse(endTxnResponse(Errors.NONE))
            producer.beginTransaction()
            assertDurationAtLeast(
                producer = producer,
                name = "txn-begin-time-ns-total",
                floor = beginFirst + tick.toNanos(),
            )
            producer.sendOffsetsToTransaction(
                offsets = mapOf(
                    TopicPartition(topic = "topic", partition = 0) to OffsetAndMetadata(10L)
                ),
                groupMetadata = ConsumerGroupMetadata("group")
            )
            assertDurationAtLeast(
                producer = producer,
                name = "txn-send-offsets-time-ns-total",
                floor = sendOffFirst + tick.toNanos(),
            )
            producer.commitTransaction()
            assertDurationAtLeast(
                producer = producer,
                name = "txn-commit-time-ns-total",
                floor = commitFirst + tick.toNanos(),
            )
        }
    }

    @Test
    fun testSendTxnOffsetsWithGroupMetadata() {
        val maxVersion = 3.toShort()
        val configs = mapOf(
            ProducerConfig.TRANSACTIONAL_ID_CONFIG to "some.id",
            ProducerConfig.MAX_BLOCK_MS_CONFIG to 10000,
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to "localhost:9000",
        )
        val time = MockTime(1)
        val initialUpdateResponse = metadataUpdateWith(
            numNodes = 1,
            topicPartitionCounts = mapOf("topic" to 1),
        )
        val metadata = newMetadata(refreshBackoffMs = 0, expirationMs = Long.MAX_VALUE)
        val client = MockClient(time, metadata)
        client.updateMetadata(initialUpdateResponse)
        client.setNodeApiVersions(
            NodeApiVersions.create(
                apiKey = ApiKeys.TXN_OFFSET_COMMIT.id,
                minVersion = 0,
                maxVersion = maxVersion,
            )
        )
        val node = metadata.fetch().nodes[0]
        client.throttle(node = node, durationMs = 5000)
        client.prepareResponse(
            FindCoordinatorResponse.prepareResponse(
                error = Errors.NONE,
                key = "some.id",
                node = NODE,
            )
        )
        client.prepareResponse(
            initProducerIdResponse(
                producerId = 1L,
                producerEpoch = 5,
                error = Errors.NONE,
            )
        )
        client.prepareResponse(addOffsetsToTxnResponse(Errors.NONE))
        client.prepareResponse(
            FindCoordinatorResponse.prepareResponse(
                error = Errors.NONE,
                key = "some.id",
                node = NODE,
            )
        )
        val groupId = "group"
        val memberId = "member"
        val generationId = 5
        val groupInstanceId = "instance"
        client.prepareResponse(
            matcher = { request ->
                val data = (request as TxnOffsetCommitRequest).data()
                data.groupId == groupId
                        && data.memberId == memberId
                        && data.generationId == generationId
                        && data.groupInstanceId == groupInstanceId
            },
            response = txnOffsetsCommitResponse(
                mapOf(TopicPartition("topic", 0) to Errors.NONE)
            )
        )
        client.prepareResponse(endTxnResponse(Errors.NONE))
        kafkaProducer(
            configs = configs,
            keySerializer = StringSerializer(),
            valueSerializer = StringSerializer(),
            metadata = metadata,
            kafkaClient = client,
            interceptors = null,
            time = time,
        ).use { producer ->
            producer.initTransactions()
            producer.beginTransaction()
            val groupMetadata = ConsumerGroupMetadata(
                groupId = groupId,
                generationId = generationId,
                memberId = memberId,
                groupInstanceId = groupInstanceId,
            )
            producer.sendOffsetsToTransaction(
                offsets = emptyMap(),
                groupMetadata = groupMetadata,
            )
            producer.commitTransaction()
        }
    }

    @Test
    @Disabled("Kotlin Migration: group metadata is not nullable")
    fun testNullGroupMetadataInSendOffsets() {
//        verifyInvalidGroupMetadata(null)
    }

    @Test
    fun testInvalidGenerationIdAndMemberIdCombinedInSendOffsets() {
        verifyInvalidGroupMetadata(
            ConsumerGroupMetadata(
                groupId = "group",
                generationId = 2,
                memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID,
                groupInstanceId = null,
            )
        )
    }

    private fun verifyInvalidGroupMetadata(groupMetadata: ConsumerGroupMetadata) {
        val configs = mapOf(
            ProducerConfig.TRANSACTIONAL_ID_CONFIG to "some.id",
            ProducerConfig.MAX_BLOCK_MS_CONFIG to 10000,
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to "localhost:9000",
        )
        val time = MockTime(1)
        val initialUpdateResponse = metadataUpdateWith(
            numNodes = 1,
            topicPartitionCounts = mapOf("topic" to 1),
        )
        val metadata = newMetadata(refreshBackoffMs = 0, expirationMs = Long.MAX_VALUE)
        val client = MockClient(time, metadata)
        client.updateMetadata(initialUpdateResponse)
        val node = metadata.fetch().nodes[0]
        client.throttle(node = node, durationMs = 5000)
        client.prepareResponse(
            FindCoordinatorResponse.prepareResponse(
                error = Errors.NONE,
                key = "some.id",
                node = NODE,
            )
        )
        client.prepareResponse(
            initProducerIdResponse(
                producerId = 1L,
                producerEpoch = 5,
                error = Errors.NONE,
            )
        )
        kafkaProducer(
            configs = configs,
            keySerializer = StringSerializer(),
            valueSerializer = StringSerializer(),
            metadata = metadata,
            kafkaClient = client,
            interceptors = null,
            time = time,
        ).use { producer ->
            producer.initTransactions()
            producer.beginTransaction()
            assertFailsWith<IllegalArgumentException> {
                producer.sendOffsetsToTransaction(
                    offsets = emptyMap(),
                    groupMetadata = groupMetadata,
                )
            }
        }
    }

    private fun initProducerIdResponse(
        producerId: Long,
        producerEpoch: Short,
        error: Errors,
    ): InitProducerIdResponse {
        val responseData = InitProducerIdResponseData()
            .setErrorCode(error.code)
            .setProducerEpoch(producerEpoch)
            .setProducerId(producerId)
            .setThrottleTimeMs(0)
        return InitProducerIdResponse(responseData)
    }

    private fun addOffsetsToTxnResponse(error: Errors): AddOffsetsToTxnResponse {
        return AddOffsetsToTxnResponse(
            AddOffsetsToTxnResponseData()
                .setErrorCode(error.code)
                .setThrottleTimeMs(10)
        )
    }

    private fun txnOffsetsCommitResponse(errorMap: Map<TopicPartition, Errors>): TxnOffsetCommitResponse =
        TxnOffsetCommitResponse(10, errorMap)

    private fun endTxnResponse(error: Errors): EndTxnResponse = EndTxnResponse(
        EndTxnResponseData()
            .setErrorCode(error.code)
            .setThrottleTimeMs(0)
    )

    @Test
    fun testOnlyCanExecuteCloseAfterInitTransactionsTimeout() {
        val configs = mapOf(
            ProducerConfig.TRANSACTIONAL_ID_CONFIG to "bad-transaction",
            ProducerConfig.MAX_BLOCK_MS_CONFIG to 5,
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to "localhost:9000",
        )
        val time = MockTime()
        val initialUpdateResponse = metadataUpdateWith(
            numNodes = 1,
            topicPartitionCounts = mapOf("topic" to 1),
        )
        val metadata = newMetadata(refreshBackoffMs = 0, expirationMs = Long.MAX_VALUE)
        metadata.updateWithCurrentRequestVersion(
            response = initialUpdateResponse,
            isPartialUpdate = false,
            nowMs = time.milliseconds(),
        )
        val client = MockClient(time, metadata)
        val producer = kafkaProducer(
            configs = configs,
            keySerializer = StringSerializer(),
            valueSerializer = StringSerializer(),
            metadata = metadata,
            kafkaClient = client,
            interceptors = null,
            time = time,
        )
        assertFailsWith<org.apache.kafka.common.errors.TimeoutException> { producer.initTransactions() }
        // other transactional operations should not be allowed if we catch the error after initTransactions failed
        try {
            assertFailsWith<IllegalStateException> { producer.beginTransaction() }
        } finally {
            producer.close(Duration.ofMillis(0))
        }
    }

    @Test
    @Throws(Exception::class)
    fun testSendToInvalidTopic() {
        val configs = mapOf(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to "localhost:9000",
            ProducerConfig.MAX_BLOCK_MS_CONFIG to "15000",
        )
        val time = MockTime()
        val initialUpdateResponse = metadataUpdateWith(
            numNodes = 1,
            topicPartitionCounts = emptyMap(),
        )
        val metadata = newMetadata(refreshBackoffMs = 0, expirationMs = Long.MAX_VALUE)
        metadata.updateWithCurrentRequestVersion(
            response = initialUpdateResponse,
            isPartialUpdate = false,
            nowMs = time.milliseconds(),
        )
        val client = MockClient(time, metadata)
        val producer = kafkaProducer(
            configs = configs,
            keySerializer = StringSerializer(),
            valueSerializer = StringSerializer(),
            metadata = metadata,
            kafkaClient = client,
            interceptors = null,
            time = time,
        )
        val invalidTopicName = "topic abc" // Invalid topic name due to space
        val record = ProducerRecord<String, String>(topic = invalidTopicName, value = "HelloKafka")
        val topicMetadata = listOf(
            MetadataResponse.TopicMetadata(
                error = Errors.INVALID_TOPIC_EXCEPTION,
                topic = invalidTopicName,
                isInternal = false,
                partitionMetadata = emptyList(),
            )
        )
        val updateResponse = metadataResponse(
            brokers = initialUpdateResponse.brokers().toList(),
            clusterId = initialUpdateResponse.clusterId,
            controllerId = initialUpdateResponse.controller!!.id,
            topicMetadataList = topicMetadata,
        )
        client.prepareMetadataUpdate(updateResponse)
        val future = producer.send(record)

        assertEquals(
            expected = setOf(invalidTopicName),
            actual = metadata.fetch().invalidTopics,
            message = "Cluster has incorrect invalid topic list.",
        )
        assertFutureError(future, InvalidTopicException::class.java)

        producer.close(Duration.ofMillis(0))
    }

    @Test
    @Throws(InterruptedException::class)
    fun testCloseWhenWaitingForMetadataUpdate() {
        val configs = mapOf(
            ProducerConfig.MAX_BLOCK_MS_CONFIG to Long.MAX_VALUE,
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to "localhost:9000",
        )

        // Simulate a case where metadata for a particular topic is not available. This will cause KafkaProducer#send to
        // block in Metadata#awaitUpdate for the configured max.block.ms. When close() is invoked, KafkaProducer#send should
        // return with a KafkaException.
        val topicName = "test"
        val time = Time.SYSTEM
        val initialUpdateResponse = metadataUpdateWith(
            numNodes = 1,
            topicPartitionCounts = emptyMap(),
        )
        val metadata = ProducerMetadata(
            refreshBackoffMs = 0,
            metadataExpireMs = Long.MAX_VALUE,
            metadataIdleMs = Long.MAX_VALUE,
            logContext = LogContext(),
            clusterResourceListeners = ClusterResourceListeners(),
            time = time,
        )
        metadata.updateWithCurrentRequestVersion(initialUpdateResponse, false, time.milliseconds())
        val client = MockClient(time, metadata)
        val producer = kafkaProducer(
            configs = configs,
            keySerializer = StringSerializer(),
            valueSerializer = StringSerializer(),
            metadata = metadata,
            kafkaClient = client,
            interceptors = null,
            time = time,
        )
        val executor = Executors.newSingleThreadExecutor()
        val sendException = AtomicReference<Exception>()
        try {
            executor.submit {
                try {
                    // Metadata for topic "test" will not be available which will cause us to block indefinitely until
                    // KafkaProducer#close is invoked.
                    producer.send(ProducerRecord(topic = topicName, key = "key", value = "value"))
                    fail()
                } catch (e: Exception) {
                    sendException.set(e)
                }
            }

            // Wait until metadata update for the topic has been requested
            TestUtils.waitForCondition(
                testCondition = { metadata.containsTopic(topicName) },
                conditionDetails = "Timeout when waiting for topic to be added to metadata",
            )
            producer.close(Duration.ofMillis(0))
            TestUtils.waitForCondition(
                testCondition = { sendException.get() != null },
                conditionDetails = "No producer exception within timeout",
            )
            assertIs<KafkaException>(sendException.get()!!)
        } finally {
            executor.shutdownNow()
        }
    }

    @Test
    fun testTransactionalMethodThrowsWhenSenderClosed() {
        val configs = mapOf(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to "localhost:9000",
            ProducerConfig.TRANSACTIONAL_ID_CONFIG to "this-is-a-transactional-id",
        )
        val time = MockTime()
        val initialUpdateResponse = metadataUpdateWith(numNodes = 1, topicPartitionCounts = emptyMap())
        val metadata = newMetadata(refreshBackoffMs = 0, expirationMs = Long.MAX_VALUE)
        metadata.updateWithCurrentRequestVersion(
            response = initialUpdateResponse,
            isPartialUpdate = false,
            nowMs = time.milliseconds(),
        )
        val client = MockClient(time, metadata)
        val producer = kafkaProducer(
            configs = configs,
            keySerializer = StringSerializer(),
            valueSerializer = StringSerializer(),
            metadata = metadata,
            kafkaClient = client,
            interceptors = null,
            time = time
        )
        producer.close()
        assertFailsWith<IllegalStateException> { producer.initTransactions() }
    }

    @Test
    @Throws(InterruptedException::class)
    fun testCloseIsForcedOnPendingFindCoordinator() {
        val configs = mapOf(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to "localhost:9000",
            ProducerConfig.TRANSACTIONAL_ID_CONFIG to "this-is-a-transactional-id",
        )
        val time = MockTime()
        val initialUpdateResponse = metadataUpdateWith(
            numNodes = 1,
            topicPartitionCounts = mapOf("testTopic" to 1),
        )
        val metadata = newMetadata(refreshBackoffMs = 0, expirationMs = Long.MAX_VALUE)
        metadata.updateWithCurrentRequestVersion(
            response = initialUpdateResponse,
            isPartialUpdate = false,
            nowMs = time.milliseconds(),
        )
        val client = MockClient(time, metadata)
        val producer = kafkaProducer(
            configs = configs,
            keySerializer = StringSerializer(),
            valueSerializer = StringSerializer(),
            metadata = metadata,
            kafkaClient = client,
            interceptors = null,
            time = time,
        )
        val executorService = Executors.newSingleThreadExecutor()
        val assertionDoneLatch = CountDownLatch(1)
        executorService.submit {
            assertFailsWith<KafkaException> { producer.initTransactions() }
            assertionDoneLatch.countDown()
        }
        client.waitForRequests(1, 2000)
        producer.close(Duration.ofMillis(1000))
        assertionDoneLatch.await(5000, TimeUnit.MILLISECONDS)
    }

    @Test
    @Throws(InterruptedException::class)
    fun testCloseIsForcedOnPendingInitProducerId() {
        val configs = mapOf(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to "localhost:9000",
            ProducerConfig.TRANSACTIONAL_ID_CONFIG to "this-is-a-transactional-id",
        )
        val time = MockTime()
        val initialUpdateResponse = metadataUpdateWith(
            numNodes = 1,
            topicPartitionCounts = mapOf("testTopic" to 1),
        )
        val metadata = newMetadata(refreshBackoffMs = 0, expirationMs = Long.MAX_VALUE)
        metadata.updateWithCurrentRequestVersion(
            response = initialUpdateResponse,
            isPartialUpdate = false,
            nowMs = time.milliseconds(),
        )
        val client = MockClient(time, metadata)
        val producer = kafkaProducer(
            configs = configs,
            keySerializer = StringSerializer(),
            valueSerializer = StringSerializer(),
            metadata = metadata,
            kafkaClient = client,
            interceptors = null,
            time = time,
        )
        val executorService = Executors.newSingleThreadExecutor()
        val assertionDoneLatch = CountDownLatch(1)
        client.prepareResponse(
            FindCoordinatorResponse.prepareResponse(
                error = Errors.NONE,
                key = "this-is-a-transactional-id",
                node = NODE,
            )
        )
        executorService.submit {
            assertFailsWith<KafkaException> { producer.initTransactions() }
            assertionDoneLatch.countDown()
        }
        client.waitForRequests(1, 2000)
        producer.close(Duration.ofMillis(1000))
        assertionDoneLatch.await(5000, TimeUnit.MILLISECONDS)
    }

    @Test
    @Throws(InterruptedException::class)
    fun testCloseIsForcedOnPendingAddOffsetRequest() {
        val configs = mapOf(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to "localhost:9000",
            ProducerConfig.TRANSACTIONAL_ID_CONFIG to "this-is-a-transactional-id",
        )
        val time = MockTime()
        val initialUpdateResponse = metadataUpdateWith(
            numNodes = 1,
            topicPartitionCounts = mapOf("testTopic" to 1),
        )
        val metadata = newMetadata(refreshBackoffMs = 0, expirationMs = Long.MAX_VALUE)
        metadata.updateWithCurrentRequestVersion(
            response = initialUpdateResponse,
            isPartialUpdate = false,
            nowMs = time.milliseconds(),
        )
        val client = MockClient(time, metadata)
        val producer = kafkaProducer(
            configs = configs,
            keySerializer = StringSerializer(),
            valueSerializer = StringSerializer(),
            metadata = metadata,
            kafkaClient = client,
            interceptors = null,
            time = time,
        )
        val executorService = Executors.newSingleThreadExecutor()
        val assertionDoneLatch = CountDownLatch(1)
        client.prepareResponse(
            FindCoordinatorResponse.prepareResponse(
                error = Errors.NONE,
                key = "this-is-a-transactional-id",
                node = NODE,
            )
        )
        executorService.submit {
            assertFailsWith<KafkaException> { producer.initTransactions() }
            assertionDoneLatch.countDown()
        }
        client.waitForRequests(1, 2000)
        producer.close(Duration.ofMillis(1000))
        assertionDoneLatch.await(5000, TimeUnit.MILLISECONDS)
    }

    @Test
    @Throws(Exception::class)
    fun testProducerJmxPrefix() {
        val props = mapOf(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to "localhost:9999",
            "client.id" to "client-1",
        )
        val producer = KafkaProducer(
            configs = props,
            keySerializer = StringSerializer(),
            valueSerializer = StringSerializer(),
        )
        val server = ManagementFactory.getPlatformMBeanServer()
        val testMetricName = producer.metrics.metricName(
            name = "test-metric",
            group = "grp1",
            description = "test metric",
        )
        producer.metrics.addMetric(
            metricName = testMetricName,
            metricValueProvider = Avg(),
        )
        assertNotNull(server.getObjectInstance(ObjectName("kafka.producer:type=grp1,client-id=client-1")))
        producer.close()
    }

    @Test
    fun configurableObjectsShouldSeeGeneratedClientId() {
        val props = Properties().apply {
            put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999")
            put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, SerializerForClientId::class.java.getName())
            put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, SerializerForClientId::class.java.getName())
            put(ProducerConfig.PARTITIONER_CLASS_CONFIG, PartitionerForClientId::class.java.getName())
            put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, ProducerInterceptorForClientId::class.java.getName())
        }
        val producer = KafkaProducer<ByteArray?, ByteArray?>(props)
        assertNotNull(producer.clientId)
        assertNotEquals(0, producer.clientId.length)
        assertEquals(4, CLIENT_IDS.size)
        CLIENT_IDS.forEach { id -> assertEquals(id, producer.clientId) }
        producer.close()
    }

    @Test
    fun testUnusedConfigs() {
        val props = mapOf(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to "localhost:9999",
            SslConfigs.SSL_PROTOCOL_CONFIG to "TLS",
        )
        val config = ProducerConfig(
            ProducerConfig.appendSerializerToConfig(
                configs = props,
                keySerializer = StringSerializer(),
                valueSerializer = StringSerializer(),
            )
        )
        assertTrue(config.unused().contains(SslConfigs.SSL_PROTOCOL_CONFIG))
        KafkaProducer<ByteArray, ByteArray>(
            config = config,
            keySerializer = null,
            valueSerializer = null,
            metadata = null,
            kafkaClient = null,
            interceptors = null,
            time = Time.SYSTEM,
        ).use { assertTrue(config.unused().contains(SslConfigs.SSL_PROTOCOL_CONFIG)) }
    }

    @Test
    @Disabled("Kotlin Migration: Topic is not nullable anymore")
    fun testNullTopicName() {
        // send a record with null topic should fail
//        assertFailsWith<IllegalArgumentException> {
//                ProducerRecord(
//                    topic = null,
//                    partition = 1,
//                    key = "key".toByteArray(StandardCharsets.UTF_8),
//                    value = "value".toByteArray(StandardCharsets.UTF_8),
//                )
//            }
    }

    @Test
    fun testCallbackAndInterceptorHandleError() {
        val configs = mapOf(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to "localhost:9000",
            ProducerConfig.MAX_BLOCK_MS_CONFIG to "1000",
            ProducerConfig.INTERCEPTOR_CLASSES_CONFIG to MockProducerInterceptor::class.java.getName(),
            MockProducerInterceptor.APPEND_STRING_PROP to "something",
        )
        val time = MockTime()
        val producerMetadata = newMetadata(refreshBackoffMs = 0, expirationMs = Long.MAX_VALUE)
        val client = MockClient(time, producerMetadata)
        val invalidTopicName = "topic abc" // Invalid topic name due to space
        val producerInterceptors = ProducerInterceptors(listOf(MockProducerInterceptor()))
        kafkaProducer(
            configs = configs,
            keySerializer = StringSerializer(),
            valueSerializer = StringSerializer(),
            metadata = producerMetadata,
            kafkaClient = client,
            interceptors = producerInterceptors,
            time = time,
        ).use { producer ->
            val record = ProducerRecord<String, String>(topic = invalidTopicName, value = "HelloKafka")

            // Here's the important piece of the test. Let's make sure that the RecordMetadata we get
            // is non-null and adheres to the onCompletion contract.
            val callBack = object : Callback {
                override fun onCompletion(metadata: RecordMetadata?, exception: Exception?) {
                    assertNotNull(exception)
                    assertNotNull(metadata)
                    assertNotNull(metadata.topic, "Topic name should be valid even on send failure")
                    assertEquals(invalidTopicName, metadata.topic)
                    assertNotNull(metadata.partition, "Partition should be valid even on send failure")
                    assertFalse(metadata.hasOffset())
                    assertEquals(ProduceResponse.INVALID_OFFSET, metadata.offset)
                    assertFalse(metadata.hasTimestamp())
                    assertEquals(RecordBatch.NO_TIMESTAMP, metadata.timestamp)
                    assertEquals(-1, metadata.serializedKeySize)
                    assertEquals(-1, metadata.serializedValueSize)
                    assertEquals(-1, metadata.partition)
                }
            }
            producer.send(record, callBack)
            assertEquals(1, MockProducerInterceptor.ON_ACKNOWLEDGEMENT_COUNT.toInt())
        }
    }

    @Test
    fun negativePartitionShouldThrow() {
        val configs = mapOf(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to "localhost:9000",
            ProducerConfig.PARTITIONER_CLASS_CONFIG to BuggyPartitioner::class.java.getName(),
        )
        val time = MockTime(1)
        val initialUpdateResponse = metadataUpdateWith(
            numNodes = 1,
            topicPartitionCounts = mapOf("topic" to 1),
        )
        val metadata = newMetadata(refreshBackoffMs = 0, expirationMs = Long.MAX_VALUE)
        val client = MockClient(time, metadata)
        client.updateMetadata(initialUpdateResponse)
        kafkaProducer(
            configs = configs,
            keySerializer = StringSerializer(),
            valueSerializer = StringSerializer(),
            metadata = metadata,
            kafkaClient = client,
            interceptors = null,
            time = time,
        ).use { producer ->
            assertFailsWith<IllegalArgumentException> {
                producer.send(ProducerRecord(topic = "topic", key = "key", value = "value"))
            }
        }
    }

    @Test
    @Throws(Exception::class)
    fun testPartitionAddedToTransaction() {
        val serializer = StringSerializer()
        val ctx = KafkaProducerTestContext(testInfo, serializer)
        val topic = "foo"
        val topicPartition = TopicPartition(topic, 0)
        val cluster = singletonCluster(topic, 1)
        `when`(ctx.sender.isRunning).thenReturn(true)
        `when`(ctx.metadata.fetch()).thenReturn(cluster)
        val timestamp = ctx.time.milliseconds()
        val record = ProducerRecord(
            topic = topic,
            partition = null,
            timestamp = timestamp,
            key = "key",
            value = "value",
        )
        val future = expectAppend(ctx, record, topicPartition, cluster)
        ctx.newKafkaProducer().use { producer ->
            assertEquals(future, producer.send(record))
            assertFalse(future.isDone())
            verify(ctx.transactionManager).maybeAddPartition(topicPartition)
        }
    }

    @Suppress("Deprecation")
    @Test
    @Throws(Exception::class)
    fun testPartitionAddedToTransactionAfterFullBatchRetry() {
        val serializer = StringSerializer()
        val ctx = KafkaProducerTestContext(testInfo, serializer)
        val topic = "foo"
        val topicPartition0 = TopicPartition(topic, 0)
        val topicPartition1 = TopicPartition(topic, 1)
        val cluster = singletonCluster(topic, 2)
        `when`(ctx.sender.isRunning).thenReturn(true)
        `when`(ctx.metadata.fetch()).thenReturn(cluster)
        val timestamp = ctx.time.milliseconds()
        val record = ProducerRecord(
            topic = topic,
            partition = null,
            timestamp = timestamp,
            key = "key",
            value = "value"
        )
        val future = expectAppendWithAbortForNewBatch(
            ctx = ctx,
            record = record,
            initialSelectedPartition = topicPartition0,
            retrySelectedPartition = topicPartition1,
            cluster = cluster
        )
        ctx.newKafkaProducer().use { producer ->
            assertEquals(future, producer.send(record))
            assertFalse(future.isDone())
            verify(ctx.partitioner).onNewBatch(topic, cluster, 0)
            verify(ctx.transactionManager, never())
                .maybeAddPartition(topicPartition0)
            verify(ctx.transactionManager).maybeAddPartition(topicPartition1)
        }
    }

    @Throws(InterruptedException::class)
    private fun <T> expectAppend(
        ctx: KafkaProducerTestContext<T>,
        record: ProducerRecord<T, T>,
        initialSelectedPartition: TopicPartition,
        cluster: Cluster,
    ): FutureRecordMetadata {
        val serializedKey = ctx.serializer.serialize(topic, record.key)
        val serializedValue = ctx.serializer.serialize(topic, record.value)
        val timestamp = record.timestamp ?: ctx.time.milliseconds()
        val requestResult = ProduceRequestResult(initialSelectedPartition)
        val futureRecordMetadata = FutureRecordMetadata(
            result = requestResult,
            batchIndex = 5,
            createTimestamp = timestamp,
            serializedKeySize = serializedKey!!.size,
            serializedValueSize = serializedValue!!.size,
            time = ctx.time,
        )
        `when`(
            ctx.partitioner.partition(
                topic = initialSelectedPartition.topic,
                key = record.key,
                keyBytes = serializedKey,
                value = record.value,
                valueBytes = serializedValue,
                cluster = cluster
            )
        ).thenReturn(initialSelectedPartition.partition)
        `when`(
            ctx.accumulator.append(
                eq(initialSelectedPartition.topic),  // 0
                eq(initialSelectedPartition.partition),  // 1
                eq(timestamp),  // 2
                eq(serializedKey),  // 3
                eq(serializedValue),  // 4
                eq(Record.EMPTY_HEADERS),  // 5
                any<RecordAccumulator.AppendCallbacks>(),  // 6 <--
                any(),
                eq(true),
                any(),
                any(),
            )
        ).thenAnswer { invocation ->
            val callbacks = invocation.arguments[6] as RecordAccumulator.AppendCallbacks
            callbacks.setPartition(initialSelectedPartition.partition)
            RecordAppendResult(
                future = futureRecordMetadata,
                batchIsFull = false,
                newBatchCreated = false,
                abortForNewBatch = false,
                appendedBytes = 0,
            )
        }
        return futureRecordMetadata
    }

    @Throws(InterruptedException::class)
    private fun <T> expectAppendWithAbortForNewBatch(
        ctx: KafkaProducerTestContext<T>,
        record: ProducerRecord<T, T>,
        initialSelectedPartition: TopicPartition,
        retrySelectedPartition: TopicPartition,
        cluster: Cluster,
    ): FutureRecordMetadata {
        val serializedKey = ctx.serializer.serialize(topic, record.key)
        val serializedValue = ctx.serializer.serialize(topic, record.value)
        val timestamp = record.timestamp ?: ctx.time.milliseconds()
        val requestResult = ProduceRequestResult(retrySelectedPartition)
        val futureRecordMetadata = FutureRecordMetadata(
            result = requestResult,
            batchIndex = 0,
            createTimestamp = timestamp,
            serializedKeySize = serializedKey!!.size,
            serializedValueSize = serializedValue!!.size,
            time = ctx.time,
        )
        `when`(
            ctx.partitioner.partition(
                topic = initialSelectedPartition.topic,
                key = record.key,
                keyBytes = serializedKey,
                value = record.value,
                valueBytes = serializedValue,
                cluster = cluster,
            )
        ).thenReturn(initialSelectedPartition.partition)
            .thenReturn(retrySelectedPartition.partition)
        `when`(
            ctx.accumulator.append(
                eq(initialSelectedPartition.topic),  // 0
                eq(initialSelectedPartition.partition),  // 1
                eq(timestamp),  // 2
                eq(serializedKey),  // 3
                eq(serializedValue),  // 4
                eq(Record.EMPTY_HEADERS),  // 5
                any<RecordAccumulator.AppendCallbacks>(),  // 6 <--
                any(),
                eq(true),  // abortOnNewBatch
                any(),
                any(),
            )
        ).thenAnswer { invocation ->
            val callbacks = invocation.arguments[6] as RecordAccumulator.AppendCallbacks
            callbacks.setPartition(initialSelectedPartition.partition)
            RecordAppendResult(
                future = null,
                batchIsFull = false,
                newBatchCreated = false,
                abortForNewBatch = true,
                appendedBytes = 0
            )
        }
        `when`(
            ctx.accumulator.append(
                eq(retrySelectedPartition.topic),  // 0
                eq(retrySelectedPartition.partition),  // 1
                eq(timestamp),  // 2
                eq(serializedKey),  // 3
                eq(serializedValue),  // 4
                eq(Record.EMPTY_HEADERS),  // 5
                any<RecordAccumulator.AppendCallbacks>(),  // 6 <--
                any(),
                eq(false),  // abortOnNewBatch
                any(),
                any(),
            )
        ).thenAnswer { invocation ->
            val callbacks = invocation.arguments[6] as RecordAccumulator.AppendCallbacks
            callbacks.setPartition(retrySelectedPartition.partition)
            RecordAppendResult(
                future = futureRecordMetadata,
                batchIsFull = false,
                newBatchCreated = true,
                abortForNewBatch = false,
                appendedBytes = 0,
            )
        }
        return futureRecordMetadata
    }

    class SerializerForClientId : Serializer<ByteArray?> {
        override fun configure(configs: Map<String, *>, isKey: Boolean) {
            CLIENT_IDS.add(configs[ProducerConfig.CLIENT_ID_CONFIG].toString())
        }

        override fun serialize(topic: String, data: ByteArray?): ByteArray? = data
    }

    class PartitionerForClientId : Partitioner {

        override fun partition(
            topic: String,
            key: Any?,
            keyBytes: ByteArray?,
            value: Any?,
            valueBytes: ByteArray?,
            cluster: Cluster,
        ): Int {
            return 0
        }

        override fun close() = Unit

        override fun configure(configs: Map<String, Any?>) {
            CLIENT_IDS.add(configs[ProducerConfig.CLIENT_ID_CONFIG].toString())
        }
    }

    class ProducerInterceptorForClientId : ProducerInterceptor<ByteArray?, ByteArray?> {

        override fun onSend(
            record: ProducerRecord<ByteArray?, ByteArray?>,
        ): ProducerRecord<ByteArray?, ByteArray?> = record

        override fun onAcknowledgement(metadata: RecordMetadata?, exception: Exception?) = Unit

        override fun close() = Unit

        override fun configure(configs: Map<String, Any?>) {
            CLIENT_IDS.add(configs[ProducerConfig.CLIENT_ID_CONFIG].toString())
        }
    }

    class BuggyPartitioner : Partitioner {

        override fun partition(
            topic: String,
            key: Any?,
            keyBytes: ByteArray?,
            value: Any?,
            valueBytes: ByteArray?,
            cluster: Cluster,
        ): Int = -1

        override fun close() = Unit

        override fun configure(configs: Map<String, Any?>) = Unit
    }

    private class KafkaProducerTestContext<T>(
        private val testInfo: TestInfo?,
        configs: MutableMap<String, Any?>,
        serializer: Serializer<T>,
    ) {

        val configs: Map<String, Any?>

        val serializer: Serializer<T>

        var metadata: ProducerMetadata = mock()

        var accumulator: RecordAccumulator = mock()

        var sender: Sender = mock()

        var transactionManager: TransactionManager = mock()

        val partitioner: Partitioner = mock()

        val ioThread: KafkaThread = mock()

        var time: Time = MockTime()

        val metrics = Metrics(time = time)

        val interceptors: MutableList<ProducerInterceptor<T, T>> = ArrayList()

        constructor(
            testInfo: TestInfo?,
            serializer: Serializer<T>,
        ) : this(testInfo, HashMap<String, Any?>(), serializer)

        init {
            this.configs = configs
            this.serializer = serializer
            if (!configs.containsKey(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG)) {
                configs[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = "localhost:9999"
            }
        }

        fun setProducerMetadata(metadata: ProducerMetadata): KafkaProducerTestContext<T> {
            this.metadata = metadata
            return this
        }

        fun setAccumulator(accumulator: RecordAccumulator): KafkaProducerTestContext<T> {
            this.accumulator = accumulator
            return this
        }

        fun setSender(sender: Sender): KafkaProducerTestContext<T> {
            this.sender = sender
            return this
        }

        fun setTransactionManager(transactionManager: TransactionManager): KafkaProducerTestContext<T> {
            this.transactionManager = transactionManager
            return this
        }

        fun addInterceptor(interceptor: ProducerInterceptor<T, T>): KafkaProducerTestContext<T> {
            interceptors.add(interceptor)
            return this
        }

        fun setTime(time: Time): KafkaProducerTestContext<T> {
            this.time = time
            return this
        }

        fun newKafkaProducer(): KafkaProducer<T, T> {
            val logContext = LogContext("[Producer test=" + testInfo!!.displayName + "] ")
            val producerConfig = ProducerConfig(
                ProducerConfig.appendSerializerToConfig(configs, serializer, serializer)
            )
            val interceptors = ProducerInterceptors(interceptors)

            return KafkaProducer(
                config = producerConfig,
                logContext = logContext,
                metrics = metrics,
                keySerializer = serializer,
                valueSerializer = serializer,
                metadata = metadata,
                accumulator = accumulator,
                transactionManager = transactionManager,
                sender = sender,
                interceptors = interceptors,
                partitioner = partitioner,
                time = time,
                ioThread = ioThread
            )
        }
    }

    companion object {

        private const val DEFAULT_METADATA_IDLE_MS = 5 * 60 * 1000

        private val NODE = Node(0, "host1", 1000)

        private fun <K, V> kafkaProducer(
            configs: Map<String, Any?>,
            keySerializer: Serializer<K>,
            valueSerializer: Serializer<V>,
            metadata: ProducerMetadata,
            kafkaClient: KafkaClient?,
            interceptors: ProducerInterceptors<K, V>?,
            time: Time,
        ): KafkaProducer<K, V> {
            return KafkaProducer(
                config = ProducerConfig(
                    ProducerConfig.appendSerializerToConfig(
                        configs = configs,
                        keySerializer = keySerializer,
                        valueSerializer = valueSerializer,
                    )
                ),
                keySerializer = keySerializer,
                valueSerializer = valueSerializer,
                metadata = metadata,
                kafkaClient = kafkaClient,
                interceptors = interceptors,
                time = time,
            )
        }

        private fun producerWithOverrideNewSender(
            configs: Map<String, Any?>,
            metadata: ProducerMetadata,
            time: Time = Time.SYSTEM,
        ): KafkaProducer<String, String> {
            // let mockClient#leastLoadedNode return the node directly so that we can isolate Metadata calls from KafkaProducer for idempotent producer
            val mockClient = object : MockClient(Time.SYSTEM, metadata) {
                override fun leastLoadedNode(now: Long): Node = NODE
            }

            return object : KafkaProducer<String, String>(
                config = ProducerConfig(
                    ProducerConfig.appendSerializerToConfig(
                        configs = configs,
                        keySerializer = StringSerializer(),
                        valueSerializer = StringSerializer(),
                    )
                ),
                keySerializer = StringSerializer(),
                valueSerializer = StringSerializer(),
                metadata = metadata,
                kafkaClient = mockClient,
                interceptors = null,
                time = time,
            ) {
                override fun newSender(
                    logContext: LogContext,
                    kafkaClient: KafkaClient?,
                    metadata: ProducerMetadata,
                ): Sender {
                    // give Sender its own Metadata instance so that we can isolate Metadata calls from KafkaProducer
                    return super.newSender(
                        logContext = logContext,
                        kafkaClient = kafkaClient,
                        metadata = newMetadata(refreshBackoffMs = 0, expirationMs = 100000),
                    )
                }
            }
        }

        private fun getMetricValue(producer: KafkaProducer<*, *>, name: String): Double {
            val metrics = producer.metrics
            val metric = metrics.metric(metrics.metricName(name, "producer-metrics"))!!

            return metric.metricValue() as Double
        }

        private fun newMetadata(refreshBackoffMs: Long, expirationMs: Long): ProducerMetadata {
            return ProducerMetadata(
                refreshBackoffMs = refreshBackoffMs,
                metadataExpireMs = expirationMs,
                metadataIdleMs = DEFAULT_METADATA_IDLE_MS.toLong(),
                logContext = LogContext(),
                clusterResourceListeners = ClusterResourceListeners(),
                time = Time.SYSTEM,
            )
        }

        private val CLIENT_IDS: MutableList<String> = ArrayList()
    }
}
