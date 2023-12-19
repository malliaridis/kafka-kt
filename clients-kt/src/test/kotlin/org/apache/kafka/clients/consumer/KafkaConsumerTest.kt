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

import java.lang.management.ManagementFactory
import java.nio.ByteBuffer
import java.time.Duration
import java.util.Properties
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference
import java.util.regex.Pattern
import javax.management.ObjectName
import org.apache.kafka.clients.ApiVersions
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.GroupRebalanceConfig
import org.apache.kafka.clients.KafkaClient
import org.apache.kafka.clients.MockClient
import org.apache.kafka.clients.NodeApiVersions
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignorTest.TestConsumerPartitionAssignor
import org.apache.kafka.clients.consumer.internals.ConsumerCoordinator
import org.apache.kafka.clients.consumer.internals.ConsumerInterceptors
import org.apache.kafka.clients.consumer.internals.ConsumerMetadata
import org.apache.kafka.clients.consumer.internals.ConsumerMetrics
import org.apache.kafka.clients.consumer.internals.ConsumerNetworkClient
import org.apache.kafka.clients.consumer.internals.ConsumerProtocol.deserializeSubscription
import org.apache.kafka.clients.consumer.internals.ConsumerProtocol.serializeAssignment
import org.apache.kafka.clients.consumer.internals.ConsumerProtocol.serializeSubscription
import org.apache.kafka.clients.consumer.internals.FetchConfig
import org.apache.kafka.clients.consumer.internals.FetchMetricsManager
import org.apache.kafka.clients.consumer.internals.Fetcher
import org.apache.kafka.clients.consumer.internals.MockRebalanceListener
import org.apache.kafka.clients.consumer.internals.OffsetFetcher
import org.apache.kafka.clients.consumer.internals.SubscriptionState
import org.apache.kafka.clients.consumer.internals.TopicMetadataFetcher
import org.apache.kafka.common.IsolationLevel
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.MetricName
import org.apache.kafka.common.Node
import org.apache.kafka.common.TopicIdPartition
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.Uuid
import org.apache.kafka.common.config.SslConfigs
import org.apache.kafka.common.errors.AuthenticationException
import org.apache.kafka.common.errors.InterruptException
import org.apache.kafka.common.errors.InvalidConfigurationException
import org.apache.kafka.common.errors.InvalidGroupIdException
import org.apache.kafka.common.errors.InvalidTopicException
import org.apache.kafka.common.errors.RecordDeserializationException
import org.apache.kafka.common.errors.SerializationException
import org.apache.kafka.common.errors.UnsupportedVersionException
import org.apache.kafka.common.errors.WakeupException
import org.apache.kafka.common.header.Headers
import org.apache.kafka.common.internals.ClusterResourceListeners
import org.apache.kafka.common.message.FetchResponseData
import org.apache.kafka.common.message.HeartbeatResponseData
import org.apache.kafka.common.message.JoinGroupResponseData
import org.apache.kafka.common.message.JoinGroupResponseData.JoinGroupResponseMember
import org.apache.kafka.common.message.LeaveGroupResponseData
import org.apache.kafka.common.message.ListOffsetsRequestData.ListOffsetsPartition
import org.apache.kafka.common.message.ListOffsetsResponseData
import org.apache.kafka.common.message.ListOffsetsResponseData.ListOffsetsPartitionResponse
import org.apache.kafka.common.message.ListOffsetsResponseData.ListOffsetsTopicResponse
import org.apache.kafka.common.message.SyncGroupResponseData
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.metrics.Sensor
import org.apache.kafka.common.metrics.stats.Avg
import org.apache.kafka.common.network.Selectable
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.record.CompressionType
import org.apache.kafka.common.record.MemoryRecords
import org.apache.kafka.common.record.MemoryRecordsBuilder
import org.apache.kafka.common.record.TimestampType
import org.apache.kafka.common.requests.AbstractResponse
import org.apache.kafka.common.requests.FetchMetadata
import org.apache.kafka.common.requests.FetchRequest
import org.apache.kafka.common.requests.FetchResponse
import org.apache.kafka.common.requests.FindCoordinatorResponse.Companion.prepareResponse
import org.apache.kafka.common.requests.HeartbeatResponse
import org.apache.kafka.common.requests.JoinGroupRequest
import org.apache.kafka.common.requests.JoinGroupResponse
import org.apache.kafka.common.requests.LeaveGroupResponse
import org.apache.kafka.common.requests.ListOffsetsRequest
import org.apache.kafka.common.requests.ListOffsetsResponse
import org.apache.kafka.common.requests.MetadataResponse
import org.apache.kafka.common.requests.OffsetCommitRequest
import org.apache.kafka.common.requests.OffsetCommitResponse
import org.apache.kafka.common.requests.OffsetFetchResponse
import org.apache.kafka.common.requests.RequestTestUtils.metadataResponse
import org.apache.kafka.common.requests.RequestTestUtils.metadataUpdateWith
import org.apache.kafka.common.requests.RequestTestUtils.metadataUpdateWithIds
import org.apache.kafka.common.requests.SyncGroupResponse
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.utils.LogContext
import org.apache.kafka.common.utils.MockTime
import org.apache.kafka.common.utils.Time
import org.apache.kafka.common.utils.Utils
import org.apache.kafka.test.MockConsumerInterceptor
import org.apache.kafka.test.MockMetricsReporter
import org.apache.kafka.test.TestUtils
import org.apache.kafka.test.TestUtils.singletonCluster
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import org.mockito.kotlin.spy
import org.mockito.kotlin.verify
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertNotEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNotSame
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * Note to future authors in this class. If you close the consumer, close with DURATION.ZERO to reduce
 * the duration of the test.
 */
class KafkaConsumerTest {

    private val topic = "test"

    private val topicId = Uuid.randomUuid()

    private val tp0 = TopicPartition(topic, 0)

    private val tp1 = TopicPartition(topic, 1)

    private val topic2 = "test2"

    private val topicId2 = Uuid.randomUuid()

    private val t2p0 = TopicPartition(topic2, 0)

    private val topic3 = "test3"

    private val topicId3 = Uuid.randomUuid()

    private val t3p0 = TopicPartition(topic3, 0)

    private val sessionTimeoutMs = 10000

    private val defaultApiTimeoutMs = 60000

    private val requestTimeoutMs = defaultApiTimeoutMs / 2

    private val heartbeatIntervalMs = 1000

    // Set auto commit interval lower than heartbeat so we don't need to deal with
    // a concurrent heartbeat request
    private val autoCommitIntervalMs = 500

    private val throttleMs = 10

    private val groupId = "mock-group"

    private val memberId = "memberId"

    private val leaderId = "leaderId"

    private val groupInstanceId = "mock-instance"

    private val topicIds = mutableMapOf(
        topic to topicId,
        topic2 to topicId2,
        topic3 to topicId3,
    )

    private val topicNames = mapOf(
        topicId to topic,
        topicId2 to topic2,
        topicId3 to topic3,
    )

    private val partitionRevoked = "Hit partition revoke "

    private val partitionAssigned = "Hit partition assign "

    private val partitionLost = "Hit partition lost "

    private val singleTopicPartition: Collection<TopicPartition> = setOf(TopicPartition(topic, 0))

    private val time: Time = MockTime()

    private val subscription = SubscriptionState(LogContext(), OffsetResetStrategy.EARLIEST)

    private val assignor: ConsumerPartitionAssignor = RoundRobinAssignor()

    private lateinit var consumer: KafkaConsumer<*, *>

    @AfterEach
    fun cleanup() {
        if (::consumer.isInitialized) consumer.close(Duration.ZERO)
    }

    @Test
    fun testMetricsReporterAutoGeneratedClientId() {
        val props = Properties().apply {
            setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999")
            setProperty(ConsumerConfig.METRIC_REPORTER_CLASSES_CONFIG, MockMetricsReporter::class.java.name)
        }
        consumer = KafkaConsumer(props, StringDeserializer(), StringDeserializer())
        
        val mockMetricsReporter = consumer.metrics.reporters[0] as MockMetricsReporter
        
        assertEquals(consumer.getClientId(), mockMetricsReporter.clientId)
        assertEquals(2, consumer.metrics.reporters.size)
    }

    @Test
    @Suppress("Deprecation")
    fun testDisableJmxReporter() {
        val props = Properties().apply {
            setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999")
            setProperty(ConsumerConfig.AUTO_INCLUDE_JMX_REPORTER_CONFIG, "false")
        }
        consumer = KafkaConsumer(props, StringDeserializer(), StringDeserializer())
        assertTrue(consumer.metrics.reporters.isEmpty())
    }

    @Test
    fun testExplicitlyEnableJmxReporter() {
        val props = Properties().apply {
            setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999")
            setProperty(ConsumerConfig.METRIC_REPORTER_CLASSES_CONFIG, "org.apache.kafka.common.metrics.JmxReporter")
        }
        consumer = KafkaConsumer(props, StringDeserializer(), StringDeserializer())
        assertEquals(1, consumer.metrics.reporters.size)
    }

    @Test
    fun testPollReturnsRecords() {
        consumer = setUpConsumerWithRecordsToPoll(tp0, 5)
        val records = consumer.poll(Duration.ZERO)
        
        assertEquals(5, records.count())
        assertEquals(setOf(tp0), records.partitions())
        assertEquals(5, records.records(tp0).size)
    }

    @Test
    fun testSecondPollWithDeserializationErrorThrowsRecordDeserializationException() {
        val invalidRecordNumber = 4
        val invalidRecordOffset = 3
        val deserializer = mockErrorDeserializer(invalidRecordNumber)
        consumer = setUpConsumerWithRecordsToPoll(
            tp = tp0,
            recordCount = 5,
            deserializer = deserializer,
        )
        val records = consumer.poll(Duration.ZERO)
        assertEquals(invalidRecordNumber - 1, records.count())
        assertEquals(setOf(tp0), records.partitions())
        assertEquals(invalidRecordNumber - 1, records.records(tp0).size)
        val lastOffset = records.records(tp0)[records.records(tp0).size - 1].offset
        assertEquals((invalidRecordNumber - 2).toLong(), lastOffset)
        val rde = assertFailsWith<RecordDeserializationException> { consumer.poll(Duration.ZERO) }
        assertEquals(invalidRecordOffset.toLong(), rde.offset)
        assertEquals(tp0, rde.partition)
        assertEquals(rde.offset, consumer.position(tp0))
    }

    /**
     * Create a mock deserializer which throws a SerializationException on the Nth record's value deserialization
     */
    private fun mockErrorDeserializer(recordNumber: Int): Deserializer<String?> {

        val recordIndex = recordNumber - 1

        return object : Deserializer<String?> {

            private val actualDeserializer = StringDeserializer()

            var i = 0

            override fun deserialize(topic: String, data: ByteArray?): String? {
                return if (i == recordIndex) throw SerializationException()
                else {
                    i++
                    actualDeserializer.deserialize(topic, data)
                }
            }

            override fun deserialize(topic: String, headers: Headers, data: ByteBuffer?): String? {
                return if (i == recordIndex) throw SerializationException()
                else {
                    i++
                    actualDeserializer.deserialize(topic, headers, data)
                }
            }
        }
    }

    private fun setUpConsumerWithRecordsToPoll(
        tp: TopicPartition,
        recordCount: Int,
        deserializer: Deserializer<String?> = StringDeserializer(),
    ): KafkaConsumer<*, *> {
        val cluster = singletonCluster(tp.topic, 1)
        val node = cluster.nodes[0]

        val metadata = createMetadata(subscription)
        val client = MockClient(time, metadata)
        initMetadata(client, mapOf(topic to 1))
        consumer = newConsumer(
            time = time,
            client = client,
            subscription = subscription,
            metadata = metadata,
            assignor = assignor,
            autoCommitEnabled = true,
            groupId = groupId,
            groupInstanceId = groupInstanceId,
            valueDeserializer = deserializer,
            throwOnStableOffsetNotSupported = false
        )
        consumer.subscribe(setOf(topic), getConsumerRebalanceListener(consumer))
        prepareRebalance(client, node, assignor, listOf(tp), null)
        consumer.updateAssignmentMetadataIfNeeded(time.timer(Long.MAX_VALUE))
        client.prepareResponseFrom(
            response = fetchResponse(
                partition = tp,
                fetchOffset = 0,
                count = recordCount,
            ),
            node = node,
        )
        return consumer
    }

    @Test
    fun testConstructorClose() {
        val props = Properties().apply {
            setProperty(ConsumerConfig.CLIENT_ID_CONFIG, "testConstructorClose")
            setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "invalid-23-8409-adsfsdj")
            setProperty(ConsumerConfig.METRIC_REPORTER_CLASSES_CONFIG, MockMetricsReporter::class.java.getName())
        }
        val oldInitCount = MockMetricsReporter.INIT_COUNT.get()
        val oldCloseCount = MockMetricsReporter.CLOSE_COUNT.get()
        val e = assertFailsWith<KafkaException>(
            message = "should have caught an exception and returned",
        ) { KafkaConsumer(props, ByteArrayDeserializer(), ByteArrayDeserializer()) }
        assertEquals(oldInitCount + 1, MockMetricsReporter.INIT_COUNT.get())
        assertEquals(oldCloseCount + 1, MockMetricsReporter.CLOSE_COUNT.get())
        assertEquals("Failed to construct kafka consumer", e.message)
    }

    @Test
    fun testOsDefaultSocketBufferSizes() {
        val config = mapOf(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to "localhost:9999",
            ConsumerConfig.SEND_BUFFER_CONFIG to Selectable.USE_DEFAULT_BUFFER_SIZE,
            ConsumerConfig.RECEIVE_BUFFER_CONFIG to Selectable.USE_DEFAULT_BUFFER_SIZE,
        )
        consumer = KafkaConsumer(
            configs = config,
            keyDeserializer = ByteArrayDeserializer(),
            valueDeserializer = ByteArrayDeserializer(),
        )
    }

    @Test
    fun testInvalidSocketSendBufferSize() {
        val config = mapOf(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to "localhost:9999",
            ConsumerConfig.SEND_BUFFER_CONFIG to -2,
        )
        assertFailsWith<KafkaException> {
            KafkaConsumer(
                configs = config,
                keyDeserializer = ByteArrayDeserializer(),
                valueDeserializer = ByteArrayDeserializer(),
            )
        }
    }

    @Test
    fun testInvalidSocketReceiveBufferSize() {
        val config = mapOf(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to "localhost:9999",
            ConsumerConfig.RECEIVE_BUFFER_CONFIG to -2,
        )
        assertFailsWith<KafkaException> {
            KafkaConsumer(
                configs = config,
                keyDeserializer = ByteArrayDeserializer(),
                valueDeserializer = ByteArrayDeserializer(),
            )
        }
    }

    @Test
    fun shouldIgnoreGroupInstanceIdForEmptyGroupId() {
        val config = mapOf(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to "localhost:9999",
            ConsumerConfig.GROUP_INSTANCE_ID_CONFIG to "instance_id",
        )
        consumer = KafkaConsumer(
            configs = config,
            keyDeserializer = ByteArrayDeserializer(),
            valueDeserializer = ByteArrayDeserializer(),
        )
    }

    @Test
    fun testSubscription() {
        consumer = newConsumer(groupId)
        consumer.subscribe(listOf(topic))
        assertEquals(setOf(topic), consumer.subscription())
        assertTrue(consumer.assignment().isEmpty())
        consumer.subscribe(emptyList())
        assertTrue(consumer.subscription().isEmpty())
        assertTrue(consumer.assignment().isEmpty())
        consumer.assign(listOf(tp0))
        assertTrue(consumer.subscription().isEmpty())
        assertEquals(setOf(tp0), consumer.assignment())
        consumer.unsubscribe()
        assertTrue(consumer.subscription().isEmpty())
        assertTrue(consumer.assignment().isEmpty())
    }

    @Test
    @Disabled("Kotlin Migration: Null values are not allowed in Kotlin")
    fun testSubscriptionOnNullTopicCollection() {
//        newConsumer(groupId).use { consumer ->
//            assertFailsWith<IllegalArgumentException> {
//                consumer.subscribe(null)
//            }
//        }
    }

    @Test
    @Disabled("Kotlin Migration: List with null values is not allowed in Kotlin")
    fun testSubscriptionOnNullTopic() {
//        newConsumer(groupId).use { consumer ->
//            assertFailsWith<IllegalArgumentException> {
//                consumer.subscribe(listOf<String?>(null))
//            }
//        }
    }

    @Test
    fun testSubscriptionOnEmptyTopic() {
        consumer = newConsumer(groupId)
        val emptyTopic = "  "
        assertFailsWith<IllegalArgumentException> {
            consumer.subscribe(listOf(emptyTopic))
        }
    }

    @Test
    @Disabled("Kotlin Migration: Nullable pattern is not allowed in Kotlin")
    fun testSubscriptionOnNullPattern() {
//        newConsumer(groupId).use { consumer ->
//            assertFailsWith<IllegalArgumentException> { consumer.subscribe(null as Pattern?) }
//        }
    }

    @Test
    fun testSubscriptionOnEmptyPattern() {
        consumer = newConsumer(groupId)
        assertFailsWith<IllegalArgumentException> {
            consumer.subscribe(Pattern.compile(""))
        }
    }

    @Test
    fun testSubscriptionWithEmptyPartitionAssignment() {
        val props = Properties().apply {
            setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999")
            setProperty(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, "")
            setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId)
        }
        consumer = newConsumer(props)
        assertFailsWith<IllegalStateException> {
            consumer.subscribe(listOf(topic))
        }
    }

    @Test
    fun testSeekNegative() {
        consumer = newConsumer(groupId = null)
        consumer.assign(
            setOf(TopicPartition(topic = "nonExistTopic", partition = 0))
        )
        assertFailsWith<IllegalArgumentException> {
            consumer.seek(
                partition = TopicPartition(topic = "nonExistTopic", partition = 0),
                offset = -1,
            )
        }
    }

    @Test
    @Disabled("Kotlin Migration: Assign does not accept nullable values in Kotlin")
    fun testAssignOnNullTopicPartition() {
//        newConsumer(groupId = null).use { consumer ->
//            assertFailsWith<IllegalArgumentException> { consumer.assign(null) }
//        }
    }

    @Test
    fun testAssignOnEmptyTopicPartition() {
        consumer = newConsumer(groupId)
        consumer.assign(emptyList())
        assertTrue(consumer.subscription().isEmpty())
        assertTrue(consumer.assignment().isEmpty())
    }

    @Test
    @Disabled("Kotlin Migration: Topic in TopicPartition is non-nullable field in Kotlin")
    fun testAssignOnNullTopicInPartition() {
//        newConsumer(groupId = null).use { consumer ->
//            assertFailsWith<IllegalArgumentException> {
//                consumer.assign(
//                    setOf(TopicPartition(topic = null, partition = 0))
//                )
//            }
//        }
    }

    @Test
    fun testAssignOnEmptyTopicInPartition() {
        consumer = newConsumer(groupId = null)
        assertFailsWith<IllegalArgumentException> {
            consumer.assign(setOf(TopicPartition(topic = "  ", partition = 0)))
        }
    }

    @Test
    fun testInterceptorConstructorClose() {
        try {
            val props = Properties().apply {
                setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999")
                setProperty(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, MockConsumerInterceptor::class.java.getName())
            }
            // test with client ID assigned by KafkaConsumer
            consumer = KafkaConsumer(
                properties = props,
                keyDeserializer = StringDeserializer(),
                valueDeserializer = StringDeserializer(),
            )
            assertEquals(1, MockConsumerInterceptor.INIT_COUNT.get())
            assertEquals(0, MockConsumerInterceptor.CLOSE_COUNT.get())
            
            consumer.close(Duration.ZERO)
            assertEquals(1, MockConsumerInterceptor.INIT_COUNT.get())
            assertEquals(1, MockConsumerInterceptor.CLOSE_COUNT.get())

            // Cluster metadata will only be updated on calling poll.
            assertNull(MockConsumerInterceptor.CLUSTER_META.get())
        } finally {
            // cleanup since we are using mutable static variables in MockConsumerInterceptor
            MockConsumerInterceptor.resetCounters()
        }
    }

    @Test
    fun testInterceptorConstructorConfigurationWithExceptionShouldCloseRemainingInstances() {
        val targetInterceptor = 3
        try {
            val props = Properties()
            props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999")
            props.setProperty(
                ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, listOf(
                    MockConsumerInterceptor::class.java.name,
                    MockConsumerInterceptor::class.java.name,
                    MockConsumerInterceptor::class.java.name,
                ).joinToString()
            )
            MockConsumerInterceptor.setThrowOnConfigExceptionThreshold(targetInterceptor)
            assertFailsWith<KafkaException> {
                KafkaConsumer(props, StringDeserializer(), StringDeserializer())
            }
            assertEquals(3, MockConsumerInterceptor.CONFIG_COUNT.get())
            assertEquals(3, MockConsumerInterceptor.CLOSE_COUNT.get())
        } finally {
            MockConsumerInterceptor.resetCounters()
        }
    }

    @Test
    fun testPause() {
        consumer = newConsumer(groupId)
        consumer.assign(listOf(tp0))
        assertEquals(setOf(tp0), consumer.assignment())
        assertTrue(consumer.paused().isEmpty())
        consumer.pause(setOf(tp0))
        assertEquals(setOf(tp0), consumer.paused())
        consumer.resume(setOf(tp0))
        assertTrue(consumer.paused().isEmpty())
        consumer.unsubscribe()
        assertTrue(consumer.paused().isEmpty())
    }

    @Test
    @Throws(Exception::class)
    fun testConsumerJmxPrefix() {
        val config = mapOf(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to "localhost:9999",
            ConsumerConfig.SEND_BUFFER_CONFIG to Selectable.USE_DEFAULT_BUFFER_SIZE,
            ConsumerConfig.RECEIVE_BUFFER_CONFIG to Selectable.USE_DEFAULT_BUFFER_SIZE,
            "client.id" to "client-1",
        )
        consumer = KafkaConsumer(
            configs = config,
            keyDeserializer = ByteArrayDeserializer(),
            valueDeserializer = ByteArrayDeserializer(),
        )
        val server = ManagementFactory.getPlatformMBeanServer()
        val testMetricName = consumer.metrics.metricName(
            name = "test-metric",
            group = "grp1",
            description = "test metric",
        )
        consumer.metrics.addMetric(metricName = testMetricName, metricValueProvider = Avg())
        assertNotNull(server.getObjectInstance(ObjectName("kafka.consumer:type=grp1,client-id=client-1")))
    }

    private fun newConsumer(
        groupId: String?,
        enableAutoCommit: Boolean? = null,
    ): KafkaConsumer<ByteArray?, ByteArray?> {
        val props = Properties().apply {
            setProperty(ConsumerConfig.CLIENT_ID_CONFIG, "my.consumer")
            setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999")
            setProperty(ConsumerConfig.METRIC_REPORTER_CLASSES_CONFIG, MockMetricsReporter::class.java.getName())

            if (groupId != null) setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId)
            enableAutoCommit?.let { autoCommit ->
                setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, autoCommit.toString())
            }
        }

        return newConsumer(props)
    }

    private fun newConsumer(props: Properties): KafkaConsumer<ByteArray?, ByteArray?> =
        KafkaConsumer(
            properties = props,
            keyDeserializer = ByteArrayDeserializer(),
            valueDeserializer = ByteArrayDeserializer(),
        )

    @Test
    @Throws(Exception::class)
    fun verifyHeartbeatSent() {
        val metadata = createMetadata(subscription)
        val client = MockClient(time, metadata)
        initMetadata(client, mapOf(topic to 1))
        val node = metadata.fetch().nodes[0]
        consumer = newConsumer(
            time = time,
            client = client,
            subscription = subscription,
            metadata = metadata,
            assignor = assignor,
            autoCommitEnabled = true,
            groupInstanceId = groupInstanceId,
        )
        consumer.subscribe(
            topics = setOf(topic),
            callback = getConsumerRebalanceListener(consumer),
        )
        val coordinator = prepareRebalance(
            client = client,
            node = node,
            assignor = assignor,
            partitions = listOf(tp0),
            coordinator = null,
        )

        // initial fetch
        client.prepareResponseFrom(
            response = fetchResponse(
                partition = tp0,
                fetchOffset = 0,
                count = 0,
            ),
            node = node,
        )
        consumer.updateAssignmentMetadataIfNeeded(time.timer(Long.MAX_VALUE))
        assertEquals(setOf(tp0), consumer.assignment())
        val heartbeatReceived = prepareHeartbeatResponse(
            client = client,
            coordinator = coordinator,
            error = Errors.NONE,
        )

        // heartbeat interval is 2 seconds
        time.sleep(heartbeatIntervalMs.toLong())
        Thread.sleep(heartbeatIntervalMs.toLong())
        consumer.updateAssignmentMetadataIfNeeded(time.timer(Long.MAX_VALUE))
        assertTrue(heartbeatReceived.get())
    }

    @Test
    @Throws(Exception::class)
    fun verifyHeartbeatSentWhenFetchedDataReady() {
        val metadata = createMetadata(subscription)
        val client = MockClient(time, metadata)
        initMetadata(client, mapOf(topic to 1))
        val node = metadata.fetch().nodes[0]
        consumer = newConsumer(
            time = time,
            client = client,
            subscription = subscription,
            metadata = metadata,
            assignor = assignor,
            autoCommitEnabled = true,
            groupInstanceId = groupInstanceId,
        )
        consumer.subscribe(setOf(topic), getConsumerRebalanceListener(consumer))
        val coordinator = prepareRebalance(
            client = client,
            node = node,
            assignor = assignor,
            partitions = listOf(tp0),
            coordinator = null,
        )
        consumer.updateAssignmentMetadataIfNeeded(time.timer(Long.MAX_VALUE))
        consumer.poll(Duration.ZERO)

        // respond to the outstanding fetch so that we have data available on the next poll
        client.respondFrom(
            response = fetchResponse(
                partition = tp0,
                fetchOffset = 0,
                count = 5,
            ),
            node = node,
        )
        client.poll(0, time.milliseconds())
        client.prepareResponseFrom(
            response = fetchResponse(
                partition = tp0,
                fetchOffset = 5,
                count = 0,
            ),
            node = node,
        )
        val heartbeatReceived = prepareHeartbeatResponse(client, coordinator, Errors.NONE)
        time.sleep(heartbeatIntervalMs.toLong())
        Thread.sleep(heartbeatIntervalMs.toLong())
        consumer.poll(Duration.ZERO)
        assertTrue(heartbeatReceived.get())
    }

    @Test
    fun verifyPollTimesOutDuringMetadataUpdate() {
        val metadata = createMetadata(subscription)
        val client = MockClient(time, metadata)
        initMetadata(client, mapOf(topic to 1))
        val node = metadata.fetch().nodes[0]
        consumer = newConsumer(
            time = time,
            client = client,
            subscription = subscription,
            metadata = metadata,
            assignor = assignor,
            autoCommitEnabled = true,
            groupInstanceId = groupInstanceId,
        )
        consumer.subscribe(setOf(topic), getConsumerRebalanceListener(consumer))
        // Since we would enable the heartbeat thread after received join-response which could
        // send the sync-group on behalf of the consumer if it is enqueued, we may still complete
        // the rebalance and send out the fetch; in order to avoid it we do not prepare sync response here.
        client.prepareResponseFrom(
            response = prepareResponse(
                error = Errors.NONE,
                key = groupId,
                node = node,
            ),
            node = node,
        )
        val coordinator = Node(Int.MAX_VALUE - node.id, node.host, node.port)
        client.prepareResponseFrom(
            response = joinGroupFollowerResponse(
                assignor = assignor,
                generationId = 1,
                memberId = memberId,
                leaderId = leaderId,
                error = Errors.NONE,
            ),
            node = coordinator,
        )
        consumer.poll(Duration.ZERO)
        val requests = client.requests()
        assertEquals(0, requests.count { request -> request.apiKey == ApiKeys.FETCH })
    }

    @Suppress("Deprecation")
    @Test
    fun verifyDeprecatedPollDoesNotTimeOutDuringMetadataUpdate() {
        val metadata = createMetadata(subscription)
        val client = MockClient(time, metadata)
        initMetadata(client, mapOf(topic to 1))
        val node = metadata.fetch().nodes[0]
        consumer = newConsumer(
            time = time,
            client = client,
            subscription = subscription,
            metadata = metadata,
            assignor = assignor,
            autoCommitEnabled = true,
            groupInstanceId = groupInstanceId,
        )
        consumer.subscribe(setOf(topic), getConsumerRebalanceListener(consumer))
        prepareRebalance(
            client = client,
            node = node,
            assignor = assignor,
            partitions = listOf(tp0),
            coordinator = null,
        )
        consumer.poll(0L)

        // The underlying client SHOULD get a fetch request
        val requests = client.requests()
        assertEquals(1, requests.size)
        val builder = requests.peek().requestBuilder()
        assertIs<FetchRequest.Builder>(builder)
    }

    @Test
    fun verifyNoCoordinatorLookupForManualAssignmentWithSeek() {
        val metadata = createMetadata(subscription)
        val client = MockClient(time, metadata)
        initMetadata(client, mapOf(topic to 1))
        consumer = newConsumer(
            time = time,
            client = client,
            subscription = subscription,
            metadata = metadata,
            assignor = assignor,
            autoCommitEnabled = true,
            groupId = null,
            groupInstanceId = groupInstanceId,
            throwOnStableOffsetNotSupported = false,
        )
        consumer.assign(setOf(tp0))
        consumer.seekToBeginning(setOf(tp0))

        // there shouldn't be any need to lookup the coordinator or fetch committed offsets.
        // we just lookup the starting position and send the record fetch.
        client.prepareResponse(listOffsetsResponse(mapOf(tp0 to 50L)))
        client.prepareResponse(
            response = fetchResponse(
                partition = tp0,
                fetchOffset = 50L,
                count = 5,
            )
        )
        val records = consumer.poll(Duration.ofMillis(1))
        assertEquals(5, records.count())
        assertEquals(55L, consumer.position(tp0))
    }

    @Test
    fun verifyNoCoordinatorLookupForManualAssignmentWithOffsetCommit() {
        val metadata = createMetadata(subscription)
        val client = MockClient(time, metadata)
        initMetadata(client, mapOf(topic to 1))
        val node = metadata.fetch().nodes[0]

        // create a consumer with groupID with manual assignment
        consumer = newConsumer(
            time = time,
            client = client,
            subscription = subscription,
            metadata = metadata,
            assignor = assignor,
            autoCommitEnabled = true,
            groupInstanceId = groupInstanceId,
        )
        consumer.assign(setOf(tp0))

        // 1st coordinator error should cause coordinator unknown
        client.prepareResponseFrom(
            response = prepareResponse(
                error = Errors.COORDINATOR_NOT_AVAILABLE,
                key = groupId,
                node = node,
            ),
            node = node,
        )
        consumer.poll(Duration.ofMillis(0))

        // 2nd coordinator error should find the correct coordinator and clear the findCoordinatorFuture
        client.prepareResponseFrom(
            response = prepareResponse(
                error = Errors.NONE,
                key = groupId,
                node = node,
            ),
            node = node,
        )
        client.prepareResponse(
            response = offsetResponse(offsets = mapOf(tp0 to 50L), error = Errors.NONE),
        )
        client.prepareResponse(
            response = fetchResponse(
                partition = tp0,
                fetchOffset = 50L,
                count = 5,
            ),
        )
        val records = consumer.poll(Duration.ofMillis(0))
        assertEquals(5, records.count())
        assertEquals(55L, consumer.position(tp0))

        // after coordinator found, consumer should be able to commit the offset successfully
        client.prepareResponse(
            response = offsetCommitResponse(responseData = mapOf(tp0 to Errors.NONE)),
        )
        consumer.commitSync(mapOf(tp0 to OffsetAndMetadata(55L)))

        // verify the offset is committed
        client.prepareResponse(
            response = offsetResponse(offsets = mapOf(tp0 to 55L), error = Errors.NONE),
        )
        assertEquals(55, consumer.committed(setOf(tp0), Duration.ZERO)[tp0]!!.offset)
    }

    @Test
    fun testFetchProgressWithMissingPartitionPosition() {
        // Verifies that we can make progress on one partition while we are awaiting
        // a reset on another partition.
        val metadata = createMetadata(subscription)
        val client = MockClient(time, metadata)
        initMetadata(client, mapOf(topic to 2))
        val node = metadata.fetch().nodes[0]
        
        consumer = newConsumerNoAutoCommit(time, client, subscription, metadata)
        consumer.assign(listOf(tp0, tp1))
        consumer.seekToEnd(setOf(tp0))
        consumer.seekToBeginning(setOf(tp1))
        client.prepareResponse(
            matcher = { body ->
                val request = assertIs<ListOffsetsRequest>(body)
                val partitions = request.topics.flatMap { t ->
                    if (t.name == topic) t.partitions else emptyList()
                }
                val expectedTp0 = ListOffsetsPartition()
                    .setPartitionIndex(tp0.partition)
                    .setTimestamp(ListOffsetsRequest.LATEST_TIMESTAMP)
                val expectedTp1 = ListOffsetsPartition()
                    .setPartitionIndex(tp1.partition)
                    .setTimestamp(ListOffsetsRequest.EARLIEST_TIMESTAMP)
                partitions.contains(expectedTp0) && partitions.contains(expectedTp1)
            },
            response = listOffsetsResponse(
                mapOf(tp0 to 50L),
                mapOf(tp1 to Errors.NOT_LEADER_OR_FOLLOWER),
            )
        )
        client.prepareResponse(
            matcher = { body ->
                val request = assertIs<FetchRequest>(body)
                val fetchData = request.fetchData(topicNames)
                val tidp0 = TopicIdPartition(topicIds[tp0.topic]!!, tp0)
                fetchData!!.keys == setOf(tidp0) && fetchData[tidp0]!!.fetchOffset == 50L
            },
            response = fetchResponse(
                partition = tp0,
                fetchOffset = 50L,
                count = 5,
            )
        )
        val records = consumer.poll(Duration.ofMillis(1))
        assertEquals(5, records.count())
        assertEquals(setOf(tp0), records.partitions())
    }

    private fun initMetadata(mockClient: MockClient, partitionCounts: Map<String, Int>) {
        val metadataIds = partitionCounts.mapValues { (name, _) -> topicIds[name]!! }
        val initialMetadata = metadataUpdateWith(
            numNodes = 1,
            topicPartitionCounts = partitionCounts,
            topicIds = metadataIds,
        )
        mockClient.updateMetadata(initialMetadata)
    }

    @Test
    fun testMissingOffsetNoResetPolicy() {
        val subscription = SubscriptionState(LogContext(), OffsetResetStrategy.NONE)
        val metadata = createMetadata(subscription)
        val client = MockClient(time, metadata)
        initMetadata(client, mapOf(topic to 1))
        val node = metadata.fetch().nodes[0]
        consumer = newConsumer(
            time = time,
            client = client,
            subscription = subscription,
            metadata = metadata,
            assignor = assignor,
            autoCommitEnabled = true,
            groupId = groupId,
            groupInstanceId = groupInstanceId,
            throwOnStableOffsetNotSupported = false,
        )
        consumer.assign(listOf(tp0))
        client.prepareResponseFrom(
            response = prepareResponse(
                error = Errors.NONE,
                key = groupId,
                node = node,
            ),
            node = node,
        )
        val coordinator = Node(Int.MAX_VALUE - node.id, node.host, node.port)

        // lookup committed offset and find nothing
        client.prepareResponseFrom(
            response = offsetResponse(offsets = mapOf(tp0 to -1L), error = Errors.NONE),
            node = coordinator,
        )
        assertFailsWith<NoOffsetForPartitionException> { consumer.poll(Duration.ZERO) }
    }

    @Test
    fun testResetToCommittedOffset() {
        val subscription = SubscriptionState(LogContext(), OffsetResetStrategy.NONE)
        val metadata = createMetadata(subscription)
        val client = MockClient(time, metadata)
        initMetadata(client, mapOf(topic to 1))
        val node = metadata.fetch().nodes[0]
        consumer = newConsumer(
            time = time,
            client = client,
            subscription = subscription,
            metadata = metadata,
            assignor = assignor,
            autoCommitEnabled = true,
            groupId = groupId,
            groupInstanceId = groupInstanceId,
            throwOnStableOffsetNotSupported = false,
        )
        consumer.assign(listOf(tp0))
        client.prepareResponseFrom(
            response = prepareResponse(
                error = Errors.NONE,
                key = groupId,
                node = node
            ),
            node = node,
        )
        val coordinator = Node(
            id = Int.MAX_VALUE - node.id,
            host = node.host,
            port = node.port,
        )
        client.prepareResponseFrom(
            response = offsetResponse(offsets = mapOf(tp0 to 539L), error = Errors.NONE),
            node = coordinator,
        )
        consumer.poll(Duration.ZERO)
        assertEquals(539L, consumer.position(tp0))
    }

    @Test
    fun testResetUsingAutoResetPolicy() {
        val subscription = SubscriptionState(LogContext(), OffsetResetStrategy.LATEST)
        val metadata = createMetadata(subscription)
        val client = MockClient(time, metadata)
        initMetadata(client, mapOf(topic to 1))
        val node = metadata.fetch().nodes[0]
        consumer = newConsumer(
            time = time,
            client = client,
            subscription = subscription,
            metadata = metadata,
            assignor = assignor,
            autoCommitEnabled = true,
            groupId = groupId,
            groupInstanceId = groupInstanceId,
            throwOnStableOffsetNotSupported = false,
        )
        consumer.assign(listOf(tp0))
        client.prepareResponseFrom(
            response = prepareResponse(
                error = Errors.NONE,
                key = groupId,
                node = node,
            ),
            node = node,
        )
        val coordinator = Node(
            id = Int.MAX_VALUE - node.id,
            host = node.host,
            port = node.port,
        )
        client.prepareResponseFrom(
            response = offsetResponse(offsets = mapOf(tp0 to -1L), error = Errors.NONE),
            node = coordinator,
        )
        client.prepareResponse(response = listOffsetsResponse(mapOf(tp0 to 50L)))
        consumer.poll(Duration.ZERO)
        assertEquals(50L, consumer.position(tp0))
    }

    @Test
    fun testOffsetIsValidAfterSeek() {
        val subscription = SubscriptionState(LogContext(), OffsetResetStrategy.LATEST)
        val metadata = createMetadata(subscription)
        val client = MockClient(time, metadata)
        initMetadata(mockClient = client, partitionCounts = mapOf(topic to 1))
        consumer = newConsumer(
            time = time,
            client = client,
            subscription = subscription,
            metadata = metadata,
            assignor = assignor,
            autoCommitEnabled = true,
            groupId = groupId,
            groupInstanceId = null,
            throwOnStableOffsetNotSupported = false,
        )
        consumer.assign(listOf(tp0))
        consumer.seek(tp0, 20L)
        consumer.poll(Duration.ZERO)
        assertEquals(subscription.validPosition(tp0)!!.offset, 20L)
    }

    @Test
    fun testCommitsFetchedDuringAssign() {
        val offset1: Long = 10000
        val offset2: Long = 20000
        val metadata = createMetadata(subscription)
        val client = MockClient(time, metadata)
        initMetadata(client, mapOf(topic to 2))
        val node = metadata.fetch().nodes[0]
        consumer = newConsumer(
            time = time,
            client = client,
            subscription = subscription,
            metadata = metadata,
            assignor = assignor,
            autoCommitEnabled = true,
            groupInstanceId = groupInstanceId,
        )
        consumer.assign(listOf(tp0))

        // lookup coordinator
        client.prepareResponseFrom(
            response = prepareResponse(Errors.NONE, groupId, node),
            node = node,
        )
        val coordinator = Node(Int.MAX_VALUE - node.id, node.host, node.port)

        // fetch offset for one topic
        client.prepareResponseFrom(
            response = offsetResponse(mapOf(tp0 to offset1), Errors.NONE),
            node = coordinator,
        )
        assertEquals(offset1, consumer.committed(setOf(tp0))[tp0]!!.offset)
        consumer.assign(listOf(tp0, tp1))

        // fetch offset for two topics
        val offsets = mutableMapOf(tp0 to offset1)
        client.prepareResponseFrom(
            response = offsetResponse(offsets, Errors.NONE),
            node = coordinator,
        )
        assertEquals(offset1, consumer.committed(setOf(tp0))[tp0]!!.offset)
        offsets.remove(tp0)
        offsets[tp1] = offset2
        client.prepareResponseFrom(
            response = offsetResponse(offsets, Errors.NONE),
            node = coordinator,
        )
        assertEquals(offset2, consumer.committed(setOf(tp1))[tp1]!!.offset)
    }

    @Test
    fun testFetchStableOffsetThrowInCommitted() {
        assertFailsWith<UnsupportedVersionException> {
            setupThrowableConsumer().committed(setOf(tp0))
        }
    }

    @Test
    fun testFetchStableOffsetThrowInPoll() {
        assertFailsWith<UnsupportedVersionException> {
            setupThrowableConsumer().poll(Duration.ZERO)
        }
    }

    @Test
    fun testFetchStableOffsetThrowInPosition() {
        assertFailsWith<UnsupportedVersionException> {
            setupThrowableConsumer().position(tp0)
        }
    }

    private fun setupThrowableConsumer(): KafkaConsumer<*, *> {
        val offset1: Long = 10000
        val metadata = createMetadata(subscription)
        val client = MockClient(time, metadata)
        initMetadata(client, mapOf(topic to 2))
        client.setNodeApiVersions(
            nodeApiVersions = NodeApiVersions.create(
                apiKey = ApiKeys.OFFSET_FETCH.id,
                minVersion = 0,
                maxVersion = 6,
            )
        )
        val node = metadata.fetch().nodes[0]
        consumer = newConsumer(
            time = time,
            client = client,
            subscription = subscription,
            metadata = metadata,
            assignor = assignor,
            autoCommitEnabled = true,
            groupId = groupId,
            groupInstanceId = groupInstanceId,
            throwOnStableOffsetNotSupported = true,
        )
        consumer.assign(listOf(tp0))
        client.prepareResponseFrom(
            response = prepareResponse(
                error = Errors.NONE,
                key = groupId,
                node = node,
            ),
            node = node,
        )
        val coordinator = Node(
            id = Int.MAX_VALUE - node.id,
            host = node.host,
            port = node.port,
        )
        client.prepareResponseFrom(
            response = offsetResponse(mapOf(tp0 to offset1), Errors.NONE),
            node = coordinator,
        )
        return consumer
    }

    @Test
    fun testNoCommittedOffsets() {
        val offset1: Long = 10000
        val metadata = createMetadata(subscription)
        val client = MockClient(time, metadata)
        initMetadata(client, mapOf(topic to 2))
        val node = metadata.fetch().nodes[0]
        consumer = newConsumer(
            time = time,
            client = client,
            subscription = subscription,
            metadata = metadata,
            assignor = assignor,
            autoCommitEnabled = true,
            groupInstanceId = groupInstanceId,
        )
        consumer.assign(listOf(tp0, tp1))

        // lookup coordinator
        client.prepareResponseFrom(
            response = prepareResponse(
                error = Errors.NONE,
                key = groupId,
                node = node,
            ),
            node = node,
        )
        val coordinator = Node(
            id = Int.MAX_VALUE - node.id,
            host = node.host,
            port = node.port,
        )

        // fetch offset for one topic
        client.prepareResponseFrom(
            response = offsetResponse(
                offsets = mapOf(tp0 to offset1, tp1 to -1L),
                error = Errors.NONE,
            ),
            node = coordinator,
        )
        val committed = consumer.committed(setOf(tp0, tp1))
        assertEquals(2, committed.size)
        assertEquals(offset1, committed[tp0]!!.offset)
        assertNull(committed[tp1])
    }

    @Test
    fun testAutoCommitSentBeforePositionUpdate() {
        val metadata = createMetadata(subscription)
        val client = MockClient(time, metadata)
        initMetadata(client, mapOf(topic to 1))
        val node = metadata.fetch().nodes[0]
        consumer = newConsumer(
            time = time,
            client = client,
            subscription = subscription,
            metadata = metadata,
            assignor = assignor,
            autoCommitEnabled = true,
            groupInstanceId = groupInstanceId,
        )
        consumer.subscribe(setOf(topic), getConsumerRebalanceListener(consumer))
        val coordinator = prepareRebalance(
            client = client,
            node = node,
            assignor = assignor,
            partitions = listOf(tp0),
            coordinator = null,
        )
        consumer.updateAssignmentMetadataIfNeeded(time.timer(Long.MAX_VALUE))
        consumer.poll(Duration.ZERO)

        // respond to the outstanding fetch so that we have data available on the next poll
        client.respondFrom(
            response = fetchResponse(
                partition = tp0,
                fetchOffset = 0,
                count = 5,
            ),
            node = node,
        )
        client.poll(0, time.milliseconds())
        time.sleep(autoCommitIntervalMs.toLong())
        client.prepareResponseFrom(
            response = fetchResponse(
                partition = tp0,
                fetchOffset = 5,
                count = 0,
            ),
            node = node,
        )

        // no data has been returned to the user yet, so the committed offset should be 0
        val commitReceived = prepareOffsetCommitResponse(
            client = client,
            coordinator = coordinator,
            partition = tp0,
            offset = 0,
        )
        consumer.poll(Duration.ZERO)
        assertTrue(commitReceived.get())
    }

    @Test
    fun testRegexSubscription() {
        val unmatchedTopic = "unmatched"
        val metadata = createMetadata(subscription)
        val client = MockClient(time, metadata)
        val partitionCounts = mapOf(topic to 1, unmatchedTopic to 1)
        topicIds[unmatchedTopic] = Uuid.randomUuid()
        initMetadata(client, partitionCounts)
        val node = metadata.fetch().nodes[0]
        consumer = newConsumer(
            time = time,
            client = client,
            subscription = subscription,
            metadata = metadata,
            assignor = assignor,
            autoCommitEnabled = true,
            groupInstanceId = groupInstanceId,
        )
        prepareRebalance(
            client = client,
            node = node,
            subscribedTopics = setOf(topic),
            assignor = assignor,
            partitions = listOf(tp0),
            coordinator = null,
        )
        consumer.subscribe(
            pattern = Pattern.compile(topic),
            callback = getConsumerRebalanceListener(consumer),
        )
        client.prepareMetadataUpdate(
            updateResponse = metadataUpdateWithIds(
                numNodes = 1,
                topicPartitionCounts = partitionCounts,
                topicIds = topicIds,
            ),
        )
        consumer.updateAssignmentMetadataIfNeeded(time.timer(Long.MAX_VALUE))
        assertEquals(setOf(topic), consumer.subscription())
        assertEquals(setOf(tp0), consumer.assignment())
    }

    @Test
    fun testChangingRegexSubscription() {
        val otherTopic = "other"
        val otherTopicPartition = TopicPartition(topic = otherTopic, partition = 0)
        val metadata = createMetadata(subscription)
        val client = MockClient(time, metadata)
        val partitionCounts = mapOf(topic to 1, otherTopic to 1)
        topicIds[otherTopic] = Uuid.randomUuid()
        initMetadata(client, partitionCounts)
        val node = metadata.fetch().nodes[0]
        consumer = newConsumer(
            time = time,
            client = client,
            subscription = subscription,
            metadata = metadata,
            assignor = assignor,
            autoCommitEnabled = false,
            groupInstanceId = groupInstanceId
        )
        val coordinator = prepareRebalance(
            client = client,
            node = node,
            subscribedTopics = setOf(topic),
            assignor = assignor,
            partitions = listOf(tp0),
            coordinator = null,
        )
        consumer.subscribe(
            pattern = Pattern.compile(topic),
            callback = getConsumerRebalanceListener(consumer)
        )
        consumer.updateAssignmentMetadataIfNeeded(time.timer(Long.MAX_VALUE))
        consumer.poll(Duration.ZERO)
        assertEquals(setOf(topic), consumer.subscription())

        consumer.subscribe(
            pattern = Pattern.compile(otherTopic),
            callback = getConsumerRebalanceListener(consumer)
        )
        client.prepareMetadataUpdate(
            updateResponse = metadataUpdateWithIds(
                numNodes = 1,
                topicPartitionCounts = partitionCounts,
                topicIds = topicIds,
            )
        )
        prepareRebalance(
            client = client,
            node = node,
            subscribedTopics = setOf(otherTopic),
            assignor = assignor,
            partitions = listOf(otherTopicPartition),
            coordinator = coordinator,
        )
        consumer.poll(Duration.ZERO)
        assertEquals(setOf(otherTopic), consumer.subscription())
    }

    @Test
    @Throws(Exception::class)
    fun testWakeupWithFetchDataAvailable() {
        val metadata = createMetadata(subscription)
        val client = MockClient(time, metadata)
        initMetadata(mockClient = client, partitionCounts = mapOf(topic to 1))
        val node = metadata.fetch().nodes[0]
        consumer = newConsumer(
            time = time,
            client = client,
            subscription = subscription,
            metadata = metadata,
            assignor = assignor,
            autoCommitEnabled = true,
            groupInstanceId = groupInstanceId
        )
        consumer.subscribe(
            topics = setOf(topic),
            callback = getConsumerRebalanceListener(consumer),
        )
        prepareRebalance(
            client = client,
            node = node,
            assignor = assignor,
            partitions = listOf(tp0),
            coordinator = null,
        )
        consumer.updateAssignmentMetadataIfNeeded(time.timer(Long.MAX_VALUE))
        consumer.poll(Duration.ZERO)

        // respond to the outstanding fetch so that we have data available on the next poll
        client.respondFrom(
            response = fetchResponse(
                partition = tp0,
                fetchOffset = 0,
                count = 5,
            ),
            node = node,
        )
        client.poll(0, time.milliseconds())
        consumer.wakeup()
        assertFailsWith<WakeupException> { consumer.poll(Duration.ZERO) }

        // make sure the position hasn't been updated
        assertEquals(0, consumer.position(tp0))

        // the next poll should return the completed fetch
        val records = consumer.poll(Duration.ZERO)
        assertEquals(5, records.count())
        // Increment time asynchronously to clear timeouts in closing the consumer
        val exec = Executors.newSingleThreadScheduledExecutor()
        exec.scheduleAtFixedRate(
            { time.sleep(sessionTimeoutMs.toLong()) },
            0L,
            10L,
            TimeUnit.MILLISECONDS,
        )
        consumer.close()
        exec.shutdownNow()
        exec.awaitTermination(5L, TimeUnit.SECONDS)
    }

    @Test
    fun testPollThrowsInterruptExceptionIfInterrupted() {
        val metadata = createMetadata(subscription)
        val client = MockClient(time, metadata)
        initMetadata(client, mapOf(topic to 1))
        val node = metadata.fetch().nodes[0]
        consumer = newConsumer(
            time = time,
            client = client,
            subscription = subscription,
            metadata = metadata,
            assignor = assignor,
            autoCommitEnabled = false,
            groupInstanceId = groupInstanceId,
        )
        consumer.subscribe(
            topics = setOf(topic),
            callback = getConsumerRebalanceListener(consumer),
        )
        prepareRebalance(
            client = client,
            node = node,
            assignor = assignor,
            partitions = listOf(tp0),
            coordinator = null,
        )
        consumer.updateAssignmentMetadataIfNeeded(time.timer(Long.MAX_VALUE))
        consumer.poll(Duration.ZERO)

        // interrupt the thread and call poll
        try {
            Thread.currentThread().interrupt()
            assertFailsWith<InterruptException> { consumer.poll(Duration.ZERO) }
        } finally {
            // clear interrupted state again since this thread may be reused by JUnit
            Thread.interrupted()
        }
    }

    @Test
    fun fetchResponseWithUnexpectedPartitionIsIgnored() {
        val metadata = createMetadata(subscription)
        val client = MockClient(time, metadata)
        initMetadata(client, mapOf(topic to 1))
        val node = metadata.fetch().nodes[0]
        consumer = newConsumer(
            time = time,
            client = client,
            subscription = subscription,
            metadata = metadata,
            assignor = assignor,
            autoCommitEnabled = true,
            groupInstanceId = groupInstanceId,
        )
        consumer.subscribe(
            topics = listOf(topic),
            callback = getConsumerRebalanceListener(consumer),
        )
        prepareRebalance(
            client = client,
            node = node,
            assignor = assignor,
            partitions = listOf(tp0),
            coordinator = null,
        )
        val fetches1: MutableMap<TopicPartition, FetchInfo> = HashMap()
        fetches1[tp0] = FetchInfo(offset = 0, count = 1)
        fetches1[t2p0] = FetchInfo(offset = 0, count = 10) // not assigned and not fetched
        client.prepareResponseFrom(
            response = fetchResponse(fetches1),
            node = node,
        )
        consumer.updateAssignmentMetadataIfNeeded(time.timer(Long.MAX_VALUE))
        val records = consumer.poll(Duration.ZERO)
        assertEquals(0, records.count())
    }

    /**
     * Verify that when a consumer changes its topic subscription its assigned partitions
     * do not immediately change, and the latest consumed offsets of its to-be-revoked
     * partitions are properly committed (when auto-commit is enabled).
     * Upon unsubscribing from subscribed topics the consumer subscription and assignment
     * are both updated right away but its consumed offsets are not auto committed.
     */
    @Test
    fun testSubscriptionChangesWithAutoCommitEnabled() {
        val metadata = createMetadata(subscription)
        val client = MockClient(time, metadata)
        val tpCounts = mapOf(
            topic to 1,
            topic2 to 1,
            topic3 to 1,
        )
        initMetadata(client, tpCounts)
        val node = metadata.fetch().nodes[0]
        val assignor = RangeAssignor()
        consumer = newConsumer(
            time = time,
            client = client,
            subscription = subscription,
            metadata = metadata,
            assignor = assignor,
            autoCommitEnabled = true,
            groupInstanceId = groupInstanceId
        )

        // initial subscription
        consumer.subscribe(
            topics = listOf(topic, topic2),
            callback = getConsumerRebalanceListener(consumer),
        )

        // verify that subscription has changed but assignment is still unchanged
        assertEquals(2, consumer.subscription().size)
        assertTrue(consumer.subscription().contains(topic) && consumer.subscription().contains(topic2))
        assertTrue(consumer.assignment().isEmpty())

        // mock rebalance responses
        val coordinator = prepareRebalance(
            client = client,
            node = node,
            assignor = assignor,
            partitions = listOf(tp0, t2p0),
            coordinator = null,
        )
        consumer.updateAssignmentMetadataIfNeeded(time.timer(Long.MAX_VALUE))
        consumer.poll(Duration.ZERO)

        // verify that subscription is still the same, and now assignment has caught up
        assertEquals(2, consumer.subscription().size)
        assertTrue(consumer.subscription().contains(topic) && consumer.subscription().contains(topic2))
        assertEquals(2, consumer.assignment().size)
        assertTrue(consumer.assignment().contains(tp0) && consumer.assignment().contains(t2p0))

        // mock a response to the outstanding fetch so that we have data available on the next poll
        val fetches1: MutableMap<TopicPartition, FetchInfo> = HashMap()
        fetches1[tp0] = FetchInfo(offset = 0, count = 1)
        fetches1[t2p0] = FetchInfo(offset = 0, count = 10)
        client.respondFrom(fetchResponse(fetches1), node)
        client.poll(0, time.milliseconds())
        var records = consumer.poll(Duration.ofMillis(1))

        // clear out the prefetch so it doesn't interfere with the rest of the test
        fetches1[tp0] = FetchInfo(offset = 1, count = 0)
        fetches1[t2p0] = FetchInfo(offset = 10, count = 0)
        client.respondFrom(fetchResponse(fetches1), node)
        client.poll(0, time.milliseconds())

        // verify that the fetch occurred as expected
        assertEquals(11, records.count())
        assertEquals(1L, consumer.position(tp0))
        assertEquals(10L, consumer.position(t2p0))

        // subscription change
        consumer.subscribe(listOf(topic, topic3), getConsumerRebalanceListener(consumer))

        // verify that subscription has changed but assignment is still unchanged
        assertEquals(2, consumer.subscription().size)
        assertTrue(consumer.subscription().contains(topic) && consumer.subscription().contains(topic3))
        assertEquals(2, consumer.assignment().size)
        assertTrue(consumer.assignment().contains(tp0) && consumer.assignment().contains(t2p0))

        // mock the offset commit response for to be revoked partitions
        val partitionOffsets1 = mapOf(tp0 to 1L, t2p0 to 10L)
        val commitReceived = prepareOffsetCommitResponse(
            client = client,
            coordinator = coordinator,
            partitionOffsets = partitionOffsets1,
        )

        // mock rebalance responses
        prepareRebalance(
            client = client,
            node = node,
            assignor = assignor,
            partitions = listOf(tp0, t3p0),
            coordinator = coordinator,
        )

        // mock a response to the next fetch from the new assignment
        val fetches2 = mapOf(
            tp0 to FetchInfo(offset = 1, count = 1),
            t3p0 to FetchInfo(offset = 0, count = 100),
        )
        client.prepareResponse(response = fetchResponse(fetches2))
        records = consumer.poll(Duration.ofMillis(1))

        // verify that the fetch occurred as expected
        assertEquals(101, records.count())
        assertEquals(2L, consumer.position(tp0))
        assertEquals(100L, consumer.position(t3p0))

        // verify that the offset commits occurred as expected
        assertTrue(commitReceived.get())

        // verify that subscription is still the same, and now assignment has caught up
        assertEquals(2, consumer.subscription().size)
        assertTrue(consumer.subscription().contains(topic) && consumer.subscription().contains(topic3))
        assertEquals(2, consumer.assignment().size)
        assertTrue(consumer.assignment().contains(tp0) && consumer.assignment().contains(t3p0))
        consumer.unsubscribe()

        // verify that subscription and assignment are both cleared
        assertTrue(consumer.subscription().isEmpty())
        assertTrue(consumer.assignment().isEmpty())
        client.requests().clear()
    }

    /**
     * Verify that when a consumer changes its topic subscription its assigned partitions
     * do not immediately change, and the consumed offsets of its to-be-revoked partitions
     * are not committed (when auto-commit is disabled).
     * Upon unsubscribing from subscribed topics, the assigned partitions immediately
     * change but if auto-commit is disabled the consumer offsets are not committed.
     */
    @Test
    fun testSubscriptionChangesWithAutoCommitDisabled() {
        val metadata = createMetadata(subscription)
        val client = MockClient(time, metadata)
        val tpCounts = mapOf(topic to 1, topic2 to 1)
        initMetadata(client, tpCounts)
        val node = metadata.fetch().nodes[0]
        val assignor: ConsumerPartitionAssignor = RangeAssignor()
        consumer = newConsumer(
            time = time,
            client = client,
            subscription = subscription,
            metadata = metadata,
            assignor = assignor,
            autoCommitEnabled = false,
            groupInstanceId = groupInstanceId
        )
        initializeSubscriptionWithSingleTopic(
            consumer = consumer,
            consumerRebalanceListener = getConsumerRebalanceListener(consumer),
        )

        // mock rebalance responses
        prepareRebalance(
            client = client,
            node = node,
            assignor = assignor,
            partitions = listOf(tp0),
            coordinator = null,
        )
        consumer.updateAssignmentMetadataIfNeeded(time.timer(Long.MAX_VALUE))
        consumer.poll(Duration.ZERO)

        // verify that subscription is still the same, and now assignment has caught up
        assertEquals(setOf(topic), consumer.subscription())
        assertEquals(setOf(tp0), consumer.assignment())
        consumer.poll(Duration.ZERO)

        // subscription change
        consumer.subscribe(
            topics = setOf(topic2),
            callback = getConsumerRebalanceListener(consumer),
        )

        // verify that subscription has changed but assignment is still unchanged
        assertEquals(setOf(topic2), consumer.subscription())
        assertEquals(setOf(tp0), consumer.assignment())

        // the auto commit is disabled, so no offset commit request should be sent
        for ((_, requestBuilder) in client.requests())
            assertNotSame(ApiKeys.OFFSET_COMMIT, requestBuilder.apiKey)

        // subscription change
        consumer.unsubscribe()

        // verify that subscription and assignment are both updated
        assertEquals(emptySet(), consumer.subscription())
        assertEquals(emptySet(), consumer.assignment())

        // the auto commit is disabled, so no offset commit request should be sent
        for ((_, requestBuilder) in client.requests())
            assertNotSame(ApiKeys.OFFSET_COMMIT, requestBuilder.apiKey)

        client.requests().clear()
    }

    @Test
    fun testUnsubscribeShouldTriggerPartitionsRevokedWithValidGeneration() {
        val metadata = createMetadata(subscription)
        val client = MockClient(time, metadata)
        initMetadata(client, mapOf(topic to 1))
        val node = metadata.fetch().nodes[0]
        val assignor = CooperativeStickyAssignor()
        consumer = newConsumer(
            time = time,
            client = client,
            subscription = subscription,
            metadata = metadata,
            assignor = assignor,
            autoCommitEnabled = false,
            groupInstanceId = groupInstanceId,
        )
        initializeSubscriptionWithSingleTopic(
            consumer = consumer,
            consumerRebalanceListener = exceptionConsumerRebalanceListener,
        )
        prepareRebalance(
            client = client,
            node = node,
            assignor = assignor,
            partitions = listOf(tp0),
            coordinator = null,
        )
        val assignmentException = assertFailsWith<RuntimeException> {
            consumer.updateAssignmentMetadataIfNeeded(time.timer(Long.MAX_VALUE))
        }
        assertEquals(partitionAssigned + singleTopicPartition, assignmentException.cause!!.message)
        val unsubscribeException = assertFailsWith<RuntimeException> { consumer.unsubscribe() }
        assertEquals(partitionRevoked + singleTopicPartition, unsubscribeException.cause!!.message)
    }

    @Test
    @Throws(Exception::class)
    fun testUnsubscribeShouldTriggerPartitionsLostWithNoGeneration() {
        val metadata = createMetadata(subscription)
        val client = MockClient(time, metadata)
        initMetadata(mockClient = client, partitionCounts = mapOf(topic to 1))
        val node = metadata.fetch().nodes[0]
        val assignor = CooperativeStickyAssignor()
        consumer = newConsumer(
            time = time,
            client = client,
            subscription = subscription,
            metadata = metadata,
            assignor = assignor,
            autoCommitEnabled = false,
            groupInstanceId = groupInstanceId,
        )
        initializeSubscriptionWithSingleTopic(
            consumer = consumer,
            consumerRebalanceListener = exceptionConsumerRebalanceListener,
        )
        val coordinator = prepareRebalance(
            client = client,
            node = node,
            assignor = assignor,
            partitions = listOf(tp0),
            coordinator = null,
        )
        val assignException = assertFailsWith<RuntimeException> {
            consumer.updateAssignmentMetadataIfNeeded(time.timer(Long.MAX_VALUE))
        }
        assertEquals(partitionAssigned + singleTopicPartition, assignException.cause!!.message)
        val heartbeatReceived = prepareHeartbeatResponse(
            client = client,
            coordinator = coordinator,
            error = Errors.UNKNOWN_MEMBER_ID,
        )
        time.sleep(heartbeatIntervalMs.toLong())
        TestUtils.waitForCondition(
            testCondition = { heartbeatReceived.get() },
            conditionDetails = "Heartbeat response did not occur within timeout.",
        )
        val unsubscribeException = assertFailsWith<RuntimeException> { consumer.unsubscribe() }
        assertEquals(partitionLost + singleTopicPartition, unsubscribeException.cause!!.message)
    }

    private fun initializeSubscriptionWithSingleTopic(
        consumer: KafkaConsumer<*, *>,
        consumerRebalanceListener: ConsumerRebalanceListener,
    ) {
        consumer.subscribe(setOf(topic), consumerRebalanceListener)
        // verify that subscription has changed but assignment is still unchanged
        assertEquals(setOf(topic), consumer.subscription())
        assertEquals(emptySet(), consumer.assignment())
    }

    @Test
    fun testManualAssignmentChangeWithAutoCommitEnabled() {
        val metadata = createMetadata(subscription)
        val client = MockClient(time, metadata)
        val tpCounts = mapOf(topic to 1, topic2 to 1)
        initMetadata(client, tpCounts)
        val node = metadata.fetch().nodes[0]
        val assignor = RangeAssignor()
        consumer = newConsumer(
            time = time,
            client = client,
            subscription = subscription,
            metadata = metadata,
            assignor = assignor,
            autoCommitEnabled = true,
            groupInstanceId = groupInstanceId,
        )

        // lookup coordinator
        client.prepareResponseFrom(
            response = prepareResponse(
                error = Errors.NONE,
                key = groupId,
                node = node,
            ),
            node = node,
        )
        val coordinator = Node(
            id = Int.MAX_VALUE - node.id,
            host = node.host,
            port = node.port,
        )

        // manual assignment
        consumer.assign(setOf(tp0))
        consumer.seekToBeginning(setOf(tp0))

        // fetch offset for one topic
        client.prepareResponseFrom(
            response = offsetResponse(offsets = mapOf(tp0 to 0L), error = Errors.NONE),
            node = coordinator,
        )
        assertEquals(0, consumer.committed(setOf(tp0))[tp0]!!.offset)

        // verify that assignment immediately changes
        assertEquals(consumer.assignment(), setOf(tp0))

        // there shouldn't be any need to lookup the coordinator or fetch committed offsets.
        // we just lookup the starting position and send the record fetch.
        client.prepareResponse(response = listOffsetsResponse(mapOf(tp0 to 10L)))
        client.prepareResponse(
            response = fetchResponse(
                partition = tp0,
                fetchOffset = 10L,
                count = 1,
            )
        )
        val records = consumer.poll(Duration.ofMillis(1))
        assertEquals(1, records.count())
        assertEquals(11L, consumer.position(tp0))

        // mock the offset commit response for to be revoked partitions
        val commitReceived = prepareOffsetCommitResponse(
            client = client,
            coordinator = coordinator,
            partition = tp0,
            offset = 11,
        )

        // new manual assignment
        consumer.assign(setOf(t2p0))

        // verify that assignment immediately changes
        assertEquals(consumer.assignment(), setOf(t2p0))
        // verify that the offset commits occurred as expected
        assertTrue(commitReceived.get())
        client.requests().clear()
    }

    @Test
    fun testManualAssignmentChangeWithAutoCommitDisabled() {
        val metadata = createMetadata(subscription)
        val client = MockClient(time, metadata)
        val tpCounts = mapOf(topic to 1, topic2 to 1)
        initMetadata(client, tpCounts)
        val node = metadata.fetch().nodes[0]
        val assignor = RangeAssignor()
        consumer = newConsumer(
            time = time,
            client = client,
            subscription = subscription,
            metadata = metadata,
            assignor = assignor,
            autoCommitEnabled = false,
            groupInstanceId = groupInstanceId,
        )

        // lookup coordinator
        client.prepareResponseFrom(
            response = prepareResponse(
                error = Errors.NONE,
                key = groupId,
                node = node,
            ),
            node = node,
        )
        val coordinator = Node(
            id = Int.MAX_VALUE - node.id,
            host = node.host,
            port = node.port,
        )

        // manual assignment
        consumer.assign(setOf(tp0))
        consumer.seekToBeginning(setOf(tp0))

        // fetch offset for one topic
        client.prepareResponseFrom(
            response = offsetResponse(mapOf(tp0 to 0L), Errors.NONE),
            node = coordinator,
        )
        assertEquals(0, consumer.committed(setOf(tp0))[tp0]!!.offset)

        // verify that assignment immediately changes
        assertEquals(consumer.assignment(), setOf(tp0))

        // there shouldn't be any need to lookup the coordinator or fetch committed offsets.
        // we just lookup the starting position and send the record fetch.
        client.prepareResponse(response = listOffsetsResponse(mapOf(tp0 to 10L)))
        client.prepareResponse(
            response = fetchResponse(
                partition = tp0,
                fetchOffset = 10L,
                count = 1
            ),
        )
        val records = consumer.poll(Duration.ofMillis(1))
        assertEquals(1, records.count())
        assertEquals(11L, consumer.position(tp0))

        // new manual assignment
        consumer.assign(setOf(t2p0))

        // verify that assignment immediately changes
        assertEquals(consumer.assignment(), setOf(t2p0))

        // the auto commit is disabled, so no offset commit request should be sent
        for ((_, requestBuilder) in client.requests())
            assertNotSame(requestBuilder.apiKey, ApiKeys.OFFSET_COMMIT)

        client.requests().clear()
    }

    @Test
    fun testOffsetOfPausedPartitions() {
        val metadata = createMetadata(subscription)
        val client = MockClient(time, metadata)
        initMetadata(client, mapOf(topic to 2))
        val node = metadata.fetch().nodes[0]
        val assignor: ConsumerPartitionAssignor = RangeAssignor()
        consumer = newConsumer(
            time = time,
            client = client,
            subscription = subscription,
            metadata = metadata,
            assignor = assignor,
            autoCommitEnabled = true,
            groupInstanceId = groupInstanceId,
        )

        // lookup coordinator
        client.prepareResponseFrom(
            response = prepareResponse(
                error = Errors.NONE,
                key = groupId,
                node = node,
            ),
            node = node,
        )
        val coordinator = Node(
            id = Int.MAX_VALUE - node.id,
            host = node.host,
            port = node.port,
        )

        // manual assignment
        val partitions = setOf(tp0, tp1)
        consumer.assign(partitions)
        // verify consumer's assignment
        assertEquals(partitions, consumer.assignment())
        consumer.pause(partitions)
        consumer.seekToEnd(partitions)

        // fetch and verify committed offset of two partitions
        val offsets = mutableMapOf(tp0 to 0L, tp1 to 0L)
        client.prepareResponseFrom(
            response = offsetResponse(offsets = offsets, error = Errors.NONE),
            node = coordinator,
        )
        assertEquals(0, consumer.committed(setOf(tp0))[tp0]!!.offset)
        offsets.remove(tp0)
        offsets[tp1] = 0L
        client.prepareResponseFrom(
            response = offsetResponse(offsets = offsets, error = Errors.NONE),
            node = coordinator,
        )
        assertEquals(0, consumer.committed(setOf(tp1))[tp1]!!.offset)

        // fetch and verify consumer's position in the two partitions
        val offsetResponse = mapOf(tp0 to 3L, tp1 to 3L)
        client.prepareResponse(response = listOffsetsResponse(offsetResponse))
        assertEquals(3L, consumer.position(tp0))
        assertEquals(3L, consumer.position(tp1))
        client.requests().clear()
        consumer.unsubscribe()
    }

    @Test
    fun testPollWithNoSubscription() {
        consumer = newConsumer(null as String?)
        assertFailsWith<IllegalStateException> { consumer.poll(Duration.ZERO) }
    }

    @Test
    fun testPollWithEmptySubscription() {
        consumer = newConsumer(groupId)
        consumer.subscribe(emptyList())
        assertFailsWith<IllegalStateException> { consumer.poll(Duration.ZERO) }
    }

    @Test
    fun testPollWithEmptyUserAssignment() {
        consumer = newConsumer(groupId)
        consumer.assign(emptySet())
        assertFailsWith<IllegalStateException> { consumer.poll(Duration.ZERO) }
    }

    @Test
    @Throws(Exception::class)
    fun testGracefulClose() {
        val response = mapOf(tp0 to Errors.NONE)
        val commitResponse = offsetCommitResponse(response)
        val leaveGroupResponse = LeaveGroupResponse(LeaveGroupResponseData().setErrorCode(Errors.NONE.code))

        val closeResponse = FetchResponse.of(
            error = Errors.NONE,
            throttleTimeMs = 0,
            sessionId = FetchMetadata.INVALID_SESSION_ID,
            responseData = mutableMapOf(),
        )
        consumerCloseTest(
            closeTimeoutMs = 5000,
            responses = listOf(commitResponse, leaveGroupResponse, closeResponse),
            waitMs = 0,
            interrupt = false,
        )
    }

    @Test
    @Throws(java.lang.Exception::class)
    fun testCloseTimeoutDueToNoResponseForCloseFetchRequest() {
        val response = mapOf(tp0 to Errors.NONE)
        val commitResponse = offsetCommitResponse(response)
        val leaveGroupResponse = LeaveGroupResponse(LeaveGroupResponseData().setErrorCode(Errors.NONE.code))
        val serverResponsesWithoutCloseResponse = listOf(commitResponse, leaveGroupResponse)

        // to ensure timeout due to no response for fetcher close request, we will ensure that we have successful
        // response from server for first two requests and the test is configured to wait for duration which is greater
        // than configured timeout.
        val closeTimeoutMs = 5000
        val waitForCloseCompletionMs = closeTimeoutMs + 1000
        consumerCloseTest(
            closeTimeoutMs = closeTimeoutMs.toLong(),
            responses = serverResponsesWithoutCloseResponse,
            waitMs = waitForCloseCompletionMs.toLong(),
            interrupt = false,
        )
    }

    @Test
    @Throws(Exception::class)
    fun testCloseTimeout() {
        consumerCloseTest(
            closeTimeoutMs = 5000,
            responses = emptyList(),
            waitMs = 5000,
            interrupt = false,
        )
    }

    @Test
    @Throws(Exception::class)
    fun testLeaveGroupTimeout() {
        val response = mapOf(tp0 to Errors.NONE)
        val commitResponse = offsetCommitResponse(response)

        consumerCloseTest(
            closeTimeoutMs = 5000,
            responses = listOf(commitResponse),
            waitMs = 5000,
            interrupt = false,
        )
    }

    @Test
    @Throws(Exception::class)
    fun testCloseNoWait() {
        consumerCloseTest(
            closeTimeoutMs = 0,
            responses = emptyList(),
            waitMs = 0,
            interrupt = false,
        )
    }

    @Test
    @Throws(Exception::class)
    fun testCloseInterrupt() {
        consumerCloseTest(
            closeTimeoutMs = Long.MAX_VALUE,
            responses = emptyList(),
            waitMs = 0,
            interrupt = true,
        )
    }

    @Test
    fun testCloseShouldBeIdempotent() {
        val metadata = createMetadata(subscription)
        val client = spy(MockClient(time, metadata))
        initMetadata(client, mapOf(topic to 1))

        consumer = newConsumer(
            time = time,
            client = client,
            subscription = subscription,
            metadata = metadata,
            assignor = assignor,
            autoCommitEnabled = false,
            groupInstanceId = groupInstanceId,
        )

        consumer.close(Duration.ZERO)
        consumer.close(Duration.ZERO)

        // verify that the call is idempotent by checking that the network client is only closed once.
        verify(client).close()
    }

    @Test
    fun testOperationsBySubscribingConsumerWithDefaultGroupId() {
        val error = assertFailsWith<KafkaException>(
            message = "Expected an InvalidConfigurationException",
        ) { newConsumer(groupId = null, enableAutoCommit = true) }
        assertIs<InvalidConfigurationException>(error.cause)

        assertFailsWith<InvalidGroupIdException>(
            message = "Expected an InvalidGroupIdException",
        ) { newConsumer(groupId = null).subscribe(setOf(topic)) }

        assertFailsWith<InvalidGroupIdException>(
            message = "Expected an InvalidGroupIdException",
        ) { newConsumer(groupId = null).committed(setOf(tp0))[tp0] }

        assertFailsWith<InvalidGroupIdException>(
            message = "Expected an InvalidGroupIdException",
        ) { newConsumer(groupId = null).commitAsync() }

        assertFailsWith<InvalidGroupIdException>(
            message = "Expected an InvalidGroupIdException",
        ) { newConsumer(groupId = null).commitSync() }
    }

    @Test
    fun testOperationsByAssigningConsumerWithDefaultGroupId() {
        val consumer = newConsumer(null as String?)
        consumer.assign(setOf(tp0))

        assertFailsWith<InvalidGroupIdException>(
            message = "Expected an InvalidGroupIdException",
        ) { consumer.committed(setOf(tp0))[tp0] }

        assertFailsWith<InvalidGroupIdException>(
            message = "Expected an InvalidGroupIdException",
        ) { consumer.commitAsync() }

        assertFailsWith<InvalidGroupIdException>(
            message = "Expected an InvalidGroupIdException",
        ) { consumer.commitSync() }
    }

    @Test
    fun testMetricConfigRecordingLevelInfo() {
        val props = Properties()
        props[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] = "localhost:9000"

        val consumer = KafkaConsumer(
            properties = props,
            keyDeserializer = ByteArrayDeserializer(),
            valueDeserializer = ByteArrayDeserializer(),
        )
        assertEquals(Sensor.RecordingLevel.INFO, consumer.metrics.config.recordingLevel)
        consumer.close(Duration.ZERO)

        props[ConsumerConfig.METRICS_RECORDING_LEVEL_CONFIG] = "DEBUG"
        val consumer2 = KafkaConsumer(
            properties = props,
            keyDeserializer = ByteArrayDeserializer(),
            valueDeserializer = ByteArrayDeserializer()
        )
        assertEquals(Sensor.RecordingLevel.DEBUG, consumer2.metrics.config.recordingLevel)
        consumer2.close(Duration.ZERO)
    }

    @Test
    @Throws(Exception::class)
    fun testShouldAttemptToRejoinGroupAfterSyncGroupFailed() {
        val metadata = createMetadata(subscription)
        val client = MockClient(time, metadata)
        initMetadata(client, mapOf(topic to 1))
        val node = metadata.fetch().nodes[0]
        consumer = newConsumer(
            time = time,
            client = client,
            subscription = subscription,
            metadata = metadata,
            assignor = assignor,
            autoCommitEnabled = false,
            groupInstanceId = groupInstanceId,
        )
        consumer.subscribe(setOf(topic), getConsumerRebalanceListener(consumer))
        client.prepareResponseFrom(
            response = prepareResponse(
                error = Errors.NONE,
                key = groupId,
                node = node,
            ),
            node = node,
        )
        val coordinator = Node(Int.MAX_VALUE - node.id, node.host, node.port)
        client.prepareResponseFrom(
            response = joinGroupFollowerResponse(
                assignor = assignor,
                generationId = 1,
                memberId = memberId,
                leaderId = leaderId,
                error = Errors.NONE,
            ),
            node = coordinator
        )
        client.prepareResponseFrom(
            response = syncGroupResponse(partitions = listOf(tp0), error = Errors.NONE),
            node = coordinator,
        )
        client.prepareResponseFrom(
            response = fetchResponse(
                partition = tp0,
                fetchOffset = 0,
                count = 1,
            ),
            node = node,
        )
        client.prepareResponseFrom(
            response = fetchResponse(
                partition = tp0,
                fetchOffset = 1,
                count = 0,
            ),
            node = node,
        )
        consumer.updateAssignmentMetadataIfNeeded(time.timer(Long.MAX_VALUE))
        consumer.poll(Duration.ZERO)

        // heartbeat fails due to rebalance in progress
        client.prepareResponseFrom(
            matcher = { true },
            response = HeartbeatResponse(
                HeartbeatResponseData().setErrorCode(Errors.REBALANCE_IN_PROGRESS.code)
            ),
            node = coordinator,
        )

        // join group
        val byteBuffer = serializeSubscription(ConsumerPartitionAssignor.Subscription(listOf(topic)))

        // This member becomes the leader
        val leaderResponse = JoinGroupResponse(
            JoinGroupResponseData()
                .setErrorCode(Errors.NONE.code)
                .setGenerationId(1)
                .setProtocolName(assignor.name())
                .setLeader(memberId)
                .setMemberId(memberId)
                .setMembers(
                    listOf(
                        JoinGroupResponseMember()
                            .setMemberId(memberId)
                            .setMetadata(byteBuffer.array())
                    )
                ),
            ApiKeys.JOIN_GROUP.latestVersion()
        )
        client.prepareResponseFrom(response = leaderResponse, node = coordinator)

        // sync group fails due to disconnect
        client.prepareResponseFrom(
            response = syncGroupResponse(partitions = listOf(tp0), error = Errors.NONE),
            node = coordinator,
            disconnected = true,
        )

        // should try and find the new coordinator
        client.prepareResponseFrom(
            response = prepareResponse(
                error = Errors.NONE,
                key = groupId,
                node = node,
            ),
            node = node,
        )

        // rejoin group
        client.prepareResponseFrom(
            response = joinGroupFollowerResponse(
                assignor = assignor,
                generationId = 1,
                memberId = memberId,
                leaderId = leaderId,
                error = Errors.NONE,
            ),
            node = coordinator,
        )
        client.prepareResponseFrom(
            response = syncGroupResponse(partitions = listOf(tp0), error = Errors.NONE),
            node = coordinator,
        )
        client.prepareResponseFrom(
            matcher = { body ->
                body is FetchRequest
                        && body.fetchData(topicNames)!!.containsKey(TopicIdPartition(topicId, tp0))
            },
            response = fetchResponse(
                partition = tp0,
                fetchOffset = 1,
                count = 1,
            ),
            node = node,
        )
        time.sleep(heartbeatIntervalMs.toLong())
        Thread.sleep(heartbeatIntervalMs.toLong())
        consumer.updateAssignmentMetadataIfNeeded(time.timer(Long.MAX_VALUE))
        val records = consumer.poll(Duration.ZERO)
        assertFalse(records.isEmpty)
    }

    @Throws(Exception::class)
    private fun consumerCloseTest(
        closeTimeoutMs: Long,
        responses: List<AbstractResponse>,
        waitMs: Long,
        interrupt: Boolean,
    ) {
        val metadata = createMetadata(subscription)
        val client = MockClient(time, metadata)
        initMetadata(client, mapOf(topic to 1))
        val node = metadata.fetch().nodes[0]
        val consumer = newConsumer(
            time = time,
            client = client,
            subscription = subscription,
            metadata = metadata,
            assignor = assignor,
            autoCommitEnabled = false,
            groupInstanceId = null,
        )
        consumer.subscribe(
            topics = setOf(topic),
            callback = getConsumerRebalanceListener(consumer),
        )
        val coordinator = prepareRebalance(
            client = client,
            node = node,
            assignor = assignor,
            partitions = listOf(tp0),
            coordinator = null,
        )
        client.prepareMetadataUpdate(
            metadataUpdateWithIds(
                numNodes = 1,
                topicPartitionCounts = mapOf(topic to 1),
                topicIds = topicIds,
            )
        )
        consumer.updateAssignmentMetadataIfNeeded(time.timer(Long.MAX_VALUE))

        // Poll with responses
        client.prepareResponseFrom(
            response = fetchResponse(
                partition = tp0,
                fetchOffset = 0,
                count = 1,
            ),
            node = node,
        )
        client.prepareResponseFrom(
            response = fetchResponse(
                partition = tp0,
                fetchOffset = 1,
                count = 0,
            ),
            node = node,
        )
        consumer.poll(Duration.ZERO)

        // Initiate close() after a commit request on another thread.
        // Kafka consumer is single-threaded, but the implementation allows calls on a
        // different thread as long as the calls are not executed concurrently. So this is safe.
        val executor = Executors.newSingleThreadExecutor()
        val closeException = AtomicReference<Exception?>()
        try {
            val future = executor.submit {
                consumer.commitAsync()
                try {
                    consumer.close(Duration.ofMillis(closeTimeoutMs))
                } catch (e: Exception) {
                    closeException.set(e)
                }
            }

            // Close task should not complete until commit succeeds or close times out
            // if close timeout is not zero.
            try {
                future[100, TimeUnit.MILLISECONDS]
                assertEquals(
                    expected = 0L,
                    actual = closeTimeoutMs,
                    message = "Close completed without waiting for commit or leave response",
                )
            } catch (e: TimeoutException) {
                // Expected exception
            }

            // Ensure close has started and queued at least one more request after commitAsync.
            //
            // Close enqueues two requests, but second is enqueued only after first has succeeded. First is
            // LEAVE_GROUP as part of coordinator close and second is FETCH with epoch=FINAL_EPOCH. At this stage
            // we expect only the first one to have been requested. Hence, waiting for total 2 requests, one for
            // commit and another for LEAVE_GROUP.
            client.waitForRequests(2, 1000)

            // In graceful mode, commit response results in close() completing immediately without a timeout
            // In non-graceful mode, close() times out without an exception even though commit response is pending

            // In graceful mode, commit response results in close() completing immediately without a timeout
            // In non-graceful mode, close() times out without an exception even though commit response is pending
            val nonCloseRequests = 1
            for (i in responses.indices) {
                client.waitForRequests(1, 1000)
                if (i == responses.size - 1 && responses[i] is FetchResponse)
                    // last request is the close session request which is sent to the leader of the partition.
                    client.respondFrom(responses[i], node)
                else client.respondFrom(responses[i], coordinator)
                if (i < nonCloseRequests) {
                    // the close request should not complete until non-close requests (commit requests) have completed.
                    assertFailsWith<TimeoutException>("Close completed without waiting for response") {
                        future[100, TimeUnit.MILLISECONDS]
                    }
                }
            }
            if (waitMs > 0) time.sleep(waitMs)
            if (interrupt) {
                assertTrue(future.cancel(true), "Close terminated prematurely")
                TestUtils.waitForCondition(
                    testCondition = { closeException.get() != null },
                    conditionDetails = "InterruptException did not occur within timeout.",
                )
                assertIs<InterruptException>(
                    value = closeException.get(),
                    message = "Expected exception not thrown $closeException",
                )
            } else {
                future[closeTimeoutMs, TimeUnit.MILLISECONDS] // Should succeed without TimeoutException or ExecutionException
                assertNull(closeException.get(), "Unexpected exception during close")
            }
        } finally {
            executor.shutdownNow()
        }
    }

    @Test
    fun testPartitionsForNonExistingTopic() {
        val metadata = createMetadata(subscription)
        val client = MockClient(time, metadata)
        initMetadata(client, mapOf(topic to 1))
        val cluster = metadata.fetch()
        val updateResponse = metadataResponse(
            brokers = cluster.nodes,
            clusterId = cluster.clusterResource.clusterId,
            controllerId = cluster.controller!!.id,
            topicMetadataList = emptyList(),
        )
        client.prepareResponse(updateResponse)
        val consumer = newConsumer(
            time = time,
            client = client,
            subscription = subscription,
            metadata = metadata,
            assignor = assignor,
            autoCommitEnabled = true,
            groupInstanceId = groupInstanceId,
        )
        assertEquals(emptyList(), consumer.partitionsFor("non-exist-topic"))
    }

    @Test
    fun testPartitionsForAuthenticationFailure() {
        val consumer = consumerWithPendingAuthenticationError()
        assertFailsWith<AuthenticationException> { consumer.partitionsFor("some other topic") }
    }

    @Test
    fun testBeginningOffsetsAuthenticationFailure() {
        val consumer = consumerWithPendingAuthenticationError()
        assertFailsWith<AuthenticationException> { consumer.beginningOffsets(setOf(tp0)) }
    }

    @Test
    fun testEndOffsetsAuthenticationFailure() {
        val consumer = consumerWithPendingAuthenticationError()
        assertFailsWith<AuthenticationException> { consumer.endOffsets(setOf(tp0)) }
    }

    @Test
    fun testPollAuthenticationFailure() {
        val consumer = consumerWithPendingAuthenticationError()
        consumer.subscribe(setOf(topic))
        assertFailsWith<AuthenticationException> { consumer.poll(Duration.ZERO) }
    }

    @Test
    fun testOffsetsForTimesAuthenticationFailure() {
        val consumer = consumerWithPendingAuthenticationError()
        assertFailsWith<AuthenticationException> { consumer.offsetsForTimes(mapOf(tp0 to 0L)) }
    }

    @Test
    fun testCommitSyncAuthenticationFailure() {
        val consumer = consumerWithPendingAuthenticationError()
        val offsets = mapOf(tp0 to OffsetAndMetadata(10L))
        assertFailsWith<AuthenticationException> { consumer.commitSync(offsets) }
    }

    @Test
    fun testCommittedAuthenticationFailure() {
        val consumer = consumerWithPendingAuthenticationError()
        assertFailsWith<AuthenticationException> { consumer.committed(setOf(tp0))[tp0] }
    }

    @Test
    fun testMeasureCommitSyncDurationOnFailure() {
        val consumer = consumerWithPendingError(MockTime(Duration.ofSeconds(1).toMillis()))
        try {
            consumer.commitSync(mapOf(tp0 to OffsetAndMetadata(10L)))
        } catch (_: RuntimeException) {
        }

        val metric = consumer.metrics()[
            consumer.metrics.metricName(
                name = "commit-sync-time-ns-total",
                group = "consumer-metrics"
            )
        ]!!
        assertTrue(metric.metricValue() as Double >= Duration.ofMillis(999).toNanos())
    }

    @Test
    fun testMeasureCommitSyncDuration() {
        val time: Time = MockTime(Duration.ofSeconds(1).toMillis())
        val subscription = SubscriptionState(LogContext(), OffsetResetStrategy.EARLIEST)
        val metadata = createMetadata(subscription)
        val client = MockClient(time, metadata)
        initMetadata(client, mapOf(topic to 2))
        val node = metadata.fetch().nodes[0]
        val consumer = newConsumer(
            time = time,
            client = client,
            subscription = subscription,
            metadata = metadata,
            assignor = assignor,
            autoCommitEnabled = true,
            groupInstanceId = groupInstanceId,
        )
        consumer.assign(listOf(tp0))
        client.prepareResponseFrom(
            response = prepareResponse(
                error = Errors.NONE,
                key = groupId,
                node = node,
            ),
            node = node,
        )
        val coordinator = Node(Int.MAX_VALUE - node.id, node.host, node.port)
        client.prepareResponseFrom(
            response = offsetCommitResponse(mapOf(tp0 to Errors.NONE)),
            node = coordinator,
        )
        consumer.commitSync(mapOf(tp0 to OffsetAndMetadata(10L)))
        val metric = consumer.metrics()[
            consumer.metrics.metricName(
                name = "commit-sync-time-ns-total",
                group = "consumer-metrics",
            )
        ]!!
        assertTrue((metric.metricValue() as Double?)!! >= Duration.ofMillis(999).toNanos())
    }

    @Test
    fun testMeasureCommittedDurationOnFailure() {
        val consumer = consumerWithPendingError(MockTime(Duration.ofSeconds(1).toMillis()))
        try {
            consumer.committed(setOf(tp0))
        } catch (_: RuntimeException) {
        }

        val metric = consumer.metrics()[
            consumer.metrics.metricName(
                name = "committed-time-ns-total",
                group = "consumer-metrics",
            )
        ]!!
        assertTrue(metric.metricValue() as Double >= Duration.ofMillis(999).toNanos())
    }

    @Test
    fun testMeasureCommittedDuration() {
        val offset1: Long = 10000
        val time = MockTime(Duration.ofSeconds(1).toMillis())
        val subscription = SubscriptionState(LogContext(), OffsetResetStrategy.EARLIEST)
        val metadata = createMetadata(subscription)
        val client = MockClient(time, metadata)
        initMetadata(client, mapOf(topic to 2))
        val node = metadata.fetch().nodes[0]
        val consumer = newConsumer(
            time = time,
            client = client,
            subscription = subscription,
            metadata = metadata,
            assignor = assignor,
            autoCommitEnabled = true,
            groupInstanceId = groupInstanceId,
        )
        consumer.assign(listOf(tp0))

        // lookup coordinator
        client.prepareResponseFrom(
            response = prepareResponse(
                error = Errors.NONE,
                key = groupId,
                node = node,
            ),
            node = node,
        )
        val coordinator = Node(
            id = Int.MAX_VALUE - node.id,
            host = node.host,
            port = node.port,
        )

        // fetch offset for one topic
        client.prepareResponseFrom(
            response = offsetResponse(offsets = mapOf(tp0 to offset1), error = Errors.NONE),
            node = coordinator,
        )
        consumer.committed(setOf(tp0))[tp0]!!
        val metric = consumer.metrics()[
            consumer.metrics.metricName(
                name = "committed-time-ns-total",
                group = "consumer-metrics"
            )
        ]!!
        assertTrue(metric.metricValue() as Double >= Duration.ofMillis(999).toNanos())
    }

    @Test
    fun testRebalanceException() {
        val metadata = createMetadata(subscription)
        val client = MockClient(time, metadata)
        initMetadata(client, mapOf(topic to 1))
        val node = metadata.fetch().nodes[0]
        val consumer = newConsumer(
            time = time,
            client = client,
            subscription = subscription,
            metadata = metadata,
            assignor = assignor,
            autoCommitEnabled = true,
            groupInstanceId = groupInstanceId,
        )
        consumer.subscribe(setOf(topic), exceptionConsumerRebalanceListener)
        val coordinator = Node(Int.MAX_VALUE - node.id, node.host, node.port)
        client.prepareResponseFrom(
            response = prepareResponse(
                error = Errors.NONE,
                key = groupId,
                node = node,
            ),
            node = node,
        )
        client.prepareResponseFrom(
            response = joinGroupFollowerResponse(
                assignor = assignor,
                generationId = 1,
                memberId = memberId,
                leaderId = leaderId,
                error = Errors.NONE,
            ),
            node = coordinator,
        )
        client.prepareResponseFrom(
            response = syncGroupResponse(
                partitions = listOf(tp0),
                error = Errors.NONE,
            ),
            node = coordinator,
        )

        // assign throws
        val e = assertFailsWith<Throwable>(message = "Should throw exception") {
            consumer.updateAssignmentMetadataIfNeeded(time.timer(Long.MAX_VALUE))
        }
        assertEquals(partitionAssigned + singleTopicPartition, e.cause!!.message)

        // the assignment is still updated regardless of the exception
        assertEquals(setOf(tp0), subscription.assignedPartitions())

        // close's revoke throws
        val error = assertFailsWith<Throwable>("Should throw exception") {
            consumer.close(Duration.ofMillis(0))
        }
        assertEquals(partitionRevoked + singleTopicPartition, error.cause!!.cause!!.message)
        consumer.close(Duration.ofMillis(0))

        // the assignment is still updated regardless of the exception
        assertTrue(subscription.assignedPartitions().isEmpty())
    }

    @Test
    @Throws(InterruptedException::class)
    fun testReturnRecordsDuringRebalance() {
        val time: Time = MockTime(1L)
        val metadata = createMetadata(subscription)
        val client = MockClient(time, metadata)
        val assignor = CooperativeStickyAssignor()
        val consumer = newConsumer(
            time = time,
            client = client,
            subscription = subscription,
            metadata = metadata,
            assignor = assignor,
            autoCommitEnabled = true,
            groupInstanceId = groupInstanceId,
        )
        initMetadata(
            mockClient = client,
            partitionCounts = mapOf(topic to 1, topic2 to 1, topic3 to 1),
        )
        consumer.subscribe(listOf(topic, topic2), getConsumerRebalanceListener(consumer))
        val node = metadata.fetch().nodes[0]
        val coordinator = prepareRebalance(
            client = client,
            node = node,
            assignor = assignor,
            partitions = listOf(tp0, t2p0),
            coordinator = null,
        )

        // a poll with non-zero milliseconds would complete three round-trips (discover, join, sync)
        TestUtils.waitForCondition(
            testCondition = {
                consumer.poll(Duration.ofMillis(100L))
                consumer.assignment() == setOf(tp0, t2p0)
            },
            conditionDetails = "Does not complete rebalance in time",
        )
        assertEquals(setOf(topic, topic2), consumer.subscription())
        assertEquals(setOf(tp0, t2p0), consumer.assignment())

        // prepare a response of the outstanding fetch so that we have data available on the next poll
        val fetches1 = mutableMapOf(
            tp0 to FetchInfo(offset = 0, count = 1),
            t2p0 to FetchInfo(offset = 0, count = 10),
        )
        client.respondFrom(fetchResponse(fetches1), node)
        var records = consumer.poll(Duration.ZERO)

        // verify that the fetch occurred as expected
        assertEquals(11, records.count())
        assertEquals(1L, consumer.position(tp0))
        assertEquals(10L, consumer.position(t2p0))

        // prepare the next response of the prefetch
        fetches1.clear()
        fetches1[tp0] = FetchInfo(offset = 1, count = 1)
        fetches1[t2p0] = FetchInfo(offset = 10, count = 20)
        client.respondFrom(fetchResponse(fetches1), node)

        // subscription change
        consumer.subscribe(
            topics = listOf(topic, topic3),
            callback = getConsumerRebalanceListener(consumer),
        )

        // verify that subscription has changed but assignment is still unchanged
        assertEquals(setOf(topic, topic3), consumer.subscription())
        assertEquals(setOf(tp0, t2p0), consumer.assignment())

        // mock the offset commit response for to be revoked partitions
        val partitionOffsets1 = mapOf(t2p0 to 10L)
        val commitReceived = prepareOffsetCommitResponse(
            client = client,
            coordinator = coordinator,
            partitionOffsets = partitionOffsets1,
        )

        // poll once which would not complete the rebalance
        records = consumer.poll(Duration.ZERO)

        // clear out the prefetch so it doesn't interfere with the rest of the test
        fetches1.clear()
        fetches1[tp0] = FetchInfo(offset = 2, count = 1)
        client.respondFrom(response = fetchResponse(fetches1), node = node)

        // verify that the fetch still occurred as expected
        assertEquals(setOf(topic, topic3), consumer.subscription())
        assertEquals(setOf(tp0), consumer.assignment())
        assertEquals(1, records.count())
        assertEquals(2L, consumer.position(tp0))

        // verify that the offset commits occurred as expected
        assertTrue(commitReceived.get())

        // mock rebalance responses
        client.respondFrom(
            response = joinGroupFollowerResponse(
                assignor = assignor,
                generationId = 2,
                memberId = "memberId",
                leaderId = "leaderId",
                error = Errors.NONE,
            ),
            node = coordinator,
        )

        // we need to poll 1) for getting the join response, and then send the sync request;
        //                 2) for getting the sync response
        records = consumer.poll(Duration.ZERO)

        // should not finish the response yet
        assertEquals(setOf(topic, topic3), consumer.subscription())
        assertEquals(setOf(tp0), consumer.assignment())
        assertEquals(1, records.count())
        assertEquals(3L, consumer.position(tp0))
        fetches1.clear()
        fetches1[tp0] = FetchInfo(offset = 3, count = 1)
        client.respondFrom(response = fetchResponse(fetches1), node = node)

        // now complete the rebalance
        client.respondFrom(
            response = syncGroupResponse(
                partitions = listOf(tp0, t3p0),
                error = Errors.NONE,
            ),
            node = coordinator
        )
        val count = AtomicInteger(0)
        TestUtils.waitForCondition(
            testCondition = {
                val recs = consumer.poll(Duration.ofMillis(100L))
                consumer.assignment() == setOf(tp0, t3p0) && count.addAndGet(recs.count()) == 1
            },
            conditionDetails = "Does not complete rebalance in time",
        )

        // should have t3 but not sent yet the t3 records
        assertEquals(setOf(topic, topic3), consumer.subscription())
        assertEquals(setOf(tp0, t3p0), consumer.assignment())
        assertEquals(4L, consumer.position(tp0))
        assertEquals(0L, consumer.position(t3p0))
        fetches1.clear()
        fetches1[tp0] = FetchInfo(offset = 4, count = 1)
        fetches1[t3p0] = FetchInfo(offset = 0, count = 100)
        client.respondFrom(fetchResponse(fetches1), node)
        count.set(0)
        TestUtils.waitForCondition(
            testCondition = {
                val recs = consumer.poll(Duration.ofMillis(100L))
                count.addAndGet(recs.count()) == 101
            },
            conditionDetails = "Does not complete rebalance in time",
        )
        assertEquals(5L, consumer.position(tp0))
        assertEquals(100L, consumer.position(t3p0))
        client.requests().clear()
        consumer.unsubscribe()
        consumer.close(Duration.ZERO)
    }

    @Test
    fun testGetGroupMetadata() {
        val metadata = createMetadata(subscription)
        val client = MockClient(time, metadata)
        initMetadata(client, mapOf(topic to 1))
        val node = metadata.fetch().nodes[0]
        val consumer = newConsumer(
            time = time,
            client = client,
            subscription = subscription,
            metadata = metadata,
            assignor = assignor,
            autoCommitEnabled = true,
            groupInstanceId = groupInstanceId,
        )
        val groupMetadata = consumer.groupMetadata()
        assertEquals(groupId, groupMetadata.groupId)
        assertEquals(JoinGroupRequest.UNKNOWN_MEMBER_ID, groupMetadata.memberId)
        assertEquals(JoinGroupRequest.UNKNOWN_GENERATION_ID, groupMetadata.generationId)
        assertEquals(groupInstanceId, groupMetadata.groupInstanceId)
        consumer.subscribe(setOf(topic), getConsumerRebalanceListener(consumer))
        prepareRebalance(client, node, assignor, listOf(tp0), null)

        // initial fetch
        client.prepareResponseFrom(
            response = fetchResponse(
                partition = tp0,
                fetchOffset = 0,
                count = 0,
            ),
            node = node,
        )
        consumer.updateAssignmentMetadataIfNeeded(time.timer(Long.MAX_VALUE))
        val groupMetadata2 = consumer.groupMetadata()
        assertEquals(groupId, groupMetadata2.groupId)
        assertEquals(memberId, groupMetadata2.memberId)
        assertEquals(1, groupMetadata2.generationId)
        assertEquals(groupInstanceId, groupMetadata2.groupInstanceId)
    }

    @Test
    @Throws(InterruptedException::class)
    fun testInvalidGroupMetadata() {
        val metadata = createMetadata(subscription)
        val client = MockClient(time, metadata)
        initMetadata(client, mapOf(topic to 1))
        val consumer = newConsumer(
            time, client, subscription, metadata,
            RoundRobinAssignor(), true, groupInstanceId
        )
        consumer.subscribe(listOf(topic))
        // concurrent access is illegal
        client.enableBlockingUntilWakeup(1)
        val service = Executors.newSingleThreadExecutor()
        service.execute { consumer.poll(Duration.ofSeconds(5)) }
        try {
            TimeUnit.SECONDS.sleep(1)
            assertFailsWith<ConcurrentModificationException> { consumer.groupMetadata() }
            client.wakeup()
            consumer.wakeup()
        } finally {
            service.shutdown()
            assertTrue(service.awaitTermination(10, TimeUnit.SECONDS))
        }

        // accessing closed consumer is illegal
        consumer.close(Duration.ZERO)
        assertFailsWith<IllegalStateException> { consumer.groupMetadata() }
    }

    @Test
    fun testCurrentLag() {
        val metadata = createMetadata(subscription)
        val client = MockClient(time, metadata)
        initMetadata(client, mapOf(topic to 1))
        consumer = newConsumer(
            time = time,
            client = client,
            subscription = subscription,
            metadata = metadata,
            assignor = assignor,
            autoCommitEnabled = true,
            groupInstanceId = groupInstanceId,
        )

        // throws for unassigned partition
        assertFailsWith<IllegalStateException> { consumer.currentLag(tp0) }
        consumer.assign(setOf(tp0))

        // poll once to update with the current metadata
        consumer.poll(Duration.ofMillis(0))
        client.respond(
            response = prepareResponse(
                error = Errors.NONE,
                key = groupId,
                node = metadata.fetch().nodes[0],
            ),
        )

        // no error for no current position
        assertEquals(null, consumer.currentLag(tp0))
        assertEquals(0, client.inFlightRequestCount())

        // poll once again, which should send the list-offset request
        consumer.seek(tp0, 50L)
        consumer.poll(Duration.ofMillis(0))
        // requests: list-offset, fetch
        assertEquals(2, client.inFlightRequestCount())

        // no error for no end offset (so unknown lag)
        assertEquals(null, consumer.currentLag(tp0))

        // poll once again, which should return the list-offset response
        // and hence next call would return correct lag result
        client.respond(
            response = listOffsetsResponse(mapOf(tp0 to 90L)),
        )
        consumer.poll(Duration.ofMillis(0))
        assertEquals(40L, consumer.currentLag(tp0))
        // requests: fetch
        assertEquals(1, client.inFlightRequestCount())

        // one successful fetch should update the log end offset and the position
        val fetchInfo = FetchInfo(
            logFirstOffset = 1L,
            logLastOffset = 99L,
            offset = 50L,
            count = 5
        )
        client.respond(
            response = fetchResponse(mapOf(tp0 to fetchInfo)),
        )
        val records = consumer.poll(Duration.ofMillis(1))
        assertEquals(5, records.count())
        assertEquals(55L, consumer.position(tp0))

        // correct lag result
        assertEquals(45L, consumer.currentLag(tp0))
    }

    @Test
    fun testListOffsetShouldUpdateSubscriptions() {
        val metadata = createMetadata(subscription)
        val client = MockClient(time, metadata)
        initMetadata(client, mapOf(topic to 1))
        consumer = newConsumer(
            time = time,
            client = client,
            subscription = subscription,
            metadata = metadata,
            assignor = assignor,
            autoCommitEnabled = true,
            groupInstanceId = groupInstanceId,
        )
        consumer.assign(setOf(tp0))

        // poll once to update with the current metadata
        consumer.poll(Duration.ofMillis(0))
        client.respond(
            response = prepareResponse(
                error = Errors.NONE,
                key = groupId,
                node = metadata.fetch().nodes[0],
            ),
        )
        consumer.seek(tp0, 50L)
        client.prepareResponse(
            response = listOffsetsResponse(mapOf(tp0 to 90L)),
        )
        assertEquals(mapOf(tp0 to 90L), consumer.endOffsets(setOf(tp0)))
        // correct lag result should be returned as well
        assertEquals(40L, consumer.currentLag(tp0))
    }

    private fun consumerWithPendingAuthenticationError(time: Time = MockTime()): KafkaConsumer<String?, String?> {
        val metadata = createMetadata(subscription)
        val client = MockClient(time, metadata)
        initMetadata(client, mapOf(topic to 1))
        val node = metadata.fetch().nodes[0]
        val assignor = RangeAssignor()
        client.createPendingAuthenticationError(node = node, backoffMs = 0)
        return newConsumer(
            time = time,
            client = client,
            subscription = subscription,
            metadata = metadata,
            assignor = assignor,
            autoCommitEnabled = false,
            groupInstanceId = groupInstanceId,
        )
    }

    private fun consumerWithPendingError(time: Time): KafkaConsumer<String?, String?> =
        consumerWithPendingAuthenticationError(time)

    private fun getConsumerRebalanceListener(consumer: KafkaConsumer<*, *>): ConsumerRebalanceListener {
        return object : ConsumerRebalanceListener {

            override fun onPartitionsRevoked(partitions: Collection<TopicPartition>) = Unit

            override fun onPartitionsAssigned(partitions: Collection<TopicPartition>) {
                // set initial position so we don't need a lookup
                for (partition in partitions) consumer.seek(partition, 0)
            }
        }
    }

    private val exceptionConsumerRebalanceListener: ConsumerRebalanceListener
        get() = object : ConsumerRebalanceListener {

            override fun onPartitionsRevoked(partitions: Collection<TopicPartition>) =
                throw RuntimeException(partitionRevoked + partitions)

            override fun onPartitionsAssigned(partitions: Collection<TopicPartition>) =
                throw RuntimeException(partitionAssigned + partitions)

            override fun onPartitionsLost(partitions: Collection<TopicPartition>) =
                throw RuntimeException(partitionLost + partitions)
        }

    private fun createMetadata(subscription: SubscriptionState): ConsumerMetadata {
        return ConsumerMetadata(
            refreshBackoffMs = 0,
            metadataExpireMs = Long.MAX_VALUE,
            includeInternalTopics = false,
            allowAutoTopicCreation = false,
            subscription = subscription,
            logContext = LogContext(),
            clusterResourceListeners = ClusterResourceListeners(),
        )
    }

    private fun prepareRebalance(
        client: MockClient,
        node: Node,
        subscribedTopics: Set<String>,
        assignor: ConsumerPartitionAssignor,
        partitions: List<TopicPartition>,
        coordinator: Node?,
    ): Node {
        val actualCoordinator = coordinator ?: run {
            // lookup coordinator
            client.prepareResponseFrom(
                response = prepareResponse(
                    error = Errors.NONE,
                    key = groupId,
                    node = node
                ),
                node = node,
            )
            Node(
                id = Int.MAX_VALUE - node.id,
                host = node.host,
                port = node.port,
            )
        }

        // join group
        client.prepareResponseFrom(
            matcher = { body ->
                val joinGroupRequest = assertIs<JoinGroupRequest>(body)
                val protocolIterator = joinGroupRequest.data().protocols.iterator()
                assertTrue(protocolIterator.hasNext())
                val protocolMetadata = ByteBuffer.wrap(protocolIterator.next().metadata)
                val subscription = deserializeSubscription(protocolMetadata)
                subscribedTopics == subscription.topics.toSet()
            },
            response = joinGroupFollowerResponse(
                assignor = assignor,
                generationId = 1,
                memberId = memberId,
                leaderId = leaderId,
                error = Errors.NONE,
            ),
            node = actualCoordinator,
        )

        // sync group
        client.prepareResponseFrom(
            response = syncGroupResponse(partitions = partitions, error = Errors.NONE),
            node = actualCoordinator,
        )
        return actualCoordinator
    }

    private fun prepareRebalance(
        client: MockClient,
        node: Node,
        assignor: ConsumerPartitionAssignor,
        partitions: List<TopicPartition>,
        coordinator: Node?,
    ): Node {
        val actualCoordinator = coordinator ?: run {
            // lookup coordinator
            client.prepareResponseFrom(
                response = prepareResponse(
                    error = Errors.NONE,
                    key = groupId,
                    node = node,
                ),
                node = node,
            )
            Node(
                id = Int.MAX_VALUE - node.id,
                host = node.host,
                port = node.port,
            )
        }

        // join group
        client.prepareResponseFrom(
            response = joinGroupFollowerResponse(
                assignor = assignor,
                generationId = 1,
                memberId = memberId,
                leaderId = leaderId,
                error = Errors.NONE,
            ),
            node = actualCoordinator,
        )

        // sync group
        client.prepareResponseFrom(
            response = syncGroupResponse(partitions = partitions, error = Errors.NONE),
            node = actualCoordinator,
        )
        return actualCoordinator
    }

    private fun prepareHeartbeatResponse(
        client: MockClient,
        coordinator: Node,
        error: Errors,
    ): AtomicBoolean {
        val heartbeatReceived = AtomicBoolean(false)
        client.prepareResponseFrom(
            matcher = {
                heartbeatReceived.set(true)
                true
            },
            response = HeartbeatResponse(HeartbeatResponseData().setErrorCode(error.code)),
            node = coordinator,
        )
        return heartbeatReceived
    }

    private fun prepareOffsetCommitResponse(
        client: MockClient,
        coordinator: Node,
        partitionOffsets: Map<TopicPartition, Long>,
    ): AtomicBoolean {
        val commitReceived = AtomicBoolean(true)
        val response = partitionOffsets.mapValues { Errors.NONE }
        client.prepareResponseFrom(
            matcher = { body ->
                val commitRequest = assertIs<OffsetCommitRequest>(body)
                val commitErrors = commitRequest.offsets()
                for ((key, value) in partitionOffsets) {
                    // verify that the expected offset has been committed
                    if (commitErrors[key] != value) {
                        commitReceived.set(false)
                        return@prepareResponseFrom false
                    }
                }
                true
            },
            response = offsetCommitResponse(response),
            node = coordinator,
        )
        return commitReceived
    }

    private fun prepareOffsetCommitResponse(
        client: MockClient,
        coordinator: Node,
        partition: TopicPartition,
        offset: Long,
    ): AtomicBoolean = prepareOffsetCommitResponse(
        client = client,
        coordinator = coordinator,
        partitionOffsets = mapOf(partition to offset),
    )

    private fun offsetCommitResponse(
        responseData: Map<TopicPartition, Errors>,
    ): OffsetCommitResponse = OffsetCommitResponse(responseData)

    private fun joinGroupFollowerResponse(
        assignor: ConsumerPartitionAssignor,
        generationId: Int,
        memberId: String,
        leaderId: String,
        error: Errors,
    ): JoinGroupResponse = JoinGroupResponse(
        JoinGroupResponseData()
            .setErrorCode(error.code)
            .setGenerationId(generationId)
            .setProtocolName(assignor.name())
            .setLeader(leaderId)
            .setMemberId(memberId)
            .setMembers(emptyList()),
        ApiKeys.JOIN_GROUP.latestVersion()
    )

    private fun syncGroupResponse(partitions: List<TopicPartition>, error: Errors): SyncGroupResponse {
        val buf = serializeAssignment(ConsumerPartitionAssignor.Assignment(partitions))
        return SyncGroupResponse(
            SyncGroupResponseData()
                .setErrorCode(error.code)
                .setAssignment(Utils.toArray(buf))
        )
    }

    private fun offsetResponse(offsets: Map<TopicPartition, Long>, error: Errors): OffsetFetchResponse {
        val partitionData = offsets.mapValues { (_, value) ->
            OffsetFetchResponse.PartitionData(
                offset = value,
                leaderEpoch = null,
                metadata = "",
                error = error,
            )
        }
        return OffsetFetchResponse(
            throttleTimeMs = throttleMs,
            errors = mapOf(groupId to Errors.NONE),
            responseData = mapOf(groupId to partitionData),
        )
    }

    private fun listOffsetsResponse(
        offsets: Map<TopicPartition, Long>,
    ): ListOffsetsResponse = listOffsetsResponse(
        partitionOffsets = offsets,
        partitionErrors = emptyMap(),
    )

    private fun listOffsetsResponse(
        partitionOffsets: Map<TopicPartition, Long>,
        partitionErrors: Map<TopicPartition, Errors>,
    ): ListOffsetsResponse {
        val responses: MutableMap<String, ListOffsetsTopicResponse> = HashMap()
        for ((tp, value) in partitionOffsets) {
            val topic = responses.computeIfAbsent(tp.topic) { ListOffsetsTopicResponse().setName(tp.topic) }
            topic.partitions += ListOffsetsPartitionResponse()
                .setPartitionIndex(tp.partition)
                .setErrorCode(Errors.NONE.code)
                .setTimestamp(ListOffsetsResponse.UNKNOWN_TIMESTAMP)
                .setOffset(value)
        }
        for ((tp, value) in partitionErrors) {
            val topic = responses.computeIfAbsent(tp.topic) { ListOffsetsTopicResponse().setName(tp.topic) }
            topic.partitions += ListOffsetsPartitionResponse()
                .setPartitionIndex(tp.partition)
                .setErrorCode(value.code)
                .setTimestamp(ListOffsetsResponse.UNKNOWN_TIMESTAMP)
                .setOffset(ListOffsetsResponse.UNKNOWN_OFFSET)
        }
        val data = ListOffsetsResponseData().setTopics(responses.values.toList())
        return ListOffsetsResponse(data)
    }

    private fun fetchResponse(fetches: Map<TopicPartition, FetchInfo>): FetchResponse {
        val tpResponses = LinkedHashMap<TopicIdPartition, FetchResponseData.PartitionData>()
        for ((partition, value) in fetches) {
            val fetchOffset = value.offset
            val fetchCount = value.count
            val highWatermark = value.logLastOffset + 1
            val logStartOffset = value.logFirstOffset
            val records: MemoryRecords = if (fetchCount == 0) MemoryRecords.EMPTY else {
                val builder: MemoryRecordsBuilder = MemoryRecords.builder(
                    buffer = ByteBuffer.allocate(1024),
                    compressionType = CompressionType.NONE,
                    timestampType = TimestampType.CREATE_TIME,
                    baseOffset = fetchOffset,
                )
                for (i in 0 until fetchCount) builder.append(
                    timestamp = 0L,
                    key = "key-$i".toByteArray(),
                    value = "value-$i".toByteArray(),
                )
                builder.build()
            }
            tpResponses[TopicIdPartition(topicIds[partition.topic]!!, partition)] = FetchResponseData.PartitionData()
                .setPartitionIndex(partition.partition)
                .setHighWatermark(highWatermark)
                .setLogStartOffset(logStartOffset)
                .setRecords(records)
        }
        return FetchResponse.of(
            error = Errors.NONE,
            throttleTimeMs = 0,
            sessionId = FetchMetadata.INVALID_SESSION_ID,
            responseData = tpResponses,
        )
    }

    private fun fetchResponse(partition: TopicPartition, fetchOffset: Long, count: Int): FetchResponse {
        return fetchResponse(fetches = mapOf(partition to FetchInfo(offset = fetchOffset, count = count)))
    }

    private fun newConsumer(
        time: Time,
        client: KafkaClient,
        subscription: SubscriptionState,
        metadata: ConsumerMetadata,
        assignor: ConsumerPartitionAssignor,
        autoCommitEnabled: Boolean,
        groupInstanceId: String?,
    ): KafkaConsumer<String?, String?> = newConsumer(
        time = time,
        client = client,
        subscription = subscription,
        metadata = metadata,
        assignor = assignor,
        autoCommitEnabled = autoCommitEnabled,
        groupId = groupId,
        groupInstanceId = groupInstanceId,
        throwOnStableOffsetNotSupported = false,
    )

    private fun newConsumerNoAutoCommit(
        time: Time,
        client: KafkaClient,
        subscription: SubscriptionState,
        metadata: ConsumerMetadata,
    ): KafkaConsumer<String?, String?> = newConsumer(
        time = time,
        client = client,
        subscription = subscription,
        metadata = metadata,
        assignor = RangeAssignor(),
        autoCommitEnabled = false,
        groupId = groupId,
        groupInstanceId = groupInstanceId,
        throwOnStableOffsetNotSupported = false,
    )

    private fun newConsumer(
        time: Time,
        client: KafkaClient,
        subscription: SubscriptionState,
        metadata: ConsumerMetadata,
        assignor: ConsumerPartitionAssignor,
        autoCommitEnabled: Boolean,
        groupId: String?,
        groupInstanceId: String?,
        throwOnStableOffsetNotSupported: Boolean,
    ): KafkaConsumer<String?, String?> = newConsumer(
        time = time,
        client = client,
        subscription = subscription,
        metadata = metadata,
        assignor = assignor,
        autoCommitEnabled = autoCommitEnabled,
        groupId = groupId,
        groupInstanceId = groupInstanceId,
        valueDeserializer = StringDeserializer(),
        throwOnStableOffsetNotSupported = throwOnStableOffsetNotSupported,
    )

    private fun newConsumer(
        time: Time,
        client: KafkaClient,
        subscription: SubscriptionState,
        metadata: ConsumerMetadata,
        assignor: ConsumerPartitionAssignor,
        autoCommitEnabled: Boolean,
        groupId: String?,
        groupInstanceId: String?,
        valueDeserializer: Deserializer<String?>?,
        throwOnStableOffsetNotSupported: Boolean,
    ): KafkaConsumer<String?, String?> {
        val clientId = "mock-consumer"
        val metricGroupPrefix = "consumer"
        val retryBackoffMs: Long = 100
        val minBytes = 1
        val maxBytes = Int.MAX_VALUE
        val maxWaitMs = 500
        val fetchSize = 1024 * 1024
        val maxPollRecords = Int.MAX_VALUE
        val checkCrcs = true
        val rebalanceTimeoutMs = 60000
        val keyDeserializer = StringDeserializer()
        val deserializer = valueDeserializer ?: StringDeserializer()
        val assignors = listOf(assignor)
        val interceptors = ConsumerInterceptors<String?, String?>(emptyList())
        val metrics = Metrics(time = time)
        val metricsRegistry = ConsumerMetrics(metricGrpPrefix = metricGroupPrefix)
        val loggerFactory = LogContext()
        val consumerClient = ConsumerNetworkClient(
            logContext = loggerFactory,
            client = client,
            metadata = metadata,
            time = time,
            retryBackoffMs = retryBackoffMs,
            requestTimeoutMs = requestTimeoutMs,
            maxPollTimeoutMs = heartbeatIntervalMs
        )
        var consumerCoordinator: ConsumerCoordinator? = null
        if (groupId != null) {
            val rebalanceConfig = GroupRebalanceConfig(
                sessionTimeoutMs = sessionTimeoutMs,
                rebalanceTimeoutMs = rebalanceTimeoutMs,
                heartbeatIntervalMs = heartbeatIntervalMs,
                groupId = groupId,
                groupInstanceId = groupInstanceId,
                retryBackoffMs = retryBackoffMs,
                leaveGroupOnClose = true,
            )
            consumerCoordinator = ConsumerCoordinator(
                rebalanceConfig = rebalanceConfig,
                logContext = loggerFactory,
                client = consumerClient,
                assignors = assignors,
                metadata = metadata,
                subscriptions = subscription,
                metrics = metrics,
                metricGrpPrefix = metricGroupPrefix,
                time = time,
                autoCommitEnabled = autoCommitEnabled,
                autoCommitIntervalMs = autoCommitIntervalMs,
                interceptors = interceptors,
                throwOnFetchStableOffsetsUnsupported = throwOnStableOffsetNotSupported,
                rackId = null,
            )
        }
        val isolationLevel = IsolationLevel.READ_UNCOMMITTED
        val metricsManager = FetchMetricsManager(metrics, metricsRegistry.fetcherMetrics)
        val fetchConfig = FetchConfig(
            minBytes = minBytes,
            maxBytes = maxBytes,
            maxWaitMs = maxWaitMs,
            fetchSize = fetchSize,
            maxPollRecords = maxPollRecords,
            checkCrcs = checkCrcs,
            clientRackId = CommonClientConfigs.DEFAULT_CLIENT_RACK,
            keyDeserializer = keyDeserializer,
            valueDeserializer = deserializer,
            isolationLevel = isolationLevel,
        )
        val fetcher = Fetcher(
            logContext = loggerFactory,
            client = consumerClient,
            metadata = metadata,
            subscriptions = subscription,
            fetchConfig = fetchConfig,
            metricsManager = metricsManager,
            time = time,
        )
        val offsetFetcher = OffsetFetcher(
            logContext = loggerFactory,
            client = consumerClient,
            metadata = metadata,
            subscriptions = subscription,
            time = time,
            retryBackoffMs = retryBackoffMs,
            requestTimeoutMs = requestTimeoutMs.toLong(),
            isolationLevel = isolationLevel,
            apiVersions = ApiVersions(),
        )
        val topicMetadataFetcher = TopicMetadataFetcher(
            logContext = loggerFactory,
            client = consumerClient,
            retryBackoffMs = requestTimeoutMs.toLong(),
        )
        return KafkaConsumer(
            logContext = loggerFactory,
            clientId = clientId,
            coordinator = consumerCoordinator,
            keyDeserializer = keyDeserializer,
            valueDeserializer = deserializer,
            fetcher = fetcher,
            offsetFetcher = offsetFetcher,
            topicMetadataFetcher = topicMetadataFetcher,
            interceptors = interceptors,
            time = time,
            client = consumerClient,
            metrics = metrics,
            subscriptions = subscription,
            metadata = metadata,
            retryBackoffMs = retryBackoffMs,
            requestTimeoutMs = requestTimeoutMs.toLong(),
            defaultApiTimeoutMs = defaultApiTimeoutMs,
            assignors = assignors,
            groupId = groupId,
        )
    }

    private data class FetchInfo(
        val offset: Long,
        val count: Int,
        val logFirstOffset: Long = 0L,
        val logLastOffset: Long = offset + count,
    )

    @Test
    fun testSubscriptionOnInvalidTopic() {
        val metadata = createMetadata(subscription)
        val client = MockClient(time, metadata)
        initMetadata(client, mapOf(topic to 1))
        val cluster = metadata.fetch()
        val invalidTopicName = "topic abc" // Invalid topic name due to space
        val topicMetadata = listOf(
            MetadataResponse.TopicMetadata(
                error = Errors.INVALID_TOPIC_EXCEPTION,
                topic = invalidTopicName,
                isInternal = false,
                partitionMetadata = emptyList(),
            ),
        )
        val updateResponse = metadataResponse(
            brokers = cluster.nodes,
            clusterId = cluster.clusterResource.clusterId,
            controllerId = cluster.controller!!.id,
            topicMetadataList = topicMetadata,
        )
        client.prepareMetadataUpdate(updateResponse)
        val consumer = newConsumer(
            time = time,
            client = client,
            subscription = subscription,
            metadata = metadata,
            assignor = assignor,
            autoCommitEnabled = true,
            groupInstanceId = groupInstanceId,
        )
        consumer.subscribe(setOf(invalidTopicName), getConsumerRebalanceListener(consumer))
        assertFailsWith<InvalidTopicException> { consumer.poll(Duration.ZERO) }
    }

    @Test
    fun testPollTimeMetrics() {
        val metadata = createMetadata(subscription)
        val client = MockClient(time, metadata)
        initMetadata(client, mapOf(topic to 1))
        val consumer = newConsumer(
            time = time,
            client = client,
            subscription = subscription,
            metadata = metadata,
            assignor = assignor,
            autoCommitEnabled = true,
            groupInstanceId = groupInstanceId,
        )
        consumer.subscribe(listOf(topic))

        // MetricName objects to check
        val metrics = consumer.metrics
        val lastPollSecondsAgoName = metrics.metricName("last-poll-seconds-ago", "consumer-metrics")
        val timeBetweenPollAvgName = metrics.metricName("time-between-poll-avg", "consumer-metrics")
        val timeBetweenPollMaxName = metrics.metricName("time-between-poll-max", "consumer-metrics")

        // Test default values
        assertEquals(-1.0, consumer.metrics()[lastPollSecondsAgoName]!!.metricValue())
        assertEquals(Double.NaN, consumer.metrics()[timeBetweenPollAvgName]!!.metricValue())
        assertEquals(Double.NaN, consumer.metrics()[timeBetweenPollMaxName]!!.metricValue())

        // Call first poll
        consumer.poll(Duration.ZERO)
        assertEquals(0.0, consumer.metrics()[lastPollSecondsAgoName]!!.metricValue())
        assertEquals(0.0, consumer.metrics()[timeBetweenPollAvgName]!!.metricValue())
        assertEquals(0.0, consumer.metrics()[timeBetweenPollMaxName]!!.metricValue())

        // Advance time by 5,000 (total time = 5,000)
        time.sleep(5 * 1000L)
        assertEquals(5.0, consumer.metrics()[lastPollSecondsAgoName]!!.metricValue())

        // Call second poll
        consumer.poll(Duration.ZERO)
        assertEquals(2.5 * 1000.0, consumer.metrics()[timeBetweenPollAvgName]!!.metricValue())
        assertEquals(5 * 1000.0, consumer.metrics()[timeBetweenPollMaxName]!!.metricValue())

        // Advance time by 10,000 (total time = 15,000)
        time.sleep(10 * 1000L)
        assertEquals(10.0, consumer.metrics()[lastPollSecondsAgoName]!!.metricValue())

        // Call third poll
        consumer.poll(Duration.ZERO)
        assertEquals(5 * 1000.0, consumer.metrics()[timeBetweenPollAvgName]!!.metricValue())
        assertEquals(10 * 1000.0, consumer.metrics()[timeBetweenPollMaxName]!!.metricValue())

        // Advance time by 5,000 (total time = 20,000)
        time.sleep(5 * 1000L)
        assertEquals(5.0, consumer.metrics()[lastPollSecondsAgoName]!!.metricValue())

        // Call fourth poll
        consumer.poll(Duration.ZERO)
        assertEquals(5 * 1000.0, consumer.metrics()[timeBetweenPollAvgName]!!.metricValue())
        assertEquals(10 * 1000.0, consumer.metrics()[timeBetweenPollMaxName]!!.metricValue())
    }

    @Test
    fun testPollIdleRatio() {
        val metadata = createMetadata(subscription)
        val client = MockClient(time, metadata)
        initMetadata(client, mapOf(topic to 1))
        val consumer = newConsumer(
            time = time,
            client = client,
            subscription = subscription,
            metadata = metadata,
            assignor = assignor,
            autoCommitEnabled = true,
            groupInstanceId = groupInstanceId,
        )
        // MetricName object to check
        val metrics = consumer.metrics
        val pollIdleRatio = metrics.metricName("poll-idle-ratio-avg", "consumer-metrics")
        // Test default value
        assertEquals(Double.NaN, consumer.metrics()[pollIdleRatio]!!.metricValue())

        // 1st poll
        // Spend 50ms in poll so value = 1.0
        consumer.kafkaConsumerMetrics.recordPollStart(time.milliseconds())
        time.sleep(50)
        consumer.kafkaConsumerMetrics.recordPollEnd(time.milliseconds())
        assertEquals(1.0, consumer.metrics()[pollIdleRatio]!!.metricValue())

        // 2nd poll
        // Spend 50m outside poll and 0ms in poll so value = 0.0
        time.sleep(50)
        consumer.kafkaConsumerMetrics.recordPollStart(time.milliseconds())
        consumer.kafkaConsumerMetrics.recordPollEnd(time.milliseconds())

        // Avg of first two data points
        assertEquals((1.0 + 0.0) / 2, consumer.metrics()[pollIdleRatio]!!.metricValue())

        // 3rd poll
        // Spend 25ms outside poll and 25ms in poll so value = 0.5
        time.sleep(25)
        consumer.kafkaConsumerMetrics.recordPollStart(time.milliseconds())
        time.sleep(25)
        consumer.kafkaConsumerMetrics.recordPollEnd(time.milliseconds())

        // Avg of three data points
        assertEquals((1.0 + 0.0 + 0.5) / 3, consumer.metrics()[pollIdleRatio]!!.metricValue())
    }

    @Test
    fun testClosingConsumerUnregistersConsumerMetrics() {
        val time: Time = MockTime(1L)
        val metadata = createMetadata(subscription)
        val client = MockClient(time, metadata)
        initMetadata(client, mapOf(topic to 1))
        val consumer = newConsumer(
            time = time,
            client = client,
            subscription = subscription,
            metadata = metadata,
            assignor = RoundRobinAssignor(),
            autoCommitEnabled = true,
            groupInstanceId = groupInstanceId
        )
        consumer.subscribe(listOf(topic))
        assertTrue(consumerMetricPresent(consumer, "last-poll-seconds-ago"))
        assertTrue(consumerMetricPresent(consumer, "time-between-poll-avg"))
        assertTrue(consumerMetricPresent(consumer, "time-between-poll-max"))
        consumer.close()
        assertFalse(consumerMetricPresent(consumer, "last-poll-seconds-ago"))
        assertFalse(consumerMetricPresent(consumer, "time-between-poll-avg"))
        assertFalse(consumerMetricPresent(consumer, "time-between-poll-max"))
    }

    @Test
    fun testEnforceRebalanceWithManualAssignment() {
        consumer = newConsumer(groupId = null)
        consumer.assign(setOf(TopicPartition(topic = "topic", partition = 0)))
        assertFailsWith<IllegalStateException> { consumer.enforceRebalance() }
    }

    @Test
    fun testEnforceRebalanceTriggersRebalanceOnNextPoll() {
        val time: Time = MockTime(1L)
        val metadata = createMetadata(subscription)
        val client = MockClient(time, metadata)
        val consumer = newConsumer(
            time = time,
            client = client,
            subscription = subscription,
            metadata = metadata,
            assignor = assignor,
            autoCommitEnabled = true,
            groupInstanceId = groupInstanceId,
        )
        val countingRebalanceListener = MockRebalanceListener()
        initMetadata(
            mockClient = client,
            partitionCounts = mapOf(topic to 1, topic2 to 1, topic3 to 1),
        )
        consumer.subscribe(
            topics = listOf(topic, topic2),
            callback = countingRebalanceListener,
        )
        val node = metadata.fetch().nodes[0]
        prepareRebalance(
            client = client,
            node = node,
            assignor = assignor,
            partitions = listOf(tp0, t2p0),
            coordinator = null,
        )

        // a first rebalance to get the assignment, we need two poll calls since we need two round trips
        // to finish join / sync-group
        consumer.poll(Duration.ZERO)
        consumer.poll(Duration.ZERO)

        // onPartitionsRevoked is not invoked when first joining the group
        assertEquals(countingRebalanceListener.revokedCount, 0)
        assertEquals(countingRebalanceListener.assignedCount, 1)
        consumer.enforceRebalance()

        // the next poll should trigger a rebalance
        consumer.poll(Duration.ZERO)
        assertEquals(countingRebalanceListener.revokedCount, 1)
    }

    @Test
    fun testEnforceRebalanceReason() {
        val time: Time = MockTime(1L)
        val metadata = createMetadata(subscription)
        val client = MockClient(time, metadata)
        initMetadata(
            mockClient = client,
            partitionCounts = mapOf(topic to 1),
        )
        val node = metadata.fetch().nodes[0]
        consumer = newConsumer(
            time = time,
            client = client,
            subscription = subscription,
            metadata = metadata,
            assignor = assignor,
            autoCommitEnabled = true,
            groupInstanceId = groupInstanceId,
        )
        consumer.subscribe(listOf(topic))

        // Lookup coordinator.
        client.prepareResponseFrom(
            response = prepareResponse(
                error = Errors.NONE,
                key = groupId,
                node = node,
            ),
            node = node
        )
        consumer.poll(Duration.ZERO)

        // Initial join sends an empty reason.
        prepareJoinGroupAndVerifyReason(
            client = client,
            node = node,
            expectedReason = "",
        )
        consumer.poll(Duration.ZERO)

        // A null reason should be replaced by the default reason.
        consumer.enforceRebalance(null)
        prepareJoinGroupAndVerifyReason(
            client = client,
            node = node,
            expectedReason = KafkaConsumer.DEFAULT_REASON,
        )
        consumer.poll(Duration.ZERO)

        // An empty reason should be replaced by the default reason.
        consumer.enforceRebalance("")
        prepareJoinGroupAndVerifyReason(
            client = client,
            node = node,
            expectedReason = KafkaConsumer.DEFAULT_REASON,
        )
        consumer.poll(Duration.ZERO)

        // A non-null and non-empty reason is sent as-is.
        val customReason = "user provided reason"
        consumer.enforceRebalance(customReason)
        prepareJoinGroupAndVerifyReason(
            client = client,
            node = node,
            expectedReason = customReason,
        )
        consumer.poll(Duration.ZERO)
    }

    private fun prepareJoinGroupAndVerifyReason(
        client: MockClient,
        node: Node,
        expectedReason: String,
    ) {
        client.prepareResponseFrom(
            matcher = { body ->
                val joinGroupRequest = assertIs<JoinGroupRequest>(body)
                expectedReason == joinGroupRequest.data().reason
            },
            response = joinGroupFollowerResponse(
                assignor = assignor,
                generationId = 1,
                memberId = memberId,
                leaderId = leaderId,
                error = Errors.NONE,
            ),
            node = node,
        )
    }

    @Test
    fun configurableObjectsShouldSeeGeneratedClientId() {
        val props = Properties()
        props[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] = "localhost:9999"
        props[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = DeserializerForClientId::class.java.getName()
        props[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = DeserializerForClientId::class.java.getName()
        props[ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG] = ConsumerInterceptorForClientId::class.java.getName()
        consumer = KafkaConsumer<ByteArray?, ByteArray?>(props)

        assertNotNull(consumer.getClientId())
        assertNotEquals(0, consumer.getClientId()!!.length)
        assertEquals(3, CLIENT_IDS.size)

        CLIENT_IDS.forEach { id -> assertEquals(id, consumer.getClientId()) }
    }

    @Test
    fun testUnusedConfigs() {
        val props: MutableMap<String, Any?> = HashMap()
        props[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] = "localhost:9999"
        props[SslConfigs.SSL_PROTOCOL_CONFIG] = "TLS"
        val config = ConsumerConfig(
            props = ConsumerConfig.appendDeserializerToConfig(
                configs = props,
                keyDeserializer = StringDeserializer(),
                valueDeserializer = StringDeserializer(),
            ),
        )
        assertTrue(config.unused().contains(SslConfigs.SSL_PROTOCOL_CONFIG))
        consumer = KafkaConsumer<ByteArray, ByteArray>(
            config = config,
            keyDeserializer = null,
            valueDeserializer = null,
        )
        assertTrue(config.unused().contains(SslConfigs.SSL_PROTOCOL_CONFIG))
    }

    @Test
    fun testAssignorNameConflict() {
        val configs = mapOf(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to "localhost:9999",
            ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG to listOf(
                RangeAssignor::class.java.getName(),
                TestConsumerPartitionAssignor::class.java.getName(),
            ),
        )
        assertFailsWith<KafkaException> {
            KafkaConsumer(
                configs = configs,
                keyDeserializer = StringDeserializer(),
                valueDeserializer = StringDeserializer(),
            )
        }
    }

    @Test
    fun testOffsetsForTimesTimeout() {
        val consumer = consumerForCheckingTimeoutException()
        assertEquals(
            expected = "Failed to get offsets by times in 60000ms",
            actual = assertFailsWith<org.apache.kafka.common.errors.TimeoutException> {
                consumer.offsetsForTimes(mapOf(tp0 to 0L))
            }.message
        )
    }

    @Test
    fun testBeginningOffsetsTimeout() {
        val consumer = consumerForCheckingTimeoutException()
        assertEquals(
            expected = "Failed to get offsets by times in 60000ms",
            actual = assertFailsWith<org.apache.kafka.common.errors.TimeoutException> {
                consumer.beginningOffsets(listOf(tp0))
            }.message,
        )
    }

    @Test
    fun testEndOffsetsTimeout() {
        val consumer = consumerForCheckingTimeoutException()
        assertEquals(
            expected = "Failed to get offsets by times in 60000ms",
            actual = assertFailsWith<org.apache.kafka.common.errors.TimeoutException> {
                consumer.endOffsets(listOf(tp0))
            }.message,
        )
    }

    private fun consumerForCheckingTimeoutException(): KafkaConsumer<String?, String?> {
        val metadata = createMetadata(subscription)
        val client = MockClient(time, metadata)
        initMetadata(client, mapOf(topic to 1))
        val assignor = RangeAssignor()
        val consumer = newConsumer(
            time = time,
            client = client,
            subscription = subscription,
            metadata = metadata,
            assignor = assignor,
            autoCommitEnabled = false,
            groupInstanceId = groupInstanceId,
        )
        for (i in 0..9) {
            client.prepareResponse(
                matcher = { request ->
                    time.sleep((defaultApiTimeoutMs / 10).toLong())
                    request is ListOffsetsRequest
                },
                response = listOffsetsResponse(
                    partitionOffsets = emptyMap(),
                    partitionErrors = mapOf(tp0 to Errors.UNKNOWN_TOPIC_OR_PARTITION),
                ),
            )
        }
        return consumer
    }

    class DeserializerForClientId : Deserializer<ByteArray?> {

        override fun configure(configs: Map<String, *>, isKey: Boolean) {
            CLIENT_IDS.add(configs[ConsumerConfig.CLIENT_ID_CONFIG].toString())
        }

        override fun deserialize(topic: String, data: ByteArray?): ByteArray? = data
    }

    class ConsumerInterceptorForClientId : ConsumerInterceptor<ByteArray, ByteArray> {

        override fun onConsume(
            records: ConsumerRecords<ByteArray, ByteArray>,
        ): ConsumerRecords<ByteArray, ByteArray> = records

        override fun onCommit(offsets: Map<TopicPartition, OffsetAndMetadata>) = Unit

        override fun close() = Unit

        override fun configure(configs: Map<String, *>) {
            CLIENT_IDS.add(configs[ConsumerConfig.CLIENT_ID_CONFIG].toString())
        }
    }

    companion object {

        private fun consumerMetricPresent(
            consumer: KafkaConsumer<String?, String?>,
            name: String,
        ): Boolean {
            val metricName = MetricName(
                name = name,
                group = "consumer-metrics",
                description = "",
                tags = emptyMap(),
            )
            return consumer.metrics.metrics.containsKey(metricName)
        }

        private val CLIENT_IDS: MutableList<String> = ArrayList()
    }
}
