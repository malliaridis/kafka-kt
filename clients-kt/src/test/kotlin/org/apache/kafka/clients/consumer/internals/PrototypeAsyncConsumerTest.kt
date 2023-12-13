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

package org.apache.kafka.clients.consumer.internals

import java.time.Duration
import java.util.concurrent.CompletableFuture
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.clients.consumer.OffsetCommitCallback
import org.apache.kafka.clients.consumer.OffsetResetStrategy
import org.apache.kafka.clients.consumer.internals.events.AssignmentChangeApplicationEvent
import org.apache.kafka.clients.consumer.internals.events.EventHandler
import org.apache.kafka.clients.consumer.internals.events.NewTopicsMetadataUpdateRequestEvent
import org.apache.kafka.clients.consumer.internals.events.OffsetFetchApplicationEvent
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.InvalidGroupIdException
import org.apache.kafka.common.internals.ClusterResourceListeners
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.utils.LogContext
import org.apache.kafka.common.utils.MockTime
import org.apache.kafka.common.utils.Time
import org.apache.kafka.test.TestUtils.assertNotFails
import org.apache.kafka.test.TestUtils.waitForCondition
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Disabled
import org.mockito.Mockito.mockConstruction
import org.mockito.kotlin.any
import org.mockito.kotlin.doReturn
import org.mockito.kotlin.isA
import org.mockito.kotlin.mock
import org.mockito.kotlin.spy
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class PrototypeAsyncConsumerTest {

    private var consumer: PrototypeAsyncConsumer<*, *>? = null

    private val consumerProps = mutableMapOf<String, Any>()

    private val time: Time = MockTime()

    private lateinit var logContext: LogContext

    private lateinit var subscriptions: SubscriptionState

    private lateinit var eventHandler: EventHandler

    private lateinit var metrics: Metrics

    private lateinit var clusterResourceListeners: ClusterResourceListeners

    private var groupId: String? = "group.id"

    private val clientId = "client-1"

    private lateinit var config: ConsumerConfig

    @BeforeEach
    fun setup() {
        injectConsumerConfigs()
        config = ConsumerConfig(consumerProps)
        logContext = LogContext()
        subscriptions = mock<SubscriptionState>()
        eventHandler = mock<DefaultEventHandler>()
        metrics = Metrics(time = time)
        clusterResourceListeners = ClusterResourceListeners()
    }

    @AfterEach
    fun cleanup() {
        consumer?.close(Duration.ZERO)
    }

    @Test
    fun testSuccessfulStartupShutdown() {
        consumer = newConsumer(time, StringDeserializer(), StringDeserializer())
        assertNotFails { consumer!!.close() }
    }

    @Test
    fun testInvalidGroupId() {
        groupId = null
        consumer = newConsumer(time, StringDeserializer(), StringDeserializer())
        assertFailsWith<InvalidGroupIdException> { consumer!!.committed(HashSet()) }
    }

    @Test
    @Throws(InterruptedException::class)
    fun testCommitAsync_NullCallback() {
        val future = CompletableFuture<Unit>()
        val offsets = mapOf(
            TopicPartition(topic = "my-topic", partition = 0) to  OffsetAndMetadata(100L),
            TopicPartition(topic = "my-topic", partition = 1) to OffsetAndMetadata(200L),
        )
        val mockedConsumer = spy(newConsumer(time, StringDeserializer(), StringDeserializer()))
        doReturn(future).whenever(mockedConsumer).commit(offsets)
        mockedConsumer.commitAsync(offsets, null)
        future.complete(null)
        waitForCondition(
            testCondition = { future.isDone },
            maxWaitMs = 2000,
            conditionDetails = "commit future should complete",
        )
        assertFalse(future.isCompletedExceptionally())
    }

    @Test
    fun testCommitAsync_UserSuppliedCallback() {
        val future = CompletableFuture<Unit>()
        val offsets = mapOf(
            TopicPartition(topic = "my-topic", partition = 0) to OffsetAndMetadata(100L),
            TopicPartition(topic = "my-topic", partition = 1) to OffsetAndMetadata(200L),
        )
        val consumer = newConsumer(
            time = time,
            keyDeserializer = StringDeserializer(),
            valueDeserializer = StringDeserializer(),
        )
        val mockedConsumer = spy(consumer)
        doReturn(future).whenever(mockedConsumer).commit(offsets)
        val customCallback = mock<OffsetCommitCallback>()
        mockedConsumer.commitAsync(offsets, customCallback)
        future.complete(Unit)
        verify(customCallback).onComplete(offsets = offsets, exception = null)
    }

    @Test
    fun testCommitted() {
        val mockTopicPartitions = mockTopicPartitionOffset().keys
        val committedFuture = CompletableFuture<Map<TopicPartition, OffsetAndMetadata>>()
        mockConstruction(OffsetFetchApplicationEvent::class.java) { mock, _ ->
            whenever(mock.future).thenReturn(committedFuture)
        }.use {
            committedFuture.complete(mockTopicPartitionOffset())
            consumer = newConsumer(
                time = time,
                keyDeserializer = StringDeserializer(),
                valueDeserializer = StringDeserializer(),
            )
            assertNotFails {
                consumer!!.committed(
                    partitions = mockTopicPartitions,
                    timeout = Duration.ofMillis(1000),
                )
            }
            verify(eventHandler).add(isA<OffsetFetchApplicationEvent>())
        }
    }

    @Test
    fun testCommitted_ExceptionThrown() {
        val mockTopicPartitions = mockTopicPartitionOffset().keys
        val committedFuture = CompletableFuture<Map<TopicPartition, OffsetAndMetadata>>()
        mockConstruction(OffsetFetchApplicationEvent::class.java) { mock, _ ->
            whenever(mock.future).thenReturn(committedFuture)
        }.use {
            committedFuture.completeExceptionally(KafkaException("Test exception"))
            consumer = newConsumer(
                time = time,
                keyDeserializer = StringDeserializer(),
                valueDeserializer = StringDeserializer(),
            )
            assertFailsWith<KafkaException> {
                consumer!!.committed(mockTopicPartitions, Duration.ofMillis(1000))
            }
            verify(eventHandler).add(isA<OffsetFetchApplicationEvent>())
        }
    }

    @Test
    fun testAssign() {
        subscriptions = SubscriptionState(logContext, OffsetResetStrategy.EARLIEST)
        consumer = newConsumer(
            time = time,
            keyDeserializer = StringDeserializer(),
            valueDeserializer = StringDeserializer(),
        )
        val tp = TopicPartition(topic = "foo", partition = 3)
        consumer!!.assign(setOf(tp))
        assertTrue(consumer!!.subscription().isEmpty())
        assertTrue(consumer!!.assignment().contains(tp))
        verify(eventHandler).add(any<AssignmentChangeApplicationEvent>())
        verify(eventHandler).add(any<NewTopicsMetadataUpdateRequestEvent>())
    }

    @Test
    @Disabled("Kotlin Migration - null partitions not allowed in Kotlin")
    fun testAssignOnNullTopicPartition() {
//        consumer = newConsumer(time, StringDeserializer(), StringDeserializer())
//        assertFailsWith<IllegalArgumentException> { consumer!!.assign(null) }
    }

    @Test
    fun testAssignOnEmptyTopicPartition() {
        consumer = spy(newConsumer(time, StringDeserializer(), StringDeserializer()))
        consumer!!.assign(emptyList())
        assertTrue(consumer!!.subscription().isEmpty())
        assertTrue(consumer!!.assignment().isEmpty())
    }

    @Test
    @Disabled("Kotlin Migration - Topics are not nullable.")
    fun testAssignOnNullTopicInPartition() {
//        consumer = newConsumer(time, StringDeserializer(), StringDeserializer())
//        assertFailsWith<IllegalArgumentException> {
//            consumer!!.assign(setOf(TopicPartition(null, 0)))
//        }
    }

    @Test
    fun testAssignOnEmptyTopicInPartition() {
        consumer = newConsumer(time, StringDeserializer(), StringDeserializer())
        assertFailsWith<IllegalArgumentException> {
            consumer!!.assign(setOf(TopicPartition(topic = "  ", partition = 0)))
        }
    }

    private fun mockTopicPartitionOffset(): Map<TopicPartition, OffsetAndMetadata> {
        val t0 = TopicPartition(topic = "t0", partition = 2)
        val t1 = TopicPartition(topic = "t0", partition = 3)
        return mapOf(
            t0 to OffsetAndMetadata(10L),
            t1 to OffsetAndMetadata(20L),
        )
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

    private fun injectConsumerConfigs() {
        consumerProps[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] = "localhost:9999"
        consumerProps[ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG] = "60000"
        consumerProps[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java
        consumerProps[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java
    }

    private fun <K, V> newConsumer(
        time: Time,
        keyDeserializer: Deserializer<K>,
        valueDeserializer: Deserializer<V>,
    ): PrototypeAsyncConsumer<K, V> {
        consumerProps[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = keyDeserializer.javaClass
        consumerProps[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = valueDeserializer.javaClass
        return PrototypeAsyncConsumer(
            time = time,
            logContext = logContext,
            config = config,
            subscriptionState = subscriptions,
            eventHandler = eventHandler,
            metrics = metrics,
            groupId = groupId,
            defaultApiTimeoutMs = config.getInt(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG)!!,
        )
    }
}
