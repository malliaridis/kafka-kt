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

import org.apache.kafka.clients.MockClient
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.internals.events.ApplicationEvent
import org.apache.kafka.clients.consumer.internals.events.BackgroundEvent
import org.apache.kafka.clients.consumer.internals.events.NoopApplicationEvent
import org.apache.kafka.common.errors.WakeupException
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.utils.LogContext
import org.apache.kafka.common.utils.MockTime
import org.apache.kafka.common.utils.Timer
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.Mockito.mock
import org.mockito.Mockito.`when`
import org.mockito.kotlin.any
import org.mockito.kotlin.inOrder
import java.util.Properties
import java.util.concurrent.BlockingQueue
import java.util.concurrent.LinkedBlockingQueue
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class DefaultBackgroundThreadTest {
    
    private val properties = Properties()
    
    private lateinit var time: MockTime
    
    private lateinit var subscriptions: SubscriptionState
    
    private lateinit  var metadata: ConsumerMetadata
    
    private lateinit var context: LogContext
    
    private lateinit var consumerClient: ConsumerNetworkClient
    
    private lateinit var metrics: Metrics
    
    private lateinit var backgroundEventsQueue: BlockingQueue<BackgroundEvent>
    
    private lateinit var applicationEventsQueue: BlockingQueue<ApplicationEvent>
    
    @BeforeEach
    fun setup() {
        time = MockTime()
        subscriptions = mock(SubscriptionState::class.java)
        metadata = mock(ConsumerMetadata::class.java)
        context = LogContext()
        consumerClient = mock(ConsumerNetworkClient::class.java)
        metrics = mock(Metrics::class.java)
        applicationEventsQueue = mock(BlockingQueue::class.java) as BlockingQueue<ApplicationEvent>
        backgroundEventsQueue = mock(BlockingQueue::class.java) as BlockingQueue<BackgroundEvent>
        properties[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java
        properties[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java
        properties[ConsumerConfig.RETRY_BACKOFF_MS_CONFIG] = REFRESH_BACK_OFF_MS
    }

    @Test
    @Throws(InterruptedException::class)
    fun testStartupAndTearDown() {
        val client = MockClient(time, metadata)
        consumerClient = ConsumerNetworkClient(
            logContext = context,
            client = client,
            metadata = metadata,
            time = time,
            retryBackoffMs = 100,
            requestTimeoutMs = 1000,
            maxPollTimeoutMs = 100,
        )
        applicationEventsQueue = LinkedBlockingQueue()
        val backgroundThread = setupMockHandler()
        backgroundThread.start()
        assertTrue(client.active())
        backgroundThread.close()
        assertFalse(client.active())
    }

    @Test
    @Throws(InterruptedException::class)
    fun testInterruption() {
        val client = MockClient(time, metadata)
        consumerClient = ConsumerNetworkClient(
            logContext = context,
            client = client,
            metadata = metadata,
            time = time,
            retryBackoffMs = 100,
            requestTimeoutMs = 1000,
            maxPollTimeoutMs = 100,
        )
        applicationEventsQueue = LinkedBlockingQueue()
        val backgroundThread = setupMockHandler()
        backgroundThread.start()
        assertTrue(client.active())
        backgroundThread.close()
        assertFalse(client.active())
    }

    @Test
    fun testWakeup() {
        time = MockTime(0)
        val client = MockClient(time, metadata)
        consumerClient = ConsumerNetworkClient(
            logContext = context,
            client = client,
            metadata = metadata,
            time = time,
            retryBackoffMs = 100,
            requestTimeoutMs = 1000,
            maxPollTimeoutMs = 100,
        )
        `when`(applicationEventsQueue.isEmpty()).thenReturn(true)
        `when`(applicationEventsQueue.isEmpty()).thenReturn(true)
        val runnable = setupMockHandler()
        client.poll(0, time.milliseconds())
        runnable.wakeup()
        assertFailsWith<WakeupException> { runnable.runOnce() }
        runnable.close()
    }

    @Test
    fun testNetworkAndBlockingQueuePoll() {
        // ensure network poll and application queue poll will happen in a
        // single iteration
        time = MockTime(100)
        val runnable = setupMockHandler()
        runnable.runOnce()
        `when`(applicationEventsQueue.isEmpty()).thenReturn(false)
        `when`(applicationEventsQueue.poll())
            .thenReturn(NoopApplicationEvent(backgroundEventsQueue, "nothing"))
        val inOrder = inOrder(applicationEventsQueue, consumerClient)
        assertFalse(inOrder.verify(applicationEventsQueue).isEmpty())
        inOrder.verify(applicationEventsQueue).poll()
        inOrder.verify(consumerClient).poll(any<Timer>())
        runnable.close()
    }

    private fun setupMockHandler(): DefaultBackgroundThread {
        return DefaultBackgroundThread(
            time = time,
            config = ConsumerConfig(properties),
            logContext = LogContext(),
            applicationEventQueue = applicationEventsQueue,
            backgroundEventQueue = backgroundEventsQueue,
            subscriptions = subscriptions,
            metadata = metadata,
            networkClient = consumerClient,
            metrics = metrics
        )
    }

    companion object {
        private const val REFRESH_BACK_OFF_MS: Long = 100
    }
}
