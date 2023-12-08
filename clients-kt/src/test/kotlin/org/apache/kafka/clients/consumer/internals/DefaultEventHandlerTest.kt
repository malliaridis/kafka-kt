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
import org.apache.kafka.clients.consumer.OffsetResetStrategy
import org.apache.kafka.clients.consumer.internals.events.ApplicationEvent
import org.apache.kafka.clients.consumer.internals.events.BackgroundEvent
import org.apache.kafka.clients.consumer.internals.events.NoopApplicationEvent
import org.apache.kafka.common.internals.ClusterResourceListeners
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.utils.LogContext
import org.apache.kafka.common.utils.MockTime
import org.apache.kafka.common.utils.Time
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import java.util.*
import java.util.concurrent.BlockingQueue
import java.util.concurrent.LinkedBlockingQueue
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

class DefaultEventHandlerTest {
    
    private val properties = Properties()
    
    @BeforeEach
    fun setup() {
        properties[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java
        properties[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java
        properties[ConsumerConfig.RETRY_BACKOFF_MS_CONFIG] = "100"
    }

    @Test
    @Timeout(1)
    fun testBasicPollAndAddWithNoopEvent() {
        val time: Time = MockTime(1)
        val logContext = LogContext()
        val subscriptions = SubscriptionState(
            logContext = LogContext(),
            defaultResetStrategy = OffsetResetStrategy.NONE,
        )
        val metadata = newConsumerMetadata(includeInternalTopics = false, subscriptions = subscriptions)
        val client = MockClient(time, metadata)
        val consumerClient = ConsumerNetworkClient(
            logContext = logContext,
            client = client,
            metadata = metadata,
            time = time,
            retryBackoffMs = 100,
            requestTimeoutMs = 1000,
            maxPollTimeoutMs = 100,
        )
        val aq = LinkedBlockingQueue<ApplicationEvent>()
        val bq = LinkedBlockingQueue<BackgroundEvent>()
        val handler = DefaultEventHandler(
            time = time,
            config = ConsumerConfig(properties),
            logContext = logContext,
            applicationEventQueue = aq,
            backgroundEventQueue = bq,
            subscriptionState = subscriptions,
            metadata = metadata,
            networkClient = consumerClient,
        )
        assertTrue(client.active())
        assertTrue(handler.isEmpty)
        handler.add(NoopApplicationEvent(bq, "testBasicPollAndAddWithNoopEvent"))
        while (handler.isEmpty) time.sleep(100)
        
        val poll: BackgroundEvent? = handler.poll()
        assertNotNull(poll)
        assertIs<NoopBackgroundEvent>(poll)
        assertFalse(client.hasInFlightRequests()) // noop does not send network request
    }

    companion object {
        private fun newConsumerMetadata(
            includeInternalTopics: Boolean,
            subscriptions: SubscriptionState,
        ): ConsumerMetadata {
            val refreshBackoffMs: Long = 50
            val expireMs: Long = 50000
            return ConsumerMetadata(
                refreshBackoffMs = refreshBackoffMs,
                metadataExpireMs = expireMs,
                includeInternalTopics = includeInternalTopics,
                allowAutoTopicCreation = false,
                subscription = subscriptions,
                logContext = LogContext(),
                clusterResourceListeners = ClusterResourceListeners(),
            )
        }
    }
}
