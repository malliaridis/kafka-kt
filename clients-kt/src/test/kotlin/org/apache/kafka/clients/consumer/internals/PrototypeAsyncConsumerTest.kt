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

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.OffsetResetStrategy
import org.apache.kafka.clients.consumer.internals.events.EventHandler
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.internals.ClusterResourceListeners
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.utils.LogContext
import org.apache.kafka.common.utils.MockTime
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.Mockito
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class PrototypeAsyncConsumerTest {
    
    private lateinit var properties: Map<String, Any>
    
    private lateinit var subscriptionState: SubscriptionState
    
    private lateinit var time: MockTime
    
    private lateinit var logContext: LogContext
    
    private lateinit var metrics: Metrics
    
    private lateinit var clusterResourceListeners: ClusterResourceListeners
    
    private var groupId: String? = null
    
    private lateinit var clientId: String
    
    private lateinit var eventHandler: EventHandler
    
    @BeforeEach
    fun setup() {
        subscriptionState = Mockito.mock(SubscriptionState::class.java)
        eventHandler = Mockito.mock(DefaultEventHandler::class.java)
        logContext = LogContext()
        time = MockTime()
        metrics = Metrics(time = time)
        groupId = null
        clientId = "client-1"
        clusterResourceListeners = ClusterResourceListeners()
        properties = mapOf(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to "localhost:9999",
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java,
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java,
            ConsumerConfig.CLIENT_ID_CONFIG to "test-client",
        )
    }

    @Test
    fun testSubscription() {
        subscriptionState = SubscriptionState(LogContext(), OffsetResetStrategy.EARLIEST)
        val consumer = setupConsumerWithDefault()
        subscriptionState.subscribe(
            setOf("t1"),
            NoOpConsumerRebalanceListener()
        )
        assertEquals(1, consumer.subscription().size)
    }

    @Test
    fun testUnimplementedException() {
        val consumer = setupConsumerWithDefault()
        assertFailsWith<KafkaException>(message = "not implemented exception") { consumer.assignment() }
    }

    fun setupConsumerWithDefault(): PrototypeAsyncConsumer<String, String> {
        val config = ConsumerConfig(properties)
        return PrototypeAsyncConsumer(
            time = time,
            logContext = logContext,
            config = config,
            subscriptionState = subscriptionState,
            eventHandler = eventHandler,
            metrics = metrics,
            clusterResourceListeners = clusterResourceListeners,
            groupId = groupId,
            clientId = clientId,
            defaultApiTimeoutMs = 0,
        )
    }
}
