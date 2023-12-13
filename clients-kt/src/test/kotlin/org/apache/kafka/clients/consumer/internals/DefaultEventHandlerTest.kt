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

import java.util.Properties
import java.util.concurrent.LinkedBlockingQueue
import org.apache.kafka.clients.GroupRebalanceConfig
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.internals.events.ApplicationEvent
import org.apache.kafka.clients.consumer.internals.events.BackgroundEvent
import org.apache.kafka.clients.consumer.internals.events.NoopApplicationEvent
import org.apache.kafka.common.serialization.StringDeserializer
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.kotlin.mock
import org.mockito.kotlin.times
import org.mockito.kotlin.verify
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

class DefaultEventHandlerTest {
    
    private val sessionTimeoutMs = 1000
    
    private val rebalanceTimeoutMs = 1000
    
    private val heartbeatIntervalMs = 1000
    
    private val groupId = "g-1"
    
    private val groupInstanceId = "g-1"
    
    private val retryBackoffMs: Long = 1000
    
    private val properties = Properties()
    
    private lateinit var rebalanceConfig: GroupRebalanceConfig
    
    @BeforeEach
    fun setup() {
        properties[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java
        properties[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java
        properties[ConsumerConfig.RETRY_BACKOFF_MS_CONFIG] = "100"

        rebalanceConfig = GroupRebalanceConfig(
            sessionTimeoutMs = sessionTimeoutMs,
            rebalanceTimeoutMs = rebalanceTimeoutMs,
            heartbeatIntervalMs = heartbeatIntervalMs,
            groupId = groupId,
            groupInstanceId = groupInstanceId,
            retryBackoffMs = retryBackoffMs,
            leaveGroupOnClose = true,
        )
    }

    @Test
    fun testBasicHandlerOps() {
        val bt = mock<DefaultBackgroundThread>()
        val aq = LinkedBlockingQueue<ApplicationEvent>()
        val bq = LinkedBlockingQueue<BackgroundEvent>()
        val handler = DefaultEventHandler(bt, aq, bq)
        assertTrue(handler.isEmpty)
        assertNull(handler.poll())
        handler.add(NoopApplicationEvent("test"))
        assertEquals(1, aq.size)
        handler.close()
        verify(bt, times(1)).close()
    }
}
