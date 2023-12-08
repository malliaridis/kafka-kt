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

import org.apache.kafka.clients.GroupRebalanceConfig
import org.apache.kafka.common.utils.MockTime
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class HeartbeatTest {
    
    private val sessionTimeoutMs = 300
    
    private val heartbeatIntervalMs = 100
    
    private val maxPollIntervalMs = 900
    
    private val retryBackoffMs = 10L
    
    private val time = MockTime()
    
    private lateinit var heartbeat: Heartbeat
    
    @BeforeEach
    fun setUp() {
        val rebalanceConfig = GroupRebalanceConfig(
            sessionTimeoutMs = sessionTimeoutMs,
            rebalanceTimeoutMs = maxPollIntervalMs,
            heartbeatIntervalMs = heartbeatIntervalMs,
            groupId = "group_id",
            groupInstanceId = null,
            retryBackoffMs = retryBackoffMs,
            leaveGroupOnClose = true,
        )
        heartbeat = Heartbeat(rebalanceConfig, time)
    }

    @Test
    fun testShouldHeartbeat() {
        heartbeat.sentHeartbeat(time.milliseconds())
        time.sleep((heartbeatIntervalMs.toFloat() * 1.1).toLong())
        assertTrue(heartbeat.shouldHeartbeat(time.milliseconds()))
    }

    @Test
    fun testShouldNotHeartbeat() {
        heartbeat.sentHeartbeat(time.milliseconds())
        time.sleep((heartbeatIntervalMs / 2).toLong())
        assertFalse(heartbeat.shouldHeartbeat(time.milliseconds()))
    }

    @Test
    fun testTimeToNextHeartbeat() {
        heartbeat.sentHeartbeat(time.milliseconds())
        assertEquals(heartbeatIntervalMs.toLong(), heartbeat.timeToNextHeartbeat(time.milliseconds()))
        time.sleep(heartbeatIntervalMs.toLong())
        assertEquals(0, heartbeat.timeToNextHeartbeat(time.milliseconds()))
        time.sleep(heartbeatIntervalMs.toLong())
        assertEquals(0, heartbeat.timeToNextHeartbeat(time.milliseconds()))
    }

    @Test
    fun testSessionTimeoutExpired() {
        heartbeat.sentHeartbeat(time.milliseconds())
        time.sleep((sessionTimeoutMs + 5).toLong())
        assertTrue(heartbeat.sessionTimeoutExpired(time.milliseconds()))
    }

    @Test
    fun testResetSession() {
        heartbeat.sentHeartbeat(time.milliseconds())
        time.sleep((sessionTimeoutMs + 5).toLong())
        heartbeat.resetSessionTimeout()
        assertFalse(heartbeat.sessionTimeoutExpired(time.milliseconds()))

        // Resetting the session timeout should not reset the poll timeout
        time.sleep((maxPollIntervalMs + 1).toLong())
        heartbeat.resetSessionTimeout()
        assertTrue(heartbeat.pollTimeoutExpired(time.milliseconds()))
    }

    @Test
    fun testResetTimeouts() {
        time.sleep(maxPollIntervalMs.toLong())
        assertTrue(heartbeat.sessionTimeoutExpired(time.milliseconds()))
        assertEquals(0, heartbeat.timeToNextHeartbeat(time.milliseconds()))
        assertTrue(heartbeat.pollTimeoutExpired(time.milliseconds()))
        heartbeat.resetTimeouts()
        assertFalse(heartbeat.sessionTimeoutExpired(time.milliseconds()))
        assertEquals(heartbeatIntervalMs.toLong(), heartbeat.timeToNextHeartbeat(time.milliseconds()))
        assertFalse(heartbeat.pollTimeoutExpired(time.milliseconds()))
    }

    @Test
    fun testPollTimeout() {
        assertFalse(heartbeat.pollTimeoutExpired(time.milliseconds()))
        time.sleep((maxPollIntervalMs / 2).toLong())
        assertFalse(heartbeat.pollTimeoutExpired(time.milliseconds()))
        time.sleep((maxPollIntervalMs / 2 + 1).toLong())
        assertTrue(heartbeat.pollTimeoutExpired(time.milliseconds()))
    }
}
