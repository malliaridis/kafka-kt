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

package org.apache.kafka.common.utils

import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class TimerTest {
    
    private val time = MockTime()
    
    @Test
    fun testTimerUpdate() {
        val timer = time.timer(500)
        assertEquals(500, timer.timeoutMs)
        assertEquals(500, timer.remainingMs)
        assertEquals(0, timer.elapsedMs)
        time.sleep(100)
        timer.update()
        assertEquals(500, timer.timeoutMs)
        assertEquals(400, timer.remainingMs)
        assertEquals(100, timer.elapsedMs)
        time.sleep(400)
        timer.update(time.milliseconds())
        assertEquals(500, timer.timeoutMs)
        assertEquals(0, timer.remainingMs)
        assertEquals(500, timer.elapsedMs)
        assertTrue(timer.isExpired)

        // Going over the expiration is fine and the elapsed time can exceed
        // the initial timeout. However, remaining time should be stuck at 0.
        time.sleep(200)
        timer.update(time.milliseconds())
        assertTrue(timer.isExpired)
        assertEquals(500, timer.timeoutMs)
        assertEquals(0, timer.remainingMs)
        assertEquals(700, timer.elapsedMs)
    }

    @Test
    fun testTimerUpdateAndReset() {
        val timer = time.timer(500)
        timer.sleep(200)
        assertEquals(500, timer.timeoutMs)
        assertEquals(300, timer.remainingMs)
        assertEquals(200, timer.elapsedMs)
        timer.updateAndReset(400)
        assertEquals(400, timer.timeoutMs)
        assertEquals(400, timer.remainingMs)
        assertEquals(0, timer.elapsedMs)
        timer.sleep(400)
        assertTrue(timer.isExpired)
        timer.updateAndReset(200)
        assertEquals(200, timer.timeoutMs)
        assertEquals(200, timer.remainingMs)
        assertEquals(0, timer.elapsedMs)
        assertFalse(timer.isExpired)
    }

    @Test
    fun testTimerResetUsesCurrentTime() {
        val timer = time.timer(500)
        timer.sleep(200)
        assertEquals(300, timer.remainingMs)
        assertEquals(200, timer.elapsedMs)
        time.sleep(300)
        timer.reset(500)
        assertEquals(500, timer.remainingMs)
        timer.update()
        assertEquals(200, timer.remainingMs)
    }

    @Test
    fun testTimerResetDeadlineUsesCurrentTime() {
        val timer = time.timer(500)
        timer.sleep(200)
        assertEquals(300, timer.remainingMs)
        assertEquals(200, timer.elapsedMs)
        timer.sleep(100)
        timer.resetDeadline(time.milliseconds() + 200)
        assertEquals(200, timer.timeoutMs)
        assertEquals(200, timer.remainingMs)
        timer.sleep(100)
        assertEquals(200, timer.timeoutMs)
        assertEquals(100, timer.remainingMs)
    }

    @Test
    fun testTimeoutOverflow() {
        val timer = time.timer(Long.MAX_VALUE)
        assertEquals(Long.MAX_VALUE - timer.currentTimeMs, timer.remainingMs)
        assertEquals(0, timer.elapsedMs)
    }

    @Test
    fun testNonMonotonicUpdate() {
        val timer = time.timer(100)
        val currentTimeMs = timer.currentTimeMs
        timer.update(currentTimeMs - 1)
        assertEquals(currentTimeMs, timer.currentTimeMs)
        assertEquals(100, timer.remainingMs)
        assertEquals(0, timer.elapsedMs)
    }

    @Test
    fun testTimerSleep() {
        val timer = time.timer(500)
        val currentTimeMs = timer.currentTimeMs
        timer.sleep(200)
        assertEquals(time.milliseconds(), timer.currentTimeMs)
        assertEquals(currentTimeMs + 200, timer.currentTimeMs)
        timer.sleep(1000)
        assertEquals(time.milliseconds(), timer.currentTimeMs)
        assertEquals(currentTimeMs + 500, timer.currentTimeMs)
        assertTrue(timer.isExpired)
    }
}
