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

package org.apache.kafka.server.utils.timer

import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.function.Consumer
import java.util.stream.Collectors
import java.util.stream.IntStream
import org.apache.kafka.common.utils.Time
import org.apache.kafka.server.util.timer.SystemTimer
import org.apache.kafka.server.util.timer.Timer
import org.apache.kafka.server.util.timer.TimerTask
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.test.fail

class TimerTest {

    private lateinit var timer: Timer

    @BeforeEach
    fun setup() {
        timer = SystemTimer(
            executorName = "test",
            tickMs = 1,
            wheelSize = 3,
            startMs = Time.SYSTEM.hiResClockMs(),
        )
    }

    @AfterEach
    @Throws(Exception::class)
    fun teardown() {
        timer.close()
    }

    @Test
    @Throws(InterruptedException::class)
    fun testAlreadyExpiredTask() {
        val output = mutableListOf<Int>()
        val latches = (-5..<0).map { i ->
            val latch = CountDownLatch(1)
            timer.add(
                TestTask(
                    delayMs = i.toLong(),
                    id = i,
                    latch = latch,
                    output = output,
                )
            )
            latch
        }
        timer.advanceClock(0L)
        latches.take(5).forEach { latch ->
            try {
                assertTrue(latch.await(3, TimeUnit.SECONDS), "already expired tasks should run immediately")
            } catch (e: InterruptedException) {
                fail("interrupted")
            }
        }
        assertEquals(
            expected = setOf(-5, -4, -3, -2, -1),
            actual = output.toSet(),
            message = "output of already expired tasks",
        )
    }

    @Test
    @Throws(InterruptedException::class)
    fun testTaskExpiration() {
        val output: MutableList<Int> = ArrayList()
        val tasks: MutableList<TestTask> = ArrayList()
        val ids: MutableList<Int> = ArrayList()
        val latches: MutableList<CountDownLatch> = ArrayList()
        (0..<5).forEach { i ->
            val latch = CountDownLatch(1)
            tasks.add(
                TestTask(
                    delayMs = i.toLong(),
                    id = i,
                    latch = latch,
                    output = output,
                )
            )
            ids.add(i)
            latches.add(latch)
        }
        (10..<100).forEach { i ->
            val latch = CountDownLatch(2)
            tasks.add(
                TestTask(
                    delayMs = i.toLong(),
                    id = i,
                    latch = latch,
                    output = output,
                )
            )
            tasks.add(
                TestTask(
                    delayMs = i.toLong(),
                    id = i,
                    latch = latch,
                    output = output,
                )
            )
            ids.add(i)
            ids.add(i)
            latches.add(latch)
        }
        (100..<500).forEach { i ->
            val latch = CountDownLatch(1)
            tasks.add(
                TestTask(
                    delayMs = i.toLong(),
                    id = i,
                    latch = latch,
                    output = output,
                )
            )
            ids.add(i)
            latches.add(latch)
        }

        // randomly submit requests
        tasks.forEach { task -> timer.add(task) }

        do Unit while (timer.advanceClock(2000))

        latches.forEach { latch ->
            try {
                latch.await()
            } catch (e: InterruptedException) {
                fail("interrupted")
            }
        }
        assertEquals(ids, output.sorted(), "output should match")
    }

    private class TestTask(
        delayMs: Long,
        val id: Int,
        val latch: CountDownLatch,
        val output: MutableList<Int>,
    ) : TimerTask(delayMs) {

        val completed = AtomicBoolean(false)

        override fun run() {
            if (completed.compareAndSet(false, true)) {
                synchronized(output) { output.add(id) }
                latch.countDown()
            }
        }
    }
}
