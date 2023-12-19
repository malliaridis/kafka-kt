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

package org.apache.kafka.server.utils

import java.util.concurrent.TimeUnit
import org.apache.kafka.common.utils.MockTime
import org.apache.kafka.common.utils.Time
import org.apache.kafka.server.util.Deadline
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

@Timeout(value = 120)
class DeadlineTest {

    @Test
    fun testOneMillisecondDeadline() {
        assertEquals(
            expected = TimeUnit.MILLISECONDS.toNanos(1),
            actual = Deadline.fromDelay(
                time = monoTime(0),
                delay = 1,
                timeUnit = TimeUnit.MILLISECONDS,
            ).nanoseconds,
        )
    }

    @Test
    fun testOneMillisecondDeadlineWithBase() {
        val nowNs = 123456789L
        assertEquals(
            expected = nowNs + TimeUnit.MILLISECONDS.toNanos(1),
            actual = Deadline.fromDelay(
                time = monoTime(nowNs),
                delay = 1,
                timeUnit = TimeUnit.MILLISECONDS,
            ).nanoseconds,
        )
    }

    @Test
    fun testNegativeDelayFails() {
        val error = assertFailsWith<RuntimeException> {
            Deadline.fromDelay(
                time = monoTime(123456789L),
                delay = -1L,
                timeUnit = TimeUnit.MILLISECONDS,
            )
        }
        assertEquals("Negative delays are not allowed.", error.message)
    }

    @Test
    fun testMaximumDelay() {
        assertEquals(
            Long.MAX_VALUE,
            Deadline.fromDelay(
                time = monoTime(123L),
                delay = Long.MAX_VALUE,
                timeUnit = TimeUnit.HOURS,
            ).nanoseconds,
        )
        assertEquals(
            Long.MAX_VALUE,
            Deadline.fromDelay(
                time = monoTime(0),
                delay = Long.MAX_VALUE / 2,
                timeUnit = TimeUnit.MILLISECONDS,
            ).nanoseconds,
        )
        assertEquals(
            Long.MAX_VALUE,
            Deadline.fromDelay(
                time = monoTime(Long.MAX_VALUE),
                delay = Long.MAX_VALUE,
                timeUnit = TimeUnit.NANOSECONDS,
            ).nanoseconds,
        )
    }

    companion object {

        private val log = LoggerFactory.getLogger(FutureUtilsTest::class.java)

        private fun monoTime(monotonicTime: Long): Time = MockTime(
            autoTickMs = 0,
            currentTimeMs = 0,
            currentHighResTimeNs = monotonicTime,
        )
    }
}
