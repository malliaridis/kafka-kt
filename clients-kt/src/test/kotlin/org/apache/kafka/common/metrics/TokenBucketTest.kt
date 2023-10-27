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

package org.apache.kafka.common.metrics

import org.apache.kafka.common.metrics.stats.TokenBucket
import org.apache.kafka.common.utils.MockTime
import org.apache.kafka.common.utils.Time
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

class TokenBucketTest {
    
    private lateinit var time: Time
    
    @BeforeEach
    fun setup() {
        time = MockTime(0, System.currentTimeMillis(), System.nanoTime())
    }

    @Test
    fun testRecord() {
        // Rate  = 5 unit / sec
        // Burst = 2 * 10 = 20 units
        val config = MetricConfig().apply {
            quota = Quota.upperBound(5.0)
            timeWindowMs = 2000
            samples = 10
        }
        val tk = TokenBucket()

        // Expect 100 credits at T
        assertEquals(
            expected = 100.0,
            actual = tk.measure(config, time.milliseconds()),
            absoluteTolerance = 0.1,
        )

        // Record 60 at T, expect 13 credits
        tk.record(config, 60.0, time.milliseconds())
        assertEquals(
            expected = 40.0,
            actual = tk.measure(config, time.milliseconds()),
            absoluteTolerance = 0.1,
        )

        // Advance by 2s, record 5, expect 45 credits
        time.sleep(2000)
        tk.record(config, 5.0, time.milliseconds())
        assertEquals(
            expected = 45.0,
            actual = tk.measure(config, time.milliseconds()),
            absoluteTolerance = 0.1,
        )

        // Advance by 2s, record 60, expect -5 credits
        time.sleep(2000)
        tk.record(config, 60.0, time.milliseconds())
        assertEquals(
            expected = -5.0,
            actual = tk.measure(config, time.milliseconds()),
            absoluteTolerance = 0.1,
        )
    }

    @Test
    fun testUnrecord() {
        // Rate  = 5 unit / sec
        // Burst = 2 * 10 = 20 units
        val config = MetricConfig().apply {
            quota = Quota.upperBound(5.0)
            timeWindowMs = 2000
            samples = 10
        }
        val tk = TokenBucket()

        // Expect 100 credits at T
        assertEquals(
            expected = 100.0,
            actual = tk.measure(config, time.milliseconds()),
            absoluteTolerance = 0.1,
        )

        // Record -60 at T, expect 100 credits
        tk.record(config, -60.0, time.milliseconds())
        assertEquals(
            expected = 100.0,
            actual = tk.measure(config, time.milliseconds()),
            absoluteTolerance = 0.1,
        )

        // Advance by 2s, record 60, expect 40 credits
        time.sleep(2000)
        tk.record(config, 60.0, time.milliseconds())
        assertEquals(
            expected = 40.0,
            actual = tk.measure(config, time.milliseconds()),
            absoluteTolerance = 0.1,
        )

        // Advance by 2s, record -60, expect 100 credits
        time.sleep(2000)
        tk.record(config, -60.0, time.milliseconds())
        assertEquals(
            expected = 100.0,
            actual = tk.measure(config, time.milliseconds()),
            absoluteTolerance = 0.1,
        )
    }
}
