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

package org.apache.kafka.common.metrics.stats

import org.apache.kafka.common.MetricName
import org.apache.kafka.common.metrics.MetricConfig
import org.junit.jupiter.api.Test
import kotlin.math.max
import kotlin.test.assertEquals

class MeterTest {
    
    @Test
    fun testMeter() {
        val emptyTags = emptyMap<String, String>()
        val rateMetricName = MetricName(
            name = "rate",
            group = "test",
            description = "",
            tags = emptyTags,
        )
        val totalMetricName = MetricName(
            name = "total",
            group = "test",
            description = "",
            tags = emptyTags,
        )
        val meter = Meter(rateMetricName, totalMetricName)
        val stats = meter.stats()

        assertEquals(2, stats.size)

        val total = stats[0]
        val rate = stats[1]

        assertEquals(rateMetricName, rate.name)
        assertEquals(totalMetricName, total.name)

        val rateStat = rate.stat as Rate
        val totalStat = total.stat as CumulativeSum
        val config = MetricConfig()
        var nextValue = 0.0
        var expectedTotal = 0.0
        var now: Long = 0
        val intervalMs = 100
        val delta = 5.0

        // Record values in multiple windows and verify that rates are reported
        // for time windows and that the total is cumulative.
        for (i in 1..100) {
            while (now < i * 1000) {
                expectedTotal += nextValue
                meter.record(config, nextValue, now)
                now += intervalMs
                nextValue += delta
            }

            assertEquals(expectedTotal, totalStat.measure(config, now), EPS)

            val windowSizeMs = rateStat.windowSize(config, now)
            val windowStartMs = max((now - windowSizeMs).toDouble(), 0.0).toLong()
            var sampledTotal = 0.0
            var prevValue = nextValue - delta
            var timeMs = now - 100
            while (timeMs >= windowStartMs) {
                sampledTotal += prevValue
                timeMs -= intervalMs
                prevValue -= delta
            }

            assertEquals(sampledTotal * 1000 / windowSizeMs, rateStat.measure(config, now), EPS)
        }
    }

    companion object {
        private const val EPS = 0.0000001
    }
}
