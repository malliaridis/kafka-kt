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
import org.apache.kafka.common.metrics.JmxReporter
import org.apache.kafka.common.metrics.MetricConfig
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.utils.MockTime
import org.apache.kafka.common.utils.Time
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class FrequenciesTest {
    
    private lateinit var config: MetricConfig
    
    private lateinit var time: Time
    
    private lateinit var metrics: Metrics
    
    @BeforeEach
    fun setup() {
        config = MetricConfig().apply {
            eventWindow = 50
            samples = 2
        }
        time = MockTime()
        metrics = Metrics(
            config = config,
            reporters = mutableListOf(JmxReporter()),
            time = time,
            enableExpiration = true
        )
    }

    @AfterEach
    fun tearDown() {
        metrics.close()
    }

    @Test
    fun testFrequencyCenterValueAboveMax() {
        assertFailsWith<IllegalArgumentException> {
            Frequencies(
                buckets = 4,
                min = 1.0,
                max = 4.0,
                frequencies = arrayOf(
                    freq("1", 1.0),
                    freq("2", 20.0),
                ),
            )
        }
    }

    @Test
    fun testFrequencyCenterValueBelowMin() {
        assertFailsWith<IllegalArgumentException> {
            Frequencies(
                buckets = 4,
                min = 1.0,
                max = 4.0,
                frequencies = arrayOf(
                    freq("1", 1.0),
                    freq("2", -20.0),
                ),
            )
        }
    }

    @Test
    fun testMoreFrequencyParametersThanBuckets() {
        assertFailsWith<IllegalArgumentException> {
            Frequencies(
                buckets = 1,
                min = 1.0,
                max = 4.0,
                frequencies = arrayOf(
                    freq("1", 1.0),
                    freq("2", -20.0),
                ),
            )
        }
    }

    @Test
    fun testBooleanFrequencies() {
        val metricTrue = name("true")
        val metricFalse = name("false")
        val frequencies = Frequencies.forBooleanValues(metricFalse, metricTrue)
        val falseMetric = frequencies.stats()[0]
        val trueMetric = frequencies.stats()[1]

        // Record 2 windows worth of values
        for (i in 0..24) frequencies.record(config, 0.0, time.milliseconds())
        for (i in 0..74) frequencies.record(config, 1.0, time.milliseconds())
        
        assertEquals(0.25, falseMetric.stat.measure(config, time.milliseconds()), DELTA)
        assertEquals(0.75, trueMetric.stat.measure(config, time.milliseconds()), DELTA)

        // Record 2 more windows worth of values
        for (i in 0..39) frequencies.record(config, 0.0, time.milliseconds())
        for (i in 0..59) frequencies.record(config, 1.0, time.milliseconds())
        
        assertEquals(0.40, falseMetric.stat.measure(config, time.milliseconds()), DELTA)
        assertEquals(0.60, trueMetric.stat.measure(config, time.milliseconds()), DELTA)
    }

    @Test
    fun testUseWithMetrics() {
        val name1 = name("1")
        val name2 = name("2")
        val name3 = name("3")
        val name4 = name("4")
        val frequencies = Frequencies(
            4, 1.0, 4.0,
            Frequency(name1, 1.0),
            Frequency(name2, 2.0),
            Frequency(name3, 3.0),
            Frequency(name4, 4.0)
        )
        val sensor = metrics.sensor("test", config)
        sensor.add(frequencies)
        val metric1 = metrics.metrics[name1]!!
        val metric2 = metrics.metrics[name2]!!
        val metric3 = metrics.metrics[name3]!!
        val metric4 = metrics.metrics[name4]!!

        // Record 2 windows worth of values
        for (i in 0..99) frequencies.record(config, (i % 4 + 1).toDouble(), time.milliseconds())
        
        assertEquals(
            expected = 0.25,
            actual = metric1.metricValue() as Double,
            absoluteTolerance = DELTA,
        )
        assertEquals(
            expected = 0.25,
            actual = metric2.metricValue() as Double,
            absoluteTolerance = DELTA,
        )
        assertEquals(
            expected = 0.25,
            actual = metric3.metricValue() as Double,
            absoluteTolerance = DELTA,
        )
        assertEquals(
            expected = 0.25,
            actual = metric4.metricValue() as Double,
            absoluteTolerance = DELTA,
        )

        // Record 2 windows worth of values
        for (i in 0..99) frequencies.record(config, (i % 2 + 1).toDouble(), time.milliseconds())

        assertEquals(
            expected = 0.50,
            actual = metric1.metricValue() as Double,
            absoluteTolerance = DELTA,
        )
        assertEquals(
            expected = 0.50,
            actual = metric2.metricValue() as Double,
            absoluteTolerance = DELTA,
        )
        assertEquals(
            expected = 0.00,
            actual = metric3.metricValue() as Double,
            absoluteTolerance = DELTA,
        )
        assertEquals(
            expected = 0.00,
            actual = metric4.metricValue() as Double,
            absoluteTolerance = DELTA,
        )

        // Record 1 window worth of values to overlap with the last window
        // that is half 1.0 and half 2.0
        for (i in 0..49) frequencies.record(config, 4.0, time.milliseconds())

        assertEquals(
            expected = 0.25,
            actual = metric1.metricValue() as Double,
            absoluteTolerance = DELTA,
        )
        assertEquals(
            expected = 0.25,
            actual = metric2.metricValue() as Double,
            absoluteTolerance = DELTA,
        )
        assertEquals(
            expected = 0.00,
            actual = metric3.metricValue() as Double,
            absoluteTolerance = DELTA,
        )
        assertEquals(
            expected = 0.50,
            actual = metric4.metricValue() as Double,
            absoluteTolerance = DELTA,
        )
    }

    private fun name(metricName: String): MetricName = MetricName(
        name = metricName,
        group = "group-id",
        description = "desc",
        tags = emptyMap(),
    )

    private fun freq(name: String, value: Double): Frequency = Frequency(name(name), value)

    companion object {
        private const val DELTA = 0.0001
    }
}
