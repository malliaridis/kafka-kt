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

import org.apache.kafka.common.MetricName
import org.apache.kafka.common.metrics.internals.MetricsUtils.convert
import org.apache.kafka.common.metrics.stats.Avg
import org.apache.kafka.common.metrics.stats.CumulativeSum
import org.apache.kafka.common.metrics.stats.Max
import org.apache.kafka.common.metrics.stats.Meter
import org.apache.kafka.common.metrics.stats.Min
import org.apache.kafka.common.metrics.stats.Percentile
import org.apache.kafka.common.metrics.stats.Percentiles
import org.apache.kafka.common.metrics.stats.Percentiles.BucketSizing
import org.apache.kafka.common.metrics.stats.Rate
import org.apache.kafka.common.metrics.stats.SimpleRate
import org.apache.kafka.common.metrics.stats.Value
import org.apache.kafka.common.metrics.stats.WindowedCount
import org.apache.kafka.common.metrics.stats.WindowedSum
import org.apache.kafka.common.utils.MockTime
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.slf4j.LoggerFactory
import java.util.Deque
import java.util.concurrent.ConcurrentLinkedDeque
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.math.abs
import kotlin.math.ceil
import kotlin.random.Random
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNotEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.fail

class MetricsTest {

    private val time = MockTime()

    private val config = MetricConfig()

    private lateinit var metrics: Metrics

    private var executorService: ExecutorService? = null

    @BeforeEach
    fun setup() {
        metrics = Metrics(
            config = config,
            reporters = mutableListOf(JmxReporter()),
            time = time,
            enableExpiration = true,
        )
    }

    @AfterEach
    @Throws(Exception::class)
    fun tearDown() {
        if (executorService != null) {
            executorService!!.shutdownNow()
            executorService!!.awaitTermination(5, TimeUnit.SECONDS)
        }
        metrics.close()
    }

    @Test
    fun testMetricName() {
        val n1 = metrics.metricName(
            name = "name",
            group = "group",
            description = "description",
            keyValue = arrayOf("key1", "value1", "key2", "value2"),
        )
        val tags: MutableMap<String, String> = HashMap()
        tags["key1"] = "value1"
        tags["key2"] = "value2"
        val n2 = metrics.metricName(
            name = "name",
            group = "group",
            description = "description",
            tags = tags,
        )
        assertEquals(n1, n2, "metric names created in two different ways should be equal")
        try {
            metrics.metricName(
                name = "name",
                group = "group",
                description = "description",
                keyValue = arrayOf("key1"),
            )
            fail("Creating MetricName with an odd number of keyValue should fail")
        } catch (_: IllegalArgumentException) {
            // this is expected
        }
    }

    @Test
    @Throws(Exception::class)
    fun testSimpleStats() {
        verifyStats { m -> m!!.metricValue() as Double }
    }

    private fun verifyStats(metricValueFunc: (KafkaMetric?) -> Double) {
        val measurable = ConstantMeasurable()
        metrics.addMetric(
            metricName = metrics.metricName(
                name = "direct.measurable",
                group = "grp1",
                description = "The fraction of time an appender waits for space allocation.",
            ),
            metricValueProvider = measurable,
        )
        val s = metrics.sensor("test.sensor")
        s.add(metrics.metricName("test.avg", "grp1"), Avg())
        s.add(metrics.metricName("test.max", "grp1"), Max())
        s.add(metrics.metricName("test.min", "grp1"), Min())
        s.add(
            Meter(
                unit = TimeUnit.SECONDS,
                rateMetricName = metrics.metricName("test.rate", "grp1"),
                totalMetricName = metrics.metricName("test.total", "grp1"),
            )
        )
        s.add(
            Meter(
                unit = TimeUnit.SECONDS,
                rateStat = WindowedCount(),
                rateMetricName = metrics.metricName("test.occurences", "grp1"),
                totalMetricName = metrics.metricName("test.occurences.total", "grp1"),
            )
        )
        s.add(metrics.metricName("test.count", "grp1"), WindowedCount())
        s.add(
            Percentiles(
                sizeInBytes = 100,
                min = -100.0,
                max = 100.0,
                bucketing = BucketSizing.CONSTANT,
                percentiles = listOf(
                    Percentile(metrics.metricName("test.median", "grp1"), 50.0),
                    Percentile(metrics.metricName("test.perc99_9", "grp1"), 99.9),
                ),
            )
        )
        val s2 = metrics.sensor("test.sensor2")
        s2.add(metrics.metricName("s2.total", "grp1"), CumulativeSum())
        s2.record(5.0)
        var sum = 0
        val count = 10
        for (i in 0 until count) {
            s.record(i.toDouble())
            sum += i
        }
        // prior to any time passing
        var elapsedSecs = config.timeWindowMs * (config.samples - 1) / 1000.0
        assertEquals(
            expected = count / elapsedSecs,
            actual = metricValueFunc(metrics.metrics[metrics.metricName("test.occurences", "grp1")]),
            absoluteTolerance = EPS,
            message = String.format("Occurrences(0...%d) = %f", count, count / elapsedSecs),
        )

        // pretend 2 seconds passed...
        val sleepTimeMs: Long = 2
        time.sleep(sleepTimeMs * 1000)
        elapsedSecs += sleepTimeMs.toDouble()
        assertEquals(
            expected = 5.0,
            actual = metricValueFunc(metrics.metric(metrics.metricName("s2.total", "grp1"))),
            absoluteTolerance = EPS,
            message = "s2 reflects the constant value",
        )
        assertEquals(
            expected = 4.5,
            actual = metricValueFunc(metrics.metric(metrics.metricName("test.avg", "grp1"))),
            absoluteTolerance = EPS,
            message = "Avg(0...9) = 4.5",
        )
        assertEquals(
            expected = (count - 1).toDouble(),
            actual = metricValueFunc(metrics.metric(metrics.metricName("test.max", "grp1"))),
            absoluteTolerance = EPS,
            message = "Max(0...9) = 9"
        )
        assertEquals(
            0.0, metricValueFunc(metrics.metric(metrics.metricName("test.min", "grp1"))), EPS,
            "Min(0...9) = 0"
        )
        assertEquals(
            expected = sum / elapsedSecs,
            actual = metricValueFunc(metrics.metric(metrics.metricName("test.rate", "grp1"))),
            absoluteTolerance = EPS,
            message = "Rate(0...9) = 1.40625",
        )
        assertEquals(
            expected = count / elapsedSecs,
            actual = metricValueFunc(metrics.metric(metrics.metricName("test.occurences", "grp1"))),
            absoluteTolerance = EPS,
            message = String.format("Occurrences(0...%d) = %f", count, count / elapsedSecs),
        )
        assertEquals(
            expected = count.toDouble(),
            actual = metricValueFunc(metrics.metric(metrics.metricName("test.count", "grp1"))),
            absoluteTolerance = EPS,
            message = "Count(0...9) = 10",
        )
    }

    @Test
    fun testHierarchicalSensors() {
        val parent1 = metrics.sensor(name = "test.parent1")
        parent1.add(metrics.metricName(name = "test.parent1.count", group = "grp1"), WindowedCount())
        val parent2 = metrics.sensor(name = "test.parent2")
        parent2.add(metrics.metricName(name = "test.parent2.count", group = "grp1"), WindowedCount())
        val child1 = metrics.sensor(name = "test.child1", parents = arrayOf(parent1, parent2))
        child1.add(metrics.metricName(name = "test.child1.count", group = "grp1"), WindowedCount())
        val child2 = metrics.sensor(name = "test.child2", parents = arrayOf(parent1))
        child2.add(metrics.metricName(name = "test.child2.count", group = "grp1"), WindowedCount())
        val grandchild = metrics.sensor(name = "test.grandchild", parents = arrayOf(child1))
        grandchild.add(metrics.metricName("test.grandchild.count", "grp1"), WindowedCount())

        /* increment each sensor one time */
        parent1.record()
        parent2.record()
        child1.record()
        child2.record()
        grandchild.record()
        val p1 = parent1.metrics()[0].metricValue() as Double
        val p2 = parent2.metrics()[0].metricValue() as Double
        val c1 = child1.metrics()[0].metricValue() as Double
        val c2 = child2.metrics()[0].metricValue() as Double
        val gc = grandchild.metrics()[0].metricValue() as Double

        /* each metric should have a count equal to one + its children's count */assertEquals(1.0, gc, EPS)
        assertEquals(1.0 + gc, c1, EPS)
        assertEquals(1.0, c2, EPS)
        assertEquals(1.0 + c1, p2, EPS)
        assertEquals(1.0 + c1 + c2, p1, EPS)
        assertEquals(listOf(child1, child2), metrics.childrenSensors()[parent1])
        assertEquals(listOf(child1), metrics.childrenSensors()[parent2])
        assertNull(metrics.childrenSensors()[grandchild])
    }

    @Test
    fun testBadSensorHierarchy() {
        val p = metrics.sensor(name = "parent")
        val c1 = metrics.sensor(name = "child1", parents = arrayOf(p))
        val c2 = metrics.sensor(name = "child2", parents = arrayOf(p))
        assertFailsWith<IllegalArgumentException> { metrics.sensor(name = "gc", parents = arrayOf(c1, c2)) }
    }

    @Test
    fun testRemoveChildSensor() {
        val metrics = Metrics()
        val parent = metrics.sensor(name = "parent")
        val child = metrics.sensor(name = "child", parents = arrayOf(parent))
        assertEquals(listOf(child), metrics.childrenSensors()[parent])
        metrics.removeSensor(name = "child")
        assertEquals(emptyList(), metrics.childrenSensors()[parent])
    }

    @Test
    fun testRemoveSensor() {
        val size = metrics.metrics.size
        val parent1 = metrics.sensor(name = "test.parent1")
        parent1.add(metrics.metricName(name = "test.parent1.count", group = "grp1"), WindowedCount())
        val parent2 = metrics.sensor(name = "test.parent2")
        parent2.add(metrics.metricName(name = "test.parent2.count", group = "grp1"), WindowedCount())
        val child1 = metrics.sensor(name = "test.child1", parents = arrayOf(parent1, parent2))
        child1.add(metrics.metricName(name = "test.child1.count", group = "grp1"), WindowedCount())
        val child2 = metrics.sensor(name = "test.child2", parents = arrayOf(parent2))
        child2.add(metrics.metricName(name = "test.child2.count", group = "grp1"), WindowedCount())
        val grandChild1 = metrics.sensor(name = "test.gchild2", parents = arrayOf(child2))
        grandChild1.add(metrics.metricName(name = "test.gchild2.count", group = "grp1"), WindowedCount())

        var sensor = metrics.getSensor(name = "test.parent1")
        assertNotNull(sensor)
        metrics.removeSensor(name = "test.parent1")
        assertNull(metrics.getSensor(name = "test.parent1"))
        assertNull(metrics.metrics[metrics.metricName(name = "test.parent1.count", group = "grp1")])
        assertNull(metrics.getSensor(name = "test.child1"))
        assertNull(metrics.childrenSensors()[sensor])
        assertNull(metrics.metrics[metrics.metricName(name = "test.child1.count", group = "grp1")])

        sensor = metrics.getSensor(name = "test.gchild2")
        assertNotNull(sensor)
        metrics.removeSensor(name = "test.gchild2")
        assertNull(metrics.getSensor(name = "test.gchild2"))
        assertNull(metrics.childrenSensors()[sensor])
        assertNull(metrics.metrics[metrics.metricName(name = "test.gchild2.count", group = "grp1")])

        sensor = metrics.getSensor(name = "test.child2")
        assertNotNull(sensor)
        metrics.removeSensor(name = "test.child2")
        assertNull(metrics.getSensor(name = "test.child2"))
        assertNull(metrics.childrenSensors()[sensor])
        assertNull(metrics.metrics[metrics.metricName(name = "test.child2.count", group = "grp1")])

        sensor = metrics.getSensor(name = "test.parent2")
        assertNotNull(sensor)
        metrics.removeSensor(name = "test.parent2")
        assertNull(metrics.getSensor("test.parent2"))
        assertNull(metrics.childrenSensors()[sensor])
        assertNull(metrics.metrics[metrics.metricName(name = "test.parent2.count", group = "grp1")])
        assertEquals(size, metrics.metrics.size)
    }

    @Test
    fun testRemoveInactiveMetrics() {
        var s1 = metrics.sensor("test.s1", null, 1)
        s1.add(metrics.metricName("test.s1.count", "grp1"), WindowedCount())
        val s2 = metrics.sensor("test.s2", null, 3)
        s2.add(metrics.metricName("test.s2.count", "grp1"), WindowedCount())
        val purger = metrics.ExpireSensorTask()
        purger.run()
        assertNotNull(actual = metrics.getSensor("test.s1"), message = "Sensor test.s1 must be present")
        assertNotNull(
            metrics.metrics[metrics.metricName(name = "test.s1.count", group = "grp1")],
            "MetricName test.s1.count must be present"
        )
        assertNotNull(metrics.getSensor("test.s2"), "Sensor test.s2 must be present")
        assertNotNull(
            metrics.metrics[metrics.metricName(name = "test.s2.count", group = "grp1")],
            "MetricName test.s2.count must be present"
        )
        time.sleep(1001)
        purger.run()
        assertNull(metrics.getSensor("test.s1"), "Sensor test.s1 should have been purged")
        assertNull(
            metrics.metrics[metrics.metricName(name = "test.s1.count", group = "grp1")],
            "MetricName test.s1.count should have been purged"
        )
        assertNotNull(actual = metrics.getSensor("test.s2"), message = "Sensor test.s2 must be present")
        assertNotNull(
            metrics.metrics[metrics.metricName(name = "test.s2.count", group = "grp1")],
            "MetricName test.s2.count must be present"
        )

        // record a value in sensor s2. This should reset the clock for that sensor.
        // It should not get purged at the 3 second mark after creation
        s2.record()
        time.sleep(2000)
        purger.run()
        assertNotNull(actual = metrics.getSensor("test.s2"), message = "Sensor test.s2 must be present")
        assertNotNull(
            metrics.metrics[metrics.metricName(name = "test.s2.count", group = "grp1")],
            "MetricName test.s2.count must be present"
        )

        // After another 1001 ms sleep, the metric should be purged
        time.sleep(1001)
        purger.run()
        assertNull(actual = metrics.getSensor("test.s2"), message = "Sensor test.s2 should have been purged")
        assertNull(
            metrics.metrics[metrics.metricName(name = "test.s2.count", group = "grp1")],
            "MetricName test.s2.count should have been purged"
        )

        // After purging, it should be possible to recreate a metric
        s1 = metrics.sensor(name = "test.s1", config = null, inactiveSensorExpirationTimeSeconds = 1)
        s1.add(metrics.metricName(name = "test.s1.count", group = "grp1"), WindowedCount())
        assertNotNull(metrics.getSensor(name = "test.s1"), "Sensor test.s1 must be present")
        assertNotNull(
            metrics.metrics[metrics.metricName("test.s1.count", "grp1")],
            "MetricName test.s1.count must be present"
        )
    }

    @Test
    fun testRemoveMetric() {
        val size = metrics.metrics.size
        metrics.addMetric(
            metricName = metrics.metricName(name = "test1", group = "grp1"),
            metricValueProvider = WindowedCount(),
        )
        metrics.addMetric(
            metricName = metrics.metricName(name = "test2", group = "grp1"),
            metricValueProvider = WindowedCount(),
        )
        assertNotNull(metrics.removeMetric(metrics.metricName(name = "test1", group = "grp1")))
        assertNull(metrics.metrics[metrics.metricName(name = "test1", group = "grp1")])
        assertNotNull(metrics.metrics[metrics.metricName(name = "test2", group = "grp1")])
        assertNotNull(metrics.removeMetric(metrics.metricName(name = "test2", group = "grp1")))
        assertNull(metrics.metrics[metrics.metricName(name = "test2", group = "grp1")])
        assertEquals(size, metrics.metrics.size)
    }

    @Test
    fun testEventWindowing() {
        val count = WindowedCount()
        val config = MetricConfig().apply {
            eventWindow = 1
            samples = 2
        }
        count.record(config = config, value = 1.0, timeMs = time.milliseconds())
        count.record(config = config, value = 1.0, timeMs = time.milliseconds())
        assertEquals(expected = 2.0, actual = count.measure(config, time.milliseconds()), absoluteTolerance = EPS)
        count.record(config = config, value = 1.0, timeMs = time.milliseconds()) // first event times out
        assertEquals(expected = 2.0, actual = count.measure(config, time.milliseconds()), absoluteTolerance = EPS)
    }

    @Test
    fun testTimeWindowing() {
        val count = WindowedCount()
        val config = MetricConfig().apply {
            timeWindowMs = 1
            samples = 2
        }
        count.record(config, 1.0, time.milliseconds())
        time.sleep(1)
        count.record(config, 1.0, time.milliseconds())
        assertEquals(2.0, count.measure(config, time.milliseconds()), EPS)
        time.sleep(1)
        count.record(config, 1.0, time.milliseconds()) // oldest event times out
        assertEquals(2.0, count.measure(config, time.milliseconds()), EPS)
    }

    @Test
    fun testOldDataHasNoEffect() {
        val max = Max()
        val windowMs: Long = 100
        val samples = 2
        val config = MetricConfig().apply {
            this.timeWindowMs = windowMs
            this.samples = samples
        }
        max.record(config, 50.0, time.milliseconds())
        time.sleep(samples * windowMs)
        assertEquals(Double.NaN, max.measure(config, time.milliseconds()), EPS)
    }

    /**
     * Some implementations of SampledStat make sense to return NaN
     * when there are no values set rather than the initial value
     */
    @Test
    fun testSampledStatReturnsNaNWhenNoValuesExist() {
        // This is tested by having a SampledStat with expired Stats,
        // because their values get reset to the initial values.
        val max = Max()
        val min = Min()
        val avg = Avg()
        val windowMs: Long = 100
        val samples = 2
        val config = MetricConfig().apply {
            this.timeWindowMs = windowMs
            this.samples = samples
        }
        max.record(config, 50.0, time.milliseconds())
        min.record(config, 50.0, time.milliseconds())
        avg.record(config, 50.0, time.milliseconds())
        time.sleep(samples * windowMs)
        assertEquals(Double.NaN, max.measure(config, time.milliseconds()), EPS)
        assertEquals(Double.NaN, min.measure(config, time.milliseconds()), EPS)
        assertEquals(Double.NaN, avg.measure(config, time.milliseconds()), EPS)
    }

    /**
     * Some implementations of SampledStat make sense to return the initial value
     * when there are no values set
     */
    @Test
    fun testSampledStatReturnsInitialValueWhenNoValuesExist() {
        val count = WindowedCount()
        val sampledTotal = WindowedSum()
        val windowMs: Long = 100
        val samples = 2
        val config = MetricConfig().apply {
            this.timeWindowMs = windowMs
            this.samples = samples
        }
        count.record(config, 50.0, time.milliseconds())
        sampledTotal.record(config, 50.0, time.milliseconds())
        time.sleep(samples * windowMs)
        assertEquals(0.0, count.measure(config, time.milliseconds()), EPS)
        assertEquals(0.0, sampledTotal.measure(config, time.milliseconds()), EPS)
    }

    @Test
    fun testDuplicateMetricName() {
        metrics.sensor("test").add(metrics.metricName("test", "grp1"), Avg())
        assertFailsWith<IllegalArgumentException> {
            metrics.sensor("test2").add(metrics.metricName("test", "grp1"), CumulativeSum())
        }
    }

    @Test
    fun testQuotas() {
        val sensor = metrics.sensor("test")
        sensor.add(
            metricName = metrics.metricName("test1.total", "grp1"),
            stat = CumulativeSum(),
            config = MetricConfig().apply { quota = Quota.upperBound(5.0) },
        )
        sensor.add(
            metricName = metrics.metricName("test2.total", "grp1"),
            stat = CumulativeSum(),
            config = MetricConfig().apply { quota = Quota.lowerBound(0.0) },
        )
        sensor.record(5.0)
        try {
            sensor.record(1.0)
            fail("Should have gotten a quota violation.")
        } catch (e: QuotaViolationException) {
            // this is good
        }
        assertEquals(
            expected = 6.0,
            actual = metrics.metrics[metrics.metricName("test1.total", "grp1")]!!.metricValue() as Double,
            absoluteTolerance = EPS,
        )
        sensor.record(-6.0)
        try {
            sensor.record(-1.0)
            fail("Should have gotten a quota violation.")
        } catch (e: QuotaViolationException) {
            // this is good
        }
    }

    @Test
    fun testQuotasEquality() {
        val quota1 = Quota.upperBound(10.5)
        val quota2 = Quota.lowerBound(10.5)
        assertNotEquals(quota1, quota2, "Quota with different upper values shouldn't be equal")
        val quota3 = Quota.lowerBound(10.5)
        assertEquals(quota2, quota3, "Quota with same upper and bound values should be equal")
    }

    @Test
    fun testPercentiles() {
        val buckets = 100
        val percs = Percentiles(
            sizeInBytes = 4 * buckets,
            min = 0.0,
            max = 100.0,
            bucketing = BucketSizing.CONSTANT,
            percentiles = listOf(
                Percentile(metrics.metricName("test.p25", "grp1"), 25.0),
                Percentile(metrics.metricName("test.p50", "grp1"), 50.0),
                Percentile(metrics.metricName("test.p75", "grp1"), 75.0),
            ),
        )
        val config = MetricConfig().apply {
            eventWindow = 50
            samples = 2
        }
        val sensor = metrics.sensor("test", config)
        sensor.add(percs)
        val p25 = metrics.metrics[metrics.metricName("test.p25", "grp1")]!!
        val p50 = metrics.metrics[metrics.metricName("test.p50", "grp1")]!!
        val p75 = metrics.metrics[metrics.metricName("test.p75", "grp1")]!!

        // record two windows worth of sequential values
        for (i in 0 until buckets) sensor.record(i.toDouble())
        assertEquals(expected = 25.0, actual = p25.metricValue() as Double, absoluteTolerance = 1.0)
        assertEquals(expected = 50.0, actual = p50.metricValue() as Double, absoluteTolerance = 1.0)
        assertEquals(expected = 75.0, actual = p75.metricValue() as Double, absoluteTolerance = 1.0)
        for (i in 0 until buckets) sensor.record(0.0)
        assertEquals(expected = 0.0, actual = p25.metricValue() as Double, absoluteTolerance = 1.0)
        assertEquals(expected = 0.0, actual = p50.metricValue() as Double, absoluteTolerance = 1.0)
        assertEquals(expected = 0.0, actual = p75.metricValue() as Double, absoluteTolerance = 1.0)

        // record two more windows worth of sequential values
        for (i in 0 until buckets) sensor.record(i.toDouble())
        assertEquals(expected = 25.0, actual = p25.metricValue() as Double, absoluteTolerance = 1.0)
        assertEquals(expected = 50.0, actual = p50.metricValue() as Double, absoluteTolerance = 1.0)
        assertEquals(expected = 75.0, actual = p75.metricValue() as Double, absoluteTolerance = 1.0)
    }

    @Test
    fun shouldPinSmallerValuesToMin() {
        val min = 0.0
        val max = 100.0
        val percs = Percentiles(
            sizeInBytes = 1000,
            min = min,
            max = max,
            bucketing = BucketSizing.LINEAR,
            percentiles = listOf(Percentile(metrics.metricName("test.p50", "grp1"), 50.0)),
        )
        val config = MetricConfig().apply {
            eventWindow = 50
            samples = 2
        }
        val sensor = metrics.sensor("test", config)
        sensor.add(percs)
        val p50 = metrics.metrics[metrics.metricName("test.p50", "grp1")]!!
        sensor.record(min - 100)
        sensor.record(min - 100)
        assertEquals(min, p50.metricValue() as Double, 0.0)
    }

    @Test
    fun shouldPinLargerValuesToMax() {
        val min = 0.0
        val max = 100.0
        val percs = Percentiles(
            sizeInBytes = 1000,
            min = min,
            max = max,
            bucketing = BucketSizing.LINEAR,
            percentiles = listOf(Percentile(metrics.metricName("test.p50", "grp1"), 50.0)),
        )
        val config = MetricConfig().apply {
            eventWindow = 50
            samples = 2
        }
        val sensor = metrics.sensor("test", config)
        sensor.add(percs)
        val p50 = metrics.metrics[metrics.metricName("test.p50", "grp1")]!!
        sensor.record(max + 100)
        sensor.record(max + 100)
        assertEquals(max, p50.metricValue() as Double, 0.0)
    }

    @Test
    fun testPercentilesWithRandomNumbersAndLinearBucketing() {
        val seed = Random.nextLong()
        val sizeInBytes = 100 * 1000 // 100kB
        val maximumValue = 1000 * 24 * 60 * 60 * 1000L // if values are ms, max is 1000 days
        try {
            val prng = Random(seed)
            val numberOfValues = 5000 + prng.nextInt(10000) // range is [5000, 15000]
            val percs = Percentiles(
                sizeInBytes = sizeInBytes,
                max = maximumValue.toDouble(),
                bucketing = BucketSizing.LINEAR,
                percentiles = listOf(
                    Percentile(metrics.metricName("test.p90", "grp1"), 90.0),
                    Percentile(metrics.metricName("test.p99", "grp1"), 99.0),
                ),
            )
            val config = MetricConfig().apply {
                eventWindow = 50
                samples = 2
            }
            val sensor = metrics.sensor("test", config)
            sensor.add(percs)
            val p90 = metrics.metrics[metrics.metricName("test.p90", "grp1")]!!
            val p99 = metrics.metrics[metrics.metricName("test.p99", "grp1")]!!
            val values: MutableList<Long> = ArrayList(numberOfValues)
            // record two windows worth of sequential values
            for (i in 0 until numberOfValues) {
                val value = ((abs(prng.nextLong().toDouble()) - 1) % maximumValue).toLong()
                values.add(value)
                sensor.record(value.toDouble())
            }
            values.sort()
            val p90Index = ceil((90 * numberOfValues).toDouble() / 100).toInt()
            val p99Index = ceil((99 * numberOfValues).toDouble() / 100).toInt()
            val expectedP90 = values[p90Index - 1].toDouble()
            val expectedP99 = values[p99Index - 1].toDouble()
            assertEquals(
                expected = expectedP90,
                actual = p90.metricValue() as Double,
                absoluteTolerance = expectedP90 / 5,
            )
            assertEquals(
                expected = expectedP99,
                actual = p99.metricValue() as Double,
                absoluteTolerance = expectedP99 / 5,
            )
        } catch (e: AssertionError) {
            throw AssertionError("Assertion failed in randomized test. Reproduce with seed = $seed .", e)
        }
    }

    @Test
    @Throws(Exception::class)
    fun testRateWindowing() {
        // Use the default time window. Set 3 samples
        val cfg = MetricConfig().apply { samples = 3 }
        val s = metrics.sensor("test.sensor", cfg)
        val rateMetricName = metrics.metricName("test.rate", "grp1")
        val totalMetricName = metrics.metricName("test.total", "grp1")
        val countRateMetricName = metrics.metricName("test.count.rate", "grp1")
        val countTotalMetricName = metrics.metricName("test.count.total", "grp1")
        s.add(
            Meter(
                unit = TimeUnit.SECONDS,
                rateMetricName = rateMetricName,
                totalMetricName = totalMetricName,
            )
        )
        s.add(
            Meter(
                unit = TimeUnit.SECONDS,
                rateStat = WindowedCount(),
                rateMetricName = countRateMetricName,
                totalMetricName = countTotalMetricName,
            )
        )
        val totalMetric = metrics.metrics[totalMetricName]!!
        val countTotalMetric = metrics.metrics[countTotalMetricName]!!
        var sum = 0
        val count = cfg.samples - 1
        // Advance 1 window after every record
        for (i in 0 until count) {
            s.record(100.0)
            sum += 100
            time.sleep(cfg.timeWindowMs)
            assertEquals(
                expected = sum.toDouble(),
                actual = totalMetric.metricValue() as Double,
                absoluteTolerance = EPS
            )
        }

        // Sleep for half the window.
        time.sleep(cfg.timeWindowMs / 2)

        // prior to any time passing, elapsedSecs = sampleWindowSize * (total samples - half of final sample)
        val elapsedSecs = convert(cfg.timeWindowMs, TimeUnit.SECONDS) * (cfg.samples - 0.5)
        val rateMetric = metrics.metrics[rateMetricName]!!
        val countRateMetric = metrics.metrics[countRateMetricName]!!
        assertEquals(
            expected = sum / elapsedSecs,
            actual = rateMetric.metricValue() as Double,
            absoluteTolerance = EPS,
            message = "Rate(0...2) = 2.666",
        )
        assertEquals(
            expected = count / elapsedSecs,
            actual = countRateMetric.metricValue() as Double,
            absoluteTolerance = EPS,
            message = "Count rate(0...2) = 0.02666",
        )
        assertEquals(
            expected = elapsedSecs,
            actual = convert((rateMetric.measurable() as Rate).windowSize(cfg, time.milliseconds()), TimeUnit.SECONDS),
            absoluteTolerance = EPS,
            message = "Elapsed Time = 75 seconds",
        )
        assertEquals(
            expected = sum.toDouble(),
            actual = totalMetric.metricValue() as Double,
            absoluteTolerance = EPS,
        )
        assertEquals(
            expected = count.toDouble(),
            actual = countTotalMetric.metricValue() as Double,
            absoluteTolerance = EPS,
        )

        // Verify that rates are expired, but total is cumulative
        time.sleep(cfg.timeWindowMs * cfg.samples)
        assertEquals(
            expected = 0.0,
            actual = rateMetric.metricValue() as Double,
            absoluteTolerance = EPS,
        )
        assertEquals(
            expected = 0.0,
            actual = countRateMetric.metricValue() as Double,
            absoluteTolerance = EPS,
        )
        assertEquals(
            expected = sum.toDouble(),
            actual = totalMetric.metricValue() as Double,
            absoluteTolerance = EPS,
        )
        assertEquals(
            expected = count.toDouble(),
            actual = countTotalMetric.metricValue() as Double,
            absoluteTolerance = EPS,
        )
    }

    class ConstantMeasurable : Measurable {
        override fun measure(config: MetricConfig, now: Long): Double = 0.0
    }

    @Test
    fun testSimpleRate() {
        val rate = SimpleRate()

        //Given
        val config = MetricConfig().apply {
            timeWindowMs = 1000
            samples = 10
        }

        //In the first window the rate is a fraction of the whole (1s) window
        //So when we record 1000 at t0, the rate should be 1000 until the window completes, or more data is recorded.
        record(rate, config, 1000)
        assertEquals(expected = 1000.0, actual = measure(rate, config), absoluteTolerance = 0.0)
        time.sleep(100)
        assertEquals(expected = 1000.0, actual = measure(rate, config), absoluteTolerance = 0.0) // 1000B / 0.1s
        time.sleep(100)
        assertEquals(expected = 1000.0, actual = measure(rate, config), absoluteTolerance = 0.0) // 1000B / 0.2s
        time.sleep(200)
        assertEquals(expected = 1000.0, actual = measure(rate, config), absoluteTolerance = 0.0) // 1000B / 0.4s

        //In the second (and subsequent) window(s), the rate will be in proportion to the elapsed time
        //So the rate will degrade over time, as the time between measurement and the initial recording grows.
        time.sleep(600)
        assertEquals(expected = 1000.0, actual = measure(rate, config), absoluteTolerance = 0.0) // 1000B / 1.0s
        time.sleep(200)
        assertEquals(expected = 1000 / 1.2, actual = measure(rate, config), absoluteTolerance = 0.0) // 1000B / 1.2s
        time.sleep(200)
        assertEquals(expected = 1000 / 1.4, actual = measure(rate, config), absoluteTolerance = 0.0) // 1000B / 1.4s

        //Adding another value, inside the same window should double the rate
        record(rate, config, 1000)
        assertEquals(expected = 2000 / 1.4, actual = measure(rate, config), absoluteTolerance = 0.0) // 2000B / 1.4s

        //Going over the next window, should not change behaviour
        time.sleep(1100)
        assertEquals(expected = 2000 / 2.5, actual = measure(rate, config), absoluteTolerance = 0.0) // 2000B / 2.5s
        record(rate, config, 1000)
        assertEquals(expected = 3000 / 2.5, actual = measure(rate, config), absoluteTolerance = 0.0) // 3000B / 2.5s

        //Sleeping for another 6.5 windows also should be the same
        time.sleep(6500)
        assertEquals((3000 / 9).toDouble(), measure(rate, config), 1.0) // 3000B / 9s
        record(rate, config, 1000)
        assertEquals((4000 / 9).toDouble(), measure(rate, config), 1.0) // 4000B / 9s

        //Going over the 10 window boundary should cause the first window's values (1000) will be purged.
        //So the rate is calculated based on the oldest reading, which is inside the second window, at 1.4s
        time.sleep(1500)
        assertEquals(expected = (4000 - 1000) / (10.5 - 1.4), actual = measure(rate, config), absoluteTolerance = 1.0)
        record(rate, config, 1000)
        assertEquals(expected = (5000 - 1000) / (10.5 - 1.4), actual = measure(rate, config), absoluteTolerance = 1.0)
    }

    private fun record(rate: Rate, config: MetricConfig, value: Int) {
        rate.record(config, value.toDouble(), time.milliseconds())
    }

    private fun measure(rate: Measurable, config: MetricConfig): Double {
        return rate.measure(config, time.milliseconds())
    }

    @Test
    fun testMetricInstances() {
        val n1 = metrics.metricInstance(SampleMetrics.METRIC1, "key1", "value1", "key2", "value2")
        val tags: MutableMap<String, String> = HashMap()
        tags["key1"] = "value1"
        tags["key2"] = "value2"
        val n2 = metrics.metricInstance(SampleMetrics.METRIC2, tags)
        assertEquals(n1, n2, "metric names created in two different ways should be equal")
        try {
            metrics.metricInstance(SampleMetrics.METRIC1, "key1")
            fail("Creating MetricName with an odd number of keyValue should fail")
        } catch (_: IllegalArgumentException) {
            // this is expected
        }
        val parentTagsWithValues: MutableMap<String, String> = HashMap()
        parentTagsWithValues["parent-tag"] = "parent-tag-value"
        val childTagsWithValues: MutableMap<String, String> = HashMap()
        childTagsWithValues["child-tag"] = "child-tag-value"
        Metrics(
            config = MetricConfig().apply { this.tags = parentTagsWithValues },
            reporters = mutableListOf(JmxReporter()),
            time = time,
            enableExpiration = true,
        ).use { inherited ->
            val (_, _, _, filledOutTags) = inherited.metricInstance(
                template = SampleMetrics.METRIC_WITH_INHERITED_TAGS,
                tags = childTagsWithValues,
            )
            assertEquals(
                expected = filledOutTags["parent-tag"],
                actual = "parent-tag-value",
                message = "parent-tag should be set properly",
            )
            assertEquals(
                expected = filledOutTags["child-tag"],
                actual = "child-tag-value",
                message = "child-tag should be set properly",
            )
            try {
                inherited.metricInstance(SampleMetrics.METRIC_WITH_INHERITED_TAGS, parentTagsWithValues)
                fail("Creating MetricName should fail if the child metrics are not defined at runtime")
            } catch (_: IllegalArgumentException) {
                // this is expected
            }
            try {
                val runtimeTags = mutableMapOf<String, String>()
                runtimeTags["child-tag"] = "child-tag-value"
                runtimeTags["tag-not-in-template"] = "unexpected-value"
                inherited.metricInstance(SampleMetrics.METRIC_WITH_INHERITED_TAGS, runtimeTags)
                fail("Creating MetricName should fail if there is a tag at runtime that is not in the template")
            } catch (_: IllegalArgumentException) {
                // this is expected
            }
        }
    }

    /**
     * Verifies that concurrent sensor add, remove, updates and read don't result
     * in errors or deadlock.
     */
    @Test
    @Throws(Exception::class)
    fun testConcurrentReadUpdate() {
        val sensors: Deque<Sensor> = ConcurrentLinkedDeque()
        metrics = Metrics(time = MockTime(10))
        val sensorCreator = SensorCreator(metrics)
        val alive = AtomicBoolean(true)
        executorService = Executors.newSingleThreadExecutor()
        executorService!!.submit(
            ConcurrentMetricOperation(alive, "record") {
                sensors.forEach { sensor -> sensor.record(Random.nextInt(10000).toDouble()) }
            }
        )
        for (i in 0..9999) {
            if (sensors.size > 5) {
                val sensor = if (Random.nextBoolean()) sensors.removeFirst() else sensors.removeLast()
                metrics.removeSensor(sensor.name)
            }
            val statType = StatType.forId(Random.nextInt(StatType.values().size))
            sensors.add(sensorCreator.createSensor(statType, i))
            for (sensor in sensors)
                for (metric in sensor.metrics())
                    assertNotNull(metric.metricValue(), "Invalid metric value")
        }
        alive.set(false)
    }

    /**
     * Verifies that concurrent sensor add, remove, updates and read with a metrics reporter
     * that synchronizes on every reporter method doesn't result in errors or deadlock.
     */
    @Test
    @Throws(Exception::class)
    fun testConcurrentReadUpdateReport() {

        class LockingReporter : MetricsReporter {
            var activeMetrics: MutableMap<MetricName, KafkaMetric> = HashMap()

            @Synchronized
            override fun init(metrics: List<KafkaMetric>) = Unit

            @Synchronized
            override fun metricChange(metric: KafkaMetric) {
                activeMetrics[metric.metricName()] = metric
            }

            @Synchronized
            override fun metricRemoval(metric: KafkaMetric) {
                activeMetrics.remove(metric.metricName(), metric)
            }

            @Synchronized
            override fun close() = Unit

            override fun configure(configs: Map<String, Any?>) = Unit

            @Synchronized
            fun processMetrics() {
                for (metric in activeMetrics.values)
                    assertNotNull(metric.metricValue(), "Invalid metric value")
            }
        }

        val reporter = LockingReporter()
        metrics.close()
        metrics = Metrics(config, mutableListOf(reporter), MockTime(10), true)
        val sensors: Deque<Sensor> = ConcurrentLinkedDeque()
        val sensorCreator = SensorCreator(metrics)
        val alive = AtomicBoolean(true)
        executorService = Executors.newFixedThreadPool(3)
        val writeFuture = executorService!!.submit(
            ConcurrentMetricOperation(alive, "record") {
                sensors.forEach { sensor -> sensor.record(Random.nextInt(10000).toDouble()) }
            }
        )
        val readFuture = executorService!!.submit(
            ConcurrentMetricOperation(alive, "read") {
                sensors.forEach { sensor ->
                    sensor.metrics().forEach { metric ->
                        assertNotNull(metric.metricValue(), "Invalid metric value")
                    }
                }
            }
        )
        val reportFuture = executorService!!.submit(
            ConcurrentMetricOperation(alive, "report") { reporter.processMetrics() }
        )
        for (i in 0..9999) {
            if (sensors.size > 10) {
                val sensor = if (Random.nextBoolean()) sensors.removeFirst() else sensors.removeLast()
                metrics.removeSensor(sensor.name)
            }
            val statType = StatType.forId(Random.nextInt(StatType.values().size))
            sensors.add(sensorCreator.createSensor(statType, i))
        }
        assertFalse(readFuture.isDone, "Read failed")
        assertFalse(writeFuture.isDone, "Write failed")
        assertFalse(reportFuture.isDone, "Report failed")
        alive.set(false)
    }

    private inner class ConcurrentMetricOperation(
        private val alive: AtomicBoolean,
        private val opName: String,
        private val op: Runnable,
    ) : Runnable {
        override fun run() {
            try {
                while (alive.get()) op.run()
            } catch (t: Throwable) {
                log.error("Metric {} failed with exception", opName, t)
            }
        }
    }

    internal enum class StatType(var id: Int) {
        AVG(0),
        TOTAL(1),
        COUNT(2),
        MAX(3),
        MIN(4),
        RATE(5),
        SIMPLE_RATE(6),
        SUM(7),
        VALUE(8),
        PERCENTILES(9),
        METER(10);

        companion object {
            fun forId(id: Int): StatType? {
                for (statType in values()) if (statType.id == id) return statType
                return null
            }
        }
    }

    private class SensorCreator(private val metrics: Metrics) {

        fun createSensor(statType: StatType?, index: Int): Sensor {
            val sensor = metrics.sensor("kafka.requests.$index")
            val tags = mapOf("tag" to "tag$index")
            when (statType) {
                StatType.AVG -> sensor.add(
                    metricName = metrics.metricName(name = "test.metric.avg", group = "avg", tags = tags),
                    stat = Avg(),
                )

                StatType.TOTAL -> sensor.add(
                    metricName = metrics.metricName(name = "test.metric.total", group = "total", tags = tags),
                    stat = CumulativeSum(),
                )

                StatType.COUNT -> sensor.add(
                    metricName = metrics.metricName(name = "test.metric.count", group = "count", tags = tags),
                    stat = WindowedCount(),
                )

                StatType.MAX -> sensor.add(
                    metricName = metrics.metricName(name = "test.metric.max", group = "max", tags = tags),
                    stat = Max(),
                )

                StatType.MIN -> sensor.add(
                    metricName = metrics.metricName(name = "test.metric.min", group = "min", tags = tags),
                    stat = Min(),
                )

                StatType.RATE -> sensor.add(
                    metricName = metrics.metricName(name = "test.metric.rate", group = "rate", tags = tags),
                    stat = Rate(),
                )

                StatType.SIMPLE_RATE -> sensor.add(
                    metricName = metrics.metricName(name = "test.metric.simpleRate", group = "simpleRate", tags = tags),
                    stat = SimpleRate()
                )

                StatType.SUM -> sensor.add(
                    metricName = metrics.metricName(name = "test.metric.sum", group = "sum", tags = tags),
                    stat = WindowedSum(),
                )

                StatType.VALUE -> sensor.add(
                    metricName = metrics.metricName(name = "test.metric.value", group = "value", tags = tags),
                    stat = Value(),
                )

                StatType.PERCENTILES -> sensor.add(
                    metricName = metrics.metricName(
                        name = "test.metric.percentiles",
                        group = "percentiles",
                        tags = tags,
                    ),
                    stat = Percentiles(
                        sizeInBytes = 100,
                        min = -100.0,
                        max = 100.0,
                        bucketing = BucketSizing.CONSTANT,
                        percentiles = listOf(
                            Percentile(metrics.metricName("test.median", "percentiles"), 50.0),
                            Percentile(metrics.metricName("test.perc99_9", "percentiles"), 99.9),
                        ),
                    )
                )

                StatType.METER -> sensor.add(
                    Meter(
                        rateMetricName = metrics.metricName(
                            name = "test.metric.meter.rate",
                            group = "meter",
                            tags = tags,
                        ),
                        totalMetricName = metrics.metricName(
                            name = "test.metric.meter.total",
                            group = "meter",
                            tags = tags,
                        ),
                    )
                )

                else -> error("Invalid stat type $statType")
            }
            return sensor
        }
    }

    companion object {

        private val log = LoggerFactory.getLogger(MetricsTest::class.java)

        private const val EPS = 0.000001
    }
}
