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
import org.apache.kafka.common.metrics.stats.Avg
import org.apache.kafka.common.metrics.stats.CumulativeCount
import org.apache.kafka.common.metrics.stats.Meter
import org.apache.kafka.common.metrics.stats.Rate
import org.apache.kafka.common.metrics.stats.TokenBucket
import org.apache.kafka.common.metrics.stats.WindowedSum
import org.apache.kafka.common.utils.MockTime
import org.apache.kafka.common.utils.SystemTime
import org.apache.kafka.common.utils.Time
import org.junit.jupiter.api.Test
import java.util.concurrent.Callable
import java.util.concurrent.CountDownLatch
import java.util.concurrent.ExecutionException
import java.util.concurrent.Executors
import java.util.concurrent.Future
import java.util.concurrent.TimeUnit
import org.mockito.kotlin.mock
import org.mockito.kotlin.verify
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlin.test.fail

class SensorTest {

    @Test
    fun testRecordLevelEnum() {
        var configLevel = Sensor.RecordingLevel.INFO
        assertTrue(Sensor.RecordingLevel.INFO.shouldRecord(configLevel.id.toInt()))
        assertFalse(Sensor.RecordingLevel.DEBUG.shouldRecord(configLevel.id.toInt()))
        assertFalse(Sensor.RecordingLevel.TRACE.shouldRecord(configLevel.id.toInt()))
        configLevel = Sensor.RecordingLevel.DEBUG
        assertTrue(Sensor.RecordingLevel.INFO.shouldRecord(configLevel.id.toInt()))
        assertTrue(Sensor.RecordingLevel.DEBUG.shouldRecord(configLevel.id.toInt()))
        assertFalse(Sensor.RecordingLevel.TRACE.shouldRecord(configLevel.id.toInt()))
        configLevel = Sensor.RecordingLevel.TRACE
        assertTrue(Sensor.RecordingLevel.INFO.shouldRecord(configLevel.id.toInt()))
        assertTrue(Sensor.RecordingLevel.DEBUG.shouldRecord(configLevel.id.toInt()))
        assertTrue(Sensor.RecordingLevel.TRACE.shouldRecord(configLevel.id.toInt()))
        assertEquals(
            Sensor.RecordingLevel.valueOf(Sensor.RecordingLevel.DEBUG.toString()),
            Sensor.RecordingLevel.DEBUG,
        )
        assertEquals(
            Sensor.RecordingLevel.valueOf(Sensor.RecordingLevel.INFO.toString()),
            Sensor.RecordingLevel.INFO,
        )
        assertEquals(
            Sensor.RecordingLevel.valueOf(Sensor.RecordingLevel.TRACE.toString()),
            Sensor.RecordingLevel.TRACE,
        )
    }

    @Test
    fun testShouldRecordForInfoLevelSensor() {
        var infoSensor = Sensor(
            registry = Metrics(),
            name = "infoSensor",
            parents = emptyList(),
            config = INFO_CONFIG,
            time = SystemTime(),
            inactiveSensorExpirationTimeSeconds = 0,
            recordingLevel = Sensor.RecordingLevel.INFO
        )
        assertTrue(infoSensor.shouldRecord())
        infoSensor = Sensor(
            registry = Metrics(),
            name = "infoSensor",
            parents = emptyList(),
            config = DEBUG_CONFIG,
            time = SystemTime(),
            inactiveSensorExpirationTimeSeconds = 0,
            recordingLevel = Sensor.RecordingLevel.INFO
        )
        assertTrue(infoSensor.shouldRecord())
        infoSensor = Sensor(
            registry = Metrics(),
            name = "infoSensor",
            parents = emptyList(),
            config = TRACE_CONFIG,
            time = SystemTime(),
            inactiveSensorExpirationTimeSeconds = 0,
            recordingLevel = Sensor.RecordingLevel.INFO
        )
        assertTrue(infoSensor.shouldRecord())
    }

    @Test
    fun testShouldRecordForDebugLevelSensor() {
        var debugSensor = Sensor(
            registry = Metrics(),
            name = "debugSensor",
            parents = emptyList(),
            config = INFO_CONFIG,
            time = SystemTime(),
            inactiveSensorExpirationTimeSeconds = 0,
            recordingLevel = Sensor.RecordingLevel.DEBUG
        )
        assertFalse(debugSensor.shouldRecord())
        debugSensor = Sensor(
            registry = Metrics(),
            name = "debugSensor",
            parents = emptyList(),
            config = DEBUG_CONFIG,
            time = SystemTime(),
            inactiveSensorExpirationTimeSeconds = 0,
            recordingLevel = Sensor.RecordingLevel.DEBUG
        )
        assertTrue(debugSensor.shouldRecord())
        debugSensor = Sensor(
            registry = Metrics(),
            name = "debugSensor",
            parents = emptyList(),
            config = TRACE_CONFIG,
            time = SystemTime(),
            inactiveSensorExpirationTimeSeconds = 0,
            recordingLevel = Sensor.RecordingLevel.DEBUG
        )
        assertTrue(debugSensor.shouldRecord())
    }

    @Test
    fun testShouldRecordForTraceLevelSensor() {
        var traceSensor = Sensor(
            registry = Metrics(),
            name = "traceSensor",
            parents = emptyList(),
            config = INFO_CONFIG,
            time = SystemTime(),
            inactiveSensorExpirationTimeSeconds = 0,
            recordingLevel = Sensor.RecordingLevel.TRACE
        )
        assertFalse(traceSensor.shouldRecord())
        traceSensor = Sensor(
            registry = Metrics(),
            name = "traceSensor",
            parents = emptyList(),
            config = DEBUG_CONFIG,
            time = SystemTime(),
            inactiveSensorExpirationTimeSeconds = 0,
            recordingLevel = Sensor.RecordingLevel.TRACE
        )
        assertFalse(traceSensor.shouldRecord())
        traceSensor = Sensor(
            registry = Metrics(),
            name = "traceSensor",
            parents = emptyList(),
            config = TRACE_CONFIG,
            time = SystemTime(),
            inactiveSensorExpirationTimeSeconds = 0,
            recordingLevel = Sensor.RecordingLevel.TRACE
        )
        assertTrue(traceSensor.shouldRecord())
    }

    @Test
    fun testExpiredSensor() {
        val config = MetricConfig()
        val mockTime: Time = MockTime()
        Metrics(
            config = config,
            reporters = mutableListOf(JmxReporter()),
            time = mockTime,
            enableExpiration = true,
        ).use { metrics ->
            val inactiveSensorExpirationTimeSeconds = 60L
            val sensor = Sensor(
                registry = metrics,
                name = "sensor",
                parents = emptyList(),
                config = config,
                time = mockTime,
                inactiveSensorExpirationTimeSeconds = inactiveSensorExpirationTimeSeconds,
                recordingLevel = Sensor.RecordingLevel.INFO,
            )
            assertTrue(sensor.add(metrics.metricName(name = "test1", group = "grp1"), Avg()))
            val emptyTags = emptyMap<String, String>()
            val rateMetricName = MetricName(name = "rate", group = "test", description = "", tags = emptyTags)
            val totalMetricName = MetricName(name = "total", group = "test", description = "", tags = emptyTags)
            val meter = Meter(rateMetricName, totalMetricName)
            assertTrue(sensor.add(meter))
            mockTime.sleep(TimeUnit.SECONDS.toMillis(inactiveSensorExpirationTimeSeconds + 1))
            assertFalse(sensor.add(metrics.metricName("test3", "grp1"), Avg()))
            assertFalse(sensor.add(meter))
        }
    }

    @Test
    fun testIdempotentAdd() {
        val metrics = Metrics()
        val sensor = metrics.sensor("sensor")
        assertTrue(sensor.add(metrics.metricName("test-metric", "test-group"), Avg()))

        // adding the same metric to the same sensor is a no-op
        assertTrue(sensor.add(metrics.metricName("test-metric", "test-group"), Avg()))

        // but adding the same metric to a DIFFERENT sensor is an error
        val anotherSensor = metrics.sensor("another-sensor")
        try {
            anotherSensor.add(metrics.metricName("test-metric", "test-group"), Avg())
            fail("should have thrown")
        } catch (ignored: IllegalArgumentException) {
            // pass
        }

        // note that adding a different metric with the same name is also a no-op
        assertTrue(sensor.add(metrics.metricName("test-metric", "test-group"), WindowedSum()))

        // so after all this, we still just have the original metric registered
        assertEquals(1, sensor.metrics().size)
        assertIs<Avg>(sensor.metrics()[0].measurable())
    }

    /**
     * The Sensor#checkQuotas should be thread-safe since the method may be used by many ReplicaFetcherThreads.
     */
    @Test
    @Throws(InterruptedException::class, ExecutionException::class)
    fun testCheckQuotasInMultiThreads() {
        val metrics = Metrics(
            MetricConfig().apply {
                quota = Quota.upperBound(Double.MAX_VALUE)
                // decreasing the value of time window make SampledStat always record the given value
                timeWindowMs = 1

                // increasing the value of samples make SampledStat store more samples
                samples = 100
            }
        )
        val sensor = metrics.sensor("sensor")
        assertTrue(sensor.add(metrics.metricName("test-metric", "test-group"), Rate()))
        val threadCount = 10
        val latch = CountDownLatch(1)
        val service = Executors.newFixedThreadPool(threadCount)
        val workers: MutableList<Future<Throwable>> = ArrayList(threadCount)
        var needShutdown = true
        try {
            for (i in 0..<threadCount) {
                workers.add(
                    service.submit(Callable {
                        try {
                            assertTrue(latch.await(5, TimeUnit.SECONDS))
                            for (j in 0..19) {
                                sensor.record((j * i).toDouble(),System.currentTimeMillis() + j, false)
                                sensor.checkQuotas()
                            }
                            return@Callable null
                        } catch (e: Throwable) {
                            return@Callable e
                        }
                    })
                )
            }
            latch.countDown()
            service.shutdown()
            assertTrue(service.awaitTermination(10, TimeUnit.SECONDS))
            needShutdown = false
            for (callable in workers) {
                assertTrue(
                    callable.isDone,
                    "If this failure happen frequently, we can try to increase the wait time"
                )
                assertNull(callable.get(), "Sensor#checkQuotas SHOULD be thread-safe!")
            }
        } finally {
            if (needShutdown) service.shutdownNow()
        }
    }

    @Test
    fun shouldReturnPresenceOfMetrics() {
        val metrics = Metrics()
        val sensor = metrics.sensor(name = "sensor")
        assertFalse(sensor.hasMetrics())
        sensor.add(
            metricName = MetricName(
                name = "name1",
                group = "group1",
                description = "description1",
                tags = emptyMap(),
            ),
            stat = WindowedSum(),
        )
        assertTrue(sensor.hasMetrics())
        sensor.add(
            metricName = MetricName(
                name = "name2",
                group = "group2",
                description = "description2",
                tags = emptyMap(),
            ),
            stat = CumulativeCount()
        )
        assertTrue(sensor.hasMetrics())
    }

    @Test
    fun testStrictQuotaEnforcementWithRate() {
        val time: Time = MockTime(0, System.currentTimeMillis(), 0)
        val metrics = Metrics(time = time)
        val sensor = metrics.sensor(
            name = "sensor",
            config = MetricConfig().apply {
                quota = Quota.upperBound(2.0)
                timeWindowMs = 1000
                samples = 11
            },
        )
        val metricName = metrics.metricName("rate", "test-group")
        assertTrue(sensor.add(metricName, Rate()))
        val rateMetric = metrics.metric(metricName)

        // Recording a first value at T+0 to bring the avg rate to 3 which is already above the quota.
        strictRecord(sensor, 30.0, time.milliseconds())
        assertEquals(3.0, rateMetric!!.measurableValue(time.milliseconds()), 0.1)

        // Theoretically, we should wait 5s to bring back the avg rate to the define quota:
        // ((30 / 10) - 2) / 2 * 10 = 5s
        time.sleep(5000)

        // But, recording a second value is rejected because the avg rate is still equal to 3 after 5s.
        assertEquals(3.0, rateMetric.measurableValue(time.milliseconds()), 0.1)
        assertFailsWith<QuotaViolationException> { strictRecord(sensor, 30.0, time.milliseconds()) }
        metrics.close()
    }

    @Test
    fun testStrictQuotaEnforcementWithTokenBucket() {
        val time: Time = MockTime(0, System.currentTimeMillis(), 0)
        val metrics = Metrics(time = time)
        val sensor = metrics.sensor(
            name = "sensor",
            config = MetricConfig().apply {
                quota = Quota.upperBound(2.0)
                timeWindowMs = 1000
                samples = 10
            },
        )
        val metricName = metrics.metricName("credits", "test-group")
        assertTrue(sensor.add(metricName, TokenBucket()))
        val tkMetric = metrics.metric(metricName)!!

        // Recording a first value at T+0 to bring the remaining credits below zero
        strictRecord(sensor, 30.0, time.milliseconds())
        assertEquals(-10.0, tkMetric.measurableValue(time.milliseconds()), 0.1)

        // Theoretically, we should wait 5s to bring back the avg rate to the define quota:
        // 10 / 2 = 5s
        time.sleep(5000)

        // Unlike the default rate based on a windowed sum, it works as expected.
        assertEquals(0.0, tkMetric.measurableValue(time.milliseconds()), 0.1)
        strictRecord(sensor, 30.0, time.milliseconds())
        assertEquals(-30.0, tkMetric.measurableValue(time.milliseconds()), 0.1)
        metrics.close()
    }

    private fun strictRecord(sensor: Sensor, value: Double, timeMs: Long) {
        synchronized(sensor) {
            sensor.checkQuotas(timeMs)
            sensor.record(value, timeMs, false)
        }
    }

    @Test
    fun testRecordAndCheckQuotaUseMetricConfigOfEachStat() {
        val time: Time = MockTime(0, System.currentTimeMillis(), 0)
        val metrics = Metrics(time = time)
        val sensor = metrics.sensor("sensor")
        val stat1 = mock<MeasurableStat>()
        val stat1Name = metrics.metricName("stat1", "test-group")
        val stat1Config = MetricConfig().apply {
            quota = Quota.upperBound(5.0)
        }
        sensor.add(stat1Name, stat1, stat1Config)
        val stat2 = mock<MeasurableStat>()
        val stat2Name = metrics.metricName("stat2", "test-group")
        val stat2Config = MetricConfig().apply {
            quota = Quota.upperBound(10.0)
        }
        sensor.add(stat2Name, stat2, stat2Config)
        sensor.record(10.0, 1)
        verify(stat1).record(stat1Config, 10.0, 1)
        verify(stat2).record(stat2Config, 10.0, 1)
        sensor.checkQuotas(2)
        verify(stat1).measure(stat1Config, 2)
        verify(stat2).measure(stat2Config, 2)
        metrics.close()
    }

    @Test
    fun testUpdatingMetricConfigIsReflectedInTheSensor() {
        val time: Time = MockTime(0, System.currentTimeMillis(), 0)
        val metrics = Metrics(time = time)
        val sensor = metrics.sensor("sensor")
        val stat = mock<MeasurableStat>()
        val statName = metrics.metricName("stat", "test-group")
        val statConfig = MetricConfig().apply {
            quota = Quota.upperBound(5.0)
        }
        sensor.add(statName, stat, statConfig)
        sensor.record(10.0, 1)
        verify(stat).record(statConfig, 10.0, 1)
        sensor.checkQuotas(2)
        verify(stat).measure(statConfig, 2)

        // Update the config of the KafkaMetric
        val newConfig = MetricConfig().apply {
            quota = Quota.upperBound(10.0)
        }
        metrics.metric(statName)!!.config(newConfig)
        sensor.record(10.0, 3)
        verify(stat).record(newConfig, 10.0, 3)
        sensor.checkQuotas(4)
        verify(stat).measure(newConfig, 4)
        metrics.close()
    }

    companion object {

        private val INFO_CONFIG = MetricConfig().apply {
            recordingLevel = Sensor.RecordingLevel.INFO
        }

        private val DEBUG_CONFIG = MetricConfig().apply {
            recordingLevel = Sensor.RecordingLevel.DEBUG
        }

        private val TRACE_CONFIG = MetricConfig().apply {
            recordingLevel = Sensor.RecordingLevel.TRACE
        }
    }
}
