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

import org.apache.kafka.common.metrics.stats.Avg
import org.apache.kafka.common.metrics.stats.CumulativeSum
import org.apache.kafka.common.utils.Time
import org.junit.jupiter.api.Test
import java.lang.management.ManagementFactory
import javax.management.ObjectName
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class JmxReporterTest {

    @Test
    @Throws(Exception::class)
    fun testJmxRegistration() {
        val newMetrics = Metrics()
        val server = ManagementFactory.getPlatformMBeanServer()
        newMetrics.use { metrics ->
            val reporter = JmxReporter()
            metrics.addReporter(reporter)
            
            assertFalse(server.isRegistered(ObjectName(":type=grp1")))
            
            val sensor = metrics.sensor("kafka.requests")
            sensor.add(metrics.metricName("pack.bean1.avg", "grp1"), Avg())
            sensor.add(metrics.metricName("pack.bean2.total", "grp2"), CumulativeSum())
            
            assertTrue(server.isRegistered(ObjectName(":type=grp1")))
            assertEquals(Double.NaN, server.getAttribute(ObjectName(":type=grp1"), "pack.bean1.avg"))
            assertTrue(server.isRegistered(ObjectName(":type=grp2")))
            assertEquals(0.0, server.getAttribute(ObjectName(":type=grp2"), "pack.bean2.total"))
            
            var metricName = metrics.metricName("pack.bean1.avg", "grp1")
            val mBeanName = JmxReporter.getMBeanName("", metricName)
            assertTrue(reporter.containsMbean(mBeanName))
            metrics.removeMetric(metricName)
            assertFalse(reporter.containsMbean(mBeanName))
            
            assertFalse(server.isRegistered(ObjectName(":type=grp1")))
            assertTrue(server.isRegistered(ObjectName(":type=grp2")))
            assertEquals(0.0, server.getAttribute(ObjectName(":type=grp2"), "pack.bean2.total"))
            
            metricName = metrics.metricName("pack.bean2.total", "grp2")
            metrics.removeMetric(metricName)
            assertFalse(reporter.containsMbean(mBeanName))
            
            assertFalse(server.isRegistered(ObjectName(":type=grp1")))
            assertFalse(server.isRegistered(ObjectName(":type=grp2")))
        }
    }

    @Test
    @Throws(Exception::class)
    fun testJmxRegistrationSanitization() {
        val newMetrics = Metrics()
        val server = ManagementFactory.getPlatformMBeanServer()
        newMetrics.use { metrics ->
            metrics.addReporter(JmxReporter())

            val sensor = metrics.sensor("kafka.requests")
            sensor.add(metrics.metricName("name", "group", "desc", "id", "foo*"), CumulativeSum())
            sensor.add(metrics.metricName("name", "group", "desc", "id", "foo+"), CumulativeSum())
            sensor.add(metrics.metricName("name", "group", "desc", "id", "foo?"), CumulativeSum())
            sensor.add(metrics.metricName("name", "group", "desc", "id", "foo:"), CumulativeSum())
            sensor.add(metrics.metricName("name", "group", "desc", "id", "foo%"), CumulativeSum())

            assertTrue(server.isRegistered(ObjectName(":type=group,id=\"foo\\*\"")))
            assertEquals(0.0, server.getAttribute(ObjectName(":type=group,id=\"foo\\*\""), "name"))
            assertTrue(server.isRegistered(ObjectName(":type=group,id=\"foo+\"")))
            assertEquals(0.0, server.getAttribute(ObjectName(":type=group,id=\"foo+\""), "name"))
            assertTrue(server.isRegistered(ObjectName(":type=group,id=\"foo\\?\"")))
            assertEquals(0.0, server.getAttribute(ObjectName(":type=group,id=\"foo\\?\""), "name"))
            assertTrue(server.isRegistered(ObjectName(":type=group,id=\"foo:\"")))
            assertEquals(0.0, server.getAttribute(ObjectName(":type=group,id=\"foo:\""), "name"))
            assertTrue(server.isRegistered(ObjectName(":type=group,id=foo%")))
            assertEquals(0.0, server.getAttribute(ObjectName(":type=group,id=foo%"), "name"))

            metrics.removeMetric(metrics.metricName("name", "group", "desc", "id", "foo*"))
            metrics.removeMetric(metrics.metricName("name", "group", "desc", "id", "foo+"))
            metrics.removeMetric(metrics.metricName("name", "group", "desc", "id", "foo?"))
            metrics.removeMetric(metrics.metricName("name", "group", "desc", "id", "foo:"))
            metrics.removeMetric(metrics.metricName("name", "group", "desc", "id", "foo%"))

            assertFalse(server.isRegistered(ObjectName(":type=group,id=\"foo\\*\"")))
            assertFalse(server.isRegistered(ObjectName(":type=group,id=foo+")))
            assertFalse(server.isRegistered(ObjectName(":type=group,id=\"foo\\?\"")))
            assertFalse(server.isRegistered(ObjectName(":type=group,id=\"foo:\"")))
            assertFalse(server.isRegistered(ObjectName(":type=group,id=foo%")))
        }
    }

    @Test
    @Throws(Exception::class)
    fun testPredicateAndDynamicReload() {
        val newMetrics = Metrics()
        val server = ManagementFactory.getPlatformMBeanServer()

        val configs = mutableMapOf<String, String?>()
        configs[JmxReporter.EXCLUDE_CONFIG] =
            JmxReporter.getMBeanName("", newMetrics.metricName("pack.bean2.total", "grp2"))

        newMetrics.use { metrics ->
            val reporter = JmxReporter()
            reporter.configure(configs)
            metrics.addReporter(reporter)

            val sensor = metrics.sensor("kafka.requests")
            sensor.add(metrics.metricName("pack.bean2.avg", "grp1"), Avg())
            sensor.add(metrics.metricName("pack.bean2.total", "grp2"), CumulativeSum())
            sensor.record()

            assertTrue(server.isRegistered(ObjectName(":type=grp1")))
            assertEquals(1.0, server.getAttribute(ObjectName(":type=grp1"), "pack.bean2.avg"))
            assertFalse(server.isRegistered(ObjectName(":type=grp2")))

            sensor.record()
            configs[JmxReporter.EXCLUDE_CONFIG] =
                JmxReporter.getMBeanName("", metrics.metricName("pack.bean2.avg", "grp1"))
            reporter.reconfigure(configs)

            assertFalse(server.isRegistered(ObjectName(":type=grp1")))
            assertTrue(server.isRegistered(ObjectName(":type=grp2")))
            assertEquals(2.0, server.getAttribute(ObjectName(":type=grp2"), "pack.bean2.total"))

            metrics.removeMetric(metrics.metricName("pack.bean2.total", "grp2"))
            assertFalse(server.isRegistered(ObjectName(":type=grp2")))
        }
    }

    @Test
    @Throws(Exception::class)
    fun testJmxPrefix() {
        val reporter = JmxReporter()
        val metricsContext = KafkaMetricsContext("kafka.server")
        val metricConfig = MetricConfig()
        val newMetrics = Metrics(
            config = metricConfig,
            reporters = mutableListOf(reporter),
            time = Time.SYSTEM,
            metricsContext = metricsContext,
        )
        val server = ManagementFactory.getPlatformMBeanServer()
        newMetrics.use { metrics ->
            val sensor = metrics.sensor("kafka.requests")
            sensor.add(metrics.metricName("pack.bean1.avg", "grp1"), Avg())
            assertEquals(
                expected = "kafka.server",
                actual = server.getObjectInstance(ObjectName("kafka.server:type=grp1")).objectName.domain,
            )
        }
    }

    @Test
    @Throws(Exception::class)
    fun testDeprecatedJmxPrefixWithDefaultMetrics() {
        @Suppress("Deprecation")
        val reporter = JmxReporter("my-prefix")

        // for backwards compatibility, ensure prefix does not get overridden by the default empty namespace
        // in metricscontext
        val metricConfig = MetricConfig()
        val newMetrics = Metrics(
            config = metricConfig,
            reporters = mutableListOf(reporter),
            time = Time.SYSTEM,
        )
        val server = ManagementFactory.getPlatformMBeanServer()
        newMetrics.use { metrics ->
            val sensor = metrics.sensor("my-sensor")
            sensor.add(metrics.metricName("pack.bean1.avg", "grp1"), Avg())
            assertEquals(
                expected = "my-prefix",
                actual = server.getObjectInstance(ObjectName("my-prefix:type=grp1")).objectName.domain
            )
        }
    }
}
