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

package org.apache.kafka.common.metrics.internals

import org.apache.kafka.common.MetricName
import org.apache.kafka.common.metrics.MetricConfig
import org.apache.kafka.common.metrics.Metrics
import org.junit.jupiter.api.Test
import org.slf4j.LoggerFactory
import kotlin.test.assertEquals
import kotlin.test.assertFalse

class IntGaugeSuiteTest {

    @Test
    fun testCreateAndClose() {
        val suite = createIntGaugeSuite()
        assertEquals(3, suite.maxEntries())
        suite.close()
        suite.close()
        suite.metrics().close()
    }

    @Test
    fun testCreateMetrics() {
        val suite = createIntGaugeSuite()
        suite.increment("foo")
        var values: Map<String, Int?> = suite.values()
        assertEquals(1, values["foo"])
        assertEquals(1, values.size)
        suite.increment("foo")
        suite.increment("bar")
        suite.increment("baz")
        suite.increment("quux")
        values = suite.values()

        assertEquals(2, values["foo"])
        assertEquals(1, values["bar"])
        assertEquals(1, values["baz"])
        assertEquals(3, values.size)
        assertFalse(values.containsKey("quux"))
        suite.close()
        suite.metrics.close()
    }

    @Test
    fun testCreateAndRemoveMetrics() {
        val suite = createIntGaugeSuite()
        suite.increment("foo")
        suite.decrement("foo")
        suite.increment("foo")
        suite.increment("foo")
        suite.increment("bar")
        suite.decrement("bar")
        suite.increment("baz")
        suite.increment("quux")
        val values = suite.values()

        assertEquals(2, values["foo"])
        assertFalse(values.containsKey("bar"))
        assertEquals(1, values["baz"])
        assertEquals(1, values["quux"])
        assertEquals(3, values.size)
        suite.close()
        suite.metrics.close()
    }

    companion object {

        private val log = LoggerFactory.getLogger(IntGaugeSuiteTest::class.java)

        private fun createIntGaugeSuite(): IntGaugeSuite<String> {
            val config = MetricConfig()
            val metrics = Metrics(config)
            return IntGaugeSuite(
                log = log,
                suiteName = "mySuite",
                metrics = metrics,
                metricNameCalculator = { name ->
                    MetricName(
                        name = name,
                        group = "group",
                        description = "myMetric",
                        tags = emptyMap(),
                    )
                },
                maxEntries = 3,
            )
        }
    }
}
