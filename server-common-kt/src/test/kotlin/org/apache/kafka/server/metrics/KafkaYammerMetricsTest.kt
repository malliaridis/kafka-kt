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

package org.apache.kafka.server.metrics

import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull

internal class KafkaYammerMetricsTest {
    
    @Test
    fun testUntaggedMetric() {
        val metricName = KafkaYammerMetrics.getMetricName(
            group = "kafka.metrics",
            typeName = "TestMetrics",
            name = "UntaggedMetric"
        )
        assertEquals("kafka.metrics", metricName.group)
        assertEquals("TestMetrics", metricName.type)
        assertEquals("UntaggedMetric", metricName.name)
        assertEquals("kafka.metrics:type=TestMetrics,name=UntaggedMetric", metricName.mBeanName)
        assertNull(metricName.scope)
    }

    @Test
    fun testTaggedMetricName() {
        val tags = LinkedHashMap<String, String>()
        tags["foo"] = "bar"
        tags["bar"] = "baz"
        tags["baz"] = "raz.taz"
        val metricName = KafkaYammerMetrics.getMetricName(
            group = "kafka.metrics",
            typeName = "TestMetrics",
            name = "TaggedMetric",
            tags = tags,
        )
        assertEquals("kafka.metrics", metricName.group)
        assertEquals("TestMetrics", metricName.type)
        assertEquals("TaggedMetric", metricName.name)

        // MBean name should preserve initial ordering
        assertEquals(
            "kafka.metrics:type=TestMetrics,name=TaggedMetric,foo=bar,bar=baz,baz=raz.taz",
            metricName.mBeanName
        )

        // Scope should be sorted by key
        assertEquals("bar.baz.baz.raz_taz.foo.bar", metricName.scope)
    }

    @Test
    fun testTaggedMetricNameWithEmptyValue() {
        val tags = LinkedHashMap<String, String>()
        tags["foo"] = "bar"
        tags["bar"] = ""
        tags["baz"] = "raz.taz"
        val metricName = KafkaYammerMetrics.getMetricName(
            group = "kafka.metrics",
            typeName = "TestMetrics",
            name = "TaggedMetric",
            tags = tags,
        )
        assertEquals("kafka.metrics", metricName.group)
        assertEquals("TestMetrics", metricName.type)
        assertEquals("TaggedMetric", metricName.name)

        // MBean name should preserve initial ordering (with empty key value removed)
        assertEquals("kafka.metrics:type=TestMetrics,name=TaggedMetric,foo=bar,baz=raz.taz", metricName.mBeanName)

        // Scope should be sorted by key (with empty key value removed)
        assertEquals("baz.raz_taz.foo.bar", metricName.scope)
    }
}
