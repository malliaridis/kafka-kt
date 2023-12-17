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

import com.yammer.metrics.core.Gauge
import com.yammer.metrics.core.Histogram
import com.yammer.metrics.core.Meter
import com.yammer.metrics.core.MetricName
import com.yammer.metrics.core.Timer
import java.util.concurrent.TimeUnit
import org.apache.kafka.common.utils.Sanitizer.jmxSanitize

class KafkaMetricsGroup(private val klass: Class<*>) {

    /**
     * Creates a new MetricName object for gauges, meters, etc. created for this metrics group.
     *
     * @param name Descriptive name of the metric.
     * @param tags Additional attributes which mBean will have.
     * @return Sanitized metric name object.
     */
    fun metricName(name: String, tags: Map<String, String>): MetricName {
        val pkg = if (klass.getPackage() == null) "" else klass.getPackage().name
        val simpleName = klass.getSimpleName().replace("\\$$".toRegex(), "")
        return explicitMetricName(pkg, simpleName, name, tags)
    }

    fun <T> newGauge(name: String, metric: Gauge<T>?, tags: Map<String, String>): Gauge<T> =
        KafkaYammerMetrics.defaultRegistry().newGauge(metricName(name, tags), metric)

    fun <T> newGauge(name: String, metric: Gauge<T>?): Gauge<T> = newGauge(name, metric, emptyMap())

    fun newMeter(
        name: String,
        eventType: String?,
        timeUnit: TimeUnit?,
        tags: Map<String, String> = emptyMap(),
    ): Meter = KafkaYammerMetrics.defaultRegistry().newMeter(metricName(name, tags), eventType, timeUnit)

    fun newMeter(metricName: MetricName?, eventType: String?, timeUnit: TimeUnit?): Meter =
        KafkaYammerMetrics.defaultRegistry().newMeter(metricName, eventType, timeUnit)

    fun newHistogram(name: String, biased: Boolean = true, tags: Map<String, String> = emptyMap()): Histogram =
        KafkaYammerMetrics.defaultRegistry().newHistogram(metricName(name, tags), biased)

    fun newTimer(
        name: String,
        durationUnit: TimeUnit?,
        rateUnit: TimeUnit?,
        tags: Map<String, String> = emptyMap(),
    ): Timer = KafkaYammerMetrics.defaultRegistry().newTimer(metricName(name, tags), durationUnit, rateUnit)

    fun removeMetric(name: String, tags: Map<String, String> = emptyMap()) =
        KafkaYammerMetrics.defaultRegistry().removeMetric(metricName(name, tags))

    companion object {

        fun explicitMetricName(
            group: String?, typeName: String?,
            name: String, tags: Map<String, String>,
        ): MetricName {
            val nameBuilder = StringBuilder(100)
            nameBuilder.append(group)
            nameBuilder.append(":type=")
            nameBuilder.append(typeName)
            if (name.isNotEmpty()) {
                nameBuilder.append(",name=")
                nameBuilder.append(name)
            }
            val scope = toScope(tags)
            val tagsName = toMBeanName(tags)
            tagsName?.let { nameBuilder.append(",").append(it) }

            return MetricName(group, typeName, name, scope, nameBuilder.toString())
        }

        private fun toMBeanName(tags: Map<String, String>): String? {
            val filteredTags = tags.filter { (_, value) -> value != "" }
            return if (filteredTags.isNotEmpty()) {
                val tagsString = filteredTags
                    .map { (key, value) -> "$key=" + jmxSanitize(value) }
                    .joinToString(",")
                tagsString
            } else null
        }

        private fun toScope(tags: Map<String, String>): String? {
            val filteredTags = tags.filter { (_, value) -> value != "" }
            return if (filteredTags.isNotEmpty()) {
                // convert dot to _ since reporters like Graphite typically use dot to represent hierarchy
                val tagsString = filteredTags.toSortedMap()
                    .map { (key, value) -> "$key." + value.replace("\\.".toRegex(), "_") }
                    .joinToString(".")
                tagsString
            } else null
        }
    }
}
