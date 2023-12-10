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

package org.apache.kafka.clients.consumer.internals

import org.apache.kafka.common.Metric
import org.apache.kafka.common.MetricNameTemplate
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.metrics.Sensor
import org.apache.kafka.common.metrics.stats.Avg
import org.apache.kafka.common.metrics.stats.Max
import org.apache.kafka.common.metrics.stats.Meter
import org.apache.kafka.common.metrics.stats.Min
import org.apache.kafka.common.metrics.stats.SampledStat
import org.apache.kafka.common.metrics.stats.Value

/**
 * `SensorBuilder` takes a bit of the boilerplate out of creating [sensors][Sensor] for recording
 * [metrics][Metric].
 */
class SensorBuilder(
    private val metrics: Metrics,
    name: String,
    tagsSupplier: () -> Map<String, String> = { emptyMap() },
) {

    private var sensor: Sensor

    private var prexisting = false

    private var tags: Map<String, String>

    init {
        val sensor = metrics.getSensor(name)
        if (sensor != null) {
            this.sensor = sensor
            tags = emptyMap()
            prexisting = true
        } else {
            this.sensor = metrics.sensor(name)
            tags = tagsSupplier()
            prexisting = false
        }
    }

    fun withAvg(name: MetricNameTemplate): SensorBuilder {
        if (!prexisting) sensor.add(metrics.metricInstance(name, tags), Avg())
        return this
    }

    fun withMin(name: MetricNameTemplate): SensorBuilder {
        if (!prexisting) sensor.add(metrics.metricInstance(name, tags), Min())
        return this
    }

    fun withMax(name: MetricNameTemplate): SensorBuilder {
        if (!prexisting) sensor.add(metrics.metricInstance(name, tags), Max())
        return this
    }

    fun withValue(name: MetricNameTemplate): SensorBuilder {
        if (!prexisting) sensor.add(metrics.metricInstance(name, tags), Value())
        return this
    }

    fun withMeter(rateName: MetricNameTemplate, totalName: MetricNameTemplate): SensorBuilder {
        if (!prexisting) sensor.add(
            Meter(
                rateMetricName = metrics.metricInstance(rateName, tags),
                totalMetricName = metrics.metricInstance(totalName, tags),
            )
        )
        return this
    }

    fun withMeter(
        sampledStat: SampledStat,
        rateName: MetricNameTemplate,
        totalName: MetricNameTemplate,
    ): SensorBuilder {
        if (!prexisting) sensor.add(
            Meter(
                rateStat = sampledStat,
                rateMetricName = metrics.metricInstance(rateName, tags),
                totalMetricName = metrics.metricInstance(totalName, tags),
            )
        )
        return this
    }

    fun build(): Sensor = sensor
}
