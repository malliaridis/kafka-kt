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

import java.util.concurrent.TimeUnit
import org.apache.kafka.common.MetricName
import org.apache.kafka.common.metrics.CompoundStat
import org.apache.kafka.common.metrics.CompoundStat.NamedMeasurable
import org.apache.kafka.common.metrics.MetricConfig

/**
 * A compound stat that includes a rate metric and a cumulative total metric.
 *
 * @constructor Construct a Meter with provided time unit
 */
class Meter(
    private val rateMetricName: MetricName,
    private val totalMetricName: MetricName,
    unit: TimeUnit = TimeUnit.SECONDS,
    rateStat: SampledStat = WindowedSum(),
) : CompoundStat {

    private val rate: Rate = Rate(unit = unit, stat = rateStat)

    private val total: CumulativeSum = CumulativeSum()

    init {
        require(rateStat is WindowedSum) {
            "Meter is supported only for WindowedCount or WindowedSum."
        }
    }

    override fun stats(): List<NamedMeasurable> = listOf(
        NamedMeasurable(totalMetricName, total),
        NamedMeasurable(rateMetricName, rate),
    )

    override fun record(config: MetricConfig, value: Double, timeMs: Long) {
        rate.record(config, value, timeMs)
        // Total metrics with Count stat should record 1.0 (as recorded in the count)
        val totalValue = if (rate.stat is WindowedCount) 1.0 else value
        total.record(config, totalValue, timeMs)
    }

    override fun toString(): String {
        return "Meter(" +
                "rate=$rate" +
                ", total=$total" +
                ", rateMetricName=$rateMetricName" +
                ", totalMetricName=$totalMetricName" +
                ')'
    }
}
