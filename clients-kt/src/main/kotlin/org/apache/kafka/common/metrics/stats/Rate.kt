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
import org.apache.kafka.common.metrics.MeasurableStat
import org.apache.kafka.common.metrics.MetricConfig
import org.apache.kafka.common.metrics.internals.MetricsUtils.convert

/**
 * The rate of the given quantity. By default, this is the total observed over a set of samples from
 * a sampled statistic divided by the elapsed time over the sample windows. Alternative
 * [SampledStat] implementations can be provided, however, to record the rate of occurrences (e.g.
 * the count of values measured over the time interval) or other such values.
 */
open class Rate(
    internal val unit: TimeUnit = TimeUnit.SECONDS,
    internal val stat: SampledStat = WindowedSum()
) : MeasurableStat {

    fun unitName(): String = unit.name
        .substring(0, unit.name.length - 2)
        .lowercase()

    override fun record(config: MetricConfig, value: Double, timeMs: Long) = stat.record(
        config = config,
        value = value,
        timeMs = timeMs,
    )

    override fun measure(config: MetricConfig, now: Long): Double {
        val value = stat.measure(config, now)
        return value / convert(windowSize(config, now), unit)
    }

    open fun windowSize(config: MetricConfig, now: Long): Long {
        // purge old samples before we compute the window size
        stat.purgeObsoleteSamples(config, now)

        /*
         * Here we check the total amount of time elapsed since the oldest non-obsolete window. This
         * give the total windowSize of the batch which is the time used for Rate computation.
         * However, there is an issue if we do not have sufficient data for e.g. if only 1 second
         * has elapsed in a 30 second window, the measured rate will be very high. Hence we assume
         * that the elapsed time is always N-1 complete windows plus whatever fraction of the final
         * window is complete.
         *
         * Note that we could simply count the amount of time elapsed in the current window and add
         * n-1 windows to get the total time, but this approach does not account for sleeps.
         * SampledStat only creates samples whenever record is called, if no record is called for a
         * period of time that time is not accounted for in windowSize and produces incorrect
         * results.
         */
        var totalElapsedTimeMs = now - stat.oldest(now).lastWindowMs

        // Check how many full windows of data we have currently retained
        val numFullWindows = (totalElapsedTimeMs / config.timeWindowMs).toInt()
        val minFullWindows = config.samples - 1

        // If the available windows are less than the minimum required, add the difference to the
        // totalElapsedTime
        if (numFullWindows < minFullWindows)
            totalElapsedTimeMs += (minFullWindows - numFullWindows) * config.timeWindowMs

        // If window size is being calculated at the exact beginning of the window with no prior
        // samples, the window size will result in a value of 0. Calculation of rate over a window
        // is size 0 is undefined, hence, we assume the minimum window size to be at least 1ms.
        return totalElapsedTimeMs.coerceAtLeast(1)
    }

    override fun toString(): String = "Rate(unit=$unit, stat=$stat)"
}
