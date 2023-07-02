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

import java.lang.IllegalArgumentException
import org.apache.kafka.common.MetricName
import org.apache.kafka.common.metrics.CompoundStat
import org.apache.kafka.common.metrics.CompoundStat.NamedMeasurable
import org.apache.kafka.common.metrics.MetricConfig
import org.apache.kafka.common.metrics.stats.Histogram.BinScheme
import org.apache.kafka.common.metrics.stats.Histogram.ConstantBinScheme

/**
 * A [CompoundStat] that represents a normalized distribution with a [Frequency] metric for each
 * bucketed value. The values of the [Frequency] metrics specify the frequency of the center value
 * appearing relative to the total number of values recorded.
 *
 *
 * For example, consider a component that records failure or success of an operation using boolean
 * values, with one metric to capture the percentage of operations that failed another to capture
 * the percentage of operations that succeeded.
 *
 *
 * This can be accomplish by created a [Sensor][org.apache.kafka.common.metrics.Sensor] to record
 * the values, with 0.0 for false and 1.0 for true. Then, create a single [Frequencies] object that
 * has two [Frequency] metrics: one centered around 0.0 and another centered around 1.0. The
 * [Frequencies] object is a [CompoundStat], and so it can be
 * [added directly to a Sensor][org.apache.kafka.common.metrics.Sensor.add] so the metrics are
 * created automatically.
 *
 * @constructor Create a Frequencies that captures the values in the specified range into the given
 * number of buckets, where the buckets are centered around the minimum, maximum, and intermediate
 * values.
 *
 * @param buckets the number of buckets; must be at least 1
 * @param min the minimum value to be captured
 * @param max the maximum value to be captured
 * @param frequencies the list of [Frequency] metrics, which at most should be one per bucket
 * centered on the bucket's value, though not every bucket need to correspond to a metric if the
 * value is not needed
 * @throws IllegalArgumentException if any of the [Frequency] objects do not have a
 * [center value][Frequency.centerValue] within the specified range
 */
class Frequencies(
    buckets: Int,
    min: Double,
    max: Double,
    private val frequencies: List<Frequency>
) : SampledStat(0.0), CompoundStat {

    constructor(
        buckets: Int,
        min: Double,
        max: Double,
        vararg frequencies: Frequency
    ) : this(
        buckets = buckets,
        min = min,
        max = max,
        frequencies = frequencies.toList(),
    )

    private val binScheme: BinScheme

    init {
        require(max >= min) {
            "The maximum value $max must be greater than the minimum value $min"
        }
        require (buckets >= 1) { "Must be at least 1 bucket" }

        require(buckets >= frequencies.size) { "More frequencies than buckets" }

        frequencies.forEach { freq: Frequency ->
            require ( freq.centerValue in min .. max) {
                "The frequency centered at '${freq.centerValue}' is not within the range [$min,$max]"
            }
        }

        val halfBucketWidth = (max - min) / (buckets - 1) / 2.0
        binScheme = ConstantBinScheme(
            bins = buckets,
            min = min - halfBucketWidth,
            max = max + halfBucketWidth,
        )
    }

    override fun stats(): List<NamedMeasurable> {
        return frequencies.map {frequency ->
            NamedMeasurable(frequency.name) { config, now ->
                frequency(config, now, frequency.centerValue)
            }
        }
    }

    /**
     * Return the computed frequency describing the number of occurrences of the values in the
     * bucket for the given center point, relative to the total number of occurrences in the
     * samples.
     *
     * @param config the metric configuration
     * @param now the current time in milliseconds
     * @param centerValue the value corresponding to the center point of the bucket
     * @return the frequency of the values in the bucket relative to the total number of samples
     */
    fun frequency(config: MetricConfig, now: Long, centerValue: Double): Double {
        purgeObsoleteSamples(config, now)

        val totalCount: Long = samples.sumOf { it.eventCount }
        if (totalCount == 0L) return 0.0

        // Add up all the counts in the bin corresponding to the center value
        var count = 0.0f
        val binNum = binScheme.toBin(centerValue)
        samples.forEach { s: Sample ->
            val sample = s as HistogramSample
            val hist = sample.histogram.counts()
            count += hist[binNum]
        }

        // Compute the ratio of counts to total counts
        return count / totalCount.toDouble()
    }

    fun totalCount(): Double {
        var count: Long = 0
        for (sample: Sample in samples) {
            count += sample.eventCount
        }
        return count.toDouble()
    }

    override fun combine(samples: List<Sample>, config: MetricConfig?, now: Long): Double = totalCount()

    override fun newSample(timeMs: Long): Sample = HistogramSample(binScheme, timeMs)

    override fun update(sample: Sample, config: MetricConfig?, value: Double, timeMs: Long) {
        val hist = sample as HistogramSample
        hist.histogram.record(value)
    }

    private class HistogramSample(
        scheme: BinScheme,
        now: Long,
    ) : Sample(
        initialValue = 0.0,
        lastWindowMs = now
    ) {

        val histogram: Histogram = Histogram(scheme)

        override fun reset(now: Long) {
            super.reset(now)
            histogram.clear()
        }
    }

    companion object {

        /**
         * Create a Frequencies instance with metrics for the frequency of a boolean sensor that
         * records 0.0 for false and 1.0 for true.
         *
         * @param falseMetricName the name of the metric capturing the frequency of failures; may
         * be `null` if not needed
         * @param trueMetricName the name of the metric capturing the frequency of successes; may
         * be `null` if not needed
         * @return the Frequencies instance
         * @throws IllegalArgumentException if both `falseMetricName` and `trueMetricName` are
         * `null`
         */
        fun forBooleanValues(
            falseMetricName: MetricName? = null,
            trueMetricName: MetricName? = null
        ): Frequencies {
            val frequencies: MutableList<Frequency> = ArrayList()

            if (falseMetricName != null)
                frequencies.add(Frequency(falseMetricName, 0.0))

            if (trueMetricName != null)
                frequencies.add(Frequency(trueMetricName, 1.0))

            if (frequencies.isEmpty())
                throw IllegalArgumentException("Must specify at least one metric name")

            return Frequencies(
                buckets = 2,
                min = 0.0,
                max = 1.0,
                frequencies = frequencies,
            )
        }
    }
}