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

import org.apache.kafka.common.metrics.CompoundStat
import org.apache.kafka.common.metrics.CompoundStat.NamedMeasurable
import org.apache.kafka.common.metrics.MetricConfig
import org.apache.kafka.common.metrics.stats.Histogram.BinScheme
import org.apache.kafka.common.metrics.stats.Histogram.ConstantBinScheme
import org.apache.kafka.common.metrics.stats.Histogram.LinearBinScheme
import org.slf4j.LoggerFactory

/**
 * A compound stat that reports one or more percentiles
 */
class Percentiles(
    sizeInBytes: Int,
    min: Double = 0.0,
    max: Double,
    bucketing: BucketSizing,
    private val percentiles: List<Percentile>
) : SampledStat(0.0), CompoundStat {

    private val log = LoggerFactory.getLogger(Percentiles::class.java)

    private val buckets: Int

    private var binScheme: BinScheme

    private val min: Double

    private val max: Double

    init {
        buckets = sizeInBytes / 4
        this.min = min
        this.max = max
        binScheme = when (bucketing) {
            BucketSizing.CONSTANT -> ConstantBinScheme(buckets, min, max)
            BucketSizing.LINEAR -> {
                require(min == 0.0) { "Linear bucket sizing requires min to be 0.0." }
                LinearBinScheme(buckets, max)
            }
        }
    }

    override fun stats(): List<NamedMeasurable> {
        val ms = percentiles.map { (name, pct) ->
            NamedMeasurable(name) { config, now -> value(config, now, pct / 100.0) }
        }
        return ms
    }

    fun value(config: MetricConfig?, now: Long, quantile: Double): Double {
        purgeObsoleteSamples(config!!, now)
        val count = samples.sumOf { it.eventCount.toDouble() }

        if (count == 0.0) return Double.NaN

        var sum = 0.0f
        val quant = quantile.toFloat()

        for (b in 0 until buckets) {
            samples.forEach { sample ->
                sample as HistogramSample
                val hist = sample.histogram.counts()
                sum += hist[b]
                if (sum / count > quant) return binScheme.fromBin(b)
            }
        }

        return Double.POSITIVE_INFINITY
    }

    override fun combine(samples: List<Sample>, config: MetricConfig?, now: Long): Double {
        return value(
            config = config,
            now = now,
            quantile = 0.5,
        )
    }

    override fun newSample(timeMs: Long): Sample = HistogramSample(binScheme, timeMs)

    override fun update(sample: Sample, config: MetricConfig?, value: Double, timeMs: Long) {
        val boundedValue: Double = if (value > max) {
            log.debug(
                "Received value {} which is greater than max recordable value {}, will be pinned " +
                        "to the max value",
                value,
                max
            )
            max
        } else if (value < min) {
            log.debug(
                "Received value {} which is less than min recordable value {}, will be pinned to " +
                        "the min value",
                value,
                min
            )
            min
        } else value

        val hist = sample as HistogramSample
        hist.histogram.record(boundedValue)
    }

    private class HistogramSample(
        scheme: BinScheme,
        now: Long
    ) : Sample(
        initialValue = 0.0,
        lastWindowMs = now,
    ) {

        val histogram: Histogram = Histogram(scheme)

        override fun reset(now: Long) {
            super.reset(now)
            histogram.clear()
        }
    }

    enum class BucketSizing {
        CONSTANT,
        LINEAR
    }
}
