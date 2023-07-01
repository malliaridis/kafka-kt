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

import org.apache.kafka.common.metrics.MeasurableStat
import org.apache.kafka.common.metrics.MetricConfig

/**
 * A SampledStat records a single scalar value measured over one or more samples. Each sample is
 * recorded over a configurable window. The window can be defined by number of events or elapsed
 * time (or both, if both are given the window is complete when *either* the event count or elapsed
 * time criterion is met).
 *
 * All the samples are combined to produce the measurement. When a window is complete the oldest
 * sample is cleared and recycled to begin recording the next sample.
 *
 * Subclasses of this class define different statistics measured using this basic pattern.
 */
abstract class SampledStat(private val initialValue: Double) : MeasurableStat {

    private var current = 0

    protected var samples: MutableList<Sample> = ArrayList(2)

    override fun record(config: MetricConfig, value: Double, timeMs: Long) {
        var sample = current(timeMs)
        if (sample.isComplete(timeMs, config)) sample = advance(config, timeMs)
        update(sample, config, value, timeMs)
        sample.eventCount += 1
    }

    private fun advance(config: MetricConfig, timeMs: Long): Sample {
        current = (current + 1) % config.samples

        return if (current >= samples.size) {
            val sample = newSample(timeMs)
            samples.add(sample)
            sample
        } else {
            val sample = current(timeMs)
            sample.reset(timeMs)
            sample
        }
    }

    protected open fun newSample(timeMs: Long): Sample {
        return Sample(initialValue, timeMs)
    }

    override fun measure(config: MetricConfig, now: Long): Double {
        purgeObsoleteSamples(config, now)
        return combine(samples, config, now)
    }

    fun current(timeMs: Long): Sample {
        if (samples.size == 0) samples.add(newSample(timeMs))
        return samples[current]
    }

    fun oldest(now: Long): Sample {
        if (samples.size == 0) samples.add(newSample(now))
        var oldest = samples[0]
        for (i in 1 until samples.size) {
            val curr = samples[i]
            if (curr.lastWindowMs < oldest.lastWindowMs) oldest = curr
        }
        return oldest
    }

    override fun toString(): String {
        return "SampledStat(" +
                "initialValue=$initialValue" +
                ", current=$current" +
                ", samples=$samples" +
                ')'
    }

    protected abstract fun update(
        sample: Sample,
        config: MetricConfig?,
        value: Double,
        timeMs: Long
    )

    abstract fun combine(samples: List<Sample>, config: MetricConfig?, now: Long): Double

    /**
     * Timeout any windows that have expired in the absence of any events.
     */
    fun purgeObsoleteSamples(config: MetricConfig, now: Long) {
        val expireAge = config.samples * config.timeWindowMs
        samples.forEach { sample ->
            if (now - sample.lastWindowMs >= expireAge) sample.reset(now)
        }
    }

    open class Sample(
        var initialValue: Double,
        var lastWindowMs: Long,
    ) {

        var eventCount: Long = 0

        var value: Double = initialValue

        open fun reset(now: Long) {
            eventCount = 0
            lastWindowMs = now
            value = initialValue
        }

        fun isComplete(timeMs: Long, config: MetricConfig): Boolean {
            return timeMs - lastWindowMs >= config.timeWindowMs || eventCount >= config.eventWindow
        }

        override fun toString(): String {
            return "Sample(" +
                    "value=$value" +
                    ", eventCount=$eventCount" +
                    ", lastWindowMs=$lastWindowMs" +
                    ", initialValue=$initialValue" +
                    ')'
        }
    }
}
