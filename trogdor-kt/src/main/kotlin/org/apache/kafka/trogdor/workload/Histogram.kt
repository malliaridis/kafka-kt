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

package org.apache.kafka.trogdor.workload

import org.slf4j.LoggerFactory

/**
 * A histogram that can easily find the average, median etc of a large number of samples in a restricted domain.
 */
class Histogram(maxValue: Int) {
    
    private val log = LoggerFactory.getLogger(Histogram::class.java)
    
    private val counts: IntArray = IntArray(maxValue + 1)

    /**
     * Add a new value to the histogram.
     *
     * Note that the value will be clipped to the maximum value available in the Histogram instance.
     * So if the histogram has 100 buckets, inserting 101 will increment the last bucket.
     */
    fun add(value: Int) {
        var mutableValue = value
        if (mutableValue < 0) throw RuntimeException("invalid negative value.")
        
        if (mutableValue >= counts.size) {
            mutableValue = counts.size - 1
        }
        synchronized(this) {
            val curCount = counts[mutableValue]
            if (curCount < Int.MAX_VALUE) {
                counts[mutableValue] = counts[mutableValue] + 1
            }
        }
    }

    /**
     * Add a new value to the histogram.
     *
     * Note that the value will be clipped to the maximum value available in the Histogram instance.
     * This method is provided for convenience, but handles the same numeric range as the method which
     * takes an int.
     */
    fun add(value: Long) {
        if (value > Int.MAX_VALUE) add(Int.MAX_VALUE)
        else if (value < Int.MIN_VALUE) add(Int.MIN_VALUE)
        else add(value.toInt())
    }

    @JvmOverloads
    fun summarize(percentiles: FloatArray = FloatArray(0)): Summary {
        val countsCopy = IntArray(counts.size)
        synchronized(this) { System.arraycopy(counts, 0, countsCopy, 0, counts.size) }
        // Verify that the percentiles array is sorted and positive.
        val prev = 0f
        for (i in percentiles.indices) {
            if (percentiles[i] < prev) throw RuntimeException(
                "Invalid percentiles fraction array. Bad element ${percentiles[i]}. " +
                        "The array must be sorted and non-negative."
            )
            if (percentiles[i] > 1.0f) throw RuntimeException(
                "Invalid percentiles fraction array. Bad element ${percentiles[i]}. " +
                         "Elements must be less than or equal to 1."
            )
        }
        // Find out how many total samples we have, and what the average is.
        var numSamples = 0L
        var total = 0f
        for (i in countsCopy.indices) {
            val count = countsCopy[i].toLong()
            numSamples += count
            total += (i * count)
        }
        val average = if ((numSamples == 0L)) 0.0f else (total / numSamples)
        val percentileSummaries = summarizePercentiles(countsCopy, percentiles, numSamples)
        return Summary(numSamples, average, percentileSummaries)
    }

    private fun summarizePercentiles(
        countsCopy: IntArray,
        percentiles: FloatArray,
        numSamples: Long,
    ): List<PercentileSummary> {
        if (percentiles.isEmpty()) return emptyList()

        val summaries: MutableList<PercentileSummary> = ArrayList(percentiles.size)
        var i = 0
        var j = 0
        var seen: Long = 0
        var next = (numSamples * percentiles[0]).toLong()
        while (true) {
            if (i == countsCopy.size - 1) {
                while (j < percentiles.size) {
                    summaries.add(PercentileSummary(percentiles[j], i))
                    j++
                }
                return summaries
            }
            seen += countsCopy[i].toLong()
            while (seen >= next) {
                summaries.add(PercentileSummary(percentiles[j], i))
                j++
                if (j == percentiles.size) {
                    return summaries
                }
                next = (numSamples * percentiles[j]).toLong()
            }
            i++
        }
    }

    /**
     * @property numSamples The total number of samples.
     * @property average The average of all samples.
     * @property percentiles Percentile information.
     * percentile(fraction=0.99) will have a value which is greater than or equal to 99%
     * of the samples. percentile(fraction=0.5) is the median sample. And so forth.
     */
    class Summary internal constructor(
        val numSamples: Long,
        val average: Float,
        val percentiles: List<PercentileSummary>,
    ) {

        @Deprecated(
            message = "Use property instead",
            replaceWith = ReplaceWith("numSamples"),
        )
        fun numSamples(): Long = numSamples

        @Deprecated(
            message = "Use property instead",
            replaceWith = ReplaceWith("average"),
        )
        fun average(): Float = average

        @Deprecated(
            message = "Use property instead",
            replaceWith = ReplaceWith("percentiles"),
        )
        fun percentiles(): List<PercentileSummary> = percentiles
    }

    /**
     * Information about a percentile.
     *
     * @property fraction The fraction of samples which are less than or equal to the value of this percentile.
     * @property value The value of this percentile.
     */
    class PercentileSummary internal constructor(
        val fraction: Float,
        val value: Int,
    ) {

        @Deprecated(
            message = "Use property instead",
            replaceWith = ReplaceWith("fraction"),
        )
        fun fraction(): Float = fraction

        @Deprecated(
            message = "Use property instead",
            replaceWith = ReplaceWith("value"),
        )
        fun value(): Int = value
    }
}
