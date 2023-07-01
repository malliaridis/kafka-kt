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

import kotlin.math.sqrt

class Histogram(private val binScheme: BinScheme) {
    
    private val hist: FloatArray = FloatArray(binScheme.bins())
    
    private var count = 0.0

    fun record(value: Double) {
        hist[binScheme.toBin(value)] += 1.0f
        count += 1.0
    }

    fun value(quantile: Double): Double {
        if (count == 0.0) return Double.NaN
        if (quantile > 1.00) return Float.POSITIVE_INFINITY.toDouble()
        if (quantile < 0.00) return Float.NEGATIVE_INFINITY.toDouble()
        var sum = 0.0f
        val quant = quantile.toFloat()

        for (i in 0 until hist.size - 1) {
            sum += hist[i]
            if (sum / count > quant) return binScheme.fromBin(i)
        }

        return binScheme.fromBin(hist.size - 1)
    }

    fun counts(): FloatArray = hist

    fun clear() {
        hist.fill(0.0f)
        count = 0.0
    }

    override fun toString(): String {
        val b = StringBuilder("{")
        for (i in 0 until hist.size - 1) {
            b.append(String.format("%.10f", binScheme.fromBin(i)))
            b.append(':')
            b.append(String.format("%.0f", hist[i]))
            b.append(',')
        }
        b.append(Float.POSITIVE_INFINITY)
        b.append(':')
        b.append(String.format("%.0f", hist[hist.size - 1]))
        b.append('}')
        return b.toString()
    }

    /**
     * An algorithm for determining the bin in which a value is to be placed as well as calculating
     * the upper end of each bin.
     */
    interface BinScheme {
        
        /**
         * Get the number of bins.
         *
         * @return the number of bins
         */
        fun bins(): Int

        /**
         * Determine the 0-based bin number in which the supplied value should be placed.
         *
         * @param value the value
         * @return the 0-based index of the bin
         */
        fun toBin(value: Double): Int

        /**
         * Determine the value at the upper range of the specified bin.
         *
         * @param bin the 0-based bin number
         * @return the value at the upper end of the bin; or
         * [negative infinity][Float.NEGATIVE_INFINITY] if the bin number is negative or
         * [positive infinity][Float.POSITIVE_INFINITY] if the 0-based bin number is greater than or
         * equal to the [number of bins][bins].
         */
        fun fromBin(bin: Int): Double
    }

    /**
     * A scheme for calculating the bins where the width of each bin is a constant determined by the
     * range of values and the number of bins.
     *
     * @constructor Create a bin scheme with the specified number of bins that all have the same
     * width.
     * @property bins the number of bins; must be at least 2
     * @property min the minimum value to be counted in the bins
     * @param max the maximum value to be counted in the bins
     */
    class ConstantBinScheme(
        private val bins: Int,
        private val min: Double,
        max: Double,
    ) : BinScheme {

        private val bucketWidth: Double

        private val maxBinNumber: Int

        init {
            require(bins >= 2) { "Must have at least 2 bins." }
            bucketWidth = (max - min) / bins
            maxBinNumber = bins - 1
        }

        override fun bins(): Int = bins

        override fun fromBin(bin: Int): Double {
            return if (bin < MIN_BIN_NUMBER) Float.NEGATIVE_INFINITY.toDouble()
            else if (bin > maxBinNumber) Float.POSITIVE_INFINITY.toDouble()
            else min + bin * bucketWidth
        }

        override fun toBin(x: Double): Int {
            val binNumber = ((x - min) / bucketWidth).toInt()
            return if (binNumber < MIN_BIN_NUMBER) MIN_BIN_NUMBER
            else binNumber.coerceAtMost(maxBinNumber)
        }

        companion object {
            private const val MIN_BIN_NUMBER = 0
        }
    }

    /**
     * A scheme for calculating the bins where the width of each bin is one more than the previous
     * bin, and therefore the bin widths are increasing at a linear rate. However, the bin widths
     * are scaled such that the specified range of values will all fit within the bins (e.g., the
     * upper range of the last bin is equal to the maximum value).
     *
     * @constructor Create a linear bin scheme with the specified number of bins and the maximum
     * value to be counted in the bins.
     *
     * @param bins the number of bins; must be at least 2
     * @param max the maximum value to be counted in the bins
     */
    class LinearBinScheme(
        private val bins: Int,
        private val max: Double,
    ) : BinScheme {

        private val scale: Double

        init {
            require(bins >= 2) { "Must have at least 2 bins." }
            val denom = bins * (bins - 1.0) / 2.0
            scale = max / denom
        }

        override fun bins(): Int = bins

        override fun fromBin(bin: Int): Double {
            return if (bin > bins - 1) Float.POSITIVE_INFINITY.toDouble()
            else if (bin < 0.0000) Float.NEGATIVE_INFINITY.toDouble()
            else scale * (bin * (bin + 1.0)) / 2.0
        }

        override fun toBin(x: Double): Int {
            require(x >= 0.0) {"Values less than 0.0 not accepted." }

            return if (x > max) bins - 1
            else (-0.5 + 0.5 * sqrt(1.0 + 8.0 * x / scale)).toInt()
        }
    }
}