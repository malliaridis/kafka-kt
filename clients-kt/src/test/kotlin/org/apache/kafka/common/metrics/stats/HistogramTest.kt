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

import org.apache.kafka.common.metrics.stats.Histogram.BinScheme
import org.apache.kafka.common.metrics.stats.Histogram.ConstantBinScheme
import org.apache.kafka.common.metrics.stats.Histogram.LinearBinScheme
import org.junit.jupiter.api.Test
import kotlin.random.Random
import kotlin.test.assertEquals

class HistogramTest {

    @Test
    fun testHistogram() {
        val scheme: BinScheme = ConstantBinScheme(10, -5.0, 5.0)
        val hist = Histogram(scheme)
        for (i in -5..4) hist.record(i.toDouble())
        for (i in 0..9) assertEquals(scheme.fromBin(i), hist.value(i / 10.0 + EPS), EPS)
    }

    @Test
    fun testConstantBinScheme() {
        val scheme = ConstantBinScheme(5, -5.0, 5.0)
        assertEquals(0, scheme.toBin(-5.01), "A value below the lower bound should map to the first bin")
        assertEquals(4, scheme.toBin(5.01), "A value above the upper bound should map to the last bin")
        assertEquals(0, scheme.toBin(-5.0001), "Check boundary of bucket 0")
        assertEquals(0, scheme.toBin(-5.0000), "Check boundary of bucket 0")
        assertEquals(0, scheme.toBin(-4.99999), "Check boundary of bucket 0")
        assertEquals(0, scheme.toBin(-3.00001), "Check boundary of bucket 0")
        assertEquals(1, scheme.toBin(-3.0), "Check boundary of bucket 1")
        assertEquals(1, scheme.toBin(-1.00001), "Check boundary of bucket 1")
        assertEquals(2, scheme.toBin(-1.0), "Check boundary of bucket 2")
        assertEquals(2, scheme.toBin(0.99999), "Check boundary of bucket 2")
        assertEquals(3, scheme.toBin(1.0), "Check boundary of bucket 3")
        assertEquals(3, scheme.toBin(2.99999), "Check boundary of bucket 3")
        assertEquals(4, scheme.toBin(3.0), "Check boundary of bucket 4")
        assertEquals(4, scheme.toBin(4.9999), "Check boundary of bucket 4")
        assertEquals(4, scheme.toBin(5.000), "Check boundary of bucket 4")
        assertEquals(4, scheme.toBin(5.001), "Check boundary of bucket 4")
        assertEquals(Float.NEGATIVE_INFINITY.toDouble(), scheme.fromBin(-1), 0.001)
        assertEquals(Float.POSITIVE_INFINITY.toDouble(), scheme.fromBin(5), 0.001)
        assertEquals(-5.0, scheme.fromBin(0), 0.001)
        assertEquals(-3.0, scheme.fromBin(1), 0.001)
        assertEquals(-1.0, scheme.fromBin(2), 0.001)
        assertEquals(1.0, scheme.fromBin(3), 0.001)
        assertEquals(3.0, scheme.fromBin(4), 0.001)
        checkBinningConsistency(scheme)
    }

    @Test
    fun testConstantBinSchemeWithPositiveRange() {
        val scheme = ConstantBinScheme(5, 0.0, 5.0)
        assertEquals(0, scheme.toBin(-1.0), "A value below the lower bound should map to the first bin")
        assertEquals(4, scheme.toBin(5.01), "A value above the upper bound should map to the last bin")
        assertEquals(0, scheme.toBin(-0.0001), "Check boundary of bucket 0")
        assertEquals(0, scheme.toBin(0.0000), "Check boundary of bucket 0")
        assertEquals(0, scheme.toBin(0.0001), "Check boundary of bucket 0")
        assertEquals(0, scheme.toBin(0.9999), "Check boundary of bucket 0")
        assertEquals(1, scheme.toBin(1.0000), "Check boundary of bucket 1")
        assertEquals(1, scheme.toBin(1.0001), "Check boundary of bucket 1")
        assertEquals(1, scheme.toBin(1.9999), "Check boundary of bucket 1")
        assertEquals(2, scheme.toBin(2.0000), "Check boundary of bucket 2")
        assertEquals(2, scheme.toBin(2.0001), "Check boundary of bucket 2")
        assertEquals(2, scheme.toBin(2.9999), "Check boundary of bucket 2")
        assertEquals(3, scheme.toBin(3.0000), "Check boundary of bucket 3")
        assertEquals(3, scheme.toBin(3.0001), "Check boundary of bucket 3")
        assertEquals(3, scheme.toBin(3.9999), "Check boundary of bucket 3")
        assertEquals(4, scheme.toBin(4.0000), "Check boundary of bucket 4")
        assertEquals(4, scheme.toBin(4.9999), "Check boundary of bucket 4")
        assertEquals(4, scheme.toBin(5.0000), "Check boundary of bucket 4")
        assertEquals(4, scheme.toBin(5.0001), "Check boundary of bucket 4")
        assertEquals(Float.NEGATIVE_INFINITY.toDouble(), scheme.fromBin(-1), 0.001)
        assertEquals(Float.POSITIVE_INFINITY.toDouble(), scheme.fromBin(5), 0.001)
        assertEquals(0.0, scheme.fromBin(0), 0.001)
        assertEquals(1.0, scheme.fromBin(1), 0.001)
        assertEquals(2.0, scheme.fromBin(2), 0.001)
        assertEquals(3.0, scheme.fromBin(3), 0.001)
        assertEquals(4.0, scheme.fromBin(4), 0.001)
        checkBinningConsistency(scheme)
    }

    @Test
    fun testLinearBinScheme() {
        val scheme = LinearBinScheme(10, 10.0)
        assertEquals(Float.NEGATIVE_INFINITY.toDouble(), scheme.fromBin(-1), 0.001)
        assertEquals(Float.POSITIVE_INFINITY.toDouble(), scheme.fromBin(11), 0.001)
        assertEquals(0.0, scheme.fromBin(0), 0.001)
        assertEquals(0.2222, scheme.fromBin(1), 0.001)
        assertEquals(0.6666, scheme.fromBin(2), 0.001)
        assertEquals(1.3333, scheme.fromBin(3), 0.001)
        assertEquals(2.2222, scheme.fromBin(4), 0.001)
        assertEquals(3.3333, scheme.fromBin(5), 0.001)
        assertEquals(4.6667, scheme.fromBin(6), 0.001)
        assertEquals(6.2222, scheme.fromBin(7), 0.001)
        assertEquals(8.0000, scheme.fromBin(8), 0.001)
        assertEquals(10.000, scheme.fromBin(9), 0.001)
        assertEquals(0, scheme.toBin(0.0000))
        assertEquals(0, scheme.toBin(0.2221))
        assertEquals(1, scheme.toBin(0.2223))
        assertEquals(2, scheme.toBin(0.6667))
        assertEquals(3, scheme.toBin(1.3334))
        assertEquals(4, scheme.toBin(2.2223))
        assertEquals(5, scheme.toBin(3.3334))
        assertEquals(6, scheme.toBin(4.6667))
        assertEquals(7, scheme.toBin(6.2223))
        assertEquals(8, scheme.toBin(8.0000))
        assertEquals(9, scheme.toBin(10.000))
        assertEquals(9, scheme.toBin(10.001))
        assertEquals(Float.POSITIVE_INFINITY.toDouble(), scheme.fromBin(10), 0.001)
        checkBinningConsistency(scheme)
    }

    private fun checkBinningConsistency(scheme: BinScheme) {
        for (bin in 0 until scheme.bins()) {
            val fromBin = scheme.fromBin(bin)
            val binAgain = scheme.toBin(fromBin + EPS)
            assertEquals(
                expected = bin,
                actual = binAgain,
                message =
                "unbinning and rebinning the bin $bin gave a different result ($fromBin was placed in bin $binAgain )",
            )
        }
    }

    companion object {

        private const val EPS = 0.0000001

        @JvmStatic
        fun main(args: Array<String>) {
            println("[-100, 100]:")
            listOf(
                ConstantBinScheme(1000, -100.0, 100.0),
                ConstantBinScheme(100, -100.0, 100.0),
                ConstantBinScheme(10, -100.0, 100.0)
            ).forEach { scheme ->
                val h = Histogram(scheme)
                for (i in 0..9999) h.record(200.0 * Random.nextDouble() - 100.0)
                var quantile = 0.0
                while (quantile < 1.0) {
                    System.out.printf("%5.2f: %.1f, ", quantile, h.value(quantile))
                    quantile += 0.05
                }
                println()
            }
            println("[0, 1000]")
            listOf(
                LinearBinScheme(1000, 1000.0),
                LinearBinScheme(100, 1000.0),
                LinearBinScheme(10, 1000.0),
            ).forEach { scheme ->
                val h = Histogram(scheme)
                for (i in 0..9999) h.record(1000.0 * Random.nextDouble())
                var quantile = 0.0
                while (quantile < 1.0) {
                    System.out.printf("%5.2f: %.1f, ", quantile, h.value(quantile))
                    quantile += 0.05
                }
                println()
            }
        }
    }
}
