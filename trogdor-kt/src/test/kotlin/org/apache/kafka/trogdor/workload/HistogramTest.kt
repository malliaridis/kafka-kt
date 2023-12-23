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

import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

class HistogramTest {

    @Test
    fun testHistogramAverage() {
        val empty = createHistogram(1)
        assertEquals(0, empty.summarize(FloatArray(0)).average.toInt())
        val histogram = createHistogram(70, 1, 2, 3, 4, 5, 6, 1)
        assertEquals(3, histogram.summarize(FloatArray(0)).average.toInt())
        histogram.add(60)
        assertEquals(10, histogram.summarize(FloatArray(0)).average.toInt())
    }

    @Test
    fun testHistogramSamples() {
        val empty = createHistogram(100)
        assertEquals(0, empty.summarize(FloatArray(0)).numSamples)

        val histogram = createHistogram(100, 4, 8, 2, 4, 1, 100, 150)

        assertEquals(7, histogram.summarize(FloatArray(0)).numSamples)
        histogram.add(60)
        assertEquals(8, histogram.summarize(FloatArray(0)).numSamples)
    }

    @Test
    fun testHistogramPercentiles() {
        var histogram = createHistogram(100, 1, 2, 3, 4, 5, 6, 80, 90)
        val percentiles = floatArrayOf(0.5f, 0.90f, 0.99f, 1f)
        var summary = histogram.summarize(percentiles)
        assertEquals(8, summary.numSamples)
        assertEquals(4, summary.percentiles[0].value)
        assertEquals(80, summary.percentiles[1].value)
        assertEquals(80, summary.percentiles[2].value)
        assertEquals(90, summary.percentiles[3].value)
        histogram.add(30)
        histogram.add(30)
        histogram.add(30)

        summary = histogram.summarize(floatArrayOf(0.5f))
        assertEquals(11, summary.numSamples)
        assertEquals(5, summary.percentiles[0].value)

        val empty = createHistogram(100)
        summary = empty.summarize(floatArrayOf(0.5f))
        assertEquals(0, summary.percentiles[0].value)

        histogram = createHistogram(1000)
        histogram.add(100)
        histogram.add(200)
        summary = histogram.summarize(floatArrayOf(0f, 0.5f, 1.0f))
        assertEquals(0, summary.percentiles[0].value)
        assertEquals(100, summary.percentiles[1].value)
        assertEquals(200, summary.percentiles[2].value)
    }

    companion object {

        private fun createHistogram(maxValue: Int, vararg values: Int): Histogram {
            val histogram = Histogram(maxValue)
            for (value in values) histogram.add(value)
            return histogram
        }
    }
}
