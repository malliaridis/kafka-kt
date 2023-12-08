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

import org.apache.kafka.common.metrics.MetricConfig
import org.apache.kafka.common.metrics.internals.MetricsUtils.convert
import org.apache.kafka.common.utils.MockTime
import org.apache.kafka.common.utils.Time
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource
import java.util.concurrent.TimeUnit
import kotlin.test.assertEquals
import kotlin.test.assertFalse

class RateTest {

    private lateinit var r: Rate

    private lateinit var timeClock: Time

    @BeforeEach
    fun setup() {
        r = Rate()
        timeClock = MockTime()
    }

    // Tests the scenario where the recording and measurement is done before the window for first sample finishes
    // with no prior samples retained.
    @ParameterizedTest
    @CsvSource("1,1", "1,11", "11,1", "11,11")
    fun testRateWithNoPriorAvailableSamples(numSample: Int, sampleWindowSizeSec: Int) {
        val config = MetricConfig().samples(numSample).timeWindow(sampleWindowSizeSec.toLong(), TimeUnit.SECONDS)
        val sampleValue = 50.0
        // record at beginning of the window
        r.record(config, sampleValue, timeClock.milliseconds())
        // forward time till almost the end of window
        val measurementTime = TimeUnit.SECONDS.toMillis(sampleWindowSizeSec.toLong()) - 1
        timeClock.sleep(measurementTime)
        // calculate rate at almost the end of window
        val observedRate = r.measure(config, timeClock.milliseconds())
        assertFalse(java.lang.Double.isNaN(observedRate))

        // In a scenario where sufficient number of samples is not available yet, the rate calculation algorithm assumes
        // presence of N-1 (where N = numSample) prior samples with sample values of 0. Hence, the window size for rate
        // calculation accounts for N-1 prior samples
        val dummyPriorSamplesAssumedByAlgorithm = numSample - 1
        val windowSize =
            convert(measurementTime, TimeUnit.SECONDS) + dummyPriorSamplesAssumedByAlgorithm * sampleWindowSizeSec
        val expectedRatePerSec = sampleValue / windowSize
        assertEquals(expectedRatePerSec, observedRate, EPS)
    }

    companion object {
        private const val EPS = 0.000001
    }
}
