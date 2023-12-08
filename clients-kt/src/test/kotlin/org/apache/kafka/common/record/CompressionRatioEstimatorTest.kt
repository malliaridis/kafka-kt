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

package org.apache.kafka.common.record

import org.apache.kafka.common.record.CompressionRatioEstimator.setEstimation
import org.apache.kafka.common.record.CompressionRatioEstimator.updateEstimation
import org.junit.jupiter.api.Test
import kotlin.test.assertTrue

class CompressionRatioEstimatorTest {

    @Test
    fun testUpdateEstimation() {
        data class EstimationsObservedRatios(
            var currentEstimation: Float,
            var observedRatio: Float,
        )

        // If currentEstimation is smaller than observedRatio, the updatedCompressionRatio is currentEstimation plus
        // COMPRESSION_RATIO_DETERIORATE_STEP 0.05, otherwise currentEstimation minus COMPRESSION_RATIO_IMPROVING_STEP
        // 0.005. There are four cases,and updatedCompressionRatio shouldn't smaller than observedRatio in all of cases.
        // Refer to non test code for more details.
        val estimationsObservedRatios = listOf(
            EstimationsObservedRatios(currentEstimation = 0.8f, observedRatio = 0.84f),
            EstimationsObservedRatios(currentEstimation = 0.6f, observedRatio = 0.7f),
            EstimationsObservedRatios(currentEstimation = 0.6f, observedRatio = 0.4f),
            EstimationsObservedRatios(currentEstimation = 0.004f, observedRatio = 0.001f)
        )
        for (estimationsObservedRatio in estimationsObservedRatios) {
            val topic = "tp"
            setEstimation(
                topic = topic,
                type = CompressionType.ZSTD,
                ratio = estimationsObservedRatio.currentEstimation,
            )
            val updatedCompressionRatio = updateEstimation(
                topic = topic,
                type = CompressionType.ZSTD,
                observedRatio = estimationsObservedRatio.observedRatio,
            )
            assertTrue(updatedCompressionRatio >= estimationsObservedRatio.observedRatio)
        }
    }
}
