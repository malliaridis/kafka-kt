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

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentMap
import kotlin.math.max

/**
 * This class help estimate the compression ratio for each topic and compression type combination.
 */
object CompressionRatioEstimator {

    // The constant speed to increase compression ratio when a batch compresses better than expected.
    const val COMPRESSION_RATIO_IMPROVING_STEP = 0.005f

    // The minimum speed to decrease compression ratio when a batch compresses worse than expected.
    const val COMPRESSION_RATIO_DETERIORATE_STEP = 0.05f

    private val COMPRESSION_RATIO: ConcurrentMap<String, FloatArray> = ConcurrentHashMap()

    /**
     * Update the compression ratio estimation for a topic and compression type.
     *
     * @param topic the topic to update compression ratio estimation.
     * @param type the compression type.
     * @param observedRatio the observed compression ratio.
     * @return the compression ratio estimation after the update.
     */
    fun updateEstimation(topic: String, type: CompressionType, observedRatio: Float): Float {
        val compressionRatioForTopic = getAndCreateEstimationIfAbsent(topic)
        val typeId = type.id.toInt()
        val currentEstimation = compressionRatioForTopic[typeId]

        synchronized(compressionRatioForTopic) {
            if (observedRatio > currentEstimation) compressionRatioForTopic[typeId] = max(
                currentEstimation + COMPRESSION_RATIO_DETERIORATE_STEP,
                observedRatio
            ) else if (observedRatio < currentEstimation) {
                compressionRatioForTopic[typeId] = max(
                    currentEstimation - COMPRESSION_RATIO_IMPROVING_STEP,
                    observedRatio
                )
            }
        }

        return compressionRatioForTopic[typeId]
    }

    /**
     * Get the compression ratio estimation for a topic and compression type.
     */
    fun estimation(topic: String, type: CompressionType): Float {
        val compressionRatioForTopic = getAndCreateEstimationIfAbsent(topic)
        return compressionRatioForTopic[type.id.toInt()]
    }

    /**
     * Reset the compression ratio estimation to the initial values for a topic.
     */
    fun resetEstimation(topic: String) {
        val compressionRatioForTopic = getAndCreateEstimationIfAbsent(topic)
        synchronized(compressionRatioForTopic) {
            for (type in CompressionType.entries)
                compressionRatioForTopic[type.id.toInt()] = type.rate
        }
    }

    /**
     * Remove the compression ratio estimation for a topic.
     */
    fun removeEstimation(topic: String) = COMPRESSION_RATIO.remove(topic)

    /**
     * Set the compression estimation for a topic compression type combination. This method is for
     * unit test purpose.
     */
    fun setEstimation(topic: String, type: CompressionType, ratio: Float) {
        val compressionRatioForTopic = getAndCreateEstimationIfAbsent(topic)
        synchronized(compressionRatioForTopic) {
            compressionRatioForTopic[type.id.toInt()] = ratio
        }
    }

    private fun getAndCreateEstimationIfAbsent(topic: String): FloatArray {
        var compressionRatioForTopic = COMPRESSION_RATIO[topic]
        if (compressionRatioForTopic == null) {
            compressionRatioForTopic = initialCompressionRatio()
            val existingCompressionRatio =
                COMPRESSION_RATIO.putIfAbsent(topic, compressionRatioForTopic)
            // Someone created the compression ratio array before us, use it.
            if (existingCompressionRatio != null) return existingCompressionRatio
        }
        return compressionRatioForTopic
    }

    private fun initialCompressionRatio(): FloatArray {
        val compressionRatio = FloatArray(CompressionType.entries.size)
        for (type in CompressionType.entries)
            compressionRatio[type.id.toInt()] = type.rate

        return compressionRatio
    }
}
