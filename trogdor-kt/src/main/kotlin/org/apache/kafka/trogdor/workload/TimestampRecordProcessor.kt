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

import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.JsonNode
import java.nio.ByteBuffer
import java.nio.ByteOrder
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.common.utils.Time
import org.apache.kafka.trogdor.common.JsonUtil
import org.slf4j.LoggerFactory

/**
 * This class will process records containing timestamps and generate a histogram based on the data.
 * It will then be present in the status from the `ConsumeBenchWorker` class. This must be used with
 * a timestamped `PayloadGenerator` implementation.
 *
 * Example spec:
 * ```
 * {
 *     "type": "timestamp",
 *     "histogramMaxMs": 10000,
 *     "histogramMinMs": 0,
 *     "histogramStepMs": 1
 * }
 * ```
 *
 * This will track total E2E latency up to 10 seconds, using 1ms resolution and a timestamp size of 8 bytes.
 */
class TimestampRecordProcessor @JsonCreator constructor(
    @param:JsonProperty("histogramMaxMs") private val histogramMaxMs: Int,
    @param:JsonProperty("histogramMinMs") private val histogramMinMs: Int,
    @param:JsonProperty("histogramStepMs") private val histogramStepMs: Int,
) : RecordProcessor {
    
    private val buffer = ByteBuffer.allocate(Long.SIZE_BYTES).apply {
        order(ByteOrder.LITTLE_ENDIAN)
    }

    private val histogram = Histogram((histogramMaxMs - histogramMinMs) / histogramStepMs)

    private val log = LoggerFactory.getLogger(TimestampRecordProcessor::class.java)

    @JsonProperty
    fun histogramMaxMs(): Int = histogramMaxMs

    @JsonProperty
    fun histogramMinMs(): Int = histogramMinMs

    @JsonProperty
    fun histogramStepMs(): Int = histogramStepMs

    private fun putHistogram(latency: Long) {
        histogram.add(((latency - histogramMinMs) / histogramStepMs).coerceAtLeast(0L))
    }

    @Synchronized
    override fun processRecords(consumerRecords: ConsumerRecords<ByteArray?, ByteArray?>) {
        // Save the current time to prevent skew by processing time.
        val curTime = Time.SYSTEM.milliseconds()
        for (record in consumerRecords) {
            try {
                buffer.clear()
                buffer.put(record.value, 0, Long.SIZE_BYTES)
                buffer.rewind()
                putHistogram(curTime - buffer.getLong())
            } catch (e: RuntimeException) {
                log.error("Error in processRecords:", e)
            }
        }
    }

    override fun processorStatus(): JsonNode? {
        val summary = histogram.summarize(PERCENTILES)
        val statusData = StatusData(
            averageLatencyMs = summary.average * histogramStepMs + histogramMinMs,
            p50LatencyMs = summary.percentiles[0].value * histogramStepMs + histogramMinMs,
            p95LatencyMs = summary.percentiles[1].value * histogramStepMs + histogramMinMs,
            p99LatencyMs = summary.percentiles[2].value * histogramStepMs + histogramMinMs,
        )
        return JsonUtil.JSON_SERDE.valueToTree(statusData)
    }

    private class StatusData @JsonCreator constructor(
        @param:JsonProperty("averageLatencyMs") private val averageLatencyMs: Float,
        @param:JsonProperty("p50LatencyMs") private val p50LatencyMs: Int,
        @param:JsonProperty("p95LatencyMs") private val p95LatencyMs: Int,
        @param:JsonProperty("p99LatencyMs") private val p99LatencyMs: Int,
    ) {

        @JsonProperty
        fun averageLatencyMs(): Float = averageLatencyMs

        @JsonProperty
        fun p50LatencyMs(): Int = p50LatencyMs

        @JsonProperty
        fun p95LatencyMs(): Int = p95LatencyMs

        @JsonProperty
        fun p99LatencyMs(): Int = p99LatencyMs

        companion object {

            /**
             * The percentiles to use when calculating the histogram data.
             * These should match up with the p50LatencyMs, p95LatencyMs, etc. fields.
             */
            val PERCENTILES = floatArrayOf(0.5f, 0.95f, 0.99f)
        }
    }

    companion object {
        val PERCENTILES = floatArrayOf(0.5f, 0.95f, 0.99f)
    }
}
