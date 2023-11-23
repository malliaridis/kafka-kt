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

package org.apache.kafka.common.metrics

import java.util.concurrent.TimeUnit
import org.apache.kafka.common.metrics.Sensor.RecordingLevel

/**
 * Configuration values for metrics
 */
class MetricConfig {

    var quota: Quota? = null

    var samples = 2
        set(value) {
            require(samples >= 1) { "The number of samples must be at least 1." }
            field = value
        }

    var eventWindow: Long = Long.MAX_VALUE

    var timeWindowMs: Long = TimeUnit.MILLISECONDS.convert(30, TimeUnit.SECONDS)

    var tags: Map<String, String?> = emptyMap()

    var recordingLevel: RecordingLevel = RecordingLevel.INFO

    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("quota")
    )
    fun quota(): Quota? = quota

    @Deprecated("Use property instead")
    fun quota(quota: Quota?): MetricConfig {
        this.quota = quota
        return this
    }

    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("eventWindow")
    )
    fun eventWindow(): Long = eventWindow

    @Deprecated("Use property instead")
    fun eventWindow(window: Long): MetricConfig {
        eventWindow = window
        return this
    }

    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("timeWindowMs")
    )
    fun timeWindowMs(): Long = timeWindowMs

    @Deprecated("Use property instead and manually convert time unit")
    fun timeWindow(window: Long, unit: TimeUnit?): MetricConfig {
        timeWindowMs = TimeUnit.MILLISECONDS.convert(window, unit)
        return this
    }

    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("tags")
    )
    fun tags(): Map<String, String?> = tags

    @Deprecated("Use property instead")
    fun tags(tags: Map<String, String>): MetricConfig {
        this.tags = tags
        return this
    }

    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("samples")
    )
    fun samples(): Int = samples

    @Deprecated("Use property instead")
    fun samples(samples: Int): MetricConfig {
        require(samples >= 1) { "The number of samples must be at least 1." }
        this.samples = samples
        return this
    }

    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("recordingLevel")
    )
    fun recordLevel(): RecordingLevel = recordingLevel

    @Deprecated("Use property instead")
    fun recordLevel(recordingLevel: RecordingLevel): MetricConfig {
        this.recordingLevel = recordingLevel
        return this
    }
}
