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
import org.apache.kafka.common.utils.Time

/**
 * This throughput generator configures constant throughput.
 *
 * The lower the window size, the smoother the traffic will be. Using a 100ms window offers no noticeable spikes in
 * traffic while still being long enough to avoid too much overhead.
 *
 * Here is an example spec:
 *
 * ```json
 * {
 *     "type": "constant",
 *     "messagesPerWindow": 50,
 *     "windowSizeMs": 100
 * }
 * ```
 *
 * This will produce a workload that runs 500 messages per second, with a maximum resolution of 50 messages per 100
 * millisecond.
 *
 * If `messagesPerWindow` is less than or equal to 0, `throttle` will not throttle at all and will return immediately.
 */
class ConstantThroughputGenerator @JsonCreator constructor(
    @JsonProperty("messagesPerWindow") messagesPerWindow: Int,
    @JsonProperty("windowSizeMs") windowSizeMs: Long,
) : ThroughputGenerator {

    private val messagesPerWindow: Int

    private val windowSizeMs: Long

    private var nextWindowStarts: Long = 0

    private var messageTracker = 0

    init {
        // Calculate the default values.
        this.windowSizeMs = if (windowSizeMs <= 0) 100 else windowSizeMs
        this.messagesPerWindow = messagesPerWindow
        calculateNextWindow()
    }

    @JsonProperty
    fun windowSizeMs(): Long = windowSizeMs

    @JsonProperty
    fun messagesPerWindow(): Int = messagesPerWindow

    private fun calculateNextWindow() {
        // Reset the message count.
        messageTracker = 0

        // Calculate the next window start time.
        val now = Time.SYSTEM.milliseconds()
        if (nextWindowStarts > 0) {
            while (nextWindowStarts <= now) {
                nextWindowStarts += windowSizeMs
            }
        } else {
            nextWindowStarts = now + windowSizeMs
        }
    }

    @Synchronized
    @Throws(InterruptedException::class)
    override fun throttle() {
        // Run unthrottled if messagesPerWindow is not positive.
        if (messagesPerWindow <= 0) return

        // Calculate the next window if we've moved beyond the current one.
        if (Time.SYSTEM.milliseconds() >= nextWindowStarts) calculateNextWindow()

        // Increment the message tracker.
        messageTracker += 1

        // Compare the tracked message count with the throttle limits.
        if (messageTracker >= messagesPerWindow) {

            // Wait the difference in time between now and when the next window starts.
            while (nextWindowStarts > Time.SYSTEM.milliseconds()) {
                (this as Object).wait(nextWindowStarts - Time.SYSTEM.milliseconds())
            }
        }
    }
}
