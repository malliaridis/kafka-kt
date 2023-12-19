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

package org.apache.kafka.server.util
/**
 * This class helps producers throttle throughput.
 *
 * If targetThroughput >= 0, the resulting average throughput will be approximately
 * min(targetThroughput, maximumPossibleThroughput). If targetThroughput < 0,
 * no throttling will occur.
 *
 * To use, do this between successive send attempts:
 *
 * ```java
 * if (throttler.shouldThrottle(...)) {
 *     throttler.throttle();
 * }
 * ```
 *
 * Note that this can be used to throttle message throughput or data throughput.
 *
 * @property targetThroughput Can be messages/sec or bytes/sec
 * @property startMs When the very first message is sent
 */
class ThroughputThrottler(
    private val targetThroughput: Long,
    private val startMs: Long,
) {

    private val sleepTimeNs: Long = if (targetThroughput > 0) NS_PER_SEC / targetThroughput else Long.MAX_VALUE

    private var sleepDeficitNs: Long = 0

    private var wakeup = false

    /**
     * @param amountSoFar bytes produced so far if you want to throttle data throughput, or
     * messages produced so far if you want to throttle message throughput.
     * @param sendStartMs timestamp of the most recently sent message
     */
    fun shouldThrottle(amountSoFar: Long, sendStartMs: Long): Boolean {
        if (targetThroughput < 0) {
            // No throttling in this case
            return false
        }
        val elapsedSec = (sendStartMs - startMs) / 1000f
        return elapsedSec > 0 && amountSoFar / elapsedSec > targetThroughput
    }

    /**
     * Occasionally blocks for small amounts of time to achieve targetThroughput.
     *
     * Note that if targetThroughput is 0, this will block extremely aggressively.
     */
    fun throttle() {
        if (targetThroughput == 0L) {
            try {
                synchronized(this) {
                    while (!wakeup) (this as Object).wait()
                }
            } catch (e: InterruptedException) {
                // do nothing
            }
            return
        }

        // throttle throughput by sleeping, on average,
        // (1 / this.throughput) seconds between "things sent"
        sleepDeficitNs += sleepTimeNs

        // If enough sleep deficit has accumulated, sleep a little
        if (sleepDeficitNs >= MIN_SLEEP_NS) {
            val sleepStartNs = System.nanoTime()
            try {
                synchronized(this) {
                    var remaining = sleepDeficitNs
                    while (!wakeup && remaining > 0) {
                        val sleepMs = remaining / 1000000
                        val sleepNs = remaining - sleepMs * 1000000
                        (this as Object).wait(sleepMs, sleepNs.toInt())
                        val elapsed = System.nanoTime() - sleepStartNs
                        remaining = sleepDeficitNs - elapsed
                    }
                    wakeup = false
                }
                sleepDeficitNs = 0
            } catch (e: InterruptedException) {
                // If sleep is cut short, reduce deficit by the amount of
                // time we actually spent sleeping
                val sleepElapsedNs = System.nanoTime() - sleepStartNs
                if (sleepElapsedNs <= sleepDeficitNs) {
                    sleepDeficitNs -= sleepElapsedNs
                }
            }
        }
    }

    /**
     * Wakeup the throttler if its sleeping.
     */
    fun wakeup() {
        synchronized(this) {
            wakeup = true
            (this as Object).notifyAll()
        }
    }

    companion object {

        private const val NS_PER_MS = 1000000L

        private const val NS_PER_SEC = 1000 * NS_PER_MS

        private const val MIN_SLEEP_NS = 2 * NS_PER_MS
    }
}
