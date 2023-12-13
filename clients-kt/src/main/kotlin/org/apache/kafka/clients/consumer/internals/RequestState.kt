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

package org.apache.kafka.clients.consumer.internals

import org.apache.kafka.common.utils.ExponentialBackoff

internal open class RequestState {

    private val exponentialBackoff: ExponentialBackoff

    private var lastSentMs: Long = -1

    private var lastReceivedMs: Long = -1

    private var numAttempts = 0

    private var backoffMs: Long = 0

    constructor(retryBackoffMs: Long) {
        exponentialBackoff = ExponentialBackoff(
            initialInterval = retryBackoffMs,
            multiplier = RETRY_BACKOFF_EXP_BASE,
            maxInterval = retryBackoffMs,
            jitter = RETRY_BACKOFF_JITTER
        )
    }

    // Visible for testing
    internal constructor(
        retryBackoffMs: Long,
        retryBackoffExpBase: Int,
        retryBackoffMaxMs: Long,
        jitter: Double,
    ) {
        exponentialBackoff = ExponentialBackoff(
            initialInterval = retryBackoffMs,
            multiplier = retryBackoffExpBase,
            maxInterval = retryBackoffMaxMs,
            jitter = jitter
        )
    }

    /**
     * Reset request state so that new requests can be sent immediately and the backoff is restored
     * to its minimal configuration.
     */
    fun reset() {
        lastSentMs = -1
        lastReceivedMs = -1
        numAttempts = 0
        backoffMs = exponentialBackoff.backoff(0)
    }

    fun canSendRequest(currentTimeMs: Long): Boolean {
        if (lastSentMs == -1L) return true // no request has been sent

        return if (lastReceivedMs == -1L || lastReceivedMs < lastSentMs) false // there is an inflight request
        else requestBackoffExpired(currentTimeMs)
    }

    fun onSendAttempt(currentTimeMs: Long) {
        // Here we update the timer everytime we try to send a request. Also increment number of attempts.
        lastSentMs = currentTimeMs
    }

    /**
     * Callback invoked after a successful send. This resets the number of attempts to 0, but the minimal backoff
     * will still be enforced prior to allowing a new send. To send immediately, use [reset].
     *
     * @param currentTimeMs Current time in milliseconds
     */
    fun onSuccessfulAttempt(currentTimeMs: Long) {
        lastReceivedMs = currentTimeMs
        backoffMs = exponentialBackoff.backoff(0)
        numAttempts = 0
    }

    /**
     * Callback invoked after a failed send. The number of attempts will be incremented, which may increase
     * the backoff before allowing the next send attempt.
     *
     * @param currentTimeMs Current time in milliseconds
     */
    fun onFailedAttempt(currentTimeMs: Long) {
        lastReceivedMs = currentTimeMs
        backoffMs = exponentialBackoff.backoff(numAttempts.toLong())
        numAttempts++
    }

    private fun requestBackoffExpired(currentTimeMs: Long): Boolean =
        remainingBackoffMs(currentTimeMs) <= 0

    fun remainingBackoffMs(currentTimeMs: Long): Long {
        val timeSinceLastReceiveMs = currentTimeMs - lastReceivedMs
        return (backoffMs - timeSinceLastReceiveMs).coerceAtLeast(0)
    }

    companion object {

        const val RETRY_BACKOFF_EXP_BASE = 2

        const val RETRY_BACKOFF_JITTER = 0.2
    }
}
