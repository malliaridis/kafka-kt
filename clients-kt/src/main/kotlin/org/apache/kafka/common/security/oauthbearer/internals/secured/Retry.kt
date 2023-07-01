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

package org.apache.kafka.common.security.oauthbearer.internals.secured

import java.util.concurrent.ExecutionException
import org.apache.kafka.common.utils.Time
import org.slf4j.LoggerFactory
import kotlin.math.pow

/**
 * Retry encapsulates the mechanism to perform a retry and then exponential backoff using provided
 * wait times between attempts.
 *
 * @property R Result type
 */
class Retry<R>(
    private val time: Time = Time.SYSTEM,
    private val retryBackoffMs: Long,
    private val retryBackoffMaxMs: Long,
) {

    init {
        require(retryBackoffMs >= 0) {
            String.format("retryBackoffMs value (%d) must be non-negative", retryBackoffMs)
        }
        require(retryBackoffMaxMs >= 0) {
            String.format("retryBackoffMaxMs value (%d) must be non-negative", retryBackoffMaxMs)
        }
        require(retryBackoffMaxMs >= retryBackoffMs) {
            String.format(
                "retryBackoffMaxMs value (%d) is less than retryBackoffMs value (%d)",
                retryBackoffMaxMs,
                retryBackoffMs,
            )
        }
    }

    @Throws(ExecutionException::class)
    fun execute(retryable: Retryable<R>): R {
        val endMs = time.milliseconds() + retryBackoffMaxMs
        var currAttempt = 0
        var error: ExecutionException? = null

        while (time.milliseconds() <= endMs) {
            currAttempt++
            try {
                return retryable.call()
            } catch (e: UnretryableException) {
                // We've deemed this error to not be worth retrying, so collect the error and
                // fail immediately.
                if (error == null) error = ExecutionException(e)
                break
            } catch (e: ExecutionException) {
                log.warn("Error during retry attempt {}", currAttempt, e)
                if (error == null) error = e

                var waitMs = retryBackoffMs * 2.0.pow((currAttempt - 1).toDouble()).toLong()
                val diff = endMs - time.milliseconds()

                waitMs = waitMs.coerceAtMost(diff)
                if (waitMs <= 0) break

                val message = String.format(
                    "Attempt %d to make call resulted in an error; sleeping %d ms before retrying",
                    currAttempt, waitMs
                )

                log.warn(message, e)
                time.sleep(waitMs)
            }
        }
        // Really shouldn't ever get to here, but...
        if (error == null) error = ExecutionException(
            IllegalStateException(
                "Exhausted all retry attempts but no attempt returned value or encountered exception"
            )
        )
        throw error
    }

    companion object {
        private val log = LoggerFactory.getLogger(Retry::class.java)
    }
}
