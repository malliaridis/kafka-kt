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

package org.apache.kafka.common.utils

import java.util.concurrent.ThreadLocalRandom
import kotlin.math.ln
import kotlin.math.pow

/**
 * A utility class for keeping the parameters and providing the value of exponential
 * retry backoff, exponential reconnect backoff, exponential timeout, etc.
 *
 * The formula is:
 * ```java
 * Backoff(attempts) = random(1 - jitter, 1 + jitter) * initialInterval * multiplier ^ attempts
 * ```
 * If [initialInterval] is greater than or equal to `maxInterval`, a constant backoff of
 * `initialInterval` will be provided.
 *
 * This class is thread-safe.
 */
class ExponentialBackoff(
    private val initialInterval: Long,
    private val multiplier: Int,
    maxInterval: Long,
    private val jitter: Double
) {

    private val expMax: Double

    init {
        expMax = if (maxInterval > initialInterval)
            ln(maxInterval / initialInterval.coerceAtLeast(1)
                .toDouble()) / ln(multiplier.toDouble())
        else 0.0
    }

    fun backoff(attempts: Long): Long {
        if (expMax == 0.0) return initialInterval

        val exp = attempts.toDouble().coerceAtMost(expMax)
        val term = initialInterval * multiplier.toDouble().pow(exp)

        val randomFactor =
            if (jitter < java.lang.Double.MIN_NORMAL) 1.0
            else ThreadLocalRandom.current().nextDouble(1 - jitter, 1 + jitter)

        return (randomFactor * term).toLong()
    }
}
