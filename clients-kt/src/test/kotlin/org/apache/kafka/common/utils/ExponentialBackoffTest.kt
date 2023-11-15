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

import org.junit.jupiter.api.Test
import kotlin.math.pow
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class ExponentialBackoffTest {

    @Test
    fun testExponentialBackoff() {
        val scaleFactor = 100L
        val ratio = 2
        val backoffMax = 2000L
        val jitter = 0.2
        val exponentialBackoff = ExponentialBackoff(
            initialInterval = scaleFactor,
            multiplier = ratio,
            maxInterval = backoffMax,
            jitter = jitter,
        )
        repeat(101) {
            repeat(11) { attempts ->
                if (attempts <= 4) assertEquals(
                    expected = scaleFactor * ratio.toDouble().pow(attempts),
                    actual = exponentialBackoff.backoff(attempts.toLong()).toDouble(),
                    absoluteTolerance = scaleFactor * ratio.toDouble().pow(attempts) * jitter
                )
                else assertTrue(exponentialBackoff.backoff(attempts.toLong()) <= backoffMax * (1 + jitter))
            }
        }
    }

    @Test
    fun testExponentialBackoffWithoutJitter() {
        val exponentialBackoff = ExponentialBackoff(
            initialInterval = 100,
            multiplier = 2,
            maxInterval = 400,
            jitter = 0.0,
        )
        assertEquals(100, exponentialBackoff.backoff(0))
        assertEquals(200, exponentialBackoff.backoff(1))
        assertEquals(400, exponentialBackoff.backoff(2))
        assertEquals(400, exponentialBackoff.backoff(3))
    }
}
