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

import java.io.IOException
import java.util.concurrent.ExecutionException
import org.apache.kafka.common.utils.MockTime
import org.apache.kafka.common.utils.Time
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class RetryTest : OAuthBearerTest() {

    @Test
    @Throws(ExecutionException::class)
    fun test() {
        val attempts = arrayOf<Exception?>(
            IOException("pretend connect error"),
            IOException("pretend timeout error"),
            IOException("pretend read error"),
            null, // success!
        )
        val retryWaitMs: Long = 1000
        val maxWaitMs: Long = 10000
        val call = createRetryable(attempts)
        val time: Time = MockTime(
            autoTickMs = 0,
            currentTimeMs = 0,
            currentHighResTimeNs = 0,
        )
        assertEquals(0L, time.milliseconds())
        val r = Retry<String>(
            time = time,
            retryBackoffMs = retryWaitMs,
            retryBackoffMaxMs = maxWaitMs,
        )
        r.execute(call)
        val secondWait = retryWaitMs * 2
        val thirdWait = retryWaitMs * 4
        val totalWait = retryWaitMs + secondWait + thirdWait
        assertEquals(totalWait, time.milliseconds())
    }

    @Test
    fun testIOExceptionFailure() {
        val attempts = arrayOf<Exception?>(
            IOException("pretend connect error"),
            IOException("pretend timeout error"),
            IOException("pretend read error"),
            IOException("pretend another read error"),
        )
        val retryWaitMs: Long = 1000
        val maxWaitMs = (1000 + 2000 + 3999).toLong()
        val call = createRetryable(attempts)
        val time: Time = MockTime(
            autoTickMs = 0,
            currentTimeMs = 0,
            currentHighResTimeNs = 0,
        )
        assertEquals(0L, time.milliseconds())
        val r = Retry<String>(
            time = time,
            retryBackoffMs = retryWaitMs,
            retryBackoffMaxMs = maxWaitMs,
        )
        assertFailsWith<ExecutionException> { r.execute(call) }
        assertEquals(maxWaitMs, time.milliseconds())
    }

    @Test
    fun testRuntimeExceptionFailureOnLastAttempt() {
        val attempts = arrayOf<Exception?>(
            IOException("pretend connect error"),
            IOException("pretend timeout error"),
            NullPointerException("pretend JSON node /userId in response is null"),
        )
        val retryWaitMs: Long = 1000
        val maxWaitMs: Long = 10000
        val call = createRetryable(attempts)
        val time = MockTime(
            autoTickMs = 0,
            currentTimeMs = 0,
            currentHighResTimeNs = 0,
        )
        assertEquals(0L, time.milliseconds())
        val r = Retry<String>(
            time = time,
            retryBackoffMs = retryWaitMs,
            retryBackoffMaxMs = maxWaitMs,
        )
        assertFailsWith<RuntimeException> { r.execute(call) }
        val secondWait = retryWaitMs * 2
        val totalWait = retryWaitMs + secondWait
        assertEquals(totalWait, time.milliseconds())
    }

    @Test
    fun testRuntimeExceptionFailureOnFirstAttempt() {
        val attempts = arrayOf<Exception?>(
            NullPointerException("pretend JSON node /userId in response is null"),
            null,
        )
        val retryWaitMs: Long = 1000
        val maxWaitMs: Long = 10000
        val call = createRetryable(attempts)
        val time = MockTime(
            autoTickMs = 0,
            currentTimeMs = 0,
            currentHighResTimeNs = 0,
        )
        assertEquals(0L, time.milliseconds())
        val r = Retry<String>(
            time = time,
            retryBackoffMs = retryWaitMs,
            retryBackoffMaxMs = maxWaitMs
        )
        assertFailsWith<RuntimeException> { r.execute(call) }
        assertEquals(0, time.milliseconds())
    }

    @Test
    @Throws(IOException::class)
    fun testUseMaxTimeout() {
        val attempts = arrayOf<Exception?>(
            IOException("pretend connect error"),
            IOException("pretend timeout error"),
            IOException("pretend read error"),
        )
        val retryWaitMs: Long = 5000
        val maxWaitMs: Long = 5000
        val call = createRetryable(attempts)
        val time: Time = MockTime(
            autoTickMs = 0,
            currentTimeMs = 0,
            currentHighResTimeNs = 0,
        )
        assertEquals(0L, time.milliseconds())
        val r = Retry<String>(
            time = time,
            retryBackoffMs = retryWaitMs,
            retryBackoffMaxMs = maxWaitMs,
        )
        assertFailsWith<ExecutionException> { r.execute(call) }
        assertEquals(maxWaitMs, time.milliseconds())
    }
}
