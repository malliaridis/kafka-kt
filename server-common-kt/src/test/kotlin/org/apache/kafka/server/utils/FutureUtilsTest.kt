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

package org.apache.kafka.server.utils

import java.util.concurrent.CompletableFuture
import java.util.concurrent.ExecutionException
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.ScheduledThreadPoolExecutor
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException
import org.apache.kafka.common.utils.Time
import org.apache.kafka.server.util.Deadline
import org.apache.kafka.server.util.FutureUtils.chainFuture
import org.apache.kafka.server.util.FutureUtils.waitWithLogging
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import org.slf4j.LoggerFactory
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertIs

@Timeout(value = 120)
class FutureUtilsTest {

    @Test
    @Throws(Throwable::class)
    fun testWaitWithLogging() {
        val executorService: ScheduledExecutorService = ScheduledThreadPoolExecutor(1)
        val future = CompletableFuture<Int>()
        executorService.schedule<Boolean>({ future.complete(123) }, 1000, TimeUnit.NANOSECONDS)
        assertEquals(
            expected = 123,
            actual = waitWithLogging(
                log = log,
                prefix = "[FutureUtilsTest] ",
                action = "the future to be completed",
                future = future,
                deadline = Deadline.fromDelay(Time.SYSTEM, 30, TimeUnit.SECONDS),
                time = Time.SYSTEM,
            )
        )
        executorService.shutdownNow()
        executorService.awaitTermination(1, TimeUnit.MINUTES)
    }

    @ParameterizedTest
    @ValueSource(booleans = [false, true])
    @Throws(Throwable::class)
    fun testWaitWithLoggingTimeout(immediateTimeout: Boolean) {
        val executorService: ScheduledExecutorService = ScheduledThreadPoolExecutor(1)
        val future = CompletableFuture<Int>()
        executorService.schedule<Boolean>(
            { future.complete(456) },
            10000,
            TimeUnit.MILLISECONDS,
        )
        assertFailsWith<TimeoutException> {
            waitWithLogging(
                log = log,
                prefix = "[FutureUtilsTest] ",
                action = "the future to be completed",
                future = future,
                deadline = if (immediateTimeout) Deadline.fromDelay(Time.SYSTEM, 0, TimeUnit.SECONDS)
                else Deadline.fromDelay(Time.SYSTEM, 1, TimeUnit.MILLISECONDS),
                time = Time.SYSTEM
            )
        }
        executorService.shutdownNow()
        executorService.awaitTermination(1, TimeUnit.MINUTES)
    }

    @Test
    @Throws(Throwable::class)
    fun testWaitWithLoggingError() {
        val executorService: ScheduledExecutorService = ScheduledThreadPoolExecutor(1)
        val future = CompletableFuture<Int>()
        executorService.schedule(
            { future.completeExceptionally(IllegalArgumentException("uh oh")) },
            1,
            TimeUnit.NANOSECONDS
        )
        val error = assertFailsWith<RuntimeException> {
            waitWithLogging(
                log = log,
                prefix = "[FutureUtilsTest] ",
                action = "the future to be completed",
                future = future,
                deadline = Deadline.fromDelay(Time.SYSTEM, 30, TimeUnit.SECONDS),
                time = Time.SYSTEM,
            )
        }
        assertEquals("Received a fatal error while waiting for the future to be completed", error.message)
        executorService.shutdown()
        executorService.awaitTermination(1, TimeUnit.MINUTES)
    }

    @Test
    @Throws(Throwable::class)
    fun testChainFuture() {
        val sourceFuture = CompletableFuture<Int>()
        val destinationFuture = CompletableFuture<Number>()
        chainFuture(sourceFuture, destinationFuture)
        assertFalse(sourceFuture.isDone)
        assertFalse(destinationFuture.isDone)
        assertFalse(sourceFuture.isCancelled())
        assertFalse(destinationFuture.isCancelled())
        assertFalse(sourceFuture.isCompletedExceptionally())
        assertFalse(destinationFuture.isCompletedExceptionally())
        sourceFuture.complete(123)
        assertEquals(123, destinationFuture.get())
    }

    @Test
    @Throws(Throwable::class)
    fun testChainFutureExceptionally() {
        val sourceFuture = CompletableFuture<Int>()
        val destinationFuture = CompletableFuture<Number>()
        chainFuture(sourceFuture, destinationFuture)
        sourceFuture.completeExceptionally(RuntimeException("source failed"))
        val cause = assertFailsWith<ExecutionException> { destinationFuture.get() }.cause
        assertIs<RuntimeException>(cause!!)
        assertEquals("source failed", cause.message)
    }

    companion object {
        private val log = LoggerFactory.getLogger(FutureUtilsTest::class.java)
    }
}
