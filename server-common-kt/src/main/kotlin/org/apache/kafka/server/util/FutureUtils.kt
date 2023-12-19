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

import java.util.concurrent.CompletableFuture
import java.util.concurrent.ExecutionException
import java.util.concurrent.TimeoutException
import org.apache.kafka.common.utils.Time
import org.slf4j.Logger

object FutureUtils {

    /**
     * Wait for a future until a specific time in the future, with copious logging.
     *
     * @param log The slf4j object to use to log success and failure.
     * @param action The action we are waiting for.
     * @param future The future we are waiting for.
     * @param deadline The deadline in the future we are waiting for.
     * @param time The clock object.
     * @param T The type of the future.
     *
     * @return The result of the future.
     *
     * @throws java.util.concurrent.TimeoutException If the future times out.
     * @throws Throwable If the future fails. Note: we unwrap ExecutionException here.
     */
    @Throws(Throwable::class)
    fun <T> waitWithLogging(
        log: Logger,
        prefix: String,
        action: String,
        future: CompletableFuture<T>,
        deadline: Deadline,
        time: Time,
    ): T {
        log.info("{}Waiting for {}", prefix, action)
        return try {
            val result = time.waitForFuture(future, deadline.nanoseconds)
            log.info("{}Finished waiting for {}", prefix, action)
            result
        } catch (t: TimeoutException) {
            log.error("{}Timed out while waiting for {}", prefix, action, t)
            val timeout = TimeoutException("Timed out while waiting for $action")
            timeout.setStackTrace(t.stackTrace)
            throw timeout
        } catch (t: Throwable) {
            var throwable = t
            if (throwable is ExecutionException) throwable = throwable.cause!!

            log.error("{}Received a fatal error while waiting for {}", prefix, action, throwable)
            throw RuntimeException("Received a fatal error while waiting for $action", throwable)
        }
    }

    /**
     * Complete a given destination future when a source future is completed.
     *
     * @param sourceFuture The future to trigger off of.
     * @param destinationFuture The future to complete when the source future is completed.
     * @param T The destination future type.
     */
    fun <T> chainFuture(
        sourceFuture: CompletableFuture<out T>,
        destinationFuture: CompletableFuture<T>,
    ) {
        sourceFuture.whenComplete { value, throwable ->
            if (throwable != null) destinationFuture.completeExceptionally(throwable)
            else destinationFuture.complete(value)
        }
    }

    /**
     * Returns a new CompletableFuture that is already completed exceptionally with the given exception.
     *
     * @param ex The exception.
     * @return The exceptionally completed CompletableFuture.
     */
    fun <T> failedFuture(ex: Throwable?): CompletableFuture<T> {
        val future = CompletableFuture<T>()
        future.completeExceptionally(ex)
        return future
    }
}
