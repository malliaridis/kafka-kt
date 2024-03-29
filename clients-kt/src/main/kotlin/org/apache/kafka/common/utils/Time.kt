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

import java.time.Duration
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ExecutionException
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException
import java.util.function.Supplier

/**
 * An interface abstracting the clock to use in unit testing classes that make use of clock time.
 *
 * Implementations of this class should be thread-safe.
 */
interface Time {

    /**
     * Returns the current time in milliseconds.
     */
    fun milliseconds(): Long

    /**
     * Returns the value returned by `nanoseconds` converted into milliseconds.
     */
    fun hiResClockMs(): Long {
        return TimeUnit.NANOSECONDS.toMillis(nanoseconds())
    }

    /**
     * Returns the current value of the running JVM's high-resolution time source, in nanoseconds.
     *
     * This method can only be used to measure elapsed time and is
     * not related to any other notion of system or wall-clock time.
     * The value returned represents nanoseconds since some fixed but
     * arbitrary *origin* time (perhaps in the future, so values
     * may be negative). The same origin is used by all invocations of
     * this method in an instance of a Java virtual machine; other
     * virtual machine instances are likely to use a different origin.
     */
    fun nanoseconds(): Long

    /**
     * Sleep for the given number of milliseconds
     */
    fun sleep(ms: Long)

    /**
     * Wait for a condition using the monitor of a given object. This avoids the implicit
     * dependence on system time when calling [Object.wait].
     *
     * @param obj The object that will be waited with [Object.wait]. Note that it is the
     * responsibility of the caller to call notify on this object when the condition is satisfied.
     * @param condition The condition we are awaiting
     * @param deadlineMs The deadline timestamp at which to raise a timeout error
     *
     * @throws org.apache.kafka.common.errors.TimeoutException if the timeout expires before the
     * condition is satisfied
     */
    @Throws(InterruptedException::class)
    fun waitObject(obj: Object, condition: Supplier<Boolean>, deadlineMs: Long)

    /**
     * Get a timer which is bound to this time instance and expires after the given timeout
     */
    fun timer(timeoutMs: Long): Timer = Timer(this, timeoutMs)

    /**
     * Get a timer which is bound to this time instance and expires after the given timeout
     */
    fun timer(timeout: Duration): Timer = timer(timeout.toMillis())

    /**
     * Wait for a future to complete, or time out.
     *
     * @param future The future to wait for.
     * @param deadlineNs The time in the future, in monotonic nanoseconds, to time out.
     * @return The result of the future.
     * @param T The type of the future.
     */
    @Throws(TimeoutException::class, InterruptedException::class, ExecutionException::class)
    fun <T> waitForFuture(future: CompletableFuture<T>, deadlineNs: Long): T {
        var timeoutException: TimeoutException? = null
        while (true) {
            val nowNs = nanoseconds()
            if (deadlineNs <= nowNs) {
                throw timeoutException ?: TimeoutException()
            }
            val deltaNs = deadlineNs - nowNs
            timeoutException = try {
                return future[deltaNs, TimeUnit.NANOSECONDS]
            } catch (t: TimeoutException) {
                t
            }
        }
    }

    companion object {
        val SYSTEM: Time = SystemTime()
    }
}
