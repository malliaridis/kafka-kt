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

/**
 * This is a helper class which makes blocking methods with a timeout easier to implement. In
 * particular it enables use cases where a high-level blocking call with a timeout is composed of
 * several lower level calls, each of which has their own respective timeouts. The idea is to create
 * a single timer object for the high level timeout and carry it along to all of the lower level
 * methods. This class also handles common problems such as integer overflow. This class also
 * ensures monotonic updates to the timer even if the underlying clock is subject to non-monotonic
 * behavior. For example, the remaining time returned by [remainingMs] is guaranteed to decrease
 * monotonically until it hits zero.
 *
 * Note that it is up to the caller to ensure progress of the timer using one of the [update]
 * methods or [sleep]. The timer will cache the current time and return it indefinitely until the
 * timer has been updated. This allows the caller to limit unnecessary system calls and update the
 * timer only when needed. For example, a timer which is waiting a request sent through the
 * [org.apache.kafka.clients.NetworkClient] should call [update] following each blocking call to
 * [org.apache.kafka.clients.NetworkClient.poll].
 *
 * A typical usage might look something like this:
 *
 * ```java
 * Time time = Time.SYSTEM;
 * Timer timer = time.timer(500);
 *
 * while (!conditionSatisfied() && timer.notExpired()) {
 *   client.poll(timer.remainingMs(), timer.currentTimeMs());
 *   timer.update();
 * }
 * ```
 */
class Timer internal constructor(private val time: Time, timeoutMs: Long) {

    private var startMs: Long = 0

    /**
     * The current cached time in milliseconds. This will return the same cached value until the
     * timer has been updated using one of the [update] methods or [sleep] is used.
     *
     * Note that the value is guaranteed to increase monotonically even if the underlying [Time]
     * implementation goes backwards. Effectively, the timer will just wait for the time to catch
     * up.
     */
    var currentTimeMs: Long = 0
        private set

    private var deadlineMs: Long = 0

    /**
     * Get the current timeout value specified through [reset] or [resetDeadline]. This value is
     * constant until altered by one of these API calls.
     */
    var timeoutMs: Long = 0
        private set

    init {
        update()
        reset(timeoutMs)
    }

    /**
     * Whether the timer is expired or not. Like [remainingMs], this depends on the current cached
     * time in milliseconds, which is only updated through one of the [update] methods or with
     * [sleep];
     *
     * Value is `true` if the timer has expired, `false` otherwise.
     */
    val isExpired: Boolean
        get() = currentTimeMs >= deadlineMs

    /**
     * Whether the timer has not yet expired.
     *
     * Value is `true` if there is still time remaining before expiration.
     *
     * @see isExpired
     */
    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("isNotExpired")
    )
    fun notExpired(): Boolean = !isExpired

    /**
     * Whether the timer has not yet expired.
     *
     * Value is `true` if there is still time remaining before expiration.
     */
    val isNotExpired
        get() = currentTimeMs < deadlineMs

    /**
     * Reset the timer to the specific timeout. This will use the underlying [Timer] implementation
     * to update the current cached time in milliseconds and it will set a new timer deadline.
     *
     * @param timeoutMs The new timeout in milliseconds
     */
    fun updateAndReset(timeoutMs: Long) {
        update()
        reset(timeoutMs)
    }

    /**
     * Reset the timer using a new timeout. Note that this does not update the cached current time
     * in milliseconds, so it typically must be accompanied with a separate call to [update].
     * Typically, you can just use [updateAndReset].
     *
     * @param timeoutMs The new timeout in milliseconds
     */
    fun reset(timeoutMs: Long) {
        require(timeoutMs >= 0) { "Invalid negative timeout $timeoutMs" }
        this.timeoutMs = timeoutMs
        startMs = currentTimeMs
        deadlineMs =
            if (currentTimeMs > Long.MAX_VALUE - timeoutMs) Long.MAX_VALUE
            else currentTimeMs + timeoutMs
    }

    /**
     * Reset the timer's deadline directly.
     *
     * @param deadlineMs The new deadline in milliseconds
     */
    fun resetDeadline(deadlineMs: Long) {
        require(deadlineMs >= 0) { "Invalid negative deadline $deadlineMs" }
        timeoutMs = Math.max(0, deadlineMs - currentTimeMs)
        startMs = currentTimeMs
        this.deadlineMs = deadlineMs
    }

    /**
     * Update the cached current time to a specific value. In some contexts, the caller may already
     * have an accurate time, so this avoids unnecessary calls to system time.
     *
     * Note that if the updated current time is smaller than the cached time, then the update
     * is ignored.
     *
     * @param currentTimeMs The current time in milliseconds to cache. Defaults to the underlying
     * [Time] implementation to update the current cached time. If the underlying time returns a
     * value which is smaller than the current cached time, the update will be ignored.
     */
    fun update(currentTimeMs: Long = time.milliseconds()) {
        this.currentTimeMs = currentTimeMs.coerceAtLeast(this.currentTimeMs)
    }

    /**
     * Get the remaining time in milliseconds until the timer expires. Like [currentTimeMs], this
     * depends on the cached current time, so the returned value will not change until the timer has
     * been updated using one of the [update] methods or [sleep].
     *
     * @return The cached remaining time in milliseconds until timer expiration
     */
    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("remainingMs")
    )
    fun remainingMs(): Long =
        (deadlineMs - currentTimeMs).coerceAtLeast(0)

    /**
     * The remaining time in milliseconds until the timer expires. Like [currentTimeMs], this
     * depends on the cached current time, so the returned value will not change until the timer has
     * been updated using one of the [update] methods or [sleep].
     */
    val remainingMs: Long
        get() = (deadlineMs - currentTimeMs).coerceAtLeast(0)

    /**
     * Get the current time in milliseconds. This will return the same cached value until the timer
     * has been updated using one of the [update] methods or [sleep] is used.
     *
     * Note that the value returned is guaranteed to increase monotonically even if the underlying
     * [Time] implementation goes backwards. Effectively, the timer will just wait for the
     * time to catch up.
     *
     * @return The current cached time in milliseconds
     */
    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("currentTimeMs")
    )
    fun currentTimeMs(): Long = currentTimeMs

    /**
     * Get the amount of time that has elapsed since the timer began. If the timer was reset, this
     * will be the amount of time since the last reset.
     *
     * @return The elapsed time since construction or the last reset
     */
    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("elapsedMs")
    )
    fun elapsedMs(): Long = currentTimeMs - startMs

    /**
     * The amount of time that has elapsed since the timer began. If the timer was reset, this will
     * be the amount of time since the last reset.
     */
    val elapsedMs: Long
        get() = currentTimeMs - startMs

    /**
     * Get the current timeout value specified through [reset] or [resetDeadline].
     * This value is constant until altered by one of these API calls.
     *
     * @return The timeout in milliseconds
     */
    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("timeoutMs")
    )
    fun timeoutMs(): Long = timeoutMs

    /**
     * Sleep for the requested duration and update the timer. Return when either the duration has
     * elapsed or the timer has expired.
     *
     * @param durationMs The duration in milliseconds to sleep
     */
    fun sleep(durationMs: Long) {
        val sleepDurationMs = Math.min(durationMs, remainingMs())
        time.sleep(sleepDurationMs)
        update()
    }
}
