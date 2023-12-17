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

package org.apache.kafka.server.util.timer

import java.util.concurrent.DelayQueue
import java.util.concurrent.atomic.AtomicInteger
import kotlin.concurrent.Volatile

/**
 * Hierarchical Timing Wheels
 *
 * A simple timing wheel is a circular list of buckets of timer tasks. Let u be the time unit.
 * A timing wheel with size n has n buckets and can hold timer tasks in `n * u` time interval.
 * Each bucket holds timer tasks that fall into the corresponding time range. At the beginning,
 * the first bucket holds tasks for `[0, u)`, the second bucket holds tasks for `[u, 2u)`, …,
 * the n-th bucket for `[u * (n -1), u * n)`. Every interval of time unit u, the timer ticks and
 * moved to the next bucket then expire all timer tasks in it. So, the timer never insert a task
 * into the bucket for the current time since it is already expired. The timer immediately runs
 * the expired task. The emptied bucket is then available for the next round, so if the current
 * bucket is for the time t, it becomes the bucket for `[t + u * n, t + (n + 1) * u)` after a tick.
 * A timing wheel has `O(1)` cost for insert/delete (start-timer/stop-timer) whereas priority queue
 * based timers, such as [java.util.concurrent.DelayQueue] and [java.util.Timer], have `O(log n)`
 * insert/delete cost.
 *
 * A major drawback of a simple timing wheel is that it assumes that a timer request is within
 * the time interval of `n * u` from the current time. If a timer request is out of this interval,
 * it is an overflow. A hierarchical timing wheel deals with such overflows. It is a hierarchically
 * organized timing wheels. The lowest level has the finest time resolution. As moving up the
 * hierarchy, time resolutions become coarser. If the resolution of a wheel at one level is u and
 * the size is n, the resolution of the next level should be n * u. At each level overflows are
 * delegated to the wheel in one level higher. When the wheel in the higher level ticks, it reinsert
 * timer tasks to the lower level. An overflow wheel can be created on-demand. When a bucket in an
 * overflow bucket expires, all tasks in it are reinserted into the timer recursively. The tasks
 * are then moved to the finer grain wheels or be executed. The insert (start-timer) cost is `O(m)`
 * where m is the number of wheels, which is usually very small compared to the number of requests
 * in the system, and the delete (stop-timer) cost is still `O(1)`.
 *
 * Example
 * Let's say that `u` is `1` and `n` is `3`. If the start time is `c`,
 * then the buckets at different levels are:
 *
 * ```
 * level  buckets
 * 1      [c,c]   [c+1,c+1]  [c+2,c+2]
 * 2      [c,c+2] [c+3,c+5]  [c+6,c+8]
 * 3      [c,c+8] [c+9,c+17] [c+18,c+26]
 * ```
 *
 * The bucket expiration is at the time of bucket beginning.
 * So at `time = c+1`, buckets `[c,c]`, `[c,c+2]` and `[c,c+8]` are expired.
 * Level 1's clock moves to `c+1`, and `[c+3,c+3]` is created.
 * Level 2 and level3's clock stay at c since their clocks move in unit of 3 and 9, respectively.
 * So, no new buckets are created in level 2 and 3.
 *
 * Note that bucket `[c,c+2]` in level 2 won't receive any task since that range is already covered in level 1.
 * The same is true for the bucket `[c,c+8]` in level 3 since its range is covered in level 2.
 * This is a bit wasteful, but simplifies the implementation.
 *
 * ```
 * 1     [c+1,c+1]  [c+2,c+2]   [c+3,c+3]
 * 2     [c,c+2]    [c+3,c+5]   [c+6,c+8]
 * 3     [c,c+8]    [c+9,c+17]  [c+18,c+26]
 * ```
 *
 * At `time = c+2`, `[c+1,c+1]` is newly expired.
 * Level 1 moves to `c+2`, and `[c+4,c+4]` is created,
 *
 * ```
 * 1     [c+2,c+2]  [c+3,c+3]  [c+4,c+4]
 * 2     [c,c+2]    [c+3,c+5]  [c+6,c+8]
 * 3     [c,c+8]    [c+9,c+17] [c+18,c+26]
 * ```
 *
 * At `time = c+3`, `[c+2,c+2]` is newly expired.
 * Level 2 moves to `c+3`, and `[c+5,c+5]` and `[c+9,c+11]` are created.
 * Level 3 stay at `c`.
 *
 * ```
 * 1     [c+3,c+3]  [c+4,c+4]  [c+5,c+5]
 * 2     [c+3,c+5]  [c+6,c+8]  [c+9,c+11]
 * 3     [c,c+8]    [c+9,c+17] [c+18,c+26]
 * ```
 *
 * The hierarchical timing wheels works especially well when operations are completed before they time out.
 * Even when everything times out, it still has advantageous when there are many items in the timer.
 * Its insert cost (including reinsert) and delete cost are `O(m)` and `O(1)`, respectively while priority
 * queue based timers takes `O(log N)` for both insert and delete where `N` is the number of items in the queue.
 *
 * This class is not thread-safe. There should not be any add calls while advanceClock is executing.
 * It is caller's responsibility to enforce it. Simultaneous add calls are thread-safe.
 */
class TimingWheel internal constructor(
    private val tickMs: Long,
    private val wheelSize: Int,
    private val startMs: Long,
    private val taskCounter: AtomicInteger,
    private val queue: DelayQueue<TimerTaskList>,
) {

    private val interval: Long

    private val buckets: Array<TimerTaskList>

    private var currentTimeMs: Long

    // overflowWheel can potentially be updated and read by two concurrent threads through add().
    // Therefore, it needs to be volatile due to the issue of Double-Checked Locking pattern with JVM
    @Volatile
    private var overflowWheel: TimingWheel? = null

    init {
        interval = tickMs * wheelSize
        // rounding down to multiple of tickMs
        currentTimeMs = startMs - startMs % tickMs

        buckets = Array(wheelSize) { TimerTaskList(taskCounter) }
    }

    @Synchronized
    private fun addOverflowWheel() {
        if (overflowWheel == null) {
            overflowWheel = TimingWheel(
                tickMs = interval,
                wheelSize = wheelSize,
                startMs = currentTimeMs,
                taskCounter = taskCounter,
                queue = queue,
            )
        }
    }

    fun add(timerTaskEntry: TimerTaskEntry): Boolean {
        val expiration = timerTaskEntry.expirationMs
        return when {
            timerTaskEntry.cancelled -> false // Cancelled
            expiration < currentTimeMs + tickMs -> false // Already expired
            expiration < currentTimeMs + interval -> {
                // Put in its own bucket
                val virtualId = expiration / tickMs
                val bucketId = (virtualId % wheelSize.toLong()).toInt()
                val bucket = buckets[bucketId]
                bucket.add(timerTaskEntry)

                // Set the bucket expiration time
                if (bucket.setExpiration(virtualId * tickMs)) {
                    // The bucket needs to be enqueued because it was an expired bucket
                    // We only need to enqueue the bucket when its expiration time has changed, i.e. the wheel
                    // has advanced and the previous buckets gets reused; further calls to set the expiration
                    // within the same wheel cycle will pass in the same value and hence return false,
                    // thus the bucket with the same expiration will not be enqueued multiple times.
                    queue.offer(bucket)
                }
                true
            }
            else -> {
                // Out of the interval. Put it into the parent timer
                if (overflowWheel == null) addOverflowWheel()
                overflowWheel!!.add(timerTaskEntry)
            }
        }
    }

    fun advanceClock(timeMs: Long) {
        if (timeMs >= currentTimeMs + tickMs) {
            currentTimeMs = timeMs - timeMs % tickMs

            // Try to advance the clock of the overflow wheel if present
            overflowWheel?.advanceClock(currentTimeMs)
        }
    }
}
