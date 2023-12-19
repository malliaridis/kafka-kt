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

package org.apache.kafka.server.utils.timer

import java.util.PriorityQueue
import org.apache.kafka.common.utils.MockTime
import org.apache.kafka.server.util.timer.Timer
import org.apache.kafka.server.util.timer.TimerTask
import org.apache.kafka.server.util.timer.TimerTaskEntry

class MockTimer(val time: MockTime = MockTime()) : Timer {

    private val taskQueue = PriorityQueue(
        Comparator.comparingLong { entry: TimerTaskEntry -> entry.expirationMs }
    )

    override fun add(timerTask: TimerTask) {
        if (timerTask.delayMs <= 0) timerTask.run()
        else synchronized(taskQueue) {
            taskQueue.add(TimerTaskEntry(timerTask, timerTask.delayMs + time.milliseconds()))
        }
    }

    @Throws(InterruptedException::class)
    override fun advanceClock(timeoutMs: Long): Boolean {
        time.sleep(timeoutMs)
        val now: Long = time.milliseconds()
        var executed = false
        var hasMore = true
        while (hasMore) {
            hasMore = false
            var taskEntry: TimerTaskEntry? = null
            synchronized(taskQueue) {
                if (!taskQueue.isEmpty() && now > taskQueue.peek().expirationMs) {
                    taskEntry = taskQueue.poll()
                    hasMore = !taskQueue.isEmpty()
                }
            }
            taskEntry?.let { entry ->
                if (!entry.cancelled) {
                    entry.timerTask!!.run()
                    executed = true
                }
            }
        }
        return executed
    }

    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("time"),
    )
    fun time(): MockTime = time

    override fun size(): Int = taskQueue.size

    @Throws(Exception::class)
    override fun close() = Unit
}
