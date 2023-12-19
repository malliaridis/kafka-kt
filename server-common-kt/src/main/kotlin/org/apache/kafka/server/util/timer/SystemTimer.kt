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
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.ReentrantReadWriteLock
import org.apache.kafka.common.utils.KafkaThread
import org.apache.kafka.common.utils.Time

class SystemTimer(
    executorName: String,
    tickMs: Long = 1,
    wheelSize: Int = 20,
    startMs: Long = Time.SYSTEM.hiResClockMs(),
) : Timer {

    // timeout timer
    private val taskExecutor: ExecutorService = Executors.newFixedThreadPool(1) { runnable ->
        KafkaThread.nonDaemon("executor-$executorName", runnable)
    }

    private val delayQueue: DelayQueue<TimerTaskList> = DelayQueue()

    private val taskCounter: AtomicInteger = AtomicInteger(0)

    private val timingWheel: TimingWheel = TimingWheel(
        tickMs = tickMs,
        wheelSize = wheelSize,
        startMs = startMs,
        taskCounter = taskCounter,
        queue = delayQueue,
    )

    // Locks used to protect data structures while ticking
    private val readWriteLock = ReentrantReadWriteLock()

    private val readLock = readWriteLock.readLock()

    private val writeLock = readWriteLock.writeLock()

    override fun add(timerTask: TimerTask) {
        readLock.lock()
        try {
            addTimerTaskEntry(
                TimerTaskEntry(
                    timerTask = timerTask,
                    expirationMs = timerTask.delayMs + Time.SYSTEM.hiResClockMs(),
                )
            )
        } finally {
            readLock.unlock()
        }
    }

    private fun addTimerTaskEntry(timerTaskEntry: TimerTaskEntry) {
        if (!timingWheel.add(timerTaskEntry)) {
            // Already expired or cancelled
            if (!timerTaskEntry.cancelled) taskExecutor.submit(timerTaskEntry.timerTask)
        }
    }

    /**
     * Advances the clock if there is an expired bucket. If there isn't any expired bucket when called,
     * waits up to timeoutMs before giving up.
     */
    @Throws(InterruptedException::class)
    override fun advanceClock(timeoutMs: Long): Boolean {
        var bucket = delayQueue.poll(timeoutMs, TimeUnit.MILLISECONDS)
        return if (bucket != null) {
            writeLock.lock()
            try {
                while (bucket != null) {
                    timingWheel.advanceClock(bucket.getExpiration())
                    bucket.flush { timerTaskEntry: TimerTaskEntry -> addTimerTaskEntry(timerTaskEntry) }
                    bucket = delayQueue.poll()
                }
            } finally {
                writeLock.unlock()
            }
            true
        } else false
    }

    override fun size(): Int = taskCounter.get()

    override fun close() = taskExecutor.shutdown()
}
