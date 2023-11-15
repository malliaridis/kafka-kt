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

import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong
import java.util.function.Supplier
import org.apache.kafka.common.errors.TimeoutException

/**
 * A clock that you can manually advance by calling sleep
 */
class MockTime constructor(
    private val autoTickMs: Long = 0,
    currentTimeMs: Long = System.currentTimeMillis(),
    currentHighResTimeNs: Long = System.nanoTime(),
) : Time {

    fun interface Listener {
        fun onTimeUpdated()
    }

    /**
     * Listeners which are waiting for time changes.
     */
    private val listeners = CopyOnWriteArrayList<Listener>()

    // Values from `nanoTime` and `currentTimeMillis` are not comparable, so we store them separately to allow tests
    // using this class to detect bugs where this is incorrectly assumed to be true
    private val timeMs: AtomicLong = AtomicLong(currentTimeMs)

    private val highResTimeNs: AtomicLong = AtomicLong(currentHighResTimeNs)

    fun addListener(listener: Listener) {
        listeners.add(listener)
    }

    override fun milliseconds(): Long {
        maybeSleep(autoTickMs)
        return timeMs.get()
    }

    override fun nanoseconds(): Long {
        maybeSleep(autoTickMs)
        return highResTimeNs.get()
    }

    private fun maybeSleep(ms: Long) {
        if (ms != 0L) sleep(ms)
    }

    override fun sleep(ms: Long) {
        timeMs.addAndGet(ms)
        highResTimeNs.addAndGet(TimeUnit.MILLISECONDS.toNanos(ms))
        tick()
    }

    @Throws(InterruptedException::class)
    override fun waitObject(obj: Any, condition: Supplier<Boolean>, deadlineMs: Long) {
        val listener = Listener { synchronized(obj) { (obj as Object).notify() } }
        listeners.add(listener)
        try {
            synchronized(obj) {
                while (milliseconds() < deadlineMs && !condition.get()) (obj as Object).wait()
                if (!condition.get()) throw TimeoutException("Condition not satisfied before deadline")
            }
        } finally {
            listeners.remove(listener)
        }
    }

    fun setCurrentTimeMs(newMs: Long) {
        val oldMs = timeMs.getAndSet(newMs)

        // does not allow to set to an older timestamp
        require(oldMs <= newMs) {
            "Setting the time to $newMs while current time $oldMs is newer; this is not allowed"
        }
        highResTimeNs.set(TimeUnit.MILLISECONDS.toNanos(newMs))
        tick()
    }

    private fun tick() {
        for (listener in listeners) listener.onTimeUpdated()
    }
}
