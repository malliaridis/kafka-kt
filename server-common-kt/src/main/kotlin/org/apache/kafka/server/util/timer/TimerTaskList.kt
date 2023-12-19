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

import java.util.concurrent.Delayed
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import java.util.function.Consumer
import org.apache.kafka.common.utils.Time
import kotlin.math.max

class TimerTaskList internal constructor(
    private val taskCounter: AtomicInteger,
    private val time: Time = Time.SYSTEM,
) : Delayed {

    private val expiration: AtomicLong = AtomicLong(-1L)

    // TimerTaskList forms a doubly linked cyclic list using a dummy root entry
    // root.next points to the head
    // root.prev points to the tail
    private val root: TimerTaskEntry = TimerTaskEntry(timerTask = null, expirationMs = -1L).apply {
        next = this
        prev = this
    }

    fun setExpiration(expirationMs: Long): Boolean {
        return expiration.getAndSet(expirationMs) != expirationMs
    }

    fun getExpiration(): Long = expiration.get()

    @Synchronized
    fun foreach(f: Consumer<TimerTask?>) {
        var entry = root.next
        while (entry != root) {
            val nextEntry = entry!!.next
            if (!entry.cancelled) f.accept(entry.timerTask)
            entry = nextEntry
        }
    }

    fun add(timerTaskEntry: TimerTaskEntry) {
        var done = false
        while (!done) {
            // Remove the timer task entry if it is already in any other list
            // We do this outside of the sync block below to avoid deadlocking.
            // We may retry until timerTaskEntry.list becomes null.
            timerTaskEntry.remove()
            synchronized(this) {
                synchronized(timerTaskEntry) {
                    if (timerTaskEntry.list == null) {
                        // put the timer task entry to the end of the list. (root.prev points to the tail entry)
                        val tail = root.prev
                        timerTaskEntry.next = root
                        timerTaskEntry.prev = tail
                        timerTaskEntry.list = this
                        tail!!.next = timerTaskEntry
                        root.prev = timerTaskEntry
                        taskCounter.incrementAndGet()
                        done = true
                    }
                }
            }
        }
    }

    @Synchronized
    fun remove(timerTaskEntry: TimerTaskEntry) {
        synchronized(timerTaskEntry) {
            if (timerTaskEntry.list == this) {
                timerTaskEntry.next!!.prev = timerTaskEntry.prev
                timerTaskEntry.prev!!.next = timerTaskEntry.next
                timerTaskEntry.next = null
                timerTaskEntry.prev = null
                timerTaskEntry.list = null
                taskCounter.decrementAndGet()
            }
        }
    }

    @Synchronized
    fun flush(flush: (TimerTaskEntry) -> Unit) {
        var head = root.next
        while (head != root) {
            remove(head!!)
            flush(head)
            head = root.next
        }
        expiration.set(-1L)
    }

    override fun getDelay(unit: TimeUnit): Long {
        return unit.convert(
            (getExpiration() - time.hiResClockMs()).coerceAtLeast(0),
            TimeUnit.MILLISECONDS,
        )
    }

    override fun compareTo(o: Delayed): Int {
        val other = o as TimerTaskList
        return getExpiration().compareTo(other.getExpiration())
    }
}
