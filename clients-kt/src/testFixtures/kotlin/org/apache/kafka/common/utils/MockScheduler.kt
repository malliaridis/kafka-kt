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

import java.util.TreeMap
import java.util.concurrent.Callable
import java.util.concurrent.Future
import java.util.concurrent.ScheduledExecutorService
import org.apache.kafka.common.internals.KafkaFutureImpl
import org.slf4j.LoggerFactory

/**
 * @property time The MockTime object.
 */
class MockScheduler(private val time: MockTime) : Scheduler, MockTime.Listener {

    /**
     * Futures which are waiting for a specified wall-clock time to arrive.
     */
    private val waiters = TreeMap<Long, MutableList<KafkaFutureImpl<Long>>>()

    init {
        time.addListener(this)
    }

    override fun time(): Time = time

    @Synchronized
    override fun onTimeUpdated() {
        val timeMs = time.milliseconds()
        while (true) {
            val entry = waiters.firstEntry()
            if (entry == null || entry.key > timeMs) break
            for (future in entry.value) future.complete(timeMs)
            waiters.remove(entry.key)
        }
    }

    @Synchronized
    fun addWaiter(delayMs: Long, waiter: KafkaFutureImpl<Long>) {
        val timeMs = time.milliseconds()
        if (delayMs <= 0) waiter.complete(timeMs)
        else {
            val triggerTimeMs = timeMs + delayMs
            var futures = waiters[triggerTimeMs]
            if (futures == null) {
                futures = ArrayList()
                waiters[triggerTimeMs] = futures
            }
            futures.add(waiter)
        }
    }

    override fun <T> schedule(
        executor: ScheduledExecutorService,
        callable: Callable<T>, delayMs: Long,
    ): Future<T> {
        val future = KafkaFutureImpl<T>()
        val waiter = KafkaFutureImpl<Long>()
        waiter.thenApply { now ->
            executor.submit { // Note: it is possible that we'll execute Callable#call right after
                // the future is cancelled.  This is a valid sequence of events
                // that the author of the Callable needs to be able to handle.
                //
                // Note 2: If the future is cancelled, we will not remove the waiter
                // from this MockTime object.  This small bit of inefficiency is acceptable
                // in testing code (at least we aren't polling!)
                if (!future.isCancelled) {
                    try {
                        log.trace("Invoking {} at {}", callable, now)
                        future.complete(callable.call())
                    } catch (throwable: Throwable) {
                        future.completeExceptionally(throwable)
                    }
                }
            }
        }
        log.trace("Scheduling {} for {} ms from now.", callable, delayMs)
        addWaiter(delayMs, waiter)
        return future
    }

    companion object {
        private val log = LoggerFactory.getLogger(MockScheduler::class.java)
    }
}
