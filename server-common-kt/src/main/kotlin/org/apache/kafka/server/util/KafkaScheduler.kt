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

import java.util.concurrent.Delayed
import java.util.concurrent.ScheduledFuture
import java.util.concurrent.ScheduledThreadPoolExecutor
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import org.apache.kafka.common.utils.KafkaThread
import org.slf4j.LoggerFactory
import kotlin.concurrent.Volatile

/**
 * A scheduler based on java.util.concurrent.ScheduledThreadPoolExecutor
 *
 * It has a pool of kafka-scheduler- threads that do the actual work.
 *
 * @constructor Creates an instance of this.
 * @property threads The number of threads in the thread pool
 * @property daemon If true the scheduler threads will be "daemon" threads and will not block jvm shutdown.
 * @property threadNamePrefix The name to use for scheduler threads. This prefix will have a number appended to it.
 */
class KafkaScheduler(
    private val threads: Int,
    private val daemon: Boolean = true,
    internal val threadNamePrefix: String = "kafka-scheduler-",
) : Scheduler {

    private val schedulerThreadId = AtomicInteger(0)

    @Volatile
    private var executor: ScheduledThreadPoolExecutor? = null

    override fun startup() {
        log.debug("Initializing task scheduler.")
        synchronized(this) {
            check(!isStarted) { "This scheduler has already been started." }
            val executor = ScheduledThreadPoolExecutor(threads)
            executor.setContinueExistingPeriodicTasksAfterShutdownPolicy(false)
            executor.setExecuteExistingDelayedTasksAfterShutdownPolicy(false)
            executor.removeOnCancelPolicy = true
            executor.setThreadFactory { runnable ->
                KafkaThread(
                    name = threadNamePrefix + schedulerThreadId.getAndIncrement(),
                    runnable = runnable,
                    daemon = daemon,
                )
            }
            this.executor = executor
        }
    }

    @Throws(InterruptedException::class)
    override fun shutdown() {
        log.debug("Shutting down task scheduler.")
        // We use the local variable to avoid NullPointerException if another thread shuts down scheduler at same time.
        var maybeExecutor: ScheduledThreadPoolExecutor? = null
        synchronized(this) {
            if (isStarted) {
                maybeExecutor = executor
                maybeExecutor!!.shutdown()
                executor = null
            }
        }
        maybeExecutor?.awaitTermination(1, TimeUnit.DAYS)
    }

    override fun schedule(name: String, task: Runnable, delayMs: Long, periodMs: Long): ScheduledFuture<*> {
        log.debug("Scheduling task {} with initial delay {} ms and period {} ms.", name, delayMs, periodMs)
        synchronized(this) {
            return if (isStarted) {
                val runnable = Runnable {
                    try {
                        log.trace("Beginning execution of scheduled task '{}'.", name)
                        task.run()
                    } catch (t: Throwable) {
                        log.error("Uncaught exception in scheduled task '{}'", name, t)
                    } finally {
                        log.trace("Completed execution of scheduled task '{}'.", name)
                    }
                }
                if (periodMs > 0) executor!!.scheduleAtFixedRate(
                    runnable,
                    delayMs,
                    periodMs,
                    TimeUnit.MILLISECONDS,
                ) else executor!!.schedule(runnable, delayMs, TimeUnit.MILLISECONDS)
            } else {
                log.info(
                    "Kafka scheduler is not running at the time task '{}' is scheduled. The task is ignored.",
                    name
                )
                NoOpScheduledFutureTask()
            }
        }
    }

    val isStarted: Boolean
        get() = executor != null

    override fun resizeThreadPool(newSize: Int) {
        synchronized(this) { if (isStarted) executor!!.setCorePoolSize(newSize) }
    }

    // Visible for testing
    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("threadNamePrefix"),
    )
    internal fun threadNamePrefix(): String = threadNamePrefix

    // Visible for testing
    internal fun taskRunning(task: ScheduledFuture<*>): Boolean {
        val e = executor
        return e != null && e.queue.contains(task as Runnable)
    }

    private class NoOpScheduledFutureTask : ScheduledFuture<Void?> {

        override fun cancel(mayInterruptIfRunning: Boolean): Boolean = true

        override fun isCancelled(): Boolean = true

        override fun isDone(): Boolean = true

        override fun get(): Void? = null

        override fun get(timeout: Long, unit: TimeUnit): Void? = null

        override fun getDelay(unit: TimeUnit): Long = 0L

        override fun compareTo(other: Delayed): Int {
            val diff = getDelay(TimeUnit.NANOSECONDS) - other.getDelay(TimeUnit.NANOSECONDS)
            return if (diff < 0) -1
            else if (diff > 0) 1
            else 0
        }
    }

    companion object {
        private val log = LoggerFactory.getLogger(KafkaScheduler::class.java)
    }
}
