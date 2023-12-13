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

import java.util.concurrent.ExecutorService
import java.util.concurrent.ThreadFactory
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong
import org.slf4j.LoggerFactory

/**
 * Utilities for working with threads.
 */
object ThreadUtils {

    private val log = LoggerFactory.getLogger(ThreadUtils::class.java)

    /**
     * Create a new ThreadFactory.
     *
     * @param pattern The pattern to use. If this contains %d, it will be replaced with a thread
     * number. It should not contain more than one %d.
     * @param daemon True if we want daemon threads.
     * @return The new ThreadFactory.
     */
    fun createThreadFactory(pattern: String, daemon: Boolean): ThreadFactory {

        return object : ThreadFactory {

            private val threadEpoch = AtomicLong(0)

            override fun newThread(r: Runnable): Thread {
                val threadName =
                    if (pattern.contains("%d"))
                        String.format(pattern, threadEpoch.addAndGet(1))
                    else pattern

                val thread = Thread(r, threadName)
                thread.isDaemon = daemon

                return thread
            }
        }
    }

    /**
     * Shuts down an executor service in two phases, first by calling shutdown to reject incoming tasks,
     * and then calling shutdownNow, if necessary, to cancel any lingering tasks.
     *
     * After the timeout/on interrupt, the service is forcefully closed.
     *
     * @param executorService The service to shut down.
     * @param timeout The timeout of the shutdown.
     * @param timeUnit The time unit of the shutdown timeout.
     */
    fun shutdownExecutorServiceQuietly(
        executorService: ExecutorService,
        timeout: Long,
        timeUnit: TimeUnit?,
    ) {
        executorService.shutdown() // Disable new tasks from being submitted
        try {
            // Wait a while for existing tasks to terminate
            if (!executorService.awaitTermination(timeout, timeUnit)) {
                executorService.shutdownNow() // Cancel currently executing tasks
                // Wait a while for tasks to respond to being cancelled
                if (!executorService.awaitTermination(timeout, timeUnit)) {
                    log.error("Executor {} did not terminate in time", executorService)
                }
            }
        } catch (e: InterruptedException) {
            // (Re-)Cancel if current thread also interrupted
            executorService.shutdownNow()
            // Preserve interrupt status
            Thread.currentThread().interrupt()
        }
    }
}
