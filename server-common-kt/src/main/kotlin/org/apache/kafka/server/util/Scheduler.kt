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

import java.util.concurrent.ScheduledFuture

/**
 * A scheduler for running jobs
 *
 * This interface controls a job scheduler that allows scheduling either repeating background jobs
 * that execute periodically or delayed one-time actions that are scheduled in the future.
 */
interface Scheduler {

    /**
     * Initialize this scheduler, so it is ready to accept scheduling of tasks
     */
    fun startup()

    /**
     * Shutdown this scheduler. When this method is complete no more executions of background tasks will occur.
     * This includes tasks scheduled with a delayed execution.
     */
    @Throws(InterruptedException::class)
    fun shutdown()

    fun scheduleOnce(
        name: String,
        task: Runnable,
        delayMs: Long = 0L,
    ): ScheduledFuture<*> = schedule(
        name = name,
        task = task,
        delayMs = delayMs,
        periodMs = -1,
    )

    /**
     * Schedule a task.
     * @param name The name of this task
     * @param task The task to run
     * @param delayMs The number of milliseconds to wait before the first execution
     * @param periodMs The period in milliseconds with which to execute the task. If < 0 the task
     * will execute only once.
     * @return A Future object to manage the task scheduled.
     */
    fun schedule(name: String, task: Runnable, delayMs: Long, periodMs: Long): ScheduledFuture<*>

    fun resizeThreadPool(newSize: Int)
}
