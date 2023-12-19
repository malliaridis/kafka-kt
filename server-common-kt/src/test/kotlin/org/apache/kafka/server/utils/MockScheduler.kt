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

package org.apache.kafka.server.utils

import java.util.PriorityQueue
import java.util.concurrent.Delayed
import java.util.concurrent.ExecutionException
import java.util.concurrent.ScheduledFuture
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException
import java.util.concurrent.atomic.AtomicLong
import java.util.function.Predicate
import org.apache.kafka.common.utils.Time
import org.apache.kafka.server.util.Scheduler

/**
 * A mock scheduler that executes tasks synchronously using a mock time instance. Tasks are executed synchronously
 * when the time is advanced. This class is meant to be used in conjunction with MockTime.
 *
 * Example usage
 * ```
 * val time = new MockTime
 * time.scheduler.schedule("a task", println("hello world: " + time.milliseconds), delay = 1000)
 * time.sleep(1001) // this should cause our scheduled task to fire
 *
 *
 * Incrementing the time to the exact next execution time of a task will result in that task executing
 * (it as if execution itself takes no time).
 */
class MockScheduler(private val time: Time) : Scheduler {

    // a priority queue of tasks ordered by next execution time
    private val tasks = PriorityQueue(Comparator.comparing(MockTask::nextExecution))

    override fun startup() = Unit

    override fun schedule(
        name: String,
        task: Runnable,
        delayMs: Long,
        periodMs: Long,
    ): ScheduledFuture<*> {
        val mockTask = MockTask(
            name = name,
            task = task,
            nextExecution = time.milliseconds() + delayMs,
            period = periodMs,
            time = time,
        )
        add(mockTask)
        tick()
        return mockTask
    }

    @Throws(InterruptedException::class)
    override fun shutdown() {
        var currentTask: MockTask?
        do {
            currentTask = poll { true }
            currentTask?.task?.run()
        } while (currentTask != null)
    }

    override fun resizeThreadPool(newSize: Int) = Unit

    /**
     * Check for any tasks that need to execute. Since this is a mock scheduler this check only occurs
     * when this method is called and the execution happens synchronously in the calling thread.
     * If you are using the scheduler associated with a MockTime instance this call be triggered automatically.
     */
    fun tick() {
        val now = time.milliseconds()
        var currentTask: MockTask?
        // pop and execute the task with the lowest next execution time if ready
        do {
            currentTask = poll { it.nextExecution() <= now }
            currentTask?.let {
                it.task.run()
                // if the task is periodic, reschedule it and re-enqueue
                if (it.rescheduleIfPeriodic()) add(it)
            }
        } while (currentTask != null)
    }

    fun clear() = synchronized(this) { tasks.clear() }

    private fun poll(predicate: Predicate<MockTask>): MockTask? = synchronized(this) {
        val result = tasks.peek()?.let { if (predicate.test(it)) it else null }

        // Remove element from the queue if `predicate` returned `true`
        result?.let { tasks.poll() }
        return result
    }

    private fun add(task: MockTask) = synchronized(this) { tasks.add(task) }

    private class MockTask(
        val name: String,
        val task: Runnable,
        nextExecution: Long,
        val period: Long,
        val time: Time,
    ) : ScheduledFuture<Void?> {

        private val nextExecution: AtomicLong = AtomicLong(nextExecution)

        /**
         * If this task is periodic, reschedule it and return true. Otherwise, do nothing and return false.
         */
        fun rescheduleIfPeriodic(): Boolean {
            return if (periodic()) {
                nextExecution.addAndGet(period)
                true
            } else false
        }

        fun nextExecution(): Long = nextExecution.get()

        override fun getDelay(unit: TimeUnit): Long =
            time.milliseconds() - nextExecution()

        override fun compareTo(other: Delayed): Int =
            getDelay(TimeUnit.MILLISECONDS).compareTo(other.getDelay(TimeUnit.MILLISECONDS))

        /**
         * Not used, so not fully implemented
         */
        override fun cancel(mayInterruptIfRunning: Boolean): Boolean = false

        override fun isCancelled(): Boolean = false

        override fun isDone(): Boolean = false

        @Throws(InterruptedException::class, ExecutionException::class)
        override fun get(): Void? = null

        @Throws(InterruptedException::class, ExecutionException::class, TimeoutException::class)
        override fun get(timeout: Long, unit: TimeUnit): Void? = null

        private fun periodic(): Boolean = period >= 0
    }
}
