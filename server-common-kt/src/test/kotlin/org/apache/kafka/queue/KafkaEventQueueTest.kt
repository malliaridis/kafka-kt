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

package org.apache.kafka.queue

import java.util.concurrent.CompletableFuture
import java.util.concurrent.ExecutionException
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import org.apache.kafka.common.errors.TimeoutException
import org.apache.kafka.common.utils.LogContext
import org.apache.kafka.common.utils.MockTime
import org.apache.kafka.common.utils.Time
import org.apache.kafka.test.TestUtils
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertTrue

@Timeout(value = 60)
class KafkaEventQueueTest {

    @Test
    @Throws(Exception::class)
    fun testCreateAndClose() {
        val queue = KafkaEventQueue(Time.SYSTEM, LogContext(), "testCreateAndClose")
        queue.close()
    }

    @Test
    @Throws(Exception::class)
    fun testHandleEvents() {
        val queue = KafkaEventQueue(Time.SYSTEM, LogContext(), "testHandleEvents")
        val numEventsExecuted = AtomicInteger(0)
        val future1 = CompletableFuture<Int>()
        queue.prepend(FutureEvent(future1) {
            assertEquals(1, numEventsExecuted.incrementAndGet())
            1
        })
        val future2 = CompletableFuture<Int>()
        queue.appendWithDeadline(
            deadlineNs = Time.SYSTEM.nanoseconds() + TimeUnit.SECONDS.toNanos(60),
            event = FutureEvent(future2) {
                assertEquals(2, numEventsExecuted.incrementAndGet())
                2
            },
        )
        val future3 = CompletableFuture<Int>()
        queue.append(FutureEvent(future3) {
            assertEquals(3, numEventsExecuted.incrementAndGet())
            3
        })
        assertEquals(1, future1.get())
        assertEquals(3, future3.get())
        assertEquals(2, future2.get())
        val future4 = CompletableFuture<Int>()
        queue.appendWithDeadline(
            deadlineNs = Time.SYSTEM.nanoseconds() + TimeUnit.SECONDS.toNanos(60),
            event = FutureEvent(future4) {
                assertEquals(4, numEventsExecuted.incrementAndGet())
                4
            },
        )
        future4.get()
        queue.beginShutdown("testHandleEvents")
        queue.close()
    }

    @Test
    @Throws(Exception::class)
    fun testTimeouts() {
        val queue = KafkaEventQueue(Time.SYSTEM, LogContext(), "testTimeouts")
        val numEventsExecuted = AtomicInteger(0)
        val future1 = CompletableFuture<Int>()
        queue.append(FutureEvent(future1) {
            assertEquals(1, numEventsExecuted.incrementAndGet())
            1
        })
        val future2 = CompletableFuture<Int>()
        queue.append(FutureEvent(future2) {
            assertEquals(2, numEventsExecuted.incrementAndGet())
            Time.SYSTEM.sleep(1)
            2
        })
        val future3 = CompletableFuture<Int>()
        queue.appendWithDeadline(
            Time.SYSTEM.nanoseconds() + 1,
            FutureEvent(future3) {
                numEventsExecuted.incrementAndGet()
                3
            })
        val future4 = CompletableFuture<Int>()
        queue.append(FutureEvent(future4) {
            numEventsExecuted.incrementAndGet()
            4
        })
        assertEquals(1, future1.get())
        assertEquals(2, future2.get())
        assertEquals(4, future4.get())
        assertIs<TimeoutException>(assertFailsWith<ExecutionException> { future3.get() }.cause)
        queue.close()
        assertEquals(3, numEventsExecuted.get())
    }

    @Test
    @Throws(Exception::class)
    fun testScheduleDeferred() {
        val queue = KafkaEventQueue(Time.SYSTEM, LogContext(), "testAppendDeferred")

        // Wait for the deferred event to happen after the non-deferred event.
        // It may not happen every time, so we keep trying until it does.
        val counter = AtomicLong(0)
        var future1: CompletableFuture<Boolean>
        do {
            counter.addAndGet(1)
            future1 = CompletableFuture()
            queue.scheduleDeferred(
                tag = null,
                deadlineNsCalculator = { Time.SYSTEM.nanoseconds() + 1000000 },
                event = FutureEvent(future1) { counter.get() % 2 == 0L },
            )
            val future2 = CompletableFuture<Long>()
            queue.append(FutureEvent(future2) { counter.addAndGet(1) })
            future2.get()
        } while (!future1.get()!!)
        queue.close()
    }

    @Test
    @Throws(Exception::class)
    fun testScheduleDeferredWithTagReplacement() {
        val queue = KafkaEventQueue(
            Time.SYSTEM, LogContext(),
            "testScheduleDeferredWithTagReplacement"
        )
        val ai = AtomicInteger(0)
        val future1 = CompletableFuture<Int>()
        queue.scheduleDeferred(
            tag = "foo",
            deadlineNsCalculator = { Time.SYSTEM.nanoseconds() + ONE_HOUR_NS },
            event = FutureEvent(future1) { ai.addAndGet(1000) },
        )
        val future2 = CompletableFuture<Int>()
        queue.scheduleDeferred(
            tag = "foo",
            deadlineNsCalculator = { prev -> (prev ?: 0) - ONE_HOUR_NS },
            event = FutureEvent(future2) { ai.addAndGet(1) },
        )
        assertFalse(future1.isDone)
        assertEquals(1, future2.get())
        assertEquals(1, ai.get())
        queue.close()
    }

    @Test
    @Throws(Exception::class)
    fun testDeferredIsQueuedAfterTriggering() {
        val time = MockTime(
            autoTickMs = 0,
            currentTimeMs = 100000,
            currentHighResTimeNs = 1,
        )
        val queue = KafkaEventQueue(
            time, LogContext(),
            "testDeferredIsQueuedAfterTriggering"
        )
        val count = AtomicInteger(0)
        val futures = listOf<CompletableFuture<Int>>(
            CompletableFuture(),
            CompletableFuture(),
            CompletableFuture(),
        )
        queue.scheduleDeferred(
            tag = "foo",
            deadlineNsCalculator = { 2L },
            event = FutureEvent(futures[0]) { count.getAndIncrement() },
        )
        queue.append(FutureEvent(futures[1]) { count.getAndAdd(1) })
        assertEquals(0, futures[1].get())
        time.sleep(1)
        queue.append(FutureEvent(futures[2]) { count.getAndAdd(1) })
        assertEquals(1, futures[0].get())
        assertEquals(2, futures[2].get())
        queue.close()
    }

    @Test
    @Throws(Exception::class)
    fun testShutdownBeforeDeferred() {
        val queue = KafkaEventQueue(
            Time.SYSTEM, LogContext(),
            "testShutdownBeforeDeferred"
        )
        val count = AtomicInteger(0)
        val future = CompletableFuture<Int>()
        queue.scheduleDeferred(
            tag = "myDeferred",
            deadlineNsCalculator = { Time.SYSTEM.nanoseconds() + TimeUnit.HOURS.toNanos(1) },
            event = FutureEvent(future) { count.getAndAdd(1) },
        )
        queue.beginShutdown("testShutdownBeforeDeferred")
        assertIs<RejectedExecutionException>(assertFailsWith<ExecutionException> { future.get() }.cause)
        assertEquals(0, count.get())
        queue.close()
    }

    @Test
    @Throws(Exception::class)
    fun testRejectedExecutionException() {
        val queue = KafkaEventQueue(
            time = Time.SYSTEM,
            logContext = LogContext(),
            threadNamePrefix = "testRejectedExecutionException",
        )
        queue.close()
        val future = CompletableFuture<Unit>()
        queue.append(object : EventQueue.Event {
            @Throws(Exception::class)
            override fun run() {
                future.complete(Unit)
            }

            override fun handleException(exception: Throwable) {
                future.completeExceptionally(exception)
            }
        })
        assertIs<RejectedExecutionException>(assertFailsWith<ExecutionException> { future.get() }.cause)
    }

    @Test
    @Throws(Exception::class)
    fun testSize() {
        val queue = KafkaEventQueue(
            time = Time.SYSTEM,
            logContext = LogContext(),
            threadNamePrefix = "testEmpty",
        )
        assertTrue(queue.isEmpty())
        val future = CompletableFuture<Void>()
        queue.append { future.get() }
        assertFalse(queue.isEmpty())
        assertEquals(1, queue.size())
        queue.append { future.get() }
        assertEquals(2, queue.size())
        future.complete(null)
        TestUtils.waitForCondition(
            testCondition = { queue.isEmpty() },
            conditionDetails = "Failed to see the queue become empty.",
        )
        queue.scheduleDeferred(
            tag = "later",
            deadlineNsCalculator = { Time.SYSTEM.nanoseconds() + TimeUnit.HOURS.toNanos(1) },
            event = {},
        )
        assertFalse(queue.isEmpty())
        queue.scheduleDeferred(
            tag = "soon",
            deadlineNsCalculator = { Time.SYSTEM.nanoseconds() + TimeUnit.MILLISECONDS.toNanos(1) },
            event = {},
        )
        assertFalse(queue.isEmpty())
        queue.cancelDeferred("later")
        queue.cancelDeferred("soon")
        TestUtils.waitForCondition(
            testCondition = { queue.isEmpty() },
            conditionDetails = "Failed to see the queue become empty.",
        )
        queue.close()
        assertTrue(queue.isEmpty())
    }

    /**
     * Test that we continue handling events after Event#handleException itself throws an exception.
     */
    @Test
    @Throws(Exception::class)
    fun testHandleExceptionThrowingAnException() {
        val queue = KafkaEventQueue(
            time = Time.SYSTEM,
            logContext = LogContext(),
            threadNamePrefix = "testHandleExceptionThrowingAnException",
        )
        val initialFuture = CompletableFuture<Unit>()
        queue.append { initialFuture.get() }
        val counter = AtomicInteger(0)
        queue.append(object : EventQueue.Event {
            @Throws(Exception::class)
            override fun run() {
                counter.incrementAndGet()
                error("First exception")
            }

            override fun handleException(exception: Throwable) {
                if (exception is IllegalStateException) {
                    counter.incrementAndGet()
                    throw RuntimeException("Second exception")
                }
            }
        })
        queue.append { counter.incrementAndGet() }
        assertEquals(3, queue.size())
        initialFuture.complete(Unit)
        TestUtils.waitForCondition(
            testCondition = { counter.get() == 3 },
            conditionDetails = "Failed to see all events execute as planned.",
        )
        queue.close()
    }

    private class InterruptibleEvent(
        private val queueThread: CompletableFuture<Thread>,
        private val numCallsToRun: AtomicInteger,
        private val numInterruptedExceptionsSeen: AtomicInteger,
    ) : EventQueue.Event {

        private val runFuture: CompletableFuture<Unit> = CompletableFuture()

        @Throws(Exception::class)
        override fun run() {
            numCallsToRun.incrementAndGet()
            queueThread.complete(Thread.currentThread())
            runFuture.get()
        }

        override fun handleException(exception: Throwable) {
            if (exception is InterruptedException) {
                numInterruptedExceptionsSeen.incrementAndGet()
                Thread.currentThread().interrupt()
            }
        }
    }

    @Test
    @Throws(Exception::class)
    fun testInterruptedExceptionHandling() {
        val queue = KafkaEventQueue(
            time = Time.SYSTEM,
            logContext = LogContext(),
            threadNamePrefix = "testInterruptedExceptionHandling",
        )
        val queueThread = CompletableFuture<Thread>()
        val numCallsToRun = AtomicInteger(0)
        val numInterruptedExceptionsSeen = AtomicInteger(0)
        queue.append(InterruptibleEvent(queueThread, numCallsToRun, numInterruptedExceptionsSeen))
        queue.append(InterruptibleEvent(queueThread, numCallsToRun, numInterruptedExceptionsSeen))
        queue.append(InterruptibleEvent(queueThread, numCallsToRun, numInterruptedExceptionsSeen))
        queue.append(InterruptibleEvent(queueThread, numCallsToRun, numInterruptedExceptionsSeen))
        queueThread.get().interrupt()
        TestUtils.retryOnExceptionWithTimeout(
            timeoutMs = 30000,
            runnable = { assertEquals(1, numCallsToRun.get()) },
        )
        TestUtils.retryOnExceptionWithTimeout(
            timeoutMs = 30000,
            runnable = { assertEquals(3, numInterruptedExceptionsSeen.get()) },
        )
        queue.close()
    }

    internal class ExceptionTrapperEvent : EventQueue.Event {
        val exception = CompletableFuture<Throwable?>()

        @Throws(Exception::class)
        override fun run() {
            exception.complete(null)
        }

        override fun handleException(exception: Throwable) {
            this.exception.complete(exception)
        }
    }

    @Test
    @Throws(Exception::class)
    fun testInterruptedWithEmptyQueue() {
        val cleanupFuture = CompletableFuture<Unit>()
        val queue = KafkaEventQueue(
            time = Time.SYSTEM,
            logContext = LogContext(),
            threadNamePrefix = "testInterruptedWithEmptyQueue",
        ) { cleanupFuture.complete(Unit) }
        val queueThread = CompletableFuture<Thread>()
        queue.append { queueThread.complete(Thread.currentThread()) }
        TestUtils.retryOnExceptionWithTimeout(
            timeoutMs = 30000,
            runnable = { assertEquals(0, queue.size()) },
        )
        queueThread.get().interrupt()
        cleanupFuture.get()
        val ieTrapper = ExceptionTrapperEvent()
        queue.append(ieTrapper)
        assertIs<InterruptedException>(ieTrapper.exception.get())
        queue.close()
        val reTrapper = ExceptionTrapperEvent()
        queue.append(reTrapper)
        assertIs<RejectedExecutionException>(reTrapper.exception.get())
    }

    @Test
    @Throws(Exception::class)
    fun testInterruptedWithDeferredEvents() {
        val cleanupFuture = CompletableFuture<Void?>()
        val queue = KafkaEventQueue(
            time = Time.SYSTEM,
            logContext = LogContext(),
            threadNamePrefix = "testInterruptedWithDeferredEvents",
        ) { cleanupFuture.complete(null) }
        val queueThread = CompletableFuture<Thread>()
        queue.append { queueThread.complete(Thread.currentThread()) }
        val ieTrapper1 = ExceptionTrapperEvent()
        val ieTrapper2 = ExceptionTrapperEvent()
        queue.scheduleDeferred(
            tag = "ie2",
            deadlineNsCalculator = { Time.SYSTEM.nanoseconds() + TimeUnit.HOURS.toNanos(2) },
            event = ieTrapper2,
        )
        queue.scheduleDeferred(
            tag = "ie1",
            deadlineNsCalculator = { Time.SYSTEM.nanoseconds() + TimeUnit.HOURS.toNanos(1) },
            event = ieTrapper1,
        )
        TestUtils.retryOnExceptionWithTimeout(
            timeoutMs = 30000,
            runnable = { assertEquals(2, queue.size()) },
        )
        queueThread.get().interrupt()
        cleanupFuture.get()
        assertIs<InterruptedException>(ieTrapper1.exception.get())
        assertIs<InterruptedException>(ieTrapper2.exception.get())
        queue.close()
    }

    private class FutureEvent<T>(
        private val future: CompletableFuture<T>,
        private val supply: () -> T,
    ) : EventQueue.Event {

        @Throws(Exception::class)
        override fun run() {
            future.complete(supply())
        }

        override fun handleException(exception: Throwable) {
            future.completeExceptionally(exception)
        }
    }

    companion object {
        private val ONE_HOUR_NS = TimeUnit.NANOSECONDS.convert(1, TimeUnit.HOURS)
    }
}
