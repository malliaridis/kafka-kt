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

import java.util.TreeMap
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.locks.ReentrantLock
import java.util.function.Function
import org.apache.kafka.common.errors.TimeoutException
import org.apache.kafka.common.utils.KafkaThread
import org.apache.kafka.common.utils.LogContext
import org.apache.kafka.common.utils.Time
import org.apache.kafka.queue.EventQueue.EventInsertionType
import org.apache.kafka.queue.EventQueue.VoidEvent
import org.slf4j.Logger

/**
 * @property time The clock to use.
 * @property cleanupEvent The event to run when the queue is closing.
 */
class KafkaEventQueue(
    val time: Time,
    logContext: LogContext,
    threadNamePrefix: String,
    private val cleanupEvent: EventQueue.Event = EventQueue.Event { VoidEvent() },
) : EventQueue {

    /**
     * The lock which protects private data.
     */
    private val lock: ReentrantLock = ReentrantLock()

    /**
     * The log4j logger to use.
     */
    private val log: Logger = logContext.logger(javaClass)

    /**
     * The runnable that our thread executes.
     */
    private val eventHandler: EventHandler = EventHandler()

    /**
     * The queue thread.
     */
    private val eventHandlerThread: Thread = KafkaThread(
        name = threadNamePrefix + "event-handler",
        runnable = eventHandler,
        daemon = false,
    )

    /**
     * True if the event queue is shutting down. Protected by the lock.
     */
    private var shuttingDown: Boolean = false

    /**
     * True if the event handler thread was interrupted. Protected by the lock.
     */
    private var interrupted: Boolean = false

    init {
        eventHandlerThread.start()
    }

    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("time")
    )
    fun time(): Time = time

    override fun enqueue(
        insertionType: EventInsertionType,
        tag: String?,
        deadlineNsCalculator: Function<Long?, Long?>?,
        event: EventQueue.Event,
    ) {
        val eventContext = EventContext(event, insertionType, tag)

        eventHandler.enqueue(eventContext, deadlineNsCalculator)?.let { exception ->
            eventContext.completeWithException(log, exception)
        }
    }

    override fun cancelDeferred(tag: String) = eventHandler.cancelDeferred(tag)

    override fun beginShutdown(source: String?) {
        lock.lock()
        try {
            if (shuttingDown) {
                log.debug("{}: Event queue is already shutting down.", source)
                return
            }
            log.info("{}: shutting down event queue.", source)
            shuttingDown = true
            eventHandler.cond.signal()
        } finally {
            lock.unlock()
        }
    }

    override fun size(): Int = eventHandler.size()

    override fun wakeup() = eventHandler.wakeUp()

    @Throws(InterruptedException::class)
    override fun close() {
        beginShutdown("KafkaEventQueue#close")
        eventHandlerThread.join()
        log.info("closed event queue.")
    }

    /**
     * A context object that wraps events.
     *
     * @property event The caller-supplied event.
     * @property insertionType How this event was inserted.
     * @property tag The tag associated with this event.
     */
    private class EventContext(
        val event: EventQueue.Event?,
        val insertionType: EventInsertionType?,
        var tag: String?,
    ) {

        /**
         * The previous pointer of our circular doubly-linked list.
         */
        private var prev = this

        /**
         * The next pointer in our circular doubly-linked list.
         */
        var next = this

        /**
         * If this event is in the delay map, this is the key it is there under.
         * If it is not in the map, this is null.
         */
        var deadlineNs: Long? = null

        /**
         * Insert the event context in the circularly linked list after this node.
         */
        fun insertAfter(other: EventContext) {
            next.prev = other
            other.next = next
            other.prev = this
            next = other
        }

        /**
         * Insert a new node in the circularly linked list before this node.
         */
        fun insertBefore(other: EventContext) {
            prev.next = other
            other.prev = prev
            other.next = this
            prev = other
        }

        /**
         * Remove this node from the circularly linked list.
         */
        fun remove() {
            prev.next = next
            next.prev = prev
            prev = this
            next = this
        }


        /**
         * `true` if this node is the only element in its list.
         */
        val isSingleton: Boolean
            get() = prev === this && next === this

        /**
         * Run the event associated with this EventContext.
         *
         * @param log The logger to use.
         * @param exceptionToDeliver If non-null, the exception to deliver to the event.
         * @return `true` if the thread was interrupted; `false` otherwise.
         */
        fun run(log: Logger, exceptionToDeliver: Throwable?): Boolean {
            var exception = exceptionToDeliver
            if (exception == null) {
                try {
                    event!!.run()
                } catch (e: InterruptedException) {
                    log.warn("Interrupted while running event. Shutting down event queue")
                    return true
                } catch (e: Throwable) {
                    log.debug("Got exception while running {}. Invoking handleException.", event, e)
                    exception = e
                }
            }
            exception?.let { completeWithException(log, it) }
            return Thread.currentThread().isInterrupted
        }

        /**
         * Complete the event associated with this EventContext with the specified
         * exception.
         */
        fun completeWithException(log: Logger, t: Throwable?) {
            try {
                event!!.handleException(t)
            } catch (e: Exception) {
                log.error("Unexpected exception in handleException", e)
            }
        }
    }

    private inner class EventHandler : Runnable {

        private var size = 0

        /**
         * Event contexts indexed by tag.  Events without a tag are not included here.
         */
        private val tagToEventContext: MutableMap<String?, EventContext?> = HashMap()

        /**
         * The head of the event queue.
         */
        private val head = EventContext(null, null, null)

        /**
         * An ordered map of times in monotonic nanoseconds to events to time out.
         */
        private val deadlineMap = TreeMap<Long, EventContext>()

        /**
         * A condition variable for waking up the event handler thread.
         */
        val cond = lock.newCondition()

        override fun run() {
            try {
                handleEvents()
            } catch (exception: Throwable) {
                log.warn("event handler thread exiting with exception", exception)
            }
            try {
                cleanupEvent.run()
            } catch (exception: Throwable) {
                log.warn("cleanup event threw exception", exception)
            }
        }

        private fun remove(eventContext: EventContext) {
            eventContext.remove()

            eventContext.deadlineNs?.let { deadlineNs ->
                deadlineMap.remove(deadlineNs)
                eventContext.deadlineNs = null
            }
            eventContext.tag?.let { tag ->
                tagToEventContext.remove(tag, eventContext)
                eventContext.tag = null
            }
        }

        @Throws(InterruptedException::class)
        private fun handleEvents() {
            var toDeliver: Throwable? = null
            var toRun: EventContext? = null
            var wasInterrupted = false

            while (true) {
                if (toRun != null) {
                    wasInterrupted = toRun.run(log, toDeliver)
                }
                lock.lock()
                try {
                    if (toRun != null) {
                        size--
                        if (wasInterrupted) {
                            interrupted = true
                        }
                        toDeliver = null
                        toRun = null
                        wasInterrupted = false
                    }
                    var awaitNs = Long.MAX_VALUE
                    val entry = deadlineMap.firstEntry()
                    if (entry != null) {
                        // Search for timed-out events or deferred events that are ready to run.
                        val now = time.nanoseconds()
                        val (timeoutNs, eventContext) = entry

                        if (timeoutNs <= now) {
                            if (eventContext!!.insertionType == EventInsertionType.DEFERRED) {
                                // The deferred event is ready to run.  Prepend it to the queue.
                                // (The value for deferred events is a schedule time rather than a timeout.)
                                remove(eventContext)
                                toDeliver = null
                                toRun = eventContext
                            } else {
                                // not a deferred event, so it is a deadline, and it is timed out.
                                remove(eventContext)
                                toDeliver = TimeoutException()
                                toRun = eventContext
                            }
                            continue
                        } else if (interrupted) {
                            remove(eventContext)
                            toDeliver = InterruptedException("The event handler thread is interrupted")
                            toRun = eventContext
                            continue
                        } else if (shuttingDown) {
                            remove(eventContext)
                            toDeliver = RejectedExecutionException("The event queue is shutting down")
                            toRun = eventContext
                            continue
                        }
                        awaitNs = timeoutNs - now
                    }
                    if (head.next == head) {
                        if (deadlineMap.isEmpty() && (shuttingDown || interrupted)) {
                            // If there are no more entries to process, and the queue is
                            // closing, exit the thread.
                            return
                        }
                    } else {
                        toDeliver =
                            if (interrupted) InterruptedException("The event handler thread is interrupted")
                            else null

                        toRun = head.next
                        remove(toRun)
                        continue
                    }
                    if (awaitNs == Long.MAX_VALUE) {
                        try {
                            cond.await()
                        } catch (_: InterruptedException) {
                            log.warn("Interrupted while waiting for a new event. Shutting down event queue")
                            interrupted = true
                        }
                    } else {
                        try {
                            cond.awaitNanos(awaitNs)
                        } catch (_: InterruptedException) {
                            log.warn("Interrupted while waiting for a deferred event. Shutting down event queue")
                            interrupted = true
                        }
                    }
                } finally {
                    lock.unlock()
                }
            }
        }

        fun enqueue(
            eventContext: EventContext,
            deadlineNsCalculator: Function<Long?, Long?>?,
        ): Exception? {
            lock.lock()
            try {
                if (shuttingDown) return RejectedExecutionException("The event queue is shutting down")
                if (interrupted) return InterruptedException("The event handler thread is interrupted")

                var existingDeadlineNs: Long? = null
                if (eventContext.tag != null) {
                    val toRemove = tagToEventContext.put(eventContext.tag, eventContext)
                    if (toRemove != null) {
                        existingDeadlineNs = toRemove.deadlineNs
                        remove(toRemove)
                        size--
                    }
                }
                val deadlineNs = deadlineNsCalculator!!.apply(existingDeadlineNs)
                val queueWasEmpty = head.isSingleton
                var shouldSignal = false
                when (eventContext.insertionType) {
                    EventInsertionType.APPEND -> {
                        head.insertBefore(eventContext)
                        if (queueWasEmpty) shouldSignal = true
                    }

                    EventInsertionType.PREPEND -> {
                        head.insertAfter(eventContext)
                        if (queueWasEmpty) shouldSignal = true
                    }

                    EventInsertionType.DEFERRED -> if (deadlineNs == null) {
                        return RuntimeException("You must specify a deadline for deferred events.")
                    }
                    null -> Unit
                }
                if (deadlineNs != null) {
                    var insertNs: Long = deadlineNs
                    val prevStartNs =
                        if (deadlineMap.isEmpty()) Long.MAX_VALUE
                        else deadlineMap.firstKey()

                    // If the time in nanoseconds is already taken, take the next one.
                    while (deadlineMap.putIfAbsent(insertNs, eventContext) != null) insertNs++

                    eventContext.deadlineNs = insertNs
                    // If the new timeout is before all the existing ones, wake up the
                    // timeout thread.
                    if (insertNs <= prevStartNs) shouldSignal = true
                }
                size++
                if (shouldSignal) cond.signal()
            } finally {
                lock.unlock()
            }
            return null
        }

        fun cancelDeferred(tag: String?) {
            lock.lock()
            try {
                val eventContext = tagToEventContext[tag]
                if (eventContext != null) {
                    remove(eventContext)
                    size--
                }
            } finally {
                lock.unlock()
            }
        }

        fun wakeUp() {
            lock.lock()
            try {
                eventHandler.cond.signal()
            } finally {
                lock.unlock()
            }
        }

        fun size(): Int {
            lock.lock()
            return try {
                size
            } finally {
                lock.unlock()
            }
        }
    }
}
