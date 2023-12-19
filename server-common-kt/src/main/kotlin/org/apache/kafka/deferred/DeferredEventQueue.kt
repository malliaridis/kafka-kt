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

package org.apache.kafka.deferred

import java.util.TreeMap
import org.apache.kafka.common.utils.LogContext
import org.slf4j.Logger

/**
 * The queue which holds deferred events that have been started, but not yet completed.
 * We wait for the high watermark of the log to advance before completing them.
 */
class DeferredEventQueue(logContext: LogContext) {

    private val log: Logger = logContext.logger(javaClass)

    /**
     * A map from log offsets to events.  Each event will be completed once the log
     * advances past its offset.
     */
    private val pending = TreeMap<Long, MutableList<DeferredEvent>>()

    /**
     * Complete some purgatory entries.
     *
     * @param offset The offset which the high water mark has advanced to.
     */
    fun completeUpTo(offset: Long) {
        val iterator = pending.iterator()
        var numCompleted = 0
        while (iterator.hasNext()) {
            val (logOffset, events) = iterator.next()
            if (logOffset > offset) break

            events.forEach { event ->
                log.debug("completeUpTo({}): successfully completing {}", offset, event)
                event.complete(null)
                numCompleted++
            }
            iterator.remove()
        }

        if (log.isTraceEnabled) log.trace(
            "completeUpTo({}): successfully completed {} deferred entries",
            offset, numCompleted
        )
    }

    /**
     * Fail all deferred events with the provided exception.
     *
     * @param exception The exception to fail the entries with.
     */
    fun failAll(exception: Exception) {
        val iterator = pending.iterator()
        while (iterator.hasNext()) {
            val (_, events) = iterator.next()
            events.forEach { event ->
                log.info("failAll({}): failing {}.", exception.javaClass.name, event)
                event.complete(exception)
            }
            iterator.remove()
        }
    }

    /**
     * Add a new deferred event to be completed by the provided offset.
     *
     * @param offset The offset to add the new event at.
     * @param event The new event.
     */
    fun add(offset: Long, event: DeferredEvent) {
        if (pending.isNotEmpty()) {
            val lastKey = pending.lastKey()
            require(offset >= lastKey) {
                "There is already a deferred event with offset $lastKey. We should not add one with an " +
                        "offset of $offset which is lower than that."
            }
        }
        var events = pending[offset]
        if (events == null) {
            events = mutableListOf()
            pending[offset] = events
        }
        events.add(event)
        if (log.isTraceEnabled) log.trace("Adding deferred event {} at offset {}", event, offset)
    }

    /**
     * Get the offset of the highest pending event, or empty if there are no pending events.
     */
    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("highestPendingOffset"),
    )
    fun highestPendingOffset(): Long? =
        if (pending.isEmpty()) null
        else pending.lastKey()

    /**
     * Get the offset of the highest pending event, or empty if there are no pending events.
     */
    val highestPendingOffset: Long?
        get() = if (pending.isEmpty()) null
        else pending.lastKey()
}
