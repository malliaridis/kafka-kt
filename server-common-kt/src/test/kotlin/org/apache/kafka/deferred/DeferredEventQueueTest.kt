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

import java.util.concurrent.CompletableFuture
import java.util.concurrent.ExecutionException
import org.apache.kafka.common.utils.LogContext
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertNull
import kotlin.test.assertTrue

@Timeout(value = 40)
class DeferredEventQueueTest {

    internal class SampleDeferredEvent : DeferredEvent {

        val future = CompletableFuture<Unit>()

        override fun complete(exception: Throwable?) {
            if (exception != null) future.completeExceptionally(exception)
            else future.complete(null)
        }

        @Deprecated(
            message = "Use property instead",
            replaceWith = ReplaceWith("future"),
        )
        fun future(): CompletableFuture<Unit> = future
    }

    @Test
    fun testCompleteEvents() {
        val deferredEventQueue = DeferredEventQueue(LogContext())
        val event1 = SampleDeferredEvent()
        val event2 = SampleDeferredEvent()
        val event3 = SampleDeferredEvent()
        deferredEventQueue.add(1, event1)
        assertEquals(1L, deferredEventQueue.highestPendingOffset)
        deferredEventQueue.add(1, event2)
        assertEquals(1L, deferredEventQueue.highestPendingOffset)
        deferredEventQueue.add(3, event3)
        assertEquals(3L, deferredEventQueue.highestPendingOffset)
        deferredEventQueue.completeUpTo(2)
        assertTrue(event1.future.isDone)
        assertTrue(event2.future.isDone)
        assertFalse(event3.future.isDone)
        deferredEventQueue.completeUpTo(4)
        assertTrue(event3.future.isDone)
        assertNull(deferredEventQueue.highestPendingOffset)
    }

    @Test
    fun testFailOnIncorrectOrdering() {
        val deferredEventQueue = DeferredEventQueue(LogContext())
        val event1 = SampleDeferredEvent()
        val event2 = SampleDeferredEvent()
        deferredEventQueue.add(2, event1)
        assertFailsWith<RuntimeException> { deferredEventQueue.add(1, event2) }
    }

    @Test
    fun testFailEvents() {
        val deferredEventQueue = DeferredEventQueue(LogContext())
        val event1 = SampleDeferredEvent()
        val event2 = SampleDeferredEvent()
        val event3 = SampleDeferredEvent()

        deferredEventQueue.add(1, event1)
        deferredEventQueue.add(3, event2)
        deferredEventQueue.add(3, event3)

        deferredEventQueue.completeUpTo(2)
        assertTrue(event1.future.isDone)
        assertFalse(event2.future.isDone)
        assertFalse(event3.future.isDone)

        deferredEventQueue.failAll(RuntimeException("failed"))
        assertTrue(event2.future.isDone)
        assertTrue(event3.future.isDone)

        assertIs<RuntimeException>(assertFailsWith<ExecutionException> { event2.future.get() }.cause)
        assertIs<RuntimeException>(assertFailsWith<ExecutionException> { event3.future.get() }.cause)
    }
}
