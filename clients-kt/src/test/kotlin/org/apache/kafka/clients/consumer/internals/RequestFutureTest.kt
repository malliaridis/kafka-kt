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

package org.apache.kafka.clients.consumer.internals

import org.junit.jupiter.api.Test
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull
import kotlin.test.assertTrue

class RequestFutureTest {
    
    @Test
    fun testBasicCompletion() {
        val future = RequestFuture<String>()
        val value = "foo"
        future.complete(value)
        assertTrue(future.isDone)
        assertEquals(value, future.value())
    }

    @Test
    fun testBasicFailure() {
        val future = RequestFuture<String>()
        val exception = RuntimeException()
        future.raise(exception)
        assertTrue(future.isDone)
        assertEquals(exception, future.exception())
    }

    @Test
    fun testUnitFuture() {
        val future = RequestFuture<Unit?>()
        future.complete(null)
        assertTrue(future.isDone)
        assertNull(future.value())
    }

    @Test
    fun testRuntimeExceptionInComplete() {
        val future = RequestFuture<Exception>()
        assertFailsWith<IllegalArgumentException> { future.complete(RuntimeException()) }
    }

    @Test
    fun invokeCompleteAfterAlreadyComplete() {
        val future = RequestFuture<Unit?>()
        future.complete(null)
        assertFailsWith<IllegalStateException> { future.complete(null) }
    }

    @Test
    fun invokeCompleteAfterAlreadyFailed() {
        val future = RequestFuture<Unit?>()
        future.raise(RuntimeException())
        assertFailsWith<IllegalStateException> { future.complete(null) }
    }

    @Test
    fun invokeRaiseAfterAlreadyFailed() {
        val future = RequestFuture<Unit>()
        future.raise(RuntimeException())
        assertFailsWith<IllegalStateException> { future.raise(RuntimeException()) }
    }

    @Test
    fun invokeRaiseAfterAlreadyCompleted() {
        val future = RequestFuture<Unit?>()
        future.complete(null)
        assertFailsWith<IllegalStateException> { future.raise(RuntimeException()) }
    }

    @Test
    fun invokeExceptionAfterSuccess() {
        val future = RequestFuture<Unit?>()
        future.complete(null)
        assertFailsWith<IllegalStateException> { future.exception() }
    }

    @Test
    fun invokeValueAfterFailure() {
        val future = RequestFuture<Unit>()
        future.raise(RuntimeException())
        assertFailsWith<IllegalStateException> { future.value() }
    }

    @Test
    fun listenerInvokedIfAddedBeforeFutureCompletion() {
        val future = RequestFuture<Unit?>()
        val listener = MockRequestFutureListener<Unit?>()
        future.addListener(listener)
        future.complete(null)
        assertOnSuccessInvoked(listener)
    }

    @Test
    fun listenerInvokedIfAddedBeforeFutureFailure() {
        val future = RequestFuture<Unit>()
        val listener = MockRequestFutureListener<Unit>()
        future.addListener(listener)
        future.raise(RuntimeException())
        assertOnFailureInvoked(listener)
    }

    @Test
    fun listenerInvokedIfAddedAfterFutureCompletion() {
        val future = RequestFuture<Unit?>()
        future.complete(null)
        val listener = MockRequestFutureListener<Unit?>()
        future.addListener(listener)
        assertOnSuccessInvoked(listener)
    }

    @Test
    fun listenerInvokedIfAddedAfterFutureFailure() {
        val future = RequestFuture<Unit>()
        future.raise(RuntimeException())
        val listener = MockRequestFutureListener<Unit>()
        future.addListener(listener)
        assertOnFailureInvoked(listener)
    }

    @Test
    fun listenersInvokedIfAddedBeforeAndAfterFailure() {
        val future = RequestFuture<Unit>()
        val beforeListener = MockRequestFutureListener<Unit>()
        future.addListener(beforeListener)
        future.raise(RuntimeException())
        val afterListener = MockRequestFutureListener<Unit>()
        future.addListener(afterListener)
        assertOnFailureInvoked(beforeListener)
        assertOnFailureInvoked(afterListener)
    }

    @Test
    fun listenersInvokedIfAddedBeforeAndAfterCompletion() {
        val future = RequestFuture<Unit?>()
        val beforeListener = MockRequestFutureListener<Unit?>()
        future.addListener(beforeListener)
        future.complete(null)
        val afterListener = MockRequestFutureListener<Unit?>()
        future.addListener(afterListener)
        assertOnSuccessInvoked(beforeListener)
        assertOnSuccessInvoked(afterListener)
    }

    @Test
    fun testComposeSuccessCase() {
        val future = RequestFuture<String>()
        val composed = future.compose(
            adapter = object : RequestFutureAdapter<String, Int>() {
                override fun onSuccess(value: String, future: RequestFuture<Int>) = future.complete(value.length)
            },
        )
        future.complete("hello")
        assertTrue(composed.isDone)
        assertTrue(composed.succeeded())
        assertEquals(5, composed.value())
    }

    @Test
    fun testComposeFailureCase() {
        val future = RequestFuture<String>()
        val composed = future.compose(
            adapter = object : RequestFutureAdapter<String, Int>() {
                override fun onSuccess(value: String, future: RequestFuture<Int>) = future.complete(value.length)
            }
        )
        val e = RuntimeException()
        future.raise(e)
        assertTrue(composed.isDone)
        assertTrue(composed.failed())
        assertEquals(e, composed.exception())
    }

    private class MockRequestFutureListener<T> : RequestFutureListener<T> {

        val numOnSuccessCalls = AtomicInteger(0)

        val numOnFailureCalls = AtomicInteger(0)

        override fun onSuccess(value: T) {
            numOnSuccessCalls.incrementAndGet()
        }

        override fun onFailure(e: RuntimeException) {
            numOnFailureCalls.incrementAndGet()
        }
    }

    companion object {

        private fun <T> assertOnSuccessInvoked(listener: MockRequestFutureListener<T>) {
            assertEquals(1, listener.numOnSuccessCalls.get())
            assertEquals(0, listener.numOnFailureCalls.get())
        }

        private fun <T> assertOnFailureInvoked(listener: MockRequestFutureListener<T>) {
            assertEquals(0, listener.numOnSuccessCalls.get())
            assertEquals(1, listener.numOnFailureCalls.get())
        }
    }
}
