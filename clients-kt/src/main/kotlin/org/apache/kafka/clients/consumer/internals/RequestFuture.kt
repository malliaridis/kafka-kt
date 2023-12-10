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

import org.apache.kafka.clients.consumer.internals.ConsumerNetworkClient.PollCondition
import org.apache.kafka.common.errors.RetriableException
import org.apache.kafka.common.protocol.Errors
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference

/**
 * Result of an asynchronous request from [ConsumerNetworkClient]. Use [ConsumerNetworkClient.poll]
 * (and variants) to finish a request future. Use [isDone] to check if the future is complete, and
 * [succeeded] to check if the request completed successfully. Typical usage might look like this:
 *
 * ```java
 * RequestFuture<ClientResponse> future = client.send(api, request);
 * client.poll(future);
 *
 * if (future.succeeded()) {
 *   ClientResponse response = future.value();
 *   // Handle response
 * } else {
 *   throw future.exception();
 * }
 * ```
 *
 * @param T Return type of the result (Can be Void if there is no response)
 */
class RequestFuture<T> : PollCondition {

    private val result = AtomicReference(INCOMPLETE_SENTINEL)

    private val listeners = ConcurrentLinkedQueue<RequestFutureListener<T>>()

    private val completedLatch = CountDownLatch(1)

    val isDone: Boolean
        /**
         * Check whether the response is ready to be handled.
         *
         * @return true if the response is ready, false otherwise
         */
        get() = result.get() !== INCOMPLETE_SENTINEL

    @Throws(InterruptedException::class)
    fun awaitDone(timeout: Long, unit: TimeUnit): Boolean = completedLatch.await(timeout, unit)

    /**
     * Get the value corresponding to this request (only available if the request succeeded).
     *
     * @return the value set in [complete]
     * @throws IllegalStateException if the future is not complete or failed
     */
    fun value(): T {
        check(succeeded()) {
            "Attempt to retrieve value from future which hasn't successfully completed"
        }
        return result.get() as T
    }

    /**
     * Check if the request succeeded;
     *
     * @return true if the request completed and was successful
     */
    fun succeeded(): Boolean = isDone && !failed()

    /**
     * Check if the request failed.
     *
     * @return true if the request completed with a failure
     */
    fun failed(): Boolean = result.get() is RuntimeException

    /**
     * Check if the request is retriable (convenience method for checking if the exception is an
     * instance of [RetriableException].
     *
     * @return true if it is retriable, false otherwise
     * @throws IllegalStateException if the future is not complete or completed successfully
     */
    val isRetriable: Boolean
        get() = exception() is RetriableException

    /**
     * Get the exception from a failed result (only available if the request failed).
     *
     * @return the exception set in [raise]
     * @throws IllegalStateException if the future is not complete or completed successfully
     */
    fun exception(): RuntimeException {
        check(failed()) { "Attempt to retrieve exception from future which hasn't failed" }
        return result.get() as RuntimeException
    }

    /**
     * Complete the request successfully. After this call, [succeeded] will return true
     * and the value can be obtained through [value].
     *
     * @param value corresponding value (or null if there is none)
     * @throws IllegalStateException if the future has already been completed
     * @throws IllegalArgumentException if the argument is an instance of [RuntimeException]
     */
    fun complete(value: T) {
        try {
            require(value !is RuntimeException) {
                "The argument to complete can not be an instance of RuntimeException"
            }
            check(result.compareAndSet(INCOMPLETE_SENTINEL, value)) {
                "Invalid attempt to complete a request future which is already complete"
            }
            fireSuccess()
        } finally {
            completedLatch.countDown()
        }
    }

    /**
     * Raise an exception. The request will be marked as failed, and the caller can either handle
     * the exception or throw it.
     *
     * @param e corresponding exception to be passed to caller
     * @throws IllegalStateException if the future has already been completed
     */
    fun raise(e: RuntimeException?) {
        try {
            requireNotNull(e) { "The exception passed to raise must not be null" }
            check(result.compareAndSet(INCOMPLETE_SENTINEL, e)) {
                "Invalid attempt to complete a request future which is already complete"
            }
            fireFailure()
        } finally {
            completedLatch.countDown()
        }
    }

    /**
     * Raise an error. The request will be marked as failed.
     *
     * @param error corresponding error to be passed to caller
     */
    fun raise(error: Errors) = raise(error.exception)

    private fun fireSuccess() {
        val value = value()
        while (true) {
            val listener = listeners.poll() ?: break
            listener.onSuccess(value)
        }
    }

    private fun fireFailure() {
        val exception = exception()
        while (true) {
            val listener = listeners.poll() ?: break
            listener.onFailure(exception)
        }
    }

    /**
     * Add a listener which will be notified when the future completes
     *
     * @param listener non-null listener to add
     */
    fun addListener(listener: RequestFutureListener<T>) {
        listeners.add(listener)
        if (failed()) fireFailure()
        else if (succeeded()) fireSuccess()
    }

    /**
     * Convert from a request future of one type to another type
     * @param adapter The adapter which does the conversion
     * @param <S> The type of the future adapted to
     * @return The new future
    </S> */
    fun <S> compose(adapter: RequestFutureAdapter<T, S>): RequestFuture<S> {
        val adapted: RequestFuture<S> = RequestFuture()

        addListener(object : RequestFutureListener<T> {
            override fun onSuccess(result: T) = adapter.onSuccess(result, adapted)
            override fun onFailure(exception: RuntimeException) = adapter.onFailure(exception, adapted)
        })

        return adapted
    }

    fun chain(future: RequestFuture<T>) {
        addListener(object : RequestFutureListener<T> {
            override fun onSuccess(result: T) = future.complete(result)
            override fun onFailure(exception: RuntimeException) = future.raise(exception)
        })
    }

    override fun shouldBlock(): Boolean = !isDone

    companion object {

        private val INCOMPLETE_SENTINEL = Any()

        fun <T> failure(e: RuntimeException?): RequestFuture<T> {
            val future: RequestFuture<T> = RequestFuture()
            future.raise(e)
            return future
        }

        fun voidSuccess(): RequestFuture<Unit> {
            val future: RequestFuture<Unit> = RequestFuture()
            future.complete(Unit)
            return future
        }

        fun <T> coordinatorNotAvailable(): RequestFuture<T> =
            failure(Errors.COORDINATOR_NOT_AVAILABLE.exception)

        fun <T> noBrokersAvailable(): RequestFuture<T> =
            failure(NoAvailableBrokersException())
    }
}
