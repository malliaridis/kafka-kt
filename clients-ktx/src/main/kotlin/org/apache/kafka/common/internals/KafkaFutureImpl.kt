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

package org.apache.kafka.common.internals

import java.util.*
import java.util.concurrent.CancellationException
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionException
import java.util.concurrent.CompletionStage
import java.util.concurrent.ExecutionException
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException
import org.apache.kafka.common.KafkaFuture

/**
 * A flexible future which supports call chaining and other asynchronous programming patterns.
 */
class KafkaFutureImpl<T> private constructor(
    private val isDependant: Boolean,
    private val completableFuture: KafkaCompletableFuture<T>
) : KafkaFuture<T>() {

    constructor() : this(
        isDependant = false,
        completableFuture = KafkaCompletableFuture<T>(),
    )

    override fun toCompletionStage(): CompletionStage<T> {
        return completableFuture
    }

    /**
     * Returns a new KafkaFuture that, when this future completes normally, is executed with this
     * future's result as the argument to the supplied function.
     */
    override fun <R> thenApply(function: BaseFunction<T, R>): KafkaFuture<R> {
        val appliedFuture = completableFuture.thenApply apply@ { value: T ->
            try {
                return@apply function.apply(value)
            } catch (t : CompletionException) {
                // KafkaFuture#thenApply, when the function threw CompletionException should return
                // an ExecutionException wrapping a CompletionException wrapping the exception
                // thrown by the function. CompletableFuture#thenApply will just return
                // ExecutionException wrapping the exception thrown by the function, so we add an
                // extra CompletionException here to maintain the KafkaFuture behaviour.
                throw CompletionException(t)
            }
        }
        return KafkaFutureImpl(true, toKafkaCompletableFuture(appliedFuture))
    }

    /**
     * @see KafkaFutureImpl.thenApply
     */
    @Deprecated("Since Kafka 3.0.")
    override fun <R> thenApply(function: Function<T, R>): KafkaFuture<R> {
        return thenApply(function as BaseFunction<T, R>)
    }

    override fun whenComplete(action: BiConsumer<in T, in Throwable>): KafkaFuture<T> {
        val tCompletableFuture = completableFuture.whenComplete(
            java.util.function.BiConsumer { a: T, b: Throwable ->
                try {
                    action.accept(a, b)
                } catch (t: CompletionException) {
                    throw CompletionException(t)
                }
            } as java.util.function.BiConsumer<in T, in Throwable>)
        return KafkaFutureImpl(true, toKafkaCompletableFuture(tCompletableFuture))
    }

    public override fun complete(newValue: T): Boolean {
        return completableFuture.kafkaComplete(newValue)
    }

    public override fun completeExceptionally(newException: Throwable): Boolean {
        // CompletableFuture#get() always wraps the _cause_ of a CompletionException in ExecutionException
        // (which KafkaFuture does not) so wrap CompletionException in an extra one to avoid losing the
        // first CompletionException in the exception chain.
        return completableFuture.kafkaCompleteExceptionally(
            if (newException is CompletionException) CompletionException(newException) else newException
        )
    }

    /**
     * If not already completed, completes this future with a CancellationException.  Dependent
     * futures that have not already completed will also complete exceptionally, with a
     * CompletionException caused by this CancellationException.
     */
    override fun cancel(mayInterruptIfRunning: Boolean): Boolean {
        return completableFuture.cancel(mayInterruptIfRunning)
    }

    /**
     * We need to deal with differences between KafkaFuture's historic API and the API of CompletableFuture:
     * CompletableFuture#get() does not wrap CancellationException in ExecutionException (nor does KafkaFuture).
     * CompletableFuture#get() always wraps the _cause_ of a CompletionException in ExecutionException
     * (which KafkaFuture does not).
     *
     * The semantics for KafkaFuture are that all exceptional completions of the future (via #completeExceptionally()
     * or exceptions from dependants) manifest as ExecutionException, as observed via both get() and getNow().
     */
    private fun maybeThrowCancellationException(cause: Throwable?) {
        if (cause is CancellationException) {
            throw (cause as CancellationException?)!!
        }
    }

    /**
     * Waits if necessary for this future to complete, and then returns its result.
     */
    @Throws(InterruptedException::class, ExecutionException::class)
    override fun get(): T {
        return try {
            completableFuture.get()
        } catch (e: ExecutionException) {
            maybeThrowCancellationException(e.cause)
            throw e
        }
    }

    /**
     * Waits if necessary for at most the given time for this future to complete, and then returns
     * its result, if available.
     */
    @Throws(
        InterruptedException::class,
        ExecutionException::class,
        TimeoutException::class
    )
    override fun get(timeout: Long, unit: TimeUnit): T {
        return try {
            completableFuture[timeout, unit]
        } catch (e: ExecutionException) {
            maybeThrowCancellationException(e.cause)
            throw e
        }
    }

    /**
     * Returns the result value (or throws any encountered exception) if completed, else returns
     * the given valueIfAbsent.
     */
    @Throws(ExecutionException::class)
    override fun getNow(valueIfAbsent: T): T {
        return try {
            completableFuture.getNow(valueIfAbsent)
        } catch (e: CompletionException) {
            maybeThrowCancellationException(e.cause)
            // Note, unlike CompletableFuture#get() which throws ExecutionException, CompletableFuture#getNow()
            // throws CompletionException, thus needs rewrapping to conform to KafkaFuture API,
            // where KafkaFuture#getNow() throws ExecutionException.
            throw ExecutionException(e.cause)
        }
    }

    /**
     * Returns true if this CompletableFuture was cancelled before it completed normally.
     */
    override fun isCancelled(): Boolean {
        return if (isDependant) {
            // Having isCancelled() for a dependent future just return
            // CompletableFuture.isCancelled() would break the historical KafkaFuture behaviour because
            // CompletableFuture#isCancelled() just checks for the exception being CancellationException
            // whereas it will be a CompletionException wrapping a CancellationException
            // due needing to compensate for CompletableFuture's CompletionException unwrapping
            // shenanigans in other methods.
            try {
                completableFuture.getNow(null)
                false
            } catch (e: CompletionException) {
                e.cause is CancellationException
            } catch (_: Exception) {
                false
            }
        } else completableFuture.isCancelled
    }

    override val isCompletedExceptionally: Boolean
        /**
         * Returns true if this CompletableFuture completed exceptionally, in any way.
         */
        get() = completableFuture.isCompletedExceptionally

    /**
     * Returns true if completed in any fashion: normally, exceptionally, or via cancellation.
     */
    override fun isDone(): Boolean {
        return completableFuture.isDone
    }

    override fun toString(): String {
        var value: T? = null
        var exception: Throwable? = null
        try {
            value = completableFuture.getNow(null)
        } catch (e: CompletionException) {
            exception = e.cause
        } catch (e: Exception) {
            exception = e
        }
        return String.format(
            Locale.getDefault(),
            "KafkaFuture{value=%s,exception=%s,done=%b}",
            value,
            exception,
            exception != null || value != null
        )
    }

    companion object {
        private fun <U> toKafkaCompletableFuture(completableFuture: CompletableFuture<U>): KafkaCompletableFuture<U> {
            return if (completableFuture is KafkaCompletableFuture<*>) {
                completableFuture as KafkaCompletableFuture<U>
            } else {
                val result = KafkaCompletableFuture<U>()
                completableFuture.whenComplete { x: U, y: Throwable? ->
                    if (y != null) {
                        result.kafkaCompleteExceptionally(y)
                    } else {
                        result.kafkaComplete(x)
                    }
                }
                result
            }
        }
    }
}
