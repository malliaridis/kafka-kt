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

package org.apache.kafka.common

import java.util.*
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionStage
import java.util.concurrent.ExecutionException
import java.util.concurrent.Future
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException
import org.apache.kafka.common.internals.KafkaFutureImpl

/**
 * A flexible future which supports call chaining and other asynchronous programming patterns.
 *
 * <h3>Relation to `CompletionStage`</h3>
 *
 * It is possible to obtain a [CompletionStage] from a [KafkaFuture] instance by calling
 * [toCompletionStage]. If converting [KafkaFuture.whenComplete] or [KafkaFuture.thenApply] to
 * [CompletableFuture.whenComplete] or [CompletableFuture.thenApply] be aware that the returned
 * `KafkaFuture` will fail with an [ExecutionException], whereas a `CompletionStage` fails
 * with a [CompletionException].
 */
abstract class KafkaFuture<T> : Future<T> {

    /**
     * A function which takes objects of type A and returns objects of type B.
     */
    fun interface BaseFunction<A, B> {
        fun apply(a: A): B
    }

    /**
     * A function which takes objects of type A and returns objects of type B.
     *
     */
    @Deprecated("Since Kafka 3.0. Use the {@link BaseFunction} functional interface.")
    abstract class Function<A, B> : BaseFunction<A, B>

    /**
     * A consumer of two different types of object.
     */
    fun interface BiConsumer<A, B> {
        fun accept(a: A, b: B)
    }

    /**
     * Gets a [CompletionStage] with the same completion properties as this [KafkaFuture].
     * The returned instance will complete when this future completes and in the same way
     * (with the same result or exception).
     *
     * Calling `toCompletableFuture()` on the returned instance will yield a `CompletableFuture`,
     * but invocation of the completion methods (`complete()` and other methods in the `complete*()`
     * and `obtrude*()` families) on that `CompletableFuture` instance will result in
     * `UnsupportedOperationException` being thrown. Unlike a "minimal" `CompletableFuture`,
     * the `get*()` and other methods of `CompletableFuture` that are not inherited from
     * `CompletionStage` will work normally.
     *
     * If you want to block on the completion of a KafkaFuture you should use [get] or [getNow],
     * rather than calling `toCompletionStage().toCompletableFuture().get()` etc.
     *
     * @since Kafka 3.0
     */
    abstract fun toCompletionStage(): CompletionStage<T>?

    /**
     * Returns a new [KafkaFuture] that, when this future completes normally, is executed with this
     * future's result as the argument to the supplied function.
     *
     * The function may be invoked by the thread that calls `thenApply` or it may be invoked by the
     * thread that completes the future.
     */
    abstract fun <R> thenApply(function: BaseFunction<T, R>): KafkaFuture<R>

    /**
     * @see KafkaFuture.thenApply
     */
    @Deprecated("Use thenApply with BaseFunction.")
    abstract fun <R> thenApply(function: Function<T, R>): KafkaFuture<R>

    /**
     * Returns a new KafkaFuture with the same result or exception as this future, that executes the
     * given action when this future completes.
     *
     * When this future is done, the given action is invoked with the result (or null if none) and
     * the exception (or null if none) of this future as arguments.
     *
     * The returned future is completed when the action returns.
     * The supplied action should not throw an exception. However, if it does, the following rules
     * apply:
     * - if this future completed normally but the supplied action throws an exception, then the
     * returned future completes exceptionally with the supplied action's exception.
     * - or, if this future completed exceptionally and the supplied action throws an exception,
     * then the returned future completes exceptionally with this future's exception.
     *
     * The action may be invoked by the thread that calls `whenComplete` or it may be invoked by the
     * thread that completes the future.
     *
     * @param action the action to preform
     * @return the new future
     */
    abstract fun whenComplete(action: BiConsumer<in T, in Throwable>): KafkaFuture<T>

    /**
     * If not already completed, sets the value returned by get() and related methods to the given
     * value.
     */
    protected abstract fun complete(newValue: T): Boolean

    /**
     * If not already completed, causes invocations of get() and related methods to throw the given
     * exception.
     */
    protected abstract fun completeExceptionally(newException: Throwable): Boolean

    /**
     * If not already completed, completes this future with a CancellationException.  Dependent
     * futures that have not already completed will also complete exceptionally, with a
     * CompletionException caused by this CancellationException.
     */
    abstract override fun cancel(mayInterruptIfRunning: Boolean): Boolean

    /**
     * Waits if necessary for this future to complete, and then returns its result.
     */
    @Throws(InterruptedException::class, ExecutionException::class)
    abstract override fun get(): T

    /**
     * Waits if necessary for at most the given time for this future to complete, and then returns
     * its result, if available.
     */
    @Throws(
        InterruptedException::class,
        ExecutionException::class,
        TimeoutException::class
    )
    abstract override fun get(timeout: Long, unit: TimeUnit): T

    /**
     * Returns the result value (or throws any encountered exception) if completed, else returns
     * the given valueIfAbsent.
     */
    @Throws(InterruptedException::class, ExecutionException::class)
    abstract fun getNow(valueIfAbsent: T): T

    /**
     * Returns true if this CompletableFuture was cancelled before it completed normally.
     */
    abstract override fun isCancelled(): Boolean

    /**
     * Returns true if this CompletableFuture completed exceptionally, in any way.
     */
    abstract val isCompletedExceptionally: Boolean

    /**
     * Returns true if completed in any fashion: normally, exceptionally, or via cancellation.
     */
    abstract override fun isDone(): Boolean

    companion object {

        /**
         * Returns a new KafkaFuture that is already completed with the given value.
         */
        fun <U> completedFuture(value: U): KafkaFuture<U> {
            val future: KafkaFuture<U> = KafkaFutureImpl()
            future.complete(value)
            return future
        }

        /**
         * Returns a new KafkaFuture that is completed when all the given futures have completed.
         * If any future throws an exception, the returned future returns it.  If multiple futures
         * throw an exception, which one gets returned is arbitrarily chosen.
         */
        fun allOf(vararg futures: KafkaFuture<*>): KafkaFuture<Unit> {
            val result = KafkaFutureImpl<Unit>()

            CompletableFuture.allOf(
                *futures.map { kafkaFuture ->
                    kafkaFuture.toCompletionStage() as CompletableFuture<*>
                }.toTypedArray()
            ).whenComplete { _, ex: Throwable? ->
                if (ex == null) result.complete(Unit)
                else {
                    // Have to unwrap the CompletionException which allOf() introduced
                    result.completeExceptionally(ex.cause!!)
                }
            }
            return result
        }
    }
}
