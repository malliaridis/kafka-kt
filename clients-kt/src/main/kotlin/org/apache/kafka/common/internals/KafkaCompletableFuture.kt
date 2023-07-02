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

import java.util.concurrent.CompletableFuture
import java.util.concurrent.Executor
import java.util.concurrent.TimeUnit
import java.util.function.Supplier

/**
 * This internal class exists because CompletableFuture exposes [complete], [completeExceptionally]
 * and other methods which would allow erroneous completion by user code of a KafkaFuture returned
 * from a Kafka API to a client application.
 *
 * @param T The type of the future value.
 */
class KafkaCompletableFuture<T> : CompletableFuture<T>() {

    /**
     * Completes this future normally. For internal use by the Kafka clients, not by user code.
     * @param value the result value
     * @return `true` if this invocation caused this CompletableFuture
     * to transition to a completed state, else `false`
     */
    fun kafkaComplete(value: T): Boolean {
        return super.complete(value)
    }

    /**
     * Completes this future exceptionally. For internal use by the Kafka clients, not by user code.
     *
     * @param throwable the exception.
     * @return `true` if this invocation caused this CompletableFuture
     * to transition to a completed state, else `false`
     */
    fun kafkaCompleteExceptionally(throwable: Throwable): Boolean {
        return super.completeExceptionally(throwable)
    }

    override fun complete(value: T): Boolean {
        throw erroneousCompletionException()
    }

    override fun completeExceptionally(ex: Throwable): Boolean {
        throw erroneousCompletionException()
    }

    override fun obtrudeValue(value: T) {
        throw erroneousCompletionException()
    }

    override fun obtrudeException(ex: Throwable) {
        throw erroneousCompletionException()
    }

    // @Override // enable once Kafka no longer supports Java 8
    fun <U> newIncompleteFuture(): CompletableFuture<U> {
        return KafkaCompletableFuture()
    }

    // @Override // enable once Kafka no longer supports Java 8
    @Deprecated("Use kotlin function parameters instead.")
    fun completeAsync(supplier: Supplier<out T>, executor: Executor): CompletableFuture<T> {
        throw erroneousCompletionException()
    }

    // @Override // enable once Kafka no longer supports Java 8
    fun completeAsync(supplier: () -> T, executor: () -> Unit): CompletableFuture<T> {
        throw erroneousCompletionException()
    }

    //@Override // enable once Kafka no longer supports Java 8
    @Deprecated("Use kotlin function parameters instead.")
    fun completeAsync(supplier: Supplier<out T>?): CompletableFuture<T> {
        throw erroneousCompletionException()
    }

    //@Override // enable once Kafka no longer supports Java 8
    fun completeAsync(supplier: () -> T): CompletableFuture<T> {
        throw erroneousCompletionException()
    }

    //@Override // enable once Kafka no longer supports Java 8
    fun completeOnTimeout(value: T, timeout: Long, unit: TimeUnit): CompletableFuture<T> {
        throw erroneousCompletionException()
    }

    private fun erroneousCompletionException(): UnsupportedOperationException {
        return UnsupportedOperationException("User code should not complete futures returned from Kafka clients")
    }
}
