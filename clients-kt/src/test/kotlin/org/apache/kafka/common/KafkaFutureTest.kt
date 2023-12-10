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

import org.apache.kafka.common.internals.KafkaFutureImpl
import org.apache.kafka.common.utils.Java
import org.junit.jupiter.api.Timeout
import java.lang.reflect.InvocationTargetException
import java.lang.reflect.Method
import java.util.concurrent.CancellationException
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionException
import java.util.concurrent.CompletionStage
import java.util.concurrent.ExecutionException
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException
import java.util.function.Supplier
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertNotEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * A unit test for KafkaFuture.
 */
@Timeout(120)
class KafkaFutureTest {

    /**
     * Asserts that the given future is done, didn't fail and wasn't cancelled.
     */
    private fun assertIsSuccessful(future: KafkaFuture<*>) {
        assertTrue(future.isDone())
        assertFalse(future.isCompletedExceptionally)
        assertFalse(future.isCancelled())
    }

    /**
     * Asserts that the given future is done, failed and wasn't cancelled.
     */
    private fun assertIsFailed(future: KafkaFuture<*>) {
        assertTrue(future.isDone())
        assertFalse(future.isCancelled())
        assertTrue(future.isCompletedExceptionally)
    }

    /**
     * Asserts that the given future is done, didn't fail and was cancelled.
     */
    private fun <T> assertIsCancelled(future: KafkaFuture<T?>) {
        assertTrue(future.isDone())
        assertTrue(future.isCancelled())
        assertTrue(future.isCompletedExceptionally)
        assertFailsWith<CancellationException> { future.getNow(null) }
        assertFailsWith<CancellationException> { future[0, TimeUnit.MILLISECONDS] }
    }

    private fun <T> awaitAndAssertResult(
        future: KafkaFuture<T>,
        expectedResult: T,
        alternativeValue: T,
    ) {
        assertNotEquals(expectedResult, alternativeValue)
        try {
            assertEquals(expectedResult, future[5, TimeUnit.MINUTES])
        } catch (e: Exception) {
            throw AssertionError("Unexpected exception", e)
        }
        try {
            assertEquals(expectedResult, future.get())
        } catch (e: Exception) {
            throw AssertionError("Unexpected exception", e)
        }
        try {
            assertEquals(expectedResult, future.getNow(alternativeValue))
        } catch (e: Exception) {
            throw AssertionError("Unexpected exception", e)
        }
    }

    private fun <T> awaitAndAssertFailure(
        future: KafkaFuture<T>,
        expectedException: Class<out Throwable>,
        expectedMessage: String,
    ): Throwable? {
        var executionException = assertFailsWith<ExecutionException> { future[5, TimeUnit.MINUTES] }
        assertEquals(expectedException, executionException.cause!!.javaClass)
        assertEquals(expectedMessage, executionException.cause!!.message)

        executionException = assertFailsWith<ExecutionException> { future.get() }
        assertEquals(expectedException, executionException.cause!!.javaClass)
        assertEquals(expectedMessage, executionException.cause!!.message)

        executionException = assertFailsWith<ExecutionException> { future.get() }
        assertEquals(expectedException, executionException.cause!!.javaClass)
        assertEquals(expectedMessage, executionException.cause!!.message)
        return executionException.cause
    }

    private fun awaitAndAssertCancelled(future: KafkaFuture<*>, expectedMessage: String?) {
        var cancellationException = assertFailsWith<CancellationException> { future[5, TimeUnit.MINUTES] }
        assertEquals(expectedMessage, cancellationException.message)
        assertEquals(CancellationException::class.java, cancellationException.javaClass)

        cancellationException = assertFailsWith<CancellationException> { future.get() }
        assertEquals(expectedMessage, cancellationException.message)
        assertEquals(CancellationException::class.java, cancellationException.javaClass)

        cancellationException = assertFailsWith<CancellationException> { future.get() }
        assertEquals(expectedMessage, cancellationException.message)
        assertEquals(CancellationException::class.java, cancellationException.javaClass)
    }

    @Throws(Throwable::class)
    private fun invokeOrThrow(method: Method, obj: Any, vararg args: Any): Any {
        return try {
            method.invoke(obj, *args)
        } catch (e: InvocationTargetException) {
            throw e.cause!!
        }
    }

    @Test
    @Throws(Exception::class)
    fun testCompleteFutures() {
        val future123 = KafkaFutureImpl<Int>()
        assertTrue(future123.complete(123))
        assertFalse(future123.complete(456))
        assertFalse(future123.cancel(true))
        assertEquals(123, future123.get())
        assertIsSuccessful(future123)
        val future456 = KafkaFuture.completedFuture(456)
        assertFalse(future456.complete(789))
        assertFalse(future456.cancel(true))
        assertEquals(456, future456.get())
        assertIsSuccessful(future456)
    }

    @Test
    fun testCompleteFuturesExceptionally() {
        val futureFail = KafkaFutureImpl<Int>()
        assertTrue(futureFail.completeExceptionally(RuntimeException("We require more vespene gas")))
        assertIsFailed(futureFail)
        assertFalse(futureFail.completeExceptionally(RuntimeException("We require more minerals")))
        assertFalse(futureFail.cancel(true))

        val executionException = assertFailsWith<ExecutionException> { futureFail.get() }
        assertIs<RuntimeException>(executionException.cause)
        assertEquals("We require more vespene gas", executionException.cause!!.message)

        val tricky1 = KafkaFutureImpl<Int>()
        assertTrue(tricky1.completeExceptionally(CompletionException(CancellationException())))
        assertIsFailed(tricky1)
        awaitAndAssertFailure(
            future = tricky1,
            expectedException = CompletionException::class.java,
            expectedMessage = "java.util.concurrent.CancellationException",
        )
    }

    @Test
    fun testCompleteFuturesViaCancellation() {
        val viaCancel = KafkaFutureImpl<Int?>()
        assertTrue(viaCancel.cancel(true))
        assertIsCancelled(viaCancel)
        awaitAndAssertCancelled(viaCancel, null)

        val viaCancellationException = KafkaFutureImpl<Int?>()
        assertTrue(viaCancellationException.completeExceptionally(CancellationException("We require more vespene gas")))
        assertIsCancelled(viaCancellationException)
        awaitAndAssertCancelled(viaCancellationException, "We require more vespene gas")
    }

    @Test
    fun testToString() {
        val success = KafkaFutureImpl<Int>()
        assertEquals("KafkaFuture{value=null,exception=null,done=false}", success.toString())
        success.complete(12)
        assertEquals("KafkaFuture{value=12,exception=null,done=true}", success.toString())

        val failure = KafkaFutureImpl<Int>()
        failure.completeExceptionally(RuntimeException("foo"))
        assertEquals(
            expected = "KafkaFuture{value=null,exception=java.lang.RuntimeException: foo,done=true}",
            actual = failure.toString(),
        )

        val tricky1 = KafkaFutureImpl<Int>()
        tricky1.completeExceptionally(CompletionException(CancellationException()))
        assertEquals(
            expected = "KafkaFuture{value=null,exception=java.util.concurrent.CompletionException: java.util.concurrent.CancellationException,done=true}",
            actual = tricky1.toString(),
        )

        val cancelled = KafkaFutureImpl<Int>()
        cancelled.cancel(true)
        assertEquals(
            expected = "KafkaFuture{value=null,exception=java.util.concurrent.CancellationException,done=true}",
            actual = cancelled.toString(),
        )
    }

    @Test
    @Throws(Exception::class)
    fun testCompletingFutures() {
        val future = KafkaFutureImpl<String?>()
        val myThread = CompleterThread(future, "You must construct additional pylons.")
        assertIsNotCompleted(future)
        assertEquals("I am ready", future.getNow("I am ready"))
        myThread.start()
        awaitAndAssertResult(future, "You must construct additional pylons.", "I am ready")
        assertIsSuccessful(future)
        myThread.join()
        assertNull(myThread.testException)
    }

    @Test
    @Throws(Exception::class)
    fun testCompletingFuturesExceptionally() {
        val future = KafkaFutureImpl<String?>()
        val myThread = CompleterThread(
            future = future,
            value = null,
            exception = RuntimeException("Ultimate efficiency achieved."),
        )
        assertIsNotCompleted(future)
        assertEquals("I am ready", future.getNow("I am ready"))
        myThread.start()
        awaitAndAssertFailure(future, RuntimeException::class.java, "Ultimate efficiency achieved.")
        assertIsFailed(future)
        myThread.join()
        assertNull(myThread.testException)
    }

    @Test
    @Throws(Exception::class)
    fun testCompletingFuturesViaCancellation() {
        val future = KafkaFutureImpl<String?>()
        val myThread = CompleterThread(
            future = future,
            value = null,
            exception = CancellationException("Ultimate efficiency achieved."),
        )
        assertIsNotCompleted(future)
        assertEquals("I am ready", future.getNow("I am ready"))
        myThread.start()
        awaitAndAssertCancelled(future, "Ultimate efficiency achieved.")
        assertIsCancelled(future)
        myThread.join()
        assertNull(myThread.testException)
    }

    private fun assertIsNotCompleted(future: KafkaFutureImpl<String?>) {
        assertFalse(future.isDone)
        assertFalse(future.isCompletedExceptionally)
        assertFalse(future.isCancelled)
    }

    @Test
    @Throws(Exception::class)
    fun testThenApplyOnSucceededFuture() {
        val future = KafkaFutureImpl<Int>()
        val doubledFuture = future.thenApply { integer: Int -> 2 * integer }
        assertFalse(doubledFuture.isDone())

        val tripledFuture = future.thenApply { integer: Int -> 3 * integer }
        assertFalse(tripledFuture.isDone())

        future.complete(21)
        assertEquals(expected = 21, actual = future.getNow(-1))
        assertEquals(expected = 42, actual = doubledFuture.getNow(-1))
        assertEquals(expected = 63, actual = tripledFuture.getNow(-1))

        val quadrupledFuture = future.thenApply { integer: Int -> 4 * integer }
        assertEquals(expected = 84, actual = quadrupledFuture.getNow(-1))
    }

    @Test
    fun testThenApplyOnFailedFuture() {
        val future = KafkaFutureImpl<Int>()
        val dependantFuture = future.thenApply { integer -> 2 * integer }
        future.completeExceptionally(RuntimeException("We require more vespene gas"))
        assertIsFailed(future)
        assertIsFailed(dependantFuture)
        awaitAndAssertFailure(
            future = future,
            expectedException = RuntimeException::class.java,
            expectedMessage = "We require more vespene gas",
        )
        awaitAndAssertFailure(
            future = dependantFuture,
            expectedException = RuntimeException::class.java,
            expectedMessage = "We require more vespene gas",
        )
    }

    @Test
    fun testThenApplyOnFailedFutureTricky() {
        val future = KafkaFutureImpl<Int>()
        val dependantFuture = future.thenApply { integer -> 2 * integer }
        future.completeExceptionally(CompletionException(RuntimeException("We require more vespene gas")))
        assertIsFailed(future)
        assertIsFailed(dependantFuture)
        awaitAndAssertFailure(
            future = future,
            expectedException = CompletionException::class.java,
            expectedMessage = "java.lang.RuntimeException: We require more vespene gas",
        )
        awaitAndAssertFailure(
            future = dependantFuture,
            expectedException = CompletionException::class.java,
            expectedMessage = "java.lang.RuntimeException: We require more vespene gas",
        )
    }

    @Test
    fun testThenApplyOnFailedFutureTricky2() {
        val future = KafkaFutureImpl<Int>()
        val dependantFuture = future.thenApply { integer -> 2 * integer }
        future.completeExceptionally(CompletionException(CancellationException()))
        assertIsFailed(future)
        assertIsFailed(dependantFuture)
        awaitAndAssertFailure(
            future,
            CompletionException::class.java,
            "java.util.concurrent.CancellationException",
        )
        awaitAndAssertFailure(
            future = dependantFuture,
            expectedException = CompletionException::class.java,
            expectedMessage = "java.util.concurrent.CancellationException",
        )
    }

    @Test
    fun testThenApplyOnSucceededFutureAndFunctionThrows() {
        val future = KafkaFutureImpl<Int?>()
        val dependantFuture = future.thenApply {
            throw RuntimeException("We require more vespene gas")
        }
        future.complete(21)
        assertIsSuccessful(future)
        assertIsFailed(dependantFuture)
        awaitAndAssertResult(
            future = future,
            expectedResult = 21,
            alternativeValue = null
        )
        awaitAndAssertFailure(
            future = dependantFuture,
            expectedException = RuntimeException::class.java,
            expectedMessage = "We require more vespene gas",
        )
    }

    @Test
    fun testThenApplyOnSucceededFutureAndFunctionThrowsCompletionException() {
        val future = KafkaFutureImpl<Int?>()
        val dependantFuture = future.thenApply {
            throw CompletionException(RuntimeException("We require more vespene gas"))
        }
        future.complete(21)
        assertIsSuccessful(future)
        assertIsFailed(dependantFuture)
        awaitAndAssertResult(
            future = future,
            expectedResult = 21,
            alternativeValue = null
        )
        val cause = awaitAndAssertFailure(
            future = dependantFuture,
            expectedException = CompletionException::class.java,
            expectedMessage = "java.lang.RuntimeException: We require more vespene gas",
        )
        assertTrue(cause!!.cause is RuntimeException)
        assertEquals(cause.cause!!.message, "We require more vespene gas")
    }

    @Test
    fun testThenApplyOnFailedFutureFunctionNotCalled() {
        val future = KafkaFutureImpl<Int>()
        val ran = booleanArrayOf(false)
        val dependantFuture = future.thenApply {
            // Because the top level future failed, this should never be called.
            ran[0] = true
            null
        }
        future.completeExceptionally(RuntimeException("We require more minerals"))
        assertIsFailed(future)
        assertIsFailed(dependantFuture)
        awaitAndAssertFailure(
            future = future,
            expectedException = RuntimeException::class.java,
            expectedMessage = "We require more minerals",
        )
        awaitAndAssertFailure(
            future = dependantFuture,
            expectedException = RuntimeException::class.java,
            expectedMessage = "We require more minerals",
        )
        assertFalse(ran[0])
    }

    @Test
    fun testThenApplyOnCancelledFuture() {
        val future = KafkaFutureImpl<Int?>()
        val dependantFuture: KafkaFuture<Int?> = future.thenApply { integer -> 2 * integer!! }
        future.cancel(true)
        assertIsCancelled(future)
        assertIsCancelled(dependantFuture)
        awaitAndAssertCancelled(future, null)
        awaitAndAssertCancelled(dependantFuture, null)
    }

    @Test
    @Throws(Throwable::class)
    fun testWhenCompleteOnSucceededFuture() {
        val future = KafkaFutureImpl<Int>()
        val err = arrayOfNulls<Throwable>(1)
        val ran = booleanArrayOf(false)
        val dependantFuture = future.whenComplete { integer: Int?, ex: Throwable? ->
            ran[0] = true
            try {
                assertEquals(21, integer)
                if (ex != null) throw ex
            } catch (e: Throwable) {
                err[0] = e
            }
        }

        assertFalse(dependantFuture.isDone())
        assertTrue(future.complete(21))
        assertTrue(ran[0])
        if (err[0] != null) throw err[0]!!
    }

    @Test
    fun testWhenCompleteOnFailedFuture() {
        val future = KafkaFutureImpl<Int>()
        val err = arrayOfNulls<Throwable>(1)
        val ran = booleanArrayOf(false)
        val dependantFuture = future.whenComplete { integer, ex ->
            ran[0] = true
            err[0] = ex
            if (integer != null) err[0] = AssertionError()
        }
        assertFalse(dependantFuture.isDone())
        val ex = RuntimeException("We require more vespene gas")
        assertTrue(future.completeExceptionally(ex))
        assertTrue(ran[0])
        assertEquals(err[0], ex)
    }

    @Test
    fun testWhenCompleteOnSucceededFutureAndConsumerThrows() {
        val future = KafkaFutureImpl<Int>()
        val ran = booleanArrayOf(false)
        val dependantFuture = future.whenComplete { _, _ ->
            ran[0] = true
            throw RuntimeException("We require more minerals")
        }
        assertFalse(dependantFuture.isDone())
        assertTrue(future.complete(21))
        assertIsSuccessful(future)
        assertTrue(ran[0])
        assertIsFailed(dependantFuture)
        awaitAndAssertFailure(
            future = dependantFuture,
            expectedException = RuntimeException::class.java,
            expectedMessage = "We require more minerals",
        )
    }

    @Test
    fun testWhenCompleteOnFailedFutureAndConsumerThrows() {
        val future = KafkaFutureImpl<Int>()
        val ran = booleanArrayOf(false)
        val dependantFuture = future.whenComplete { _, _ ->
            ran[0] = true
            throw RuntimeException("We require more minerals")
        }
        assertFalse(dependantFuture.isDone())
        assertTrue(future.completeExceptionally(RuntimeException("We require more vespene gas")))
        assertIsFailed(future)
        assertTrue(ran[0])
        assertIsFailed(dependantFuture)
        awaitAndAssertFailure(
            future = dependantFuture,
            expectedException = RuntimeException::class.java,
            expectedMessage = "We require more vespene gas",
        )
    }

    @Test
    fun testWhenCompleteOnCancelledFuture() {
        val future = KafkaFutureImpl<Int>()
        val err = arrayOfNulls<Throwable>(1)
        val ran = booleanArrayOf(false)
        val dependantFuture = future.whenComplete { integer, ex ->
            ran[0] = true
            err[0] = ex
            if (integer != null) err[0] = AssertionError()
        }
        assertFalse(dependantFuture.isDone())
        assertTrue(future.cancel(true))
        assertTrue(ran[0])
        assertTrue(err[0] is CancellationException)
    }

    private class CompleterThread<T> : Thread {
        private val future: KafkaFutureImpl<T>
        private val value: T
        private val exception: Throwable?
        var testException: Throwable? = null

        constructor(future: KafkaFutureImpl<T>, value: T) {
            this.future = future
            this.value = value
            exception = null
        }

        constructor(future: KafkaFutureImpl<T>, value: T, exception: Exception?) {
            this.future = future
            this.value = value
            this.exception = exception
        }

        override fun run() {
            try {
                try {
                    sleep(0, 200)
                } catch (e: InterruptedException) {
                    // ignore
                }
                if (exception == null) future.complete(value)
                else future.completeExceptionally(exception)
            } catch (testException: Throwable) {
                this.testException = testException
            }
        }
    }

    private class WaiterThread<T>(
        private val future: KafkaFutureImpl<T>,
        private val expected: T,
    ) : Thread() {
        var testException: Throwable? = null
        override fun run() {
            try {
                val value = future.get()
                assertEquals(expected, value)
            } catch (testException: Throwable) {
                this.testException = testException
            }
        }
    }

    @Test
    @Throws(Exception::class)
    fun testAllOfFutures() {
        val numThreads = 5
        val futures = mutableListOf<KafkaFutureImpl<Int>>()
        for (i in 0 until numThreads) futures.add(KafkaFutureImpl())

        val allFuture = KafkaFuture.allOf(futures)
        val completerThreads = mutableListOf<CompleterThread<Int>>()
        val waiterThreads = mutableListOf<WaiterThread<Int>>()
        for (i in 0 until numThreads) {
            completerThreads.add(CompleterThread(futures[i], i))
            waiterThreads.add(WaiterThread(futures[i], i))
        }
        assertFalse(allFuture.isDone())

        for (i in 0 until numThreads) waiterThreads[i].start()
        for (i in 0 until numThreads - 1) completerThreads[i].start()

        assertFalse(allFuture.isDone())
        completerThreads[numThreads - 1].start()
        allFuture.get()
        assertIsSuccessful(allFuture)
        for (i in 0 until numThreads) assertEquals(i, futures[i].get())
        for (i in 0 until numThreads) {
            completerThreads[i].join()
            waiterThreads[i].join()
            assertNull(completerThreads[i].testException)
            assertNull(waiterThreads[i].testException)
        }
    }

    @Test
    @Throws(Exception::class)
    fun testAllOfFuturesWithFailure() {
        val numThreads = 5
        val futures = mutableListOf<KafkaFutureImpl<Int?>>()
        for (i in 0 until numThreads) futures.add(KafkaFutureImpl())

        val allFuture = KafkaFuture.allOf(futures)
        val completerThreads = mutableListOf<CompleterThread<Int?>>()
        val waiterThreads = mutableListOf<WaiterThread<Int?>>()
        val lastIndex = numThreads - 1

        for (i in 0 until lastIndex) {
            completerThreads.add(CompleterThread(futures[i], i))
            waiterThreads.add(WaiterThread(futures[i], i))
        }

        completerThreads.add(CompleterThread(futures[lastIndex], null, RuntimeException("Last one failed")))
        waiterThreads.add(WaiterThread(futures[lastIndex], lastIndex))
        assertFalse(allFuture.isDone())
        for (i in 0 until numThreads) waiterThreads[i].start()
        for (i in 0 until lastIndex) completerThreads[i].start()

        assertFalse(allFuture.isDone())
        completerThreads[lastIndex].start()
        awaitAndAssertFailure(allFuture, RuntimeException::class.java, "Last one failed")
        assertIsFailed(allFuture)

        for (i in 0 until lastIndex) assertEquals(i, futures[i].get())
        assertIsFailed(futures[lastIndex])

        for (i in 0 until numThreads) {
            completerThreads[i].join()
            waiterThreads[i].join()
            assertNull(completerThreads[i].testException)
            if (i == lastIndex) {
                assertIs<ExecutionException>(waiterThreads[i].testException)
                assertIs<RuntimeException>(waiterThreads[i].testException!!.cause)
                assertEquals("Last one failed", waiterThreads[i].testException!!.cause!!.message)
            } else assertNull(waiterThreads[i].testException)
        }
    }

    @Test
    @Throws(Exception::class)
    fun testAllOfFuturesHandlesZeroFutures() {
        val allFuture: KafkaFuture<Unit> = KafkaFuture.allOf()
        assertTrue(allFuture.isDone())
        assertFalse(allFuture.isCancelled())
        assertFalse(allFuture.isCompletedExceptionally)
        allFuture.get()
    }

    @Test
    fun testFutureTimeoutWithZeroWait() {
        val future = KafkaFutureImpl<String>()
        assertFailsWith<TimeoutException> { future[0, TimeUnit.MILLISECONDS] }
    }

    @Test
    @Throws(Throwable::class)
    fun testLeakCompletableFuture() {
        val kfut = KafkaFutureImpl<String>()
        val comfut = kfut.toCompletionStage().toCompletableFuture()
        assertFailsWith<UnsupportedOperationException> { comfut.complete("") }
        assertFailsWith<UnsupportedOperationException> { comfut.completeExceptionally(RuntimeException()) }
        // Annoyingly CompletableFuture added some more methods in Java 9, but the tests need to run on Java 8
        // so test reflectively
        if (Java.IS_JAVA9_COMPATIBLE) {
            val completeOnTimeout = CompletableFuture::class.java.getDeclaredMethod(
                "completeOnTimeout",
                Any::class.java,
                Long::class.java,
                TimeUnit::class.java,
            )
            assertFailsWith<UnsupportedOperationException> {
                invokeOrThrow(completeOnTimeout, comfut, "", 1L, TimeUnit.MILLISECONDS)
            }
            val completeAsync =
                CompletableFuture::class.java.getDeclaredMethod("completeAsync", Supplier::class.java)
            assertFailsWith<UnsupportedOperationException> {
                invokeOrThrow(completeAsync, comfut, Supplier { "" })
            }
            val obtrudeValue = CompletableFuture::class.java.getDeclaredMethod("obtrudeValue", Any::class.java)
            assertFailsWith<UnsupportedOperationException> { invokeOrThrow(obtrudeValue, comfut, "") }
            val obtrudeException =
                CompletableFuture::class.java.getDeclaredMethod("obtrudeException", Throwable::class.java)
            assertFailsWith<UnsupportedOperationException> {
                invokeOrThrow(obtrudeException, comfut, RuntimeException())
            }

            // Check the CF from a minimal CompletionStage doesn't cause completion of the original KafkaFuture
            val minimal = CompletableFuture::class.java.getDeclaredMethod("minimalCompletionStage")
            val cs = invokeOrThrow(minimal, comfut) as CompletionStage<String>
            cs.toCompletableFuture().complete("")
            assertFalse(kfut.isDone)
            assertFalse(comfut.isDone)
        }
    }
}
