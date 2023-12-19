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

package org.apache.kafka.server.immutable

import org.mockito.kotlin.doAnswer
import org.mockito.kotlin.doCallRealMethod
import org.mockito.kotlin.whenever
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

/**
 * Facilitate testing of wrapper class delegation.
 *
 * We require the following things to test delegation:
 *
 * 1. A mock object to which the wrapper is expected to delegate method invocations
 * 2. A way to define how the mock is expected to behave when its method is invoked
 * 3. A way to define how to invoke the method on the wrapper
 * 4. A way to test that the method on the mock is invoked correctly when the wrapper method is invoked
 * 5. A way to test that any return value from the wrapper method is correct
 *
 * @param mock mock for the underlying delegate
 * @param wrapperCreator how to create a wrapper for the mock
 * @param D delegate type
 * @param W wrapper type
 * @param T delegating method return type, if any
 */
abstract class DelegationChecker<D, W, T> protected constructor(
    mock: D,
    wrapperCreator: (D) -> W,
) {
    
    private val mock: D
    
    private val wrapper: W
    
    private var mockConsumer: ((D) -> Unit)? = null
    
    private var mockConfigurationFunction: ((D) -> T?)? = null
    
    private var mockFunctionReturnValue: T? = null
    
    private var wrapperConsumer: ((W) -> Unit)? = null
    
    private var wrapperFunctionApplier: ((W) -> T?)? = null
    
    private var mockFunctionReturnValueTransformation: ((T) -> Any?)? = null
    
    private var expectWrapperToWrapMockFunctionReturnValue = false
    
    private var persistentCollectionMethodInvokedCorrectly = false

    init {
        this.mock = requireNotNull(mock)
        wrapper = wrapperCreator.invoke(mock)
    }

    /**
     * @param wrapper the wrapper
     * @return the underlying delegate for the given wrapper
     */
    abstract fun unwrap(wrapper: W): D
    
    fun defineMockConfigurationForVoidMethodInvocation(
        mockConsumer: (D) -> Unit,
    ): DelegationChecker<D, W, T> {
        this.mockConsumer = mockConsumer
        return this
    }

    fun defineMockConfigurationForFunctionInvocation(
        mockConfigurationFunction: (D) -> T,
        mockFunctionReturnValue: T,
    ): DelegationChecker<D, W, T> {
        this.mockConfigurationFunction = mockConfigurationFunction
        this.mockFunctionReturnValue = mockFunctionReturnValue
        return this
    }

    fun defineMockConfigurationForUnsupportedFunction(mockConfigurationFunction: (D) -> T?): DelegationChecker<D, W, T> {
        this.mockConfigurationFunction = mockConfigurationFunction
        return this
    }

    fun defineWrapperVoidMethodInvocation(wrapperConsumer: (W) -> Unit): DelegationChecker<D, W, T> {
        this.wrapperConsumer = wrapperConsumer
        return this
    }

    fun <R> defineWrapperFunctionInvocationAndMockReturnValueTransformation(
        wrapperFunctionApplier: (W) -> T,
        expectedFunctionReturnValueTransformation: (T) -> R,
    ): DelegationChecker<D, W, T> {
        this.wrapperFunctionApplier = wrapperFunctionApplier
        mockFunctionReturnValueTransformation = expectedFunctionReturnValueTransformation
        return this
    }

    fun defineWrapperUnsupportedFunctionInvocation(
        wrapperFunctionApplier: (W) -> T?,
    ): DelegationChecker<D, W, T> {
        this.wrapperFunctionApplier = wrapperFunctionApplier
        return this
    }

    fun expectWrapperToWrapMockFunctionReturnValue(): DelegationChecker<D, W, T> {
        expectWrapperToWrapMockFunctionReturnValue = true
        return this
    }

    fun doVoidMethodDelegationCheck() {
        if (
            mockConsumer == null
            || wrapperConsumer == null
            || mockConfigurationFunction != null
            || wrapperFunctionApplier != null
            || mockFunctionReturnValue != null
            || mockFunctionReturnValueTransformation != null
        ) throwExceptionForIllegalTestSetup()
        
        // configure the mock to behave as desired
        mockConsumer!!.invoke(doAnswer {
            persistentCollectionMethodInvokedCorrectly = true
            null
        }.whenever(mock))
        // invoke the wrapper, which should invoke the mock as desired
        wrapperConsumer!!.invoke(wrapper)
        // assert that the expected delegation to the mock actually occurred
        assertTrue(persistentCollectionMethodInvokedCorrectly)
    }

    fun doUnsupportedVoidFunctionDelegationCheck() {
        if (mockConsumer == null || wrapperConsumer == null) {
            throwExceptionForIllegalTestSetup()
        }

        // configure the mock to behave as desired
        mockConsumer!!.invoke(doCallRealMethod().whenever(mock))
        assertFailsWith<UnsupportedOperationException>(
            message = "Expected to Throw UnsupportedOperationException",
        ) { wrapperConsumer!!.invoke(wrapper) }
    }

    fun doFunctionDelegationCheck() {
        if (
            mockConfigurationFunction == null
            || wrapperFunctionApplier == null
            || mockFunctionReturnValueTransformation == null
            || mockConsumer != null
            || wrapperConsumer != null
        ) throwExceptionForIllegalTestSetup()
        // configure the mock to behave as desired
        whenever(mockConfigurationFunction!!.invoke(mock)).thenAnswer {
            persistentCollectionMethodInvokedCorrectly = true
            mockFunctionReturnValue
        }
        // invoke the wrapper, which should invoke the mock as desired
        val wrapperReturnValue = wrapperFunctionApplier!!.invoke(wrapper)
        // assert that the expected delegation to the mock actually occurred, including any return value transformation
        assertTrue(persistentCollectionMethodInvokedCorrectly)
        val transformedMockFunctionReturnValue = mockFunctionReturnValueTransformation!!.invoke(mockFunctionReturnValue!!)
        if (expectWrapperToWrapMockFunctionReturnValue)
            assertEquals(transformedMockFunctionReturnValue, unwrap(wrapperReturnValue as W))
        else assertEquals(transformedMockFunctionReturnValue, wrapperReturnValue)
    }

    fun doUnsupportedFunctionDelegationCheck() {
        if (mockConfigurationFunction == null || wrapperFunctionApplier == null) {
            throwExceptionForIllegalTestSetup()
        }
        whenever(mockConfigurationFunction!!.invoke(mock)).thenCallRealMethod()
        assertFailsWith<UnsupportedOperationException>(
            message = "Expected to Throw UnsupportedOperationException",
        ) { wrapperFunctionApplier!!.invoke(wrapper) }
    }

    companion object {

        private fun throwExceptionForIllegalTestSetup() {
            throw IllegalStateException(
                "test setup error: must define both mock and wrapper consumers or both mock and wrapper functions"
            )
        }
    }
}
