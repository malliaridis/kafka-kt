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

package org.apache.kafka.server.immutable.pcollections

import java.util.function.BiConsumer
import java.util.function.BiFunction
import java.util.function.Function
import org.apache.kafka.server.immutable.DelegationChecker
import org.apache.kafka.server.immutable.ImmutableMap
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import org.mockito.kotlin.eq
import org.mockito.kotlin.mock
import org.pcollections.HashPMap
import org.pcollections.HashTreePMap
import kotlin.test.assertEquals
import kotlin.test.assertNotEquals
import kotlin.test.assertSame

@Suppress("Deprecation")
class PCollectionsImmutableMapTest {

    // Kotlin Migration - Some tests fail if Kotlin function type is used for mocking, therefore usage
    // of java's Function, BiFunction etc. is mandatory here

    @Test
    fun testEmptyMap() {
        assertEquals(
            HashTreePMap.empty(),
            (ImmutableMap.empty<Any, Any>() as PCollectionsImmutableMap<Any, Any>).underlying()
        )
    }

    @Test
    fun testSingletonMap() {
        assertEquals(
            HashTreePMap.singleton(1, 2),
            (ImmutableMap.singleton(1, 2) as PCollectionsImmutableMap<*, *>).underlying()
        )
    }

    @Test
    fun testUnderlying() {
        assertSame(SINGLETON_MAP, PCollectionsImmutableMap(SINGLETON_MAP).underlying())
    }

    @Test
    fun testDelegationOfUpdated() {
        PCollectionsHashMapWrapperDelegationChecker<Any?>()
            .defineMockConfigurationForFunctionInvocation(
                mockConfigurationFunction = { mock -> mock.plus(eq(this), eq(this)) },
                mockFunctionReturnValue = SINGLETON_MAP,
            )
            .defineWrapperFunctionInvocationAndMockReturnValueTransformation(
                wrapperFunctionApplier = { wrapper -> wrapper.updated(this, this) },
                expectedFunctionReturnValueTransformation = { it },
            )
            .expectWrapperToWrapMockFunctionReturnValue()
            .doFunctionDelegationCheck()
    }

    @Test
    fun testDelegationOfRemoved() {
        PCollectionsHashMapWrapperDelegationChecker<Any?>()
            .defineMockConfigurationForFunctionInvocation(
                mockConfigurationFunction = { mock -> mock - eq(this) },
                mockFunctionReturnValue = SINGLETON_MAP,
            )
            .defineWrapperFunctionInvocationAndMockReturnValueTransformation(
                wrapperFunctionApplier = { wrapper -> wrapper.removed(this) },
                expectedFunctionReturnValueTransformation = { it },
            )
            .expectWrapperToWrapMockFunctionReturnValue()
            .doFunctionDelegationCheck()
    }

    @ParameterizedTest
    @ValueSource(ints = [1, 2])
    fun testDelegationOfSize(mockFunctionReturnValue: Int) {
        PCollectionsHashMapWrapperDelegationChecker<Any>()
            .defineMockConfigurationForFunctionInvocation(
                mockConfigurationFunction = { it.size },
                mockFunctionReturnValue = mockFunctionReturnValue,
            )
            .defineWrapperFunctionInvocationAndMockReturnValueTransformation(
                wrapperFunctionApplier = { it.size },
                expectedFunctionReturnValueTransformation = { it },
            )
            .doFunctionDelegationCheck()
    }

    @ParameterizedTest
    @ValueSource(booleans = [true, false])
    fun testDelegationOfIsEmpty(mockFunctionReturnValue: Boolean) {
        PCollectionsHashMapWrapperDelegationChecker<Any>()
            .defineMockConfigurationForFunctionInvocation(
                mockConfigurationFunction = { it.isEmpty() },
                mockFunctionReturnValue = mockFunctionReturnValue,
            )
            .defineWrapperFunctionInvocationAndMockReturnValueTransformation(
                wrapperFunctionApplier = { it.isEmpty() },
                expectedFunctionReturnValueTransformation = { it },
            )
            .doFunctionDelegationCheck()
    }

    @ParameterizedTest
    @ValueSource(booleans = [true, false])
    fun testDelegationOfContainsKey(mockFunctionReturnValue: Boolean) {
        PCollectionsHashMapWrapperDelegationChecker<Any>()
            .defineMockConfigurationForFunctionInvocation(
                mockConfigurationFunction = { mock -> mock.containsKey(eq(this)) },
                mockFunctionReturnValue = mockFunctionReturnValue,
            )
            .defineWrapperFunctionInvocationAndMockReturnValueTransformation(
                wrapperFunctionApplier = { wrapper -> wrapper.containsKey(this) },
                expectedFunctionReturnValueTransformation = { it },
            )
            .doFunctionDelegationCheck()
    }

    @ParameterizedTest
    @ValueSource(booleans = [true, false])
    fun testDelegationOfContainsValue(mockFunctionReturnValue: Boolean) {
        PCollectionsHashMapWrapperDelegationChecker<Any>()
            .defineMockConfigurationForFunctionInvocation(
                mockConfigurationFunction = { mock -> mock.containsValue(eq(this)) },
                mockFunctionReturnValue = mockFunctionReturnValue,
            )
            .defineWrapperFunctionInvocationAndMockReturnValueTransformation(
                wrapperFunctionApplier = { wrapper -> wrapper.containsValue(this) },
                expectedFunctionReturnValueTransformation = { it },
            )
            .doFunctionDelegationCheck()
    }

    @Test
    fun testDelegationOfGet() {
        PCollectionsHashMapWrapperDelegationChecker<Any?>()
            .defineMockConfigurationForFunctionInvocation(
                mockConfigurationFunction = { mock -> mock[eq(this)] },
                mockFunctionReturnValue = Any(),
            )
            .defineWrapperFunctionInvocationAndMockReturnValueTransformation(
                wrapperFunctionApplier = { wrapper -> wrapper[this] },
                expectedFunctionReturnValueTransformation = { it },
            )
            .doFunctionDelegationCheck()
    }

    @Test
    fun testDelegationOfUnsupportedFunctionPut() {
        PCollectionsHashMapWrapperDelegationChecker<Any?>()
            .defineMockConfigurationForUnsupportedFunction { mock ->
                mock.put(eq(this), eq(this))
            }
            .defineWrapperUnsupportedFunctionInvocation { wrapper -> wrapper.put(this, this) }
            .doUnsupportedFunctionDelegationCheck()
    }

    @Test
    fun testDelegationOfUnsupportedFunctionRemoveByKey() {
        PCollectionsHashMapWrapperDelegationChecker<Any?>()
            .defineMockConfigurationForUnsupportedFunction { mock -> mock.remove(eq(this)) }
            .defineWrapperUnsupportedFunctionInvocation { wrapper -> wrapper.remove(this) }
            .doUnsupportedFunctionDelegationCheck()
    }

    @Test
    fun testDelegationOfUnsupportedFunctionPutAll() {
        PCollectionsHashMapWrapperDelegationChecker<Any?>()
            .defineMockConfigurationForVoidMethodInvocation { mock -> mock.putAll(eq(emptyMap())) }
            .defineWrapperVoidMethodInvocation { wrapper -> wrapper.putAll(emptyMap()) }
            .doUnsupportedVoidFunctionDelegationCheck()
    }

    @Test
    fun testDelegationOfUnsupportedFunctionClear() {
        PCollectionsHashMapWrapperDelegationChecker<Any>()
            .defineMockConfigurationForVoidMethodInvocation { it.clear() }
            .defineWrapperVoidMethodInvocation { it.clear() }
            .doUnsupportedVoidFunctionDelegationCheck()
    }

    @Test
    fun testDelegationOfKeySet() {
        PCollectionsHashMapWrapperDelegationChecker<Any>()
            .defineMockConfigurationForFunctionInvocation(
                mockConfigurationFunction = { it.keys },
                mockFunctionReturnValue = emptySet<Any>(),
            )
            .defineWrapperFunctionInvocationAndMockReturnValueTransformation(
                wrapperFunctionApplier = { it.keys },
                expectedFunctionReturnValueTransformation = { it },
            )
            .doFunctionDelegationCheck()
    }

    @Test
    fun testDelegationOfValues() {
        PCollectionsHashMapWrapperDelegationChecker<Any>()
            .defineMockConfigurationForFunctionInvocation(
                mockConfigurationFunction = { it.values },
                mockFunctionReturnValue = emptySet<Any>(),
            )
            .defineWrapperFunctionInvocationAndMockReturnValueTransformation(
                wrapperFunctionApplier = { it.values },
                expectedFunctionReturnValueTransformation = { it },
            )
            .doFunctionDelegationCheck()
    }

    @Test
    fun testDelegationOfEntrySet() {
        PCollectionsHashMapWrapperDelegationChecker<Any>()
            .defineMockConfigurationForFunctionInvocation(
                mockConfigurationFunction = { it.entries },
                mockFunctionReturnValue = emptySet<Any>(),
            )
            .defineWrapperFunctionInvocationAndMockReturnValueTransformation(
                wrapperFunctionApplier = { it.entries },
                expectedFunctionReturnValueTransformation = { it },
            )
            .doFunctionDelegationCheck()
    }

    @Test
    fun testEquals() {
        val mock = mock<HashPMap<Any, Any>>()
        assertEquals(PCollectionsImmutableMap(mock), PCollectionsImmutableMap(mock))
        val someOtherMock = mock<HashPMap<Any, Any>>()
        assertNotEquals(PCollectionsImmutableMap(mock), PCollectionsImmutableMap(someOtherMock))
    }

    @Test
    fun testHashCode() {
        val mock = mock<HashPMap<Any, Any>>()
        assertEquals(mock.hashCode(), PCollectionsImmutableMap(mock).hashCode())
        val someOtherMock = mock<HashPMap<Any, Any>>()
        assertNotEquals(mock.hashCode(), PCollectionsImmutableMap(someOtherMock).hashCode())
    }

    @Test
    fun testDelegationOfGetOrDefault() {
        PCollectionsHashMapWrapperDelegationChecker<Any?>()
            .defineMockConfigurationForFunctionInvocation(
                mockConfigurationFunction = { mock -> mock.getOrDefault(eq(this), eq(this)) },
                mockFunctionReturnValue = this,
            )
            .defineWrapperFunctionInvocationAndMockReturnValueTransformation(
                wrapperFunctionApplier = { wrapper -> wrapper.getOrDefault(this, this) },
                expectedFunctionReturnValueTransformation = { it },
            )
            .doFunctionDelegationCheck()
    }

    @Test
    fun testDelegationOfForEach() {
        val mockBiConsumer = mock<BiConsumer<Any?, Any?>>()
        PCollectionsHashMapWrapperDelegationChecker<Any?>()
            .defineMockConfigurationForVoidMethodInvocation { mock -> mock.forEach(eq(mockBiConsumer)) }
            .defineWrapperVoidMethodInvocation { wrapper -> wrapper.forEach(mockBiConsumer) }
            .doVoidMethodDelegationCheck()
    }

    @Test
    fun testDelegationOfUnsupportedFunctionReplaceAll() {
        val mockBiFunction = mock<BiFunction<Any, Any, Any>>()
        PCollectionsHashMapWrapperDelegationChecker<Any>()
            .defineMockConfigurationForVoidMethodInvocation { mock -> mock.replaceAll(eq(mockBiFunction)) }
            .defineWrapperVoidMethodInvocation { wrapper -> wrapper.replaceAll(mockBiFunction) }
            .doUnsupportedVoidFunctionDelegationCheck()
    }

    @Test
    fun testDelegationOfUnsupportedFunctionPutIfAbsent() {
        PCollectionsHashMapWrapperDelegationChecker<Any?>()
            .defineMockConfigurationForUnsupportedFunction { mock -> mock.putIfAbsent(eq(this), eq(this)) }
            .defineWrapperUnsupportedFunctionInvocation { wrapper -> wrapper.putIfAbsent(this, this) }
            .doUnsupportedFunctionDelegationCheck()
    }

    @ParameterizedTest
    @ValueSource(booleans = [true, false])
    fun testDelegationOfUnsupportedFunctionRemoveByKeyAndValue(mockFunctionReturnValue: Boolean) {
        PCollectionsHashMapWrapperDelegationChecker<Any>()
            .defineMockConfigurationForFunctionInvocation(
                mockConfigurationFunction = { mock -> mock.remove(eq(this), eq(this)) },
                mockFunctionReturnValue = mockFunctionReturnValue,
            )
            .defineWrapperFunctionInvocationAndMockReturnValueTransformation(
                wrapperFunctionApplier = { wrapper -> wrapper.remove(this, this) },
                expectedFunctionReturnValueTransformation = { it },
            )
            .doFunctionDelegationCheck()
    }

    @ParameterizedTest
    @ValueSource(booleans = [true, false])
    fun testDelegationOfUnsupportedFunctionReplaceWhenMappedToSpecificValue(mockFunctionReturnValue: Boolean) {
        PCollectionsHashMapWrapperDelegationChecker<Any>()
            .defineMockConfigurationForFunctionInvocation(
                mockConfigurationFunction = { mock -> mock.replace(eq(this), eq(this), eq(this)) },
                mockFunctionReturnValue = mockFunctionReturnValue,
            )
            .defineWrapperFunctionInvocationAndMockReturnValueTransformation(
                wrapperFunctionApplier = { wrapper -> wrapper.replace(this, this, this) },
                expectedFunctionReturnValueTransformation = { it },
            )
            .doFunctionDelegationCheck()
    }

    @Test
    fun testDelegationOfUnsupportedFunctionReplaceWhenMappedToAnyValue() {
        PCollectionsHashMapWrapperDelegationChecker<Any?>()
            .defineMockConfigurationForUnsupportedFunction { mock -> mock.replace(eq(this), eq(this)) }
            .defineWrapperUnsupportedFunctionInvocation { wrapper -> wrapper.replace(this, this) }
            .doUnsupportedFunctionDelegationCheck()
    }

    @Test
    fun testDelegationOfUnsupportedFunctionComputeIfAbsent() {
        val mockFunction = mock<Function<Any, Any>>()
        PCollectionsHashMapWrapperDelegationChecker<Any>()
            .defineMockConfigurationForUnsupportedFunction { mock ->
                mock.computeIfAbsent(eq(this), eq(mockFunction))
            }
            .defineWrapperUnsupportedFunctionInvocation { wrapper ->
                wrapper.computeIfAbsent(this, mockFunction)
            }
            .doUnsupportedFunctionDelegationCheck()
    }

    @Test
    fun testDelegationOfUnsupportedFunctionComputeIfPresent() {
        val mockBiFunction = mock<BiFunction<Any, Any, Any>>()
        PCollectionsHashMapWrapperDelegationChecker<Any>()
            .defineMockConfigurationForUnsupportedFunction { mock ->
                mock.computeIfPresent(eq(this), eq(mockBiFunction))
            }
            .defineWrapperUnsupportedFunctionInvocation { wrapper ->
                wrapper.computeIfPresent(this, mockBiFunction)
            }
            .doUnsupportedFunctionDelegationCheck()
    }

    @Test
    fun testDelegationOfUnsupportedFunctionCompute() {
        val mockBiFunction = mock<BiFunction<Any?, Any?, Any>>()
        PCollectionsHashMapWrapperDelegationChecker<Any?>()
            .defineMockConfigurationForUnsupportedFunction { mock -> mock.compute(eq(this), eq(mockBiFunction)) }
            .defineWrapperUnsupportedFunctionInvocation { wrapper -> wrapper.compute(this, mockBiFunction) }
            .doUnsupportedFunctionDelegationCheck()
    }

    @Test
    fun testDelegationOfUnsupportedFunctionMerge() {
        val mockBiFunction = mock<BiFunction<Any, Any, Any>>()
        PCollectionsHashMapWrapperDelegationChecker<Any?>()
            .defineMockConfigurationForUnsupportedFunction { mock ->
                mock.merge(eq(this), eq(this), eq(mockBiFunction))
            }
            .defineWrapperUnsupportedFunctionInvocation { wrapper ->
                wrapper.merge(this, this, mockBiFunction)
            }
            .doUnsupportedFunctionDelegationCheck()
    }

    @ParameterizedTest
    @ValueSource(strings = ["a", "b"])
    fun testDelegationOfToString(mockFunctionReturnValue: String?) {
        PCollectionsHashMapWrapperDelegationChecker<Any?>()
            .defineMockConfigurationForFunctionInvocation(
                mockConfigurationFunction = { it.toString() },
                mockFunctionReturnValue = mockFunctionReturnValue,
            )
            .defineWrapperFunctionInvocationAndMockReturnValueTransformation(
                wrapperFunctionApplier = { it.toString() },
                expectedFunctionReturnValueTransformation = { text -> "PCollectionsImmutableMap{underlying=$text}" }
            )
            .doFunctionDelegationCheck()
    }

    private class PCollectionsHashMapWrapperDelegationChecker<R> :
        DelegationChecker<HashPMap<Any, Any>, PCollectionsImmutableMap<Any, Any>, R>(
            mock = mock<HashPMap<Any, Any>>(),
            wrapperCreator = { underlying -> PCollectionsImmutableMap(underlying) },
        ) {

        override fun unwrap(
            wrapper: PCollectionsImmutableMap<Any, Any>,
        ): HashPMap<Any, Any> = wrapper.underlying()
    }

    companion object {
        private val SINGLETON_MAP = HashTreePMap.singleton(Any(), Any())
    }
}
