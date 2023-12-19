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

import java.util.Spliterator
import java.util.function.Consumer
import java.util.function.Function
import java.util.function.Predicate
import java.util.stream.Stream
import org.apache.kafka.server.immutable.DelegationChecker
import org.apache.kafka.server.immutable.ImmutableSet
import org.junit.jupiter.api.Assertions.assertNotEquals
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import org.mockito.kotlin.eq
import org.mockito.kotlin.mock
import org.pcollections.HashTreePSet
import org.pcollections.MapPSet
import kotlin.test.assertEquals
import kotlin.test.assertSame

@Suppress("Deprecation")
class PCollectionsImmutableSetTest {

    @Test
    fun testEmptySet() {
        assertEquals(
            HashTreePSet.empty(),
            (ImmutableSet.empty<Any>() as PCollectionsImmutableSet<Any>).underlying()
        )
    }

    @Test
    fun testSingletonSet() {
        assertEquals(
            HashTreePSet.singleton(1),
            (ImmutableSet.singleton(1) as PCollectionsImmutableSet<*>).underlying()
        )
    }

    @Test
    fun testUnderlying() {
        assertSame(SINGLETON_SET, PCollectionsImmutableSet(SINGLETON_SET).underlying())
    }

    @Test
    fun testDelegationOfAdded() {
        PCollectionsHashSetWrapperDelegationChecker<Any?>()
            .defineMockConfigurationForFunctionInvocation(
                mockConfigurationFunction = { mock -> mock.plus(eq(this)) },
                mockFunctionReturnValue = SINGLETON_SET,
            )
            .defineWrapperFunctionInvocationAndMockReturnValueTransformation(
                wrapperFunctionApplier = { wrapper ->  wrapper.added(this) },
                expectedFunctionReturnValueTransformation = { it },
            )
            .expectWrapperToWrapMockFunctionReturnValue()
            .doFunctionDelegationCheck()
    }

    @Test
    fun testDelegationOfRemoved() {
        PCollectionsHashSetWrapperDelegationChecker<Any?>()
            .defineMockConfigurationForFunctionInvocation(
                mockConfigurationFunction = { mock -> mock.minus(eq(this)) },
                mockFunctionReturnValue = SINGLETON_SET,
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
        PCollectionsHashSetWrapperDelegationChecker<Any?>()
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
        PCollectionsHashSetWrapperDelegationChecker<Any?>()
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
    fun testDelegationOfContains(mockFunctionReturnValue: Boolean) {
        PCollectionsHashSetWrapperDelegationChecker<Any?>()
            .defineMockConfigurationForFunctionInvocation(
                mockConfigurationFunction = { mock -> mock.contains(eq(this)) },
                mockFunctionReturnValue = mockFunctionReturnValue,
            )
            .defineWrapperFunctionInvocationAndMockReturnValueTransformation(
                wrapperFunctionApplier = { wrapper -> wrapper.contains(this) },
                expectedFunctionReturnValueTransformation = { it },
            )
            .doFunctionDelegationCheck()
    }

    @Test
    fun testDelegationOfIterator() {
        PCollectionsHashSetWrapperDelegationChecker<Any?>()
            .defineMockConfigurationForFunctionInvocation(
                mockConfigurationFunction = { it.iterator() },
                mockFunctionReturnValue = mock<Iterator<Any>>(),
            )
            .defineWrapperFunctionInvocationAndMockReturnValueTransformation(
                wrapperFunctionApplier = { it.iterator() },
                expectedFunctionReturnValueTransformation = { it },
            )
            .doFunctionDelegationCheck()
    }

    @Test
    fun testDelegationOfForEach() {
        val mockConsumer = mock<Consumer<Any?>>()
        PCollectionsHashSetWrapperDelegationChecker<Any?>()
            .defineMockConfigurationForVoidMethodInvocation { mock -> mock.forEach(eq(mockConsumer)) }
            .defineWrapperVoidMethodInvocation { wrapper -> wrapper.forEach(mockConsumer) }
            .doVoidMethodDelegationCheck()
    }

    @Test
    @Disabled("Kotlin Migration - toArray is not supported in Kotlin")
    fun testDelegationOfToArray() {
        PCollectionsHashSetWrapperDelegationChecker<Any?>()
            .defineMockConfigurationForFunctionInvocation(
                mockConfigurationFunction = { it.toTypedArray() },
                mockFunctionReturnValue = arrayOfNulls<Any>(0),
            )
            .defineWrapperFunctionInvocationAndMockReturnValueTransformation(
                wrapperFunctionApplier = { it.toTypedArray() },
                expectedFunctionReturnValueTransformation = { it },
            )
            .doFunctionDelegationCheck()
    }

    @Test
    @Disabled("Kotlin Migration - toArray is not supported in Kotlin")
    fun testDelegationOfToArrayIntoGivenDestination() {
        val destinationArray = arrayOfNulls<Any?>(0)
        PCollectionsHashSetWrapperDelegationChecker<Any?>()
            .defineMockConfigurationForFunctionInvocation(
                mockConfigurationFunction = { mock -> mock.toArray(eq(destinationArray)) },
                mockFunctionReturnValue = arrayOfNulls<Any>(0),
            )
            .defineWrapperFunctionInvocationAndMockReturnValueTransformation(
                wrapperFunctionApplier = { wrapper -> wrapper.toTypedArray() },
                expectedFunctionReturnValueTransformation = { it },
            )
            .doFunctionDelegationCheck()
    }

    @Test
    fun testDelegationOfUnsupportedFunctionAdd() {
        PCollectionsHashSetWrapperDelegationChecker<Any?>()
            .defineMockConfigurationForUnsupportedFunction { mock -> mock.add(eq(this)) }
            .defineWrapperUnsupportedFunctionInvocation { wrapper -> wrapper.add(this) }
            .doUnsupportedFunctionDelegationCheck()
    }

    @Test
    fun testDelegationOfUnsupportedFunctionRemove() {
        PCollectionsHashSetWrapperDelegationChecker<Any?>()
            .defineMockConfigurationForUnsupportedFunction { mock -> mock.remove(eq(this)) }
            .defineWrapperUnsupportedFunctionInvocation { wrapper -> wrapper.remove(this) }
            .doUnsupportedFunctionDelegationCheck()
    }

    @ParameterizedTest
    @ValueSource(booleans = [true, false])
    fun testDelegationOfContainsAll(mockFunctionReturnValue: Boolean) {
        PCollectionsHashSetWrapperDelegationChecker<Any?>()
            .defineMockConfigurationForFunctionInvocation({ mock ->
                mock.containsAll(
                    eq(
                        emptyList<Any>()
                    )
                )
            }, mockFunctionReturnValue)
            .defineWrapperFunctionInvocationAndMockReturnValueTransformation(
                wrapperFunctionApplier = { wrapper -> wrapper.containsAll(emptyList()) },
                expectedFunctionReturnValueTransformation = { it },
            )
            .doFunctionDelegationCheck()
    }

    @Test
    fun testDelegationOfUnsupportedFunctionAddAll() {
        PCollectionsHashSetWrapperDelegationChecker<Any?>()
            .defineMockConfigurationForUnsupportedFunction { mock -> mock.addAll(eq(emptyList())) }
            .defineWrapperUnsupportedFunctionInvocation { wrapper -> wrapper.addAll(emptyList()) }
            .doUnsupportedFunctionDelegationCheck()
    }

    @Test
    fun testDelegationOfUnsupportedFunctionRetainAll() {
        PCollectionsHashSetWrapperDelegationChecker<Any?>()
            .defineMockConfigurationForUnsupportedFunction { mock -> mock.retainAll(eq(emptyList())) }
            .defineWrapperUnsupportedFunctionInvocation { wrapper -> wrapper.retainAll(emptyList()) }
            .doUnsupportedFunctionDelegationCheck()
    }

    @Test
    fun testDelegationOfUnsupportedFunctionRemoveAll() {
        PCollectionsHashSetWrapperDelegationChecker<Any?>()
            .defineMockConfigurationForUnsupportedFunction { mock -> mock.removeAll(eq(emptyList())) }
            .defineWrapperUnsupportedFunctionInvocation { wrapper -> wrapper.removeAll(emptyList()) }
            .doUnsupportedFunctionDelegationCheck()
    }

    @Test
    fun testDelegationOfUnsupportedFunctionRemoveIf() {
        val mockPredicate: Predicate<Any?> = mock()
        PCollectionsHashSetWrapperDelegationChecker<Any?>()
            .defineMockConfigurationForUnsupportedFunction { mock -> mock.removeIf(eq(mockPredicate)) }
            .defineWrapperUnsupportedFunctionInvocation { wrapper -> wrapper.removeIf(mockPredicate) }
            .doUnsupportedFunctionDelegationCheck()
    }

    @Test
    fun testDelegationOfUnsupportedFunctionClear() {
        PCollectionsHashSetWrapperDelegationChecker<Any?>()
            .defineMockConfigurationForVoidMethodInvocation { it.clear() }
            .defineWrapperVoidMethodInvocation { it.clear() }
            .doUnsupportedVoidFunctionDelegationCheck()
    }

    @Test
    fun testDelegationOfSpliterator() {
        PCollectionsHashSetWrapperDelegationChecker<Any?>()
            .defineMockConfigurationForFunctionInvocation(
                mockConfigurationFunction = { it.spliterator() },
                mockFunctionReturnValue = mock<Spliterator<*>>()
            )
            .defineWrapperFunctionInvocationAndMockReturnValueTransformation(
                wrapperFunctionApplier = { it.spliterator() },
                expectedFunctionReturnValueTransformation = { it }
            )
            .doFunctionDelegationCheck()
    }

    @Test
    fun testDelegationOfStream() {
        PCollectionsHashSetWrapperDelegationChecker<Any?>()
            .defineMockConfigurationForFunctionInvocation(
                mockConfigurationFunction = { it.stream() },
                mockFunctionReturnValue = mock<Stream<*>>(),
            )
            .defineWrapperFunctionInvocationAndMockReturnValueTransformation(
                wrapperFunctionApplier = { it.stream() },
                expectedFunctionReturnValueTransformation = { it },
            )
            .doFunctionDelegationCheck()
    }

    @Test
    fun testDelegationOfParallelStream() {
        PCollectionsHashSetWrapperDelegationChecker<Any?>()
            .defineMockConfigurationForFunctionInvocation(
                mockConfigurationFunction = { it.parallelStream() },
                mockFunctionReturnValue = mock<Stream<*>>(),
            )
            .defineWrapperFunctionInvocationAndMockReturnValueTransformation(
                wrapperFunctionApplier = { it.parallelStream() },
                expectedFunctionReturnValueTransformation = { it },
            )
            .doFunctionDelegationCheck()
    }

    @Test
    fun testEquals() {
        val mock: MapPSet<Any> = mock()
        assertEquals(PCollectionsImmutableSet(mock), PCollectionsImmutableSet(mock))
        val someOtherMock: MapPSet<Any> = mock()
        assertNotEquals(PCollectionsImmutableSet(mock), PCollectionsImmutableSet(someOtherMock))
    }

    @Test
    fun testHashCode() {
        val mock: MapPSet<Any> = mock()
        assertEquals(mock.hashCode(), PCollectionsImmutableSet(mock).hashCode())
        val someOtherMock: MapPSet<Any> = mock()
        assertNotEquals(mock.hashCode(), PCollectionsImmutableSet(someOtherMock).hashCode())
    }

    @ParameterizedTest
    @ValueSource(strings = ["a", "b"])
    fun testDelegationOfToString(mockFunctionReturnValue: String) {
        PCollectionsHashSetWrapperDelegationChecker<Any?>()
            .defineMockConfigurationForFunctionInvocation(
                mockConfigurationFunction = { it.toString() },
                mockFunctionReturnValue = mockFunctionReturnValue,
            )
            .defineWrapperFunctionInvocationAndMockReturnValueTransformation(
                wrapperFunctionApplier = { it.toString() },
                expectedFunctionReturnValueTransformation = { "PCollectionsImmutableSet{underlying=$it}" },
            )
            .doFunctionDelegationCheck()
    }

    private class PCollectionsHashSetWrapperDelegationChecker<R> :
        DelegationChecker<MapPSet<Any>, PCollectionsImmutableSet<Any>, R>(
            mock = mock<MapPSet<Any>>(),
            wrapperCreator = { underlying -> PCollectionsImmutableSet(underlying) },
        ) {

        override fun unwrap(wrapper: PCollectionsImmutableSet<Any>): MapPSet<Any> = wrapper.underlying()
    }

    companion object {
        private val SINGLETON_SET = HashTreePSet.singleton(Any())
    }
}
