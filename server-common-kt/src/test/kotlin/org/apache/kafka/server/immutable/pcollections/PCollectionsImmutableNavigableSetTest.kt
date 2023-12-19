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
import java.util.function.Predicate
import java.util.stream.Stream
import org.apache.kafka.server.immutable.DelegationChecker
import org.apache.kafka.server.immutable.ImmutableNavigableSet
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import org.mockito.kotlin.eq
import org.mockito.kotlin.mock
import org.pcollections.HashTreePSet
import org.pcollections.MapPSet
import org.pcollections.TreePSet
import kotlin.random.Random
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertNotEquals
import kotlin.test.assertSame

@Suppress("Deprecation")
class PCollectionsImmutableNavigableSetTest {
    
    @Test
    fun testEmptySet() {
        assertEquals(
            HashTreePSet.empty<Int>() as Set<*>,
            (ImmutableNavigableSet.empty<Int>() as PCollectionsImmutableNavigableSet<Int>).underlying(),
        )
    }

    @Test
    fun testSingletonSet() {
        assertEquals(
            HashTreePSet.singleton(1) as Set<*>,
            (ImmutableNavigableSet.singleton(1) as PCollectionsImmutableNavigableSet<Int>).underlying(),
        )
    }

    @Test
    fun testUnderlying() {
        assertSame(SINGLETON_SET, PCollectionsImmutableNavigableSet(SINGLETON_SET).underlying())
    }

    @Test
    fun testDelegationOfAdded() {
        PCollectionsTreeSetWrapperDelegationChecker<Any?>()
            .defineMockConfigurationForFunctionInvocation(
                mockConfigurationFunction = { mock -> mock.plus(eq<Int>(SINGLETON_SET.first())) },
                mockFunctionReturnValue = SINGLETON_SET,
            )
            .defineWrapperFunctionInvocationAndMockReturnValueTransformation(
                wrapperFunctionApplier = { wrapper -> wrapper.added(SINGLETON_SET.first()) },
                expectedFunctionReturnValueTransformation = { it },
            )
            .expectWrapperToWrapMockFunctionReturnValue()
            .doFunctionDelegationCheck()
    }

    @Test
    fun testDelegationOfRemoved() {
        PCollectionsTreeSetWrapperDelegationChecker<Any?>()
            .defineMockConfigurationForFunctionInvocation(
                mockConfigurationFunction = { mock -> mock.minus(eq(10)) },
                mockFunctionReturnValue = SINGLETON_SET,
            )
            .defineWrapperFunctionInvocationAndMockReturnValueTransformation(
                wrapperFunctionApplier = { wrapper -> wrapper.removed(10) },
                expectedFunctionReturnValueTransformation = { it },
            )
            .expectWrapperToWrapMockFunctionReturnValue()
            .doFunctionDelegationCheck()
    }

    @ParameterizedTest
    @ValueSource(ints = [9, 4])
    fun testDelegationOfLower(mockFunctionReturnValue: Int) {
        PCollectionsTreeSetWrapperDelegationChecker<Any?>()
            .defineMockConfigurationForFunctionInvocation(
                mockConfigurationFunction = { mock -> mock.lower(eq(10)) },
                mockFunctionReturnValue = mockFunctionReturnValue,
            )
            .defineWrapperFunctionInvocationAndMockReturnValueTransformation(
                wrapperFunctionApplier = { wrapper -> wrapper.lower(10) },
                expectedFunctionReturnValueTransformation = { it },
            )
            .doFunctionDelegationCheck()
    }

    @ParameterizedTest
    @ValueSource(ints = [9, 10])
    fun testDelegationOfFloor(mockFunctionReturnValue: Int) {
        PCollectionsTreeSetWrapperDelegationChecker<Any?>()
            .defineMockConfigurationForFunctionInvocation(
                mockConfigurationFunction = { mock -> mock.floor(eq(10)) },
                mockFunctionReturnValue = mockFunctionReturnValue,
            )
            .defineWrapperFunctionInvocationAndMockReturnValueTransformation(
                wrapperFunctionApplier = { wrapper -> wrapper.floor(10) },
                expectedFunctionReturnValueTransformation = { it },
            )
            .doFunctionDelegationCheck()
    }

    @ParameterizedTest
    @ValueSource(ints = [11, 10])
    fun testDelegationOfCeiling(mockFunctionReturnValue: Int) {
        PCollectionsTreeSetWrapperDelegationChecker<Any?>()
            .defineMockConfigurationForFunctionInvocation(
                mockConfigurationFunction = { mock -> mock.ceiling(eq(10)) },
                mockFunctionReturnValue = mockFunctionReturnValue,
            )
            .defineWrapperFunctionInvocationAndMockReturnValueTransformation(
                wrapperFunctionApplier = { wrapper -> wrapper.ceiling(10) },
                expectedFunctionReturnValueTransformation = { it },
            )
            .doFunctionDelegationCheck()
    }

    @ParameterizedTest
    @ValueSource(ints = [12, 13])
    fun testDelegationOfHigher(mockFunctionReturnValue: Int) {
        PCollectionsTreeSetWrapperDelegationChecker<Any?>()
            .defineMockConfigurationForFunctionInvocation(
                mockConfigurationFunction = { mock -> mock.higher(eq(10)) },
                mockFunctionReturnValue = mockFunctionReturnValue,
            )
            .defineWrapperFunctionInvocationAndMockReturnValueTransformation(
                wrapperFunctionApplier = { wrapper -> wrapper.higher(10) },
                expectedFunctionReturnValueTransformation = { it },
            )
            .doFunctionDelegationCheck()
    }

    @Test
    fun testDelegationOfUnsupportedFunctionPollFirst() {
        PCollectionsTreeSetWrapperDelegationChecker<Any?>()
            .defineMockConfigurationForUnsupportedFunction { it.pollFirst() }
            .defineWrapperUnsupportedFunctionInvocation { it.pollFirst() }
            .doUnsupportedFunctionDelegationCheck()
    }

    @Test
    fun testDelegationOfUnsupportedFunctionPollLast() {
        PCollectionsTreeSetWrapperDelegationChecker<Any?>()
            .defineMockConfigurationForUnsupportedFunction { it.pollLast() }
            .defineWrapperUnsupportedFunctionInvocation { it.pollLast() }
            .doUnsupportedFunctionDelegationCheck()
    }

    @Test
    fun testDelegationOfDescendingSet() {
        val testSet = TreePSet.from(mutableListOf(2, 3, 4))
        PCollectionsTreeSetWrapperDelegationChecker<Any?>()
            .defineMockConfigurationForFunctionInvocation(
                mockConfigurationFunction = { it.descendingSet() },
                mockFunctionReturnValue = testSet.descendingSet(),
            )
            .defineWrapperFunctionInvocationAndMockReturnValueTransformation(
                wrapperFunctionApplier = { it.descendingSet() },
                expectedFunctionReturnValueTransformation = { it },
            )
            .expectWrapperToWrapMockFunctionReturnValue()
            .doFunctionDelegationCheck()
    }

    @Test
    fun testDelegationOfDescendingIterator() {
        val testSet = TreePSet.from(mutableListOf(2, 3, 4))
        PCollectionsTreeSetWrapperDelegationChecker<Any?>()
            .defineMockConfigurationForFunctionInvocation(
                mockConfigurationFunction = { it.descendingIterator() },
                mockFunctionReturnValue = testSet.descendingIterator(),
            )
            .defineWrapperFunctionInvocationAndMockReturnValueTransformation(
                wrapperFunctionApplier = { it.descendingIterator() },
                expectedFunctionReturnValueTransformation = { it },
            )
            .doFunctionDelegationCheck()
    }

    @Test
    fun testDelegationOfSubSetWithFromAndToElements() {
        PCollectionsTreeSetWrapperDelegationChecker<Any?>()
            .defineMockConfigurationForFunctionInvocation(
                mockConfigurationFunction = { mock ->
                    mock.subSet(eq(10), eq(true), eq(30), eq(false))
                },
                mockFunctionReturnValue = TreePSet.singleton(15),
            )
            .defineWrapperFunctionInvocationAndMockReturnValueTransformation(
                wrapperFunctionApplier = { wrapper ->
                    wrapper.subSet(10, true, 30, false)
                },
                expectedFunctionReturnValueTransformation = { it },
            )
            .expectWrapperToWrapMockFunctionReturnValue()
            .doFunctionDelegationCheck()
    }

    @Test
    fun testDelegationOfHeadSetInclusive() {
        PCollectionsTreeSetWrapperDelegationChecker<Any?>()
            .defineMockConfigurationForFunctionInvocation(
                mockConfigurationFunction = { mock -> mock.headSet(eq(15), eq(true)) },
                mockFunctionReturnValue = TreePSet.singleton(13),
            )
            .defineWrapperFunctionInvocationAndMockReturnValueTransformation(
                wrapperFunctionApplier = { wrapper -> wrapper.headSet( 15, true) },
                expectedFunctionReturnValueTransformation = { it }
            )
            .expectWrapperToWrapMockFunctionReturnValue()
            .doFunctionDelegationCheck()
    }

    @Test
    fun testDelegationOfTailSetInclusive() {
        PCollectionsTreeSetWrapperDelegationChecker<Any?>()
            .defineMockConfigurationForFunctionInvocation(
                mockConfigurationFunction = { mock -> mock.tailSet(eq(15), eq(true)) },
                mockFunctionReturnValue = TreePSet.singleton(15),
            )
            .defineWrapperFunctionInvocationAndMockReturnValueTransformation(
                wrapperFunctionApplier = { wrapper -> wrapper.tailSet(15, true) },
                expectedFunctionReturnValueTransformation = { it },
            )
            .expectWrapperToWrapMockFunctionReturnValue()
            .doFunctionDelegationCheck()
    }

    @Test
    fun testDelegationOfComparator() {
        val testSet = TreePSet.from(mutableListOf(3, 4, 5))
        PCollectionsTreeSetWrapperDelegationChecker<Any?>()
            .defineMockConfigurationForFunctionInvocation(
                mockConfigurationFunction = { it.comparator() },
                mockFunctionReturnValue = testSet.comparator(),
            )
            .defineWrapperFunctionInvocationAndMockReturnValueTransformation(
                wrapperFunctionApplier = { it.comparator() },
                expectedFunctionReturnValueTransformation = { it },
            )
            .doFunctionDelegationCheck()
    }

    @Test
    fun testDelegationOfSubSetWithFromElement() {
        PCollectionsTreeSetWrapperDelegationChecker<Any?>()
            .defineMockConfigurationForFunctionInvocation(
                mockConfigurationFunction = { mock -> mock.subSet(eq(15), eq(true)) },
                mockFunctionReturnValue = TreePSet.singleton(13),
            )
            .defineWrapperFunctionInvocationAndMockReturnValueTransformation(
                wrapperFunctionApplier = { wrapper -> wrapper.subSet(15, true) },
                expectedFunctionReturnValueTransformation = { it },
            )
            .expectWrapperToWrapMockFunctionReturnValue()
            .doFunctionDelegationCheck()
    }

    @Test
    fun testDelegationOfHeadSet() {
        PCollectionsTreeSetWrapperDelegationChecker<Any?>()
            .defineMockConfigurationForFunctionInvocation(
                mockConfigurationFunction = { mock -> mock.headSet(eq(15)) },
                mockFunctionReturnValue = TreePSet.singleton(13),
            )
            .defineWrapperFunctionInvocationAndMockReturnValueTransformation(
                wrapperFunctionApplier = { wrapper -> wrapper.headSet(15) },
                expectedFunctionReturnValueTransformation = { it },
            )
            .expectWrapperToWrapMockFunctionReturnValue()
            .doFunctionDelegationCheck()
    }

    @Test
    fun testDelegationOfTailSet() {
        PCollectionsTreeSetWrapperDelegationChecker<Any?>()
            .defineMockConfigurationForFunctionInvocation(
                mockConfigurationFunction = { mock -> mock.tailSet(eq(15)) },
                mockFunctionReturnValue = TreePSet.singleton(13),
            )
            .defineWrapperFunctionInvocationAndMockReturnValueTransformation(
                wrapperFunctionApplier = { wrapper -> wrapper.tailSet(15) },
                expectedFunctionReturnValueTransformation = { it },
            )
            .expectWrapperToWrapMockFunctionReturnValue()
            .doFunctionDelegationCheck()
    }

    @ParameterizedTest
    @ValueSource(ints = [1, 2])
    fun testDelegationOfFirst() {
        PCollectionsTreeSetWrapperDelegationChecker<Any?>()
            .defineMockConfigurationForFunctionInvocation(
                mockConfigurationFunction = { it.first() },
                mockFunctionReturnValue = 13,
            )
            .defineWrapperFunctionInvocationAndMockReturnValueTransformation(
                wrapperFunctionApplier = { it.first() },
                expectedFunctionReturnValueTransformation = { it },
            )
            .doFunctionDelegationCheck()
    }

    @ParameterizedTest
    @ValueSource(ints = [1, 2])
    fun testDelegationOfLast() {
        PCollectionsTreeSetWrapperDelegationChecker<Any?>()
            .defineMockConfigurationForFunctionInvocation(
                mockConfigurationFunction = { it.last() },
                mockFunctionReturnValue = 13,
            )
            .defineWrapperFunctionInvocationAndMockReturnValueTransformation(
                wrapperFunctionApplier = { it.last() },
                expectedFunctionReturnValueTransformation = { it },
            )
            .doFunctionDelegationCheck()
    }

    @ParameterizedTest
    @ValueSource(ints = [1, 2])
    fun testDelegationOfSize(mockFunctionReturnValue: Int) {
        PCollectionsTreeSetWrapperDelegationChecker<Any?>()
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
        PCollectionsTreeSetWrapperDelegationChecker<Any?>()
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
        PCollectionsTreeSetWrapperDelegationChecker<Any?>()
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
        PCollectionsTreeSetWrapperDelegationChecker<Any?>()
            .defineMockConfigurationForFunctionInvocation(
                mockConfigurationFunction = { it.iterator() },
                mockFunctionReturnValue = mock<MutableIterator<*>>(),
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
        PCollectionsTreeSetWrapperDelegationChecker<Any?>()
            .defineMockConfigurationForVoidMethodInvocation { mock -> mock.forEach(eq(mockConsumer)) }
            .defineWrapperVoidMethodInvocation { wrapper -> wrapper.forEach(mockConsumer) }
            .doVoidMethodDelegationCheck()
    }

    @Test
    @Disabled("Kotlin Migration - toArray is not supported in Kotlin.")
    fun testDelegationOfToArray() {
        PCollectionsTreeSetWrapperDelegationChecker<Any?>()
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
    @Disabled("Kotlin Migration - toArray not migrated.")
    fun testDelegationOfToArrayIntoGivenDestination() {
//        val destinationArray = arrayOfNulls<Any>(0)
//        PCollectionsTreeSetWrapperDelegationChecker<Any?>()
//            .defineMockConfigurationForFunctionInvocation(
//                mockConfigurationFunction = { mock -> mock.toArray(eq(destinationArray)) },
//                mockFunctionReturnValue = arrayOfNulls<Any>(0),
//            )
//            .defineWrapperFunctionInvocationAndMockReturnValueTransformation(
//                wrapperFunctionApplier = { wrapper -> wrapper.toArray(destinationArray) },
//                expectedFunctionReturnValueTransformation = { it },
//            )
//            .doFunctionDelegationCheck()
    }

    @Test
    fun testDelegationOfUnsupportedFunctionAdd() {
        PCollectionsTreeSetWrapperDelegationChecker<Any?>()
            .defineMockConfigurationForUnsupportedFunction { mock -> mock.add(eq(this)) }
            .defineWrapperUnsupportedFunctionInvocation { wrapper -> wrapper.add(this) }
            .doUnsupportedFunctionDelegationCheck()
    }

    @Test
    fun testDelegationOfUnsupportedFunctionRemove() {
        PCollectionsTreeSetWrapperDelegationChecker<Any?>()
            .defineMockConfigurationForUnsupportedFunction { mock -> mock.remove(eq(this)) }
            .defineWrapperUnsupportedFunctionInvocation { wrapper -> wrapper.remove(this) }
            .doUnsupportedFunctionDelegationCheck()
    }

    @ParameterizedTest
    @ValueSource(booleans = [true, false])
    fun testDelegationOfContainsAll(mockFunctionReturnValue: Boolean) {
        PCollectionsTreeSetWrapperDelegationChecker<Any?>()
            .defineMockConfigurationForFunctionInvocation(
                mockConfigurationFunction = { mock -> mock.containsAll(eq(emptyList<Any>())) },
                mockFunctionReturnValue = mockFunctionReturnValue,
            )
            .defineWrapperFunctionInvocationAndMockReturnValueTransformation(
                wrapperFunctionApplier = { wrapper -> wrapper.containsAll(emptyList<Any>()) },
                expectedFunctionReturnValueTransformation = { it },
            )
            .doFunctionDelegationCheck()
    }

    @Test
    fun testDelegationOfUnsupportedFunctionAddAll() {
        PCollectionsTreeSetWrapperDelegationChecker<Any?>()
            .defineMockConfigurationForUnsupportedFunction { mock -> mock.addAll(eq(emptyList<Any>())) }
            .defineWrapperUnsupportedFunctionInvocation { wrapper -> wrapper.addAll(emptyList<Any>()) }
            .doUnsupportedFunctionDelegationCheck()
    }

    @Test
    fun testDelegationOfUnsupportedFunctionRetainAll() {
        PCollectionsTreeSetWrapperDelegationChecker<Any?>()
            .defineMockConfigurationForUnsupportedFunction { mock -> mock.retainAll(eq(emptyList<Any>())) }
            .defineWrapperUnsupportedFunctionInvocation { wrapper -> wrapper.retainAll(emptyList<Any>()) }
            .doUnsupportedFunctionDelegationCheck()
    }

    @Test
    fun testDelegationOfUnsupportedFunctionRemoveAll() {
        PCollectionsTreeSetWrapperDelegationChecker<Any?>()
            .defineMockConfigurationForUnsupportedFunction { mock-> mock.removeAll(eq(emptyList<Any>())) }
            .defineWrapperUnsupportedFunctionInvocation { wrapper -> wrapper.removeAll(emptyList<Any>()) }
            .doUnsupportedFunctionDelegationCheck()
    }

    @Test
    fun testDelegationOfUnsupportedFunctionRemoveIf() {
        val mockPredicate: Predicate<Any?> = mock()
        PCollectionsTreeSetWrapperDelegationChecker<Any?>()
            .defineMockConfigurationForUnsupportedFunction { mock -> mock.removeIf(eq(mockPredicate)) }
            .defineWrapperUnsupportedFunctionInvocation { wrapper -> wrapper.removeIf(mockPredicate) }
            .doUnsupportedFunctionDelegationCheck()
    }

    @Test
    fun testDelegationOfUnsupportedFunctionClear() {
        PCollectionsTreeSetWrapperDelegationChecker<Any?>()
            .defineMockConfigurationForVoidMethodInvocation { it.clear() }
            .defineWrapperVoidMethodInvocation { it.clear() }
            .doUnsupportedVoidFunctionDelegationCheck()
    }

    @Test
    fun testDelegationOfSpliterator() {
        PCollectionsTreeSetWrapperDelegationChecker<Any?>()
            .defineMockConfigurationForFunctionInvocation(
                mockConfigurationFunction = { it.spliterator() },
                mockFunctionReturnValue = mock<Spliterator<*>>(),
            )
            .defineWrapperFunctionInvocationAndMockReturnValueTransformation(
                wrapperFunctionApplier = { it.spliterator() },
                expectedFunctionReturnValueTransformation = { it },
            )
            .doFunctionDelegationCheck()
    }

    @Test
    fun testDelegationOfStream() {
        PCollectionsTreeSetWrapperDelegationChecker<Any?>()
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
        PCollectionsTreeSetWrapperDelegationChecker<Any?>()
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
        val mock: TreePSet<Any> = mock()
        assertEquals(PCollectionsImmutableNavigableSet(mock), PCollectionsImmutableNavigableSet(mock))
        val someOtherMock: TreePSet<Any> = mock()
        assertNotEquals(
            PCollectionsImmutableNavigableSet(mock),
            PCollectionsImmutableNavigableSet(someOtherMock)
        )
    }

    @Test
    fun testHashCode() {
        val mock: TreePSet<Any> = mock()
        assertEquals(mock.hashCode(), PCollectionsImmutableNavigableSet(mock).hashCode())
        val someOtherMock: TreePSet<Any> = mock()
        assertNotEquals(mock.hashCode(), PCollectionsImmutableNavigableSet(someOtherMock).hashCode())
    }

    @ParameterizedTest
    @ValueSource(strings = ["a", "b"])
    fun testDelegationOfToString(mockFunctionReturnValue: String) {
        PCollectionsTreeSetWrapperDelegationChecker<Any?>()
            .defineMockConfigurationForFunctionInvocation(
                mockConfigurationFunction = { it.toString() },
                mockFunctionReturnValue = mockFunctionReturnValue
            )
            .defineWrapperFunctionInvocationAndMockReturnValueTransformation(
                wrapperFunctionApplier = { it.toString() },
                expectedFunctionReturnValueTransformation = { "PCollectionsImmutableNavigableSet{underlying=$it}" },
            )
            .doFunctionDelegationCheck()
    }

    private class PCollectionsTreeSetWrapperDelegationChecker<R> :
        DelegationChecker<TreePSet<Any?>, PCollectionsImmutableNavigableSet<Any?>, R>(
            mock<TreePSet<Any?>>(), { underlying -> PCollectionsImmutableNavigableSet(underlying) },
        ) {

        override fun unwrap(
            wrapper: PCollectionsImmutableNavigableSet<Any?>,
        ): TreePSet<Any?> = wrapper.underlying()
    }

    companion object {
        private val SINGLETON_SET = TreePSet.singleton(Random.nextInt())
    }
}
