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
import java.util.function.IntFunction
import java.util.function.Predicate
import java.util.stream.Stream
import org.apache.kafka.server.immutable.ImmutableNavigableSet
import org.pcollections.TreePSet

@Suppress("Deprecation")
class PCollectionsImmutableNavigableSet<E>(internal val underlying: TreePSet<E>) : ImmutableNavigableSet<E> {

    override fun added(e: E): PCollectionsImmutableNavigableSet<E> =
        PCollectionsImmutableNavigableSet(underlying.plus(e))

    override fun removed(e: E): PCollectionsImmutableNavigableSet<E> =
        PCollectionsImmutableNavigableSet(underlying.minus(e))

    override fun lower(e: E): E = underlying.lower(e)

    override fun floor(e: E): E = underlying.floor(e)

    override fun ceiling(e: E): E = underlying.ceiling(e)

    override fun higher(e: E): E = underlying.higher(e)

    // will throw UnsupportedOperationException
    override fun pollFirst(): E = underlying.pollFirst()

    // will throw UnsupportedOperationException
    override fun pollLast(): E = underlying.pollLast()

    override fun descendingSet(): PCollectionsImmutableNavigableSet<E> =
        PCollectionsImmutableNavigableSet(underlying.descendingSet())

    override fun descendingIterator(): Iterator<E> = underlying.descendingIterator()

    override fun subSet(
        fromElement: E,
        fromInclusive: Boolean,
        toElement: E,
        toInclusive: Boolean,
    ): PCollectionsImmutableNavigableSet<E> = PCollectionsImmutableNavigableSet(
        underlying.subSet(
            fromElement,
            fromInclusive,
            toElement,
            toInclusive
        )
    )

    override fun headSet(toElement: E, inclusive: Boolean): PCollectionsImmutableNavigableSet<E> =
        PCollectionsImmutableNavigableSet(underlying.headSet(toElement, inclusive))

    override fun tailSet(fromElement: E, inclusive: Boolean): PCollectionsImmutableNavigableSet<E> =
        PCollectionsImmutableNavigableSet(underlying.tailSet(fromElement, inclusive))

    override fun comparator(): Comparator<in E> = underlying.comparator()

    override fun subSet(fromElement: E, toElement: E): PCollectionsImmutableNavigableSet<E> =
        PCollectionsImmutableNavigableSet(underlying.subSet(fromElement, toElement))

    override fun headSet(toElement: E): PCollectionsImmutableNavigableSet<E> =
        PCollectionsImmutableNavigableSet(underlying.headSet(toElement))

    override fun tailSet(fromElement: E): PCollectionsImmutableNavigableSet<E> =
        PCollectionsImmutableNavigableSet(underlying.tailSet(fromElement))

    override fun first(): E = underlying.first()

    override fun last(): E = underlying.last()

    override val size: Int
        get() = underlying.size

    override fun isEmpty(): Boolean = underlying.isEmpty()

    override operator fun contains(element: E): Boolean = underlying.contains(element)

    override fun iterator(): MutableIterator<E> = underlying.iterator()

    override fun forEach(action: Consumer<in E>) = underlying.forEach(action)

    // Kotlin Migration - toArray override does not exist in kotlin
    // override fun toArray(): Array<E> = underlying.toArray()

    // override fun <T> toArray(a: Array<T>?): Array<T> = underlying.toArray(a)

    // will throw UnsupportedOperationException
    override fun add(element: E): Boolean = underlying.add(element)

    // will throw UnsupportedOperationException
    override fun remove(element: E): Boolean = underlying.remove(element)

    override fun containsAll(elements: Collection<E>): Boolean = underlying.containsAll(elements)

    // will throw UnsupportedOperationException
    override fun addAll(elements: Collection<E>): Boolean = underlying.addAll(elements)

    // will throw UnsupportedOperationException
    override fun retainAll(elements: Collection<E>): Boolean = underlying.retainAll(elements)

    // will throw UnsupportedOperationException
    override fun removeAll(elements: Collection<E>): Boolean = underlying.removeAll(elements)

    // will throw UnsupportedOperationException
    override fun removeIf(filter: Predicate<in E>): Boolean = underlying.removeIf(filter)

    // will throw UnsupportedOperationException
    override fun clear() = underlying.clear()

    override fun spliterator(): Spliterator<E> = underlying.spliterator()

    override fun stream(): Stream<E> = underlying.stream()

    override fun parallelStream(): Stream<E> = underlying.parallelStream()

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other == null || javaClass != other.javaClass) return false
        val that = other as PCollectionsImmutableNavigableSet<*>
        return underlying == that.underlying
    }

    override fun hashCode(): Int = underlying.hashCode()

    override fun toString(): String = "PCollectionsImmutableNavigableSet{underlying=$underlying}"

    // package-private for testing
    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("underlying"),
    )
    internal fun underlying(): TreePSet<E> = underlying

    companion object {

        /**
         * @param E the element type
         * @return a wrapped tree-based persistent navigable set that is empty
         */
        fun <E : Comparable<E>> empty(): PCollectionsImmutableNavigableSet<E> =
            PCollectionsImmutableNavigableSet(TreePSet.empty())

        /**
         * @param e the element
         * @param E the element type
         * @return a wrapped tree-based persistent set that is empty
         */
        fun <E : Comparable<E>> singleton(e: E): PCollectionsImmutableNavigableSet<E> =
            PCollectionsImmutableNavigableSet(TreePSet.singleton(e))
    }
}
