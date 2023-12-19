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
import org.apache.kafka.server.immutable.ImmutableSet
import org.pcollections.HashTreePSet
import org.pcollections.MapPSet

@Suppress("Deprecation")
class PCollectionsImmutableSet<E>(internal val underlying: MapPSet<E>) : ImmutableSet<E> {

    override fun added(e: E): ImmutableSet<E> = PCollectionsImmutableSet(underlying.plus(e))

    override fun removed(e: E): ImmutableSet<E> = PCollectionsImmutableSet(underlying.minus(e))

    override val size: Int
        get() = underlying.size

    override fun isEmpty(): Boolean = underlying.isEmpty()

    override operator fun contains(element: E): Boolean = underlying.contains(element)

    override fun iterator(): MutableIterator<E> = underlying.iterator()

    override fun forEach(action: Consumer<in E>) = underlying.forEach(action)

    // Kotlin Migration - toArray override does not exist in kotlin
    // override fun toArray(): Array<E> = underlying.toTypedArray()

    // override fun <T> toArray(a: Array<T>?): Array<T> = underlying.toArray(a)

    override fun add(element: E): Boolean {
        // will throw UnsupportedOperationException; delegate anyway for testability
        return underlying.add(element)
    }

    // will throw UnsupportedOperationException; delegate anyway for testability
    override fun remove(element: E): Boolean = underlying.remove(element)

    override fun containsAll(elements: Collection<E>): Boolean = underlying.containsAll(elements)

    // will throw UnsupportedOperationException; delegate anyway for testability
    override fun addAll(elements: Collection<E>): Boolean = underlying.addAll(elements)

    // will throw UnsupportedOperationException; delegate anyway for testability
    override fun retainAll(elements: Collection<E>): Boolean = underlying.retainAll(elements)

    // will throw UnsupportedOperationException; delegate anyway for testability
    override fun removeAll(elements: Collection<E>): Boolean = underlying.removeAll(elements)

    // will throw UnsupportedOperationException; delegate anyway for testability
    override fun removeIf(filter: Predicate<in E>): Boolean = underlying.removeIf(filter)

    // will throw UnsupportedOperationException; delegate anyway for testability
    override fun clear() = underlying.clear()

    override fun spliterator(): Spliterator<E> = underlying.spliterator()

    override fun stream(): Stream<E> = underlying.stream()

    override fun parallelStream(): Stream<E> = underlying.parallelStream()

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other == null || javaClass != other.javaClass) return false
        val that = other as PCollectionsImmutableSet<*>
        return underlying == that.underlying
    }

    override fun hashCode(): Int = underlying.hashCode()

    override fun toString(): String = "PCollectionsImmutableSet{underlying=$underlying}"

    // package-private for testing
    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("underlying"),
    )
    internal fun underlying(): MapPSet<E> = underlying

    companion object {

        /**
         * @param E the element type
         * @return a wrapped hash-based persistent set that is empty
         */
        fun <E> empty(): PCollectionsImmutableSet<E> = PCollectionsImmutableSet(HashTreePSet.empty())

        /**
         * @param e the element
         * @param E the element type
         * @return a wrapped hash-based persistent set that has a single element
         */
        fun <E> singleton(e: E): PCollectionsImmutableSet<E> = PCollectionsImmutableSet(HashTreePSet.singleton(e))
    }
}
