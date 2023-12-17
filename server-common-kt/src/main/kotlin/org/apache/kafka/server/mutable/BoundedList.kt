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

package org.apache.kafka.server.mutable

import java.util.function.IntFunction

/**
 * A list which cannot grow beyond a certain length. If the maximum length would be exceeded by an
 * operation, the operation throws a BoundedListTooLongException exception rather than completing.
 * For simplicity, mutation through iterators or sublists is not allowed.
 *
 * @param E the element type
 */
class BoundedList<E>(maxLength: Int, underlying: MutableList<E>) : MutableList<E> {

    private val maxLength: Int

    private val underlying: MutableList<E>

    init {
        require(maxLength > 0) { "Invalid non-positive maxLength of $maxLength" }
        this.maxLength = maxLength
        if (underlying.size > maxLength) throw BoundedListTooLongException(
            "Cannot wrap list, because it is longer than the maximum length $maxLength"
        )
        this.underlying = underlying
    }

    override val size: Int
        get() = underlying.size

    override fun isEmpty(): Boolean = underlying.isEmpty()

    override operator fun contains(element: E): Boolean = underlying.contains(element)

    override fun iterator(): MutableIterator<E> = underlying.iterator()

    override fun <T> toArray(generator: IntFunction<Array<T>>?): Array<T> = underlying.toArray(generator)

    // Kotlin Migration - toArray does not exist in Kotlin
    // override fun toArray(): Array<Any> = underlying.toTypedArray()

    // override fun <T> toArray(a: Array<T>): Array<T> = underlying.toArray(a)

    override fun add(element: E): Boolean {
        if (underlying.size >= maxLength) throw BoundedListTooLongException(
            "Cannot add another element to the list because it would exceed the maximum length of $maxLength"
        )
        return underlying.add(element)
    }

    override fun remove(element: E): Boolean = underlying.remove(element)

    override fun containsAll(elements: Collection<E>): Boolean = underlying.containsAll(elements)

    override fun addAll(elements: Collection<E>): Boolean {
        val numToAdd = elements.size
        if (underlying.size > maxLength - numToAdd) throw BoundedListTooLongException(
            "Cannot add another $numToAdd element(s) to the list because it would exceed the maximum length of $maxLength"
        )
        return underlying.addAll(elements)
    }

    override fun addAll(index: Int, elements: Collection<E>): Boolean {
        val numToAdd = elements.size
        if (underlying.size > maxLength - numToAdd) throw BoundedListTooLongException(
            "Cannot add another $numToAdd element(s) to the list because it would exceed the maximum length of $maxLength"
        )
        return underlying.addAll(index, elements)
    }

    override fun removeAll(elements: Collection<E>): Boolean = underlying.removeAll(elements)

    override fun retainAll(elements: Collection<E>): Boolean = underlying.retainAll(elements)

    override fun clear() = underlying.clear()

    override fun get(index: Int): E = underlying[index]

    override fun set(index: Int, element: E): E = underlying.set(index, element)

    override fun add(index: Int, element: E) {
        if (underlying.size >= maxLength) throw BoundedListTooLongException(
            "Cannot add another element to the list because it would exceed the maximum length of $maxLength"
        )
        underlying.add(index, element)
    }

    override fun removeAt(index: Int): E = underlying.removeAt(index)

    override fun indexOf(element: E): Int = underlying.indexOf(element)

    override fun lastIndexOf(element: E): Int = underlying.lastIndexOf(element)

    override fun listIterator(): MutableListIterator<E> = underlying.listIterator()

    override fun listIterator(index: Int): MutableListIterator<E> = underlying.listIterator(index)

    override fun subList(fromIndex: Int, toIndex: Int): MutableList<E> = underlying.subList(fromIndex, toIndex)

    override fun equals(other: Any?): Boolean {
        return underlying == other
    }

    override fun hashCode(): Int = underlying.hashCode()

    override fun toString(): String = underlying.toString()

    companion object {

        fun <E> newArrayBacked(maxLength: Int): BoundedList<E> = BoundedList(maxLength, ArrayList())

        fun <E> newArrayBacked(maxLength: Int, initialCapacity: Int): BoundedList<E> =
            BoundedList(maxLength, ArrayList(initialCapacity))
    }
}
