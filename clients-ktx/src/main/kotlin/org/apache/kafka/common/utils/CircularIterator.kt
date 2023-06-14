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

package org.apache.kafka.common.utils

/**
 * An iterator that cycles through the `Iterator` of a `Collection` indefinitely. Useful for tasks
 * such as round-robin load balancing. This class does not provide thread-safe access. This
 * `Iterator` supports `null` elements in the underlying `Collection`. This `Iterator` does not
 * support any modification to the underlying `Collection` after it has been wrapped by this class.
 * Changing the underlying `Collection` may cause a [ConcurrentModificationException] or some other
 * undefined behavior.
 *
 * @constructor Create a new instance of a CircularIterator. The ordering of this Iterator will be
 * dictated by the Iterator returned by Collection itself.
 * @param collection The collection to iterate indefinitely
 * @throws IllegalArgumentException if collection is empty.
 */
class CircularIterator<T>(collection: Collection<T>) : MutableIterator<T> {

    private val iterable: Iterable<T>

    private var iterator: Iterator<T>

    private var nextValue: T

    init {
        iterable = collection
        iterator = collection.iterator()
        require(collection.isNotEmpty()) { "CircularIterator can only be used on non-empty lists" }
        nextValue = advance()
    }

    /**
     * Returns true since the iteration will forever cycle through the provided `Collection`.
     *
     * @return Always true
     */
    override fun hasNext(): Boolean = true

    override fun next(): T {
        val next = nextValue
        nextValue = advance()
        return next
    }

    /**
     * Return the next value in the `Iterator`, restarting the `Iterator` if necessary.
     *
     * @return The next value in the iterator
     */
    private fun advance(): T {
        if (!iterator.hasNext()) iterator = iterable.iterator()
        return iterator.next()
    }

    /**
     * Peek at the next value in the Iterator. Calling this method multiple times will return the
     * same element without advancing this Iterator. The value returned by this method will be the
     * next item returned by [next].
     *
     * @return The next value in this `Iterator`
     */
    fun peek(): T = nextValue

    override fun remove() = throw UnsupportedOperationException()
}
