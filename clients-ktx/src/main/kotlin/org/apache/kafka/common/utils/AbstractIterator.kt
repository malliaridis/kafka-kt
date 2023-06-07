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
 * A base class that simplifies implementing an iterator
 *
 * @param T The type of thing we are iterating over
 */
abstract class AbstractIterator<T> : MutableIterator<T> {

    private enum class State {
        READY,
        NOT_READY,
        DONE,
        FAILED
    }

    private var state = State.NOT_READY

    private var next: T? = null

    override fun hasNext(): Boolean {
        return when (state) {
            State.FAILED -> error("Iterator is in failed state")
            State.DONE -> false
            State.READY -> true
            else -> maybeComputeNext()
        }
    }

    override fun next(): T {
        if (!hasNext()) throw NoSuchElementException()
        state = State.NOT_READY
        return checkNotNull(next) { "Expected item but none found." }
    }

    override fun remove() {
        throw UnsupportedOperationException("Removal not supported")
    }

    fun peek(): T {
        if (!hasNext()) throw NoSuchElementException()
        return checkNotNull(next) { "Expected item but none found." }
    }

    protected fun allDone(): T? {
        state = State.DONE
        return null
    }

    protected abstract fun makeNext(): T?

    private fun maybeComputeNext(): Boolean {
        state = State.FAILED
        next = makeNext()
        return if (state == State.DONE) false
        else {
            state = State.READY
            true
        }
    }
}
