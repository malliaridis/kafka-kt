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

import java.io.Closeable

/**
 * Iterators that need to be closed in order to release resources should implement this interface.
 *
 * Warning: before implementing this interface, consider if there are better options. The chance of misuse is
 * a bit high since people are used to iterating without closing.
 */
interface CloseableIterator<T> : MutableIterator<T>, Closeable {
    override fun close()

    companion object {
        fun <R> wrap(inner: MutableIterator<R>): CloseableIterator<R> {
            return object : CloseableIterator<R> {

                override fun close() {}

                override fun hasNext(): Boolean {
                    return inner.hasNext()
                }

                override fun next(): R {
                    return inner.next()
                }

                override fun remove() {
                    inner.remove()
                }
            }
        }
    }
}
