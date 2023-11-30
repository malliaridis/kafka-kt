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

package org.apache.kafka.common.header.internals

import java.util.*
import org.apache.kafka.common.header.Header
import org.apache.kafka.common.header.Headers
import org.apache.kafka.common.record.Record
import org.apache.kafka.common.utils.AbstractIterator

class RecordHeaders private constructor(
    private val headers: MutableList<Header> = mutableListOf(),
) : Headers {

    @Volatile
    private var isReadOnly = false

    constructor() : this(headers = mutableListOf())

    constructor(headers: Iterable<Header>?) : this(headers?.toMutableList() ?: mutableListOf())

    constructor(headers: Array<Header>) : this(headers.toMutableList())

    @Throws(IllegalStateException::class)
    override fun add(header: Header): Headers {
        canWrite()
        headers.add(header)
        return this
    }

    @Throws(IllegalStateException::class)
    override fun add(key: String, value: ByteArray): Headers {
        return add(RecordHeader(key, value))
    }

    @Throws(IllegalStateException::class)
    override fun remove(key: String): Headers {
        canWrite()
        val iterator = iterator()
        while (iterator.hasNext()) {
            if (iterator.next().key == key) iterator.remove()
        }
        return this
    }

    override fun lastHeader(key: String): Header? {
        headers.indices.reversed().forEach { i ->
            val header = headers[i]
            if (header.key == key) {
                return header
            }
        }
        return null
    }

    override fun headers(key: String): Iterable<Header> {
        return Iterable {
            FilterByKeyIterator(headers.iterator(), key)
        }
    }

    override fun iterator(): MutableIterator<Header> {
        return closeAware(headers.iterator())
    }

    fun setReadOnly() {
        isReadOnly = true
    }

    override fun toArray(): Array<Header> {
        return if (headers.isEmpty()) Record.EMPTY_HEADERS else headers.toTypedArray<Header>()
    }

    private fun canWrite() {
        check(!isReadOnly) { "RecordHeaders has been closed." }
    }

    private fun closeAware(original: MutableIterator<Header>): MutableIterator<Header> {
        return object : MutableIterator<Header> {

            override fun hasNext(): Boolean = original.hasNext()

            override fun next(): Header = original.next()

            override fun remove() {
                canWrite()
                original.remove()
            }
        }
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) {
            return true
        }
        if (other == null || javaClass != other.javaClass) {
            return false
        }
        val headers1 = other as RecordHeaders
        return headers == headers1.headers
    }

    override fun hashCode(): Int {
        return headers.hashCode()
    }

    override fun toString(): String {
        return "RecordHeaders(" +
                "headers = $headers" +
                ", isReadOnly = $isReadOnly" +
                ')'
    }

    private class FilterByKeyIterator(
        private val original: Iterator<Header>,
        private val key: String
    ) :
        AbstractIterator<Header>() {
        override fun makeNext(): Header? {
            while (true) {
                if (original.hasNext()) {
                    val header = original.next()
                    if (header.key != key) continue
                    return header
                }
                return allDone()
            }
        }
    }
}
