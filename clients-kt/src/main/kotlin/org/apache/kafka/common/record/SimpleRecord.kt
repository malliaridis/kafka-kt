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

package org.apache.kafka.common.record

import org.apache.kafka.common.header.Header
import org.apache.kafka.common.utils.Utils.wrapNullable
import java.nio.ByteBuffer

/**
 * High-level representation of a kafka record. This is useful when building record sets to
 * avoid depending on a specific magic version.
 */
data class SimpleRecord(
    val timestamp: Long = RecordBatch.NO_TIMESTAMP,
    val key: ByteBuffer? = null,
    val value: ByteBuffer?,
    val headers: Array<Header> = Record.EMPTY_HEADERS,
) {

    constructor(
        timestamp: Long = RecordBatch.NO_TIMESTAMP,
        key: ByteArray? = null,
        value: ByteArray?,
        headers: Array<Header> = Record.EMPTY_HEADERS,
    ) : this(
        timestamp = timestamp,
        key = wrapNullable(key),
        value = wrapNullable(value),
        headers = headers,
    )

    constructor(record: Record) : this(
        timestamp = record.timestamp(),
        key = record.key(),
        value = record.value(),
        headers = record.headers()
    )

    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("key"),
    )
    fun key(): ByteBuffer? = key


    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("value"),
    )
    fun value(): ByteBuffer? = value

    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("timestamp"),
    )
    fun timestamp(): Long = timestamp

    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("headers"),
    )
    fun headers(): Array<Header> = headers

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as SimpleRecord

        if (timestamp != other.timestamp) return false
        if (key != other.key) return false
        if (value != other.value) return false
        return headers.contentEquals(other.headers)
    }

    override fun hashCode(): Int {
        var result = timestamp.hashCode()
        result = 31 * result + (key?.hashCode() ?: 0)
        result = 31 * result + (value?.hashCode() ?: 0)
        result = 31 * result + headers.contentHashCode()
        return result
    }

    override fun toString(): String = "SimpleRecord(" +
            "timestamp=${timestamp}" +
            ", key=${key?.limit() ?: 0} bytes" +
            ", value=${value?.limit() ?: 0} bytes)"
}
