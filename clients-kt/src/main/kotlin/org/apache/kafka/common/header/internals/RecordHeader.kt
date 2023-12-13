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

import java.nio.ByteBuffer
import org.apache.kafka.common.header.Header
import org.apache.kafka.common.utils.Utils
import org.apache.kafka.common.utils.Utils.utf8

data class RecordHeader(
    override val key: String,
    override val value: ByteArray?,
) : Header {

    constructor(
        keyBuffer: ByteBuffer,
        valueBuffer: ByteBuffer?,
    ) : this(
        key = utf8(keyBuffer, keyBuffer.remaining()),
        value = valueBuffer?.let { Utils.toArray(it) }
    )

    @Deprecated("Use property instead", replaceWith = ReplaceWith("key"))
    override fun key(): String = key

    @Deprecated("Use property instead", replaceWith = ReplaceWith("value"))
    override fun value(): ByteArray? = value

    override fun toString(): String {
        return "RecordHeader(key = $key, value = ${value?.decodeToString()}"
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as RecordHeader

        if (key != other.key) return false
        if (value != null) {
            if (other.value == null) return false
            if (!value.contentEquals(other.value)) return false
        } else if (other.value != null) return false

        return true
    }

    override fun hashCode(): Int {
        var result = key.hashCode()
        result = 31 * result + (value?.contentHashCode() ?: 0)
        return result
    }
}
