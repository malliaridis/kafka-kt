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
import java.nio.ByteBuffer

class PartialDefaultRecord internal constructor(
    sizeInBytes: Int,
    attributes: Byte,
    offset: Long,
    timestamp: Long,
    sequence: Int,
    private val keySize: Int,
    private val valueSize: Int,
) : DefaultRecord(
    sizeInBytes = sizeInBytes,
    attributes = attributes,
    offset = offset,
    timestamp = timestamp,
    sequence = sequence,
    key = null,
    value = null,
    headers = Record.EMPTY_HEADERS,
) {

    override fun equals(o: Any?): Boolean {
        return super.equals(o) && keySize == (o as PartialDefaultRecord).keySize && valueSize == o.valueSize
    }

    override fun hashCode(): Int {
        var result = super.hashCode()
        result = 31 * result + keySize
        result = 31 * result + valueSize
        return result
    }

    override fun toString(): String = "PartialDefaultRecord(" +
            "offset=${offset()}" +
            ", timestamp=${timestamp()}" +
            ", key=$keySize bytes" +
            ", value=$valueSize bytes)"

    override fun keySize(): Int = keySize

    override fun hasKey(): Boolean = keySize >= 0

    override fun key(): ByteBuffer? {
        throw UnsupportedOperationException("key is skipped in PartialDefaultRecord")
    }

    override fun valueSize(): Int = valueSize

    override fun hasValue(): Boolean = valueSize >= 0

    override fun value(): ByteBuffer? {
        throw UnsupportedOperationException("value is skipped in PartialDefaultRecord")
    }

    override fun headers(): Array<Header> {
        throw UnsupportedOperationException("headers is skipped in PartialDefaultRecord")
    }
}
