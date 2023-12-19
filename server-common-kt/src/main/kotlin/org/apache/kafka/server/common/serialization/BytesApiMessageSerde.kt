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

package org.apache.kafka.server.common.serialization

import java.nio.ByteBuffer
import org.apache.kafka.common.protocol.ApiMessage
import org.apache.kafka.common.protocol.ByteBufferAccessor
import org.apache.kafka.common.protocol.ObjectSerializationCache
import org.apache.kafka.common.protocol.Readable
import org.apache.kafka.server.common.ApiMessageAndVersion

/**
 * This class provides conversion of `ApiMessageAndVersion` to bytes and vice versa. This can be used
 * as serialization protocol for any metadata records derived of `ApiMessage`s. It internally uses
 * [AbstractApiMessageSerde] for serialization/deserialization mechanism.
 *
 * Implementors need to extend this class and implement [apiMessageFor] method to return a respective
 * `ApiMessage` for the given `apiKey`. This is required to deserialize the bytes to build the respective
 * `ApiMessage` instance.
 */
abstract class BytesApiMessageSerde {

    private val apiMessageSerde: AbstractApiMessageSerde = object : AbstractApiMessageSerde() {
        override fun apiMessageFor(apiKey: Short): ApiMessage {
            return this@BytesApiMessageSerde.apiMessageFor(apiKey)
        }
    }

    fun serialize(messageAndVersion: ApiMessageAndVersion): ByteArray {
        val cache = ObjectSerializationCache()
        val size = apiMessageSerde.recordSize(messageAndVersion, cache)
        val writable = ByteBufferAccessor(ByteBuffer.allocate(size))
        apiMessageSerde.write(messageAndVersion, cache, writable)

        return writable.buffer().array()
    }

    fun deserialize(data: ByteArray): ApiMessageAndVersion {
        val readable: Readable = ByteBufferAccessor(ByteBuffer.wrap(data))

        return apiMessageSerde.read(readable, data.size)
    }

    /**
     * Return `ApiMessage` instance for the given `apiKey`. This is used while deserializing the bytes
     * payload into the respective `ApiMessage` in [deserialize] method.
     *
     * @param apiKey apiKey for which a `ApiMessage` to be created.
     */
    abstract fun apiMessageFor(apiKey: Short): ApiMessage
}
