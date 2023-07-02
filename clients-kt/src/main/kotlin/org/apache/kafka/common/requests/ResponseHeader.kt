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

package org.apache.kafka.common.requests

import java.nio.ByteBuffer
import org.apache.kafka.common.message.ResponseHeaderData
import org.apache.kafka.common.protocol.ByteBufferAccessor
import org.apache.kafka.common.protocol.ObjectSerializationCache

/**
 * A response header in the kafka protocol.
 */
data class ResponseHeader(
    val data: ResponseHeaderData,
    val headerVersion: Short,
) : AbstractRequestResponse {

    private var size = SIZE_NOT_INITIALIZED

    constructor(
        correlationId: Int,
        headerVersion: Short
    ) : this(
        data = ResponseHeaderData().setCorrelationId(correlationId),
        headerVersion = headerVersion,
    )

    /**
     * Calculates the size of [ResponseHeader] in bytes.
     *
     * This method to calculate size should be only when it is immediately followed by [write]
     * method call. In such cases, ObjectSerializationCache helps to avoid the serialization twice.
     * In all other cases, [size] should be preferred instead.
     *
     * Calls to this method leads to calculation of size every time it is invoked. [size] should be
     * preferred* instead.
     *
     * Visible for testing.
     */
    fun size(serializationCache: ObjectSerializationCache): Int {
        return data.size(serializationCache, headerVersion)
    }

    /**
     * Returns the size of [ResponseHeader] in bytes.
     *
     * Calls to this method are idempotent and inexpensive since it returns the cached value of
     * size after the first invocation.
     */
    fun size(): Int {
        if (size == SIZE_NOT_INITIALIZED) {
            size = size(ObjectSerializationCache())
        }
        return size
    }

    @Deprecated(
        message = "Use property instead.",
        replaceWith = ReplaceWith("correlationId"),
    )
    fun correlationId(): Int {
        return data.correlationId()
    }

    val correlationId: Int
        get() = data.correlationId()

    @Deprecated(
        message = "Use property instead.",
        replaceWith = ReplaceWith("headerVersion"),
    )
    fun headerVersion(): Short {
        return headerVersion
    }

    @Deprecated(
        message = "Use property instead.",
        replaceWith = ReplaceWith("data"),
    )
    override fun data(): ResponseHeaderData {
        return data
    }

    // visible for testing
    fun write(buffer: ByteBuffer, serializationCache: ObjectSerializationCache) {
        data.write(ByteBufferAccessor(buffer), serializationCache, headerVersion)
    }

    override fun toString(): String {
        return "ResponseHeader(" +
                "correlationId=${data.correlationId()}" +
                ", headerVersion=$headerVersion" +
                ")"
    }

    companion object {

        private const val SIZE_NOT_INITIALIZED = -1

        fun parse(buffer: ByteBuffer, headerVersion: Short): ResponseHeader {
            val bufferStartPositionForHeader = buffer.position()
            val header = ResponseHeader(
                ResponseHeaderData(ByteBufferAccessor(buffer), headerVersion), headerVersion
            )
            // Size of header is calculated by the shift in the position of buffer's start position during parsing.
            // Prior to parsing, the buffer's start position points to header data and after the parsing operation
            // the buffer's start position points to api message. For more information on how the buffer is
            // constructed, see RequestUtils#serialize()
            header.size = Math.max(buffer.position() - bufferStartPositionForHeader, 0)
            return header
        }
    }
}
