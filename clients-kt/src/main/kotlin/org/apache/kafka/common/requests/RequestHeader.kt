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
import org.apache.kafka.common.errors.InvalidRequestException
import org.apache.kafka.common.errors.UnsupportedVersionException
import org.apache.kafka.common.message.RequestHeaderData
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.ByteBufferAccessor
import org.apache.kafka.common.protocol.ObjectSerializationCache

/**
 * The header for a request in the Kafka protocol
 */
class RequestHeader(
    private val data: RequestHeaderData,
    val headerVersion: Short,
) : AbstractRequestResponse {

    private var size = SIZE_NOT_INITIALIZED

    constructor(
        requestApiKey: ApiKeys,
        requestVersion: Short,
        clientId: String?,
        correlationId: Int,
    ) : this(
        data = RequestHeaderData()
            .setRequestApiKey(requestApiKey.id)
            .setRequestApiVersion(requestVersion)
            .setClientId(clientId)
            .setCorrelationId(correlationId),
        headerVersion = requestApiKey.requestHeaderVersion(requestVersion),
    )

    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("apiKey"),
    )
    fun apiKey(): ApiKeys = ApiKeys.forId(data.requestApiKey.toInt())

    val apiKey: ApiKeys
        get() = ApiKeys.forId(data.requestApiKey.toInt())

    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("apiVersion"),
    )
    fun apiVersion(): Short = data.requestApiVersion

    val apiVersion: Short
        get() = data.requestApiVersion

    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("headerVersion"),
    )
    fun headerVersion(): Short = headerVersion

    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("clientId"),
    )
    fun clientId(): String? = data.clientId

    val clientId: String?
        get() = data.clientId

    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("correlationId"),
    )
    fun correlationId(): Int = data.correlationId

    val correlationId: Int
        get() = data.correlationId

    override fun data(): RequestHeaderData = data

    // Visible for testing.
    fun write(buffer: ByteBuffer?, serializationCache: ObjectSerializationCache?) {
        data.write(ByteBufferAccessor(buffer!!), serializationCache!!, headerVersion)
    }

    /**
     * Calculates the size of [RequestHeader] in bytes.
     *
     * This method to calculate size should be only when it is immediately followed by
     * [write] method call. In such cases, ObjectSerializationCache
     * helps to avoid the serialization twice. In all other cases, [size] should be preferred
     * instead.
     *
     * Calls to this method leads to calculation of size every time it is invoked. [size] should be
     * preferred instead.
     *
     * Visible for testing.
     */
    fun size(serializationCache: ObjectSerializationCache): Int {
        size = data.size(serializationCache, headerVersion)
        return size
    }

    /**
     * Returns the size of [RequestHeader] in bytes.
     *
     * Calls to this method are idempotent and inexpensive since it returns the cached value of size
     * after the first invocation.
     */
    fun size(): Int {
        if (size == SIZE_NOT_INITIALIZED) size = size(ObjectSerializationCache())
        return size
    }

    fun toResponseHeader(): ResponseHeader {
        return ResponseHeader(data.correlationId, apiKey.responseHeaderVersion(apiVersion))
    }

    override fun toString(): String {
        return "RequestHeader(apiKey=$apiKey" +
                ", apiVersion=$apiVersion" +
                ", clientId=$clientId" +
                ", correlationId=$correlationId" +
                ", headerVersion=$headerVersion" +
                ")"
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as RequestHeader

        if (headerVersion != other.headerVersion) return false
        if (data != other.data) return false

        return true
    }

    override fun hashCode(): Int {
        var result = data.hashCode()
        result = 31 * result + headerVersion
        return result
    }

    companion object {

        private const val SIZE_NOT_INITIALIZED = -1

        fun parse(buffer: ByteBuffer): RequestHeader {
            var apiKey: Short = -1
            return try {

                // We derive the header version from the request api version, so we read that first.
                // The request api version is part of `RequestHeaderData`, so we reset the buffer
                // position after the read.
                val bufferStartPositionForHeader = buffer.position()
                apiKey = buffer.getShort()
                val apiVersion = buffer.getShort()
                val headerVersion = ApiKeys.forId(apiKey.toInt()).requestHeaderVersion(apiVersion)
                buffer.position(bufferStartPositionForHeader)
                val headerData = RequestHeaderData(ByteBufferAccessor(buffer), headerVersion)

                // Due to a quirk in the protocol, client ID is marked as nullable.
                // However, we treat a null client ID as equivalent to an empty client ID.
                if (headerData.clientId == null) headerData.setClientId("")

                val header = RequestHeader(headerData, headerVersion)

                // Size of header is calculated by the shift in the position of buffer's start
                // position during parsing. Prior to parsing, the buffer's start position points to
                // header data and after the parsing operation the buffer's start position points to
                // api message. For more information on how the buffer is constructed, see
                // RequestUtils#serialize()
                header.size = (buffer.position() - bufferStartPositionForHeader).coerceAtLeast(0)
                header
            } catch (e: UnsupportedVersionException) {
                throw InvalidRequestException("Unknown API key $apiKey", e)
            } catch (ex: Throwable) {
                throw InvalidRequestException(
                    "Error parsing request header. Our best guess of the apiKey is: $apiKey",
                    ex
                )
            }
        }
    }
}
