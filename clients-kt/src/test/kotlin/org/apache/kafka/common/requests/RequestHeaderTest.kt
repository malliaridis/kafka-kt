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
import org.apache.kafka.common.message.RequestHeaderData
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.ByteBufferAccessor
import org.apache.kafka.common.protocol.ObjectSerializationCache
import org.apache.kafka.common.requests.RequestTestUtils.serializeRequestHeader
import org.junit.jupiter.api.Test
import org.mockito.ArgumentMatchers.any
import org.mockito.kotlin.spy
import org.mockito.kotlin.verify
import kotlin.test.assertEquals

class RequestHeaderTest {
    @Test
    fun testSerdeControlledShutdownV0() {
        // Verify that version 0 of controlled shutdown does not include the clientId field
        val apiVersion: Short = 0
        val correlationId = 2342
        val rawBuffer = ByteBuffer.allocate(32)
        rawBuffer.putShort(ApiKeys.CONTROLLED_SHUTDOWN.id)
        rawBuffer.putShort(apiVersion)
        rawBuffer.putInt(correlationId)
        rawBuffer.flip()

        val deserialized = RequestHeader.parse(rawBuffer)
        assertEquals(ApiKeys.CONTROLLED_SHUTDOWN, deserialized.apiKey)
        assertEquals(0, deserialized.apiVersion)
        assertEquals(correlationId, deserialized.correlationId)
        assertEquals("", deserialized.clientId)
        assertEquals(0, deserialized.headerVersion)

        val serializedBuffer = serializeRequestHeader(deserialized)

        assertEquals(ApiKeys.CONTROLLED_SHUTDOWN.id, serializedBuffer.getShort(0))
        assertEquals(0, serializedBuffer.getShort(2))
        assertEquals(correlationId, serializedBuffer.getInt(4))
        assertEquals(8, serializedBuffer.limit())
    }

    @Test
    fun testRequestHeaderV1() {
        val apiVersion: Short = 1
        val header = RequestHeader(
            requestApiKey = ApiKeys.FIND_COORDINATOR,
            requestVersion = apiVersion,
            clientId = "",
            correlationId = 10,
        )
        assertEquals(1, header.headerVersion)

        val buffer = serializeRequestHeader(header)
        assertEquals(10, buffer.remaining())
        val deserialized = RequestHeader.parse(buffer)
        assertEquals(header, deserialized)
    }

    @Test
    fun testRequestHeaderV2() {
        val apiVersion: Short = 2
        val header = RequestHeader(
            requestApiKey = ApiKeys.CREATE_DELEGATION_TOKEN,
            requestVersion = apiVersion,
            clientId = "",
            correlationId = 10,
        )
        assertEquals(2, header.headerVersion)

        val buffer = serializeRequestHeader(header)
        assertEquals(11, buffer.remaining())
        val deserialized = RequestHeader.parse(buffer)
        assertEquals(header, deserialized)
    }

    @Test
    fun parseHeaderFromBufferWithNonZeroPosition() {
        val buffer = ByteBuffer.allocate(64)
        buffer.position(10)

        val header = RequestHeader(
            requestApiKey = ApiKeys.FIND_COORDINATOR,
            requestVersion = 1,
            clientId = "",
            correlationId = 10,
        )
        val serializationCache = ObjectSerializationCache()
        // size must be called before write to avoid an NPE with the current implementation
        header.size(serializationCache)
        header.write(buffer, serializationCache)
        val limit = buffer.position()
        buffer.position(10)
        buffer.limit(limit)

        val parsed = RequestHeader.parse(buffer)
        assertEquals(header, parsed)
    }

    @Test
    fun parseHeaderWithNullClientId() {
        val headerData = RequestHeaderData()
            .setClientId(null)
            .setCorrelationId(123)
            .setRequestApiKey(ApiKeys.FIND_COORDINATOR.id)
            .setRequestApiVersion(10)
        val serializationCache = ObjectSerializationCache()
        val buffer = ByteBuffer.allocate(headerData.size(serializationCache, 2.toShort()))
        headerData.write(
            writable = ByteBufferAccessor(buffer),
            cache = serializationCache,
            version = 2,
        )
        buffer.flip()
        val parsed = RequestHeader.parse(buffer)
        assertEquals("", parsed.clientId)
        assertEquals(123, parsed.correlationId)
        assertEquals(ApiKeys.FIND_COORDINATOR, parsed.apiKey)
        assertEquals(10.toShort(), parsed.apiVersion)
    }

    @Test
    fun verifySizeMethodsReturnSameValue() {
        // Create a dummy RequestHeaderData
        val headerData = RequestHeaderData().setClientId("hakuna-matata").setCorrelationId(123)
            .setRequestApiKey(ApiKeys.FIND_COORDINATOR.id).setRequestApiVersion(10.toShort())

        // Serialize RequestHeaderData to a buffer
        val serializationCache = ObjectSerializationCache()
        val buffer = ByteBuffer.allocate(headerData.size(serializationCache, 2.toShort()))
        headerData.write(ByteBufferAccessor(buffer), serializationCache, 2.toShort())
        buffer.flip()

        // actual call to generate the RequestHeader from buffer containing RequestHeaderData
        val parsed: RequestHeader = spy(RequestHeader.parse(buffer))

        // verify that the result of cached value of size is same as actual calculation of size
        val sizeCalculatedFromData = parsed.size(ObjectSerializationCache())
        val sizeFromCache = parsed.size()
        assertEquals(sizeCalculatedFromData, sizeFromCache)

        // verify that size(ObjectSerializationCache) is only called once, i.e. during assertEquals call. This validates
        // that size() method does not calculate the size instead it uses the cached value
        verify(parsed).size(any(ObjectSerializationCache::class.java))
    }
}
