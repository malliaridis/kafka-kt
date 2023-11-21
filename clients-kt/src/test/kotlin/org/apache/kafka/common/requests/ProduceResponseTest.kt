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

import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.record.RecordBatch
import org.apache.kafka.common.requests.ProduceResponse.RecordError
import org.apache.kafka.common.requests.RequestTestUtils.serializeResponseWithHeader
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

class ProduceResponseTest {

    @Suppress("Deprecation")
    @Test
    fun produceResponseV5Test() {
        val tp0 = TopicPartition("test", 0)
        val responseData = mapOf(
            tp0 to ProduceResponse.PartitionResponse(
                error = Errors.NONE,
                baseOffset = 10000,
                logAppendTime = RecordBatch.NO_TIMESTAMP,
                logStartOffset = 100
            ),
        )
        
        val v5Response = ProduceResponse(responseData, 10)
        val version: Short = 5
        
        val buffer = serializeResponseWithHeader(
            response = v5Response,
            version = version,
            correlationId = 0,
        )
        
        ResponseHeader.parse(buffer, ApiKeys.PRODUCE.responseHeaderVersion(version)) // throw away.
        val v5FromBytes = AbstractResponse.parseResponse(
            apiKey = ApiKeys.PRODUCE,
            responseBuffer = buffer,
            version = version,
        ) as ProduceResponse
        
        assertEquals(1, v5FromBytes.data().responses.size)
        val topicProduceResponse = v5FromBytes.data().responses.first()
        assertEquals(1, topicProduceResponse.partitionResponses.size)
        val partitionProduceResponse = topicProduceResponse.partitionResponses.first()
        val tp = TopicPartition(topicProduceResponse.name, partitionProduceResponse.index)
        assertEquals(tp0, tp)
        
        assertEquals(100, partitionProduceResponse.logStartOffset)
        assertEquals(10000, partitionProduceResponse.baseOffset)
        assertEquals(RecordBatch.NO_TIMESTAMP, partitionProduceResponse.logAppendTimeMs)
        assertEquals(Errors.NONE, Errors.forCode(partitionProduceResponse.errorCode))
        assertNull(partitionProduceResponse.errorMessage)
        assertTrue(partitionProduceResponse.recordErrors.isEmpty())
    }

    @Suppress("Deprecation")
    @Test
    fun produceResponseVersionTest() {
        val responseData = mapOf(
            TopicPartition("test", 0)to ProduceResponse.PartitionResponse(
                error = Errors.NONE,
                baseOffset = 10000,
                logAppendTime = RecordBatch.NO_TIMESTAMP,
                logStartOffset = 100,
            )
        )
        val v0Response = ProduceResponse(responseData)
        val v1Response = ProduceResponse(responseData, 10)
        val v2Response = ProduceResponse(responseData, 10)
        assertEquals(0, v0Response.throttleTimeMs(), "Throttle time must be zero")
        assertEquals(10, v1Response.throttleTimeMs(), "Throttle time must be 10")
        assertEquals(10, v2Response.throttleTimeMs(), "Throttle time must be 10")

        val arrResponse = listOf(v0Response, v1Response, v2Response)
        for (produceResponse in arrResponse) {
            assertEquals(1, produceResponse.data().responses.size)
            val topicProduceResponse = produceResponse.data().responses.first()
            assertEquals(1, topicProduceResponse.partitionResponses.size)
            val partitionProduceResponse = topicProduceResponse.partitionResponses.first()
            assertEquals(100, partitionProduceResponse.logStartOffset)
            assertEquals(10000, partitionProduceResponse.baseOffset)
            assertEquals(RecordBatch.NO_TIMESTAMP, partitionProduceResponse.logAppendTimeMs)
            assertEquals(Errors.NONE, Errors.forCode(partitionProduceResponse.errorCode))
            assertNull(partitionProduceResponse.errorMessage)
            assertTrue(partitionProduceResponse.recordErrors.isEmpty())
        }
    }

    @Suppress("Deprecation")
    @Test
    fun produceResponseRecordErrorsTest() {
        val tp = TopicPartition("test", 0)
        val partResponse = ProduceResponse.PartitionResponse(
            error = Errors.NONE,
            baseOffset = 10000,
            logAppendTime = RecordBatch.NO_TIMESTAMP,
            logStartOffset = 100,
            recordErrors = listOf(RecordError(3, "Record error")),
            errorMessage = "Produce failed",
        )
        val responseData = mapOf(tp to partResponse)

        for (version in ApiKeys.PRODUCE.allVersions()) {
            val response = ProduceResponse(responseData)
            val produceResponse = ProduceResponse.parse(response.serialize(version), version)
            val topicProduceResponse = produceResponse.data().responses.first()
            val deserialized = topicProduceResponse.partitionResponses.first()
            if (version >= 8) {
                assertEquals(1, deserialized.recordErrors.size)
                assertEquals(3, deserialized.recordErrors[0].batchIndex)
                assertEquals("Record error", deserialized.recordErrors[0].batchIndexErrorMessage)
                assertEquals("Produce failed", deserialized.errorMessage)
            } else {
                assertEquals(0, deserialized.recordErrors.size)
                assertNull(deserialized.errorMessage)
            }
        }
    }
}
