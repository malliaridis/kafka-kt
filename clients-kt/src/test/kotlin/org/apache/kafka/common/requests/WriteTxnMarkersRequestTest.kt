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
import org.apache.kafka.common.requests.WriteTxnMarkersRequest.TxnMarkerEntry
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

class WriteTxnMarkersRequestTest {
    
    @BeforeEach
    fun setUp() {
        markers = listOf(
            TxnMarkerEntry(
                producerId = PRODUCER_ID,
                producerEpoch = PRODUCER_EPOCH,
                coordinatorEpoch = COORDINATOR_EPOCH,
                transactionResult = result,
                partitions = listOf(topicPartition),
            )
        )
    }

    @Test
    fun testConstructor() {
        val builder = WriteTxnMarkersRequest.Builder(
            version = ApiKeys.WRITE_TXN_MARKERS.latestVersion(),
            markers = markers,
        )
        for (version in ApiKeys.WRITE_TXN_MARKERS.allVersions()) {
            val request = builder.build(version)
            assertEquals(1, request.markers().size)
            val marker = request.markers()[0]

            assertEquals(PRODUCER_ID, marker.producerId)
            assertEquals(PRODUCER_EPOCH, marker.producerEpoch)
            assertEquals(COORDINATOR_EPOCH, marker.coordinatorEpoch)
            assertEquals(result, marker.transactionResult)
            assertEquals(listOf(topicPartition), marker.partitions)
        }
    }

    @Test
    fun testGetErrorResponse() {
        val builder = WriteTxnMarkersRequest.Builder(
            version = ApiKeys.WRITE_TXN_MARKERS.latestVersion(),
            markers = markers
        )
        for (version in ApiKeys.WRITE_TXN_MARKERS.allVersions()) {
            val request = builder.build(version)
            val errorResponse = request.getErrorResponse(
                throttleTimeMs = throttleTimeMs,
                e = Errors.UNKNOWN_PRODUCER_ID.exception!!,
            )
            assertEquals(
                expected = mapOf(topicPartition to Errors.UNKNOWN_PRODUCER_ID),
                actual = errorResponse.errorsByProducerId()[PRODUCER_ID],
            )
            assertEquals(
                expected = mapOf(Errors.UNKNOWN_PRODUCER_ID to 1),
                actual = errorResponse.errorCounts(),
            )
            // Write txn marker has no throttle time defined in response.
            assertEquals(expected = 0, actual = errorResponse.throttleTimeMs())
        }
    }

    companion object {
        
        private const val PRODUCER_ID = 10L
        
        private const val PRODUCER_EPOCH: Short = 2
        
        private const val COORDINATOR_EPOCH = 1
        
        private val result = TransactionResult.COMMIT
        
        private val topicPartition = TopicPartition(topic = "topic", partition = 73)
        
        internal var throttleTimeMs = 10
        
        private lateinit var markers: List<TxnMarkerEntry>
    }
}
