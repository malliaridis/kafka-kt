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
import org.apache.kafka.common.protocol.Errors
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

class WriteTxnMarkersResponseTest {

    @BeforeEach
    fun setUp() {
        errorMap = mapOf(
            producerIdOne to mapOf(tp1 to pidOneError),
            producerIdTwo to mapOf(tp2 to pidTwoError),
        )
    }

    @Test
    fun testConstructor() {
        val expectedErrorCounts = mapOf(
            Errors.UNKNOWN_PRODUCER_ID to 1,
            Errors.INVALID_PRODUCER_EPOCH to 1,
        )
        val response = WriteTxnMarkersResponse(errorMap)

        assertEquals(expectedErrorCounts, response.errorCounts())
        assertEquals(mapOf(tp1 to pidOneError), response.errorsByProducerId()[producerIdOne])
        assertEquals(mapOf(tp2 to pidTwoError), response.errorsByProducerId()[producerIdTwo])
    }

    companion object {
        
        private const val producerIdOne = 1L
        
        private const val producerIdTwo = 2L
        
        private val tp1 = TopicPartition("topic", 1)
        
        private val tp2 = TopicPartition("topic", 2)
        
        private val pidOneError = Errors.UNKNOWN_PRODUCER_ID
        
        private val pidTwoError = Errors.INVALID_PRODUCER_EPOCH
        
        private lateinit var errorMap: Map<Long, Map<TopicPartition, Errors>>
    }
}
