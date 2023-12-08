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
import org.apache.kafka.common.message.AddPartitionsToTxnResponseData
import org.apache.kafka.common.message.AddPartitionsToTxnResponseData.AddPartitionsToTxnPartitionResult
import org.apache.kafka.common.message.AddPartitionsToTxnResponseData.AddPartitionsToTxnTopicResult
import org.apache.kafka.common.message.AddPartitionsToTxnResponseData.AddPartitionsToTxnTopicResultCollection
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.Errors
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

class AddPartitionsToTxnResponseTest {
    
    internal val throttleTimeMs = 10
    
    internal val topicOne = "topic1"
    
    internal val partitionOne = 1
    
    internal val errorOne = Errors.COORDINATOR_NOT_AVAILABLE
    
    internal val errorTwo = Errors.NOT_COORDINATOR
    
    internal val topicTwo = "topic2"
    
    internal val partitionTwo = 2
    
    internal var tp1 = TopicPartition(topic = topicOne, partition = partitionOne)
    
    internal var tp2 = TopicPartition(topic = topicTwo, partition = partitionTwo)
    
    internal lateinit var expectedErrorCounts: MutableMap<Errors, Int>
    
    internal lateinit var errorsMap: MutableMap<TopicPartition, Errors>
    
    @BeforeEach
    fun setUp() {
        expectedErrorCounts = mutableMapOf(errorOne to 1, errorTwo to 1)
        errorsMap = mutableMapOf(tp1 to errorOne, tp2 to errorTwo)
    }

    @Test
    fun testConstructorWithErrorResponse() {
        val response = AddPartitionsToTxnResponse(throttleTimeMs, errorsMap)
        assertEquals(expectedErrorCounts, response.errorCounts())
        assertEquals(throttleTimeMs, response.throttleTimeMs())
    }

    @Test
    fun testParse() {
        val topicCollection = AddPartitionsToTxnTopicResultCollection()
        val topicResult = AddPartitionsToTxnTopicResult()
        topicResult.setName(topicOne)
        topicResult.results.add(
            AddPartitionsToTxnPartitionResult()
                .setErrorCode(errorOne.code)
                .setPartitionIndex(partitionOne)
        )
        topicResult.results.add(
            AddPartitionsToTxnPartitionResult()
                .setErrorCode(errorTwo.code)
                .setPartitionIndex(partitionTwo)
        )
        topicCollection.add(topicResult)
        val data = AddPartitionsToTxnResponseData()
            .setResults(topicCollection)
            .setThrottleTimeMs(throttleTimeMs)
        val response = AddPartitionsToTxnResponse(data)
        ApiKeys.ADD_PARTITIONS_TO_TXN.allVersions().forEach { version ->
            val parsedResponse = AddPartitionsToTxnResponse.parse(response.serialize(version), version)
            assertEquals(expectedErrorCounts, parsedResponse.errorCounts())
            assertEquals(throttleTimeMs, parsedResponse.throttleTimeMs())
            assertEquals(version >= 1, parsedResponse.shouldClientThrottle(version))
        }
    }
}
