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
import org.apache.kafka.common.message.AddPartitionsToTxnResponseData.AddPartitionsToTxnResult
import org.apache.kafka.common.message.AddPartitionsToTxnResponseData.AddPartitionsToTxnResultCollection
import org.apache.kafka.common.message.AddPartitionsToTxnResponseData.AddPartitionsToTxnTopicResult
import org.apache.kafka.common.message.AddPartitionsToTxnResponseData.AddPartitionsToTxnTopicResultCollection
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.AddPartitionsToTxnResponse.Companion.errorsForTransaction
import org.apache.kafka.common.utils.annotation.ApiKeyVersionsSource
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class AddPartitionsToTxnResponseTest {
    
    internal val throttleTimeMs = 10
    
    internal val topicOne = "topic1"
    
    internal val partitionOne = 1
    
    internal val errorOne = Errors.COORDINATOR_NOT_AVAILABLE
    
    internal val errorTwo = Errors.NOT_COORDINATOR
    
    internal val topicTwo = "topic2"
    
    internal val partitionTwo = 2
    
    internal val tp1 = TopicPartition(topic = topicOne, partition = partitionOne)
    
    internal val tp2 = TopicPartition(topic = topicTwo, partition = partitionTwo)
    
    internal lateinit var expectedErrorCounts: MutableMap<Errors, Int>
    
    internal lateinit var errorsMap: MutableMap<TopicPartition, Errors>
    
    @BeforeEach
    fun setUp() {
        expectedErrorCounts = mutableMapOf(errorOne to 1, errorTwo to 1)
        errorsMap = mutableMapOf(tp1 to errorOne, tp2 to errorTwo)
    }

    @ParameterizedTest
    @ApiKeyVersionsSource(apiKey = ApiKeys.ADD_PARTITIONS_TO_TXN)
    fun testParse(version: Short) {
        val topicCollection = AddPartitionsToTxnTopicResultCollection()
        val topicResult = AddPartitionsToTxnTopicResult()
        topicResult.setName(topicOne)
        topicResult.resultsByPartition.add(
            AddPartitionsToTxnPartitionResult()
                .setPartitionErrorCode(errorOne.code)
                .setPartitionIndex(partitionOne)
        )
        topicResult.resultsByPartition.add(
            AddPartitionsToTxnPartitionResult()
                .setPartitionErrorCode(errorTwo.code)
                .setPartitionIndex(partitionTwo)
        )
        topicCollection.add(topicResult)

        if (version < 4) {
            val data = AddPartitionsToTxnResponseData()
                .setResultsByTopicV3AndBelow(topicCollection)
                .setThrottleTimeMs(throttleTimeMs)
            val response = AddPartitionsToTxnResponse(data)
            val parsedResponse = AddPartitionsToTxnResponse.parse(
                buffer = response.serialize(version),
                version = version,
            )
            assertEquals(expectedErrorCounts, parsedResponse.errorCounts())
            assertEquals(throttleTimeMs, parsedResponse.throttleTimeMs())
            assertEquals(version >= 1, parsedResponse.shouldClientThrottle(version))
        } else {
            val results = AddPartitionsToTxnResultCollection()
            results.add(
                AddPartitionsToTxnResult()
                    .setTransactionalId("txn1")
                    .setTopicResults(topicCollection)
            )

            // Create another transaction with new name and errorOne for a single partition.
            val txnTwoExpectedErrors = mapOf(tp2 to errorOne)
            results.add(AddPartitionsToTxnResponse.resultForTransaction("txn2", txnTwoExpectedErrors))

            val data = AddPartitionsToTxnResponseData()
                .setResultsByTransaction(results)
                .setThrottleTimeMs(throttleTimeMs)
            val response = AddPartitionsToTxnResponse(data)

            val newExpectedErrorCounts = mapOf(
                Errors.NONE to 1, // top level error
                errorOne to 2,
                errorTwo to 1,
            )

            val parsedResponse = AddPartitionsToTxnResponse.parse(
                buffer = response.serialize(version),
                version = version,
            )
            assertEquals(
                expected = txnTwoExpectedErrors,
                actual = errorsForTransaction(response.getTransactionTopicResults("txn2")),
            )
            assertEquals(newExpectedErrorCounts, parsedResponse.errorCounts())
            assertEquals(throttleTimeMs, parsedResponse.throttleTimeMs())
            assertTrue(parsedResponse.shouldClientThrottle(version))
        }
    }

    @Test
    fun testBatchedErrors() {
        val txn1Errors = mapOf(tp1 to errorOne)
        val txn2Errors = mapOf(tp1 to errorOne)
        val transaction1 = AddPartitionsToTxnResponse.resultForTransaction("txn1", txn1Errors)
        val transaction2 = AddPartitionsToTxnResponse.resultForTransaction("txn2", txn2Errors)

        val results = AddPartitionsToTxnResultCollection()
        results.add(transaction1)
        results.add(transaction2)

        val response = AddPartitionsToTxnResponse(AddPartitionsToTxnResponseData().setResultsByTransaction(results))
        assertEquals(txn1Errors, errorsForTransaction(response.getTransactionTopicResults("txn1")))
        assertEquals(txn2Errors, errorsForTransaction(response.getTransactionTopicResults("txn2")))

        val expectedErrors = mapOf(
            "txn1" to txn1Errors,
            "txn2" to txn2Errors,
        )

        assertEquals(expectedErrors, response.errors())
    }
}
