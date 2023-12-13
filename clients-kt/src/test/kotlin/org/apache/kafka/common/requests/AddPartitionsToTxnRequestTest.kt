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
import org.apache.kafka.common.message.AddPartitionsToTxnRequestData.AddPartitionsToTxnTopic
import org.apache.kafka.common.message.AddPartitionsToTxnRequestData.AddPartitionsToTxnTopicCollection
import org.apache.kafka.common.message.AddPartitionsToTxnRequestData.AddPartitionsToTxnTransaction
import org.apache.kafka.common.message.AddPartitionsToTxnRequestData.AddPartitionsToTxnTransactionCollection
import org.apache.kafka.common.message.AddPartitionsToTxnResponseData
import org.apache.kafka.common.message.AddPartitionsToTxnResponseData.AddPartitionsToTxnResultCollection
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.AddPartitionsToTxnResponse.Companion.errorsForTransaction
import org.apache.kafka.common.utils.annotation.ApiKeyVersionsSource
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import kotlin.test.assertEquals
import kotlin.test.assertNotNull

class AddPartitionsToTxnRequestTest {

    @ParameterizedTest
    @ApiKeyVersionsSource(apiKey = ApiKeys.ADD_PARTITIONS_TO_TXN)
    fun testConstructor(version: Short) {
        val request: AddPartitionsToTxnRequest

        if (version < 4) {
            val partitions: MutableList<TopicPartition> = ArrayList()
            partitions.add(tp0)
            partitions.add(tp1)
            
            val builder = AddPartitionsToTxnRequest.Builder.forClient(
                TRANSACTIONAL_ID_1,
                PRODUCER_ID,
                PRODUCER_EPOCH,
                partitions
            )
            request = builder.build(version)
            
            assertEquals(TRANSACTIONAL_ID_1, request.data().v3AndBelowTransactionalId)
            assertEquals(PRODUCER_ID, request.data().v3AndBelowProducerId)
            assertEquals(PRODUCER_EPOCH, request.data().v3AndBelowProducerEpoch)
            assertEquals(partitions, AddPartitionsToTxnRequest.getPartitions(request.data().v3AndBelowTopics))
        } else {
            val transactions = createTwoTransactionCollection()
            
            val builder = AddPartitionsToTxnRequest.Builder.forBroker(transactions)
            request = builder.build(version)
            
            val reqTxn1 = request.data().transactions.find(TRANSACTIONAL_ID_1)
            val reqTxn2 = request.data().transactions.find(TRANSACTIONAL_ID_2)
            
            assertEquals(transactions.find(TRANSACTIONAL_ID_1), reqTxn1)
            assertEquals(transactions.find(TRANSACTIONAL_ID_2), reqTxn2)
        }
        val response = request.getErrorResponse(
            throttleTimeMs = THROTTLE_TIME_MS,
            e = Errors.UNKNOWN_TOPIC_OR_PARTITION.exception!!
        )

        assertEquals(THROTTLE_TIME_MS, response.throttleTimeMs())

        if (version >= 4) {
            assertEquals(Errors.UNKNOWN_TOPIC_OR_PARTITION.code, response.data().errorCode)
            // Since the error is top level, we count it as one error in the counts.
            assertEquals(mapOf(Errors.UNKNOWN_TOPIC_OR_PARTITION to 1), response.errorCounts())
        } else assertEquals(mapOf(Errors.UNKNOWN_TOPIC_OR_PARTITION to 2), response.errorCounts())
    }

    @Test
    fun testBatchedRequests() {
        val transactions = createTwoTransactionCollection()

        val builder = AddPartitionsToTxnRequest.Builder.forBroker(transactions)
        val request = builder.build(ApiKeys.ADD_PARTITIONS_TO_TXN.latestVersion())

        val expectedMap = mapOf(
            TRANSACTIONAL_ID_1 to listOf(tp0),
            TRANSACTIONAL_ID_2 to listOf(tp1),
        )

        assertEquals(expectedMap, request.partitionsByTransaction())

        val results = AddPartitionsToTxnResultCollection()
        results.add(
            request.errorResponseForTransaction(
                transactionalId = TRANSACTIONAL_ID_1,
                e = Errors.UNKNOWN_TOPIC_OR_PARTITION,
            )
        )
        results.add(
            request.errorResponseForTransaction(
                transactionalId = TRANSACTIONAL_ID_2,
                e = Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED,
            )
        )

        val response = AddPartitionsToTxnResponse(
            AddPartitionsToTxnResponseData()
                .setResultsByTransaction(results)
                .setThrottleTimeMs(THROTTLE_TIME_MS)
        )

        assertEquals(
            expected = mapOf(tp0 to Errors.UNKNOWN_TOPIC_OR_PARTITION),
            actual = errorsForTransaction(response.getTransactionTopicResults(TRANSACTIONAL_ID_1))
        )
        assertEquals(
            expected = mapOf(tp1 to Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED),
            actual = errorsForTransaction(response.getTransactionTopicResults(TRANSACTIONAL_ID_2))
        )
    }

    @Test
    fun testNormalizeRequest() {
        val partitions = listOf(tp0, tp1)
        val builder = AddPartitionsToTxnRequest.Builder.forClient(
            transactionalId = TRANSACTIONAL_ID_1,
            producerId = PRODUCER_ID,
            producerEpoch = PRODUCER_EPOCH,
            partitions = partitions
        )
        val request = builder.build(version = 3)
        val singleton = request.normalizeRequest()
        assertEquals(partitions, singleton.partitionsByTransaction()[TRANSACTIONAL_ID_1])

        val transaction = assertNotNull(singleton.data().transactions.find(TRANSACTIONAL_ID_1))
        assertEquals(PRODUCER_ID, transaction.producerId)
        assertEquals(PRODUCER_EPOCH, transaction.producerEpoch)
    }

    private fun createTwoTransactionCollection(): AddPartitionsToTxnTransactionCollection {
        val topics0 = AddPartitionsToTxnTopicCollection()
        topics0.add(
            AddPartitionsToTxnTopic()
                .setName(tp0.topic)
                .setPartitions(intArrayOf(tp0.partition))
        )
        val topics1 = AddPartitionsToTxnTopicCollection()
        topics1.add(
            AddPartitionsToTxnTopic()
                .setName(tp1.topic)
                .setPartitions(intArrayOf(tp1.partition))
        )
        val transactions = AddPartitionsToTxnTransactionCollection()
        transactions.add(
            AddPartitionsToTxnTransaction()
                .setTransactionalId(TRANSACTIONAL_ID_1)
                .setProducerId(PRODUCER_ID)
                .setProducerEpoch(PRODUCER_EPOCH)
                .setVerifyOnly(true)
                .setTopics(topics0)
        )
        transactions.add(
            AddPartitionsToTxnTransaction()
                .setTransactionalId(TRANSACTIONAL_ID_2)
                .setProducerId(PRODUCER_ID + 1)
                .setProducerEpoch((PRODUCER_EPOCH + 1).toShort())
                .setVerifyOnly(false)
                .setTopics(topics1)
        )
        return transactions
    }

    companion object {

        private const val TRANSACTIONAL_ID_1 = "transaction1"
        
        private const val TRANSACTIONAL_ID_2 = "transaction2"

        private const val PRODUCER_ID = 10L

        private const val PRODUCER_EPOCH: Short = 1

        private const val THROTTLE_TIME_MS = 10
        
        private val tp0 = TopicPartition("topic", 0)
        
        private val tp1 = TopicPartition("topic", 1)
    }
}
