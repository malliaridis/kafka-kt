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

package org.apache.kafka.clients.admin.internals

import org.apache.kafka.clients.admin.TransactionDescription
import org.apache.kafka.clients.admin.internals.AdminApiHandler.ApiResult
import org.apache.kafka.common.Node
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.message.DescribeTransactionsResponseData
import org.apache.kafka.common.message.DescribeTransactionsResponseData.TopicDataCollection
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.DescribeTransactionsResponse
import org.apache.kafka.common.utils.LogContext
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class DescribeTransactionsHandlerTest {
    
    private val logContext = LogContext()
    
    private val node = Node(id = 1, host = "host", port = 1234)
    
    @Test
    fun testBuildRequest() {
        val transactionalId1 = "foo"
        val transactionalId2 = "bar"
        val transactionalId3 = "baz"
        val transactionalIds = setOf(transactionalId1, transactionalId2, transactionalId3)
        val handler = DescribeTransactionsHandler(logContext)
        assertLookup(handler, transactionalIds)
        assertLookup(handler, setOf(transactionalId1))
        assertLookup(handler, setOf(transactionalId2, transactionalId3))
    }

    @Test
    fun testHandleSuccessfulResponse() {
        val transactionalId1 = "foo"
        val transactionalId2 = "bar"
        val transactionalIds: Set<String> = setOf(transactionalId1, transactionalId2)
        val handler = DescribeTransactionsHandler(logContext)
        val transactionState1 = sampleTransactionState1(transactionalId1)
        val transactionState2 = sampleTransactionState2(transactionalId2)
        val keys = coordinatorKeys(transactionalIds)
        val response = DescribeTransactionsResponse(
            DescribeTransactionsResponseData()
                .setTransactionStates(listOf(transactionState1, transactionState2))
        )
        val result = handler.handleResponse(node, keys, response)
        
        assertEquals(keys, result.completedKeys.keys)
        assertMatchingTransactionState(
            expectedCoordinatorId = node.id,
            expected = transactionState1,
            actual = result.completedKeys[CoordinatorKey.byTransactionalId(transactionalId1)],
        )
        assertMatchingTransactionState(
            expectedCoordinatorId = node.id,
            expected = transactionState2,
            actual = result.completedKeys[CoordinatorKey.byTransactionalId(transactionalId2)],
        )
    }

    @Test
    fun testHandleErrorResponse() {
        val transactionalId = "foo"
        val handler = DescribeTransactionsHandler(logContext)
        
        assertFatalError(handler, transactionalId, Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED)
        assertFatalError(handler, transactionalId, Errors.TRANSACTIONAL_ID_NOT_FOUND)
        assertFatalError(handler, transactionalId, Errors.UNKNOWN_SERVER_ERROR)
        assertRetriableError(handler, transactionalId, Errors.COORDINATOR_LOAD_IN_PROGRESS)
        assertUnmappedKey(handler, transactionalId, Errors.NOT_COORDINATOR)
        assertUnmappedKey(handler, transactionalId, Errors.COORDINATOR_NOT_AVAILABLE)
    }

    private fun assertFatalError(
        handler: DescribeTransactionsHandler,
        transactionalId: String,
        error: Errors,
    ) {
        val key = CoordinatorKey.byTransactionalId(transactionalId)
        val result = handleResponseError(handler, transactionalId, error)

        assertEquals(emptyList(), result.unmappedKeys)
        assertEquals(setOf(key), result.failedKeys.keys)

        val throwable = result.failedKeys[key]

        assertTrue(error.exception!!.javaClass.isInstance(throwable))
    }

    private fun assertRetriableError(
        handler: DescribeTransactionsHandler,
        transactionalId: String,
        error: Errors,
    ) {
        val result = handleResponseError(handler, transactionalId, error)

        assertEquals(emptyList(), result.unmappedKeys)
        assertEquals(emptyMap(), result.failedKeys)
    }

    private fun assertUnmappedKey(
        handler: DescribeTransactionsHandler,
        transactionalId: String,
        error: Errors,
    ) {
        val key = CoordinatorKey.byTransactionalId(transactionalId)
        val result = handleResponseError(handler, transactionalId, error)

        assertEquals(emptyMap(), result.failedKeys)
        assertEquals(listOf(key), result.unmappedKeys)
    }

    private fun handleResponseError(
        handler: DescribeTransactionsHandler,
        transactionalId: String,
        error: Errors,
    ): ApiResult<CoordinatorKey, TransactionDescription> {
        val key = CoordinatorKey.byTransactionalId(transactionalId)
        val keys = setOf(key)
        val transactionState = DescribeTransactionsResponseData.TransactionState()
            .setErrorCode(error.code)
            .setTransactionalId(transactionalId)
        val response = DescribeTransactionsResponse(
            DescribeTransactionsResponseData()
                .setTransactionStates(listOf(transactionState))
        )
        val result = handler.handleResponse(node, keys, response)

        assertEquals(emptyMap(), result.completedKeys)

        return result
    }

    private fun assertLookup(
        handler: DescribeTransactionsHandler,
        transactionalIds: Set<String>,
    ) {
        val keys = coordinatorKeys(transactionalIds)
        val request = handler.buildBatchedRequest(1, keys)
        assertEquals(transactionalIds, request.data.transactionalIds.toSet())
    }

    private fun sampleTransactionState1(transactionalId: String): DescribeTransactionsResponseData.TransactionState {
        return DescribeTransactionsResponseData.TransactionState()
            .setErrorCode(Errors.NONE.code)
            .setTransactionState("Ongoing")
            .setTransactionalId(transactionalId)
            .setProducerId(12345L)
            .setProducerEpoch(15.toShort())
            .setTransactionStartTimeMs(1599151791L)
            .setTransactionTimeoutMs(10000)
            .setTopics(
                TopicDataCollection(
                    listOf(
                        DescribeTransactionsResponseData.TopicData()
                            .setTopic("foo")
                            .setPartitions(intArrayOf(1, 3, 5)),
                        DescribeTransactionsResponseData.TopicData()
                            .setTopic("bar")
                            .setPartitions(intArrayOf(1, 3, 5)),
                    ).iterator()
                )
            )
    }

    private fun sampleTransactionState2(transactionalId: String): DescribeTransactionsResponseData.TransactionState {
        return DescribeTransactionsResponseData.TransactionState()
            .setErrorCode(Errors.NONE.code)
            .setTransactionState("Empty")
            .setTransactionalId(transactionalId)
            .setProducerId(98765L)
            .setProducerEpoch(30.toShort())
            .setTransactionStartTimeMs(-1)
    }

    private fun assertMatchingTransactionState(
        expectedCoordinatorId: Int,
        expected: DescribeTransactionsResponseData.TransactionState,
        actual: TransactionDescription?,
    ) {
        assertEquals(expectedCoordinatorId, actual?.coordinatorId)
        assertEquals(expected.producerId, actual?.producerId)
        assertEquals(expected.producerEpoch.toInt(), actual?.producerEpoch)
        assertEquals(expected.transactionTimeoutMs.toLong(), actual?.transactionTimeoutMs)
        assertEquals(expected.transactionStartTimeMs, actual?.transactionStartTimeMs ?: -1)
        assertEquals(collectTransactionPartitions(expected), actual?.topicPartitions)
    }

    private fun collectTransactionPartitions(
        transactionState: DescribeTransactionsResponseData.TransactionState,
    ): Set<TopicPartition> {
        val topicPartitions = mutableSetOf<TopicPartition>()
        for (topicData in transactionState.topics)
            for (partitionId in topicData.partitions)
                topicPartitions.add(TopicPartition(topicData.topic, partitionId))

        return topicPartitions
    }

    companion object {

        private fun coordinatorKeys(transactionalIds: Set<String>): Set<CoordinatorKey> =
            transactionalIds.map(CoordinatorKey::byTransactionalId).toSet()
    }
}
