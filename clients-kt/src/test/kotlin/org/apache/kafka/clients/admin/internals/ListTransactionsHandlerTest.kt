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

import org.apache.kafka.clients.admin.ListTransactionsOptions
import org.apache.kafka.clients.admin.TransactionListing
import org.apache.kafka.clients.admin.TransactionState
import org.apache.kafka.clients.admin.internals.AdminApiHandler.ApiResult
import org.apache.kafka.clients.admin.internals.AllBrokersStrategy.BrokerKey
import org.apache.kafka.common.Node
import org.apache.kafka.common.message.ListTransactionsResponseData
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.ListTransactionsResponse
import org.apache.kafka.common.utils.LogContext
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull

class ListTransactionsHandlerTest {

    private val logContext = LogContext()

    private val node = Node(1, "host", 1234)

    @Test
    fun testBuildRequestWithoutFilters() {
        val brokerId = 1
        val brokerKey = BrokerKey(brokerId)
        val options = ListTransactionsOptions()
        val handler = ListTransactionsHandler(options, logContext)
        val request = handler.buildBatchedRequest(brokerId, setOf(brokerKey)).build()
        
        assertEquals(longArrayOf(), request.data().producerIdFilters)
        assertEquals(emptyList(), request.data().stateFilters)
    }

    @Test
    fun testBuildRequestWithFilteredProducerId() {
        val brokerId = 1
        val brokerKey = BrokerKey(brokerId)
        val filteredProducerId = 23423L
        val options = ListTransactionsOptions()
            .filterProducerIds(setOf(filteredProducerId))
        val handler = ListTransactionsHandler(options, logContext)
        val request = handler.buildBatchedRequest(brokerId, setOf(brokerKey)).build()
        
        assertEquals(longArrayOf(filteredProducerId), request.data().producerIdFilters)
        assertEquals(emptyList(), request.data().stateFilters)
    }

    @Test
    fun testBuildRequestWithFilteredState() {
        val brokerId = 1
        val brokerKey = BrokerKey(brokerId)
        val filteredState = TransactionState.ONGOING
        val options = ListTransactionsOptions().filterStates(setOf(filteredState))
        val handler = ListTransactionsHandler(options, logContext)
        val request = handler.buildBatchedRequest(brokerId, setOf(brokerKey)).build()
        
        assertEquals(listOf(filteredState.toString()), request.data().stateFilters)
        assertEquals(longArrayOf(), request.data().producerIdFilters)
    }

    @Test
    fun testHandleSuccessfulResponse() {
        val brokerId = 1
        val brokerKey = BrokerKey(brokerId)
        val options = ListTransactionsOptions()
        val handler = ListTransactionsHandler(options, logContext)
        val response = sampleListTransactionsResponse1()
        val result = handler.handleResponse(node, setOf(brokerKey), response)
        assertEquals(setOf(brokerKey), result.completedKeys.keys)
        assertExpectedTransactions(response.data().transactionStates, result.completedKeys[brokerKey]!!)
    }

    @Test
    fun testCoordinatorLoadingErrorIsRetriable() {
        val brokerId = 1
        val result = handleResponseWithError(brokerId, Errors.COORDINATOR_LOAD_IN_PROGRESS)
        assertEquals(emptyMap(), result.completedKeys)
        assertEquals(emptyMap(), result.failedKeys)
        assertEquals(emptyList(), result.unmappedKeys)
    }

    @Test
    fun testHandleResponseWithFatalErrors() {
        assertFatalError(Errors.COORDINATOR_NOT_AVAILABLE)
        assertFatalError(Errors.UNKNOWN_SERVER_ERROR)
    }

    private fun assertFatalError(
        error: Errors,
    ) {
        val brokerId = 1
        val brokerKey = BrokerKey(brokerId)
        val result = handleResponseWithError(brokerId, error)

        assertEquals(emptyMap(), result.completedKeys)
        assertEquals(emptyList(), result.unmappedKeys)
        assertEquals(setOf(brokerKey), result.failedKeys.keys)

        val throwable = result.failedKeys[brokerKey]
        assertEquals(error, Errors.forException(throwable))
    }

    private fun handleResponseWithError(
        brokerId: Int,
        error: Errors,
    ): ApiResult<BrokerKey, Collection<TransactionListing>> {
        val brokerKey = BrokerKey(brokerId)
        val options = ListTransactionsOptions()
        val handler = ListTransactionsHandler(options, logContext)
        val response = ListTransactionsResponse(
            ListTransactionsResponseData().setErrorCode(error.code)
        )
        return handler.handleResponse(node, setOf(brokerKey), response)
    }

    private fun sampleListTransactionsResponse1(): ListTransactionsResponse {
        return ListTransactionsResponse(
            ListTransactionsResponseData()
                .setErrorCode(Errors.NONE.code)
                .setTransactionStates(
                    listOf(
                        ListTransactionsResponseData.TransactionState()
                            .setTransactionalId("foo")
                            .setProducerId(12345L)
                            .setTransactionState("Ongoing"),
                        ListTransactionsResponseData.TransactionState()
                            .setTransactionalId("bar")
                            .setProducerId(98765L)
                            .setTransactionState("PrepareAbort"),
                    )
                )
        )
    }

    private fun assertExpectedTransactions(
        expected: List<ListTransactionsResponseData.TransactionState>,
        actual: Collection<TransactionListing>,
    ) {
        assertEquals(expected.size, actual.size)
        val expectedMap = expected.associateBy(ListTransactionsResponseData.TransactionState::transactionalId)
        for (actualListing in actual) {
            val expectedState = expectedMap[actualListing.transactionalId]
            assertNotNull(expectedState)
            assertExpectedTransactionState(expectedState, actualListing)
        }
    }

    private fun assertExpectedTransactionState(
        expected: ListTransactionsResponseData.TransactionState,
        actual: TransactionListing,
    ) {
        assertEquals(expected.transactionalId, actual.transactionalId)
        assertEquals(expected.producerId, actual.producerId)
        assertEquals(expected.transactionState, actual.state.toString())
    }
}
