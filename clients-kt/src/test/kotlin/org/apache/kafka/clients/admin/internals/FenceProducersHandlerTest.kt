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

import org.apache.kafka.clients.admin.internals.AdminApiHandler.ApiResult
import org.apache.kafka.common.Node
import org.apache.kafka.common.message.InitProducerIdResponseData
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.InitProducerIdResponse
import org.apache.kafka.common.utils.LogContext
import org.apache.kafka.common.utils.ProducerIdAndEpoch
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class FenceProducersHandlerTest {

    private val logContext = LogContext()

    private val node = Node(id = 1, host = "host", port = 1234)

    @Test
    fun testBuildRequest() {
        val handler = FenceProducersHandler(logContext)
        setOf("foo", "bar", "baz").forEach { transactionalId -> assertLookup(handler, transactionalId) }
    }

    @Test
    fun testHandleSuccessfulResponse() {
        val transactionalId = "foo"
        val key = CoordinatorKey.byTransactionalId(transactionalId)
        val handler = FenceProducersHandler(logContext)
        val epoch: Short = 57
        val producerId: Long = 7
        val response = InitProducerIdResponse(
            InitProducerIdResponseData()
                .setProducerEpoch(epoch)
                .setProducerId(producerId)
        )
        val result = handler.handleSingleResponse(node, key, response)
        assertEquals(emptyList(), result.unmappedKeys)
        assertEquals(emptyMap(), result.failedKeys)
        assertEquals(setOf(key), result.completedKeys.keys)
        val expected = ProducerIdAndEpoch(producerId, epoch)
        assertEquals(expected, result.completedKeys[key])
    }

    @Test
    fun testHandleErrorResponse() {
        val transactionalId = "foo"
        val handler = FenceProducersHandler(logContext)

        assertFatalError(handler, transactionalId, Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED)
        assertFatalError(handler, transactionalId, Errors.CLUSTER_AUTHORIZATION_FAILED)
        assertFatalError(handler, transactionalId, Errors.UNKNOWN_SERVER_ERROR)
        assertFatalError(handler, transactionalId, Errors.PRODUCER_FENCED)
        assertFatalError(handler, transactionalId, Errors.TRANSACTIONAL_ID_NOT_FOUND)
        assertFatalError(handler, transactionalId, Errors.INVALID_PRODUCER_EPOCH)

        assertRetriableError(handler, transactionalId, Errors.COORDINATOR_LOAD_IN_PROGRESS)
        assertUnmappedKey(handler, transactionalId, Errors.NOT_COORDINATOR)
        assertUnmappedKey(handler, transactionalId, Errors.COORDINATOR_NOT_AVAILABLE)
    }

    private fun assertFatalError(
        handler: FenceProducersHandler,
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
        handler: FenceProducersHandler,
        transactionalId: String,
        error: Errors,
    ) {
        val result = handleResponseError(handler, transactionalId, error)

        assertEquals(emptyList(), result.unmappedKeys)
        assertEquals(emptyMap(), result.failedKeys)
    }

    private fun assertUnmappedKey(
        handler: FenceProducersHandler,
        transactionalId: String,
        error: Errors,
    ) {
        val key = CoordinatorKey.byTransactionalId(transactionalId)
        val result = handleResponseError(handler, transactionalId, error)

        assertEquals(emptyMap(), result.failedKeys)
        assertEquals(listOf(key), result.unmappedKeys)
    }

    private fun handleResponseError(
        handler: FenceProducersHandler,
        transactionalId: String,
        error: Errors,
    ): ApiResult<CoordinatorKey, ProducerIdAndEpoch> {
        val brokerId = 1
        val key = CoordinatorKey.byTransactionalId(transactionalId)
        val keys = setOf(key)
        val response = InitProducerIdResponse(
            InitProducerIdResponseData().setErrorCode(error.code)
        )
        val result = handler.handleResponse(node, keys, response)

        assertEquals(emptyMap(), result.completedKeys)

        return result
    }

    private fun assertLookup(handler: FenceProducersHandler, transactionalId: String) {
        val key = CoordinatorKey.byTransactionalId(transactionalId)
        val request = handler.buildSingleRequest(1, key)

        assertEquals(transactionalId, request.data.transactionalId)
        assertEquals(1, request.data.transactionTimeoutMs)
    }
}
