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

package org.apache.kafka.clients.consumer.internals

import org.apache.kafka.clients.ClientResponse
import org.apache.kafka.clients.consumer.internals.NetworkClientDelegate.UnsentRequest
import org.apache.kafka.common.Node
import org.apache.kafka.common.errors.GroupAuthorizationException
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.FindCoordinatorRequest
import org.apache.kafka.common.requests.FindCoordinatorResponse
import org.apache.kafka.common.requests.RequestHeader
import org.apache.kafka.common.utils.LogContext
import org.apache.kafka.common.utils.MockTime
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.kotlin.argThat
import org.mockito.kotlin.mock
import org.mockito.kotlin.verify
import org.mockito.kotlin.verifyNoInteractions
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertIs
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

class CoordinatorRequestManagerTest {
    
    private lateinit var time: MockTime
    
    private lateinit var errorEventHandler: ErrorEventHandler
    
    private lateinit var node: Node
    
    @BeforeEach
    fun setup() {
        time = MockTime(0)
        node = Node(1, "localhost", 9092)
        errorEventHandler = mock<ErrorEventHandler>()
    }

    @Test
    fun testSuccessfulResponse() {
        val coordinatorManager = setupCoordinatorManager(GROUP_ID)
        expectFindCoordinatorRequest(coordinatorManager, Errors.NONE)
        
        val coordinator = assertNotNull(coordinatorManager.coordinator)
        assertEquals(Int.MAX_VALUE - node.id, coordinator.id)
        assertEquals(node.host, coordinator.host)
        assertEquals(node.port, coordinator.port)
        
        val (_, unsentRequests) = coordinatorManager.poll(time.milliseconds())
        assertTrue(unsentRequests.isEmpty())
    }

    @Test
    fun testMarkCoordinatorUnknown() {
        val coordinatorManager = setupCoordinatorManager(GROUP_ID)
        
        expectFindCoordinatorRequest(coordinatorManager, Errors.NONE)
        assertNotNull(coordinatorManager.coordinator)

        // It may take time for metadata to converge between after a coordinator has
        // been demoted. This can cause a tight loop in which FindCoordinator continues to
        // return node X while that node continues to reply with NOT_COORDINATOR. Hence we
        // still want to ensure a backoff after successfully finding the coordinator.
        coordinatorManager.markCoordinatorUnknown("coordinator changed", time.milliseconds())
        assertTrue(coordinatorManager.poll(time.milliseconds()).unsentRequests.isEmpty())
        
        time.sleep((RETRY_BACKOFF_MS - 1).toLong())
        assertTrue(coordinatorManager.poll(time.milliseconds()).unsentRequests.isEmpty())
        
        time.sleep(RETRY_BACKOFF_MS.toLong())
        expectFindCoordinatorRequest(coordinatorManager, Errors.NONE)
        assertNotNull(coordinatorManager.coordinator)
    }

    @Test
    fun testBackoffAfterRetriableFailure() {
        val coordinatorManager = setupCoordinatorManager(GROUP_ID)
        expectFindCoordinatorRequest(coordinatorManager, Errors.COORDINATOR_LOAD_IN_PROGRESS)
        verifyNoInteractions(errorEventHandler)
        
        time.sleep((RETRY_BACKOFF_MS - 1).toLong())
        assertTrue(coordinatorManager.poll(time.milliseconds()).unsentRequests.isEmpty())
        
        time.sleep(1)
        expectFindCoordinatorRequest(coordinatorManager, Errors.NONE)
    }

    @Test
    fun testPropagateAndBackoffAfterFatalError() {
        val coordinatorManager = setupCoordinatorManager(GROUP_ID)
        expectFindCoordinatorRequest(coordinatorManager, Errors.GROUP_AUTHORIZATION_FAILED)
        
        verify(errorEventHandler).handle(argThat { exception ->
            if (exception !is GroupAuthorizationException) return@argThat false
            exception.groupId == GROUP_ID
        })
        
        time.sleep((RETRY_BACKOFF_MS - 1).toLong())
        assertTrue(coordinatorManager.poll(time.milliseconds()).unsentRequests.isEmpty())
        
        time.sleep(1)
        assertEquals(1, coordinatorManager.poll(time.milliseconds()).unsentRequests.size)
        assertNull(coordinatorManager.coordinator)
    }

    @Test
    fun testNullGroupIdShouldThrow() {
        assertFailsWith<RuntimeException> { setupCoordinatorManager(null) }
    }

    @Test
    fun testFindCoordinatorResponseVersions() {
        // v4
        val respNew = FindCoordinatorResponse.prepareResponse(
            Errors.NONE, GROUP_ID,
            node
        )
        var coordinator = assertNotNull(respNew.coordinatorByKey(GROUP_ID))
        assertEquals(GROUP_ID, coordinator.key)
        assertEquals(node.id, coordinator.nodeId)

        // <= v3
        val respOld = FindCoordinatorResponse.prepareOldResponse(Errors.NONE, node)
        coordinator = assertNotNull(respOld.coordinatorByKey(GROUP_ID))
        assertEquals(node.id, coordinator.nodeId)
    }

    private fun expectFindCoordinatorRequest(
        coordinatorManager: CoordinatorRequestManager,
        error: Errors,
    ) {
        val result = coordinatorManager.poll(time.milliseconds())
        assertEquals(1, result.unsentRequests.size)
        val unsentRequest = result.unsentRequests[0]
        unsentRequest.future.complete(buildResponse(unsentRequest, error))
        val expectCoordinatorFound = error == Errors.NONE
        assertEquals(expectCoordinatorFound, coordinatorManager.coordinator != null)
    }

    private fun setupCoordinatorManager(groupId: String?): CoordinatorRequestManager {
        return CoordinatorRequestManager(
            time = time,
            logContext = LogContext(),
            retryBackoffMs = RETRY_BACKOFF_MS.toLong(),
            nonRetriableErrorHandler = errorEventHandler,
            groupId = groupId!!
        )
    }

    private fun buildResponse(
        request: UnsentRequest,
        error: Errors,
    ): ClientResponse {
        val abstractRequest = request.requestBuilder.build()
        val findCoordinatorRequest = assertIs<FindCoordinatorRequest>(abstractRequest)

        val findCoordinatorResponse =
            FindCoordinatorResponse.prepareResponse(error, GROUP_ID, node)

        return ClientResponse(
            requestHeader = RequestHeader(
                requestApiKey = ApiKeys.FIND_COORDINATOR,
                requestVersion = findCoordinatorRequest.version,
                clientId = "",
                correlationId = 1,
            ),
            callback = request.callback,
            destination = node.idString(),
            createdTimeMs = time.milliseconds(),
            receivedTimeMs = time.milliseconds(),
            disconnected = false,
            versionMismatch = null,
            authenticationException = null,
            responseBody = findCoordinatorResponse,
        )
    }

    companion object {

        private const val RETRY_BACKOFF_MS = 500

        private const val GROUP_ID = "group-1"
    }
}
