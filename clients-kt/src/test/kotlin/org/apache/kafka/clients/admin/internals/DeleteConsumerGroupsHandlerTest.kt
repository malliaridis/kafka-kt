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
import org.apache.kafka.common.errors.GroupAuthorizationException
import org.apache.kafka.common.errors.GroupIdNotFoundException
import org.apache.kafka.common.errors.GroupNotEmptyException
import org.apache.kafka.common.errors.InvalidGroupIdException
import org.apache.kafka.common.message.DeleteGroupsResponseData
import org.apache.kafka.common.message.DeleteGroupsResponseData.DeletableGroupResult
import org.apache.kafka.common.message.DeleteGroupsResponseData.DeletableGroupResultCollection
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.DeleteGroupsResponse
import org.apache.kafka.common.utils.LogContext
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class DeleteConsumerGroupsHandlerTest {

    private val logContext = LogContext()

    private val groupId1 = "group-id1"

    @Test
    fun testBuildRequest() {
        val handler = DeleteConsumerGroupsHandler(logContext)
        val request = handler.buildBatchedRequest(
            brokerId = 1,
            keys = setOf(CoordinatorKey.byGroupId(groupId1)),
        ).build()
        
        assertEquals(1, request.data().groupsNames.size)
        assertEquals(groupId1, request.data().groupsNames[0])
    }

    @Test
    fun testSuccessfulHandleResponse() {
        assertCompleted(handleWithError(Errors.NONE))
    }

    @Test
    fun testUnmappedHandleResponse() {
        assertUnmapped(handleWithError(Errors.NOT_COORDINATOR))
        assertUnmapped(handleWithError(Errors.COORDINATOR_NOT_AVAILABLE))
    }

    @Test
    fun testRetriableHandleResponse() {
        assertRetriable(handleWithError(Errors.COORDINATOR_LOAD_IN_PROGRESS))
    }

    @Test
    fun testFailedHandleResponse() {
        assertFailed(GroupAuthorizationException::class.java, handleWithError(Errors.GROUP_AUTHORIZATION_FAILED))
        assertFailed(GroupIdNotFoundException::class.java, handleWithError(Errors.GROUP_ID_NOT_FOUND))
        assertFailed(InvalidGroupIdException::class.java, handleWithError(Errors.INVALID_GROUP_ID))
        assertFailed(GroupNotEmptyException::class.java, handleWithError(Errors.NON_EMPTY_GROUP))
    }

    private fun buildResponse(error: Errors): DeleteGroupsResponse {
        return DeleteGroupsResponse(
            DeleteGroupsResponseData()
                .setResults(
                    DeletableGroupResultCollection(
                        listOf(
                            DeletableGroupResult()
                                .setErrorCode(error.code)
                                .setGroupId(groupId1)
                        ).iterator()
                    )
                )
        )
    }

    private fun handleWithError(
        error: Errors,
    ): ApiResult<CoordinatorKey, Unit> {
        val handler = DeleteConsumerGroupsHandler(logContext)
        val response = buildResponse(error)
        return handler.handleResponse(
            broker = Node(id = 1, host = "host", port = 1234),
            keys = setOf(CoordinatorKey.byGroupId(groupId1)),
            response = response,
        )
    }

    private fun assertUnmapped(
        result: ApiResult<CoordinatorKey, Unit>,
    ) {
        assertEquals(emptySet(), result.completedKeys.keys)
        assertEquals(emptySet(), result.failedKeys.keys)
        assertEquals(listOf(CoordinatorKey.byGroupId(groupId1)), result.unmappedKeys)
    }

    private fun assertRetriable(
        result: ApiResult<CoordinatorKey, Unit>,
    ) {
        assertEquals(emptySet(), result.completedKeys.keys)
        assertEquals(emptySet(), result.failedKeys.keys)
        assertEquals(emptyList(), result.unmappedKeys)
    }

    private fun assertCompleted(
        result: ApiResult<CoordinatorKey, Unit>,
    ) {
        val key = CoordinatorKey.byGroupId(groupId1)
        assertEquals(emptySet(), result.failedKeys.keys)
        assertEquals(emptyList(), result.unmappedKeys)
        assertEquals(setOf(key), result.completedKeys.keys)
    }

    private fun assertFailed(
        expectedExceptionType: Class<out Throwable>,
        result: ApiResult<CoordinatorKey, Unit>,
    ) {
        val key = CoordinatorKey.byGroupId(groupId1)
        assertEquals(emptySet<Any>(), result.completedKeys.keys)
        assertEquals(emptyList<Any>(), result.unmappedKeys)
        assertEquals(setOf(key), result.failedKeys.keys)
        assertTrue(expectedExceptionType.isInstance(result.failedKeys[key]))
    }
}
