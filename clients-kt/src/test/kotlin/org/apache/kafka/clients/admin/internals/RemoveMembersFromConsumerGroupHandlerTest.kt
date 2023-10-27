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
import org.apache.kafka.common.errors.UnknownServerException
import org.apache.kafka.common.message.LeaveGroupRequestData.MemberIdentity
import org.apache.kafka.common.message.LeaveGroupResponseData
import org.apache.kafka.common.message.LeaveGroupResponseData.MemberResponse
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.LeaveGroupResponse
import org.apache.kafka.common.utils.LogContext
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class RemoveMembersFromConsumerGroupHandlerTest {
    
    private val logContext = LogContext()
    
    private val groupId = "group-id"
    
    private val m1 = MemberIdentity()
        .setMemberId("m1")
        .setGroupInstanceId("m1-gii")
    
    private val m2 = MemberIdentity()
        .setMemberId("m2")
        .setGroupInstanceId("m2-gii")
    
    private val members = listOf(m1, m2)
    
    @Test
    fun testBuildRequest() {
        val handler = RemoveMembersFromConsumerGroupHandler(groupId, members, logContext)
        val request = handler.buildBatchedRequest(1, setOf(CoordinatorKey.byGroupId(groupId))).build()
        assertEquals(groupId, request.data().groupId)
        assertEquals(2, request.data().members.size)
    }

    @Test
    fun testSuccessfulHandleResponse() {
        val responseData = mapOf(m1 to Errors.NONE)
        assertCompleted(handleWithGroupError(Errors.NONE), responseData)
    }

    @Test
    fun testUnmappedHandleResponse() {
        assertUnmapped(handleWithGroupError(Errors.COORDINATOR_NOT_AVAILABLE))
        assertUnmapped(handleWithGroupError(Errors.NOT_COORDINATOR))
    }

    @Test
    fun testRetriableHandleResponse() {
        assertRetriable(handleWithGroupError(Errors.COORDINATOR_LOAD_IN_PROGRESS))
    }

    @Test
    fun testFailedHandleResponse() {
        assertFailed(GroupAuthorizationException::class.java, handleWithGroupError(Errors.GROUP_AUTHORIZATION_FAILED))
        assertFailed(UnknownServerException::class.java, handleWithGroupError(Errors.UNKNOWN_SERVER_ERROR))
    }

    @Test
    fun testFailedHandleResponseInMemberLevel() {
        assertMemberFailed(Errors.FENCED_INSTANCE_ID, handleWithMemberError(Errors.FENCED_INSTANCE_ID))
        assertMemberFailed(Errors.UNKNOWN_MEMBER_ID, handleWithMemberError(Errors.UNKNOWN_MEMBER_ID))
    }

    private fun buildResponse(error: Errors): LeaveGroupResponse {
        return LeaveGroupResponse(
            LeaveGroupResponseData()
                .setErrorCode(error.code)
                .setMembers(
                    listOf(
                        MemberResponse()
                            .setErrorCode(Errors.NONE.code)
                            .setMemberId("m1")
                            .setGroupInstanceId("m1-gii")
                    )
                )
        )
    }

    private fun buildResponseWithMemberError(error: Errors): LeaveGroupResponse {
        return LeaveGroupResponse(
            LeaveGroupResponseData()
                .setErrorCode(Errors.NONE.code)
                .setMembers(
                    listOf(
                        MemberResponse()
                            .setErrorCode(error.code)
                            .setMemberId("m1")
                            .setGroupInstanceId("m1-gii")
                    )
                )
        )
    }

    private fun handleWithGroupError(
        error: Errors,
    ): ApiResult<CoordinatorKey, Map<MemberIdentity, Errors>> {
        val handler = RemoveMembersFromConsumerGroupHandler(groupId, members, logContext)
        val response = buildResponse(error)
        return handler.handleResponse(
            broker = Node(1, "host", 1234),
            keys = setOf(CoordinatorKey.byGroupId(groupId)),
            response = response,
        )
    }

    private fun handleWithMemberError(
        error: Errors,
    ): ApiResult<CoordinatorKey, Map<MemberIdentity, Errors>> {
        val handler = RemoveMembersFromConsumerGroupHandler(groupId, members, logContext)
        val response = buildResponseWithMemberError(error)
        return handler.handleResponse(
            broker = Node(1, "host", 1234),
            keys = setOf(CoordinatorKey.byGroupId(groupId)),
            response = response,
        )
    }

    private fun assertUnmapped(
        result: ApiResult<CoordinatorKey, Map<MemberIdentity, Errors>>,
    ) {
        assertEquals(emptySet(), result.completedKeys.keys)
        assertEquals(emptySet(), result.failedKeys.keys)
        assertEquals(listOf(CoordinatorKey.byGroupId(groupId)), result.unmappedKeys)
    }

    private fun assertRetriable(
        result: ApiResult<CoordinatorKey, Map<MemberIdentity, Errors>>,
    ) {
        assertEquals(emptySet(), result.completedKeys.keys)
        assertEquals(emptySet(), result.failedKeys.keys)
        assertEquals(emptyList(), result.unmappedKeys)
    }

    private fun assertCompleted(
        result: ApiResult<CoordinatorKey, Map<MemberIdentity, Errors>>,
        expected: Map<MemberIdentity, Errors>,
    ) {
        val key = CoordinatorKey.byGroupId(groupId)
        assertEquals(emptySet(), result.failedKeys.keys)
        assertEquals(emptyList(), result.unmappedKeys)
        assertEquals(setOf(key), result.completedKeys.keys)
        assertEquals(expected, result.completedKeys[key])
    }

    private fun assertFailed(
        expectedExceptionType: Class<out Throwable?>,
        result: ApiResult<CoordinatorKey, Map<MemberIdentity, Errors>>,
    ) {
        val key = CoordinatorKey.byGroupId(groupId)
        assertEquals(emptySet(), result.completedKeys.keys)
        assertEquals(emptyList(), result.unmappedKeys)
        assertEquals(setOf(key), result.failedKeys.keys)
        assertTrue(expectedExceptionType.isInstance(result.failedKeys[key]))
    }

    private fun assertMemberFailed(
        expectedError: Errors,
        result: ApiResult<CoordinatorKey, Map<MemberIdentity, Errors>>,
    ) {
        val expectedResponseData = mapOf(m1 to expectedError)
        val key = CoordinatorKey.byGroupId(groupId)
        assertEquals(emptySet(), result.failedKeys.keys)
        assertEquals(emptyList(), result.unmappedKeys)
        assertEquals(setOf(key), result.completedKeys.keys)
        assertEquals(expectedResponseData, result.completedKeys[key])
    }
}
