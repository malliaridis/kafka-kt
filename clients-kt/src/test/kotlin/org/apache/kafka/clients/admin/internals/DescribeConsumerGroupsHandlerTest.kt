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

import org.apache.kafka.clients.admin.ConsumerGroupDescription
import org.apache.kafka.clients.admin.MemberAssignment
import org.apache.kafka.clients.admin.MemberDescription
import org.apache.kafka.clients.admin.internals.AdminApiHandler.ApiResult
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor
import org.apache.kafka.clients.consumer.internals.ConsumerProtocol.serializeAssignment
import org.apache.kafka.common.ConsumerGroupState
import org.apache.kafka.common.Node
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.GroupAuthorizationException
import org.apache.kafka.common.errors.GroupIdNotFoundException
import org.apache.kafka.common.errors.InvalidGroupIdException
import org.apache.kafka.common.message.DescribeGroupsResponseData
import org.apache.kafka.common.message.DescribeGroupsResponseData.DescribedGroup
import org.apache.kafka.common.message.DescribeGroupsResponseData.DescribedGroupMember
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.DescribeGroupsResponse
import org.apache.kafka.common.utils.LogContext
import org.apache.kafka.common.utils.Utils.to32BitField
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class DescribeConsumerGroupsHandlerTest {
    
    private val logContext = LogContext()
    
    private val groupId1 = "group-id1"
    
    private val groupId2 = "group-id2"
    
    private val groupIds = setOf(groupId1, groupId2)
    
    private val keys = groupIds.map(CoordinatorKey::byGroupId).toSet()
    
    private val coordinator = Node(1, "host", 1234)
    
    private val tps: Set<TopicPartition> = setOf(
        TopicPartition("foo", 0),
        TopicPartition("bar", 1),
    )

    @Test
    fun testBuildRequest() {
        var handler = DescribeConsumerGroupsHandler(false, logContext)
        var request = handler.buildBatchedRequest(1, keys).build()
        
        assertEquals(2, request.data().groups.size)
        assertFalse(request.data().includeAuthorizedOperations)
        
        handler = DescribeConsumerGroupsHandler(true, logContext)
        request = handler.buildBatchedRequest(1, keys).build()
        
        assertEquals(2, request.data().groups.size)
        assertTrue(request.data().includeAuthorizedOperations)
    }

    @Test
    fun testInvalidBuildRequest() {
        val handler = DescribeConsumerGroupsHandler(false, logContext)
        assertFailsWith<IllegalArgumentException> {
            handler.buildRequest(1, setOf(CoordinatorKey.byTransactionalId("tId")))
        }
    }

    @Test
    fun testSuccessfulHandleResponse() {
        val members = listOf(
            MemberDescription(
                memberId = "memberId",
                groupInstanceId = "clientId",
                clientId = "host",
                assignment = MemberAssignment(tps),
            )
        )
        val expected = ConsumerGroupDescription(
            groupId = groupId1,
            isSimpleConsumerGroup = true,
            members = members,
            partitionAssignor = "assignor",
            state = ConsumerGroupState.STABLE,
            coordinator = coordinator,
        )
        assertCompleted(handleWithError(Errors.NONE, ""), expected)
    }

    @Test
    fun testUnmappedHandleResponse() {
        assertUnmapped(handleWithError(Errors.COORDINATOR_NOT_AVAILABLE, ""))
        assertUnmapped(handleWithError(Errors.NOT_COORDINATOR, ""))
    }

    @Test
    fun testRetriableHandleResponse() {
        assertRetriable(handleWithError(Errors.COORDINATOR_LOAD_IN_PROGRESS, ""))
    }

    @Test
    fun testFailedHandleResponse() {
        assertFailed(GroupAuthorizationException::class.java, handleWithError(Errors.GROUP_AUTHORIZATION_FAILED, ""))
        assertFailed(GroupIdNotFoundException::class.java, handleWithError(Errors.GROUP_ID_NOT_FOUND, ""))
        assertFailed(InvalidGroupIdException::class.java, handleWithError(Errors.INVALID_GROUP_ID, ""))
        assertFailed(IllegalArgumentException::class.java, handleWithError(Errors.NONE, "custom-protocol"))
    }

    private fun buildResponse(error: Errors, protocolType: String): DescribeGroupsResponse {
        return DescribeGroupsResponse(
            DescribeGroupsResponseData()
                .setGroups(
                    listOf(
                        DescribedGroup()
                            .setErrorCode(error.code)
                            .setGroupId(groupId1)
                            .setGroupState(ConsumerGroupState.STABLE.toString())
                            .setProtocolType(protocolType)
                            .setProtocolData("assignor")
                            .setAuthorizedOperations(to32BitField(emptySet()))
                            .setMembers(
                                listOf(
                                    DescribedGroupMember()
                                        .setClientHost("host")
                                        .setClientId("clientId")
                                        .setMemberId("memberId")
                                        .setMemberAssignment(
                                            serializeAssignment(
                                                ConsumerPartitionAssignor.Assignment(ArrayList(tps))
                                            ).array()
                                        )
                                )
                            )
                    )
                )
        )
    }

    private fun handleWithError(
        error: Errors,
        protocolType: String,
    ): ApiResult<CoordinatorKey, ConsumerGroupDescription> {
        val handler = DescribeConsumerGroupsHandler(true, logContext)
        val response = buildResponse(error, protocolType)
        return handler.handleResponse(
            broker = coordinator,
            keys = setOf(CoordinatorKey.byGroupId(groupId1)),
            response = response,
        )
    }

    private fun assertUnmapped(result: ApiResult<CoordinatorKey, ConsumerGroupDescription>) {
        assertEquals(emptySet(), result.completedKeys.keys)
        assertEquals(emptySet(), result.failedKeys.keys)
        assertEquals(listOf(CoordinatorKey.byGroupId(groupId1)), result.unmappedKeys)
    }

    private fun assertRetriable(result: ApiResult<CoordinatorKey, ConsumerGroupDescription>) {
        assertEquals(emptySet(), result.completedKeys.keys)
        assertEquals(emptySet(), result.failedKeys.keys)
        assertEquals(emptyList(), result.unmappedKeys)
    }

    private fun assertCompleted(
        result: ApiResult<CoordinatorKey, ConsumerGroupDescription>,
        expected: ConsumerGroupDescription,
    ) {
        val key = CoordinatorKey.byGroupId(groupId1)
        assertEquals(emptySet(), result.failedKeys.keys)
        assertEquals(emptyList(), result.unmappedKeys)
        assertEquals(setOf(key), result.completedKeys.keys)
        assertEquals(expected, result.completedKeys[CoordinatorKey.byGroupId(groupId1)])
    }

    private fun assertFailed(
        expectedExceptionType: Class<out Throwable>,
        result: ApiResult<CoordinatorKey, ConsumerGroupDescription>,
    ) {
        val key = CoordinatorKey.byGroupId(groupId1)
        assertEquals(emptySet(), result.completedKeys.keys)
        assertEquals(emptyList(), result.unmappedKeys)
        assertEquals(setOf(key), result.failedKeys.keys)
        assertTrue(expectedExceptionType.isInstance(result.failedKeys[key]))
    }
}
