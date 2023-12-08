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
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.GroupAuthorizationException
import org.apache.kafka.common.errors.GroupIdNotFoundException
import org.apache.kafka.common.errors.GroupNotEmptyException
import org.apache.kafka.common.errors.InvalidGroupIdException
import org.apache.kafka.common.message.OffsetDeleteResponseData
import org.apache.kafka.common.message.OffsetDeleteResponseData.OffsetDeleteResponsePartition
import org.apache.kafka.common.message.OffsetDeleteResponseData.OffsetDeleteResponsePartitionCollection
import org.apache.kafka.common.message.OffsetDeleteResponseData.OffsetDeleteResponseTopic
import org.apache.kafka.common.message.OffsetDeleteResponseData.OffsetDeleteResponseTopicCollection
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.OffsetDeleteResponse
import org.apache.kafka.common.utils.LogContext
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class DeleteConsumerGroupOffsetsHandlerTest {

    private val logContext = LogContext()

    private val groupId = "group-id"

    private val t0p0 = TopicPartition("t0", 0)

    private val t0p1 = TopicPartition("t0", 1)

    private val t1p0 = TopicPartition("t1", 0)

    private val tps: Set<TopicPartition> = setOf(t0p0, t0p1, t1p0)

    @Test
    fun testBuildRequest() {
        val handler = DeleteConsumerGroupOffsetsHandler(groupId, tps, logContext)
        val request = handler.buildBatchedRequest(1, setOf(CoordinatorKey.byGroupId(groupId))).build()
        assertEquals(groupId, request.data().groupId)
        assertEquals(2, request.data().topics.size)
        assertEquals(2, request.data().topics.find("t0")?.partitions?.size)
        assertEquals(1, request.data().topics.find("t1")?.partitions?.size)
    }

    @Test
    fun testSuccessfulHandleResponse() {
        val responseData = mapOf(t0p0 to Errors.NONE)
        assertCompleted(handleWithGroupError(Errors.NONE), responseData)
    }

    @Test
    fun testUnmappedHandleResponse() {
        assertUnmapped(handleWithGroupError(Errors.NOT_COORDINATOR))
        assertUnmapped(handleWithGroupError(Errors.COORDINATOR_NOT_AVAILABLE))
    }

    @Test
    fun testRetriableHandleResponse() {
        assertRetriable(handleWithGroupError(Errors.COORDINATOR_LOAD_IN_PROGRESS))
    }

    @Test
    fun testFailedHandleResponseWithGroupError() {
        assertGroupFailed(
            GroupAuthorizationException::class.java,
            handleWithGroupError(Errors.GROUP_AUTHORIZATION_FAILED)
        )
        assertGroupFailed(GroupIdNotFoundException::class.java, handleWithGroupError(Errors.GROUP_ID_NOT_FOUND))
        assertGroupFailed(InvalidGroupIdException::class.java, handleWithGroupError(Errors.INVALID_GROUP_ID))
        assertGroupFailed(GroupNotEmptyException::class.java, handleWithGroupError(Errors.NON_EMPTY_GROUP))
    }

    @Test
    fun testFailedHandleResponseWithPartitionError() {
        assertPartitionFailed(
            mapOf(t0p0 to Errors.GROUP_SUBSCRIBED_TO_TOPIC),
            handleWithPartitionError(Errors.GROUP_SUBSCRIBED_TO_TOPIC)
        )
        assertPartitionFailed(
            mapOf(t0p0 to Errors.TOPIC_AUTHORIZATION_FAILED),
            handleWithPartitionError(Errors.TOPIC_AUTHORIZATION_FAILED)
        )
        assertPartitionFailed(
            mapOf(t0p0 to Errors.UNKNOWN_TOPIC_OR_PARTITION),
            handleWithPartitionError(Errors.UNKNOWN_TOPIC_OR_PARTITION)
        )
    }

    private fun buildGroupErrorResponse(error: Errors): OffsetDeleteResponse {
        val response = OffsetDeleteResponse(OffsetDeleteResponseData().setErrorCode(error.code))
        if (error === Errors.NONE) {
            response.data()
                .setThrottleTimeMs(0)
                .setTopics(
                    OffsetDeleteResponseTopicCollection(
                        listOf(
                            OffsetDeleteResponseTopic()
                                .setName(t0p0.topic)
                                .setPartitions(
                                    OffsetDeleteResponsePartitionCollection(
                                        listOf(
                                            OffsetDeleteResponsePartition()
                                                .setPartitionIndex(t0p0.partition)
                                                .setErrorCode(error.code)
                                        ).iterator()
                                    )
                                )
                        ).iterator()
                    )
                )
        }
        return response
    }

    private fun buildPartitionErrorResponse(error: Errors): OffsetDeleteResponse {
        return OffsetDeleteResponse(
            OffsetDeleteResponseData()
                .setThrottleTimeMs(0)
                .setTopics(
                    OffsetDeleteResponseTopicCollection(
                        listOf(
                            OffsetDeleteResponseTopic()
                                .setName(t0p0.topic)
                                .setPartitions(
                                    OffsetDeleteResponsePartitionCollection(
                                        listOf(
                                            OffsetDeleteResponsePartition()
                                                .setPartitionIndex(t0p0.partition)
                                                .setErrorCode(error.code)
                                        ).iterator()
                                    )
                                )
                        ).iterator()
                    )
                )
        )
    }

    private fun handleWithGroupError(error: Errors): ApiResult<CoordinatorKey, Map<TopicPartition, Errors>> {
        val handler = DeleteConsumerGroupOffsetsHandler(groupId, tps, logContext)
        val response = buildGroupErrorResponse(error)
        return handler.handleResponse(
            broker = Node(id = 1, host = "host", port = 1234),
            keys = setOf(CoordinatorKey.byGroupId(groupId)),
            response = response,
        )
    }

    private fun handleWithPartitionError(error: Errors): ApiResult<CoordinatorKey, Map<TopicPartition, Errors>> {
        val handler = DeleteConsumerGroupOffsetsHandler(groupId, tps, logContext)
        val response = buildPartitionErrorResponse(error)
        return handler.handleResponse(
            broker = Node(id = 1, host = "host", port = 1234),
            keys = setOf(CoordinatorKey.byGroupId(groupId)),
            response = response,
        )
    }

    private fun assertUnmapped(result: ApiResult<CoordinatorKey, Map<TopicPartition, Errors>>) {
        assertEquals(emptySet(), result.completedKeys.keys)
        assertEquals(emptySet(), result.failedKeys.keys)
        assertEquals(listOf(CoordinatorKey.byGroupId(groupId)), result.unmappedKeys)
    }

    private fun assertRetriable(result: ApiResult<CoordinatorKey, Map<TopicPartition, Errors>>) {
        assertEquals(emptySet(), result.completedKeys.keys)
        assertEquals(emptySet(), result.failedKeys.keys)
        assertEquals(emptyList(), result.unmappedKeys)
    }

    private fun assertCompleted(
        result: ApiResult<CoordinatorKey, Map<TopicPartition, Errors>>,
        expected: Map<TopicPartition, Errors>,
    ) {
        val key = CoordinatorKey.byGroupId(groupId)
        assertEquals(emptySet(), result.failedKeys.keys)
        assertEquals(emptyList(), result.unmappedKeys)
        assertEquals(setOf(key), result.completedKeys.keys)
        assertEquals(expected, result.completedKeys[key])
    }

    private fun assertGroupFailed(
        expectedExceptionType: Class<out Throwable?>,
        result: ApiResult<CoordinatorKey, Map<TopicPartition, Errors>>,
    ) {
        val key = CoordinatorKey.byGroupId(groupId)
        assertEquals(emptySet(), result.completedKeys.keys)
        assertEquals(emptyList(), result.unmappedKeys)
        assertEquals(setOf(key), result.failedKeys.keys)
        assertTrue(expectedExceptionType.isInstance(result.failedKeys[key]))
    }

    private fun assertPartitionFailed(
        expectedResult: Map<TopicPartition, Errors>,
        result: ApiResult<CoordinatorKey, Map<TopicPartition, Errors>>,
    ) {
        val key = CoordinatorKey.byGroupId(groupId)
        assertEquals(setOf(key), result.completedKeys.keys)

        // verify the completed value is expected result
        val completeCollection = result.completedKeys.values
        assertEquals(1, completeCollection.size)
        assertEquals(expectedResult, result.completedKeys[key])
        assertEquals(emptyList(), result.unmappedKeys)
        assertEquals(emptySet(), result.failedKeys.keys)
    }
}
