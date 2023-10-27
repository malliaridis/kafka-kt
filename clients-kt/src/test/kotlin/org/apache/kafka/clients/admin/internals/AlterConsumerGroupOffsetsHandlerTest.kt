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
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.Node
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.OffsetCommitResponse
import org.apache.kafka.common.utils.LogContext
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.*
import java.util.function.Consumer
import kotlin.test.assertEquals


class AlterConsumerGroupOffsetsHandlerTest {

    private val logContext = LogContext()

    private val groupId = "group-id"

    private val t0p0 = TopicPartition("t0", 0)

    private val t0p1 = TopicPartition("t0", 1)

    private val t1p0 = TopicPartition("t1", 0)

    private val t1p1 = TopicPartition("t1", 1)

    private val partitions = mutableMapOf<TopicPartition, OffsetAndMetadata>()

    private val offset = 1L

    private val node = Node(1, "host", 1234)

    @BeforeEach
    fun setUp() {
        partitions[t0p0] = OffsetAndMetadata(offset)
        partitions[t0p1] = OffsetAndMetadata(offset)
        partitions[t1p0] = OffsetAndMetadata(offset)
        partitions[t1p1] = OffsetAndMetadata(offset)
    }

    @Test
    fun testBuildRequest() {
        val handler = AlterConsumerGroupOffsetsHandler(groupId, partitions, logContext)
        val request = handler.buildBatchedRequest(-1, setOf(CoordinatorKey.byGroupId(groupId))).build()
        assertEquals(groupId, request.data().groupId)
        assertEquals(2, request.data().topics.size)
        assertEquals(2, request.data().topics[0].partitions.size)
        assertEquals(offset, request.data().topics[0].partitions[0].committedOffset)
    }

    @Test
    fun testHandleSuccessfulResponse() {
        val handler = AlterConsumerGroupOffsetsHandler(groupId, partitions, logContext)
        val responseData = Collections.singletonMap(t0p0, Errors.NONE)
        val response = OffsetCommitResponse(0, responseData)
        val result = handler.handleResponse(node, setOf(CoordinatorKey.byGroupId(groupId)), response)
        assertCompleted(result, responseData)
    }

    @Test
    fun testHandleRetriableResponse() {
        assertUnmappedKey(partitionErrors(Errors.NOT_COORDINATOR))
        assertUnmappedKey(partitionErrors(Errors.COORDINATOR_NOT_AVAILABLE))
        assertRetriableError(partitionErrors(Errors.COORDINATOR_LOAD_IN_PROGRESS))
        assertRetriableError(partitionErrors(Errors.REBALANCE_IN_PROGRESS))
    }

    @Test
    fun testHandleErrorResponse() {
        assertFatalError(partitionErrors(Errors.TOPIC_AUTHORIZATION_FAILED))
        assertFatalError(partitionErrors(Errors.GROUP_AUTHORIZATION_FAILED))
        assertFatalError(partitionErrors(Errors.INVALID_GROUP_ID))
        assertFatalError(partitionErrors(Errors.UNKNOWN_TOPIC_OR_PARTITION))
        assertFatalError(partitionErrors(Errors.OFFSET_METADATA_TOO_LARGE))
        assertFatalError(partitionErrors(Errors.ILLEGAL_GENERATION))
        assertFatalError(partitionErrors(Errors.UNKNOWN_MEMBER_ID))
        assertFatalError(partitionErrors(Errors.INVALID_COMMIT_OFFSET_SIZE))
        assertFatalError(partitionErrors(Errors.UNKNOWN_SERVER_ERROR))
    }

    @Test
    fun testHandleMultipleErrorsResponse() {
        val partitionErrors: MutableMap<TopicPartition, Errors> = HashMap()
        partitionErrors[t0p0] = Errors.UNKNOWN_TOPIC_OR_PARTITION
        partitionErrors[t0p1] = Errors.INVALID_COMMIT_OFFSET_SIZE
        partitionErrors[t1p0] = Errors.TOPIC_AUTHORIZATION_FAILED
        partitionErrors[t1p1] = Errors.OFFSET_METADATA_TOO_LARGE
        assertFatalError(partitionErrors)
    }

    private fun handleResponse(
        groupKey: CoordinatorKey,
        partitions: Map<TopicPartition, OffsetAndMetadata>,
        partitionResults: Map<TopicPartition, Errors>,
    ): ApiResult<CoordinatorKey, Map<TopicPartition, Errors>> {
        val handler = AlterConsumerGroupOffsetsHandler(groupKey.idValue, partitions, logContext)
        val response = OffsetCommitResponse(0, partitionResults)
        return handler.handleResponse(node, setOf(groupKey), response)
    }

    private fun partitionErrors(error: Errors): Map<TopicPartition, Errors> {
        val partitionErrors = mutableMapOf<TopicPartition, Errors>()
        partitions.keys.forEach { partition -> partitionErrors[partition] = error }
        return partitionErrors
    }

    private fun assertFatalError(partitionResults: Map<TopicPartition, Errors>) {
        val groupKey = CoordinatorKey.byGroupId(groupId)
        val result = handleResponse(
            groupKey = groupKey,
            partitions = partitions,
            partitionResults = partitionResults,
        )
        assertEquals(setOf(groupKey), result.completedKeys.keys)
        assertEquals(partitionResults, result.completedKeys[groupKey])
        assertEquals(emptyList(), result.unmappedKeys)
        assertEquals(emptyMap(), result.failedKeys)
    }

    private fun assertRetriableError(partitionResults: Map<TopicPartition, Errors>) {
        val groupKey = CoordinatorKey.byGroupId(groupId)
        val result = handleResponse(
            groupKey = groupKey,
            partitions = partitions,
            partitionResults = partitionResults,
        )
        assertEquals(emptySet(), result.completedKeys.keys)
        assertEquals(emptyList(), result.unmappedKeys)
        assertEquals(emptyMap(), result.failedKeys)
    }

    private fun assertUnmappedKey(partitionResults: Map<TopicPartition, Errors>) {
        val groupKey = CoordinatorKey.byGroupId(groupId)
        val result = handleResponse(
            groupKey = groupKey,
            partitions = partitions,
            partitionResults = partitionResults,
        )
        assertEquals(emptySet(), result.completedKeys.keys)
        assertEquals(emptySet(), result.failedKeys.keys)
        assertEquals(listOf(CoordinatorKey.byGroupId(groupId)), result.unmappedKeys)
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
}
