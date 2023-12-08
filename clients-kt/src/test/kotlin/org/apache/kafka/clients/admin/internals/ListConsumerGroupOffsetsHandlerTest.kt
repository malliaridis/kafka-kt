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

import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsSpec
import org.apache.kafka.clients.admin.internals.AdminApiHandler.ApiResult
import org.apache.kafka.clients.admin.internals.AdminApiHandler.RequestAndKeys
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.Node
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.GroupAuthorizationException
import org.apache.kafka.common.errors.GroupIdNotFoundException
import org.apache.kafka.common.errors.InvalidGroupIdException
import org.apache.kafka.common.message.OffsetFetchRequestData.OffsetFetchRequestGroup
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.OffsetFetchRequest
import org.apache.kafka.common.requests.OffsetFetchResponse
import org.apache.kafka.common.requests.OffsetFetchResponse.PartitionData
import org.apache.kafka.common.utils.LogContext
import org.junit.jupiter.api.Test
import java.util.stream.Collectors
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class ListConsumerGroupOffsetsHandlerTest {

    private val logContext = LogContext()

    private val throttleMs = 10

    private val groupZero = "group0"

    private val groupOne = "group1"

    private val groupTwo = "group2"

    private val groups = listOf(groupZero, groupOne, groupTwo)

    private val t0p0 = TopicPartition("t0", 0)

    private val t0p1 = TopicPartition("t0", 1)

    private val t1p0 = TopicPartition("t1", 0)

    private val t1p1 = TopicPartition("t1", 1)

    private val t2p0 = TopicPartition("t2", 0)

    private val t2p1 = TopicPartition("t2", 1)

    private val t2p2 = TopicPartition("t2", 2)

    private val singleRequestMap = mapOf(
        groupZero to ListConsumerGroupOffsetsSpec().apply {
            topicPartitions = listOf(t0p0, t0p1, t1p0, t1p1)
        }
    )

    private val batchedRequestMap = mapOf(
        groupZero to ListConsumerGroupOffsetsSpec()
            .apply { topicPartitions = listOf(t0p0) },
        groupOne to ListConsumerGroupOffsetsSpec()
            .apply { topicPartitions = listOf(t0p0, t1p0, t1p1) },
        groupTwo to ListConsumerGroupOffsetsSpec()
            .apply { topicPartitions = listOf(t0p0, t1p0, t1p1, t2p0, t2p1, t2p2) },
    )

    @Test
    fun testBuildRequest() {
        val handler = ListConsumerGroupOffsetsHandler(singleRequestMap, false, logContext)
        val request = handler.buildBatchedRequest(coordinatorKeys(groupZero)).build()

        assertEquals(groupZero, request.data().groups[0].groupId)
        assertEquals(2, request.data().groups[0].topics!!.size)
        assertEquals(2, request.data().groups[0].topics!![0].partitionIndexes.size)
        assertEquals(2, request.data().groups[0].topics!![1].partitionIndexes.size)
    }

    @Test
    fun testBuildRequestWithMultipleGroups() {
        val requestMap = batchedRequestMap.toMutableMap()
        val groupThree = "group3"
        requestMap[groupThree] = ListConsumerGroupOffsetsSpec().apply {
            topicPartitions = listOf(
                TopicPartition("t3", 0),
                TopicPartition("t3", 1),
            )
        }
        val handler = ListConsumerGroupOffsetsHandler(requestMap, false, logContext)
        val request1 = handler.buildBatchedRequest(coordinatorKeys(groupZero, groupOne, groupTwo)).build()
        assertEquals(setOf(groupZero, groupOne, groupTwo), requestGroups(request1))
        
        val request2 = handler.buildBatchedRequest(coordinatorKeys(groupThree)).build()
        assertEquals(setOf(groupThree), requestGroups(request2))
        
        val builtRequests = mutableMapOf<String, ListConsumerGroupOffsetsSpec>()
        request1.groupIdsToPartitions().forEach { (group, partitions) ->
            builtRequests[group] = ListConsumerGroupOffsetsSpec().apply { topicPartitions = partitions }
        }
        request2.groupIdsToPartitions() .forEach { (group, partitions) ->
            builtRequests[group] = ListConsumerGroupOffsetsSpec().apply { topicPartitions = partitions }
        }
        assertEquals(requestMap, builtRequests)
        
        var groupIdsToTopics = request1.groupIdsToTopics()
        
        assertEquals(3, groupIdsToTopics.size)
        assertEquals(1, groupIdsToTopics[groupZero]?.size)
        assertEquals(2, groupIdsToTopics[groupOne]?.size)
        assertEquals(3, groupIdsToTopics[groupTwo]?.size)
        assertEquals(1, groupIdsToTopics[groupZero]?.get(0)?.partitionIndexes?.size)
        assertEquals(1, groupIdsToTopics[groupOne]?.get(0)?.partitionIndexes?.size)
        assertEquals(2, groupIdsToTopics[groupOne]?.get(1)?.partitionIndexes?.size)
        assertEquals(1, groupIdsToTopics[groupTwo]?.get(0)?.partitionIndexes?.size)
        assertEquals(2, groupIdsToTopics[groupTwo]?.get(1)?.partitionIndexes?.size)
        assertEquals(3, groupIdsToTopics[groupTwo]?.get(2)?.partitionIndexes?.size)
        
        groupIdsToTopics = request2.groupIdsToTopics()
        assertEquals(1, groupIdsToTopics.size)
        assertEquals(1, groupIdsToTopics[groupThree]?.size)
        assertEquals(2, groupIdsToTopics[groupThree]?.get(0)?.partitionIndexes?.size)
    }

    @Test
    fun testBuildRequestBatchGroups() {
        val handler = ListConsumerGroupOffsetsHandler(
            groupSpecs = batchedRequestMap,
            requireStable = false,
            logContext = logContext,
        )
        val requests = handler.buildRequest(
            brokerId = 1,
            keys = coordinatorKeys(groupZero, groupOne, groupTwo),
        )
        assertEquals(1, requests.size)
        assertEquals(
            expected = setOf(groupZero, groupOne, groupTwo),
            actual = requestGroups(requests.first().request.build() as OffsetFetchRequest)
        )
    }

    @Test
    fun testBuildRequestDoesNotBatchGroup() {
        val handler = ListConsumerGroupOffsetsHandler(batchedRequestMap, false, logContext)
        // Disable batching.
        (handler.lookupStrategy() as CoordinatorStrategy).disableBatch()
        val requests = handler.buildRequest(1, coordinatorKeys(groupZero, groupOne, groupTwo))
        assertEquals(3, requests.size)
        assertEquals(
            setOf(setOf(groupZero), setOf(groupOne), setOf(groupTwo)),
            requests.stream().map { requestAndKey: RequestAndKeys<CoordinatorKey> ->
                requestGroups(
                    requestAndKey.request.build() as OffsetFetchRequest
                )
            }.collect(Collectors.toSet())
        )
    }

    @Test
    fun testSuccessfulHandleResponse() {
        val expected = emptyMap<TopicPartition, OffsetAndMetadata>()
        assertCompleted(handleWithError(Errors.NONE), expected)
    }

    @Test
    fun testSuccessfulHandleResponseWithOnePartitionError() {
        val expectedResult = mapOf(t0p0 to OffsetAndMetadata(10L))

        // expected that there's only 1 partition result returned because the other partition is skipped with error
        assertCompleted(handleWithPartitionError(Errors.UNKNOWN_TOPIC_OR_PARTITION), expectedResult)
        assertCompleted(handleWithPartitionError(Errors.TOPIC_AUTHORIZATION_FAILED), expectedResult)
        assertCompleted(handleWithPartitionError(Errors.UNSTABLE_OFFSET_COMMIT), expectedResult)
    }

    @Test
    fun testSuccessfulHandleResponseWithOnePartitionErrorWithMultipleGroups() {
        val offsetAndMetadataMapZero = mapOf(t0p0 to OffsetAndMetadata(10L))
        val offsetAndMetadataMapOne = mapOf(t1p1 to OffsetAndMetadata(10L))
        val offsetAndMetadataMapTwo = mapOf(t2p2 to OffsetAndMetadata(10L))
        val expectedResult = mapOf(
            groupZero to offsetAndMetadataMapZero,
            groupOne to offsetAndMetadataMapOne,
            groupTwo to offsetAndMetadataMapTwo,
        )
        assertCompletedForMultipleGroups(
            result = handleWithPartitionErrorMultipleGroups(Errors.UNKNOWN_TOPIC_OR_PARTITION),
            expected = expectedResult,
        )
        assertCompletedForMultipleGroups(
            result = handleWithPartitionErrorMultipleGroups(Errors.TOPIC_AUTHORIZATION_FAILED),
            expected = expectedResult,
        )
        assertCompletedForMultipleGroups(
            result = handleWithPartitionErrorMultipleGroups(Errors.UNSTABLE_OFFSET_COMMIT),
            expected = expectedResult,
        )
    }

    @Test
    fun testSuccessfulHandleResponseWithMultipleGroups() {
        val expected = emptyMap<String, Map<TopicPartition, OffsetAndMetadata>>()
        val errorMap = errorMap(groups, Errors.NONE)
        assertCompletedForMultipleGroups(handleWithErrorWithMultipleGroups(errorMap, batchedRequestMap), expected)
    }

    @Test
    fun testUnmappedHandleResponse() {
        assertUnmapped(handleWithError(Errors.COORDINATOR_NOT_AVAILABLE))
        assertUnmapped(handleWithError(Errors.NOT_COORDINATOR))
    }

    @Test
    fun testUnmappedHandleResponseWithMultipleGroups() {
        val errorMap = mapOf(
            groupZero to Errors.NOT_COORDINATOR,
            groupOne to Errors.COORDINATOR_NOT_AVAILABLE,
            groupTwo to Errors.NOT_COORDINATOR,
        )
        assertUnmappedWithMultipleGroups(handleWithErrorWithMultipleGroups(errorMap, batchedRequestMap))
    }

    @Test
    fun testRetriableHandleResponse() = assertRetriable(handleWithError(Errors.COORDINATOR_LOAD_IN_PROGRESS))

    @Test
    fun testRetriableHandleResponseWithMultipleGroups() {
        val errorMap = errorMap(groups, Errors.COORDINATOR_LOAD_IN_PROGRESS)
        assertRetriable(handleWithErrorWithMultipleGroups(errorMap, batchedRequestMap))
    }

    @Test
    fun testFailedHandleResponse() {
        assertFailed(GroupAuthorizationException::class.java, handleWithError(Errors.GROUP_AUTHORIZATION_FAILED))
        assertFailed(GroupIdNotFoundException::class.java, handleWithError(Errors.GROUP_ID_NOT_FOUND))
        assertFailed(InvalidGroupIdException::class.java, handleWithError(Errors.INVALID_GROUP_ID))
    }

    @Test
    fun testFailedHandleResponseWithMultipleGroups() {
        val errorMap = mapOf(
            groupZero to Errors.GROUP_AUTHORIZATION_FAILED,
            groupOne to Errors.GROUP_ID_NOT_FOUND,
            groupTwo to Errors.INVALID_GROUP_ID,
        )
        val groupToExceptionMap = mapOf(
            groupZero to GroupAuthorizationException::class.java,
            groupOne to GroupIdNotFoundException::class.java,
            groupTwo to InvalidGroupIdException::class.java,
        )
        assertFailedForMultipleGroups(
            groupToExceptionMap = groupToExceptionMap,
            result = handleWithErrorWithMultipleGroups(errorMap, batchedRequestMap),
        )
    }

    private fun buildResponse(error: Errors): OffsetFetchResponse = OffsetFetchResponse(
        throttleMs,
        mapOf(groupZero to error),
        mapOf(groupZero to HashMap())
    )

    private fun buildResponseWithMultipleGroups(
        errorMap: Map<String, Errors>,
        responseData: Map<String, Map<TopicPartition, PartitionData>>,
    ): OffsetFetchResponse = OffsetFetchResponse(throttleMs, errorMap, responseData)

    private fun handleWithErrorWithMultipleGroups(
        errorMap: Map<String, Errors>,
        groupSpecs: Map<String, ListConsumerGroupOffsetsSpec>,
    ): ApiResult<CoordinatorKey, Map<TopicPartition, OffsetAndMetadata?>> {
        val handler = ListConsumerGroupOffsetsHandler(groupSpecs, false, logContext)
        
        val responseData = errorMap.keys.associateWith { emptyMap<TopicPartition, PartitionData>() }
        val response = buildResponseWithMultipleGroups(errorMap, responseData)
        return handler.handleResponse(
            broker = Node(id = 1, host = "host", port = 1234),
            keys = errorMap.keys.map(CoordinatorKey::byGroupId).toSet(),
            response = response,
        )
    }

    private fun buildResponseWithPartitionError(error: Errors): OffsetFetchResponse {
        val responseData = mapOf(
            t0p0 to PartitionData(
                offset = 10,
                leaderEpoch = null,
                metadata = "",
                error = Errors.NONE,
            ),
            t0p1 to PartitionData(
                offset = 10,
                leaderEpoch = null,
                metadata = "",
                error = error,
            ),
        )
        return OffsetFetchResponse(Errors.NONE, responseData)
    }

    private fun buildResponseWithPartitionErrorWithMultipleGroups(error: Errors): OffsetFetchResponse {
        val responseDataZero: MutableMap<TopicPartition, PartitionData> = HashMap()
        responseDataZero[t0p0] = PartitionData(offset = 10, leaderEpoch = null, metadata = "", error = Errors.NONE)

        val responseDataOne: MutableMap<TopicPartition, PartitionData> = HashMap()
        responseDataOne[t0p0] = PartitionData(offset = 10, leaderEpoch = null, metadata = "", error = error)
        responseDataOne[t1p0] = PartitionData(offset = 10, leaderEpoch = null, metadata = "", error = error)
        responseDataOne[t1p1] = PartitionData(offset = 10, leaderEpoch = null, metadata = "", error = Errors.NONE)

        val responseDataTwo: MutableMap<TopicPartition, PartitionData> = HashMap()
        responseDataTwo[t0p0] = PartitionData(offset = 10, leaderEpoch = null, metadata = "", error = error)
        responseDataTwo[t1p0] = PartitionData(offset = 10, leaderEpoch = null, metadata = "", error = error)
        responseDataTwo[t1p1] = PartitionData(offset = 10, leaderEpoch = null, metadata = "", error = error)
        responseDataTwo[t2p0] = PartitionData(offset = 10, leaderEpoch = null, metadata = "", error = error)
        responseDataTwo[t2p1] = PartitionData(offset = 10, leaderEpoch = null, metadata = "", error = error)
        responseDataTwo[t2p2] = PartitionData(offset = 10, leaderEpoch = null, metadata = "", error = Errors.NONE)

        val responseData = mapOf(
            groupZero to responseDataZero,
            groupOne to responseDataOne,
            groupTwo to responseDataTwo,
        )
        val errorMap = errorMap(groups, Errors.NONE)
        return OffsetFetchResponse(
            throttleTimeMs = 0,
            errors = errorMap,
            responseData = responseData,
        )
    }

    private fun handleWithPartitionError(
        error: Errors,
    ): ApiResult<CoordinatorKey, Map<TopicPartition, OffsetAndMetadata?>> {
        val handler = ListConsumerGroupOffsetsHandler(
            groupSpecs = singleRequestMap,
            requireStable = false,
            logContext = logContext,
        )
        val response = buildResponseWithPartitionError(error)
        return handler.handleResponse(
            broker = Node(id = 1, host = "host", port = 1234),
            keys = setOf(CoordinatorKey.byGroupId(groupZero)),
            response = response,
        )
    }

    private fun handleWithPartitionErrorMultipleGroups(
        error: Errors,
    ): ApiResult<CoordinatorKey, Map<TopicPartition, OffsetAndMetadata?>> {
        val handler = ListConsumerGroupOffsetsHandler(
            groupSpecs = batchedRequestMap,
            requireStable = false,
            logContext = logContext,
        )
        val response = buildResponseWithPartitionErrorWithMultipleGroups(error)
        return handler.handleResponse(
            broker = Node(id = 1, host = "host", port = 1234),
            keys = coordinatorKeys(groupZero, groupOne, groupTwo),
            response = response,
        )
    }

    private fun handleWithError(error: Errors): ApiResult<CoordinatorKey, Map<TopicPartition, OffsetAndMetadata?>> {
        val handler = ListConsumerGroupOffsetsHandler(
            groupSpecs = singleRequestMap,
            requireStable = false,
            logContext = logContext,
        )
        val response = buildResponse(error)
        return handler.handleResponse(
            broker = Node(id = 1, host = "host", port = 1234),
            keys = setOf(CoordinatorKey.byGroupId(groupZero)),
            response = response
        )
    }

    private fun assertUnmapped(result: ApiResult<CoordinatorKey, Map<TopicPartition, OffsetAndMetadata?>>) {
        assertEquals(emptySet(), result.completedKeys.keys)
        assertEquals(emptySet(), result.failedKeys.keys)
        assertEquals(listOf(CoordinatorKey.byGroupId(groupZero)), result.unmappedKeys)
    }

    private fun assertUnmappedWithMultipleGroups(
        result: ApiResult<CoordinatorKey, Map<TopicPartition, OffsetAndMetadata?>>,
    ) {
        assertEquals(emptySet(), result.completedKeys.keys)
        assertEquals(emptySet(), result.failedKeys.keys)
        assertEquals(coordinatorKeys(groupZero, groupOne, groupTwo), HashSet(result.unmappedKeys))
    }

    private fun assertRetriable(
        result: ApiResult<CoordinatorKey, Map<TopicPartition, OffsetAndMetadata?>>,
    ) {
        assertEquals(emptySet(), result.completedKeys.keys)
        assertEquals(emptySet(), result.failedKeys.keys)
        assertEquals(emptyList(), result.unmappedKeys)
    }

    private fun assertCompleted(
        result: ApiResult<CoordinatorKey, Map<TopicPartition, OffsetAndMetadata?>>,
        expected: Map<TopicPartition, OffsetAndMetadata>,
    ) {
        val key = CoordinatorKey.byGroupId(groupZero)
        assertEquals(emptySet(), result.failedKeys.keys)
        assertEquals(emptyList(), result.unmappedKeys)
        assertEquals(setOf(key), result.completedKeys.keys)
        assertEquals(expected, result.completedKeys[key])
    }

    private fun assertCompletedForMultipleGroups(
        result: ApiResult<CoordinatorKey, Map<TopicPartition, OffsetAndMetadata?>>,
        expected: Map<String, Map<TopicPartition, OffsetAndMetadata>>,
    ) {
        assertEquals(emptySet(), result.failedKeys.keys)
        assertEquals(emptyList(), result.unmappedKeys)
        for (g in expected.keys) {
            val key = CoordinatorKey.byGroupId(g)
            assertTrue(result.completedKeys.containsKey(key))
            assertEquals(expected[g], result.completedKeys[key])
        }
    }

    private fun assertFailed(
        expectedExceptionType: Class<out Throwable>,
        result: ApiResult<CoordinatorKey, Map<TopicPartition, OffsetAndMetadata?>>,
    ) {
        val key = CoordinatorKey.byGroupId(groupZero)

        assertEquals(emptySet(), result.completedKeys.keys)
        assertEquals(emptyList(), result.unmappedKeys)
        assertEquals(setOf(key), result.failedKeys.keys)
        assertTrue(expectedExceptionType.isInstance(result.failedKeys[key]))
    }

    private fun assertFailedForMultipleGroups(
        groupToExceptionMap: Map<String, Class<out Throwable>>,
        result: ApiResult<CoordinatorKey, Map<TopicPartition, OffsetAndMetadata?>>,
    ) {
        assertEquals(emptySet(), result.completedKeys.keys)
        assertEquals(emptyList(), result.unmappedKeys)

        for (g in groupToExceptionMap.keys) {
            val key = CoordinatorKey.byGroupId(g)

            assertTrue(result.failedKeys.containsKey(key))
            assertTrue(groupToExceptionMap[g]!!.isInstance(result.failedKeys[key]))
        }
    }

    private fun coordinatorKeys(vararg groups: String): Set<CoordinatorKey> =
        groups.map(CoordinatorKey::byGroupId).toSet()

    private fun requestGroups(request: OffsetFetchRequest): Set<String> =
        request.data().groups
            .map(OffsetFetchRequestGroup::groupId)
            .toSet()

    private fun errorMap(groups: Collection<String>, error: Errors): Map<String, Errors> =
        groups.associateWith { error }
}
