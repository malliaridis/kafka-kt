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

import org.apache.kafka.clients.admin.ListOffsetsOptions
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo
import org.apache.kafka.clients.admin.OffsetSpec
import org.apache.kafka.clients.admin.internals.AdminApiHandler.ApiResult
import org.apache.kafka.common.IsolationLevel
import org.apache.kafka.common.Node
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.UnsupportedVersionException
import org.apache.kafka.common.message.ListOffsetsRequestData.ListOffsetsPartition
import org.apache.kafka.common.message.ListOffsetsResponseData
import org.apache.kafka.common.message.ListOffsetsResponseData.ListOffsetsPartitionResponse
import org.apache.kafka.common.message.ListOffsetsResponseData.ListOffsetsTopicResponse
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.ListOffsetsRequest
import org.apache.kafka.common.requests.ListOffsetsResponse
import org.apache.kafka.common.utils.LogContext
import org.junit.jupiter.api.Test
import kotlin.test.assertContains
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class ListOffsetsHandlerTest {
    
    private val logContext = LogContext()

    @Test
    fun testBuildRequestSimple() {
        val handler = ListOffsetsHandler(
            offsetTimestampsByPartition = offsetTimestampsByPartition,
            options = ListOffsetsOptions(),
            logContext = logContext,
        )
        val request = handler.buildBatchedRequest(
            brokerId = node.id,
            keys = setOf(t0p0, t0p1),
        ).build()

        val topics = request.topics
        assertEquals(1, topics.size)

        val topic = topics[0]
        assertEquals(2, topic.partitions.size)

        for (partition in topic.partitions) {
            val topicPartition = TopicPartition(topic.name, partition.partitionIndex)
            assertExpectedTimestamp(topicPartition, partition.timestamp)
        }
        assertEquals(IsolationLevel.READ_UNCOMMITTED, request.isolationLevel)
    }

    @Test
    fun testBuildRequestMultipleTopicsWithReadCommitted() {
        val handler = ListOffsetsHandler(
            offsetTimestampsByPartition = offsetTimestampsByPartition,
            options = ListOffsetsOptions(IsolationLevel.READ_COMMITTED),
            logContext = logContext,
        )
        val request = handler.buildBatchedRequest(
            brokerId = node.id,
            keys = offsetTimestampsByPartition.keys,
        ).build()

        val topics = request.topics
        assertEquals(2, topics.size)

        val partitions = mutableMapOf<TopicPartition, ListOffsetsPartition>()

        for (topic in topics) {
            for (partition in topic.partitions) {
                partitions[TopicPartition(topic.name, partition.partitionIndex)] = partition
            }
        }
        assertEquals(4, partitions.size)

        for ((key, value) in partitions)
            assertExpectedTimestamp(key, value.timestamp)

        assertEquals(IsolationLevel.READ_COMMITTED, request.isolationLevel)
    }

    @Test
    fun testBuildRequestAllowedVersions() {
        val defaultOptionsHandler = ListOffsetsHandler(
            offsetTimestampsByPartition = offsetTimestampsByPartition,
            options = ListOffsetsOptions(),
            logContext = logContext,
        )
        var builder = defaultOptionsHandler.buildBatchedRequest(
            brokerId = node.id,
            keys = setOf(t0p0, t0p1, t1p0),
        )
        assertEquals(1, builder.oldestAllowedVersion)

        val readCommittedHandler = ListOffsetsHandler(
            offsetTimestampsByPartition = offsetTimestampsByPartition,
            options = ListOffsetsOptions(IsolationLevel.READ_COMMITTED),
            logContext = logContext,
        )
        builder = readCommittedHandler.buildBatchedRequest(
            brokerId = node.id,
            keys = setOf(t0p0, t0p1, t1p0),
        )
        assertEquals(2, builder.oldestAllowedVersion)

        builder = readCommittedHandler.buildBatchedRequest(
            brokerId = node.id,
            keys = setOf(t0p0, t0p1, t1p0, t1p1),
        )
        assertEquals(7, builder.oldestAllowedVersion)
    }

    @Test
    fun testHandleSuccessfulResponse() {
        val result = handleResponse(createResponse(emptyMap()))
        assertResult(
            result = result,
            expectedCompleted = offsetTimestampsByPartition.keys,
            expectedFailed = emptyMap<TopicPartition, Throwable>(),
            expectedUnmapped = emptyList(),
            expectedRetriable = emptySet(),
        )
    }

    @Test
    fun testHandleRetriablePartitionTimeoutResponse() {
        val errorPartition = t0p0
        val errorsByPartition = mapOf(errorPartition to Errors.REQUEST_TIMED_OUT.code)
        val result = handleResponse(createResponse(errorsByPartition))

        // Timeouts should be retried within the fulfillment stage as they are a common type of retriable error.
        val retriable = setOf(errorPartition)
        val completed = offsetTimestampsByPartition.keys - retriable
        assertResult(
            result = result,
            expectedCompleted = completed,
            expectedFailed = emptyMap<TopicPartition, Throwable>(),
            expectedUnmapped = emptyList(),
            expectedRetriable = retriable,
        )
    }

    @Test
    fun testHandleLookupRetriablePartitionInvalidMetadataResponse() {
        val errorPartition = t0p0
        val error = Errors.NOT_LEADER_OR_FOLLOWER
        val errorsByPartition = mapOf(errorPartition to error.code)
        val result = handleResponse(createResponse(errorsByPartition))

        // Some invalid metadata errors should be retried from the lookup stage as the partition-to-leader
        // mappings should be recalculated.
        val unmapped = listOf(errorPartition)
        val completed = offsetTimestampsByPartition.keys - unmapped.toSet()

        assertResult(
            result = result,
            expectedCompleted = completed,
            expectedFailed = emptyMap<TopicPartition, Throwable>(),
            expectedUnmapped = unmapped,
            expectedRetriable = emptySet(),
        )
    }

    @Test
    fun testHandleUnexpectedPartitionErrorResponse() {
        val errorPartition = t0p0
        val error = Errors.UNKNOWN_SERVER_ERROR
        val errorsByPartition = mapOf(errorPartition to error.code)
        val result = handleResponse(createResponse(errorsByPartition))
        val failed = mapOf(errorPartition to error.exception)
        val completed = offsetTimestampsByPartition.keys - failed.keys

        assertResult(result, completed, failed, emptyList(), emptySet())
    }

    @Test
    fun testHandleResponseSanityCheck() {
        val errorPartition = t0p0
        val specsByPartition = offsetTimestampsByPartition - errorPartition

        val result = handleResponse(createResponse(emptyMap(), specsByPartition))
        assertEquals(offsetTimestampsByPartition.size - 1, result.completedKeys.size)
        assertEquals(1, result.failedKeys.size)
        assertEquals(errorPartition, result.failedKeys.keys.first())

        val sanityCheckMessage = result.failedKeys[errorPartition]!!.message!!
        assertContains(sanityCheckMessage, "did not contain a result for topic partition")
        assertTrue(result.unmappedKeys.isEmpty())
    }

    @Test
    fun testHandleResponseUnsupportedVersion() {
        val brokerId = 1
        val uve = UnsupportedVersionException("")
        val maxTimestampPartitions = mutableMapOf(t1p1 to OffsetSpec.maxTimestamp())
        val handler = ListOffsetsHandler(
            offsetTimestampsByPartition = offsetTimestampsByPartition,
            options = ListOffsetsOptions(),
            logContext = logContext,
        )
        val nonMaxTimestampPartitions = offsetTimestampsByPartition.toMutableMap()
        maxTimestampPartitions.forEach { (k, _) ->
            nonMaxTimestampPartitions.remove(k)
        }

        // Unsupported version exceptions currently cannot be handled if there's no partition with a
        // MAX_TIMESTAMP spec...
        var keysToTest = nonMaxTimestampPartitions.keys
        var expectedFailures = keysToTest
        assertEquals(
            expected = mapToError(expectedFailures, uve),
            actual = handler.handleUnsupportedVersionException(brokerId, uve, keysToTest)
        )

        // ...or if there are only partitions with MAX_TIMESTAMP specs.
        keysToTest = maxTimestampPartitions.keys
        expectedFailures = keysToTest
        assertEquals(
            expected = mapToError(expectedFailures, uve),
            actual = handler.handleUnsupportedVersionException(brokerId, uve, keysToTest)
        )

        // What can be handled is a request with a mix of partitions with MAX_TIMESTAMP specs
        // and partitions with non-MAX_TIMESTAMP specs.
        keysToTest = offsetTimestampsByPartition.keys.toMutableSet()
        expectedFailures = maxTimestampPartitions.keys
        assertEquals(
            expected = mapToError(expectedFailures, uve),
            actual = handler.handleUnsupportedVersionException(brokerId, uve, keysToTest)
        )
    }

    private fun assertExpectedTimestamp(topicPartition: TopicPartition, actualTimestamp: Long) {
        val expectedTimestamp = offsetTimestampsByPartition[topicPartition]
        assertEquals(expectedTimestamp, actualTimestamp)
    }

    private fun handleResponse(response: ListOffsetsResponse): ApiResult<TopicPartition, ListOffsetsResultInfo> {
        val handler = ListOffsetsHandler(
            offsetTimestampsByPartition = offsetTimestampsByPartition,
            options = ListOffsetsOptions(),
            logContext = logContext,
        )
        return handler.handleResponse(
            broker = node,
            keys = offsetTimestampsByPartition.keys,
            response = response,
        )
    }

    private fun assertResult(
        result: ApiResult<TopicPartition, ListOffsetsResultInfo>,
        expectedCompleted: Set<TopicPartition>,
        expectedFailed: Map<TopicPartition, Throwable?>,
        expectedUnmapped: List<TopicPartition>,
        expectedRetriable: Set<TopicPartition>,
    ) {
        assertEquals(expectedCompleted, result.completedKeys.keys)
        assertEquals(expectedFailed, result.failedKeys)
        assertEquals(expectedUnmapped, result.unmappedKeys)
        val actualRetriable = offsetTimestampsByPartition.keys.toMutableSet()
        actualRetriable.removeAll(result.completedKeys.keys)
        actualRetriable.removeAll(result.failedKeys.keys)
        actualRetriable.removeAll(result.unmappedKeys.toSet())
        assertEquals(expectedRetriable, actualRetriable)
    }

    companion object {

        private val t0p0 = TopicPartition("t0", 0)

        private val t0p1 = TopicPartition("t0", 1)

        private val t1p0 = TopicPartition("t1", 0)

        private val t1p1 = TopicPartition("t1", 1)

        private val node = Node(1, "host", 1234)


        private val offsetTimestampsByPartition = mapOf(
            t0p0 to ListOffsetsRequest.LATEST_TIMESTAMP,
            t0p1 to ListOffsetsRequest.EARLIEST_TIMESTAMP,
            t1p0 to 123L,
            t1p1 to ListOffsetsRequest.MAX_TIMESTAMP,
        )

        private fun mapToError(
            keys: Set<TopicPartition>,
            throwable: Throwable,
        ): Map<TopicPartition, Throwable> = keys.associateWith { throwable }

        private fun createResponse(
            errorsByPartition: Map<TopicPartition, Short>,
            specsByPartition: Map<TopicPartition, Long> = offsetTimestampsByPartition,
        ): ListOffsetsResponse {
            val responsesByTopic: MutableMap<String, ListOffsetsTopicResponse> = HashMap()
            for ((topicPartition, value) in specsByPartition) {
                val topicResponse = responsesByTopic.computeIfAbsent(topicPartition.topic) {
                    ListOffsetsTopicResponse()
                }
                topicResponse.setName(topicPartition.topic)
                val partitionResponse = ListOffsetsPartitionResponse()
                partitionResponse.setPartitionIndex(topicPartition.partition)
                partitionResponse.setOffset(getOffset(topicPartition, value))
                partitionResponse.setErrorCode(errorsByPartition[topicPartition] ?: 0)
                topicResponse.partitions += partitionResponse
            }
            val responseData = ListOffsetsResponseData()
            responseData.setTopics(responsesByTopic.values.toList())

            return ListOffsetsResponse(responseData)
        }

        private fun getOffset(topicPartition: TopicPartition, offsetQuery: Long): Long {
            val base = (1 shl 10).toLong()
            return if (offsetQuery == ListOffsetsRequest.EARLIEST_TIMESTAMP)
                topicPartition.hashCode().toLong() and base - 1
            else if (offsetQuery >= 0L) base
            else if (offsetQuery == ListOffsetsRequest.LATEST_TIMESTAMP)
                base + 1 + (topicPartition.hashCode().toLong() and base - 1)
            else 2 * base + 1
        }
    }
}
