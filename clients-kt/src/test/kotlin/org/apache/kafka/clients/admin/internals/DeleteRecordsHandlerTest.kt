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

import org.apache.kafka.clients.admin.DeletedRecords
import org.apache.kafka.clients.admin.RecordsToDelete
import org.apache.kafka.clients.admin.internals.AdminApiHandler.ApiResult
import org.apache.kafka.common.Node
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.message.DeleteRecordsResponseData
import org.apache.kafka.common.message.DeleteRecordsResponseData.DeleteRecordsPartitionResult
import org.apache.kafka.common.message.DeleteRecordsResponseData.DeleteRecordsTopicResult
import org.apache.kafka.common.message.DeleteRecordsResponseData.DeleteRecordsTopicResultCollection
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.DeleteRecordsResponse
import org.apache.kafka.common.utils.LogContext
import org.junit.jupiter.api.Test
import kotlin.test.assertContains
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class DeleteRecordsHandlerTest {
    
    private val logContext = LogContext()
    
    private val timeout = 2000
    
    private val t0p0 = TopicPartition("t0", 0)
    
    private val t0p1 = TopicPartition("t0", 1)
    
    private val t0p2 = TopicPartition("t0", 2)
    
    private val t0p3 = TopicPartition("t0", 3)
    
    private val node = Node(1, "host", 1234)
    
    private val recordsToDelete: Map<TopicPartition, RecordsToDelete> = mapOf(
        t0p0 to RecordsToDelete(beforeOffset = 10L),
        t0p1 to RecordsToDelete(beforeOffset = 10L),
        t0p2 to RecordsToDelete(beforeOffset = 10L),
        t0p3 to RecordsToDelete(beforeOffset = 10L),
    )
    @Test
    fun testBuildRequestSimple() {
        val handler = DeleteRecordsHandler(recordsToDelete, logContext, timeout)
        val request = handler.buildBatchedRequest(
            brokerId = node.id,
            keys = setOf(t0p0, t0p1),
        ).build()
        val topicPartitions = request.data().topics
        assertEquals(1, topicPartitions.size)
        val topic = topicPartitions[0]
        assertEquals(4, topic.partitions.size)
    }

    @Test
    fun testHandleSuccessfulResponse() {
        val result = handleResponse(createResponse(emptyMap(), recordsToDelete.keys))
        assertResult(result, recordsToDelete.keys, emptyMap<TopicPartition, Throwable>(), emptyList(), emptySet())
    }

    @Test
    fun testHandleRetriablePartitionTimeoutResponse() {
        val errorPartition = t0p0
        val errorsByPartition = mapOf(errorPartition to Errors.REQUEST_TIMED_OUT.code)
        val result = handleResponse(createResponse(errorsByPartition))

        // Timeouts should be retried within the fulfillment stage as they are a common type of
        // retriable error.
        val retriable = setOf(errorPartition)
        val completed = recordsToDelete.keys - retriable
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
        val unmapped = setOf(errorPartition)
        val completed = recordsToDelete.keys - unmapped
        assertResult(
            result = result,
            expectedCompleted = completed,
            expectedFailed = emptyMap<TopicPartition, Throwable>(),
            expectedUnmapped = unmapped.toList(),
            expectedRetriable = emptySet()
        )
    }

    @Test
    fun testHandlePartitionErrorResponse() {
        val errorPartition = t0p0
        val error = Errors.TOPIC_AUTHORIZATION_FAILED
        val errorsByPartition = mapOf(errorPartition to error.code)

        val result = handleResponse(createResponse(errorsByPartition))

        val failed = mapOf(errorPartition to error.exception)
        val completed = recordsToDelete.keys - failed.keys
        assertResult(
            result = result,
            expectedCompleted = completed,
            expectedFailed = failed,
            expectedUnmapped = emptyList(),
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
        val completed = recordsToDelete.keys - failed.keys
        assertResult(
            result = result,
            expectedCompleted = completed,
            expectedFailed = failed,
            expectedUnmapped = emptyList(),
            expectedRetriable = emptySet(),
        )
    }

    @Test
    fun testMixedResponse() {
        val errorPartition = t0p0
        val error = Errors.UNKNOWN_SERVER_ERROR
        val retriableErrorPartition = t0p1
        val retriableError = Errors.NOT_LEADER_OR_FOLLOWER
        val retriableErrorPartition2 = t0p2
        val retriableError2 = Errors.REQUEST_TIMED_OUT

        val errorsByPartition = mapOf(
            errorPartition to error.code,
            retriableErrorPartition to retriableError.code,
            retriableErrorPartition2 to retriableError2.code,
        )

        val result = handleResponse(createResponse(errorsByPartition))
        val completed = recordsToDelete.keys.toMutableSet()

        val failed = mapOf(errorPartition to error.exception)
        completed.removeAll(failed.keys)

        val unmapped = listOf(retriableErrorPartition)
        completed.removeAll(unmapped.toSet())

        val retriable = setOf(retriableErrorPartition2)
        completed.removeAll(retriable)

        assertResult(
            result = result,
            expectedCompleted = completed,
            expectedFailed = failed,
            expectedUnmapped = unmapped,
            expectedRetriable = retriable,
        )
    }

    @Test
    fun testHandleResponseSanityCheck() {
        val errorPartition = t0p0
        val recordsToDeleteMap = recordsToDelete.toMutableMap()
        recordsToDeleteMap.remove(errorPartition)

        val result = handleResponse(createResponse(emptyMap(), recordsToDeleteMap.keys))

        assertEquals(recordsToDelete.size - 1, result.completedKeys.size)
        assertEquals(1, result.failedKeys.size)
        assertEquals(errorPartition, result.failedKeys.keys.firstOrNull())

        val sanityCheckMessage = result.failedKeys[errorPartition]!!.message!!
        assertContains(sanityCheckMessage, "did not contain a result for topic partition")
        assertTrue(result.unmappedKeys.isEmpty())
    }

    private fun createResponse(
        errorsByPartition: Map<TopicPartition, Short>,
        topicPartitions: Set<TopicPartition> = recordsToDelete.keys,
    ): DeleteRecordsResponse {
        val responsesByTopic = mutableMapOf<String, DeleteRecordsTopicResultCollection>()

        lateinit var topicResponse: DeleteRecordsTopicResultCollection
        for (topicPartition in topicPartitions) {
            topicResponse = responsesByTopic.computeIfAbsent(topicPartition.topic) {
                DeleteRecordsTopicResultCollection()
            }
            topicResponse.add(DeleteRecordsTopicResult().setName(topicPartition.topic))

            val partitionResponse = DeleteRecordsPartitionResult()
            partitionResponse.setPartitionIndex(topicPartition.partition)
            partitionResponse.setErrorCode(errorsByPartition[topicPartition] ?: 0)

            topicResponse.find(topicPartition.topic)!!.partitions.add(partitionResponse)
        }
        val responseData = DeleteRecordsResponseData()
        responseData.setTopics(topicResponse)
        return DeleteRecordsResponse(responseData)
    }

    private fun handleResponse(response: DeleteRecordsResponse): ApiResult<TopicPartition, DeletedRecords> {
        val handler = DeleteRecordsHandler(
            recordsToDelete = recordsToDelete,
            logContext = logContext,
            timeout = timeout,
        )
        return handler.handleResponse(
            broker = node,
            keys = recordsToDelete.keys,
            response = response,
        )
    }

    private fun assertResult(
        result: ApiResult<TopicPartition, DeletedRecords>,
        expectedCompleted: Set<TopicPartition>,
        expectedFailed: Map<TopicPartition, Throwable?>,
        expectedUnmapped: List<TopicPartition>,
        expectedRetriable: Set<TopicPartition>,
    ) {
        assertEquals(expectedCompleted, result.completedKeys.keys)
        assertEquals(expectedFailed, result.failedKeys)
        assertEquals(expectedUnmapped, result.unmappedKeys)

        val actualRetriable = recordsToDelete.keys.toMutableSet()
        actualRetriable.removeAll(result.completedKeys.keys)
        actualRetriable.removeAll(result.failedKeys.keys)
        actualRetriable.removeAll(HashSet(result.unmappedKeys))

        assertEquals(expectedRetriable, actualRetriable)
    }
}
