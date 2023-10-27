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

import org.apache.kafka.clients.admin.DescribeProducersOptions
import org.apache.kafka.clients.admin.DescribeProducersResult.PartitionProducerState
import org.apache.kafka.clients.admin.internals.AdminApiHandler.ApiResult
import org.apache.kafka.common.Node
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.InvalidTopicException
import org.apache.kafka.common.errors.NotLeaderOrFollowerException
import org.apache.kafka.common.errors.TopicAuthorizationException
import org.apache.kafka.common.errors.UnknownServerException
import org.apache.kafka.common.message.DescribeProducersRequestData.TopicRequest
import org.apache.kafka.common.message.DescribeProducersResponseData
import org.apache.kafka.common.message.DescribeProducersResponseData.TopicResponse
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.DescribeProducersResponse
import org.apache.kafka.common.utils.CollectionUtils.groupPartitionDataByTopic
import org.apache.kafka.common.utils.LogContext
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

class DescribeProducersHandlerTest {
    
    private fun newHandler(options: DescribeProducersOptions): DescribeProducersHandler =
        DescribeProducersHandler(options, LogContext())

    @Test
    fun testBrokerIdSetInOptions() {
        val brokerId = 3
        val topicPartitions: Set<TopicPartition> = setOf(
            TopicPartition("foo", 5),
            TopicPartition("bar", 3),
            TopicPartition("foo", 4),
        )
        val handler = newHandler(DescribeProducersOptions().apply {this.brokerId = brokerId })
        topicPartitions.forEach { topicPartition ->
            val scope = handler.lookupStrategy().lookupScope(topicPartition)
            assertEquals(
                expected = brokerId,
                actual = scope.destinationBrokerId(),
                message = "Unexpected brokerId for $topicPartition",
            )
        }
    }

    @Test
    fun testBrokerIdNotSetInOptions() {
        val topicPartitions: Set<TopicPartition> = setOf(
            TopicPartition("foo", 5),
            TopicPartition("bar", 3),
            TopicPartition("foo", 4),
        )
        val handler = newHandler(DescribeProducersOptions())
        topicPartitions.forEach { topicPartition ->
            val scope = handler.lookupStrategy().lookupScope(topicPartition)
            assertEquals(
                expected = null,
                actual = scope.destinationBrokerId(),
                message = "Unexpected brokerId for $topicPartition",
            )
        }
    }

    @Test
    fun testBuildRequest() {
        val topicPartitions: Set<TopicPartition> = setOf(
            TopicPartition("foo", 5),
            TopicPartition("bar", 3),
            TopicPartition("foo", 4),
        )
        val handler = newHandler(DescribeProducersOptions())
        val brokerId = 3
        val request = handler.buildBatchedRequest(brokerId, topicPartitions)
        val topics = request.data.topics
        
        assertEquals(
            expected = setOf("foo", "bar"),
            actual = topics.map(TopicRequest::name).toSet(),
        )
        
        topics.forEach { topic ->
            val expectedTopicPartitions = if ("foo" == topic.name) setOf(4, 5) else setOf(3)
            assertEquals(expectedTopicPartitions, topic.partitionIndexes.toSet())
        }
    }

    @Test
    fun testAuthorizationFailure() {
        val topicPartition = TopicPartition("foo", 5)
        val exception = assertFatalError(topicPartition, Errors.TOPIC_AUTHORIZATION_FAILED)
        
        assertTrue(exception is TopicAuthorizationException)
        
        val authException = exception as TopicAuthorizationException?
        
        assertEquals(setOf("foo"), authException!!.unauthorizedTopics)
    }

    @Test
    fun testInvalidTopic() {
        val topicPartition = TopicPartition("foo", 5)
        val exception = assertFatalError(topicPartition, Errors.INVALID_TOPIC_EXCEPTION)
        
        assertTrue(exception is InvalidTopicException)
        
        val invalidTopicException = exception as InvalidTopicException?
        assertEquals(setOf("foo"), invalidTopicException!!.invalidTopics)
    }

    @Test
    fun testUnexpectedError() {
        val topicPartition = TopicPartition("foo", 5)
        val exception = assertFatalError(topicPartition, Errors.UNKNOWN_SERVER_ERROR)
        assertTrue(exception is UnknownServerException)
    }

    @Test
    fun testRetriableErrors() {
        val topicPartition = TopicPartition("foo", 5)
        assertRetriableError(topicPartition, Errors.UNKNOWN_TOPIC_OR_PARTITION)
    }

    @Test
    fun testUnmappedAfterNotLeaderError() {
        val topicPartition = TopicPartition("foo", 5)
        val result = handleResponseWithError(DescribeProducersOptions(), topicPartition, Errors.NOT_LEADER_OR_FOLLOWER)
        
        assertEquals(emptyMap(), result.failedKeys)
        assertEquals(emptyMap(), result.completedKeys)
        assertEquals(listOf(topicPartition), result.unmappedKeys)
    }

    @Test
    fun testFatalNotLeaderErrorIfStaticMapped() {
        val topicPartition = TopicPartition("foo", 5)
        val options = DescribeProducersOptions().apply { brokerId = 1 }
        val result = handleResponseWithError(options, topicPartition, Errors.NOT_LEADER_OR_FOLLOWER)
        assertEquals(emptyMap(), result.completedKeys)
        assertEquals(emptyList(), result.unmappedKeys)
        assertEquals(setOf(topicPartition), result.failedKeys.keys)
        val exception = result.failedKeys[topicPartition]
        assertTrue(exception is NotLeaderOrFollowerException)
    }

    @Test
    fun testCompletedResult() {
        val topicPartition = TopicPartition("foo", 5)
        val options = DescribeProducersOptions().apply { brokerId = 1 }
        val handler = newHandler(options)
        val partitionResponse = sampleProducerState(topicPartition)
        val response = describeProducersResponse(mapOf(topicPartition to partitionResponse))
        val node = Node(3, "host", 1)
        val result = handler.handleResponse(node, setOf(topicPartition), response)
        assertEquals(setOf(topicPartition), result.completedKeys.keys)
        assertEquals(emptyMap(), result.failedKeys)
        assertEquals(emptyList(), result.unmappedKeys)
        val producerState = result.completedKeys[topicPartition]
        assertMatchingProducers(partitionResponse, producerState)
    }

    private fun assertRetriableError(topicPartition: TopicPartition, error: Errors) {
        val result = handleResponseWithError(DescribeProducersOptions(), topicPartition, error)
        assertEquals(emptyMap(), result.failedKeys)
        assertEquals(emptyMap(), result.completedKeys)
        assertEquals(emptyList(), result.unmappedKeys)
    }

    private fun assertFatalError(topicPartition: TopicPartition, error: Errors): Throwable? {
        val result = handleResponseWithError(
            options = DescribeProducersOptions(),
            topicPartition = topicPartition,
            error = error,
        )
        assertEquals(emptyMap(), result.completedKeys)
        assertEquals(emptyList(), result.unmappedKeys)
        assertEquals(setOf(topicPartition), result.failedKeys.keys)
        return result.failedKeys[topicPartition]
    }

    private fun handleResponseWithError(
        options: DescribeProducersOptions,
        topicPartition: TopicPartition,
        error: Errors,
    ): ApiResult<TopicPartition, PartitionProducerState> {
        val handler = newHandler(options)
        val response = buildResponseWithError(topicPartition, error)
        val node = Node(id = options.brokerId ?: 3, host = "host", port = 1)
        return handler.handleResponse(node, setOf(topicPartition), response)
    }

    private fun buildResponseWithError(
        topicPartition: TopicPartition,
        error: Errors,
    ): DescribeProducersResponse {
        val partitionResponse = DescribeProducersResponseData.PartitionResponse()
            .setPartitionIndex(topicPartition.partition)
            .setErrorCode(error.code)
        return describeProducersResponse(mapOf(topicPartition to partitionResponse))
    }

    private fun sampleProducerState(topicPartition: TopicPartition): DescribeProducersResponseData.PartitionResponse {
        val partitionResponse = DescribeProducersResponseData.PartitionResponse()
            .setPartitionIndex(topicPartition.partition)
            .setErrorCode(Errors.NONE.code)

        partitionResponse.setActiveProducers(
            listOf(
                DescribeProducersResponseData.ProducerState()
                    .setProducerId(12345L)
                    .setProducerEpoch(15)
                    .setLastSequence(75)
                    .setLastTimestamp(System.currentTimeMillis())
                    .setCurrentTxnStartOffset(-1L),
                DescribeProducersResponseData.ProducerState()
                    .setProducerId(98765L)
                    .setProducerEpoch(30)
                    .setLastSequence(150)
                    .setLastTimestamp(System.currentTimeMillis() - 5000)
                    .setCurrentTxnStartOffset(5000),
            )
        )
        return partitionResponse
    }

    private fun assertMatchingProducers(
        expected: DescribeProducersResponseData.PartitionResponse,
        actual: PartitionProducerState?,
    ) {
        val expectedProducers = expected.activeProducers
        val actualProducers = actual!!.activeProducers()
        assertEquals(expectedProducers.size, actualProducers.size)
        val expectedByProducerId = expectedProducers.associateBy { it.producerId }

        for (actualProducerState in actualProducers)
            with(expectedByProducerId[actualProducerState.producerId]) {
                assertNotNull(this)
                assertEquals(producerEpoch, actualProducerState.producerEpoch)
                assertEquals(lastSequence, actualProducerState.lastSequence)
                assertEquals(lastTimestamp, actualProducerState.lastTimestamp)
                assertEquals(currentTxnStartOffset, actualProducerState.currentTransactionStartOffset ?: -1L)
            }
    }

    private fun describeProducersResponse(
        partitionResponses: Map<TopicPartition, DescribeProducersResponseData.PartitionResponse>,
    ): DescribeProducersResponse {
        val response = DescribeProducersResponseData()
        val partitionResponsesByTopic = groupPartitionDataByTopic(partitionResponses)

        for ((topic, topicPartitionResponses) in partitionResponsesByTopic) {
            val topicResponse = TopicResponse().setName(topic)
            response.topics += topicResponse

            for ((partitionId, partitionResponse) in topicPartitionResponses)
                topicResponse.partitions += partitionResponse.setPartitionIndex(partitionId)
        }
        return DescribeProducersResponse(response)
    }
}
