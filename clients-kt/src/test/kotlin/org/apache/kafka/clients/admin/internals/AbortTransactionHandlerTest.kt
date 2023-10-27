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

import org.apache.kafka.clients.admin.AbortTransactionSpec
import org.apache.kafka.clients.admin.internals.AdminApiHandler.ApiResult
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.Node
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.ClusterAuthorizationException
import org.apache.kafka.common.errors.InvalidProducerEpochException
import org.apache.kafka.common.errors.TransactionCoordinatorFencedException
import org.apache.kafka.common.errors.UnknownServerException
import org.apache.kafka.common.message.WriteTxnMarkersRequestData.WritableTxnMarkerTopic
import org.apache.kafka.common.message.WriteTxnMarkersResponseData
import org.apache.kafka.common.message.WriteTxnMarkersResponseData.WritableTxnMarkerPartitionResult
import org.apache.kafka.common.message.WriteTxnMarkersResponseData.WritableTxnMarkerResult
import org.apache.kafka.common.message.WriteTxnMarkersResponseData.WritableTxnMarkerTopicResult
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.WriteTxnMarkersResponse
import org.apache.kafka.common.utils.LogContext
import kotlin.reflect.KClass
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull
import kotlin.test.assertTrue

class AbortTransactionHandlerTest {

    private val logContext = LogContext()

    private val topicPartition = TopicPartition(topic = "foo", partition = 5)

    private val abortSpec = AbortTransactionSpec(
        producerId = 12345L,
        producerEpoch = 15.toShort(),
        coordinatorEpoch = 4321,
        topicPartition = topicPartition,
    )

    private val node = Node(id = 1, host = "host", port = 1234)

    @Test
    fun testInvalidBuildRequestCall() {
        val handler = AbortTransactionHandler(abortSpec, logContext)
        assertFailsWith<IllegalArgumentException> {
            handler.buildRequest(brokerId = 1, keys = emptySet())
        }
        assertFailsWith<IllegalArgumentException> {
            handler.buildRequest(brokerId = 1, keys = setOf(TopicPartition("foo", 1)))
        }
        assertFailsWith<IllegalArgumentException> {
            handler.buildRequest(brokerId = 1, keys = setOf(topicPartition, TopicPartition("foo", 1)))
        }
    }

    @Test
    fun testValidBuildRequestCall() {
        val handler = AbortTransactionHandler(abortSpec, logContext)
        val request = handler.buildBatchedRequest(1, setOf(topicPartition))
        assertEquals(
            expected = 1,
            actual = request.data.markers.size,
        )

        val markerRequest = request.data.markers[0]

        assertEquals(
            expected = abortSpec.producerId,
            actual = markerRequest.producerId,
        )
        assertEquals(
            expected = abortSpec.producerEpoch,
            actual = markerRequest.producerEpoch,
        )
        assertEquals(
            expected = abortSpec.coordinatorEpoch,
            actual = markerRequest.coordinatorEpoch,
        )
        assertEquals(
            expected = 1,
            actual = markerRequest.topics.size,
        )
        val topicRequest: WritableTxnMarkerTopic = markerRequest.topics[0]
        assertEquals(
            expected = abortSpec.topicPartition.topic,
            actual = topicRequest.name,
        )
        assertEquals(
            expected = intArrayOf(abortSpec.topicPartition.partition),
            actual = topicRequest.partitionIndexes,
        )
    }

    @Test
    fun testInvalidHandleResponseCall() {
        val handler = AbortTransactionHandler(abortSpec, logContext)
        val response = WriteTxnMarkersResponseData()
        assertFailsWith<IllegalArgumentException> {
            handler.handleResponse(
                broker = node,
                keys = emptySet(),
                response = WriteTxnMarkersResponse(response),
            )
        }
        assertFailsWith<IllegalArgumentException> {
            handler.handleResponse(
                broker = node,
                keys = setOf(TopicPartition("foo", 1)),
                response = WriteTxnMarkersResponse(response),
            )
        }
        assertFailsWith<IllegalArgumentException> {
            handler.handleResponse(
                broker = node,
                keys = setOf(topicPartition, TopicPartition("foo", 1)),
                response = WriteTxnMarkersResponse(response),
            )
        }
    }

    @Test
    fun testInvalidResponse() {
        val handler = AbortTransactionHandler(abortSpec, logContext)
        val response = WriteTxnMarkersResponseData()
        assertFailed(
            expectedExceptionType = KafkaException::class,
            topicPartition = topicPartition,
            result = handler.handleResponse(
                broker = node,
                keys = setOf(topicPartition),
                response = WriteTxnMarkersResponse(response),
            ),
        )
        val markerResponse = WritableTxnMarkerResult()
        response.markers += markerResponse
        assertFailed(
            expectedExceptionType = KafkaException::class,
            topicPartition = topicPartition,
            result = handler.handleResponse(
                broker = node,
                keys = setOf(topicPartition),
                response = WriteTxnMarkersResponse(response),
            ),
        )
        markerResponse.setProducerId(abortSpec.producerId)
        assertFailed(
            expectedExceptionType = KafkaException::class,
            topicPartition = topicPartition,
            result = handler.handleResponse(
                broker = node,
                keys = setOf(topicPartition),
                response = WriteTxnMarkersResponse(response),
            ),
        )
        val topicResponse = WritableTxnMarkerTopicResult()
        markerResponse.topics += topicResponse
        assertFailed(
            expectedExceptionType = KafkaException::class,
            topicPartition = topicPartition,
            result = handler.handleResponse(
                broker = node,
                keys = setOf(topicPartition),
                response = WriteTxnMarkersResponse(response),
            ),
        )
        topicResponse.setName(abortSpec.topicPartition.topic)
        assertFailed(
            expectedExceptionType = KafkaException::class,
            topicPartition = topicPartition,
            result = handler.handleResponse(
                broker = node,
                keys = setOf(topicPartition),
                response = WriteTxnMarkersResponse(response),
            ),
        )
        val partitionResponse = WritableTxnMarkerPartitionResult()
        topicResponse.partitions += partitionResponse
        assertFailed(
            expectedExceptionType = KafkaException::class,
            topicPartition = topicPartition,
            result = handler.handleResponse(
                broker = node,
                keys = setOf(topicPartition),
                response = WriteTxnMarkersResponse(response),
            ),
        )
        partitionResponse.setPartitionIndex(abortSpec.topicPartition.partition)
        topicResponse.setName(abortSpec.topicPartition.topic + "random")
        assertFailed(
            expectedExceptionType = KafkaException::class,
            topicPartition = topicPartition,
            result = handler.handleResponse(
                broker = node,
                keys = setOf(topicPartition),
                response = WriteTxnMarkersResponse(response),
            ),
        )
        topicResponse.setName(abortSpec.topicPartition.topic)
        markerResponse.setProducerId(abortSpec.producerId + 1)
        assertFailed(
            expectedExceptionType = KafkaException::class,
            topicPartition = topicPartition,
            result = handler.handleResponse(
                broker = node,
                keys = setOf(topicPartition),
                response = WriteTxnMarkersResponse(response),
            ),
        )
    }

    @Test
    fun testSuccessfulResponse() {
        assertCompleted(abortSpec.topicPartition, handleWithError(abortSpec, Errors.NONE))
    }

    @Test
    fun testRetriableErrors() {
        assertUnmapped(abortSpec.topicPartition, handleWithError(abortSpec, Errors.NOT_LEADER_OR_FOLLOWER))
        assertUnmapped(abortSpec.topicPartition, handleWithError(abortSpec, Errors.UNKNOWN_TOPIC_OR_PARTITION))
        assertUnmapped(abortSpec.topicPartition, handleWithError(abortSpec, Errors.REPLICA_NOT_AVAILABLE))
        assertUnmapped(abortSpec.topicPartition, handleWithError(abortSpec, Errors.BROKER_NOT_AVAILABLE))
    }

    @Test
    fun testFatalErrors() {
        assertFailed(
            expectedExceptionType = ClusterAuthorizationException::class,
            topicPartition = abortSpec.topicPartition,
            result = handleWithError(abortSpec, Errors.CLUSTER_AUTHORIZATION_FAILED),
        )
        assertFailed(
            expectedExceptionType = InvalidProducerEpochException::class,
            topicPartition = abortSpec.topicPartition,
            result = handleWithError(abortSpec, Errors.INVALID_PRODUCER_EPOCH),
        )
        assertFailed(
            expectedExceptionType = TransactionCoordinatorFencedException::class,
            topicPartition = abortSpec.topicPartition,
            result = handleWithError(abortSpec, Errors.TRANSACTION_COORDINATOR_FENCED),
        )
        assertFailed(
            expectedExceptionType = UnknownServerException::class,
            topicPartition = abortSpec.topicPartition,
            result = handleWithError(abortSpec, Errors.UNKNOWN_SERVER_ERROR),
        )
    }

    private fun handleWithError(
        abortSpec: AbortTransactionSpec,
        error: Errors,
    ): ApiResult<TopicPartition, Unit> {
        val handler = AbortTransactionHandler(abortSpec, logContext)
        val partitionResponse = WritableTxnMarkerPartitionResult()
            .setPartitionIndex(abortSpec.topicPartition.partition)
            .setErrorCode(error.code)
        val topicResponse = WritableTxnMarkerTopicResult()
            .setName(abortSpec.topicPartition.topic)
        topicResponse.partitions += partitionResponse
        val markerResponse = WritableTxnMarkerResult()
            .setProducerId(abortSpec.producerId)
        markerResponse.topics += topicResponse
        val response = WriteTxnMarkersResponseData()
        response.markers += markerResponse
        return handler.handleResponse(
            node, setOf(abortSpec.topicPartition),
            WriteTxnMarkersResponse(response)
        )
    }

    private fun assertUnmapped(
        topicPartition: TopicPartition,
        result: ApiResult<TopicPartition, Unit>,
    ) {
        assertEquals(emptySet(), result.completedKeys.keys)
        assertEquals(emptySet(), result.failedKeys.keys)
        assertEquals(listOf(topicPartition), result.unmappedKeys)
    }

    private fun assertCompleted(
        topicPartition: TopicPartition,
        result: ApiResult<TopicPartition, Unit>,
    ) {
        assertEquals(emptySet(), result.failedKeys.keys)
        assertEquals(emptyList(), result.unmappedKeys)
        assertEquals(setOf(topicPartition), result.completedKeys.keys)
        assertNull(result.completedKeys[topicPartition])
    }

    private fun assertFailed(
        expectedExceptionType: KClass<out Throwable>,
        topicPartition: TopicPartition,
        result: ApiResult<TopicPartition, Unit>,
    ) {
        assertEquals(emptySet(), result.completedKeys.keys)
        assertEquals(emptyList(), result.unmappedKeys)
        assertEquals(setOf(topicPartition), result.failedKeys.keys)
        assertTrue(expectedExceptionType.isInstance(result.failedKeys[topicPartition]))
    }
}
