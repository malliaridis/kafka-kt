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

import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.InvalidTopicException
import org.apache.kafka.common.errors.TopicAuthorizationException
import org.apache.kafka.common.errors.UnknownServerException
import org.apache.kafka.common.message.MetadataResponseData
import org.apache.kafka.common.message.MetadataResponseData.MetadataResponsePartition
import org.apache.kafka.common.message.MetadataResponseData.MetadataResponseTopic
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.MetadataResponse
import org.apache.kafka.common.utils.LogContext
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertIs

class PartitionLeaderStrategyTest {
    
    private fun newStrategy(): PartitionLeaderStrategy = PartitionLeaderStrategy(LogContext())

    @Test
    fun testBuildLookupRequest() {
        val topicPartitions: Set<TopicPartition> = setOf(
            TopicPartition("foo", 0),
            TopicPartition("bar", 0),
            TopicPartition("foo", 1),
            TopicPartition("baz", 0)
        )
        val strategy = newStrategy()
        val allRequest = strategy.buildRequest(topicPartitions).build()
        
        assertEquals(setOf("foo", "bar", "baz"), allRequest.topics()?.toSet())
        assertFalse(allRequest.allowAutoTopicCreation())
        
        val partialRequest = strategy.buildRequest(
            topicPartitions.filter { it.topic == "foo" }.toSet()
        ).build()
        
        assertEquals(setOf("foo"), partialRequest.topics()?.toSet())
        assertFalse(partialRequest.allowAutoTopicCreation())
    }

    @Test
    fun testTopicAuthorizationFailure() {
        val topicPartition = TopicPartition("foo", 0)
        val exception = assertFatalTopicError(topicPartition, Errors.TOPIC_AUTHORIZATION_FAILED)
        
        assertIs<TopicAuthorizationException>(exception)
        assertEquals(setOf("foo"), exception.unauthorizedTopics)
    }

    @Test
    fun testInvalidTopicError() {
        val topicPartition = TopicPartition("foo", 0)
        val exception = assertFatalTopicError(topicPartition, Errors.INVALID_TOPIC_EXCEPTION)
        
        assertIs<InvalidTopicException>(exception)
        assertEquals(setOf("foo"), exception.invalidTopics)
    }

    @Test
    fun testUnexpectedTopicError() {
        val topicPartition = TopicPartition("foo", 0)
        val exception = assertFatalTopicError(topicPartition, Errors.UNKNOWN_SERVER_ERROR)
        
        assertIs<UnknownServerException>(exception)
    }

    @Test
    fun testRetriableTopicErrors() {
        val topicPartition = TopicPartition("foo", 0)
        
        assertRetriableTopicError(topicPartition, Errors.UNKNOWN_TOPIC_OR_PARTITION)
        assertRetriableTopicError(topicPartition, Errors.LEADER_NOT_AVAILABLE)
        assertRetriableTopicError(topicPartition, Errors.BROKER_NOT_AVAILABLE)
    }

    @Test
    fun testRetriablePartitionErrors() {
        val topicPartition = TopicPartition("foo", 0)
        
        assertRetriablePartitionError(topicPartition, Errors.NOT_LEADER_OR_FOLLOWER)
        assertRetriablePartitionError(topicPartition, Errors.REPLICA_NOT_AVAILABLE)
        assertRetriablePartitionError(topicPartition, Errors.LEADER_NOT_AVAILABLE)
        assertRetriablePartitionError(topicPartition, Errors.BROKER_NOT_AVAILABLE)
        assertRetriablePartitionError(topicPartition, Errors.KAFKA_STORAGE_ERROR)
    }

    @Test
    fun testUnexpectedPartitionError() {
        val topicPartition = TopicPartition("foo", 0)
        val exception = assertFatalPartitionError(topicPartition, Errors.UNKNOWN_SERVER_ERROR)
        
        assertIs<UnknownServerException>(exception)
    }

    @Test
    fun testPartitionSuccessfullyMapped() {
        val topicPartition1 = TopicPartition("foo", 0)
        val topicPartition2 = TopicPartition("bar", 1)
        val responsePartitions = mapOf(
            topicPartition1 to partitionResponseDataWithLeader(
                topicPartition = topicPartition1,
                leaderId = 5,
                replicas = intArrayOf(5, 6, 7),
            ),
            topicPartition2 to partitionResponseDataWithLeader(
                topicPartition = topicPartition2,
                leaderId = 1,
                replicas = intArrayOf(2, 1, 3),
            ) ,
        )
        
        val result = handleLookupResponse(
            setOf(topicPartition1, topicPartition2),
            responseWithPartitionData(responsePartitions)
        )

        assertEquals(emptyMap(), result.failedKeys)
        assertEquals(setOf(topicPartition1, topicPartition2), result.mappedKeys.keys)
        assertEquals(5, result.mappedKeys[topicPartition1])
        assertEquals(1, result.mappedKeys[topicPartition2])
    }

    @Test
    fun testIgnoreUnrequestedPartitions() {
        val requestedTopicPartition = TopicPartition("foo", 0)
        val unrequestedTopicPartition = TopicPartition("foo", 1)
        val responsePartitions = mapOf(
            requestedTopicPartition to partitionResponseDataWithLeader(
                topicPartition = requestedTopicPartition,
                leaderId = 5,
                replicas = intArrayOf(5, 6, 7),
            ),
            unrequestedTopicPartition to partitionResponseDataWithError(
                topicPartition = unrequestedTopicPartition,
                error = Errors.UNKNOWN_SERVER_ERROR,
            ),
        )

        val result = handleLookupResponse(
            topicPartitions = setOf(requestedTopicPartition),
            response = responseWithPartitionData(responsePartitions),
        )
        assertEquals(emptyMap(), result.failedKeys)
        assertEquals(setOf(requestedTopicPartition), result.mappedKeys.keys)
        assertEquals(5, result.mappedKeys[requestedTopicPartition])
    }

    @Test
    fun testRetryIfLeaderUnknown() {
        val topicPartition = TopicPartition("foo", 0)
        val responsePartitions = mapOf(
            topicPartition to partitionResponseDataWithLeader(
                topicPartition = topicPartition,
                leaderId = -1,
                replicas = intArrayOf(5, 6, 7),
            )
        )
        val result = handleLookupResponse(
            topicPartitions = setOf(topicPartition),
            response = responseWithPartitionData(responsePartitions),
        )
        assertEquals(emptyMap(), result.failedKeys)
        assertEquals(emptyMap(), result.mappedKeys)
    }

    private fun assertRetriableTopicError(
        topicPartition: TopicPartition,
        error: Errors,
    ) = assertRetriableError(
        topicPartition = topicPartition,
        response = responseWithTopicError(topicPartition.topic, error),
    )

    private fun assertRetriablePartitionError(
        topicPartition: TopicPartition,
        error: Errors,
    ) {
        val response = responseWithPartitionData(
            mapOf(topicPartition to partitionResponseDataWithError(topicPartition, error))
        )
        assertRetriableError(topicPartition, response)
    }

    private fun assertFatalTopicError(
        topicPartition: TopicPartition,
        error: Errors,
    ): Throwable? = assertFatalError(
        topicPartition = topicPartition,
        response = responseWithTopicError(topicPartition.topic, error)
    )

    private fun assertFatalPartitionError(
        topicPartition: TopicPartition,
        error: Errors,
    ): Throwable? = assertFatalError(
        topicPartition = topicPartition,
        response = responseWithPartitionData(
            mapOf(topicPartition to partitionResponseDataWithError(topicPartition, error))
        ),
    )

    private fun assertRetriableError(
        topicPartition: TopicPartition,
        response: MetadataResponse,
    ) {
        val result = handleLookupResponse(setOf(topicPartition), response)

        assertEquals(emptyMap(), result.failedKeys)
        assertEquals(emptyMap(), result.mappedKeys)
    }

    private fun assertFatalError(
        topicPartition: TopicPartition,
        response: MetadataResponse,
    ): Throwable? {
        val result = handleLookupResponse(setOf(topicPartition), response)

        assertEquals(setOf(topicPartition), result.failedKeys.keys)

        return result.failedKeys[topicPartition]
    }

    private fun handleLookupResponse(
        topicPartitions: Set<TopicPartition>,
        response: MetadataResponse,
    ): AdminApiLookupStrategy.LookupResult<TopicPartition> = newStrategy().handleResponse(topicPartitions, response)

    private fun responseWithTopicError(topic: String, error: Errors): MetadataResponse {
        val responseTopic = MetadataResponseTopic()
            .setName(topic)
            .setErrorCode(error.code)
        val responseData = MetadataResponseData()
        responseData.topics.add(responseTopic)
        return MetadataResponse(responseData, ApiKeys.METADATA.latestVersion())
    }

    private fun partitionResponseDataWithError(
        topicPartition: TopicPartition,
        error: Errors,
    ): MetadataResponsePartition = MetadataResponsePartition()
        .setPartitionIndex(topicPartition.partition)
        .setErrorCode(error.code)

    private fun partitionResponseDataWithLeader(
        topicPartition: TopicPartition,
        leaderId: Int,
        replicas: IntArray,
    ): MetadataResponsePartition = MetadataResponsePartition()
        .setPartitionIndex(topicPartition.partition)
        .setErrorCode(Errors.NONE.code)
        .setLeaderId(leaderId)
        .setReplicaNodes(replicas)
        .setIsrNodes(replicas)

    private fun responseWithPartitionData(
        responsePartitions: Map<TopicPartition, MetadataResponsePartition>,
    ): MetadataResponse {
        val responseData = MetadataResponseData()
        for ((topicPartition, value) in responsePartitions) {
            var responseTopic = responseData.topics.find(topicPartition.topic)
            if (responseTopic == null) {
                responseTopic = MetadataResponseTopic()
                    .setName(topicPartition.topic)
                    .setErrorCode(Errors.NONE.code)
                responseData.topics.add(responseTopic)
            }
            responseTopic.partitions += value
        }
        return MetadataResponse(responseData, ApiKeys.METADATA.latestVersion())
    }
}
