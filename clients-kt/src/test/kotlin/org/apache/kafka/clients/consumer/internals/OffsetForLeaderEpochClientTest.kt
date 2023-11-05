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

package org.apache.kafka.clients.consumer.internals

import org.apache.kafka.clients.Metadata
import org.apache.kafka.clients.Metadata.LeaderAndEpoch
import org.apache.kafka.clients.MockClient
import org.apache.kafka.clients.consumer.OffsetResetStrategy
import org.apache.kafka.clients.consumer.internals.SubscriptionState.FetchPosition
import org.apache.kafka.common.Node
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.TopicAuthorizationException
import org.apache.kafka.common.internals.ClusterResourceListeners
import org.apache.kafka.common.message.OffsetForLeaderEpochResponseData
import org.apache.kafka.common.message.OffsetForLeaderEpochResponseData.OffsetForLeaderTopicResult
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.OffsetsForLeaderEpochResponse
import org.apache.kafka.common.utils.LogContext
import org.apache.kafka.common.utils.MockTime
import org.apache.kafka.common.utils.Time
import org.junit.jupiter.api.Test
import kotlin.test.assertContains
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertTrue

class OffsetForLeaderEpochClientTest {
    
    private lateinit var consumerClient: ConsumerNetworkClient
    
    private lateinit var subscriptions: SubscriptionState
    
    private lateinit var metadata: Metadata
    
    private lateinit var client: MockClient
    
    private lateinit var time: Time
    
    private val tp0 = TopicPartition("topic", 0)
    
    @Test
    fun testEmptyResponse() {
        val offsetClient = newOffsetClient()
        val future = offsetClient.sendAsyncRequest(Node.noNode(), emptyMap())
        val resp = OffsetsForLeaderEpochResponse(
            OffsetForLeaderEpochResponseData()
        )
        client.prepareResponse(resp)
        consumerClient.pollNoWakeup()
        val (endOffsets, partitionsToRetry) = future.value()
        assertTrue(partitionsToRetry.isEmpty())
        assertTrue(endOffsets.isEmpty())
    }

    @Test
    fun testUnexpectedEmptyResponse() {
        val positionMap = mapOf(
            tp0 to FetchPosition(
                offset = 0,
                offsetEpoch = 1,
                currentLeader = LeaderAndEpoch(leader = null, epoch = 1),
            ),
        )
        val offsetClient = newOffsetClient()
        val future = offsetClient.sendAsyncRequest(Node.noNode(), positionMap)
        val resp = OffsetsForLeaderEpochResponse(OffsetForLeaderEpochResponseData())
        client.prepareResponse(resp)
        consumerClient.pollNoWakeup()
        val (endOffsets, partitionsToRetry) = future.value()
        assertFalse(partitionsToRetry.isEmpty())
        assertTrue(endOffsets.isEmpty())
    }

    @Test
    fun testOkResponse() {
        val positionMap = mapOf(
            tp0 to FetchPosition(
                offset = 0,
                offsetEpoch = 1,
                currentLeader = LeaderAndEpoch(leader = null, epoch = 1)
            ),
        )
        val offsetClient = newOffsetClient()
        val future = offsetClient.sendAsyncRequest(Node.noNode(), positionMap)
        client.prepareResponse(
            response = prepareOffsetForLeaderEpochResponse(
                tp = tp0,
                error = Errors.NONE,
                leaderEpoch = 1,
                endOffset = 10L,
            ),
        )
        consumerClient.pollNoWakeup()
        val (endOffsets, partitionsToRetry) = future.value()
        assertTrue(partitionsToRetry.isEmpty())
        assertTrue(endOffsets.containsKey(tp0))
        assertEquals(endOffsets[tp0]!!.errorCode, Errors.NONE.code)
        assertEquals(endOffsets[tp0]!!.leaderEpoch, 1)
        assertEquals(endOffsets[tp0]!!.endOffset, 10L)
    }

    @Test
    fun testUnauthorizedTopic() {
        val positionMap = mapOf(
            tp0 to FetchPosition(
                offset = 0,
                offsetEpoch = 1,
                currentLeader = LeaderAndEpoch(leader = null, epoch = 1),
            ),
        )
        val offsetClient = newOffsetClient()
        val future = offsetClient.sendAsyncRequest(Node.noNode(), positionMap)
        client.prepareResponse(
            response = prepareOffsetForLeaderEpochResponse(
                tp = tp0,
                error = Errors.TOPIC_AUTHORIZATION_FAILED,
                leaderEpoch = -1,
                endOffset = -1,
            ),
        )
        consumerClient.pollNoWakeup()
        assertTrue(future.failed())
        val exception = future.exception()
        assertIs<TopicAuthorizationException>(exception)
        assertContains(exception.unauthorizedTopics, tp0.topic)
    }

    @Test
    fun testRetriableError() {
        val positionMap = mapOf(
            tp0 to FetchPosition(
                offset = 0,
                offsetEpoch = 1,
                currentLeader = LeaderAndEpoch(leader = null, epoch = 1),
            ),
        )
        val offsetClient = newOffsetClient()
        val future = offsetClient.sendAsyncRequest(Node.noNode(), positionMap)
        client.prepareResponse(
            response = prepareOffsetForLeaderEpochResponse(
                tp = tp0,
                error = Errors.LEADER_NOT_AVAILABLE,
                leaderEpoch = -1,
                endOffset = -1,
            ),
        )
        consumerClient.pollNoWakeup()
        assertFalse(future.failed())
        val (endOffsets, partitionsToRetry) = future.value()
        assertTrue(partitionsToRetry.contains(tp0))
        assertFalse(endOffsets.containsKey(tp0))
    }

    private fun newOffsetClient(): OffsetsForLeaderEpochClient {
        buildDependencies(OffsetResetStrategy.EARLIEST)
        return OffsetsForLeaderEpochClient(consumerClient, LogContext())
    }

    private fun buildDependencies(offsetResetStrategy: OffsetResetStrategy) {
        val logContext = LogContext()
        time = MockTime(1)
        subscriptions = SubscriptionState(logContext, offsetResetStrategy)
        metadata = ConsumerMetadata(
            refreshBackoffMs = 0,
            metadataExpireMs = Long.MAX_VALUE,
            includeInternalTopics = false,
            allowAutoTopicCreation = false,
            subscription = subscriptions,
            logContext = logContext,
            clusterResourceListeners = ClusterResourceListeners(),
        )
        client = MockClient(time, metadata)
        consumerClient = ConsumerNetworkClient(
            logContext = logContext,
            client = client,
            metadata = metadata,
            time = time,
            retryBackoffMs = 100,
            requestTimeoutMs = 1000,
            maxPollTimeoutMs = Int.MAX_VALUE,
        )
    }

    companion object {

        private fun prepareOffsetForLeaderEpochResponse(
            tp: TopicPartition,
            error: Errors,
            leaderEpoch: Int,
            endOffset: Long,
        ): OffsetsForLeaderEpochResponse {
            val data = OffsetForLeaderEpochResponseData()
            val topic = OffsetForLeaderTopicResult().setTopic(tp.topic)
            data.topics.add(topic)
            topic.partitions += OffsetForLeaderEpochResponseData.EpochEndOffset()
                .setPartition(tp.partition)
                .setErrorCode(error.code)
                .setLeaderEpoch(leaderEpoch)
                .setEndOffset(endOffset)

            return OffsetsForLeaderEpochResponse(data)
        }
    }
}
