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

import java.time.Duration
import org.apache.kafka.clients.ApiVersions
import org.apache.kafka.clients.ClientDnsLookup
import org.apache.kafka.clients.ClientUtils.parseAndValidateAddresses
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.Metadata.LeaderAndEpoch
import org.apache.kafka.clients.MockClient
import org.apache.kafka.clients.MockClient.RequestMatcher
import org.apache.kafka.clients.NodeApiVersions
import org.apache.kafka.clients.consumer.LogTruncationException
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.clients.consumer.OffsetAndTimestamp
import org.apache.kafka.clients.consumer.OffsetResetStrategy
import org.apache.kafka.clients.consumer.internals.SubscriptionState.FetchPosition
import org.apache.kafka.common.IsolationLevel
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.Uuid
import org.apache.kafka.common.errors.TimeoutException
import org.apache.kafka.common.errors.TopicAuthorizationException
import org.apache.kafka.common.internals.ClusterResourceListeners
import org.apache.kafka.common.message.ListOffsetsRequestData.ListOffsetsPartition
import org.apache.kafka.common.message.ListOffsetsRequestData.ListOffsetsTopic
import org.apache.kafka.common.message.ListOffsetsResponseData
import org.apache.kafka.common.message.ListOffsetsResponseData.ListOffsetsPartitionResponse
import org.apache.kafka.common.message.ListOffsetsResponseData.ListOffsetsTopicResponse
import org.apache.kafka.common.message.OffsetForLeaderEpochRequestData
import org.apache.kafka.common.message.OffsetForLeaderEpochRequestData.OffsetForLeaderPartition
import org.apache.kafka.common.message.OffsetForLeaderEpochResponseData
import org.apache.kafka.common.message.OffsetForLeaderEpochResponseData.OffsetForLeaderTopicResult
import org.apache.kafka.common.metrics.MetricConfig
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.AbstractRequest
import org.apache.kafka.common.requests.ListOffsetsRequest
import org.apache.kafka.common.requests.ListOffsetsResponse
import org.apache.kafka.common.requests.MetadataResponse
import org.apache.kafka.common.requests.OffsetsForLeaderEpochRequest
import org.apache.kafka.common.requests.OffsetsForLeaderEpochResponse
import org.apache.kafka.common.requests.RequestTestUtils.metadataUpdateWith
import org.apache.kafka.common.requests.RequestTestUtils.metadataUpdateWithIds
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.utils.LogContext
import org.apache.kafka.common.utils.MockTime
import org.apache.kafka.test.TestUtils.assertNullable
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertNotEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlin.test.fail

class OffsetFetcherTest {

    private val topicName = "test"

    private val topicId = Uuid.randomUuid()

    private val topicIds = mutableMapOf(topicName to topicId)

    private val tp0 = TopicPartition(topicName, 0)

    private val tp1 = TopicPartition(topicName, 1)

    private val tp2 = TopicPartition(topicName, 2)

    private val tp3 = TopicPartition(topicName, 3)

    private val validLeaderEpoch = 0

    private val initialUpdateResponse = metadataUpdateWithIds(
        numNodes = 1,
        topicPartitionCounts = mapOf(topicName to 4),
        topicIds = topicIds,
    )

    private val retryBackoffMs: Long = 100

    private var time = MockTime(1)

    private lateinit var subscriptions: SubscriptionState

    private lateinit var metadata: ConsumerMetadata

    private lateinit var client: MockClient

    private lateinit var metrics: Metrics

    private val apiVersions = ApiVersions()

    private lateinit var consumerClient: ConsumerNetworkClient

    private lateinit var offsetFetcher: OffsetFetcher

    private fun assignFromUser(partitions: Set<TopicPartition>) {
        subscriptions.assignFromUser(partitions)
        client.updateMetadata(initialUpdateResponse)

        // A dummy metadata update to ensure valid leader epoch.
        metadata.updateWithCurrentRequestVersion(
            response = metadataUpdateWith(
                clusterId = "dummy",
                numNodes = 1,
                topicErrors = emptyMap(),
                topicPartitionCounts = mapOf(topicName to 4),
                epochSupplier = { validLeaderEpoch },
                topicIds = topicIds,
            ),
            isPartialUpdate = false,
            nowMs = 0L,
        )
    }

    @BeforeEach
    fun setup() = Unit

    @AfterEach
    @Throws(Exception::class)
    fun teardown() {
        if (::metrics.isInitialized) metrics.close()
    }

    @Test
    fun testUpdateFetchPositionNoOpWithPositionSet() {
        buildFetcher()
        assignFromUser(setOf(tp0))
        subscriptions.seek(tp0, 5L)

        offsetFetcher.resetPositionsIfNeeded()
        assertFalse(client.hasInFlightRequests())
        assertTrue(subscriptions.isFetchable(tp0))
        assertEquals(5, subscriptions.position(tp0)!!.offset)
    }

    @Test
    fun testUpdateFetchPositionResetToDefaultOffset() {
        buildFetcher()
        assignFromUser(setOf(tp0))
        subscriptions.requestOffsetReset(tp0)

        client.prepareResponse(
            matcher = listOffsetRequestMatcher(
                ListOffsetsRequest.EARLIEST_TIMESTAMP,
                validLeaderEpoch
            ),
            response = listOffsetResponse(Errors.NONE, 1L, 5L),
        )
        offsetFetcher.resetPositionsIfNeeded()
        consumerClient.pollNoWakeup()
        assertFalse(subscriptions.isOffsetResetNeeded(tp0))
        assertTrue(subscriptions.isFetchable(tp0))
        assertEquals(5, subscriptions.position(tp0)!!.offset)
    }

    @Test
    fun testUpdateFetchPositionResetToLatestOffset() {
        buildFetcher()
        assignFromUser(setOf(tp0))
        subscriptions.requestOffsetReset(tp0, OffsetResetStrategy.LATEST)

        client.updateMetadata(initialUpdateResponse)

        client.prepareResponse(
            matcher = listOffsetRequestMatcher(ListOffsetsRequest.LATEST_TIMESTAMP),
            response = listOffsetResponse(Errors.NONE, 1L, 5L),
        )
        offsetFetcher.resetPositionsIfNeeded()
        consumerClient.pollNoWakeup()
        assertFalse(subscriptions.isOffsetResetNeeded(tp0))
        assertTrue(subscriptions.isFetchable(tp0))
        assertEquals(5, subscriptions.position(tp0)!!.offset)
    }

    /**
     * Make sure the client behaves appropriately when receiving an exception for unavailable offsets
     */
    @Test
    fun testFetchOffsetErrors() {
        buildFetcher()
        assignFromUser(setOf(tp0))
        subscriptions.requestOffsetReset(tp0, OffsetResetStrategy.LATEST)

        // Fail with OFFSET_NOT_AVAILABLE
        client.prepareResponse(
            matcher = listOffsetRequestMatcher(
                ListOffsetsRequest.LATEST_TIMESTAMP,
                validLeaderEpoch
            ),
            response = listOffsetResponse(Errors.OFFSET_NOT_AVAILABLE, 1L, 5L),
            disconnected = false,
        )
        offsetFetcher.resetPositionsIfNeeded()
        consumerClient.pollNoWakeup()
        assertFalse(subscriptions.hasValidPosition(tp0))
        assertTrue(subscriptions.isOffsetResetNeeded(tp0))
        assertFalse(subscriptions.isFetchable(tp0))

        // Fail with LEADER_NOT_AVAILABLE
        time.sleep(retryBackoffMs)
        client.prepareResponse(
            matcher = listOffsetRequestMatcher(
                ListOffsetsRequest.LATEST_TIMESTAMP,
                validLeaderEpoch
            ),
            response = listOffsetResponse(Errors.LEADER_NOT_AVAILABLE, 1L, 5L),
            disconnected = false,
        )
        offsetFetcher.resetPositionsIfNeeded()
        consumerClient.pollNoWakeup()
        assertFalse(subscriptions.hasValidPosition(tp0))
        assertTrue(subscriptions.isOffsetResetNeeded(tp0))
        assertFalse(subscriptions.isFetchable(tp0))

        // Back to normal
        time.sleep(retryBackoffMs)
        client.prepareResponse(
            matcher = listOffsetRequestMatcher(ListOffsetsRequest.LATEST_TIMESTAMP),
            response = listOffsetResponse(Errors.NONE, 1L, 5L),
            disconnected = false,
        )
        offsetFetcher.resetPositionsIfNeeded()
        consumerClient.pollNoWakeup()
        assertTrue(subscriptions.hasValidPosition(tp0))
        assertFalse(subscriptions.isOffsetResetNeeded(tp0))
        assertTrue(subscriptions.isFetchable(tp0))
        assertEquals(subscriptions.position(tp0)!!.offset, 5L)
    }

    @Test
    fun testListOffsetSendsReadUncommitted() {
        testListOffsetsSendsIsolationLevel(IsolationLevel.READ_UNCOMMITTED)
    }

    @Test
    fun testListOffsetSendsReadCommitted() {
        testListOffsetsSendsIsolationLevel(IsolationLevel.READ_COMMITTED)
    }

    private fun testListOffsetsSendsIsolationLevel(isolationLevel: IsolationLevel) {
        buildFetcher(isolationLevel = isolationLevel)
        assignFromUser(setOf(tp0))
        subscriptions.requestOffsetReset(tp0, OffsetResetStrategy.LATEST)
        client.prepareResponse(
            matcher = { body ->
                val request = body as ListOffsetsRequest
                request.isolationLevel == isolationLevel
            },
            response = listOffsetResponse(Errors.NONE, 1L, 5L),
        )
        offsetFetcher.resetPositionsIfNeeded()
        consumerClient.pollNoWakeup()
        assertFalse(subscriptions.isOffsetResetNeeded(tp0))
        assertTrue(subscriptions.isFetchable(tp0))
        assertEquals(5, subscriptions.position(tp0)!!.offset)
    }

    @Test
    fun testresetPositionsSkipsBlackedOutConnections() {
        buildFetcher()
        assignFromUser(setOf(tp0))
        subscriptions.requestOffsetReset(tp0, OffsetResetStrategy.EARLIEST)

        // Check that we skip sending the ListOffset request when the node is blacked out
        client.updateMetadata(initialUpdateResponse)
        val node = initialUpdateResponse.brokers().iterator().next()
        client.backoff(node, 500)
        offsetFetcher.resetPositionsIfNeeded()
        assertEquals(0, consumerClient.pendingRequestCount())
        consumerClient.pollNoWakeup()
        assertTrue(subscriptions.isOffsetResetNeeded(tp0))
        assertEquals(OffsetResetStrategy.EARLIEST, subscriptions.resetStrategy(tp0))

        time.sleep(500)
        client.prepareResponse(
            matcher = listOffsetRequestMatcher(ListOffsetsRequest.EARLIEST_TIMESTAMP),
            response = listOffsetResponse(Errors.NONE, 1L, 5L),
        )
        offsetFetcher.resetPositionsIfNeeded()
        consumerClient.pollNoWakeup()

        assertFalse(subscriptions.isOffsetResetNeeded(tp0))
        assertTrue(subscriptions.isFetchable(tp0))
        assertEquals(5, subscriptions.position(tp0)!!.offset)
    }

    @Test
    fun testUpdateFetchPositionResetToEarliestOffset() {
        buildFetcher()
        assignFromUser(setOf(tp0))
        subscriptions.requestOffsetReset(tp0, OffsetResetStrategy.EARLIEST)
        client.prepareResponse(
            matcher = listOffsetRequestMatcher(
                timestamp = ListOffsetsRequest.EARLIEST_TIMESTAMP,
                leaderEpoch = validLeaderEpoch,
            ),
            response = listOffsetResponse(Errors.NONE, 1L, 5L),
        )
        offsetFetcher.resetPositionsIfNeeded()
        consumerClient.pollNoWakeup()
        assertFalse(subscriptions.isOffsetResetNeeded(tp0))
        assertTrue(subscriptions.isFetchable(tp0))
        assertEquals(5, subscriptions.position(tp0)!!.offset)
    }

    @Test
    fun testresetPositionsMetadataRefresh() {
        buildFetcher()
        assignFromUser(setOf(tp0))
        subscriptions.requestOffsetReset(tp0, OffsetResetStrategy.LATEST)

        // First fetch fails with stale metadata
        client.prepareResponse(
            matcher = listOffsetRequestMatcher(
                timestamp = ListOffsetsRequest.LATEST_TIMESTAMP,
                leaderEpoch = validLeaderEpoch,
            ),
            response = listOffsetResponse(
                error = Errors.NOT_LEADER_OR_FOLLOWER,
                timestamp = 1L,
                offset = 5L,
            ),
            disconnected = false,
        )
        offsetFetcher.resetPositionsIfNeeded()
        consumerClient.pollNoWakeup()
        assertFalse(subscriptions.hasValidPosition(tp0))

        // Expect a metadata refresh
        client.prepareMetadataUpdate(initialUpdateResponse)
        consumerClient.pollNoWakeup()
        assertFalse(client.hasPendingMetadataUpdates())

        // Next fetch succeeds
        time.sleep(retryBackoffMs)
        client.prepareResponse(
            matcher = listOffsetRequestMatcher(ListOffsetsRequest.LATEST_TIMESTAMP),
            response = listOffsetResponse(Errors.NONE, 1L, 5L),
        )
        offsetFetcher.resetPositionsIfNeeded()
        consumerClient.pollNoWakeup()
        assertFalse(subscriptions.isOffsetResetNeeded(tp0))
        assertTrue(subscriptions.isFetchable(tp0))
        assertEquals(5, subscriptions.position(tp0)!!.offset)
    }

    @Test
    fun testListOffsetNoUpdateMissingEpoch() {
        buildFetcher()

        // Set up metadata with no leader epoch
        subscriptions.assignFromUser(setOf(tp0))
        val metadataWithNoLeaderEpochs: MetadataResponse = metadataUpdateWith(
            clusterId = "kafka-cluster",
            numNodes = 1,
            topicErrors = emptyMap(),
            topicPartitionCounts = mapOf(topicName to 4),
            epochSupplier = { null },
            topicIds = topicIds,
        )
        client.updateMetadata(metadataWithNoLeaderEpochs)

        // Return a ListOffsets response with leaderEpoch=1, we should ignore it
        subscriptions.requestOffsetReset(tp0, OffsetResetStrategy.LATEST)
        client.prepareResponse(
            matcher = listOffsetRequestMatcher(ListOffsetsRequest.LATEST_TIMESTAMP),
            response = listOffsetResponse(tp0, Errors.NONE, 1L, 5L, 1)
        )
        offsetFetcher.resetPositionsIfNeeded()
        consumerClient.pollNoWakeup()

        // Reset should be satisfied and no metadata update requested
        assertFalse(subscriptions.isOffsetResetNeeded(tp0))
        assertFalse(metadata.updateRequested())
        assertNull(metadata.lastSeenLeaderEpochs[tp0])
    }

    @Test
    fun testListOffsetUpdateEpoch() {
        buildFetcher()

        // Set up metadata with leaderEpoch=1
        subscriptions.assignFromUser(setOf(tp0))
        val metadataWithLeaderEpochs: MetadataResponse = metadataUpdateWith(
            clusterId = "kafka-cluster",
            numNodes = 1,
            topicErrors = emptyMap(),
            topicPartitionCounts = mapOf(topicName to 4),
            epochSupplier = { 1 },
            topicIds = topicIds,
        )
        client.updateMetadata(metadataWithLeaderEpochs)

        // Reset offsets to trigger ListOffsets call
        subscriptions.requestOffsetReset(tp0, OffsetResetStrategy.LATEST)

        // Now we see a ListOffsets with leaderEpoch=2 epoch, we trigger a metadata update
        client.prepareResponse(
            matcher = listOffsetRequestMatcher(ListOffsetsRequest.LATEST_TIMESTAMP, 1),
            response = listOffsetResponse(tp0, Errors.NONE, 1L, 5L, 2),
        )
        offsetFetcher.resetPositionsIfNeeded()
        consumerClient.pollNoWakeup()
        assertFalse(subscriptions.isOffsetResetNeeded(tp0))
        assertTrue(metadata.updateRequested())
        assertNullable(metadata.lastSeenLeaderEpochs[tp0]) { epoch -> assertEquals(2, epoch) }
    }

    @Test
    fun testUpdateFetchPositionDisconnect() {
        buildFetcher()
        assignFromUser(setOf(tp0))
        subscriptions.requestOffsetReset(tp0, OffsetResetStrategy.LATEST)

        // First request gets a disconnect
        client.prepareResponse(
            matcher = listOffsetRequestMatcher(ListOffsetsRequest.LATEST_TIMESTAMP, validLeaderEpoch),
            response = listOffsetResponse(Errors.NONE, 1L, 5L),
            disconnected = true,
        )
        offsetFetcher.resetPositionsIfNeeded()
        consumerClient.pollNoWakeup()
        assertFalse(subscriptions.hasValidPosition(tp0))

        // Expect a metadata refresh
        client.prepareMetadataUpdate(initialUpdateResponse)
        consumerClient.pollNoWakeup()
        assertFalse(client.hasPendingMetadataUpdates())

        // No retry until the backoff passes
        offsetFetcher.resetPositionsIfNeeded()
        consumerClient.pollNoWakeup()
        assertFalse(client.hasInFlightRequests())
        assertFalse(subscriptions.hasValidPosition(tp0))

        // Next one succeeds
        time.sleep(retryBackoffMs)
        client.prepareResponse(
            matcher = listOffsetRequestMatcher(ListOffsetsRequest.LATEST_TIMESTAMP),
            response = listOffsetResponse(Errors.NONE, 1L, 5L),
        )
        offsetFetcher.resetPositionsIfNeeded()
        consumerClient.pollNoWakeup()
        assertFalse(subscriptions.isOffsetResetNeeded(tp0))
        assertTrue(subscriptions.isFetchable(tp0))
        assertEquals(5, subscriptions.position(tp0)!!.offset)
    }

    @Test
    fun testAssignmentChangeWithInFlightReset() {
        buildFetcher()
        assignFromUser(setOf(tp0))
        subscriptions.requestOffsetReset(tp0, OffsetResetStrategy.LATEST)

        // Send the ListOffsets request to reset the position
        offsetFetcher.resetPositionsIfNeeded()
        consumerClient.pollNoWakeup()
        assertFalse(subscriptions.hasValidPosition(tp0))
        assertTrue(client.hasInFlightRequests())

        // Now we have an assignment change
        assignFromUser(setOf(tp1))

        // The response returns and is discarded
        client.respond(listOffsetResponse(Errors.NONE, 1L, 5L))
        consumerClient.pollNoWakeup()
        assertFalse(client.hasPendingResponses())
        assertFalse(client.hasInFlightRequests())
        assertFalse(subscriptions.isAssigned(tp0))
    }

    @Test
    fun testSeekWithInFlightReset() {
        buildFetcher()
        assignFromUser(setOf(tp0))
        subscriptions.requestOffsetReset(tp0, OffsetResetStrategy.LATEST)

        // Send the ListOffsets request to reset the position
        offsetFetcher.resetPositionsIfNeeded()
        consumerClient.pollNoWakeup()
        assertFalse(subscriptions.hasValidPosition(tp0))
        assertTrue(client.hasInFlightRequests())

        // Now we get a seek from the user
        subscriptions.seek(tp0, 237)

        // The response returns and is discarded
        client.respond(listOffsetResponse(Errors.NONE, 1L, 5L))
        consumerClient.pollNoWakeup()
        assertFalse(client.hasPendingResponses())
        assertFalse(client.hasInFlightRequests())
        assertEquals(237L, subscriptions.position(tp0)!!.offset)
    }

    private fun listOffsetMatchesExpectedReset(
        tp: TopicPartition,
        strategy: OffsetResetStrategy,
        request: AbstractRequest,
    ): Boolean {
        val req = assertIs<ListOffsetsRequest>(request)
        assertEquals(
            expected = setOf(tp.topic),
            actual = req.data().topics.map(ListOffsetsTopic::name).toSet(),
        )
        val listTopic = req.data().topics[0]
        assertEquals(
            expected = setOf(tp.partition),
            actual = listTopic.partitions.map(ListOffsetsPartition::partitionIndex).toSet()
        )
        val listPartition = listTopic.partitions[0]
        if (strategy == OffsetResetStrategy.EARLIEST)
            assertEquals(ListOffsetsRequest.EARLIEST_TIMESTAMP, listPartition.timestamp)
        else if (strategy == OffsetResetStrategy.LATEST)
            assertEquals(ListOffsetsRequest.LATEST_TIMESTAMP, listPartition.timestamp)
        return true
    }

    @Test
    fun testEarlierOffsetResetArrivesLate() {
        buildFetcher()
        assignFromUser(setOf(tp0))
        subscriptions.requestOffsetReset(tp0, OffsetResetStrategy.EARLIEST)
        offsetFetcher.resetPositionsIfNeeded()
        client.prepareResponse(
            matcher = { req ->
                if (listOffsetMatchesExpectedReset(tp0, OffsetResetStrategy.EARLIEST, req!!)) {
                    // Before the response is handled, we get a request to reset to the latest offset
                    subscriptions.requestOffsetReset(tp0, OffsetResetStrategy.LATEST)
                    return@prepareResponse true
                } else {
                    return@prepareResponse false
                }
            },
            response = listOffsetResponse(Errors.NONE, 1L, 0L),
        )
        consumerClient.pollNoWakeup()

        // The list offset result should be ignored
        assertTrue(subscriptions.isOffsetResetNeeded(tp0))
        assertEquals(OffsetResetStrategy.LATEST, subscriptions.resetStrategy(tp0))
        offsetFetcher.resetPositionsIfNeeded()
        client.prepareResponse(
            matcher = { req -> listOffsetMatchesExpectedReset(tp0, OffsetResetStrategy.LATEST, req!!) },
            response = listOffsetResponse(Errors.NONE, 1L, 10L),
        )
        consumerClient.pollNoWakeup()
        assertFalse(subscriptions.isOffsetResetNeeded(tp0))
        assertEquals(10, subscriptions.position(tp0)!!.offset)
    }

    @Test
    fun testChangeResetWithInFlightReset() {
        buildFetcher()
        assignFromUser(setOf(tp0))
        subscriptions.requestOffsetReset(tp0, OffsetResetStrategy.LATEST)

        // Send the ListOffsets request to reset the position
        offsetFetcher.resetPositionsIfNeeded()
        consumerClient.pollNoWakeup()
        assertFalse(subscriptions.hasValidPosition(tp0))
        assertTrue(client.hasInFlightRequests())

        // Now we get a seek from the user
        subscriptions.requestOffsetReset(tp0, OffsetResetStrategy.EARLIEST)

        // The response returns and is discarded
        client.respond(listOffsetResponse(Errors.NONE, 1L, 5L))
        consumerClient.pollNoWakeup()
        assertFalse(client.hasPendingResponses())
        assertFalse(client.hasInFlightRequests())
        assertTrue(subscriptions.isOffsetResetNeeded(tp0))
        assertEquals(OffsetResetStrategy.EARLIEST, subscriptions.resetStrategy(tp0))
    }

    @Test
    fun testIdempotentResetWithInFlightReset() {
        buildFetcher()
        assignFromUser(setOf(tp0))
        subscriptions.requestOffsetReset(tp0, OffsetResetStrategy.LATEST)

        // Send the ListOffsets request to reset the position
        offsetFetcher.resetPositionsIfNeeded()
        consumerClient.pollNoWakeup()
        assertFalse(subscriptions.hasValidPosition(tp0))
        assertTrue(client.hasInFlightRequests())

        // Now we get a seek from the user
        subscriptions.requestOffsetReset(tp0, OffsetResetStrategy.LATEST)
        client.respond(listOffsetResponse(Errors.NONE, 1L, 5L))
        consumerClient.pollNoWakeup()
        assertFalse(client.hasInFlightRequests())
        assertFalse(subscriptions.isOffsetResetNeeded(tp0))
        assertEquals(5L, subscriptions.position(tp0)!!.offset)
    }

    @Test
    fun testRestOffsetsAuthorizationFailure() {
        buildFetcher()
        assignFromUser(setOf(tp0))
        subscriptions.requestOffsetReset(tp0, OffsetResetStrategy.LATEST)

        // First request gets a disconnect
        client.prepareResponse(
            matcher = listOffsetRequestMatcher(
                ListOffsetsRequest.LATEST_TIMESTAMP,
                validLeaderEpoch
            ),
            response = listOffsetResponse(Errors.TOPIC_AUTHORIZATION_FAILED, -1, -1),
            disconnected = false,
        )
        offsetFetcher.resetPositionsIfNeeded()
        consumerClient.pollNoWakeup()
        assertFalse(subscriptions.hasValidPosition(tp0))
        val error = assertFailsWith<TopicAuthorizationException>(
            message = "Expected authorization error to be raised",
        ){ offsetFetcher.resetPositionsIfNeeded() }
        assertEquals(setOf(tp0.topic), error.unauthorizedTopics)

        // The exception should clear after being raised, but no retry until the backoff
        offsetFetcher.resetPositionsIfNeeded()
        consumerClient.pollNoWakeup()
        assertFalse(client.hasInFlightRequests())
        assertFalse(subscriptions.hasValidPosition(tp0))

        // Next one succeeds
        time.sleep(retryBackoffMs)
        client.prepareResponse(
            matcher = listOffsetRequestMatcher(ListOffsetsRequest.LATEST_TIMESTAMP),
            response = listOffsetResponse(Errors.NONE, 1L, 5L),
        )
        offsetFetcher.resetPositionsIfNeeded()
        consumerClient.pollNoWakeup()
        assertFalse(subscriptions.isOffsetResetNeeded(tp0))
        assertTrue(subscriptions.isFetchable(tp0))
        assertEquals(5, subscriptions.position(tp0)!!.offset)
    }

    @Test
    fun testFetchingPendingPartitionsBeforeAndAfterSubscriptionReset() {
        buildFetcher()
        assignFromUser(setOf(tp0))
        subscriptions.seek(tp0, 100)
        assertEquals(100, subscriptions.position(tp0)!!.offset)
        assertTrue(subscriptions.isFetchable(tp0))
        subscriptions.markPendingRevocation(setOf(tp0))
        offsetFetcher.resetPositionsIfNeeded()

        // once a partition is marked pending, it should not be fetchable
        assertFalse(subscriptions.isOffsetResetNeeded(tp0))
        assertFalse(subscriptions.isFetchable(tp0))
        assertTrue(subscriptions.hasValidPosition(tp0))
        assertEquals(100, subscriptions.position(tp0)!!.offset)
        subscriptions.seek(tp0, 100)
        assertEquals(100, subscriptions.position(tp0)!!.offset)

        // reassignment should enable fetching of the same partition
        subscriptions.unsubscribe()
        assignFromUser(setOf(tp0))
        subscriptions.seek(tp0, 100)
        assertEquals(100, subscriptions.position(tp0)!!.offset)
        assertTrue(subscriptions.isFetchable(tp0))
    }

    @Test
    fun testUpdateFetchPositionOfPausedPartitionsRequiringOffsetReset() {
        buildFetcher()
        assignFromUser(setOf(tp0))
        subscriptions.pause(tp0) // paused partition does not have a valid position
        subscriptions.requestOffsetReset(tp0, OffsetResetStrategy.LATEST)
        client.prepareResponse(
            matcher = listOffsetRequestMatcher(
                ListOffsetsRequest.LATEST_TIMESTAMP,
                validLeaderEpoch
            ),
            response = listOffsetResponse(Errors.NONE, 1L, 10L),
        )
        offsetFetcher.resetPositionsIfNeeded()
        consumerClient.pollNoWakeup()
        assertFalse(subscriptions.isOffsetResetNeeded(tp0))
        assertFalse(subscriptions.isFetchable(tp0)) // because tp is paused
        assertTrue(subscriptions.hasValidPosition(tp0))
        assertEquals(10, subscriptions.position(tp0)!!.offset)
    }

    @Test
    fun testUpdateFetchPositionOfPausedPartitionsWithoutAValidPosition() {
        buildFetcher()
        assignFromUser(setOf(tp0))
        subscriptions.requestOffsetReset(tp0)
        subscriptions.pause(tp0) // paused partition does not have a valid position
        offsetFetcher.resetPositionsIfNeeded()
        consumerClient.pollNoWakeup()
        assertTrue(subscriptions.isOffsetResetNeeded(tp0))
        assertFalse(subscriptions.isFetchable(tp0)) // because tp is paused
        assertFalse(subscriptions.hasValidPosition(tp0))
    }

    @Test
    fun testUpdateFetchPositionOfPausedPartitionsWithAValidPosition() {
        buildFetcher()
        assignFromUser(setOf(tp0))
        subscriptions.seek(tp0, 10)
        subscriptions.pause(tp0) // paused partition already has a valid position
        offsetFetcher.resetPositionsIfNeeded()
        assertFalse(subscriptions.isOffsetResetNeeded(tp0))
        assertFalse(subscriptions.isFetchable(tp0)) // because tp is paused
        assertTrue(subscriptions.hasValidPosition(tp0))
        assertEquals(10, subscriptions.position(tp0)!!.offset)
    }

    @Test
    fun testGetOffsetsForTimesTimeout() {
        buildFetcher()
        assertFailsWith<TimeoutException> {
            offsetFetcher.offsetsForTimes(
                timestampsToSearch = mapOf(TopicPartition(topicName, 2) to 1000L),
                timer = time.timer(100L),
            )
        }
    }

    @Test
    fun testGetOffsetsForTimes() {
        buildFetcher()

        // Empty map
        assertTrue(offsetFetcher.offsetsForTimes(HashMap(), time.timer(100L)).isEmpty())
        // Unknown Offset
        testGetOffsetsForTimesWithUnknownOffset()
        // Error code none with unknown offset
        testGetOffsetsForTimesWithError(Errors.NONE, Errors.NONE, -1L, null)
        // Error code none with known offset
        testGetOffsetsForTimesWithError(Errors.NONE, Errors.NONE, 10L, 10L)
        // Test both of partition has error.
        testGetOffsetsForTimesWithError(Errors.NOT_LEADER_OR_FOLLOWER, Errors.INVALID_REQUEST, 10L, 10L)
        // Test the second partition has error.
        testGetOffsetsForTimesWithError(Errors.NONE, Errors.NOT_LEADER_OR_FOLLOWER, 10L, 10L)
        // Test different errors.
        testGetOffsetsForTimesWithError(Errors.NOT_LEADER_OR_FOLLOWER, Errors.NONE, 10L, 10L)
        testGetOffsetsForTimesWithError(Errors.UNKNOWN_TOPIC_OR_PARTITION, Errors.NONE, 10L, 10L)
        testGetOffsetsForTimesWithError(Errors.UNSUPPORTED_FOR_MESSAGE_FORMAT, Errors.NONE, 10L, null)
        testGetOffsetsForTimesWithError(Errors.BROKER_NOT_AVAILABLE, Errors.NONE, 10L, 10L)
    }

    @Test
    fun testGetOffsetsFencedLeaderEpoch() {
        buildFetcher()
        subscriptions.assignFromUser(setOf(tp0))
        client.updateMetadata(initialUpdateResponse)
        subscriptions.requestOffsetReset(tp0, OffsetResetStrategy.LATEST)
        client.prepareResponse(listOffsetResponse(Errors.FENCED_LEADER_EPOCH, 1L, 5L))
        offsetFetcher.resetPositionsIfNeeded()
        consumerClient.pollNoWakeup()
        assertTrue(subscriptions.isOffsetResetNeeded(tp0))
        assertFalse(subscriptions.isFetchable(tp0))
        assertFalse(subscriptions.hasValidPosition(tp0))
        assertEquals(0L, metadata.timeToNextUpdate(time.milliseconds()))
    }

    @Test
    fun testGetOffsetByTimeWithPartitionsRetryCouldTriggerMetadataUpdate() {
        val retriableErrors = listOf(
            Errors.NOT_LEADER_OR_FOLLOWER,
            Errors.REPLICA_NOT_AVAILABLE, Errors.KAFKA_STORAGE_ERROR, Errors.OFFSET_NOT_AVAILABLE,
            Errors.LEADER_NOT_AVAILABLE, Errors.FENCED_LEADER_EPOCH, Errors.UNKNOWN_LEADER_EPOCH
        )
        val newLeaderEpoch = 3
        val updatedMetadata = metadataUpdateWith(
            clusterId = "dummy",
            numNodes = 3,
            topicErrors = mapOf(topicName to Errors.NONE),
            topicPartitionCounts = mapOf(topicName to 4),
            epochSupplier = { newLeaderEpoch },
            topicIds = topicIds,
        )
        val originalLeader = initialUpdateResponse.buildCluster().leaderFor(tp1)
        val newLeader = updatedMetadata.buildCluster().leaderFor(tp1)
        assertNotEquals(originalLeader, newLeader)
        for (retriableError in retriableErrors) {
            buildFetcher()
            subscriptions.assignFromUser(setOf(tp0, tp1))
            client.updateMetadata(initialUpdateResponse)
            val fetchTimestamp = 10L
            val tp0NoError = ListOffsetsPartitionResponse()
                .setPartitionIndex(tp0.partition)
                .setErrorCode(Errors.NONE.code)
                .setTimestamp(fetchTimestamp)
                .setOffset(4L)
            val topics = listOf(
                ListOffsetsTopicResponse()
                    .setName(tp0.topic)
                    .setPartitions(
                        listOf(
                            tp0NoError,
                            ListOffsetsPartitionResponse()
                                .setPartitionIndex(tp1.partition)
                                .setErrorCode(retriableError.code)
                                .setTimestamp(ListOffsetsRequest.LATEST_TIMESTAMP)
                                .setOffset(-1L)
                        )
                    )
            )
            val data = ListOffsetsResponseData()
                .setThrottleTimeMs(0)
                .setTopics(topics)
            client.prepareResponseFrom(
                matcher = { request ->
                    if (request is ListOffsetsRequest) {
                        val expectedTopics = listOf(
                            ListOffsetsTopic()
                                .setName(tp0.topic)
                                .setPartitions(
                                    listOf(
                                        ListOffsetsPartition()
                                            .setPartitionIndex(tp1.partition)
                                            .setTimestamp(fetchTimestamp)
                                            .setCurrentLeaderEpoch(ListOffsetsResponse.UNKNOWN_EPOCH),
                                        ListOffsetsPartition()
                                            .setPartitionIndex(tp0.partition)
                                            .setTimestamp(fetchTimestamp)
                                            .setCurrentLeaderEpoch(ListOffsetsResponse.UNKNOWN_EPOCH)
                                    )
                                )
                        )
                        return@prepareResponseFrom request.topics == expectedTopics
                    } else return@prepareResponseFrom false
                },
                response = ListOffsetsResponse(data),
                node = originalLeader,
            )
            client.prepareMetadataUpdate(updatedMetadata)

            // If the metadata wasn't updated before retrying, the fetcher would consult the original leader
            // and hit a NOT_LEADER exception.
            // We will count the answered future response in the end to verify if this is the case.
            val topicsWithFatalError = listOf(
                ListOffsetsTopicResponse()
                    .setName(tp0.topic)
                    .setPartitions(
                        listOf(
                            tp0NoError,
                            ListOffsetsPartitionResponse()
                                .setPartitionIndex(tp1.partition)
                                .setErrorCode(Errors.NOT_LEADER_OR_FOLLOWER.code)
                                .setTimestamp(ListOffsetsRequest.LATEST_TIMESTAMP)
                                .setOffset(-1L)
                        )
                    )
            )
            val dataWithFatalError = ListOffsetsResponseData()
                .setThrottleTimeMs(0)
                .setTopics(topicsWithFatalError)
            client.prepareResponseFrom(
                response = ListOffsetsResponse(dataWithFatalError),
                node = originalLeader,
            )

            // The request to new leader must only contain one partition tp1 with error.
            client.prepareResponseFrom({ body: AbstractRequest? ->
                val isListOffsetRequest = body is ListOffsetsRequest
                if (isListOffsetRequest) {
                    val request = body as ListOffsetsRequest?
                    val requestTopic = request!!.topics[0]
                    val expectedPartition = ListOffsetsPartition()
                        .setPartitionIndex(tp1.partition)
                        .setTimestamp(fetchTimestamp)
                        .setCurrentLeaderEpoch(newLeaderEpoch)
                    return@prepareResponseFrom expectedPartition == requestTopic.partitions[0]
                } else {
                    return@prepareResponseFrom false
                }
            }, listOffsetResponse(tp1, Errors.NONE, fetchTimestamp, 5L), newLeader)
            val offsetAndTimestampMap = offsetFetcher.offsetsForTimes(
                timestampsToSearch = mapOf(
                    tp0 to fetchTimestamp,
                    tp1 to fetchTimestamp,
                ),
                timer = time.timer(Int.MAX_VALUE.toLong()),
            )
            assertEquals(
                expected = mapOf(
                    tp0 to OffsetAndTimestamp(4L, fetchTimestamp),
                    tp1 to OffsetAndTimestamp(5L, fetchTimestamp),
                ),
                actual = offsetAndTimestampMap,
            )

            // The NOT_LEADER exception future should not be cleared as we already refreshed the metadata before
            // first retry, thus never hitting.
            assertEquals(1, client.numAwaitingResponses())
        }
    }

    @Test
    fun testGetOffsetsUnknownLeaderEpoch() {
        buildFetcher()
        subscriptions.assignFromUser(setOf(tp0))
        subscriptions.requestOffsetReset(tp0, OffsetResetStrategy.LATEST)
        client.prepareResponse(listOffsetResponse(Errors.UNKNOWN_LEADER_EPOCH, 1L, 5L))
        offsetFetcher.resetPositionsIfNeeded()
        consumerClient.pollNoWakeup()
        assertTrue(subscriptions.isOffsetResetNeeded(tp0))
        assertFalse(subscriptions.isFetchable(tp0))
        assertFalse(subscriptions.hasValidPosition(tp0))
        assertEquals(0L, metadata.timeToNextUpdate(time.milliseconds()))
    }

    @Test
    fun testGetOffsetsIncludesLeaderEpoch() {
        buildFetcher()
        subscriptions.assignFromUser(setOf(tp0))
        client.updateMetadata(initialUpdateResponse)

        // Metadata update with leader epochs
        val metadataResponse = metadataUpdateWith(
            clusterId = "dummy",
            numNodes = 1,
            topicErrors = emptyMap(),
            topicPartitionCounts = mapOf(topicName to 4),
            epochSupplier = { 99 },
            topicIds = topicIds,
        )
        client.updateMetadata(metadataResponse)

        // Request latest offset
        subscriptions.requestOffsetReset(tp0)
        offsetFetcher.resetPositionsIfNeeded()

        // Check for epoch in outgoing request
        val matcher = RequestMatcher { request ->
            if (request is ListOffsetsRequest) {
                val epoch = request.topics[0].partitions[0].currentLeaderEpoch
                assertNotEquals(
                    illegal = ListOffsetsResponse.UNKNOWN_EPOCH,
                    actual = epoch,
                    message = "Expected Fetcher to set leader epoch in request",
                )
                assertEquals(
                    expected = 99,
                    actual = epoch,
                    message = "Expected leader epoch to match epoch from metadata update",
                )
                return@RequestMatcher true
            } else fail("Should have seen ListOffsetRequest")
        }
        client.prepareResponse(
            matcher = matcher,
            response = listOffsetResponse(Errors.NONE, 1L, 5L),
        )
        consumerClient.pollNoWakeup()
    }

    @Test
    fun testGetOffsetsForTimesWhenSomeTopicPartitionLeadersNotKnownInitially() {
        buildFetcher()
        subscriptions.assignFromUser(setOf(tp0, tp1))
        val anotherTopic = "another-topic"
        val t2p0 = TopicPartition(anotherTopic, 0)
        client.reset()

        // Metadata initially has one topic
        val initialMetadata = metadataUpdateWith(
            numNodes = 3,
            topicPartitionCounts = mapOf(topicName to 2),
            topicIds = topicIds,
        )
        client.updateMetadata(initialMetadata)

        // The first metadata refresh should contain one topic
        client.prepareMetadataUpdate(initialMetadata)
        client.prepareResponseFrom(
            response = listOffsetResponse(tp0, Errors.NONE, 1000L, 11L),
            node = metadata.fetch().leaderFor(tp0),
        )
        client.prepareResponseFrom(
            response = listOffsetResponse(tp1, Errors.NONE, 1000L, 32L),
            node = metadata.fetch().leaderFor(tp1),
        )

        // Second metadata refresh should contain two topics
        val partitionNumByTopic: MutableMap<String, Int> = HashMap()
        partitionNumByTopic[topicName] = 2
        partitionNumByTopic[anotherTopic] = 1
        topicIds["another-topic"] = Uuid.randomUuid()
        val updatedMetadata = metadataUpdateWith(
            numNodes = 3,
            topicPartitionCounts = partitionNumByTopic,
            topicIds = topicIds,
        )
        client.prepareMetadataUpdate(updatedMetadata)
        client.prepareResponseFrom(
            response = listOffsetResponse(t2p0, Errors.NONE, 1000L, 54L),
            node = metadata.fetch().leaderFor(t2p0),
        )
        val timestampToSearch: MutableMap<TopicPartition, Long> = HashMap()
        timestampToSearch[tp0] = ListOffsetsRequest.LATEST_TIMESTAMP
        timestampToSearch[tp1] = ListOffsetsRequest.LATEST_TIMESTAMP
        timestampToSearch[t2p0] = ListOffsetsRequest.LATEST_TIMESTAMP
        val offsetAndTimestampMap = offsetFetcher.offsetsForTimes(
            timestampsToSearch = timestampToSearch,
            timer = time.timer(Long.MAX_VALUE),
        )
        assertNotNull(
            actual = offsetAndTimestampMap[tp0],
            message = "Expect MetadataFetcher.offsetsForTimes() to return non-null result for $tp0",
        )
        assertNotNull(
            actual = offsetAndTimestampMap[tp1],
            message = "Expect MetadataFetcher.offsetsForTimes() to return non-null result for $tp1",
        )
        assertNotNull(
            actual = offsetAndTimestampMap[t2p0],
            message = "Expect MetadataFetcher.offsetsForTimes() to return non-null result for $t2p0",
        )
        assertEquals(11L, offsetAndTimestampMap[tp0]!!.offset)
        assertEquals(32L, offsetAndTimestampMap[tp1]!!.offset)
        assertEquals(54L, offsetAndTimestampMap[t2p0]!!.offset)
    }

    @Test
    fun testGetOffsetsForTimesWhenSomeTopicPartitionLeadersDisconnectException() {
        buildFetcher()
        val anotherTopic = "another-topic"
        val t2p0 = TopicPartition(anotherTopic, 0)
        subscriptions.assignFromUser(setOf(tp0, t2p0))
        client.reset()
        val initialMetadata = metadataUpdateWith(
            numNodes = 1,
            topicPartitionCounts = mapOf(topicName to 1),
            topicIds = topicIds,
        )
        client.updateMetadata(initialMetadata)
        val partitionNumByTopic = mapOf(
            topicName to 1,
            anotherTopic to 1,
        )
        topicIds["another-topic"] = Uuid.randomUuid()
        val updatedMetadata = metadataUpdateWith(
            numNodes = 1,
            topicPartitionCounts = partitionNumByTopic,
            topicIds = topicIds,
        )
        client.prepareMetadataUpdate(updatedMetadata)
        client.prepareResponse(
            matcher = listOffsetRequestMatcher(ListOffsetsRequest.LATEST_TIMESTAMP),
            response = listOffsetResponse(tp0, Errors.NONE, 1000L, 11L),
            disconnected = true,
        )
        client.prepareResponseFrom(
            response = listOffsetResponse(tp0, Errors.NONE, 1000L, 11L),
            node = metadata.fetch().leaderFor(tp0),
        )
        val timestampToSearch: MutableMap<TopicPartition, Long> = HashMap()
        timestampToSearch[tp0] = ListOffsetsRequest.LATEST_TIMESTAMP
        val offsetAndTimestampMap = offsetFetcher.offsetsForTimes(
            timestampsToSearch = timestampToSearch,
            timer = time.timer(Long.MAX_VALUE),
        )
        assertNotNull(
            actual = offsetAndTimestampMap[tp0],
            message = "Expect MetadataFetcher.offsetsForTimes() to return non-null result for $tp0",
        )
        assertEquals(11L, offsetAndTimestampMap[tp0]!!.offset)
        assertNotNull(metadata.fetch().partitionCountForTopic(anotherTopic))
    }

    @Test
    fun testListOffsetsWithZeroTimeout() {
        buildFetcher()
        val offsetsToSearch = mapOf(
            tp0 to ListOffsetsRequest.EARLIEST_TIMESTAMP,
            tp1 to ListOffsetsRequest.EARLIEST_TIMESTAMP,
        )
        val offsetsToExpect = mapOf<TopicPartition, OffsetAndTimestamp?>(
            tp0 to null,
            tp1 to null,
        )
        assertEquals(offsetsToExpect, offsetFetcher.offsetsForTimes(offsetsToSearch, time.timer(0)))
    }

    @Test
    fun testBatchedListOffsetsMetadataErrors() {
        buildFetcher()
        val data = ListOffsetsResponseData()
            .setThrottleTimeMs(0)
            .setTopics(
                listOf(
                    ListOffsetsTopicResponse()
                        .setName(tp0.topic)
                        .setPartitions(
                            listOf(
                                ListOffsetsPartitionResponse()
                                    .setPartitionIndex(tp0.partition)
                                    .setErrorCode(Errors.NOT_LEADER_OR_FOLLOWER.code)
                                    .setTimestamp(ListOffsetsResponse.UNKNOWN_TIMESTAMP)
                                    .setOffset(ListOffsetsResponse.UNKNOWN_OFFSET),
                                ListOffsetsPartitionResponse()
                                    .setPartitionIndex(tp1.partition)
                                    .setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code)
                                    .setTimestamp(ListOffsetsResponse.UNKNOWN_TIMESTAMP)
                                    .setOffset(ListOffsetsResponse.UNKNOWN_OFFSET)
                            )
                        )
                )
            )
        client.prepareResponse(ListOffsetsResponse(data))
        val offsetsToSearch: MutableMap<TopicPartition, Long> = HashMap()
        offsetsToSearch[tp0] = ListOffsetsRequest.EARLIEST_TIMESTAMP
        offsetsToSearch[tp1] = ListOffsetsRequest.EARLIEST_TIMESTAMP
        assertFailsWith<TimeoutException> {
            offsetFetcher.offsetsForTimes(
                timestampsToSearch = offsetsToSearch,
                timer = time.timer(1),
            )
        }
    }

    private fun testGetOffsetsForTimesWithError(
        errorForP0: Errors,
        errorForP1: Errors,
        offsetForP0: Long,
        expectedOffsetForP0: Long?,
    ) {
        val offsetForP1 = 100L
        val expectedOffsetForP1 = 100L
        client.reset()
        val topicName2 = "topic2"
        val t2p0 = TopicPartition(topicName2, 0)
        // Expect a metadata refresh.
        metadata.bootstrap(
            parseAndValidateAddresses(
                urls = listOf("1.1.1.1:1111"),
                clientDnsLookup = ClientDnsLookup.USE_ALL_DNS_IPS,
            )
        )
        val partitionNumByTopic = mapOf(
            topicName to 2,
            topicName2 to 1,
        )
        val updateMetadataResponse = metadataUpdateWith(
            numNodes = 2,
            topicPartitionCounts = partitionNumByTopic,
            topicIds = topicIds,
        )
        val updatedCluster = updateMetadataResponse.buildCluster()

        // The metadata refresh should contain all the topics.
        client.prepareMetadataUpdate(updateMetadataResponse, true)

        // First try should fail due to metadata error.
        client.prepareResponseFrom(
            response = listOffsetResponse(t2p0, errorForP0, offsetForP0, offsetForP0),
            node = updatedCluster.leaderFor(t2p0)
        )
        client.prepareResponseFrom(
            response = listOffsetResponse(tp1, errorForP1, offsetForP1, offsetForP1),
            node = updatedCluster.leaderFor(tp1),
        )
        // Second try should succeed.
        client.prepareResponseFrom(
            response = listOffsetResponse(t2p0, Errors.NONE, offsetForP0, offsetForP0),
            node = updatedCluster.leaderFor(t2p0),
        )
        client.prepareResponseFrom(
            response = listOffsetResponse(tp1, Errors.NONE, offsetForP1, offsetForP1),
            node = updatedCluster.leaderFor(tp1),
        )
        val timestampToSearch: MutableMap<TopicPartition, Long> = HashMap()
        timestampToSearch[t2p0] = 0L
        timestampToSearch[tp1] = 0L
        val offsetAndTimestampMap = offsetFetcher.offsetsForTimes(
            timestampsToSearch = timestampToSearch,
            timer = time.timer(Long.MAX_VALUE),
        )
        if (expectedOffsetForP0 == null) assertNull(offsetAndTimestampMap[t2p0])
        else {
            assertEquals(expectedOffsetForP0, offsetAndTimestampMap[t2p0]!!.timestamp)
            assertEquals(expectedOffsetForP0, offsetAndTimestampMap[t2p0]!!.offset)
        }
        assertEquals(expectedOffsetForP1, offsetAndTimestampMap[tp1]!!.timestamp)
        assertEquals(expectedOffsetForP1, offsetAndTimestampMap[tp1]!!.offset)
    }

    private fun testGetOffsetsForTimesWithUnknownOffset() {
        client.reset()
        // Ensure metadata has both partitions.
        val initialMetadataUpdate = metadataUpdateWith(
            numNodes = 1,
            topicPartitionCounts = mapOf(topicName to 1),
            topicIds = topicIds,
        )
        client.updateMetadata(initialMetadataUpdate)
        val data = ListOffsetsResponseData()
            .setThrottleTimeMs(0)
            .setTopics(
                listOf(
                    ListOffsetsTopicResponse()
                        .setName(tp0.topic)
                        .setPartitions(
                            listOf(
                                ListOffsetsPartitionResponse()
                                    .setPartitionIndex(tp0.partition)
                                    .setErrorCode(Errors.NONE.code)
                                    .setTimestamp(ListOffsetsResponse.UNKNOWN_TIMESTAMP)
                                    .setOffset(ListOffsetsResponse.UNKNOWN_OFFSET)
                            )
                        )
                )
            )
        client.prepareResponseFrom(
            response = ListOffsetsResponse(data),
            node = metadata.fetch().leaderFor(tp0),
        )
        val timestampToSearch = mapOf(tp0 to 0L)
        val offsetAndTimestampMap = offsetFetcher.offsetsForTimes(
            timestampsToSearch = timestampToSearch,
            timer = time.timer(Long.MAX_VALUE),
        )
        assertTrue(offsetAndTimestampMap.containsKey(tp0))
        assertNull(offsetAndTimestampMap[tp0])
    }

    @Test
    fun testGetOffsetsForTimesWithUnknownOffsetV0() {
        buildFetcher()
        // Empty map
        assertTrue(offsetFetcher.offsetsForTimes(HashMap(), time.timer(100L)).isEmpty())
        // Unknown Offset
        client.reset()
        // Ensure metadata has both partition.
        val initialMetadataUpdate = metadataUpdateWith(
            numNodes = 1,
            topicPartitionCounts = mapOf(topicName to 1),
            topicIds = topicIds,
        )
        client.updateMetadata(initialMetadataUpdate)
        // Force LIST_OFFSETS version 0
        val node = metadata.fetch().nodes[0]
        apiVersions.update(
            node.idString(), NodeApiVersions.create(
                apiKey = ApiKeys.LIST_OFFSETS.id,
                minVersion = 0,
                maxVersion = 0,
            )
        )
        val data = ListOffsetsResponseData()
            .setThrottleTimeMs(0)
            .setTopics(
                listOf(
                    ListOffsetsTopicResponse()
                        .setName(tp0.topic)
                        .setPartitions(
                            listOf(
                                ListOffsetsPartitionResponse()
                                    .setPartitionIndex(tp0.partition)
                                    .setErrorCode(Errors.NONE.code)
                                    .setTimestamp(ListOffsetsResponse.UNKNOWN_TIMESTAMP)
                                    .setOldStyleOffsets(longArrayOf())
                            )
                        )
                )
            )
        client.prepareResponseFrom(
            response = ListOffsetsResponse(data),
            node = metadata.fetch().leaderFor(tp0),
        )
        val timestampToSearch = mapOf(tp0 to 0L)
        val offsetAndTimestampMap = offsetFetcher.offsetsForTimes(
            timestampsToSearch = timestampToSearch,
            timer = time.timer(Long.MAX_VALUE),
        )
        assertTrue(offsetAndTimestampMap.containsKey(tp0))
        assertNull(offsetAndTimestampMap[tp0])
    }

    @Test
    fun testOffsetValidationRequestGrouping() {
        buildFetcher()
        assignFromUser(setOf(tp0, tp1, tp2, tp3))
        metadata.updateWithCurrentRequestVersion(
            response = metadataUpdateWith(
                clusterId = "dummy",
                numNodes = 3,
                topicErrors = emptyMap(),
                topicPartitionCounts = mapOf(topicName to 4),
                epochSupplier = { 5 },
                topicIds = topicIds,
            ), isPartialUpdate = false, nowMs = 0L
        )
        for (tp in subscriptions.assignedPartitions()) {
            val leaderAndEpoch = LeaderAndEpoch(metadata.currentLeader(tp).leader, 4)
            subscriptions.seekUnvalidated(tp, FetchPosition(0, 4, leaderAndEpoch))
        }
        val allRequestedPartitions: MutableSet<TopicPartition> = HashSet()
        for (node in metadata.fetch().nodes) {
            apiVersions.update(node.idString(), NodeApiVersions.create())
            val expectedPartitions = subscriptions.assignedPartitions()
                .filter { tp -> metadata.currentLeader(tp).leader == node }
                .toSet()
            assertTrue(expectedPartitions.none { o -> allRequestedPartitions.contains(o) })
            assertTrue(expectedPartitions.isNotEmpty())
            allRequestedPartitions.addAll(expectedPartitions)
            val data = OffsetForLeaderEpochResponseData()
            expectedPartitions.forEach { tp ->
                var topic = data.topics.find(tp.topic)
                if (topic == null) {
                    topic = OffsetForLeaderTopicResult().setTopic(tp.topic)
                    data.topics.add(topic)
                }
                topic.partitions += OffsetForLeaderEpochResponseData.EpochEndOffset()
                    .setPartition(tp.partition)
                    .setErrorCode(Errors.NONE.code)
                    .setLeaderEpoch(4)
                    .setEndOffset(0)
            }
            val response = OffsetsForLeaderEpochResponse(data)
            client.prepareResponseFrom({ body: AbstractRequest? ->
                val request = body as OffsetsForLeaderEpochRequest?
                expectedPartitions == offsetForLeaderPartitionMap(request!!.data()).keys
            }, response, node)
        }
        assertEquals(subscriptions.assignedPartitions(), allRequestedPartitions)
        offsetFetcher.validatePositionsIfNeeded()
        consumerClient.pollNoWakeup()
        assertTrue(subscriptions.assignedPartitions().none { tp -> subscriptions.awaitingValidation(tp) })
    }

    @Test
    fun testOffsetValidationAwaitsNodeApiVersion() {
        buildFetcher()
        assignFromUser(setOf(tp0))
        val partitionCounts: MutableMap<String, Int> = HashMap()
        partitionCounts[tp0.topic] = 4
        val epochOne = 1
        metadata.updateWithCurrentRequestVersion(
            response = metadataUpdateWith(
                clusterId = "dummy",
                numNodes = 1,
                topicErrors = emptyMap(),
                topicPartitionCounts = partitionCounts,
                epochSupplier = { epochOne },
                topicIds = topicIds,
            ),
            isPartialUpdate = false,
            nowMs = 0L,
        )
        val node = metadata.fetch().nodes[0]
        assertFalse(client.isConnected(node.idString()))

        // Seek with a position and leader+epoch
        val leaderAndEpoch = LeaderAndEpoch(metadata.currentLeader(tp0).leader, epochOne)
        subscriptions.seekUnvalidated(tp0, FetchPosition(20L, epochOne, leaderAndEpoch))
        assertFalse(client.isConnected(node.idString()))
        assertTrue(subscriptions.awaitingValidation(tp0))

        // No version information is initially available, but the node is now connected
        offsetFetcher.validatePositionsIfNeeded()
        assertTrue(subscriptions.awaitingValidation(tp0))
        assertTrue(client.isConnected(node.idString()))
        apiVersions.update(node.idString(), NodeApiVersions.create())

        // On the next call, the OffsetForLeaderEpoch request is sent and validation completes
        client.prepareResponseFrom(
            response = prepareOffsetsForLeaderEpochResponse(tp0, epochOne, 30L),
            node = node,
        )
        offsetFetcher.validatePositionsIfNeeded()
        consumerClient.pollNoWakeup()
        assertFalse(subscriptions.awaitingValidation(tp0))
        assertEquals(20L, subscriptions.position(tp0)!!.offset)
    }

    @Test
    fun testOffsetValidationSkippedForOldBroker() {
        // Old brokers may require CLUSTER permission to use the OffsetForLeaderEpoch API,
        // so we should skip offset validation and not send the request.
        val isolationLevel = IsolationLevel.READ_UNCOMMITTED
        val maxPollRecords = Int.MAX_VALUE
        val metadataExpireMs = Long.MAX_VALUE
        val offsetResetStrategy = OffsetResetStrategy.EARLIEST
        val minBytes = 1
        val maxBytes = Int.MAX_VALUE
        val maxWaitMs = 0
        val fetchSize = 1000
        val metricConfig = MetricConfig()
        val logContext = LogContext()
        val subscriptionState = SubscriptionState(logContext, offsetResetStrategy)
        buildFetcher(
            metricConfig = metricConfig,
            isolationLevel = isolationLevel,
            metadataExpireMs = metadataExpireMs,
            subscriptionState = subscriptionState,
            logContext = logContext,
        )
        val metricsRegistry = FetchMetricsRegistry(metricConfig.tags().keys, "consumertest-group")
        val fetchConfig = FetchConfig(
            minBytes = minBytes,
            maxBytes = maxBytes,
            maxWaitMs = maxWaitMs,
            fetchSize = fetchSize,
            maxPollRecords = maxPollRecords,
            checkCrcs = true,  // check crc
            clientRackId = CommonClientConfigs.DEFAULT_CLIENT_RACK,
            keyDeserializer = ByteArrayDeserializer(),
            valueDeserializer = ByteArrayDeserializer(),
            isolationLevel = isolationLevel,
        )
        val fetcher = Fetcher(
            logContext = logContext,
            client = consumerClient,
            metadata = metadata,
            subscriptions = subscriptions,
            fetchConfig = fetchConfig,
            metricsManager = FetchMetricsManager(metrics, metricsRegistry),
            time = time,
        )
        assignFromUser(setOf(tp0))
        val partitionCounts = mapOf(tp0.topic to 4)
        val epochOne = 1
        val epochTwo = 2

        // Start with metadata, epoch=1
        metadata.updateWithCurrentRequestVersion(
            response = metadataUpdateWith(
                clusterId = "dummy",
                numNodes = 1,
                topicErrors = emptyMap(),
                topicPartitionCounts = partitionCounts,
                epochSupplier = { epochOne },
                topicIds = topicIds,
            ),
            isPartialUpdate = false,
            nowMs = 0L,
        )

        // Offset validation requires OffsetForLeaderEpoch request v3 or higher
        val node = metadata.fetch().nodes[0]
        apiVersions.update(
            nodeId = node.idString(),
            nodeApiVersions = NodeApiVersions.create(
                apiKey = ApiKeys.OFFSET_FOR_LEADER_EPOCH.id,
                minVersion = 0,
                maxVersion = 2,
            )
        )
        run {
            // Seek with a position and leader+epoch
            val leaderAndEpoch = LeaderAndEpoch(metadata.currentLeader(tp0).leader, epochOne)
            subscriptions.seekUnvalidated(tp0, FetchPosition(0, epochOne, leaderAndEpoch))

            // Update metadata to epoch=2, enter validation
            metadata.updateWithCurrentRequestVersion(
                response = metadataUpdateWith(
                    clusterId = "dummy",
                    numNodes = 1,
                    topicErrors = emptyMap(),
                    topicPartitionCounts = partitionCounts,
                    epochSupplier = { epochTwo },
                    topicIds = topicIds,
                ),
                isPartialUpdate = false,
                nowMs = 0L,
            )
            offsetFetcher.validatePositionsIfNeeded()

            // Offset validation is skipped
            assertFalse(subscriptions.awaitingValidation(tp0))
        }
        run {

            // Seek with a position and leader+epoch
            val leaderAndEpoch = LeaderAndEpoch(metadata.currentLeader(tp0).leader, epochOne)
            subscriptions.seekUnvalidated(tp0, FetchPosition(0, epochOne, leaderAndEpoch))

            // Update metadata to epoch=2, enter validation
            metadata.updateWithCurrentRequestVersion(
                response = metadataUpdateWith(
                    clusterId = "dummy",
                    numNodes = 1,
                    topicErrors = emptyMap(),
                    topicPartitionCounts = partitionCounts,
                    epochSupplier = { epochTwo },
                    topicIds = topicIds,
                ),
                isPartialUpdate = false,
                nowMs = 0L,
            )

            // Subscription should not stay in AWAITING_VALIDATION in prepareFetchRequest
            offsetFetcher.validatePositionsOnMetadataChange()
            assertEquals(1, fetcher.sendFetches())
            assertFalse(subscriptions.awaitingValidation(tp0))
        }
    }

    @Test
    fun testOffsetValidationSkippedForOldResponse() {
        // Old responses may provide unreliable leader epoch,
        // so we should skip offset validation and not send the request.
        buildFetcher()
        assignFromUser(setOf(tp0))
        val partitionCounts = mapOf(tp0.topic to 4)
        val epochOne = 1
        metadata.updateWithCurrentRequestVersion(
            response = metadataUpdateWith(
                clusterId = "dummy",
                numNodes = 1,
                topicErrors = emptyMap(),
                topicPartitionCounts = partitionCounts,
                epochSupplier = { epochOne },
                topicIds = topicIds,
            ),
            isPartialUpdate = false,
            nowMs = 0L,
        )
        val node = metadata.fetch().nodes[0]
        assertFalse(client.isConnected(node.idString()))

        // Seek with a position and leader+epoch
        val leaderAndEpoch = LeaderAndEpoch(metadata.currentLeader(tp0).leader, epochOne)
        subscriptions.seekUnvalidated(tp0, FetchPosition(20L, epochOne, leaderAndEpoch))
        assertFalse(client.isConnected(node.idString()))
        assertTrue(subscriptions.awaitingValidation(tp0))

        // Inject an older version of the metadata response
        val responseVersion: Short = 8
        metadata.updateWithCurrentRequestVersion(
            response = metadataUpdateWith(
                clusterId = "dummy",
                numNodes = 1,
                topicErrors = emptyMap(),
                topicPartitionCounts = partitionCounts,
                epochSupplier = { null },
                partitionSupplier = { error, topicPartition, leaderId, leaderEpoch, replicaIds, inSyncReplicaIds, offlineReplicaIds ->
                    MetadataResponse.PartitionMetadata(
                        error = error,
                        topicPartition = topicPartition,
                        leaderId = leaderId,
                        leaderEpoch = leaderEpoch,
                        replicaIds = replicaIds,
                        inSyncReplicaIds = inSyncReplicaIds,
                        offlineReplicaIds = offlineReplicaIds,
                    )
                },
                responseVersion = responseVersion,
                topicIds = topicIds,
            ),
            isPartialUpdate = false,
            nowMs = 0L,
        )
        offsetFetcher.validatePositionsIfNeeded()
        // Offset validation is skipped
        assertFalse(subscriptions.awaitingValidation(tp0))
    }

    @Test
    fun testOffsetValidationresetPositionForUndefinedEpochWithDefinedResetPolicy() {
        testOffsetValidationWithGivenEpochOffset(
            leaderEpoch = OffsetsForLeaderEpochResponse.UNDEFINED_EPOCH,
            endOffset = 0L,
            offsetResetStrategy = OffsetResetStrategy.EARLIEST,
        )
    }

    @Test
    fun testOffsetValidationresetPositionForUndefinedOffsetWithDefinedResetPolicy() {
        testOffsetValidationWithGivenEpochOffset(
            leaderEpoch = 2,
            endOffset = OffsetsForLeaderEpochResponse.UNDEFINED_EPOCH_OFFSET,
            offsetResetStrategy = OffsetResetStrategy.EARLIEST,
        )
    }

    @Test
    fun testOffsetValidationresetPositionForUndefinedEpochWithUndefinedResetPolicy() {
        testOffsetValidationWithGivenEpochOffset(
            leaderEpoch = OffsetsForLeaderEpochResponse.UNDEFINED_EPOCH,
            endOffset = 0L,
            offsetResetStrategy = OffsetResetStrategy.NONE,
        )
    }

    @Test
    fun testOffsetValidationresetPositionForUndefinedOffsetWithUndefinedResetPolicy() {
        testOffsetValidationWithGivenEpochOffset(
            leaderEpoch = 2,
            endOffset = OffsetsForLeaderEpochResponse.UNDEFINED_EPOCH_OFFSET,
            offsetResetStrategy = OffsetResetStrategy.NONE,
        )
    }

    @Test
    fun testOffsetValidationTriggerLogTruncationForBadOffsetWithUndefinedResetPolicy() {
        testOffsetValidationWithGivenEpochOffset(
            leaderEpoch = 1,
            endOffset = 1L,
            offsetResetStrategy = OffsetResetStrategy.NONE,
        )
    }

    private fun testOffsetValidationWithGivenEpochOffset(
        leaderEpoch: Int,
        endOffset: Long,
        offsetResetStrategy: OffsetResetStrategy,
    ) {
        buildFetcher(offsetResetStrategy = offsetResetStrategy)
        assignFromUser(setOf(tp0))
        val partitionCounts = mapOf(tp0.topic to 4)
        val epochOne = 1
        val initialOffset: Long = 5
        metadata.updateWithCurrentRequestVersion(
            response = metadataUpdateWith(
                clusterId = "dummy",
                numNodes = 1,
                topicErrors = emptyMap(),
                topicPartitionCounts = partitionCounts,
                epochSupplier = { epochOne },
                topicIds = topicIds,
            ),
            isPartialUpdate = false,
            nowMs = 0L,
        )

        // Offset validation requires OffsetForLeaderEpoch request v3 or higher
        val node = metadata.fetch().nodes[0]
        apiVersions.update(node.idString(), NodeApiVersions.create())
        val leaderAndEpoch = LeaderAndEpoch(metadata.currentLeader(tp0).leader, epochOne)
        subscriptions.seekUnvalidated(tp0, FetchPosition(initialOffset, epochOne, leaderAndEpoch))
        offsetFetcher.validatePositionsIfNeeded()
        consumerClient.poll(time.timer(Duration.ZERO))
        assertTrue(subscriptions.awaitingValidation(tp0))
        assertTrue(client.hasInFlightRequests())
        client.respond(
            matcher = offsetsForLeaderEpochRequestMatcher(tp0),
            response = prepareOffsetsForLeaderEpochResponse(tp0, leaderEpoch, endOffset),
        )
        consumerClient.poll(time.timer(Duration.ZERO))
        if (offsetResetStrategy == OffsetResetStrategy.NONE) {
            val thrown = assertFailsWith<LogTruncationException> { offsetFetcher.validatePositionsIfNeeded() }
            assertEquals(mapOf(tp0 to initialOffset), thrown.offsetOutOfRangePartitions)
            if (
                endOffset == OffsetsForLeaderEpochResponse.UNDEFINED_EPOCH_OFFSET
                || leaderEpoch == OffsetsForLeaderEpochResponse.UNDEFINED_EPOCH
            ) assertTrue(thrown.divergentOffsets.isEmpty())
            else {
                val expectedDivergentOffset = OffsetAndMetadata(endOffset, leaderEpoch, "")
                assertEquals(mapOf(tp0 to expectedDivergentOffset), thrown.divergentOffsets)
            }
            assertTrue(subscriptions.awaitingValidation(tp0))
        } else {
            offsetFetcher.validatePositionsIfNeeded()
            assertFalse(subscriptions.awaitingValidation(tp0))
        }
    }

    @Test
    fun testOffsetValidationHandlesSeekWithInflightOffsetForLeaderRequest() {
        buildFetcher()
        assignFromUser(setOf(tp0))
        val partitionCounts = mapOf(tp0.topic to 4)
        val epochOne = 1
        val epochOneOpt = epochOne
        metadata.updateWithCurrentRequestVersion(
            response = metadataUpdateWith(
                clusterId = "dummy",
                numNodes = 1,
                topicErrors = emptyMap(),
                topicPartitionCounts = partitionCounts,
                epochSupplier = { epochOne },
                topicIds = topicIds,
            ),
            isPartialUpdate = false,
            nowMs = 0L,
        )

        // Offset validation requires OffsetForLeaderEpoch request v3 or higher
        val node = metadata.fetch().nodes[0]
        apiVersions.update(node.idString(), NodeApiVersions.create())
        val leaderAndEpoch = LeaderAndEpoch(metadata.currentLeader(tp0).leader, epochOneOpt)
        subscriptions.seekUnvalidated(tp0, FetchPosition(0, epochOneOpt, leaderAndEpoch))
        offsetFetcher.validatePositionsIfNeeded()
        consumerClient.poll(time.timer(Duration.ZERO))
        assertTrue(subscriptions.awaitingValidation(tp0))
        assertTrue(client.hasInFlightRequests())

        // While the OffsetForLeaderEpoch request is in-flight, we seek to a different offset.
        subscriptions.seekUnvalidated(tp0, FetchPosition(5, epochOneOpt, leaderAndEpoch))
        assertTrue(subscriptions.awaitingValidation(tp0))
        client.respond(
            matcher = offsetsForLeaderEpochRequestMatcher(tp0),
            response = prepareOffsetsForLeaderEpochResponse(tp0, 0, 0L),
        )
        consumerClient.poll(time.timer(Duration.ZERO))

        // The response should be ignored since we were validating a different position.
        assertTrue(subscriptions.awaitingValidation(tp0))
    }

    @Test
    fun testOffsetValidationFencing() {
        buildFetcher()
        assignFromUser(setOf(tp0))
        val partitionCounts: MutableMap<String, Int> = HashMap()
        partitionCounts[tp0.topic] = 4
        val epochOne = 1
        val epochTwo = 2
        val epochThree = 3

        // Start with metadata, epoch=1
        metadata.updateWithCurrentRequestVersion(
            response = metadataUpdateWith(
                clusterId = "dummy",
                numNodes = 1,
                topicErrors = emptyMap(),
                topicPartitionCounts = partitionCounts,
                epochSupplier = { epochOne },
                topicIds = topicIds,
            ),
            isPartialUpdate = false,
            nowMs = 0L,
        )

        // Offset validation requires OffsetForLeaderEpoch request v3 or higher
        val node = metadata.fetch().nodes[0]
        apiVersions.update(node.idString(), NodeApiVersions.create())

        // Seek with a position and leader+epoch
        val leaderAndEpoch = LeaderAndEpoch(metadata.currentLeader(tp0).leader, epochOne)
        subscriptions.seekValidated(tp0, FetchPosition(0, epochOne, leaderAndEpoch))

        // Update metadata to epoch=2, enter validation
        metadata.updateWithCurrentRequestVersion(
            response = metadataUpdateWith(
                clusterId = "dummy",
                numNodes = 1,
                topicErrors = emptyMap(),
                topicPartitionCounts = partitionCounts,
                epochSupplier = { epochTwo },
                topicIds = topicIds,
            ),
            isPartialUpdate = false,
            nowMs = 0L,
        )
        offsetFetcher.validatePositionsIfNeeded()
        assertTrue(subscriptions.awaitingValidation(tp0))

        // Update the position to epoch=3, as we would from a fetch
        subscriptions.completeValidation(tp0)
        val nextPosition = FetchPosition(
            offset = 10,
            offsetEpoch = epochTwo,
            currentLeader = LeaderAndEpoch(leaderAndEpoch.leader, epochTwo),
        )
        subscriptions.position(tp0, nextPosition)
        subscriptions.maybeValidatePositionForCurrentLeader(
            apiVersions = apiVersions,
            topicPartition = tp0,
            leaderAndEpoch = LeaderAndEpoch(leaderAndEpoch.leader, epochThree),
        )

        // Prepare offset list response from async validation with epoch=2
        client.prepareResponse(prepareOffsetsForLeaderEpochResponse(tp0, epochTwo, 10L))
        consumerClient.pollNoWakeup()
        assertTrue(
            actual = subscriptions.awaitingValidation(tp0),
            message = "Expected validation to fail since leader epoch changed"
        )

        // Next round of validation, should succeed in validating the position
        offsetFetcher.validatePositionsIfNeeded()
        client.prepareResponse(prepareOffsetsForLeaderEpochResponse(tp0, epochThree, 10L))
        consumerClient.pollNoWakeup()
        assertFalse(
            actual = subscriptions.awaitingValidation(tp0),
            message = "Expected validation to succeed with latest epoch"
        )
    }

    @Test
    fun testBeginningOffsets() {
        buildFetcher()
        assignFromUser(setOf(tp0))
        client.prepareResponse(
            listOffsetResponse(
                tp = tp0,
                error = Errors.NONE,
                timestamp = ListOffsetsRequest.EARLIEST_TIMESTAMP,
                offset = 2L,
            )
        )
        assertEquals(
            expected = mapOf(tp0 to 2L),
            actual = offsetFetcher.beginningOffsets(
                partitions = setOf(tp0),
                timer = time.timer(5000L),
            )
        )
    }

    @Test
    fun testBeginningOffsetsDuplicateTopicPartition() {
        buildFetcher()
        assignFromUser(setOf(tp0))
        client.prepareResponse(
            listOffsetResponse(
                tp = tp0,
                error = Errors.NONE,
                timestamp = ListOffsetsRequest.EARLIEST_TIMESTAMP,
                offset = 2L,
            ),
        )
        assertEquals(
            expected = mapOf(tp0 to 2L),
            actual = offsetFetcher.beginningOffsets(
                partitions = listOf(tp0, tp0),
                timer = time.timer(5000L),
            )
        )
    }

    @Test
    fun testBeginningOffsetsMultipleTopicPartitions() {
        buildFetcher()
        val expectedOffsets: MutableMap<TopicPartition, Long> = HashMap()
        expectedOffsets[tp0] = 2L
        expectedOffsets[tp1] = 4L
        expectedOffsets[tp2] = 6L
        assignFromUser(expectedOffsets.keys)
        client.prepareResponse(
            listOffsetResponse(
                offsets = expectedOffsets,
                error = Errors.NONE,
                timestamp = ListOffsetsRequest.EARLIEST_TIMESTAMP,
                leaderEpoch = ListOffsetsResponse.UNKNOWN_EPOCH,
            )
        )
        assertEquals(
            expected = expectedOffsets,
            actual = offsetFetcher.beginningOffsets(
                partitions = listOf(tp0, tp1, tp2),
                timer = time.timer(5000L),
            ),
        )
    }

    @Test
    fun testBeginningOffsetsEmpty() {
        buildFetcher()
        assertTrue(offsetFetcher.beginningOffsets(emptyList(), time.timer(5000L)).isEmpty())
    }

    @Test
    fun testEndOffsets() {
        buildFetcher()
        assignFromUser(setOf(tp0))
        client.prepareResponse(
            listOffsetResponse(
                tp = tp0,
                error = Errors.NONE,
                timestamp = ListOffsetsRequest.LATEST_TIMESTAMP,
                offset = 5L,
            )
        )
        assertEquals(
            expected = mapOf(tp0 to 5L),
            actual = offsetFetcher.endOffsets(
                partitions = setOf(tp0),
                timer = time.timer(5000L),
            ),
        )
    }

    @Test
    fun testEndOffsetsDuplicateTopicPartition() {
        buildFetcher()
        assignFromUser(setOf(tp0))
        client.prepareResponse(
            listOffsetResponse(
                tp = tp0,
                error = Errors.NONE,
                timestamp = ListOffsetsRequest.LATEST_TIMESTAMP,
                offset = 5L,
            )
        )
        assertEquals(
            expected = mapOf(tp0 to 5L),
            actual = offsetFetcher.endOffsets(
                partitions = listOf(tp0, tp0),
                timer = time.timer(5000L),
            )
        )
    }

    @Test
    fun testEndOffsetsMultipleTopicPartitions() {
        buildFetcher()
        val expectedOffsets: MutableMap<TopicPartition, Long> = HashMap()
        expectedOffsets[tp0] = 5L
        expectedOffsets[tp1] = 7L
        expectedOffsets[tp2] = 9L
        assignFromUser(expectedOffsets.keys)
        client.prepareResponse(
            listOffsetResponse(
                offsets = expectedOffsets,
                error = Errors.NONE,
                timestamp = ListOffsetsRequest.LATEST_TIMESTAMP,
                leaderEpoch = ListOffsetsResponse.UNKNOWN_EPOCH,
            )
        )
        assertEquals(
            expected = expectedOffsets,
            actual = offsetFetcher.endOffsets(
                partitions = listOf(tp0, tp1, tp2),
                timer = time.timer(5000L),
            ),
        )
    }

    @Test
    fun testEndOffsetsEmpty() {
        buildFetcher()
        assertTrue(offsetFetcher.endOffsets(emptyList(), time.timer(5000L)).isEmpty())
    }

    private fun offsetsForLeaderEpochRequestMatcher(topicPartition: TopicPartition): RequestMatcher {
        val currentLeaderEpoch = 1
        val leaderEpoch = 1
        return RequestMatcher { request ->
            val epochRequest = request as OffsetsForLeaderEpochRequest
            val partition = offsetForLeaderPartitionMap(epochRequest.data())[topicPartition]
            partition != null
                    && partition.currentLeaderEpoch == currentLeaderEpoch
                    && partition.leaderEpoch == leaderEpoch
        }
    }

    private fun prepareOffsetsForLeaderEpochResponse(
        topicPartition: TopicPartition,
        leaderEpoch: Int,
        endOffset: Long,
    ): OffsetsForLeaderEpochResponse {
        val data = OffsetForLeaderEpochResponseData()
        data.topics.add(
            OffsetForLeaderTopicResult()
                .setTopic(topicPartition.topic)
                .setPartitions(
                    listOf(
                        OffsetForLeaderEpochResponseData.EpochEndOffset()
                            .setPartition(topicPartition.partition)
                            .setErrorCode(Errors.NONE.code)
                            .setLeaderEpoch(leaderEpoch)
                            .setEndOffset(endOffset)
                    )
                )
        )
        return OffsetsForLeaderEpochResponse(data)
    }

    private fun offsetForLeaderPartitionMap(
        data: OffsetForLeaderEpochRequestData,
    ): Map<TopicPartition, OffsetForLeaderPartition> {
        val result = mutableMapOf<TopicPartition, OffsetForLeaderPartition>()
        data.topics.forEach { topic ->
            topic.partitions.forEach { partition ->
                result[TopicPartition(topic.topic, partition.partition)] = partition
            }
        }
        return result
    }

    private fun listOffsetRequestMatcher(
        timestamp: Long,
        leaderEpoch: Int = ListOffsetsResponse.UNKNOWN_EPOCH,
    ): RequestMatcher {
        // matches any list offset request with the provided timestamp
        return RequestMatcher { body ->
            val req = body as ListOffsetsRequest
            val topic = req.topics[0]
            val partition: ListOffsetsPartition = topic.partitions[0]
            tp0.topic == topic.name
                    && tp0.partition == partition.partitionIndex
                    && timestamp == partition.timestamp
                    && leaderEpoch == partition.currentLeaderEpoch
        }
    }

    private fun listOffsetResponse(error: Errors, timestamp: Long, offset: Long): ListOffsetsResponse {
        return listOffsetResponse(tp0, error, timestamp, offset)
    }

    private fun listOffsetResponse(
        tp: TopicPartition,
        error: Errors,
        timestamp: Long,
        offset: Long,
        leaderEpoch: Int = ListOffsetsResponse.UNKNOWN_EPOCH,
    ): ListOffsetsResponse {
        val offsets = mapOf(tp to offset)
        return listOffsetResponse(offsets, error, timestamp, leaderEpoch)
    }

    private fun listOffsetResponse(
        offsets: Map<TopicPartition, Long>,
        error: Errors,
        timestamp: Long,
        leaderEpoch: Int,
    ): ListOffsetsResponse {
        val responses: MutableMap<String, MutableList<ListOffsetsPartitionResponse>> = HashMap()
        for ((tp, value) in offsets) {
            responses.putIfAbsent(tp.topic, ArrayList())
            responses[tp.topic]!!.add(
                ListOffsetsPartitionResponse()
                    .setPartitionIndex(tp.partition)
                    .setErrorCode(error.code)
                    .setOffset(value)
                    .setTimestamp(timestamp)
                    .setLeaderEpoch(leaderEpoch)
            )
        }
        val topics: MutableList<ListOffsetsTopicResponse> = ArrayList()
        for ((key, value) in responses) {
            topics.add(
                ListOffsetsTopicResponse()
                    .setName(key)
                    .setPartitions(value)
            )
        }
        val data = ListOffsetsResponseData().setTopics(topics)
        return ListOffsetsResponse(data)
    }

    private fun buildFetcher(
        metricConfig: MetricConfig = MetricConfig(),
        offsetResetStrategy: OffsetResetStrategy = OffsetResetStrategy.EARLIEST,
        isolationLevel: IsolationLevel = IsolationLevel.READ_UNCOMMITTED,
        metadataExpireMs: Long = Long.MAX_VALUE,
        logContext: LogContext = LogContext(),
        subscriptionState: SubscriptionState = SubscriptionState(logContext, offsetResetStrategy),
    ) {
        buildDependencies(metricConfig, metadataExpireMs, subscriptionState, logContext)
        val requestTimeoutMs: Long = 30000
        offsetFetcher = OffsetFetcher(
            logContext = logContext,
            client = consumerClient,
            metadata = metadata,
            subscriptions = subscriptions,
            time = time,
            retryBackoffMs = retryBackoffMs,
            requestTimeoutMs = requestTimeoutMs,
            isolationLevel = isolationLevel,
            apiVersions = apiVersions
        )
    }

    private fun buildDependencies(
        metricConfig: MetricConfig,
        metadataExpireMs: Long,
        subscriptionState: SubscriptionState,
        logContext: LogContext,
    ) {
        time = MockTime(1)
        subscriptions = subscriptionState
        metadata = ConsumerMetadata(
            refreshBackoffMs = 0,
            metadataExpireMs = metadataExpireMs,
            includeInternalTopics = false,
            allowAutoTopicCreation = false,
            subscription = subscriptions,
            logContext = logContext,
            clusterResourceListeners = ClusterResourceListeners(),
        )
        client = MockClient(time = time, metadata = metadata)
        metrics = Metrics(config = metricConfig, time = time)
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
}

