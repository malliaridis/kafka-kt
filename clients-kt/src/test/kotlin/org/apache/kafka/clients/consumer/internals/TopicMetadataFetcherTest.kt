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

import org.apache.kafka.clients.MockClient
import org.apache.kafka.clients.consumer.OffsetResetStrategy
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.Uuid
import org.apache.kafka.common.errors.InvalidTopicException
import org.apache.kafka.common.errors.TimeoutException
import org.apache.kafka.common.errors.TopicAuthorizationException
import org.apache.kafka.common.internals.ClusterResourceListeners
import org.apache.kafka.common.metrics.MetricConfig
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.MetadataResponse
import org.apache.kafka.common.requests.MetadataResponse.PartitionMetadata
import org.apache.kafka.common.requests.RequestTestUtils.metadataResponse
import org.apache.kafka.common.requests.RequestTestUtils.metadataUpdateWith
import org.apache.kafka.common.utils.LogContext
import org.apache.kafka.common.utils.MockTime
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertNull

class TopicMetadataFetcherTest {

    private val topicName = "test"

    private val topicId = Uuid.randomUuid()

    private val topicIds = mapOf(topicName to topicId)

    private val tp0 = TopicPartition(topicName, 0)

    private val validLeaderEpoch = 0

    private val initialUpdateResponse = metadataUpdateWith(
        numNodes = 1,
        topicPartitionCounts = mapOf(topicName to 4),
        topicIds = topicIds,
    )

    private var time = MockTime(1)

    private lateinit var subscriptions: SubscriptionState

    private lateinit var metadata: ConsumerMetadata

    private lateinit var client: MockClient

    private lateinit var metrics: Metrics

    private lateinit var consumerClient: ConsumerNetworkClient

    private lateinit var topicMetadataFetcher: TopicMetadataFetcher

    @BeforeEach
    fun setup() = Unit

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

    @AfterEach
    @Throws(Exception::class)
    fun teardown() {
        if (::metrics.isInitialized) metrics.close()
    }

    @Test
    fun testGetAllTopics() {
        // sending response before request, as getTopicMetadata is a blocking call
        buildFetcher()
        assignFromUser(setOf(tp0))
        client.prepareResponse(newMetadataResponse(Errors.NONE))
        val allTopics = topicMetadataFetcher.getAllTopicMetadata(time.timer(5000L))
        assertEquals(initialUpdateResponse.topicMetadata().size, allTopics.size)
    }

    @Test
    fun testGetAllTopicsDisconnect() {
        // first try gets a disconnect, next succeeds
        buildFetcher()
        assignFromUser(setOf(tp0))
        client.prepareResponse(response = null, disconnected = true)
        client.prepareResponse(newMetadataResponse(Errors.NONE))
        val allTopics = topicMetadataFetcher.getAllTopicMetadata(time.timer(5000L))
        assertEquals(initialUpdateResponse.topicMetadata().size, allTopics.size)
    }

    @Test
    fun testGetAllTopicsTimeout() {
        // since no response is prepared, the request should time out
        buildFetcher()
        assignFromUser(setOf(tp0))
        assertFailsWith<TimeoutException> { topicMetadataFetcher.getAllTopicMetadata(time.timer(50L)) }
    }

    @Test
    fun testGetAllTopicsUnauthorized() {
        buildFetcher()
        assignFromUser(setOf(tp0))
        client.prepareResponse(newMetadataResponse(Errors.TOPIC_AUTHORIZATION_FAILED))

        val error = assertFailsWith<TopicAuthorizationException> {
            topicMetadataFetcher.getAllTopicMetadata(time.timer(10L))
        }
        assertEquals(setOf(topicName), error.unauthorizedTopics)
    }

    @Test
    fun testGetTopicMetadataInvalidTopic() {
        buildFetcher()
        assignFromUser(setOf(tp0))
        client.prepareResponse(newMetadataResponse(Errors.INVALID_TOPIC_EXCEPTION))
        assertFailsWith<InvalidTopicException> {
            topicMetadataFetcher.getTopicMetadata(
                topic = topicName,
                allowAutoTopicCreation = true,
                timer = time.timer(5000L),
            )
        }
    }

    @Test
    fun testGetTopicMetadataUnknownTopic() {
        buildFetcher()
        assignFromUser(setOf(tp0))
        client.prepareResponse(newMetadataResponse(Errors.UNKNOWN_TOPIC_OR_PARTITION))
        val topicMetadata = topicMetadataFetcher.getTopicMetadata(
            topic = topicName,
            allowAutoTopicCreation = true,
            timer = time.timer(5000L),
        )
        assertNull(topicMetadata)
    }

    @Test
    fun testGetTopicMetadataLeaderNotAvailable() {
        buildFetcher()
        assignFromUser(setOf(tp0))
        client.prepareResponse(newMetadataResponse(Errors.LEADER_NOT_AVAILABLE))
        client.prepareResponse(newMetadataResponse(Errors.NONE))
        val topicMetadata = topicMetadataFetcher.getTopicMetadata(
            topic = topicName,
            allowAutoTopicCreation = true,
            timer = time.timer(5000L)
        )
        assertNotNull(topicMetadata)
    }

    @Test
    fun testGetTopicMetadataOfflinePartitions() {
        buildFetcher()
        assignFromUser(setOf(tp0))
        val originalResponse = newMetadataResponse(Errors.NONE) //baseline ok response

        //create a response based on the above one with all partitions being leaderless
        val altTopics: MutableList<MetadataResponse.TopicMetadata> = ArrayList()
        for (item in originalResponse.topicMetadata()) {
            val altPartitions = item.partitionMetadata.map { partition ->
                PartitionMetadata(
                    error = partition.error,
                    topicPartition = partition.topicPartition,
                    leaderId = null,  //no leader
                    leaderEpoch = null,
                    replicaIds = partition.replicaIds,
                    inSyncReplicaIds = partition.inSyncReplicaIds,
                    offlineReplicaIds = partition.offlineReplicaIds,
                )
            }
            val alteredTopic = MetadataResponse.TopicMetadata(
                error = item.error,
                topic = item.topic,
                isInternal = item.isInternal,
                partitionMetadata = altPartitions,
            )
            altTopics.add(alteredTopic)
        }
        val controller = originalResponse.controller
        val altered = metadataResponse(
            brokers = originalResponse.brokers(),
            clusterId = originalResponse.clusterId,
            controllerId = controller?.id ?: MetadataResponse.NO_CONTROLLER_ID,
            topicMetadataList = altTopics,
        )
        client.prepareResponse(altered)
        val topicMetadata = topicMetadataFetcher.getTopicMetadata(
            topic = topicName,
            allowAutoTopicCreation = false,
            timer = time.timer(5000L),
        )
        assertNotNull(topicMetadata)
        assertFalse(topicMetadata.isEmpty())
        assertEquals(metadata.fetch().partitionCountForTopic(topicName)!!, topicMetadata.size)
    }

    private fun newMetadataResponse(error: Errors): MetadataResponse {
        val partitionsMetadata = mutableListOf<PartitionMetadata>()
        if (error == Errors.NONE) {
            val foundMetadata = initialUpdateResponse.topicMetadata()
                .firstOrNull { metadata -> metadata.topic == topicName }
            foundMetadata?.let { partitionsMetadata.addAll(it.partitionMetadata) }
        }
        val topicMetadata = MetadataResponse.TopicMetadata(
            error = error,
            topic = topicName,
            isInternal = false,
            partitionMetadata = partitionsMetadata,
        )
        val brokers = ArrayList(initialUpdateResponse.brokers())
        return metadataResponse(
            brokers = brokers,
            clusterId = initialUpdateResponse.clusterId,
            controllerId = initialUpdateResponse.controller!!.id,
            topicMetadataList = listOf(topicMetadata),
        )
    }

    private fun buildFetcher() {
        val metricConfig = MetricConfig()
        val metadataExpireMs = Long.MAX_VALUE
        val retryBackoffMs: Long = 100
        val logContext = LogContext()
        val subscriptionState = SubscriptionState(logContext, OffsetResetStrategy.EARLIEST)
        buildDependencies(
            metricConfig = metricConfig,
            metadataExpireMs = metadataExpireMs,
            subscriptionState = subscriptionState,
            logContext = logContext,
        )
        topicMetadataFetcher = TopicMetadataFetcher(
            logContext = logContext,
            client = consumerClient,
            retryBackoffMs = retryBackoffMs,
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
        client = MockClient(time, metadata)
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
