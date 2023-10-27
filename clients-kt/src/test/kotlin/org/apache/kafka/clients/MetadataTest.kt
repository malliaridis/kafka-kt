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

package org.apache.kafka.clients

import org.apache.kafka.common.Cluster
import org.apache.kafka.common.Node
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.Uuid
import org.apache.kafka.common.errors.InvalidTopicException
import org.apache.kafka.common.errors.TopicAuthorizationException
import org.apache.kafka.common.internals.ClusterResourceListeners
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.common.message.MetadataResponseData
import org.apache.kafka.common.message.MetadataResponseData.MetadataResponseBroker
import org.apache.kafka.common.message.MetadataResponseData.MetadataResponseBrokerCollection
import org.apache.kafka.common.message.MetadataResponseData.MetadataResponsePartition
import org.apache.kafka.common.message.MetadataResponseData.MetadataResponseTopic
import org.apache.kafka.common.message.MetadataResponseData.MetadataResponseTopicCollection
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.protocol.MessageUtil.toByteBuffer
import org.apache.kafka.common.requests.MetadataRequest
import org.apache.kafka.common.requests.MetadataResponse
import org.apache.kafka.common.requests.MetadataResponse.PartitionMetadata
import org.apache.kafka.common.requests.RequestTestUtils.metadataResponse
import org.apache.kafka.common.requests.RequestTestUtils.metadataUpdateWith
import org.apache.kafka.common.requests.RequestTestUtils.metadataUpdateWithIds
import org.apache.kafka.common.utils.LogContext
import org.apache.kafka.common.utils.MockTime
import org.apache.kafka.test.MockClusterResourceListener
import org.apache.kafka.test.TestUtils.assertNullable
import java.net.InetSocketAddress
import java.util.concurrent.atomic.AtomicReference
import kotlin.math.max
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

class MetadataTest {

    private val refreshBackoffMs = 100L

    private val metadataExpireMs = 1000L

    private var metadata = Metadata(
        refreshBackoffMs = refreshBackoffMs,
        metadataExpireMs = metadataExpireMs,
        logContext = LogContext(),
        clusterResourceListeners = ClusterResourceListeners(),
    )

    @Test
    fun testMetadataUpdateAfterClose() {
        metadata.close()
        assertFailsWith<IllegalStateException> {
            metadata.updateWithCurrentRequestVersion(
                response = emptyMetadataResponse(),
                isPartialUpdate = false,
                nowMs = 1000,
            )
        }
    }

    @Test
    fun testUpdateMetadataAllowedImmediatelyAfterBootstrap() {
        val time = MockTime()
        val metadata = Metadata(
            refreshBackoffMs = refreshBackoffMs,
            metadataExpireMs = metadataExpireMs,
            logContext = LogContext(),
            clusterResourceListeners = ClusterResourceListeners(),
        )
        metadata.bootstrap(listOf(InetSocketAddress("localhost", 9002)))
        assertEquals(expected = 0, actual = metadata.timeToAllowUpdate(time.milliseconds()))
        assertEquals(expected = 0, actual = metadata.timeToNextUpdate(time.milliseconds()))
    }

    @Test
    fun testTimeToNextUpdate() {
        checkTimeToNextUpdate(refreshBackoffMs = 100, metadataExpireMs = 1000)
        checkTimeToNextUpdate(refreshBackoffMs = 1000, metadataExpireMs = 100)
        checkTimeToNextUpdate(refreshBackoffMs = 0, metadataExpireMs = 0)
        checkTimeToNextUpdate(refreshBackoffMs = 0, metadataExpireMs = 100)
        checkTimeToNextUpdate(refreshBackoffMs = 100, metadataExpireMs = 0)
    }

    @Test
    fun testTimeToNextUpdateRetryBackoff() {
        var now = 10000L

        // lastRefreshMs updated to now.
        metadata.failedUpdate(now)

        // Backing off. Remaining time until next try should be returned.
        assertEquals(refreshBackoffMs, metadata.timeToNextUpdate(now))

        // Even though metadata update requested explicitly, still respects backoff.
        metadata.requestUpdate()
        assertEquals(refreshBackoffMs, metadata.timeToNextUpdate(now))

        // refreshBackoffMs elapsed.
        now += refreshBackoffMs
        // It should return 0 to let next try.
        assertEquals(expected = 0, actual = metadata.timeToNextUpdate(now))
        assertEquals(expected = 0, actual = metadata.timeToNextUpdate(now + 1))
    }

    /**
     * Prior to Kafka version 2.4 (which coincides with Metadata version 9), the broker does not
     * propagate leader epoch information accurately while a reassignment is in progress, so we
     * cannot rely on it. This is explained in more detail in MetadataResponse's constructor.
     */
    @Test
    fun testIgnoreLeaderEpochInOlderMetadataResponse() {
        val tp = TopicPartition("topic", 0)
        val partitionMetadata = MetadataResponsePartition()
            .setPartitionIndex(tp.partition)
            .setLeaderId(5)
            .setLeaderEpoch(10)
            .setReplicaNodes(intArrayOf(1, 2, 3))
            .setIsrNodes(intArrayOf(1, 2, 3))
            .setOfflineReplicas(intArrayOf())
            .setErrorCode(Errors.NONE.code)
        val topicMetadata = MetadataResponseTopic()
            .setName(tp.topic)
            .setErrorCode(Errors.NONE.code)
            .setPartitions(listOf(partitionMetadata))
            .setIsInternal(false)
        val topics = MetadataResponseTopicCollection()
        topics.add(topicMetadata)
        val data = MetadataResponseData()
            .setClusterId("clusterId")
            .setControllerId(0)
            .setTopics(topics)
            .setBrokers(MetadataResponseBrokerCollection())
        for (version in ApiKeys.METADATA.oldestVersion()..8) {
            val buffer = toByteBuffer(data, version.toShort())
            val response = MetadataResponse.parse(buffer, version.toShort())
            assertFalse(response.hasReliableLeaderEpochs())
            metadata.updateWithCurrentRequestVersion(response, false, 100)
            val pm = metadata.partitionMetadataIfCurrent(tp)!!
            assertNotNull(pm)
            val (_, _, _, leaderEpoch) = pm
            assertEquals(expected = null, actual = leaderEpoch)
        }
        for (version in 9..ApiKeys.METADATA.latestVersion()) {
            val buffer = toByteBuffer(data, version.toShort())
            val response = MetadataResponse.parse(buffer, version.toShort())
            assertTrue(response.hasReliableLeaderEpochs())
            metadata.updateWithCurrentRequestVersion(response, false, 100)
            val pm = metadata.partitionMetadataIfCurrent(tp)
            assertNotNull(pm)
            val (_, _, _, leaderEpoch) = pm
            assertEquals(expected = 10, actual = leaderEpoch)
        }
    }

    @Test
    fun testStaleMetadata() {
        val tp = TopicPartition("topic", 0)
        val partitionMetadata = MetadataResponsePartition()
            .setPartitionIndex(tp.partition)
            .setLeaderId(1)
            .setLeaderEpoch(10)
            .setReplicaNodes(intArrayOf(1, 2, 3))
            .setIsrNodes(intArrayOf(1, 2, 3))
            .setOfflineReplicas(intArrayOf())
            .setErrorCode(Errors.NONE.code)
        val topicMetadata = MetadataResponseTopic()
            .setName(tp.topic)
            .setErrorCode(Errors.NONE.code)
            .setPartitions(listOf(partitionMetadata))
            .setIsInternal(false)
        val topics = MetadataResponseTopicCollection()
        topics.add(topicMetadata)
        val data = MetadataResponseData()
            .setClusterId("clusterId")
            .setControllerId(0)
            .setTopics(topics)
            .setBrokers(MetadataResponseBrokerCollection())
        metadata.updateWithCurrentRequestVersion(
            response = MetadataResponse(data = data, version = ApiKeys.METADATA.latestVersion()),
            isPartialUpdate = false,
            nowMs = 100,
        )

        // Older epoch with changed ISR should be ignored
        partitionMetadata
            .setPartitionIndex(tp.partition)
            .setLeaderId(1)
            .setLeaderEpoch(9)
            .setReplicaNodes(intArrayOf(1, 2, 3))
            .setIsrNodes(intArrayOf(1, 2))
            .setOfflineReplicas(intArrayOf())
            .setErrorCode(Errors.NONE.code)
        metadata.updateWithCurrentRequestVersion(
            response = MetadataResponse(data, ApiKeys.METADATA.latestVersion()),
            isPartialUpdate = false,
            nowMs = 101,
        )
        assertEquals(expected = 10, actual = metadata.lastSeenLeaderEpochs[tp])
        val pm = metadata.partitionMetadataIfCurrent(tp)
        assertNotNull(pm)
        val (_, _, _, leaderEpoch, _, inSyncReplicaIds) = pm
        assertEquals(expected = mutableListOf(1, 2, 3), actual = inSyncReplicaIds)
        assertEquals(expected = 10, actual = leaderEpoch)
    }

    @Test
    fun testFailedUpdate() {
        val time = 100L
        metadata.updateWithCurrentRequestVersion(
            response = emptyMetadataResponse(),
            isPartialUpdate = false,
            nowMs = time,
        )
        assertEquals(expected = 100, actual = metadata.timeToNextUpdate(nowMs = 1000))
        metadata.failedUpdate(now = 1100)
        assertEquals(expected = 100, actual = metadata.timeToNextUpdate(nowMs = 1100))
        assertEquals(expected = 100, actual = metadata.lastSuccessfulUpdate())
        metadata.updateWithCurrentRequestVersion(
            response = emptyMetadataResponse(),
            isPartialUpdate = false,
            nowMs = time,
        )
        assertEquals(expected = 100, actual = metadata.timeToNextUpdate(nowMs = 1000))
    }

    @Test
    fun testClusterListenerGetsNotifiedOfUpdate() {
        val mockClusterListener = MockClusterResourceListener()
        val listeners = ClusterResourceListeners()
        listeners.maybeAdd(mockClusterListener)
        metadata = Metadata(
            refreshBackoffMs = refreshBackoffMs,
            metadataExpireMs = metadataExpireMs,
            logContext = LogContext(),
            clusterResourceListeners = listeners,
        )
        val hostName = "www.example.com"
        metadata.bootstrap(listOf(InetSocketAddress(hostName, 9002)))
        assertFalse(
            MockClusterResourceListener.IS_ON_UPDATE_CALLED.get(),
            "ClusterResourceListener should not called when metadata is updated with bootstrap Cluster"
        )
        val partitionCounts: MutableMap<String, Int> = HashMap()
        partitionCounts["topic"] = 1
        partitionCounts["topic1"] = 1
        val metadataResponse = metadataUpdateWith(
            clusterId = "dummy",
            numNodes = 1,
            topicPartitionCounts = partitionCounts,
        )
        metadata.updateWithCurrentRequestVersion(
            response = metadataResponse,
            isPartialUpdate = false,
            nowMs = 100,
        )
        assertEquals(
            expected = "dummy",
            actual = mockClusterListener.clusterResource!!.clusterId,
            message = "MockClusterResourceListener did not get cluster metadata correctly"
        )
        assertTrue(
            MockClusterResourceListener.IS_ON_UPDATE_CALLED.get(),
            "MockClusterResourceListener should be called when metadata is updated with non-bootstrap Cluster"
        )
    }

    @Test
    fun testRequestUpdate() {
        assertFalse(metadata.updateRequested())
        val epochs = intArrayOf(42, 42, 41, 41, 42, 43, 43, 42, 41, 44)
        val updateResult =
            booleanArrayOf(true, false, false, false, false, true, false, false, false, true)
        val tp = TopicPartition("topic", 0)
        val metadataResponse = metadataUpdateWith(
            clusterId = "dummy",
            numNodes = 1,
            topicErrors = emptyMap(),
            topicPartitionCounts = mapOf("topic" to 1),
            epochSupplier = { 0 },
        )
        metadata.updateWithCurrentRequestVersion(
            response = metadataResponse,
            isPartialUpdate = false,
            nowMs = 10L,
        )
        for (i in epochs.indices) {
            metadata.updateLastSeenEpochIfNewer(tp, epochs[i])
            if (updateResult[i]) assertTrue(
                metadata.updateRequested(),
                "Expected metadata update to be requested [$i]"
            ) else assertFalse(
                metadata.updateRequested(),
                "Did not expect metadata update to be requested [$i]"
            )
            metadata.updateWithCurrentRequestVersion(
                response = emptyMetadataResponse(),
                isPartialUpdate = false,
                nowMs = 0L,
            )
            assertFalse(metadata.updateRequested())
        }
    }

    @Test
    fun testUpdateLastEpoch() {
        val tp = TopicPartition(topic = "topic-1", partition = 0)
        var metadataResponse = emptyMetadataResponse()
        metadata.updateWithCurrentRequestVersion(
            response = metadataResponse,
            isPartialUpdate = false,
            nowMs = 0L,
        )

        // if we have no leader epoch, this call shouldn't do anything
        assertFalse(metadata.updateLastSeenEpochIfNewer(tp, 0))
        assertFalse(metadata.updateLastSeenEpochIfNewer(tp, 1))
        assertFalse(metadata.updateLastSeenEpochIfNewer(tp, 2))
        assertNull(metadata.lastSeenLeaderEpochs[tp])

        // Metadata with newer epoch is handled
        metadataResponse = metadataUpdateWith(
            clusterId = "dummy",
            numNodes = 1,
            topicErrors = emptyMap(),
            topicPartitionCounts = mapOf("topic-1" to 1),
            epochSupplier = { 10 },
        )
        metadata.updateWithCurrentRequestVersion(metadataResponse, false, 1L)
        assertNullable(metadata.lastSeenLeaderEpochs[tp]) { leaderAndEpoch ->
            assertEquals(expected = leaderAndEpoch, actual = 10)
        }

        // Don't update to an older one
        assertFalse(metadata.updateLastSeenEpochIfNewer(tp, 1))
        assertNullable(metadata.lastSeenLeaderEpochs[tp]) { leaderAndEpoch ->
            assertEquals(expected = leaderAndEpoch, actual = 10)
        }

        // Don't cause update if it's the same one
        assertFalse(metadata.updateLastSeenEpochIfNewer(tp, 10))
        assertNullable(metadata.lastSeenLeaderEpochs[tp]) { leaderAndEpoch ->
            assertEquals(expected = leaderAndEpoch, actual = 10)
        }

        // Update if we see newer epoch
        assertTrue(metadata.updateLastSeenEpochIfNewer(tp, 12))
        assertNullable(metadata.lastSeenLeaderEpochs[tp]) { leaderAndEpoch ->
            assertEquals(expected = leaderAndEpoch, actual = 12)
        }
        metadataResponse = metadataUpdateWith(
            clusterId = "dummy",
            numNodes = 1,
            topicErrors = emptyMap(),
            topicPartitionCounts = mapOf("topic-1" to 1),
            epochSupplier = { 12 },
        )
        metadata.updateWithCurrentRequestVersion(metadataResponse, false, 2L)
        assertNullable(metadata.lastSeenLeaderEpochs[tp]) { leaderAndEpoch ->
            assertEquals(expected = leaderAndEpoch, actual = 12)
        }

        // Don't overwrite metadata with older epoch
        metadataResponse = metadataUpdateWith(
            clusterId = "dummy",
            numNodes = 1,
            topicErrors = emptyMap(),
            topicPartitionCounts = mapOf("topic-1" to 1),
            epochSupplier = { 11 },
        )
        metadata.updateWithCurrentRequestVersion(metadataResponse, false, 3L)
        assertNullable(metadata.lastSeenLeaderEpochs[tp]) { leaderAndEpoch ->
            assertEquals(expected = leaderAndEpoch, actual = 12)
        }
    }

    @Test
    fun testEpochUpdateAfterTopicDeletion() {
        val tp = TopicPartition("topic-1", 0)
        var metadataResponse = emptyMetadataResponse()
        metadata.updateWithCurrentRequestVersion(
            response = metadataResponse,
            isPartialUpdate = false,
            nowMs = 0L,
        )

        // Start with a Topic topic-1 with a random topic ID
        val topicIds = mapOf("topic-1" to Uuid.randomUuid())
        metadataResponse = metadataUpdateWithIds(
            clusterId = "dummy",
            numNodes = 1,
            topicErrors = emptyMap(),
            topicPartitionCounts = mapOf("topic-1" to 1),
            epochSupplier = { 10 },
            topicIds = topicIds,
        )
        metadata.updateWithCurrentRequestVersion(metadataResponse, false, 1L)
        assertEquals(expected = 10, actual = metadata.lastSeenLeaderEpochs[tp])

        // Topic topic-1 is now deleted so Response contains an Error. LeaderEpoch should still maintain Old value
        metadataResponse = metadataUpdateWith(
            clusterId = "dummy",
            numNodes = 1,
            topicErrors = mapOf("topic-1" to Errors.UNKNOWN_TOPIC_OR_PARTITION),
            topicPartitionCounts = emptyMap(),
        )
        metadata.updateWithCurrentRequestVersion(metadataResponse, false, 1L)
        assertEquals(expected = 10, actual = metadata.lastSeenLeaderEpochs[tp])

        // Create topic-1 again but this time with a different topic ID. LeaderEpoch should be updated to new even if lower.
        val newTopicIds = mapOf("topic-1" to Uuid.randomUuid())
        metadataResponse = metadataUpdateWithIds(
            clusterId = "dummy",
            numNodes = 1,
            topicErrors = emptyMap(),
            topicPartitionCounts = mapOf("topic-1" to 1),
            epochSupplier = { 5 },
            topicIds = newTopicIds,
        )
        metadata.updateWithCurrentRequestVersion(
            response = metadataResponse,
            isPartialUpdate = false,
            nowMs = 1,
        )
        assertEquals(expected = 5, actual = metadata.lastSeenLeaderEpochs[tp])
    }

    @Test
    fun testEpochUpdateOnChangedTopicIds() {
        val tp = TopicPartition("topic-1", 0)
        val topicIds = mapOf("topic-1" to Uuid.randomUuid())
        var metadataResponse = emptyMetadataResponse()
        metadata.updateWithCurrentRequestVersion(
            response = metadataResponse,
            isPartialUpdate = false,
            nowMs = 0,
        )

        // Start with a topic with no topic ID
        metadataResponse = metadataUpdateWith(
            clusterId = "dummy",
            numNodes = 1,
            topicErrors = emptyMap(),
            topicPartitionCounts = mapOf("topic-1" to 1),
            epochSupplier = { 100 },
        )
        metadata.updateWithCurrentRequestVersion(metadataResponse, false, 1L)
        assertEquals(expected = 100, actual = metadata.lastSeenLeaderEpochs[tp])

        // If the older topic ID is null, we should go with the new topic ID as the leader epoch
        metadataResponse = metadataUpdateWithIds(
            clusterId = "dummy",
            numNodes = 1,
            topicErrors = emptyMap(),
            topicPartitionCounts = mapOf("topic-1" to 1),
            epochSupplier = { 10 },
            topicIds = topicIds,
        )
        metadata.updateWithCurrentRequestVersion(
            response = metadataResponse,
            isPartialUpdate = false,
            nowMs = 2,
        )
        assertEquals(expected = 10, actual = metadata.lastSeenLeaderEpochs[tp])

        // Don't cause update if it's the same one
        metadataResponse = metadataUpdateWithIds(
            clusterId = "dummy",
            numNodes = 1,
            topicErrors = emptyMap(),
            topicPartitionCounts = mapOf("topic-1" to 1),
            epochSupplier = { 10 },
            topicIds = topicIds,
        )
        metadata.updateWithCurrentRequestVersion(
            response = metadataResponse,
            isPartialUpdate = false,
            nowMs = 3,
        )
        assertEquals(expected = 10, actual = metadata.lastSeenLeaderEpochs[tp])

        // Update if we see newer epoch
        metadataResponse = metadataUpdateWithIds(
            clusterId = "dummy",
            numNodes = 1,
            topicErrors = emptyMap(),
            topicPartitionCounts = mapOf("topic-1" to 1),
            epochSupplier = { 12 },
            topicIds = topicIds,
        )
        metadata.updateWithCurrentRequestVersion(
            response = metadataResponse,
            isPartialUpdate = false,
            nowMs = 4,
        )
        assertEquals(expected = 12, actual = metadata.lastSeenLeaderEpochs[tp])

        // We should also update if we see a new topicId even if the epoch is lower
        val newTopicIds = mapOf("topic-1" to Uuid.randomUuid())
        metadataResponse = metadataUpdateWithIds(
            clusterId = "dummy",
            numNodes = 1,
            topicErrors = emptyMap(),
            topicPartitionCounts = mapOf("topic-1" to 1),
            epochSupplier = { 3 },
            topicIds = newTopicIds,
        )
        metadata.updateWithCurrentRequestVersion(
            response = metadataResponse,
            isPartialUpdate = false,
            nowMs = 5,
        )
        assertEquals(expected = 3, actual = metadata.lastSeenLeaderEpochs[tp])

        // Finally, update when the topic ID is new and the epoch is higher
        val newTopicIds2 = mapOf("topic-1" to Uuid.randomUuid())
        metadataResponse = metadataUpdateWithIds(
            clusterId = "dummy",
            numNodes = 1,
            topicErrors = emptyMap(),
            topicPartitionCounts = mapOf("topic-1" to 1),
            epochSupplier = { 20 },
            topicIds = newTopicIds2,
        )
        metadata.updateWithCurrentRequestVersion(
            response = metadataResponse,
            isPartialUpdate = false,
            nowMs = 6,
        )
        assertEquals(expected = 20, actual = metadata.lastSeenLeaderEpochs[tp])
    }

    @Test
    fun testRejectOldMetadata() {
        val partitionCounts: MutableMap<String, Int> = HashMap()
        partitionCounts["topic-1"] = 1
        val tp = TopicPartition("topic-1", 0)
        metadata.updateWithCurrentRequestVersion(emptyMetadataResponse(), false, 0L)

        // First epoch seen, accept it
        run {
            val metadataResponse = metadataUpdateWith(
                clusterId = "dummy",
                numNodes = 1,
                topicErrors = emptyMap(),
                topicPartitionCounts = partitionCounts,
                epochSupplier = { 100 },
            )
            metadata.updateWithCurrentRequestVersion(
                response = metadataResponse,
                isPartialUpdate = false,
                nowMs = 10,
            )
            assertNotNull(metadata.fetch().partition(tp))
            assertNotNull(metadata.lastSeenLeaderEpochs[tp])
            assertEquals(expected = 100, actual = metadata.lastSeenLeaderEpochs[tp])
        }

        // Fake an empty ISR, but with an older epoch, should reject it
        run {
            val metadataResponse = metadataUpdateWith(
                clusterId = "dummy",
                numNodes = 1,
                topicErrors = emptyMap(),
                topicPartitionCounts = partitionCounts,
                epochSupplier = { 99 },
                partitionSupplier = { error, partition, leader, leaderEpoch, replicas, _, offlineReplicas ->
                    PartitionMetadata(
                        error = error,
                        topicPartition = partition,
                        leaderId = leader,
                        leaderEpoch = leaderEpoch,
                        replicaIds = replicas,
                        inSyncReplicaIds = emptyList(),
                        offlineReplicaIds = offlineReplicas,
                    )
                },
                responseVersion = ApiKeys.METADATA.latestVersion(),
                topicIds = emptyMap(),
            )
            metadata.updateWithCurrentRequestVersion(
                response = metadataResponse,
                isPartialUpdate = false,
                nowMs = 20,
            )
            assertEquals(
                expected = 1,
                actual = metadata.fetch().partition(tp)!!.inSyncReplicas.size,
            )
            assertEquals(expected = 100, actual = metadata.lastSeenLeaderEpochs[tp])
        }

        // Fake an empty ISR, with same epoch, accept it
        run {
            val metadataResponse = metadataUpdateWith(
                clusterId = "dummy",
                numNodes = 1,
                topicErrors = emptyMap(),
                topicPartitionCounts = partitionCounts,
                epochSupplier = { 100 },
                partitionSupplier = { error, partition, leader, leaderEpoch, replicas, _, offlineReplicas ->
                    PartitionMetadata(
                        error = error,
                        topicPartition = partition,
                        leaderId = leader,
                        leaderEpoch = leaderEpoch,
                        replicaIds = replicas,
                        inSyncReplicaIds = emptyList(),
                        offlineReplicaIds = offlineReplicas,
                    )
                },
                responseVersion = ApiKeys.METADATA.latestVersion(),
                topicIds = emptyMap()
            )
            metadata.updateWithCurrentRequestVersion(
                response = metadataResponse,
                isPartialUpdate = false,
                nowMs = 20,
            )
            assertEquals(
                expected = 0,
                actual = metadata.fetch().partition(tp)!!.inSyncReplicas.size,
            )
            assertEquals(expected = 100, actual = metadata.lastSeenLeaderEpochs[tp])
        }

        // Empty metadata response, should not keep old partition but should keep the last-seen epoch
        run {
            val metadataResponse = metadataUpdateWith(
                clusterId = "dummy",
                numNodes = 1,
                topicErrors = emptyMap(),
                topicPartitionCounts = emptyMap(),
            )
            metadata.updateWithCurrentRequestVersion(
                response = metadataResponse,
                isPartialUpdate = false,
                nowMs = 20,
            )
            assertNull(metadata.fetch().partition(tp))
            assertEquals(expected = 100, actual = metadata.lastSeenLeaderEpochs[tp])
        }

        // Back in the metadata, with old epoch, should not get added
        run {
            val metadataResponse = metadataUpdateWith(
                clusterId = "dummy",
                numNodes = 1,
                topicErrors = emptyMap(),
                topicPartitionCounts = partitionCounts,
                epochSupplier = { 99 },
            )
            metadata.updateWithCurrentRequestVersion(
                response = metadataResponse,
                isPartialUpdate = false,
                nowMs = 10,
            )
            assertNull(metadata.fetch().partition(tp))
            assertEquals(expected = 100, actual = metadata.lastSeenLeaderEpochs[tp])
        }
    }

    @Test
    fun testOutOfBandEpochUpdate() {
        val partitionCounts: MutableMap<String, Int> = HashMap()
        partitionCounts["topic-1"] = 5
        val tp = TopicPartition(topic = "topic-1", partition = 0)
        metadata.updateWithCurrentRequestVersion(
            response = emptyMetadataResponse(),
            isPartialUpdate = false,
            nowMs = 0,
        )
        assertFalse(metadata.updateLastSeenEpochIfNewer(topicPartition = tp, leaderEpoch = 99))

        // Update epoch to 100
        var metadataResponse = metadataUpdateWith(
            clusterId = "dummy",
            numNodes = 1,
            topicErrors = emptyMap(),
            topicPartitionCounts = partitionCounts,
            epochSupplier = { 100 },
        )
        metadata.updateWithCurrentRequestVersion(
            response = metadataResponse,
            isPartialUpdate = false,
            nowMs = 10,
        )
        assertNotNull(metadata.fetch().partition(tp))
        assertNotNull(metadata.lastSeenLeaderEpochs[tp])
        assertEquals(expected = 100, actual = metadata.lastSeenLeaderEpochs[tp])

        // Simulate a leader epoch from another response, like a fetch response or list offsets
        assertTrue(metadata.updateLastSeenEpochIfNewer(tp, 101))

        // Cache of partition stays, but current partition info is not available since it's stale
        assertNotNull(metadata.fetch().partition(tp))
        assertEquals(
            expected = 5,
            actual = metadata.fetch().partitionCountForTopic(topic = "topic-1")
        )
        assertNull(metadata.partitionMetadataIfCurrent(tp))
        assertEquals(expected = 101, actual = metadata.lastSeenLeaderEpochs[tp])

        // Metadata with older epoch is rejected, metadata state is unchanged
        metadata.updateWithCurrentRequestVersion(
            response = metadataResponse,
            isPartialUpdate = false,
            nowMs = 20,
        )
        assertNotNull(metadata.fetch().partition(tp))
        assertEquals(
            expected = 5,
            actual = metadata.fetch().partitionCountForTopic(topic = "topic-1")
        )
        assertNull(metadata.partitionMetadataIfCurrent(tp))
        assertEquals(expected = 101, actual = metadata.lastSeenLeaderEpochs[tp])

        // Metadata with equal or newer epoch is accepted
        metadataResponse = metadataUpdateWith(
            clusterId = "dummy",
            numNodes = 1,
            topicErrors = emptyMap(),
            topicPartitionCounts = partitionCounts,
            epochSupplier = { 101 },
        )
        metadata.updateWithCurrentRequestVersion(
            response = metadataResponse,
            isPartialUpdate = false,
            nowMs = 30,
        )
        assertNotNull(metadata.fetch().partition(tp))
        assertEquals(
            expected = 5,
            actual = metadata.fetch().partitionCountForTopic(topic = "topic-1"),
        )
        assertNotNull(metadata.partitionMetadataIfCurrent(tp))
        assertEquals(expected = 101, actual = metadata.lastSeenLeaderEpochs[tp])
    }

    @Test
    fun testNoEpoch() {
        metadata.updateWithCurrentRequestVersion(
            response = emptyMetadataResponse(),
            isPartialUpdate = false,
            nowMs = 0,
        )
        val metadataResponse = metadataUpdateWith(
            clusterId = "dummy",
            numNodes = 1,
            topicErrors = emptyMap(),
            topicPartitionCounts = mapOf("topic-1" to 1),
        )
        metadata.updateWithCurrentRequestVersion(
            response = metadataResponse,
            isPartialUpdate = false,
            nowMs = 10,
        )
        val tp = TopicPartition(topic = "topic-1", partition = 0)

        // no epoch
        assertNull(metadata.lastSeenLeaderEpochs[tp])

        // still works
        val pm = metadata.partitionMetadataIfCurrent(tp)
        assertNotNull(pm)
        assertEquals(expected = 0, actual = pm.partition)
        assertEquals(expected = 0, actual = pm.leaderId)

        // Since epoch was null, this shouldn't update it
        metadata.updateLastSeenEpochIfNewer(topicPartition = tp, leaderEpoch = 10)
        assertNotNull(metadata.partitionMetadataIfCurrent(tp))
        assertNull(metadata.partitionMetadataIfCurrent(tp)!!.leaderEpoch)
    }

    @Test
    fun testClusterCopy() {
        val counts = mapOf(
            "topic1" to 2,
            "topic2" to 3,
            Topic.GROUP_METADATA_TOPIC_NAME to 3,
        )
        val errors = mapOf(
            "topic3" to Errors.INVALID_TOPIC_EXCEPTION,
            "topic4" to Errors.TOPIC_AUTHORIZATION_FAILED,
        )
        val metadataResponse = metadataUpdateWith(
            clusterId = "dummy",
            numNodes = 4,
            topicErrors = errors,
            topicPartitionCounts = counts,
        )
        metadata.updateWithCurrentRequestVersion(
            response = metadataResponse,
            isPartialUpdate = false,
            nowMs = 0,
        )
        val cluster = metadata.fetch()
        assertEquals(expected = "dummy", actual = cluster.clusterResource.clusterId)
        assertEquals(expected = 4, actual = cluster.nodes.size)

        // topic counts
        assertEquals(expected = setOf("topic3"), actual = cluster.invalidTopics)
        assertEquals(expected = setOf("topic4"), actual = cluster.unauthorizedTopics)
        assertEquals(expected = 3, actual = cluster.topics.size)
        assertEquals(
            expected = setOf(Topic.GROUP_METADATA_TOPIC_NAME),
            actual = cluster.internalTopics,
        )

        // partition counts
        assertEquals(expected = 2, actual = cluster.partitionsForTopic("topic1").size)
        assertEquals(expected = 3, actual = cluster.partitionsForTopic("topic2").size)

        // Sentinel instances
        val address = InetSocketAddress.createUnresolved("localhost", 0)
        val fromMetadata = MetadataCache.bootstrap(listOf(address)).cluster()
        val fromCluster = Cluster.bootstrap(listOf(address))
        assertEquals(expected = fromMetadata, actual = fromCluster)
        val fromMetadataEmpty = MetadataCache.empty().cluster()
        val fromClusterEmpty = Cluster.empty()
        assertEquals(expected = fromMetadataEmpty, actual = fromClusterEmpty)
    }

    @Test
    fun testRequestVersion() {
        val time = MockTime()
        metadata.requestUpdate()
        var versionAndBuilder = metadata.newMetadataRequestAndVersion(time.milliseconds())
        metadata.update(
            requestVersion = versionAndBuilder.requestVersion,
            response = metadataUpdateWith(
                numNodes = 1,
                topicPartitionCounts = mapOf("topic" to 1)
            ),
            isPartialUpdate = false,
            nowMs = time.milliseconds(),
        )
        assertFalse(metadata.updateRequested())

        // bump the request version for new topics added to the metadata
        metadata.requestUpdateForNewTopics()

        // simulating a bump while a metadata request is in flight
        versionAndBuilder = metadata.newMetadataRequestAndVersion(time.milliseconds())
        metadata.requestUpdateForNewTopics()
        metadata.update(
            requestVersion = versionAndBuilder.requestVersion,
            response = metadataUpdateWith(
                numNodes = 1,
                topicPartitionCounts = mapOf("topic" to 1),
            ),
            isPartialUpdate = true,
            nowMs = time.milliseconds(),
        )

        // metadata update is still needed
        assertTrue(metadata.updateRequested())

        // the next update will resolve it
        versionAndBuilder = metadata.newMetadataRequestAndVersion(time.milliseconds())
        metadata.update(
            requestVersion = versionAndBuilder.requestVersion,
            response = metadataUpdateWith(
                numNodes = 1,
                topicPartitionCounts = mapOf("topic" to 1)
            ),
            isPartialUpdate = true,
            nowMs = time.milliseconds(),
        )
        assertFalse(metadata.updateRequested())
    }

    @Test
    fun testPartialMetadataUpdate() {
        val time = MockTime()
        metadata = object : Metadata(
            refreshBackoffMs = refreshBackoffMs,
            metadataExpireMs = metadataExpireMs,
            logContext = LogContext(),
            clusterResourceListeners = ClusterResourceListeners(),
        ) {
            override fun newMetadataRequestBuilderForNewTopics(): MetadataRequest.Builder {
                return newMetadataRequestBuilder()
            }
        }
        assertFalse(metadata.updateRequested())

        // Request a metadata update. This must force a full metadata update request.
        metadata.requestUpdate()
        var versionAndBuilder = metadata.newMetadataRequestAndVersion(time.milliseconds())
        assertFalse(versionAndBuilder.isPartialUpdate)
        metadata.update(
            requestVersion = versionAndBuilder.requestVersion,
            response = metadataUpdateWith(
                numNodes = 1,
                topicPartitionCounts = mapOf("topic" to 1),
            ),
            isPartialUpdate = false,
            nowMs = time.milliseconds(),
        )
        assertFalse(metadata.updateRequested())

        // Request a metadata update for a new topic. This should perform a partial metadata update.
        metadata.requestUpdateForNewTopics()
        versionAndBuilder = metadata.newMetadataRequestAndVersion(time.milliseconds())
        assertTrue(versionAndBuilder.isPartialUpdate)
        metadata.update(
            requestVersion = versionAndBuilder.requestVersion,
            response = metadataUpdateWith(
                numNodes = 1,
                topicPartitionCounts = mapOf("topic" to 1)
            ),
            isPartialUpdate = true,
            nowMs = time.milliseconds(),
        )
        assertFalse(metadata.updateRequested())

        // Request both types of metadata updates. This should always perform a full update.
        metadata.requestUpdate()
        metadata.requestUpdateForNewTopics()
        versionAndBuilder = metadata.newMetadataRequestAndVersion(time.milliseconds())
        assertFalse(versionAndBuilder.isPartialUpdate)
        metadata.update(
            requestVersion = versionAndBuilder.requestVersion,
            response = metadataUpdateWith(
                numNodes = 1,
                topicPartitionCounts = mapOf("topic" to 1)
            ),
            isPartialUpdate = false,
            nowMs = time.milliseconds(),
        )
        assertFalse(metadata.updateRequested())

        // Request only a partial metadata update, but elapse enough time such that a full refresh is needed.
        metadata.requestUpdateForNewTopics()
        val refreshTimeMs = time.milliseconds() + metadata.metadataExpireMs()
        versionAndBuilder = metadata.newMetadataRequestAndVersion(refreshTimeMs)
        assertFalse(versionAndBuilder.isPartialUpdate)
        metadata.update(
            requestVersion = versionAndBuilder.requestVersion,
            response = metadataUpdateWith(
                numNodes = 1,
                topicPartitionCounts = mapOf("topic" to 1),
            ),
            isPartialUpdate = true,
            nowMs = refreshTimeMs,
        )
        assertFalse(metadata.updateRequested())

        // Request two partial metadata updates that are overlapping.
        metadata.requestUpdateForNewTopics()
        versionAndBuilder = metadata.newMetadataRequestAndVersion(time.milliseconds())
        assertTrue(versionAndBuilder.isPartialUpdate)
        metadata.requestUpdateForNewTopics()
        val overlappingVersionAndBuilder =
            metadata.newMetadataRequestAndVersion(time.milliseconds())
        assertTrue(overlappingVersionAndBuilder.isPartialUpdate)
        assertTrue(metadata.updateRequested())
        metadata.update(
            requestVersion = versionAndBuilder.requestVersion,
            response = metadataUpdateWith(
                numNodes = 1,
                topicPartitionCounts = mapOf("topic-1" to 1),
            ),
            isPartialUpdate = true,
            nowMs = time.milliseconds(),
        )
        assertTrue(metadata.updateRequested())
        metadata.update(
            requestVersion = overlappingVersionAndBuilder.requestVersion,
            response = metadataUpdateWith(
                numNodes = 1,
                topicPartitionCounts = mapOf("topic-2" to 1),
            ),
            isPartialUpdate = true,
            nowMs = time.milliseconds(),
        )
        assertFalse(metadata.updateRequested())
    }

    @Test
    fun testInvalidTopicError() {
        val time = MockTime()
        val invalidTopic = "topic dfsa"
        val invalidTopicResponse = metadataUpdateWith(
            clusterId = "clusterId",
            numNodes = 1,
            topicErrors = mapOf(invalidTopic to Errors.INVALID_TOPIC_EXCEPTION),
            topicPartitionCounts = emptyMap(),
        )
        metadata.updateWithCurrentRequestVersion(
            response = invalidTopicResponse,
            isPartialUpdate = false,
            nowMs = time.milliseconds(),
        )
        val e = assertFailsWith<InvalidTopicException> { metadata.maybeThrowAnyException() }
        assertEquals(expected = setOf(invalidTopic), actual = e.invalidTopics)
        // We clear the exception once it has been raised to the user
        metadata.maybeThrowAnyException()

        // Reset the invalid topic error
        metadata.updateWithCurrentRequestVersion(
            response = invalidTopicResponse,
            isPartialUpdate = false,
            nowMs = time.milliseconds(),
        )

        // If we get a good update, the error should clear even if we haven't had a chance to raise it to the user
        metadata.updateWithCurrentRequestVersion(
            response = emptyMetadataResponse(),
            isPartialUpdate = false,
            nowMs = time.milliseconds(),
        )
        metadata.maybeThrowAnyException()
    }

    @Test
    fun testTopicAuthorizationError() {
        val time = MockTime()
        val invalidTopic = "foo"
        val unauthorizedTopicResponse = metadataUpdateWith(
            clusterId = "clusterId",
            numNodes = 1,
            topicErrors = mapOf(invalidTopic to Errors.TOPIC_AUTHORIZATION_FAILED),
            topicPartitionCounts = emptyMap(),
        )
        metadata.updateWithCurrentRequestVersion(
            response = unauthorizedTopicResponse,
            isPartialUpdate = false,
            nowMs = time.milliseconds(),
        )
        val e = assertFailsWith<TopicAuthorizationException> { metadata.maybeThrowAnyException() }
        assertEquals(expected = setOf(invalidTopic), actual = e.unauthorizedTopics)
        // We clear the exception once it has been raised to the user
        metadata.maybeThrowAnyException()

        // Reset the unauthorized topic error
        metadata.updateWithCurrentRequestVersion(
            response = unauthorizedTopicResponse,
            isPartialUpdate = false,
            nowMs = time.milliseconds(),
        )

        // If we get a good update, the error should clear even if we haven't had a chance to raise it to the user
        metadata.updateWithCurrentRequestVersion(
            response = emptyMetadataResponse(),
            isPartialUpdate = false,
            nowMs = time.milliseconds(),
        )
        metadata.maybeThrowAnyException()
    }

    @Test
    fun testMetadataTopicErrors() {
        val time = MockTime()
        val topicErrors = mapOf(
            "invalidTopic" to Errors.INVALID_TOPIC_EXCEPTION,
            "sensitiveTopic1" to Errors.TOPIC_AUTHORIZATION_FAILED,
            "sensitiveTopic2" to Errors.TOPIC_AUTHORIZATION_FAILED,
        )
        val metadataResponse = metadataUpdateWith(
            clusterId = "clusterId",
            numNodes = 1,
            topicErrors = topicErrors,
            topicPartitionCounts = emptyMap(),
        )
        metadata.updateWithCurrentRequestVersion(
            response = metadataResponse,
            isPartialUpdate = false,
            nowMs = time.milliseconds(),
        )
        val e1 = assertFailsWith<TopicAuthorizationException> {
            metadata.maybeThrowExceptionForTopic("sensitiveTopic1")
        }
        assertEquals(expected = setOf("sensitiveTopic1"), actual = e1.unauthorizedTopics)
        // We clear the exception once it has been raised to the user
        metadata.maybeThrowAnyException()
        metadata.updateWithCurrentRequestVersion(
            response = metadataResponse,
            isPartialUpdate = false,
            nowMs = time.milliseconds(),
        )
        val e2 = assertFailsWith<TopicAuthorizationException> {
            metadata.maybeThrowExceptionForTopic("sensitiveTopic2")
        }
        assertEquals(expected = setOf("sensitiveTopic2"), actual = e2.unauthorizedTopics)
        metadata.maybeThrowAnyException()
        metadata.updateWithCurrentRequestVersion(
            response = metadataResponse,
            isPartialUpdate = false,
            nowMs = time.milliseconds(),
        )
        val e3 = assertFailsWith<InvalidTopicException> {
            metadata.maybeThrowExceptionForTopic("invalidTopic")
        }
        assertEquals(expected = setOf("invalidTopic"), actual = e3.invalidTopics)
        metadata.maybeThrowAnyException()

        // Other topics should not throw exception, but they should clear existing exception
        metadata.updateWithCurrentRequestVersion(
            response = metadataResponse,
            isPartialUpdate = false,
            nowMs = time.milliseconds(),
        )
        metadata.maybeThrowExceptionForTopic("anotherTopic")
        metadata.maybeThrowAnyException()
    }

    @Test
    fun testNodeIfOffline() {
        val partitionCounts = mapOf("topic-1" to 1)
        val (id) = Node(id = 0, host = "localhost", port = 9092)
        val (id1) = Node(id = 1, host = "localhost", port = 9093)
        val metadataResponse = metadataUpdateWith(
            clusterId = "dummy",
            numNodes = 2,
            topicErrors = emptyMap(),
            topicPartitionCounts = partitionCounts,
            epochSupplier = { 99 },
            partitionSupplier = { error, partition, _, leaderEpoch, _, _, _ ->
                PartitionMetadata(
                    error = error,
                    topicPartition = partition,
                    leaderId = id,
                    leaderEpoch = leaderEpoch,
                    replicaIds = listOf(id),
                    inSyncReplicaIds = emptyList(),
                    offlineReplicaIds = listOf(id1),
                )
            },
            responseVersion = ApiKeys.METADATA.latestVersion(),
            topicIds = emptyMap(),
        )
        metadata.updateWithCurrentRequestVersion(
            response = emptyMetadataResponse(),
            isPartialUpdate = false,
            nowMs = 0,
        )
        metadata.updateWithCurrentRequestVersion(
            response = metadataResponse,
            isPartialUpdate = false,
            nowMs = 10,
        )
        val tp = TopicPartition(topic = "topic-1", partition = 0)
        assertNullable(metadata.fetch().nodeIfOnline(tp, 0)) { node ->
            assertEquals(expected = 0, actual = node.id)
        }
        assertNull(metadata.fetch().nodeIfOnline(tp, 1))
        assertEquals(expected = 0, actual = metadata.fetch().nodeById(0)!!.id)
        assertEquals(expected = 1, actual = metadata.fetch().nodeById(1)!!.id)
    }

    @Test
    fun testNodeIfOnlineWhenNotInReplicaSet() {
        val partitionCounts: MutableMap<String, Int> = HashMap()
        partitionCounts["topic-1"] = 1
        val (id) = Node(0, "localhost", 9092)
        val metadataResponse = metadataUpdateWith(
            clusterId = "dummy",
            numNodes = 2,
            topicErrors = emptyMap(),
            topicPartitionCounts = partitionCounts,
            epochSupplier = { 99 },
            partitionSupplier = { error, partition, _, leaderEpoch, _, _, _ ->
                PartitionMetadata(
                    error = error,
                    topicPartition = partition,
                    leaderId = id,
                    leaderEpoch = leaderEpoch,
                    replicaIds = listOf(id),
                    inSyncReplicaIds = emptyList(),
                    offlineReplicaIds = emptyList(),
                )
            },
            responseVersion = ApiKeys.METADATA.latestVersion(),
            topicIds = emptyMap(),
        )
        metadata.updateWithCurrentRequestVersion(
            response = emptyMetadataResponse(),
            isPartialUpdate = false,
            nowMs = 0,
        )
        metadata.updateWithCurrentRequestVersion(
            response = metadataResponse,
            isPartialUpdate = false,
            nowMs = 10,
        )
        val tp = TopicPartition(topic = "topic-1", partition = 0)
        assertEquals(expected = 1, actual = metadata.fetch().nodeById(1)!!.id)
        assertNull(metadata.fetch().nodeIfOnline(tp, 1))
    }

    @Test
    fun testNodeIfOnlineNonExistentTopicPartition() {
        val metadataResponse = metadataUpdateWith(numNodes = 2, topicPartitionCounts = emptyMap())
        metadata.updateWithCurrentRequestVersion(
            response = metadataResponse,
            isPartialUpdate = false,
            nowMs = 0,
        )
        val tp = TopicPartition(topic = "topic-1", partition = 0)
        assertEquals(expected = 0, actual = metadata.fetch().nodeById(0)!!.id)
        assertNull(metadata.fetch().partition(tp))
        assertNull(metadata.fetch().nodeIfOnline(tp, 0))
    }

    @Test
    fun testLeaderMetadataInconsistentWithBrokerMetadata() {
        // Tests a reordering scenario which can lead to inconsistent leader state.
        // A partition initially has one broker offline. That broker comes online and
        // is elected leader. The client sees these two events in the opposite order.
        val tp = TopicPartition("topic", 0)
        val node0 = Node(id = 0, host = "localhost", port = 9092)
        val node1 = Node(id = 1, host = "localhost", port = 9093)
        val node2 = Node(id = 2, host = "localhost", port = 9094)

        // The first metadata received by broker (epoch=10)
        val firstPartitionMetadata = MetadataResponsePartition()
            .setPartitionIndex(tp.partition)
            .setErrorCode(Errors.NONE.code)
            .setLeaderEpoch(10)
            .setLeaderId(0)
            .setReplicaNodes(intArrayOf(0, 1, 2))
            .setIsrNodes(intArrayOf(0, 1, 2))
            .setOfflineReplicas(intArrayOf())

        // The second metadata received has stale metadata (epoch=8)
        val secondPartitionMetadata = MetadataResponsePartition()
            .setPartitionIndex(tp.partition)
            .setErrorCode(Errors.NONE.code)
            .setLeaderEpoch(8)
            .setLeaderId(1)
            .setReplicaNodes(intArrayOf(0, 1, 2))
            .setIsrNodes(intArrayOf(1, 2))
            .setOfflineReplicas(intArrayOf(0))
        metadata.updateWithCurrentRequestVersion(
            response = MetadataResponse(
                MetadataResponseData()
                    .setTopics(buildTopicCollection(tp.topic, firstPartitionMetadata))
                    .setBrokers(buildBrokerCollection(listOf(node0, node1, node2))),
                ApiKeys.METADATA.latestVersion()
            ),
            isPartialUpdate = false,
            nowMs = 10,
        )
        metadata.updateWithCurrentRequestVersion(
            response = MetadataResponse(
                MetadataResponseData()
                    .setTopics(buildTopicCollection(tp.topic, secondPartitionMetadata))
                    .setBrokers(buildBrokerCollection(listOf(node1, node2))),
                ApiKeys.METADATA.latestVersion()
            ),
            isPartialUpdate = false,
            nowMs = 20,
        )
        assertNull(metadata.fetch().leaderFor(tp))
        assertEquals(expected = 10, actual = metadata.lastSeenLeaderEpochs[tp])
        assertNull(metadata.currentLeader(tp).leader)
    }

    private fun buildTopicCollection(
        topic: String,
        partitionMetadata: MetadataResponsePartition,
    ): MetadataResponseTopicCollection {
        val topicMetadata = MetadataResponseTopic()
            .setErrorCode(Errors.NONE.code)
            .setName(topic)
            .setIsInternal(false)
        topicMetadata.setPartitions(listOf(partitionMetadata))
        val topics = MetadataResponseTopicCollection()
        topics.add(topicMetadata)
        return topics
    }

    private fun buildBrokerCollection(nodes: List<Node>): MetadataResponseBrokerCollection {
        val brokers = MetadataResponseBrokerCollection()
        for ((id, host, port, rack) in nodes) {
            val broker = MetadataResponseBroker()
                .setNodeId(id)
                .setHost(host)
                .setPort(port)
                .setRack(rack)
            brokers.add(broker)
        }
        return brokers
    }

    @Test
    fun testMetadataMerge() {
        val time = MockTime()
        val topicIds = mutableMapOf<String, Uuid>()
        val retainTopics = AtomicReference<Set<String>>(HashSet())
        metadata = object : Metadata(
            refreshBackoffMs = refreshBackoffMs,
            metadataExpireMs = metadataExpireMs,
            logContext = LogContext(),
            clusterResourceListeners = ClusterResourceListeners(),
        ) {
            override fun retainTopic(
                topic: String,
                isInternal: Boolean,
                nowMs: Long,
            ): Boolean = retainTopics.get().contains(topic)
        }

        // Initialize a metadata instance with two topic variants "old" and "keep". Both will be retained.
        val oldClusterId = "oldClusterId"
        val oldNodes = 2
        val oldTopicErrors = mapOf(
            "oldInvalidTopic" to Errors.INVALID_TOPIC_EXCEPTION,
            "keepInvalidTopic" to Errors.INVALID_TOPIC_EXCEPTION,
            "oldUnauthorizedTopic" to Errors.TOPIC_AUTHORIZATION_FAILED,
            "keepUnauthorizedTopic" to Errors.TOPIC_AUTHORIZATION_FAILED,
        )
        val oldTopicPartitionCounts = mapOf(
            "oldValidTopic" to 2,
            "keepValidTopic" to 3,
        )
        retainTopics.set(
            setOf(
                "oldInvalidTopic",
                "keepInvalidTopic",
                "oldUnauthorizedTopic",
                "keepUnauthorizedTopic",
                "oldValidTopic",
                "keepValidTopic",
            )
        )
        topicIds["oldValidTopic"] =  Uuid.randomUuid()
        topicIds["keepValidTopic"] =  Uuid.randomUuid()
        var metadataResponse = metadataUpdateWithIds(
            clusterId = oldClusterId,
            numNodes = oldNodes,
            topicErrors = oldTopicErrors,
            topicPartitionCounts = oldTopicPartitionCounts,
            epochSupplier = { 100 },
            topicIds = topicIds,
        )
        metadata.updateWithCurrentRequestVersion(
            response = metadataResponse,
            isPartialUpdate = true,
            nowMs = time.milliseconds(),
        )
        val metadataTopicIds1 = metadata.topicIds()
        retainTopics.get().forEach { topic ->
            assertEquals(expected = metadataTopicIds1[topic], actual = topicIds[topic])
        }

        // Update the metadata to add a new topic variant, "new", which will be retained with
        // "keep". Note this means that all the "old" topics should be dropped.
        var cluster = metadata.fetch()
        assertEquals(cluster.clusterResource.clusterId, oldClusterId)
        assertEquals(cluster.nodes.size, oldNodes)
        assertEquals(
            expected = setOf("oldInvalidTopic", "keepInvalidTopic"),
            actual = cluster.invalidTopics,
        )
        assertEquals(
            expected = setOf("oldUnauthorizedTopic", "keepUnauthorizedTopic"),
            actual = cluster.unauthorizedTopics,
        )
        assertEquals(
            expected = setOf("oldValidTopic", "keepValidTopic"),
            actual = cluster.topics,
        )
        assertEquals(expected = 2, actual = cluster.partitionsForTopic("oldValidTopic").size)
        assertEquals(expected = 3, actual = cluster.partitionsForTopic("keepValidTopic").size)
        assertEquals(expected = topicIds.values.toSet(), actual = cluster.topicIds().toSet())
        val newClusterId = "newClusterId"
        val newNodes = oldNodes + 1
        val newTopicErrors = mapOf(
            "newInvalidTopic" to Errors.INVALID_TOPIC_EXCEPTION,
            "newUnauthorizedTopic" to Errors.TOPIC_AUTHORIZATION_FAILED,
        )
        val newTopicPartitionCounts = mapOf(
            "keepValidTopic" to 2,
            "newValidTopic" to 4,    
        )
        retainTopics.set(
            setOf(
                "keepInvalidTopic",
                "newInvalidTopic",
                "keepUnauthorizedTopic",
                "newUnauthorizedTopic",
                "keepValidTopic",
                "newValidTopic",
            )
        )
        topicIds["newValidTopic"] = Uuid.randomUuid()
        metadataResponse = metadataUpdateWithIds(
            clusterId = newClusterId,
            numNodes = newNodes,
            topicErrors = newTopicErrors,
            topicPartitionCounts = newTopicPartitionCounts,
            epochSupplier = { 200 },
            topicIds = topicIds,
        )
        metadata.updateWithCurrentRequestVersion(
            response = metadataResponse,
            isPartialUpdate = true,
            nowMs = time.milliseconds(),
        )
        topicIds.remove("oldValidTopic")
        val metadataTopicIds2 = metadata.topicIds()
        retainTopics.get().forEach { topic ->
            assertEquals(expected = metadataTopicIds2[topic], actual = topicIds[topic])
        }
        assertNull(metadataTopicIds2["oldValidTopic"])
        cluster = metadata.fetch()
        assertEquals(cluster.clusterResource.clusterId, newClusterId)
        assertEquals(cluster.nodes.size, newNodes)
        assertEquals(
            expected = setOf("keepInvalidTopic", "newInvalidTopic"),
            actual = cluster.invalidTopics,
        )
        assertEquals(
            expected = setOf("keepUnauthorizedTopic", "newUnauthorizedTopic"),
            actual = cluster.unauthorizedTopics,
        )
        assertEquals(
            expected = setOf("keepValidTopic", "newValidTopic"),
            actual = cluster.topics,
        )
        assertEquals(expected = 2, actual = cluster.partitionsForTopic("keepValidTopic").size)
        assertEquals(expected = 4, actual = cluster.partitionsForTopic("newValidTopic").size)
        assertEquals(expected = topicIds.values.toSet(), actual = cluster.topicIds().toSet())

        // Perform another metadata update, but this time all topic metadata should be cleared.
        retainTopics.set(emptySet())
        metadataResponse = metadataUpdateWithIds(
            clusterId = newClusterId,
            numNodes = newNodes,
            topicErrors = newTopicErrors,
            topicPartitionCounts = newTopicPartitionCounts,
            epochSupplier = { 300 },
            topicIds = topicIds,
        )
        metadata.updateWithCurrentRequestVersion(metadataResponse, true, time.milliseconds())
        val metadataTopicIds3 = metadata.topicIds()
        topicIds.forEach { (topicName, _) ->
            assertNull(metadataTopicIds3[topicName])
        }
        cluster = metadata.fetch()
        assertEquals(expected = newClusterId, actual = cluster.clusterResource.clusterId)
        assertEquals(expected = newNodes, actual = cluster.nodes.size)
        assertEquals(expected = emptySet(), actual = cluster.invalidTopics)
        assertEquals(expected = emptySet(), actual = cluster.unauthorizedTopics)
        assertEquals(expected = emptySet(), actual = cluster.topics)
        assertTrue(cluster.topicIds().isEmpty())
    }

    @Test
    fun testMetadataMergeOnIdDowngrade() {
        val time = MockTime()
        val topicIds = mutableMapOf<String, Uuid>()
        val retainTopics = AtomicReference<Set<String>>(emptySet())
        metadata = object : Metadata(
            refreshBackoffMs = refreshBackoffMs,
            metadataExpireMs = metadataExpireMs,
            logContext = LogContext(),
            clusterResourceListeners = ClusterResourceListeners(),
        ) {
            override fun retainTopic(
                topic: String,
                isInternal: Boolean,
                nowMs: Long,
            ): Boolean = retainTopics.get().contains(topic)
        }

        // Initialize a metadata instance with two topics. Both will be retained.
        val clusterId = "clusterId"
        val nodes = 2
        val topicPartitionCounts = mapOf(
            "validTopic1" to 2,
            "validTopic2" to 3,    
        )
        retainTopics.set(
            setOf(
                "validTopic1",
                "validTopic2",
            )
        )
        topicIds["validTopic1"] = Uuid.randomUuid()
        topicIds["validTopic2"] = Uuid.randomUuid()
        var metadataResponse = metadataUpdateWithIds(
            clusterId = clusterId,
            numNodes = nodes,
            topicErrors = emptyMap(),
            topicPartitionCounts = topicPartitionCounts,
            epochSupplier = { 100 },
            topicIds = topicIds,
        )
        metadata.updateWithCurrentRequestVersion(
            response = metadataResponse,
            isPartialUpdate = true,
            nowMs = time.milliseconds(),
        )
        val metadataTopicIds1 = metadata.topicIds()
        retainTopics.get().forEach { topic ->
            assertEquals(expected = metadataTopicIds1[topic], actual = topicIds[topic])
        }

        // Try removing the topic ID from keepValidTopic (simulating receiving a request from a controller with an older IBP)
        topicIds.remove("validTopic1")
        metadataResponse = metadataUpdateWithIds(
            clusterId = clusterId,
            numNodes = nodes,
            topicErrors = emptyMap(),
            topicPartitionCounts = topicPartitionCounts,
            epochSupplier = { 200 },
            topicIds = topicIds,
        )
        metadata.updateWithCurrentRequestVersion(
            response = metadataResponse,
            isPartialUpdate = true,
            nowMs = time.milliseconds(),
        )
        val metadataTopicIds2 = metadata.topicIds()
        retainTopics.get().forEach { topic ->
            assertEquals(expected = metadataTopicIds2[topic], actual = topicIds[topic])
        }
        val cluster = metadata.fetch()
        // We still have the topic, but it just doesn't have an ID.
        assertEquals(expected = setOf("validTopic1", "validTopic2"), actual = cluster.topics)
        assertEquals(expected = 2, actual = cluster.partitionsForTopic("validTopic1").size)
        assertEquals(expected = topicIds.values.toSet(), actual = cluster.topicIds().toSet())
        assertEquals(expected = Uuid.ZERO_UUID, actual = cluster.topicId("validTopic1"))
    }

    companion object {
        private fun emptyMetadataResponse(): MetadataResponse {
            return metadataResponse(
                brokers = emptyList(),
                clusterId = null,
                controllerId = -1,
                topicMetadataList = emptyList(),
            )
        }

        private fun checkTimeToNextUpdate(refreshBackoffMs: Long, metadataExpireMs: Long) {
            var now = 10000L

            // Metadata timeToNextUpdate is implicitly relying on the premise that the
            // currentTimeMillis is always larger than the metadataExpireMs or refreshBackoffMs.
            // It won't be a problem practically since all usages of Metadata calls first update()
            // immediately after it's construction.
            require(!(metadataExpireMs > now || refreshBackoffMs > now)) {
                "metadataExpireMs and refreshBackoffMs must be smaller than 'now'"
            }
            val largerOfBackoffAndExpire =
                max(refreshBackoffMs.toDouble(), metadataExpireMs.toDouble()).toLong()
            val metadata = Metadata(
                refreshBackoffMs = refreshBackoffMs,
                metadataExpireMs = metadataExpireMs,
                logContext = LogContext(),
                clusterResourceListeners = ClusterResourceListeners(),
            )
            assertEquals(expected = 0, actual = metadata.timeToNextUpdate(now))

            // lastSuccessfulRefreshMs updated to now.
            metadata.updateWithCurrentRequestVersion(
                response = emptyMetadataResponse(),
                isPartialUpdate = false,
                nowMs = now,
            )

            // The last update was successful so the remaining time to expire the current metadata
            // should be returned.
            assertEquals(
                expected = largerOfBackoffAndExpire,
                actual = metadata.timeToNextUpdate(now),
            )

            // Metadata update requested explicitly
            metadata.requestUpdate()
            // Update requested so metadataExpireMs should no longer take effect.
            assertEquals(expected = refreshBackoffMs, actual = metadata.timeToNextUpdate(now))

            // Reset needUpdate to false.
            metadata.updateWithCurrentRequestVersion(
                response = emptyMetadataResponse(),
                isPartialUpdate = false,
                nowMs = now,
            )
            assertEquals(
                expected = largerOfBackoffAndExpire,
                actual = metadata.timeToNextUpdate(now),
            )

            // Both metadataExpireMs and refreshBackoffMs elapsed.
            now += largerOfBackoffAndExpire
            assertEquals(expected = 0, actual = metadata.timeToNextUpdate(now))
            assertEquals(expected = 0, actual = metadata.timeToNextUpdate(now + 1))
        }
    }
}
