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

@file:Suppress("LargeClass", "LongMethod", "LongParameterList")

package org.apache.kafka.clients.consumer.internals

import org.apache.kafka.clients.ApiVersions
import org.apache.kafka.clients.ClientDnsLookup
import org.apache.kafka.clients.ClientUtils.parseAndValidateAddresses
import org.apache.kafka.clients.FetchSessionHandler
import org.apache.kafka.clients.Metadata.LeaderAndEpoch
import org.apache.kafka.clients.MockClient
import org.apache.kafka.clients.MockClient.RequestMatcher
import org.apache.kafka.clients.NetworkClient
import org.apache.kafka.clients.NodeApiVersions
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.LogTruncationException
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.clients.consumer.OffsetAndTimestamp
import org.apache.kafka.clients.consumer.OffsetOutOfRangeException
import org.apache.kafka.clients.consumer.OffsetResetStrategy
import org.apache.kafka.clients.consumer.internals.Fetcher.ListOffsetData
import org.apache.kafka.clients.consumer.internals.SubscriptionState.FetchPosition
import org.apache.kafka.common.IsolationLevel
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.MetricNameTemplate
import org.apache.kafka.common.Node
import org.apache.kafka.common.TopicIdPartition
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.Uuid
import org.apache.kafka.common.errors.InvalidTopicException
import org.apache.kafka.common.errors.RecordTooLargeException
import org.apache.kafka.common.errors.SerializationException
import org.apache.kafka.common.errors.TimeoutException
import org.apache.kafka.common.errors.TopicAuthorizationException
import org.apache.kafka.common.header.Header
import org.apache.kafka.common.header.internals.RecordHeader
import org.apache.kafka.common.internals.ClusterResourceListeners
import org.apache.kafka.common.message.ApiMessageType
import org.apache.kafka.common.message.FetchResponseData
import org.apache.kafka.common.message.FetchResponseData.AbortedTransaction
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
import org.apache.kafka.common.network.NetworkReceive
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.record.CompressionType
import org.apache.kafka.common.record.ControlRecordType
import org.apache.kafka.common.record.DefaultRecordBatch
import org.apache.kafka.common.record.EndTransactionMarker
import org.apache.kafka.common.record.LegacyRecord
import org.apache.kafka.common.record.MemoryRecords
import org.apache.kafka.common.record.MemoryRecords.RecordFilter
import org.apache.kafka.common.record.MemoryRecordsBuilder
import org.apache.kafka.common.record.Record
import org.apache.kafka.common.record.RecordBatch
import org.apache.kafka.common.record.Records
import org.apache.kafka.common.record.SimpleRecord
import org.apache.kafka.common.record.TimestampType
import org.apache.kafka.common.requests.AbstractRequest
import org.apache.kafka.common.requests.ApiVersionsResponse
import org.apache.kafka.common.requests.FetchMetadata
import org.apache.kafka.common.requests.FetchRequest
import org.apache.kafka.common.requests.FetchResponse
import org.apache.kafka.common.requests.ListOffsetsRequest
import org.apache.kafka.common.requests.ListOffsetsResponse
import org.apache.kafka.common.requests.MetadataRequest
import org.apache.kafka.common.requests.MetadataResponse
import org.apache.kafka.common.requests.MetadataResponse.PartitionMetadata
import org.apache.kafka.common.requests.OffsetsForLeaderEpochRequest
import org.apache.kafka.common.requests.OffsetsForLeaderEpochResponse
import org.apache.kafka.common.requests.RequestTestUtils.metadataResponse
import org.apache.kafka.common.requests.RequestTestUtils.metadataUpdateWith
import org.apache.kafka.common.requests.RequestTestUtils.metadataUpdateWithIds
import org.apache.kafka.common.requests.RequestTestUtils.serializeResponseWithHeader
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.serialization.BytesDeserializer
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.utils.BufferSupplier
import org.apache.kafka.common.utils.ByteBufferOutputStream
import org.apache.kafka.common.utils.LogContext
import org.apache.kafka.common.utils.MockTime
import org.apache.kafka.common.utils.Utils.utf8
import org.apache.kafka.test.DelayedReceive
import org.apache.kafka.test.MockSelector
import org.apache.kafka.test.TestUtils.assertNullable
import org.apache.kafka.test.TestUtils.checkEquals
import org.apache.kafka.test.TestUtils.singletonCluster
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import java.io.DataOutputStream
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.time.Duration
import java.util.concurrent.Callable
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.function.Function
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertNotEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlin.test.fail

class FetcherTest {

    private val listener: ConsumerRebalanceListener = NoOpConsumerRebalanceListener()

    private val topicName = "test"

    private val groupId = "test-group"

    private val topicId = Uuid.randomUuid()

    private val topicIds = mutableMapOf(topicName to topicId)

    private val topicNames = mapOf(topicId to topicName)

    private val metricGroup = "consumer$groupId-fetch-manager-metrics"

    private val tp0 = TopicPartition(topicName, 0)

    private val tp1 = TopicPartition(topicName, 1)

    private val tp2 = TopicPartition(topicName, 2)

    private val tp3 = TopicPartition(topicName, 3)

    private val tidp0 = TopicIdPartition(topicId, tp0)

    private val tidp1 = TopicIdPartition(topicId, tp1)

    private val tidp2 = TopicIdPartition(topicId, tp2)

    private val tidp3 = TopicIdPartition(topicId, tp3)

    private val validLeaderEpoch = 0

    private val initialUpdateResponse = metadataUpdateWithIds(
        numNodes = 1,
        topicPartitionCounts = mapOf(topicName to 4),
        topicIds = topicIds,
    )

    private val minBytes = 1

    private val maxBytes = Int.MAX_VALUE

    private val maxWaitMs = 0

    private var fetchSize = 1000

    private val retryBackoffMs = 100L

    private val requestTimeoutMs = 30000L

    private var time = MockTime(1)

    private lateinit var subscriptions: SubscriptionState

    private var metadata: ConsumerMetadata? = null

    private var metricsRegistry: FetcherMetricsRegistry? = null

    private var client: MockClient? = null

    private lateinit var metrics: Metrics

    private val apiVersions = ApiVersions()

    private var consumerClient: ConsumerNetworkClient? = null

    private lateinit var fetcher: Fetcher<*, *>

    private lateinit var records: MemoryRecords

    private lateinit var nextRecords: MemoryRecords

    private lateinit var emptyRecords: MemoryRecords

    private lateinit var partialRecords: MemoryRecords

    private lateinit var executorService: ExecutorService

    @BeforeEach
    fun setup() {
        records = buildRecords(baseOffset = 1L, count = 3, firstMessageId = 1)
        nextRecords = buildRecords(baseOffset = 4L, count = 2, firstMessageId = 4)
        emptyRecords = buildRecords(baseOffset = 0L, count = 0, firstMessageId = 0)
        partialRecords = buildRecords(baseOffset = 4L, count = 1, firstMessageId = 0)
        partialRecords.buffer().putInt(Records.SIZE_OFFSET, 10000)
    }

    private fun assignFromUser(partitions: Set<TopicPartition>) {
        subscriptions.assignFromUser(partitions)
        client!!.updateMetadata(initialUpdateResponse)

        // A dummy metadata update to ensure valid leader epoch.
        metadata!!.updateWithCurrentRequestVersion(
            response = metadataUpdateWith(
                clusterId = "dummy",
                numNodes = 1,
                topicErrors = emptyMap(),
                topicPartitionCounts = mapOf(topicName to 4),
                epochSupplier = { validLeaderEpoch },
                topicIds = topicIds
            ),
            isPartialUpdate = false,
            nowMs = 0L,
        )
    }

    private fun assignFromUserNoId(partitions: Set<TopicPartition>) {
        subscriptions.assignFromUser(partitions)
        client!!.updateMetadata(
            metadataUpdateWithIds(
                numNodes = 1,
                topicPartitionCounts = mapOf("noId" to 1),
                topicIds = emptyMap(),
            )
        )

        // A dummy metadata update to ensure valid leader epoch.
        metadata!!.update(
            requestVersion = 9,
            response = metadataUpdateWith(
                clusterId = "dummy",
                numNodes = 1,
                topicErrors = emptyMap(),
                topicPartitionCounts = mapOf("noId" to 1),
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
        if (::fetcher.isInitialized) fetcher.close()
        if (::executorService.isInitialized) {
            executorService.shutdownNow()
            assertTrue(executorService.awaitTermination(5, TimeUnit.SECONDS))
        }
    }

    @Test
    fun testFetchNormal() {
        buildFetcher()
        assignFromUser(setOf(tp0))
        subscriptions.seek(tp0, 0)

        // normal fetch
        assertEquals(1, fetcher.sendFetches())
        assertFalse(fetcher.hasCompletedFetches())
        client!!.prepareResponse(
            fullFetchResponse(
                tp = tidp0,
                records = records,
                error = Errors.NONE,
                hw = 100L,
                throttleTime = 0,
            )
        )
        consumerClient!!.poll(time.timer(0))
        assertTrue(fetcher.hasCompletedFetches())
        val partitionRecords = fetchedRecords<ByteArray, ByteArray>()
        assertTrue(partitionRecords.containsKey(tp0))
        val records = partitionRecords[tp0]!!
        assertEquals(3, records.size)
        assertEquals(4L, subscriptions.position(tp0)!!.offset) // this is the next fetching position
        var offset: Long = 1
        for ((_, _, offset1) in records) {
            assertEquals(offset, offset1)
            offset += 1
        }
    }

    @Test
    fun testInflightFetchOnPendingPartitions() {
        buildFetcher()
        assignFromUser(setOf(tp0))
        subscriptions.seek(tp0, 0)
        assertEquals(1, fetcher.sendFetches())
        subscriptions.markPendingRevocation(setOf(tp0))
        client!!.prepareResponse(
            fullFetchResponse(
                tp = tidp0,
                records = records,
                error = Errors.NONE,
                hw = 100L,
                throttleTime = 0,
            )
        )
        consumerClient!!.poll(time.timer(0))
        assertNull(fetchedRecords<Any, Any>()[tp0])
    }

    @Test
    fun testFetchingPendingPartitions() {
        buildFetcher()
        assignFromUser(setOf(tp0))
        subscriptions.seek(tp0, 0)

        // normal fetch
        assertEquals(1, fetcher.sendFetches())

        client!!.prepareResponse(
            response = fullFetchResponse(
                tp = tidp0,
                records = records,
                error = Errors.NONE,
                hw = 100L,
                throttleTime = 0,
            ),
        )
        consumerClient!!.poll(time.timer(0))
        assertTrue(fetcher.hasCompletedFetches())

        fetchedRecords<Any, Any>()
        assertEquals(4L, subscriptions.position(tp0)!!.offset) // this is the next fetching position

        // mark partition unfetchable
        subscriptions.markPendingRevocation(setOf(tp0))
        assertEquals(0, fetcher.sendFetches())

        consumerClient!!.poll(time.timer(0))
        assertFalse(fetcher.hasCompletedFetches())

        fetchedRecords<Any, Any>()
        assertEquals(4L, subscriptions.position(tp0)!!.offset)
    }

    @Test
    fun testFetchWithNoTopicId() {
        // Should work and default to using old request type.
        buildFetcher()
        val noId = TopicIdPartition(Uuid.ZERO_UUID, TopicPartition("noId", 0))
        assignFromUserNoId(setOf(noId.topicPartition))
        subscriptions.seek(noId.topicPartition, 0)

        // Fetch should use request version 12
        assertEquals(1, fetcher.sendFetches())
        assertFalse(fetcher.hasCompletedFetches())
        client!!.prepareResponse(
            matcher = fetchRequestMatcher(
                expectedVersion = 12,
                tp = noId,
                expectedFetchOffset = 0,
                expectedCurrentLeaderEpoch = validLeaderEpoch,
            ),
            response = fullFetchResponse(
                tp = noId,
                records = records,
                error = Errors.NONE,
                hw = 100L,
                throttleTime = 0,
            ),
        )
        consumerClient!!.poll(time.timer(0))
        assertTrue(fetcher.hasCompletedFetches())
        val partitionRecords = fetchedRecords<ByteArray, ByteArray>()
        assertTrue(partitionRecords.containsKey(noId.topicPartition))
        val records = partitionRecords[noId.topicPartition]!!
        assertEquals(3, records.size)
        assertEquals(4L, subscriptions.position(noId.topicPartition)!!.offset) // this is the next fetching position
        var offset: Long = 1
        for ((_, _, offset1) in records) {
            assertEquals(offset, offset1)
            offset += 1
        }
    }

    @Test
    fun testFetchWithTopicId() {
        buildFetcher()
        val tp = TopicIdPartition(topicId, TopicPartition(topicName, 0))
        assignFromUser(setOf(tp.topicPartition))
        subscriptions.seek(tp.topicPartition, 0)

        // Fetch should use latest version
        assertEquals(1, fetcher.sendFetches())
        assertFalse(fetcher.hasCompletedFetches())
        client!!.prepareResponse(
            matcher = fetchRequestMatcher(
                expectedVersion = ApiKeys.FETCH.latestVersion(),
                tp = tp,
                expectedFetchOffset = 0,
                expectedCurrentLeaderEpoch = validLeaderEpoch,
            ),
            response = fullFetchResponse(
                tp = tp,
                records = records,
                error = Errors.NONE,
                hw = 100L,
                throttleTime = 0,
            ),
        )
        consumerClient!!.poll(time.timer(0))
        assertTrue(fetcher.hasCompletedFetches())
        val partitionRecords = fetchedRecords<ByteArray, ByteArray>()
        assertTrue(partitionRecords.containsKey(tp.topicPartition))
        val records = partitionRecords[tp.topicPartition]!!
        assertEquals(3, records.size)
        assertEquals(4L, subscriptions.position(tp.topicPartition)!!.offset) // this is the next fetching position
        var offset = 1L
        for ((_, _, offset1) in records) {
            assertEquals(offset, offset1)
            offset += 1
        }
    }

    @Test
    fun testFetchForgetTopicIdWhenUnassigned() {
        buildFetcher()
        val foo = TopicIdPartition(
            topicId = Uuid.randomUuid(),
            topicPartition = TopicPartition("foo", 0),
        )
        val bar = TopicIdPartition(
            topicId = Uuid.randomUuid(),
            topicPartition = TopicPartition("bar", 0),
        )

        // Assign foo and bar.
        subscriptions.assignFromUser(setOf(foo.topicPartition))
        client!!.updateMetadata(
            metadataUpdateWithIds(
                numNodes = 1,
                partitions = setOf(foo),
                epochSupplier = { validLeaderEpoch },
            )
        )
        subscriptions.seek(foo.topicPartition, 0)

        // Fetch should use latest version.
        assertEquals(1, fetcher.sendFetches())
        client!!.prepareResponse(
            matcher = fetchRequestMatcher(
                expectedVersion = ApiKeys.FETCH.latestVersion(),
                fetch = mapOf(
                    foo to FetchRequest.PartitionData(
                        topicId = foo.topicId,
                        fetchOffset = 0,
                        logStartOffset = FetchRequest.INVALID_LOG_START_OFFSET,
                        maxBytes = fetchSize,
                        currentLeaderEpoch = validLeaderEpoch,
                    ),
                ),
                forgotten = emptyList(),
            ),
            response = fullFetchResponse(
                sessionId = 1,
                tp = foo,
                records = records,
                error = Errors.NONE,
                hw = 100L,
                throttleTime = 0,
            ),
        )
        consumerClient!!.poll(time.timer(0))
        assertTrue(fetcher.hasCompletedFetches())
        fetchedRecords<Any, Any>()

        // Assign bar and unassign foo.
        subscriptions.assignFromUser(setOf(bar.topicPartition))
        client!!.updateMetadata(
            metadataUpdateWithIds(
                numNodes = 1,
                partitions = setOf(bar),
                epochSupplier = { validLeaderEpoch },
            ),
        )
        subscriptions.seek(bar.topicPartition, 0)

        // Fetch should use latest version.
        assertEquals(1, fetcher.sendFetches())
        assertFalse(fetcher.hasCompletedFetches())
        client!!.prepareResponse(
            matcher = fetchRequestMatcher(
                expectedVersion = ApiKeys.FETCH.latestVersion(),
                fetch = mapOf(
                    bar to FetchRequest.PartitionData(
                        topicId = bar.topicId,
                        fetchOffset = 0,
                        logStartOffset = FetchRequest.INVALID_LOG_START_OFFSET,
                        maxBytes = fetchSize,
                        currentLeaderEpoch = validLeaderEpoch,
                    )
                ),
                forgotten = listOf(foo),
            ),
            response = fullFetchResponse(
                sessionId = 1,
                tp = bar,
                records = records,
                error = Errors.NONE,
                hw = 100L,
                throttleTime = 0,
            ),
        )
        consumerClient!!.poll(time.timer(0))
        assertTrue(fetcher.hasCompletedFetches())
        fetchedRecords<Any, Any>()
    }

    @Test
    fun testFetchForgetTopicIdWhenReplaced() {
        buildFetcher()
        val fooWithOldTopicId = TopicIdPartition(Uuid.randomUuid(), TopicPartition("foo", 0))
        val fooWithNewTopicId = TopicIdPartition(Uuid.randomUuid(), TopicPartition("foo", 0))

        // Assign foo with old topic id.
        subscriptions.assignFromUser(setOf(fooWithOldTopicId.topicPartition))
        client!!.updateMetadata(
            metadataUpdateWithIds(1, setOf(fooWithOldTopicId),
                Function { tp: TopicPartition? -> validLeaderEpoch })
        )
        subscriptions.seek(fooWithOldTopicId.topicPartition, 0)

        // Fetch should use latest version.
        assertEquals(1, fetcher.sendFetches())
        client!!.prepareResponse(
            matcher = fetchRequestMatcher(
                expectedVersion = ApiKeys.FETCH.latestVersion(),
                fetch = mapOf(
                    fooWithOldTopicId to FetchRequest.PartitionData(
                        topicId = fooWithOldTopicId.topicId,
                        fetchOffset = 0,
                        logStartOffset = FetchRequest.INVALID_LOG_START_OFFSET,
                        maxBytes = fetchSize,
                        currentLeaderEpoch = validLeaderEpoch,
                    ),
                ),
                forgotten = emptyList(),
            ),
            response = fullFetchResponse(
                sessionId = 1,
                tp = fooWithOldTopicId,
                records = records,
                error = Errors.NONE,
                hw = 100L,
                throttleTime = 0,
            ),
        )
        consumerClient!!.poll(time.timer(0))
        assertTrue(fetcher.hasCompletedFetches())
        fetchedRecords<Any, Any>()

        // Replace foo with old topic id with foo with new topic id.
        subscriptions.assignFromUser(setOf(fooWithNewTopicId.topicPartition))
        client!!.updateMetadata(
            metadataUpdateWithIds(
                numNodes = 1,
                partitions = setOf(fooWithNewTopicId),
                epochSupplier = { validLeaderEpoch },
            )
        )
        subscriptions.seek(fooWithNewTopicId.topicPartition, 0)

        // Fetch should use latest version.
        assertEquals(1, fetcher.sendFetches())
        assertFalse(fetcher.hasCompletedFetches())

        // foo with old topic id should be removed from the session.
        client!!.prepareResponse(
            matcher = fetchRequestMatcher(
                expectedVersion = ApiKeys.FETCH.latestVersion(),
                fetch = mapOf(
                    fooWithNewTopicId to FetchRequest.PartitionData(
                        topicId = fooWithNewTopicId.topicId,
                        fetchOffset = 0,
                        logStartOffset = FetchRequest.INVALID_LOG_START_OFFSET,
                        maxBytes = fetchSize,
                        currentLeaderEpoch = validLeaderEpoch,
                    )
                ),
                forgotten = listOf(fooWithOldTopicId),
            ),
            response = fullFetchResponse(
                sessionId = 1,
                tp = fooWithNewTopicId,
                records = records,
                error = Errors.NONE,
                hw = 100L,
                throttleTime = 0,
            ),
        )
        consumerClient!!.poll(time.timer(0))
        assertTrue(fetcher.hasCompletedFetches())
        fetchedRecords<Any, Any>()
    }

    @Test
    fun testFetchTopicIdUpgradeDowngrade() {
        buildFetcher()
        val fooWithoutId = TopicIdPartition(Uuid.ZERO_UUID, TopicPartition("foo", 0))
        val fooWithId = TopicIdPartition(Uuid.randomUuid(), TopicPartition("foo", 0))

        // Assign foo without a topic id.
        subscriptions.assignFromUser(setOf(fooWithoutId.topicPartition))
        client!!.updateMetadata(
            metadataUpdateWithIds(
                numNodes = 1,
                partitions = setOf(fooWithoutId),
                epochSupplier = { validLeaderEpoch },
            )
        )
        subscriptions.seek(fooWithoutId.topicPartition, 0)

        // Fetch should use version 12.
        assertEquals(1, fetcher.sendFetches())

        client!!.prepareResponse(
            matcher = fetchRequestMatcher(
                expectedVersion = 12.toShort(),
                fetch = mapOf(
                    fooWithoutId to FetchRequest.PartitionData(
                        topicId = fooWithoutId.topicId,
                        fetchOffset = 0,
                        logStartOffset = FetchRequest.INVALID_LOG_START_OFFSET,
                        maxBytes = fetchSize,
                        currentLeaderEpoch = validLeaderEpoch,
                    )
                ),
                forgotten = emptyList()
            ),
            response = fullFetchResponse(
                sessionId = 1,
                tp = fooWithoutId,
                records = records,
                error = Errors.NONE,
                hw = 100L,
                throttleTime = 0,
            ),
        )
        consumerClient!!.poll(time.timer(0))
        assertTrue(fetcher.hasCompletedFetches())
        fetchedRecords<Any, Any>()

        // Upgrade.
        subscriptions.assignFromUser(setOf(fooWithId.topicPartition))
        client!!.updateMetadata(
            metadataUpdateWithIds(
                numNodes = 1,
                partitions = setOf(fooWithId),
                epochSupplier = { validLeaderEpoch },
            )
        )
        subscriptions.seek(fooWithId.topicPartition, 0)

        // Fetch should use latest version.
        assertEquals(1, fetcher.sendFetches())
        assertFalse(fetcher.hasCompletedFetches())

        // foo with old topic id should be removed from the session.
        client!!.prepareResponse(
            matcher = fetchRequestMatcher(
                expectedVersion = ApiKeys.FETCH.latestVersion(),
                fetch = mapOf(
                    fooWithId to FetchRequest.PartitionData(
                        topicId = fooWithId.topicId,
                        fetchOffset = 0,
                        logStartOffset = FetchRequest.INVALID_LOG_START_OFFSET,
                        maxBytes = fetchSize,
                        currentLeaderEpoch = validLeaderEpoch,
                    ),
                ),
                forgotten = emptyList(),
            ),
            response = fullFetchResponse(
                sessionId = 1,
                tp = fooWithId,
                records = records,
                error = Errors.NONE,
                hw = 100L,
                throttleTime = 0,
            ),
        )
        consumerClient!!.poll(time.timer(0))
        assertTrue(fetcher.hasCompletedFetches())
        fetchedRecords<Any, Any>()

        // Downgrade.
        subscriptions.assignFromUser(setOf(fooWithoutId.topicPartition))
        client!!.updateMetadata(
            metadataUpdateWithIds(
                numNodes = 1,
                partitions = setOf(fooWithoutId),
                epochSupplier = { validLeaderEpoch },
            )
        )
        subscriptions.seek(fooWithoutId.topicPartition, 0)

        // Fetch should use version 12.
        assertEquals(1, fetcher.sendFetches())
        assertFalse(fetcher.hasCompletedFetches())

        // foo with old topic id should be removed from the session.
        client!!.prepareResponse(
            matcher = fetchRequestMatcher(
                expectedVersion = 12,
                fetch = mapOf(
                    fooWithoutId to FetchRequest.PartitionData(
                        topicId = fooWithoutId.topicId,
                        fetchOffset = 0,
                        logStartOffset = FetchRequest.INVALID_LOG_START_OFFSET,
                        maxBytes = fetchSize,
                        currentLeaderEpoch = validLeaderEpoch,
                    ),
                ),
                forgotten = emptyList(),
            ),
            response = fullFetchResponse(
                sessionId = 1,
                tp = fooWithoutId,
                records = records,
                error = Errors.NONE,
                hw = 100L,
                throttleTime = 0,
            ),
        )
        consumerClient!!.poll(time.timer(0))
        assertTrue(fetcher.hasCompletedFetches())
        fetchedRecords<Any, Any>()
    }

    private fun fetchRequestMatcher(
        expectedVersion: Short,
        tp: TopicIdPartition,
        expectedFetchOffset: Long,
        expectedCurrentLeaderEpoch: Int?,
    ): RequestMatcher {
        return fetchRequestMatcher(
            expectedVersion = expectedVersion,
            fetch = mapOf(
                tp to FetchRequest.PartitionData(
                    topicId = tp.topicId,
                    fetchOffset = expectedFetchOffset,
                    logStartOffset = FetchRequest.INVALID_LOG_START_OFFSET,
                    maxBytes = fetchSize,
                    currentLeaderEpoch = expectedCurrentLeaderEpoch,
                )
            ),
            forgotten = emptyList(),
        )
    }

    private fun fetchRequestMatcher(
        expectedVersion: Short,
        fetch: Map<TopicIdPartition, FetchRequest.PartitionData>,
        forgotten: List<TopicIdPartition>,
    ): RequestMatcher {
        return RequestMatcher { body ->
            assertIs<FetchRequest>(value = body, message = "Should have seen FetchRequest")
            assertEquals(expectedVersion, body.version)
            assertEquals(fetch, body.fetchData(topicNames(fetch.keys)))
            assertEquals(forgotten, body.forgottenTopics(topicNames(forgotten)))
            return@RequestMatcher true
        }
    }

    private fun topicNames(partitions: Collection<TopicIdPartition>): Map<Uuid, String> {
        val topicNames = mutableMapOf<Uuid, String>()
        partitions.forEach { partition ->
            topicNames.putIfAbsent(partition.topicId, partition.topic)
        }
        return topicNames
    }

    @Test
    fun testMissingLeaderEpochInRecords() {
        buildFetcher()
        assignFromUser(setOf(tp0))
        subscriptions.seek(tp0, 0)
        val buffer = ByteBuffer.allocate(1024)
        val builder = MemoryRecords.builder(
            buffer = buffer,
            magic = RecordBatch.MAGIC_VALUE_V0,
            compressionType = CompressionType.NONE,
            timestampType = TimestampType.CREATE_TIME,
            baseOffset = 0L,
            logAppendTime = System.currentTimeMillis(),
            producerId = RecordBatch.NO_PARTITION_LEADER_EPOCH.toLong(),
        )
        builder.append(timestamp = 0L, key = "key".toByteArray(), value = "1".toByteArray())
        builder.append(timestamp = 0L, key = "key".toByteArray(), value = "2".toByteArray())
        val records = builder.build()
        assertEquals(1, fetcher.sendFetches())
        assertFalse(fetcher.hasCompletedFetches())
        client!!.prepareResponse(
            fullFetchResponse(
                tp = tidp0,
                records = records,
                error = Errors.NONE,
                hw = 100L,
                throttleTime = 0,
            )
        )
        consumerClient!!.poll(time.timer(0))
        assertTrue(fetcher.hasCompletedFetches())
        val partitionRecords = fetchedRecords<ByteArray, ByteArray>()
        assertTrue(partitionRecords.containsKey(tp0))
        assertEquals(2, partitionRecords[tp0]!!.size)
        for (record in partitionRecords[tp0]!!) assertNull(record.leaderEpoch)
    }

    @Test
    fun testLeaderEpochInConsumerRecord() {
        buildFetcher()
        assignFromUser(setOf(tp0))
        subscriptions.seek(tp0, 0)
        var partitionLeaderEpoch = 1
        val buffer = ByteBuffer.allocate(1024)
        var builder = MemoryRecords.builder(
            buffer = buffer,
            magic = RecordBatch.CURRENT_MAGIC_VALUE,
            compressionType = CompressionType.NONE,
            timestampType = TimestampType.CREATE_TIME,
            baseOffset = 0L,
            logAppendTime = System.currentTimeMillis(),
            producerId = partitionLeaderEpoch.toLong(),
        )
        builder.append(
            timestamp = 0L,
            key = "key".toByteArray(),
            value = partitionLeaderEpoch.toString().toByteArray(),
        )
        builder.append(
            timestamp = 0L,
            key = "key".toByteArray(),
            value = partitionLeaderEpoch.toString().toByteArray(),
        )
        builder.close()
        partitionLeaderEpoch += 7
        builder = MemoryRecords.builder(
            buffer = buffer,
            magic = RecordBatch.CURRENT_MAGIC_VALUE,
            compressionType = CompressionType.NONE,
            timestampType = TimestampType.CREATE_TIME,
            baseOffset = 2L,
            logAppendTime = System.currentTimeMillis(),
            producerId = partitionLeaderEpoch.toLong(),
        )
        builder.append(
            timestamp = 0L,
            key = "key".toByteArray(),
            value = partitionLeaderEpoch.toString().toByteArray(),
        )
        builder.close()
        partitionLeaderEpoch += 5
        builder = MemoryRecords.builder(
            buffer = buffer,
            magic = RecordBatch.CURRENT_MAGIC_VALUE,
            compressionType = CompressionType.NONE,
            timestampType = TimestampType.CREATE_TIME,
            baseOffset = 3L,
            logAppendTime = System.currentTimeMillis(),
            producerId = partitionLeaderEpoch.toLong(),
        )
        builder.append(
            timestamp = 0L,
            key = "key".toByteArray(),
            value = partitionLeaderEpoch.toString().toByteArray(),
        )
        builder.append(
            timestamp = 0L,
            key = "key".toByteArray(),
            value = partitionLeaderEpoch.toString().toByteArray(),
        )
        builder.append(
            timestamp = 0L,
            key = "key".toByteArray(),
            value = partitionLeaderEpoch.toString().toByteArray(),
        )
        builder.close()
        buffer.flip()
        val records = MemoryRecords.readableRecords(buffer)
        assertEquals(1, fetcher.sendFetches())
        assertFalse(fetcher.hasCompletedFetches())
        client!!.prepareResponse(
            fullFetchResponse(
                tp = tidp0,
                records = records,
                error = Errors.NONE,
                hw = 100L,
                throttleTime = 0,
            )
        )
        consumerClient!!.poll(time.timer(0))
        assertTrue(fetcher.hasCompletedFetches())
        val partitionRecords = fetchedRecords<ByteArray, ByteArray>()
        assertTrue(partitionRecords.containsKey(tp0))
        assertEquals(6, partitionRecords[tp0]!!.size)
        for (record in partitionRecords[tp0]!!) {
            val expectedLeaderEpoch = utf8(record.value).toInt()
            assertEquals(expectedLeaderEpoch, record.leaderEpoch)
        }
    }

    @Test
    fun testClearBufferedDataForTopicPartitions() {
        buildFetcher()
        assignFromUser(setOf(tp0))
        subscriptions.seek(tp0, 0)

        // normal fetch
        assertEquals(1, fetcher.sendFetches())
        assertFalse(fetcher.hasCompletedFetches())
        client!!.prepareResponse(
            response = fullFetchResponse(
                tp = tidp0,
                records = records,
                error = Errors.NONE,
                hw = 100L,
                throttleTime = 0,
            ),
        )
        consumerClient!!.poll(time.timer(0))
        assertTrue(fetcher.hasCompletedFetches())
        val newAssignedTopicPartitions: MutableSet<TopicPartition> = HashSet()
        newAssignedTopicPartitions.add(tp1)
        fetcher.clearBufferedDataForUnassignedPartitions(newAssignedTopicPartitions)
        assertFalse(fetcher.hasCompletedFetches())
    }

    @Test
    fun testFetchSkipsBlackedOutNodes() {
        buildFetcher()
        assignFromUser(setOf(tp0))
        subscriptions.seek(tp0, 0)
        val node = initialUpdateResponse.brokers().iterator().next()
        client!!.backoff(node, 500)

        assertEquals(0, fetcher.sendFetches())

        time.sleep(500)

        assertEquals(1, fetcher.sendFetches())
    }

    @Test
    fun testFetcherIgnoresControlRecords() {
        buildFetcher()
        assignFromUser(setOf(tp0))
        subscriptions.seek(tp0, 0)

        // normal fetch
        assertEquals(1, fetcher.sendFetches())
        assertFalse(fetcher.hasCompletedFetches())

        val producerId: Long = 1
        val producerEpoch: Short = 0
        val baseSequence = 0
        val partitionLeaderEpoch = 0
        val buffer = ByteBuffer.allocate(1024)
        val builder = MemoryRecords.idempotentBuilder(
            buffer = buffer,
            compressionType = CompressionType.NONE,
            baseOffset = 0L,
            producerId = producerId,
            producerEpoch = producerEpoch,
            baseSequence = baseSequence,
        )
        builder.append(0L, "key".toByteArray(), null)
        builder.close()
        MemoryRecords.writeEndTransactionalMarker(
            buffer = buffer,
            initialOffset = 1L,
            timestamp = time.milliseconds(),
            partitionLeaderEpoch = partitionLeaderEpoch,
            producerId = producerId,
            producerEpoch = producerEpoch,
            marker = EndTransactionMarker(ControlRecordType.ABORT, 0),
        )
        buffer.flip()
        client!!.prepareResponse(
            fullFetchResponse(
                tp = tidp0,
                records = MemoryRecords.readableRecords(buffer),
                error = Errors.NONE,
                hw = 100L,
                throttleTime = 0,
            )
        )
        consumerClient!!.poll(time.timer(0))
        assertTrue(fetcher.hasCompletedFetches())
        val partitionRecords = fetchedRecords<ByteArray, ByteArray>()
        assertTrue(partitionRecords.containsKey(tp0))
        val records = partitionRecords[tp0]!!
        assertEquals(1, records.size)
        assertEquals(2L, subscriptions.position(tp0)!!.offset)
        assertContentEquals("key".toByteArray(), records[0].key)
    }

    @Test
    fun testFetchError() {
        buildFetcher()
        assignFromUser(setOf(tp0))
        subscriptions.seek(tp0, 0)
        assertEquals(1, fetcher.sendFetches())
        assertFalse(fetcher.hasCompletedFetches())
        client!!.prepareResponse(
            fullFetchResponse(
                tp = tidp0,
                records = records,
                error = Errors.NOT_LEADER_OR_FOLLOWER,
                hw = 100L,
                throttleTime = 0,
            )
        )
        consumerClient!!.poll(time.timer(0))
        assertTrue(fetcher.hasCompletedFetches())
        val partitionRecords = fetchedRecords<ByteArray, ByteArray>()
        assertFalse(partitionRecords.containsKey(tp0))
    }

    private fun matchesOffset(tp: TopicIdPartition, offset: Long): RequestMatcher {
        return RequestMatcher { body ->
            assertIs<FetchRequest>(body)
            val fetchData = body.fetchData(topicNames)
            fetchData!!.containsKey(tp) && fetchData[tp]!!.fetchOffset == offset
        }
    }

    @Test
    fun testFetchedRecordsRaisesOnSerializationErrors() {
        // raise an exception from somewhere in the middle of the fetch response
        // so that we can verify that our position does not advance after raising
        val deserializer: ByteArrayDeserializer = object : ByteArrayDeserializer() {
            var i = 0
            override fun deserialize(topic: String, data: ByteArray?): ByteArray? {
                if (i++ % 2 == 1) {
                    // Should be blocked on the value deserialization of the first record.
                    assertEquals("value-1", String(data!!, StandardCharsets.UTF_8))
                    throw SerializationException()
                }
                return data
            }
        }
        buildFetcher(
            keyDeserializer = deserializer,
            valueDeserializer = deserializer,
        )
        assignFromUser(setOf(tp0))
        subscriptions.seek(tp0, 1)
        client!!.prepareResponse(
            matcher = matchesOffset(tidp0, 1),
            response = fullFetchResponse(
                tp = tidp0,
                records = records,
                error = Errors.NONE,
                hw = 100L,
                throttleTime = 0,
            )
        )
        assertEquals(1, fetcher.sendFetches())
        consumerClient!!.poll(time.timer(0))
        // The fetcher should block on Deserialization error
        for (i in 0..1) {
            try {
                fetcher.collectFetch()
                fail("fetchedRecords should have raised")
            } catch (_: SerializationException) {
                // the position should not advance since no data has been returned
                assertEquals(1, subscriptions.position(tp0)!!.offset)
            }
        }
    }

    @Test
    @Throws(Exception::class)
    fun testParseCorruptedRecord() {
        buildFetcher()
        assignFromUser(setOf(tp0))
        val buffer = ByteBuffer.allocate(1024)
        val out = DataOutputStream(ByteBufferOutputStream(buffer))
        val magic = RecordBatch.MAGIC_VALUE_V1
        val key = "foo".toByteArray()
        val value = "baz".toByteArray()
        val offset: Long = 0
        val timestamp = 500L
        val size = LegacyRecord.recordSize(magic, key.size, value.size)
        val attributes = LegacyRecord.computeAttributes(magic, CompressionType.NONE, TimestampType.CREATE_TIME)
        val crc = LegacyRecord.computeChecksum(magic, attributes, timestamp, key, value)

        // write one valid record
        out.writeLong(offset)
        out.writeInt(size)
        LegacyRecord.write(
            out = out,
            magic = magic,
            crc = crc,
            attributes = LegacyRecord.computeAttributes(
                magic = magic,
                type = CompressionType.NONE,
                timestampType = TimestampType.CREATE_TIME,
            ),
            timestamp = timestamp,
            key = key,
            value = value,
        )

        // and one invalid record (note the crc)
        out.writeLong(offset + 1)
        out.writeInt(size)
        LegacyRecord.write(
            out = out,
            magic = magic,
            crc = crc + 1,
            attributes = LegacyRecord.computeAttributes(
                magic = magic,
                type = CompressionType.NONE,
                timestampType = TimestampType.CREATE_TIME,
            ),
            timestamp = timestamp,
            key = key,
            value = value
        )

        // write one valid record
        out.writeLong(offset + 2)
        out.writeInt(size)
        LegacyRecord.write(
            out = out,
            magic = magic,
            crc = crc,
            attributes = LegacyRecord.computeAttributes(
                magic = magic,
                type = CompressionType.NONE,
                timestampType = TimestampType.CREATE_TIME,
            ),
            timestamp = timestamp,
            key = key,
            value = value
        )

        // Write a record whose size field is invalid.
        out.writeLong(offset + 3)
        out.writeInt(1)

        // write one valid record
        out.writeLong(offset + 4)
        out.writeInt(size)
        LegacyRecord.write(
            out = out,
            magic = magic,
            crc = crc,
            attributes = LegacyRecord.computeAttributes(
                magic = magic,
                type = CompressionType.NONE,
                timestampType = TimestampType.CREATE_TIME,
            ),
            timestamp = timestamp,
            key = key,
            value = value
        )
        buffer.flip()
        subscriptions.seekUnvalidated(
            tp = tp0,
            position = FetchPosition(
                offset = 0,
                offsetEpoch = null,
                currentLeader = metadata!!.currentLeader(tp0),
            )
        )

        // normal fetch
        assertEquals(1, fetcher.sendFetches())
        client!!.prepareResponse(
            fullFetchResponse(
                tp = tidp0,
                records = MemoryRecords.readableRecords(buffer),
                error = Errors.NONE,
                hw = 100L,
                throttleTime = 0,
            )
        )
        consumerClient!!.poll(time.timer(0))

        // the first fetchedRecords() should return the first valid message
        assertEquals(1, fetchedRecords<Any, Any>()[tp0]!!.size)
        assertEquals(1, subscriptions.position(tp0)!!.offset)
        ensureBlockOnRecord(1L)
        seekAndConsumeRecord(buffer, 2L)
        ensureBlockOnRecord(3L)

        // For a record that cannot be retrieved from the iterator, we cannot seek over it within the batch.
        assertFailsWith<KafkaException>(
            message = "Should have thrown exception when fail to retrieve a record from iterator.",
        ) { (seekAndConsumeRecord(buffer, 4L)) }
        ensureBlockOnRecord(4L)
    }

    private fun ensureBlockOnRecord(blockedOffset: Long) {
        // the fetchedRecords() should always throw exception due to the invalid message at the starting offset.
        for (i in 0..1) {
            try {
                fetcher.collectFetch()
                fail("fetchedRecords should have raised KafkaException")
            } catch (e: KafkaException) {
                assertEquals(blockedOffset, subscriptions.position(tp0)!!.offset)
            }
        }
    }

    private fun seekAndConsumeRecord(responseBuffer: ByteBuffer, toOffset: Long) {
        // Seek to skip the bad record and fetch again.
        subscriptions.seekUnvalidated(
            tp = tp0,
            position = FetchPosition(
                offset = toOffset,
                offsetEpoch = null,
                currentLeader = metadata!!.currentLeader(tp0),
            )
        )
        // Should not throw exception after the seek.
        fetcher.collectFetch()
        assertEquals(1, fetcher.sendFetches())
        client!!.prepareResponse(
            fullFetchResponse(
                tp = tidp0,
                records = MemoryRecords.readableRecords(responseBuffer),
                error = Errors.NONE,
                hw = 100L,
                throttleTime = 0,
            )
        )
        consumerClient!!.poll(time.timer(0))
        val recordsByPartition = fetchedRecords<ByteArray, ByteArray>()
        val records = recordsByPartition[tp0]!!
        assertEquals(1, records.size)
        assertEquals(toOffset, records[0].offset)
        assertEquals(toOffset + 1, subscriptions.position(tp0)!!.offset)
    }

    @Test
    fun testInvalidDefaultRecordBatch() {
        buildFetcher()
        val buffer = ByteBuffer.allocate(1024)
        val out = ByteBufferOutputStream(buffer)
        val builder = MemoryRecordsBuilder(
            bufferStream = out,
            magic = RecordBatch.CURRENT_MAGIC_VALUE,
            compressionType = CompressionType.NONE,
            timestampType = TimestampType.CREATE_TIME,
            baseOffset = 0L,
            logAppendTime = 10L,
            producerId = 0L,
            producerEpoch = 0.toShort(),
            baseSequence = 0,
            isTransactional = false,
            isControlBatch = false,
            partitionLeaderEpoch = 0,
            writeLimit = 1024
        )
        builder.append(
            timestamp = 10L,
            key = "key".toByteArray(),
            value = "value".toByteArray(),
        )
        builder.close()
        buffer.flip()

        // Garble the CRC
        buffer.position(17)
        buffer.put("beef".toByteArray())
        buffer.position(0)
        assignFromUser(setOf(tp0))
        subscriptions.seek(tp0, 0)

        // normal fetch
        assertEquals(1, fetcher.sendFetches())
        client!!.prepareResponse(
            fullFetchResponse(
                tp = tidp0,
                records = MemoryRecords.readableRecords(buffer),
                error = Errors.NONE,
                hw = 100L,
                throttleTime = 0,
            )
        )
        consumerClient!!.poll(time.timer(0))

        // the fetchedRecords() should always throw exception due to the bad batch.
        for (i in 0..1) {
            try {
                fetcher.collectFetch()
                fail("fetchedRecords should have raised KafkaException")
            } catch (_: KafkaException) {
                assertEquals(0, subscriptions.position(tp0)!!.offset)
            }
        }
    }

    @Test
    fun testParseInvalidRecordBatch() {
        buildFetcher()
        val records = MemoryRecords.withRecords(
            magic = RecordBatch.MAGIC_VALUE_V2,
            initialOffset = 0L,
            compressionType = CompressionType.NONE,
            timestampType = TimestampType.CREATE_TIME,
            records = arrayOf(
                SimpleRecord(1L, "a".toByteArray(), "1".toByteArray()),
                SimpleRecord(2L, "b".toByteArray(), "2".toByteArray()),
                SimpleRecord(3L, "c".toByteArray(), "3".toByteArray()),
            ),
        )
        val buffer = records.buffer()

        // flip some bits to fail the crc
        buffer.putInt(32, buffer[32].toInt() xor 87238423)
        assignFromUser(setOf(tp0))
        subscriptions.seek(tp0, 0)

        // normal fetch
        assertEquals(1, fetcher.sendFetches())
        client!!.prepareResponse(
            fullFetchResponse(
                tp = tidp0,
                records = MemoryRecords.readableRecords(buffer),
                error = Errors.NONE,
                hw = 100L,
                throttleTime = 0,
            )
        )
        consumerClient!!.poll(time.timer(0))
        try {
            fetcher.collectFetch()
            fail("fetchedRecords should have raised")
        } catch (e: KafkaException) {
            // the position should not advance since no data has been returned
            assertEquals(0, subscriptions.position(tp0)!!.offset)
        }
    }

    @Test
    fun testHeaders() {
        buildFetcher()
        val builder = MemoryRecords.builder(
            buffer = ByteBuffer.allocate(1024),
            compressionType = CompressionType.NONE,
            timestampType = TimestampType.CREATE_TIME,
            baseOffset = 1L,
        )
        builder.append(0L, "key".toByteArray(), "value-1".toByteArray())
        val headersArray = arrayOf<Header>(RecordHeader("headerKey", "headerValue".toByteArray(StandardCharsets.UTF_8)))
        builder.append(
            timestamp = 0L,
            key = "key".toByteArray(),
            value = "value-2".toByteArray(),
            headers = headersArray,
        )
        val headersArray2 = arrayOf<Header>(
            RecordHeader("headerKey", "headerValue".toByteArray(StandardCharsets.UTF_8)),
            RecordHeader("headerKey", "headerValue2".toByteArray(StandardCharsets.UTF_8)),
        )
        builder.append(
            timestamp = 0L,
            key = "key".toByteArray(),
            value = "value-3".toByteArray(),
            headers = headersArray2,
        )
        val memoryRecords = builder.build()
        val records: List<ConsumerRecord<ByteArray, ByteArray>>
        assignFromUser(setOf(tp0))
        subscriptions.seek(tp0, 1)
        client!!.prepareResponse(
            matcher = matchesOffset(tidp0, 1),
            response = fullFetchResponse(
                tp = tidp0,
                records = memoryRecords,
                error = Errors.NONE,
                hw = 100L,
                throttleTime = 0,
            ),
        )
        assertEquals(1, fetcher.sendFetches())
        consumerClient!!.poll(time.timer(0))
        val recordsByPartition = fetchedRecords<ByteArray, ByteArray>()
        records = recordsByPartition[tp0]!!
        assertEquals(3, records.size)
        val recordIterator = records.iterator()
        var record = recordIterator.next()
        assertNull(record.headers.lastHeader("headerKey"))
        record = recordIterator.next()
        assertEquals(
            expected = "headerValue",
            actual = String(record.headers.lastHeader("headerKey")!!.value!!, StandardCharsets.UTF_8),
        )
        assertEquals("headerKey", record.headers.lastHeader("headerKey")!!.key)
        record = recordIterator.next()
        assertEquals(
            expected = "headerValue2",
            actual = String(record.headers.lastHeader("headerKey")!!.value!!, StandardCharsets.UTF_8),
        )
        assertEquals("headerKey", record.headers.lastHeader("headerKey")!!.key)
    }

    @Test
    fun testFetchMaxPollRecords() {
        buildFetcher(maxPollRecords = 2)
        var records: List<ConsumerRecord<ByteArray?, ByteArray?>>
        assignFromUser(setOf(tp0))
        subscriptions.seek(tp0, 1)
        client!!.prepareResponse(
            matcher = matchesOffset(tidp0, 1),
            response = fullFetchResponse(
                tp = tidp0,
                records = this.records,
                error = Errors.NONE,
                hw = 100L,
                throttleTime = 0,
            ),
        )
        client!!.prepareResponse(
            matcher = matchesOffset(tidp0, 4),
            response = fullFetchResponse(
                tp = tidp0,
                records = nextRecords,
                error = Errors.NONE,
                hw = 100L,
                throttleTime = 0,
            )
        )
        assertEquals(1, fetcher.sendFetches())
        consumerClient!!.poll(time.timer(0))
        var recordsByPartition = fetchedRecords<ByteArray?, ByteArray?>()
        records = recordsByPartition[tp0]!!
        assertEquals(2, records.size)
        assertEquals(3L, subscriptions.position(tp0)!!.offset)
        assertEquals(1, records[0].offset)
        assertEquals(2, records[1].offset)
        assertEquals(0, fetcher.sendFetches())
        consumerClient!!.poll(time.timer(0))
        recordsByPartition = fetchedRecords()
        records = recordsByPartition[tp0]!!
        assertEquals(1, records.size)
        assertEquals(4L, subscriptions.position(tp0)!!.offset)
        assertEquals(3, records[0].offset)
        assertTrue(fetcher.sendFetches() > 0)
        consumerClient!!.poll(time.timer(0))
        recordsByPartition = fetchedRecords()
        records = recordsByPartition[tp0]!!
        assertEquals(2, records.size)
        assertEquals(6L, subscriptions.position(tp0)!!.offset)
        assertEquals(4, records[0].offset)
        assertEquals(5, records[1].offset)
    }

    /**
     * Test the scenario where a partition with fetched but not consumed records (i.e. max.poll.records is
     * less than the number of fetched records) is unassigned and a different partition is assigned. This is a
     * pattern used by Streams state restoration and KAFKA-5097 would have been caught by this test.
     */
    @Test
    fun testFetchAfterPartitionWithFetchedRecordsIsUnassigned() {
        buildFetcher(maxPollRecords = 2)
        var records: List<ConsumerRecord<ByteArray, ByteArray>>
        assignFromUser(setOf(tp0))
        subscriptions.seek(tp0, 1)

        // Returns 3 records while `max.poll.records` is configured to 2
        client!!.prepareResponse(
            matcher = matchesOffset(tidp0, 1),
            response = fullFetchResponse(
                tp = tidp0,
                records = this.records,
                error = Errors.NONE,
                hw = 100L,
                throttleTime = 0,
            ),
        )
        assertEquals(1, fetcher.sendFetches())
        consumerClient!!.poll(time.timer(0))
        val recordsByPartition = fetchedRecords<ByteArray, ByteArray>()
        records = recordsByPartition[tp0]!!
        assertEquals(2, records.size)
        assertEquals(3L, subscriptions.position(tp0)!!.offset)
        assertEquals(1, records[0].offset)
        assertEquals(2, records[1].offset)
        assignFromUser(setOf(tp1))
        client!!.prepareResponse(
            matcher = matchesOffset(tidp1, 4),
            response = fullFetchResponse(
                tp = tidp1,
                records = nextRecords,
                error = Errors.NONE,
                hw = 100L,
                throttleTime = 0,
            ),
        )
        subscriptions.seek(tp1, 4)
        assertEquals(1, fetcher.sendFetches())
        consumerClient!!.poll(time.timer(0))
        val fetchedRecords = fetchedRecords<ByteArray, ByteArray>()
        assertNull(fetchedRecords[tp0])
        records = fetchedRecords[tp1]!!
        assertEquals(2, records.size)
        assertEquals(6L, subscriptions.position(tp1)!!.offset)
        assertEquals(4, records[0].offset)
        assertEquals(5, records[1].offset)
    }

    @Test
    fun testFetchNonContinuousRecords() {
        // if we are fetching from a compacted topic, there may be gaps in the returned records
        // this test verifies the fetcher updates the current fetched/consumed positions correctly for this case
        buildFetcher()
        val builder: MemoryRecordsBuilder = MemoryRecords.builder(
            buffer = ByteBuffer.allocate(1024),
            compressionType = CompressionType.NONE,
            timestampType = TimestampType.CREATE_TIME,
            baseOffset = 0L,
        )
        builder.appendWithOffset(15L, 0L, "key".toByteArray(), "value-1".toByteArray())
        builder.appendWithOffset(20L, 0L, "key".toByteArray(), "value-2".toByteArray())
        builder.appendWithOffset(30L, 0L, "key".toByteArray(), "value-3".toByteArray())
        val records = builder.build()
        val consumerRecords: List<ConsumerRecord<ByteArray, ByteArray>>
        assignFromUser(setOf(tp0))
        subscriptions.seek(tp0, 0)

        // normal fetch
        assertEquals(1, fetcher.sendFetches())
        client!!.prepareResponse(
            response = fullFetchResponse(
                tp = tidp0,
                records = records,
                error = Errors.NONE,
                hw = 100L,
                throttleTime = 0,
            ),
        )
        consumerClient!!.poll(time.timer(0))
        val recordsByPartition = fetchedRecords<ByteArray, ByteArray>()
        consumerRecords = recordsByPartition[tp0]!!
        assertEquals(3, consumerRecords.size)
        assertEquals(31L, subscriptions.position(tp0)!!.offset) // this is the next fetching position
        assertEquals(15L, consumerRecords[0].offset)
        assertEquals(20L, consumerRecords[1].offset)
        assertEquals(30L, consumerRecords[2].offset)
    }

    /**
     * Test the case where the client makes a pre-v3 FetchRequest, but the server replies with only a partial
     * request. This happens when a single message is larger than the per-partition limit.
     */
    @Test
    fun testFetchRequestWhenRecordTooLarge() {
        try {
            buildFetcher()
            client!!.setNodeApiVersions(NodeApiVersions.create(ApiKeys.FETCH.id, 2.toShort(), 2.toShort()))
            makeFetchRequestWithIncompleteRecord()
            try {
                fetcher.collectFetch()
                fail("RecordTooLargeException should have been raised")
            } catch (e: RecordTooLargeException) {
                assertTrue(e.message!!.startsWith("There are some messages at [Partition=Offset]: "))
                // the position should not advance since no data has been returned
                assertEquals(0, subscriptions.position(tp0)!!.offset)
            }
        } finally {
            client!!.setNodeApiVersions(NodeApiVersions.create())
        }
    }

    /**
     * Test the case where the client makes a post KIP-74 FetchRequest, but the server replies with only a
     * partial request. For v3 and later FetchRequests, the implementation of KIP-74 changed the behavior
     * so that at least one message is always returned. Therefore, this case should not happen, and it indicates
     * that an internal error has taken place.
     */
    @Test
    fun testFetchRequestInternalError() {
        buildFetcher()
        makeFetchRequestWithIncompleteRecord()
        try {
            fetcher.collectFetch()
            fail("RecordTooLargeException should have been raised")
        } catch (e: KafkaException) {
            assertTrue(e.message!!.startsWith("Failed to make progress reading messages"))
            // the position should not advance since no data has been returned
            assertEquals(0, subscriptions.position(tp0)!!.offset)
        }
    }

    private fun makeFetchRequestWithIncompleteRecord() {
        assignFromUser(setOf(tp0))
        subscriptions.seek(tp0, 0)
        assertEquals(1, fetcher.sendFetches())
        assertFalse(fetcher.hasCompletedFetches())
        val partialRecord = MemoryRecords.readableRecords(
            ByteBuffer.wrap(ByteArray(8) { 0 })
        )
        client!!.prepareResponse(
            fullFetchResponse(
                tp = tidp0,
                records = partialRecord,
                error = Errors.NONE,
                hw = 100L,
                throttleTime = 0
            )
        )
        consumerClient!!.poll(time.timer(0))
        assertTrue(fetcher.hasCompletedFetches())
    }

    @Test
    fun testUnauthorizedTopic() {
        buildFetcher()
        assignFromUser(setOf(tp0))
        subscriptions.seek(tp0, 0)

        // resize the limit of the buffer to pretend it is only fetch-size large
        assertEquals(1, fetcher.sendFetches())
        client!!.prepareResponse(
            fullFetchResponse(
                tp = tidp0,
                records = records,
                error = Errors.TOPIC_AUTHORIZATION_FAILED,
                hw = 100L,
                throttleTime = 0,
            )
        )
        consumerClient!!.poll(time.timer(0))
        try {
            fetcher.collectFetch()
            fail("fetchedRecords should have thrown")
        } catch (e: TopicAuthorizationException) {
            assertEquals(setOf(topicName), e.unauthorizedTopics)
        }
    }

    @Test
    fun testFetchDuringEagerRebalance() {
        buildFetcher()
        subscriptions.subscribe(setOf(topicName), listener)
        subscriptions.assignFromSubscribed(setOf(tp0))
        subscriptions.seek(tp0, 0)
        client!!.updateMetadata(
            metadataUpdateWithIds(
                numNodes = 1,
                topicPartitionCounts = mapOf(topicName to 4),
                epochSupplier = { validLeaderEpoch },
                topicIds = topicIds,
            )
        )
        assertEquals(1, fetcher.sendFetches())

        // Now the eager rebalance happens and fetch positions are cleared
        subscriptions.assignFromSubscribed(emptyList())
        subscriptions.assignFromSubscribed(setOf(tp0))
        client!!.prepareResponse(
            fullFetchResponse(
                tp = tidp0,
                records = records,
                error = Errors.NONE,
                hw = 100L,
                throttleTime = 0
            )
        )
        consumerClient!!.poll(time.timer(0))

        // The active fetch should be ignored since its position is no longer valid
        assertTrue(fetchedRecords<Any, Any>().isEmpty())
    }

    @Test
    fun testFetchDuringCooperativeRebalance() {
        buildFetcher()
        subscriptions.subscribe(setOf(topicName), listener)
        subscriptions.assignFromSubscribed(setOf(tp0))
        subscriptions.seek(tp0, 0)
        client!!.updateMetadata(
            metadataUpdateWithIds(
                numNodes = 1,
                topicPartitionCounts = mapOf(topicName to 4),
                epochSupplier = { validLeaderEpoch },
                topicIds = topicIds,
            )
        )
        assertEquals(1, fetcher.sendFetches())

        // Now the cooperative rebalance happens and fetch positions are NOT cleared for unrevoked partitions
        subscriptions.assignFromSubscribed(setOf(tp0))
        client!!.prepareResponse(
            fullFetchResponse(
                tp = tidp0,
                records = records,
                error = Errors.NONE,
                hw = 100L,
                throttleTime = 0,
            )
        )
        consumerClient!!.poll(time.timer(0))
        val fetchedRecords = fetchedRecords<ByteArray, ByteArray>()

        // The active fetch should NOT be ignored since the position for tp0 is still valid
        assertEquals(1, fetchedRecords.size)
        assertEquals(3, fetchedRecords[tp0]!!.size)
    }

    @Test
    fun testInFlightFetchOnPausedPartition() {
        buildFetcher()
        assignFromUser(setOf(tp0))
        subscriptions.seek(tp0, 0)
        assertEquals(1, fetcher.sendFetches())
        subscriptions.pause(tp0)
        client!!.prepareResponse(
            fullFetchResponse(
                tp = tidp0,
                records = records,
                error = Errors.NONE,
                hw = 100L,
                throttleTime = 0,
            )
        )
        consumerClient!!.poll(time.timer(0))
        assertNull(fetchedRecords<Any, Any>()[tp0])
    }

    @Test
    fun testFetchOnPausedPartition() {
        buildFetcher()
        assignFromUser(setOf(tp0))
        subscriptions.seek(tp0, 0)
        subscriptions.pause(tp0)
        assertFalse(fetcher.sendFetches() > 0)
        assertTrue(client!!.requests().isEmpty())
    }

    @Test
    fun testFetchOnCompletedFetchesForPausedAndResumedPartitions() {
        buildFetcher()
        assignFromUser(setOf(tp0))
        subscriptions.seek(tp0, 0)
        assertEquals(1, fetcher.sendFetches())
        subscriptions.pause(tp0)
        client!!.prepareResponse(
            fullFetchResponse(
                tp = tidp0,
                records = records,
                error = Errors.NONE,
                hw = 100L,
                throttleTime = 0,
            )
        )
        consumerClient!!.poll(time.timer(0))

        assertEmptyFetch("Should not return any records or advance position when partition is paused")
        assertTrue(fetcher.hasCompletedFetches(), "Should still contain completed fetches")
        assertFalse(
            fetcher.hasAvailableFetches(),
            "Should not have any available (non-paused) completed fetches"
        )
        assertEquals(0, fetcher.sendFetches())

        subscriptions.resume(tp0)

        assertTrue(fetcher.hasAvailableFetches(), "Should have available (non-paused) completed fetches")

        consumerClient!!.poll(time.timer(0))
        val fetchedRecords = fetchedRecords<ByteArray, ByteArray>()

        assertEquals(1, fetchedRecords.size, "Should return records when partition is resumed")
        assertNotNull(fetchedRecords[tp0])
        assertEquals(3, fetchedRecords[tp0]!!.size)

        consumerClient!!.poll(time.timer(0))

        assertEmptyFetch("Should not return records or advance position after previously paused partitions are fetched")
        assertFalse(fetcher.hasCompletedFetches(), "Should no longer contain completed fetches")
    }

    @Test
    fun testFetchOnCompletedFetchesForSomePausedPartitions() {
        buildFetcher()
        assignFromUser(setOf(tp0, tp1))

        // seek to tp0 and tp1 in two polls to generate 2 complete requests and responses

        // #1 seek, request, poll, response
        subscriptions.seekUnvalidated(tp0, FetchPosition(1, null, metadata!!.currentLeader(tp0)))
        assertEquals(1, fetcher.sendFetches())
        client!!.prepareResponse(
            response = fullFetchResponse(
                tp = tidp0,
                records = records,
                error = Errors.NONE,
                hw = 100L,
                throttleTime = 0,
            ),
        )
        consumerClient!!.poll(time.timer(0))

        // #2 seek, request, poll, response
        subscriptions.seekUnvalidated(
            tp = tp1,
            position = FetchPosition(
                offset = 1,
                offsetEpoch = null,
                currentLeader = metadata!!.currentLeader(tp1),
            ),
        )

        assertEquals(1, fetcher.sendFetches())

        client!!.prepareResponse(
            fullFetchResponse(
                tp = tidp1,
                records = nextRecords,
                error = Errors.NONE,
                hw = 100L,
                throttleTime = 0,
            )
        )
        subscriptions.pause(tp0)
        consumerClient!!.poll(time.timer(0))
        val fetchedRecords = fetchedRecords<ByteArray, ByteArray>()

        assertEquals(1, fetchedRecords.size, "Should return completed fetch for unpaused partitions")
        assertTrue(fetcher.hasCompletedFetches(), "Should still contain completed fetches")
        assertNotNull(fetchedRecords[tp1])
        assertNull(fetchedRecords[tp0])
        assertEmptyFetch("Should not return records or advance position for remaining paused partition")
        assertTrue(fetcher.hasCompletedFetches(), "Should still contain completed fetches")
    }

    @Test
    fun testFetchOnCompletedFetchesForAllPausedPartitions() {
        buildFetcher()
        var fetchedRecords: Map<TopicPartition?, List<ConsumerRecord<ByteArray?, ByteArray?>?>?>
        assignFromUser(setOf(tp0, tp1))

        // seek to tp0 and tp1 in two polls to generate 2 complete requests and responses

        // #1 seek, request, poll, response
        subscriptions.seekUnvalidated(tp0, FetchPosition(1, null, metadata!!.currentLeader(tp0)))
        assertEquals(1, fetcher.sendFetches())
        client!!.prepareResponse(
            response = fullFetchResponse(
                tp = tidp0,
                records = records,
                error = Errors.NONE,
                hw = 100L,
                throttleTime = 0,
            ),
        )
        consumerClient!!.poll(time.timer(0))

        // #2 seek, request, poll, response
        subscriptions.seekUnvalidated(tp1, FetchPosition(1, null, metadata!!.currentLeader(tp1)))
        assertEquals(1, fetcher.sendFetches())
        client!!.prepareResponse(
            fullFetchResponse(
                tp = tidp1,
                records = nextRecords,
                error = Errors.NONE,
                hw = 100L,
                throttleTime = 0,
            )
        )
        subscriptions.pause(tp0)
        subscriptions.pause(tp1)
        consumerClient!!.poll(time.timer(0))
        assertEmptyFetch("Should not return records or advance position for all paused partitions")
        assertTrue(fetcher.hasCompletedFetches(), "Should still contain completed fetches")
        assertFalse(
            fetcher.hasAvailableFetches(),
            "Should not have any available (non-paused) completed fetches"
        )
    }

    @Test
    fun testPartialFetchWithPausedPartitions() {
        // this test sends creates a completed fetch with 3 records and a max poll of 2 records to assert
        // that a fetch that must be returned over at least 2 polls can be cached successfully when its partition is
        // paused, then returned successfully after its been resumed again later
        buildFetcher(maxPollRecords = 2)
        assignFromUser(setOf(tp0, tp1))
        subscriptions.seek(tp0, 1)
        assertEquals(1, fetcher.sendFetches())
        client!!.prepareResponse(
            response = fullFetchResponse(
                tp = tidp0,
                records = records,
                error = Errors.NONE,
                hw = 100L,
                throttleTime = 0,
            ),
        )
        consumerClient!!.poll(time.timer(0))
        var fetchedRecords = fetchedRecords<ByteArray?, ByteArray?>()

        assertEquals(2, fetchedRecords[tp0]!!.size, "Should return 2 records from fetch with 3 records")
        assertFalse(fetcher.hasCompletedFetches(), "Should have no completed fetches")

        subscriptions.pause(tp0)
        consumerClient!!.poll(time.timer(0))
        fetchedRecords = fetchedRecords()

        assertEmptyFetch("Should not return records or advance position for paused partitions")
        assertTrue(fetcher.hasCompletedFetches(), "Should have 1 entry in completed fetches")
        assertFalse(
            fetcher.hasAvailableFetches(),
            "Should not have any available (non-paused) completed fetches"
        )

        subscriptions.resume(tp0)
        consumerClient!!.poll(time.timer(0))
        fetchedRecords = fetchedRecords()

        assertEquals(1, fetchedRecords[tp0]!!.size, "Should return last remaining record")
        assertFalse(fetcher.hasCompletedFetches(), "Should have no completed fetches")
    }

    @Test
    fun testFetchDiscardedAfterPausedPartitionResumedAndSeekedToNewOffset() {
        buildFetcher()
        assignFromUser(setOf(tp0))
        subscriptions.seek(tp0, 0)

        assertEquals(1, fetcher.sendFetches())

        subscriptions.pause(tp0)
        client!!.prepareResponse(
            fullFetchResponse(
                tp = tidp0,
                records = records,
                error = Errors.NONE,
                hw = 100L,
                throttleTime = 0,
            )
        )
        subscriptions.seek(tp0, 3)
        subscriptions.resume(tp0)
        consumerClient!!.poll(time.timer(0))

        assertTrue(fetcher.hasCompletedFetches(), "Should have 1 entry in completed fetches")

        val fetch = collectFetch<ByteArray, ByteArray>()

        assertEquals(
            expected = emptyMap(),
            actual = fetch.records(),
            message = "Should not return any records because we seeked to a new offset",
        )
        assertFalse(fetch.positionAdvanced())
        assertFalse(fetcher.hasCompletedFetches(), "Should have no completed fetches")
    }

    @Test
    fun testFetchNotLeaderOrFollower() {
        buildFetcher()
        assignFromUser(setOf(tp0))
        subscriptions.seek(tp0, 0)

        assertEquals(1, fetcher.sendFetches())

        client!!.prepareResponse(
            fullFetchResponse(
                tp = tidp0,
                records = records,
                error = Errors.NOT_LEADER_OR_FOLLOWER,
                hw = 100L,
                throttleTime = 0,
            )
        )
        consumerClient!!.poll(time.timer(0))
        assertEmptyFetch("Should not return records or advance position on fetch error")
        assertEquals(0L, metadata!!.timeToNextUpdate(time.milliseconds()))
    }

    @Test
    fun testFetchUnknownTopicOrPartition() {
        buildFetcher()
        assignFromUser(setOf(tp0))
        subscriptions.seek(tp0, 0)
        assertEquals(1, fetcher.sendFetches())
        client!!.prepareResponse(
            fullFetchResponse(
                tp = tidp0,
                records = records,
                error = Errors.UNKNOWN_TOPIC_OR_PARTITION,
                hw = 100L,
                throttleTime = 0,
            )
        )
        consumerClient!!.poll(time.timer(0))
        assertEmptyFetch("Should not return records or advance position on fetch error")
        assertEquals(0L, metadata!!.timeToNextUpdate(time.milliseconds()))
    }

    @Test
    fun testFetchUnknownTopicId() {
        buildFetcher()
        assignFromUser(setOf(tp0))
        subscriptions.seek(tp0, 0)

        assertEquals(1, fetcher.sendFetches())

        client!!.prepareResponse(
            fullFetchResponse(
                tp = tidp0,
                records = records,
                error = Errors.UNKNOWN_TOPIC_ID,
                hw = -1L,
                throttleTime = 0,
            )
        )
        consumerClient!!.poll(time.timer(0))

        assertEmptyFetch("Should not return records or advance position on fetch error")
        assertEquals(0L, metadata!!.timeToNextUpdate(time.milliseconds()))
    }

    @Test
    fun testFetchSessionIdError() {
        buildFetcher()
        assignFromUser(setOf(tp0))
        subscriptions.seek(tp0, 0)
        assertEquals(1, fetcher.sendFetches())
        client!!.prepareResponse(
            fetchResponseWithTopLevelError(
                tp = tidp0,
                error = Errors.FETCH_SESSION_TOPIC_ID_ERROR,
                throttleTime = 0,
            )
        )
        consumerClient!!.poll(time.timer(0))
        assertEmptyFetch("Should not return records or advance position on fetch error")
        assertEquals(0L, metadata!!.timeToNextUpdate(time.milliseconds()))
    }

    @Test
    fun testFetchInconsistentTopicId() {
        buildFetcher()
        assignFromUser(setOf(tp0))
        subscriptions.seek(tp0, 0)

        assertEquals(1, fetcher.sendFetches())

        client!!.prepareResponse(
            fullFetchResponse(
                tp = tidp0,
                records = records,
                error = Errors.INCONSISTENT_TOPIC_ID,
                hw = -1L,
                throttleTime = 0,
            )
        )
        consumerClient!!.poll(time.timer(0))

        assertEmptyFetch("Should not return records or advance position on fetch error")
        assertEquals(0L, metadata!!.timeToNextUpdate(time.milliseconds()))
    }

    @Test
    fun testFetchFencedLeaderEpoch() {
        buildFetcher()
        assignFromUser(setOf(tp0))
        subscriptions.seek(tp0, 0)

        assertEquals(1, fetcher.sendFetches())

        client!!.prepareResponse(
            fullFetchResponse(
                tp = tidp0,
                records = records,
                error = Errors.FENCED_LEADER_EPOCH,
                hw = 100L,
                throttleTime = 0
            )
        )
        consumerClient!!.poll(time.timer(0))

        assertEmptyFetch("Should not return records or advance position on fetch error")
        assertEquals(
            expected = 0L,
            actual = metadata!!.timeToNextUpdate(time.milliseconds()),
            message = "Should have requested metadata update",
        )
    }

    @Test
    fun testFetchUnknownLeaderEpoch() {
        buildFetcher()
        assignFromUser(setOf(tp0))
        subscriptions.seek(tp0, 0)

        assertEquals(1, fetcher.sendFetches())

        client!!.prepareResponse(
            fullFetchResponse(
                tp = tidp0,
                records = records,
                error = Errors.UNKNOWN_LEADER_EPOCH,
                hw = 100L,
                throttleTime = 0,
            )
        )
        consumerClient!!.poll(time.timer(0))

        assertEmptyFetch("Should not return records or advance position on fetch error")
        assertNotEquals(
            illegal = 0L,
            actual = metadata!!.timeToNextUpdate(time.milliseconds()),
            message = "Should not have requested metadata update",
        )
    }

    @Test
    fun testEpochSetInFetchRequest() {
        buildFetcher()
        subscriptions.assignFromUser(setOf(tp0))
        val metadataResponse = metadataUpdateWith(
            clusterId = "dummy",
            numNodes = 1,
            topicErrors = emptyMap(),
            topicPartitionCounts = mapOf(topicName to 4),
            epochSupplier = { 99 },
            topicIds = topicIds,
        )
        client!!.updateMetadata(metadataResponse)
        subscriptions.seek(tp0, 10)
        assertEquals(1, fetcher.sendFetches())

        // Check for epoch in outgoing request
        val matcher = RequestMatcher { body ->
            assertIs<FetchRequest>(body, "Should have seen FetchRequest")

            body.fetchData(topicNames)!!.values.forEach { (_, _, _, _, currentLeaderEpoch) ->
                assertNotNull(currentLeaderEpoch, "Expected Fetcher to set leader epoch in request")
                assertEquals(
                    expected = 99,
                    actual = currentLeaderEpoch,
                    message = "Expected leader epoch to match epoch from metadata update",
                )
            }
            return@RequestMatcher true
        }
        client!!.prepareResponse(
            matcher = matcher,
            response = fullFetchResponse(
                tp = tidp0,
                records = records,
                error = Errors.NONE,
                hw = 100L,
                throttleTime = 0,
            )
        )
        consumerClient!!.pollNoWakeup()
    }

    @Test
    fun testFetchOffsetOutOfRange() {
        buildFetcher()
        assignFromUser(setOf(tp0))
        subscriptions.seek(tp0, 0)

        assertEquals(1, fetcher.sendFetches())

        client!!.prepareResponse(
            response = fullFetchResponse(
                tp = tidp0,
                records = records,
                error = Errors.OFFSET_OUT_OF_RANGE,
                hw = 100L,
                throttleTime = 0,
            ),
        )
        consumerClient!!.poll(time.timer(0))

        assertEmptyFetch("Should not return records or advance position on fetch error")
        assertTrue(subscriptions.isOffsetResetNeeded(tp0))
        assertNull(subscriptions.validPosition(tp0))
        assertNull(subscriptions.position(tp0))
    }

    @Test
    fun testStaleOutOfRangeError() {
        // verify that an out of range error which arrives after a seek
        // does not cause us to reset our position or throw an exception
        buildFetcher()
        assignFromUser(setOf(tp0))
        subscriptions.seek(tp0, 0)

        assertEquals(1, fetcher.sendFetches())

        client!!.prepareResponse(
            response = fullFetchResponse(
                tp = tidp0,
                records = records,
                error = Errors.OFFSET_OUT_OF_RANGE,
                hw = 100L,
                throttleTime = 0,
            )
        )
        subscriptions.seek(tp0, 1)
        consumerClient!!.poll(time.timer(0))

        assertEmptyFetch("Should not return records or advance position on fetch error")
        assertFalse(subscriptions.isOffsetResetNeeded(tp0))
        assertEquals(1, subscriptions.position(tp0)!!.offset)
    }

    @Test
    fun testFetchedRecordsAfterSeek() {
        buildFetcher(
            offsetResetStrategy = OffsetResetStrategy.NONE,
            keyDeserializer = ByteArrayDeserializer(),
            valueDeserializer = ByteArrayDeserializer(),
            maxPollRecords = 2,
            isolationLevel = IsolationLevel.READ_UNCOMMITTED,
        )
        assignFromUser(setOf(tp0))
        subscriptions.seek(tp0, 0)

        assertTrue(fetcher.sendFetches() > 0)

        client!!.prepareResponse(
            response = fullFetchResponse(
                tp = tidp0,
                records = records,
                error = Errors.OFFSET_OUT_OF_RANGE,
                hw = 100L,
                throttleTime = 0,
            ),
        )
        consumerClient!!.poll(time.timer(0))

        assertFalse(subscriptions.isOffsetResetNeeded(tp0))

        subscriptions.seek(tp0, 2)

        assertEmptyFetch("Should not return records or advance position after seeking to end of topic partition")
    }

    @Test
    fun testFetchOffsetOutOfRangeException() {
        buildFetcher(
            offsetResetStrategy = OffsetResetStrategy.NONE,
            keyDeserializer = ByteArrayDeserializer(),
            valueDeserializer = ByteArrayDeserializer(),
            maxPollRecords = 2,
            isolationLevel = IsolationLevel.READ_UNCOMMITTED
        )
        assignFromUser(setOf(tp0))
        subscriptions.seek(tp0, 0)
        fetcher.sendFetches()
        client!!.prepareResponse(
            response = fullFetchResponse(
                tp = tidp0,
                records = records,
                error = Errors.OFFSET_OUT_OF_RANGE,
                hw = 100L,
                throttleTime = 0,
            ),
        )
        consumerClient!!.poll(time.timer(0))

        assertFalse(subscriptions.isOffsetResetNeeded(tp0))
        for (i in 0..1) {
            val e = assertFailsWith<OffsetOutOfRangeException> { fetcher.collectFetch() }
            assertEquals(setOf(tp0), e.offsetOutOfRangePartitions.keys)
            assertEquals(0L, e.offsetOutOfRangePartitions[tp0])
        }
    }

    @Test
    fun testFetchPositionAfterException() {
        // verify the advancement in the next fetch offset equals to the number of fetched records when
        // some fetched partitions cause Exception. This ensures that consumer won't lose record upon exception
        buildFetcher(
            offsetResetStrategy = OffsetResetStrategy.NONE,
            keyDeserializer = ByteArrayDeserializer(),
            valueDeserializer = ByteArrayDeserializer(),
            maxPollRecords = Int.MAX_VALUE,
            isolationLevel = IsolationLevel.READ_UNCOMMITTED,
        )
        assignFromUser(setOf(tp0, tp1))
        subscriptions.seek(tp0, 1)
        subscriptions.seek(tp1, 1)

        assertEquals(1, fetcher.sendFetches())

        val partitions = mapOf(
            tidp1 to FetchResponseData.PartitionData()
                .setPartitionIndex(tp1.partition)
                .setHighWatermark(100)
                .setRecords(records),
            tidp0 to FetchResponseData.PartitionData()
                .setPartitionIndex(tp0.partition)
                .setErrorCode(Errors.OFFSET_OUT_OF_RANGE.code)
                .setHighWatermark(100)
        )
        client!!.prepareResponse(
            response = FetchResponse.of(
                error = Errors.NONE,
                throttleTimeMs = 0,
                sessionId = FetchMetadata.INVALID_SESSION_ID,
                responseData = LinkedHashMap(partitions),
            )
        )
        consumerClient!!.poll(time.timer(0))
        val allFetchedRecords: MutableList<ConsumerRecord<ByteArray, ByteArray>> = ArrayList()
        fetchRecordsInto(allFetchedRecords)

        assertEquals(1, subscriptions.position(tp0)!!.offset)
        assertEquals(4, subscriptions.position(tp1)!!.offset)
        assertEquals(3, allFetchedRecords.size)
        val e = assertFailsWith<OffsetOutOfRangeException> { fetchRecordsInto(allFetchedRecords) }
        assertEquals(setOf(tp0), e.offsetOutOfRangePartitions.keys)
        assertEquals(1L, e.offsetOutOfRangePartitions[tp0])
        assertEquals(1, subscriptions.position(tp0)!!.offset)
        assertEquals(4, subscriptions.position(tp1)!!.offset)
        assertEquals(3, allFetchedRecords.size)
    }

    private fun fetchRecordsInto(allFetchedRecords: MutableList<ConsumerRecord<ByteArray, ByteArray>>) {
        val fetchedRecords = fetchedRecords<ByteArray, ByteArray>()
        fetchedRecords.values.forEach { allFetchedRecords.addAll(it) }
    }

    @Test
    fun testCompletedFetchRemoval() {
        // Ensure the removal of completed fetches that cause an Exception if and only if they contain empty records.
        buildFetcher(
            offsetResetStrategy = OffsetResetStrategy.NONE,
            keyDeserializer = ByteArrayDeserializer(),
            valueDeserializer = ByteArrayDeserializer(),
            maxPollRecords = Int.MAX_VALUE,
            isolationLevel = IsolationLevel.READ_UNCOMMITTED
        )
        assignFromUser(setOf(tp0, tp1, tp2, tp3))
        subscriptions.seek(tp0, 1)
        subscriptions.seek(tp1, 1)
        subscriptions.seek(tp2, 1)
        subscriptions.seek(tp3, 1)
        assertEquals(1, fetcher.sendFetches())
        val partitions = mapOf(
            tidp1 to FetchResponseData.PartitionData()
                .setPartitionIndex(tp1.partition)
                .setHighWatermark(100)
                .setRecords(records),
            tidp0 to FetchResponseData.PartitionData()
                .setPartitionIndex(tp0.partition)
                .setErrorCode(Errors.OFFSET_OUT_OF_RANGE.code)
                .setHighWatermark(100),
            tidp2 to FetchResponseData.PartitionData()
                .setPartitionIndex(tp2.partition)
                .setHighWatermark(100)
                .setLastStableOffset(4)
                .setLogStartOffset(0)
                .setRecords(nextRecords),
            tidp3 to FetchResponseData.PartitionData()
                .setPartitionIndex(tp3.partition)
                .setHighWatermark(100)
                .setLastStableOffset(4)
                .setLogStartOffset(0)
                .setRecords(partialRecords),
        )

        client!!.prepareResponse(
            response = FetchResponse.of(
                error = Errors.NONE,
                throttleTimeMs = 0,
                sessionId = FetchMetadata.INVALID_SESSION_ID,
                responseData = LinkedHashMap(partitions),
            )
        )
        consumerClient!!.poll(time.timer(0))
        val fetchedRecords = mutableListOf<ConsumerRecord<ByteArray, ByteArray>>()
        var recordsByPartition = fetchedRecords<ByteArray, ByteArray>()
        for (records in recordsByPartition.values) fetchedRecords.addAll(records)

        assertEquals(fetchedRecords.size.toLong(), subscriptions.position(tp1)!!.offset - 1)
        assertEquals(4, subscriptions.position(tp1)!!.offset)
        assertEquals(3, fetchedRecords.size)

        val oorExceptions = mutableListOf<OffsetOutOfRangeException>()
        try {
            recordsByPartition = fetchedRecords()
            for (records in recordsByPartition.values) fetchedRecords.addAll(records)
        } catch (oor: OffsetOutOfRangeException) {
            oorExceptions.add(oor)
        }

        // Should have received one OffsetOutOfRangeException for partition tp1
        assertEquals(1, oorExceptions.size)

        val oor = oorExceptions[0]

        assertTrue(oor.offsetOutOfRangePartitions.containsKey(tp0))
        assertEquals(oor.offsetOutOfRangePartitions.size, 1)

        recordsByPartition = fetchedRecords()
        for (records in recordsByPartition.values) fetchedRecords.addAll(records)

        // Should not have received an Exception for tp2.
        assertEquals(6, subscriptions.position(tp2)!!.offset)
        assertEquals(5, fetchedRecords.size)

        val numExceptionsExpected = 3
        val kafkaExceptions = mutableListOf<KafkaException>()
        for (i in 1..numExceptionsExpected) {
            try {
                recordsByPartition = fetchedRecords()
                for (records in recordsByPartition.values) fetchedRecords.addAll(records)
            } catch (e: KafkaException) {
                kafkaExceptions.add(e)
            }
        }
        // Should have received as much as numExceptionsExpected Kafka exceptions for tp3.
        assertEquals(numExceptionsExpected, kafkaExceptions.size)
    }

    @Test
    fun testSeekBeforeException() {
        buildFetcher(
            offsetResetStrategy = OffsetResetStrategy.NONE,
            keyDeserializer = ByteArrayDeserializer(),
            valueDeserializer = ByteArrayDeserializer(),
            maxPollRecords = 2,
            isolationLevel = IsolationLevel.READ_UNCOMMITTED,
        )
        assignFromUser(setOf(tp0))
        subscriptions.seek(tp0, 1)

        assertEquals(1, fetcher.sendFetches())

        var partitions = mapOf(
            tidp0 to FetchResponseData.PartitionData()
                .setPartitionIndex(tp0.partition)
                .setHighWatermark(100)
                .setRecords(records)
        )
        client!!.prepareResponse(
            response = fullFetchResponse(
                tp = tidp0,
                records = records,
                error = Errors.NONE,
                hw = 100L,
                throttleTime = 0,
            ),
        )
        consumerClient!!.poll(time.timer(0))

        assertEquals(2, fetchedRecords<Any, Any>()[tp0]!!.size)

        subscriptions.assignFromUser(setOf(tp0, tp1))
        subscriptions.seekUnvalidated(tp1, FetchPosition(1, null, metadata!!.currentLeader(tp1)))

        assertEquals(1, fetcher.sendFetches())

        partitions = mapOf(
            tidp1 to FetchResponseData.PartitionData()
                .setPartitionIndex(tp1.partition)
                .setErrorCode(Errors.OFFSET_OUT_OF_RANGE.code)
                .setHighWatermark(100)
        )

        client!!.prepareResponse(
            response = FetchResponse.of(
                error = Errors.NONE,
                throttleTimeMs = 0,
                sessionId = FetchMetadata.INVALID_SESSION_ID,
                responseData = LinkedHashMap(partitions),
            )
        )
        consumerClient!!.poll(time.timer(0))

        assertEquals(1, fetchedRecords<Any, Any>()[tp0]!!.size)

        subscriptions.seek(tp1, 10)

        // Should not throw OffsetOutOfRangeException after the seek
        assertEmptyFetch("Should not return records or advance position after seeking to end of topic partitions")
    }

    @Test
    fun testFetchDisconnected() {
        buildFetcher()
        assignFromUser(setOf(tp0))
        subscriptions.seek(tp0, 0)
        assertEquals(1, fetcher.sendFetches())
        client!!.prepareResponse(
            response = fullFetchResponse(
                tp = tidp0,
                records = records,
                error = Errors.NONE,
                hw = 100L,
                throttleTime = 0,
            ),
            disconnected = true,
        )
        consumerClient!!.poll(time.timer(0))
        assertEmptyFetch("Should not return records or advance position on disconnect")

        // disconnects should have no affect on subscription state
        assertFalse(subscriptions.isOffsetResetNeeded(tp0))
        assertTrue(subscriptions.isFetchable(tp0))
        assertEquals(0, subscriptions.position(tp0)!!.offset)
    }

    @Test
    fun testUpdateFetchPositionNoOpWithPositionSet() {
        buildFetcher()
        assignFromUser(setOf(tp0))
        subscriptions.seek(tp0, 5L)
        fetcher.resetOffsetsIfNeeded()

        assertFalse(client!!.hasInFlightRequests())
        assertTrue(subscriptions.isFetchable(tp0))
        assertEquals(5, subscriptions.position(tp0)!!.offset)
    }

    @Test
    fun testUpdateFetchPositionResetToDefaultOffset() {
        buildFetcher()
        assignFromUser(setOf(tp0))
        subscriptions.requestOffsetReset(tp0)
        client!!.prepareResponse(
            matcher = listOffsetRequestMatcher(
                timestamp = ListOffsetsRequest.EARLIEST_TIMESTAMP,
                leaderEpoch = validLeaderEpoch,
            ),
            response = listOffsetResponse(
                error = Errors.NONE,
                timestamp = 1L,
                offset = 5L,
            ),
        )
        fetcher.resetOffsetsIfNeeded()
        consumerClient!!.pollNoWakeup()

        assertFalse(subscriptions.isOffsetResetNeeded(tp0))
        assertTrue(subscriptions.isFetchable(tp0))
        assertEquals(5, subscriptions.position(tp0)!!.offset)
    }

    @Test
    fun testUpdateFetchPositionResetToLatestOffset() {
        buildFetcher()
        assignFromUser(setOf(tp0))
        subscriptions.requestOffsetReset(tp0, OffsetResetStrategy.LATEST)
        client!!.updateMetadata(initialUpdateResponse)
        client!!.prepareResponse(
            matcher = listOffsetRequestMatcher(ListOffsetsRequest.LATEST_TIMESTAMP),
            response = listOffsetResponse(
                error = Errors.NONE,
                timestamp = 1L,
                offset = 5L,
            ),
        )
        fetcher.resetOffsetsIfNeeded()
        consumerClient!!.pollNoWakeup()

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
        client!!.prepareResponse(
            matcher = listOffsetRequestMatcher(
                timestamp = ListOffsetsRequest.LATEST_TIMESTAMP,
                leaderEpoch = validLeaderEpoch,
            ),
            response = listOffsetResponse(
                error = Errors.OFFSET_NOT_AVAILABLE,
                timestamp = 1L,
                offset = 5L
            ),
            disconnected = false,
        )
        fetcher.resetOffsetsIfNeeded()
        consumerClient!!.pollNoWakeup()
        assertFalse(subscriptions.hasValidPosition(tp0))
        assertTrue(subscriptions.isOffsetResetNeeded(tp0))
        assertFalse(subscriptions.isFetchable(tp0))

        // Fail with LEADER_NOT_AVAILABLE
        time.sleep(retryBackoffMs)
        client!!.prepareResponse(
            matcher = listOffsetRequestMatcher(
                timestamp = ListOffsetsRequest.LATEST_TIMESTAMP,
                leaderEpoch = validLeaderEpoch,
            ),
            response = listOffsetResponse(
                error = Errors.LEADER_NOT_AVAILABLE,
                timestamp = 1L,
                offset = 5L
            ),
            disconnected = false,
        )
        fetcher.resetOffsetsIfNeeded()
        consumerClient!!.pollNoWakeup()

        assertFalse(subscriptions.hasValidPosition(tp0))
        assertTrue(subscriptions.isOffsetResetNeeded(tp0))
        assertFalse(subscriptions.isFetchable(tp0))

        // Back to normal
        time.sleep(retryBackoffMs)
        client!!.prepareResponse(
            matcher = listOffsetRequestMatcher(ListOffsetsRequest.LATEST_TIMESTAMP),
            response = listOffsetResponse(error = Errors.NONE, timestamp = 1L, offset = 5L),
            disconnected = false,
        )
        fetcher.resetOffsetsIfNeeded()
        consumerClient!!.pollNoWakeup()
        assertTrue(subscriptions.hasValidPosition(tp0))
        assertFalse(subscriptions.isOffsetResetNeeded(tp0))
        assertTrue(subscriptions.isFetchable(tp0))
        assertEquals(subscriptions.position(tp0)!!.offset, 5L)
    }

    @Test
    fun testListOffsetSendsReadUncommitted() = testListOffsetsSendsIsolationLevel(IsolationLevel.READ_UNCOMMITTED)

    @Test
    fun testListOffsetSendsReadCommitted() = testListOffsetsSendsIsolationLevel(IsolationLevel.READ_COMMITTED)

    private fun testListOffsetsSendsIsolationLevel(isolationLevel: IsolationLevel) {
        buildFetcher(
            offsetResetStrategy = OffsetResetStrategy.EARLIEST,
            keyDeserializer = ByteArrayDeserializer(),
            valueDeserializer = ByteArrayDeserializer(),
            maxPollRecords = Int.MAX_VALUE,
            isolationLevel = isolationLevel,
        )
        assignFromUser(setOf(tp0))
        subscriptions.requestOffsetReset(tp0, OffsetResetStrategy.LATEST)
        client!!.prepareResponse(
            matcher = { body ->
                val request = body as? ListOffsetsRequest
                request?.isolationLevel == isolationLevel
            },
            response = listOffsetResponse(error = Errors.NONE, timestamp = 1L, offset = 5L),
        )
        fetcher.resetOffsetsIfNeeded()
        consumerClient!!.pollNoWakeup()

        assertFalse(subscriptions.isOffsetResetNeeded(tp0))
        assertTrue(subscriptions.isFetchable(tp0))
        assertEquals(5, subscriptions.position(tp0)!!.offset)
    }

    @Test
    fun testResetOffsetsSkipsBlackedOutConnections() {
        buildFetcher()
        assignFromUser(setOf(tp0))
        subscriptions.requestOffsetReset(tp0, OffsetResetStrategy.EARLIEST)

        // Check that we skip sending the ListOffset request when the node is blacked out
        client!!.updateMetadata(initialUpdateResponse)
        val node = initialUpdateResponse.brokers().iterator().next()
        client!!.backoff(node, 500)
        fetcher.resetOffsetsIfNeeded()

        assertEquals(0, consumerClient!!.pendingRequestCount())

        consumerClient!!.pollNoWakeup()

        assertTrue(subscriptions.isOffsetResetNeeded(tp0))
        assertEquals(OffsetResetStrategy.EARLIEST, subscriptions.resetStrategy(tp0))

        time.sleep(500)
        client!!.prepareResponse(
            matcher = listOffsetRequestMatcher(ListOffsetsRequest.EARLIEST_TIMESTAMP),
            response = listOffsetResponse(error = Errors.NONE, timestamp = 1L, offset = 5L),
        )
        fetcher.resetOffsetsIfNeeded()
        consumerClient!!.pollNoWakeup()

        assertFalse(subscriptions.isOffsetResetNeeded(tp0))
        assertTrue(subscriptions.isFetchable(tp0))
        assertEquals(5, subscriptions.position(tp0)!!.offset)
    }

    @Test
    fun testUpdateFetchPositionResetToEarliestOffset() {
        buildFetcher()
        assignFromUser(setOf(tp0))
        subscriptions.requestOffsetReset(tp0, OffsetResetStrategy.EARLIEST)
        client!!.prepareResponse(
            matcher = listOffsetRequestMatcher(
                timestamp = ListOffsetsRequest.EARLIEST_TIMESTAMP,
                leaderEpoch = validLeaderEpoch,
            ),
            response = listOffsetResponse(error = Errors.NONE, timestamp = 1L, offset = 5L),
        )
        fetcher.resetOffsetsIfNeeded()
        consumerClient!!.pollNoWakeup()

        assertFalse(subscriptions.isOffsetResetNeeded(tp0))
        assertTrue(subscriptions.isFetchable(tp0))
        assertEquals(5, subscriptions.position(tp0)!!.offset)
    }

    @Test
    fun testResetOffsetsMetadataRefresh() {
        buildFetcher()
        assignFromUser(setOf(tp0))
        subscriptions.requestOffsetReset(tp0, OffsetResetStrategy.LATEST)

        // First fetch fails with stale metadata
        client!!.prepareResponse(
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
        fetcher.resetOffsetsIfNeeded()
        consumerClient!!.pollNoWakeup()
        assertFalse(subscriptions.hasValidPosition(tp0))

        // Expect a metadata refresh
        client!!.prepareMetadataUpdate(initialUpdateResponse)
        consumerClient!!.pollNoWakeup()
        assertFalse(client!!.hasPendingMetadataUpdates())

        // Next fetch succeeds
        time.sleep(retryBackoffMs)
        client!!.prepareResponse(
            matcher = listOffsetRequestMatcher(ListOffsetsRequest.LATEST_TIMESTAMP),
            response = listOffsetResponse(error = Errors.NONE, timestamp = 1L, offset = 5L),
        )
        fetcher.resetOffsetsIfNeeded()
        consumerClient!!.pollNoWakeup()

        assertFalse(subscriptions.isOffsetResetNeeded(tp0))
        assertTrue(subscriptions.isFetchable(tp0))
        assertEquals(5, subscriptions.position(tp0)!!.offset)
    }

    @Test
    fun testListOffsetNoUpdateMissingEpoch() {
        buildFetcher()

        // Set up metadata with no leader epoch
        subscriptions.assignFromUser(setOf(tp0))
        val metadataWithNoLeaderEpochs = metadataUpdateWith(
            clusterId = "kafka-cluster",
            numNodes = 1,
            topicErrors = emptyMap(),
            topicPartitionCounts = mapOf(topicName to 4),
            epochSupplier = { null },
            topicIds = topicIds,
        )
        client!!.updateMetadata(metadataWithNoLeaderEpochs)

        // Return a ListOffsets response with leaderEpoch=1, we should ignore it
        subscriptions.requestOffsetReset(tp0, OffsetResetStrategy.LATEST)
        client!!.prepareResponse(
            matcher = listOffsetRequestMatcher(ListOffsetsRequest.LATEST_TIMESTAMP),
            response = listOffsetResponse(
                tp = tp0,
                error = Errors.NONE,
                timestamp = 1L,
                offset = 5L,
                leaderEpoch = 1,
            ),
        )
        fetcher.resetOffsetsIfNeeded()
        consumerClient!!.pollNoWakeup()

        // Reset should be satisfied and no metadata update requested
        assertFalse(subscriptions.isOffsetResetNeeded(tp0))
        assertFalse(metadata!!.updateRequested())
        assertNull(metadata!!.lastSeenLeaderEpoch(tp0))
    }

    @Test
    fun testListOffsetUpdateEpoch() {
        buildFetcher()

        // Set up metadata with leaderEpoch=1
        subscriptions.assignFromUser(setOf(tp0))
        val metadataWithLeaderEpochs = metadataUpdateWith(
            clusterId = "kafka-cluster",
            numNodes = 1,
            topicErrors = emptyMap(),
            topicPartitionCounts = mapOf(topicName to 4),
            epochSupplier = { 1 },
            topicIds = topicIds,
        )
        client!!.updateMetadata(metadataWithLeaderEpochs)

        // Reset offsets to trigger ListOffsets call
        subscriptions.requestOffsetReset(tp0, OffsetResetStrategy.LATEST)

        // Now we see a ListOffsets with leaderEpoch=2 epoch, we trigger a metadata update
        client!!.prepareResponse(
            matcher = listOffsetRequestMatcher(ListOffsetsRequest.LATEST_TIMESTAMP, 1),
            response = listOffsetResponse(
                tp = tp0,
                error = Errors.NONE,
                timestamp = 1L,
                offset = 5L,
                leaderEpoch = 2,
            ),
        )
        fetcher.resetOffsetsIfNeeded()
        consumerClient!!.pollNoWakeup()
        assertFalse(subscriptions.isOffsetResetNeeded(tp0))
        assertTrue(metadata!!.updateRequested())
        assertNullable(metadata!!.lastSeenLeaderEpochs[tp0]) { epoch ->
            assertEquals(epoch, 2)
        }
    }

    @Test
    fun testUpdateFetchPositionDisconnect() {
        buildFetcher()
        assignFromUser(setOf(tp0))
        subscriptions.requestOffsetReset(tp0, OffsetResetStrategy.LATEST)

        // First request gets a disconnect
        client!!.prepareResponse(
            matcher = listOffsetRequestMatcher(
                timestamp = ListOffsetsRequest.LATEST_TIMESTAMP,
                leaderEpoch = validLeaderEpoch,
            ),
            response = listOffsetResponse(
                error = Errors.NONE,
                timestamp = 1L,
                offset = 5L,
            ),
            disconnected = true,
        )
        fetcher.resetOffsetsIfNeeded()
        consumerClient!!.pollNoWakeup()
        assertFalse(subscriptions.hasValidPosition(tp0))

        // Expect a metadata refresh
        client!!.prepareMetadataUpdate(initialUpdateResponse)
        consumerClient!!.pollNoWakeup()
        assertFalse(client!!.hasPendingMetadataUpdates())

        // No retry until the backoff passes
        fetcher.resetOffsetsIfNeeded()
        consumerClient!!.pollNoWakeup()
        assertFalse(client!!.hasInFlightRequests())
        assertFalse(subscriptions.hasValidPosition(tp0))

        // Next one succeeds
        time.sleep(retryBackoffMs)
        client!!.prepareResponse(
            matcher = listOffsetRequestMatcher(ListOffsetsRequest.LATEST_TIMESTAMP),
            response = listOffsetResponse(error = Errors.NONE, timestamp = 1L, offset = 5L),
        )
        fetcher.resetOffsetsIfNeeded()
        consumerClient!!.pollNoWakeup()

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
        fetcher.resetOffsetsIfNeeded()
        consumerClient!!.pollNoWakeup()
        assertFalse(subscriptions.hasValidPosition(tp0))
        assertTrue(client!!.hasInFlightRequests())

        // Now we have an assignment change
        assignFromUser(setOf(tp1))

        // The response returns and is discarded
        client!!.respond(listOffsetResponse(Errors.NONE, 1L, 5L))
        consumerClient!!.pollNoWakeup()
        assertFalse(client!!.hasPendingResponses())
        assertFalse(client!!.hasInFlightRequests())
        assertFalse(subscriptions.isAssigned(tp0))
    }

    @Test
    fun testSeekWithInFlightReset() {
        buildFetcher()
        assignFromUser(setOf(tp0))
        subscriptions.requestOffsetReset(tp0, OffsetResetStrategy.LATEST)

        // Send the ListOffsets request to reset the position
        fetcher.resetOffsetsIfNeeded()
        consumerClient!!.pollNoWakeup()
        assertFalse(subscriptions.hasValidPosition(tp0))
        assertTrue(client!!.hasInFlightRequests())

        // Now we get a seek from the user
        subscriptions.seek(tp0, 237)

        // The response returns and is discarded
        client!!.respond(listOffsetResponse(Errors.NONE, 1L, 5L))
        consumerClient!!.pollNoWakeup()
        assertFalse(client!!.hasPendingResponses())
        assertFalse(client!!.hasInFlightRequests())
        assertEquals(237L, subscriptions.position(tp0)!!.offset)
    }

    private fun listOffsetMatchesExpectedReset(
        tp: TopicPartition,
        strategy: OffsetResetStrategy,
        request: AbstractRequest,
    ): Boolean {
        assertIs<ListOffsetsRequest>(request)
        assertEquals(
            expected = setOf(tp.topic),
            actual = request.data().topics.map(ListOffsetsTopic::name).toSet(),
        )
        val listTopic = request.data().topics[0]
        assertEquals(
            expected = setOf(tp.partition),
            actual = listTopic.partitions.map(ListOffsetsPartition::partitionIndex).toSet(),
        )
        val listPartition = listTopic.partitions[0]
        if (strategy === OffsetResetStrategy.EARLIEST)
            assertEquals(ListOffsetsRequest.EARLIEST_TIMESTAMP, listPartition.timestamp)
        else if (strategy === OffsetResetStrategy.LATEST)
            assertEquals(ListOffsetsRequest.LATEST_TIMESTAMP, listPartition.timestamp)

        return true
    }

    @Test
    fun testEarlierOffsetResetArrivesLate() {
        buildFetcher()
        assignFromUser(setOf(tp0))
        subscriptions.requestOffsetReset(tp0, OffsetResetStrategy.EARLIEST)
        fetcher.resetOffsetsIfNeeded()
        client!!.prepareResponse(
            matcher = { req ->
                assertNotNull(req)
                if (listOffsetMatchesExpectedReset(tp0, OffsetResetStrategy.EARLIEST, req)) {
                    // Before the response is handled, we get a request to reset to the latest offset
                    subscriptions.requestOffsetReset(tp0, OffsetResetStrategy.LATEST)
                    true
                } else false
            },
            response = listOffsetResponse(
                error = Errors.NONE,
                timestamp = 1L,
                offset = 0L,
            ),
        )
        consumerClient!!.pollNoWakeup()

        // The list offset result should be ignored
        assertTrue(subscriptions.isOffsetResetNeeded(tp0))
        assertEquals(OffsetResetStrategy.LATEST, subscriptions.resetStrategy(tp0))
        fetcher.resetOffsetsIfNeeded()
        client!!.prepareResponse(
            matcher = { req ->
                listOffsetMatchesExpectedReset(
                    tp = tp0,
                    strategy = OffsetResetStrategy.LATEST,
                    request = req!!,
                )
            },
            response = listOffsetResponse(
                error = Errors.NONE,
                timestamp = 1L,
                offset = 10L,
            ),
        )
        consumerClient!!.pollNoWakeup()
        assertFalse(subscriptions.isOffsetResetNeeded(tp0))
        assertEquals(10, subscriptions.position(tp0)!!.offset)
    }

    @Test
    fun testChangeResetWithInFlightReset() {
        buildFetcher()
        assignFromUser(setOf(tp0))
        subscriptions.requestOffsetReset(tp0, OffsetResetStrategy.LATEST)

        // Send the ListOffsets request to reset the position
        fetcher.resetOffsetsIfNeeded()
        consumerClient!!.pollNoWakeup()
        assertFalse(subscriptions.hasValidPosition(tp0))
        assertTrue(client!!.hasInFlightRequests())

        // Now we get a seek from the user
        subscriptions.requestOffsetReset(tp0, OffsetResetStrategy.EARLIEST)

        // The response returns and is discarded
        client!!.respond(listOffsetResponse(Errors.NONE, 1L, 5L))
        consumerClient!!.pollNoWakeup()
        assertFalse(client!!.hasPendingResponses())
        assertFalse(client!!.hasInFlightRequests())
        assertTrue(subscriptions.isOffsetResetNeeded(tp0))
        assertEquals(OffsetResetStrategy.EARLIEST, subscriptions.resetStrategy(tp0))
    }

    @Test
    fun testIdempotentResetWithInFlightReset() {
        buildFetcher()
        assignFromUser(setOf(tp0))
        subscriptions.requestOffsetReset(tp0, OffsetResetStrategy.LATEST)

        // Send the ListOffsets request to reset the position
        fetcher.resetOffsetsIfNeeded()
        consumerClient!!.pollNoWakeup()
        assertFalse(subscriptions.hasValidPosition(tp0))
        assertTrue(client!!.hasInFlightRequests())

        // Now we get a seek from the user
        subscriptions.requestOffsetReset(tp0, OffsetResetStrategy.LATEST)
        client!!.respond(listOffsetResponse(Errors.NONE, 1L, 5L))
        consumerClient!!.pollNoWakeup()
        assertFalse(client!!.hasInFlightRequests())
        assertFalse(subscriptions.isOffsetResetNeeded(tp0))
        assertEquals(5L, subscriptions.position(tp0)!!.offset)
    }

    @Test
    fun testRestOffsetsAuthorizationFailure() {
        buildFetcher()
        assignFromUser(setOf(tp0))
        subscriptions.requestOffsetReset(tp0, OffsetResetStrategy.LATEST)

        // First request gets a disconnect
        client!!.prepareResponse(
            matcher = listOffsetRequestMatcher(
                timestamp = ListOffsetsRequest.LATEST_TIMESTAMP,
                leaderEpoch = validLeaderEpoch,
            ),
            response = listOffsetResponse(
                error = Errors.TOPIC_AUTHORIZATION_FAILED,
                timestamp = -1,
                offset = -1,
            ),
            disconnected = false,
        )
        fetcher.resetOffsetsIfNeeded()
        consumerClient!!.pollNoWakeup()
        assertFalse(subscriptions.hasValidPosition(tp0))

        val error = assertFailsWith<TopicAuthorizationException>(
            message = "Expected authorization error to be raised",
        ) { fetcher.resetOffsetsIfNeeded() }
        assertEquals(setOf(tp0.topic), error.unauthorizedTopics)

        // The exception should clear after being raised, but no retry until the backoff
        fetcher.resetOffsetsIfNeeded()
        consumerClient!!.pollNoWakeup()
        assertFalse(client!!.hasInFlightRequests())
        assertFalse(subscriptions.hasValidPosition(tp0))

        // Next one succeeds
        time.sleep(retryBackoffMs)
        client!!.prepareResponse(
            matcher = listOffsetRequestMatcher(ListOffsetsRequest.LATEST_TIMESTAMP),
            response = listOffsetResponse(error = Errors.NONE, timestamp = 1L, offset = 5L),
        )
        fetcher.resetOffsetsIfNeeded()
        consumerClient!!.pollNoWakeup()

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
        fetcher.resetOffsetsIfNeeded()

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
        client!!.prepareResponse(
            matcher = listOffsetRequestMatcher(
                timestamp = ListOffsetsRequest.LATEST_TIMESTAMP,
                leaderEpoch = validLeaderEpoch,
            ),
            response = listOffsetResponse(error = Errors.NONE, timestamp = 1L, offset = 10L),
        )
        fetcher.resetOffsetsIfNeeded()
        consumerClient!!.pollNoWakeup()
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
        fetcher.resetOffsetsIfNeeded()
        consumerClient!!.pollNoWakeup()

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
        fetcher.resetOffsetsIfNeeded()

        assertFalse(subscriptions.isOffsetResetNeeded(tp0))
        assertFalse(subscriptions.isFetchable(tp0)) // because tp is paused
        assertTrue(subscriptions.hasValidPosition(tp0))
        assertEquals(10, subscriptions.position(tp0)!!.offset)
    }

    @Test
    fun testGetAllTopics() {
        // sending response before request, as getTopicMetadata is a blocking call
        buildFetcher()
        assignFromUser(setOf(tp0))
        client!!.prepareResponse(newMetadataResponse(topicName, Errors.NONE))
        val allTopics = fetcher.getAllTopicMetadata(time.timer(5000L))

        assertEquals(initialUpdateResponse.topicMetadata().size, allTopics.size)
    }

    @Test
    fun testGetAllTopicsDisconnect() {
        // first try gets a disconnect, next succeeds
        buildFetcher()
        assignFromUser(setOf(tp0))
        client!!.prepareResponse(response = null, disconnected = true)
        client!!.prepareResponse(newMetadataResponse(topicName, Errors.NONE))
        val allTopics = fetcher.getAllTopicMetadata(time.timer(5000L))

        assertEquals(initialUpdateResponse.topicMetadata().size, allTopics.size)
    }

    @Test
    fun testGetAllTopicsTimeout() {
        // since no response is prepared, the request should timeout
        buildFetcher()
        assignFromUser(setOf(tp0))
        assertFailsWith<TimeoutException> { fetcher.getAllTopicMetadata(time.timer(50L)) }
    }

    @Test
    fun testGetAllTopicsUnauthorized() {
        buildFetcher()
        assignFromUser(setOf(tp0))
        client!!.prepareResponse(newMetadataResponse(topicName, Errors.TOPIC_AUTHORIZATION_FAILED))
        val error = assertFailsWith<TopicAuthorizationException> {
            fetcher.getAllTopicMetadata(time.timer(10L))
        }
        assertEquals(setOf(topicName), error.unauthorizedTopics)
    }

    @Test
    fun testGetTopicMetadataInvalidTopic() {
        buildFetcher()
        assignFromUser(setOf(tp0))
        client!!.prepareResponse(newMetadataResponse(topicName, Errors.INVALID_TOPIC_EXCEPTION))
        assertFailsWith<InvalidTopicException> {
            fetcher.getTopicMetadata(
                request = MetadataRequest.Builder(
                    topics = listOf(topicName),
                    allowAutoTopicCreation = true,
                ),
                timer = time.timer(5000L),
            )
        }
    }

    @Test
    fun testGetTopicMetadataUnknownTopic() {
        buildFetcher()
        assignFromUser(setOf(tp0))
        client!!.prepareResponse(
            response = newMetadataResponse(topic = topicName, error = Errors.UNKNOWN_TOPIC_OR_PARTITION),
        )
        val topicMetadata = fetcher.getTopicMetadata(
            request = MetadataRequest.Builder(
                topics = listOf(topicName),
                allowAutoTopicCreation = true,
            ),
            timer = time.timer(5000L)
        )

        assertNull(topicMetadata[topicName])
    }

    @Test
    fun testGetTopicMetadataLeaderNotAvailable() {
        buildFetcher()
        assignFromUser(setOf(tp0))
        client!!.prepareResponse(newMetadataResponse(topicName, Errors.LEADER_NOT_AVAILABLE))
        client!!.prepareResponse(newMetadataResponse(topicName, Errors.NONE))
        val topicMetadata = fetcher.getTopicMetadata(
            request = MetadataRequest.Builder(
                topics = listOf(topicName),
                allowAutoTopicCreation = true,
            ),
            timer = time.timer(5000L),
        )

        assertTrue(topicMetadata.containsKey(topicName))
    }

    @Test
    fun testGetTopicMetadataOfflinePartitions() {
        buildFetcher()
        assignFromUser(setOf(tp0))
        val originalResponse = newMetadataResponse(topicName, Errors.NONE) //baseline ok response

        //create a response based on the above one with all partitions being leaderless
        val altTopics = mutableListOf<MetadataResponse.TopicMetadata>()

        for ((error, topic, _, isInternal, partitions) in originalResponse.topicMetadata()) {
            val altPartitions = partitions.map {
                PartitionMetadata(
                    error = it.error,
                    topicPartition = it.topicPartition,
                    leaderId = null,  //no leader
                    leaderEpoch = null,
                    replicaIds = it.replicaIds,
                    inSyncReplicaIds = it.inSyncReplicaIds,
                    offlineReplicaIds = it.offlineReplicaIds,
                )
            }
            val alteredTopic = MetadataResponse.TopicMetadata(
                error = error,
                topic = topic,
                isInternal = isInternal,
                partitionMetadata = altPartitions,
            )
            altTopics.add(alteredTopic)
        }
        val controller = originalResponse.controller
        val altered = metadataResponse(
            brokers = originalResponse.brokers(),
            clusterId = originalResponse.clusterId,
            controllerId = controller?.id ?: MetadataResponse.NO_CONTROLLER_ID,
            topicMetadataList = altTopics
        )
        client!!.prepareResponse(altered)
        val topicMetadata = fetcher.getTopicMetadata(
            request = MetadataRequest.Builder(
                topics = listOf(topicName),
                allowAutoTopicCreation = false,
            ),
            timer = time.timer(5000L),
        )
        assertNotNull(topicMetadata)
        assertNotNull(topicMetadata[topicName])
        assertEquals(
            expected = metadata!!.fetch().partitionCountForTopic(topicName)!!.toLong(),
            actual = topicMetadata[topicName]!!.size.toLong(),
        )
    }

    /*
     * Send multiple requests. Verify that the client side quota metrics have the right values
     */
    @Test
    fun testQuotaMetrics() {
        buildFetcher()
        val selector = MockSelector(time)
        val throttleTimeSensor = Fetcher.throttleTimeSensor(metrics, metricsRegistry!!)
        val cluster = singletonCluster(topic = "test", partitions = 1)
        val node = cluster.nodes[0]
        val client = NetworkClient(
            selector = selector,
            metadata = metadata,
            clientId = "mock",
            maxInFlightRequestsPerConnection = Int.MAX_VALUE,
            reconnectBackoffMs = 1000,
            reconnectBackoffMax = 1000,
            socketSendBuffer = 64 * 1024,
            socketReceiveBuffer = 64 * 1024,
            defaultRequestTimeoutMs = 1000,
            connectionSetupTimeoutMs = 10 * 1000,
            connectionSetupTimeoutMaxMs = 127 * 1000,
            time = time,
            discoverBrokerVersions = true,
            apiVersions = ApiVersions(),
            throttleTimeSensor = throttleTimeSensor,
            logContext = LogContext(),
        )
        val apiVersionsResponse = ApiVersionsResponse.defaultApiVersionsResponse(
            throttleTimeMs = 400,
            listenerType = ApiMessageType.ListenerType.ZK_BROKER,
        )
        var buffer = serializeResponseWithHeader(
            response = apiVersionsResponse,
            version = ApiKeys.API_VERSIONS.latestVersion(),
            correlationId = 0,
        )
        selector.delayedReceive(
            DelayedReceive(
                source = node.idString(),
                receive = NetworkReceive(source = node.idString(), buffer = buffer),
            )
        )
        while (!client.ready(node, time.milliseconds())) {
            client.poll(1, time.milliseconds())
            // If a throttled response is received, advance the time to ensure progress.
            time.sleep(client.throttleDelayMs(node, time.milliseconds()))
        }
        selector.clear()
        for (i in 1..3) {
            val throttleTimeMs = 100 * i
            val builder = FetchRequest.Builder.forConsumer(
                maxVersion = ApiKeys.FETCH.latestVersion(),
                maxWait = 100,
                minBytes = 100,
                fetchData = LinkedHashMap(),
            )
            builder.rackId("")
            val request = client.newClientRequest(
                nodeId = node.idString(),
                requestBuilder = builder,
                createdTimeMs = time.milliseconds(),
                expectResponse = true,
            )
            client.send(request, time.milliseconds())
            client.poll(1, time.milliseconds())
            val response = fullFetchResponse(
                tp = tidp0,
                records = nextRecords,
                error = Errors.NONE,
                hw = i.toLong(),
                throttleTime = throttleTimeMs,
            )
            buffer = serializeResponseWithHeader(
                response = response,
                version = ApiKeys.FETCH.latestVersion(),
                correlationId = request.correlationId,
            )
            selector.completeReceive(NetworkReceive(source = node.idString(), buffer = buffer))
            client.poll(1, time.milliseconds())
            // If a throttled response is received, advance the time to ensure progress.
            time.sleep(client.throttleDelayMs(node, time.milliseconds()))
            selector.clear()
        }
        val allMetrics = metrics.metrics
        val avgMetric = allMetrics[metrics.metricInstance(metricsRegistry!!.fetchThrottleTimeAvg)]
        val maxMetric = allMetrics[metrics.metricInstance(metricsRegistry!!.fetchThrottleTimeMax)]

        // Throttle times are ApiVersions=400, Fetch=(100, 200, 300)
        assertEquals(
            expected = 250.0,
            actual = (avgMetric!!.metricValue() as Double),
            absoluteTolerance = EPSILON,
        )
        assertEquals(
            expected = 400.0,
            actual = (maxMetric!!.metricValue() as Double),
            absoluteTolerance = EPSILON,
        )
        client.close()
    }

    /*
     * Send multiple requests. Verify that the client side quota metrics have the right values
     */
    @Test
    fun testFetcherMetrics() {
        buildFetcher()
        assignFromUser(setOf(tp0))
        subscriptions.seek(tp0, 0)
        val maxLagMetric = metrics.metricInstance(metricsRegistry!!.recordsLagMax)
        val tags: MutableMap<String, String> = HashMap()
        tags["topic"] = tp0.topic
        tags["partition"] = tp0.partition.toString()
        val partitionLagMetric = metrics.metricName(name = "records-lag", group = metricGroup, tags = tags)
        val allMetrics = metrics.metrics
        val recordsFetchLagMax = allMetrics[maxLagMetric]

        // recordsFetchLagMax should be initialized to NaN
        assertEquals(
            expected = Double.NaN,
            actual = (recordsFetchLagMax!!.metricValue() as Double),
            absoluteTolerance = EPSILON,
        )

        // recordsFetchLagMax should be hw - fetchOffset after receiving an empty FetchResponse
        fetchRecords(
            tp = tidp0,
            records = MemoryRecords.EMPTY,
            error = Errors.NONE,
            hw = 100L,
            throttleTime = 0,
        )
        assertEquals(
            expected = 100.0,
            actual = (recordsFetchLagMax.metricValue() as Double),
            absoluteTolerance = EPSILON,
        )
        val partitionLag = allMetrics[partitionLagMetric]
        assertEquals(
            expected = 100.0,
            actual = (partitionLag!!.metricValue() as Double),
            absoluteTolerance = EPSILON,
        )

        // recordsFetchLagMax should be hw - offset of the last message after receiving a non-empty FetchResponse
        val builder: MemoryRecordsBuilder = MemoryRecords.builder(
            buffer = ByteBuffer.allocate(1024),
            compressionType = CompressionType.NONE,
            timestampType = TimestampType.CREATE_TIME,
            baseOffset = 0L,
        )
        for (v in 0..2) builder.appendWithOffset(
            offset = v.toLong(),
            timestamp = RecordBatch.NO_TIMESTAMP,
            key = "key".toByteArray(),
            value = "value-$v".toByteArray(),
        )
        fetchRecords(
            tp = tidp0,
            records = builder.build(),
            error = Errors.NONE,
            hw = 200L,
            throttleTime = 0,
        )
        assertEquals(
            expected = 197.0,
            actual = (recordsFetchLagMax.metricValue() as Double),
            absoluteTolerance = EPSILON,
        )
        assertEquals(
            expected = 197.0,
            actual = (partitionLag.metricValue() as Double),
            absoluteTolerance = EPSILON,
        )

        // verify de-registration of partition lag
        subscriptions.unsubscribe()
        fetcher.sendFetches()
        assertFalse(allMetrics.containsKey(partitionLagMetric))
    }

    @Test
    fun testFetcherLeadMetric() {
        buildFetcher()
        assignFromUser(setOf(tp0))
        subscriptions.seek(tp0, 0)
        val minLeadMetric = metrics.metricInstance(metricsRegistry!!.recordsLeadMin)
        val tags = mapOf(
            "topic" to tp0.topic,
            "partition" to tp0.partition.toString(),
        )
        val partitionLeadMetric = metrics.metricName(
            name = "records-lead",
            group = metricGroup,
            description = "",
            tags = tags,
        )
        val allMetrics = metrics.metrics
        val recordsFetchLeadMin = allMetrics[minLeadMetric]

        // recordsFetchLeadMin should be initialized to NaN
        assertEquals(
            expected = Double.NaN,
            actual = (recordsFetchLeadMin!!.metricValue() as Double),
            absoluteTolerance = EPSILON,
        )

        // recordsFetchLeadMin should be position - logStartOffset after receiving an empty FetchResponse
        fetchRecords(
            tp = tidp0,
            records = MemoryRecords.EMPTY,
            error = Errors.NONE,
            hw = 100L,
            lastStableOffset = -1L,
            logStartOffset = 0L,
            throttleTime = 0,
        )
        assertEquals(
            expected = 0.0,
            actual = (recordsFetchLeadMin.metricValue() as Double),
            absoluteTolerance = EPSILON,
        )
        val partitionLead = allMetrics[partitionLeadMetric]
        assertEquals(
            expected = 0.0,
            actual = (partitionLead!!.metricValue() as Double),
            absoluteTolerance = EPSILON
        )

        // recordsFetchLeadMin should be position - logStartOffset after receiving a non-empty FetchResponse
        val builder: MemoryRecordsBuilder = MemoryRecords.builder(
            buffer = ByteBuffer.allocate(1024),
            compressionType = CompressionType.NONE,
            timestampType = TimestampType.CREATE_TIME,
            baseOffset = 0L
        )
        for (v in 0..2) {
            builder.appendWithOffset(
                offset = v.toLong(),
                timestamp = RecordBatch.NO_TIMESTAMP,
                key = "key".toByteArray(),
                value = "value-$v".toByteArray(),
            )
        }
        fetchRecords(
            tp = tidp0,
            records = builder.build(),
            error = Errors.NONE,
            hw = 200L,
            lastStableOffset = -1L,
            logStartOffset = 0L,
            throttleTime = 0,
        )
        assertEquals(
            expected = 0.0,
            actual = (recordsFetchLeadMin.metricValue() as Double),
            absoluteTolerance = EPSILON,
        )
        assertEquals(
            expected = 3.0,
            actual = (partitionLead.metricValue() as Double),
            absoluteTolerance = EPSILON,
        )

        // verify de-registration of partition lag
        subscriptions.unsubscribe()
        fetcher.sendFetches()
        assertFalse(allMetrics.containsKey(partitionLeadMetric))
    }

    @Test
    fun testReadCommittedLagMetric() {
        buildFetcher(
            offsetResetStrategy = OffsetResetStrategy.EARLIEST,
            keyDeserializer = ByteArrayDeserializer(),
            valueDeserializer = ByteArrayDeserializer(),
            maxPollRecords = Int.MAX_VALUE,
            isolationLevel = IsolationLevel.READ_COMMITTED,
        )
        assignFromUser(setOf(tp0))
        subscriptions.seek(tp0, 0)
        val maxLagMetric = metrics.metricInstance(metricsRegistry!!.recordsLagMax)
        val tags: MutableMap<String, String> = HashMap()
        tags["topic"] = tp0.topic
        tags["partition"] = tp0.partition.toString()
        val partitionLagMetric = metrics.metricName(name = "records-lag", group = metricGroup, tags = tags)
        val allMetrics = metrics.metrics
        val recordsFetchLagMax = allMetrics[maxLagMetric]

        // recordsFetchLagMax should be initialized to NaN
        assertEquals(
            expected = Double.NaN,
            actual = (recordsFetchLagMax!!.metricValue() as Double),
            absoluteTolerance = EPSILON,
        )

        // recordsFetchLagMax should be lso - fetchOffset after receiving an empty FetchResponse
        fetchRecords(
            tp = tidp0,
            records = MemoryRecords.EMPTY,
            error = Errors.NONE,
            hw = 100L,
            lastStableOffset = 50L,
            throttleTime = 0,
        )
        assertEquals(
            expected = 50.0,
            actual = (recordsFetchLagMax.metricValue() as Double),
            absoluteTolerance = EPSILON,
        )
        val partitionLag = allMetrics[partitionLagMetric]
        assertEquals(
            expected = 50.0,
            actual = (partitionLag!!.metricValue() as Double),
            absoluteTolerance = EPSILON,
        )

        // recordsFetchLagMax should be lso - offset of the last message after receiving a non-empty FetchResponse
        val builder: MemoryRecordsBuilder = MemoryRecords.builder(
            buffer = ByteBuffer.allocate(1024),
            compressionType = CompressionType.NONE,
            timestampType = TimestampType.CREATE_TIME,
            baseOffset = 0L,
        )
        for (v in 0..2) builder.appendWithOffset(
            offset = v.toLong(),
            timestamp = RecordBatch.NO_TIMESTAMP,
            key = "key".toByteArray(),
            value = "value-$v".toByteArray(),
        )
        fetchRecords(
            tp = tidp0,
            records = builder.build(),
            error = Errors.NONE,
            hw = 200L,
            lastStableOffset = 150L,
            throttleTime = 0,
        )
        assertEquals(
            expected = 147.0,
            actual = (recordsFetchLagMax.metricValue() as Double),
            absoluteTolerance = EPSILON,
        )
        assertEquals(
            expected = 147.0,
            actual = (partitionLag.metricValue() as Double),
            absoluteTolerance = EPSILON,
        )

        // verify de-registration of partition lag
        subscriptions.unsubscribe()
        fetcher.sendFetches()
        assertFalse(allMetrics.containsKey(partitionLagMetric))
    }

    @Test
    fun testFetchResponseMetrics() {
        buildFetcher()
        val topic1 = "foo"
        val topic2 = "bar"
        val tp1 = TopicPartition(topic1, 0)
        val tp2 = TopicPartition(topic2, 0)
        subscriptions.assignFromUser(setOf(tp1, tp2))
        val partitionCounts = mapOf(
            topic1 to 1,
            topic2 to 1,
        )
        topicIds[topic1] = Uuid.randomUuid()
        topicIds[topic2] = Uuid.randomUuid()
        val tidp1 = TopicIdPartition(topicIds[topic1]!!, tp1)
        val tidp2 = TopicIdPartition(topicIds[topic2]!!, tp2)
        client!!.updateMetadata(
            metadataUpdateWithIds(
                numNodes = 1,
                topicPartitionCounts = partitionCounts,
                epochSupplier = { validLeaderEpoch },
                topicIds = topicIds,
            ),
        )
        var expectedBytes = 0
        val fetchPartitionData = LinkedHashMap<TopicIdPartition, FetchResponseData.PartitionData>()
        for (tp in setOf(tidp1, tidp2)) {
            subscriptions.seek(tp.topicPartition, 0)
            val builder: MemoryRecordsBuilder = MemoryRecords.builder(
                buffer = ByteBuffer.allocate(1024),
                compressionType = CompressionType.NONE,
                timestampType = TimestampType.CREATE_TIME,
                baseOffset = 0L
            )
            for (v in 0..2) builder.appendWithOffset(
                offset = v.toLong(),
                timestamp = RecordBatch.NO_TIMESTAMP,
                key = "key".toByteArray(),
                value = "value-$v".toByteArray(),
            )
            val records = builder.build()
            for (record in records.records()) expectedBytes += record.sizeInBytes()
            fetchPartitionData[tp] = FetchResponseData.PartitionData()
                .setPartitionIndex(tp.topicPartition.partition)
                .setHighWatermark(15)
                .setLogStartOffset(0)
                .setRecords(records)
        }
        assertEquals(1, fetcher.sendFetches())
        client!!.prepareResponse(
            response = FetchResponse.of(
                error = Errors.NONE,
                throttleTimeMs = 0,
                sessionId = FetchMetadata.INVALID_SESSION_ID,
                responseData = fetchPartitionData,
            ),
        )
        consumerClient!!.poll(time.timer(0))
        val fetchedRecords = fetchedRecords<ByteArray, ByteArray>()
        assertEquals(3, fetchedRecords[tp1]!!.size)
        assertEquals(3, fetchedRecords[tp2]!!.size)
        val allMetrics = metrics.metrics
        val fetchSizeAverage = allMetrics[metrics.metricInstance(metricsRegistry!!.fetchSizeAvg)]
        val recordsCountAverage = allMetrics[metrics.metricInstance(metricsRegistry!!.recordsPerRequestAvg)]
        assertEquals(
            expected = expectedBytes.toDouble(),
            actual = (fetchSizeAverage!!.metricValue() as Double),
            absoluteTolerance = EPSILON,
        )
        assertEquals(
            expected = 6.0,
            actual = (recordsCountAverage!!.metricValue() as Double),
            absoluteTolerance = EPSILON,
        )
    }

    @Test
    fun testFetchResponseMetricsPartialResponse() {
        buildFetcher()
        assignFromUser(setOf(tp0))
        subscriptions.seek(tp0, 1)
        val allMetrics = metrics.metrics
        val fetchSizeAverage = allMetrics[metrics.metricInstance(metricsRegistry!!.fetchSizeAvg)]
        val recordsCountAverage = allMetrics[metrics.metricInstance(metricsRegistry!!.recordsPerRequestAvg)]
        val builder = MemoryRecords.builder(
            buffer = ByteBuffer.allocate(1024),
            compressionType = CompressionType.NONE,
            timestampType = TimestampType.CREATE_TIME,
            baseOffset = 0L,
        )
        for (v in 0..2) builder.appendWithOffset(
            offset = v.toLong(),
            timestamp = RecordBatch.NO_TIMESTAMP,
            key = "key".toByteArray(),
            value = "value-$v".toByteArray(),
        )
        val records = builder.build()
        var expectedBytes = 0
        for (record in records.records()) {
            if (record.offset() >= 1) expectedBytes += record.sizeInBytes()
        }
        fetchRecords(
            tp = tidp0,
            records = records,
            error = Errors.NONE,
            hw = 100L,
            throttleTime = 0,
        )
        assertEquals(
            expected = expectedBytes.toDouble(),
            actual = (fetchSizeAverage!!.metricValue() as Double),
            absoluteTolerance = EPSILON,
        )
        assertEquals(
            expected = 2.0,
            actual = (recordsCountAverage!!.metricValue() as Double),
            absoluteTolerance = EPSILON,
        )
    }

    @Test
    fun testFetchResponseMetricsWithOnePartitionError() {
        buildFetcher()
        assignFromUser(setOf(tp0, tp1))
        subscriptions.seek(tp0, 0)
        subscriptions.seek(tp1, 0)
        val allMetrics = metrics.metrics
        val fetchSizeAverage = allMetrics[metrics.metricInstance(metricsRegistry!!.fetchSizeAvg)]
        val recordsCountAverage = allMetrics[metrics.metricInstance(metricsRegistry!!.recordsPerRequestAvg)]
        val builder: MemoryRecordsBuilder = MemoryRecords.builder(
            buffer = ByteBuffer.allocate(1024),
            compressionType = CompressionType.NONE,
            timestampType = TimestampType.CREATE_TIME,
            baseOffset = 0L,
        )
        for (v in 0..2) builder.appendWithOffset(
            offset = v.toLong(),
            timestamp = RecordBatch.NO_TIMESTAMP,
            key = "key".toByteArray(),
            value = "value-$v".toByteArray(),
        )
        val records = builder.build()
        val partitions = mapOf(
            tidp0 to FetchResponseData.PartitionData()
                .setPartitionIndex(tp0.partition)
                .setHighWatermark(100)
                .setLogStartOffset(0)
                .setRecords(records),
            tidp1 to FetchResponseData.PartitionData()
                .setPartitionIndex(tp1.partition)
                .setErrorCode(Errors.OFFSET_OUT_OF_RANGE.code)
                .setHighWatermark(100)
                .setLogStartOffset(0),
        )

        assertEquals(1, fetcher.sendFetches())

        client!!.prepareResponse(
            response = FetchResponse.of(
                error = Errors.NONE,
                throttleTimeMs = 0,
                sessionId = FetchMetadata.INVALID_SESSION_ID,
                responseData = LinkedHashMap(partitions),
            )
        )
        consumerClient!!.poll(time.timer(0))
        fetcher.collectFetch()
        val expectedBytes = records.records().sumOf(Record::sizeInBytes)

        assertEquals(
            expected = expectedBytes.toDouble(),
            actual = (fetchSizeAverage!!.metricValue() as Double),
            absoluteTolerance = EPSILON,
        )
        assertEquals(
            expected = 3.0,
            actual = (recordsCountAverage!!.metricValue() as Double),
            absoluteTolerance = EPSILON,
        )
    }

    @Test
    fun testFetchResponseMetricsWithOnePartitionAtTheWrongOffset() {
        buildFetcher()
        assignFromUser(setOf(tp0, tp1))
        subscriptions.seek(tp0, 0)
        subscriptions.seek(tp1, 0)
        val allMetrics = metrics.metrics
        val fetchSizeAverage = allMetrics[metrics.metricInstance(metricsRegistry!!.fetchSizeAvg)]
        val recordsCountAverage = allMetrics[metrics.metricInstance(metricsRegistry!!.recordsPerRequestAvg)]

        // send the fetch and then seek to a new offset
        assertEquals(1, fetcher.sendFetches())

        subscriptions.seek(tp1, 5)
        val builder: MemoryRecordsBuilder = MemoryRecords.builder(
            buffer = ByteBuffer.allocate(1024),
            compressionType = CompressionType.NONE,
            timestampType = TimestampType.CREATE_TIME,
            baseOffset = 0L,
        )
        for (v in 0..2) builder.appendWithOffset(
            offset = v.toLong(),
            timestamp = RecordBatch.NO_TIMESTAMP,
            key = "key".toByteArray(),
            value = "value-$v".toByteArray(),
        )
        val records = builder.build()
        val partitions = mapOf(
            tidp0 to FetchResponseData.PartitionData()
                .setPartitionIndex(tp0.partition)
                .setHighWatermark(100)
                .setLogStartOffset(0)
                .setRecords(records),
            tidp1 to FetchResponseData.PartitionData()
                .setPartitionIndex(tp1.partition)
                .setHighWatermark(100)
                .setLogStartOffset(0)
                .setRecords(
                    MemoryRecords.withRecords(
                        compressionType = CompressionType.NONE,
                        records = arrayOf(SimpleRecord(value = "val".toByteArray())),
                    )
                ),
        )
        client!!.prepareResponse(
            response = FetchResponse.of(
                error = Errors.NONE,
                throttleTimeMs = 0,
                sessionId = FetchMetadata.INVALID_SESSION_ID,
                responseData = LinkedHashMap(partitions),
            ),
        )
        consumerClient!!.poll(time.timer(0))
        fetcher.collectFetch()

        // we should have ignored the record at the wrong offset
        val expectedBytes = records.records().sumOf(Record::sizeInBytes)

        assertEquals(
            expected = expectedBytes.toDouble(),
            actual = (fetchSizeAverage!!.metricValue() as Double),
            absoluteTolerance = EPSILON,
        )
        assertEquals(
            expected = 3.0,
            actual = (recordsCountAverage!!.metricValue() as Double),
            absoluteTolerance = EPSILON,
        )
    }

    @Test
    fun testFetcherMetricsTemplates() {
        val clientTags = mapOf("client-id" to "clientA")
        buildFetcher(
            metricConfig = MetricConfig().tags(clientTags),
            offsetResetStrategy = OffsetResetStrategy.EARLIEST,
            keyDeserializer = ByteArrayDeserializer(),
            valueDeserializer = ByteArrayDeserializer(),
            maxPollRecords = Int.MAX_VALUE,
            isolationLevel = IsolationLevel.READ_UNCOMMITTED,
        )

        // Fetch from topic to generate topic metrics
        assignFromUser(setOf(tp0))
        subscriptions.seek(tp0, 0)

        assertEquals(1, fetcher.sendFetches())

        client!!.prepareResponse(
            response = fullFetchResponse(
                tp = tidp0,
                records = records,
                error = Errors.NONE,
                hw = 100L,
                throttleTime = 0,
            ),
        )
        consumerClient!!.poll(time.timer(0))
        assertTrue(fetcher.hasCompletedFetches())
        val partitionRecords = fetchedRecords<ByteArray, ByteArray>()
        assertTrue(partitionRecords.containsKey(tp0))

        // Create throttle metrics
        Fetcher.throttleTimeSensor(metrics, metricsRegistry!!)

        // Verify that all metrics except metrics-count have registered templates
        val allMetrics = mutableSetOf<MetricNameTemplate>()
        for (metricName in metrics.metrics.keys) {
            val name = metricName.name.replace(tp0.toString().toRegex(), "{topic}-{partition}")
            if (metricName.group != "kafka-metrics-count") allMetrics.add(
                MetricNameTemplate(
                    name = name,
                    group = metricName.group,
                    description = "",
                    tagsNames = metricName.tags.keys,
                )
            )
        }
        checkEquals(
            c1 = allMetrics,
            c2 = metricsRegistry!!.allTemplates.toSet(),
            firstDesc = "metrics",
            secondDesc = "templates",
        )
    }

    private fun fetchRecords(
        tp: TopicIdPartition,
        records: MemoryRecords,
        error: Errors,
        hw: Long,
        throttleTime: Int,
    ): Map<TopicPartition, List<ConsumerRecord<ByteArray, ByteArray>>> = fetchRecords(
        tp = tp,
        records = records,
        error = error,
        hw = hw,
        lastStableOffset = FetchResponse.INVALID_LAST_STABLE_OFFSET,
        throttleTime = throttleTime,
    )

    private fun fetchRecords(
        tp: TopicIdPartition,
        records: MemoryRecords,
        error: Errors,
        hw: Long,
        lastStableOffset: Long,
        throttleTime: Int,
    ): Map<TopicPartition, List<ConsumerRecord<ByteArray, ByteArray>>> {
        assertEquals(1, fetcher.sendFetches())

        client!!.prepareResponse(
            response = fullFetchResponse(
                tp = tp,
                records = records,
                error = error,
                hw = hw,
                lastStableOffset = lastStableOffset,
                throttleTime = throttleTime,
            ),
        )
        consumerClient!!.poll(time.timer(0))
        return fetchedRecords()
    }

    private fun fetchRecords(
        tp: TopicIdPartition,
        records: MemoryRecords,
        error: Errors,
        hw: Long,
        lastStableOffset: Long,
        logStartOffset: Long,
        throttleTime: Int,
    ): Map<TopicPartition, List<ConsumerRecord<ByteArray, ByteArray>>> {
        assertEquals(1, fetcher.sendFetches())

        client!!.prepareResponse(
            response = fullFetchResponse(
                tp = tp,
                records = records,
                error = error,
                hw = hw,
                lastStableOffset = lastStableOffset,
                logStartOffset = logStartOffset,
                throttleTime = throttleTime,
            ),
        )
        consumerClient!!.poll(time.timer(0))

        return fetchedRecords()
    }

    @Test
    fun testGetOffsetsForTimesTimeout() {
        buildFetcher()
        assertFailsWith<TimeoutException> {
            fetcher.offsetsForTimes(
                timestampsToSearch = mapOf(TopicPartition(topicName, 2) to 1000L),
                timer = time.timer(100L),
            )
        }
    }

    @Test
    fun testGetOffsetsForTimes() {
        buildFetcher()

        // Empty map
        assertTrue(fetcher.offsetsForTimes(HashMap(), time.timer(100L)).isEmpty())
        // Unknown Offset
        testGetOffsetsForTimesWithUnknownOffset()
        // Error code none with unknown offset
        testGetOffsetsForTimesWithError(
            errorForP0 = Errors.NONE,
            errorForP1 = Errors.NONE,
            offsetForP0 = -1L,
            offsetForP1 = 100L,
            expectedOffsetForP0 = null,
            expectedOffsetForP1 = 100L,
        )
        // Error code none with known offset
        testGetOffsetsForTimesWithError(
            errorForP0 = Errors.NONE,
            errorForP1 = Errors.NONE,
            offsetForP0 = 10L,
            offsetForP1 = 100L,
            expectedOffsetForP0 = 10L,
            expectedOffsetForP1 = 100L,
        )
        // Test both of partition has error.
        testGetOffsetsForTimesWithError(
            errorForP0 = Errors.NOT_LEADER_OR_FOLLOWER,
            errorForP1 = Errors.INVALID_REQUEST,
            offsetForP0 = 10L,
            offsetForP1 = 100L,
            expectedOffsetForP0 = 10L,
            expectedOffsetForP1 = 100L,
        )
        // Test the second partition has error.
        testGetOffsetsForTimesWithError(
            errorForP0 = Errors.NONE,
            errorForP1 = Errors.NOT_LEADER_OR_FOLLOWER,
            offsetForP0 = 10L,
            offsetForP1 = 100L,
            expectedOffsetForP0 = 10L,
            expectedOffsetForP1 = 100L,
        )
        // Test different errors.
        testGetOffsetsForTimesWithError(
            errorForP0 = Errors.NOT_LEADER_OR_FOLLOWER,
            errorForP1 = Errors.NONE,
            offsetForP0 = 10L,
            offsetForP1 = 100L,
            expectedOffsetForP0 = 10L,
            expectedOffsetForP1 = 100L,
        )
        testGetOffsetsForTimesWithError(
            errorForP0 = Errors.UNKNOWN_TOPIC_OR_PARTITION,
            errorForP1 = Errors.NONE,
            offsetForP0 = 10L,
            offsetForP1 = 100L,
            expectedOffsetForP0 = 10L,
            expectedOffsetForP1 = 100L,
        )
        testGetOffsetsForTimesWithError(
            errorForP0 = Errors.UNSUPPORTED_FOR_MESSAGE_FORMAT,
            errorForP1 = Errors.NONE,
            offsetForP0 = 10L,
            offsetForP1 = 100L,
            expectedOffsetForP0 = null,
            expectedOffsetForP1 = 100L,
        )
        testGetOffsetsForTimesWithError(
            errorForP0 = Errors.BROKER_NOT_AVAILABLE,
            errorForP1 = Errors.NONE,
            offsetForP0 = 10L,
            offsetForP1 = 100L,
            expectedOffsetForP0 = 10L,
            expectedOffsetForP1 = 100L,
        )
    }

    @Test
    fun testGetOffsetsFencedLeaderEpoch() {
        buildFetcher()
        subscriptions.assignFromUser(setOf(tp0))
        client!!.updateMetadata(initialUpdateResponse)
        subscriptions.requestOffsetReset(tp0, OffsetResetStrategy.LATEST)
        client!!.prepareResponse(
            response = listOffsetResponse(
                error = Errors.FENCED_LEADER_EPOCH,
                timestamp = 1L,
                offset = 5L,
            ),
        )
        fetcher.resetOffsetsIfNeeded()
        consumerClient!!.pollNoWakeup()

        assertTrue(subscriptions.isOffsetResetNeeded(tp0))
        assertFalse(subscriptions.isFetchable(tp0))
        assertFalse(subscriptions.hasValidPosition(tp0))
        assertEquals(0L, metadata!!.timeToNextUpdate(time.milliseconds()))
    }

    @Test
    fun testGetOffsetByTimeWithPartitionsRetryCouldTriggerMetadataUpdate() {
        val retriableErrors = listOf(
            Errors.NOT_LEADER_OR_FOLLOWER,
            Errors.REPLICA_NOT_AVAILABLE,
            Errors.KAFKA_STORAGE_ERROR,
            Errors.OFFSET_NOT_AVAILABLE,
            Errors.LEADER_NOT_AVAILABLE,
            Errors.FENCED_LEADER_EPOCH,
            Errors.UNKNOWN_LEADER_EPOCH,
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
            client!!.updateMetadata(initialUpdateResponse)
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
                                .setOffset(-1L),
                        )
                    )
            )
            val data = ListOffsetsResponseData()
                .setThrottleTimeMs(0)
                .setTopics(topics)
            client!!.prepareResponseFrom(
                matcher = { body ->
                    val isListOffsetRequest = body is ListOffsetsRequest
                    return@prepareResponseFrom if (isListOffsetRequest) {
                        val request = body as ListOffsetsRequest
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
                        request.topics == expectedTopics
                    } else false
                },
                response = ListOffsetsResponse(data),
                node = originalLeader,
            )
            client!!.prepareMetadataUpdate(updatedMetadata)

            // If the metadata wasn't updated before retrying, the fetcher would consult the original leader and
            // hit a NOT_LEADER exception.
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
                                .setOffset(-1L),
                        )
                    )
            )
            val dataWithFatalError = ListOffsetsResponseData()
                .setThrottleTimeMs(0)
                .setTopics(topicsWithFatalError)
            client!!.prepareResponseFrom(
                response = ListOffsetsResponse(dataWithFatalError),
                node = originalLeader,
            )

            // The request to new leader must only contain one partition tp1 with error.
            client!!.prepareResponseFrom(
                matcher = { body ->
                    val isListOffsetRequest = body is ListOffsetsRequest
                    return@prepareResponseFrom if (isListOffsetRequest) {
                        val request = body as ListOffsetsRequest
                        val requestTopic = request.topics[0]
                        val expectedPartition = ListOffsetsPartition()
                            .setPartitionIndex(tp1.partition)
                            .setTimestamp(fetchTimestamp)
                            .setCurrentLeaderEpoch(newLeaderEpoch)
                        expectedPartition == requestTopic.partitions[0]
                    } else false
                },
                response = listOffsetResponse(tp1, Errors.NONE, fetchTimestamp, 5L),
                node = newLeader,
            )
            val offsetAndTimestampMap = fetcher.offsetsForTimes(
                timestampsToSearch = mapOf(
                    tp0 to fetchTimestamp,
                    tp1 to fetchTimestamp,
                ),
                timer = time.timer(timeoutMs = Int.MAX_VALUE.toLong()),
            )
            assertEquals(
                expected = mapOf(
                    tp0 to OffsetAndTimestamp(offset = 4L, timestamp = fetchTimestamp),
                    tp1 to OffsetAndTimestamp(offset = 5L, timestamp = fetchTimestamp),
                ),
                actual = offsetAndTimestampMap,
            )

            // The NOT_LEADER exception future should not be cleared as we already refreshed the metadata before
            // first retry, thus never hitting.
            assertEquals(1, client!!.numAwaitingResponses())
            fetcher.close()
        }
    }

    @Test
    fun testGetOffsetsUnknownLeaderEpoch() {
        buildFetcher()
        subscriptions.assignFromUser(setOf(tp0))
        subscriptions.requestOffsetReset(tp0, OffsetResetStrategy.LATEST)
        client!!.prepareResponse(
            response = listOffsetResponse(
                error = Errors.UNKNOWN_LEADER_EPOCH,
                timestamp = 1L,
                offset = 5L,
            ),
        )
        fetcher.resetOffsetsIfNeeded()
        consumerClient!!.pollNoWakeup()
        assertTrue(subscriptions.isOffsetResetNeeded(tp0))
        assertFalse(subscriptions.isFetchable(tp0))
        assertFalse(subscriptions.hasValidPosition(tp0))
        assertEquals(0L, metadata!!.timeToNextUpdate(time.milliseconds()))
    }

    @Test
    fun testGetOffsetsIncludesLeaderEpoch() {
        buildFetcher()
        subscriptions.assignFromUser(setOf(tp0))
        client!!.updateMetadata(initialUpdateResponse)

        // Metadata update with leader epochs
        val metadataResponse = metadataUpdateWith(
            clusterId = "dummy",
            numNodes = 1,
            topicErrors = emptyMap(),
            topicPartitionCounts = mapOf(topicName to 4),
            epochSupplier = { 99 },
            topicIds = topicIds,
        )
        client!!.updateMetadata(metadataResponse)

        // Request latest offset
        subscriptions.requestOffsetReset(tp0)
        fetcher.resetOffsetsIfNeeded()

        // Check for epoch in outgoing request
        val matcher = RequestMatcher { body ->
            assertIs<ListOffsetsRequest>(
                value = body,
                message = "Should have seen ListOffsetRequest",
            )
            val epoch = body.topics[0].partitions[0].currentLeaderEpoch
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
        }
        client!!.prepareResponse(
            matcher = matcher,
            response = listOffsetResponse(
                error = Errors.NONE,
                timestamp = 1L,
                offset = 5L,
            ),
        )
        consumerClient!!.pollNoWakeup()
    }

    @Test
    fun testGetOffsetsForTimesWhenSomeTopicPartitionLeadersNotKnownInitially() {
        buildFetcher()
        subscriptions.assignFromUser(setOf(tp0, tp1))
        val anotherTopic = "another-topic"
        val t2p0 = TopicPartition(anotherTopic, 0)
        client!!.reset()

        // Metadata initially has one topic
        val initialMetadata = metadataUpdateWithIds(
            numNodes = 3,
            topicPartitionCounts = mapOf(topicName to 2),
            topicIds = topicIds,
        )
        client!!.updateMetadata(initialMetadata)

        // The first metadata refresh should contain one topic
        client!!.prepareMetadataUpdate(initialMetadata)
        client!!.prepareResponseFrom(
            response = listOffsetResponse(
                tp = tp0,
                error = Errors.NONE,
                timestamp = 1000L,
                offset = 11L,
            ),
            node = metadata!!.fetch().leaderFor(tp0),
        )
        client!!.prepareResponseFrom(
            response = listOffsetResponse(
                tp = tp1,
                error = Errors.NONE,
                timestamp = 1000L,
                offset = 32L,
            ),
            node = metadata!!.fetch().leaderFor(tp1),
        )

        // Second metadata refresh should contain two topics
        val partitionNumByTopic = mapOf(
            topicName to 2,
            anotherTopic to 1,
        )
        topicIds["another-topic"] = Uuid.randomUuid()
        val updatedMetadata = metadataUpdateWithIds(
            numNodes = 3,
            topicPartitionCounts = partitionNumByTopic,
            topicIds = topicIds,
        )
        client!!.prepareMetadataUpdate(updatedMetadata)
        client!!.prepareResponseFrom(
            response = listOffsetResponse(
                tp = t2p0,
                error = Errors.NONE,
                timestamp = 1000L,
                offset = 54L,
            ),
            node = metadata!!.fetch().leaderFor(t2p0),
        )
        val timestampToSearch = mapOf(
            tp0 to ListOffsetsRequest.LATEST_TIMESTAMP,
            tp1 to ListOffsetsRequest.LATEST_TIMESTAMP,
            t2p0 to ListOffsetsRequest.LATEST_TIMESTAMP,
        )
        val offsetAndTimestampMap = fetcher.offsetsForTimes(timestampToSearch, time.timer(Long.MAX_VALUE))
        assertNotNull(
            actual = offsetAndTimestampMap[tp0],
            message = "Expect Fetcher.offsetsForTimes() to return non-null result for $tp0",
        )
        assertNotNull(
            actual = offsetAndTimestampMap[tp1],
            message = "Expect Fetcher.offsetsForTimes() to return non-null result for $tp1",
        )
        assertNotNull(
            actual = offsetAndTimestampMap[t2p0],
            message = "Expect Fetcher.offsetsForTimes() to return non-null result for $t2p0",
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
        client!!.reset()
        val initialMetadata = metadataUpdateWithIds(
            numNodes = 1,
            topicPartitionCounts = mapOf(topicName to 1),
            topicIds = topicIds,
        )
        client!!.updateMetadata(initialMetadata)
        val partitionNumByTopic: MutableMap<String, Int> = HashMap()
        partitionNumByTopic[topicName] = 1
        partitionNumByTopic[anotherTopic] = 1
        topicIds["another-topic"] = Uuid.randomUuid()
        val updatedMetadata = metadataUpdateWithIds(
            numNodes = 1,
            topicPartitionCounts = partitionNumByTopic,
            topicIds = topicIds,
        )
        client!!.prepareMetadataUpdate(updatedMetadata)
        client!!.prepareResponse(
            matcher = listOffsetRequestMatcher(timestamp = ListOffsetsRequest.LATEST_TIMESTAMP),
            response = listOffsetResponse(
                tp = tp0,
                error = Errors.NONE,
                timestamp = 1000L,
                offset = 11L,
            ),
            disconnected = true,
        )
        client!!.prepareResponseFrom(
            response = listOffsetResponse(
                tp = tp0,
                error = Errors.NONE,
                timestamp = 1000L,
                offset = 11L,
            ),
            node = metadata!!.fetch().leaderFor(tp0),
        )
        val timestampToSearch = mapOf(tp0 to ListOffsetsRequest.LATEST_TIMESTAMP)
        val offsetAndTimestampMap = fetcher.offsetsForTimes(timestampToSearch, time.timer(Long.MAX_VALUE))
        assertNotNull(
            actual = offsetAndTimestampMap[tp0],
            message = "Expect Fetcher.offsetsForTimes() to return non-null result for $tp0",
        )
        assertEquals(11L, offsetAndTimestampMap[tp0]!!.offset)
        assertNotNull(metadata!!.fetch().partitionCountForTopic(anotherTopic))
    }

    @Test
    @Disabled("Kotlin Migration: Offsets to expect require a value, since types will be incompatible.")
    fun testListOffsetsWithZeroTimeout() {
//        buildFetcher()
//        val offsetsToSearch = mapOf(
//            tp0 to ListOffsetsRequest.EARLIEST_TIMESTAMP,
//            tp1 to ListOffsetsRequest.EARLIEST_TIMESTAMP,
//        )
//        val offsetsToExpect = mapOf<TopicPartition, Long?>(
//            tp0 to null,
//            tp1 to null,
//        )
//        assertEquals(
//            expected = offsetsToExpect,
//            actual = fetcher.offsetsForTimes(
//                timestampsToSearch = offsetsToSearch,
//                timer = time.timer(0),
//            ),
//        )
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
        client!!.prepareResponse(ListOffsetsResponse(data))
        val offsetsToSearch = mapOf(
            tp0 to ListOffsetsRequest.EARLIEST_TIMESTAMP,
            tp1 to ListOffsetsRequest.EARLIEST_TIMESTAMP,
        )

        assertFailsWith<TimeoutException> {
            fetcher.offsetsForTimes(timestampsToSearch = offsetsToSearch, timer = time.timer(1))
        }
    }

    @Test
    fun testSkippingAbortedTransactions() {
        buildFetcher(
            offsetResetStrategy = OffsetResetStrategy.EARLIEST,
            keyDeserializer = ByteArrayDeserializer(),
            valueDeserializer = ByteArrayDeserializer(),
            maxPollRecords = Int.MAX_VALUE,
            isolationLevel = IsolationLevel.READ_COMMITTED
        )
        val buffer = ByteBuffer.allocate(1024)
        var currentOffset = 0
        currentOffset += appendTransactionalRecords(
            buffer = buffer,
            pid = 1L,
            baseOffset = currentOffset.toLong(),
            records = arrayOf(
                SimpleRecord(time.milliseconds(), "key".toByteArray(), "value".toByteArray()),
                SimpleRecord(time.milliseconds(), "key".toByteArray(), "value".toByteArray()),
            ),
        )
        abortTransaction(buffer = buffer, producerId = 1L, baseOffset = currentOffset.toLong())
        buffer.flip()
        val abortedTransactions = listOf(AbortedTransaction().setProducerId(1).setFirstOffset(0))
        val records = MemoryRecords.readableRecords(buffer)
        assignFromUser(setOf(tp0))
        subscriptions.seek(tp0, 0)

        // normal fetch
        assertEquals(1, fetcher.sendFetches())
        assertFalse(fetcher.hasCompletedFetches())

        client!!.prepareResponse(
            response = fullFetchResponseWithAbortedTransactions(
                records = records,
                abortedTransactions = abortedTransactions,
                error = Errors.NONE,
                lastStableOffset = 100L,
                hw = 100L,
                throttleTime = 0,
            ),
        )
        consumerClient!!.poll(time.timer(0))

        assertTrue(fetcher.hasCompletedFetches())

        val fetch = collectFetch<ByteArray, ByteArray>()

        assertEquals(emptyMap(), fetch.records())
        assertTrue(fetch.positionAdvanced())
    }

    @Test
    fun testReturnCommittedTransactions() {
        buildFetcher(
            offsetResetStrategy = OffsetResetStrategy.EARLIEST,
            keyDeserializer = ByteArrayDeserializer(),
            valueDeserializer = ByteArrayDeserializer(),
            maxPollRecords = Int.MAX_VALUE,
            isolationLevel = IsolationLevel.READ_COMMITTED
        )
        val buffer = ByteBuffer.allocate(1024)
        var currentOffset = 0
        currentOffset += appendTransactionalRecords(
            buffer = buffer,
            pid = 1L,
            baseOffset = currentOffset.toLong(),
            records = arrayOf(
                SimpleRecord(time.milliseconds(), "key".toByteArray(), "value".toByteArray()),
                SimpleRecord(time.milliseconds(), "key".toByteArray(), "value".toByteArray()),
            ),
        )
        commitTransaction(buffer = buffer, producerId = 1L, baseOffset = currentOffset.toLong())
        buffer.flip()
        val records = MemoryRecords.readableRecords(buffer)
        assignFromUser(setOf(tp0))
        subscriptions.seek(tp0, 0)

        // normal fetch
        assertEquals(1, fetcher.sendFetches())
        assertFalse(fetcher.hasCompletedFetches())

        client!!.prepareResponse(
            matcher = { body ->
                val request = assertIs<FetchRequest>(body)
                assertEquals(IsolationLevel.READ_COMMITTED, request.isolationLevel())
                true
            },
            response = fullFetchResponseWithAbortedTransactions(
                records = records,
                abortedTransactions = emptyList(),
                error = Errors.NONE,
                lastStableOffset = 100L,
                hw = 100L,
                throttleTime = 0,
            ),
        )
        consumerClient!!.poll(time.timer(0))
        assertTrue(fetcher.hasCompletedFetches())
        val fetchedRecords = fetchedRecords<ByteArray, ByteArray>()
        assertTrue(fetchedRecords.containsKey(tp0))
        assertEquals(fetchedRecords[tp0]!!.size, 2)
    }

    @Test
    fun testReadCommittedWithCommittedAndAbortedTransactions() {
        buildFetcher(
            offsetResetStrategy = OffsetResetStrategy.EARLIEST,
            keyDeserializer = ByteArrayDeserializer(),
            valueDeserializer = ByteArrayDeserializer(),
            maxPollRecords = Int.MAX_VALUE,
            isolationLevel = IsolationLevel.READ_COMMITTED,
        )
        val buffer = ByteBuffer.allocate(1024)
        val abortedTransactions = mutableListOf<AbortedTransaction>()
        val pid1 = 1L
        val pid2 = 2L

        // Appends for producer 1 (eventually committed)
        appendTransactionalRecords(
            buffer = buffer,
            pid = pid1,
            baseOffset = 0L,
            records = arrayOf(
                SimpleRecord(key = "commit1-1".toByteArray(), value = "value".toByteArray()),
                SimpleRecord(key = "commit1-2".toByteArray(), value = "value".toByteArray()),
            ),
        )

        // Appends for producer 2 (eventually aborted)
        appendTransactionalRecords(
            buffer = buffer,
            pid = pid2,
            baseOffset = 2L,
            records = arrayOf(SimpleRecord(key = "abort2-1".toByteArray(), value = "value".toByteArray())),
        )

        // commit producer 1
        commitTransaction(buffer = buffer, producerId = pid1, baseOffset = 3L)

        // append more for producer 2 (eventually aborted)
        appendTransactionalRecords(
            buffer = buffer,
            pid = pid2,
            baseOffset = 4L,
            records = arrayOf(SimpleRecord(key = "abort2-2".toByteArray(), value = "value".toByteArray())),
        )

        // abort producer 2
        abortTransaction(buffer = buffer, producerId = pid2, baseOffset = 5L)
        abortedTransactions.add(AbortedTransaction().setProducerId(pid2).setFirstOffset(2L))

        // New transaction for producer 1 (eventually aborted)
        appendTransactionalRecords(
            buffer = buffer,
            pid = pid1,
            baseOffset = 6L,
            records = arrayOf(SimpleRecord(key = "abort1-1".toByteArray(), value = "value".toByteArray())),
        )

        // New transaction for producer 2 (eventually committed)
        appendTransactionalRecords(
            buffer = buffer,
            pid = pid2,
            baseOffset = 7L,
            records = arrayOf(SimpleRecord(key = "commit2-1".toByteArray(), value = "value".toByteArray())),
        )

        // Add messages for producer 1 (eventually aborted)
        appendTransactionalRecords(
            buffer = buffer,
            pid = pid1,
            baseOffset = 8L,
            records = arrayOf(SimpleRecord(key = "abort1-2".toByteArray(), value = "value".toByteArray())),
        )

        // abort producer 1
        abortTransaction(buffer = buffer, producerId = pid1, baseOffset = 9L)
        abortedTransactions.add(AbortedTransaction().setProducerId(1).setFirstOffset(6))

        // commit producer 2
        commitTransaction(buffer = buffer, producerId = pid2, baseOffset = 10L)
        buffer.flip()
        val records = MemoryRecords.readableRecords(buffer)
        assignFromUser(setOf(tp0))
        subscriptions.seek(tp0, 0)

        // normal fetch
        assertEquals(1, fetcher.sendFetches())
        assertFalse(fetcher.hasCompletedFetches())
        client!!.prepareResponse(
            response = fullFetchResponseWithAbortedTransactions(
                records = records,
                abortedTransactions = abortedTransactions,
                error = Errors.NONE,
                lastStableOffset = 100L,
                hw = 100L,
                throttleTime = 0,
            ),
        )
        consumerClient!!.poll(time.timer(0))
        assertTrue(fetcher.hasCompletedFetches())
        val fetchedRecords = fetchedRecords<ByteArray, ByteArray>()
        assertTrue(fetchedRecords.containsKey(tp0))
        // There are only 3 committed records
        val fetchedConsumerRecords = fetchedRecords[tp0]!!
        val fetchedKeys = fetchedConsumerRecords.map { record -> String(record.key) }.toSet()
        assertEquals(setOf("commit1-1", "commit1-2", "commit2-1"), fetchedKeys)
    }

    @Test
    fun testMultipleAbortMarkers() {
        buildFetcher(
            offsetResetStrategy = OffsetResetStrategy.EARLIEST,
            keyDeserializer = ByteArrayDeserializer(),
            valueDeserializer = ByteArrayDeserializer(),
            maxPollRecords = Int.MAX_VALUE,
            isolationLevel = IsolationLevel.READ_COMMITTED
        )
        val buffer = ByteBuffer.allocate(1024)
        var currentOffset = 0
        currentOffset += appendTransactionalRecords(
            buffer = buffer,
            pid = 1L,
            baseOffset = currentOffset.toLong(),
            records = arrayOf(
                SimpleRecord(
                    timestamp = time.milliseconds(),
                    key = "abort1-1".toByteArray(),
                    value = "value".toByteArray(),
                ),
                SimpleRecord(
                    timestamp = time.milliseconds(),
                    key = "abort1-2".toByteArray(),
                    value = "value".toByteArray(),
                ),
            ),
        )
        currentOffset += abortTransaction(buffer, 1L, currentOffset.toLong())
        // Duplicate abort -- should be ignored.
        currentOffset += abortTransaction(buffer, 1L, currentOffset.toLong())
        // Now commit a transaction.
        currentOffset += appendTransactionalRecords(
            buffer = buffer,
            pid = 1L,
            baseOffset = currentOffset.toLong(),
            records = arrayOf(
                SimpleRecord(
                    timestamp = time.milliseconds(),
                    key = "commit1-1".toByteArray(),
                    value = "value".toByteArray(),
                ),
                SimpleRecord(
                    timestamp = time.milliseconds(),
                    key = "commit1-2".toByteArray(),
                    value = "value".toByteArray(),
                ),
            ),
        )
        commitTransaction(buffer, 1L, currentOffset.toLong())
        buffer.flip()
        val abortedTransactions = listOf(AbortedTransaction().setProducerId(1).setFirstOffset(0))
        val records = MemoryRecords.readableRecords(buffer)
        assignFromUser(setOf(tp0))
        subscriptions.seek(tp0, 0)

        // normal fetch
        assertEquals(1, fetcher.sendFetches())
        assertFalse(fetcher.hasCompletedFetches())
        client!!.prepareResponse(
            response = fullFetchResponseWithAbortedTransactions(
                records = records,
                abortedTransactions = abortedTransactions,
                error = Errors.NONE,
                lastStableOffset = 100L,
                hw = 100L,
                throttleTime = 0,
            ),
        )
        consumerClient!!.poll(time.timer(0))

        assertTrue(fetcher.hasCompletedFetches())

        val fetchedRecords = fetchedRecords<ByteArray, ByteArray>()

        assertTrue(fetchedRecords.containsKey(tp0))
        assertEquals(fetchedRecords[tp0]!!.size, 2)

        val fetchedConsumerRecords = fetchedRecords[tp0]!!
        val committedKeys = setOf("commit1-1", "commit1-2")
        val actuallyCommittedKeys = fetchedConsumerRecords.map { record -> String(record.key) }.toSet()

        assertEquals(actuallyCommittedKeys, committedKeys)
    }

    @Test
    fun testReadCommittedAbortMarkerWithNoData() {
        buildFetcher(
            offsetResetStrategy = OffsetResetStrategy.EARLIEST,
            keyDeserializer = StringDeserializer(),
            valueDeserializer = StringDeserializer(),
            maxPollRecords = Int.MAX_VALUE,
            isolationLevel = IsolationLevel.READ_COMMITTED,
        )
        val buffer = ByteBuffer.allocate(1024)
        val producerId = 1L
        abortTransaction(buffer, producerId, 5L)
        appendTransactionalRecords(
            buffer = buffer,
            pid = producerId,
            baseOffset = 6L,
            records = arrayOf(
                SimpleRecord(key = "6".toByteArray(), value = null),
                SimpleRecord(key = "7".toByteArray(), value = null),
                SimpleRecord(key = "8".toByteArray(), value = null),
            )
        )
        commitTransaction(buffer = buffer, producerId = producerId, baseOffset = 9L)
        buffer.flip()

        // send the fetch
        assignFromUser(setOf(tp0))
        subscriptions.seek(tp0, 0)
        assertEquals(1, fetcher.sendFetches())

        // prepare the response. the aborted transactions begin at offsets which are no longer in the log
        val abortedTransactions = listOf(AbortedTransaction().setProducerId(producerId).setFirstOffset(0L))
        client!!.prepareResponse(
            response = fullFetchResponseWithAbortedTransactions(
                records = MemoryRecords.readableRecords(buffer),
                abortedTransactions = abortedTransactions,
                error = Errors.NONE,
                lastStableOffset = 100L,
                hw = 100L,
                throttleTime = 0,
            ),
        )
        consumerClient!!.poll(time.timer(0))

        assertTrue(fetcher.hasCompletedFetches())

        val allFetchedRecords = fetchedRecords<String, String>()

        assertTrue(allFetchedRecords.containsKey(tp0))

        val fetchedRecords = allFetchedRecords[tp0]!!

        assertEquals(3, fetchedRecords.size)
        assertEquals(mutableListOf(6L, 7L, 8L), collectRecordOffsets(fetchedRecords))
    }

    @Test
    fun testUpdatePositionWithLastRecordMissingFromBatch() {
        buildFetcher()
        val records = MemoryRecords.withRecords(
            compressionType = CompressionType.NONE,
            records = arrayOf(
                SimpleRecord(key = "0".toByteArray(), value = "v".toByteArray()),
                SimpleRecord(key = "1".toByteArray(), value = "v".toByteArray()),
                SimpleRecord(key = "2".toByteArray(), value = "v".toByteArray()),
                SimpleRecord(key = null, value = "value".toByteArray()),
            ),
        )

        // Remove the last record to simulate compaction
        val result = records.filterTo(
            partition = tp0,
            filter = object : RecordFilter(currentTime = 0, deleteRetentionMs = 0) {
                override fun checkBatchRetention(batch: RecordBatch): BatchRetentionResult {
                    return BatchRetentionResult(
                        batchRetention = BatchRetention.DELETE_EMPTY,
                        containsMarkerForEmptyTxn = false,
                    )
                }

                override fun shouldRetainRecord(recordBatch: RecordBatch, record: Record): Boolean {
                    return record.key() != null
                }
            },
            destinationBuffer = ByteBuffer.allocate(1024),
            maxRecordBatchSize = Int.MAX_VALUE,
            decompressionBufferSupplier = BufferSupplier.NO_CACHING,
        )
        result.outputBuffer().flip()
        val compactedRecords = MemoryRecords.readableRecords(result.outputBuffer())
        assignFromUser(setOf(tp0))
        subscriptions.seek(tp0, 0)

        assertEquals(1, fetcher.sendFetches())

        client!!.prepareResponse(
            response = fullFetchResponse(
                tp = tidp0,
                records = compactedRecords,
                error = Errors.NONE,
                hw = 100L,
                throttleTime = 0,
            ),
        )
        consumerClient!!.poll(time.timer(0))

        assertTrue(fetcher.hasCompletedFetches())

        val allFetchedRecords = fetchedRecords<ByteArray, ByteArray>()

        assertTrue(allFetchedRecords.containsKey(tp0))

        val fetchedRecords = allFetchedRecords[tp0]!!

        assertEquals(3, fetchedRecords.size)
        for (i in 0..2) assertEquals(i.toString(), String(fetchedRecords[i].key))

        // The next offset should point to the next batch
        assertEquals(4L, subscriptions.position(tp0)!!.offset)
    }

    @Test
    fun testUpdatePositionOnEmptyBatch() {
        buildFetcher()
        val producerId = 1L
        val producerEpoch: Short = 0
        val sequence = 1
        val baseOffset = 37L
        val lastOffset = 54L
        val partitionLeaderEpoch = 7
        val buffer = ByteBuffer.allocate(DefaultRecordBatch.RECORD_BATCH_OVERHEAD)
        DefaultRecordBatch.writeEmptyHeader(
            buffer = buffer,
            magic = RecordBatch.CURRENT_MAGIC_VALUE,
            producerId = producerId,
            producerEpoch = producerEpoch,
            baseSequence = sequence,
            baseOffset = baseOffset,
            lastOffset = lastOffset,
            partitionLeaderEpoch = partitionLeaderEpoch,
            timestampType = TimestampType.CREATE_TIME,
            timestamp = System.currentTimeMillis(),
            isTransactional = false,
            isControlRecord = false,
        )
        buffer.flip()
        val recordsWithEmptyBatch = MemoryRecords.readableRecords(buffer)
        assignFromUser(setOf(tp0))
        subscriptions.seek(tp0, 0)

        assertEquals(1, fetcher.sendFetches())

        client!!.prepareResponse(
            response = fullFetchResponse(
                tp = tidp0,
                records = recordsWithEmptyBatch,
                error = Errors.NONE,
                hw = 100L,
                throttleTime = 0,
            ),
        )
        consumerClient!!.poll(time.timer(0))

        assertTrue(fetcher.hasCompletedFetches())

        val fetch = collectFetch<ByteArray, ByteArray>()

        assertEquals(emptyMap(), fetch.records())
        assertTrue(fetch.positionAdvanced())

        // The next offset should point to the next batch
        assertEquals(lastOffset + 1, subscriptions.position(tp0)!!.offset)
    }

    @Test
    fun testReadCommittedWithCompactedTopic() {
        buildFetcher(
            offsetResetStrategy = OffsetResetStrategy.EARLIEST,
            keyDeserializer = StringDeserializer(),
            valueDeserializer = StringDeserializer(),
            maxPollRecords = Int.MAX_VALUE,
            isolationLevel = IsolationLevel.READ_COMMITTED,
        )
        val buffer = ByteBuffer.allocate(1024)
        val pid1 = 1L
        val pid2 = 2L
        val pid3 = 3L
        appendTransactionalRecords(
            buffer = buffer,
            pid = pid3,
            baseOffset = 3L,
            records = arrayOf(
                SimpleRecord(key = "3".toByteArray(), value = "value".toByteArray()),
                SimpleRecord(key = "4".toByteArray(), value = "value".toByteArray()),
            )
        )
        appendTransactionalRecords(
            buffer = buffer,
            pid = pid2,
            baseOffset = 15L,
            records = arrayOf(
                SimpleRecord(key = "15".toByteArray(), value = "value".toByteArray()),
                SimpleRecord(key = "16".toByteArray(), value = "value".toByteArray()),
                SimpleRecord(key = "17".toByteArray(), value = "value".toByteArray()),
            ),
        )
        appendTransactionalRecords(
            buffer = buffer,
            pid = pid1,
            baseOffset = 22L,
            records = arrayOf(
                SimpleRecord(key = "22".toByteArray(), value = "value".toByteArray()),
                SimpleRecord(key = "23".toByteArray(), value = "value".toByteArray()),
            ),
        )
        abortTransaction(buffer, pid2, 28L)
        appendTransactionalRecords(
            buffer = buffer,
            pid = pid3,
            baseOffset = 30L,
            records = arrayOf(
                SimpleRecord(key = "30".toByteArray(), value = "value".toByteArray()),
                SimpleRecord(key = "31".toByteArray(), value = "value".toByteArray()),
                SimpleRecord(key = "32".toByteArray(), value = "value".toByteArray()),
            ),
        )
        commitTransaction(buffer, pid3, 35L)
        appendTransactionalRecords(
            buffer = buffer,
            pid = pid1,
            baseOffset = 39L,
            records = arrayOf(
                SimpleRecord(key = "39".toByteArray(), value = "value".toByteArray()),
                SimpleRecord(key = "40".toByteArray(), value = "value".toByteArray()),
            ),
        )

        // transaction from pid1 is aborted, but the marker is not included in the fetch
        buffer.flip()

        // send the fetch
        assignFromUser(setOf(tp0))
        subscriptions.seek(tp0, 0)
        assertEquals(1, fetcher.sendFetches())

        // prepare the response. the aborted transactions begin at offsets which are no longer in the log
        val abortedTransactions = listOf(
            AbortedTransaction().setProducerId(pid2).setFirstOffset(6),
            AbortedTransaction().setProducerId(pid1).setFirstOffset(0),
        )
        client!!.prepareResponse(
            response = fullFetchResponseWithAbortedTransactions(
                records = MemoryRecords.readableRecords(buffer),
                abortedTransactions = abortedTransactions,
                error = Errors.NONE,
                lastStableOffset = 100L,
                hw = 100L,
                throttleTime = 0,
            )
        )
        consumerClient!!.poll(time.timer(0))
        assertTrue(fetcher.hasCompletedFetches())

        val allFetchedRecords = fetchedRecords<String, String>()
        assertTrue(allFetchedRecords.containsKey(tp0))

        val fetchedRecords = allFetchedRecords[tp0]!!

        assertEquals(5, fetchedRecords.size)
        assertEquals(listOf(3L, 4L, 30L, 31L, 32L), collectRecordOffsets(fetchedRecords))
    }

    @Test
    fun testReturnAbortedTransactionsinUncommittedMode() {
        buildFetcher(
            offsetResetStrategy = OffsetResetStrategy.EARLIEST,
            keyDeserializer = ByteArrayDeserializer(),
            valueDeserializer = ByteArrayDeserializer(),
            maxPollRecords = Int.MAX_VALUE,
            isolationLevel = IsolationLevel.READ_UNCOMMITTED,
        )
        val buffer = ByteBuffer.allocate(1024)
        var currentOffset = 0
        currentOffset += appendTransactionalRecords(
            buffer = buffer,
            pid = 1L,
            baseOffset = currentOffset.toLong(),
            records = arrayOf(
                SimpleRecord(
                    timestamp = time.milliseconds(),
                    key = "key".toByteArray(),
                    value = "value".toByteArray(),
                ),
                SimpleRecord(
                    timestamp = time.milliseconds(),
                    key = "key".toByteArray(),
                    value = "value".toByteArray(),
                ),
            )
        )
        abortTransaction(buffer = buffer, producerId = 1L, baseOffset = currentOffset.toLong())
        buffer.flip()
        val abortedTransactions = listOf(AbortedTransaction().setProducerId(1).setFirstOffset(0))
        val records = MemoryRecords.readableRecords(buffer)
        assignFromUser(setOf(tp0))
        subscriptions.seek(tp0, 0)

        // normal fetch
        assertEquals(1, fetcher.sendFetches())
        assertFalse(fetcher.hasCompletedFetches())
        client!!.prepareResponse(
            response = fullFetchResponseWithAbortedTransactions(
                records = records,
                abortedTransactions = abortedTransactions,
                error = Errors.NONE,
                lastStableOffset = 100L,
                hw = 100L,
                throttleTime = 0,
            ),
        )
        consumerClient!!.poll(time.timer(0))
        assertTrue(fetcher.hasCompletedFetches())

        val fetchedRecords = fetchedRecords<ByteArray, ByteArray>()
        assertTrue(fetchedRecords.containsKey(tp0))
    }

    @Test
    fun testConsumerPositionUpdatedWhenSkippingAbortedTransactions() {
        buildFetcher(
            offsetResetStrategy = OffsetResetStrategy.EARLIEST,
            keyDeserializer = ByteArrayDeserializer(),
            valueDeserializer = ByteArrayDeserializer(),
            maxPollRecords = Int.MAX_VALUE,
            isolationLevel = IsolationLevel.READ_COMMITTED,
        )
        val buffer = ByteBuffer.allocate(1024)
        var currentOffset: Long = 0
        currentOffset += appendTransactionalRecords(
            buffer = buffer,
            pid = 1L,
            baseOffset = currentOffset,
            records = arrayOf(
                SimpleRecord(
                    timestamp = time.milliseconds(),
                    key = "abort1-1".toByteArray(),
                    value = "value".toByteArray(),
                ),
                SimpleRecord(
                    timestamp = time.milliseconds(),
                    key = "abort1-2".toByteArray(),
                    value = "value".toByteArray(),
                ),
            ),
        ).toLong()
        currentOffset += abortTransaction(buffer, 1L, currentOffset).toLong()
        buffer.flip()
        val abortedTransactions = listOf(AbortedTransaction().setProducerId(1).setFirstOffset(0))
        val records = MemoryRecords.readableRecords(buffer)
        assignFromUser(setOf(tp0))
        subscriptions.seek(tp0, 0)

        // normal fetch
        assertEquals(1, fetcher.sendFetches())
        assertFalse(fetcher.hasCompletedFetches())
        client!!.prepareResponse(
            response = fullFetchResponseWithAbortedTransactions(
                records = records,
                abortedTransactions = abortedTransactions,
                error = Errors.NONE,
                lastStableOffset = 100L,
                hw = 100L,
                throttleTime = 0,
            )
        )
        consumerClient!!.poll(time.timer(0))
        assertTrue(fetcher.hasCompletedFetches())
        val fetchedRecords = fetchedRecords<ByteArray, ByteArray>()

        // Ensure that we don't return any of the aborted records, but yet advance the consumer position.
        assertFalse(fetchedRecords.containsKey(tp0))
        assertEquals(currentOffset, subscriptions.position(tp0)!!.offset)
    }

    @Test
    fun testConsumingViaIncrementalFetchRequests() {
        buildFetcher(maxPollRecords = 2)
        var records: List<ConsumerRecord<ByteArray?, ByteArray?>>
        assignFromUser(setOf(tp0, tp1))
        subscriptions.seekValidated(
            tp = tp0,
            position = FetchPosition(
                offset = 0,
                offsetEpoch = null,
                currentLeader = metadata!!.currentLeader(tp0),
            ),
        )
        subscriptions.seekValidated(
            tp = tp1,
            position = FetchPosition(
                offset = 1,
                offsetEpoch = null,
                currentLeader = metadata!!.currentLeader(tp1),
            ),
        )

        // Fetch some records and establish an incremental fetch session.
        val partitions1 = LinkedHashMap<TopicIdPartition, FetchResponseData.PartitionData>()
        partitions1[tidp0] = FetchResponseData.PartitionData()
            .setPartitionIndex(tp0.partition)
            .setHighWatermark(2)
            .setLastStableOffset(2)
            .setLogStartOffset(0)
            .setRecords(this.records)
        partitions1[tidp1] = FetchResponseData.PartitionData()
            .setPartitionIndex(tp1.partition)
            .setHighWatermark(100)
            .setLogStartOffset(0)
            .setRecords(emptyRecords)
        val resp1 = FetchResponse.of(
            error = Errors.NONE,
            throttleTimeMs = 0,
            sessionId = 123,
            responseData = partitions1,
        )
        client!!.prepareResponse(resp1)
        assertEquals(1, fetcher.sendFetches())
        assertFalse(fetcher.hasCompletedFetches())

        consumerClient!!.poll(time.timer(0))
        assertTrue(fetcher.hasCompletedFetches())

        var fetchedRecords = fetchedRecords<ByteArray?, ByteArray?>()
        assertFalse(fetchedRecords.containsKey(tp1))

        records = fetchedRecords[tp0]!!
        assertEquals(2, records.size)
        assertEquals(3L, subscriptions.position(tp0)!!.offset)
        assertEquals(1L, subscriptions.position(tp1)!!.offset)
        assertEquals(1, records[0].offset)
        assertEquals(2, records[1].offset)

        // There is still a buffered record.
        assertEquals(0, fetcher.sendFetches())
        fetchedRecords = fetchedRecords()
        assertFalse(fetchedRecords.containsKey(tp1))
        records = fetchedRecords[tp0]!!
        assertEquals(1, records.size)
        assertEquals(3, records[0].offset)
        assertEquals(4L, subscriptions.position(tp0)!!.offset)

        // The second response contains no new records.
        val partitions2 = LinkedHashMap<TopicIdPartition, FetchResponseData.PartitionData>()
        val resp2 = FetchResponse.of(
            error = Errors.NONE,
            throttleTimeMs = 0,
            sessionId = 123,
            responseData = partitions2,
        )
        client!!.prepareResponse(resp2)
        assertEquals(1, fetcher.sendFetches())
        consumerClient!!.poll(time.timer(0))
        fetchedRecords = fetchedRecords()
        assertTrue(fetchedRecords.isEmpty())
        assertEquals(4L, subscriptions.position(tp0)!!.offset)
        assertEquals(1L, subscriptions.position(tp1)!!.offset)

        // The third response contains some new records for tp0.
        val partitions3 = LinkedHashMap<TopicIdPartition, FetchResponseData.PartitionData>()
        partitions3[tidp0] = FetchResponseData.PartitionData()
            .setPartitionIndex(tp0.partition)
            .setHighWatermark(100)
            .setLastStableOffset(4)
            .setLogStartOffset(0)
            .setRecords(nextRecords)
        val resp3 = FetchResponse.of(
            error = Errors.NONE,
            throttleTimeMs = 0,
            sessionId = 123,
            responseData = partitions3,
        )
        client!!.prepareResponse(resp3)
        assertEquals(1, fetcher.sendFetches())
        consumerClient!!.poll(time.timer(0))
        fetchedRecords = fetchedRecords()
        assertFalse(fetchedRecords.containsKey(tp1))

        records = fetchedRecords[tp0]!!
        assertEquals(2, records.size)
        assertEquals(6L, subscriptions.position(tp0)!!.offset)
        assertEquals(1L, subscriptions.position(tp1)!!.offset)
        assertEquals(4, records[0].offset)
        assertEquals(5, records[1].offset)
    }

    @Test
    @Throws(Exception::class)
    fun testFetcherConcurrency() {
        val numPartitions = 20
        val topicPartitions = mutableSetOf<TopicPartition>()
        for (i in 0 until numPartitions) topicPartitions.add(TopicPartition(topicName, i))
        val logContext = LogContext()
        buildDependencies(
            MetricConfig(),
            Long.MAX_VALUE,
            SubscriptionState(logContext, OffsetResetStrategy.EARLIEST),
            logContext
        )
        fetcher = object : Fetcher<ByteArray, ByteArray>(
            logContext = LogContext(),
            client = consumerClient!!,
            minBytes = minBytes,
            maxBytes = maxBytes,
            maxWaitMs = maxWaitMs,
            fetchSize = fetchSize,
            maxPollRecords = 2 * numPartitions,
            checkCrcs = true,
            clientRackId = "",
            keyDeserializer = ByteArrayDeserializer(),
            valueDeserializer = ByteArrayDeserializer(),
            metadata = metadata!!,
            subscriptions = subscriptions!!,
            metrics = metrics,
            metricsRegistry = metricsRegistry!!,
            time = time,
            retryBackoffMs = retryBackoffMs,
            requestTimeoutMs = requestTimeoutMs,
            isolationLevel = IsolationLevel.READ_UNCOMMITTED,
            apiVersions = apiVersions,
        ) {
            override fun sessionHandler(node: Int): FetchSessionHandler? {
                val handler = super.sessionHandler(node)
                return if (handler == null) null else {
                    object : FetchSessionHandler(logContext = LogContext(), node = node) {
                        override fun newBuilder(): Builder {
                            verifySessionPartitions()
                            return handler.newBuilder()
                        }

                        override fun handleResponse(response: FetchResponse, version: Short): Boolean {
                            verifySessionPartitions()
                            return handler.handleResponse(response, version)
                        }

                        override fun handleError(t: Throwable?) {
                            verifySessionPartitions()
                            handler.handleError(t)
                        }

                        // Verify that session partitions can be traversed safely.
                        private fun verifySessionPartitions() {
                            try {
                                val field = FetchSessionHandler::class.java.getDeclaredField("sessionPartitions")
                                field.setAccessible(true)
                                val sessionPartitions = field[handler] as LinkedHashMap<*, *>
                                sessionPartitions.forEach {
                                    // If `sessionPartitions` are modified on another thread, Thread.yield will increase the
                                    // possibility of ConcurrentModificationException if appropriate synchronization is not used.
                                    Thread.yield()
                                }
                            } catch (e: Exception) {
                                throw RuntimeException(e)
                            }
                        }
                    }
                }
            }
        }
        val initialMetadataResponse = metadataUpdateWithIds(
            numNodes = 1,
            topicPartitionCounts = mapOf(topicName to numPartitions),
            epochSupplier = { validLeaderEpoch },
            topicIds = topicIds,
        )
        client!!.updateMetadata(initialMetadataResponse)
        fetchSize = 10000
        assignFromUser(topicPartitions)
        topicPartitions.forEach { tp -> subscriptions.seek(tp, 0L) }
        val fetchesRemaining = AtomicInteger(1000)

        executorService = Executors.newSingleThreadExecutor()
        val future = executorService.submit(Callable {
            while (fetchesRemaining.get() > 0) {
                synchronized(consumerClient!!) {
                    if (!client!!.requests().isEmpty()) {
                        val request = client!!.requests().peek()
                        val fetchRequest = request.requestBuilder().build() as FetchRequest
                        val responseMap = LinkedHashMap<TopicIdPartition, FetchResponseData.PartitionData>()
                        for ((tp, value) in fetchRequest.fetchData(topicNames)!!) {
                            val offset = value.fetchOffset
                            responseMap[tp] = FetchResponseData.PartitionData()
                                .setPartitionIndex(tp.topicPartition.partition)
                                .setHighWatermark(offset + 2)
                                .setLastStableOffset(offset + 2)
                                .setLogStartOffset(0)
                                .setRecords(buildRecords(offset, 2, offset))
                        }
                        client!!.respondToRequest(
                            clientRequest = request,
                            response = FetchResponse.of(
                                error = Errors.NONE,
                                throttleTimeMs = 0,
                                sessionId = 123,
                                responseData = responseMap,
                            )
                        )
                        consumerClient!!.poll(time.timer(0))
                    }
                }
            }
            fetchesRemaining.get()
        })

        val nextFetchOffsets = topicPartitions.associateWith { 0L }.toMutableMap()

        while (fetchesRemaining.get() > 0 && !future.isDone) {
            if ((fetcher as Fetcher<ByteArray, ByteArray>).sendFetches() == 1) {
                synchronized(consumerClient!!) { consumerClient!!.poll(time.timer(0)) }
            }
            if ((fetcher as Fetcher<ByteArray, ByteArray>).hasCompletedFetches()) {
                val fetchedRecords = fetchedRecords<ByteArray, ByteArray>()
                if (!fetchedRecords.isEmpty()) {
                    fetchesRemaining.decrementAndGet()
                    fetchedRecords.forEach { (tp, records) ->
                        assertEquals(2, records.size)

                        val nextOffset = nextFetchOffsets[tp]!!
                        assertEquals(nextOffset, records[0].offset)
                        assertEquals(nextOffset + 1, records[1].offset)

                        nextFetchOffsets[tp] = nextOffset + 2
                    }
                }
            }
        }
        assertEquals(0, future.get())
    }

    @Test
    @Throws(Exception::class)
    fun testFetcherSessionEpochUpdate() {
        buildFetcher(maxPollRecords = 2)
        val initialMetadataResponse = metadataUpdateWithIds(
            numNodes = 1,
            topicPartitionCounts = mapOf(topicName to 1),
            topicIds = topicIds,
        )
        client!!.updateMetadata(initialMetadataResponse)
        assignFromUser(setOf(tp0))
        subscriptions.seek(tp0, 0L)
        val fetchesRemaining = AtomicInteger(1000)

        executorService = Executors.newSingleThreadExecutor()
        val future = executorService.submit(Callable {
            var nextOffset: Long = 0
            var nextEpoch: Long = 0
            while (fetchesRemaining.get() > 0) {
                synchronized(consumerClient!!) {
                    if (!client!!.requests().isEmpty()) {
                        val request = client!!.requests().peek()
                        val fetchRequest = request.requestBuilder().build() as FetchRequest
                        val epoch = fetchRequest.metadata().epoch
                        assertTrue(
                            actual = epoch == 0 || epoch.toLong() == nextEpoch,
                            message = "Unexpected epoch expected $nextEpoch got $epoch",
                        )
                        nextEpoch++
                        val responseMap = LinkedHashMap<TopicIdPartition, FetchResponseData.PartitionData>()
                        responseMap[tidp0] = FetchResponseData.PartitionData()
                            .setPartitionIndex(tp0.partition)
                            .setHighWatermark(nextOffset + 2)
                            .setLastStableOffset(nextOffset + 2)
                            .setLogStartOffset(0)
                            .setRecords(buildRecords(nextOffset, 2, nextOffset))
                        nextOffset += 2
                        client!!.respondToRequest(
                            clientRequest = request,
                            response = FetchResponse.of(
                                error = Errors.NONE,
                                throttleTimeMs = 0,
                                sessionId = 123,
                                responseData = responseMap,
                            )
                        )
                        consumerClient!!.poll(time.timer(0))
                    }
                }
            }
            fetchesRemaining.get()
        })
        var nextFetchOffset = 0L
        while (fetchesRemaining.get() > 0 && !future.isDone) {
            if (fetcher.sendFetches() == 1) {
                synchronized(consumerClient!!) { consumerClient!!.poll(time.timer(0)) }
            }
            if (fetcher.hasCompletedFetches()) {
                val fetchedRecords = fetchedRecords<ByteArray, ByteArray>()
                if (fetchedRecords.isNotEmpty()) {
                    fetchesRemaining.decrementAndGet()
                    val records = fetchedRecords[tp0]!!

                    assertEquals(2, records.size)
                    assertEquals(nextFetchOffset, records[0].offset)
                    assertEquals(nextFetchOffset + 1, records[1].offset)

                    nextFetchOffset += 2
                }
                assertTrue(fetchedRecords<Any, Any>().isEmpty())
            }
        }
        assertEquals(0, future.get())
    }

    @Test
    fun testEmptyControlBatch() {
        buildFetcher(
            offsetResetStrategy = OffsetResetStrategy.EARLIEST,
            keyDeserializer = ByteArrayDeserializer(),
            valueDeserializer = ByteArrayDeserializer(),
            maxPollRecords = Int.MAX_VALUE,
            isolationLevel = IsolationLevel.READ_COMMITTED,
        )
        val buffer = ByteBuffer.allocate(1024)
        var currentOffset = 1

        // Empty control batch should not cause an exception
        DefaultRecordBatch.writeEmptyHeader(
            buffer = buffer,
            magic = RecordBatch.MAGIC_VALUE_V2,
            producerId = 1L,
            producerEpoch = 0.toShort(),
            baseSequence = -1,
            baseOffset = 0,
            lastOffset = 0,
            partitionLeaderEpoch = RecordBatch.NO_PARTITION_LEADER_EPOCH,
            timestampType = TimestampType.CREATE_TIME,
            timestamp = time.milliseconds(),
            isTransactional = true,
            isControlRecord = true,
        )
        currentOffset += appendTransactionalRecords(
            buffer = buffer,
            pid = 1L,
            baseOffset = currentOffset.toLong(),
            records = arrayOf(
                SimpleRecord(
                    timestamp = time.milliseconds(),
                    key = "key".toByteArray(),
                    value = "value".toByteArray(),
                ),
                SimpleRecord(
                    timestamp = time.milliseconds(),
                    key = "key".toByteArray(),
                    value = "value".toByteArray(),
                ),
            )
        )
        commitTransaction(buffer, 1L, currentOffset.toLong())
        buffer.flip()
        val records = MemoryRecords.readableRecords(buffer)
        assignFromUser(setOf(tp0))
        subscriptions.seek(tp0, 0)

        // normal fetch
        assertEquals(1, fetcher.sendFetches())
        assertFalse(fetcher.hasCompletedFetches())
        client!!.prepareResponse(
            matcher = { body ->
                val request = assertIs<FetchRequest>(body)
                assertEquals(IsolationLevel.READ_COMMITTED, request.isolationLevel())
                true
            },
            response = fullFetchResponseWithAbortedTransactions(
                records = records,
                abortedTransactions = emptyList(),
                error = Errors.NONE,
                lastStableOffset = 100L,
                hw = 100L,
                throttleTime = 0,
            ),
        )
        consumerClient!!.poll(time.timer(0))
        assertTrue(fetcher.hasCompletedFetches())

        val fetchedRecords = fetchedRecords<ByteArray, ByteArray>()
        assertTrue(fetchedRecords.containsKey(tp0))
        assertEquals(fetchedRecords[tp0]!!.size, 2)
    }

    private fun buildRecords(baseOffset: Long, count: Int, firstMessageId: Long): MemoryRecords {
        val builder: MemoryRecordsBuilder = MemoryRecords.builder(
            buffer = ByteBuffer.allocate(1024),
            compressionType = CompressionType.NONE,
            timestampType = TimestampType.CREATE_TIME,
            baseOffset = baseOffset,
        )
        for (i in 0 until count) builder.append(
            timestamp = 0L,
            key = "key".toByteArray(),
            value = "value-${firstMessageId + i}".toByteArray(),
        )
        return builder.build()
    }

    private fun appendTransactionalRecords(
        buffer: ByteBuffer,
        pid: Long,
        baseOffset: Long,
        baseSequence: Int,
        vararg records: SimpleRecord,
    ): Int {
        val builder = MemoryRecords.builder(
            buffer = buffer,
            magic = RecordBatch.CURRENT_MAGIC_VALUE,
            compressionType = CompressionType.NONE,
            timestampType = TimestampType.CREATE_TIME,
            baseOffset = baseOffset,
            logAppendTime = time.milliseconds(),
            producerId = pid,
            producerEpoch = 0.toShort(),
            baseSequence = baseSequence,
            isTransactional = true,
            partitionLeaderEpoch = RecordBatch.NO_PARTITION_LEADER_EPOCH,
        )
        for (record in records) builder.append(record)
        builder.build()
        return records.size
    }

    @Deprecated(message = "This overload will be removed.")
    private fun appendTransactionalRecords(
        buffer: ByteBuffer,
        pid: Long,
        baseOffset: Long,
        vararg records: SimpleRecord,
    ): Int = appendTransactionalRecords(
        buffer = buffer,
        pid = pid,
        baseOffset = baseOffset,
        baseSequence = baseOffset.toInt(),
        records = records,
    )

    private fun commitTransaction(buffer: ByteBuffer, producerId: Long, baseOffset: Long) {
        val producerEpoch: Short = 0
        val partitionLeaderEpoch = 0
        MemoryRecords.writeEndTransactionalMarker(
            buffer = buffer,
            initialOffset = baseOffset,
            timestamp = time.milliseconds(),
            partitionLeaderEpoch = partitionLeaderEpoch,
            producerId = producerId,
            producerEpoch = producerEpoch,
            marker = EndTransactionMarker(
                controlType = ControlRecordType.COMMIT,
                coordinatorEpoch = 0,
            ),
        )
    }

    private fun abortTransaction(buffer: ByteBuffer, producerId: Long, baseOffset: Long): Int {
        val producerEpoch: Short = 0
        val partitionLeaderEpoch = 0
        MemoryRecords.writeEndTransactionalMarker(
            buffer = buffer,
            initialOffset = baseOffset,
            timestamp = time.milliseconds(),
            partitionLeaderEpoch = partitionLeaderEpoch,
            producerId = producerId,
            producerEpoch = producerEpoch,
            marker = EndTransactionMarker(
                controlType = ControlRecordType.ABORT,
                coordinatorEpoch = 0,
            ),
        )
        return 1
    }

    private fun testGetOffsetsForTimesWithError(
        errorForP0: Errors,
        errorForP1: Errors,
        offsetForP0: Long,
        offsetForP1: Long,
        expectedOffsetForP0: Long?,
        expectedOffsetForP1: Long?,
    ) {
        client!!.reset()
        val topicName2 = "topic2"
        val t2p0 = TopicPartition(topicName2, 0)
        // Expect a metadata refresh.
        metadata!!.bootstrap(
            parseAndValidateAddresses(
                urls = listOf("1.1.1.1:1111"),
                clientDnsLookup = ClientDnsLookup.USE_ALL_DNS_IPS,
            )
        )
        val partitionNumByTopic = mapOf(topicName to 2, topicName2 to 1)
        val updateMetadataResponse = metadataUpdateWithIds(
            numNodes = 2,
            topicPartitionCounts = partitionNumByTopic,
            topicIds = topicIds,
        )
        val updatedCluster = updateMetadataResponse.buildCluster()

        // The metadata refresh should contain all the topics.
        client!!.prepareMetadataUpdate(
            updateResponse = updateMetadataResponse,
            expectMatchMetadataTopics = true,
        )

        // First try should fail due to metadata error.
        client!!.prepareResponseFrom(
            response = listOffsetResponse(
                tp = t2p0,
                error = errorForP0,
                timestamp = offsetForP0,
                offset = offsetForP0,
            ),
            node = updatedCluster.leaderFor(t2p0),
        )
        client!!.prepareResponseFrom(
            response = listOffsetResponse(
                tp = tp1,
                error = errorForP1,
                timestamp = offsetForP1,
                offset = offsetForP1,
            ),
            node = updatedCluster.leaderFor(tp1),
        )
        // Second try should succeed.
        client!!.prepareResponseFrom(
            response = listOffsetResponse(
                tp = t2p0,
                error = Errors.NONE,
                timestamp = offsetForP0,
                offset = offsetForP0,
            ),
            node = updatedCluster.leaderFor(t2p0),
        )
        client!!.prepareResponseFrom(
            response = listOffsetResponse(
                tp = tp1,
                error = Errors.NONE,
                timestamp = offsetForP1,
                offset = offsetForP1
            ),
            node = updatedCluster.leaderFor(tp1),
        )
        val timestampToSearch = mapOf(t2p0 to 0L, tp1 to 0L)
        val offsetAndTimestampMap = fetcher.offsetsForTimes(timestampToSearch, time.timer(Long.MAX_VALUE))
        if (expectedOffsetForP0 == null) assertNull(offsetAndTimestampMap[t2p0]) else {
            assertEquals(expectedOffsetForP0, offsetAndTimestampMap[t2p0]!!.timestamp)
            assertEquals(expectedOffsetForP0, offsetAndTimestampMap[t2p0]!!.offset)
        }
        if (expectedOffsetForP1 == null) assertNull(offsetAndTimestampMap[tp1]) else {
            assertEquals(expectedOffsetForP1, offsetAndTimestampMap[tp1]!!.timestamp)
            assertEquals(expectedOffsetForP1, offsetAndTimestampMap[tp1]!!.offset)
        }
    }

    private fun testGetOffsetsForTimesWithUnknownOffset() {
        client!!.reset()
        // Ensure metadata has both partitions.
        val initialMetadataUpdate = metadataUpdateWithIds(
            numNodes = 1,
            topicPartitionCounts = mapOf(topicName to 1),
            topicIds = topicIds,
        )
        client!!.updateMetadata(initialMetadataUpdate)
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
        client!!.prepareResponseFrom(
            response = ListOffsetsResponse(data),
            node = metadata!!.fetch().leaderFor(tp0),
        )
        val timestampToSearch = mapOf(tp0 to 0L)
        val offsetAndTimestampMap = fetcher.offsetsForTimes(timestampToSearch, time.timer(Long.MAX_VALUE))
        assertTrue(offsetAndTimestampMap.containsKey(tp0))
        assertNull(offsetAndTimestampMap[tp0])
    }

    @Test
    fun testGetOffsetsForTimesWithUnknownOffsetV0() {
        buildFetcher()
        // Empty map
        assertTrue(fetcher.offsetsForTimes(HashMap(), time.timer(100L)).isEmpty())
        // Unknown Offset
        client!!.reset()
        // Ensure metadata has both partition.
        val initialMetadataUpdate = metadataUpdateWithIds(
            numNodes = 1,
            topicPartitionCounts = mapOf(topicName to 1),
            topicIds = topicIds,
        )
        client!!.updateMetadata(initialMetadataUpdate)
        // Force LIST_OFFSETS version 0
        val node = metadata!!.fetch().nodes[0]
        apiVersions.update(
            nodeId = node.idString(),
            nodeApiVersions = NodeApiVersions.create(
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
        client!!.prepareResponseFrom(
            response = ListOffsetsResponse(data),
            node = metadata!!.fetch().leaderFor(tp0),
        )
        val timestampToSearch = mapOf(tp0 to 0L)
        val offsetAndTimestampMap = fetcher.offsetsForTimes(timestampToSearch, time.timer(Long.MAX_VALUE))
        assertTrue(offsetAndTimestampMap.containsKey(tp0))
        assertNull(offsetAndTimestampMap[tp0])
    }

    @Test
    fun testSubscriptionPositionUpdatedWithEpoch() {
        // Create some records that include a leader epoch (1)
        val builder = MemoryRecords.builder(
            buffer = ByteBuffer.allocate(1024),
            magic = RecordBatch.CURRENT_MAGIC_VALUE,
            compressionType = CompressionType.NONE,
            timestampType = TimestampType.CREATE_TIME,
            baseOffset = 0L,
            logAppendTime = RecordBatch.NO_TIMESTAMP,
            producerId = RecordBatch.NO_PRODUCER_ID,
            producerEpoch = RecordBatch.NO_PRODUCER_EPOCH,
            baseSequence = RecordBatch.NO_SEQUENCE,
            isTransactional = false,
            partitionLeaderEpoch = 1,
        )
        builder.appendWithOffset(
            offset = 0L,
            timestamp = 0L,
            key = "key".toByteArray(),
            value = "value-1".toByteArray(),
        )
        builder.appendWithOffset(
            offset = 1L,
            timestamp = 0L,
            key = "key".toByteArray(),
            value = "value-2".toByteArray(),
        )
        builder.appendWithOffset(
            offset = 2L,
            timestamp = 0L,
            key = "key".toByteArray(),
            value = "value-3".toByteArray(),
        )
        val records = builder.build()
        buildFetcher()
        assignFromUser(setOf(tp0))

        // Initialize the epoch=1
        val partitionCounts: MutableMap<String, Int> = HashMap()
        partitionCounts[tp0.topic] = 4
        val metadataResponse = metadataUpdateWith(
            clusterId = "dummy",
            numNodes = 1,
            topicErrors = emptyMap(),
            topicPartitionCounts = partitionCounts,
            epochSupplier = { 1 },
            topicIds = topicIds,
        )
        metadata!!.updateWithCurrentRequestVersion(
            response = metadataResponse,
            isPartialUpdate = false,
            nowMs = 0L,
        )

        // Seek
        subscriptions.seek(tp0, 0)

        // Do a normal fetch
        assertEquals(1, fetcher.sendFetches())
        assertFalse(fetcher.hasCompletedFetches())
        client!!.prepareResponse(
            response = fullFetchResponse(
                tp = tidp0,
                records = records,
                error = Errors.NONE,
                hw = 100L,
                throttleTime = 0,
            ),
        )
        consumerClient!!.pollNoWakeup()
        assertTrue(fetcher.hasCompletedFetches())
        val partitionRecords = fetchedRecords<ByteArray, ByteArray>()
        assertTrue(partitionRecords.containsKey(tp0))
        assertEquals(subscriptions.position(tp0)!!.offset, 3L)
        assertNullable(subscriptions.position(tp0)!!.offsetEpoch) { value -> assertEquals(1, value) }
    }

    @Test
    fun testOffsetValidationRequestGrouping() {
        buildFetcher()
        assignFromUser(setOf(tp0, tp1, tp2, tp3))
        metadata!!.updateWithCurrentRequestVersion(
            response = metadataUpdateWith(
                clusterId = "dummy",
                numNodes = 3,
                topicErrors = emptyMap(),
                topicPartitionCounts = mapOf(topicName to 4),
                epochSupplier = { 5 },
                topicIds = topicIds,
            ),
            isPartialUpdate = false,
            nowMs = 0L,
        )
        for (tp in subscriptions.assignedPartitions()) {
            val leaderAndEpoch = LeaderAndEpoch(
                leader = metadata!!.currentLeader(tp).leader,
                epoch = 4,
            )
            subscriptions.seekUnvalidated(
                tp = tp,
                position = FetchPosition(
                    offset = 0,
                    offsetEpoch = 4,
                    currentLeader = leaderAndEpoch,
                ),
            )
        }
        val allRequestedPartitions = mutableSetOf<TopicPartition>()
        for (node in metadata!!.fetch().nodes) {
            apiVersions.update(node.idString(), NodeApiVersions.create())
            val expectedPartitions = subscriptions.assignedPartitions()
                .filter { tp -> metadata!!.currentLeader(tp).leader == node }
                .toSet()
            assertTrue(expectedPartitions.none { o -> allRequestedPartitions.contains(o) })
            assertTrue(expectedPartitions.isNotEmpty())
            allRequestedPartitions.addAll(expectedPartitions)
            val data = OffsetForLeaderEpochResponseData()
            expectedPartitions.forEach { (topic1, partition) ->
                var topic = data.topics.find(topic1)
                if (topic == null) {
                    topic = OffsetForLeaderTopicResult().setTopic(topic1)
                    data.topics.add(topic)
                }
                topic.partitions += OffsetForLeaderEpochResponseData.EpochEndOffset()
                    .setPartition(partition)
                    .setErrorCode(Errors.NONE.code)
                    .setLeaderEpoch(4)
                    .setEndOffset(0)
            }
            val response = OffsetsForLeaderEpochResponse(data)
            client!!.prepareResponseFrom(
                matcher = { body ->
                    val request = assertIs<OffsetsForLeaderEpochRequest>(body)
                    expectedPartitions == offsetForLeaderPartitionMap(request.data()).keys
                },
                response = response,
                node = node,
            )
        }
        assertEquals(subscriptions.assignedPartitions(), allRequestedPartitions)
        fetcher.validateOffsetsIfNeeded()
        consumerClient!!.pollNoWakeup()
        assertTrue(
            subscriptions.assignedPartitions().none { tp -> subscriptions.awaitingValidation(tp) }
        )
    }

    @Test
    fun testOffsetValidationAwaitsNodeApiVersion() {
        buildFetcher()
        assignFromUser(setOf(tp0))
        val partitionCounts: MutableMap<String, Int> = HashMap()
        partitionCounts[tp0.topic] = 4
        val epochOne = 1
        metadata!!.updateWithCurrentRequestVersion(
            response = metadataUpdateWith(
                clusterId = "dummy",
                numNodes = 1,
                topicErrors = emptyMap(),
                topicPartitionCounts = partitionCounts,
                epochSupplier = { epochOne },
                topicIds = topicIds
            ),
            isPartialUpdate = false,
            nowMs = 0L,
        )
        val node = metadata!!.fetch().nodes[0]
        assertFalse(client!!.isConnected(node.idString()))

        // Seek with a position and leader+epoch
        val leaderAndEpoch = LeaderAndEpoch(
            leader = metadata!!.currentLeader(tp0).leader,
            epoch = epochOne,
        )
        subscriptions.seekUnvalidated(
            tp = tp0,
            position = FetchPosition(
                offset = 20L,
                offsetEpoch = epochOne,
                currentLeader = leaderAndEpoch,
            ),
        )
        assertFalse(client!!.isConnected(node.idString()))
        assertTrue(subscriptions.awaitingValidation(tp0))

        // No version information is initially available, but the node is now connected
        fetcher.validateOffsetsIfNeeded()
        assertTrue(subscriptions.awaitingValidation(tp0))
        assertTrue(client!!.isConnected(node.idString()))
        apiVersions.update(node.idString(), NodeApiVersions.create())

        // On the next call, the OffsetForLeaderEpoch request is sent and validation completes
        client!!.prepareResponseFrom(
            response = prepareOffsetsForLeaderEpochResponse(
                topicPartition = tp0,
                error = Errors.NONE,
                leaderEpoch = epochOne,
                endOffset = 30L,
            ),
            node = node,
        )
        fetcher.validateOffsetsIfNeeded()
        consumerClient!!.pollNoWakeup()
        assertFalse(subscriptions.awaitingValidation(tp0))
        assertEquals(20L, subscriptions.position(tp0)!!.offset)
    }

    @Test
    fun testOffsetValidationSkippedForOldBroker() {
        // Old brokers may require CLUSTER permission to use the OffsetForLeaderEpoch API,
        // so we should skip offset validation and not send the request.
        buildFetcher()
        assignFromUser(setOf(tp0))
        val partitionCounts = mapOf(tp0.topic to 4)
        val epochOne = 1
        val epochTwo = 2

        // Start with metadata, epoch=1
        metadata!!.updateWithCurrentRequestVersion(
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
        val node = metadata!!.fetch().nodes[0]
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
            val leaderAndEpoch = LeaderAndEpoch(
                leader = metadata!!.currentLeader(tp0).leader,
                epoch = epochOne,
            )
            subscriptions.seekUnvalidated(
                tp = tp0,
                position = FetchPosition(
                    offset = 0,
                    offsetEpoch = epochOne,
                    currentLeader = leaderAndEpoch,
                ),
            )

            // Update metadata to epoch=2, enter validation
            metadata!!.updateWithCurrentRequestVersion(
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
            fetcher.validateOffsetsIfNeeded()

            // Offset validation is skipped
            assertFalse(subscriptions.awaitingValidation(tp0))
        }
        run {

            // Seek with a position and leader+epoch
            val leaderAndEpoch = LeaderAndEpoch(
                leader = metadata!!.currentLeader(tp0).leader,
                epoch = epochOne,
            )
            subscriptions.seekUnvalidated(
                tp = tp0,
                position = FetchPosition(
                    offset = 0,
                    offsetEpoch = epochOne,
                    currentLeader = leaderAndEpoch,
                ),
            )

            // Update metadata to epoch=2, enter validation
            metadata!!.updateWithCurrentRequestVersion(
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
        metadata!!.updateWithCurrentRequestVersion(
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
        val node = metadata!!.fetch().nodes[0]
        assertFalse(client!!.isConnected(node.idString()))

        // Seek with a position and leader+epoch
        val leaderAndEpoch = LeaderAndEpoch(
            leader = metadata!!.currentLeader(tp0).leader,
            epoch = epochOne,
        )
        subscriptions.seekUnvalidated(
            tp = tp0,
            position = FetchPosition(
                offset = 20L,
                offsetEpoch = epochOne,
                currentLeader = leaderAndEpoch,
            ),
        )
        assertFalse(client!!.isConnected(node.idString()))
        assertTrue(subscriptions.awaitingValidation(tp0))

        // Inject an older version of the metadata response
        val responseVersion: Short = 8
        metadata!!.updateWithCurrentRequestVersion(
            response = metadataUpdateWith(
                clusterId = "dummy",
                numNodes = 1,
                topicErrors = emptyMap(),
                topicPartitionCounts = partitionCounts,
                epochSupplier = { null },
                partitionSupplier = { error, topicPartition, leaderId, leaderEpoch, replicaIds, inSyncReplicaIds, offlineReplicaIds ->
                    PartitionMetadata(
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
        fetcher.validateOffsetsIfNeeded()
        // Offset validation is skipped
        assertFalse(subscriptions.awaitingValidation(tp0))
    }

    @Test
    fun testOffsetValidationResetOffsetForUndefinedEpochWithDefinedResetPolicy() =
        testOffsetValidationWithGivenEpochOffset(
            leaderEpoch = OffsetsForLeaderEpochResponse.UNDEFINED_EPOCH,
            endOffset = 0L,
            offsetResetStrategy = OffsetResetStrategy.EARLIEST,
        )

    @Test
    fun testOffsetValidationResetOffsetForUndefinedOffsetWithDefinedResetPolicy() =
        testOffsetValidationWithGivenEpochOffset(
            leaderEpoch = 2,
            endOffset = OffsetsForLeaderEpochResponse.UNDEFINED_EPOCH_OFFSET,
            offsetResetStrategy = OffsetResetStrategy.EARLIEST,
        )

    @Test
    fun testOffsetValidationResetOffsetForUndefinedEpochWithUndefinedResetPolicy() =
        testOffsetValidationWithGivenEpochOffset(
            leaderEpoch = OffsetsForLeaderEpochResponse.UNDEFINED_EPOCH,
            endOffset = 0L,
            offsetResetStrategy = OffsetResetStrategy.NONE,
        )

    @Test
    fun testOffsetValidationResetOffsetForUndefinedOffsetWithUndefinedResetPolicy() =
        testOffsetValidationWithGivenEpochOffset(
            leaderEpoch = 2,
            endOffset = OffsetsForLeaderEpochResponse.UNDEFINED_EPOCH_OFFSET,
            offsetResetStrategy = OffsetResetStrategy.NONE,
        )

    @Test
    fun testOffsetValidationTriggerLogTruncationForBadOffsetWithUndefinedResetPolicy() =
        testOffsetValidationWithGivenEpochOffset(
            leaderEpoch = 1,
            endOffset = 1L,
            offsetResetStrategy = OffsetResetStrategy.NONE,
        )

    private fun testOffsetValidationWithGivenEpochOffset(
        leaderEpoch: Int,
        endOffset: Long,
        offsetResetStrategy: OffsetResetStrategy,
    ) {
        buildFetcher(offsetResetStrategy = offsetResetStrategy)
        assignFromUser(setOf(tp0))
        val partitionCounts = mapOf(tp0.topic to 4)
        val epochOne = 1
        val initialOffset = 5L
        metadata!!.updateWithCurrentRequestVersion(
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
        val node = metadata!!.fetch().nodes[0]
        apiVersions.update(node.idString(), NodeApiVersions.create())
        val leaderAndEpoch = LeaderAndEpoch(
            leader = metadata!!.currentLeader(tp0).leader,
            epoch = epochOne,
        )
        subscriptions.seekUnvalidated(
            tp = tp0,
            position = FetchPosition(
                offset = initialOffset,
                offsetEpoch = epochOne,
                currentLeader = leaderAndEpoch,
            ),
        )
        fetcher.validateOffsetsIfNeeded()
        consumerClient!!.poll(time.timer(Duration.ZERO))
        assertTrue(subscriptions.awaitingValidation(tp0))
        assertTrue(client!!.hasInFlightRequests())
        client!!.respond(
            matcher = offsetsForLeaderEpochRequestMatcher(
                topicPartition = tp0,
                currentLeaderEpoch = epochOne,
                leaderEpoch = epochOne,
            ),
            response = prepareOffsetsForLeaderEpochResponse(
                topicPartition = tp0,
                error = Errors.NONE,
                leaderEpoch = leaderEpoch,
                endOffset = endOffset,
            )
        )
        consumerClient!!.poll(time.timer(Duration.ZERO))
        if (offsetResetStrategy === OffsetResetStrategy.NONE) {
            val thrown = assertFailsWith<LogTruncationException> { fetcher.validateOffsetsIfNeeded() }
            assertEquals(mapOf(tp0 to initialOffset), thrown.offsetOutOfRangePartitions)
            if (
                endOffset == OffsetsForLeaderEpochResponse.UNDEFINED_EPOCH_OFFSET
                || leaderEpoch == OffsetsForLeaderEpochResponse.UNDEFINED_EPOCH
            ) assertEquals(emptyMap(), thrown.divergentOffsets)
            else {
                val expectedDivergentOffset = OffsetAndMetadata(
                    offset = endOffset,
                    leaderEpoch = leaderEpoch,
                    metadata = "",
                )
                assertEquals(mapOf(tp0 to expectedDivergentOffset), thrown.divergentOffsets)
            }
            assertTrue(subscriptions.awaitingValidation(tp0))
        } else {
            fetcher.validateOffsetsIfNeeded()
            assertFalse(subscriptions.awaitingValidation(tp0))
        }
    }

    @Test
    fun testOffsetValidationHandlesSeekWithInflightOffsetForLeaderRequest() {
        buildFetcher()
        assignFromUser(setOf(tp0))
        val partitionCounts = mapOf(tp0.topic to 4)
        val epochOne = 1
        metadata!!.updateWithCurrentRequestVersion(
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
        val node = metadata!!.fetch().nodes[0]
        apiVersions.update(node.idString(), NodeApiVersions.create())
        val leaderAndEpoch = LeaderAndEpoch(
            leader = metadata!!.currentLeader(tp0).leader,
            epoch = epochOne,
        )
        subscriptions.seekUnvalidated(
            tp = tp0,
            position = FetchPosition(
                offset = 0,
                offsetEpoch = epochOne,
                currentLeader = leaderAndEpoch,
            ),
        )
        fetcher.validateOffsetsIfNeeded()
        consumerClient!!.poll(time.timer(Duration.ZERO))
        assertTrue(subscriptions.awaitingValidation(tp0))
        assertTrue(client!!.hasInFlightRequests())

        // While the OffsetForLeaderEpoch request is in-flight, we seek to a different offset.
        subscriptions.seekUnvalidated(
            tp = tp0,
            position = FetchPosition(
                offset = 5,
                offsetEpoch = epochOne,
                currentLeader = leaderAndEpoch,
            ),
        )
        assertTrue(subscriptions.awaitingValidation(tp0))
        client!!.respond(
            matcher = offsetsForLeaderEpochRequestMatcher(
                topicPartition = tp0,
                currentLeaderEpoch = epochOne,
                leaderEpoch = epochOne,
            ),
            response = prepareOffsetsForLeaderEpochResponse(
                topicPartition = tp0,
                error = Errors.NONE,
                leaderEpoch = 0,
                endOffset = 0L,
            ),
        )
        consumerClient!!.poll(time.timer(Duration.ZERO))

        // The response should be ignored since we were validating a different position.
        assertTrue(subscriptions.awaitingValidation(tp0))
    }

    @Test
    fun testOffsetValidationFencing() {
        buildFetcher()
        assignFromUser(setOf(tp0))
        val partitionCounts = mapOf(tp0.topic to 4)
        val epochOne = 1
        val epochTwo = 2
        val epochThree = 3

        // Start with metadata, epoch=1
        metadata!!.updateWithCurrentRequestVersion(
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
        val node = metadata!!.fetch().nodes[0]
        apiVersions.update(node.idString(), NodeApiVersions.create())

        // Seek with a position and leader+epoch
        val leaderAndEpoch = LeaderAndEpoch(
            leader = metadata!!.currentLeader(tp0).leader,
            epoch = epochOne,
        )
        subscriptions.seekValidated(
            tp = tp0,
            position = FetchPosition(
                offset = 0,
                offsetEpoch = epochOne,
                currentLeader = leaderAndEpoch,
            ),
        )

        // Update metadata to epoch=2, enter validation
        metadata!!.updateWithCurrentRequestVersion(
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
        fetcher.validateOffsetsIfNeeded()
        assertTrue(subscriptions.awaitingValidation(tp0))

        // Update the position to epoch=3, as we would from a fetch
        subscriptions.completeValidation(tp0)
        val nextPosition = FetchPosition(
            offset = 10,
            offsetEpoch = epochTwo,
            currentLeader = LeaderAndEpoch(
                leader = leaderAndEpoch.leader,
                epoch = epochTwo,
            ),
        )
        subscriptions.position(tp0, nextPosition)
        subscriptions.maybeValidatePositionForCurrentLeader(
            apiVersions = apiVersions,
            tp = tp0,
            leaderAndEpoch = LeaderAndEpoch(
                leader = leaderAndEpoch.leader,
                epoch = epochThree,
            ),
        )

        // Prepare offset list response from async validation with epoch=2
        client!!.prepareResponse(
            response = prepareOffsetsForLeaderEpochResponse(
                topicPartition = tp0,
                error = Errors.NONE,
                leaderEpoch = epochTwo,
                endOffset = 10L,
            ),
        )
        consumerClient!!.pollNoWakeup()
        assertTrue(
            actual = subscriptions.awaitingValidation(tp0),
            message = "Expected validation to fail since leader epoch changed",
        )

        // Next round of validation, should succeed in validating the position
        fetcher.validateOffsetsIfNeeded()
        client!!.prepareResponse(
            response = prepareOffsetsForLeaderEpochResponse(
                topicPartition = tp0,
                error = Errors.NONE,
                leaderEpoch = epochThree,
                endOffset = 10L,
            ),
        )
        consumerClient!!.pollNoWakeup()
        assertFalse(
            actual = subscriptions.awaitingValidation(tp0),
            message = "Expected validation to succeed with latest epoch",
        )
    }

    @Test
    fun testSkipValidationForOlderApiVersion() {
        buildFetcher()
        assignFromUser(setOf(tp0))
        val partitionCounts = mapOf(tp0.topic to 4)
        apiVersions.update(
            nodeId = "0",
            nodeApiVersions = NodeApiVersions.create(
                apiKey = ApiKeys.OFFSET_FOR_LEADER_EPOCH.id,
                minVersion = 0,
                maxVersion = 2,
            ),
        )

        // Start with metadata, epoch=1
        metadata!!.updateWithCurrentRequestVersion(
            response = metadataUpdateWith(
                clusterId = "dummy",
                numNodes = 1,
                topicErrors = emptyMap(),
                topicPartitionCounts = partitionCounts,
                epochSupplier = { 1 },
                topicIds = topicIds,
            ),
            isPartialUpdate = false,
            nowMs = 0L,
        )

        // Request offset reset
        subscriptions.requestOffsetReset(
            partition = tp0,
            offsetResetStrategy = OffsetResetStrategy.LATEST,
        )

        // Since we have no position due to reset, no fetch is sent
        assertEquals(0, fetcher.sendFetches())

        // Still no position, ensure offset validation logic did not transition us to FETCHING state
        assertEquals(0, fetcher.sendFetches())

        // Complete reset and now we can fetch
        fetcher.resetOffsetIfNeeded(
            partition = tp0,
            requestedResetStrategy = OffsetResetStrategy.LATEST,
            offsetData = ListOffsetData(
                offset = 100,
                timestamp = 1L,
                leaderEpoch = null,
            ),
        )
        assertEquals(1, fetcher.sendFetches())
    }

    @Test
    fun testTruncationDetected() {
        // Create some records that include a leader epoch (1)
        val builder = MemoryRecords.builder(
            buffer = ByteBuffer.allocate(1024),
            magic = RecordBatch.CURRENT_MAGIC_VALUE,
            compressionType = CompressionType.NONE,
            timestampType = TimestampType.CREATE_TIME,
            baseOffset = 0L,
            logAppendTime = RecordBatch.NO_TIMESTAMP,
            producerId = RecordBatch.NO_PRODUCER_ID,
            producerEpoch = RecordBatch.NO_PRODUCER_EPOCH,
            baseSequence = RecordBatch.NO_SEQUENCE,
            isTransactional = false,
            partitionLeaderEpoch = 1, // record epoch is earlier than the leader epoch on the client
        )
        builder.appendWithOffset(
            offset = 0L,
            timestamp = 0L,
            key = "key".toByteArray(),
            value = "value-1".toByteArray(),
        )
        builder.appendWithOffset(
            offset = 1L,
            timestamp = 0L,
            key = "key".toByteArray(),
            value = "value-2".toByteArray(),
        )
        builder.appendWithOffset(
            offset = 2L,
            timestamp = 0L,
            key = "key".toByteArray(),
            value = "value-3".toByteArray(),
        )
        val records = builder.build()
        buildFetcher()
        assignFromUser(setOf(tp0))

        // Initialize the epoch=2
        val partitionCounts = mapOf(tp0.topic to 4)
        val metadataResponse = metadataUpdateWith(
            clusterId = "dummy",
            numNodes = 1,
            topicErrors = emptyMap(),
            topicPartitionCounts = partitionCounts,
            epochSupplier = { 2 },
            topicIds = topicIds,
        )
        metadata!!.updateWithCurrentRequestVersion(
            response = metadataResponse,
            isPartialUpdate = false,
            nowMs = 0L,
        )

        // Offset validation requires OffsetForLeaderEpoch request v3 or higher
        val node = metadata!!.fetch().nodes[0]
        apiVersions.update(node.idString(), NodeApiVersions.create())

        // Seek
        val leaderAndEpoch = LeaderAndEpoch(
            leader = metadata!!.currentLeader(tp0).leader,
            epoch = 1,
        )
        subscriptions.seekValidated(
            tp = tp0,
            position = FetchPosition(
                offset = 0,
                offsetEpoch = 1,
                currentLeader = leaderAndEpoch,
            ),
        )

        // Check for truncation, this should cause tp0 to go into validation
        fetcher.validateOffsetsIfNeeded()

        // No fetches sent since we entered validation
        assertEquals(0, fetcher.sendFetches())
        assertFalse(fetcher.hasCompletedFetches())
        assertTrue(subscriptions.awaitingValidation(tp0))

        // Prepare OffsetForEpoch response then check that we update the subscription position correctly.
        client!!.prepareResponse(
            response = prepareOffsetsForLeaderEpochResponse(
                topicPartition = tp0,
                error = Errors.NONE,
                leaderEpoch = 1,
                endOffset = 10L,
            ),
        )
        consumerClient!!.pollNoWakeup()
        assertFalse(subscriptions.awaitingValidation(tp0))

        // Fetch again, now it works
        assertEquals(1, fetcher.sendFetches())
        assertFalse(fetcher.hasCompletedFetches())
        client!!.prepareResponse(
            response = fullFetchResponse(
                tp = tidp0,
                records = records,
                error = Errors.NONE,
                hw = 100L,
                throttleTime = 0,
            ),
        )
        consumerClient!!.pollNoWakeup()
        assertTrue(fetcher.hasCompletedFetches())
        val partitionRecords = fetchedRecords<ByteArray, ByteArray>()
        assertTrue(partitionRecords.containsKey(tp0))
        assertEquals(subscriptions.position(tp0)!!.offset, 3L)
        assertNullable(subscriptions.position(tp0)!!.offsetEpoch) { value -> assertEquals(value, 1) }
    }

    @Test
    fun testPreferredReadReplica() {
        buildFetcher(
            metricConfig = MetricConfig(),
            offsetResetStrategy = OffsetResetStrategy.EARLIEST,
            keyDeserializer = BytesDeserializer(),
            valueDeserializer = BytesDeserializer(),
            maxPollRecords = Int.MAX_VALUE,
            isolationLevel = IsolationLevel.READ_COMMITTED,
            metadataExpireMs = Duration.ofMinutes(5).toMillis(),
        )
        subscriptions.assignFromUser(setOf(tp0))
        client!!.updateMetadata(
            updateResponse = metadataUpdateWithIds(
                numNodes = 2,
                topicPartitionCounts = mapOf(topicName to 4),
                epochSupplier = { validLeaderEpoch },
                topicIds = topicIds,
                leaderOnly = false,
            )
        )
        subscriptions.seek(tp0, 0)

        // Node preferred replica before first fetch response
        var selected = fetcher.selectReadReplica(tp0, Node.noNode(), time.milliseconds())
        assertEquals(-1, selected.id)
        assertEquals(1, fetcher.sendFetches())
        assertFalse(fetcher.hasCompletedFetches())

        // Set preferred read replica to node=1
        client!!.prepareResponse(
            response = fullFetchResponse(
                tp = tidp0,
                records = records,
                error = Errors.NONE,
                hw = 100L,
                lastStableOffset = FetchResponse.INVALID_LAST_STABLE_OFFSET,
                throttleTime = 0,
                preferredReplicaId = 1,
            ),
        )
        consumerClient!!.poll(time.timer(0))
        assertTrue(fetcher.hasCompletedFetches())
        val partitionRecords = fetchedRecords<ByteArray, ByteArray>()
        assertTrue(partitionRecords.containsKey(tp0))

        // Verify
        selected = fetcher.selectReadReplica(tp0, Node.noNode(), time.milliseconds())
        assertEquals(1, selected.id)
        assertEquals(1, fetcher.sendFetches())
        assertFalse(fetcher.hasCompletedFetches())

        // Set preferred read replica to node=2, which isn't in our metadata, should revert to leader
        client!!.prepareResponse(
            response = fullFetchResponse(
                tp = tidp0,
                records = records,
                error = Errors.NONE,
                hw = 100L,
                lastStableOffset = FetchResponse.INVALID_LAST_STABLE_OFFSET,
                throttleTime = 0,
                preferredReplicaId = 2,
            ),
        )
        consumerClient!!.poll(time.timer(0))
        assertTrue(fetcher.hasCompletedFetches())

        fetchedRecords<Any, Any>()
        selected = fetcher.selectReadReplica(
            partition = tp0,
            leaderReplica = Node.noNode(),
            currentTimeMs = time.milliseconds(),
        )
        assertEquals(-1, selected.id)
    }

    @Test
    fun testFetchDisconnectedShouldClearPreferredReadReplica() {
        buildFetcher(
            metricConfig = MetricConfig(),
            offsetResetStrategy = OffsetResetStrategy.EARLIEST,
            keyDeserializer = BytesDeserializer(),
            valueDeserializer = BytesDeserializer(),
            maxPollRecords = Int.MAX_VALUE,
            isolationLevel = IsolationLevel.READ_COMMITTED,
            metadataExpireMs = Duration.ofMinutes(5).toMillis(),
        )
        subscriptions.assignFromUser(setOf(tp0))
        client!!.updateMetadata(
            updateResponse = metadataUpdateWithIds(
                numNodes = 2,
                topicPartitionCounts = mapOf(topicName to 4),
                epochSupplier = { validLeaderEpoch },
                topicIds = topicIds,
                leaderOnly = false,
            ),
        )
        subscriptions.seek(tp0, 0)
        assertEquals(1, fetcher.sendFetches())

        // Set preferred read replica to node=1
        client!!.prepareResponse(
            response = fullFetchResponse(
                tp = tidp0,
                records = records,
                error = Errors.NONE,
                hw = 100L,
                lastStableOffset = FetchResponse.INVALID_LAST_STABLE_OFFSET,
                throttleTime = 0,
                preferredReplicaId = 1,
            ),
        )
        consumerClient!!.poll(time.timer(0))
        assertTrue(fetcher.hasCompletedFetches())
        fetchedRecords<Any, Any>()

        // Verify
        var selected = fetcher.selectReadReplica(
            partition = tp0,
            leaderReplica = Node.noNode(),
            currentTimeMs = time.milliseconds(),
        )
        assertEquals(1, selected.id)
        assertEquals(1, fetcher.sendFetches())
        assertFalse(fetcher.hasCompletedFetches())

        // Disconnect - preferred read replica should be cleared.
        client!!.prepareResponse(
            response = fullFetchResponse(
                tp = tidp0,
                records = records,
                error = Errors.NONE,
                hw = 100L,
                throttleTime = 0
            ),
            disconnected = true,
        )
        consumerClient!!.poll(time.timer(0))
        assertFalse(fetcher.hasCompletedFetches())
        fetchedRecords<Any, Any>()
        selected = fetcher.selectReadReplica(
            partition = tp0,
            leaderReplica = Node.noNode(),
            currentTimeMs = time.milliseconds(),
        )
        assertEquals(-1, selected.id)
    }

    @Test
    fun testFetchDisconnectedShouldNotClearPreferredReadReplicaIfUnassigned() {
        buildFetcher(
            metricConfig = MetricConfig(),
            offsetResetStrategy = OffsetResetStrategy.EARLIEST,
            keyDeserializer = BytesDeserializer(),
            valueDeserializer = BytesDeserializer(),
            maxPollRecords = Int.MAX_VALUE,
            isolationLevel = IsolationLevel.READ_COMMITTED,
            metadataExpireMs = Duration.ofMinutes(5).toMillis(),
        )
        subscriptions.assignFromUser(setOf(tp0))
        client!!.updateMetadata(
            updateResponse = metadataUpdateWithIds(
                numNodes = 2,
                topicPartitionCounts = mapOf(topicName to 4),
                epochSupplier = { validLeaderEpoch },
                topicIds = topicIds,
                leaderOnly = false,
            ),
        )
        subscriptions.seek(tp0, 0)
        assertEquals(1, fetcher.sendFetches())

        // Set preferred read replica to node=1
        client!!.prepareResponse(
            response = fullFetchResponse(
                tp = tidp0,
                records = records,
                error = Errors.NONE,
                hw = 100L,
                lastStableOffset = FetchResponse.INVALID_LAST_STABLE_OFFSET,
                throttleTime = 0,
                preferredReplicaId = 1,
            ),
        )
        consumerClient!!.poll(time.timer(0))
        assertTrue(fetcher.hasCompletedFetches())
        fetchedRecords<Any, Any>()

        // Verify
        var selected = fetcher.selectReadReplica(
            partition = tp0,
            leaderReplica = Node.noNode(),
            currentTimeMs = time.milliseconds(),
        )
        assertEquals(1, selected.id)
        assertEquals(1, fetcher.sendFetches())
        assertFalse(fetcher.hasCompletedFetches())

        // Disconnect and remove tp0 from assignment
        client!!.prepareResponse(
            response = fullFetchResponse(
                tp = tidp0,
                records = records,
                error = Errors.NONE,
                hw = 100L,
                throttleTime = 0,
            ),
            disconnected = true,
        )
        subscriptions.assignFromUser(emptySet())

        // Preferred read replica should not be cleared
        consumerClient!!.poll(time.timer(0))
        assertFalse(fetcher.hasCompletedFetches())
        fetchedRecords<Any, Any>()
        selected = fetcher.selectReadReplica(tp0, Node.noNode(), time.milliseconds())
        assertEquals(-1, selected.id)
    }

    @Test
    fun testFetchErrorShouldClearPreferredReadReplica() {
        buildFetcher(
            metricConfig = MetricConfig(),
            offsetResetStrategy = OffsetResetStrategy.EARLIEST,
            keyDeserializer = BytesDeserializer(),
            valueDeserializer = BytesDeserializer(),
            maxPollRecords = Int.MAX_VALUE,
            isolationLevel = IsolationLevel.READ_COMMITTED,
            metadataExpireMs = Duration.ofMinutes(5).toMillis(),
        )
        subscriptions.assignFromUser(setOf(tp0))
        client!!.updateMetadata(
            updateResponse = metadataUpdateWithIds(
                numNodes = 2,
                topicPartitionCounts = mapOf(topicName to 4),
                epochSupplier = { validLeaderEpoch },
                topicIds = topicIds,
                leaderOnly = false,
            ),
        )
        subscriptions.seek(tp0, 0)
        assertEquals(1, fetcher.sendFetches())

        // Set preferred read replica to node=1
        client!!.prepareResponse(
            response = fullFetchResponse(
                tp = tidp0,
                records = records,
                error = Errors.NONE,
                hw = 100L,
                lastStableOffset = FetchResponse.INVALID_LAST_STABLE_OFFSET,
                throttleTime = 0,
                preferredReplicaId = 1,
            ),
        )
        consumerClient!!.poll(time.timer(0))
        assertTrue(fetcher.hasCompletedFetches())
        fetchedRecords<Any, Any>()

        // Verify
        var selected = fetcher.selectReadReplica(
            partition = tp0,
            leaderReplica = Node.noNode(),
            currentTimeMs = time.milliseconds(),
        )
        assertEquals(1, selected.id)
        assertEquals(1, fetcher.sendFetches())
        assertFalse(fetcher.hasCompletedFetches())

        // Error - preferred read replica should be cleared. An actual error response will contain -1 as the
        // preferred read replica. In the test we want to ensure that we are handling the error.
        client!!.prepareResponse(
            response = fullFetchResponse(
                tp = tidp0,
                records = MemoryRecords.EMPTY,
                error = Errors.NOT_LEADER_OR_FOLLOWER,
                hw = -1L,
                lastStableOffset = FetchResponse.INVALID_LAST_STABLE_OFFSET,
                throttleTime = 0,
                preferredReplicaId = 1,
            ),
        )
        consumerClient!!.poll(time.timer(0))
        assertTrue(fetcher.hasCompletedFetches())
        fetchedRecords<Any, Any>()
        selected = fetcher.selectReadReplica(
            partition = tp0,
            leaderReplica = Node.noNode(),
            currentTimeMs = time.milliseconds(),
        )
        assertEquals(-1, selected.id)
    }

    @Test
    fun testPreferredReadReplicaOffsetError() {
        buildFetcher(
            metricConfig = MetricConfig(),
            offsetResetStrategy = OffsetResetStrategy.EARLIEST,
            keyDeserializer = BytesDeserializer(),
            valueDeserializer = BytesDeserializer(),
            maxPollRecords = Int.MAX_VALUE,
            isolationLevel = IsolationLevel.READ_COMMITTED,
            metadataExpireMs = Duration.ofMinutes(5).toMillis(),
        )
        subscriptions.assignFromUser(setOf(tp0))
        client!!.updateMetadata(
            updateResponse = metadataUpdateWithIds(
                numNodes = 2,
                topicPartitionCounts = mapOf(topicName to 4),
                epochSupplier = { validLeaderEpoch },
                topicIds = topicIds,
                leaderOnly = false,
            ),
        )
        subscriptions.seek(tp0, 0)
        assertEquals(1, fetcher.sendFetches())
        assertFalse(fetcher.hasCompletedFetches())
        client!!.prepareResponse(
            response = fullFetchResponse(
                tp = tidp0,
                records = records,
                error = Errors.NONE,
                hw = 100L,
                lastStableOffset = FetchResponse.INVALID_LAST_STABLE_OFFSET,
                throttleTime = 0,
                preferredReplicaId = 1,
            ),
        )
        consumerClient!!.poll(time.timer(0))
        assertTrue(fetcher.hasCompletedFetches())
        fetchedRecords<Any, Any>()
        var selected = fetcher.selectReadReplica(
            partition = tp0,
            leaderReplica = Node.noNode(),
            currentTimeMs = time.milliseconds(),
        )
        assertEquals(selected.id, 1)

        // Return an error, should unset the preferred read replica
        assertEquals(1, fetcher.sendFetches())
        assertFalse(fetcher.hasCompletedFetches())
        client!!.prepareResponse(
            response = fullFetchResponse(
                tp = tidp0,
                records = records,
                error = Errors.OFFSET_OUT_OF_RANGE,
                hw = 100L,
                lastStableOffset = FetchResponse.INVALID_LAST_STABLE_OFFSET,
                throttleTime = 0,
                preferredReplicaId = null,
            ),
        )
        consumerClient!!.poll(time.timer(0))
        assertTrue(fetcher.hasCompletedFetches())
        fetchedRecords<Any, Any>()
        selected = fetcher.selectReadReplica(
            partition = tp0,
            leaderReplica = Node.noNode(),
            currentTimeMs = time.milliseconds(),
        )
        assertEquals(selected.id, -1)
    }

    @Test
    fun testFetchCompletedBeforeHandlerAdded() {
        buildFetcher()
        assignFromUser(setOf(tp0))
        subscriptions.seek(tp0, 0)
        fetcher.sendFetches()
        client!!.prepareResponse(
            response = fullFetchResponse(
                tp = tidp0,
                records = buildRecords(baseOffset = 1L, count = 1, firstMessageId = 1),
                error = Errors.NONE,
                hw = 100L,
                throttleTime = 0,
            ),
        )
        consumerClient!!.poll(time.timer(0))
        fetchedRecords<Any, Any>()
        val (leader) = subscriptions.position(tp0)!!.currentLeader
        assertNotNull(leader)
        val readReplica = fetcher.selectReadReplica(
            partition = tp0,
            leaderReplica = leader,
            currentTimeMs = time.milliseconds(),
        )
        val wokenUp = AtomicBoolean(false)
        client!!.setWakeupHook {
            if (!wokenUp.getAndSet(true)) {
                consumerClient!!.disconnectAsync(readReplica)
                consumerClient!!.poll(time.timer(0))
            }
        }
        assertEquals(1, fetcher.sendFetches())
        consumerClient!!.disconnectAsync(readReplica)
        consumerClient!!.poll(time.timer(0))
        assertEquals(1, fetcher.sendFetches())
    }

    @Test
    fun testCorruptMessageError() {
        buildFetcher()
        assignFromUser(setOf(tp0))
        subscriptions.seek(tp0, 0)
        assertEquals(1, fetcher.sendFetches())
        assertFalse(fetcher.hasCompletedFetches())

        // Prepare a response with the CORRUPT_MESSAGE error.
        client!!.prepareResponse(
            response = fullFetchResponse(
                tp = tidp0,
                records = buildRecords(baseOffset = 1L, count = 1, firstMessageId = 1),
                error = Errors.CORRUPT_MESSAGE,
                hw = 100L, throttleTime = 0
            ),
        )
        consumerClient!!.poll(time.timer(0))
        assertTrue(fetcher.hasCompletedFetches())

        // Trigger the exception.
        assertFailsWith<KafkaException> { fetchedRecords<Any?, Any?>() }
    }

    @Test
    fun testBeginningOffsets() {
        buildFetcher()
        assignFromUser(setOf(tp0))
        client!!.prepareResponse(
            response = listOffsetResponse(
                tp = tp0,
                error = Errors.NONE,
                timestamp = ListOffsetsRequest.EARLIEST_TIMESTAMP,
                offset = 2L,
            ),
        )
        assertEquals(
            mapOf(tp0 to 2L), fetcher.beginningOffsets(setOf(tp0), time.timer(5000L))
        )
    }

    @Test
    fun testBeginningOffsetsDuplicateTopicPartition() {
        buildFetcher()
        assignFromUser(setOf(tp0))
        client!!.prepareResponse(
            response = listOffsetResponse(
                tp = tp0,
                error = Errors.NONE,
                timestamp = ListOffsetsRequest.EARLIEST_TIMESTAMP,
                offset = 2L,
            ),
        )
        assertEquals(
            expected = mapOf(tp0 to 2L),
            actual = fetcher.beginningOffsets(listOf(tp0, tp0), time.timer(5000L))
        )
    }

    @Test
    fun testBeginningOffsetsMultipleTopicPartitions() {
        buildFetcher()
        val expectedOffsets = mapOf(
            tp0 to 2L,
            tp1 to 4L,
            tp2 to 6L,
        )
        assignFromUser(expectedOffsets.keys)
        client!!.prepareResponse(
            response = listOffsetResponse(
                offsets = expectedOffsets,
                error = Errors.NONE,
                timestamp = ListOffsetsRequest.EARLIEST_TIMESTAMP,
                leaderEpoch = ListOffsetsResponse.UNKNOWN_EPOCH,
            ),
        )
        assertEquals(
            expected = expectedOffsets,
            actual = fetcher.beginningOffsets(listOf(tp0, tp1, tp2), time.timer(5000L))
        )
    }

    @Test
    fun testBeginningOffsetsEmpty() {
        buildFetcher()
        assertEquals(emptyMap(), fetcher.beginningOffsets(emptyList(), time.timer(5000L)))
    }

    @Test
    fun testEndOffsets() {
        buildFetcher()
        assignFromUser(setOf(tp0))
        client!!.prepareResponse(
            response = listOffsetResponse(
                tp = tp0,
                error = Errors.NONE,
                timestamp = ListOffsetsRequest.LATEST_TIMESTAMP,
                offset = 5L,
            ),
        )
        assertEquals(
            expected = mapOf(tp0 to 5L),
            actual = fetcher.endOffsets(setOf(tp0), time.timer(5000L)),
        )
    }

    @Test
    fun testEndOffsetsDuplicateTopicPartition() {
        buildFetcher()
        assignFromUser(setOf(tp0))
        client!!.prepareResponse(
            response = listOffsetResponse(
                tp = tp0,
                error = Errors.NONE,
                timestamp = ListOffsetsRequest.LATEST_TIMESTAMP,
                offset = 5L,
            ),
        )
        assertEquals(
            expected = mapOf(tp0 to 5L),
            actual = fetcher.endOffsets(listOf(tp0, tp0), time.timer(5000L)),
        )
    }

    @Test
    fun testEndOffsetsMultipleTopicPartitions() {
        buildFetcher()
        val expectedOffsets = mapOf(
            tp0 to 5L,
            tp1 to 7L,
            tp2 to 9L,
        )
        assignFromUser(expectedOffsets.keys)
        client!!.prepareResponse(
            response = listOffsetResponse(
                offsets = expectedOffsets,
                error = Errors.NONE,
                timestamp = ListOffsetsRequest.LATEST_TIMESTAMP,
                leaderEpoch = ListOffsetsResponse.UNKNOWN_EPOCH,
            ),
        )
        assertEquals(expectedOffsets, fetcher.endOffsets(listOf(tp0, tp1, tp2), time.timer(5000L)))
    }

    @Test
    fun testEndOffsetsEmpty() {
        buildFetcher()
        assertEquals(emptyMap(), fetcher.endOffsets(emptyList(), time.timer(5000L)))
    }

    private fun offsetsForLeaderEpochRequestMatcher(
        topicPartition: TopicPartition,
        currentLeaderEpoch: Int,
        leaderEpoch: Int,
    ): RequestMatcher {
        return RequestMatcher { request ->
            val epochRequest = assertIs<OffsetsForLeaderEpochRequest>(request)
            val partition = offsetForLeaderPartitionMap(epochRequest.data())[topicPartition]
            partition != null
                    && partition.currentLeaderEpoch == currentLeaderEpoch
                    && partition.leaderEpoch == leaderEpoch
        }
    }

    private fun prepareOffsetsForLeaderEpochResponse(
        topicPartition: TopicPartition,
        error: Errors,
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
                            .setErrorCode(error.code)
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
        val result: MutableMap<TopicPartition, OffsetForLeaderPartition> = HashMap()
        data.topics.forEach { topic ->
            topic.partitions.forEach { partition ->
                result[TopicPartition(
                    topic = topic.topic,
                    partition = partition.partition,
                )] = partition
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
            val req = assertIs<ListOffsetsRequest>(body)
            val topic = req.topics[0]
            val partition = topic.partitions[0]
            tp0.topic == topic.name
                    && tp0.partition == partition.partitionIndex
                    && timestamp == partition.timestamp
                    && leaderEpoch == partition.currentLeaderEpoch
        }
    }

    private fun listOffsetResponse(error: Errors, timestamp: Long, offset: Long): ListOffsetsResponse =
        listOffsetResponse(tp = tp0, error = error, timestamp = timestamp, offset = offset)

    private fun listOffsetResponse(
        tp: TopicPartition,
        error: Errors,
        timestamp: Long,
        offset: Long,
        leaderEpoch: Int = ListOffsetsResponse.UNKNOWN_EPOCH,
    ): ListOffsetsResponse = listOffsetResponse(
        offsets = mapOf(tp to offset),
        error = error,
        timestamp = timestamp,
        leaderEpoch = leaderEpoch
    )

    private fun listOffsetResponse(
        offsets: Map<TopicPartition, Long>,
        error: Errors,
        timestamp: Long,
        leaderEpoch: Int,
    ): ListOffsetsResponse {
        val responses = mutableMapOf<String, MutableList<ListOffsetsPartitionResponse>>()
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
        val topics = mutableListOf<ListOffsetsTopicResponse>()
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

    private fun fetchResponseWithTopLevelError(
        tp: TopicIdPartition,
        error: Errors,
        throttleTime: Int,
    ): FetchResponse {
        val partitions = mapOf(
            tp to FetchResponseData.PartitionData()
                .setPartitionIndex(tp.topicPartition.partition)
                .setErrorCode(error.code)
                .setHighWatermark(FetchResponse.INVALID_HIGH_WATERMARK)
        )
        return FetchResponse.of(
            error = error,
            throttleTimeMs = throttleTime,
            sessionId = FetchMetadata.INVALID_SESSION_ID,
            responseData = LinkedHashMap(partitions),
        )
    }

    private fun fullFetchResponseWithAbortedTransactions(
        records: MemoryRecords,
        abortedTransactions: List<AbortedTransaction>,
        error: Errors,
        lastStableOffset: Long,
        hw: Long,
        throttleTime: Int,
    ): FetchResponse {
        val partitions = mapOf(
            tidp0 to FetchResponseData.PartitionData()
                .setPartitionIndex(tp0.partition)
                .setErrorCode(error.code)
                .setHighWatermark(hw)
                .setLastStableOffset(lastStableOffset)
                .setLogStartOffset(0)
                .setAbortedTransactions(abortedTransactions)
                .setRecords(records)
        )
        return FetchResponse.of(
            error = Errors.NONE,
            throttleTimeMs = throttleTime,
            sessionId = FetchMetadata.INVALID_SESSION_ID,
            responseData = LinkedHashMap(partitions),
        )
    }

    private fun fullFetchResponse(
        sessionId: Int = FetchMetadata.INVALID_SESSION_ID,
        tp: TopicIdPartition,
        records: MemoryRecords?,
        error: Errors,
        hw: Long,
        lastStableOffset: Long = FetchResponse.INVALID_LAST_STABLE_OFFSET,
        logStartOffset: Long = 0,
        throttleTime: Int,
    ): FetchResponse {
        val partitions = mapOf(
            tp to FetchResponseData.PartitionData()
                .setPartitionIndex(tp.topicPartition.partition)
                .setErrorCode(error.code)
                .setHighWatermark(hw)
                .setLastStableOffset(lastStableOffset)
                .setLogStartOffset(logStartOffset)
                .setRecords(records)
        )
        return FetchResponse.of(
            error = Errors.NONE,
            throttleTimeMs = throttleTime,
            sessionId = sessionId,
            responseData = LinkedHashMap(partitions),
        )
    }

    private fun fullFetchResponse(
        tp: TopicIdPartition,
        records: MemoryRecords,
        error: Errors,
        hw: Long,
        lastStableOffset: Long,
        throttleTime: Int,
        preferredReplicaId: Int?,
    ): FetchResponse {
        val partitions = mapOf(
            tp to FetchResponseData.PartitionData()
                .setPartitionIndex(tp.topicPartition.partition)
                .setErrorCode(error.code)
                .setHighWatermark(hw)
                .setLastStableOffset(lastStableOffset)
                .setLogStartOffset(0)
                .setRecords(records)
                .setPreferredReadReplica(preferredReplicaId ?: FetchResponse.INVALID_PREFERRED_REPLICA_ID)
        )
        return FetchResponse.of(
            error = Errors.NONE,
            throttleTimeMs = throttleTime,
            sessionId = FetchMetadata.INVALID_SESSION_ID,
            responseData = LinkedHashMap(partitions),
        )
    }

    private fun newMetadataResponse(topic: String, error: Errors): MetadataResponse {
        val partitionsMetadata = mutableListOf<PartitionMetadata>()
        if (error === Errors.NONE) {
            val foundMetadata = initialUpdateResponse.topicMetadata()
                .firstOrNull { (_, topic1) -> topic1 == topic }
            foundMetadata?.let { metadata ->
                partitionsMetadata.addAll(metadata.partitionMetadata)
            }
        }
        val topicMetadata = MetadataResponse.TopicMetadata(
            error = error,
            topic = topic,
            isInternal = false,
            partitionMetadata = partitionsMetadata,
        )
        val brokers = initialUpdateResponse.brokers().toList()
        return metadataResponse(
            brokers = brokers,
            clusterId = initialUpdateResponse.clusterId,
            controllerId = initialUpdateResponse.controller!!.id,
            topicMetadataList = listOf(topicMetadata),
        )
    }

    /**
     * Assert that the [latest fetch][Fetcher.collectFetch] does not contain any
     * [user-visible records][Fetch.records], did not
     * [advance the consumer&#39;s position][Fetch.positionAdvanced],
     * and is [empty][Fetch.isEmpty].
     * @param reason the reason to include for assertion methods such as [assertTrue]
     */
    private fun assertEmptyFetch(reason: String) {
        val fetch = collectFetch<Any, Any>()
        assertEquals(emptyMap(), fetch.records(), reason)
        assertFalse(fetch.positionAdvanced(), reason)
        assertTrue(fetch.isEmpty, reason)
    }

    private fun <K, V> fetchedRecords(): Map<TopicPartition, List<ConsumerRecord<K, V>>> {
        val fetch = collectFetch<K, V>()
        return fetch.records()
    }

    private fun <K, V> collectFetch(): Fetch<K, V> {
        return (fetcher as Fetcher<K, V>).collectFetch()
    }

    private fun buildFetcher(
        offsetResetStrategy: OffsetResetStrategy = OffsetResetStrategy.EARLIEST,
        maxPollRecords: Int = Int.MAX_VALUE,
        isolationLevel: IsolationLevel = IsolationLevel.READ_UNCOMMITTED,
    ) = buildFetcher(
        metricConfig = MetricConfig(),
        offsetResetStrategy = offsetResetStrategy,
        keyDeserializer = ByteArrayDeserializer(),
        valueDeserializer = ByteArrayDeserializer(),
        maxPollRecords = maxPollRecords,
        isolationLevel = isolationLevel,
    )

    private fun <K, V> buildFetcher(
        metricConfig: MetricConfig = MetricConfig(),
        offsetResetStrategy: OffsetResetStrategy = OffsetResetStrategy.EARLIEST,
        keyDeserializer: Deserializer<K>,
        valueDeserializer: Deserializer<V>,
        maxPollRecords: Int = Int.MAX_VALUE,
        isolationLevel: IsolationLevel = IsolationLevel.READ_UNCOMMITTED,
        metadataExpireMs: Long = Long.MAX_VALUE,
        logContext: LogContext = LogContext(),
        subscriptionState: SubscriptionState = SubscriptionState(logContext, offsetResetStrategy),
    ) {
        buildDependencies(
            metricConfig = metricConfig,
            metadataExpireMs = metadataExpireMs,
            subscriptionState = subscriptionState,
            logContext = logContext,
        )
        fetcher = Fetcher(
            logContext = LogContext(),
            client = consumerClient!!,
            minBytes = minBytes,
            maxBytes = maxBytes,
            maxWaitMs = maxWaitMs,
            fetchSize = fetchSize,
            maxPollRecords = maxPollRecords,
            checkCrcs = true,  // check crc
            clientRackId = "",
            keyDeserializer = keyDeserializer,
            valueDeserializer = valueDeserializer,
            metadata = metadata!!,
            subscriptions = subscriptions!!,
            metrics = metrics,
            metricsRegistry = metricsRegistry!!,
            time = time,
            retryBackoffMs = retryBackoffMs,
            requestTimeoutMs = requestTimeoutMs,
            isolationLevel = isolationLevel,
            apiVersions = apiVersions,
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
            subscription = subscriptions!!,
            logContext = logContext,
            clusterResourceListeners = ClusterResourceListeners(),
        )
        client = MockClient(time, metadata!!)
        metrics = Metrics(config = metricConfig, time = time)
        consumerClient = ConsumerNetworkClient(
            logContext = logContext,
            client = client!!,
            metadata = metadata!!,
            time = time,
            retryBackoffMs = 100,
            requestTimeoutMs = 1000,
            maxPollTimeoutMs = Int.MAX_VALUE,
        )
        metricsRegistry = FetcherMetricsRegistry(
            tags = metricConfig.tags.keys,
            metricGrpPrefix = "consumer$groupId",
        )
    }

    private fun <T> collectRecordOffsets(records: List<ConsumerRecord<T, T>>): List<Long> =
        records.map { (_, _, offset) -> offset }

    companion object {
        private const val EPSILON = 0.0001
    }
}
