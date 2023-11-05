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

import org.apache.kafka.clients.ClientResponse
import org.apache.kafka.clients.GroupRebalanceConfig
import org.apache.kafka.clients.MockClient
import org.apache.kafka.clients.MockClient.RequestMatcher
import org.apache.kafka.clients.NodeApiVersions
import org.apache.kafka.clients.consumer.CommitFailedException
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.RebalanceProtocol
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.clients.consumer.OffsetCommitCallback
import org.apache.kafka.clients.consumer.OffsetResetStrategy
import org.apache.kafka.clients.consumer.RetriableCommitFailedException
import org.apache.kafka.clients.consumer.internals.ConsumerProtocol.deserializeSubscription
import org.apache.kafka.clients.consumer.internals.ConsumerProtocol.serializeAssignment
import org.apache.kafka.clients.consumer.internals.ConsumerProtocol.serializeSubscription
import org.apache.kafka.clients.consumer.internals.SubscriptionState.FetchPosition
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.Metric
import org.apache.kafka.common.MetricName
import org.apache.kafka.common.Node
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.ApiException
import org.apache.kafka.common.errors.AuthenticationException
import org.apache.kafka.common.errors.DisconnectException
import org.apache.kafka.common.errors.FencedInstanceIdException
import org.apache.kafka.common.errors.GroupAuthorizationException
import org.apache.kafka.common.errors.OffsetMetadataTooLarge
import org.apache.kafka.common.errors.RebalanceInProgressException
import org.apache.kafka.common.errors.TopicAuthorizationException
import org.apache.kafka.common.errors.UnsupportedVersionException
import org.apache.kafka.common.errors.WakeupException
import org.apache.kafka.common.internals.ClusterResourceListeners
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.common.message.HeartbeatResponseData
import org.apache.kafka.common.message.JoinGroupResponseData
import org.apache.kafka.common.message.JoinGroupResponseData.JoinGroupResponseMember
import org.apache.kafka.common.message.LeaveGroupResponseData
import org.apache.kafka.common.message.OffsetCommitRequestData
import org.apache.kafka.common.message.OffsetCommitRequestData.OffsetCommitRequestPartition
import org.apache.kafka.common.message.OffsetCommitRequestData.OffsetCommitRequestTopic
import org.apache.kafka.common.message.OffsetCommitResponseData
import org.apache.kafka.common.message.SyncGroupResponseData
import org.apache.kafka.common.metrics.KafkaMetric
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.protocol.types.Field
import org.apache.kafka.common.protocol.types.Schema
import org.apache.kafka.common.protocol.types.Struct
import org.apache.kafka.common.protocol.types.Type
import org.apache.kafka.common.record.RecordBatch
import org.apache.kafka.common.requests.AbstractRequest
import org.apache.kafka.common.requests.FindCoordinatorResponse
import org.apache.kafka.common.requests.HeartbeatResponse
import org.apache.kafka.common.requests.JoinGroupRequest
import org.apache.kafka.common.requests.JoinGroupResponse
import org.apache.kafka.common.requests.LeaveGroupRequest
import org.apache.kafka.common.requests.LeaveGroupResponse
import org.apache.kafka.common.requests.MetadataResponse
import org.apache.kafka.common.requests.MetadataResponse.PartitionMetadata
import org.apache.kafka.common.requests.OffsetCommitRequest
import org.apache.kafka.common.requests.OffsetCommitResponse
import org.apache.kafka.common.requests.OffsetFetchResponse
import org.apache.kafka.common.requests.RequestTestUtils.metadataResponse
import org.apache.kafka.common.requests.RequestTestUtils.metadataUpdateWith
import org.apache.kafka.common.requests.SyncGroupRequest
import org.apache.kafka.common.requests.SyncGroupResponse
import org.apache.kafka.common.utils.LogContext
import org.apache.kafka.common.utils.MockTime
import org.apache.kafka.common.utils.SystemTime
import org.apache.kafka.common.utils.Timer
import org.apache.kafka.common.utils.Utils
import org.apache.kafka.common.utils.Utils.mkSet
import org.apache.kafka.test.TestUtils
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.mockito.Mockito
import org.mockito.kotlin.argumentCaptor
import java.nio.ByteBuffer
import java.util.*
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference
import java.util.regex.Pattern
import kotlin.collections.ArrayList
import kotlin.collections.HashMap
import kotlin.collections.HashSet
import kotlin.math.min
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlin.test.fail

@Suppress("Deprecation")
abstract class ConsumerCoordinatorTest(private val protocol: RebalanceProtocol) {

    private val topic1 = "test1"

    private val topic2 = "test2"

    private val t1p = TopicPartition(topic1, 0)

    private val t2p = TopicPartition(topic2, 0)

    private val groupId = "test-group"

    private val groupInstanceId: String = "test-instance"

    private val rebalanceTimeoutMs = 60000

    private val sessionTimeoutMs = 10000

    private val heartbeatIntervalMs = 5000

    private val retryBackoffMs: Long = 100

    private val autoCommitIntervalMs = 2000

    private val requestTimeoutMs = 30000

    private val throttleMs = 10

    private val time = MockTime()

    private var rebalanceConfig: GroupRebalanceConfig? = null

    private val partitionAssignor: MockPartitionAssignor = MockPartitionAssignor(listOf(protocol))
    private val throwOnAssignmentAssignor = ThrowOnAssignmentAssignor(
        supportedProtocols = listOf(protocol),
        bookeepedException = KafkaException("Kaboom for assignment!"),
        name = "throw-on-assignment-assignor",
    )
    private val throwFatalErrorOnAssignmentAssignor = ThrowOnAssignmentAssignor(
        supportedProtocols = listOf(protocol),
        bookeepedException = IllegalStateException("Illegal state for assignment!"),
        name = "throw-fatal-error-on-assignment-assignor",
    )
    private val assignors = listOf(
        partitionAssignor,
        throwOnAssignmentAssignor,
        throwFatalErrorOnAssignmentAssignor,
    )
    private val assignorMap = mapOf(
        partitionAssignor.name() to partitionAssignor,
        throwOnAssignmentAssignor.name() to throwOnAssignmentAssignor,
        throwFatalErrorOnAssignmentAssignor.name() to throwFatalErrorOnAssignmentAssignor,
    )
    private val consumerId = "consumer"
    private val consumerId2 = "consumer2"
    private lateinit var client: MockClient
    private val metadataResponse = metadataUpdateWith(
        numNodes = 1,
        topicPartitionCounts = mapOf(
            topic1 to 1,
            topic2 to 1,
        ),
    )
    private val node = metadataResponse.brokers().iterator().next()

    private lateinit var subscriptions: SubscriptionState
    private lateinit var metadata: ConsumerMetadata
    private lateinit var metrics: Metrics
    private lateinit var consumerClient: ConsumerNetworkClient
    private lateinit var rebalanceListener: MockRebalanceListener
    private lateinit var mockOffsetCommitCallback: MockCommitCallback
    private lateinit var coordinator: ConsumerCoordinator

    @BeforeEach
    fun setup() {
        val logContext = LogContext()
        subscriptions = SubscriptionState(logContext, OffsetResetStrategy.EARLIEST)
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
        client.updateMetadata(metadataResponse)
        consumerClient = ConsumerNetworkClient(
            logContext = logContext,
            client = client,
            metadata = metadata,
            time = time,
            retryBackoffMs = 100,
            requestTimeoutMs = requestTimeoutMs,
            maxPollTimeoutMs = Int.MAX_VALUE,
        )
        metrics = Metrics(time = time)
        rebalanceListener = MockRebalanceListener()
        mockOffsetCommitCallback = MockCommitCallback()
        partitionAssignor.clear()
        rebalanceConfig = buildRebalanceConfig(null)
        coordinator = buildCoordinator(
            rebalanceConfig = rebalanceConfig,
            metrics = metrics,
            assignors = assignors,
            autoCommitEnabled = false,
            subscriptionState = subscriptions,
        )
    }

    private fun buildRebalanceConfig(groupInstanceId: String?): GroupRebalanceConfig {
        return GroupRebalanceConfig(
            sessionTimeoutMs = sessionTimeoutMs,
            rebalanceTimeoutMs = rebalanceTimeoutMs,
            heartbeatIntervalMs = heartbeatIntervalMs,
            groupId = groupId,
            groupInstanceId = groupInstanceId,
            retryBackoffMs = retryBackoffMs,
            leaveGroupOnClose = groupInstanceId == null,
        )
    }

    @AfterEach
    fun teardown() {
        metrics.close()
        coordinator.close(time.timer(0))
    }

    @Test
    fun testMetrics() {
        assertNotNull(getMetric("commit-latency-avg"))
        assertNotNull(getMetric("commit-latency-max"))
        assertNotNull(getMetric("commit-rate"))
        assertNotNull(getMetric("commit-total"))

        assertNotNull(getMetric("partition-revoked-latency-avg"))
        assertNotNull(getMetric("partition-revoked-latency-max"))
        assertNotNull(getMetric("partition-assigned-latency-avg"))
        assertNotNull(getMetric("partition-assigned-latency-max"))
        assertNotNull(getMetric("partition-lost-latency-avg"))
        assertNotNull(getMetric("partition-lost-latency-max"))
        assertNotNull(getMetric("assigned-partitions"))

        metrics.sensor("commit-latency").record(1.0)
        metrics.sensor("commit-latency").record(6.0)
        metrics.sensor("commit-latency").record(2.0)

        assertEquals(3.0, getMetric("commit-latency-avg")!!.metricValue())
        assertEquals(6.0, getMetric("commit-latency-max")!!.metricValue())
        assertEquals(0.1, getMetric("commit-rate")!!.metricValue())
        assertEquals(3.0, getMetric("commit-total")!!.metricValue())

        metrics.sensor("partition-revoked-latency").record(1.0)
        metrics.sensor("partition-revoked-latency").record(2.0)
        metrics.sensor("partition-assigned-latency").record(1.0)
        metrics.sensor("partition-assigned-latency").record(2.0)
        metrics.sensor("partition-lost-latency").record(1.0)
        metrics.sensor("partition-lost-latency").record(2.0)

        assertEquals(1.5, getMetric("partition-revoked-latency-avg")!!.metricValue())
        assertEquals(2.0, getMetric("partition-revoked-latency-max")!!.metricValue())
        assertEquals(1.5, getMetric("partition-assigned-latency-avg")!!.metricValue())
        assertEquals(2.0, getMetric("partition-assigned-latency-max")!!.metricValue())
        assertEquals(1.5, getMetric("partition-lost-latency-avg")!!.metricValue())
        assertEquals(2.0, getMetric("partition-lost-latency-max")!!.metricValue())
        assertEquals(0.0, getMetric("assigned-partitions")!!.metricValue())

        subscriptions.assignFromUser(setOf(t1p))
        assertEquals(1.0, getMetric("assigned-partitions")!!.metricValue())

        subscriptions.assignFromUser(mkSet(t1p, t2p))
        assertEquals(2.0, getMetric("assigned-partitions")!!.metricValue())
    }

    private fun getMetric(name: String): KafkaMetric? {
        return metrics.metrics[
            metrics.metricName(name, "$consumerId$groupId-coordinator-metrics")
        ]
    }

    @Test
    fun testPerformAssignmentShouldUpdateGroupSubscriptionAfterAssignmentIfNeeded() {
        val mockSubscriptionState = Mockito.mock(SubscriptionState::class.java)

        // the consumer only subscribed to "topic1"
        val memberSubscriptions = mapOf(consumerId to listOf(topic1))
        val metadata: MutableList<JoinGroupResponseMember> = ArrayList()
        for (subscriptionEntry in memberSubscriptions.entries) {
            val subscription = ConsumerPartitionAssignor.Subscription(subscriptionEntry.value)
            val buf = serializeSubscription(subscription)
            metadata.add(
                JoinGroupResponseMember()
                    .setMemberId(subscriptionEntry.key)
                    .setMetadata(buf.array())
            )
        }

        // normal case: the assignment result will have partitions for only the subscribed topic: "topic1"
        partitionAssignor.prepare(mapOf(consumerId to listOf(t1p)))
        buildCoordinator(
            rebalanceConfig = rebalanceConfig,
            metrics = Metrics(),
            assignors = assignors,
            autoCommitEnabled = false,
            subscriptionState = mockSubscriptionState,
        ).use { coordinator ->
            coordinator.onLeaderElected("1", partitionAssignor.name(), metadata, false)
            val topicsCaptor = argumentCaptor<Collection<String>>()
            // groupSubscribe should be only called 1 time, which is before assignment,
            // because the assigned topics are the same as the subscribed topics
            Mockito.verify(mockSubscriptionState, Mockito.times(1))
                .groupSubscribe(topicsCaptor.capture())
            val capturedTopics: List<Collection<String>> = topicsCaptor.allValues

            // expected the final group subscribed topics to be updated to "topic1"
            val expectedTopicsGotCalled = HashSet(listOf(topic1))
            assertEquals(expectedTopicsGotCalled, capturedTopics[0])
        }
        Mockito.clearInvocations(mockSubscriptionState)

        // unsubscribed topic partition assigned case: the assignment result will have partitions for
        // (1) subscribed topic: "topic1" and
        // (2) the additional unsubscribed topic: "topic2". We should add "topic2" into group subscription list
        partitionAssignor.prepare(mapOf(consumerId to listOf(t1p, t2p)))
        buildCoordinator(
            rebalanceConfig = rebalanceConfig,
            metrics = Metrics(),
            assignors = assignors,
            autoCommitEnabled = false,
            subscriptionState = mockSubscriptionState,
        ).use { coordinator ->
            coordinator.onLeaderElected("1", partitionAssignor.name(), metadata, false)

            val topicsCaptor = argumentCaptor<Collection<String>>()
            // val topicsCaptor = ArgumentCaptor.forClass<Collection<String>, Collection<String>>(MutableCollection::class.java)
            // groupSubscribe should be called 2 times, once before assignment, once after assignment
            // (because the assigned topics are not the same as the subscribed topics)
            Mockito.verify(mockSubscriptionState, Mockito.times(2))
                .groupSubscribe(topicsCaptor.capture())

            val capturedTopics: List<Collection<String>> = topicsCaptor.allValues

            // expected the final group subscribed topics to be updated to "topic1" and "topic2"
            val expectedTopicsGotCalled = setOf(topic1, topic2)
            assertEquals(expectedTopicsGotCalled, capturedTopics[1])
        }
    }

    fun subscriptionUserData(generation: Int): ByteBuffer {
        val generationKeyName = "generation"
        val cooperativeStickyAssignorUserDataV0 = Schema(Field(generationKeyName, Type.INT32))
        val struct = Struct(cooperativeStickyAssignorUserDataV0)
        struct[generationKeyName] = generation
        val buffer = ByteBuffer.allocate(cooperativeStickyAssignorUserDataV0.sizeOf(struct))
        cooperativeStickyAssignorUserDataV0.write(buffer, struct)
        buffer.flip()
        return buffer
    }

    private fun validateCooperativeAssignmentTestSetup(): List<JoinGroupResponseMember> {
        // consumer1 and consumer2 subscribed to "topic1" with 2 partitions: t1p, t2p
        val subscribedTopics = listOf(topic1)
        val memberSubscriptions = mapOf(
            consumerId to subscribedTopics,
            consumerId2 to subscribedTopics,
        )

        // the ownedPartition for consumer1 is t1p, t2p
        val subscriptionConsumer1 = ConsumerPartitionAssignor.Subscription(
            subscribedTopics, subscriptionUserData(1), listOf(t1p, t2p)
        )

        // the ownedPartition for consumer2 is empty
        val subscriptionConsumer2 = ConsumerPartitionAssignor.Subscription(
            subscribedTopics, subscriptionUserData(1), emptyList()
        )
        val metadata: MutableList<JoinGroupResponseMember> = ArrayList()
        for (subscriptionEntry: Map.Entry<String, List<String>> in memberSubscriptions.entries) {
            val buf = serializeSubscription(
                if (subscriptionEntry.key == consumerId) subscriptionConsumer1 else subscriptionConsumer2
            )
            metadata.add(
                JoinGroupResponseMember()
                    .setMemberId(subscriptionEntry.key)
                    .setMetadata(buf.array())
            )
        }
        return metadata
    }

    @Test
    fun testPerformAssignmentShouldValidateCooperativeAssignment() {
        val mockSubscriptionState = Mockito.mock(
            SubscriptionState::class.java
        )
        val metadata = validateCooperativeAssignmentTestSetup()

        // simulate the custom cooperative assignor didn't revoke the partition first before assign to other consumer
        val assignment: MutableMap<String, List<TopicPartition>> = HashMap()
        assignment[consumerId] = listOf(t1p)
        assignment[consumerId2] = listOf(t2p)
        partitionAssignor.prepare(assignment)
        buildCoordinator(
            rebalanceConfig,
            Metrics(),
            assignors,
            false,
            mockSubscriptionState
        ).use { coordinator ->
            if (protocol === RebalanceProtocol.COOPERATIVE) {
                // in cooperative protocol, we should throw exception when validating cooperative assignment
                val e: Exception = assertFailsWith<IllegalStateException> {
                    coordinator.onLeaderElected(
                        leaderId = "1",
                        protocol = partitionAssignor.name(),
                        allMemberMetadata = metadata,
                        skipAssignment = false,
                    )
                }
                assertTrue(
                    e.message!!.contains("Assignor supporting the COOPERATIVE protocol violates its requirements"),
                )
            } else {
                // in eager protocol, we should not validate assignment
                coordinator.onLeaderElected("1", partitionAssignor.name(), metadata, false)
            }
        }
    }

    @Test
    fun testOnLeaderElectedShouldSkipAssignment() {
        val mockSubscriptionState = Mockito.mock(
            SubscriptionState::class.java
        )
        val assignor = Mockito.mock(
            ConsumerPartitionAssignor::class.java
        )
        val assignorName = "mock-assignor"
        Mockito.`when`(assignor.name()).thenReturn(assignorName)
        Mockito.`when`(assignor.supportedProtocols()).thenReturn(
            listOf(
                protocol
            )
        )
        val memberSubscriptions = mapOf(consumerId to listOf(topic1))
        val metadata: MutableList<JoinGroupResponseMember> = ArrayList()
        for (subscriptionEntry: Map.Entry<String, List<String>> in memberSubscriptions.entries) {
            val subscription = ConsumerPartitionAssignor.Subscription(subscriptionEntry.value)
            val buf = serializeSubscription(subscription)
            metadata.add(
                JoinGroupResponseMember()
                    .setMemberId(subscriptionEntry.key)
                    .setMetadata(buf.array())
            )
        }
        buildCoordinator(
            rebalanceConfig = rebalanceConfig,
            metrics = Metrics(),
            assignors = listOf<ConsumerPartitionAssignor>(assignor),
            autoCommitEnabled = false,
            subscriptionState = mockSubscriptionState,
        ).use { coordinator ->
            assertEquals(
                expected = emptyMap(),
                actual = coordinator.onLeaderElected(
                    leaderId = "1",
                    protocol = assignorName,
                    allMemberMetadata = metadata,
                    skipAssignment = true,
                ),
            )
            assertTrue(coordinator.isLeader)
        }
        Mockito.verify(assignor, Mockito.never()).assign(Mockito.any(), Mockito.any())
    }

    @Test
    fun testPerformAssignmentShouldSkipValidateCooperativeAssignmentForBuiltInCooperativeStickyAssignor() {
        val mockSubscriptionState = Mockito.mock(SubscriptionState::class.java)
        val metadata = validateCooperativeAssignmentTestSetup()
        val assignorsWithCooperativeStickyAssignor = ArrayList(assignors)
        // create a mockPartitionAssignor with the same name as cooperative sticky assignor
        val mockCooperativeStickyAssignor = object : MockPartitionAssignor(listOf(protocol)) {
            override fun name(): String = CooperativeStickyAssignor.COOPERATIVE_STICKY_ASSIGNOR_NAME
        }
        assignorsWithCooperativeStickyAssignor.add(mockCooperativeStickyAssignor)

        // simulate the cooperative sticky assignor do the assignment with out-of-date ownedPartition
        val assignment = mapOf(
            consumerId to listOf(t1p),
            consumerId2 to listOf(t2p),
        )
        mockCooperativeStickyAssignor.prepare(assignment)
        buildCoordinator(
            rebalanceConfig = rebalanceConfig,
            metrics = Metrics(),
            assignors = assignorsWithCooperativeStickyAssignor,
            autoCommitEnabled = false,
            subscriptionState = mockSubscriptionState,
        ).use { coordinator ->
            // should not validate assignment for built-in cooperative sticky assignor
            coordinator.onLeaderElected(
                leaderId = "1",
                protocol = mockCooperativeStickyAssignor.name(),
                allMemberMetadata = metadata,
                skipAssignment = false,
            )
        }
    }

    @Test
    fun testSelectRebalanceProtcol() {
        val assignors: MutableList<ConsumerPartitionAssignor> = ArrayList()
        assignors.add(MockPartitionAssignor(listOf(RebalanceProtocol.EAGER)))
        assignors.add(MockPartitionAssignor(listOf(RebalanceProtocol.COOPERATIVE)))

        // no commonly supported protocols
        assertFailsWith<IllegalArgumentException> {
            buildCoordinator(
                rebalanceConfig = rebalanceConfig,
                metrics = Metrics(),
                assignors = assignors,
                autoCommitEnabled = false,
                subscriptionState = subscriptions,
            )
        }
        assignors.clear()
        assignors.add(
            MockPartitionAssignor(listOf(RebalanceProtocol.EAGER, RebalanceProtocol.COOPERATIVE))
        )
        assignors.add(
            MockPartitionAssignor(listOf(RebalanceProtocol.EAGER, RebalanceProtocol.COOPERATIVE))
        )
        buildCoordinator(
            rebalanceConfig = rebalanceConfig,
            metrics = Metrics(),
            assignors = assignors,
            autoCommitEnabled = false,
            subscriptionState = subscriptions,
        ).use { coordinator ->
            assertEquals(RebalanceProtocol.COOPERATIVE, coordinator.protocol)
        }
    }

    @Test
    fun testNormalHeartbeat() {
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE))
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE))

        // normal heartbeat
        time.sleep(sessionTimeoutMs.toLong())
        val future = coordinator.sendHeartbeatRequest() // should send out the heartbeat
        assertEquals(1, consumerClient.pendingRequestCount())
        assertFalse(future.isDone)
        client.prepareResponse(heartbeatResponse(Errors.NONE))
        consumerClient.poll(time.timer(0))
        assertTrue(future.isDone)
        assertTrue(future.succeeded())
    }

    @Test
    fun testGroupDescribeUnauthorized() {
        client.prepareResponse(groupCoordinatorResponse(node, Errors.GROUP_AUTHORIZATION_FAILED))
        assertFailsWith<GroupAuthorizationException> {
            coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE))
        }
    }

    @Test
    fun testGroupReadUnauthorized() {
        subscriptions.subscribe(setOf(topic1), rebalanceListener)
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE))
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE))
        client.prepareResponse(
            joinGroupLeaderResponse(
                generationId = 0,
                memberId = "memberId",
                subscriptions = emptyMap(),
                error = Errors.GROUP_AUTHORIZATION_FAILED,
            )
        )
        assertFailsWith<GroupAuthorizationException> {
            coordinator.poll(time.timer(Long.MAX_VALUE))
        }
    }

    @Test
    fun testCoordinatorNotAvailableWithUserAssignedType() {
        subscriptions.assignFromUser(setOf(t1p))
        // should mark coordinator unknown after COORDINATOR_NOT_AVAILABLE error
        client.prepareResponse(groupCoordinatorResponse(node, Errors.COORDINATOR_NOT_AVAILABLE))
        // set timeout to 0 because we don't want to retry after the error
        coordinator.poll(time.timer(0))
        assertTrue(coordinator.coordinatorUnknown())

        // should not try to find coordinator since we are in manual assignment
        // hence the prepared response should not be returned
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE))
        coordinator.poll(time.timer(Long.MAX_VALUE))
        assertTrue(coordinator.coordinatorUnknown())
    }

    @Test
    fun testAutoCommitAsyncWithUserAssignedType() {
        buildCoordinator(
            rebalanceConfig = rebalanceConfig,
            metrics = Metrics(),
            assignors = assignors,
            autoCommitEnabled = true,
            subscriptionState = subscriptions,
        ).use { coordinator ->
            subscriptions.assignFromUser(setOf(t1p))
            // set timeout to 0 because we expect no requests sent
            coordinator.poll(time.timer(0))
            assertTrue(coordinator.coordinatorUnknown())
            assertFalse(client.hasInFlightRequests())

            // elapse auto commit interval and set committable position
            time.sleep(autoCommitIntervalMs.toLong())
            subscriptions.seekUnvalidated(t1p, FetchPosition(100L))

            // should try to find coordinator since we are auto committing
            coordinator.poll(time.timer(0))
            assertTrue(coordinator.coordinatorUnknown())
            assertTrue(client.hasInFlightRequests())
            client.respond(groupCoordinatorResponse(node, Errors.NONE))
            coordinator.poll(time.timer(0))
            assertFalse(coordinator.coordinatorUnknown())
            // after we've discovered the coordinator we should send
            // out the commit request immediately
            assertTrue(client.hasInFlightRequests())
        }
    }

    @Test
    fun testCommitAsyncWithUserAssignedType() {
        subscriptions.assignFromUser(setOf(t1p))
        // set timeout to 0 because we expect no requests sent
        coordinator.poll(time.timer(0))
        assertTrue(coordinator.coordinatorUnknown())
        assertFalse(client.hasInFlightRequests())

        // should try to find coordinator since we are commit async
        coordinator.commitOffsetsAsync(
            offsets = mapOf(t1p to OffsetAndMetadata(100L)),
            callback = { offsets, exception ->
                fail("Commit should not get responses, but got offsets:$offsets, and exception:$exception")
            },
        )
        coordinator.poll(time.timer(0))
        assertTrue(coordinator.coordinatorUnknown())
        assertTrue(client.hasInFlightRequests())
        client.respond(groupCoordinatorResponse(node, Errors.NONE))
        coordinator.poll(time.timer(0))
        assertFalse(coordinator.coordinatorUnknown())
        // after we've discovered the coordinator we should send
        // out the commit request immediately
        assertTrue(client.hasInFlightRequests())
    }

    @Test
    fun testCoordinatorNotAvailable() {
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE))
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE))

        // COORDINATOR_NOT_AVAILABLE will mark coordinator as unknown
        time.sleep(sessionTimeoutMs.toLong())
        val future = coordinator.sendHeartbeatRequest() // should send out the heartbeat
        assertEquals(1, consumerClient.pendingRequestCount())
        assertFalse(future.isDone)
        client.prepareResponse(heartbeatResponse(Errors.COORDINATOR_NOT_AVAILABLE))
        time.sleep(sessionTimeoutMs.toLong())
        consumerClient.poll(time.timer(0))
        assertTrue(future.isDone)
        assertTrue(future.failed())
        assertEquals(Errors.COORDINATOR_NOT_AVAILABLE.exception!!, future.exception())
        assertTrue(coordinator.coordinatorUnknown())
    }

    @Test
    fun testManyInFlightAsyncCommitsWithCoordinatorDisconnect() {
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE))
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE))
        val numRequests = 1000
        val tp = TopicPartition("foo", 0)
        val responses = AtomicInteger(0)
        for (i in 0 until numRequests) {
            val offsets = mapOf(tp to OffsetAndMetadata(i.toLong()))
            coordinator.commitOffsetsAsync(
                offsets = offsets,
                callback = { _, exception ->
                    responses.incrementAndGet()
                    val cause = exception!!.cause
                    assertTrue(
                        actual = cause is DisconnectException,
                        message = "Unexpected exception cause type: ${if (cause == null) null else cause.javaClass}",
                    )
                },
            )
        }
        coordinator.markCoordinatorUnknown("test cause")
        consumerClient.pollNoWakeup()
        coordinator.invokeCompletedOffsetCommitCallbacks()
        assertEquals(numRequests, responses.get())
    }

    @Test
    fun testCoordinatorUnknownInUnsentCallbacksAfterCoordinatorDead() {
        // When the coordinator is marked dead, all unsent or in-flight requests are cancelled
        // with a disconnect error. This test case ensures that the corresponding callbacks see
        // the coordinator as unknown which prevents additional retries to the same coordinator.
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE))
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE))
        val asyncCallbackInvoked = AtomicBoolean(false)
        val offsetCommitRequestData = OffsetCommitRequestData()
            .setGroupId(groupId)
            .setTopics(
                listOf(
                    OffsetCommitRequestTopic()
                        .setName("foo")
                        .setPartitions(
                            listOf(
                                OffsetCommitRequestPartition()
                                    .setPartitionIndex(0)
                                    .setCommittedLeaderEpoch(RecordBatch.NO_PARTITION_LEADER_EPOCH)
                                    .setCommittedMetadata("")
                                    .setCommittedOffset(13L)
                                    .setCommitTimestamp(0)
                            )
                        )
                )
            )
        consumerClient.send(
            coordinator.checkAndGetCoordinator()!!,
            OffsetCommitRequest.Builder(offsetCommitRequestData)
        ).compose(
            object : RequestFutureAdapter<ClientResponse, Any>() {
                override fun onSuccess(value: ClientResponse, future: RequestFuture<Any>) = Unit
                override fun onFailure(exception: RuntimeException, future: RequestFuture<Any>) {
                    assertTrue(
                        actual = exception is DisconnectException,
                        message = "Unexpected exception type: ${exception.javaClass}"
                    )
                    assertTrue(coordinator.coordinatorUnknown())
                    asyncCallbackInvoked.set(true)
                }
            },
        )
        coordinator.markCoordinatorUnknown("test cause")
        consumerClient.pollNoWakeup()
        assertTrue(asyncCallbackInvoked.get())
    }

    @Test
    fun testNotCoordinator() {
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE))
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE))

        // not_coordinator will mark coordinator as unknown
        time.sleep(sessionTimeoutMs.toLong())
        val future = coordinator.sendHeartbeatRequest() // should send out the heartbeat
        assertEquals(1, consumerClient.pendingRequestCount())
        assertFalse(future.isDone)
        client.prepareResponse(heartbeatResponse(Errors.NOT_COORDINATOR))
        time.sleep(sessionTimeoutMs.toLong())
        consumerClient.poll(time.timer(0))
        assertTrue(future.isDone)
        assertTrue(future.failed())
        assertEquals(Errors.NOT_COORDINATOR.exception!!, future.exception())
        assertTrue(coordinator.coordinatorUnknown())
    }

    @Test
    fun testIllegalGeneration() {
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE))
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE))

        // illegal_generation will cause re-partition
        subscriptions.subscribe(setOf(topic1), rebalanceListener)
        subscriptions.assignFromSubscribed(listOf(t1p))
        time.sleep(sessionTimeoutMs.toLong())
        val future = coordinator.sendHeartbeatRequest() // should send out the heartbeat
        assertEquals(1, consumerClient.pendingRequestCount())
        assertFalse(future.isDone)
        client.prepareResponse(heartbeatResponse(Errors.ILLEGAL_GENERATION))
        time.sleep(sessionTimeoutMs.toLong())
        consumerClient.poll(time.timer(0))
        assertTrue(future.isDone)
        assertTrue(future.failed())
        assertEquals(Errors.ILLEGAL_GENERATION.exception!!, future.exception())
        assertTrue(coordinator.rejoinNeededOrPending())
        coordinator.poll(time.timer(0))
        assertEquals(1, rebalanceListener.lostCount)
        assertEquals(setOf(t1p), rebalanceListener.lost)
    }

    @Test
    fun testUnsubscribeWithValidGeneration() {
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE))
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE))
        subscriptions.subscribe(setOf(topic1), rebalanceListener)
        val buffer = serializeAssignment(
            ConsumerPartitionAssignor.Assignment(listOf(t1p), ByteBuffer.wrap(ByteArray(0)))
        )
        coordinator.onJoinComplete(1, "memberId", partitionAssignor.name(), buffer)
        coordinator.onLeavePrepare()
        assertEquals(1, rebalanceListener.lostCount)
        assertEquals(0, rebalanceListener.revokedCount)
    }

    @Test
    fun testRevokeExceptionThrownFirstNonBlockingSubCallbacks() {
        val throwOnRevokeListener = object : MockRebalanceListener() {
            override fun onPartitionsRevoked(partitions: Collection<TopicPartition>) {
                super.onPartitionsRevoked(partitions)
                throw KafkaException("Kaboom on revoke!")
            }
        }
        if (protocol === RebalanceProtocol.COOPERATIVE) {
            verifyOnCallbackExceptions(
                rebalanceListener = throwOnRevokeListener,
                assignorName = throwOnAssignmentAssignor.name(),
                exceptionMessage = "Kaboom on revoke!",
                causeMessage = null,
            )
        } else {
            // Eager protocol doesn't revoke partitions.
            verifyOnCallbackExceptions(
                rebalanceListener = throwOnRevokeListener,
                assignorName = throwOnAssignmentAssignor.name(),
                exceptionMessage = "Kaboom for assignment!",
                causeMessage = null,
            )
        }
    }

    @Test
    fun testOnAssignmentExceptionThrownFirstNonBlockingSubCallbacks() {
        val throwOnAssignListener = object : MockRebalanceListener() {
            override fun onPartitionsAssigned(partitions: Collection<TopicPartition>) {
                super.onPartitionsAssigned(partitions)
                throw KafkaException("Kaboom on partition assign!")
            }
        }
        verifyOnCallbackExceptions(
            rebalanceListener = throwOnAssignListener,
            assignorName = throwOnAssignmentAssignor.name(),
            exceptionMessage = "Kaboom for assignment!",
            causeMessage = null,
        )
    }

    @Test
    fun testOnPartitionsAssignExceptionThrownWhenNoPreviousThrownCallbacks() {
        val throwOnAssignListener = object : MockRebalanceListener() {
            override fun onPartitionsAssigned(partitions: Collection<TopicPartition>) {
                super.onPartitionsAssigned(partitions)
                throw KafkaException("Kaboom on partition assign!")
            }
        }
        verifyOnCallbackExceptions(
            rebalanceListener = throwOnAssignListener,
            assignorName = partitionAssignor.name(),
            exceptionMessage = "Kaboom on partition assign!",
            causeMessage = null,
        )
    }

    @Test
    fun testOnRevokeExceptionShouldBeRenderedIfNotKafkaException() {
        val throwOnRevokeListener = object : MockRebalanceListener() {
            override fun onPartitionsRevoked(partitions: Collection<TopicPartition>) {
                super.onPartitionsRevoked(partitions)
                error("Illegal state on partition revoke!")
            }
        }
        if (protocol === RebalanceProtocol.COOPERATIVE) {
            verifyOnCallbackExceptions(
                rebalanceListener = throwOnRevokeListener,
                assignorName = throwOnAssignmentAssignor.name(),
                exceptionMessage = "User rebalance callback throws an error",
                causeMessage = "Illegal state on partition revoke!"
            )
        } else {
            // Eager protocol doesn't revoke partitions.
            verifyOnCallbackExceptions(
                rebalanceListener = throwOnRevokeListener,
                assignorName = throwOnAssignmentAssignor.name(),
                exceptionMessage = "Kaboom for assignment!",
                causeMessage = null,
            )
        }
    }

    @Test
    fun testOnAssignmentExceptionShouldBeRenderedIfNotKafkaException() {
        val throwOnAssignListener = object : MockRebalanceListener() {
            override fun onPartitionsAssigned(partitions: Collection<TopicPartition>) {
                super.onPartitionsAssigned(partitions)
                throw KafkaException("Kaboom on partition assign!")
            }
        }
        verifyOnCallbackExceptions(
            rebalanceListener = throwOnAssignListener,
            assignorName = throwFatalErrorOnAssignmentAssignor.name(),
            exceptionMessage = "User rebalance callback throws an error",
            causeMessage = "Illegal state for assignment!",
        )
    }

    @Test
    fun testOnPartitionsAssignExceptionShouldBeRenderedIfNotKafkaException() {
        val throwOnAssignListener = object : MockRebalanceListener() {
            override fun onPartitionsAssigned(partitions: Collection<TopicPartition>) {
                super.onPartitionsAssigned(partitions)
                error("Illegal state on partition assign!")
            }
        }
        verifyOnCallbackExceptions(
            rebalanceListener = throwOnAssignListener,
            assignorName = partitionAssignor.name(),
            exceptionMessage = "User rebalance callback throws an error",
            causeMessage = "Illegal state on partition assign!",
        )
    }

    private fun verifyOnCallbackExceptions(
        rebalanceListener: MockRebalanceListener,
        assignorName: String,
        exceptionMessage: String?,
        causeMessage: String?,
    ) {
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE))
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE))
        subscriptions.subscribe(setOf(topic1), rebalanceListener)
        val buffer = serializeAssignment(
            ConsumerPartitionAssignor.Assignment(listOf(t1p), ByteBuffer.wrap(ByteArray(0)))
        )
        subscriptions.assignFromSubscribed(setOf(t2p))
        if (exceptionMessage != null) {
            val exception = assertFailsWith<KafkaException> {
                coordinator.onJoinComplete(
                    generation = 1,
                    memberId = "memberId",
                    protocol = assignorName,
                    memberAssignment = buffer,
                )
            }
            assertEquals(exceptionMessage, exception.message)
            if (causeMessage != null) {
                assertEquals(causeMessage, exception.cause!!.message)
            }
        }

        // Eager doesn't trigger on partition revoke.
        assertEquals(
            if (protocol === RebalanceProtocol.COOPERATIVE) 1 else 0,
            rebalanceListener.revokedCount
        )
        assertEquals(0, rebalanceListener.lostCount)
        assertEquals(1, rebalanceListener.assignedCount)
        assertTrue(
            actual = assignorMap.containsKey(assignorName),
            message = "Unknown assignor name: $assignorName",
        )
        assertEquals(1, assignorMap[assignorName]!!.numAssignment())
    }

    @Test
    fun testUnsubscribeWithInvalidGeneration() {
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE))
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE))
        subscriptions.subscribe(setOf(topic1), rebalanceListener)
        subscriptions.assignFromSubscribed(listOf(t1p))
        coordinator.onLeavePrepare()
        assertEquals(1, rebalanceListener.lostCount)
        assertEquals(0, rebalanceListener.revokedCount)
    }

    @Test
    fun testUnknownMemberId() {
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE))
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE))

        // illegal_generation will cause re-partition
        subscriptions.subscribe(setOf(topic1), rebalanceListener)
        subscriptions.assignFromSubscribed(listOf(t1p))
        time.sleep(sessionTimeoutMs.toLong())
        val future = coordinator.sendHeartbeatRequest() // should send out the heartbeat
        assertEquals(1, consumerClient.pendingRequestCount())
        assertFalse(future.isDone)
        client.prepareResponse(heartbeatResponse(Errors.UNKNOWN_MEMBER_ID))
        time.sleep(sessionTimeoutMs.toLong())
        consumerClient.poll(time.timer(0))
        assertTrue(future.isDone)
        assertTrue(future.failed())
        assertEquals(Errors.UNKNOWN_MEMBER_ID.exception!!, future.exception())
        assertTrue(coordinator.rejoinNeededOrPending())
        coordinator.poll(time.timer(0))
        assertEquals(1, rebalanceListener.lostCount)
        assertEquals(setOf(t1p), rebalanceListener.lost)
    }

    @Test
    fun testCoordinatorDisconnect() {
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE))
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE))

        // coordinator disconnect will mark coordinator as unknown
        time.sleep(sessionTimeoutMs.toLong())
        val future = coordinator.sendHeartbeatRequest() // should send out the heartbeat
        assertEquals(1, consumerClient.pendingRequestCount())
        assertFalse(future.isDone)
        client.prepareResponse(
            response = heartbeatResponse(Errors.NONE),
            disconnected = true, // return disconnected
        )
        time.sleep(sessionTimeoutMs.toLong())
        consumerClient.poll(time.timer(0))
        assertTrue(future.isDone)
        assertTrue(future.failed())
        assertTrue(future.exception() is DisconnectException)
        assertTrue(coordinator.coordinatorUnknown())
    }

    @Test
    fun testJoinGroupInvalidGroupId() {
        val consumerId = "leader"
        subscriptions.subscribe(setOf(topic1), rebalanceListener)

        // ensure metadata is up-to-date for leader
        client.updateMetadata(metadataResponse)
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE))
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE))
        client.prepareResponse(
            joinGroupLeaderResponse(
                generationId = 0,
                memberId = consumerId,
                subscriptions = emptyMap(),
                error = Errors.INVALID_GROUP_ID,
            )
        )
        assertFailsWith<ApiException> { coordinator.poll(time.timer(Long.MAX_VALUE)) }
    }

    @Test
    fun testNormalJoinGroupLeader() {
        val consumerId = "leader"
        val subscription = setOf(topic1)
        val owned = emptyList<TopicPartition>()
        val assigned = listOf(t1p)
        subscriptions.subscribe(setOf(topic1), rebalanceListener)

        // ensure metadata is up-to-date for leader
        client.updateMetadata(metadataResponse)
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE))
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE))

        // normal join group
        val memberSubscriptions = mapOf(consumerId to listOf(topic1))
        partitionAssignor.prepare(mapOf(consumerId to assigned))
        client.prepareResponse(
            joinGroupLeaderResponse(
                generationId = 1,
                memberId = consumerId,
                subscriptions = memberSubscriptions,
                error = Errors.NONE,
            )
        )
        client.prepareResponse(
            matcher = { body ->
                val sync = body as SyncGroupRequest
                sync.data().memberId == consumerId
                        && sync.data().generationId == 1
                        && sync.groupAssignments().containsKey(consumerId)
            },
            response = syncGroupResponse(assigned, Errors.NONE),
        )
        coordinator.poll(time.timer(Long.MAX_VALUE))
        assertFalse(coordinator.rejoinNeededOrPending())
        assertEquals(assigned.toSet(), subscriptions.assignedPartitions())
        assertEquals(subscription, subscriptions.metadataTopics())
        assertEquals(0, rebalanceListener.revokedCount)
        assertNull(rebalanceListener.revoked)
        assertEquals(1, rebalanceListener.assignedCount)
        assertEquals(getAdded(owned, assigned), rebalanceListener.assigned)
    }

    @Test
    fun testOutdatedCoordinatorAssignment() {
        val consumerId = "outdated_assignment"
        val owned = emptyList<TopicPartition>()
        val oldSubscription = listOf(topic2)
        val oldAssignment = listOf(t2p)
        val newSubscription = listOf(topic1)
        val newAssignment = listOf(t1p)
        subscriptions.subscribe(oldSubscription.toSet(), rebalanceListener)
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE))
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE))

        // Test coordinator returning unsubscribed partitions
        partitionAssignor.prepare(mapOf(consumerId to newAssignment))

        // First incorrect assignment for subscription
        client.prepareResponse(
            joinGroupLeaderResponse(
                generationId = 1,
                memberId = consumerId,
                subscriptions = mapOf(consumerId to oldSubscription),
                error = Errors.NONE,
            )
        )
        client.prepareResponse(
            matcher = { body ->
                val sync = body as SyncGroupRequest
                sync.data().memberId == consumerId
                        && sync.data().generationId == 1
                        && sync.groupAssignments().containsKey(consumerId)
            },
            response = syncGroupResponse(oldAssignment, Errors.NONE),
        )

        // Second correct assignment for subscription
        client.prepareResponse(
            joinGroupLeaderResponse(
                generationId = 1,
                memberId = consumerId,
                subscriptions = mapOf(consumerId to newSubscription),
                error = Errors.NONE,
            )
        )
        client.prepareResponse(
            matcher = { body ->
                val sync = body as SyncGroupRequest
                sync.data().memberId == consumerId
                        && sync.data().generationId == 1
                        && sync.groupAssignments().containsKey(consumerId)
            },
            response = syncGroupResponse(newAssignment, Errors.NONE),
        )

        // Poll once so that the join group future gets created and complete
        coordinator.poll(time.timer(0))

        // Before the sync group response gets completed change the subscription
        subscriptions.subscribe(newSubscription.toSet(), rebalanceListener)
        coordinator.poll(time.timer(0))
        coordinator.poll(time.timer(Long.MAX_VALUE))
        val assigned = getAdded(owned, newAssignment)
        assertFalse(coordinator.rejoinNeededOrPending())
        assertEquals(newAssignment.toSet(), subscriptions.assignedPartitions())
        assertEquals(newSubscription.toSet(), subscriptions.metadataTopics())
        assertEquals(
            expected = if (protocol === RebalanceProtocol.EAGER) 1 else 0,
            actual = rebalanceListener.revokedCount,
        )
        assertEquals(1, rebalanceListener.assignedCount)
        assertEquals(assigned, rebalanceListener.assigned)
    }

    @Test
    fun testMetadataTopicsDuringSubscriptionChange() {
        val consumerId = "subscription_change"
        val oldSubscription = listOf(topic1)
        val oldAssignment = listOf(t1p)
        val newSubscription = listOf(topic2)
        val newAssignment = listOf(t2p)
        subscriptions.subscribe(oldSubscription.toSet(), rebalanceListener)
        assertEquals(oldSubscription.toSet(), subscriptions.metadataTopics())
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE))
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE))
        prepareJoinAndSyncResponse(consumerId, 1, oldSubscription, oldAssignment)
        coordinator.poll(time.timer(0))
        assertEquals(oldSubscription.toSet(), subscriptions.metadataTopics())
        subscriptions.subscribe(newSubscription.toSet(), rebalanceListener)
        assertEquals(mkSet(topic1, topic2), subscriptions.metadataTopics())
        prepareJoinAndSyncResponse(consumerId, 2, newSubscription, newAssignment)
        coordinator.poll(time.timer(Long.MAX_VALUE))
        assertFalse(coordinator.rejoinNeededOrPending())
        assertEquals(newAssignment.toSet(), subscriptions.assignedPartitions())
        assertEquals(newSubscription.toSet(), subscriptions.metadataTopics())
    }

    @Test
    fun testPatternJoinGroupLeader() {
        val consumerId = "leader"
        val assigned = listOf(t1p, t2p)
        val owned = emptyList<TopicPartition>()
        subscriptions.subscribe(Pattern.compile("test.*"), rebalanceListener)

        // partially update the metadata with one topic first,
        // let the leader to refresh metadata during assignment
        client.updateMetadata(
            metadataUpdateWith(
                numNodes = 1,
                topicPartitionCounts = mapOf(topic1 to 1)
            ),
        )
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE))
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE))

        // normal join group
        val memberSubscriptions = mapOf(consumerId to listOf(topic1))
        partitionAssignor.prepare(mapOf(consumerId to assigned))
        client.prepareResponse(
            joinGroupLeaderResponse(
                generationId = 1,
                memberId = consumerId,
                subscriptions = memberSubscriptions,
                error = Errors.NONE,
            )
        )
        client.prepareResponse(
            matcher = { body ->
                val sync = body as SyncGroupRequest
                sync.data().memberId == consumerId
                        && sync.data().generationId == 1
                        && sync.groupAssignments().containsKey(consumerId)
            },
            response = syncGroupResponse(assigned, Errors.NONE),
        )
        // expect client to force updating the metadata, if yes gives it both topics
        client.prepareMetadataUpdate(metadataResponse)
        coordinator.poll(time.timer(Long.MAX_VALUE))
        assertFalse(coordinator.rejoinNeededOrPending())
        assertEquals(2, subscriptions.numAssignedPartitions())
        assertEquals(2, subscriptions.metadataTopics().size)
        assertEquals(2, subscriptions.subscription().size)
        // callback not triggered at all since there's nothing to be revoked
        assertEquals(0, rebalanceListener.revokedCount)
        assertNull(rebalanceListener.revoked)
        assertEquals(1, rebalanceListener.assignedCount)
        assertEquals(getAdded(owned, assigned), rebalanceListener.assigned)
    }

    @Test
    fun testMetadataRefreshDuringRebalance() {
        val consumerId = "leader"
        val owned = emptyList<TopicPartition>()
        val oldAssigned = listOf(t1p)
        subscriptions.subscribe(Pattern.compile(".*"), rebalanceListener)
        client.updateMetadata(
            metadataUpdateWith(
                numNodes = 1,
                topicPartitionCounts = mapOf(topic1 to 1),
            ),
        )
        coordinator.maybeUpdateSubscriptionMetadata()
        assertEquals(setOf(topic1), subscriptions.subscription())
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE))
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE))
        val initialSubscription = mapOf(consumerId to listOf(topic1))
        partitionAssignor.prepare(mapOf(consumerId to oldAssigned))

        // the metadata will be updated in flight with a new topic added
        val updatedSubscription = listOf(topic1, topic2)
        client.prepareResponse(
            joinGroupLeaderResponse(
                generationId = 1,
                memberId = consumerId,
                subscriptions = initialSubscription,
                error = Errors.NONE,
            )
        )
        client.prepareResponse(
            matcher = { body ->
                val updatedPartitions: MutableMap<String, Int> = HashMap()
                for (topic in updatedSubscription) updatedPartitions[topic] = 1
                client.updateMetadata(
                    metadataUpdateWith(
                        numNodes = 1,
                        topicPartitionCounts = updatedPartitions,
                    ),
                )
                true
            },
            response = syncGroupResponse(oldAssigned, Errors.NONE),
        )
        coordinator.poll(time.timer(Long.MAX_VALUE))

        // rejoin will only be set in the next poll call
        assertFalse(coordinator.rejoinNeededOrPending())
        assertEquals(setOf(topic1), subscriptions.subscription())
        assertEquals(oldAssigned.toSet(), subscriptions.assignedPartitions())
        // nothing to be revoked and hence no callback triggered
        assertEquals(0, rebalanceListener.revokedCount)
        assertNull(rebalanceListener.revoked)
        assertEquals(1, rebalanceListener.assignedCount)
        assertEquals(getAdded(owned, oldAssigned), rebalanceListener.assigned)
        val newAssigned = listOf(t1p, t2p)
        val updatedSubscriptions =
            mapOf(consumerId to listOf(topic1, topic2))
        partitionAssignor.prepare(mapOf(consumerId to newAssigned))

        // we expect to see a second rebalance with the new-found topics
        client.prepareResponse(
            matcher = { body ->
                val join = body as JoinGroupRequest
                val protocolIterator = join.data().protocols.iterator()
                assertTrue(protocolIterator.hasNext())
                val protocolMetadata = protocolIterator.next()
                val metadata = ByteBuffer.wrap(protocolMetadata.metadata)
                val subscription = deserializeSubscription(metadata)
                metadata.rewind()
                subscription.topics.containsAll(updatedSubscription)
            },
            response = joinGroupLeaderResponse(
                generationId = 2,
                memberId = consumerId,
                subscriptions = updatedSubscriptions,
                error = Errors.NONE,
            ),
        )
        // update the metadata again back to topic1
        client.prepareResponse(
            matcher = { body ->
                client.updateMetadata(metadataUpdateWith(numNodes = 1, topicPartitionCounts = mapOf(topic1 to 1)))
                true
            },
            response = syncGroupResponse(newAssigned, Errors.NONE),
        )
        coordinator.poll(time.timer(Long.MAX_VALUE))
        var revoked = getRevoked(oldAssigned, newAssigned)
        var revokedCount = if (revoked.isEmpty()) 0 else 1
        assertFalse(coordinator.rejoinNeededOrPending())
        assertEquals(updatedSubscription.toSet(), subscriptions.subscription())
        assertEquals(newAssigned.toSet(), subscriptions.assignedPartitions())
        assertEquals(revokedCount, rebalanceListener.revokedCount)
        assertEquals(
            expected = revoked.ifEmpty { null },
            actual = rebalanceListener.revoked,
        )
        assertEquals(2, rebalanceListener.assignedCount)
        assertEquals(getAdded(oldAssigned, newAssigned), rebalanceListener.assigned)

        // we expect to see a third rebalance with the new-found topics
        partitionAssignor.prepare(mapOf(consumerId to oldAssigned))
        client.prepareResponse(
            matcher = { body ->
                val join = body as JoinGroupRequest
                val protocolIterator = join.data().protocols.iterator()
                assertTrue(protocolIterator.hasNext())
                val protocolMetadata = protocolIterator.next()
                val metadata = ByteBuffer.wrap(protocolMetadata.metadata)
                val subscription = deserializeSubscription(metadata)
                metadata.rewind()
                subscription.topics.contains(topic1)
            },
            response = joinGroupLeaderResponse(
                generationId = 3,
                memberId = consumerId,
                subscriptions = initialSubscription,
                error = Errors.NONE,
            ),
        )
        client.prepareResponse(syncGroupResponse(oldAssigned, Errors.NONE))
        coordinator.poll(time.timer(Long.MAX_VALUE))
        revoked = getRevoked(newAssigned, oldAssigned)
        assertFalse(revoked.isEmpty())
        revokedCount += 1
        val added = getAdded(newAssigned, oldAssigned)
        assertFalse(coordinator.rejoinNeededOrPending())
        assertEquals(setOf(topic1), subscriptions.subscription())
        assertEquals(oldAssigned.toSet(), subscriptions.assignedPartitions())
        assertEquals(revokedCount, rebalanceListener.revokedCount)
        assertEquals(
            revoked.ifEmpty { null },
            rebalanceListener.revoked
        )
        assertEquals(3, rebalanceListener.assignedCount)
        assertEquals(added, rebalanceListener.assigned)
        assertEquals(0, rebalanceListener.lostCount)
    }

    @Test
    fun testForceMetadataRefreshForPatternSubscriptionDuringRebalance() {
        // Set up a non-leader consumer with pattern subscription and a cluster containing one topic matching the
        // pattern.
        subscriptions.subscribe(Pattern.compile(".*"), rebalanceListener)
        client.updateMetadata(metadataUpdateWith(numNodes = 1, topicPartitionCounts = mapOf(topic1 to 1)))
        coordinator.maybeUpdateSubscriptionMetadata()
        assertEquals(setOf(topic1), subscriptions.subscription())
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE))
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE))

        // Instrument the test so that metadata will contain two topics after next refresh.
        client.prepareMetadataUpdate(metadataResponse)
        client.prepareResponse(
            joinGroupFollowerResponse(
                generationId = 1,
                memberId = consumerId,
                leaderId = "leader",
                error = Errors.NONE,
            )
        )
        client.prepareResponse(
            matcher = { body ->
                val sync = body as SyncGroupRequest
                sync.data().memberId == consumerId
                        && sync.data().generationId == 1
                        && sync.groupAssignments().isEmpty()
            },
            response = syncGroupResponse(listOf(t1p), Errors.NONE),
        )
        partitionAssignor.prepare(mapOf(consumerId to listOf(t1p)))

        // This will trigger rebalance.
        coordinator.poll(time.timer(Long.MAX_VALUE))

        // Make sure that the metadata was refreshed during the rebalance and thus subscriptions now contain two topics.
        val updatedSubscriptionSet: Set<String> = HashSet(listOf(topic1, topic2))
        assertEquals(updatedSubscriptionSet, subscriptions.subscription())

        // Refresh the metadata again. Since there have been no changes since the last refresh, it won't trigger
        // rebalance again.
        metadata.requestUpdate()
        consumerClient.poll(time.timer(Long.MAX_VALUE))
        assertFalse(coordinator.rejoinNeededOrPending())
    }

    @Test
    fun testForceMetadataDeleteForPatternSubscriptionDuringRebalance() {
        buildCoordinator(
            rebalanceConfig = rebalanceConfig,
            metrics = Metrics(),
            assignors = assignors,
            autoCommitEnabled = true,
            subscriptionState = subscriptions,
        ).use { coordinator ->
            subscriptions.subscribe(Pattern.compile("test.*"), rebalanceListener)
            client.updateMetadata(
                metadataUpdateWith(
                    numNodes = 1,
                    topicPartitionCounts = mapOf(
                        topic1 to 1,
                        topic2 to 1,
                    ),
                )
            )
            coordinator.maybeUpdateSubscriptionMetadata()
            assertEquals(setOf(topic1, topic2), subscriptions.subscription())
            client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE))
            coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE))
            val deletedMetadataResponse: MetadataResponse = metadataUpdateWith(
                numNodes = 1,
                topicPartitionCounts = mapOf(topic1 to 1),
            )
            // Instrument the test so that metadata will contain only one topic after next refresh.
            client.prepareMetadataUpdate(deletedMetadataResponse)
            client.prepareResponse(
                joinGroupFollowerResponse(
                    generationId = 1,
                    memberId = consumerId,
                    leaderId = "leader",
                    error = Errors.NONE,
                )
            )
            client.prepareResponse(
                matcher = { body ->
                    val sync = body as SyncGroupRequest
                    sync.data().memberId == consumerId
                            && sync.data().generationId == 1
                            && sync.groupAssignments().isEmpty()
                },
                response = syncGroupResponse(listOf(t1p), Errors.NONE),
            )
            partitionAssignor.prepare(mapOf(consumerId to listOf(t1p)))

            // This will trigger rebalance.
            coordinator.poll(time.timer(Long.MAX_VALUE))

            // Make sure that the metadata was refreshed during the rebalance and thus subscriptions
            // now contain only one topic.
            assertEquals(setOf(topic1), subscriptions.subscription())

            // Refresh the metadata again. Since there have been no changes since the last refresh,
            // it won't trigger rebalance again.
            metadata.requestUpdate()
            consumerClient.poll(time.timer(Long.MAX_VALUE))
            assertFalse(coordinator.rejoinNeededOrPending())
        }
    }

    @Test
    fun testOnJoinPrepareWithOffsetCommitShouldSuccessAfterRetry() {
        prepareCoordinatorForCloseTest(
            useGroupManagement = true,
            autoCommit = true,
            groupInstanceId = null,
            shouldPoll = false,
        ).use { coordinator ->
            val generationId = 42
            val memberId = "consumer-42"
            var pollTimer = time.timer(100L)
            client.prepareResponse(
                offsetCommitResponse(mapOf(t1p to Errors.UNKNOWN_TOPIC_OR_PARTITION))
            )
            var res = coordinator.onJoinPrepare(pollTimer, generationId, memberId)
            assertFalse(res)
            pollTimer = time.timer(100L)
            client.prepareResponse(offsetCommitResponse(mapOf(t1p to Errors.NONE)))
            res = coordinator.onJoinPrepare(pollTimer, generationId, memberId)
            assertTrue(res)
            assertFalse(client.hasPendingResponses())
            assertFalse(client.hasInFlightRequests())
            assertFalse(coordinator.coordinatorUnknown())
        }
    }

    @Test
    fun testOnJoinPrepareWithOffsetCommitShouldKeepJoinAfterNonRetryableException() {
        prepareCoordinatorForCloseTest(
            useGroupManagement = true,
            autoCommit = true,
            groupInstanceId = null,
            shouldPoll = false,
        ).use { coordinator ->
            val generationId = 42
            val memberId = "consumer-42"
            val pollTimer: Timer = time.timer(100L)
            client.prepareResponse(offsetCommitResponse(mapOf(t1p to Errors.UNKNOWN_MEMBER_ID)))
            val res: Boolean = coordinator.onJoinPrepare(pollTimer, generationId, memberId)
            assertTrue(res)
            assertFalse(client.hasPendingResponses())
            assertFalse(client.hasInFlightRequests())
            assertFalse(coordinator.coordinatorUnknown())
        }
    }

    @Test
    fun testOnJoinPrepareWithOffsetCommitShouldKeepJoinAfterRebalanceTimeout() {
        prepareCoordinatorForCloseTest(
            useGroupManagement = true,
            autoCommit = true,
            groupInstanceId = null,
            shouldPoll = false,
        ).use { coordinator ->
            val generationId = 42
            val memberId = "consumer-42"
            var pollTimer = time.timer(0L)
            var res: Boolean = coordinator.onJoinPrepare(pollTimer, generationId, memberId)
            assertFalse(res)
            pollTimer = time.timer(100L)
            time.sleep(rebalanceTimeoutMs.toLong())
            client.respond(offsetCommitResponse(mapOf(t1p to Errors.UNKNOWN_TOPIC_OR_PARTITION)))
            res = coordinator.onJoinPrepare(pollTimer, generationId, memberId)
            assertTrue(res)
            assertFalse(client.hasPendingResponses())
            assertFalse(client.hasInFlightRequests())
            assertFalse(coordinator.coordinatorUnknown())
        }
    }

    @Test
    fun testJoinPrepareWithDisableAutoCommit() {
        prepareCoordinatorForCloseTest(
            useGroupManagement = true,
            autoCommit = false,
            groupInstanceId = "group-id",
            shouldPoll = true,
        ).use { coordinator ->
            coordinator.ensureActiveGroup()
            prepareOffsetCommitRequest(mapOf(t1p to 100L), Errors.NONE)
            val generationId = 42
            val memberId = "consumer-42"
            val res = coordinator.onJoinPrepare(time.timer(0L), generationId, memberId)
            assertTrue(res)
            assertTrue(client.hasPendingResponses())
            assertFalse(client.hasInFlightRequests())
            assertFalse(coordinator.coordinatorUnknown())
        }
    }

    @Test
    fun testJoinPrepareAndCommitCompleted() {
        prepareCoordinatorForCloseTest(
            useGroupManagement = true,
            autoCommit = true,
            groupInstanceId = "group-id",
            shouldPoll = true,
        ).use { coordinator ->
            coordinator.ensureActiveGroup()
            prepareOffsetCommitRequest(mapOf(t1p to 100L), Errors.NONE)
            val generationId = 42
            val memberId = "consumer-42"
            val res = coordinator.onJoinPrepare(time.timer(0L), generationId, memberId)
            coordinator.invokeCompletedOffsetCommitCallbacks()
            assertTrue(res)
            assertFalse(client.hasPendingResponses())
            assertFalse(client.hasInFlightRequests())
            assertFalse(coordinator.coordinatorUnknown())
        }
    }

    @Test
    fun testJoinPrepareAndCommitWithCoordinatorNotAvailable() {
        prepareCoordinatorForCloseTest(
            useGroupManagement = true,
            autoCommit = true,
            groupInstanceId = "group-id",
            shouldPoll = true,
        ).use { coordinator ->
            coordinator.ensureActiveGroup()
            prepareOffsetCommitRequest(mapOf(t1p to 100L), Errors.COORDINATOR_NOT_AVAILABLE)
            val generationId = 42
            val memberId = "consumer-42"
            val res = coordinator.onJoinPrepare(time.timer(0L), generationId, memberId)
            coordinator.invokeCompletedOffsetCommitCallbacks()
            assertFalse(res)
            assertFalse(client.hasPendingResponses())
            assertFalse(client.hasInFlightRequests())
            assertTrue(coordinator.coordinatorUnknown())
        }
    }

    @Test
    fun testJoinPrepareAndCommitWithUnknownMemberId() {
        prepareCoordinatorForCloseTest(
            useGroupManagement = true,
            autoCommit = true,
            groupInstanceId = "group-id",
            shouldPoll = true,
        ).use { coordinator ->
            coordinator.ensureActiveGroup()
            prepareOffsetCommitRequest(mapOf(t1p to 100L), Errors.UNKNOWN_MEMBER_ID)
            val generationId = 42
            val memberId = "consumer-42"
            val res = coordinator.onJoinPrepare(time.timer(0L), generationId, memberId)
            coordinator.invokeCompletedOffsetCommitCallbacks()
            assertTrue(res)
            assertFalse(client.hasPendingResponses())
            assertFalse(client.hasInFlightRequests())
            assertFalse(coordinator.coordinatorUnknown())
        }
    }

    /**
     * Verifies that the consumer re-joins after a metadata change. If JoinGroup fails
     * and metadata reverts to its original value, the consumer should still retry JoinGroup.
     */
    @Test
    fun testRebalanceWithMetadataChange() {
        val consumerId = "leader"
        val topics = listOf(topic1, topic2)
        val partitions = listOf(t1p, t2p)
        subscriptions.subscribe(topics.toSet(), rebalanceListener)
        client.updateMetadata(
            metadataUpdateWith(
                numNodes = 1,
                topicPartitionCounts = mapOf(topic1 to 1, topic2 to 1)
            )
        )
        coordinator.maybeUpdateSubscriptionMetadata()
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE))
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE))
        val initialSubscription = mapOf(consumerId to topics)
        partitionAssignor.prepare(mapOf(consumerId to partitions))
        client.prepareResponse(
            joinGroupLeaderResponse(
                generationId = 1,
                memberId = consumerId,
                subscriptions = initialSubscription,
                error = Errors.NONE,
            )
        )
        client.prepareResponse(syncGroupResponse(partitions, Errors.NONE))
        coordinator.poll(time.timer(Long.MAX_VALUE))

        // rejoin will only be set in the next poll call
        assertFalse(coordinator.rejoinNeededOrPending())
        assertEquals(topics.toSet(), subscriptions.subscription())
        assertEquals(partitions.toSet(), subscriptions.assignedPartitions())
        assertEquals(0, rebalanceListener.revokedCount)
        assertNull(rebalanceListener.revoked)
        assertEquals(1, rebalanceListener.assignedCount)

        // Change metadata to trigger rebalance.
        client.updateMetadata(
            metadataUpdateWith(
                numNodes = 1,
                topicPartitionCounts = mapOf(topic1 to 1),
            )
        )
        coordinator.poll(time.timer(0))

        // Revert metadata to original value. Fail pending JoinGroup. Another
        // JoinGroup should be sent, which will be completed successfully.
        client.updateMetadata(
            metadataUpdateWith(
                numNodes = 1,
                topicPartitionCounts = mapOf(topic1 to 1, topic2 to 1),
            )
        )
        client.respond(
            joinGroupFollowerResponse(
                generationId = 1,
                memberId = consumerId,
                leaderId = "leader",
                error = Errors.NOT_COORDINATOR,
            )
        )
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE))
        coordinator.poll(time.timer(0))
        assertTrue(coordinator.rejoinNeededOrPending())
        client.respond(
            matcher = { request ->
                if (request !is JoinGroupRequest) return@respond false
                else return@respond (consumerId == request.data().memberId)
            },
            response = joinGroupLeaderResponse(
                generationId = 2,
                memberId = consumerId,
                subscriptions = initialSubscription,
                error = Errors.NONE,
            ),
        )
        client.prepareResponse(syncGroupResponse(partitions, Errors.NONE))
        coordinator.poll(time.timer(Long.MAX_VALUE))
        assertFalse(coordinator.rejoinNeededOrPending())
        val revoked = getRevoked(partitions, partitions)
        assertEquals(if (revoked.isEmpty()) 0 else 1, rebalanceListener.revokedCount)
        assertEquals(revoked.ifEmpty { null }, rebalanceListener.revoked)
        // No partitions have been lost since the rebalance failure was not fatal
        assertEquals(0, rebalanceListener.lostCount)
        assertNull(rebalanceListener.lost)
        val added = getAdded(partitions, partitions)
        assertEquals(2, rebalanceListener.assignedCount)
        assertEquals(
            expected = if (added.isEmpty()) emptySet() else partitions.toSet(),
            actual = rebalanceListener.assigned,
        )
        assertEquals(partitions.toSet(), subscriptions.assignedPartitions())
    }

    @Test
    fun testWakeupDuringJoin() {
        val consumerId = "leader"
        val owned = emptyList<TopicPartition>()
        val assigned = listOf(t1p)
        subscriptions.subscribe(setOf(topic1), rebalanceListener)

        // ensure metadata is up-to-date for leader
        client.updateMetadata(metadataResponse)
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE))
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE))
        val memberSubscriptions = mapOf(consumerId to listOf(topic1))
        partitionAssignor.prepare(mapOf(consumerId to assigned))

        // prepare only the first half of the join and then trigger the wakeup
        client.prepareResponse(
            joinGroupLeaderResponse(
                generationId = 1,
                memberId = consumerId,
                subscriptions = memberSubscriptions,
                error = Errors.NONE,
            )
        )
        consumerClient.wakeup()
        try {
            coordinator.poll(time.timer(Long.MAX_VALUE))
        } catch (_: WakeupException) {
            // ignore
        }

        // now complete the second half
        client.prepareResponse(syncGroupResponse(assigned, Errors.NONE))
        coordinator.poll(time.timer(Long.MAX_VALUE))
        assertFalse(coordinator.rejoinNeededOrPending())
        assertEquals(assigned.toSet(), subscriptions.assignedPartitions())
        assertEquals(0, rebalanceListener.revokedCount)
        assertNull(rebalanceListener.revoked)
        assertEquals(1, rebalanceListener.assignedCount)
        assertEquals(getAdded(owned, assigned), rebalanceListener.assigned)
    }

    @Test
    fun testNormalJoinGroupFollower() {
        val subscription = setOf(topic1)
        val owned = emptyList<TopicPartition>()
        val assigned = listOf(t1p)
        subscriptions.subscribe(subscription, rebalanceListener)
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE))
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE))

        // normal join group
        client.prepareResponse(
            joinGroupFollowerResponse(
                generationId = 1,
                memberId = consumerId,
                leaderId = "leader",
                error = Errors.NONE
            )
        )
        client.prepareResponse(
            matcher = { body ->
                val sync = body as SyncGroupRequest
                sync.data().memberId == consumerId
                        && sync.data().generationId == 1
                        && sync.groupAssignments().isEmpty()
            },
            response = syncGroupResponse(assigned, Errors.NONE),
        )
        coordinator.joinGroupIfNeeded(time.timer(Long.MAX_VALUE))
        assertFalse(coordinator.rejoinNeededOrPending())
        assertEquals(assigned.toSet(), subscriptions.assignedPartitions())
        assertEquals(subscription, subscriptions.metadataTopics())
        assertEquals(0, rebalanceListener.revokedCount)
        assertNull(rebalanceListener.revoked)
        assertEquals(1, rebalanceListener.assignedCount)
        assertEquals(getAdded(owned, assigned), rebalanceListener.assigned)
    }

    @Test
    @Throws(Exception::class)
    fun testUpdateLastHeartbeatPollWhenCoordinatorUnknown() {
        // If we are part of an active group and we cannot find the coordinator, we should nevertheless
        // continue to update the last poll time so that we do not expire the consumer
        subscriptions.subscribe(setOf(topic1), rebalanceListener)
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE))
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE))

        // Join the group, but signal a coordinator change after the first heartbeat
        client.prepareResponse(
            joinGroupFollowerResponse(
                generationId = 1,
                memberId = consumerId,
                leaderId = "leader",
                error = Errors.NONE,
            )
        )
        client.prepareResponse(syncGroupResponse(listOf(t1p), Errors.NONE))
        client.prepareResponse(heartbeatResponse(Errors.NOT_COORDINATOR))
        coordinator.poll(time.timer(Long.MAX_VALUE))
        time.sleep(heartbeatIntervalMs.toLong())

        // Await the first heartbeat which forces us to find a new coordinator
        TestUtils.waitForCondition(
            testCondition = { !client.hasPendingResponses() },
            conditionDetails = "Failed to observe expected heartbeat from background thread",
        )
        assertTrue(coordinator.coordinatorUnknown())
        assertFalse(coordinator.poll(time.timer(0)))
        assertEquals(time.milliseconds(), coordinator.heartbeat().lastPollTime)
        time.sleep((rebalanceTimeoutMs - 1).toLong())
        assertFalse(coordinator.heartbeat().pollTimeoutExpired(time.milliseconds()))
    }

    @Test
    fun testPatternJoinGroupFollower() {
        val subscription = mkSet(topic1, topic2)
        val owned = emptyList<TopicPartition>()
        val assigned = listOf(t1p, t2p)
        subscriptions.subscribe(Pattern.compile("test.*"), rebalanceListener)

        // partially update the metadata with one topic first,
        // let the leader to refresh metadata during assignment
        client.updateMetadata(metadataUpdateWith(numNodes = 1, topicPartitionCounts = mapOf(topic1 to 1)))
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE))
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE))

        // normal join group
        client.prepareResponse(joinGroupFollowerResponse(1, consumerId, "leader", Errors.NONE))
        client.prepareResponse(
            matcher = { body ->
                val sync = body as SyncGroupRequest
                sync.data().memberId == consumerId
                        && sync.data().generationId == 1
                        && sync.groupAssignments().isEmpty()
            },
            response = syncGroupResponse(assigned, Errors.NONE),
        )
        // expect client to force updating the metadata, if yes gives it both topics
        client.prepareMetadataUpdate(metadataResponse)
        coordinator.joinGroupIfNeeded(time.timer(Long.MAX_VALUE))
        assertFalse(coordinator.rejoinNeededOrPending())
        assertEquals(assigned.size, subscriptions.numAssignedPartitions())
        assertEquals(subscription, subscriptions.subscription())
        assertEquals(0, rebalanceListener.revokedCount)
        assertNull(rebalanceListener.revoked)
        assertEquals(1, rebalanceListener.assignedCount)
        assertEquals(getAdded(owned, assigned), rebalanceListener.assigned)
    }

    @Test
    fun testLeaveGroupOnClose() {
        subscriptions.subscribe(setOf(topic1), rebalanceListener)
        joinAsFollowerAndReceiveAssignment(coordinator, listOf(t1p))
        val received = AtomicBoolean(false)
        client.prepareResponse(
            matcher = { body ->
                received.set(true)
                val leaveRequest = body as LeaveGroupRequest
                validateLeaveGroup(groupId, consumerId, leaveRequest)
            },
            response = LeaveGroupResponse(LeaveGroupResponseData().setErrorCode(Errors.NONE.code)),
        )
        coordinator.close(time.timer(0))
        assertTrue(received.get())
    }

    @Test
    fun testMaybeLeaveGroup() {
        subscriptions.subscribe(setOf(topic1), rebalanceListener)
        joinAsFollowerAndReceiveAssignment(coordinator, listOf(t1p))
        val received = AtomicBoolean(false)
        client.prepareResponse(
            matcher = { body ->
                received.set(true)
                val leaveRequest = body as LeaveGroupRequest
                validateLeaveGroup(groupId, consumerId, leaveRequest)
            },
            response = LeaveGroupResponse(LeaveGroupResponseData().setErrorCode(Errors.NONE.code)),
        )
        coordinator.maybeLeaveGroup("test maybe leave group")
        assertTrue(received.get())
        val generation = coordinator.generationIfStable
        assertNull(generation)
    }

    private fun validateLeaveGroup(
        groupId: String,
        consumerId: String,
        leaveRequest: LeaveGroupRequest,
    ): Boolean {
        val members = leaveRequest.data().members
        return leaveRequest.data().groupId == groupId
                && members.size == 1
                && members[0].memberId == consumerId
    }

    /**
     * This test checks if a consumer that has a valid member ID but an invalid generation
     * ([org.apache.kafka.clients.consumer.internals.AbstractCoordinator.Generation.NO_GENERATION])
     * can still execute a leave group request. Such a situation may arise when a consumer has
     * initiated a JoinGroup request without a memberId, but is shutdown or restarted before it has
     * a chance to initiate and complete the second request.
     */
    @Test
    fun testPendingMemberShouldLeaveGroup() {
        val consumerId = "consumer-id"
        subscriptions.subscribe(setOf(topic1), rebalanceListener)
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE))
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE))

        // here we return a DEFAULT_GENERATION_ID, but valid member id and leader id.
        client.prepareResponse(
            joinGroupFollowerResponse(
                generationId = -1,
                memberId = consumerId,
                leaderId = "leader-id",
                error = Errors.MEMBER_ID_REQUIRED,
            )
        )

        // execute join group
        coordinator.joinGroupIfNeeded(time.timer(0))
        val received = AtomicBoolean(false)
        client.prepareResponse(
            matcher = { body ->
                received.set(true)
                val leaveRequest = body as LeaveGroupRequest
                validateLeaveGroup(groupId, consumerId, leaveRequest)
            },
            response = LeaveGroupResponse(LeaveGroupResponseData().setErrorCode(Errors.NONE.code)),
        )
        coordinator.maybeLeaveGroup("pending member leaves")
        assertTrue(received.get())
    }

    @Test
    fun testUnexpectedErrorOnSyncGroup() {
        subscriptions.subscribe(setOf(topic1), rebalanceListener)
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE))
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE))

        // join initially, but let coordinator rebalance on sync
        client.prepareResponse(
            joinGroupFollowerResponse(
                generationId = 1,
                memberId = consumerId,
                leaderId = "leader",
                error = Errors.NONE,
            )
        )
        client.prepareResponse(syncGroupResponse(emptyList(), Errors.UNKNOWN_SERVER_ERROR))
        assertFailsWith<KafkaException> {
            coordinator.joinGroupIfNeeded(time.timer(Long.MAX_VALUE))
        }
    }

    @Test
    fun testUnknownMemberIdOnSyncGroup() {
        subscriptions.subscribe(setOf(topic1), rebalanceListener)
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE))
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE))

        // join initially, but let coordinator returns unknown member id
        client.prepareResponse(
            joinGroupFollowerResponse(
                generationId = 1,
                memberId = consumerId,
                leaderId = "leader",
                error = Errors.NONE,
            )
        )
        client.prepareResponse(syncGroupResponse(emptyList(), Errors.UNKNOWN_MEMBER_ID))

        // now we should see a new join with the empty UNKNOWN_MEMBER_ID
        client.prepareResponse(
            matcher = { body ->
                val joinRequest = body as JoinGroupRequest
                joinRequest.data().memberId == JoinGroupRequest.UNKNOWN_MEMBER_ID
            },
            response = joinGroupFollowerResponse(
                generationId = 2,
                memberId = consumerId,
                leaderId = "leader",
                error = Errors.NONE,
            ),
        )
        client.prepareResponse(syncGroupResponse(listOf(t1p), Errors.NONE))
        coordinator.joinGroupIfNeeded(time.timer(Long.MAX_VALUE))
        assertFalse(coordinator.rejoinNeededOrPending())
        assertEquals(setOf(t1p), subscriptions.assignedPartitions())
    }

    @Test
    fun testRebalanceInProgressOnSyncGroup() {
        subscriptions.subscribe(setOf(topic1), rebalanceListener)
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE))
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE))

        // join initially, but let coordinator rebalance on sync
        client.prepareResponse(
            joinGroupFollowerResponse(
                generationId = 1,
                memberId = consumerId,
                leaderId = "leader",
                error = Errors.NONE,
            )
        )
        client.prepareResponse(syncGroupResponse(emptyList(), Errors.REBALANCE_IN_PROGRESS))

        // then let the full join/sync finish successfully
        client.prepareResponse(
            joinGroupFollowerResponse(
                generationId = 2,
                memberId = consumerId,
                leaderId = "leader",
                error = Errors.NONE,
            )
        )
        client.prepareResponse(syncGroupResponse(listOf(t1p), Errors.NONE))
        coordinator.joinGroupIfNeeded(time.timer(Long.MAX_VALUE))
        assertFalse(coordinator.rejoinNeededOrPending())
        assertEquals(setOf(t1p), subscriptions.assignedPartitions())
    }

    @Test
    fun testIllegalGenerationOnSyncGroup() {
        subscriptions.subscribe(setOf(topic1), rebalanceListener)
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE))
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE))

        // join initially, but let coordinator rebalance on sync
        client.prepareResponse(
            joinGroupFollowerResponse(
                generationId = 1,
                memberId = consumerId,
                leaderId = "leader",
                error = Errors.NONE,
            )
        )
        client.prepareResponse(syncGroupResponse(emptyList(), Errors.ILLEGAL_GENERATION))

        // then let the full join/sync finish successfully
        client.prepareResponse(
            matcher = { body ->
                val joinRequest = body as JoinGroupRequest
                joinRequest.data().memberId == consumerId
            },
            response = joinGroupFollowerResponse(
                generationId = 2,
                memberId = consumerId,
                leaderId = "leader",
                error = Errors.NONE,
            ),
        )
        client.prepareResponse(syncGroupResponse(listOf(t1p), Errors.NONE))
        coordinator.joinGroupIfNeeded(time.timer(Long.MAX_VALUE))
        assertFalse(coordinator.rejoinNeededOrPending())
        assertEquals(setOf(t1p), subscriptions.assignedPartitions())
    }

    @Test
    fun testMetadataChangeTriggersRebalance() {
        // ensure metadata is up-to-date for leader
        subscriptions.subscribe(setOf(topic1), rebalanceListener)
        client.updateMetadata(metadataResponse)
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE))
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE))
        val memberSubscriptions = mapOf(consumerId to listOf(topic1))
        partitionAssignor.prepare(mapOf(consumerId to listOf(t1p)))

        // the leader is responsible for picking up metadata changes and forcing a group rebalance
        client.prepareResponse(
            joinGroupLeaderResponse(
                generationId = 1,
                memberId = consumerId,
                subscriptions = memberSubscriptions,
                error = Errors.NONE,
            )
        )
        client.prepareResponse(syncGroupResponse(listOf(t1p), Errors.NONE))
        coordinator.poll(time.timer(Long.MAX_VALUE))
        assertFalse(coordinator.rejoinNeededOrPending())

        // a new partition is added to the topic
        metadata.updateWithCurrentRequestVersion(
            response = metadataUpdateWith(numNodes = 1, topicPartitionCounts = mapOf(topic1 to 2)),
            isPartialUpdate = false,
            nowMs = time.milliseconds(),
        )
        coordinator.maybeUpdateSubscriptionMetadata()

        // we should detect the change and ask for reassignment
        assertTrue(coordinator.rejoinNeededOrPending())
    }

    @Test
    fun testStaticLeaderRejoinsGroupAndCanTriggersRebalance() {
        // ensure metadata is up-to-date for leader
        subscriptions.subscribe(setOf(topic1), rebalanceListener)
        client.updateMetadata(metadataResponse)
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE))
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE))

        // the leader is responsible for picking up metadata changes and forcing a group rebalance.
        // note that `MockPartitionAssignor.prepare` is not called therefore calling `MockPartitionAssignor.assign`
        // will throw a IllegalStateException. this indirectly verifies that `assign` is correctly skipped.
        val memberSubscriptions = mapOf(consumerId to listOf(topic1))
        client.prepareResponse(
            joinGroupLeaderResponse(
                generationId = 1,
                memberId = consumerId,
                subscriptions = memberSubscriptions,
                skipAssignment = true,
                error = Errors.NONE,
                rackId = null,
            )
        )
        client.prepareResponse(syncGroupResponse(listOf(t1p), Errors.NONE))
        coordinator.poll(time.timer(Long.MAX_VALUE))
        assertFalse(coordinator.rejoinNeededOrPending())
        assertEquals(setOf(topic1), coordinator.subscriptionState().metadataTopics())

        // a new partition is added to the topic
        metadata.updateWithCurrentRequestVersion(
            response = metadataUpdateWith(
                numNodes = 1,
                topicPartitionCounts = mapOf(topic1 to 2),
            ),
            isPartialUpdate = false,
            nowMs = time.milliseconds(),
        )
        coordinator.maybeUpdateSubscriptionMetadata()

        // we should detect the change and ask for reassignment
        assertTrue(coordinator.rejoinNeededOrPending())
    }

    @Test
    fun testStaticLeaderRejoinsGroupAndCanDetectMetadataChangesForOtherMembers() {
        // ensure metadata is up-to-date for leader
        subscriptions.subscribe(setOf(topic1), rebalanceListener)
        client.updateMetadata(metadataResponse)
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE))
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE))

        // the leader is responsible for picking up metadata changes and forcing a group rebalance.
        // note that `MockPartitionAssignor.prepare` is not called therefore calling
        // `MockPartitionAssignor.assign` will throw a IllegalStateException. this indirectly
        // verifies that `assign` is correctly skipped.
        val memberSubscriptions = mapOf(
            consumerId to listOf(topic1),
            consumerId2 to listOf(topic2),
        )
        client.prepareResponse(
            joinGroupLeaderResponse(
                generationId = 1,
                memberId = consumerId,
                subscriptions = memberSubscriptions,
                skipAssignment = true,
                error = Errors.NONE,
                rackId = null,
            )
        )
        client.prepareResponse(syncGroupResponse(listOf(t1p), Errors.NONE))
        coordinator.poll(time.timer(Long.MAX_VALUE))
        assertFalse(coordinator.rejoinNeededOrPending())
        assertEquals(mkSet(topic1, topic2), coordinator.subscriptionState().metadataTopics())

        // a new partition is added to the topic2 that only consumerId2 is subscribed to
        metadata.updateWithCurrentRequestVersion(
            response = metadataUpdateWith(
                numNodes = 1,
                topicPartitionCounts = mapOf(topic2 to 2),
            ),
            isPartialUpdate = false,
            nowMs = time.milliseconds(),
        )
        coordinator.maybeUpdateSubscriptionMetadata()

        // we should detect the change and ask for reassignment
        assertTrue(coordinator.rejoinNeededOrPending())
    }

    @Test
    fun testUpdateMetadataDuringRebalance() {
        val topic1 = "topic1"
        val topic2 = "topic2"
        val tp1 = TopicPartition(topic1, 0)
        val tp2 = TopicPartition(topic2, 0)
        val consumerId = "leader"
        val topics = listOf(topic1, topic2)
        subscriptions.subscribe(HashSet(topics), rebalanceListener)

        // we only have metadata for one topic initially
        client.updateMetadata(
            metadataUpdateWith(
                numNodes = 1,
                topicPartitionCounts = mapOf(topic1 to 1),
            )
        )
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE))
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE))

        // prepare initial rebalance
        val memberSubscriptions = mapOf(consumerId to topics)
        partitionAssignor.prepare(mapOf(consumerId to listOf(tp1)))
        client.prepareResponse(
            joinGroupLeaderResponse(
                generationId = 1,
                memberId = consumerId,
                subscriptions = memberSubscriptions,
                error = Errors.NONE,
            )
        )
        client.prepareResponse(
            matcher = { body ->
                val sync = body as SyncGroupRequest
                if (sync.data().memberId == consumerId
                    && sync.data().generationId == 1
                    && sync.groupAssignments().containsKey(consumerId)
                ) {
                    // trigger the metadata update including both topics after the sync group request has been sent
                    val topicPartitionCounts = mapOf(
                        topic1 to 1,
                        topic2 to 1,
                    )
                    client.updateMetadata(
                        metadataUpdateWith(
                            numNodes = 1,
                            topicPartitionCounts = topicPartitionCounts,
                        )
                    )
                    return@prepareResponse true
                }
                false
            },
            response = syncGroupResponse(listOf(tp1), Errors.NONE),
        )
        coordinator.poll(time.timer(Long.MAX_VALUE))

        // the metadata update should trigger a second rebalance
        client.prepareResponse(
            joinGroupLeaderResponse(
                generationId = 2,
                memberId = consumerId,
                subscriptions = memberSubscriptions,
                error = Errors.NONE,
            )
        )
        client.prepareResponse(syncGroupResponse(listOf(tp1, tp2), Errors.NONE))
        coordinator.poll(time.timer(Long.MAX_VALUE))
        assertFalse(coordinator.rejoinNeededOrPending())
        assertEquals(setOf(tp1, tp2), subscriptions.assignedPartitions())
    }

    /**
     * Verifies that subscription change updates SubscriptionState correctly even after JoinGroup failures
     * that don't re-invoke onJoinPrepare.
     */
    @Test
    fun testSubscriptionChangeWithAuthorizationFailure() {
        // Subscribe to two topics of which only one is authorized and verify that metadata failure is propagated.
        subscriptions.subscribe(setOf(topic1, topic2), rebalanceListener)
        client.prepareMetadataUpdate(
            metadataUpdateWith(
                clusterId = "kafka-cluster",
                numNodes = 1,
                topicErrors = mapOf(topic2 to Errors.TOPIC_AUTHORIZATION_FAILED),
                topicPartitionCounts = mapOf(topic1 to 1),
            )
        )
        assertFailsWith<TopicAuthorizationException> {
            coordinator.poll(time.timer(Long.MAX_VALUE))
        }
        client.respond(groupCoordinatorResponse(node, Errors.NONE))
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE))

        // Fail the first JoinGroup request
        client.prepareResponse(
            joinGroupLeaderResponse(
                generationId = 0,
                memberId = consumerId,
                subscriptions = emptyMap(),
                error = Errors.GROUP_AUTHORIZATION_FAILED,
            )
        )
        assertFailsWith<GroupAuthorizationException> {
            coordinator.poll(time.timer(Long.MAX_VALUE))
        }

        // Change subscription to include only the authorized topic. Complete rebalance and check that
        // references to topic2 have been removed from SubscriptionState.
        subscriptions.subscribe(setOf(topic1), rebalanceListener)
        assertEquals(setOf(topic1), subscriptions.metadataTopics())
        client.prepareMetadataUpdate(
            metadataUpdateWith(
                clusterId = "kafka-cluster",
                numNodes = 1,
                topicErrors = emptyMap(),
                topicPartitionCounts = mapOf(topic1 to 1)
            )
        )
        val memberSubscriptions = mapOf(consumerId to listOf(topic1))
        partitionAssignor.prepare(mapOf(consumerId to listOf(t1p)))
        client.prepareResponse(
            joinGroupLeaderResponse(
                generationId = 1,
                memberId = consumerId,
                subscriptions = memberSubscriptions,
                error = Errors.NONE,
            )
        )
        client.prepareResponse(syncGroupResponse(listOf(t1p), Errors.NONE))
        coordinator.poll(time.timer(Long.MAX_VALUE))
        assertEquals(setOf(topic1), subscriptions.subscription())
        assertEquals(setOf(topic1), subscriptions.metadataTopics())
    }

    @Test
    fun testWakeupFromAssignmentCallback() {
        val topic = "topic1"
        val partition = TopicPartition(topic, 0)
        val consumerId = "follower"
        val topics = setOf(topic)
        val rebalanceListener = object : MockRebalanceListener() {
            override fun onPartitionsAssigned(partitions: Collection<TopicPartition>) {
                val raiseWakeup = assignedCount == 0
                super.onPartitionsAssigned(partitions)
                if (raiseWakeup) throw WakeupException()
            }
        }
        subscriptions.subscribe(topics, rebalanceListener)

        // we only have metadata for one topic initially
        client.updateMetadata(
            metadataUpdateWith(
                numNodes = 1,
                topicPartitionCounts = mapOf(topic1 to 1)
            )
        )
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE))
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE))

        // prepare initial rebalance
        partitionAssignor.prepare(mapOf(consumerId to listOf(partition)))
        client.prepareResponse(
            joinGroupFollowerResponse(
                generationId = 1,
                memberId = consumerId,
                leaderId = "leader",
                error = Errors.NONE,
            )
        )
        client.prepareResponse(syncGroupResponse(listOf(partition), Errors.NONE))

        // The first call to poll should raise the exception from the rebalance listener
        try {
            coordinator.poll(time.timer(Long.MAX_VALUE))
            fail("Expected exception thrown from assignment callback")
        } catch (_: WakeupException) {}

        // The second call should retry the assignment callback and succeed
        coordinator.poll(time.timer(Long.MAX_VALUE))
        assertFalse(coordinator.rejoinNeededOrPending())
        assertEquals(0, rebalanceListener.revokedCount)
        assertEquals(2, rebalanceListener.assignedCount)
    }

    @Test
    fun testRebalanceAfterTopicUnavailableWithSubscribe() {
        unavailableTopicTest(false, emptySet())
    }

    @Test
    fun testRebalanceAfterTopicUnavailableWithPatternSubscribe() {
        unavailableTopicTest(true, emptySet())
    }

    @Test
    fun testRebalanceAfterNotMatchingTopicUnavailableWithPatternSubscribe() {
        unavailableTopicTest(true, setOf("notmatching"))
    }

    private fun unavailableTopicTest(
        patternSubscribe: Boolean,
        unavailableTopicsInLastMetadata: Set<String>,
    ) {
        if (patternSubscribe) subscriptions.subscribe(
            pattern = Pattern.compile("test.*"),
            listener = rebalanceListener,
        ) else subscriptions.subscribe(setOf(topic1), rebalanceListener)
        client.prepareMetadataUpdate(
            metadataUpdateWith(
                clusterId = "kafka-cluster",
                numNodes = 1,
                topicErrors = mapOf(topic1 to Errors.UNKNOWN_TOPIC_OR_PARTITION),
                topicPartitionCounts = emptyMap(),
            )
        )
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE))
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE))
        val memberSubscriptions = mapOf(consumerId to listOf(topic1))
        partitionAssignor.prepare(emptyMap())
        client.prepareResponse(
            joinGroupLeaderResponse(
                generationId = 1,
                memberId = consumerId,
                subscriptions = memberSubscriptions,
                error = Errors.NONE,
            )
        )
        client.prepareResponse(syncGroupResponse(emptyList(), Errors.NONE))
        coordinator.poll(time.timer(Long.MAX_VALUE))
        assertFalse(coordinator.rejoinNeededOrPending())
        // callback not triggered since there's nothing to be assigned
        assertEquals(emptySet(), rebalanceListener.assigned)
        assertTrue(
            actual = metadata.updateRequested(),
            message = "Metadata refresh not requested for unavailable partitions"
        )
        val topicErrors = unavailableTopicsInLastMetadata.associateWith {
            Errors.UNKNOWN_TOPIC_OR_PARTITION
        }
        client.prepareMetadataUpdate(
            metadataUpdateWith(
                clusterId = "kafka-cluster",
                numNodes = 1,
                topicErrors = topicErrors,
                topicPartitionCounts = mapOf(topic1 to 1),
            )
        )
        consumerClient.poll(time.timer(0))
        client.prepareResponse(
            joinGroupLeaderResponse(
                generationId = 2,
                memberId = consumerId,
                subscriptions = memberSubscriptions,
                error = Errors.NONE,
            )
        )
        client.prepareResponse(syncGroupResponse(listOf(t1p), Errors.NONE))
        coordinator.poll(time.timer(Long.MAX_VALUE))
        assertFalse(metadata.updateRequested(), "Metadata refresh requested unnecessarily")
        assertFalse(coordinator.rejoinNeededOrPending())
        assertEquals(setOf(t1p), rebalanceListener.assigned)
    }

    @Test
    fun testExcludeInternalTopicsConfigOption() {
        testInternalTopicInclusion(false)
    }

    @Test
    fun testIncludeInternalTopicsConfigOption() {
        testInternalTopicInclusion(true)
    }

    private fun testInternalTopicInclusion(includeInternalTopics: Boolean) {
        metadata = ConsumerMetadata(
            refreshBackoffMs = 0,
            metadataExpireMs = Long.MAX_VALUE,
            includeInternalTopics = includeInternalTopics,
            allowAutoTopicCreation = false,
            subscription = subscriptions,
            logContext = LogContext(),
            clusterResourceListeners = ClusterResourceListeners(),
        )
        client = MockClient(time, metadata)
        buildCoordinator(
            rebalanceConfig = rebalanceConfig,
            metrics = Metrics(),
            assignors = assignors,
            autoCommitEnabled = false,
            subscriptionState = subscriptions,
        ).use { coordinator ->
            subscriptions.subscribe(Pattern.compile(".*"), rebalanceListener)
            val node = Node(id = 0, host = "localhost", port = 9999)
            val partitionMetadata = PartitionMetadata(
                error = Errors.NONE,
                topicPartition = TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 0),
                leaderId = node.id,
                leaderEpoch = null,
                replicaIds = listOf(node.id),
                inSyncReplicaIds = listOf(node.id),
                offlineReplicaIds = listOf(node.id),
            )
            val topicMetadata = MetadataResponse.TopicMetadata(
                error = Errors.NONE,
                topic = Topic.GROUP_METADATA_TOPIC_NAME,
                isInternal = true,
                partitionMetadata = listOf(partitionMetadata),
            )
            client.updateMetadata(
                metadataResponse(
                    brokers = listOf(node),
                    clusterId = "clusterId",
                    controllerId = node.id,
                    topicMetadataList = listOf(topicMetadata),
                )
            )
            coordinator.maybeUpdateSubscriptionMetadata()
            assertEquals(
                expected = includeInternalTopics,
                actual = subscriptions.subscription().contains(Topic.GROUP_METADATA_TOPIC_NAME),
            )
        }
    }

    @Test
    fun testRejoinGroup() {
        val otherTopic = "otherTopic"
        val owned = emptyList<TopicPartition>()
        val assigned = listOf(t1p)
        subscriptions.subscribe(setOf(topic1), rebalanceListener)

        // join the group once
        joinAsFollowerAndReceiveAssignment(coordinator, assigned)
        assertEquals(0, rebalanceListener.revokedCount)
        assertNull(rebalanceListener.revoked)
        assertEquals(1, rebalanceListener.assignedCount)
        assertEquals(getAdded(owned, assigned), rebalanceListener.assigned)

        // and join the group again
        rebalanceListener.revoked = null
        rebalanceListener.assigned = null
        subscriptions.subscribe(setOf(topic1, otherTopic), rebalanceListener)
        client.prepareResponse(
            joinGroupFollowerResponse(
                generationId = 2,
                memberId = consumerId,
                leaderId = "leader",
                error = Errors.NONE,
            )
        )
        client.prepareResponse(syncGroupResponse(assigned, Errors.NONE))
        coordinator.joinGroupIfNeeded(time.timer(Long.MAX_VALUE))
        val revoked = getRevoked(assigned, assigned)
        val added = getAdded(assigned, assigned)
        assertEquals(if (revoked.isEmpty()) 0 else 1, rebalanceListener.revokedCount)
        assertEquals(revoked.ifEmpty { null }, rebalanceListener.revoked)
        assertEquals(2, rebalanceListener.assignedCount)
        assertEquals(added, rebalanceListener.assigned)
    }

    @Test
    fun testDisconnectInJoin() {
        subscriptions.subscribe(setOf(topic1), rebalanceListener)
        val owned = emptyList<TopicPartition>()
        val assigned = listOf(t1p)
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE))
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE))

        // disconnected from original coordinator will cause re-discover and join again
        client.prepareResponse(
            response = joinGroupFollowerResponse(
                generationId = 1,
                memberId = consumerId,
                leaderId = "leader",
                error = Errors.NONE,
            ),
            disconnected = true,
        )
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE))
        client.prepareResponse(joinGroupFollowerResponse(1, consumerId, "leader", Errors.NONE))
        client.prepareResponse(syncGroupResponse(assigned, Errors.NONE))
        coordinator.joinGroupIfNeeded(time.timer(Long.MAX_VALUE))
        assertFalse(coordinator.rejoinNeededOrPending())
        assertEquals(assigned.toSet(), subscriptions.assignedPartitions())
        // nothing to be revoked hence callback not triggered
        assertEquals(0, rebalanceListener.revokedCount)
        assertNull(rebalanceListener.revoked)
        assertEquals(1, rebalanceListener.assignedCount)
        assertEquals(getAdded(owned, assigned), rebalanceListener.assigned)
    }

    @Test
    fun testInvalidSessionTimeout() {
        subscriptions.subscribe(setOf(topic1), rebalanceListener)
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE))
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE))

        // coordinator doesn't like the session timeout
        client.prepareResponse(
            joinGroupFollowerResponse(
                generationId = 0,
                memberId = consumerId,
                leaderId = "",
                error = Errors.INVALID_SESSION_TIMEOUT,
            )
        )
        assertFailsWith<ApiException> { coordinator.joinGroupIfNeeded(time.timer(Long.MAX_VALUE)) }
    }

    @Test
    fun testCommitOffsetOnly() {
        subscriptions.assignFromUser(setOf(t1p))
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE))
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE))
        prepareOffsetCommitRequest(mapOf(t1p to 100L), Errors.NONE)
        val success = AtomicBoolean(false)
        coordinator.commitOffsetsAsync(
            offsets = mapOf(t1p to OffsetAndMetadata(100L)),
            callback = callback(success),
        )
        coordinator.invokeCompletedOffsetCommitCallbacks()
        assertTrue(success.get())
    }

    @Test
    fun testCoordinatorDisconnectAfterNotCoordinatorError() {
        testInFlightRequestsFailedAfterCoordinatorMarkedDead(Errors.NOT_COORDINATOR)
    }

    @Test
    fun testCoordinatorDisconnectAfterCoordinatorNotAvailableError() {
        testInFlightRequestsFailedAfterCoordinatorMarkedDead(Errors.COORDINATOR_NOT_AVAILABLE)
    }

    private fun testInFlightRequestsFailedAfterCoordinatorMarkedDead(error: Errors) {
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE))
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE))

        // Send two async commits and fail the first one with an error.
        // This should cause a coordinator disconnect which will cancel the second request.
        val firstCommitCallback = MockCommitCallback()
        val secondCommitCallback = MockCommitCallback()
        coordinator.commitOffsetsAsync(
            mapOf(t1p to OffsetAndMetadata(100L)),
            firstCommitCallback
        )
        coordinator.commitOffsetsAsync(
            mapOf(t1p to OffsetAndMetadata(100L)),
            secondCommitCallback
        )
        respondToOffsetCommitRequest(mapOf(t1p to 100L), error)
        consumerClient.pollNoWakeup()
        consumerClient.pollNoWakeup() // second poll since coordinator disconnect is async
        coordinator.invokeCompletedOffsetCommitCallbacks()
        assertTrue(coordinator.coordinatorUnknown())
        assertTrue(firstCommitCallback.exception is RetriableCommitFailedException)
        assertTrue(secondCommitCallback.exception is RetriableCommitFailedException)
    }

    @Test
    fun testAutoCommitDynamicAssignment() {
        buildCoordinator(
            rebalanceConfig,
            Metrics(),
            assignors,
            true,
            subscriptions
        ).use { coordinator ->
            subscriptions.subscribe(
                setOf(topic1),
                rebalanceListener
            )
            joinAsFollowerAndReceiveAssignment(
                coordinator,
                listOf(t1p)
            )
            subscriptions.seek(t1p, 100)
            prepareOffsetCommitRequest(
                mapOf(t1p to 100L),
                Errors.NONE
            )
            time.sleep(autoCommitIntervalMs.toLong())
            coordinator.poll(time.timer(Long.MAX_VALUE))
            assertFalse(client.hasPendingResponses())
        }
    }

    @Test
    fun testAutoCommitRetryBackoff() {
        buildCoordinator(
            rebalanceConfig,
            Metrics(),
            assignors,
            true,
            subscriptions
        ).use { coordinator ->
            subscriptions.subscribe(
                setOf(topic1),
                rebalanceListener
            )
            joinAsFollowerAndReceiveAssignment(
                coordinator,
                listOf(t1p)
            )
            subscriptions.seek(t1p, 100)
            time.sleep(autoCommitIntervalMs.toLong())

            // Send an offset commit, but let it fail with a retriable error
            prepareOffsetCommitRequest(mapOf(t1p to 100L), Errors.NOT_COORDINATOR)
            coordinator.poll(time.timer(Long.MAX_VALUE))
            assertTrue(coordinator.coordinatorUnknown())

            // After the disconnect, we should rediscover the coordinator
            client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE))
            coordinator.poll(time.timer(Long.MAX_VALUE))
            subscriptions.seek(t1p, 200)

            // Until the retry backoff has expired, we should not retry the offset commit
            time.sleep(retryBackoffMs / 2)
            coordinator.poll(time.timer(Long.MAX_VALUE))
            assertEquals(0, client.inFlightRequestCount())

            // Once the backoff expires, we should retry
            time.sleep(retryBackoffMs / 2)
            coordinator.poll(time.timer(Long.MAX_VALUE))
            assertEquals(1, client.inFlightRequestCount())
            respondToOffsetCommitRequest(mapOf(t1p to 200L), Errors.NONE)
        }
    }

    @Test
    fun testAutoCommitAwaitsInterval() {
        buildCoordinator(
            rebalanceConfig,
            Metrics(),
            assignors,
            true,
            subscriptions
        ).use { coordinator ->
            subscriptions.subscribe(
                setOf(topic1),
                rebalanceListener
            )
            joinAsFollowerAndReceiveAssignment(
                coordinator,
                listOf(t1p)
            )
            subscriptions.seek(t1p, 100)
            time.sleep(autoCommitIntervalMs.toLong())

            // Send the offset commit request, but do not respond
            coordinator.poll(time.timer(Long.MAX_VALUE))
            assertEquals(1, client.inFlightRequestCount())
            time.sleep((autoCommitIntervalMs / 2).toLong())

            // Ensure that no additional offset commit is sent
            coordinator.poll(time.timer(Long.MAX_VALUE))
            assertEquals(1, client.inFlightRequestCount())
            respondToOffsetCommitRequest(mapOf(t1p to 100L), Errors.NONE)
            coordinator.poll(time.timer(Long.MAX_VALUE))
            assertEquals(0, client.inFlightRequestCount())
            subscriptions.seek(t1p, 200)

            // If we poll again before the auto-commit interval, there should be no new sends
            coordinator.poll(time.timer(Long.MAX_VALUE))
            assertEquals(0, client.inFlightRequestCount())

            // After the remainder of the interval passes, we send a new offset commit
            time.sleep((autoCommitIntervalMs / 2).toLong())
            coordinator.poll(time.timer(Long.MAX_VALUE))
            assertEquals(1, client.inFlightRequestCount())
            respondToOffsetCommitRequest(mapOf(t1p to 200L), Errors.NONE)
        }
    }

    @Test
    fun testAutoCommitDynamicAssignmentRebalance() {
        buildCoordinator(
            rebalanceConfig = rebalanceConfig,
            metrics = Metrics(),
            assignors = assignors,
            autoCommitEnabled = true,
            subscriptionState = subscriptions,
        ).use { coordinator ->
            subscriptions.subscribe(setOf(topic1), rebalanceListener)
            client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE))
            coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE))

            // haven't joined, so should not cause a commit
            time.sleep(autoCommitIntervalMs.toLong())
            consumerClient.poll(time.timer(0))
            client.prepareResponse(
                joinGroupFollowerResponse(
                    generationId = 1,
                    memberId = consumerId,
                    leaderId = "leader",
                    error = Errors.NONE,
                )
            )
            client.prepareResponse(
                syncGroupResponse(
                    listOf(
                        t1p
                    ), Errors.NONE
                )
            )
            coordinator.joinGroupIfNeeded(time.timer(Long.MAX_VALUE))
            subscriptions.seek(t1p, 100)
            prepareOffsetCommitRequest(mapOf(t1p to 100L), Errors.NONE)
            time.sleep(autoCommitIntervalMs.toLong())
            coordinator.poll(time.timer(Long.MAX_VALUE))
            assertFalse(client.hasPendingResponses())
        }
    }

    @Test
    fun testAutoCommitManualAssignment() {
        buildCoordinator(
            rebalanceConfig,
            Metrics(),
            assignors,
            true,
            subscriptions
        ).use { coordinator ->
            subscriptions.assignFromUser(
                setOf(
                    t1p
                )
            )
            subscriptions.seek(t1p, 100)
            client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE))
            coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE))
            prepareOffsetCommitRequest(mapOf(t1p to 100L), Errors.NONE)
            time.sleep(autoCommitIntervalMs.toLong())
            coordinator.poll(time.timer(Long.MAX_VALUE))
            assertFalse(client.hasPendingResponses())
        }
    }

    @Test
    fun testAutoCommitManualAssignmentCoordinatorUnknown() {
        buildCoordinator(
            rebalanceConfig,
            Metrics(),
            assignors,
            true,
            subscriptions
        ).use { coordinator ->
            subscriptions.assignFromUser(setOf(t1p))
            subscriptions.seek(t1p, 100)

            // no commit initially since coordinator is unknown
            consumerClient.poll(time.timer(0))
            time.sleep(autoCommitIntervalMs.toLong())
            consumerClient.poll(time.timer(0))

            // now find the coordinator
            client.prepareResponse(
                groupCoordinatorResponse(
                    node,
                    Errors.NONE
                )
            )
            coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE))

            // sleep only for the retry backoff
            time.sleep(retryBackoffMs)
            prepareOffsetCommitRequest(mapOf(t1p to 100L), Errors.NONE)
            coordinator.poll(time.timer(Long.MAX_VALUE))
            assertFalse(client.hasPendingResponses())
        }
    }

    @Test
    fun testCommitOffsetMetadata() {
        subscriptions.assignFromUser(setOf(t1p))
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE))
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE))
        prepareOffsetCommitRequest(mapOf(t1p to 100L), Errors.NONE)
        val success = AtomicBoolean(false)
        val offsets = mapOf(t1p to OffsetAndMetadata(offset = 100L, metadata = "hello"))
        coordinator.commitOffsetsAsync(offsets, callback(offsets, success))
        coordinator.invokeCompletedOffsetCommitCallbacks()
        assertTrue(success.get())
    }

    @Test
    fun testCommitOffsetAsyncWithDefaultCallback() {
        val invokedBeforeTest = mockOffsetCommitCallback.invoked
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE))
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE))
        prepareOffsetCommitRequest(mapOf(t1p to 100L), Errors.NONE)
        coordinator.commitOffsetsAsync(
            mapOf(t1p to OffsetAndMetadata(100L)),
            mockOffsetCommitCallback
        )
        coordinator.invokeCompletedOffsetCommitCallbacks()
        assertEquals(invokedBeforeTest + 1, mockOffsetCommitCallback.invoked)
        assertNull(mockOffsetCommitCallback.exception)
    }

    @Test
    fun testCommitAfterLeaveGroup() {
        // enable auto-assignment
        subscriptions.subscribe(setOf(topic1), rebalanceListener)
        joinAsFollowerAndReceiveAssignment(coordinator, listOf(t1p))

        // now switch to manual assignment
        client.prepareResponse(
            response = LeaveGroupResponse(LeaveGroupResponseData().setErrorCode(Errors.NONE.code)),
        )
        subscriptions.unsubscribe()
        coordinator.maybeLeaveGroup("test commit after leave")
        subscriptions.assignFromUser(setOf(t1p))

        // the client should not reuse generation/memberId from auto-subscribed generation
        client.prepareResponse(
            matcher = { body ->
                val commitRequest = body as OffsetCommitRequest
                commitRequest.data().memberId == OffsetCommitRequest.DEFAULT_MEMBER_ID
                        && commitRequest.data().generationId == OffsetCommitRequest.DEFAULT_GENERATION_ID
            },
            response = offsetCommitResponse(mapOf(t1p to Errors.NONE)),
        )
        val success = AtomicBoolean(false)
        coordinator.commitOffsetsAsync(
            mapOf(t1p to OffsetAndMetadata(100L)),
            callback(success)
        )
        coordinator.invokeCompletedOffsetCommitCallbacks()
        assertTrue(success.get())
    }

    @Test
    fun testCommitOffsetAsyncFailedWithDefaultCallback() {
        val invokedBeforeTest = mockOffsetCommitCallback.invoked
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE))
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE))
        prepareOffsetCommitRequest(
            expectedOffsets = mapOf(t1p to 100L),
            error = Errors.COORDINATOR_NOT_AVAILABLE,
        )
        coordinator.commitOffsetsAsync(
            offsets = mapOf(t1p to OffsetAndMetadata(100L)),
            callback = mockOffsetCommitCallback,
        )
        coordinator.invokeCompletedOffsetCommitCallbacks()
        assertEquals(invokedBeforeTest + 1, mockOffsetCommitCallback.invoked)
        assertTrue(mockOffsetCommitCallback.exception is RetriableCommitFailedException)
    }

    @Test
    fun testCommitOffsetAsyncCoordinatorNotAvailable() {
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE))
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE))

        // async commit with coordinator not available
        val cb = MockCommitCallback()
        prepareOffsetCommitRequest(
            mapOf(t1p to 100L),
            Errors.COORDINATOR_NOT_AVAILABLE
        )
        coordinator.commitOffsetsAsync(mapOf(t1p to OffsetAndMetadata(100L)), cb)
        coordinator.invokeCompletedOffsetCommitCallbacks()
        assertTrue(coordinator.coordinatorUnknown())
        assertEquals(1, cb.invoked)
        assertTrue(cb.exception is RetriableCommitFailedException)
    }

    @Test
    fun testCommitOffsetAsyncNotCoordinator() {
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE))
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE))

        // async commit with not coordinator
        val cb = MockCommitCallback()
        prepareOffsetCommitRequest(mapOf(t1p to 100L), Errors.NOT_COORDINATOR)
        coordinator.commitOffsetsAsync(mapOf(t1p to OffsetAndMetadata(100L)), cb)
        coordinator.invokeCompletedOffsetCommitCallbacks()
        assertTrue(coordinator.coordinatorUnknown())
        assertEquals(1, cb.invoked)
        assertTrue(cb.exception is RetriableCommitFailedException)
    }

    @Test
    fun testCommitOffsetAsyncDisconnected() {
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE))
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE))

        // async commit with coordinator disconnected
        val cb = MockCommitCallback()
        prepareOffsetCommitRequestDisconnect(mapOf(t1p to 100L))
        coordinator.commitOffsetsAsync(mapOf(t1p to OffsetAndMetadata(100L)), cb)
        coordinator.invokeCompletedOffsetCommitCallbacks()
        assertTrue(coordinator.coordinatorUnknown())
        assertEquals(1, cb.invoked)
        assertTrue(cb.exception is RetriableCommitFailedException)
    }

    @Test
    fun testCommitOffsetSyncNotCoordinator() {
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE))
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE))

        // sync commit with coordinator disconnected (should connect, get metadata, and then submit the commit request)
        prepareOffsetCommitRequest(mapOf(t1p to 100L), Errors.NOT_COORDINATOR)
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE))
        prepareOffsetCommitRequest(mapOf(t1p to 100L), Errors.NONE)
        coordinator.commitOffsetsSync(
            offsets = mapOf(t1p to OffsetAndMetadata(100L)),
            timer = time.timer(Long.MAX_VALUE),
        )
    }

    @Test
    fun testCommitOffsetSyncCoordinatorNotAvailable() {
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE))
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE))

        // sync commit with coordinator disconnected (should connect, get metadata, and then submit the commit request)
        prepareOffsetCommitRequest(
            mapOf(t1p to 100L),
            Errors.COORDINATOR_NOT_AVAILABLE
        )
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE))
        prepareOffsetCommitRequest(mapOf(t1p to 100L), Errors.NONE)
        coordinator.commitOffsetsSync(
            mapOf(t1p to OffsetAndMetadata(100L)), time.timer(Long.MAX_VALUE)
        )
    }

    @Test
    fun testCommitOffsetSyncCoordinatorDisconnected() {
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE))
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE))

        // sync commit with coordinator disconnected (should connect, get metadata, and then submit the commit request)
        prepareOffsetCommitRequestDisconnect(mapOf(t1p to 100L))
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE))
        prepareOffsetCommitRequest(mapOf(t1p to 100L), Errors.NONE)
        coordinator.commitOffsetsSync(
            offsets = mapOf(t1p to OffsetAndMetadata(100L)),
            timer = time.timer(Long.MAX_VALUE)
        )
    }

    @Test
    @Throws(Exception::class)
    fun testAsyncCommitCallbacksInvokedPriorToSyncCommitCompletion() {
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE))
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE))
        val committedOffsets = Collections.synchronizedList(ArrayList<OffsetAndMetadata>())
        val firstOffset = OffsetAndMetadata(0L)
        val secondOffset = OffsetAndMetadata(1L)
        coordinator.commitOffsetsAsync(
            mapOf(t1p to firstOffset)
        ) { _, _ -> committedOffsets.add(firstOffset) }

        // Do a synchronous commit in the background so that we can send both responses at the same time
        val thread: Thread = object : Thread() {
            override fun run() {
                coordinator.commitOffsetsSync(
                    mapOf(t1p to secondOffset),
                    time.timer(10000)
                )
                committedOffsets.add(secondOffset)
            }
        }
        thread.start()
        client.waitForRequests(2, 5000)
        respondToOffsetCommitRequest(mapOf(t1p to firstOffset.offset), Errors.NONE)
        respondToOffsetCommitRequest(mapOf(t1p to secondOffset.offset), Errors.NONE)
        thread.join()
        assertEquals(listOf(firstOffset, secondOffset), committedOffsets)
    }

    @Test
    fun testRetryCommitUnknownTopicOrPartition() {
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE))
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE))
        client.prepareResponse(
            offsetCommitResponse(mapOf(t1p to Errors.UNKNOWN_TOPIC_OR_PARTITION))
        )
        client.prepareResponse(offsetCommitResponse(mapOf(t1p to Errors.NONE)))
        assertTrue(
            coordinator.commitOffsetsSync(
                offsets = mapOf(t1p to OffsetAndMetadata(offset = 100L, metadata = "metadata")),
                timer = time.timer(10000),
            )
        )
    }

    @Test
    fun testCommitOffsetMetadataTooLarge() {
        // since offset metadata is provided by the user, we have to propagate the exception so they can handle it
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE))
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE))
        prepareOffsetCommitRequest(
            mapOf(t1p to 100L),
            Errors.OFFSET_METADATA_TOO_LARGE
        )
        assertFailsWith<OffsetMetadataTooLarge> {
            coordinator.commitOffsetsSync(
                offsets = mapOf(t1p to OffsetAndMetadata(offset = 100L, metadata = "metadata")),
                timer = time.timer(Long.MAX_VALUE),
            )
        }
    }

    @Test
    fun testCommitOffsetIllegalGeneration() {
        // we cannot retry if a rebalance occurs before the commit completed
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE))
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE))
        prepareOffsetCommitRequest(mapOf(t1p to 100L), Errors.ILLEGAL_GENERATION)

        assertFailsWith<CommitFailedException> {
            coordinator.commitOffsetsSync(
                offsets = mapOf(t1p to OffsetAndMetadata(offset = 100L, metadata = "metadata")),
                timer = time.timer(Long.MAX_VALUE),
            )
        }
    }

    @Test
    fun testCommitOffsetUnknownMemberId() {
        // we cannot retry if a rebalance occurs before the commit completed
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE))
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE))
        prepareOffsetCommitRequest(mapOf(t1p to 100L), Errors.UNKNOWN_MEMBER_ID)

        assertFailsWith<CommitFailedException> {
            coordinator.commitOffsetsSync(
                offsets = mapOf(t1p to OffsetAndMetadata(offset = 100L, metadata = "metadata")),
                timer = time.timer(Long.MAX_VALUE),
            )
        }
    }

    @Test
    fun testCommitOffsetIllegalGenerationWithNewGeneration() {
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE))
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE))
        val currGen = AbstractCoordinator.Generation(
            generationId = 1,
            memberId = "memberId",
            protocolName = null,
        )
        coordinator.setNewGeneration(currGen)
        prepareOffsetCommitRequest(mapOf(t1p to 100L), Errors.ILLEGAL_GENERATION)
        val future = coordinator.sendOffsetCommitRequest(
            mapOf(t1p to OffsetAndMetadata(offset = 100L, metadata = "metadata")),
        )

        // change the generation
        val newGen = AbstractCoordinator.Generation(
            generationId = 2,
            memberId = "memberId-new",
            protocolName = null
        )
        coordinator.setNewGeneration(newGen)
        coordinator.setNewState(AbstractCoordinator.MemberState.PREPARING_REBALANCE)
        assertTrue(consumerClient.poll(future, time.timer(30000)))
        assertTrue(future.exception().javaClass.isInstance(Errors.REBALANCE_IN_PROGRESS.exception))

        // the generation should not be reset
        assertEquals(newGen, coordinator.generation())
    }

    @Test
    fun testCommitOffsetIllegalGenerationShouldResetGenerationId() {
        subscriptions.subscribe(setOf(topic1), rebalanceListener)
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE))
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE))
        client.prepareResponse(joinGroupFollowerResponse(1, consumerId, "leader", Errors.NONE))
        client.prepareResponse(syncGroupResponse(emptyList(), Errors.NONE))
        coordinator.joinGroupIfNeeded(time.timer(Long.MAX_VALUE))
        prepareOffsetCommitRequest(mapOf(t1p to 100L), Errors.ILLEGAL_GENERATION)
        val future = coordinator.sendOffsetCommitRequest(
            mapOf(t1p to OffsetAndMetadata(offset = 100L, metadata = "metadata"))
        )
        assertTrue(consumerClient.poll(future, time.timer(30000)))
        assertEquals(
            AbstractCoordinator.Generation.NO_GENERATION.generationId,
            coordinator.generation().generationId
        )
        assertEquals(
            AbstractCoordinator.Generation.NO_GENERATION.protocolName,
            coordinator.generation().protocolName
        )
        // member ID should not be reset
        assertEquals(consumerId, coordinator.generation().memberId)
    }

    @Test
    fun testCommitOffsetIllegalGenerationWithResetGeneration() {
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE))
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE))
        val currGen = AbstractCoordinator.Generation(
            generationId = 1,
            memberId = "memberId",
            protocolName = null,
        )
        coordinator.setNewGeneration(currGen)
        prepareOffsetCommitRequest(mapOf(t1p to 100L), Errors.ILLEGAL_GENERATION)
        val future = coordinator.sendOffsetCommitRequest(
            mapOf(t1p to OffsetAndMetadata(offset = 100L, metadata = "metadata"))
        )

        // reset the generation
        coordinator.setNewGeneration(AbstractCoordinator.Generation.NO_GENERATION)
        assertTrue(consumerClient.poll(future, time.timer(30000)))
        assertTrue(future.exception().javaClass.isInstance(CommitFailedException()))

        // the generation should not be reset
        assertEquals(
            expected = AbstractCoordinator.Generation.NO_GENERATION,
            actual = coordinator.generation(),
        )
    }

    @Test
    fun testCommitOffsetUnknownMemberWithNewGeneration() {
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE))
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE))
        val currGen = AbstractCoordinator.Generation(
            generationId = 1,
            memberId = "memberId",
            protocolName = null,
        )
        coordinator.setNewGeneration(currGen)
        prepareOffsetCommitRequest(mapOf(t1p to 100L), Errors.UNKNOWN_MEMBER_ID)
        val future = coordinator.sendOffsetCommitRequest(
            mapOf(t1p to OffsetAndMetadata(offset = 100L, metadata = "metadata"))
        )

        // change the generation
        val newGen = AbstractCoordinator.Generation(
            generationId = 2,
            memberId = "memberId-new",
            protocolName = null,
        )
        coordinator.setNewGeneration(newGen)
        coordinator.setNewState(AbstractCoordinator.MemberState.PREPARING_REBALANCE)
        assertTrue(consumerClient.poll(future, time.timer(30000)))
        assertTrue(future.exception().javaClass.isInstance(Errors.REBALANCE_IN_PROGRESS.exception))

        // the generation should not be reset
        assertEquals(newGen, coordinator.generation())
    }

    @Test
    fun testCommitOffsetUnknownMemberWithResetGeneration() {
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE))
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE))
        val currGen = AbstractCoordinator.Generation(
            generationId = 1,
            memberId = "memberId",
            protocolName = null,
        )
        coordinator.setNewGeneration(currGen)
        prepareOffsetCommitRequest(mapOf(t1p to 100L), Errors.UNKNOWN_MEMBER_ID)
        val future = coordinator.sendOffsetCommitRequest(
            mapOf(t1p to OffsetAndMetadata(offset = 100L, metadata = "metadata"))
        )

        // reset the generation
        coordinator.setNewGeneration(AbstractCoordinator.Generation.NO_GENERATION)
        assertTrue(consumerClient.poll(future, time.timer(30000)))
        assertTrue(future.exception().javaClass.isInstance(CommitFailedException()))

        // the generation should be reset
        assertEquals(
            expected = AbstractCoordinator.Generation.NO_GENERATION,
            actual = coordinator.generation(),
        )
    }

    @Test
    fun testCommitOffsetUnknownMemberShouldResetToNoGeneration() {
        subscriptions.subscribe(setOf(topic1), rebalanceListener)
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE))
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE))
        client.prepareResponse(joinGroupFollowerResponse(1, consumerId, "leader", Errors.NONE))
        client.prepareResponse(syncGroupResponse(emptyList(), Errors.NONE))
        coordinator.joinGroupIfNeeded(time.timer(Long.MAX_VALUE))
        prepareOffsetCommitRequest(mapOf(t1p to 100L), Errors.UNKNOWN_MEMBER_ID)
        val future = coordinator.sendOffsetCommitRequest(
            mapOf(t1p to OffsetAndMetadata(offset = 100L, metadata = "metadata"))
        )
        assertTrue(consumerClient.poll(future, time.timer(30000)))
        assertEquals(
            AbstractCoordinator.Generation.NO_GENERATION,
            coordinator.generation()
        )
    }

    @Test
    fun testCommitOffsetFencedInstanceWithRebalancingGeneration() {
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE))
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE))
        val currGen = AbstractCoordinator.Generation(
            generationId = 1,
            memberId = "memberId",
            protocolName = null,
        )
        coordinator.setNewGeneration(currGen)
        coordinator.setNewState(AbstractCoordinator.MemberState.PREPARING_REBALANCE)
        prepareOffsetCommitRequest(mapOf(t1p to 100L), Errors.FENCED_INSTANCE_ID)
        val future = coordinator.sendOffsetCommitRequest(
            mapOf(t1p to OffsetAndMetadata(offset = 100L, metadata = "metadata"))
        )

        // change the generation
        val newGen = AbstractCoordinator.Generation(
            generationId = 2,
            memberId = "memberId-new",
            protocolName = null,
        )
        coordinator.setNewGeneration(newGen)
        assertTrue(consumerClient.poll(future, time.timer(30000)))
        assertTrue(future.exception().javaClass.isInstance(Errors.REBALANCE_IN_PROGRESS.exception))

        // the generation should not be reset
        assertEquals(newGen, coordinator.generation())
    }

    @Test
    fun testCommitOffsetFencedInstanceWithNewGeneration() {
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE))
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE))
        val currGen = AbstractCoordinator.Generation(
            1,
            "memberId",
            null
        )
        coordinator.setNewGeneration(currGen)
        prepareOffsetCommitRequest(mapOf(t1p to 100L), Errors.FENCED_INSTANCE_ID)
        val future = coordinator.sendOffsetCommitRequest(
            mapOf(t1p to OffsetAndMetadata(offset = 100L, metadata = "metadata"))
        )

        // change the generation
        val newGen = AbstractCoordinator.Generation(
            2,
            "memberId-new",
            null
        )
        coordinator.setNewGeneration(newGen)
        assertTrue(consumerClient.poll(future, time.timer(30000)))
        assertTrue(future.exception().javaClass.isInstance(CommitFailedException()))

        // the generation should not be reset
        assertEquals(newGen, coordinator.generation())
    }

    @Test
    fun testCommitOffsetShouldNotSetInstanceIdIfMemberIdIsUnknown() {
        rebalanceConfig = buildRebalanceConfig(groupInstanceId)
        val coordinator = buildCoordinator(
            rebalanceConfig,
            Metrics(),
            assignors,
            false,
            subscriptions
        )

        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE))
        coordinator.ensureCoordinatorReady(time.timer(5000))

        client.prepareResponse(
            matcher = { body ->
                val data = (body as OffsetCommitRequest).data()
                data.groupInstanceId == null && data.memberId.isEmpty()
            },
            response = offsetCommitResponse(emptyMap()),
        )

        val future = coordinator.sendOffsetCommitRequest(
            mapOf(t1p to OffsetAndMetadata(offset = 100L, metadata = "metadata"))
        )

        assertTrue(consumerClient.poll(future, time.timer(5000)))
        assertFalse(future.failed())
    }

    @Test
    fun testCommitOffsetRebalanceInProgress() {
        // we cannot retry if a rebalance occurs before the commit completed
        val consumerId = "leader"
        subscriptions.subscribe(setOf(topic1), rebalanceListener)

        // ensure metadata is up-to-date for leader
        client.updateMetadata(metadataResponse)
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE))
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE))

        // normal join group
        val memberSubscriptions = mapOf(consumerId to listOf(topic1))
        partitionAssignor.prepare(mapOf(consumerId to listOf(t1p)))
        coordinator.ensureActiveGroup(time.timer(0L))
        assertTrue(coordinator.rejoinNeededOrPending())
        assertNull(coordinator.generationIfStable)

        // when the state is REBALANCING, we would not even send out the request but fail immediately
        assertFailsWith<RebalanceInProgressException> {
            coordinator.commitOffsetsSync(
                offsets = mapOf(t1p to OffsetAndMetadata(offset = 100L, metadata = "metadata")),
                timer = time.timer(Long.MAX_VALUE),
            )
        }
        val coordinatorNode = Node(Int.MAX_VALUE - node.id, node.host(), node.port())
        client.respondFrom(
            joinGroupLeaderResponse(
                1,
                consumerId,
                memberSubscriptions,
                Errors.NONE
            ), coordinatorNode
        )
        client.prepareResponse(
            matcher = { body ->
                val sync = body as SyncGroupRequest
                sync.data().memberId.equals(consumerId)
                        && sync.data().generationId == 1
                        && sync.groupAssignments().containsKey(consumerId)
            },
            response = syncGroupResponse(listOf(t1p), Errors.NONE),
        )
        coordinator.poll(time.timer(Long.MAX_VALUE))
        val expectedGeneration = AbstractCoordinator.Generation(
            generationId = 1,
            memberId = consumerId,
            protocolName = partitionAssignor.name(),
        )
        assertFalse(coordinator.rejoinNeededOrPending())
        assertEquals(expectedGeneration, coordinator.generationIfStable)
        prepareOffsetCommitRequest(mapOf(t1p to 100L), Errors.REBALANCE_IN_PROGRESS)

        assertFailsWith<RebalanceInProgressException> {
            coordinator.commitOffsetsSync(
                offsets = mapOf(t1p to OffsetAndMetadata(offset = 100L, metadata = "metadata")),
                timer = time.timer(Long.MAX_VALUE),
            )
        }
        assertTrue(coordinator.rejoinNeededOrPending())
        assertEquals(expectedGeneration, coordinator.generationIfStable)
    }

    @Test
    fun testCommitOffsetSyncCallbackWithNonRetriableException() {
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE))
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE))

        // sync commit with invalid partitions should throw if we have no callback
        prepareOffsetCommitRequest(mapOf(t1p to 100L), Errors.UNKNOWN_SERVER_ERROR)

        assertFailsWith<KafkaException> {
            coordinator.commitOffsetsSync(
                offsets = mapOf(t1p to OffsetAndMetadata(100L)),
                timer = time.timer(Long.MAX_VALUE),
            )
        }
    }

    @Test
    fun testCommitOffsetSyncWithoutFutureGetsCompleted() {
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE))
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE))
        assertFalse(
            coordinator.commitOffsetsSync(
                offsets = mapOf(t1p to OffsetAndMetadata(100L)),
                timer = time.timer(0),
            )
        )
    }

    @Test
    fun testRefreshOffset() {
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE))
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE))
        subscriptions.assignFromUser(setOf(t1p))
        client.prepareResponse(offsetFetchResponse(t1p, Errors.NONE, "", 100L))
        coordinator.refreshCommittedOffsetsIfNeeded(time.timer(Long.MAX_VALUE))
        assertEquals(emptySet<Any>(), subscriptions.initializingPartitions())
        assertTrue(subscriptions.hasAllFetchPositions())
        assertEquals(100L, subscriptions.position(t1p)!!.offset)
    }

    @Test
    fun testRefreshOffsetWithValidation() {
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE))
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE))
        subscriptions.assignFromUser(setOf(t1p))

        // Initial leader epoch of 4
        val metadataResponse = metadataUpdateWith(
            clusterId = "kafka-cluster",
            numNodes = 1,
            topicErrors = emptyMap(),
            topicPartitionCounts = mapOf(topic1 to 1),
            epochSupplier = { 4 },
        )
        client.updateMetadata(metadataResponse)

        // Load offsets from previous epoch
        client.prepareResponse(
            response = offsetFetchResponse(
                tp = t1p,
                partitionLevelError = Errors.NONE,
                metadata = "",
                offset = 100L,
                epoch = 3,
            ),
        )
        coordinator.refreshCommittedOffsetsIfNeeded(time.timer(Long.MAX_VALUE))

        // Offset gets loaded, but requires validation
        assertEquals(emptySet<Any>(), subscriptions.initializingPartitions())
        assertFalse(subscriptions.hasAllFetchPositions())
        assertTrue(subscriptions.awaitingValidation(t1p))
        assertEquals(subscriptions.position(t1p)!!.offset, 100L)
        assertNull(subscriptions.validPosition(t1p))
    }

    @Test
    fun testFetchCommittedOffsets() {
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE))
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE))
        val offset = 500L
        val metadata = "blahblah"
        val leaderEpoch = 15
        val data = OffsetFetchResponse.PartitionData(
            offset = offset,
            leaderEpoch = leaderEpoch,
            metadata = metadata,
            error = Errors.NONE,
        )
        client.prepareResponse(
            offsetFetchResponse(
                Errors.NONE,
                mapOf(t1p to data)
            )
        )
        val fetchedOffsets = coordinator.fetchCommittedOffsets(
            setOf(t1p),
            time.timer(Long.MAX_VALUE)
        )
        assertNotNull(fetchedOffsets)
        assertEquals(
            OffsetAndMetadata(offset, leaderEpoch, metadata),
            fetchedOffsets[t1p]
        )
    }

    @Test
    fun testTopicAuthorizationFailedInOffsetFetch() {
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE))
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE))
        val data = OffsetFetchResponse.PartitionData(
            offset = -1,
            leaderEpoch = null,
            metadata = "",
            error = Errors.TOPIC_AUTHORIZATION_FAILED,
        )
        client.prepareResponse(
            offsetFetchResponse(Errors.NONE, mapOf(t1p to data))
        )

        val exception = assertFailsWith<TopicAuthorizationException> {
            coordinator.fetchCommittedOffsets(
                partitions = setOf(t1p),
                timer = time.timer(Long.MAX_VALUE)
            )
        }
        assertEquals(setOf(topic1), exception.unauthorizedTopics)
    }

    @Test
    fun testRefreshOffsetLoadInProgress() {
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE))
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE))
        subscriptions.assignFromUser(setOf(t1p))
        client.prepareResponse(
            offsetFetchResponse(
                Errors.COORDINATOR_LOAD_IN_PROGRESS,
                emptyMap()
            )
        )
        client.prepareResponse(offsetFetchResponse(t1p, Errors.NONE, "", 100L))
        coordinator.refreshCommittedOffsetsIfNeeded(time.timer(Long.MAX_VALUE))
        assertEquals(emptySet<Any>(), subscriptions.initializingPartitions())
        assertTrue(subscriptions.hasAllFetchPositions())
        assertEquals(100L, subscriptions.position(t1p)!!.offset)
    }

    @Test
    fun testRefreshOffsetsGroupNotAuthorized() {
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE))
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE))
        subscriptions.assignFromUser(setOf(t1p))
        client.prepareResponse(offsetFetchResponse(Errors.GROUP_AUTHORIZATION_FAILED, emptyMap()))
        try {
            coordinator.refreshCommittedOffsetsIfNeeded(time.timer(Long.MAX_VALUE))
            fail("Expected group authorization error")
        } catch (e: GroupAuthorizationException) {
            assertEquals(groupId, e.groupId)
        }
    }

    @Test
    fun testRefreshOffsetWithPendingTransactions() {
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE))
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE))
        subscriptions.assignFromUser(setOf(t1p))
        client.prepareResponse(offsetFetchResponse(t1p, Errors.UNSTABLE_OFFSET_COMMIT, "", -1L))
        client.prepareResponse(offsetFetchResponse(t1p, Errors.NONE, "", 100L))
        assertEquals(setOf(t1p), subscriptions.initializingPartitions())
        coordinator.refreshCommittedOffsetsIfNeeded(time.timer(0L))
        assertEquals(setOf(t1p), subscriptions.initializingPartitions())
        coordinator.refreshCommittedOffsetsIfNeeded(time.timer(0L))
        assertEquals(emptySet<Any>(), subscriptions.initializingPartitions())
        assertTrue(subscriptions.hasAllFetchPositions())
        assertEquals(100L, subscriptions.position(t1p)!!.offset)
    }

    @Test
    fun testRefreshOffsetUnknownTopicOrPartition() {
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE))
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE))
        subscriptions.assignFromUser(setOf(t1p))
        client.prepareResponse(
            offsetFetchResponse(
                t1p,
                Errors.UNKNOWN_TOPIC_OR_PARTITION,
                "",
                100L
            )
        )

        assertFailsWith<KafkaException> {
            coordinator.refreshCommittedOffsetsIfNeeded(
                time.timer(Long.MAX_VALUE)
            )
        }
    }

    @Test
    fun testRefreshOffsetNotCoordinatorForConsumer() {
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE))
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE))
        subscriptions.assignFromUser(setOf(t1p))
        client.prepareResponse(offsetFetchResponse(Errors.NOT_COORDINATOR, emptyMap()))
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE))
        client.prepareResponse(offsetFetchResponse(t1p, Errors.NONE, "", 100L))
        coordinator.refreshCommittedOffsetsIfNeeded(time.timer(Long.MAX_VALUE))
        assertEquals(emptySet<Any>(), subscriptions.initializingPartitions())
        assertTrue(subscriptions.hasAllFetchPositions())
        assertEquals(100L, subscriptions.position(t1p)!!.offset)
    }

    @Test
    fun testRefreshOffsetWithNoFetchableOffsets() {
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE))
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE))
        subscriptions.assignFromUser(setOf(t1p))
        client.prepareResponse(offsetFetchResponse(t1p, Errors.NONE, "", -1L))
        coordinator.refreshCommittedOffsetsIfNeeded(time.timer(Long.MAX_VALUE))
        assertEquals(setOf(t1p), subscriptions.initializingPartitions())
        assertEquals(
            emptySet<Any>(),
            subscriptions.partitionsNeedingReset(time.milliseconds())
        )
        assertFalse(subscriptions.hasAllFetchPositions())
        assertNull(subscriptions.position(t1p))
    }

    @Test
    fun testNoCoordinatorDiscoveryIfPositionsKnown() {
        assertTrue(coordinator.coordinatorUnknown())
        subscriptions.assignFromUser(setOf(t1p))
        subscriptions.seek(t1p, 500L)
        coordinator.refreshCommittedOffsetsIfNeeded(time.timer(Long.MAX_VALUE))
        assertEquals(emptySet<Any>(), subscriptions.initializingPartitions())
        assertTrue(subscriptions.hasAllFetchPositions())
        assertEquals(500L, subscriptions.position(t1p)!!.offset)
        assertTrue(coordinator.coordinatorUnknown())
    }

    @Test
    fun testNoCoordinatorDiscoveryIfPartitionAwaitingReset() {
        assertTrue(coordinator.coordinatorUnknown())
        subscriptions.assignFromUser(setOf(t1p))
        subscriptions.requestOffsetReset(t1p, OffsetResetStrategy.EARLIEST)
        coordinator.refreshCommittedOffsetsIfNeeded(time.timer(Long.MAX_VALUE))
        assertEquals(emptySet<Any>(), subscriptions.initializingPartitions())
        assertFalse(subscriptions.hasAllFetchPositions())
        assertEquals(
            setOf(t1p),
            subscriptions.partitionsNeedingReset(time.milliseconds())
        )
        assertEquals(OffsetResetStrategy.EARLIEST, subscriptions.resetStrategy(t1p))
        assertTrue(coordinator.coordinatorUnknown())
    }

    @Test
    fun testAuthenticationFailureInEnsureActiveGroup() {
        client.createPendingAuthenticationError(node, 300)
        try {
            coordinator.ensureActiveGroup()
            fail("Expected an authentication error.")
        } catch (e: AuthenticationException) {
            // OK
        }
    }

    @Test
    @Throws(Exception::class)
    fun testThreadSafeAssignedPartitionsMetric() {
        // Get the assigned-partitions metric
        val metric: Metric? = metrics.metric(
            MetricName(
                "assigned-partitions",
                "$consumerId$groupId-coordinator-metrics",
                "", emptyMap<String, String>()
            )
        )

        // Start polling the metric in the background
        val doStop = AtomicBoolean()
        val exceptionHolder = AtomicReference<Exception?>()
        val observedSize = AtomicInteger()
        val poller: Thread = object : Thread() {
            override fun run() {
                // Poll as fast as possible to reproduce ConcurrentModificationException
                while (!doStop.get()) {
                    try {
                        val size = (metric!!.metricValue() as Double?)!!.toInt()
                        observedSize.set(size)
                    } catch (e: Exception) {
                        exceptionHolder.set(e)
                        return
                    }
                }
            }
        }
        poller.start()

        // Assign two partitions to trigger a metric change that can lead to ConcurrentModificationException
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE))
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE))

        // Change the assignment several times to increase likelihood of concurrent updates
        val partitions: MutableSet<TopicPartition> = HashSet()
        val totalPartitions = 10
        for (partition in 0 until totalPartitions) {
            partitions.add(TopicPartition(topic1, partition))
            subscriptions.assignFromUser(partitions)
        }

        // Wait for the metric poller to observe the final assignment change or raise an error
        TestUtils.waitForCondition(
            testCondition = { observedSize.get() == totalPartitions || exceptionHolder.get() != null },
            conditionDetails = "Failed to observe expected assignment change",
        )
        doStop.set(true)
        poller.join()
        assertNull(exceptionHolder.get(), "Failed fetching the metric at least once")
    }

    @Test
    fun testCloseDynamicAssignment() {
        prepareCoordinatorForCloseTest(
            useGroupManagement = true,
            autoCommit = true,
            groupInstanceId = null,
            shouldPoll = true,
        ).use { coordinator -> gracefulCloseTest(coordinator, true) }
    }

    @Test
    fun testCloseManualAssignment() {
        prepareCoordinatorForCloseTest(
            useGroupManagement = false,
            autoCommit = true,
            groupInstanceId = null,
            shouldPoll = true
        ).use { coordinator -> gracefulCloseTest(coordinator, false) }
    }

    @Test
    @Throws(Exception::class)
    fun testCloseCoordinatorNotKnownManualAssignment() {
        prepareCoordinatorForCloseTest(
            useGroupManagement = false,
            autoCommit = true,
            groupInstanceId = null,
            shouldPoll = true
        ).use { coordinator ->
            makeCoordinatorUnknown(coordinator, Errors.NOT_COORDINATOR)
            time.sleep(autoCommitIntervalMs.toLong())
            closeVerifyTimeout(
                coordinator = coordinator,
                closeTimeoutMs = 1000,
                expectedMinTimeMs = 1000,
                expectedMaxTimeMs = 1000,
            )
        }
    }

    @Test
    @Throws(Exception::class)
    fun testCloseCoordinatorNotKnownNoCommits() {
        prepareCoordinatorForCloseTest(
            useGroupManagement = true,
            autoCommit = false,
            groupInstanceId = null,
            shouldPoll = true
        ).use { coordinator ->
            makeCoordinatorUnknown(coordinator, Errors.NOT_COORDINATOR)
            closeVerifyTimeout(
                coordinator = coordinator,
                closeTimeoutMs = 1000,
                expectedMinTimeMs = 0,
                expectedMaxTimeMs = 0,
            )
        }
    }

    @Test
    @Throws(Exception::class)
    fun testCloseCoordinatorNotKnownWithCommits() {
        prepareCoordinatorForCloseTest(
            useGroupManagement = true,
            autoCommit = true,
            groupInstanceId = null,
            shouldPoll = true,
        ).use { coordinator ->
            makeCoordinatorUnknown(coordinator, Errors.NOT_COORDINATOR)
            time.sleep(autoCommitIntervalMs.toLong())
            closeVerifyTimeout(
                coordinator = coordinator,
                closeTimeoutMs = 1000,
                expectedMinTimeMs = 1000,
                expectedMaxTimeMs = 1000,
            )
        }
    }

    @Test
    @Throws(Exception::class)
    fun testCloseCoordinatorUnavailableNoCommits() {
        prepareCoordinatorForCloseTest(
            useGroupManagement = true,
            autoCommit = false,
            groupInstanceId = null,
            shouldPoll = true,
        ).use { coordinator ->
            makeCoordinatorUnknown(coordinator, Errors.COORDINATOR_NOT_AVAILABLE)
            closeVerifyTimeout(
                coordinator = coordinator,
                closeTimeoutMs = 1000,
                expectedMinTimeMs = 0,
                expectedMaxTimeMs = 0,
            )
        }
    }

    @Test
    @Throws(Exception::class)
    fun testCloseTimeoutCoordinatorUnavailableForCommit() {
        prepareCoordinatorForCloseTest(
            useGroupManagement = true,
            autoCommit = true,
            groupInstanceId = groupInstanceId,
            shouldPoll = true,
        ).use { coordinator ->
            makeCoordinatorUnknown(coordinator, Errors.COORDINATOR_NOT_AVAILABLE)
            time.sleep(autoCommitIntervalMs.toLong())
            closeVerifyTimeout(
                coordinator = coordinator,
                closeTimeoutMs = 1000,
                expectedMinTimeMs = 1000,
                expectedMaxTimeMs = 1000,
            )
        }
    }

    @Test
    @Throws(Exception::class)
    fun testCloseMaxWaitCoordinatorUnavailableForCommit() {
        prepareCoordinatorForCloseTest(
            useGroupManagement = true,
            autoCommit = true,
            groupInstanceId = groupInstanceId,
            shouldPoll = true,
        ).use { coordinator ->
            makeCoordinatorUnknown(coordinator, Errors.COORDINATOR_NOT_AVAILABLE)
            time.sleep(autoCommitIntervalMs.toLong())
            closeVerifyTimeout(
                coordinator = coordinator,
                closeTimeoutMs = Long.MAX_VALUE,
                expectedMinTimeMs = requestTimeoutMs.toLong(),
                expectedMaxTimeMs = requestTimeoutMs.toLong()
            )
        }
    }

    @Test
    @Throws(Exception::class)
    fun testCloseNoResponseForCommit() {
        prepareCoordinatorForCloseTest(
            useGroupManagement = true,
            autoCommit = true,
            groupInstanceId = groupInstanceId,
            shouldPoll = true,
        ).use { coordinator ->
            time.sleep(autoCommitIntervalMs.toLong())
            closeVerifyTimeout(
                coordinator = coordinator,
                closeTimeoutMs = Long.MAX_VALUE,
                expectedMinTimeMs = requestTimeoutMs.toLong(),
                expectedMaxTimeMs = requestTimeoutMs.toLong(),
            )
        }
    }

    @Test
    @Throws(Exception::class)
    fun testCloseNoResponseForLeaveGroup() {
        prepareCoordinatorForCloseTest(
            useGroupManagement = true,
            autoCommit = false,
            groupInstanceId = null,
            shouldPoll = true,
        ).use { coordinator ->
            closeVerifyTimeout(
                coordinator = coordinator,
                closeTimeoutMs = Long.MAX_VALUE,
                expectedMinTimeMs = requestTimeoutMs.toLong(),
                expectedMaxTimeMs = requestTimeoutMs.toLong(),
            )
        }
    }

    @Test
    @Throws(Exception::class)
    fun testCloseNoWait() {
        prepareCoordinatorForCloseTest(
            useGroupManagement = true,
            autoCommit = true,
            groupInstanceId = groupInstanceId,
            shouldPoll = true,
        ).use { coordinator ->
            time.sleep(autoCommitIntervalMs.toLong())
            closeVerifyTimeout(
                coordinator = coordinator,
                closeTimeoutMs = 0,
                expectedMinTimeMs = 0,
                expectedMaxTimeMs = 0,
            )
        }
    }

    @Test
    @Throws(Exception::class)
    fun testHeartbeatThreadClose() {
        prepareCoordinatorForCloseTest(
            useGroupManagement = true,
            autoCommit = true,
            groupInstanceId = groupInstanceId,
            shouldPoll = true,
        ).use { coordinator ->
            coordinator.ensureActiveGroup()
            time.sleep((heartbeatIntervalMs + 100).toLong())
            Thread.yield() // Give heartbeat thread a chance to attempt heartbeat
            closeVerifyTimeout(
                coordinator = coordinator,
                closeTimeoutMs = Long.MAX_VALUE,
                expectedMinTimeMs = requestTimeoutMs.toLong(),
                expectedMaxTimeMs = requestTimeoutMs.toLong(),
            )
            val threads: Array<Thread?> =
                arrayOfNulls(Thread.activeCount())
            val threadCount: Int = Thread.enumerate(threads)
            for (i in 0 until threadCount) {
                assertFalse(threads[i]!!.name.contains(groupId), "Heartbeat thread active after close")
            }
        }
    }

    @Test
    fun testAutoCommitAfterCoordinatorBackToService() {
        buildCoordinator(
            rebalanceConfig = rebalanceConfig,
            metrics = Metrics(),
            assignors = assignors,
            autoCommitEnabled = true,
            subscriptionState = subscriptions
        ).use { coordinator ->
            subscriptions.assignFromUser(setOf(t1p))
            subscriptions.seek(t1p, 100L)
            coordinator.markCoordinatorUnknown("test cause")
            assertTrue(coordinator.coordinatorUnknown())
            client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE))
            prepareOffsetCommitRequest(mapOf(t1p to 100L), Errors.NONE)

            // async commit offset should find coordinator
            time.sleep(autoCommitIntervalMs.toLong()) // sleep for a while to ensure auto commit does happen
            coordinator.maybeAutoCommitOffsetsAsync(time.milliseconds())
            assertFalse(coordinator.coordinatorUnknown())
            assertEquals(expected = 100L, actual = subscriptions.position(t1p)!!.offset)
        }
    }

    @Test
    fun testCommitOffsetRequestSyncWithFencedInstanceIdException() {
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE))
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE))

        // sync commit with invalid partitions should throw if we have no callback
        prepareOffsetCommitRequest(mapOf(t1p to 100L), Errors.FENCED_INSTANCE_ID)
        assertFailsWith<FencedInstanceIdException> {
            coordinator.commitOffsetsSync(
                offsets = mapOf(t1p to OffsetAndMetadata(100L)),
                timer = time.timer(Long.MAX_VALUE),
            )
        }
    }

    @Test
    fun testCommitOffsetRequestAsyncWithFencedInstanceIdException() {
        assertFailsWith<FencedInstanceIdException> { receiveFencedInstanceIdException() }
    }

    @Test
    fun testCommitOffsetRequestAsyncAlwaysReceiveFencedException() {
        // Once we get fenced exception once, we should always hit fencing case.
        assertFailsWith<FencedInstanceIdException> { receiveFencedInstanceIdException() }
        assertFailsWith<FencedInstanceIdException> {
            coordinator.commitOffsetsAsync(
                offsets = mapOf(t1p to OffsetAndMetadata(100L)),
                callback = MockCommitCallback(),
            )
        }
        assertFailsWith<FencedInstanceIdException> {
            coordinator.commitOffsetsSync(
                offsets = mapOf(t1p to OffsetAndMetadata(100L)),
                timer = time.timer(Long.MAX_VALUE),
            )
        }
    }

    @Test
    fun testGetGroupMetadata() {
        val groupMetadata = coordinator.groupMetadata()
        assertNotNull(groupMetadata)
        assertEquals(groupId, groupMetadata.groupId())
        assertEquals(
            JoinGroupRequest.UNKNOWN_GENERATION_ID,
            groupMetadata.generationId()
        )
        assertEquals(JoinGroupRequest.UNKNOWN_MEMBER_ID, groupMetadata.memberId())
        assertNull(groupMetadata.groupInstanceId)
        prepareCoordinatorForCloseTest(
            useGroupManagement = true,
            autoCommit = true,
            groupInstanceId = groupInstanceId,
            shouldPoll = true,
        ).use { coordinator ->
            coordinator.ensureActiveGroup()
            val joinedGroupMetadata: ConsumerGroupMetadata = coordinator.groupMetadata()
            assertNotNull(joinedGroupMetadata)
            assertEquals(groupId, joinedGroupMetadata.groupId())
            assertEquals(1, joinedGroupMetadata.generationId())
            assertEquals(
                consumerId,
                joinedGroupMetadata.memberId()
            )
            assertEquals(
                groupInstanceId,
                joinedGroupMetadata.groupInstanceId()
            )
        }
    }

    @Test
    fun shouldUpdateConsumerGroupMetadataBeforeCallbacks() {
        val rebalanceListener = object : MockRebalanceListener() {
            override fun onPartitionsRevoked(partitions: Collection<TopicPartition>) {
                assertEquals(2, coordinator.groupMetadata().generationId())
            }
        }
        subscriptions.subscribe(setOf(topic1), rebalanceListener)
        run {
            val buffer: ByteBuffer = serializeAssignment(
                ConsumerPartitionAssignor.Assignment(
                    listOf(
                        t1p
                    ), ByteBuffer.wrap(ByteArray(0))
                )
            )
            coordinator.onJoinComplete(1, "memberId", partitionAssignor.name(), buffer)
        }
        val buffer = serializeAssignment(
            ConsumerPartitionAssignor.Assignment(emptyList(), ByteBuffer.wrap(ByteArray(0)))
        )
        coordinator.onJoinComplete(2, "memberId", partitionAssignor.name(), buffer)
    }

    @Test
    fun testPrepareJoinAndRejoinAfterFailedRebalance() {
        val partitions = listOf(t1p)
        prepareCoordinatorForCloseTest(
            useGroupManagement = true,
            autoCommit = false,
            groupInstanceId = "group-id",
            shouldPoll = true,
        ).use { coordinator ->
            coordinator.ensureActiveGroup()
            prepareOffsetCommitRequest(mapOf(t1p to 100L), Errors.REBALANCE_IN_PROGRESS)
            assertFailsWith<RebalanceInProgressException> {
                coordinator.commitOffsetsSync(
                    offsets = mapOf(t1p to OffsetAndMetadata(100L)),
                    timer = time.timer(Long.MAX_VALUE)
                )
            }
            assertFalse(client.hasPendingResponses())
            assertFalse(client.hasInFlightRequests())
            val generationId = 42
            val memberId = "consumer-42"
            client.prepareResponse(
                joinGroupFollowerResponse(
                    generationId,
                    memberId,
                    "leader",
                    Errors.NONE
                )
            )
            val time = MockTime(1)

            // onJoinPrepare will be executed and onJoinComplete will not.
            var res: Boolean = coordinator.joinGroupIfNeeded(time.timer(100))
            assertFalse(res)
            assertFalse(client.hasPendingResponses())
            // SynGroupRequest not responded.
            assertEquals(1, client.inFlightRequestCount())
            assertEquals(
                expected = generationId,
                actual = coordinator.generation().generationId
            )
            assertEquals(
                expected = memberId,
                actual = coordinator.generation().memberId
            )

            // Imitating heartbeat thread that clears generation data.
            coordinator.maybeLeaveGroup("Clear generation data.")
            assertEquals(
                AbstractCoordinator.Generation.NO_GENERATION,
                coordinator.generation()
            )
            client.respond(
                syncGroupResponse(
                    partitions,
                    Errors.NONE
                )
            )

            // Join future should succeed but generation already cleared so result of join is false.
            res = coordinator.joinGroupIfNeeded(time.timer(1))
            assertFalse(res)

            // should have retried sending a join group request already
            assertFalse(client.hasPendingResponses())
            assertEquals(1, client.inFlightRequestCount())
            println(client.requests())

            // Retry join should then succeed
            client.respond(
                joinGroupFollowerResponse(
                    generationId,
                    memberId,
                    "leader",
                    Errors.NONE
                )
            )
            client.prepareResponse(
                syncGroupResponse(
                    partitions,
                    Errors.NONE
                )
            )
            res = coordinator.joinGroupIfNeeded(time.timer(3000))
            assertTrue(res)
            assertFalse(client.hasPendingResponses())
            assertFalse(client.hasInFlightRequests())
        }
        val lost = getLost(partitions)
        assertEquals(if (lost.isEmpty()) null else lost, rebalanceListener.lost)
        assertEquals(lost.size, rebalanceListener.lostCount)
    }

    @Test
    fun shouldLoseAllOwnedPartitionsBeforeRejoiningAfterDroppingOutOfTheGroup() {
        val partitions = listOf(t1p)
        prepareCoordinatorForCloseTest(
            useGroupManagement = true,
            autoCommit = false,
            groupInstanceId = "group-id",
            shouldPoll = true,
        ).use { coordinator ->
            val realTime = SystemTime()
            coordinator.ensureActiveGroup()
            prepareOffsetCommitRequest(
                expectedOffsets = mapOf(t1p to 100L),
                error = Errors.REBALANCE_IN_PROGRESS,
            )

            assertFailsWith<RebalanceInProgressException> {
                coordinator.commitOffsetsSync(
                    offsets = mapOf(t1p to OffsetAndMetadata(100L)),
                    timer = time.timer(Long.MAX_VALUE),
                )
            }

            val generationId = 42
            val memberId = "consumer-42"
            client.prepareResponse(
                joinGroupFollowerResponse(
                    generationId = generationId,
                    memberId = memberId,
                    leaderId = "leader",
                    error = Errors.NONE,
                )
            )
            client.prepareResponse(syncGroupResponse(emptyList(), Errors.UNKNOWN_MEMBER_ID))
            var res: Boolean = coordinator.joinGroupIfNeeded(realTime.timer(1000))
            assertFalse(res)
            assertEquals(
                AbstractCoordinator.Generation.NO_GENERATION,
                coordinator.generation()
            )
            assertEquals("", coordinator.generation().memberId)
            res = coordinator.joinGroupIfNeeded(realTime.timer(1000))
            assertFalse(res)
        }
        val lost = getLost(partitions)
        assertEquals(if (lost.isEmpty()) 0 else 1, rebalanceListener.lostCount)
        assertEquals(lost.ifEmpty { null }, rebalanceListener.lost)
    }

    @Test
    fun shouldLoseAllOwnedPartitionsBeforeRejoiningAfterResettingGenerationId() {
        val partitions = listOf(t1p)
        prepareCoordinatorForCloseTest(
            useGroupManagement = true,
            autoCommit = false,
            groupInstanceId = "group-id",
            shouldPoll = true
        ).use { coordinator ->
            val realTime: SystemTime = SystemTime()
            coordinator.ensureActiveGroup()
            prepareOffsetCommitRequest(
                expectedOffsets = mapOf(t1p to 100L),
                error = Errors.REBALANCE_IN_PROGRESS,
            )
            assertFailsWith<RebalanceInProgressException> {
                coordinator.commitOffsetsSync(
                    offsets = mapOf(t1p to OffsetAndMetadata(100L)),
                    timer = time.timer(Long.MAX_VALUE),
                )
            }
            val generationId = 42
            val memberId = "consumer-42"
            client.prepareResponse(
                joinGroupFollowerResponse(
                    generationId = generationId,
                    memberId = memberId,
                    leaderId = "leader",
                    error = Errors.NONE,
                )
            )
            client.prepareResponse(syncGroupResponse(emptyList(), Errors.ILLEGAL_GENERATION))
            var res = coordinator.joinGroupIfNeeded(realTime.timer(1000))
            assertFalse(res)
            assertEquals(
                expected = AbstractCoordinator.Generation.NO_GENERATION.generationId,
                actual = coordinator.generation().generationId,
            )
            assertEquals(
                expected = AbstractCoordinator.Generation.NO_GENERATION.protocolName,
                actual = coordinator.generation().protocolName,
            )
            // member ID should not be reset
            assertEquals(
                expected = memberId,
                actual = coordinator.generation().memberId,
            )
            res = coordinator.joinGroupIfNeeded(realTime.timer(1000))
            assertFalse(res)
        }
        val lost = getLost(partitions)
        assertEquals(if (lost.isEmpty()) 0 else 1, rebalanceListener.lostCount)
        assertEquals(lost.ifEmpty { null }, rebalanceListener.lost)
    }

    @Test
    fun testSubscriptionRackId() {
        metrics.close()
        coordinator.close(time.timer(0))
        val rackId = "rack-a"
        metrics = Metrics(time = time)
        val assignor = RackAwareAssignor()
        coordinator = ConsumerCoordinator(
            rebalanceConfig = rebalanceConfig!!,
            logContext = LogContext(),
            client = consumerClient,
            assignors = listOf(assignor),
            metadata = metadata,
            subscriptions = subscriptions,
            metrics = metrics,
            metricGrpPrefix = consumerId + groupId,
            time = time,
            autoCommitEnabled = false,
            autoCommitIntervalMs = autoCommitIntervalMs,
            interceptors = null,
            throwOnFetchStableOffsetsUnsupported = false,
            rackId = rackId,
        )
        subscriptions.subscribe(setOf(topic1), rebalanceListener)
        client.updateMetadata(metadataResponse)
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE))
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE))
        val memberSubscriptions = mapOf(consumerId to listOf(topic1))
        assignor.prepare(mapOf(consumerId to listOf(t1p)))
        client.prepareResponse(
            joinGroupLeaderResponse(
                generationId = 1,
                memberId = consumerId,
                subscriptions = memberSubscriptions,
                skipAssignment = false,
                error = Errors.NONE,
                rackId = rackId,
            )
        )
        client.prepareResponse(syncGroupResponse(listOf(t1p), Errors.NONE))
        coordinator.poll(time.timer(Long.MAX_VALUE))
        assertEquals(setOf(t1p), coordinator.subscriptionState().assignedPartitions())
        assertEquals(setOf(rackId), assignor.rackIds)
    }

    @Test
    fun testThrowOnUnsupportedStableFlag() {
        supportStableFlag(6.toShort(), true)
    }

    @Test
    fun testNoThrowWhenStableFlagIsSupported() {
        supportStableFlag(7.toShort(), false)
    }

    private fun supportStableFlag(upperVersion: Short, expectThrows: Boolean) {
        val coordinator = ConsumerCoordinator(
            rebalanceConfig = rebalanceConfig!!,
            logContext = LogContext(),
            client = consumerClient,
            assignors = assignors,
            metadata = metadata,
            subscriptions = subscriptions,
            metrics = Metrics(time = time),
            metricGrpPrefix = consumerId + groupId,
            time = time,
            autoCommitEnabled = false,
            autoCommitIntervalMs = autoCommitIntervalMs,
            interceptors = null,
            throwOnFetchStableOffsetsUnsupported = true,
            rackId = null
        )
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE))
        client.setNodeApiVersions(
            NodeApiVersions.create(
                apiKey = ApiKeys.OFFSET_FETCH.id,
                minVersion = 0.toShort(),
                maxVersion = upperVersion,
            )
        )
        val offset = 500L
        val metadata = "blahblah"
        val leaderEpoch = 15
        val data = OffsetFetchResponse.PartitionData(
            offset = offset,
            leaderEpoch = leaderEpoch,
            metadata = metadata,
            error = Errors.NONE,
        )
        if (upperVersion < 8) client.prepareResponse(OffsetFetchResponse(Errors.NONE, mapOf(t1p to data)))
        else client.prepareResponse(offsetFetchResponse(Errors.NONE, mapOf(t1p to data)))

        if (expectThrows) assertFailsWith<UnsupportedVersionException> {
            coordinator.fetchCommittedOffsets(
                partitions = setOf(t1p),
                timer = time.timer(Long.MAX_VALUE),
            )
        } else {
            val fetchedOffsets = coordinator.fetchCommittedOffsets(
                partitions = setOf(t1p),
                timer = time.timer(Long.MAX_VALUE),
            )
            assertNotNull(fetchedOffsets)
            assertEquals(
                expected = OffsetAndMetadata(offset, leaderEpoch, metadata),
                actual = fetchedOffsets[t1p],
            )
        }
    }

    private fun receiveFencedInstanceIdException() {
        subscriptions.assignFromUser(setOf(t1p))
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE))
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE))
        prepareOffsetCommitRequest(mapOf(t1p to 100L), Errors.FENCED_INSTANCE_ID)
        coordinator.commitOffsetsAsync(
            offsets = mapOf(t1p to OffsetAndMetadata(100L)),
            callback = MockCommitCallback(),
        )
        coordinator.invokeCompletedOffsetCommitCallbacks()
    }

    private fun prepareCoordinatorForCloseTest(
        useGroupManagement: Boolean,
        autoCommit: Boolean,
        groupInstanceId: String?,
        shouldPoll: Boolean,
    ): ConsumerCoordinator {
        rebalanceConfig = buildRebalanceConfig(groupInstanceId)
        val coordinator = buildCoordinator(
            rebalanceConfig = rebalanceConfig,
            metrics = Metrics(),
            assignors = assignors,
            autoCommitEnabled = autoCommit,
            subscriptionState = subscriptions,
        )
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE))
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE))
        if (useGroupManagement) {
            subscriptions.subscribe(topics = setOf(topic1), listener = rebalanceListener)
            client.prepareResponse(
                joinGroupFollowerResponse(
                    generationId = 1,
                    memberId = consumerId,
                    leaderId = "leader",
                    error = Errors.NONE
                )
            )
            client.prepareResponse(syncGroupResponse(listOf(t1p), Errors.NONE))
            coordinator.joinGroupIfNeeded(time.timer(Long.MAX_VALUE))
        } else subscriptions.assignFromUser(setOf(t1p))

        subscriptions.seek(t1p, 100)
        if (shouldPoll) {
            coordinator.poll(time.timer(Long.MAX_VALUE))
        }
        return coordinator
    }

    private fun makeCoordinatorUnknown(coordinator: ConsumerCoordinator, error: Errors) {
        time.sleep(sessionTimeoutMs.toLong())
        coordinator.sendHeartbeatRequest()
        client.prepareResponse(heartbeatResponse(error))
        time.sleep(sessionTimeoutMs.toLong())
        consumerClient.poll(time.timer(0))
        assertTrue(coordinator.coordinatorUnknown())
    }

    @Throws(Exception::class)
    private fun closeVerifyTimeout(
        coordinator: ConsumerCoordinator,
        closeTimeoutMs: Long,
        expectedMinTimeMs: Long,
        expectedMaxTimeMs: Long,
    ) {
        val executor = Executors.newSingleThreadExecutor()
        try {
            val coordinatorUnknown = coordinator.coordinatorUnknown()
            // Run close on a different thread. Coordinator is locked by this thread, so it is
            // not safe to use the coordinator from the main thread until the task completes.
            val future = executor.submit {
                val timeoutMs = min(closeTimeoutMs.toDouble(), requestTimeoutMs.toDouble()).toLong()
                coordinator.close(time.timer(timeoutMs))
            }
            // Wait for close to start. If coordinator is known, wait for close to queue
            // at least one request. Otherwise, sleep for a short time.
            if (!coordinatorUnknown) client.waitForRequests(1, 1000) else Thread.sleep(200)
            if (expectedMinTimeMs > 0) {
                time.sleep(expectedMinTimeMs - 1)
                try {
                    future[500, TimeUnit.MILLISECONDS]
                    fail("Close completed ungracefully without waiting for timeout")
                } catch (e: TimeoutException) {
                    // Expected timeout
                }
            }
            if (expectedMaxTimeMs >= 0) time.sleep(expectedMaxTimeMs - expectedMinTimeMs + 2)
            future[2000, TimeUnit.MILLISECONDS]
        } finally {
            executor.shutdownNow()
        }
    }

    private fun gracefulCloseTest(coordinator: ConsumerCoordinator, shouldLeaveGroup: Boolean) {
        val commitRequested = AtomicBoolean()
        val leaveGroupRequested = AtomicBoolean()
        client.prepareResponse(
            matcher = { body ->
                commitRequested.set(true)
                val commitRequest = body as OffsetCommitRequest
                commitRequest.data().groupId == groupId
            },
            response = OffsetCommitResponse(OffsetCommitResponseData()),
        )
        if (shouldLeaveGroup) client.prepareResponse(
            matcher = { body ->
                leaveGroupRequested.set(true)
                val leaveRequest = body as LeaveGroupRequest
                leaveRequest.data().groupId.equals(groupId)
            },
            response = LeaveGroupResponse(LeaveGroupResponseData().setErrorCode(Errors.NONE.code)),
        )
        client.prepareResponse(
            matcher = { body ->
                commitRequested.set(true)
                val commitRequest = body as OffsetCommitRequest
                commitRequest.data().groupId.equals(groupId)
            },
            response = OffsetCommitResponse(OffsetCommitResponseData()),
        )
        coordinator.close()
        assertTrue(commitRequested.get(), "Commit not requested")
        assertEquals(
            shouldLeaveGroup, leaveGroupRequested.get(),
            "leaveGroupRequested should be $shouldLeaveGroup"
        )
        if (shouldLeaveGroup) {
            assertEquals(1, rebalanceListener.revokedCount)
            assertEquals(setOf(t1p), rebalanceListener.revoked)
        }
    }

    private fun buildCoordinator(
        rebalanceConfig: GroupRebalanceConfig?,
        metrics: Metrics,
        assignors: List<ConsumerPartitionAssignor>,
        autoCommitEnabled: Boolean,
        subscriptionState: SubscriptionState?,
    ): ConsumerCoordinator {
        return ConsumerCoordinator(
            rebalanceConfig = rebalanceConfig!!,
            logContext = LogContext(),
            client = consumerClient,
            assignors = assignors,
            metadata = metadata,
            subscriptions = (subscriptionState)!!,
            metrics = metrics,
            metricGrpPrefix = consumerId + groupId,
            time = time,
            autoCommitEnabled = autoCommitEnabled,
            autoCommitIntervalMs = autoCommitIntervalMs,
            interceptors = null,
            throwOnFetchStableOffsetsUnsupported = false,
            rackId = null,
        )
    }

    private fun getRevoked(
        owned: List<TopicPartition>,
        assigned: List<TopicPartition>,
    ): Collection<TopicPartition?> {
        return when (protocol) {
            RebalanceProtocol.EAGER -> owned.toSet()
            RebalanceProtocol.COOPERATIVE -> {
                val revoked: MutableList<TopicPartition> = ArrayList(owned)
                revoked.removeAll(assigned)
                revoked.toSet()
            }

            else -> error("This should not happen")
        }
    }

    private fun getLost(owned: List<TopicPartition>): Collection<TopicPartition> {
        return when (protocol) {
            RebalanceProtocol.EAGER -> emptySet()
            RebalanceProtocol.COOPERATIVE -> owned.toSet()
            else -> error("This should not happen")
        }
    }

    private fun getAdded(
        owned: List<TopicPartition>,
        assigned: List<TopicPartition>,
    ): Collection<TopicPartition> {
        return when (protocol) {
            RebalanceProtocol.EAGER -> assigned.toSet()
            RebalanceProtocol.COOPERATIVE -> {
                val added: MutableList<TopicPartition> = ArrayList(assigned)
                added.removeAll(owned)
                added.toSet()
            }

            else -> error("This should not happen")
        }
    }

    private fun groupCoordinatorResponse(node: Node, error: Errors): FindCoordinatorResponse {
        return FindCoordinatorResponse.prepareResponse(error, groupId, node)
    }

    private fun heartbeatResponse(error: Errors): HeartbeatResponse {
        return HeartbeatResponse(HeartbeatResponseData().setErrorCode(error.code))
    }

    private fun joinGroupLeaderResponse(
        generationId: Int,
        memberId: String,
        subscriptions: Map<String, List<String>>,
        error: Errors,
    ): JoinGroupResponse {
        return joinGroupLeaderResponse(
            generationId,
            memberId,
            subscriptions,
            false,
            error,
            null
        )
    }

    private fun joinGroupLeaderResponse(
        generationId: Int,
        memberId: String,
        subscriptions: Map<String, List<String>>,
        skipAssignment: Boolean,
        error: Errors,
        rackId: String?,
    ): JoinGroupResponse {
        val metadata: MutableList<JoinGroupResponseMember> = ArrayList()
        for (subscriptionEntry: Map.Entry<String, List<String>> in subscriptions.entries) {
            val subscription = ConsumerPartitionAssignor.Subscription(
                subscriptionEntry.value,
                null, emptyList(), AbstractStickyAssignor.DEFAULT_GENERATION, rackId
            )
            val buf = serializeSubscription(subscription)
            metadata.add(
                JoinGroupResponseMember()
                    .setMemberId(subscriptionEntry.key)
                    .setMetadata(buf.array())
            )
        }
        return JoinGroupResponse(
            JoinGroupResponseData()
                .setErrorCode(error.code)
                .setGenerationId(generationId)
                .setProtocolName(partitionAssignor.name())
                .setLeader(memberId)
                .setSkipAssignment(skipAssignment)
                .setMemberId(memberId)
                .setMembers(metadata),
            ApiKeys.JOIN_GROUP.latestVersion()
        )
    }

    private fun joinGroupFollowerResponse(
        generationId: Int,
        memberId: String,
        leaderId: String,
        error: Errors,
    ): JoinGroupResponse {
        return JoinGroupResponse(
            JoinGroupResponseData()
                .setErrorCode(error.code)
                .setGenerationId(generationId)
                .setProtocolName(partitionAssignor.name())
                .setLeader(leaderId)
                .setMemberId(memberId)
                .setMembers(emptyList()),
            ApiKeys.JOIN_GROUP.latestVersion()
        )
    }

    private fun syncGroupResponse(
        partitions: List<TopicPartition>,
        error: Errors,
    ): SyncGroupResponse {
        val buf = serializeAssignment(ConsumerPartitionAssignor.Assignment(partitions))
        return SyncGroupResponse(
            SyncGroupResponseData()
                .setErrorCode(error.code)
                .setAssignment(Utils.toArray(buf))
        )
    }

    private fun offsetCommitResponse(responseData: Map<TopicPartition, Errors>): OffsetCommitResponse {
        return OffsetCommitResponse(responseData)
    }

    private fun offsetFetchResponse(
        error: Errors,
        responseData: Map<TopicPartition, OffsetFetchResponse.PartitionData>,
    ): OffsetFetchResponse {
        return OffsetFetchResponse(
            throttleTimeMs = throttleMs,
            errors = mapOf(groupId to error),
            responseData = mapOf(groupId to responseData),
        )
    }

    private fun offsetFetchResponse(
        tp: TopicPartition,
        partitionLevelError: Errors,
        metadata: String,
        offset: Long,
        epoch: Int? = null,
    ): OffsetFetchResponse {
        val data = OffsetFetchResponse.PartitionData(
            offset = offset,
            leaderEpoch = epoch,
            metadata = metadata,
            error = partitionLevelError,
        )
        return offsetFetchResponse(Errors.NONE, mapOf(tp to data))
    }

    private fun callback(success: AtomicBoolean): OffsetCommitCallback {
        return OffsetCommitCallback { _, exception ->
            if (exception == null) success.set(true)
        }
    }

    private fun joinAsFollowerAndReceiveAssignment(
        coordinator: ConsumerCoordinator,
        assignment: List<TopicPartition>,
    ) {
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE))
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE))
        client.prepareResponse(joinGroupFollowerResponse(1, consumerId, "leader", Errors.NONE))
        client.prepareResponse(syncGroupResponse(assignment, Errors.NONE))
        coordinator.joinGroupIfNeeded(time.timer(Long.MAX_VALUE))
    }

    private fun prepareOffsetCommitRequestDisconnect(expectedOffsets: Map<TopicPartition, Long>) {
        prepareOffsetCommitRequest(expectedOffsets, Errors.NONE, true)
    }

    private fun prepareOffsetCommitRequest(
        expectedOffsets: Map<TopicPartition, Long>,
        error: Errors,
        disconnected: Boolean = false,
    ) {
        val errors = partitionErrors(expectedOffsets.keys, error)
        client.prepareResponse(
            matcher = offsetCommitRequestMatcher(expectedOffsets),
            response = offsetCommitResponse(errors),
            disconnected = disconnected,
        )
    }

    private fun prepareJoinAndSyncResponse(
        consumerId: String,
        generation: Int,
        subscription: List<String>,
        assignment: List<TopicPartition>,
    ) {
        partitionAssignor.prepare(mapOf(consumerId to assignment))
        client.prepareResponse(
            joinGroupLeaderResponse(
                generationId = generation,
                memberId = consumerId,
                subscriptions = mapOf(consumerId to subscription),
                error = Errors.NONE,
            )
        )
        client.prepareResponse(
            matcher = { body ->
                val sync = body as SyncGroupRequest
                sync.data().memberId == consumerId
                        && sync.data().generationId == generation
                        && sync.groupAssignments().containsKey(consumerId)
            },
            response = syncGroupResponse(assignment, Errors.NONE),
        )
    }

    private fun partitionErrors(
        partitions: Collection<TopicPartition>,
        error: Errors,
    ): Map<TopicPartition, Errors> {
        val errors: MutableMap<TopicPartition, Errors> = HashMap()
        for (partition: TopicPartition in partitions) {
            errors[partition] = error
        }
        return errors
    }

    private fun respondToOffsetCommitRequest(
        expectedOffsets: Map<TopicPartition, Long>,
        error: Errors,
    ) {
        val errors = partitionErrors(expectedOffsets.keys, error)
        client.respond(offsetCommitRequestMatcher(expectedOffsets), offsetCommitResponse(errors))
    }

    private fun offsetCommitRequestMatcher(expectedOffsets: Map<TopicPartition, Long>): RequestMatcher {
        return RequestMatcher { body ->
            val req = body as OffsetCommitRequest?
            val offsets: Map<TopicPartition, Long> = req!!.offsets()
            if (offsets.size != expectedOffsets.size) return@RequestMatcher false
            for (expectedOffset: Map.Entry<TopicPartition, Long> in expectedOffsets.entries) {
                if (!offsets.containsKey(expectedOffset.key)) {
                    return@RequestMatcher false
                } else {
                    val actualOffset: Long? = offsets[expectedOffset.key]
                    if (actualOffset != expectedOffset.value) {
                        return@RequestMatcher false
                    }
                }
            }
            true
        }
    }

    private fun callback(
        expectedOffsets: Map<TopicPartition, OffsetAndMetadata>,
        success: AtomicBoolean,
    ): OffsetCommitCallback {
        return OffsetCommitCallback { offsets, exception ->
            if ((expectedOffsets == offsets) && exception == null) success.set(true)
        }
    }

    private class MockCommitCallback : OffsetCommitCallback {
        var invoked = 0
        var exception: Exception? = null
        override fun onComplete(
            offsets: Map<TopicPartition, OffsetAndMetadata>?,
            exception: Exception?,
        ) {
            invoked++
            this.exception = exception
        }
    }

    private class RackAwareAssignor : MockPartitionAssignor(listOf(RebalanceProtocol.EAGER)) {
        val rackIds: MutableSet<String> = HashSet()
        override fun assign(
            partitionsPerTopic: Map<String, Int>,
            subscriptions: Map<String, ConsumerPartitionAssignor.Subscription>,
        ): Map<String, List<TopicPartition>> {
            subscriptions.forEach { (consumer: String, subscription: ConsumerPartitionAssignor.Subscription) ->
                check(subscription.rackId != null) {
                    "Rack id not provided in subscription for $consumer"
                }
                rackIds.add(subscription.rackId!!)
            }
            return super.assign(partitionsPerTopic, subscriptions)
        }
    }
}
