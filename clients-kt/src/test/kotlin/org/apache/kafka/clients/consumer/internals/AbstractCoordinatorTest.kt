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

import org.apache.kafka.clients.GroupRebalanceConfig
import org.apache.kafka.clients.MockClient
import org.apache.kafka.clients.MockClient.RequestMatcher
import org.apache.kafka.clients.consumer.OffsetResetStrategy
import org.apache.kafka.common.Node
import org.apache.kafka.common.errors.AuthenticationException
import org.apache.kafka.common.errors.DisconnectException
import org.apache.kafka.common.errors.FencedInstanceIdException
import org.apache.kafka.common.errors.GroupMaxSizeReachedException
import org.apache.kafka.common.errors.InconsistentGroupProtocolException
import org.apache.kafka.common.errors.UnknownMemberIdException
import org.apache.kafka.common.errors.WakeupException
import org.apache.kafka.common.internals.ClusterResourceListeners
import org.apache.kafka.common.message.HeartbeatResponseData
import org.apache.kafka.common.message.JoinGroupRequestData.JoinGroupRequestProtocol
import org.apache.kafka.common.message.JoinGroupRequestData.JoinGroupRequestProtocolCollection
import org.apache.kafka.common.message.JoinGroupResponseData
import org.apache.kafka.common.message.JoinGroupResponseData.JoinGroupResponseMember
import org.apache.kafka.common.message.LeaveGroupResponseData
import org.apache.kafka.common.message.LeaveGroupResponseData.MemberResponse
import org.apache.kafka.common.message.SyncGroupResponseData
import org.apache.kafka.common.metrics.KafkaMetric
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.AbstractRequest
import org.apache.kafka.common.requests.AbstractResponse
import org.apache.kafka.common.requests.FindCoordinatorResponse
import org.apache.kafka.common.requests.HeartbeatRequest
import org.apache.kafka.common.requests.HeartbeatResponse
import org.apache.kafka.common.requests.JoinGroupRequest
import org.apache.kafka.common.requests.JoinGroupResponse
import org.apache.kafka.common.requests.LeaveGroupRequest
import org.apache.kafka.common.requests.LeaveGroupResponse
import org.apache.kafka.common.requests.RequestTestUtils.metadataUpdateWith
import org.apache.kafka.common.requests.SyncGroupRequest
import org.apache.kafka.common.requests.SyncGroupResponse
import org.apache.kafka.common.utils.LogContext
import org.apache.kafka.common.utils.MockTime
import org.apache.kafka.common.utils.Time
import org.apache.kafka.common.utils.Timer
import org.apache.kafka.common.utils.Utils.closeQuietly
import org.apache.kafka.test.TestUtils.waitForCondition
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import java.nio.ByteBuffer
import java.util.*
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertNotNull
import kotlin.test.assertNotSame
import kotlin.test.assertSame
import kotlin.test.assertTrue
import kotlin.test.fail

class AbstractCoordinatorTest {
    private lateinit var node: Node
    private lateinit var metrics: Metrics
    private lateinit var mockTime: MockTime
    private lateinit var coordinatorNode: Node
    private lateinit var mockClient: MockClient
    private lateinit var coordinator: DummyCoordinator
    private lateinit var consumerClient: ConsumerNetworkClient

    private val memberId = "memberId"
    private val leaderId = "leaderId"
    private val defaultGeneration = -1

    @AfterEach
    fun closeCoordinator() {
        closeQuietly(coordinator, "close coordinator")
        closeQuietly(consumerClient, "close consumer client")
    }

    private fun setupCoordinator(
        retryBackoffMs: Int = RETRY_BACKOFF_MS,
        rebalanceTimeoutMs: Int = REBALANCE_TIMEOUT_MS,
        groupInstanceId: String? = null,
    ) {
        val logContext = LogContext()
        mockTime = MockTime()
        val metadata = ConsumerMetadata(
            refreshBackoffMs = retryBackoffMs.toLong(),
            metadataExpireMs = 60 * 60 * 1000L,
            includeInternalTopics = false,
            allowAutoTopicCreation = false,
            subscription = SubscriptionState(logContext, OffsetResetStrategy.EARLIEST),
            logContext = logContext,
            clusterResourceListeners = ClusterResourceListeners(),
        )

        mockClient = MockClient(mockTime, metadata)
        consumerClient = ConsumerNetworkClient(
            logContext = logContext,
            client = mockClient,
            metadata = metadata,
            time = mockTime,
            retryBackoffMs = retryBackoffMs.toLong(),
            requestTimeoutMs = REQUEST_TIMEOUT_MS,
            maxPollTimeoutMs = HEARTBEAT_INTERVAL_MS,
        )
        metrics = Metrics(time = mockTime)
        mockClient.updateMetadata(
            metadataUpdateWith(
                numNodes = 1,
                topicPartitionCounts = emptyMap(),
            )
        )
        node = metadata.fetch().nodes[0]
        coordinatorNode = Node(
            id = Int.MAX_VALUE - node.id,
            host = node.host,
            port = node.port,
        )
        val rebalanceConfig = GroupRebalanceConfig(
            sessionTimeoutMs = SESSION_TIMEOUT_MS,
            rebalanceTimeoutMs = rebalanceTimeoutMs,
            heartbeatIntervalMs = HEARTBEAT_INTERVAL_MS,
            groupId = GROUP_ID,
            groupInstanceId = groupInstanceId,
            retryBackoffMs = retryBackoffMs.toLong(),
            leaveGroupOnClose = groupInstanceId == null,
        )
        coordinator = DummyCoordinator(
            rebalanceConfig = rebalanceConfig,
            client = consumerClient,
            metrics = metrics,
            time = mockTime,
        )
    }

    private fun joinGroup() {
        mockClient.prepareResponse(groupCoordinatorResponse(node, Errors.NONE))

        val generation = 1

        mockClient.prepareResponse(
            joinGroupFollowerResponse(
                generationId = generation,
                memberId = memberId,
                leaderId = JoinGroupRequest.UNKNOWN_MEMBER_ID,
                error = Errors.NONE,
            )
        )
        mockClient.prepareResponse(syncGroupResponse(Errors.NONE))

        coordinator.ensureActiveGroup()
    }

    @Test
    fun testMetrics() {
        setupCoordinator()
        assertNotNull(getMetric("heartbeat-response-time-max"))
        assertNotNull(getMetric("heartbeat-rate"))
        assertNotNull(getMetric("heartbeat-total"))
        assertNotNull(getMetric("last-heartbeat-seconds-ago"))
        assertNotNull(getMetric("join-time-avg"))
        assertNotNull(getMetric("join-time-max"))
        assertNotNull(getMetric("join-rate"))
        assertNotNull(getMetric("join-total"))
        assertNotNull(getMetric("sync-time-avg"))
        assertNotNull(getMetric("sync-time-max"))
        assertNotNull(getMetric("sync-rate"))
        assertNotNull(getMetric("sync-total"))
        assertNotNull(getMetric("rebalance-latency-avg"))
        assertNotNull(getMetric("rebalance-latency-max"))
        assertNotNull(getMetric("rebalance-latency-total"))
        assertNotNull(getMetric("rebalance-rate-per-hour"))
        assertNotNull(getMetric("rebalance-total"))
        assertNotNull(getMetric("last-rebalance-seconds-ago"))
        assertNotNull(getMetric("failed-rebalance-rate-per-hour"))
        assertNotNull(getMetric("failed-rebalance-total"))

        metrics.sensor("heartbeat-latency").record(1.0)
        metrics.sensor("heartbeat-latency").record(6.0)
        metrics.sensor("heartbeat-latency").record(2.0)

        assertEquals(6.0, getMetric("heartbeat-response-time-max")!!.metricValue())
        assertEquals(0.1, getMetric("heartbeat-rate")!!.metricValue())
        assertEquals(3.0, getMetric("heartbeat-total")!!.metricValue())
        assertEquals(-1.0, getMetric("last-heartbeat-seconds-ago")!!.metricValue())

        coordinator.heartbeat().sentHeartbeat(mockTime.milliseconds())
        assertEquals(0.0, getMetric("last-heartbeat-seconds-ago")!!.metricValue())
        mockTime.sleep(10 * 1000L)
        assertEquals(10.0, getMetric("last-heartbeat-seconds-ago")!!.metricValue())

        metrics.sensor("join-latency").record(1.0)
        metrics.sensor("join-latency").record(6.0)
        metrics.sensor("join-latency").record(2.0)

        assertEquals(3.0, getMetric("join-time-avg")!!.metricValue())
        assertEquals(6.0, getMetric("join-time-max")!!.metricValue())
        assertEquals(0.1, getMetric("join-rate")!!.metricValue())
        assertEquals(3.0, getMetric("join-total")!!.metricValue())

        metrics.sensor("sync-latency").record(1.0)
        metrics.sensor("sync-latency").record(6.0)
        metrics.sensor("sync-latency").record(2.0)

        assertEquals(3.0, getMetric("sync-time-avg")!!.metricValue())
        assertEquals(6.0, getMetric("sync-time-max")!!.metricValue())
        assertEquals(0.1, getMetric("sync-rate")!!.metricValue())
        assertEquals(3.0, getMetric("sync-total")!!.metricValue())

        metrics.sensor("rebalance-latency").record(1.0)
        metrics.sensor("rebalance-latency").record(6.0)
        metrics.sensor("rebalance-latency").record(2.0)

        assertEquals(3.0, getMetric("rebalance-latency-avg")!!.metricValue())
        assertEquals(6.0, getMetric("rebalance-latency-max")!!.metricValue())
        assertEquals(9.0, getMetric("rebalance-latency-total")!!.metricValue())
        assertEquals(360.0, getMetric("rebalance-rate-per-hour")!!.metricValue())
        assertEquals(3.0, getMetric("rebalance-total")!!.metricValue())

        metrics.sensor("failed-rebalance").record(1.0)
        metrics.sensor("failed-rebalance").record(6.0)
        metrics.sensor("failed-rebalance").record(2.0)

        assertEquals(360.0, getMetric("failed-rebalance-rate-per-hour")!!.metricValue())
        assertEquals(3.0, getMetric("failed-rebalance-total")!!.metricValue())
        assertEquals(-1.0, getMetric("last-rebalance-seconds-ago")!!.metricValue())

        coordinator.setLastRebalanceTime(mockTime.milliseconds())
        assertEquals(0.0, getMetric("last-rebalance-seconds-ago")!!.metricValue())
        mockTime.sleep(10 * 1000L)
        assertEquals(10.0, getMetric("last-rebalance-seconds-ago")!!.metricValue())
    }

    private fun getMetric(name: String): KafkaMetric? {
        return metrics.metrics[metrics.metricName(name, "consumer-coordinator-metrics")]
    }

    @Test
    fun testCoordinatorDiscoveryBackoff() {
        setupCoordinator()

        mockClient.prepareResponse(groupCoordinatorResponse(node, Errors.NONE))
        mockClient.prepareResponse(groupCoordinatorResponse(node, Errors.NONE))

        // cut out the coordinator for 10 milliseconds to simulate a disconnect.
        // after backing off, we should be able to connect.
        mockClient.backoff(coordinatorNode, 10L)

        val initialTime = mockTime.milliseconds()
        coordinator.ensureCoordinatorReady(mockTime.timer(Long.MAX_VALUE))
        val endTime = mockTime.milliseconds()

        assertTrue(endTime - initialTime >= RETRY_BACKOFF_MS)
    }

    @Test
    fun testWakeupFromEnsureCoordinatorReady() {
        setupCoordinator()

        consumerClient.wakeup()

        // No wakeup should occur from the async variation.
        coordinator.ensureCoordinatorReadyAsync()

        // But should wakeup in sync variation even if timer is 0.
        assertFailsWith<WakeupException> {
            coordinator.ensureCoordinatorReady(mockTime.timer(0))
        }
    }

    @Test
    @Throws(Exception::class)
    fun testTimeoutAndRetryJoinGroupIfNeeded() {
        setupCoordinator()
        mockClient.prepareResponse(groupCoordinatorResponse(node, Errors.NONE))
        coordinator.ensureCoordinatorReady(mockTime.timer(0))

        val executor = Executors.newFixedThreadPool(1)
        try {
            val firstAttemptTimer = mockTime.timer(REQUEST_TIMEOUT_MS.toLong())
            val firstAttempt = executor.submit<Boolean> {
                coordinator.joinGroupIfNeeded(firstAttemptTimer)
            }

            mockTime.sleep(REQUEST_TIMEOUT_MS.toLong())
            assertFalse(firstAttempt.get())
            assertTrue(consumerClient.hasPendingRequests(coordinatorNode))

            mockClient.respond(joinGroupFollowerResponse(1, memberId, leaderId, Errors.NONE))
            mockClient.prepareResponse(syncGroupResponse(Errors.NONE))

            val secondAttemptTimer = mockTime.timer(REQUEST_TIMEOUT_MS.toLong())
            val secondAttempt = executor.submit<Boolean> {
                coordinator.joinGroupIfNeeded(secondAttemptTimer)
            }

            assertTrue(secondAttempt.get())
        } finally {
            executor.shutdownNow()
            executor.awaitTermination(1000, TimeUnit.MILLISECONDS)
        }
    }

    @Test
    fun testGroupMaxSizeExceptionIsFatal() {
        setupCoordinator()
        mockClient.prepareResponse(groupCoordinatorResponse(node, Errors.NONE))
        coordinator.ensureCoordinatorReady(mockTime.timer(0))

        mockClient.prepareResponse(
            joinGroupFollowerResponse(
                generationId = defaultGeneration,
                memberId = memberId,
                leaderId = JoinGroupRequest.UNKNOWN_MEMBER_ID,
                error = Errors.GROUP_MAX_SIZE_REACHED,
            )
        )
        val future = coordinator.sendJoinGroupRequest()
        assertTrue(consumerClient.poll(future, mockTime.timer(REQUEST_TIMEOUT_MS.toLong())))
        assertTrue(future.exception().javaClass.isInstance(Errors.GROUP_MAX_SIZE_REACHED.exception))
        assertFalse(future.isRetriable)
    }

    @Test
    fun testJoinGroupRequestTimeout() {
        setupCoordinator(RETRY_BACKOFF_MS, REBALANCE_TIMEOUT_MS, null)
        mockClient.prepareResponse(groupCoordinatorResponse(node, Errors.NONE))
        coordinator.ensureCoordinatorReady(mockTime.timer(0))

        val future = coordinator.sendJoinGroupRequest()

        mockTime.sleep((REQUEST_TIMEOUT_MS + 1).toLong())
        assertFalse(consumerClient.poll(future, mockTime.timer(0)))

        mockTime.sleep((REBALANCE_TIMEOUT_MS - REQUEST_TIMEOUT_MS + AbstractCoordinator.JOIN_GROUP_TIMEOUT_LAPSE).toLong())
        assertTrue(consumerClient.poll(future, mockTime.timer(0)))
        assertTrue(future.exception() is DisconnectException)
    }

    @Test
    fun testJoinGroupRequestTimeoutLowerBoundedByDefaultRequestTimeout() {
        val rebalanceTimeoutMs = REQUEST_TIMEOUT_MS - 10000
        setupCoordinator(RETRY_BACKOFF_MS, rebalanceTimeoutMs, null)
        mockClient.prepareResponse(groupCoordinatorResponse(node, Errors.NONE))
        coordinator.ensureCoordinatorReady(mockTime.timer(0))

        val future = coordinator.sendJoinGroupRequest()

        val expectedRequestDeadline = mockTime.milliseconds() + REQUEST_TIMEOUT_MS
        mockTime.sleep((rebalanceTimeoutMs + AbstractCoordinator.JOIN_GROUP_TIMEOUT_LAPSE + 1).toLong())
        assertFalse(consumerClient.poll(future, mockTime.timer(0)))

        mockTime.sleep(expectedRequestDeadline - mockTime.milliseconds() + 1)
        assertTrue(consumerClient.poll(future, mockTime.timer(0)))
        assertTrue(future.exception() is DisconnectException)
    }

    @Test
    fun testJoinGroupRequestMaxTimeout() {
        // Ensure we can handle the maximum allowed rebalance timeout

        setupCoordinator(RETRY_BACKOFF_MS, Int.MAX_VALUE, null)
        mockClient.prepareResponse(groupCoordinatorResponse(node, Errors.NONE))
        coordinator.ensureCoordinatorReady(mockTime.timer(0))

        val future = coordinator.sendJoinGroupRequest()
        assertFalse(consumerClient.poll(future, mockTime.timer(0)))

        mockTime.sleep(Int.MAX_VALUE + 1L)
        assertTrue(consumerClient.poll(future, mockTime.timer(0)))
    }

    @Test
    fun testJoinGroupRequestWithMemberIdRequired() {
        setupCoordinator()
        mockClient.prepareResponse(groupCoordinatorResponse(node, Errors.NONE))
        coordinator.ensureCoordinatorReady(mockTime.timer(0))

        mockClient.prepareResponse(
            joinGroupFollowerResponse(
                generationId = defaultGeneration,
                memberId = memberId,
                leaderId = JoinGroupRequest.UNKNOWN_MEMBER_ID,
                error = Errors.MEMBER_ID_REQUIRED,
            )
        )

        mockClient.prepareResponse(
            matcher = { body ->
                if (body !is JoinGroupRequest) return@prepareResponse false
                val joinGroupRequest = body.data()
                joinGroupRequest.memberId == memberId
            },
            response = joinGroupResponse(Errors.UNKNOWN_MEMBER_ID),
        )

        var future = coordinator.sendJoinGroupRequest()
        assertTrue(consumerClient.poll(future, mockTime.timer(REQUEST_TIMEOUT_MS.toLong())))
        assertEquals(Errors.MEMBER_ID_REQUIRED.message, future.exception().message)
        assertTrue(coordinator.rejoinNeededOrPending())
        assertTrue(coordinator.hasValidMemberId())
        assertTrue(coordinator.hasMatchingGenerationId(defaultGeneration))
        future = coordinator.sendJoinGroupRequest()
        assertTrue(consumerClient.poll(future, mockTime.timer(REBALANCE_TIMEOUT_MS.toLong())))
    }

    @Test
    fun testJoinGroupRequestWithFencedInstanceIdException() {
        setupCoordinator()
        mockClient.prepareResponse(groupCoordinatorResponse(node, Errors.NONE))
        coordinator.ensureCoordinatorReady(mockTime.timer(0))

        mockClient.prepareResponse(
            joinGroupFollowerResponse(
                generationId = defaultGeneration,
                memberId = memberId,
                leaderId = JoinGroupRequest.UNKNOWN_MEMBER_ID,
                error = Errors.FENCED_INSTANCE_ID,
            )
        )

        val future = coordinator.sendJoinGroupRequest()
        assertTrue(consumerClient.poll(future, mockTime.timer(REQUEST_TIMEOUT_MS.toLong())))
        assertEquals(Errors.FENCED_INSTANCE_ID.message, future.exception().message)
        // Make sure the exception is fatal.
        assertFalse(future.isRetriable)
    }

    @Test
    fun testJoinGroupProtocolTypeAndName() {
        val wrongProtocolType = "wrong-type"
        val wrongProtocolName = "wrong-name"

        // No Protocol Type in both JoinGroup and SyncGroup responses
        assertTrue(
            joinGroupWithProtocolTypeAndName(
                joinGroupResponseProtocolType = null,
                syncGroupResponseProtocolType = null,
                syncGroupResponseProtocolName = null,
            ),
        )

        // Protocol Type in both JoinGroup and SyncGroup responses
        assertTrue(
            joinGroupWithProtocolTypeAndName(
                joinGroupResponseProtocolType = PROTOCOL_TYPE,
                syncGroupResponseProtocolType = PROTOCOL_TYPE,
                syncGroupResponseProtocolName = PROTOCOL_NAME,
            )
        )

        // Wrong protocol type in the JoinGroupResponse
        assertFailsWith<InconsistentGroupProtocolException> {
            joinGroupWithProtocolTypeAndName(
                joinGroupResponseProtocolType = "wrong",
                syncGroupResponseProtocolType = null,
                syncGroupResponseProtocolName = null,
            )
        }

        // Correct protocol type in the JoinGroupResponse
        // Wrong protocol type in the SyncGroupResponse
        // Correct protocol name in the SyncGroupResponse
        assertFailsWith<InconsistentGroupProtocolException> {
            joinGroupWithProtocolTypeAndName(
                joinGroupResponseProtocolType = PROTOCOL_TYPE,
                syncGroupResponseProtocolType = wrongProtocolType,
                syncGroupResponseProtocolName = PROTOCOL_NAME,
            )
        }

        // Correct protocol type in the JoinGroupResponse
        // Correct protocol type in the SyncGroupResponse
        // Wrong protocol name in the SyncGroupResponse
        assertFailsWith<InconsistentGroupProtocolException> {
            joinGroupWithProtocolTypeAndName(
                joinGroupResponseProtocolType = PROTOCOL_TYPE,
                syncGroupResponseProtocolType = PROTOCOL_TYPE,
                syncGroupResponseProtocolName = wrongProtocolName,
            )
        }
    }

    @Test
    fun testRetainMemberIdAfterJoinGroupDisconnect() {
        setupCoordinator()
        val memberId = "memberId"
        val generation = 5

        // Rebalance once to initialize the generation and memberId
        mockClient.prepareResponse(groupCoordinatorResponse(node, Errors.NONE))
        expectJoinGroup("", generation, memberId)
        expectSyncGroup(generation, memberId)
        ensureActiveGroup(generation, memberId)

        // Force a rebalance
        coordinator.requestRejoin("Manual test trigger")
        assertTrue(coordinator.rejoinNeededOrPending())

        // Disconnect during the JoinGroup and ensure that the retry preserves the memberId
        val rejoinedGeneration = 10
        expectDisconnectInJoinGroup(memberId)
        mockClient.prepareResponse(groupCoordinatorResponse(node, Errors.NONE))
        expectJoinGroup(memberId, rejoinedGeneration, memberId)
        expectSyncGroup(rejoinedGeneration, memberId)
        ensureActiveGroup(rejoinedGeneration, memberId)
    }

    @Test
    fun testRetainMemberIdAfterSyncGroupDisconnect() {
        setupCoordinator()
        val memberId = "memberId"
        val generation = 5

        // Rebalance once to initialize the generation and memberId
        mockClient.prepareResponse(groupCoordinatorResponse(node, Errors.NONE))
        expectJoinGroup("", generation, memberId)
        expectSyncGroup(generation, memberId)
        ensureActiveGroup(generation, memberId)

        // Force a rebalance
        coordinator.requestRejoin("Manual test trigger")
        assertTrue(coordinator.rejoinNeededOrPending())

        // Disconnect during the SyncGroup and ensure that the retry preserves the memberId
        val rejoinedGeneration = 10
        expectJoinGroup(memberId, rejoinedGeneration, memberId)
        expectDisconnectInSyncGroup(rejoinedGeneration, memberId)
        mockClient.prepareResponse(groupCoordinatorResponse(node, Errors.NONE))

        // Note that the consumer always starts from JoinGroup after a failed rebalance
        expectJoinGroup(memberId, rejoinedGeneration, memberId)
        expectSyncGroup(rejoinedGeneration, memberId)
        ensureActiveGroup(rejoinedGeneration, memberId)
    }

    @Test
    fun testRejoinReason() {
        setupCoordinator()
        val memberId = "memberId"
        val generation = 5

        // test initial reason
        mockClient.prepareResponse(groupCoordinatorResponse(node, Errors.NONE))
        expectJoinGroup("", "", generation, memberId)

        // successful sync group response should reset reason
        expectSyncGroup(generation, memberId)
        ensureActiveGroup(generation, memberId)
        assertEquals(expected = "", actual = coordinator.rejoinReason())

        // force a rebalance
        expectJoinGroup(memberId, "Manual test trigger", generation, memberId)
        expectSyncGroup(generation, memberId)
        coordinator.requestRejoin("Manual test trigger")
        ensureActiveGroup(generation, memberId)
        assertEquals(expected = "", actual = coordinator.rejoinReason())

        // max group size reached
        mockClient.prepareResponse(
            joinGroupFollowerResponse(
                generationId = defaultGeneration,
                memberId = memberId,
                leaderId = JoinGroupRequest.UNKNOWN_MEMBER_ID,
                error = Errors.GROUP_MAX_SIZE_REACHED,
            )
        )
        coordinator.requestRejoin("Manual test trigger 2")
        val e = assertFailsWith<GroupMaxSizeReachedException> {
            coordinator.joinGroupIfNeeded(mockTime.timer(100))
        }

        // next join group request should contain exception message
        expectJoinGroup(
            expectedMemberId = memberId,
            expectedReason = "rebalance failed due to ${e.javaClass.getSimpleName()}",
            responseGeneration = generation,
            responseMemberId = memberId,
        )
        expectSyncGroup(generation, memberId)
        ensureActiveGroup(generation, memberId)
        assertEquals(expected = "", actual = coordinator.rejoinReason())

        // check limit length of reason field
        val reason =
            "Very looooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooong reason that is 271 characters long to make sure that length limit logic handles the scenario nicely"
        val truncatedReason = reason.substring(0, 255)
        expectJoinGroup(memberId, truncatedReason, generation, memberId)
        expectSyncGroup(generation, memberId)
        coordinator.requestRejoin(reason)
        ensureActiveGroup(generation, memberId)
        assertEquals("", coordinator.rejoinReason())
    }

    private fun ensureActiveGroup(
        generation: Int,
        memberId: String,
    ) {
        coordinator.ensureActiveGroup()
        assertEquals(generation, coordinator.generation().generationId)
        assertEquals(memberId, coordinator.generation().memberId)
        assertFalse(coordinator.rejoinNeededOrPending())
    }

    private fun expectSyncGroup(
        expectedGeneration: Int,
        expectedMemberId: String,
    ) {
        mockClient.prepareResponse(
            matcher = { body ->
                if (body !is SyncGroupRequest) return@prepareResponse false
                val syncGroupRequest = body.data()

                syncGroupRequest.generationId == expectedGeneration
                        && syncGroupRequest.memberId == expectedMemberId
                        && syncGroupRequest.protocolType.equals(PROTOCOL_TYPE)
                        && syncGroupRequest.protocolName.equals(PROTOCOL_NAME)
            },
            response = syncGroupResponse(Errors.NONE, PROTOCOL_TYPE, PROTOCOL_NAME),
        )
    }

    private fun expectDisconnectInSyncGroup(
        expectedGeneration: Int,
        expectedMemberId: String,
    ) {
        mockClient.prepareResponse(
            matcher = { body ->
                if (body !is SyncGroupRequest) return@prepareResponse false
                val syncGroupRequest = body.data()

                syncGroupRequest.generationId == expectedGeneration
                        && syncGroupRequest.memberId == expectedMemberId
                        && syncGroupRequest.protocolType.equals(PROTOCOL_TYPE)
                        && syncGroupRequest.protocolName.equals(PROTOCOL_NAME)
            },
            response = null,
            disconnected = true,
        )
    }

    private fun expectDisconnectInJoinGroup(expectedMemberId: String) {
        mockClient.prepareResponse(
            matcher = { body ->
                if (body !is JoinGroupRequest) return@prepareResponse false
                val joinGroupRequest = body.data()

                joinGroupRequest.memberId == expectedMemberId
                        && joinGroupRequest.protocolType == PROTOCOL_TYPE
            },
            response = null,
            disconnected = true,
        )
    }

    private fun expectJoinGroup(
        expectedMemberId: String,
        responseGeneration: Int,
        responseMemberId: String,
    ) = expectJoinGroup(
        expectedMemberId = expectedMemberId,
        expectedReason = null,
        responseGeneration = responseGeneration,
        responseMemberId = responseMemberId,
    )

    private fun expectJoinGroup(
        expectedMemberId: String,
        expectedReason: String?,
        responseGeneration: Int,
        responseMemberId: String,
    ) {
        val response = joinGroupFollowerResponse(
            generationId = responseGeneration,
            memberId = responseMemberId,
            leaderId = "leaderId",
            error = Errors.NONE,
            protocolType = PROTOCOL_TYPE,
        )
        mockClient.prepareResponse(
            matcher = { body ->
                if (body !is JoinGroupRequest) return@prepareResponse false
                val joinGroupRequest = body.data()

                // abstract coordinator never sets reason to null
                val actualReason = joinGroupRequest.reason
                val isReasonMatching = expectedReason == null || expectedReason == actualReason
                joinGroupRequest.memberId == expectedMemberId
                        && joinGroupRequest.protocolType == PROTOCOL_TYPE
                        && isReasonMatching
            },
            response = response,
        )
    }

    @Test
    fun testNoGenerationWillNotTriggerProtocolNameCheck() {
        val wrongProtocolName = "wrong-name"
        setupCoordinator()
        mockClient.reset()
        mockClient.prepareResponse(groupCoordinatorResponse(node, Errors.NONE))
        coordinator.ensureCoordinatorReady(mockTime.timer(0))

        mockClient.prepareResponse(
            matcher = { body ->
                if (body !is JoinGroupRequest) return@prepareResponse false
                val joinGroupRequest = body.data()

                joinGroupRequest.protocolType == PROTOCOL_TYPE
            },
            response = joinGroupFollowerResponse(
                generationId = defaultGeneration,
                memberId = memberId,
                leaderId = "memberid",
                error = Errors.NONE,
                protocolType = PROTOCOL_TYPE,
            )
        )

        mockClient.prepareResponse(
            matcher = { body ->
                if (body !is SyncGroupRequest) return@prepareResponse false
                coordinator.resetGenerationOnLeaveGroup()
                val syncGroupRequest = body.data()

                syncGroupRequest.protocolType.equals(PROTOCOL_TYPE)
                        && syncGroupRequest.protocolName.equals(PROTOCOL_NAME)
            },
            response = syncGroupResponse(Errors.NONE, PROTOCOL_TYPE, wrongProtocolName),
        )

        // let the retry to complete successfully to break out of the while loop
        mockClient.prepareResponse(
            matcher = { body ->
                if (body !is JoinGroupRequest) return@prepareResponse false
                val joinGroupRequest = body.data()

                joinGroupRequest.protocolType == PROTOCOL_TYPE
            },
            response = joinGroupFollowerResponse(
                generationId = 1,
                memberId = memberId,
                leaderId = "memberid",
                error = Errors.NONE,
                protocolType = PROTOCOL_TYPE,
            ),
        )
        mockClient.prepareResponse(
            matcher = { body ->
                if (body !is SyncGroupRequest) return@prepareResponse false
                val syncGroupRequest = body.data()

                syncGroupRequest.protocolType.equals(PROTOCOL_TYPE)
                        && syncGroupRequest.protocolName.equals(PROTOCOL_NAME)
            },
            response = syncGroupResponse(Errors.NONE, PROTOCOL_TYPE, PROTOCOL_NAME),
        )

        // No exception shall be thrown as the generation is reset.
        coordinator.joinGroupIfNeeded(mockTime.timer(100L))
    }

    private fun joinGroupWithProtocolTypeAndName(
        joinGroupResponseProtocolType: String?,
        syncGroupResponseProtocolType: String?,
        syncGroupResponseProtocolName: String?,
    ): Boolean {
        setupCoordinator()
        mockClient.reset()
        mockClient.prepareResponse(groupCoordinatorResponse(node, Errors.NONE))
        coordinator.ensureCoordinatorReady(mockTime.timer(0))

        mockClient.prepareResponse(
            matcher = { body ->
                if (body !is JoinGroupRequest) return@prepareResponse false
                val joinGroupRequest = body.data()

                joinGroupRequest.protocolType == PROTOCOL_TYPE
            },
            response = joinGroupFollowerResponse(
                generationId = defaultGeneration,
                memberId = memberId,
                leaderId = "memberid",
                error = Errors.NONE,
                protocolType = joinGroupResponseProtocolType,
            )
        )
        mockClient.prepareResponse(
            matcher = { body ->
                if (body !is SyncGroupRequest) return@prepareResponse false
                val syncGroupRequest = body.data()

                syncGroupRequest.protocolType.equals(PROTOCOL_TYPE)
                        && syncGroupRequest.protocolName.equals(PROTOCOL_NAME)
            },
            response = syncGroupResponse(
                error = Errors.NONE,
                protocolType = syncGroupResponseProtocolType,
                protocolName = syncGroupResponseProtocolName,
            )
        )
        return coordinator.joinGroupIfNeeded(mockTime.timer(5000L))
    }

    @Test
    fun testSyncGroupRequestWithFencedInstanceIdException() {
        setupCoordinator()
        mockClient.prepareResponse(groupCoordinatorResponse(node, Errors.NONE))
        val generation = -1

        mockClient.prepareResponse(
            joinGroupFollowerResponse(
                generationId = generation,
                memberId = memberId,
                leaderId = JoinGroupRequest.UNKNOWN_MEMBER_ID,
                error = Errors.NONE
            )
        )
        mockClient.prepareResponse(syncGroupResponse(Errors.FENCED_INSTANCE_ID))

        assertFailsWith<FencedInstanceIdException> { coordinator.ensureActiveGroup() }
    }

    @Test
    @Throws(InterruptedException::class)
    fun testJoinGroupUnknownMemberResponseWithOldGeneration() {
        setupCoordinator()
        joinGroup()
        val (generationId, memberId1, protocolName) = coordinator.generation()
        val future = coordinator.sendJoinGroupRequest()

        waitForCondition(
            testCondition = { !mockClient.requests().isEmpty() },
            maxWaitMs = 2000,
            conditionDetails = "The join-group request was not sent",
        )

        // change the generation after the join-group request
        val newGen = AbstractCoordinator.Generation(
            generationId = generationId,
            memberId = "$memberId1-new",
            protocolName = protocolName,
        )
        coordinator.setNewGeneration(newGen)
        mockClient.respond(
            joinGroupFollowerResponse(
                generationId = generationId + 1,
                memberId = memberId,
                leaderId = JoinGroupRequest.UNKNOWN_MEMBER_ID,
                error = Errors.UNKNOWN_MEMBER_ID,
            )
        )

        assertTrue(consumerClient.poll(future, mockTime.timer(REQUEST_TIMEOUT_MS.toLong())))
        assertTrue(future.exception().javaClass.isInstance(Errors.UNKNOWN_MEMBER_ID.exception))

        // the generation should not be reset
        assertEquals(newGen, coordinator.generation())
    }

    @Test
    @Throws(InterruptedException::class)
    fun testSyncGroupUnknownMemberResponseWithOldGeneration() {
        setupCoordinator()
        joinGroup()
        val (generationId, memberId1, protocolName) = coordinator.generation()
        coordinator.setNewState(AbstractCoordinator.MemberState.PREPARING_REBALANCE)
        val future = coordinator.sendJoinGroupRequest()

        waitForCondition(
            testCondition = {
                consumerClient.poll(mockTime.timer(REQUEST_TIMEOUT_MS.toLong()))
                !mockClient.requests().isEmpty()
            },
            maxWaitMs = 2000,
            conditionDetails = "The join-group request was not sent",
        )

        mockClient.respond(
            joinGroupFollowerResponse(
                generationId = generationId,
                memberId = memberId,
                leaderId = JoinGroupRequest.UNKNOWN_MEMBER_ID,
                error = Errors.NONE,
            )
        )
        assertTrue(mockClient.requests().isEmpty())

        waitForCondition(
            testCondition = {
                consumerClient.poll(mockTime.timer(REQUEST_TIMEOUT_MS.toLong()))
                !mockClient.requests().isEmpty()
            },
            maxWaitMs = 2000,
            conditionDetails = "The sync-group request was not sent",
        )

        // change the generation after the sync-group request
        val newGen = AbstractCoordinator.Generation(
            generationId = generationId,
            memberId = "$memberId1-new",
            protocolName = protocolName,
        )
        coordinator.setNewGeneration(newGen)
        mockClient.respond(syncGroupResponse(Errors.UNKNOWN_MEMBER_ID))

        assertTrue(consumerClient.poll(future, mockTime.timer(REQUEST_TIMEOUT_MS.toLong())))
        assertTrue(future.exception().javaClass.isInstance(Errors.UNKNOWN_MEMBER_ID.exception))

        // the generation should not be reset
        assertEquals(newGen, coordinator.generation())
    }

    @Test
    @Throws(InterruptedException::class)
    fun testSyncGroupIllegalGenerationResponseWithOldGeneration() {
        setupCoordinator()
        joinGroup()
        val (generationId, memberId1, protocolName) = coordinator.generation()
        coordinator.setNewState(AbstractCoordinator.MemberState.PREPARING_REBALANCE)
        val future = coordinator.sendJoinGroupRequest()

        waitForCondition(
            testCondition = {
                consumerClient.poll(mockTime.timer(REQUEST_TIMEOUT_MS.toLong()))
                !mockClient.requests().isEmpty()
            },
            maxWaitMs = 2000,
            conditionDetails = "The join-group request was not sent",
        )

        mockClient.respond(
            joinGroupFollowerResponse(
                generationId = generationId,
                memberId = memberId,
                leaderId = JoinGroupRequest.UNKNOWN_MEMBER_ID,
                error = Errors.NONE,
            )
        )

        assertTrue(mockClient.requests().isEmpty())
        waitForCondition(
            testCondition = {
                consumerClient.poll(mockTime.timer(REQUEST_TIMEOUT_MS.toLong()))
                !mockClient.requests().isEmpty()
            },
            maxWaitMs = 2000,
            conditionDetails = "The sync-group request was not sent",
        )

        // change the generation after the sync-group request
        val newGen = AbstractCoordinator.Generation(
            generationId = generationId,
            memberId = "$memberId1-new",
            protocolName = protocolName,
        )
        coordinator.setNewGeneration(newGen)
        mockClient.respond(syncGroupResponse(Errors.ILLEGAL_GENERATION))

        assertTrue(consumerClient.poll(future, mockTime.timer(REQUEST_TIMEOUT_MS.toLong())))
        assertTrue(future.exception().javaClass.isInstance(Errors.ILLEGAL_GENERATION.exception))

        // the generation should not be reset
        assertEquals(newGen, coordinator.generation())
    }

    @Test
    @Throws(Exception::class)
    fun testHeartbeatSentWhenCompletingRebalance() {
        setupCoordinator()
        joinGroup()
        val currGen = coordinator.generation()
        coordinator.setNewState(AbstractCoordinator.MemberState.COMPLETING_REBALANCE)

        // the heartbeat should be sent out during a rebalance
        mockTime.sleep(HEARTBEAT_INTERVAL_MS.toLong())
        waitForCondition(
            testCondition = { !mockClient.requests().isEmpty() },
            maxWaitMs = 2000,
            conditionDetails = "The heartbeat request was not sent",
        )
        assertTrue(coordinator.heartbeat().hasInflight())

        mockClient.respond(heartbeatResponse(Errors.REBALANCE_IN_PROGRESS))
        assertEquals(currGen, coordinator.generation())
    }

    @Test
    @Throws(InterruptedException::class)
    fun testHeartbeatIllegalGenerationResponseWithOldGeneration() {
        setupCoordinator()
        joinGroup()
        val (generationId, memberId1, protocolName) = coordinator.generation()

        // let the heartbeat thread send out a request
        mockTime.sleep(HEARTBEAT_INTERVAL_MS.toLong())
        waitForCondition(
            testCondition = { !mockClient.requests().isEmpty() },
            maxWaitMs = 2000,
            conditionDetails = "The heartbeat request was not sent",
        )
        assertTrue(coordinator.heartbeat().hasInflight())

        // change the generation
        val newGen = AbstractCoordinator.Generation(
            generationId = generationId + 1,
            memberId = memberId1,
            protocolName = protocolName,
        )
        coordinator.setNewGeneration(newGen)
        mockClient.respond(heartbeatResponse(Errors.ILLEGAL_GENERATION))

        // the heartbeat error code should be ignored
        waitForCondition(
            testCondition = {
                coordinator.pollHeartbeat(mockTime.milliseconds())
                !coordinator.heartbeat().hasInflight()
            },
            maxWaitMs = 2000,
            conditionDetails = "The heartbeat response was not received",
        )

        // the generation should not be reset
        assertEquals(newGen, coordinator.generation())
    }

    @Test
    @Throws(InterruptedException::class)
    fun testHeartbeatUnknownMemberResponseWithOldGeneration() {
        setupCoordinator()
        joinGroup()
        val (generationId, memberId1, protocolName) = coordinator.generation()

        // let the heartbeat request to send out a request
        mockTime.sleep(HEARTBEAT_INTERVAL_MS.toLong())
        waitForCondition(
            testCondition = { !mockClient.requests().isEmpty() },
            maxWaitMs = 2000,
            conditionDetails = "The heartbeat request was not sent",
        )
        assertTrue(coordinator.heartbeat().hasInflight())

        // change the generation
        val newGen = AbstractCoordinator.Generation(
            generationId = generationId,
            memberId = "$memberId1-new",
            protocolName = protocolName,
        )
        coordinator.setNewGeneration(newGen)
        mockClient.respond(heartbeatResponse(Errors.UNKNOWN_MEMBER_ID))

        // the heartbeat error code should be ignored
        waitForCondition(
            testCondition = {
                coordinator.pollHeartbeat(mockTime.milliseconds())
                !coordinator.heartbeat().hasInflight()
            },
            maxWaitMs = 2000,
            conditionDetails = "The heartbeat response was not received",
        )

        // the generation should not be reset
        assertEquals(newGen, coordinator.generation())
    }

    @Test
    @Throws(InterruptedException::class)
    fun testHeartbeatRebalanceInProgressResponseDuringRebalancing() {
        setupCoordinator()
        joinGroup()
        val currGen = coordinator.generation()

        // let the heartbeat request to send out a request
        mockTime.sleep(HEARTBEAT_INTERVAL_MS.toLong())
        waitForCondition(
            testCondition = { !mockClient.requests().isEmpty() },
            maxWaitMs = 2000,
            conditionDetails = "The heartbeat request was not sent",
        )
        assertTrue(coordinator.heartbeat().hasInflight())

        mockClient.respond(heartbeatResponse(Errors.REBALANCE_IN_PROGRESS))
        coordinator.requestRejoin("test")
        waitForCondition(
            testCondition = {
                coordinator.ensureActiveGroup(MockTime(1L).timer(100L))
                !coordinator.heartbeat().hasInflight()
            },
            maxWaitMs = 2000,
            conditionDetails = "The heartbeat response was not received",
        )

        // the generation would not be reset while the rebalance is in progress
        assertEquals(currGen, coordinator.generation())
        mockClient.respond(
            joinGroupFollowerResponse(
                generationId = currGen.generationId,
                memberId = memberId,
                leaderId = JoinGroupRequest.UNKNOWN_MEMBER_ID,
                error = Errors.NONE,
            )
        )
        mockClient.prepareResponse(syncGroupResponse(Errors.NONE))
        coordinator.ensureActiveGroup()
        assertEquals(currGen, coordinator.generation())
    }

    @Test
    @Throws(InterruptedException::class)
    fun testHeartbeatInstanceFencedResponseWithOldGeneration() {
        setupCoordinator()
        joinGroup()
        val (generationId, memberId1, protocolName) = coordinator.generation()

        // let the heartbeat request to send out a request
        mockTime.sleep(HEARTBEAT_INTERVAL_MS.toLong())
        waitForCondition(
            testCondition = { !mockClient.requests().isEmpty() },
            maxWaitMs = 2000,
            conditionDetails = "The heartbeat request was not sent",
        )
        assertTrue(coordinator.heartbeat().hasInflight())

        // change the generation
        val newGen = AbstractCoordinator.Generation(
            generationId = generationId,
            memberId = "$memberId1-new",
            protocolName = protocolName,
        )
        coordinator.setNewGeneration(newGen)
        mockClient.respond(heartbeatResponse(Errors.FENCED_INSTANCE_ID))

        // the heartbeat error code should be ignored
        waitForCondition(
            testCondition = {
                coordinator.pollHeartbeat(mockTime.milliseconds())
                !coordinator.heartbeat().hasInflight()
            },
            maxWaitMs = 2000,
            conditionDetails = "The heartbeat response was not received",
        )

        // the generation should not be reset
        assertEquals(newGen, coordinator.generation())
    }

    @Test
    @Throws(InterruptedException::class)
    fun testHeartbeatRequestWithFencedInstanceIdException() {
        setupCoordinator()
        mockClient.prepareResponse(groupCoordinatorResponse(node, Errors.NONE))
        val generation = -1
        mockClient.prepareResponse(
            joinGroupFollowerResponse(
                generationId = generation,
                memberId = memberId,
                leaderId = JoinGroupRequest.UNKNOWN_MEMBER_ID,
                error = Errors.NONE,
            )
        )
        mockClient.prepareResponse(syncGroupResponse(Errors.NONE))
        mockClient.prepareResponse(heartbeatResponse(Errors.FENCED_INSTANCE_ID))
        try {
            coordinator.ensureActiveGroup()
            mockTime.sleep(HEARTBEAT_INTERVAL_MS.toLong())
            val startMs = System.currentTimeMillis()
            while (System.currentTimeMillis() - startMs < 1000) {
                Thread.sleep(10)
                coordinator.pollHeartbeat(mockTime.milliseconds())
            }
            fail("Expected pollHeartbeat to raise fenced instance id exception in 1 second")
        } catch (exception: RuntimeException) {
            assertTrue(exception is FencedInstanceIdException)
        }
    }

    @Test
    fun testJoinGroupRequestWithGroupInstanceIdNotFound() {
        setupCoordinator()
        mockClient.prepareResponse(groupCoordinatorResponse(node, Errors.NONE))
        coordinator.ensureCoordinatorReady(mockTime.timer(0))
        mockClient.prepareResponse(
            joinGroupFollowerResponse(
                defaultGeneration,
                memberId,
                JoinGroupRequest.UNKNOWN_MEMBER_ID,
                Errors.UNKNOWN_MEMBER_ID
            )
        )
        val future = coordinator.sendJoinGroupRequest()
        assertTrue(consumerClient.poll(future, mockTime.timer(REQUEST_TIMEOUT_MS.toLong())))
        assertEquals(Errors.UNKNOWN_MEMBER_ID.message, future.exception().message)
        assertTrue(coordinator.rejoinNeededOrPending())
        assertTrue(coordinator.hasUnknownGeneration())
    }

    @Test
    fun testJoinGroupRequestWithRebalanceInProgress() {
        setupCoordinator()
        mockClient.prepareResponse(groupCoordinatorResponse(node, Errors.NONE))
        coordinator.ensureCoordinatorReady(mockTime.timer(0))
        mockClient.prepareResponse(
            joinGroupFollowerResponse(
                generationId = defaultGeneration,
                memberId = memberId,
                leaderId = JoinGroupRequest.UNKNOWN_MEMBER_ID,
                error = Errors.REBALANCE_IN_PROGRESS,
            )
        )
        val future = coordinator.sendJoinGroupRequest()
        assertTrue(consumerClient.poll(future, mockTime.timer(REQUEST_TIMEOUT_MS.toLong())))
        assertTrue(future.exception().javaClass.isInstance(Errors.REBALANCE_IN_PROGRESS.exception))
        assertEquals(Errors.REBALANCE_IN_PROGRESS.message, future.exception().message)
        assertTrue(coordinator.rejoinNeededOrPending())

        // make sure we'll retry on next poll
        assertEquals(0, coordinator.onJoinPrepareInvokes)
        assertEquals(0, coordinator.onJoinCompleteInvokes)
        mockClient.prepareResponse(
            joinGroupFollowerResponse(
                generationId = defaultGeneration,
                memberId = memberId,
                leaderId = JoinGroupRequest.UNKNOWN_MEMBER_ID,
                error = Errors.NONE,
            )
        )
        mockClient.prepareResponse(syncGroupResponse(Errors.NONE))
        coordinator.ensureActiveGroup()
        // make sure both onJoinPrepare and onJoinComplete got called
        assertEquals(1, coordinator.onJoinPrepareInvokes)
        assertEquals(1, coordinator.onJoinCompleteInvokes)
    }

    @Test
    fun testLeaveGroupSentWithGroupInstanceIdUnSet() {
        checkLeaveGroupRequestSent(null)
        checkLeaveGroupRequestSent("groupInstanceId")
    }

    private fun checkLeaveGroupRequestSent(groupInstanceId: String?) {
        setupCoordinator(RETRY_BACKOFF_MS, Int.MAX_VALUE, groupInstanceId)
        mockClient.prepareResponse(groupCoordinatorResponse(node, Errors.NONE))
        mockClient.prepareResponse(
            joinGroupFollowerResponse(
                generationId = 1,
                memberId = memberId,
                leaderId = leaderId,
                error = Errors.NONE,
            ),
        )
        mockClient.prepareResponse(syncGroupResponse(Errors.NONE))
        val e = RuntimeException()

        // raise the error when the coordinator tries to send leave group request.
        mockClient.prepareResponse(
            matcher = { body ->
                if (body is LeaveGroupRequest) throw e
                false
            },
            response = heartbeatResponse(Errors.UNKNOWN_SERVER_ERROR),
        )
        try {
            coordinator.ensureActiveGroup()
            coordinator.close()
            if (coordinator.isDynamicMember) fail("Expected leavegroup to raise an error.")
        } catch (exception: RuntimeException) {
            if (coordinator.isDynamicMember) {
                assertEquals(exception, e)
            } else {
                fail("Coordinator with group.instance.id set shouldn't send leave group request.")
            }
        }
    }

    @Test
    fun testHandleNormalLeaveGroupResponse() {
        val memberResponse = MemberResponse()
            .setMemberId(memberId)
            .setErrorCode(Errors.NONE.code)
        val response = leaveGroupResponse(listOf(memberResponse))
        val leaveGroupFuture = setupLeaveGroup(response)

        assertNotNull(leaveGroupFuture)
        assertTrue(leaveGroupFuture.succeeded())
    }

    @Test
    fun testHandleNormalLeaveGroupResponseAndTruncatedLeaveReason() {
        val memberResponse = MemberResponse()
            .setMemberId(memberId)
            .setErrorCode(Errors.NONE.code)
        val response = leaveGroupResponse(listOf(memberResponse))
        val leaveReason =
            "Very looooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooong leaveReason that is 271 characters long to make sure that length limit logic handles the scenario nicely"
        val leaveGroupFuture = setupLeaveGroup(response, leaveReason, leaveReason.substring(0, 255))

        assertNotNull(leaveGroupFuture)
        assertTrue(leaveGroupFuture.succeeded())
    }

    @Test
    fun testHandleMultipleMembersLeaveGroupResponse() {
        val memberResponse = MemberResponse()
            .setMemberId(memberId)
            .setErrorCode(Errors.NONE.code)
        val response = leaveGroupResponse(listOf(memberResponse, memberResponse))
        val leaveGroupFuture = setupLeaveGroup(response)

        assertNotNull(leaveGroupFuture)
        assertTrue(leaveGroupFuture.exception() is IllegalStateException)
    }

    @Test
    fun testHandleLeaveGroupResponseWithEmptyMemberResponse() {
        val response = leaveGroupResponse(emptyList())
        val leaveGroupFuture = setupLeaveGroup(response)

        assertNotNull(leaveGroupFuture)
        assertTrue(leaveGroupFuture.succeeded())
    }

    @Test
    fun testHandleLeaveGroupResponseWithException() {
        val memberResponse = MemberResponse()
            .setMemberId(memberId)
            .setErrorCode(Errors.UNKNOWN_MEMBER_ID.code)
        val response = leaveGroupResponse(listOf(memberResponse))
        val leaveGroupFuture = setupLeaveGroup(response)

        assertNotNull(leaveGroupFuture)
        assertIs<UnknownMemberIdException>(leaveGroupFuture.exception())
    }

    private fun setupLeaveGroup(
        leaveGroupResponse: LeaveGroupResponse,
        leaveReason: String = "test maybe leave group",
        expectedLeaveReason: String = "test maybe leave group",
    ): RequestFuture<Unit>? {
        setupCoordinator(RETRY_BACKOFF_MS, Int.MAX_VALUE, null)
        mockClient.prepareResponse(groupCoordinatorResponse(node, Errors.NONE))
        mockClient.prepareResponse(joinGroupFollowerResponse(1, memberId, leaderId, Errors.NONE))
        mockClient.prepareResponse(syncGroupResponse(Errors.NONE))
        mockClient.prepareResponse(
            matcher = { body ->
                if (body !is LeaveGroupRequest) return@prepareResponse false
                val leaveGroupRequest = body.data()

                leaveGroupRequest.members[0].memberId == memberId
                        && leaveGroupRequest.members[0].reason.equals(expectedLeaveReason)
            },
            response = leaveGroupResponse,
        )
        coordinator.ensureActiveGroup()
        return coordinator.maybeLeaveGroup(leaveReason)
    }

    @Test
    @Throws(Exception::class)
    fun testUncaughtExceptionInHeartbeatThread() {
        setupCoordinator()
        mockClient.prepareResponse(groupCoordinatorResponse(node, Errors.NONE))
        mockClient.prepareResponse(joinGroupFollowerResponse(1, memberId, leaderId, Errors.NONE))
        mockClient.prepareResponse(syncGroupResponse(Errors.NONE))
        val e = RuntimeException()

        // raise the error when the background thread tries to send a heartbeat
        mockClient.prepareResponse(
            matcher = { body ->
                if (body is HeartbeatRequest) throw e
                false
            },
            response = heartbeatResponse(Errors.UNKNOWN_SERVER_ERROR),
        )
        try {
            coordinator.ensureActiveGroup()
            mockTime.sleep(HEARTBEAT_INTERVAL_MS.toLong())
            val startMs = System.currentTimeMillis()

            while (System.currentTimeMillis() - startMs < 1000) {
                Thread.sleep(10)
                coordinator.pollHeartbeat(mockTime.milliseconds())
            }

            fail("Expected pollHeartbeat to raise an error in 1 second")
        } catch (exception: RuntimeException) {
            assertEquals(exception, e)
        }
    }

    @Test
    @Throws(Exception::class)
    fun testPollHeartbeatAwakesHeartbeatThread() {
        val longRetryBackoffMs = 10000
        setupCoordinator(longRetryBackoffMs)

        mockClient.prepareResponse(groupCoordinatorResponse(node, Errors.NONE))
        mockClient.prepareResponse(joinGroupFollowerResponse(1, memberId, leaderId, Errors.NONE))
        mockClient.prepareResponse(syncGroupResponse(Errors.NONE))

        coordinator.ensureActiveGroup()

        val heartbeatDone = CountDownLatch(1)
        mockClient.prepareResponse(
            matcher = { body ->
                heartbeatDone.countDown()
                body is HeartbeatRequest
            },
            response = heartbeatResponse(Errors.NONE),
        )
        mockTime.sleep(HEARTBEAT_INTERVAL_MS.toLong())
        coordinator.pollHeartbeat(mockTime.milliseconds())

        if (!heartbeatDone.await(1, TimeUnit.SECONDS)) {
            fail("Should have received a heartbeat request after calling pollHeartbeat")
        }
    }

    @Test
    fun testLookupCoordinator() {
        setupCoordinator()

        mockClient.backoff(node, 50)
        val noBrokersAvailableFuture: RequestFuture<Unit> = coordinator.lookupCoordinator()
        assertTrue(noBrokersAvailableFuture.failed(), "Failed future expected")
        mockTime.sleep(50)

        val future = coordinator.lookupCoordinator()
        assertFalse(future.isDone, "Request not sent")
        assertSame(
            expected = future,
            actual = coordinator.lookupCoordinator(),
            message = "New request sent while one is in progress",
        )

        mockClient.prepareResponse(groupCoordinatorResponse(node, Errors.NONE))
        coordinator.ensureCoordinatorReady(mockTime.timer(Long.MAX_VALUE))
        assertNotSame(
            illegal = future,
            actual = coordinator.lookupCoordinator(),
            message = "New request not sent after previous completed",
        )
    }

    @Test
    @Throws(Exception::class)
    fun testWakeupAfterJoinGroupSent() {
        setupCoordinator()

        mockClient.prepareResponse(groupCoordinatorResponse(node, Errors.NONE))
        mockClient.prepareResponse(
            matcher = object : RequestMatcher {
                private var invocations = 0
                override fun matches(body: AbstractRequest?): Boolean {
                    invocations++
                    val isJoinGroupRequest = body is JoinGroupRequest
                    if (isJoinGroupRequest && invocations == 1) // simulate wakeup before the request returns
                        throw WakeupException()
                    return isJoinGroupRequest
                }
            },
            response = joinGroupFollowerResponse(1, memberId, leaderId, Errors.NONE),
        )
        mockClient.prepareResponse(syncGroupResponse(Errors.NONE))
        val heartbeatReceived = prepareFirstHeartbeat()

        try {
            coordinator.ensureActiveGroup()
            fail("Should have woken up from ensureActiveGroup()")
        } catch (ignored: WakeupException) {}

        assertEquals(1, coordinator.onJoinPrepareInvokes)
        assertEquals(0, coordinator.onJoinCompleteInvokes)
        assertFalse(heartbeatReceived.get())
        coordinator.ensureActiveGroup()
        assertEquals(1, coordinator.onJoinPrepareInvokes)
        assertEquals(1, coordinator.onJoinCompleteInvokes)
        awaitFirstHeartbeat(heartbeatReceived)
    }

    @Test
    @Throws(Exception::class)
    fun testWakeupAfterJoinGroupSentExternalCompletion() {
        setupCoordinator()
        mockClient.prepareResponse(groupCoordinatorResponse(node, Errors.NONE))
        mockClient.prepareResponse(
            matcher = object : RequestMatcher {
                private var invocations = 0
                override fun matches(body: AbstractRequest?): Boolean {
                    invocations++
                    val isJoinGroupRequest = body is JoinGroupRequest
                    if (isJoinGroupRequest && invocations == 1) // simulate wakeup before the request returns
                        throw WakeupException()
                    return isJoinGroupRequest
                }
            },
            response = joinGroupFollowerResponse(1, memberId, leaderId, Errors.NONE),
        )
        mockClient.prepareResponse(syncGroupResponse(Errors.NONE))
        val heartbeatReceived = prepareFirstHeartbeat()

        try {
            coordinator.ensureActiveGroup()
            fail("Should have woken up from ensureActiveGroup()")
        } catch (ignored: WakeupException) {}

        assertEquals(1, coordinator.onJoinPrepareInvokes)
        assertEquals(0, coordinator.onJoinCompleteInvokes)
        assertFalse(heartbeatReceived.get())

        // the join group completes in this poll()
        consumerClient.poll(mockTime.timer(0))
        coordinator.ensureActiveGroup()

        assertEquals(1, coordinator.onJoinPrepareInvokes)
        assertEquals(1, coordinator.onJoinCompleteInvokes)

        awaitFirstHeartbeat(heartbeatReceived)
    }

    @Test
    @Throws(Exception::class)
    fun testWakeupAfterJoinGroupReceived() {
        setupCoordinator()

        mockClient.prepareResponse(groupCoordinatorResponse(node, Errors.NONE))
        mockClient.prepareResponse(
            matcher = { body ->
                val isJoinGroupRequest = body is JoinGroupRequest
                if (isJoinGroupRequest) consumerClient.wakeup() // wakeup after the request returns
                isJoinGroupRequest
            },
            response = joinGroupFollowerResponse(1, memberId, leaderId, Errors.NONE),
        )
        mockClient.prepareResponse(syncGroupResponse(Errors.NONE))
        val heartbeatReceived = prepareFirstHeartbeat()

        try {
            coordinator.ensureActiveGroup()
            fail("Should have woken up from ensureActiveGroup()")
        } catch (ignored: WakeupException) {}

        assertEquals(1, coordinator.onJoinPrepareInvokes)
        assertEquals(0, coordinator.onJoinCompleteInvokes)
        assertFalse(heartbeatReceived.get())

        coordinator.ensureActiveGroup()

        assertEquals(1, coordinator.onJoinPrepareInvokes)
        assertEquals(1, coordinator.onJoinCompleteInvokes)

        awaitFirstHeartbeat(heartbeatReceived)
    }

    @Test
    @Throws(Exception::class)
    fun testWakeupAfterJoinGroupReceivedExternalCompletion() {
        setupCoordinator()
        mockClient.prepareResponse(groupCoordinatorResponse(node, Errors.NONE))
        mockClient.prepareResponse(
            matcher = { body ->
                val isJoinGroupRequest = body is JoinGroupRequest
                if (isJoinGroupRequest) consumerClient.wakeup() // wakeup after the request returns
                isJoinGroupRequest
            },
            response = joinGroupFollowerResponse(1, memberId, leaderId, Errors.NONE),
        )
        mockClient.prepareResponse(syncGroupResponse(Errors.NONE))
        val heartbeatReceived = prepareFirstHeartbeat()

        try {
            coordinator.ensureActiveGroup()
            fail("Should have woken up from ensureActiveGroup()")
        } catch (e: WakeupException) {}

        assertEquals(1, coordinator.onJoinPrepareInvokes)
        assertEquals(0, coordinator.onJoinCompleteInvokes)
        assertFalse(heartbeatReceived.get())

        // the join group completes in this poll()
        consumerClient.poll(mockTime.timer(0))
        coordinator.ensureActiveGroup()

        assertEquals(1, coordinator.onJoinPrepareInvokes)
        assertEquals(1, coordinator.onJoinCompleteInvokes)
        awaitFirstHeartbeat(heartbeatReceived)
    }

    @Test
    @Throws(Exception::class)
    fun testWakeupAfterSyncGroupSentExternalCompletion() {
        setupCoordinator()
        mockClient.prepareResponse(groupCoordinatorResponse(node, Errors.NONE))
        mockClient.prepareResponse(joinGroupFollowerResponse(1, memberId, leaderId, Errors.NONE))
        mockClient.prepareResponse(
            matcher = object : RequestMatcher {
                private var invocations = 0
                override fun matches(body: AbstractRequest?): Boolean {
                    invocations++
                    val isSyncGroupRequest = body is SyncGroupRequest
                    if (isSyncGroupRequest && invocations == 1) // wakeup after the request returns
                        consumerClient.wakeup()
                    return isSyncGroupRequest
                }
            },
            response = syncGroupResponse(Errors.NONE),
        )
        val heartbeatReceived = prepareFirstHeartbeat()

        try {
            coordinator.ensureActiveGroup()
            fail("Should have woken up from ensureActiveGroup()")
        } catch (e: WakeupException) {}

        assertEquals(1, coordinator.onJoinPrepareInvokes)
        assertEquals(0, coordinator.onJoinCompleteInvokes)
        assertFalse(heartbeatReceived.get())

        // the join group completes in this poll()
        consumerClient.poll(mockTime.timer(0))
        coordinator.ensureActiveGroup()

        assertEquals(1, coordinator.onJoinPrepareInvokes)
        assertEquals(1, coordinator.onJoinCompleteInvokes)

        awaitFirstHeartbeat(heartbeatReceived)
    }

    @Test
    @Throws(Exception::class)
    fun testWakeupAfterSyncGroupReceived() {
        setupCoordinator()

        mockClient.prepareResponse(groupCoordinatorResponse(node, Errors.NONE))
        mockClient.prepareResponse(joinGroupFollowerResponse(1, memberId, leaderId, Errors.NONE))
        mockClient.prepareResponse(
            matcher = { body ->
                val isSyncGroupRequest = body is SyncGroupRequest
                if (isSyncGroupRequest) consumerClient.wakeup() // wakeup after the request returns
                isSyncGroupRequest
            },
            response = syncGroupResponse(Errors.NONE),
        )
        val heartbeatReceived = prepareFirstHeartbeat()

        try {
            coordinator.ensureActiveGroup()
            fail("Should have woken up from ensureActiveGroup()")
        } catch (ignored: WakeupException) {
        }

        assertEquals(1, coordinator.onJoinPrepareInvokes)
        assertEquals(0, coordinator.onJoinCompleteInvokes)
        assertFalse(heartbeatReceived.get())

        coordinator.ensureActiveGroup()

        assertEquals(1, coordinator.onJoinPrepareInvokes)
        assertEquals(1, coordinator.onJoinCompleteInvokes)

        awaitFirstHeartbeat(heartbeatReceived)
    }

    @Test
    @Throws(Exception::class)
    fun testWakeupAfterSyncGroupReceivedExternalCompletion() {
        setupCoordinator()

        mockClient.prepareResponse(groupCoordinatorResponse(node, Errors.NONE))
        mockClient.prepareResponse(joinGroupFollowerResponse(1, memberId, leaderId, Errors.NONE))
        mockClient.prepareResponse(
            matcher = { body ->
                val isSyncGroupRequest = body is SyncGroupRequest
                if (isSyncGroupRequest) consumerClient.wakeup() // wakeup after the request returns
                isSyncGroupRequest
            },
            response = syncGroupResponse(Errors.NONE),
        )
        val heartbeatReceived = prepareFirstHeartbeat()

        try {
            coordinator.ensureActiveGroup()
            fail("Should have woken up from ensureActiveGroup()")
        } catch (e: WakeupException) {}

        assertEquals(1, coordinator.onJoinPrepareInvokes)
        assertEquals(0, coordinator.onJoinCompleteInvokes)
        assertFalse(heartbeatReceived.get())

        coordinator.ensureActiveGroup()

        assertEquals(1, coordinator.onJoinPrepareInvokes)
        assertEquals(1, coordinator.onJoinCompleteInvokes)

        awaitFirstHeartbeat(heartbeatReceived)
    }

    @Test
    @Throws(Exception::class)
    fun testWakeupInOnJoinComplete() {
        setupCoordinator()

        coordinator.wakeupOnJoinComplete = true
        mockClient.prepareResponse(groupCoordinatorResponse(node, Errors.NONE))
        mockClient.prepareResponse(joinGroupFollowerResponse(1, memberId, leaderId, Errors.NONE))
        mockClient.prepareResponse(syncGroupResponse(Errors.NONE))
        val heartbeatReceived = prepareFirstHeartbeat()

        try {
            coordinator.ensureActiveGroup()
            fail("Should have woken up from ensureActiveGroup()")
        } catch (ignored: WakeupException) {
        }

        assertEquals(1, coordinator.onJoinPrepareInvokes)
        assertEquals(0, coordinator.onJoinCompleteInvokes)
        assertFalse(heartbeatReceived.get())

        // the join group completes in this poll()
        coordinator.wakeupOnJoinComplete = false
        consumerClient.poll(mockTime.timer(0))
        coordinator.ensureActiveGroup()

        assertEquals(1, coordinator.onJoinPrepareInvokes)
        assertEquals(1, coordinator.onJoinCompleteInvokes)

        awaitFirstHeartbeat(heartbeatReceived)
    }

    @Test
    fun testAuthenticationErrorInEnsureCoordinatorReady() {
        setupCoordinator()

        mockClient.createPendingAuthenticationError(node, 300)

        try {
            coordinator.ensureCoordinatorReady(mockTime.timer(Long.MAX_VALUE))
            fail("Expected an authentication error.")
        } catch (e: AuthenticationException) {
            // OK
        }
    }

    private fun prepareFirstHeartbeat(): AtomicBoolean {
        val heartbeatReceived = AtomicBoolean(false)
        mockClient.prepareResponse(
            matcher = { body ->
                val isHeartbeatRequest = body is HeartbeatRequest
                if (isHeartbeatRequest) heartbeatReceived.set(true)
                isHeartbeatRequest
            },
            response = heartbeatResponse(Errors.UNKNOWN_SERVER_ERROR),
        )
        return heartbeatReceived
    }

    @Throws(Exception::class)
    private fun awaitFirstHeartbeat(heartbeatReceived: AtomicBoolean) {
        mockTime.sleep(HEARTBEAT_INTERVAL_MS.toLong())
        waitForCondition(
            testCondition = { heartbeatReceived.get() },
            maxWaitMs = 3000,
            conditionDetails = "Should have received a heartbeat request after joining the group",
        )
    }

    private fun groupCoordinatorResponse(node: Node, error: Errors): FindCoordinatorResponse {
        return FindCoordinatorResponse.prepareResponse(error, GROUP_ID, node)
    }

    private fun heartbeatResponse(error: Errors): HeartbeatResponse {
        return HeartbeatResponse(HeartbeatResponseData().setErrorCode(error.code))
    }

    private fun joinGroupFollowerResponse(
        generationId: Int,
        memberId: String,
        leaderId: String,
        error: Errors,
        protocolType: String? = null,
    ): JoinGroupResponse {
        return JoinGroupResponse(
            data = JoinGroupResponseData()
                .setErrorCode(error.code)
                .setGenerationId(generationId)
                .setProtocolType(protocolType)
                .setProtocolName(PROTOCOL_NAME)
                .setMemberId(memberId)
                .setLeader(leaderId)
                .setMembers(emptyList()),
            version = ApiKeys.JOIN_GROUP.latestVersion(),
        )
    }

    private fun joinGroupResponse(error: Errors): JoinGroupResponse {
        return joinGroupFollowerResponse(
            generationId = JoinGroupRequest.UNKNOWN_GENERATION_ID,
            memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID,
            leaderId = JoinGroupRequest.UNKNOWN_MEMBER_ID,
            error = error,
        )
    }

    private fun syncGroupResponse(
        error: Errors,
        protocolType: String? = null,
        protocolName: String? = null,
    ): SyncGroupResponse {
        return SyncGroupResponse(
            SyncGroupResponseData()
                .setErrorCode(error.code)
                .setProtocolType(protocolType)
                .setProtocolName(protocolName)
                .setAssignment(ByteArray(0))
        )
    }

    private fun leaveGroupResponse(members: List<MemberResponse>): LeaveGroupResponse {
        return LeaveGroupResponse(
            LeaveGroupResponseData()
                .setErrorCode(Errors.NONE.code)
                .setMembers(members)
        )
    }

    class DummyCoordinator internal constructor(
        rebalanceConfig: GroupRebalanceConfig,
        client: ConsumerNetworkClient,
        metrics: Metrics,
        time: Time,
    ) :
        AbstractCoordinator(
            rebalanceConfig = rebalanceConfig,
            logContext = LogContext(),
            client = client,
            metrics = metrics,
            metricGrpPrefix = METRIC_GROUP_PREFIX,
            time = time,
        ) {
        var onJoinPrepareInvokes = 0
        var onJoinCompleteInvokes = 0
        var wakeupOnJoinComplete = false
        override fun protocolType(): String = PROTOCOL_TYPE

        override fun metadata(): JoinGroupRequestProtocolCollection {
            return JoinGroupRequestProtocolCollection(
                setOf(
                    JoinGroupRequestProtocol()
                        .setName(PROTOCOL_NAME)
                        .setMetadata(EMPTY_DATA.array())
                ).iterator()
            )
        }

        override fun onLeaderElected(
            leaderId: String,
            protocol: String,
            allMemberMetadata: List<JoinGroupResponseMember>,
            skipAssignment: Boolean
        ): Map<String, ByteBuffer> {
            return allMemberMetadata.associate { member -> member.memberId to EMPTY_DATA }
        }

        override fun onJoinPrepare(timer: Timer, generation: Int, memberId: String): Boolean {
            onJoinPrepareInvokes++
            return true
        }

        override fun onJoinComplete(
            generation: Int,
            memberId: String,
            protocol: String?,
            memberAssignment: ByteBuffer,
        ) {
            if (wakeupOnJoinComplete) throw WakeupException()
            onJoinCompleteInvokes++
        }
    }

    companion object {

        private val EMPTY_DATA = ByteBuffer.wrap(ByteArray(0))

        private const val REBALANCE_TIMEOUT_MS = 60000

        private const val SESSION_TIMEOUT_MS = 10000

        private const val HEARTBEAT_INTERVAL_MS = 3000

        private const val RETRY_BACKOFF_MS = 100

        private const val REQUEST_TIMEOUT_MS = 40000

        private const val GROUP_ID = "dummy-group"

        private const val METRIC_GROUP_PREFIX = "consumer"

        private const val PROTOCOL_TYPE = "dummy"

        private const val PROTOCOL_NAME = "dummy-subprotocol"
    }
}
