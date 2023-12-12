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
import org.apache.kafka.clients.MockClient
import org.apache.kafka.clients.NetworkClient
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.Node
import org.apache.kafka.common.errors.AuthenticationException
import org.apache.kafka.common.errors.DisconnectException
import org.apache.kafka.common.errors.InvalidTopicException
import org.apache.kafka.common.errors.TimeoutException
import org.apache.kafka.common.errors.TopicAuthorizationException
import org.apache.kafka.common.errors.WakeupException
import org.apache.kafka.common.internals.ClusterResourceListeners
import org.apache.kafka.common.message.HeartbeatRequestData
import org.apache.kafka.common.message.HeartbeatResponseData
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.HeartbeatRequest
import org.apache.kafka.common.requests.HeartbeatResponse
import org.apache.kafka.common.requests.RequestTestUtils.metadataUpdateWith
import org.apache.kafka.common.utils.LogContext
import org.apache.kafka.common.utils.MockTime
import org.apache.kafka.test.TestUtils.singletonCluster
import org.junit.jupiter.api.Test
import java.time.Duration
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import org.mockito.kotlin.any
import org.mockito.kotlin.eq
import org.mockito.kotlin.mock
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertTrue

class ConsumerNetworkClientTest {

    private val topicName = "test"

    private val time = MockTime(1)

    private val cluster = singletonCluster(topic = topicName, partitions = 1)

    private val node = cluster.nodes[0]

    private val metadata = Metadata(
        refreshBackoffMs = 100,
        metadataExpireMs = 50000,
        logContext = LogContext(),
        clusterResourceListeners = ClusterResourceListeners(),
    )

    private var client = MockClient(time, metadata)

    private var consumerClient = ConsumerNetworkClient(
        logContext = LogContext(),
        client = client,
        metadata = metadata,
        time = time,
        retryBackoffMs = 100,
        requestTimeoutMs = 1000,
        maxPollTimeoutMs = Int.MAX_VALUE,
    )

    @Test
    fun send() {
        client.prepareResponse(heartbeatResponse(Errors.NONE))
        val future = consumerClient.send(node, heartbeat())

        assertEquals(1, consumerClient.pendingRequestCount())
        assertEquals(1, consumerClient.pendingRequestCount(node))
        assertFalse(future.isDone)

        consumerClient.poll(future)

        assertTrue(future.isDone)
        assertTrue(future.succeeded())

        val clientResponse = future.value()
        val response = assertIs<HeartbeatResponse>(clientResponse.responseBody)
        assertEquals(Errors.NONE, response.error())
    }

    @Test
    fun sendWithinBackoffPeriodAfterAuthenticationFailure() {
        client.authenticationFailed(node, 300)
        client.prepareResponse(heartbeatResponse(Errors.NONE))
        val future = consumerClient.send(node, heartbeat())
        consumerClient.poll(future)

        assertTrue(future.failed())
        assertIs<AuthenticationException>(future.exception(), "Expected only an authentication error.")

        time.sleep(30) // wait less than the backoff period
        assertTrue(client.connectionFailed(node))

        val future2 = consumerClient.send(node, heartbeat())
        consumerClient.poll(future2)
        assertTrue(future2.failed())
        assertIs<AuthenticationException>(future2.exception(), "Expected only an authentication error.")
    }

    @Test
    fun multiSend() {
        client.prepareResponse(heartbeatResponse(Errors.NONE))
        client.prepareResponse(heartbeatResponse(Errors.NONE))
        val future1 = consumerClient.send(node, heartbeat())
        val future2 = consumerClient.send(node, heartbeat())
        assertEquals(2, consumerClient.pendingRequestCount())
        assertEquals(2, consumerClient.pendingRequestCount(node))
        consumerClient.awaitPendingRequests(node, time.timer(Long.MAX_VALUE))
        assertTrue(future1.succeeded())
        assertTrue(future2.succeeded())
    }

    @Test
    fun testDisconnectWithUnsentRequests() {
        val future = consumerClient.send(node, heartbeat())
        assertTrue(consumerClient.hasPendingRequests(node))
        assertFalse(client.hasInFlightRequests(node.idString()))

        consumerClient.disconnectAsync(node)
        consumerClient.pollNoWakeup()

        assertTrue(future.failed())
        assertIs<DisconnectException>(future.exception())
    }

    @Test
    fun testDisconnectWithInFlightRequests() {
        val future = consumerClient.send(node, heartbeat())
        consumerClient.pollNoWakeup()
        assertTrue(consumerClient.hasPendingRequests(node))
        assertTrue(client.hasInFlightRequests(node.idString()))

        consumerClient.disconnectAsync(node)
        consumerClient.pollNoWakeup()
        assertTrue(future.failed())
        assertIs<DisconnectException>(future.exception())
    }

    @Test
    fun testTimeoutUnsentRequest() {
        // Delay connection to the node so that the request remains unsent
        client.delayReady(node, 1000)
        val future = consumerClient.send(node, heartbeat(), 500)
        consumerClient.pollNoWakeup()

        // Ensure the request is pending, but hasn't been sent
        assertTrue(consumerClient.hasPendingRequests())
        assertFalse(client.hasInFlightRequests())

        time.sleep(501)
        consumerClient.pollNoWakeup()

        assertFalse(consumerClient.hasPendingRequests())
        assertTrue(future.failed())
        assertIs<TimeoutException>(future.exception())
    }

    @Test
    fun doNotBlockIfPollConditionIsSatisfied() {
        val mockNetworkClient = mock<NetworkClient>()
        val consumerClient = ConsumerNetworkClient(
            logContext = LogContext(),
            client = mockNetworkClient,
            metadata = metadata,
            time = time,
            retryBackoffMs = 100,
            requestTimeoutMs = 1000,
            maxPollTimeoutMs = Int.MAX_VALUE,
        )

        // expect poll, but with no timeout
        consumerClient.poll(time.timer(Long.MAX_VALUE), { false })
        verify(mockNetworkClient).poll(eq(0L), any())
    }

    @Test
    fun blockWhenPollConditionNotSatisfied() {
        val timeout = 4000L
        val mockNetworkClient = mock<NetworkClient>()
        val consumerClient = ConsumerNetworkClient(
            logContext = LogContext(),
            client = mockNetworkClient,
            metadata = metadata,
            time = time,
            retryBackoffMs = 100,
            requestTimeoutMs = 1000,
            maxPollTimeoutMs = Int.MAX_VALUE,
        )

        whenever(mockNetworkClient.inFlightRequestCount()).thenReturn(1)
        consumerClient.poll(time.timer(timeout), { true })
        verify(mockNetworkClient).poll(eq(timeout), any())
    }

    @Test
    fun blockOnlyForRetryBackoffIfNoInflightRequests() {
        val retryBackoffMs = 100L
        val mockNetworkClient = mock<NetworkClient>()
        val consumerClient = ConsumerNetworkClient(
            logContext = LogContext(),
            client = mockNetworkClient,
            metadata = metadata,
            time = time,
            retryBackoffMs = retryBackoffMs,
            requestTimeoutMs = 1000,
            maxPollTimeoutMs = Int.MAX_VALUE,
        )

        whenever(mockNetworkClient.inFlightRequestCount()).thenReturn(0)
        consumerClient.poll(time.timer(Long.MAX_VALUE), { true })
        verify(mockNetworkClient).poll(eq(retryBackoffMs), any())
    }

    @Test
    fun wakeup() {
        val future = consumerClient.send(node, heartbeat())
        consumerClient.wakeup()

        assertFailsWith<WakeupException> { consumerClient.poll(time.timer(0)) }

        client.respond(heartbeatResponse(Errors.NONE))
        consumerClient.poll(future)

        assertTrue(future.isDone)
    }

    @Test
    @Throws(Exception::class)
    fun testDisconnectWakesUpPoll() {
        val future = consumerClient.send(node, heartbeat())
        client.enableBlockingUntilWakeup(1)
        val t = Thread { consumerClient.poll(future) }
        t.start()
        consumerClient.disconnectAsync(node)
        t.join()

        assertTrue(future.failed())
        assertIs<DisconnectException>(future.exception())
    }

    @Test
    fun testAuthenticationExceptionPropagatedFromMetadata() {
        metadata.fatalError(AuthenticationException("Authentication failed"))
        assertFailsWith<AuthenticationException>(
            message = "Expected authentication error thrown",
        ) { consumerClient.poll(time.timer(Duration.ZERO))}

        // After the exception is raised, it should have been cleared
        metadata.maybeThrowAnyException()
    }

    @Test
    fun testInvalidTopicExceptionPropagatedFromMetadata() {
        val metadataResponse = metadataUpdateWith(
            clusterId = "clusterId",
            numNodes = 1,
            topicErrors = mapOf("topic" to Errors.INVALID_TOPIC_EXCEPTION),
            topicPartitionCounts = emptyMap(),
        )
        metadata.updateWithCurrentRequestVersion(
            response = metadataResponse,
            isPartialUpdate = false,
            nowMs = time.milliseconds(),
        )

        assertFailsWith<InvalidTopicException> { consumerClient.poll(time.timer(Duration.ZERO)) }
    }

    @Test
    fun testTopicAuthorizationExceptionPropagatedFromMetadata() {
        val metadataResponse = metadataUpdateWith(
            clusterId = "clusterId",
            numNodes = 1,
            topicErrors = mapOf("topic" to Errors.TOPIC_AUTHORIZATION_FAILED),
            topicPartitionCounts = emptyMap(),
        )
        metadata.updateWithCurrentRequestVersion(
            response = metadataResponse,
            isPartialUpdate = false,
            nowMs = time.milliseconds(),
        )
        assertFailsWith<TopicAuthorizationException> { consumerClient.poll(time.timer(Duration.ZERO)) }
    }

    @Test
    fun testMetadataFailurePropagated() {
        val metadataException = KafkaException()
        metadata.fatalError(metadataException)

        val error = assertFailsWith<Exception>(
            message = "Expected poll to throw exception",
        ) { consumerClient.poll(time.timer(Duration.ZERO)) }
        assertEquals(metadataException, error)
    }

    @Test
    @Throws(Exception::class)
    fun testFutureCompletionOutsidePoll() {
        // Tests the scenario in which the request that is being awaited in one thread
        // is received and completed in another thread.
        val future = consumerClient.send(node, heartbeat())
        consumerClient.pollNoWakeup() // dequeue and send the request
        client.enableBlockingUntilWakeup(2)
        val t1 = Thread { consumerClient.pollNoWakeup() }
        t1.start()

        // Sleep a little so that t1 is blocking in poll
        Thread.sleep(50)
        val t2 = Thread { consumerClient.poll(future) }
        t2.start()

        // Sleep a little so that t2 is awaiting the network client lock
        Thread.sleep(50)

        // Simulate a network response and return from the poll in t1
        client.respond(heartbeatResponse(Errors.NONE))
        client.wakeup()

        // Both threads should complete since t1 should wakeup t2
        t1.join()
        t2.join()
        assertTrue(future.succeeded())
    }

    @Test
    fun testAwaitForMetadataUpdateWithTimeout() =
        assertFalse(consumerClient.awaitMetadataUpdate(time.timer(10L)))

    @Test
    fun sendExpiry() {
        val requestTimeoutMs = 10
        val isReady = AtomicBoolean()
        val disconnected = AtomicBoolean()
        client = object : MockClient(time, metadata) {
            override fun ready(node: Node, now: Long): Boolean =
                if (isReady.get()) super.ready(node, now) else false

            override fun connectionFailed(node: Node): Boolean = disconnected.get()
        }
        // Queue first send, sleep long enough for this to expire and then queue second send
        consumerClient = ConsumerNetworkClient(
            logContext = LogContext(),
            client = client,
            metadata = metadata,
            time = time,
            retryBackoffMs = 100,
            requestTimeoutMs = requestTimeoutMs,
            maxPollTimeoutMs = Int.MAX_VALUE,
        )
        val future1 = consumerClient.send(node, heartbeat())

        assertEquals(1, consumerClient.pendingRequestCount())
        assertEquals(1, consumerClient.pendingRequestCount(node))
        assertFalse(future1.isDone)

        time.sleep((requestTimeoutMs + 1).toLong())
        val future2 = consumerClient.send(node, heartbeat())

        assertEquals(2, consumerClient.pendingRequestCount())
        assertEquals(2, consumerClient.pendingRequestCount(node))
        assertFalse(future2.isDone)

        // First send should have expired and second send still pending
        consumerClient.poll(time.timer(0))

        assertTrue(future1.isDone)
        assertFalse(future1.succeeded())
        assertEquals(1, consumerClient.pendingRequestCount())
        assertEquals(1, consumerClient.pendingRequestCount(node))
        assertFalse(future2.isDone)

        // Enable send, the un-expired send should succeed on poll
        isReady.set(true)
        client.prepareResponse(heartbeatResponse(Errors.NONE))
        consumerClient.poll(future2)
        val clientResponse = future2.value()
        val response = assertIs<HeartbeatResponse>(clientResponse.responseBody)

        assertEquals(Errors.NONE, response.error())

        // Disable ready flag to delay send and queue another send. Disconnection should remove pending send
        isReady.set(false)
        val future3 = consumerClient.send(node, heartbeat())

        assertEquals(1, consumerClient.pendingRequestCount())
        assertEquals(1, consumerClient.pendingRequestCount(node))

        disconnected.set(true)
        consumerClient.poll(time.timer(0))

        assertTrue(future3.isDone)
        assertFalse(future3.succeeded())
        assertEquals(0, consumerClient.pendingRequestCount())
        assertEquals(0, consumerClient.pendingRequestCount(node))
    }

    @Test
    fun testTrySend() {
        val isReady = AtomicBoolean()
        val checkCount = AtomicInteger()
        client = object : MockClient(time, metadata) {
            override fun ready(node: Node, now: Long): Boolean {
                checkCount.incrementAndGet()
                return if (isReady.get()) super.ready(node, now) else false
            }
        }
        consumerClient = ConsumerNetworkClient(
            logContext = LogContext(),
            client = client,
            metadata = metadata,
            time = time,
            retryBackoffMs = 100,
            requestTimeoutMs = 10,
            maxPollTimeoutMs = Int.MAX_VALUE,
        )
        consumerClient.send(node, heartbeat())
        consumerClient.send(node, heartbeat())

        assertEquals(2, consumerClient.pendingRequestCount(node))
        assertEquals(0, client.inFlightRequestCount(node.idString()))

        consumerClient.trySend(time.milliseconds())

        // only check one time when the node doesn't ready
        assertEquals(1, checkCount.getAndSet(0))
        assertEquals(2, consumerClient.pendingRequestCount(node))
        assertEquals(0, client.inFlightRequestCount(node.idString()))

        isReady.set(true)
        consumerClient.trySend(time.milliseconds())

        // check node ready or not for every request
        assertEquals(2, checkCount.getAndSet(0))
        assertEquals(2, consumerClient.pendingRequestCount(node))
        assertEquals(2, client.inFlightRequestCount(node.idString()))
    }

    private fun heartbeat(): HeartbeatRequest.Builder = HeartbeatRequest.Builder(
        HeartbeatRequestData()
            .setGroupId("group")
            .setGenerationId(1)
            .setMemberId("memberId")
    )

    private fun heartbeatResponse(error: Errors): HeartbeatResponse =
        HeartbeatResponse(HeartbeatResponseData().setErrorCode(error.code))
}
