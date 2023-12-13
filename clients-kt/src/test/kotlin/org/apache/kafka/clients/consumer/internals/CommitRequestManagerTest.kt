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

import java.util.Properties
import java.util.concurrent.CompletableFuture
import java.util.stream.Stream
import org.apache.kafka.clients.ClientResponse
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.clients.consumer.internals.NetworkClientDelegate.UnsentRequest
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.Node
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.OffsetCommitRequest
import org.apache.kafka.common.requests.OffsetFetchRequest
import org.apache.kafka.common.requests.OffsetFetchResponse
import org.apache.kafka.common.requests.RequestHeader
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.utils.LogContext
import org.apache.kafka.common.utils.MockTime
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import org.mockito.kotlin.mock
import org.mockito.kotlin.whenever
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertTrue

class CommitRequestManagerTest {
    
    private lateinit var subscriptionState: SubscriptionState
    
    private lateinit var groupState: GroupState
    
    private lateinit var logContext: LogContext
    
    private lateinit var time: MockTime
    
    private lateinit var coordinatorRequestManager: CoordinatorRequestManager
    
    private lateinit var props: Properties
    
    private val mockedNode = Node(1, "host1", 9092)
    
    @BeforeEach
    fun setup() {
        logContext = LogContext()
        time = MockTime(0)
        subscriptionState = mock<SubscriptionState>()
        coordinatorRequestManager = mock<CoordinatorRequestManager>()
        groupState = GroupState(groupId = "group-1", groupInstanceId = null)
        
        props = Properties()
        props[ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG] = 100
        props[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java
        props[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java
    }

    @Test
    fun testPoll_SkipIfCoordinatorUnknown() {
        val commitRequestManger = create(autoCommitEnabled = false, autoCommitInterval = 0)
        assertPoll(coordinatorDiscovered = false, numRes = 0, manager = commitRequestManger)
        
        val offsets = mapOf(
            TopicPartition("t1", 0) to OffsetAndMetadata(0),
        )
        commitRequestManger.addOffsetCommitRequest(offsets)
        assertPoll(coordinatorDiscovered = false, numRes = 0, manager = commitRequestManger)
    }

    @Test
    fun testPoll_EnsureManualCommitSent() {
        val commitRequestManger = create(autoCommitEnabled = false, autoCommitInterval = 0)
        assertPoll(numRes = 0, manager = commitRequestManger)
        
        val offsets = mapOf(
            TopicPartition("t1", 0) to OffsetAndMetadata(0),
        )
        commitRequestManger.addOffsetCommitRequest(offsets)
        assertPoll(numRes = 1, manager = commitRequestManger)
    }

    @Test
    fun testPoll_EnsureAutocommitSent() {
        val commitRequestManger = create(autoCommitEnabled = true, autoCommitInterval = 100)
        assertPoll(numRes = 0, manager = commitRequestManger)
        
        val offsets = mapOf(
            TopicPartition("t1", 0) to OffsetAndMetadata(0),
        )
        commitRequestManger.updateAutoCommitTimer(time.milliseconds())
        whenever(subscriptionState.allConsumed()).thenReturn(offsets)
        time.sleep(100)
        commitRequestManger.updateAutoCommitTimer(time.milliseconds())
        assertPoll(numRes = 1, manager = commitRequestManger)
    }

    @Test
    fun testPoll_EnsureCorrectInflightRequestBufferSize() {
        val commitManager = create(autoCommitEnabled = false, autoCommitInterval = 100)
        whenever(coordinatorRequestManager.coordinator).thenReturn(mockedNode)

        // Create some offset commit requests
        val offsets1 = mapOf(
            TopicPartition(topic = "test", partition = 0) to OffsetAndMetadata(offset = 10L),
            TopicPartition(topic = "test", partition = 1) to OffsetAndMetadata(offset = 20L),
        )
        val offsets2 = mapOf(
            TopicPartition(topic = "test", partition = 3) to OffsetAndMetadata(offset = 20L),
            TopicPartition(topic = "test", partition = 4) to OffsetAndMetadata(offset = 20L),
        )

        // Add the requests to the CommitRequestManager and store their futures
        val commitFutures = listOf(
            commitManager.addOffsetCommitRequest(offsets1),
            commitManager.addOffsetCommitRequest(offsets2),
        )
        val fetchFutures = listOf(
            commitManager.addOffsetFetchRequest(setOf(TopicPartition(topic = "test", partition = 0))),
            commitManager.addOffsetFetchRequest(setOf(TopicPartition(topic = "test", partition = 1))),
        )

        // Poll the CommitRequestManager and verify that the inflightOffsetFetches size is correct
        val (_, unsentRequests) = commitManager.poll(time.milliseconds())
        assertEquals(4, unsentRequests.size)
        assertTrue(unsentRequests.any { it.requestBuilder is OffsetCommitRequest.Builder })
        assertTrue(unsentRequests.any { it.requestBuilder is OffsetFetchRequest.Builder })
        assertFalse(commitManager.pendingRequests.hasUnsentRequests())
        assertEquals(2, commitManager.pendingRequests.inflightOffsetFetches.size)

        // Verify that the inflight offset fetch requests have been removed from the pending request buffer
        commitFutures.forEach { it.complete(null) }
        fetchFutures.forEach { it.complete(null) }
        assertEquals(0, commitManager.pendingRequests.inflightOffsetFetches.size)
    }

    @Test
    fun testPoll_EnsureEmptyPendingRequestAfterPoll() {
        val commitRequestManger = create(autoCommitEnabled = true, autoCommitInterval = 100)
        whenever(coordinatorRequestManager.coordinator).thenReturn(mockedNode)
        commitRequestManger.addOffsetCommitRequest(emptyMap())
        assertEquals(1, commitRequestManger.unsentOffsetCommitRequests().size)
        commitRequestManger.poll(time.milliseconds())
        assertTrue(commitRequestManger.unsentOffsetCommitRequests().isEmpty())
        assertEmptyPendingRequests(commitRequestManger)
    }

    @Test
    fun testAutocommit_ResendAutocommitAfterException() {
        val commitRequestManger = create(autoCommitEnabled = true, autoCommitInterval = 100)
        time.sleep(100)
        commitRequestManger.updateAutoCommitTimer(time.milliseconds())
        val futures = assertPoll(numRes = 1, manager = commitRequestManger)
        time.sleep(99)
        // complete the autocommit request (exceptionally)
        futures[0].completeExceptionally(KafkaException("test exception"))

        // we can then autocommit again
        commitRequestManger.updateAutoCommitTimer(time.milliseconds())
        assertPoll(numRes = 0, manager = commitRequestManger)
        time.sleep(1)
        commitRequestManger.updateAutoCommitTimer(time.milliseconds())
        assertPoll(numRes = 1, manager = commitRequestManger)
        assertEmptyPendingRequests(commitRequestManger)
    }

    @Test
    fun testAutocommit_EnsureOnlyOneInflightRequest() {
        val commitRequestManger = create(autoCommitEnabled = true, autoCommitInterval = 100)
        time.sleep(100)
        commitRequestManger.updateAutoCommitTimer(time.milliseconds())
        val futures = assertPoll(numRes = 1, manager = commitRequestManger)
        time.sleep(100)
        commitRequestManger.updateAutoCommitTimer(time.milliseconds())
        // We want to make sure we don't resend autocommit if the previous request has not been completed
        assertPoll(numRes = 0, manager = commitRequestManger)
        assertEmptyPendingRequests(commitRequestManger)

        // complete the unsent request and re-poll
        futures[0].complete(null)
        assertPoll(numRes = 1, manager = commitRequestManger)
    }

    @Test
    fun testOffsetFetchRequest_EnsureDuplicatedRequestSucceed() {
        val commitRequestManger = create(true, 100)
        whenever(coordinatorRequestManager.coordinator).thenReturn(mockedNode)
        val partitions = setOf(TopicPartition(topic = "t1", partition = 0))
        val futures = sendAndVerifyDuplicatedRequests(
            commitRequestManger = commitRequestManger,
            partitions = partitions,
            numRequest = 2,
            error = Errors.NONE,
        )
        futures.forEach { future ->
            assertTrue(future.isDone)
            assertFalse(future.isCompletedExceptionally())
        }
        // expecting the buffers to be emptied after being completed successfully
        commitRequestManger.poll(currentTimeMs = 0)
        assertEmptyPendingRequests(commitRequestManger)
    }

    @ParameterizedTest
    @MethodSource("exceptionSupplier")
    fun testOffsetFetchRequest_ErroredRequests(error: Errors, isRetriable: Boolean) {
        val commitRequestManger = create(true, 100)
        whenever(coordinatorRequestManager.coordinator).thenReturn(mockedNode)
        val partitions = setOf(TopicPartition(topic = "t1", partition = 0))
        val futures = sendAndVerifyDuplicatedRequests(
            commitRequestManger = commitRequestManger,
            partitions = partitions,
            numRequest = 5,
            error = error,
        )
        // we only want to make sure to purge the outbound buffer for non-retriables, so retriable will be re-queued.
        if (isRetriable) testRetriable(commitRequestManger, futures)
        else {
            testNonRetriable(futures)
            assertEmptyPendingRequests(commitRequestManger)
        }
    }

    private fun testRetriable(
        commitRequestManger: CommitRequestManager,
        futures: List<CompletableFuture<Map<TopicPartition, OffsetAndMetadata>>>,
    ) {
        futures.forEach { future -> assertFalse(future.isDone) }
        time.sleep(500)
        commitRequestManger.poll(time.milliseconds())
        futures.forEach { future -> assertFalse(future.isDone) }
    }

    private fun testNonRetriable(futures: List<CompletableFuture<Map<TopicPartition, OffsetAndMetadata>>>) {
        futures.forEach { future -> assertTrue(future.isCompletedExceptionally()) }
    }

    @ParameterizedTest
    @MethodSource("partitionDataErrorSupplier")
    fun testOffsetFetchRequest_PartitionDataError(error: Errors?, isRetriable: Boolean) {
        val commitRequestManger = create(true, 100)
        whenever(coordinatorRequestManager.coordinator).thenReturn(mockedNode)
        val tp1 = TopicPartition(topic = "t1", partition = 2)
        val tp2 = TopicPartition(topic = "t2", partition = 3)
        val partitions = setOf(tp1, tp2)
        val future = commitRequestManger.addOffsetFetchRequest(partitions)
        val result = commitRequestManger.poll(time.milliseconds())
        assertEquals(1, result.unsentRequests.size)

        // Setting 1 partition with error
        val topicPartitionData = mapOf(
            tp1 to OffsetFetchResponse.PartitionData(
                offset = 100L,
                leaderEpoch = 1,
                metadata = "metadata",
                error = (error)!!
            ),
            tp2 to OffsetFetchResponse.PartitionData(
                offset = 100L,
                leaderEpoch = 1,
                metadata = "metadata",
                error = Errors.NONE
            ),
        )
        result.unsentRequests[0].future.complete(
            buildOffsetFetchClientResponse(
                request = result.unsentRequests[0],
                topicPartitionData = topicPartitionData,
                error = Errors.NONE,
            )
        )
        if (isRetriable) testRetriable(commitRequestManger, listOf(future))
        else testNonRetriable(listOf(future))
    }

    private fun sendAndVerifyDuplicatedRequests(
        commitRequestManger: CommitRequestManager,
        partitions: Set<TopicPartition>,
        numRequest: Int,
        error: Errors,
    ): List<CompletableFuture<Map<TopicPartition, OffsetAndMetadata>>> {
        val futures = List(numRequest) {
            commitRequestManger.addOffsetFetchRequest(partitions)
        }
        var res = commitRequestManger.poll(time.milliseconds())
        assertEquals(1, res.unsentRequests.size)
        res.unsentRequests[0].future.complete(
            buildOffsetFetchClientResponse(
                request = res.unsentRequests[0],
                topicPartitions = partitions,
                error = error,
            )
        )
        res = commitRequestManger.poll(time.milliseconds())
        assertEquals(0, res.unsentRequests.size)
        return futures
    }

    private fun assertPoll(
        numRes: Int,
        manager: CommitRequestManager,
        coordinatorDiscovered: Boolean = true,
    ): List<CompletableFuture<ClientResponse>> {
        if (coordinatorDiscovered) whenever(coordinatorRequestManager.coordinator).thenReturn(mockedNode)
        else whenever(coordinatorRequestManager.coordinator).thenReturn(null)

        val result = manager.poll(time.milliseconds())
        assertEquals(numRes, result.unsentRequests.size)
        return result.unsentRequests.map { it.future }
    }

    private fun create(autoCommitEnabled: Boolean, autoCommitInterval: Long): CommitRequestManager {
        props.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, autoCommitInterval.toString())
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, autoCommitEnabled.toString())
        return CommitRequestManager(
            time = time,
            logContext = logContext,
            subscriptionState = subscriptionState,
            config = ConsumerConfig(props),
            coordinatorRequestManager = coordinatorRequestManager,
            groupState = groupState,
        )
    }

    private fun buildOffsetFetchClientResponse(
        request: UnsentRequest,
        topicPartitions: Set<TopicPartition>,
        error: Errors,
    ): ClientResponse {
        val topicPartitionData = topicPartitions.associateWith {
            OffsetFetchResponse.PartitionData(
                offset = 100L,
                leaderEpoch = 1,
                metadata = "metadata",
                error = Errors.NONE,
            )
        }
        return buildOffsetFetchClientResponse(request, topicPartitionData, error)
    }

    private fun buildOffsetFetchClientResponse(
        request: UnsentRequest,
        topicPartitionData: Map<TopicPartition, OffsetFetchResponse.PartitionData>,
        error: Errors,
    ): ClientResponse {
        val offsetFetchRequest = request.requestBuilder.build()
        assertIs<OffsetFetchRequest>(offsetFetchRequest)

        val response = OffsetFetchResponse(error, topicPartitionData)
        return ClientResponse(
            requestHeader = RequestHeader(
                requestApiKey = ApiKeys.OFFSET_FETCH,
                requestVersion = offsetFetchRequest.version,
                clientId = "",
                correlationId = 1,
            ),
            callback = request.callback,
            destination = "-1",
            createdTimeMs = time.milliseconds(),
            receivedTimeMs = time.milliseconds(),
            disconnected = false,
            versionMismatch = null,
            authenticationException = null,
            responseBody = response,
        )
    }

    companion object {

        // Supplies (error, isRetriable)
        @JvmStatic
        private fun exceptionSupplier(): Stream<Arguments> {
            return Stream.of(
                Arguments.of(Errors.NOT_COORDINATOR, true),
                Arguments.of(Errors.COORDINATOR_LOAD_IN_PROGRESS, true),
                Arguments.of(Errors.UNKNOWN_SERVER_ERROR, false),
                Arguments.of(Errors.GROUP_AUTHORIZATION_FAILED, false),
                Arguments.of(Errors.TOPIC_AUTHORIZATION_FAILED, false)
            )
        }

        private fun assertEmptyPendingRequests(commitRequestManger: CommitRequestManager) {
            assertTrue(commitRequestManger.pendingRequests.inflightOffsetFetches.isEmpty())
            assertTrue(commitRequestManger.pendingRequests.unsentOffsetFetches.isEmpty())
            assertTrue(commitRequestManger.pendingRequests.unsentOffsetCommits.isEmpty())
        }

        // Supplies (error, isRetriable)
        @JvmStatic
        private fun partitionDataErrorSupplier(): Stream<Arguments> {
            return Stream.of(
                Arguments.of(Errors.UNSTABLE_OFFSET_COMMIT, true),
                Arguments.of(Errors.UNKNOWN_TOPIC_OR_PARTITION, false),
                Arguments.of(Errors.TOPIC_AUTHORIZATION_FAILED, false),
                Arguments.of(Errors.UNKNOWN_SERVER_ERROR, false)
            )
        }
    }
}
