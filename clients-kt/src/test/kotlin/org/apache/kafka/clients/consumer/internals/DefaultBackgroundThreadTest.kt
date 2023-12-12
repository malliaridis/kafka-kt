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
import java.util.concurrent.BlockingQueue
import java.util.concurrent.LinkedBlockingQueue
import org.apache.kafka.clients.GroupRebalanceConfig
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.clients.consumer.internals.NetworkClientDelegate.PollResult
import org.apache.kafka.clients.consumer.internals.NetworkClientDelegate.UnsentRequest
import org.apache.kafka.clients.consumer.internals.events.ApplicationEvent
import org.apache.kafka.clients.consumer.internals.events.ApplicationEventProcessor
import org.apache.kafka.clients.consumer.internals.events.AssignmentChangeApplicationEvent
import org.apache.kafka.clients.consumer.internals.events.BackgroundEvent
import org.apache.kafka.clients.consumer.internals.events.CommitApplicationEvent
import org.apache.kafka.clients.consumer.internals.events.NewTopicsMetadataUpdateRequestEvent
import org.apache.kafka.clients.consumer.internals.events.NoopApplicationEvent
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.message.FindCoordinatorRequestData
import org.apache.kafka.common.requests.FindCoordinatorRequest
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.utils.LogContext
import org.apache.kafka.common.utils.MockTime
import org.apache.kafka.common.utils.Time
import org.apache.kafka.test.TestUtils
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.kotlin.any
import org.mockito.kotlin.mock
import org.mockito.kotlin.spy
import org.mockito.kotlin.times
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import kotlin.test.assertEquals
import kotlin.test.assertFalse

class DefaultBackgroundThreadTest {
    
    private val properties = Properties()
    
    private lateinit var time: MockTime
    
    private lateinit  var metadata: ConsumerMetadata
    
    private lateinit var networkClient: NetworkClientDelegate
    
    private lateinit var backgroundEventsQueue: BlockingQueue<BackgroundEvent>
    
    private lateinit var applicationEventsQueue: BlockingQueue<ApplicationEvent>
    
    private lateinit var processor: ApplicationEventProcessor
    
    private lateinit var coordinatorManager: CoordinatorRequestManager
    
    private lateinit var errorEventHandler: ErrorEventHandler
    
    private lateinit var subscriptionState: SubscriptionState
    
    private val requestTimeoutMs = 500
    
    private lateinit var groupState: GroupState
    
    private lateinit var commitManager: CommitRequestManager
    
    @BeforeEach
    fun setup() {
        time = MockTime(0)
        metadata = mock<ConsumerMetadata>()
        networkClient = mock<NetworkClientDelegate>()
        applicationEventsQueue = mock<BlockingQueue<ApplicationEvent>>()
        backgroundEventsQueue = mock<BlockingQueue<BackgroundEvent>>()
        processor = mock<ApplicationEventProcessor>()
        coordinatorManager = mock<CoordinatorRequestManager>()
        errorEventHandler = mock<ErrorEventHandler>()
        subscriptionState = mock<SubscriptionState>()
        val rebalanceConfig = GroupRebalanceConfig(
            sessionTimeoutMs = 100,
            rebalanceTimeoutMs = 100,
            heartbeatIntervalMs = 100,
            groupId = "group_id",
            groupInstanceId = null,
            retryBackoffMs = 100,
            leaveGroupOnClose = true,
        )
        groupState = GroupState(rebalanceConfig)
        commitManager = mock<CommitRequestManager>()
        properties[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java
        properties[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java
        properties[ConsumerConfig.RETRY_BACKOFF_MS_CONFIG] = RETRY_BACKOFF_MS
    }

    @Test
    @Throws(InterruptedException::class)
    fun testStartupAndTearDown() {
        whenever(coordinatorManager.poll(any())).thenReturn(mockPollCoordinatorResult())
        whenever(commitManager.poll(any())).thenReturn(mockPollCommitResult())
        
        val backgroundThread = mockBackgroundThread()
        backgroundThread.start()
        TestUtils.waitForCondition(
            testCondition = backgroundThread::isRunning,
            conditionDetails = "Failed awaiting for the background thread to be running",
        )
        backgroundThread.close()
        assertFalse(backgroundThread.isRunning)
    }

    @Test
    fun testApplicationEvent() {
        applicationEventsQueue = LinkedBlockingQueue()
        backgroundEventsQueue = LinkedBlockingQueue()
        
        whenever(coordinatorManager.poll(any())).thenReturn(mockPollCoordinatorResult())
        whenever(commitManager.poll(any())).thenReturn(mockPollCommitResult())
        
        val backgroundThread = mockBackgroundThread()
        val event = NoopApplicationEvent("noop event")
        applicationEventsQueue.add(event)
        backgroundThread.runOnce()
        verify(processor, times(1)).process(event)
        backgroundThread.close()
    }

    @Test
    fun testMetadataUpdateEvent() {
        applicationEventsQueue = LinkedBlockingQueue()
        backgroundEventsQueue = LinkedBlockingQueue()
        processor = ApplicationEventProcessor(
            backgroundEventQueue = backgroundEventsQueue,
            registry = mockRequestManagerRegistry(),
            metadata = metadata,
        )
        
        whenever(coordinatorManager.poll(any())).thenReturn(mockPollCoordinatorResult())
        whenever(commitManager.poll(any())).thenReturn(mockPollCommitResult())
        
        val backgroundThread = mockBackgroundThread()
        val event = NewTopicsMetadataUpdateRequestEvent()
        applicationEventsQueue.add(event)
        backgroundThread.runOnce()
        verify(metadata).requestUpdateForNewTopics()
        backgroundThread.close()
    }

    @Test
    fun testCommitEvent() {
        applicationEventsQueue = LinkedBlockingQueue()
        backgroundEventsQueue = LinkedBlockingQueue()
        
        whenever(coordinatorManager.poll(any())).thenReturn(mockPollCoordinatorResult())
        whenever(commitManager.poll(any())).thenReturn(mockPollCommitResult())
        
        val backgroundThread = mockBackgroundThread()
        val event = CommitApplicationEvent(HashMap())
        applicationEventsQueue.add(event)
        backgroundThread.runOnce()
        verify(processor).process(any<CommitApplicationEvent>())
        backgroundThread.close()
    }

    @Test
    fun testAssignmentChangeEvent() {
        applicationEventsQueue = LinkedBlockingQueue()
        backgroundEventsQueue = LinkedBlockingQueue()
        processor = spy(
            ApplicationEventProcessor(
                backgroundEventsQueue,
                mockRequestManagerRegistry(),
                metadata
            )
        )
        val backgroundThread: DefaultBackgroundThread = mockBackgroundThread()
        val offset = mockTopicPartitionOffset()
        val currentTimeMs = time.milliseconds()
        val event = AssignmentChangeApplicationEvent(offset, currentTimeMs)
        applicationEventsQueue.add(event)
        
        whenever(coordinatorManager.poll(any())).thenReturn(mockPollCoordinatorResult())
        whenever(commitManager.poll(any())).thenReturn(mockPollCommitResult())
        
        backgroundThread.runOnce()
        verify(processor).process(any<AssignmentChangeApplicationEvent>())
        verify(networkClient, times(1)).poll(any(), any())
        verify(commitManager, times(1)).updateAutoCommitTimer(currentTimeMs)
        verify(commitManager, times(1)).maybeAutoCommit(offset)
        backgroundThread.close()
    }

    @Test
    fun testFindCoordinator() {
        val backgroundThread = mockBackgroundThread()
        
        whenever(coordinatorManager.poll(any())).thenReturn(mockPollCoordinatorResult())
        whenever(commitManager.poll(any())).thenReturn(mockPollCommitResult())
        
        backgroundThread.runOnce()
        verify(coordinatorManager, times(1)).poll(any())
        verify(networkClient, times(1)).poll(any(), any())
        backgroundThread.close()
    }

    @Test
    fun testPollResultTimer() {
        val backgroundThread = mockBackgroundThread()
        // purposely setting a non MAX time to ensure it is returning Long.MAX_VALUE upon success
        val success = PollResult(
            timeUntilNextPollMs = 10,
            unsentRequests = listOf(findCoordinatorUnsentRequest(time, requestTimeoutMs.toLong())),
        )
        assertEquals(10, backgroundThread.handlePollResult(success))
        val failure = PollResult(timeUntilNextPollMs = 10, unsentRequests = emptyList())
        assertEquals(10, backgroundThread.handlePollResult(failure))
    }

    private fun mockTopicPartitionOffset(): Map<TopicPartition, OffsetAndMetadata> {
        return mapOf(
            TopicPartition(topic = "t0", partition = 2) to OffsetAndMetadata(10L),
            TopicPartition(topic = "t0", partition = 3) to OffsetAndMetadata(20L),
        )
    }

    private fun mockRequestManagerRegistry(): Map<RequestManager.Type, RequestManager?> {
        return mapOf(
            RequestManager.Type.COORDINATOR to coordinatorManager,
            RequestManager.Type.COMMIT to commitManager,
        )
    }

    private fun mockBackgroundThread(): DefaultBackgroundThread = DefaultBackgroundThread(
        time = time,
        config = ConsumerConfig(properties),
        logContext = LogContext(),
        applicationEventQueue = applicationEventsQueue,
        backgroundEventQueue = backgroundEventsQueue,
        subscriptionState = subscriptionState,
        errorEventHandler = errorEventHandler,
        processor = processor,
        metadata = metadata,
        networkClient = networkClient,
        groupState = groupState,
        coordinatorManager = coordinatorManager,
        commitRequestManager = commitManager,
    )

    private fun mockPollCoordinatorResult(): PollResult = PollResult(
        timeUntilNextPollMs = RETRY_BACKOFF_MS,
        unsentRequests = listOf(
            findCoordinatorUnsentRequest(
                time = time,
                timeout = requestTimeoutMs.toLong()
            ),
        ),
    )

    private fun mockPollCommitResult(): PollResult = PollResult(
        timeUntilNextPollMs = RETRY_BACKOFF_MS,
        unsentRequests = listOf(
            findCoordinatorUnsentRequest(
                time = time,
                timeout = requestTimeoutMs.toLong(),
            ),
        ),
    )

    companion object {
        
        private const val RETRY_BACKOFF_MS: Long = 100

        private fun findCoordinatorUnsentRequest(
            time: Time,
            timeout: Long,
        ): UnsentRequest = UnsentRequest(
            requestBuilder = FindCoordinatorRequest.Builder(
                FindCoordinatorRequestData()
                    .setKeyType(FindCoordinatorRequest.CoordinatorType.TRANSACTION.id)
                    .setKey("foobar")
            ),
            node = null,
        ).apply { setTimer(time, timeout) }
    }
}
