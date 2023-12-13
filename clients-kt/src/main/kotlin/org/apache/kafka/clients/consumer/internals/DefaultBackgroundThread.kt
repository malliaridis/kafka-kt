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

import java.util.LinkedList
import java.util.Objects
import java.util.Queue
import java.util.concurrent.BlockingQueue
import org.apache.kafka.clients.ApiVersions
import org.apache.kafka.clients.ClientUtils.createNetworkClient
import org.apache.kafka.clients.GroupRebalanceConfig
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.internals.ConsumerUtils.CONSUMER_MAX_INFLIGHT_REQUESTS_PER_CONNECTION
import org.apache.kafka.clients.consumer.internals.ConsumerUtils.CONSUMER_METRIC_GROUP_PREFIX
import org.apache.kafka.clients.consumer.internals.NetworkClientDelegate.PollResult
import org.apache.kafka.clients.consumer.internals.events.ApplicationEvent
import org.apache.kafka.clients.consumer.internals.events.ApplicationEventProcessor
import org.apache.kafka.clients.consumer.internals.events.BackgroundEvent
import org.apache.kafka.clients.consumer.internals.events.NoopApplicationEvent
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.errors.WakeupException
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.metrics.Sensor
import org.apache.kafka.common.utils.KafkaThread
import org.apache.kafka.common.utils.LogContext
import org.apache.kafka.common.utils.Time
import org.apache.kafka.common.utils.Utils.closeQuietly
import org.slf4j.Logger

/**
 * Background thread runnable that consumes `ApplicationEvent` and produces `BackgroundEvent`. It
 * uses an event loop to consume and produce events, and poll the network client to handle network
 * IO.
 *
 * It holds a reference to the [SubscriptionState], which is initialized by the polling thread.
 */
class DefaultBackgroundThread : KafkaThread {

    private val time: Time

    private val config: ConsumerConfig

    private val applicationEventQueue: BlockingQueue<ApplicationEvent>

    private val backgroundEventQueue: BlockingQueue<BackgroundEvent>

    private val metadata: ConsumerMetadata?

    // empty if groupId is null
    private val applicationEventProcessor: ApplicationEventProcessor

    private val networkClientDelegate: NetworkClientDelegate

    private var log: Logger

    private val errorEventHandler: ErrorEventHandler

    private val groupState: GroupState

    private val subscriptionState: SubscriptionState

    var isRunning = false
        private set

    private val requestManagerRegistry: Map<RequestManager.Type, RequestManager?>

    internal constructor(
        time: Time,
        config: ConsumerConfig,
        logContext: LogContext,
        applicationEventQueue: BlockingQueue<ApplicationEvent>,
        backgroundEventQueue: BlockingQueue<BackgroundEvent>,
        subscriptionState: SubscriptionState,
        errorEventHandler: ErrorEventHandler,
        processor: ApplicationEventProcessor,
        metadata: ConsumerMetadata,
        networkClient: NetworkClientDelegate,
        groupState: GroupState,
        coordinatorManager: CoordinatorRequestManager,
        commitRequestManager: CommitRequestManager,
    ) : super(BACKGROUND_THREAD_NAME, true) {
        this.time = time
        this.isRunning = true
        this.log = logContext.logger(javaClass)
        this.applicationEventQueue = applicationEventQueue
        this.backgroundEventQueue = backgroundEventQueue
        this.applicationEventProcessor = processor
        this.config = config
        this.metadata = metadata
        this.networkClientDelegate = networkClient
        this.errorEventHandler = errorEventHandler
        this.groupState = groupState
        this.subscriptionState = subscriptionState

        this.requestManagerRegistry = mapOf(
            RequestManager.Type.COORDINATOR to coordinatorManager,
            RequestManager.Type.COMMIT to commitRequestManager,
        )
    }

    constructor(
        time: Time,
        config: ConsumerConfig,
        rebalanceConfig: GroupRebalanceConfig,
        logContext: LogContext,
        applicationEventQueue: BlockingQueue<ApplicationEvent>,
        backgroundEventQueue: BlockingQueue<BackgroundEvent>,
        metadata: ConsumerMetadata,
        subscriptionState: SubscriptionState,
        apiVersions: ApiVersions,
        metrics: Metrics,
        fetcherThrottleTimeSensor: Sensor?,
    ) : super(BACKGROUND_THREAD_NAME, true) {
        try {
            this.time = time
            this.log = logContext.logger(javaClass)
            this.applicationEventQueue = applicationEventQueue
            this.backgroundEventQueue = backgroundEventQueue
            this.subscriptionState = subscriptionState
            this.config = config
            this.metadata = metadata
            val networkClient = createNetworkClient(
                config = config,
                metrics = metrics,
                metricsGroupPrefix = CONSUMER_METRIC_GROUP_PREFIX,
                logContext = logContext,
                apiVersions = apiVersions,
                time = time,
                maxInFlightRequestsPerConnection = CONSUMER_MAX_INFLIGHT_REQUESTS_PER_CONNECTION,
                metadata = metadata,
                throttleTimeSensor = fetcherThrottleTimeSensor,
            )
            networkClientDelegate = NetworkClientDelegate(
                time = this.time,
                config = this.config,
                logContext = logContext,
                client = networkClient,
            )
            isRunning = true
            this.errorEventHandler = ErrorEventHandler(this.backgroundEventQueue)
            this.groupState = GroupState(rebalanceConfig)
            this.requestManagerRegistry = buildRequestManagerRegistry(logContext)
            this.applicationEventProcessor = ApplicationEventProcessor(
                backgroundEventQueue = backgroundEventQueue,
                registry = requestManagerRegistry,
                metadata = metadata,
            )
        } catch (e: Exception) {
            close()
            throw KafkaException("Failed to construct background processor", e.cause)
        }
    }

    private fun buildRequestManagerRegistry(logContext: LogContext): Map<RequestManager.Type, RequestManager?> {
        val coordinatorManager =
            if (groupState.groupId == null) null
            else CoordinatorRequestManager(
                time = time,
                logContext = logContext,
                retryBackoffMs = config.getLong(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG)!!,
                nonRetriableErrorHandler = errorEventHandler,
                groupId = groupState.groupId,
            )
        val commitRequestManager =
            if (coordinatorManager == null) null
            else CommitRequestManager(
                time = time,
                logContext = logContext,
                subscriptionState = subscriptionState,
                config = config,
                coordinatorRequestManager = coordinatorManager,
                groupState = groupState,
            )
        return mapOf(
            RequestManager.Type.COORDINATOR to coordinatorManager,
            RequestManager.Type.COMMIT to commitRequestManager,
        )
    }

    override fun run() {
        try {
            log.debug("Background thread started")
            while (isRunning) {
                try {
                    runOnce()
                } catch (e: WakeupException) {
                    log.debug("WakeupException caught, background thread won't be interrupted")
                    // swallow the wakeup exception to prevent killing the background thread.
                }
            }
        } catch (t: Throwable) {
            log.error("The background thread failed due to unexpected error", t)
            throw RuntimeException(t)
        } finally {
            close()
            log.debug("{} closed", javaClass)
        }
    }

    /**
     * Poll and process an [ApplicationEvent]. It performs the following tasks:
     * 1. Drains and try to process all the requests in the queue.
     * 2. Iterate through the registry, poll, and get the next poll time for the network poll
     * 3. Poll the networkClient to send and retrieve the response.
     */
    fun runOnce() {
        drain()
        val currentTimeMs = time.milliseconds()
        val pollWaitTimeMs = requestManagerRegistry.values
            .filterNotNull()
            .minOf { handlePollResult(it.poll(currentTimeMs)) }
            .coerceAtMost(MAX_POLL_TIMEOUT_MS)

        networkClientDelegate.poll(pollWaitTimeMs, currentTimeMs)
    }

    private fun drain() {
        val events: Queue<ApplicationEvent> = pollApplicationEvent()
        for (event in events) {
            log.debug("Consuming application event: {}", event)
            consumeApplicationEvent(event)
        }
    }

    fun handlePollResult(res: PollResult): Long {
        if (res.unsentRequests.isNotEmpty()) networkClientDelegate.addAll(res.unsentRequests)
        return res.timeUntilNextPollMs
    }

    private fun pollApplicationEvent(): Queue<ApplicationEvent> {
        return if (applicationEventQueue.isEmpty()) LinkedList()
        else {
            val res = LinkedList<ApplicationEvent>()
            applicationEventQueue.drainTo(res)
            res
        }
    }

    private fun consumeApplicationEvent(event: ApplicationEvent) =
        applicationEventProcessor.process(event)

    fun wakeup() = networkClientDelegate.wakeup()

    fun close() {
        isRunning = false
        wakeup()
        closeQuietly(networkClientDelegate, "consumer network client utils")
        closeQuietly(metadata, "consumer metadata client")
    }

    companion object {
        private const val MAX_POLL_TIMEOUT_MS = 5000L

        private const val BACKGROUND_THREAD_NAME = "consumer_background_thread"
    }
}
