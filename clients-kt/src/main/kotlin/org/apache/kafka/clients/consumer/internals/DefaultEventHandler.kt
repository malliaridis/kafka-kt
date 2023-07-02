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

import org.apache.kafka.clients.ApiVersions
import org.apache.kafka.clients.ClientUtils.createChannelBuilder
import org.apache.kafka.clients.ClientUtils.parseAndValidateAddresses
import org.apache.kafka.clients.NetworkClient
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.internals.events.ApplicationEvent
import org.apache.kafka.clients.consumer.internals.events.BackgroundEvent
import org.apache.kafka.clients.consumer.internals.events.EventHandler
import org.apache.kafka.common.internals.ClusterResourceListeners
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.metrics.Sensor
import org.apache.kafka.common.network.Selector
import org.apache.kafka.common.utils.LogContext
import org.apache.kafka.common.utils.Time
import java.util.concurrent.BlockingQueue
import java.util.concurrent.LinkedBlockingQueue

/**
 * An `EventHandler` that uses a single background thread to consume `ApplicationEvent` and produce
 * `BackgroundEvent` from the {@ConsumerBackgroundThread}.
 */
class DefaultEventHandler : EventHandler {

    private val applicationEventQueue: BlockingQueue<ApplicationEvent>

    private val backgroundEventQueue: BlockingQueue<BackgroundEvent>

    private val backgroundThread: DefaultBackgroundThread

    constructor(
        time: Time = Time.SYSTEM,
        config: ConsumerConfig,
        logContext: LogContext,
        applicationEventQueue: BlockingQueue<ApplicationEvent> = LinkedBlockingQueue(),
        backgroundEventQueue: BlockingQueue<BackgroundEvent> = LinkedBlockingQueue(),
        subscriptionState: SubscriptionState,
        apiVersions: ApiVersions,
        metrics: Metrics,
        clusterResourceListeners: ClusterResourceListeners,
        fetcherThrottleTimeSensor: Sensor? = null,
    ) {
        this.applicationEventQueue = applicationEventQueue
        this.backgroundEventQueue = backgroundEventQueue
        val metadata = bootstrapMetadata(
            logContext = logContext,
            clusterResourceListeners = clusterResourceListeners,
            config = config,
            subscriptions = subscriptionState,
        )
        val channelBuilder = createChannelBuilder(
            config = config,
            time = time,
            logContext = logContext,
        )

        val selector = Selector(
            connectionMaxIdleMs = config.getLong(ConsumerConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG)!!,
            metrics = metrics,
            time = time,
            metricGrpPrefix = METRIC_GRP_PREFIX,
            channelBuilder = channelBuilder,
            logContext = logContext,
        )
        val netClient = NetworkClient(
            selector = selector,
            metadata = metadata,
            clientId = config.getString(ConsumerConfig.CLIENT_ID_CONFIG)!!,
            maxInFlightRequestsPerConnection = 100, // a fixed large enough value will suffice for
            // max in-flight requests
            reconnectBackoffMs = config.getLong(ConsumerConfig.RECONNECT_BACKOFF_MS_CONFIG)!!,
            reconnectBackoffMax = config.getLong(ConsumerConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG)!!,
            socketSendBuffer = config.getInt(ConsumerConfig.SEND_BUFFER_CONFIG)!!,
            socketReceiveBuffer = config.getInt(ConsumerConfig.RECEIVE_BUFFER_CONFIG)!!,
            defaultRequestTimeoutMs = config.getInt(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG)!!,
            connectionSetupTimeoutMs =
            config.getLong(ConsumerConfig.SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG)!!,
            connectionSetupTimeoutMaxMs =
            config.getLong(ConsumerConfig.SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG)!!,
            time = time,
            discoverBrokerVersions = true,
            apiVersions = apiVersions,
            throttleTimeSensor = fetcherThrottleTimeSensor,
            logContext = logContext,
        )

        val networkClient = ConsumerNetworkClient(
            logContext = logContext,
            client = netClient,
            metadata = metadata,
            time = time,
            retryBackoffMs = config.getLong(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG)!!,
            requestTimeoutMs = config.getInt(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG)!!,
            maxPollTimeoutMs = config.getInt(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG)!!,
        )
        backgroundThread = DefaultBackgroundThread(
            time = time,
            config = config,
            logContext = logContext,
            applicationEventQueue = this.applicationEventQueue,
            backgroundEventQueue = this.backgroundEventQueue,
            subscriptions = subscriptionState,
            metadata = metadata,
            networkClient = networkClient,
            metrics = Metrics(time = time),
        )
    }

    // VisibleForTesting
    internal constructor(
        time: Time,
        config: ConsumerConfig,
        logContext: LogContext,
        applicationEventQueue: BlockingQueue<ApplicationEvent>,
        backgroundEventQueue: BlockingQueue<BackgroundEvent>,
        subscriptionState: SubscriptionState?,
        metadata: ConsumerMetadata?,
        networkClient: ConsumerNetworkClient,
    ) {
        this.applicationEventQueue = applicationEventQueue
        this.backgroundEventQueue = backgroundEventQueue
        backgroundThread = DefaultBackgroundThread(
            time = time,
            config = config,
            logContext = logContext,
            applicationEventQueue = this.applicationEventQueue,
            backgroundEventQueue = this.backgroundEventQueue,
            subscriptions = subscriptionState,
            metadata = metadata,
            networkClient = networkClient,
            metrics = Metrics(time = time),
        )
        backgroundThread.start()
    }

    // VisibleForTesting
    internal constructor(
        backgroundThread: DefaultBackgroundThread,
        applicationEventQueue: BlockingQueue<ApplicationEvent>,
        backgroundEventQueue: BlockingQueue<BackgroundEvent>,
    ) {
        this.backgroundThread = backgroundThread
        this.applicationEventQueue = applicationEventQueue
        this.backgroundEventQueue = backgroundEventQueue
        backgroundThread.start()
    }

    override fun poll(): BackgroundEvent? = backgroundEventQueue.poll()

    override val isEmpty: Boolean
        get() = backgroundEventQueue.isEmpty()

    override fun add(event: ApplicationEvent): Boolean {
        backgroundThread.wakeup()
        return applicationEventQueue.add(event)
    }

    // bootstrap a metadata object with the bootstrap server IP address, which will be used once for
    // the subsequent metadata refresh once the background thread has started up.
    private fun bootstrapMetadata(
        logContext: LogContext,
        clusterResourceListeners: ClusterResourceListeners,
        config: ConsumerConfig,
        subscriptions: SubscriptionState,
    ): ConsumerMetadata {
        val metadata = ConsumerMetadata(
            refreshBackoffMs = config.getLong(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG)!!,
            metadataExpireMs = config.getLong(ConsumerConfig.METADATA_MAX_AGE_CONFIG)!!,
            includeInternalTopics = !config.getBoolean(ConsumerConfig.EXCLUDE_INTERNAL_TOPICS_CONFIG)!!,
            allowAutoTopicCreation = config.getBoolean(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG)!!,
            subscription = subscriptions,
            logContext = logContext,
            clusterResourceListeners = clusterResourceListeners,
        )
        val addresses = parseAndValidateAddresses(
            urls = config.getList(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG)!!,
            clientDnsLookupConfig = config.getString(ConsumerConfig.CLIENT_DNS_LOOKUP_CONFIG)!!,
        )
        metadata.bootstrap(addresses)
        return metadata
    }

    fun close() {
        try {
            backgroundThread.close()
        } catch (e: Exception) {
            throw RuntimeException(e)
        }
    }

    companion object {
        private const val METRIC_GRP_PREFIX = "consumer"
    }
}
