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

import java.util.concurrent.BlockingQueue
import java.util.concurrent.LinkedBlockingQueue
import org.apache.kafka.clients.ApiVersions
import org.apache.kafka.clients.ClientUtils.parseAndValidateAddresses
import org.apache.kafka.clients.GroupRebalanceConfig
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.internals.events.ApplicationEvent
import org.apache.kafka.clients.consumer.internals.events.BackgroundEvent
import org.apache.kafka.clients.consumer.internals.events.EventHandler
import org.apache.kafka.common.internals.ClusterResourceListeners
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.metrics.Sensor
import org.apache.kafka.common.utils.LogContext
import org.apache.kafka.common.utils.Time

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
        groupRebalanceConfig: GroupRebalanceConfig,
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

        // Bootstrap a metadata object with the bootstrap server IP address, which will be used once for the
        // subsequent metadata refresh once the background thread has started up.
        val metadata = ConsumerMetadata(
            config = config,
            subscriptions = subscriptionState,
            logContext = logContext,
            clusterResourceListeners = clusterResourceListeners,
        )
        val addresses = parseAndValidateAddresses(config)
        metadata.bootstrap(addresses)

        backgroundThread = DefaultBackgroundThread(
            time = time,
            config = config,
            rebalanceConfig = groupRebalanceConfig,
            logContext = logContext,
            applicationEventQueue = this.applicationEventQueue,
            backgroundEventQueue = this.backgroundEventQueue,
            metadata = metadata,
            subscriptionState = subscriptionState,
            apiVersions = apiVersions,
            metrics = metrics,
            fetcherThrottleTimeSensor = fetcherThrottleTimeSensor,
        )
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

    override fun close() {
        try {
            backgroundThread.close()
        } catch (e: Exception) {
            throw RuntimeException(e)
        }
    }
}
