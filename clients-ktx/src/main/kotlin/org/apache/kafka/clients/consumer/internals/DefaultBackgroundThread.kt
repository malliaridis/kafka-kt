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

import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.internals.events.ApplicationEvent
import org.apache.kafka.clients.consumer.internals.events.BackgroundEvent
import org.apache.kafka.clients.consumer.internals.events.NoopApplicationEvent
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.errors.WakeupException
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.utils.KafkaThread
import org.apache.kafka.common.utils.LogContext
import org.apache.kafka.common.utils.Time
import org.apache.kafka.common.utils.Utils.closeQuietly
import org.slf4j.Logger
import java.util.concurrent.BlockingQueue
import java.util.concurrent.atomic.AtomicReference

/**
 * Background thread runnable that consumes `ApplicationEvent` and produces `BackgroundEvent`. It
 * uses an event loop to consume and produce events, and poll the network client to handle network
 * IO.
 *
 * It holds a reference to the [SubscriptionState], which is initialized by the polling thread.
 */
class DefaultBackgroundThread(
    private val time: Time,
    private val config: ConsumerConfig,
    logContext: LogContext,
    private val applicationEventQueue: BlockingQueue<ApplicationEvent>,
    private val backgroundEventQueue: BlockingQueue<BackgroundEvent>,
    // subscriptionState is initialized by the polling thread
    private val subscriptions: SubscriptionState?,
    private val metadata: ConsumerMetadata?,
    private val networkClient: ConsumerNetworkClient,
    private val metrics: Metrics?,
) : KafkaThread(BACKGROUND_THREAD_NAME, true) {

    private var log: Logger = logContext.logger(DefaultBackgroundThread::class.java)

    private var clientId: String? = null

    private var retryBackoffMs: Long = 0

    private var heartbeatIntervalMs = 0

    var isRunning = false
        private set

    private var inflightEvent: ApplicationEvent? = null

    private val exception = AtomicReference<RuntimeException?>(null)

    init {
        try {
            setConfig()
            isRunning = true
        } catch (e: Exception) {
            // now propagate the exception
            close()
            throw KafkaException("Failed to construct background processor", e)
        }
    }

    private fun setConfig() {
        retryBackoffMs = config.getLong(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG)!!
        clientId = config.getString(CommonClientConfigs.CLIENT_ID_CONFIG)
        heartbeatIntervalMs = config.getInt(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG)!!
    }

    override fun run() {
        try {
            log.debug("Background thread started")
            while (isRunning) {
                try {
                    runOnce()
                } catch (e: WakeupException) {
                    log.debug("Exception thrown, background thread won't terminate", e)
                    // swallow the wakeup exception to prevent killing the
                    // background thread.
                }
            }
        } catch (t: Throwable) {
            log.error("The background thread failed due to unexpected error", t)
            if (t is RuntimeException) exception.set(t)
            else exception.set(RuntimeException(t))
        } finally {
            close()
            log.debug("{} closed", javaClass)
        }
    }

    /**
     * Process event from a single poll
     */
    fun runOnce() {
        inflightEvent = maybePollEvent()
        if (inflightEvent != null) log.debug("processing application event: {}", inflightEvent)

        inflightEvent?.let {
            // clear inflight event upon successful consumption
            if (maybeConsumeInflightEvent(it)) inflightEvent = null
        }

        // if there are pending events to process, poll then continue without
        // blocking.
        if (!applicationEventQueue.isEmpty() || inflightEvent != null) {
            networkClient.poll(time.timer(0))
            return
        }
        // if there are no events to process, poll until timeout. The timeout
        // will be the minimum of the requestTimeoutMs, nextHeartBeatMs, and
        // nextMetadataUpdate. See NetworkClient.poll impl.
        networkClient.poll(time.timer(timeToNextHeartbeatMs(time.milliseconds())))
    }

    private fun timeToNextHeartbeatMs(nowMs: Long): Long {
        // TODO: implemented when heartbeat is added to the impl
        return 100
    }

    private fun maybePollEvent(): ApplicationEvent? {
        return if (inflightEvent != null || applicationEventQueue.isEmpty()) inflightEvent
        else applicationEventQueue.poll()
    }

    /**
     * ApplicationEvent are consumed here.
     *
     * @param event an [ApplicationEvent]
     * @return true when successfully consumed the event.
     */
    private fun maybeConsumeInflightEvent(event: ApplicationEvent): Boolean {
        log.debug("try consuming event: {}", event)
        return event.process()
    }

    /**
     * Processes [NoopApplicationEvent] and equeue a [NoopBackgroundEvent]. This is intentionally
     * left here for demonstration purpose.
     *
     * @param event a [NoopApplicationEvent]
     */
    private fun process(event: NoopApplicationEvent) =
        backgroundEventQueue.add(NoopBackgroundEvent(event.message))

    fun wakeup() = networkClient.wakeup()

    fun close() {
        isRunning = false
        wakeup()
        closeQuietly(networkClient, "consumer network client")
        closeQuietly(metadata, "consumer metadata client")
    }

    companion object {
        private const val BACKGROUND_THREAD_NAME = "consumer_background_thread"
    }
}
