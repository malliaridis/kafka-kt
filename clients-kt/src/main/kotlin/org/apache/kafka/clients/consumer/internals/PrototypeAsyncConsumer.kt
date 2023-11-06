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
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.CommonClientConfigs.metricsReporters
import org.apache.kafka.clients.GroupRebalanceConfig
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata
import org.apache.kafka.clients.consumer.ConsumerInterceptor
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.clients.consumer.OffsetAndTimestamp
import org.apache.kafka.clients.consumer.OffsetCommitCallback
import org.apache.kafka.clients.consumer.OffsetResetStrategy
import org.apache.kafka.clients.consumer.internals.events.ApplicationEvent
import org.apache.kafka.clients.consumer.internals.events.BackgroundEvent
import org.apache.kafka.clients.consumer.internals.events.EventHandler
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.Metric
import org.apache.kafka.common.MetricName
import org.apache.kafka.common.PartitionInfo
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.internals.ClusterResourceListeners
import org.apache.kafka.common.metrics.KafkaMetricsContext
import org.apache.kafka.common.metrics.MetricConfig
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.metrics.MetricsContext
import org.apache.kafka.common.metrics.Sensor
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.utils.LogContext
import org.apache.kafka.common.utils.Time
import org.slf4j.Logger
import java.time.Duration
import java.util.*
import java.util.concurrent.CompletableFuture
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException
import java.util.regex.Pattern

/**
 * This prototype consumer uses the EventHandler to process application events so that the network
 * IO can be processed in a background thread. Visit
 * [this document](https://cwiki.apache.org/confluence/display/KAFKA/Proposal%3A+Consumer+Threading+Model+Refactor)
 * for detail implementation.
 */
class PrototypeAsyncConsumer<K, V> : Consumer<K, V> {

    private val logContext: LogContext

    private val eventHandler: EventHandler

    private val time: Time

    private val groupId: String?

    private val clientId: String?

    private val log: Logger

    private val subscriptions: SubscriptionState

    private val metrics: Metrics

    private val defaultApiTimeoutMs: Long

    constructor(
        time: Time,
        config: ConsumerConfig,
        keyDeserializer: Deserializer<K>,
        valueDeserializer: Deserializer<V>,
    ) {
        this.time = time
        val groupRebalanceConfig = GroupRebalanceConfig(
            config = config,
            protocolType = GroupRebalanceConfig.ProtocolType.CONSUMER,
        )
        groupId = groupRebalanceConfig.groupId
        clientId = config.getString(CommonClientConfigs.CLIENT_ID_CONFIG)!!
        defaultApiTimeoutMs = config.getLong(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG)!!

        // If group.instance.id is set, we will append it to the log context.
        logContext = LogContext(
            if (groupRebalanceConfig.groupInstanceId != null)
                "[Consumer instanceId=${groupRebalanceConfig.groupInstanceId}, " +
                        "clientId=$clientId, groupId=$groupId] "
            else "[Consumer clientId=$clientId, groupId=$groupId] "
        )

        log = logContext.logger(javaClass)
        val offsetResetStrategy = OffsetResetStrategy.valueOf(
            config.getString(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG)!!.uppercase()
        )
        subscriptions = SubscriptionState(logContext, offsetResetStrategy)
        metrics = buildMetrics(config, time, clientId)

        val clusterResourceListeners = configureClusterResourceListeners(
            keyDeserializer,
            valueDeserializer,
            metrics.reporters,
            config.getConfiguredInstances(
                key = ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG,
                t = ConsumerInterceptor::class.java,
                configOverrides = mapOf(ConsumerConfig.CLIENT_ID_CONFIG to clientId)
            )
        )
        eventHandler = DefaultEventHandler(
            config = config,
            logContext = logContext,
            subscriptionState = subscriptions,
            apiVersions = ApiVersions(),
            metrics = metrics,
            clusterResourceListeners = clusterResourceListeners,
            // fetcherThrottleTimeSensor is coming from the fetcher, but we don't have one
        )
    }

    // Visible for testing
    internal constructor(
        time: Time,
        logContext: LogContext,
        config: ConsumerConfig?,
        subscriptionState: SubscriptionState,
        eventHandler: EventHandler,
        metrics: Metrics,
        clusterResourceListeners: ClusterResourceListeners?,
        groupId: String?,
        clientId: String?,
        defaultApiTimeoutMs: Int,
    ) {
        this.time = time
        this.logContext = logContext
        log = logContext.logger(javaClass)
        subscriptions = subscriptionState
        this.metrics = metrics
        this.groupId = groupId
        this.defaultApiTimeoutMs = defaultApiTimeoutMs.toLong()
        this.clientId = clientId
        this.eventHandler = eventHandler
    }

    /**
     * Poll implementation using [EventHandler].
     * 1. Poll for background events. If there's a fetch response event, process the record and
     *    return it. If it is another type of event, process it.
     * 2. Send fetches if needed.
     *
     * If the timeout expires, return an empty ConsumerRecord.
     *
     * @param timeout timeout of the poll loop
     * @return ConsumerRecord. It can be empty if time timeout expires.
     */
    override fun poll(timeout: Duration): ConsumerRecords<K, V> {
        try {
            do {
                if (!eventHandler.isEmpty) {
                    val backgroundEvent: BackgroundEvent? = eventHandler.poll()
                    // processEvent() may process 3 types of event:
                    // 1. Errors
                    // 2. Callback Invocation
                    // 3. Fetch responses
                    // Errors will be handled or rethrown.
                    // Callback invocation will trigger callback function execution, which is
                    // blocking until completion. Successful fetch responses will be added to the
                    // completedFetches in the fetcher, which will then be processed in the
                    // collectFetches().
                    backgroundEvent?.let { event -> processEvent(event, timeout) }
                }
                // The idea here is to have the background thread sending fetches autonomously, and
                // the fetcher uses the poll loop to retrieve successful fetchResponse and process
                // them on the polling thread.
                val fetch = collectFetches()
                if (!fetch.isEmpty) return processFetchResults(fetch)

                // We will wait for retryBackoffMs
            } while (time.timer(timeout).isNotExpired)
        } catch (e: Exception) {
            throw RuntimeException(e)
        }
        return ConsumerRecords.empty()
    }

    /**
     * Commit offsets returned on the last [poll()][.poll] for all the subscribed list of topics and
     * partitions.
     */
    override fun commitSync() = commitSync(Duration.ofMillis(defaultApiTimeoutMs))

    // stubbed class
    private fun processEvent(backgroundEvent: BackgroundEvent, timeout: Duration) = Unit

    // stubbed class
    private fun processFetchResults(fetch: Fetch<K, V>): ConsumerRecords<K, V> =
        ConsumerRecords.empty()

    // stubbed class
    private fun collectFetches(): Fetch<K, V> = Fetch.empty()

    /**
     * This method sends a commit event to the EventHandler and return.
     */
    override fun commitAsync() {
        val commitEvent: ApplicationEvent = CommitApplicationEvent()
        eventHandler.add(commitEvent)
    }

    override fun commitAsync(callback: OffsetCommitCallback?) =
        throw KafkaException("method not implemented")

    override fun commitAsync(
        offsets: Map<TopicPartition, OffsetAndMetadata>,
        callback: OffsetCommitCallback?,
    ) = throw KafkaException("method not implemented")

    override fun seek(partition: TopicPartition, offset: Long) =
        throw KafkaException("method not implemented")

    override fun seek(partition: TopicPartition, offsetAndMetadata: OffsetAndMetadata) =
        throw KafkaException("method not implemented")

    override fun seekToBeginning(partitions: Collection<TopicPartition>) =
        throw KafkaException("method not implemented")

    override fun seekToEnd(partitions: Collection<TopicPartition>) =
        throw KafkaException("method not implemented")

    override fun position(partition: TopicPartition): Long =
        throw KafkaException("method not implemented")

    override fun position(partition: TopicPartition, timeout: Duration): Long =
        throw KafkaException("method not implemented")

    @Deprecated("")
    override fun committed(partition: TopicPartition): OffsetAndMetadata =
        throw KafkaException("method not implemented")

    @Deprecated("")
    override fun committed(partition: TopicPartition, timeout: Duration): OffsetAndMetadata =
        throw KafkaException("method not implemented")

    override fun committed(partitions: Set<TopicPartition>): Map<TopicPartition, OffsetAndMetadata> =
        throw KafkaException("method not implemented")

    override fun committed(
        partitions: Set<TopicPartition>,
        timeout: Duration
    ): Map<TopicPartition, OffsetAndMetadata> = throw KafkaException("method not implemented")

    override fun metrics(): Map<MetricName, Metric> =
        throw KafkaException("method not implemented")

    override fun partitionsFor(topic: String): List<PartitionInfo> =
        throw KafkaException("method not implemented")

    override fun partitionsFor(topic: String, timeout: Duration): List<PartitionInfo> =
        throw KafkaException("method not implemented")

    override fun listTopics(): Map<String, List<PartitionInfo>> =
        throw KafkaException("method not implemented")

    override fun listTopics(timeout: Duration): Map<String, List<PartitionInfo>> =
        throw KafkaException("method not implemented")

    override fun paused(): Set<TopicPartition> =
        throw KafkaException("method not implemented")

    override fun pause(partitions: Collection<TopicPartition>) =
        throw KafkaException("method not implemented")

    override fun resume(partitions: Collection<TopicPartition>) =
        throw KafkaException("method not implemented")

    override fun offsetsForTimes(
        timestampsToSearch: Map<TopicPartition, Long>,
    ): Map<TopicPartition, OffsetAndTimestamp> = throw KafkaException("method not implemented")

    override fun offsetsForTimes(
        timestampsToSearch: Map<TopicPartition, Long>,
        timeout: Duration,
    ): Map<TopicPartition, OffsetAndTimestamp> = throw KafkaException("method not implemented")

    override fun beginningOffsets(
        partitions: Collection<TopicPartition>,
    ): Map<TopicPartition, Long> = throw KafkaException("method not implemented")

    override fun beginningOffsets(
        partitions: Collection<TopicPartition>,
        timeout: Duration
    ): Map<TopicPartition, Long> = throw KafkaException("method not implemented")

    override fun endOffsets(partitions: Collection<TopicPartition>): Map<TopicPartition, Long> =
        throw KafkaException("method not implemented")

    override fun endOffsets(
        partitions: Collection<TopicPartition>,
        timeout: Duration
    ): Map<TopicPartition, Long> = throw KafkaException("method not implemented")

    override fun currentLag(topicPartition: TopicPartition): Long =
        throw KafkaException("method not implemented")

    override fun groupMetadata(): ConsumerGroupMetadata =
        throw KafkaException("method not implemented")

    override fun enforceRebalance() = throw KafkaException("method not implemented")

    override fun enforceRebalance(reason: String?) = throw KafkaException("method not implemented")

    override fun close() = throw KafkaException("method not implemented")

    override fun close(timeout: Duration) = throw KafkaException("method not implemented")

    override fun wakeup() = throw KafkaException("method not implemented")

    /**
     * This method sends a commit event to the EventHandler and waits for
     * the event to finish.
     *
     * @param timeout max wait time for the blocking operation.
     */
    override fun commitSync(timeout: Duration) {
        val commitEvent = CommitApplicationEvent()
        eventHandler.add(commitEvent)
        val commitFuture = commitEvent.commitFuture

        try {
            commitFuture[timeout.toMillis(), TimeUnit.MILLISECONDS]
        } catch (e: TimeoutException) {
            throw org.apache.kafka.common.errors.TimeoutException("timeout")
        } catch (e: Exception) {
            // handle exception here
            throw RuntimeException(e)
        }
    }

    override fun commitSync(offsets: Map<TopicPartition, OffsetAndMetadata>) =
        throw KafkaException("method not implemented")

    override fun commitSync(offsets: Map<TopicPartition, OffsetAndMetadata>, timeout: Duration) =
        throw KafkaException("method not implemented")

    override fun assignment(): Set<TopicPartition> = throw KafkaException("method not implemented")

    /**
     * Get the current subscription. or an empty set if no such call has
     * been made.
     * @return The set of topics currently subscribed to
     */
    override fun subscription(): Set<String> {
        return Collections.unmodifiableSet(subscriptions.subscription())
    }

    override fun subscribe(topics: Collection<String>) =
        throw KafkaException("method not implemented")

    override fun subscribe(topics: Collection<String>, callback: ConsumerRebalanceListener) =
        throw KafkaException("method not implemented")

    override fun assign(partitions: Collection<TopicPartition>) =
        throw KafkaException("method not implemented")

    override fun subscribe(pattern: Pattern, callback: ConsumerRebalanceListener) =
    throw KafkaException("method not implemented")

    override fun subscribe(pattern: Pattern) = throw KafkaException("method not implemented")

    override fun unsubscribe() = throw KafkaException("method not implemented")

    @Deprecated("")
    override fun poll(timeout: Long): ConsumerRecords<K, V> =
        throw KafkaException("method not implemented")

    /**
     * A stubbed ApplicationEvent for demonstration purpose
     */
    private inner class CommitApplicationEvent : ApplicationEvent() {

        // this is the stubbed commitAsyncEvents
        var commitFuture = CompletableFuture<Void>()

        override fun process(): Boolean = true
    }

    companion object {

        private val CLIENT_ID_METRIC_TAG = "client-id"

        private val JMX_PREFIX = "kafka.consumer"

        private fun <K, V> configureClusterResourceListeners(
            keyDeserializer: Deserializer<K>,
            valueDeserializer: Deserializer<V>,
            vararg candidateLists: List<*>,
        ): ClusterResourceListeners {
            val clusterResourceListeners = ClusterResourceListeners()

            for (candidateList in candidateLists)
                clusterResourceListeners.maybeAddAll(candidateList)

            clusterResourceListeners.maybeAdd(keyDeserializer)
            clusterResourceListeners.maybeAdd(valueDeserializer)
            return clusterResourceListeners
        }

        private fun buildMetrics(
            config: ConsumerConfig,
            time: Time,
            clientId: String,
        ): Metrics {
            val metricsTags = mapOf(CLIENT_ID_METRIC_TAG to clientId)
            val metricConfig = MetricConfig().apply {
                samples = config.getInt(ConsumerConfig.METRICS_NUM_SAMPLES_CONFIG)!!
                timeWindowMs = TimeUnit.MILLISECONDS.convert(
                    config.getLong(ConsumerConfig.METRICS_SAMPLE_WINDOW_MS_CONFIG)!!,
                    TimeUnit.MILLISECONDS,
                )
                recordingLevel = Sensor.RecordingLevel.forName(
                    config.getString(ConsumerConfig.METRICS_RECORDING_LEVEL_CONFIG)!!
                )
                tags = metricsTags
            }

            val reporters = metricsReporters(clientId, config).toMutableList()
            val metricsContext: MetricsContext = KafkaMetricsContext(
                namespace = JMX_PREFIX,
                contextLabels =
                config.originalsWithPrefix(CommonClientConfigs.METRICS_CONTEXT_PREFIX),
            )

            return Metrics(
                config = metricConfig,
                reporters = reporters,
                time = time,
                metricsContext = metricsContext,
            )
        }
    }
}
