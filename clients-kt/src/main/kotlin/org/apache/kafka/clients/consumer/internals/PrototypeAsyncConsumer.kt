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

import java.time.Duration
import java.util.Properties
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ExecutionException
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException
import java.util.concurrent.atomic.AtomicReference
import java.util.regex.Pattern
import org.apache.kafka.clients.ApiVersions
import org.apache.kafka.clients.GroupRebalanceConfig
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.clients.consumer.OffsetAndTimestamp
import org.apache.kafka.clients.consumer.OffsetCommitCallback
import org.apache.kafka.clients.consumer.internals.ConsumerUtils.createConsumerInterceptors
import org.apache.kafka.clients.consumer.internals.ConsumerUtils.createKeyDeserializer
import org.apache.kafka.clients.consumer.internals.ConsumerUtils.createLogContext
import org.apache.kafka.clients.consumer.internals.ConsumerUtils.createMetrics
import org.apache.kafka.clients.consumer.internals.ConsumerUtils.createSubscriptionState
import org.apache.kafka.clients.consumer.internals.ConsumerUtils.createValueDeserializer
import org.apache.kafka.clients.consumer.internals.events.AssignmentChangeApplicationEvent
import org.apache.kafka.clients.consumer.internals.events.BackgroundEvent
import org.apache.kafka.clients.consumer.internals.events.CommitApplicationEvent
import org.apache.kafka.clients.consumer.internals.events.EventHandler
import org.apache.kafka.clients.consumer.internals.events.NewTopicsMetadataUpdateRequestEvent
import org.apache.kafka.clients.consumer.internals.events.OffsetFetchApplicationEvent
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.Metric
import org.apache.kafka.common.MetricName
import org.apache.kafka.common.PartitionInfo
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.common.errors.InterruptException
import org.apache.kafka.common.errors.InvalidGroupIdException
import org.apache.kafka.common.internals.ClusterResourceListeners
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.utils.LogContext
import org.apache.kafka.common.utils.Time
import org.apache.kafka.common.utils.Utils.closeQuietly
import org.apache.kafka.common.utils.Utils.propsToMap
import org.slf4j.Logger

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

    private val keyDeserializer: Deserializer<K>

    private val valueDeserializer: Deserializer<V>

    private val log: Logger

    private val subscriptions: SubscriptionState

    private val metrics: Metrics

    private val defaultApiTimeoutMs: Long

    constructor(
        properties: Properties,
        keyDeserializer: Deserializer<K>,
        valueDeserializer: Deserializer<V>,
    ) : this(
        configs = propsToMap(properties),
        keyDeserializer = keyDeserializer,
        valueDeserializer = valueDeserializer,
    )

    constructor(
        configs: Map<String, Any?>,
        keyDeserializer: Deserializer<K>,
        valueDeserializer: Deserializer<V>,
    ) : this(
        config = ConsumerConfig(
            appendDeserializerToConfig(
                configs = configs,
                keyDeserializer = keyDeserializer,
                valueDeserializer = valueDeserializer,
            ),
        ),
        keyDeserializer = keyDeserializer,
        valueDeserializer = valueDeserializer,
    )

    constructor(
        config: ConsumerConfig,
        keyDeserializer: Deserializer<K>,
        valueDeserializer: Deserializer<V>,
    ) {
        this.time = Time.SYSTEM
        val groupRebalanceConfig = GroupRebalanceConfig(
            config = config,
            protocolType = GroupRebalanceConfig.ProtocolType.CONSUMER,
        )
        this.groupId = groupRebalanceConfig.groupId
        this.defaultApiTimeoutMs = config.getLong(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG)!!
        this.logContext = createLogContext(config, groupRebalanceConfig)
        this.log = logContext.logger(javaClass)
        this.keyDeserializer = createKeyDeserializer(config, keyDeserializer)
        this.valueDeserializer = createValueDeserializer(config, valueDeserializer)
        this.subscriptions = createSubscriptionState(config, logContext)
        this.metrics = createMetrics(config, time)
        val interceptorList = createConsumerInterceptors<K, V>(config)
        val clusterResourceListeners = configureClusterResourceListeners(
            keyDeserializer = this.keyDeserializer,
            valueDeserializer = this.valueDeserializer,
            candidateLists = arrayOf(metrics.reporters, interceptorList),
        )
        eventHandler = DefaultEventHandler(
            config = config,
            groupRebalanceConfig = groupRebalanceConfig,
            logContext = logContext,
            subscriptionState = subscriptions,
            apiVersions = ApiVersions(),
            metrics = metrics,
            clusterResourceListeners = clusterResourceListeners,
            fetcherThrottleTimeSensor = null, // this is coming from the fetcher, but we don't have one
        )
    }

    // Visible for testing
    internal constructor(
        time: Time,
        logContext: LogContext,
        config: ConsumerConfig,
        subscriptionState: SubscriptionState,
        eventHandler: EventHandler,
        metrics: Metrics,
        groupId: String?,
        defaultApiTimeoutMs: Int,
    ) {
        this.time = time
        this.logContext = logContext
        log = logContext.logger(javaClass)
        subscriptions = subscriptionState
        this.metrics = metrics
        this.groupId = groupId
        this.defaultApiTimeoutMs = defaultApiTimeoutMs.toLong()
        keyDeserializer = createKeyDeserializer(config, null)
        valueDeserializer = createValueDeserializer(config, null)
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
    override fun commitAsync() = commitAsync(null)

    override fun commitAsync(callback: OffsetCommitCallback?) =
        commitAsync(subscriptions.allConsumed(), callback)

    override fun commitAsync(
        offsets: Map<TopicPartition, OffsetAndMetadata>,
        callback: OffsetCommitCallback?,
    ) {
        val future = commit(offsets)
        val commitCallback = callback ?: DefaultOffsetCommitCallback()
        future.whenComplete { result, throwable ->
            if (throwable != null) commitCallback.onComplete(offsets, KafkaException(cause = throwable))
            else commitCallback.onComplete(offsets, null)
        }.exceptionally { exception: Throwable? ->
            println(exception)
            throw KafkaException(cause = exception)
        }
    }

    // Visible for testing
    internal fun commit(offsets: Map<TopicPartition, OffsetAndMetadata>): CompletableFuture<Unit> {
        maybeThrowInvalidGroupIdException()
        val commitEvent = CommitApplicationEvent(offsets)
        eventHandler.add(commitEvent)
        return commitEvent.future()
    }

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
        committed(partitions, Duration.ofMillis(defaultApiTimeoutMs))

    override fun committed(
        partitions: Set<TopicPartition>,
        timeout: Duration,
    ): Map<TopicPartition, OffsetAndMetadata> {
        maybeThrowInvalidGroupIdException()
        if (partitions.isEmpty()) return HashMap()

        val event = OffsetFetchApplicationEvent(partitions)
        eventHandler.add(event)

        return try {
            event.future().get(timeout.toMillis(), TimeUnit.MILLISECONDS)
        } catch (exception: InterruptedException) {
            throw InterruptException(exception)
        } catch (exception: TimeoutException) {
            throw org.apache.kafka.common.errors.TimeoutException(exception)
        } catch (exception: ExecutionException) {
            // Execution exception is thrown here
            throw KafkaException(exception)
        } catch (excpetion: Exception) {
            throw excpetion
        }
    }

    private fun maybeThrowInvalidGroupIdException() {
        if (groupId.isNullOrEmpty()) throw InvalidGroupIdException(
            "To use the group management or offset commit APIs, you must provide " +
                    "a valid ${ConsumerConfig.GROUP_ID_CONFIG} in the consumer configuration."
        )
    }

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
        timeout: Duration,
    ): Map<TopicPartition, Long> = throw KafkaException("method not implemented")

    override fun endOffsets(partitions: Collection<TopicPartition>): Map<TopicPartition, Long> =
        throw KafkaException("method not implemented")

    override fun endOffsets(
        partitions: Collection<TopicPartition>,
        timeout: Duration,
    ): Map<TopicPartition, Long> = throw KafkaException("method not implemented")

    override fun currentLag(topicPartition: TopicPartition): Long =
        throw KafkaException("method not implemented")

    override fun groupMetadata(): ConsumerGroupMetadata =
        throw KafkaException("method not implemented")

    override fun enforceRebalance() = throw KafkaException("method not implemented")

    override fun enforceRebalance(reason: String?) = throw KafkaException("method not implemented")

    override fun close() = close(Duration.ofMillis(DEFAULT_CLOSE_TIMEOUT_MS))

    override fun close(timeout: Duration) {
        val firstException = AtomicReference<Throwable?>()
        closeQuietly(eventHandler, "event handler", firstException)
        log.debug("Kafka consumer has been closed")
        val exception = firstException.get()
        if (exception != null) {
            if (exception is InterruptException) throw exception
            else throw KafkaException("Failed to close kafka consumer", exception)
        }
    }

    override fun wakeup() = Unit

    /**
     * This method sends a commit event to the EventHandler and waits for
     * the event to finish.
     *
     * @param timeout max wait time for the blocking operation.
     */
    override fun commitSync(timeout: Duration) = commitSync(subscriptions.allConsumed(), timeout)

    override fun commitSync(offsets: Map<TopicPartition, OffsetAndMetadata>) =
        commitSync(offsets, Duration.ofMillis(defaultApiTimeoutMs))

    override fun commitSync(offsets: Map<TopicPartition, OffsetAndMetadata>, timeout: Duration) {
        val commitFuture = commit(offsets)
        try {
            commitFuture[timeout.toMillis(), TimeUnit.MILLISECONDS]
        } catch (exception: TimeoutException) {
            throw org.apache.kafka.common.errors.TimeoutException(exception)
        } catch (exception: InterruptedException) {
            throw InterruptException(exception)
        } catch (exception: ExecutionException) {
            throw KafkaException(exception)
        } catch (exception: Exception) {
            throw exception
        }
    }

    override fun assignment(): Set<TopicPartition> = subscriptions.assignedPartitions()

    /**
     * Get the current subscription. or an empty set if no such call has
     * been made.
     * @return The set of topics currently subscribed to
     */
    override fun subscription(): Set<String> = subscriptions.subscription()

    override fun subscribe(topics: Collection<String>) =
        throw KafkaException("method not implemented")

    override fun subscribe(topics: Collection<String>, callback: ConsumerRebalanceListener) =
        throw KafkaException("method not implemented")

    override fun assign(partitions: Collection<TopicPartition>) {
        if (partitions.isEmpty()) {
            // TODO: implementation of unsubscribe() will be included in forthcoming commits.
            // this.unsubscribe();
            return
        }

        for ((topic) in partitions) {
            require(topic.isNotBlank()) { "Topic partitions to assign to cannot have null or empty topic" }
        }

        // TODO: implementation of refactored Fetcher will be included in forthcoming commits.
        // fetcher.clearBufferedDataForUnassignedPartitions(partitions);

        // assignment change event will trigger autocommit if it is configured and the group id is specified. This is
        // to make sure offsets of topic partitions the consumer is unsubscribing from are committed since there will
        // be no following rebalance

        // TODO: implementation of refactored Fetcher will be included in forthcoming commits.
        // fetcher.clearBufferedDataForUnassignedPartitions(partitions);

        // assignment change event will trigger autocommit if it is configured and the group id is specified. This is
        // to make sure offsets of topic partitions the consumer is unsubscribing from are committed since there will
        // be no following rebalance
        eventHandler.add(AssignmentChangeApplicationEvent(subscriptions.allConsumed(), time.milliseconds()))

        log.info("Assigned to partition(s): {}", partitions.joinToString())
        if (subscriptions.assignFromUser(HashSet<TopicPartition>(partitions)))
            eventHandler.add(NewTopicsMetadataUpdateRequestEvent())
    }

    override fun subscribe(pattern: Pattern, callback: ConsumerRebalanceListener) =
        throw KafkaException("method not implemented")

    override fun subscribe(pattern: Pattern) = throw KafkaException("method not implemented")

    override fun unsubscribe() = throw KafkaException("method not implemented")

    @Deprecated("")
    override fun poll(timeout: Long): ConsumerRecords<K, V> =
        throw KafkaException("method not implemented")

    private inner class DefaultOffsetCommitCallback : OffsetCommitCallback {
        override fun onComplete(offsets: Map<TopicPartition, OffsetAndMetadata>?, exception: Exception?) {
            if (exception != null) log.error("Offset commit with offsets {} failed", offsets, exception)
        }
    }

    companion object {

        internal const val DEFAULT_CLOSE_TIMEOUT_MS = (30 * 1000).toLong()

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

        // This is here temporary as we don't have public access to the ConsumerConfig in this module.
        fun appendDeserializerToConfig(
            configs: Map<String, Any?>,
            keyDeserializer: Deserializer<*>?,
            valueDeserializer: Deserializer<*>?,
        ): Map<String, Any?> {
            // validate deserializer configuration, if the passed deserializer instance is null,
            // the user must explicitly set a valid deserializer configuration value
            val newConfigs = configs.toMutableMap()
            if (keyDeserializer != null)
                newConfigs[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = keyDeserializer.javaClass
            else if (newConfigs[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] == null)
                throw ConfigException(
                    name = ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                    value = null,
                    message = "must be non-null.",
                )
            if (valueDeserializer != null)
                newConfigs[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = valueDeserializer.javaClass
            else if (newConfigs[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] == null)
                throw ConfigException(
                    name = ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                    value = null,
                    message = "must be non-null.",
                )
            return newConfigs
        }
    }
}
