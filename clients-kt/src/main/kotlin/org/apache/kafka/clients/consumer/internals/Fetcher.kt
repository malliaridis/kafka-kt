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
import org.apache.kafka.clients.ClientResponse
import org.apache.kafka.clients.FetchSessionHandler
import org.apache.kafka.clients.NodeApiVersions
import org.apache.kafka.clients.StaleMetadataException
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.LogTruncationException
import org.apache.kafka.clients.consumer.OffsetAndTimestamp
import org.apache.kafka.clients.consumer.OffsetOutOfRangeException
import org.apache.kafka.clients.consumer.OffsetResetStrategy
import org.apache.kafka.clients.consumer.internals.OffsetsForLeaderEpochClient.OffsetForEpochResult
import org.apache.kafka.clients.consumer.internals.SubscriptionState.FetchPosition
import org.apache.kafka.clients.consumer.internals.SubscriptionState.LogTruncation
import org.apache.kafka.common.IsolationLevel
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.MetricName
import org.apache.kafka.common.Node
import org.apache.kafka.common.PartitionInfo
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.Uuid
import org.apache.kafka.common.errors.CorruptRecordException
import org.apache.kafka.common.errors.InvalidTopicException
import org.apache.kafka.common.errors.RecordDeserializationException
import org.apache.kafka.common.errors.RecordTooLargeException
import org.apache.kafka.common.errors.RetriableException
import org.apache.kafka.common.errors.SerializationException
import org.apache.kafka.common.errors.TimeoutException
import org.apache.kafka.common.errors.TopicAuthorizationException
import org.apache.kafka.common.header.Headers
import org.apache.kafka.common.header.internals.RecordHeaders
import org.apache.kafka.common.message.FetchResponseData
import org.apache.kafka.common.message.FetchResponseData.AbortedTransaction
import org.apache.kafka.common.message.ListOffsetsRequestData.ListOffsetsPartition
import org.apache.kafka.common.metrics.Gauge
import org.apache.kafka.common.metrics.MetricConfig
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.metrics.Sensor
import org.apache.kafka.common.metrics.stats.Avg
import org.apache.kafka.common.metrics.stats.Max
import org.apache.kafka.common.metrics.stats.Meter
import org.apache.kafka.common.metrics.stats.Min
import org.apache.kafka.common.metrics.stats.Value
import org.apache.kafka.common.metrics.stats.WindowedCount
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.record.ControlRecordType
import org.apache.kafka.common.record.Record
import org.apache.kafka.common.record.RecordBatch
import org.apache.kafka.common.requests.FetchRequest
import org.apache.kafka.common.requests.FetchResponse
import org.apache.kafka.common.requests.ListOffsetsRequest
import org.apache.kafka.common.requests.ListOffsetsResponse
import org.apache.kafka.common.requests.MetadataRequest
import org.apache.kafka.common.requests.MetadataResponse
import org.apache.kafka.common.requests.OffsetsForLeaderEpochRequest
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.utils.BufferSupplier
import org.apache.kafka.common.utils.CloseableIterator
import org.apache.kafka.common.utils.LogContext
import org.apache.kafka.common.utils.Time
import org.apache.kafka.common.utils.Timer
import org.apache.kafka.common.utils.Utils
import org.slf4j.Logger
import org.slf4j.helpers.MessageFormatter
import java.io.Closeable
import java.util.*
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference
import java.util.stream.Collectors

/**
 * This class manages the fetching process with the brokers.
 *
 * Thread-safety:
 *
 * Requests and responses of Fetcher may be processed by different threads since heartbeat
 * thread may process responses. Other operations are single-threaded and invoked only from
 * the thread polling the consumer.
 *
 * - If a response handler accesses any shared state of the Fetcher (e.g. FetchSessionHandler), all
 *   access to that state must be synchronized on the Fetcher instance.
 * - If a response handler accesses any shared state of the coordinator (e.g. SubscriptionState), it
 *   is assumed that all access to that state is synchronized on the coordinator instance by the
 *   caller.
 * - Responses that collate partial responses from multiple brokers (e.g. to list offsets) are
 *   synchronized on the response future.
 * - At most one request is pending for each node at any time. Nodes with pending requests are
 *   tracked and updated after processing the response. This ensures that any state (e.g. epoch)
 *   updated while processing responses on one thread are visible while creating the subsequent
 *   request on a different thread.
 */
open class Fetcher<K, V>(
    private val logContext: LogContext,
    private val client: ConsumerNetworkClient,
    private val minBytes: Int,
    private val maxBytes: Int,
    private val maxWaitMs: Int,
    private val fetchSize: Int,
    private val maxPollRecords: Int,
    private val checkCrcs: Boolean,
    private val clientRackId: String,
    private val keyDeserializer: Deserializer<K>,
    private val valueDeserializer: Deserializer<V>,
    private val metadata: ConsumerMetadata,
    private val subscriptions: SubscriptionState,
    metrics: Metrics,
    metricsRegistry: FetcherMetricsRegistry,
    private val time: Time,
    private val retryBackoffMs: Long,
    private val requestTimeoutMs: Long,
    private val isolationLevel: IsolationLevel,
    private val apiVersions: ApiVersions,
) : Closeable {

    private val log: Logger = logContext.logger(Fetcher::class.java)

    private val sensors: FetchManagerMetrics = FetchManagerMetrics(metrics, metricsRegistry)

    private val completedFetches: ConcurrentLinkedQueue<CompletedFetch> = ConcurrentLinkedQueue()

    private val decompressionBufferSupplier = BufferSupplier.create()

    private val sessionHandlers: MutableMap<Int, FetchSessionHandler> = HashMap()

    private val cachedListOffsetsException = AtomicReference<RuntimeException?>()

    private val cachedOffsetForLeaderException = AtomicReference<RuntimeException?>()

    private val offsetsForLeaderEpochClient: OffsetsForLeaderEpochClient =
        OffsetsForLeaderEpochClient(client, logContext)

    private val nodesWithPendingFetchRequests: MutableSet<Int> = HashSet()

    private val metadataUpdateVersion = AtomicInteger(-1)

    private var nextInLineFetch: CompletedFetch? = null

    /**
     * Return whether we have any completed fetches pending return to the user. This method is
     * thread-safe. Has visibility for testing.
     *
     * @return true if there are completed fetches, false otherwise
     */
    internal fun hasCompletedFetches(): Boolean = !completedFetches.isEmpty()

    /**
     * Return whether we have any completed fetches that are fetchable. This method is thread-safe.
     *
     * @return true if there are completed fetches that can be returned, false otherwise
     */
    fun hasAvailableFetches(): Boolean =
        completedFetches.any { fetch -> subscriptions.isFetchable(fetch.partition) }

    /**
     * Set-up a fetch request for any node that we have assigned partitions for which doesn't
     * already have an in-flight fetch or pending fetch data.
     *
     * @return number of fetches sent
     */
    @Synchronized
    fun sendFetches(): Int {
        // Update metrics in case there was an assignment change
        sensors.maybeUpdateAssignment(subscriptions)
        val fetchRequestMap = prepareFetchRequests()
        for ((fetchTarget, data) in fetchRequestMap) {
            val maxVersion = if (!data.canUseTopicIds) 12.toShort()
            else ApiKeys.FETCH.latestVersion()

            val request = FetchRequest.Builder
                .forConsumer(maxVersion, maxWaitMs, minBytes, data.toSend)
                .isolationLevel(isolationLevel)
                .setMaxBytes(maxBytes)
                .metadata(data.metadata)
                .removed(data.toForget)
                .replaced(data.toReplace)
                .rackId(clientRackId)

            if (log.isDebugEnabled) log.debug(
                "Sending {} {} to broker {}",
                isolationLevel,
                data.toString(),
                fetchTarget,
            )

            val future = client.send(fetchTarget, request)
            // We add the node to the set of nodes with pending fetch requests before adding the
            // listener because the future may have been fulfilled on another thread (e.g. during a
            // disconnection being handled by the heartbeat thread) which will mean the listener
            // will be invoked synchronously.
            nodesWithPendingFetchRequests.add(fetchTarget.id)
            future.addListener(object : RequestFutureListener<ClientResponse> {
                override fun onSuccess(value: ClientResponse) = synchronized(this@Fetcher) {
                    try {
                        val response = value.responseBody as FetchResponse?
                        val handler = sessionHandler(fetchTarget.id)
                        if (handler == null) {
                            log.error(
                                "Unable to find FetchSessionHandler for node {}. Ignoring fetch " +
                                        "response.",
                                fetchTarget.id,
                            )
                            return
                        }
                        if (!handler.handleResponse(response!!, value.requestHeader.apiVersion)) {
                            if (response.error() === Errors.FETCH_SESSION_TOPIC_ID_ERROR)
                                metadata.requestUpdate()
                            return
                        }
                        val responseData = response.responseData(
                            topicNames = handler.sessionTopicNames(),
                            version = value.requestHeader.apiVersion,
                        )
                        val partitions = responseData.keys.toMutableSet()
                        val metricAggregator = FetchResponseMetricAggregator(
                            sensors = sensors,
                            partitions = partitions,
                        )
                        for ((partition, partitionData) in responseData) {

                            val requestData = data.sessionPartitions[partition]

                            // Received fetch response for missing session partition
                            if (requestData == null) throw IllegalStateException(
                                if (data.metadata.isFull)
                                    MessageFormatter.arrayFormat(
                                        "Response for missing full request " +
                                                "partition: partition={}; metadata={}",
                                        arrayOf(partition, data.metadata)
                                    ).message
                                else MessageFormatter.arrayFormat(
                                    "Response for missing session request " +
                                            "partition: partition={}; metadata={}; " +
                                            "toSend={}; toForget={}; toReplace={}",
                                    arrayOf(
                                        partition,
                                        data.metadata,
                                        data.toSend,
                                        data.toForget,
                                        data.toReplace,
                                    )
                                ).message
                            )
                            else {
                                val fetchOffset = requestData.fetchOffset
                                log.debug(
                                    "Fetch {} at offset {} for partition {} returned fetch data {}",
                                    isolationLevel,
                                    fetchOffset,
                                    partition,
                                    partitionData,
                                )
                                val batches = FetchResponse.recordsOrFail(partitionData)
                                    .batches()
                                    .iterator()

                                val responseVersion = value.requestHeader.apiVersion
                                completedFetches.add(
                                    CompletedFetch(
                                        partition = partition,
                                        partitionData = partitionData,
                                        metricAggregator = metricAggregator,
                                        batches = batches,
                                        nextFetchOffset = fetchOffset,
                                        responseVersion = responseVersion,
                                    )
                                )
                            }
                        }
                        sensors.fetchLatency.record(value.requestLatencyMs.toDouble())
                    } finally {
                        nodesWithPendingFetchRequests.remove(fetchTarget.id)
                    }
                }

                override fun onFailure(e: RuntimeException) = synchronized(this@Fetcher) {
                    try {
                        val handler = sessionHandler(fetchTarget.id) ?: return
                        handler.handleError(e)
                        handler.sessionTopicPartitions().forEach { tp ->
                            subscriptions.clearPreferredReadReplica(tp)
                        }
                    } finally {
                        nodesWithPendingFetchRequests.remove(fetchTarget.id)
                    }
                }
            })
        }
        return fetchRequestMap.size
    }

    /**
     * Get topic metadata for all topics in the cluster
     *
     * @param timer Timer bounding how long this method can block
     * @return The map of topics with their partition information
     */
    fun getAllTopicMetadata(timer: Timer): Map<String, List<PartitionInfo>> =
        getTopicMetadata(MetadataRequest.Builder.allTopics(), timer)

    /**
     * Get metadata for all topics present in Kafka cluster
     *
     * @param request The MetadataRequest to send
     * @param timer Timer bounding how long this method can block
     * @return The map of topics with their partition information
     */
    fun getTopicMetadata(
        request: MetadataRequest.Builder,
        timer: Timer,
    ): Map<String, List<PartitionInfo>> {
        // Save the round trip if no topics are requested.
        if (!request.isAllTopics && request.emptyTopicList()) return emptyMap()
        do {
            val future = sendMetadataRequest(request)
            client.poll(future, timer)
            if (future.failed() && !future.isRetriable) throw future.exception()
            if (future.succeeded()) {
                val response = future.value().responseBody as MetadataResponse?
                val cluster = response!!.buildCluster()
                val unauthorizedTopics = cluster.unauthorizedTopics

                if (unauthorizedTopics.isNotEmpty())
                    throw TopicAuthorizationException(unauthorizedTopics)

                var shouldRetry = false
                val errors = response.errors()
                if (errors.isNotEmpty()) {
                    // if there were errors, we need to check whether they were fatal or whether
                    // we should just retry
                    log.debug("Topic metadata fetch included errors: {}", errors)
                    for ((topic, error) in errors) {
                        if (error === Errors.INVALID_TOPIC_EXCEPTION)
                            throw InvalidTopicException("Topic '$topic' is invalid")
                        else if (error === Errors.UNKNOWN_TOPIC_OR_PARTITION)
                        // if a requested topic is unknown, we just continue and let it be absent
                        // in the returned map
                            continue
                        else if (error.exception is RetriableException)
                            shouldRetry = true
                        else throw KafkaException(
                            message = "Unexpected error fetching metadata for topic $topic",
                            cause = error.exception,
                        )
                    }
                }
                if (!shouldRetry) {
                    val topicsPartitionInfos = HashMap<String, List<PartitionInfo>>()
                    for (topic: String in cluster.topics)
                        topicsPartitionInfos[topic] = cluster.partitionsForTopic(topic)

                    return topicsPartitionInfos
                }
            }
            timer.sleep(retryBackoffMs)
        } while (timer.isNotExpired)
        throw TimeoutException("Timeout expired while fetching topic metadata")
    }

    /**
     * Send Metadata Request to least loaded node in Kafka cluster asynchronously
     *
     * @return A future that indicates result of sent metadata request
     */
    private fun sendMetadataRequest(
        request: MetadataRequest.Builder,
    ): RequestFuture<ClientResponse> {
        val node = client.leastLoadedNode()
        return if (node == null) RequestFuture.noBrokersAvailable()
        else client.send(node, request)
    }

    private fun offsetResetStrategyTimestamp(partition: TopicPartition): Long? {
        return when (subscriptions.resetStrategy(partition)) {
            OffsetResetStrategy.EARLIEST -> ListOffsetsRequest.EARLIEST_TIMESTAMP
            OffsetResetStrategy.LATEST -> ListOffsetsRequest.LATEST_TIMESTAMP
            else -> null
        }
    }

    private fun timestampToOffsetResetStrategy(timestamp: Long): OffsetResetStrategy? {
        return when (timestamp) {
            ListOffsetsRequest.EARLIEST_TIMESTAMP -> OffsetResetStrategy.EARLIEST
            ListOffsetsRequest.LATEST_TIMESTAMP -> OffsetResetStrategy.LATEST
            else -> null
        }
    }

    /**
     * Reset offsets for all assigned partitions that require it.
     *
     * @throws org.apache.kafka.clients.consumer.NoOffsetForPartitionException If no offset reset
     * strategy is defined and one or more partitions aren't awaiting a seekToBeginning() or
     * seekToEnd().
     */
    fun resetOffsetsIfNeeded() {
        // Raise exception from previous offset fetch if there is one
        val exception = cachedListOffsetsException.getAndSet(null)
        if (exception != null) throw exception
        val partitions = subscriptions.partitionsNeedingReset(time.milliseconds())
        if (partitions.isEmpty()) return
        val offsetResetTimestamps: MutableMap<TopicPartition, Long> = HashMap()
        for (partition: TopicPartition in partitions) {
            val timestamp = offsetResetStrategyTimestamp(partition)
            if (timestamp != null) offsetResetTimestamps[partition] = timestamp
        }
        resetOffsetsAsync(offsetResetTimestamps)
    }

    /**
     * Validate offsets for all assigned partitions for which a leader change has been detected.
     */
    fun validateOffsetsIfNeeded() {
        val exception = cachedOffsetForLeaderException.getAndSet(null)
        if (exception != null) throw exception

        // Validate each partition against the current leader and epoch
        // If we see a new metadata version, check all partitions
        validatePositionsOnMetadataChange()

        // Collect positions needing validation, with backoff
        val partitionsToValidate = subscriptions.partitionsNeedingValidation(time.milliseconds())
            .filter { tp -> subscriptions.position(tp) != null }
            .associateWith { tp -> subscriptions.position(tp)!! }

        validateOffsetsAsync(partitionsToValidate)
    }

    fun offsetsForTimes(
        timestampsToSearch: Map<TopicPartition, Long>,
        timer: Timer,
    ): Map<TopicPartition, OffsetAndTimestamp?> {
        metadata.addTransientTopics(topicsForPartitions(timestampsToSearch.keys))

        try {
            val fetchedOffsets: Map<TopicPartition, ListOffsetData> = fetchOffsetsByTimes(
                timestampsToSearch,
                timer, true
            ).fetchedOffsets

            val offsetsByTimes: MutableMap<TopicPartition, OffsetAndTimestamp?> =
                timestampsToSearch.mapValues { null }.toMutableMap()

            offsetsByTimes.putAll(fetchedOffsets.mapValues { (_, offsetData) ->
                OffsetAndTimestamp(
                    offset = offsetData.offset!!,
                    timestamp = offsetData.timestamp!!,
                    leaderEpoch = offsetData.leaderEpoch,
                )
            })

            return offsetsByTimes
        } finally {
            metadata.clearTransientTopics()
        }
    }

    private fun fetchOffsetsByTimes(
        timestampsToSearch: Map<TopicPartition, Long>,
        timer: Timer,
        requireTimestamps: Boolean
    ): ListOffsetResult {
        val result = ListOffsetResult()
        if (timestampsToSearch.isEmpty()) return result
        val remainingToSearch = timestampsToSearch.toMutableMap()
        do {
            val future = sendListOffsetsRequests(remainingToSearch, requireTimestamps)
            future.addListener(object : RequestFutureListener<ListOffsetResult> {

                override fun onSuccess(value: ListOffsetResult) = synchronized(future) {
                    result.fetchedOffsets.putAll(value.fetchedOffsets)
                    remainingToSearch.keys.retainAll(value.partitionsToRetry)
                    for ((partition, offsetData) in value.fetchedOffsets) {

                        // if the interested partitions are part of the subscriptions, use the
                        // returned offset to update the subscription state as well:
                        // - with read-committed, the returned offset would be LSO;
                        // - with read-uncommitted, the returned offset would be HW;
                        if (subscriptions.isAssigned(partition)) {
                            val offset: Long = offsetData.offset!!
                            if (isolationLevel === IsolationLevel.READ_COMMITTED) {
                                log.trace(
                                    "Updating last stable offset for partition {} to {}",
                                    partition,
                                    offset,
                                )
                                subscriptions.updateLastStableOffset(partition, offset)
                            } else {
                                log.trace(
                                    "Updating high watermark for partition {} to {}",
                                    partition,
                                    offset,
                                )
                                subscriptions.updateHighWatermark(partition, offset)
                            }
                        }
                    }
                }

                override fun onFailure(e: RuntimeException) {
                    if (e !is RetriableException) throw future.exception()
                }
            })

            // if timeout is set to zero, do not try to poll the network client at all and return
            // empty immediately; otherwise try to get the results synchronously and throw timeout
            // exception if cannot complete in time
            if (timer.timeoutMs == 0L) return result
            client.poll(future, timer)

            if (!future.isDone) break
            else if (remainingToSearch.isEmpty()) return result
            else client.awaitMetadataUpdate(timer)
        } while (timer.isNotExpired)
        throw TimeoutException("Failed to get offsets by times in ${timer.elapsedMs}ms")
    }

    fun beginningOffsets(
        partitions: Collection<TopicPartition>,
        timer: Timer,
    ): Map<TopicPartition, Long> = beginningOrEndOffset(
        partitions = partitions,
        timestamp = ListOffsetsRequest.EARLIEST_TIMESTAMP,
        timer = timer,
    )

    fun endOffsets(
        partitions: Collection<TopicPartition>,
        timer: Timer,
    ): Map<TopicPartition, Long> = beginningOrEndOffset(
        partitions = partitions,
        timestamp = ListOffsetsRequest.LATEST_TIMESTAMP,
        timer = timer,
    )

    private fun beginningOrEndOffset(
        partitions: Collection<TopicPartition>,
        timestamp: Long,
        timer: Timer,
    ): Map<TopicPartition, Long> {
        metadata.addTransientTopics(topicsForPartitions(partitions))

        try {
            val timestampsToSearch = partitions.distinct().associateWith { timestamp }
            val result = fetchOffsetsByTimes(
                timestampsToSearch = timestampsToSearch,
                timer = timer,
                requireTimestamps = false,
            )

            return result.fetchedOffsets.mapValues { (_, value) -> value.offset!! }
        } finally {
            metadata.clearTransientTopics()
        }
    }

    /**
     * Return the fetched records, empty the record buffer and update the consumed position.
     *
     * NOTE: returning an [empty][Fetch.isEmpty] fetch guarantees the consumed position is not
     * updated.
     *
     * @return A [Fetch] for the requested partitions
     * @throws OffsetOutOfRangeException If there is OffsetOutOfRange error in fetchResponse and
     * the defaultResetPolicy is NONE
     * @throws TopicAuthorizationException If there is TopicAuthorization error in fetchResponse.
     */
    fun collectFetch(): Fetch<K, V> {
        val fetch: Fetch<K, V> = Fetch.empty()
        val pausedCompletedFetches: Queue<CompletedFetch> = ArrayDeque()
        var recordsRemaining = maxPollRecords
        try {
            while (recordsRemaining > 0) {
                val nextInLineFetch = nextInLineFetch
                if (nextInLineFetch == null || nextInLineFetch.isConsumed) {
                    val records = completedFetches.peek() ?: break
                    if (records.notInitialized()) try {
                        this.nextInLineFetch = initializeCompletedFetch(records)
                    } catch (e: Exception) {
                        // Remove a completedFetch upon a parse with exception if (1) it contains no
                        // records, and (2) there are no fetched records with actual content
                        // preceding this exception. The first condition ensures that the
                        // completedFetches is not stuck with the same completedFetch in cases such
                        // as the TopicAuthorizationException, and the second condition ensures that
                        // no potential data loss due to an exception in a following record.
                        val partition = records.partitionData
                        if (
                            fetch.isEmpty
                            && FetchResponse.recordsOrFail(partition).sizeInBytes() == 0
                        ) completedFetches.poll()

                        throw e
                    } else this.nextInLineFetch = records

                    completedFetches.poll()
                } else if (subscriptions.isPaused(nextInLineFetch.partition)) {
                    // when the partition is paused we add the records back to the completedFetches
                    // queue instead of draining them so that they can be returned on a subsequent
                    // poll if the partition is resumed at that time
                    log.debug(
                        "Skipping fetching records for assigned partition {} because it is paused",
                        nextInLineFetch.partition,
                    )
                    pausedCompletedFetches.add(nextInLineFetch)
                    this.nextInLineFetch = null
                } else {
                    val nextFetch = fetchRecords(nextInLineFetch, recordsRemaining)
                    recordsRemaining -= nextFetch.numRecords()
                    fetch.add(nextFetch)
                }
            }
        } catch (e: KafkaException) {
            if (fetch.isEmpty) throw e
        } finally {
            // add any polled completed fetches for paused partitions back to the completed fetches
            // queue to be re-evaluated in the next poll
            completedFetches.addAll(pausedCompletedFetches)
        }
        return fetch
    }

    private fun fetchRecords(completedFetch: CompletedFetch, maxRecords: Int): Fetch<K, V> {
        if (!subscriptions.isAssigned(completedFetch.partition)) {
            // this can happen when a rebalance happened before fetched records are returned to the
            // consumer's poll call
            log.debug(
                "Not returning fetched records for partition {} since it is no longer assigned",
                completedFetch.partition
            )
        } else if (!subscriptions.isFetchable(completedFetch.partition)) {
            // this can happen when a partition is paused before fetched records are returned to the
            // consumer's poll call or if the offset is being reset
            log.debug(
                "Not returning fetched records for assigned partition {} since it is no longer fetchable",
                completedFetch.partition
            )
        } else {
            val position = subscriptions.position(completedFetch.partition)
                ?: error("Missing position for fetchable partition ${completedFetch.partition}")
            if (completedFetch.nextFetchOffset == position.offset) {
                val partRecords = completedFetch.fetchRecords(maxRecords)
                log.trace(
                    "Returning {} fetched records at offset {} for assigned partition {}",
                    partRecords.size,
                    position,
                    completedFetch.partition,
                )
                var positionAdvanced = false
                if (completedFetch.nextFetchOffset > position.offset) {
                    val nextPosition = FetchPosition(
                        offset = completedFetch.nextFetchOffset,
                        offsetEpoch = completedFetch.lastEpoch,
                        currentLeader = position.currentLeader,
                    )
                    log.trace(
                        "Updating fetch position from {} to {} for partition {} and returning {} " +
                                "records from `poll()`",
                        position,
                        nextPosition,
                        completedFetch.partition,
                        partRecords.size,
                    )
                    subscriptions.position(completedFetch.partition, nextPosition)
                    positionAdvanced = true
                }
                val partitionLag = subscriptions.partitionLag(
                    tp = completedFetch.partition,
                    isolationLevel = isolationLevel,
                )
                if (partitionLag != null) sensors.recordPartitionLag(
                    tp = completedFetch.partition,
                    lag = partitionLag,
                )
                val lead = subscriptions.partitionLead(completedFetch.partition)
                if (lead != null) sensors.recordPartitionLead(completedFetch.partition, lead)

                return Fetch.forPartition(
                    partition = completedFetch.partition,
                    records = partRecords,
                    positionAdvanced = positionAdvanced,
                )
            } else {
                // these records aren't next in line based on the last consumed position, ignore
                // them they must be from an obsolete request
                log.debug(
                    "Ignoring fetched records for {} at offset {} since the current position is {}",
                    completedFetch.partition,
                    completedFetch.nextFetchOffset,
                    position
                )
            }
        }
        log.trace("Draining fetched records for partition {}", completedFetch.partition)
        completedFetch.drain()
        return Fetch.empty()
    }

    // Visible for testing
    fun resetOffsetIfNeeded(
        partition: TopicPartition,
        requestedResetStrategy: OffsetResetStrategy,
        offsetData: ListOffsetData,
    ) {
        val position = FetchPosition(
            offset = offsetData.offset!!,
            offsetEpoch = null, // This will ensure we skip validation
            currentLeader = metadata.currentLeader(partition)
        )
        offsetData.leaderEpoch?.let { epoch ->
            metadata.updateLastSeenEpochIfNewer(partition, epoch)
        }
        subscriptions.maybeSeekUnvalidated(partition, position, requestedResetStrategy)
    }

    private fun resetOffsetsAsync(partitionResetTimestamps: Map<TopicPartition, Long>) {
        val timestampsToSearchByNode = groupListOffsetRequests(partitionResetTimestamps, HashSet())
        for ((node, resetTimestamps) in timestampsToSearchByNode) {
            subscriptions.setNextAllowedRetry(
                partitions = resetTimestamps.keys,
                nextAllowResetTimeMs = time.milliseconds() + requestTimeoutMs,
            )
            val future = sendListOffsetRequest(
                node = node,
                timestampsToSearch = resetTimestamps,
                requireTimestamp = false,
            )

            future.addListener(object : RequestFutureListener<ListOffsetResult> {

                override fun onSuccess(value: ListOffsetResult) {
                    if (value.partitionsToRetry.isNotEmpty()) {
                        subscriptions.requestFailed(
                            value.partitionsToRetry,
                            time.milliseconds() + retryBackoffMs
                        )
                        metadata.requestUpdate()
                    }
                    for (fetchedOffset in value.fetchedOffsets) {
                        val partition = fetchedOffset.key
                        val offsetData = fetchedOffset.value
                        val requestedReset = resetTimestamps[partition]
                        resetOffsetIfNeeded(
                            partition = partition,
                            requestedResetStrategy =
                            timestampToOffsetResetStrategy(requestedReset!!.timestamp)!!,
                            offsetData = offsetData
                        )
                    }
                }

                override fun onFailure(e: RuntimeException) {
                    subscriptions.requestFailed(
                        partitions = resetTimestamps.keys,
                        nextRetryTimeMs = time.milliseconds() + retryBackoffMs,
                    )
                    metadata.requestUpdate()
                    if (
                        e !is RetriableException
                        && !cachedListOffsetsException.compareAndSet(null, e)
                    ) log.error(
                        "Discarding error in ListOffsetResponse because another error is pending",
                        e
                    )
                }
            })
        }
    }

    /**
     * For each partition which needs validation, make an asynchronous request to get the
     * end-offsets for the partition with the epoch less than or equal to the epoch the partition
     * last saw.
     *
     * Requests are grouped by Node for efficiency.
     */
    private fun validateOffsetsAsync(partitionsToValidate: Map<TopicPartition, FetchPosition>) {
        val regrouped = regroupFetchPositionsByLeader(partitionsToValidate)
        val nextResetTimeMs = time.milliseconds() + requestTimeoutMs
        regrouped.forEach { (node, fetchPositions) ->
            if (node.isEmpty) {
                metadata.requestUpdate()
                return@forEach
            }

            val nodeApiVersions: NodeApiVersions? = apiVersions[node.idString()]
            if (nodeApiVersions == null) {
                client.tryConnect(node)
                return@forEach
            }
            if (!hasUsableOffsetForLeaderEpochVersion(nodeApiVersions)) {
                log.debug(
                    "Skipping validation of fetch offsets for partitions {} since the broker " +
                            "does not support the required protocol version (introduced in Kafka " +
                            "2.3)",
                    fetchPositions.keys
                )
                for (partition in fetchPositions.keys) subscriptions.completeValidation(partition)
                return@forEach
            }
            subscriptions.setNextAllowedRetry(fetchPositions.keys, nextResetTimeMs)
            val future = offsetsForLeaderEpochClient.sendAsyncRequest(node, fetchPositions)

            future.addListener(object : RequestFutureListener<OffsetForEpochResult> {

                override fun onSuccess(value: OffsetForEpochResult) {
                    val truncations: MutableList<LogTruncation> =
                        ArrayList()
                    if (value.partitionsToRetry.isNotEmpty()) {
                        subscriptions.setNextAllowedRetry(
                            partitions = value.partitionsToRetry,
                            nextAllowResetTimeMs = time.milliseconds() + retryBackoffMs,
                        )
                        metadata.requestUpdate()
                    }

                    // For each OffsetsForLeader response, check if the end-offset is lower than our
                    // current offset for the partition. If so, it means we have experienced log
                    // truncation and need to reposition that partition's offset.
                    //
                    // In addition, check whether the returned offset and epoch are valid. If not,
                    // then we should reset its offset if reset policy is configured, or throw out
                    // of range exception.
                    value.endOffsets.forEach { (topicPartition, respEndOffset) ->
                        val requestPosition = fetchPositions[topicPartition]!!
                        val truncation = subscriptions.maybeCompleteValidation(
                            topicPartition,
                            requestPosition,
                            respEndOffset,
                        )
                        truncation?.let { e -> truncations.add(e) }
                    }
                    if (truncations.isNotEmpty())
                        maybeSetOffsetForLeaderException(buildLogTruncationException(truncations))
                }

                override fun onFailure(e: RuntimeException) {
                    subscriptions.requestFailed(
                        partitions = fetchPositions.keys,
                        nextRetryTimeMs = time.milliseconds() + retryBackoffMs,
                    )
                    metadata.requestUpdate()
                    if (e !is RetriableException) maybeSetOffsetForLeaderException(e)
                }
            })
        }
    }

    private fun buildLogTruncationException(
        truncations: List<LogTruncation>,
    ): LogTruncationException {
        val divergentOffsets = truncations.mapNotNull { truncation ->
            truncation.divergentOffset?.let { divergentOffset ->
                truncation.topicPartition to divergentOffset
            }
        }.toMap()

        val truncatedFetchOffsets = truncations.associate { truncation ->
            truncation.topicPartition to truncation.fetchPosition.offset
        }

        return LogTruncationException(
            "Detected truncated partitions: $truncations",
            truncatedFetchOffsets,
            divergentOffsets,
        )
    }

    private fun maybeSetOffsetForLeaderException(e: RuntimeException) {
        if (!cachedOffsetForLeaderException.compareAndSet(null, e)) log.error(
            "Discarding error in OffsetsForLeaderEpoch because another error is pending",
            e
        )
    }

    /**
     * Search the offsets by target times for the specified partitions.
     *
     * @param timestampsToSearch the mapping between partitions and target time
     * @param requireTimestamps true if we should fail with an UnsupportedVersionException if the
     * broker does not support fetching precise timestamps for offsets
     * @return A response which can be polled to obtain the corresponding timestamps and offsets.
     */
    private fun sendListOffsetsRequests(
        timestampsToSearch: Map<TopicPartition, Long>,
        requireTimestamps: Boolean,
    ): RequestFuture<ListOffsetResult> {
        val partitionsToRetry = mutableSetOf<TopicPartition>()
        val timestampsToSearchByNode =
            groupListOffsetRequests(timestampsToSearch, partitionsToRetry)

        if (timestampsToSearchByNode.isEmpty())
            return RequestFuture.failure(StaleMetadataException())

        val listOffsetRequestsFuture = RequestFuture<ListOffsetResult>()
        val fetchedTimestampOffsets: MutableMap<TopicPartition, ListOffsetData> = HashMap()
        val remainingResponses = AtomicInteger(timestampsToSearchByNode.size)

        for ((node, timestamps) in timestampsToSearchByNode) {
            val future = sendListOffsetRequest(
                node = node,
                timestampsToSearch = timestamps,
                requireTimestamp = requireTimestamps
            )
            future.addListener(object : RequestFutureListener<ListOffsetResult> {

                override fun onSuccess(value: ListOffsetResult) =
                    synchronized(listOffsetRequestsFuture) {
                        fetchedTimestampOffsets.putAll(value.fetchedOffsets)
                        partitionsToRetry.addAll(value.partitionsToRetry)
                        if (
                            remainingResponses.decrementAndGet() == 0
                            && !listOffsetRequestsFuture.isDone
                        ) {
                            val result = ListOffsetResult(
                                fetchedOffsets = fetchedTimestampOffsets,
                                partitionsToRetry = partitionsToRetry,
                            )
                            listOffsetRequestsFuture.complete(result)
                        }
                    }

                override fun onFailure(e: RuntimeException) =
                    synchronized(listOffsetRequestsFuture) {
                        if (!listOffsetRequestsFuture.isDone) listOffsetRequestsFuture.raise(e)
                    }
            })
        }
        return listOffsetRequestsFuture
    }

    /**
     * Groups timestamps to search by node for topic partitions in `timestampsToSearch` that have
     * leaders available. Topic partitions from `timestampsToSearch` that do not have their leader
     * available are added to `partitionsToRetry`.
     *
     * @param timestampsToSearch The mapping from partitions ot the target timestamps
     * @param partitionsToRetry A set of topic partitions that will be extended with partitions
     * that need metadata update or re-connect to the leader.
     */
    private fun groupListOffsetRequests(
        timestampsToSearch: Map<TopicPartition, Long>,
        partitionsToRetry: MutableSet<TopicPartition>
    ): Map<Node, Map<TopicPartition, ListOffsetsPartition>> {
        val partitionDataMap: MutableMap<TopicPartition, ListOffsetsPartition> = HashMap()
        for ((tp, offset) in timestampsToSearch) {
            val leaderAndEpoch = metadata.currentLeader(tp)
            if (leaderAndEpoch.leader == null) {
                log.debug("Leader for partition {} is unknown for fetching offset {}", tp, offset)
                metadata.requestUpdate()
                partitionsToRetry.add(tp)
            } else {
                val leader: Node = leaderAndEpoch.leader
                if (client.isUnavailable(leader)) {
                    client.maybeThrowAuthFailure(leader)

                    // The connection has failed and we need to await the backoff period before we
                    // can try again. No need to request a metadata update since the disconnect will
                    // have done so already.
                    log.debug(
                        "Leader {} for partition {} is unavailable for fetching offset until " +
                                "reconnect backoff expires",
                        leader,
                        tp,
                    )
                    partitionsToRetry.add(tp)
                } else {
                    val currentLeaderEpoch =
                        leaderAndEpoch.epoch ?: ListOffsetsResponse.UNKNOWN_EPOCH

                    partitionDataMap[tp] = ListOffsetsPartition()
                        .setPartitionIndex(tp.partition)
                        .setTimestamp(offset)
                        .setCurrentLeaderEpoch(currentLeaderEpoch)
                }
            }
        }
        return regroupPartitionMapByNode(partitionDataMap)
    }

    /**
     * Send the ListOffsetRequest to a specific broker for the partitions and target timestamps.
     *
     * @param node The node to send the ListOffsetRequest to.
     * @param timestampsToSearch The mapping from partitions to the target timestamps.
     * @param requireTimestamp  True if we require a timestamp in the response.
     * @return A response which can be polled to obtain the corresponding timestamps and offsets.
     */
    private fun sendListOffsetRequest(
        node: Node?,
        timestampsToSearch: Map<TopicPartition, ListOffsetsPartition>,
        requireTimestamp: Boolean,
    ): RequestFuture<ListOffsetResult> {
        val builder = ListOffsetsRequest.Builder.forConsumer(
            requireTimestamp = requireTimestamp,
            isolationLevel = isolationLevel,
            requireMaxTimestamp = false
        ).setTargetTimes(ListOffsetsRequest.toListOffsetsTopics(timestampsToSearch))

        log.debug("Sending ListOffsetRequest {} to broker {}", builder, node)
        return client.send(node!!, builder).compose(
            object : RequestFutureAdapter<ClientResponse, ListOffsetResult>() {
                override fun onSuccess(
                    value: ClientResponse,
                    future: RequestFuture<ListOffsetResult>,
                ) {
                    val lor = value.responseBody as ListOffsetsResponse
                    log.trace("Received ListOffsetResponse {} from broker {}", lor, node)
                    handleListOffsetResponse(lor, future)
                }
            }
        )
    }

    /**
     * Callback for the response of the list offset call above.
     *
     * @param listOffsetsResponse The response from the server.
     * @param future The future to be completed when the response returns. Note that any
     * partition-level errors will generally fail the entire future result. The one exception is
     * UNSUPPORTED_FOR_MESSAGE_FORMAT, which indicates that the broker does not support the v1
     * message format. Partitions with this particular error are simply left out of the future map.
     * Note that the corresponding timestamp value of each partition may be null only for v0. In v1
     * and later the ListOffset API would not return a null timestamp (-1 is returned instead when
     * necessary).
     */
    private fun handleListOffsetResponse(
        listOffsetsResponse: ListOffsetsResponse,
        future: RequestFuture<ListOffsetResult>,
    ) {
        val fetchedOffsets = mutableMapOf<TopicPartition, ListOffsetData>()
        val partitionsToRetry = mutableSetOf<TopicPartition>()
        val unauthorizedTopics = mutableSetOf<String>()

        for (topic in listOffsetsResponse.topics) {
            for (partition in topic.partitions) {
                val topicPartition = TopicPartition(topic.name, partition.partitionIndex)

                when (val error = Errors.forCode(partition.errorCode)) {
                    Errors.NONE -> if (partition.oldStyleOffsets.isNotEmpty()) {
                        // Handle v0 response with offsets
                        var offset: Long
                        if (partition.oldStyleOffsets.size > 1) {
                            future.raise(
                                IllegalStateException(
                                    "Unexpected partitionData response of length " +
                                            partition.oldStyleOffsets.size
                                )
                            )
                            return
                        } else offset = partition.oldStyleOffsets[0]

                        log.debug(
                            "Handling v0 ListOffsetResponse response for {}. Fetched offset {}",
                            topicPartition,
                            offset,
                        )
                        if (offset != ListOffsetsResponse.UNKNOWN_OFFSET) {
                            val offsetData = ListOffsetData(
                                offset = offset,
                                timestamp = null,
                                leaderEpoch = null,
                            )
                            fetchedOffsets[topicPartition] = offsetData
                        }
                    } else {
                        // Handle v1 and later response or v0 without offsets
                        log.debug(
                            "Handling ListOffsetResponse response for {}. Fetched offset {}, timestamp {}",
                            topicPartition,
                            partition.offset,
                            partition.timestamp,
                        )
                        if (partition.offset != ListOffsetsResponse.UNKNOWN_OFFSET) {
                            val leaderEpoch =
                                if (partition.leaderEpoch == ListOffsetsResponse.UNKNOWN_EPOCH) null
                                else partition.leaderEpoch
                            val offsetData = ListOffsetData(
                                offset = partition.offset,
                                timestamp = partition.timestamp,
                                leaderEpoch = leaderEpoch
                            )
                            fetchedOffsets[topicPartition] = offsetData
                        }
                    }

                    Errors.UNSUPPORTED_FOR_MESSAGE_FORMAT ->
                        // The message format on the broker side is before 0.10.0, which means it
                        // does not support timestamps. We treat this case the same as if we weren't
                        // able to find an offset corresponding to the requested timestamp and leave
                        // it out of the result.
                        log.debug(
                            "Cannot search by timestamp for partition {} because the message " +
                                    "format version is before 0.10.0",
                            topicPartition,
                        )

                    Errors.NOT_LEADER_OR_FOLLOWER,
                    Errors.REPLICA_NOT_AVAILABLE,
                    Errors.KAFKA_STORAGE_ERROR,
                    Errors.OFFSET_NOT_AVAILABLE,
                    Errors.LEADER_NOT_AVAILABLE,
                    Errors.FENCED_LEADER_EPOCH,
                    Errors.UNKNOWN_LEADER_EPOCH -> {
                        log.debug(
                            "Attempt to fetch offsets for partition {} failed due to {}, retrying.",
                            topicPartition,
                            error,
                        )
                        partitionsToRetry.add(topicPartition)
                    }

                    Errors.UNKNOWN_TOPIC_OR_PARTITION -> {
                        log.warn(
                            "Received unknown topic or partition error in ListOffset request for " +
                                    "partition {}",
                            topicPartition
                        )
                        partitionsToRetry.add(topicPartition)
                    }

                    Errors.TOPIC_AUTHORIZATION_FAILED ->
                        unauthorizedTopics.add(topicPartition.topic)

                    else -> {
                        log.warn(
                            "Attempt to fetch offsets for partition {} failed due to unexpected " +
                                    "exception: {}, retrying.",
                            topicPartition,
                            error.message,
                        )
                        partitionsToRetry.add(topicPartition)
                    }
                }
            }
        }
        if (unauthorizedTopics.isNotEmpty())
            future.raise(TopicAuthorizationException(unauthorizedTopics))
        else future.complete(ListOffsetResult(fetchedOffsets, partitionsToRetry))
    }

    private fun fetchablePartitions(): List<TopicPartition> {
        val exclude: MutableSet<TopicPartition?> = HashSet()
        if (nextInLineFetch != null && !nextInLineFetch!!.isConsumed) {
            exclude.add(nextInLineFetch!!.partition)
        }
        for (completedFetch: CompletedFetch in completedFetches) {
            exclude.add(completedFetch.partition)
        }
        return subscriptions.fetchablePartitions { tp -> !exclude.contains(tp) }
    }

    /**
     * Determine which replica to read from.
     */
    fun selectReadReplica(
        partition: TopicPartition,
        leaderReplica: Node,
        currentTimeMs: Long,
    ): Node {
        val nodeId: Int? = subscriptions.preferredReadReplica(partition, currentTimeMs)
        return if (nodeId != null) {
            val node = metadata.fetch().nodeIfOnline(partition, nodeId)
            if (node != null) node
            else {
                log.trace(
                    "Not fetching from {} for partition {} since it is marked offline or is " +
                            "missing from our metadata, using the leader instead.",
                    nodeId,
                    partition,
                )
                // Note that this condition may happen due to stale metadata, so we clear preferred
                // replica and refresh metadata.
                requestMetadataUpdate(partition)
                leaderReplica
            }
        } else leaderReplica
    }

    /**
     * If we have seen new metadata (as tracked by
     * [org.apache.kafka.clients.Metadata.updateVersion]), then we should check that all the
     * assignments have a valid position.
     */
    private fun validatePositionsOnMetadataChange() {
        val newMetadataUpdateVersion = metadata.updateVersion()
        if (metadataUpdateVersion.getAndSet(newMetadataUpdateVersion) != newMetadataUpdateVersion) {
            subscriptions.assignedPartitions().forEach { topicPartition ->
                val leaderAndEpoch = metadata.currentLeader(topicPartition)

                subscriptions.maybeValidatePositionForCurrentLeader(
                    apiVersions = apiVersions,
                    tp = topicPartition,
                    leaderAndEpoch = leaderAndEpoch,
                )
            }
        }
    }

    /**
     * Create fetch requests for all nodes for which we have assigned partitions that have no
     * existing requests in flight.
     */
    private fun prepareFetchRequests(): Map<Node, FetchSessionHandler.FetchRequestData> {
        val fetchable = mutableMapOf<Node, FetchSessionHandler.Builder>()
        validatePositionsOnMetadataChange()
        val currentTimeMs = time.milliseconds()
        val topicIds = metadata.topicIds()
        for (partition in fetchablePartitions()) {
            val position = subscriptions.position(partition)
                ?: error("Missing position for fetchable partition $partition")
            val leaderOpt = position.currentLeader.leader
            if (leaderOpt == null) {
                log.debug(
                    "Requesting metadata update for partition {} since the position {} is " +
                            "missing the current leader node",
                    partition,
                    position
                )
                metadata.requestUpdate()
                continue
            }

            // Use the preferred read replica if set, otherwise the partition's leader
            val node = selectReadReplica(partition, leaderOpt, currentTimeMs)
            if (client.isUnavailable(node)) {
                client.maybeThrowAuthFailure(node)

                // If we try to send during the reconnect backoff window, then the request is just
                // going to be failed anyway before being sent, so skip the send for now
                log.trace(
                    "Skipping fetch for partition {} because node {} is awaiting reconnect backoff",
                    partition,
                    node
                )
            } else if (nodesWithPendingFetchRequests.contains(node.id)) log.trace(
                "Skipping fetch for partition {} because previous request to {} has not been " +
                        "processed",
                partition,
                node
            ) else {
                // if there is a leader and no in-flight requests, issue a new fetch
                val builder = fetchable[node] ?: run {
                    val id = node.id
                    var handler = sessionHandler(id)
                    if (handler == null) {
                        handler = FetchSessionHandler(logContext, id)
                        sessionHandlers[id] = handler
                    }
                    handler.newBuilder().also { fetchable[node] = it }
                }
                builder.add(
                    partition, FetchRequest.PartitionData(
                        topicId = topicIds.getOrDefault(partition.topic, Uuid.ZERO_UUID),
                        fetchOffset = position.offset,
                        logStartOffset = FetchRequest.INVALID_LOG_START_OFFSET,
                        maxBytes = fetchSize,
                        currentLeaderEpoch = position.currentLeader.epoch,
                    )
                )
                log.debug(
                    "Added {} fetch request for partition {} at position {} to node {}",
                    isolationLevel,
                    partition,
                    position,
                    node,
                )
            }
        }
        return fetchable.mapValues { (_, value) -> value.build() }
    }

    private fun regroupFetchPositionsByLeader(
        partitionMap: Map<TopicPartition, FetchPosition>,
    ): Map<Node, Map<TopicPartition, FetchPosition>> {
        return partitionMap.filter { it.value.currentLeader.leader != null }
            .entries.groupBy { it.value.currentLeader.leader!! }
            .mapValues { (_, value) -> value.associate { it.key to it.value } }
    }

    private fun <T> regroupPartitionMapByNode(
        partitionMap: Map<TopicPartition, T>,
    ): Map<Node, Map<TopicPartition, T>> {
        return partitionMap.entries.groupBy { metadata.fetch().leaderFor(it.key)!! }
            .mapValues { (_, value) -> value.associate { it.key to it.value } }
    }

    /**
     * Initialize a CompletedFetch object.
     */
    private fun initializeCompletedFetch(nextCompletedFetch: CompletedFetch): CompletedFetch? {
        val tp = nextCompletedFetch.partition
        val partition = nextCompletedFetch.partitionData
        val fetchOffset = nextCompletedFetch.nextFetchOffset
        var completedFetch: CompletedFetch? = null
        val error = Errors.forCode(partition.errorCode)

        try {
            if (!subscriptions.hasValidPosition(tp)) {
                // this can happen when a rebalance happened while fetch is still in-flight
                log.debug(
                    "Ignoring fetched records for partition {} since it no longer has valid position",
                    tp
                )
            } else if (error === Errors.NONE) {
                // we are interested in this fetch only if the beginning offset matches the
                // current consumed position
                val position = subscriptions.position(tp)
                if (position == null || position.offset != fetchOffset) {
                    log.debug(
                        "Discarding stale fetch response for partition {} since its offset {} " +
                                "does not match the expected offset {}",
                        tp,
                        fetchOffset,
                        position,
                    )
                    return null
                }
                log.trace(
                    "Preparing to read {} bytes of data for partition {} with offset {}",
                    FetchResponse.recordsSize(partition),
                    tp,
                    position,
                )
                val batches = FetchResponse.recordsOrFail(partition).batches().iterator()
                completedFetch = nextCompletedFetch
                if (!batches.hasNext() && FetchResponse.recordsSize(partition) > 0) {
                    if (completedFetch.responseVersion < 3) {
                        // Implement the pre KIP-74 behavior of throwing a RecordTooLargeException.
                        val recordTooLargePartitions = mapOf(tp to fetchOffset)
                        throw RecordTooLargeException(
                            message = "There are some messages at [Partition=Offset]: " +
                                    "$recordTooLargePartitions whose size is larger than the " +
                                    "fetch size $fetchSize and hence cannot be returned. Please " +
                                    "considering upgrading your broker to 0.10.1.0 or newer to " +
                                    "avoid this issue. Alternately, increase the fetch size on " +
                                    "the client (using " +
                                    "${ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG})",
                            recordTooLargePartitions = recordTooLargePartitions,
                        )
                    } else {
                        // This should not happen with brokers that support FetchRequest/Response V3
                        // or higher (i.e. KIP-74)
                        throw KafkaException(
                            "Failed to make progress reading messages at $tp=$fetchOffset. " +
                                    "Received a non-empty fetch response from the server, but no " +
                                    "complete records were found."
                        )
                    }
                }
                if (partition.highWatermark >= 0) {
                    log.trace(
                        "Updating high watermark for partition {} to {}",
                        tp,
                        partition.highWatermark,
                    )
                    subscriptions.updateHighWatermark(tp, partition.highWatermark)
                }
                if (partition.logStartOffset >= 0) {
                    log.trace(
                        "Updating log start offset for partition {} to {}",
                        tp,
                        partition.logStartOffset,
                    )
                    subscriptions.updateLogStartOffset(tp, partition.logStartOffset)
                }
                if (partition.lastStableOffset >= 0) {
                    log.trace(
                        "Updating last stable offset for partition {} to {}",
                        tp,
                        partition.lastStableOffset,
                    )
                    subscriptions.updateLastStableOffset(tp, partition.lastStableOffset)
                }
                if (FetchResponse.isPreferredReplica(partition)) {
                    subscriptions.updatePreferredReadReplica(
                        tp = completedFetch.partition,
                        preferredReadReplicaId = partition.preferredReadReplica,
                    ) {
                        val expireTimeMs = time.milliseconds() + metadata.metadataExpireMs()
                        log.debug(
                            "Updating preferred read replica for partition {} to {}, set to " +
                                    "expire at {}",
                            tp,
                            partition.preferredReadReplica,
                            expireTimeMs,
                        )
                        expireTimeMs
                    }
                }
                nextCompletedFetch.initialized = true
            } else if (
                error === Errors.NOT_LEADER_OR_FOLLOWER
                || error === Errors.REPLICA_NOT_AVAILABLE
                || error === Errors.KAFKA_STORAGE_ERROR
                || error === Errors.FENCED_LEADER_EPOCH
                || error === Errors.OFFSET_NOT_AVAILABLE
            ) {
                log.debug("Error in fetch for partition {}: {}", tp, error.exceptionName)
                requestMetadataUpdate(tp)
            } else if (error === Errors.UNKNOWN_TOPIC_OR_PARTITION) {
                log.warn("Received unknown topic or partition error in fetch for partition {}", tp)
                requestMetadataUpdate(tp)
            } else if (error === Errors.UNKNOWN_TOPIC_ID) {
                log.warn("Received unknown topic ID error in fetch for partition {}", tp)
                requestMetadataUpdate(tp)
            } else if (error === Errors.INCONSISTENT_TOPIC_ID) {
                log.warn("Received inconsistent topic ID error in fetch for partition {}", tp)
                requestMetadataUpdate(tp)
            } else if (error === Errors.OFFSET_OUT_OF_RANGE) {
                val clearedReplicaId = subscriptions.clearPreferredReadReplica(tp)
                if (clearedReplicaId == null) {
                    // If there's no preferred replica to clear, we're fetching from the leader so
                    // handle this error normally
                    val position = subscriptions.position(tp)
                    if (position == null || fetchOffset != position.offset) {
                        log.debug(
                            "Discarding stale fetch response for partition {} since the fetched " +
                                    "offset {} does not match the current offset {}",
                            tp,
                            fetchOffset,
                            position
                        )
                    } else handleOffsetOutOfRange(position, tp)
                } else log.debug(
                    "Unset the preferred read replica {} for partition {} since we got {} when " +
                            "fetching {}",
                    clearedReplicaId,
                    tp,
                    error,
                    fetchOffset
                )
            } else if (error === Errors.TOPIC_AUTHORIZATION_FAILED) {
                //we log the actual partition and not just the topic to help with ACL propagation
                // issues in large clusters
                log.warn("Not authorized to read from partition {}.", tp)
                throw TopicAuthorizationException(setOf(tp.topic))
            } else if (error === Errors.UNKNOWN_LEADER_EPOCH)
                log.debug("Received unknown leader epoch error in fetch for partition {}", tp)
            else if (error === Errors.UNKNOWN_SERVER_ERROR) log.warn(
                "Unknown server error while fetching offset {} for topic-partition {}",
                fetchOffset, tp
            ) else if (error === Errors.CORRUPT_MESSAGE) {
                throw KafkaException(
                    "Encountered corrupt message when fetching offset $fetchOffset for " +
                            "topic-partition $tp",
                )
            } else error(
                "Unexpected error code ${error.code} while fetching at offset $fetchOffset from " +
                        "topic-partition $tp"
            )
        } finally {
            if (completedFetch == null) nextCompletedFetch.metricAggregator.record(
                partition = tp,
                bytes = 0,
                records = 0,
            )
            if (error !== Errors.NONE)
            // we move the partition to the end if there was an error. This way, it's more
            // likely that partitions for the same topic can remain together (allowing for more
            // efficient serialization).
                subscriptions.movePartitionToEnd(tp)
        }
        return completedFetch
    }

    private fun handleOffsetOutOfRange(
        fetchPosition: FetchPosition,
        topicPartition: TopicPartition,
    ) {
        val errorMessage =
            "Fetch position $fetchPosition is out of range for partition $topicPartition"
        if (subscriptions.hasDefaultOffsetResetPolicy()) {
            log.info("{}, resetting offset", errorMessage)
            subscriptions.requestOffsetReset(topicPartition)
        } else {
            log.info(
                "{}, raising error to the application since no reset policy is configured",
                errorMessage,
            )
            throw OffsetOutOfRangeException(
                errorMessage,
                mapOf(topicPartition to fetchPosition.offset),
            )
        }
    }

    /**
     * Parse the record entry, deserializing the key / value fields if necessary
     */
    private fun parseRecord(
        partition: TopicPartition,
        batch: RecordBatch,
        record: Record,
    ): ConsumerRecord<*, *> {
        try {
            val offset = record.offset()
            val timestamp = record.timestamp()
            val leaderEpoch = maybeLeaderEpoch(batch.partitionLeaderEpoch())
            val timestampType = batch.timestampType()
            val headers: Headers = RecordHeaders(record.headers())
            val keyBytes = record.key()
            val keyByteArray = if (keyBytes == null) null else Utils.toArray(keyBytes)
            val key = if (keyBytes == null) null else keyDeserializer.deserialize(
                topic = partition.topic,
                headers = headers,
                data = keyByteArray,
            )
            val valueBytes = record.value()
            val valueByteArray = if (valueBytes == null) null else Utils.toArray(valueBytes)
            val value = if (valueBytes == null) null else valueDeserializer.deserialize(
                topic = partition.topic,
                headers = headers,
                data = valueByteArray,
            )
            return ConsumerRecord(
                topic = partition.topic,
                partition = partition.partition,
                offset = offset,
                timestamp = timestamp,
                timestampType = timestampType,
                serializedKeySize = keyByteArray?.size ?: ConsumerRecord.NULL_SIZE,
                serializedValueSize = valueByteArray?.size ?: ConsumerRecord.NULL_SIZE,
                key = key,
                value = value,
                headers = headers,
                leaderEpoch = leaderEpoch,
            )
        } catch (e: RuntimeException) {
            throw RecordDeserializationException(
                partition = partition,
                offset = record.offset(),
                message = "Error deserializing key/value for partition $partition at offset " +
                        "${record.offset()}. If needed, please seek past the record to continue " +
                        "consumption.",
                cause = e,
            )
        }
    }

    private fun maybeLeaderEpoch(leaderEpoch: Int): Int? {
        return if (leaderEpoch == RecordBatch.NO_PARTITION_LEADER_EPOCH) null
        else leaderEpoch
    }

    /**
     * Clear the buffered data which are not a part of newly assigned partitions
     *
     * @param assignedPartitions  newly assigned [TopicPartition]
     */
    fun clearBufferedDataForUnassignedPartitions(assignedPartitions: Collection<TopicPartition>) {
        val completedFetchesItr = completedFetches.iterator()
        while (completedFetchesItr.hasNext()) {
            val records = completedFetchesItr.next()
            val tp = records.partition
            if (!assignedPartitions.contains(tp)) {
                records.drain()
                completedFetchesItr.remove()
            }
        }
        nextInLineFetch?.let { nextInLineFetch ->
            if (!assignedPartitions.contains(nextInLineFetch.partition)) {
                nextInLineFetch.drain()
                this.nextInLineFetch = null
            }
        }
    }

    /**
     * Clear the buffered data which are not a part of newly assigned topics
     *
     * @param assignedTopics  newly assigned topics
     */
    fun clearBufferedDataForUnassignedTopics(assignedTopics: Collection<String>) {
        val currentTopicPartitions = subscriptions.assignedPartitions()
            .filter { tp -> assignedTopics.contains(tp.topic) }

        clearBufferedDataForUnassignedPartitions(currentTopicPartitions)
    }

    // Visible for testing
    internal open fun sessionHandler(node: Int): FetchSessionHandler? = sessionHandlers[node]

    private inner class CompletedFetch(
        val partition: TopicPartition,
        val partitionData: FetchResponseData.PartitionData,
        val metricAggregator: FetchResponseMetricAggregator,
        private val batches: Iterator<RecordBatch>,
        var nextFetchOffset: Long,
        val responseVersion: Short,
    ) {

        private val abortedProducerIds = mutableSetOf<Long>()

        private val abortedTransactions: PriorityQueue<AbortedTransaction>? =
            abortedTransactions(partitionData)

        private var recordsRead = 0

        private var bytesRead = 0

        private var currentBatch: RecordBatch? = null

        private var lastRecord: Record? = null

        private var records: CloseableIterator<Record>? = null

        var lastEpoch: Int? = null

        var isConsumed = false

        private var cachedRecordException: Exception? = null

        private var corruptLastRecord = false

        var initialized = false

        fun drain() {
            if (isConsumed) return
            maybeCloseRecordStream()
            cachedRecordException = null
            isConsumed = true
            metricAggregator.record(
                partition = partition,
                bytes = bytesRead,
                records = recordsRead,
            )

            // we move the partition to the end if we received some bytes. This way, it's more
            // likely that partitions for the same topic can remain together (allowing for more
            // efficient serialization).
            if (bytesRead > 0) subscriptions.movePartitionToEnd(partition)
        }

        private fun maybeEnsureValid(batch: RecordBatch) {
            if (checkCrcs && currentBatch!!.magic() >= RecordBatch.MAGIC_VALUE_V2) {
                try {
                    batch.ensureValid()
                } catch (e: CorruptRecordException) {
                    throw KafkaException(
                        "Record batch for partition $partition at offset ${batch.baseOffset()} " +
                                "is invalid, cause: ${e.message}"
                    )
                }
            }
        }

        private fun maybeEnsureValid(record: Record) {
            if (!checkCrcs) return

            try {
                record.ensureValid()
            } catch (e: CorruptRecordException) {
                throw KafkaException(
                    "Record for partition $partition at offset ${record.offset()} is invalid, " +
                            "cause: ${e.message}"
                )
            }
        }

        private fun maybeCloseRecordStream() {
            records?.let {
                it.close()
                records = null
            }
        }

        private fun nextFetchedRecord(): Record? {
            while (true) {
                if (records?.hasNext() != true) {
                    maybeCloseRecordStream()
                    if (!batches.hasNext()) {
                        // Message format v2 preserves the last offset in a batch even if the last
                        // record is removed through compaction. By using the next offset computed
                        // from the last offset in the batch, we ensure that the offset of the next
                        // fetch will point to the next batch, which avoids unnecessary re-fetching
                        // of the same batch (in the worst case, the consumer could get stuck
                        // fetching the same batch repeatedly).
                        currentBatch?.let { nextFetchOffset = it.nextOffset() }
                        drain()
                        return null
                    }
                    val currentBatch = batches.next()
                    this.currentBatch = currentBatch

                    lastEpoch =
                        if (currentBatch.partitionLeaderEpoch() == RecordBatch.NO_PARTITION_LEADER_EPOCH) null
                        else currentBatch.partitionLeaderEpoch()
                    maybeEnsureValid(currentBatch)

                    if (isolationLevel === IsolationLevel.READ_COMMITTED && currentBatch.hasProducerId()) {
                        // remove from the aborted transaction queue all aborted transactions which have begun
                        // before the current batch's last offset and add the associated producerIds to the
                        // aborted producer set
                        consumeAbortedTransactionsUpTo(currentBatch.lastOffset())
                        val producerId = currentBatch.producerId()
                        if (containsAbortMarker(currentBatch)) abortedProducerIds.remove(producerId)
                        else if (isBatchAborted(currentBatch)) {
                            log.debug(
                                "Skipping aborted record batch from partition {} with producerId " +
                                        "{} and offsets {} to {}",
                                partition,
                                producerId,
                                currentBatch.baseOffset(),
                                currentBatch.lastOffset(),
                            )
                            nextFetchOffset = currentBatch.nextOffset()
                            continue
                        }
                    }
                    records = currentBatch.streamingIterator(decompressionBufferSupplier)
                } else {
                    val record = records!!.next()
                    // skip any records out of range
                    if (record.offset() >= nextFetchOffset) {
                        // we only do validation when the message should not be skipped.
                        maybeEnsureValid(record)

                        // control records are not returned to the user
                        if (!currentBatch!!.isControlBatch) return record
                        else // Increment the next fetch offset when we skip a control batch.
                            nextFetchOffset = record.offset() + 1
                    }
                }
            }
        }

        fun fetchRecords(maxRecords: Int): List<ConsumerRecord<K, V>> {
            // Error when fetching the next record before deserialization.
            if (corruptLastRecord) throw KafkaException(
                "Received exception when fetching the next record from $partition. If needed, " +
                        "please seek past the record to continue consumption.",
                cachedRecordException,
            )
            if (isConsumed) return emptyList()
            val records = mutableListOf<ConsumerRecord<K, V>>()

            try {
                for (i in 0 until maxRecords) {
                    // Only move to next record if there was no exception in the last fetch.
                    // Otherwise we should use the last record to do deserialization again.
                    if (cachedRecordException == null) {
                        corruptLastRecord = true
                        lastRecord = nextFetchedRecord()
                        corruptLastRecord = false
                    }
                    if (lastRecord == null) break
                    records.add(
                        parseRecord(
                            partition = partition,
                            batch = currentBatch!!,
                            record = lastRecord!!,
                        ) as ConsumerRecord<K, V>
                    )
                    recordsRead++
                    bytesRead += lastRecord!!.sizeInBytes()
                    nextFetchOffset = lastRecord!!.offset() + 1
                    // In some cases, the deserialization may have thrown an exception and the retry
                    // may succeed, we allow user to move forward in this case.
                    cachedRecordException = null
                }
            } catch (se: SerializationException) {
                cachedRecordException = se
                if (records.isEmpty()) throw se
            } catch (e: KafkaException) {
                cachedRecordException = e
                if (records.isEmpty()) throw KafkaException(
                    "Received exception when fetching the next record from $partition. If " +
                            "needed, please seek past the record to continue consumption.",
                    e,
                )
            }
            return records
        }

        private fun consumeAbortedTransactionsUpTo(offset: Long) {
            if (abortedTransactions == null) return
            while (!abortedTransactions.isEmpty() && abortedTransactions.peek()
                    .firstOffset <= offset
            ) {
                val abortedTransaction = abortedTransactions.poll()
                abortedProducerIds.add(abortedTransaction.producerId)
            }
        }

        private fun isBatchAborted(batch: RecordBatch?): Boolean {
            return batch!!.isTransactional && abortedProducerIds.contains(batch.producerId())
        }

        private fun abortedTransactions(
            partition: FetchResponseData.PartitionData,
        ): PriorityQueue<AbortedTransaction>? {
            val abortedTransactionsList = partition.abortedTransactions
            if (abortedTransactionsList.isEmpty()) return null

            val abortedTransactions = PriorityQueue(
                abortedTransactionsList.size,
                Comparator.comparingLong<AbortedTransaction> { it.firstOffset },
            )
            abortedTransactions.addAll(abortedTransactionsList)
            return abortedTransactions
        }

        private fun containsAbortMarker(batch: RecordBatch?): Boolean {
            if (!batch!!.isControlBatch) return false
            val batchIterator = batch.iterator()
            if (!batchIterator.hasNext()) return false
            val firstRecord = batchIterator.next()
            return ControlRecordType.ABORT === ControlRecordType.parse((firstRecord.key())!!)
        }

        fun notInitialized(): Boolean {
            return !initialized
        }
    }

    /**
     * Since we parse the message data for each partition from each fetch response lazily, fetch-level
     * metrics need to be aggregated as the messages from each partition are parsed. This class is used
     * to facilitate this incremental aggregation.
     */
    private class FetchResponseMetricAggregator(
        sensors: FetchManagerMetrics,
        partitions: MutableSet<TopicPartition>
    ) {
        private val sensors: FetchManagerMetrics
        private val unrecordedPartitions: MutableSet<TopicPartition>
        private val fetchMetrics = FetchMetrics()
        private val topicFetchMetrics: MutableMap<String, FetchMetrics> = HashMap()

        init {
            this.sensors = sensors
            unrecordedPartitions = partitions
        }

        /**
         * After each partition is parsed, we update the current metric totals with the total bytes
         * and number of records parsed. After all partitions have reported, we write the metric.
         */
        fun record(partition: TopicPartition, bytes: Int, records: Int) {
            unrecordedPartitions.remove(partition)
            fetchMetrics.increment(bytes, records)

            // collect and aggregate per-topic metrics
            val topic = partition.topic
            var topicFetchMetric = topicFetchMetrics[topic]
            if (topicFetchMetric == null) {
                topicFetchMetric = FetchMetrics()
                topicFetchMetrics[topic] = topicFetchMetric
            }
            topicFetchMetric.increment(bytes, records)
            if (unrecordedPartitions.isEmpty()) {
                // once all expected partitions from the fetch have reported in, record the metrics
                sensors.bytesFetched.record(fetchMetrics.fetchBytes.toDouble())
                sensors.recordsFetched.record(fetchMetrics.fetchRecords.toDouble())

                // also record per-topic metrics
                for ((key, metric) in topicFetchMetrics) {
                    sensors.recordTopicFetchMetrics(
                        topic = key,
                        bytes = metric.fetchBytes,
                        records = metric.fetchRecords,
                    )
                }
            }
        }

        private class FetchMetrics {

            var fetchBytes = 0

            var fetchRecords = 0

            fun increment(bytes: Int, records: Int) {
                fetchBytes += bytes
                fetchRecords += records
            }
        }
    }

    private class FetchManagerMetrics(
        private val metrics: Metrics,
        private val metricsRegistry: FetcherMetricsRegistry,
    ) {

        val bytesFetched: Sensor = metrics.sensor("bytes-fetched").apply {
            add(metrics.metricInstance(metricsRegistry.fetchSizeAvg), Avg())
            add(metrics.metricInstance(metricsRegistry.fetchSizeMax), Max())
            add(
                Meter(
                    rateMetricName = metrics.metricInstance(metricsRegistry.bytesConsumedRate),
                    totalMetricName = metrics.metricInstance(metricsRegistry.bytesConsumedTotal),
                )
            )
        }

        val recordsFetched: Sensor = metrics.sensor("records-fetched").apply {
            add(metrics.metricInstance(metricsRegistry.recordsPerRequestAvg), Avg())
            add(
                Meter(
                    rateMetricName = metrics.metricInstance(metricsRegistry.recordsConsumedRate),
                    totalMetricName = metrics.metricInstance(metricsRegistry.recordsConsumedTotal),
                )
            )
        }

        val fetchLatency: Sensor = metrics.sensor("fetch-latency").apply {
            add(metrics.metricInstance(metricsRegistry.fetchLatencyAvg), Avg())
            add(metrics.metricInstance(metricsRegistry.fetchLatencyMax), Max())
            add(
                Meter(
                    rateStat = WindowedCount(),
                    rateMetricName = metrics.metricInstance(metricsRegistry.fetchRequestRate),
                    totalMetricName = metrics.metricInstance(metricsRegistry.fetchRequestTotal),
                )
            )
        }

        private val recordsFetchLag: Sensor = metrics.sensor("records-lag").apply {
            add(metrics.metricInstance(metricsRegistry.recordsLagMax), Max())
        }

        private val recordsFetchLead: Sensor = metrics.sensor("records-lead").apply {
            add(metrics.metricInstance(metricsRegistry.recordsLeadMin), Min())
        }

        private var assignmentId = 0

        private var assignedPartitions = emptySet<TopicPartition>()

        fun recordTopicFetchMetrics(topic: String, bytes: Int, records: Int) {
            // record bytes fetched
            val bytesFetchedName = "topic.$topic.bytes-fetched"
            var bytesFetched = metrics.getSensor(bytesFetchedName)
            if (bytesFetched == null) {
                val metricTags = mapOf("topic" to topic.replace('.', '_'))

                bytesFetched = metrics.sensor(bytesFetchedName)
                bytesFetched.add(
                    metricName = metrics.metricInstance(
                        template = metricsRegistry.topicFetchSizeAvg,
                        tags = metricTags
                    ),
                    stat = Avg()
                )
                bytesFetched.add(
                    metricName = metrics.metricInstance(
                        template = metricsRegistry.topicFetchSizeMax,
                        tags = metricTags
                    ),
                    stat = Max()
                )
                bytesFetched.add(
                    Meter(
                        rateMetricName = metrics.metricInstance(
                            template = metricsRegistry.topicBytesConsumedRate,
                            tags = metricTags,
                        ),
                        totalMetricName = metrics.metricInstance(
                            template = metricsRegistry.topicBytesConsumedTotal,
                            tags = metricTags,
                        )
                    )
                )
            }
            bytesFetched.record(bytes.toDouble())

            // record records fetched
            val recordsFetchedName = "topic.$topic.records-fetched"
            var recordsFetched = metrics.getSensor(recordsFetchedName)
            if (recordsFetched == null) {
                val metricTags: MutableMap<String, String> = HashMap(1)
                metricTags["topic"] = topic.replace('.', '_')

                recordsFetched = metrics.sensor(recordsFetchedName)
                recordsFetched.add(
                    metricName = metrics.metricInstance(
                        template = metricsRegistry.topicRecordsPerRequestAvg,
                        tags = metricTags
                    ),
                    stat = Avg()
                )
                recordsFetched.add(
                    Meter(
                        rateMetricName = metrics.metricInstance(
                            template = metricsRegistry.topicRecordsConsumedRate,
                            tags = metricTags
                        ),
                        totalMetricName = metrics.metricInstance(
                            template = metricsRegistry.topicRecordsConsumedTotal,
                            tags = metricTags
                        )
                    )
                )
            }
            recordsFetched.record(records.toDouble())
        }

        fun maybeUpdateAssignment(subscription: SubscriptionState) {
            val newAssignmentId = subscription.assignmentId()
            if (assignmentId != newAssignmentId) {
                val newAssignedPartitions = subscription.assignedPartitions()
                for (tp: TopicPartition in assignedPartitions) {
                    if (!newAssignedPartitions.contains(tp)) {
                        metrics.removeSensor(partitionLagMetricName(tp))
                        metrics.removeSensor(partitionLeadMetricName(tp))
                        metrics.removeMetric(partitionPreferredReadReplicaMetricName(tp))
                    }
                }
                for (tp: TopicPartition in newAssignedPartitions) {
                    if (!assignedPartitions.contains(tp)) {
                        val metricName = partitionPreferredReadReplicaMetricName(tp)
                        metrics.addMetricIfAbsent(
                            metricName = metricName,
                            metricValueProvider = Gauge { _, _ ->
                                subscription.preferredReadReplica(tp = tp, timeMs = 0L) ?: -1
                            }
                        )
                    }
                }
                assignedPartitions = newAssignedPartitions
                assignmentId = newAssignmentId
            }
        }

        fun recordPartitionLead(tp: TopicPartition, lead: Long) {
            recordsFetchLead.record(lead.toDouble())
            val name = partitionLeadMetricName(tp)

            var recordsLead = metrics.getSensor(name)
            if (recordsLead == null) {
                val metricTags = topicPartitionTags(tp)
                recordsLead = metrics.sensor(name)
                recordsLead.add(
                    metricName = metrics.metricInstance(
                        template = metricsRegistry.partitionRecordsLead,
                        tags = metricTags,
                    ),
                    stat = Value(),
                )
                recordsLead.add(
                    metricName = metrics.metricInstance(
                        template = metricsRegistry.partitionRecordsLeadMin,
                        tags = metricTags,
                    ),
                    stat = Min(),
                )
                recordsLead.add(
                    metricName = metrics.metricInstance(
                        template = metricsRegistry.partitionRecordsLeadAvg,
                        tags = metricTags,
                    ),
                    stat = Avg(),
                )
            }
            recordsLead.record(lead.toDouble())
        }

        fun recordPartitionLag(tp: TopicPartition, lag: Long) {
            recordsFetchLag.record(lag.toDouble())
            val name = partitionLagMetricName(tp)
            var recordsLag = metrics.getSensor(name)
            if (recordsLag == null) {
                val metricTags = topicPartitionTags(tp)
                recordsLag = metrics.sensor(name)
                recordsLag.add(
                    metricName = metrics.metricInstance(
                        template = metricsRegistry.partitionRecordsLag,
                        tags = metricTags,
                    ),
                    stat = Value(),
                )
                recordsLag.add(
                    metricName = metrics.metricInstance(
                        template = metricsRegistry.partitionRecordsLagMax,
                        tags = metricTags,
                    ),
                    stat = Max(),
                )
                recordsLag.add(
                    metricName = metrics.metricInstance(
                        template = metricsRegistry.partitionRecordsLagAvg,
                        tags = metricTags,
                    ),
                    stat = Avg(),
                )
            }
            recordsLag.record(lag.toDouble())
        }

        private fun partitionPreferredReadReplicaMetricName(tp: TopicPartition): MetricName {
            val metricTags = topicPartitionTags(tp)
            return metrics.metricInstance(metricsRegistry.partitionPreferredReadReplica, metricTags)
        }

        private fun topicPartitionTags(tp: TopicPartition): Map<String, String> {
            val metricTags: MutableMap<String, String> = HashMap(2)
            metricTags["topic"] = tp.topic.replace('.', '_')
            metricTags["partition"] = tp.partition.toString()
            return metricTags
        }

        companion object {

            private fun partitionLagMetricName(tp: TopicPartition): String = "$tp.records-lag"

            private fun partitionLeadMetricName(tp: TopicPartition): String = "$tp.records-lead"
        }
    }

    override fun close() {
        nextInLineFetch?.drain()
        decompressionBufferSupplier.close()
    }

    private fun topicsForPartitions(partitions: Collection<TopicPartition>): Set<String> =
        partitions.map { it.topic }.toSet()

    private fun requestMetadataUpdate(topicPartition: TopicPartition) {
        metadata.requestUpdate()
        subscriptions.clearPreferredReadReplica(topicPartition)
    }

    /**
     * Represents data about an offset returned by a broker.
     */
    data class ListOffsetData(
        val offset: Long?, // null if the broker does not support returning timestamps
        val timestamp: Long?, // empty if the leader epoch is not known
        val leaderEpoch: Int?,
    )

    internal data class ListOffsetResult(
        val fetchedOffsets: MutableMap<TopicPartition, ListOffsetData> = mutableMapOf(),
        val partitionsToRetry: Set<TopicPartition> = mutableSetOf(),
    )

    companion object {
        fun hasUsableOffsetForLeaderEpochVersion(nodeApiVersions: NodeApiVersions): Boolean {
            val apiVersion =
                nodeApiVersions.apiVersion(ApiKeys.OFFSET_FOR_LEADER_EPOCH) ?: return false
            return OffsetsForLeaderEpochRequest.supportsTopicPermission(apiVersion.maxVersion)
        }

        fun throttleTimeSensor(metrics: Metrics, metricsRegistry: FetcherMetricsRegistry): Sensor {
            return metrics.sensor("fetch-throttle-time").apply {
                add(metrics.metricInstance(metricsRegistry.fetchThrottleTimeAvg), Avg())
                add(metrics.metricInstance(metricsRegistry.fetchThrottleTimeMax), Max())
            }
        }
    }
}
