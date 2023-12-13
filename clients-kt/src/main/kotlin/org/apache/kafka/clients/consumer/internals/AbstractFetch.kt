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

import java.io.Closeable
import java.util.ArrayDeque
import java.util.Queue
import java.util.concurrent.ConcurrentLinkedQueue
import org.apache.kafka.clients.ClientResponse
import org.apache.kafka.clients.FetchSessionHandler
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.OffsetOutOfRangeException
import org.apache.kafka.clients.consumer.internals.SubscriptionState.FetchPosition
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.Node
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.Uuid
import org.apache.kafka.common.errors.RecordTooLargeException
import org.apache.kafka.common.errors.TopicAuthorizationException
import org.apache.kafka.common.message.FetchResponseData
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.record.RecordBatch
import org.apache.kafka.common.requests.FetchRequest
import org.apache.kafka.common.requests.FetchResponse
import org.apache.kafka.common.utils.BufferSupplier
import org.apache.kafka.common.utils.LogContext
import org.apache.kafka.common.utils.Time
import org.apache.kafka.common.utils.Timer
import org.apache.kafka.common.utils.Utils.closeQuietly
import org.slf4j.Logger
import org.slf4j.helpers.MessageFormatter

/**
 * `AbstractFetch` represents the basic state and logic for record fetching processing.
 *
 * @param K Type for the message key
 * @param V Type for the message value
 */
abstract class AbstractFetch<K, V>(
    protected val logContext: LogContext,
    protected val client: ConsumerNetworkClient,
    protected val metadata: ConsumerMetadata,
    protected val subscriptions: SubscriptionState,
    protected val fetchConfig: FetchConfig<K, V>,
    protected val metricsManager: FetchMetricsManager,
    protected val time: Time,
) : Closeable {

    private val log: Logger = logContext.logger(javaClass)

    private val decompressionBufferSupplier: BufferSupplier = BufferSupplier.create()

    private val completedFetches: ConcurrentLinkedQueue<CompletedFetch<K, V>> =
        ConcurrentLinkedQueue<CompletedFetch<K, V>>()

    private val sessionHandlers: MutableMap<Int, FetchSessionHandler> = mutableMapOf()

    private val nodesWithPendingFetchRequests: MutableSet<Int> = mutableSetOf()

    private var nextInLineFetch: CompletedFetch<K, V>? = null

    /**
     * Return whether we have any completed fetches pending return to the user. This method is thread-safe. Has
     * visibility for testing.
     *
     * @return true if there are completed fetches, false otherwise
     */
    fun hasCompletedFetches(): Boolean = !completedFetches.isEmpty()

    /**
     * Return whether we have any completed fetches that are fetchable. This method is thread-safe.
     * @return true if there are completed fetches that can be returned, false otherwise
     */
    fun hasAvailableFetches(): Boolean =
        completedFetches.any { fetch -> subscriptions.isFetchable(fetch.partition) }

    /**
     * Implements the core logic for a successful fetch request/response.
     *
     * @param fetchTarget [Node] from which the fetch data was requested
     * @param data [FetchSessionHandler.FetchRequestData] that represents the session data
     * @param resp [ClientResponse] from which the [FetchResponse] will be retrieved
     */
    protected fun handleFetchResponse(
        fetchTarget: Node,
        data: FetchSessionHandler.FetchRequestData,
        resp: ClientResponse,
    ) {
        try {
            val response = resp.responseBody as FetchResponse
            val handler = sessionHandler(fetchTarget.id)

            if (handler == null) {
                log.error("Unable to find FetchSessionHandler for node {}. Ignoring fetch response.", fetchTarget.id)
                return
            }

            val requestVersion = resp.requestHeader.apiVersion

            if (!handler.handleResponse(response, requestVersion)) {
                if (response.error() == Errors.FETCH_SESSION_TOPIC_ID_ERROR) metadata.requestUpdate()
                return
            }

            val responseData = response.responseData(handler.sessionTopicNames(), requestVersion)
            val partitions = responseData.keys.toSet()
            val metricAggregator = FetchMetricsAggregator(metricsManager, partitions)

            for ((partition, partitionData) in responseData) {
                val requestData = data.sessionPartitions[partition]

                if (requestData == null) {
                    val message = if (data.metadata.isFull) MessageFormatter.arrayFormat(
                        "Response for missing full request partition: partition={}; metadata={}",
                        arrayOf(partition, data.metadata)
                    ).message
                    else MessageFormatter.arrayFormat(
                        "Response for missing session request partition: partition={}; " +
                                "metadata={}; toSend={}; toForget={}; toReplace={}",
                        arrayOf(partition, data.metadata, data.toSend, data.toForget, data.toReplace)
                    ).message

                    // Received fetch response for missing session partition
                    throw IllegalStateException(message)
                }

                val fetchOffset = requestData.fetchOffset

                log.debug(
                    "Fetch {} at offset {} for partition {} returned fetch data {}",
                    fetchConfig.isolationLevel, fetchOffset, partition, partitionData
                )

                val completedFetch = CompletedFetch(
                    logContext = logContext,
                    subscriptions = subscriptions,
                    fetchConfig = fetchConfig,
                    decompressionBufferSupplier = decompressionBufferSupplier,
                    partition = partition,
                    partitionData = partitionData,
                    metricAggregator = metricAggregator,
                    fetchOffset = fetchOffset,
                    requestVersion = requestVersion,
                )
                completedFetches.add(completedFetch)
            }

            metricsManager.recordLatency(resp.requestLatencyMs)
        } finally {
            log.debug("Removing pending request for node {}", fetchTarget)
            nodesWithPendingFetchRequests.remove(fetchTarget.id)
        }
    }

    /**
     * Implements the core logic for a failed fetch request/response.
     *
     * @param fetchTarget [Node] from which the fetch data was requested
     * @param e [RuntimeException] representing the error that resulted in the failure
     */
    protected fun handleFetchResponse(fetchTarget: Node, e: RuntimeException?) {
        try {
            sessionHandler(fetchTarget.id)?.let { handler  ->
                handler.handleError(e)
                handler.sessionTopicPartitions().forEach { tp -> subscriptions.clearPreferredReadReplica(tp) }
            }
        } finally {
            log.debug("Removing pending request for node {}", fetchTarget)
            nodesWithPendingFetchRequests.remove(fetchTarget.id)
        }
    }

    /**
     * Creates a new [fetch request][FetchRequest] in preparation for sending to the Kafka cluster.
     *
     * @param fetchTarget [Node] from which the fetch data will be requested
     * @param requestData [FetchSessionHandler.FetchRequestData] that represents the session data
     * @return [FetchRequest.Builder] that can be submitted to the broker
     */
    protected fun createFetchRequest(
        fetchTarget: Node,
        requestData: FetchSessionHandler.FetchRequestData,
    ): FetchRequest.Builder {
        // Version 12 is the maximum version that could be used without topic IDs. See FetchRequest.json for schema
        // changelog.
        val maxVersion =
            if (requestData.canUseTopicIds) ApiKeys.FETCH.latestVersion()
            else 12.toShort()

        val request = FetchRequest.Builder
            .forConsumer(
                maxVersion = maxVersion,
                maxWait = fetchConfig.maxWaitMs,
                minBytes = fetchConfig.minBytes,
                fetchData = requestData.toSend,
            )
            .isolationLevel(fetchConfig.isolationLevel)
            .setMaxBytes(fetchConfig.maxBytes)
            .metadata(requestData.metadata)
            .removed(requestData.toForget)
            .replaced(requestData.toReplace)
            .rackId(fetchConfig.clientRackId)

        log.debug("Sending {} {} to broker {}", fetchConfig.isolationLevel, requestData, fetchTarget)

        // We add the node to the set of nodes with pending fetch requests before adding the
        // listener because the future may have been fulfilled on another thread (e.g. during a
        // disconnection being handled by the heartbeat thread) which will mean the listener
        // will be invoked synchronously.
        log.debug("Adding pending request for node {}", fetchTarget)
        nodesWithPendingFetchRequests.add(fetchTarget.id)

        return request
    }

    /**
     * Return the fetched records, empty the record buffer and update the consumed position.
     *
     * NOTE: returning an [empty][Fetch.isEmpty] fetch guarantees the consumed position is not updated.
     *
     * @return A [Fetch] for the requested partitions
     * @throws OffsetOutOfRangeException If there is OffsetOutOfRange error in fetchResponse and
     * the defaultResetPolicy is NONE
     * @throws TopicAuthorizationException If there is TopicAuthorization error in fetchResponse.
     */
    fun collectFetch(): Fetch<K, V> {
        val fetch: Fetch<K, V> = Fetch.empty()
        val pausedCompletedFetches: Queue<CompletedFetch<K, V>> = ArrayDeque()
        var recordsRemaining = fetchConfig.maxPollRecords
        try {
            while (recordsRemaining > 0) {
                if (nextInLineFetch == null || nextInLineFetch!!.isConsumed) {
                    val records = completedFetches.peek() ?: break
                    if (!records.initialized) {
                        try {
                            nextInLineFetch = initializeCompletedFetch(records)
                        } catch (e: Exception) {
                            // Remove a completedFetch upon a parse with exception if
                            // (1) it contains no records, and
                            // (2) there are no fetched records with actual content preceding this exception.
                            // The first condition ensures that the completedFetches is not stuck with the
                            // same completedFetch in cases such as the TopicAuthorizationException,
                            // and the second condition ensures that no potential data loss due to an exception
                            // in a following record.
                            if (
                                fetch.isEmpty
                                && FetchResponse.recordsOrFail(records.partitionData).sizeInBytes() == 0
                            ) completedFetches.poll()

                            throw e
                        }
                    } else nextInLineFetch = records
                    completedFetches.poll()
                } else if (subscriptions.isPaused(nextInLineFetch!!.partition)) {
                    // when the partition is paused we add the records back to the completedFetches queue
                    // instead of draining them so that they can be returned on a subsequent poll if the partition
                    // is resumed at that time
                    log.debug(
                        "Skipping fetching records for assigned partition {} because it is paused",
                        nextInLineFetch!!.partition
                    )
                    pausedCompletedFetches.add(nextInLineFetch)
                    nextInLineFetch = null
                } else {
                    val nextFetch = fetchRecords(recordsRemaining)
                    recordsRemaining -= nextFetch.numRecords
                    fetch.add(nextFetch)
                }
            }
        } catch (e: KafkaException) {
            if (fetch.isEmpty) throw e
        } finally {
            // add any polled completed fetches for paused partitions back to the completed fetches queue to be
            // re-evaluated in the next poll
            completedFetches.addAll(pausedCompletedFetches)
        }
        return fetch
    }

    private fun fetchRecords(maxRecords: Int): Fetch<K, V> {
        if (!subscriptions.isAssigned(nextInLineFetch!!.partition)) {
            // this can happen when a rebalance happened before fetched records are returned to the consumer's poll call
            log.debug(
                "Not returning fetched records for partition {} since it is no longer assigned",
                nextInLineFetch!!.partition
            )
        } else if (!subscriptions.isFetchable(nextInLineFetch!!.partition)) {
            // this can happen when a partition is paused before fetched records are returned to the consumer's
            // poll call or if the offset is being reset
            log.debug(
                "Not returning fetched records for assigned partition {} since it is no longer fetchable",
                nextInLineFetch!!.partition
            )
        } else {
            val position = checkNotNull(subscriptions.position(nextInLineFetch!!.partition)) {
                "Missing position for fetchable partition " + nextInLineFetch!!.partition
            }
            if (nextInLineFetch!!.nextFetchOffset == position.offset) {
                val partRecords = nextInLineFetch!!.fetchRecords(maxRecords)
                log.trace(
                    "Returning {} fetched records at offset {} for assigned partition {}",
                    partRecords.size, position, nextInLineFetch!!.partition
                )
                var positionAdvanced = false
                if (nextInLineFetch!!.nextFetchOffset > position.offset) {
                    val nextPosition = FetchPosition(
                        offset = nextInLineFetch!!.nextFetchOffset,
                        offsetEpoch = nextInLineFetch!!.lastEpoch,
                        currentLeader = position.currentLeader,
                    )
                    log.trace(
                        "Updating fetch position from {} to {} for partition {} and returning {} records from `poll()`",
                        position, nextPosition, nextInLineFetch!!.partition, partRecords.size
                    )
                    subscriptions.position(nextInLineFetch!!.partition, nextPosition)
                    positionAdvanced = true
                }
                val partitionLag = subscriptions.partitionLag(nextInLineFetch!!.partition, fetchConfig.isolationLevel)
                if (partitionLag != null) metricsManager.recordPartitionLag(nextInLineFetch!!.partition, partitionLag)
                val lead = subscriptions.partitionLead(nextInLineFetch!!.partition)
                if (lead != null) metricsManager.recordPartitionLead(nextInLineFetch!!.partition, lead)

                return Fetch.forPartition(nextInLineFetch!!.partition, partRecords, positionAdvanced)
            } else {
                // these records aren't next in line based on the last consumed position, ignore them
                // they must be from an obsolete request
                log.debug(
                    "Ignoring fetched records for {} at offset {} since the current position is {}",
                    nextInLineFetch!!.partition, nextInLineFetch!!.nextFetchOffset, position
                )
            }
        }
        log.trace("Draining fetched records for partition {}", nextInLineFetch!!.partition)
        nextInLineFetch!!.drain()

        return Fetch.empty()
    }

    private fun fetchablePartitions(): List<TopicPartition> {
        val exclude = mutableSetOf<TopicPartition>()

        nextInLineFetch?.let {
            if (!it.isConsumed) exclude.add(it.partition)
        }

        for (completedFetch in completedFetches)
            exclude.add(completedFetch.partition)

        return subscriptions.fetchablePartitions { tp -> !exclude.contains(tp) }
    }

    /**
     * Determine from which replica to read: the *preferred* or the *leader*. The preferred replica is used iff:
     *
     * - A preferred replica was previously set
     * - We're still within the lease time for the preferred replica
     * - The replica is still online/available
     *
     * If any of the above are not met, the leader node is returned.
     *
     * @param partition [TopicPartition] for which we want to fetch data
     * @param leaderReplica [Node] for the leader of the given partition
     * @param currentTimeMs Current time in milliseconds; used to determine if we're within the optional lease window
     * @return Replic [node][Node] from which to request the data
     * @see SubscriptionState.preferredReadReplica
     *
     * @see SubscriptionState.updatePreferredReadReplica
     */
    fun selectReadReplica(partition: TopicPartition, leaderReplica: Node, currentTimeMs: Long): Node {
        val nodeId = subscriptions.preferredReadReplica(partition, currentTimeMs)
        return if (nodeId != null) {
            val node = metadata.fetch().nodeIfOnline(partition, nodeId)
            node ?: run {
                log.trace(
                    "Not fetching from {} for partition {} since it is marked offline or is missing from " +
                            "our metadata, using the leader instead.",
                    nodeId, partition
                )
                // Note that this condition may happen due to stale metadata, so we clear preferred replica and
                // refresh metadata.
                requestMetadataUpdate(partition)
                leaderReplica
            }
        } else leaderReplica
    }

    /**
     * Create fetch requests for all nodes for which we have assigned partitions that
     * have no existing requests in flight.
     */
    protected fun prepareFetchRequests(): Map<Node, FetchSessionHandler.FetchRequestData> {
        // Update metrics in case there was an assignment change
        metricsManager.maybeUpdateAssignment(subscriptions)

        val fetchable = mutableMapOf<Node, FetchSessionHandler.Builder>()
        val currentTimeMs = time.milliseconds()
        val topicIds = metadata.topicIds()

        for (partition in fetchablePartitions()) {

            val position = checkNotNull(subscriptions.position(partition)) {
                "Missing position for fetchable partition $partition"
            }

            val leaderOpt = position.currentLeader.leader
            if (leaderOpt == null) {
                log.debug(
                    "Requesting metadata update for partition {} since the position {} " +
                            "is missing the current leader node",
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
                // going to be failed anyway before being sent, so skip sending the request for now
                log.trace(
                    "Skipping fetch for partition {} because node {} is awaiting reconnect backoff",
                    partition,
                    node
                )
            } else if (nodesWithPendingFetchRequests.contains(node.id)) {
                log.trace(
                    "Skipping fetch for partition {} because previous request to {} has not been processed",
                    partition,
                    node
                )
            } else {
                // if there is a leader and no in-flight requests, issue a new fetch
                val builder = fetchable.computeIfAbsent(node) {
                    val fetchSessionHandler =
                        sessionHandlers.computeIfAbsent(node.id) { n -> FetchSessionHandler(logContext, n) }
                    fetchSessionHandler.newBuilder()
                }
                val topicId = topicIds.getOrDefault(partition.topic, Uuid.ZERO_UUID)
                val partitionData = FetchRequest.PartitionData(
                    topicId = topicId,
                    fetchOffset = position.offset,
                    logStartOffset = FetchRequest.INVALID_LOG_START_OFFSET,
                    maxBytes = fetchConfig.fetchSize,
                    currentLeaderEpoch = position.currentLeader.epoch,
                    lastFetchedEpoch = null,
                )
                builder.add(partition, partitionData)
                log.debug(
                    "Added {} fetch request for partition {} at position {} to node {}",
                    fetchConfig.isolationLevel, partition, position, node
                )
            }
        }

        return fetchable.mapValues { (_, value) -> value.build() }
    }

    /**
     * Initialize a CompletedFetch object.
     */
    private fun initializeCompletedFetch(completedFetch: CompletedFetch<K, V>): CompletedFetch<K, V>? {
        val tp: TopicPartition = completedFetch.partition
        val error = Errors.forCode(completedFetch.partitionData.errorCode)
        var recordMetrics = true
        try {
            return if (!subscriptions.hasValidPosition(tp)) {
                // this can happen when a rebalance happened while fetch is still in-flight
                log.debug("Ignoring fetched records for partition {} since it no longer has valid position", tp)
                null
            } else if (error == Errors.NONE) {
                val ret = handleInitializeCompletedFetchSuccess(completedFetch)
                recordMetrics = ret == null
                ret
            } else {
                handleInitializeCompletedFetchErrors(completedFetch, error)
                null
            }
        } finally {
            if (recordMetrics) completedFetch.recordAggregatedMetrics(bytes = 0, records = 0)
            if (error != Errors.NONE)
            // we move the partition to the end if there was an error. This way, it's more likely that
            // partitions for the same topic can remain together (allowing for more efficient serialization).
                subscriptions.movePartitionToEnd(tp)
        }
    }

    private fun handleInitializeCompletedFetchSuccess(completedFetch: CompletedFetch<K, V>): CompletedFetch<K, V>? {
        val tp = completedFetch.partition
        val fetchOffset = completedFetch.nextFetchOffset

        // we are interested in this fetch only if the beginning offset matches the
        // current consumed position
        val position = subscriptions.position(tp)
        if (position == null || position.offset != fetchOffset) {
            log.debug(
                "Discarding stale fetch response for partition {} since its offset {} does not match " +
                        "the expected offset {}",
                tp, fetchOffset, position
            )
            return null
        }
        val partition = completedFetch.partitionData
        log.trace(
            "Preparing to read {} bytes of data for partition {} with offset {}",
            FetchResponse.recordsSize(partition), tp, position
        )
        val batches = FetchResponse.recordsOrFail(partition).batches().iterator()
        if (!batches.hasNext() && FetchResponse.recordsSize(partition) > 0) {
            if (completedFetch.requestVersion < 3) {
                // Implement the pre KIP-74 behavior of throwing a RecordTooLargeException.
                val recordTooLargePartitions = mapOf(tp to fetchOffset)
                throw RecordTooLargeException(
                    "There are some messages at [Partition=Offset]: $recordTooLargePartitions whose size is larger " +
                            "than the fetch size ${fetchConfig.fetchSize} and hence cannot be returned. " +
                            "Please considering upgrading your broker to 0.10.1.0 or newer to avoid this issue. " +
                            "Alternately, increase the fetch size on the client " +
                            "(using ${ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG})",
                    recordTooLargePartitions
                )
            } else {
                // This should not happen with brokers that support FetchRequest/Response V3 or higher (i.e. KIP-74)
                throw KafkaException(
                    "Failed to make progress reading messages at $tp=$fetchOffset. " +
                            "Received a non-empty fetch response from the server, but no complete records were found."
                )
            }
        }
        if (partition.highWatermark >= 0) {
            log.trace("Updating high watermark for partition {} to {}", tp, partition.highWatermark)
            subscriptions.updateHighWatermark(tp, partition.highWatermark)
        }
        if (partition.logStartOffset >= 0) {
            log.trace("Updating log start offset for partition {} to {}", tp, partition.logStartOffset)
            subscriptions.updateLogStartOffset(tp, partition.logStartOffset)
        }
        if (partition.lastStableOffset >= 0) {
            log.trace("Updating last stable offset for partition {} to {}", tp, partition.lastStableOffset)
            subscriptions.updateLastStableOffset(tp, partition.lastStableOffset)
        }
        if (FetchResponse.isPreferredReplica(partition)) {
            subscriptions.updatePreferredReadReplica(
                tp = completedFetch.partition,
                preferredReadReplicaId = partition.preferredReadReplica,
                timeMs = {
                    val expireTimeMs = time.milliseconds() + metadata.metadataExpireMs()
                    log.debug(
                        "Updating preferred read replica for partition {} to {}, set to expire at {}",
                        tp, partition.preferredReadReplica, expireTimeMs
                    )
                    expireTimeMs
                }
            )
        }
        completedFetch.initialized = true
        return completedFetch
    }

    private fun handleInitializeCompletedFetchErrors(
        completedFetch: CompletedFetch<K, V>,
        error: Errors,
    ) {
        val tp: TopicPartition = completedFetch.partition
        val fetchOffset: Long = completedFetch.nextFetchOffset
        if (
            error == Errors.NOT_LEADER_OR_FOLLOWER
            || error == Errors.REPLICA_NOT_AVAILABLE
            || error == Errors.KAFKA_STORAGE_ERROR
            || error == Errors.FENCED_LEADER_EPOCH
            || error == Errors.OFFSET_NOT_AVAILABLE
        ) {
            log.debug("Error in fetch for partition {}: {}", tp, error.exceptionName)
            requestMetadataUpdate(tp)
        } else if (error == Errors.UNKNOWN_TOPIC_OR_PARTITION) {
            log.warn("Received unknown topic or partition error in fetch for partition {}", tp)
            requestMetadataUpdate(tp)
        } else if (error == Errors.UNKNOWN_TOPIC_ID) {
            log.warn("Received unknown topic ID error in fetch for partition {}", tp)
            requestMetadataUpdate(tp)
        } else if (error == Errors.INCONSISTENT_TOPIC_ID) {
            log.warn("Received inconsistent topic ID error in fetch for partition {}", tp)
            requestMetadataUpdate(tp)
        } else if (error == Errors.OFFSET_OUT_OF_RANGE) {
            val clearedReplicaId = subscriptions.clearPreferredReadReplica(tp)
            if (clearedReplicaId == null) {
                // If there's no preferred replica to clear, we're fetching from the leader so handle
                // this error normally
                val position = subscriptions.position(tp)
                if (position == null || fetchOffset != position.offset) {
                    log.debug(
                        "Discarding stale fetch response for partition {} since the fetched offset {} " +
                                "does not match the current offset {}", tp, fetchOffset, position
                    )
                } else handleOffsetOutOfRange(position, tp)
            } else {
                log.debug(
                    "Unset the preferred read replica {} for partition {} since we got {} when fetching {}",
                    clearedReplicaId, tp, error, fetchOffset
                )
            }
        } else if (error == Errors.TOPIC_AUTHORIZATION_FAILED) {
            //we log the actual partition and not just the topic to help with ACL propagation issues in large clusters
            log.warn("Not authorized to read from partition {}.", tp)
            throw TopicAuthorizationException(setOf(tp.topic))
        } else if (error == Errors.UNKNOWN_LEADER_EPOCH)
            log.debug("Received unknown leader epoch error in fetch for partition {}", tp)
        else if (error == Errors.UNKNOWN_SERVER_ERROR)
            log.warn("Unknown server error while fetching offset {} for topic-partition {}", fetchOffset, tp)
        else if (error == Errors.CORRUPT_MESSAGE) throw KafkaException(
            "Encountered corrupt message when fetching offset $fetchOffset for topic-partition $tp"
        )
        else error("Unexpected error code ${error.code} while fetching at offset $fetchOffset from topic-partition $tp")
    }

    private fun handleOffsetOutOfRange(
        fetchPosition: FetchPosition,
        topicPartition: TopicPartition,
    ) {
        val errorMessage = "Fetch position $fetchPosition is out of range for partition $topicPartition"
        if (subscriptions.hasDefaultOffsetResetPolicy()) {
            log.info("{}, resetting offset", errorMessage)
            subscriptions.requestOffsetReset(topicPartition)
        } else {
            log.info("{}, raising error to the application since no reset policy is configured", errorMessage)
            throw OffsetOutOfRangeException(
                message = errorMessage,
                offsetOutOfRangePartitions = mapOf(topicPartition to fetchPosition.offset),
            )
        }
    }

    /**
     * Clear the buffered data which are not a part of newly assigned partitions. Any previously
     * [fetched data][CompletedFetch] is dropped if it is for a partition that is no longer in
     * `assignedPartitions`.
     *
     * @param assignedPartitions Newly-assigned [TopicPartition]
     */
    fun clearBufferedDataForUnassignedPartitions(assignedPartitions: Collection<TopicPartition?>) {
        val completedFetchesItr = completedFetches.iterator()
        while (completedFetchesItr.hasNext()) {
            val completedFetch = completedFetchesItr.next()
            val tp = completedFetch.partition
            if (!assignedPartitions.contains(tp)) {
                log.debug("Removing {} from buffered data as it is no longer an assigned partition", tp)
                completedFetch.drain()
                completedFetchesItr.remove()
            }
        }

        nextInLineFetch?.let {
            if (!assignedPartitions.contains(it.partition)) {
                it.drain()
                nextInLineFetch = null
            }
        }
    }

    /**
     * Clear the buffered data which are not a part of newly assigned topics
     *
     * @param assignedTopics  newly assigned topics
     */
    fun clearBufferedDataForUnassignedTopics(assignedTopics: Collection<String>) {
        val currentTopicPartitions = mutableSetOf<TopicPartition>()
        for (tp in subscriptions.assignedPartitions()) {
            if (assignedTopics.contains(tp.topic)) currentTopicPartitions.add(tp)
        }
        clearBufferedDataForUnassignedPartitions(currentTopicPartitions)
    }

    protected open fun sessionHandler(node: Int): FetchSessionHandler? = sessionHandlers[node]

    // Visible for testing
    internal fun maybeCloseFetchSessions(timer: Timer) {
        val cluster = metadata.fetch()
        val requestFutures = mutableListOf<RequestFuture<ClientResponse>>()
        sessionHandlers.forEach { (fetchTargetNodeId, sessionHandler) ->
            // set the session handler to notify close. This will set the next metadata request to send close message.
            sessionHandler.notifyClose()

            val sessionId = sessionHandler.sessionId
            // FetchTargetNode may not be available as it may have disconnected the connection. In such cases, we will
            // skip sending the close request.
            val fetchTarget = cluster.nodeById(fetchTargetNodeId)
            if (fetchTarget == null || client.isUnavailable(fetchTarget)) {
                log.debug("Skip sending close session request to broker {} since it is not reachable", fetchTarget)
                return@forEach
            }

            val request = createFetchRequest(fetchTarget, sessionHandler.newBuilder().build())
            val responseFuture = client.send(fetchTarget, request)

            responseFuture.addListener(object : RequestFutureListener<ClientResponse> {
                override fun onSuccess(result: ClientResponse) {
                    log.debug(
                        "Successfully sent a close message for fetch session: {} to node: {}",
                        sessionId,
                        fetchTarget
                    )
                }

                override fun onFailure(exception: RuntimeException) {
                    log.debug(
                        "Unable to a close message for fetch session: {} to node: {}. " +
                                "This may result in unnecessary fetch sessions at the broker.",
                        sessionId,
                        fetchTarget,
                        exception
                    )
                }
            })

            requestFutures.add(responseFuture)
        }

        // Poll to ensure that request has been written to the socket. Wait until either the timer has expired or until
        // all requests have received a response.
        while (timer.isNotExpired && !requestFutures.all { it.isDone() })
            client.poll(timer, null, true)

        if (!requestFutures.all { it.isDone() }) {
            // we ran out of time before completing all futures. It is ok since we don't want to block
            // the shutdown here.
            log.debug(
                "All requests couldn't be sent in the specific timeout period {}ms. This may result in " +
                        "unnecessary fetch sessions at the broker. Consider increasing the timeout passed " +
                        "for KafkaConsumer.close(Duration timeout)",
                timer.timeoutMs
            )
        }
    }

    open fun close(timer: Timer) {
        // we do not need to re-enable wakeups since we are closing already
        client.disableWakeups()

        nextInLineFetch?.let {
            it.drain()
            nextInLineFetch = null
        }

        maybeCloseFetchSessions(timer)
        closeQuietly(decompressionBufferSupplier, "decompressionBufferSupplier")
        sessionHandlers.clear()
    }

    override fun close() = close(time.timer(timeoutMs = 0))

    private fun requestMetadataUpdate(topicPartition: TopicPartition) {
        metadata.requestUpdate()
        subscriptions.clearPreferredReadReplica(topicPartition)
    }
}
