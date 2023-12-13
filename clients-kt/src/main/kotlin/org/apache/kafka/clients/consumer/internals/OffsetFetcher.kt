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

import java.util.concurrent.atomic.AtomicInteger
import org.apache.kafka.clients.ApiVersions
import org.apache.kafka.clients.ClientResponse
import org.apache.kafka.clients.StaleMetadataException
import org.apache.kafka.clients.consumer.LogTruncationException
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.clients.consumer.OffsetAndTimestamp
import org.apache.kafka.clients.consumer.internals.OffsetFetcherUtils.Companion.buildOffsetsForTimesResult
import org.apache.kafka.clients.consumer.internals.OffsetFetcherUtils.Companion.hasUsableOffsetForLeaderEpochVersion
import org.apache.kafka.clients.consumer.internals.OffsetFetcherUtils.Companion.regroupFetchPositionsByLeader
import org.apache.kafka.clients.consumer.internals.OffsetFetcherUtils.Companion.topicsForPartitions
import org.apache.kafka.clients.consumer.internals.OffsetFetcherUtils.ListOffsetData
import org.apache.kafka.clients.consumer.internals.OffsetFetcherUtils.ListOffsetResult
import org.apache.kafka.clients.consumer.internals.OffsetsForLeaderEpochClient.OffsetForEpochResult
import org.apache.kafka.clients.consumer.internals.SubscriptionState.FetchPosition
import org.apache.kafka.clients.consumer.internals.SubscriptionState.LogTruncation
import org.apache.kafka.common.IsolationLevel
import org.apache.kafka.common.Node
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.RetriableException
import org.apache.kafka.common.errors.TimeoutException
import org.apache.kafka.common.message.ListOffsetsRequestData.ListOffsetsPartition
import org.apache.kafka.common.requests.ListOffsetsRequest
import org.apache.kafka.common.requests.ListOffsetsResponse
import org.apache.kafka.common.utils.LogContext
import org.apache.kafka.common.utils.Time
import org.apache.kafka.common.utils.Timer
import org.slf4j.Logger

/**
 * [OffsetFetcher] is responsible for fetching the [offsets][OffsetAndTimestamp] for a given set of
 * [topic and partition pairs][TopicPartition] and for validation and resetting of positions, as needed.
 */
class OffsetFetcher(
    logContext: LogContext,
    private val client: ConsumerNetworkClient,
    private val metadata: ConsumerMetadata,
    private val subscriptions: SubscriptionState,
    private val time: Time,
    private val retryBackoffMs: Long,
    private val requestTimeoutMs: Long,
    private val isolationLevel: IsolationLevel,
    private val apiVersions: ApiVersions,
) {

    private val log: Logger = logContext.logger(javaClass)

    private val offsetsForLeaderEpochClient = OffsetsForLeaderEpochClient(client, logContext)

    private val offsetFetcherUtils = OffsetFetcherUtils(
        logContext = logContext,
        metadata = metadata,
        subscriptionState = subscriptions,
        time = time,
        retryBackoffMs = retryBackoffMs,
        apiVersions = apiVersions,
    )

    /**
     * Reset offsets for all assigned partitions that require it.
     *
     * @throws org.apache.kafka.clients.consumer.NoOffsetForPartitionException If no offset reset strategy is defined
     * and one or more partitions aren't awaiting a seekToBeginning() or seekToEnd().
     */
    fun resetPositionsIfNeeded() {
        val offsetResetTimestamps = offsetFetcherUtils.offsetResetTimestamp
        if (offsetResetTimestamps.isEmpty()) return
        resetPositionsAsync(offsetResetTimestamps)
    }

    /**
     * Validate offsets for all assigned partitions for which a leader change has been detected.
     */
    fun validatePositionsIfNeeded() {
        val partitionsToValidate = offsetFetcherUtils.partitionsToValidate
        validatePositionsAsync(partitionsToValidate)
    }

    fun offsetsForTimes(
        timestampsToSearch: Map<TopicPartition, Long>,
        timer: Timer,
    ): Map<TopicPartition, OffsetAndTimestamp?> {
        metadata.addTransientTopics(topicsForPartitions(timestampsToSearch.keys))
        return try {
            val fetchedOffsets = fetchOffsetsByTimes(
                timestampsToSearch = timestampsToSearch,
                timer = timer,
                requireTimestamps = true,
            ).fetchedOffsets
            buildOffsetsForTimesResult(timestampsToSearch, fetchedOffsets)
        } finally {
            metadata.clearTransientTopics()
        }
    }

    private fun fetchOffsetsByTimes(
        timestampsToSearch: Map<TopicPartition, Long>,
        timer: Timer,
        requireTimestamps: Boolean,
    ): ListOffsetResult {
        val actualResult = ListOffsetResult()
        if (timestampsToSearch.isEmpty()) return actualResult
        val remainingToSearch = HashMap(timestampsToSearch)
        do {
            val future = sendListOffsetsRequests(remainingToSearch, requireTimestamps)
            future.addListener(object : RequestFutureListener<ListOffsetResult> {
                override fun onSuccess(result: ListOffsetResult) {
                    synchronized(future) {
                        actualResult.fetchedOffsets.putAll(result.fetchedOffsets)
                        remainingToSearch.keys.retainAll(result.partitionsToRetry)
                        offsetFetcherUtils.updateSubscriptionState(result.fetchedOffsets, isolationLevel)
                    }
                }

                override fun onFailure(exception: RuntimeException) {
                    if (exception !is RetriableException) throw future.exception()
                }
            })

            // if timeout is set to zero, do not try to poll the network client at all
            // and return empty immediately; otherwise try to get the results synchronously
            // and throw timeout exception if it cannot complete in time
            if (timer.timeoutMs == 0L) return actualResult
            client.poll(future, timer)
            if (!future.isDone()) break
            else if (remainingToSearch.isEmpty()) return actualResult
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
        timer = timer
    )

    fun endOffsets(
        partitions: Collection<TopicPartition>,
        timer: Timer,
    ): Map<TopicPartition, Long> = beginningOrEndOffset(
        partitions = partitions,
        timestamp = ListOffsetsRequest.LATEST_TIMESTAMP,
        timer = timer
    )

    private fun beginningOrEndOffset(
        partitions: Collection<TopicPartition>,
        timestamp: Long,
        timer: Timer,
    ): Map<TopicPartition, Long> {
        metadata.addTransientTopics(topicsForPartitions(partitions))
        return try {
            val timestampsToSearch = partitions.distinct().associateWith { timestamp }
            val result = fetchOffsetsByTimes(
                timestampsToSearch = timestampsToSearch,
                timer = timer,
                requireTimestamps = false,
            )
            result.fetchedOffsets.mapValues { it.value.offset!! }
        } finally {
            metadata.clearTransientTopics()
        }
    }

    private fun resetPositionsAsync(partitionResetTimestamps: Map<TopicPartition, Long>) {
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
                override fun onSuccess(result: ListOffsetResult) {
                    offsetFetcherUtils.onSuccessfulRequestForResettingPositions(resetTimestamps, result)
                }

                override fun onFailure(exception: RuntimeException) {
                    offsetFetcherUtils.onFailedRequestForResettingPositions(resetTimestamps, exception)
                }
            })
        }
    }

    /**
     * For each partition which needs validation, make an asynchronous request to get the end-offsets
     * for the partition with the epoch less than or equal to the epoch the partition last saw.
     *
     * Requests are grouped by Node for efficiency.
     */
    private fun validatePositionsAsync(partitionsToValidate: Map<TopicPartition, FetchPosition>) {
        val regrouped = regroupFetchPositionsByLeader(partitionsToValidate)
        val nextResetTimeMs = time.milliseconds() + requestTimeoutMs
        regrouped.forEach { (node, fetchPositions) ->
            if (node.isEmpty) {
                metadata.requestUpdate()
                return@forEach
            }
            val nodeApiVersions = apiVersions[node.idString()]
            if (nodeApiVersions == null) {
                client.tryConnect(node)
                return@forEach
            }
            if (!hasUsableOffsetForLeaderEpochVersion(nodeApiVersions)) {
                log.debug(
                    "Skipping validation of fetch offsets for partitions {} since the broker does not " +
                            "support the required protocol version (introduced in Kafka 2.3)",
                    fetchPositions.keys
                )
                for (partition in fetchPositions.keys)
                    subscriptions.completeValidation(partition)

                return@forEach
            }
            subscriptions.setNextAllowedRetry(fetchPositions.keys, nextResetTimeMs)
            val future =
                offsetsForLeaderEpochClient.sendAsyncRequest(node, fetchPositions)
            future.addListener(object : RequestFutureListener<OffsetForEpochResult> {
                override fun onSuccess(result: OffsetForEpochResult) {
                    val truncations = mutableListOf<LogTruncation>()
                    if (result.partitionsToRetry.isNotEmpty()) {
                        subscriptions.setNextAllowedRetry(
                            partitions = result.partitionsToRetry,
                            nextAllowResetTimeMs = time.milliseconds() + retryBackoffMs,
                        )
                        metadata.requestUpdate()
                    }

                    // For each OffsetsForLeader response, check if the end-offset is lower than our current offset
                    // for the partition. If so, it means we have experienced log truncation and need to reposition
                    // that partition's offset.
                    //
                    // In addition, check whether the returned offset and epoch are valid. If not, then we should reset
                    // its offset if reset policy is configured, or throw out of range exception.
                    result.endOffsets.forEach { (topicPartition, respEndOffset) ->
                        val requestPosition = fetchPositions[topicPartition]!!

                        subscriptions.maybeCompleteValidation(
                            tp = topicPartition,
                            requestPosition = requestPosition,
                            epochEndOffset = respEndOffset
                        )?.let { truncation -> truncations.add(truncation) }
                    }
                    if (truncations.isNotEmpty()) offsetFetcherUtils.maybeSetOffsetForLeaderException(
                        buildLogTruncationException(truncations)
                    )
                }

                override fun onFailure(exception: RuntimeException) {
                    subscriptions.requestFailed(
                        partitions = fetchPositions.keys,
                        nextRetryTimeMs = time.milliseconds() + retryBackoffMs,
                    )
                    metadata.requestUpdate()
                    if (exception !is RetriableException)
                        offsetFetcherUtils.maybeSetOffsetForLeaderException(exception)
                }
            })
        }
    }

    private fun buildLogTruncationException(truncations: List<LogTruncation>): LogTruncationException {
        val divergentOffsets = mutableMapOf<TopicPartition, OffsetAndMetadata>()
        val truncatedFetchOffsets = mutableMapOf<TopicPartition, Long>()
        for (truncation in truncations) {
            truncation.divergentOffset?.let { divergentOffset ->
                divergentOffsets[truncation.topicPartition] = divergentOffset
            }
            truncatedFetchOffsets[truncation.topicPartition] = truncation.fetchPosition.offset
        }
        return LogTruncationException(
            message = "Detected truncated partitions: $truncations",
            fetchOffsets = truncatedFetchOffsets,
            divergentOffsets = divergentOffsets,
        )
    }

    /**
     * Search the offsets by target times for the specified partitions.
     *
     * @param timestampsToSearch the mapping between partitions and target time
     * @param requireTimestamps  true if we should fail with an UnsupportedVersionException if the broker does
     * not support fetching precise timestamps for offsets
     * @return A response which can be polled to obtain the corresponding timestamps and offsets.
     */
    private fun sendListOffsetsRequests(
        timestampsToSearch: Map<TopicPartition, Long>,
        requireTimestamps: Boolean,
    ): RequestFuture<ListOffsetResult> {
        val partitionsToRetry = mutableSetOf<TopicPartition>()
        val timestampsToSearchByNode = groupListOffsetRequests(timestampsToSearch, partitionsToRetry)
        if (timestampsToSearchByNode.isEmpty()) return RequestFuture.failure(StaleMetadataException())

        val listOffsetRequestsFuture = RequestFuture<ListOffsetResult>()
        val fetchedTimestampOffsets = mutableMapOf<TopicPartition, ListOffsetData>()
        val remainingResponses = AtomicInteger(timestampsToSearchByNode.size)

        for ((node, timestamps) in timestampsToSearchByNode) {
            val future = sendListOffsetRequest(
                node = node,
                timestampsToSearch = timestamps,
                requireTimestamp = requireTimestamps,
            )
            future.addListener(object : RequestFutureListener<ListOffsetResult> {
                override fun onSuccess(result: ListOffsetResult) {
                    synchronized(listOffsetRequestsFuture) {
                        fetchedTimestampOffsets.putAll(result.fetchedOffsets)
                        partitionsToRetry.addAll(result.partitionsToRetry)
                        if (remainingResponses.decrementAndGet() == 0 && !listOffsetRequestsFuture.isDone()) {
                            listOffsetRequestsFuture.complete(
                                ListOffsetResult(fetchedTimestampOffsets, partitionsToRetry)
                            )
                        }
                    }
                }

                override fun onFailure(exception: RuntimeException) {
                    synchronized(listOffsetRequestsFuture) {
                        if (!listOffsetRequestsFuture.isDone())
                            listOffsetRequestsFuture.raise(exception)
                    }
                }
            })
        }
        return listOffsetRequestsFuture
    }

    /**
     * Groups timestamps to search by node for topic partitions in `timestampsToSearch` that have
     * leaders available. Topic partitions from `timestampsToSearch` that do not have their leader
     * available are added to `partitionsToRetry`
     *
     * @param timestampsToSearch The mapping from partitions to the target timestamps
     * @param partitionsToRetry  A set of topic partitions that will be extended with partitions
     * that need metadata update or re-connect to the leader.
     */
    private fun groupListOffsetRequests(
        timestampsToSearch: Map<TopicPartition, Long>,
        partitionsToRetry: MutableSet<TopicPartition>,
    ): Map<Node, Map<TopicPartition, ListOffsetsPartition>> {
        val partitionDataMap: MutableMap<TopicPartition, ListOffsetsPartition> = HashMap()
        for ((tp, offset) in timestampsToSearch) {
            val (leader, epoch) = metadata.currentLeader(tp)
            if (leader == null) {
                log.debug("Leader for partition {} is unknown for fetching offset {}", tp, offset)
                metadata.requestUpdate()
                partitionsToRetry.add(tp)
            } else {
                if (client.isUnavailable(leader)) {
                    client.maybeThrowAuthFailure(leader)

                    // The connection has failed and we need to await the backoff period before we can
                    // try again. No need to request a metadata update since the disconnect will have
                    // done so already.
                    log.debug(
                        "Leader {} for partition {} is unavailable for fetching offset until reconnect backoff expires",
                        leader, tp
                    )
                    partitionsToRetry.add(tp)
                } else {
                    val currentLeaderEpoch = epoch ?: ListOffsetsResponse.UNKNOWN_EPOCH
                    partitionDataMap[tp] = ListOffsetsPartition()
                        .setPartitionIndex(tp.partition)
                        .setTimestamp(offset)
                        .setCurrentLeaderEpoch(currentLeaderEpoch)
                }
            }
        }
        return offsetFetcherUtils.regroupPartitionMapByNode(partitionDataMap)
    }

    /**
     * Send the ListOffsetRequest to a specific broker for the partitions and target timestamps.
     *
     * @param node               The node to send the ListOffsetRequest to.
     * @param timestampsToSearch The mapping from partitions to the target timestamps.
     * @param requireTimestamp   True if we require a timestamp in the response.
     * @return A response which can be polled to obtain the corresponding timestamps and offsets.
     */
    private fun sendListOffsetRequest(
        node: Node,
        timestampsToSearch: Map<TopicPartition, ListOffsetsPartition>,
        requireTimestamp: Boolean,
    ): RequestFuture<ListOffsetResult> {
        val builder = ListOffsetsRequest.Builder
            .forConsumer(requireTimestamp, isolationLevel, false)
            .setTargetTimes(ListOffsetsRequest.toListOffsetsTopics(timestampsToSearch))
        log.debug("Sending ListOffsetRequest {} to broker {}", builder, node)
        return client.send(node, builder)
            .compose(object : RequestFutureAdapter<ClientResponse, ListOffsetResult>() {
                override fun onSuccess(response: ClientResponse, future: RequestFuture<ListOffsetResult>) {
                    val lor = response.responseBody as ListOffsetsResponse
                    log.trace("Received ListOffsetResponse {} from broker {}", lor, node)
                    handleListOffsetResponse(lor, future)
                }
            })
    }

    /**
     * Callback for the response of the list offset call above.
     *
     * @param listOffsetsResponse The response from the server.
     * @param future The future to be completed when the response returns. Note that any partition-level errors
     * will generally fail the entire future result. The one exception is UNSUPPORTED_FOR_MESSAGE_FORMAT,
     * which indicates that the broker does not support the v1 message format.
     * Partitions with this particular error are simply left out of the future map.
     * Note that the corresponding timestamp value of each partition may be null only for v0. In v1 and
     * later the ListOffset API would not return a null timestamp (-1 is returned instead when necessary).
     */
    private fun handleListOffsetResponse(
        listOffsetsResponse: ListOffsetsResponse,
        future: RequestFuture<ListOffsetResult>,
    ) {
        try {
            val result = offsetFetcherUtils.handleListOffsetResponse(listOffsetsResponse)
            future.complete(result)
        } catch (e: RuntimeException) {
            future.raise(e)
        }
    }

    /**
     * If we have seen new metadata (as tracked by [org.apache.kafka.clients.Metadata.updateVersion]),
     * then we should check that all the assignments have a valid position.
     */
    fun validatePositionsOnMetadataChange() {
        offsetFetcherUtils.validatePositionsOnMetadataChange()
    }
}
