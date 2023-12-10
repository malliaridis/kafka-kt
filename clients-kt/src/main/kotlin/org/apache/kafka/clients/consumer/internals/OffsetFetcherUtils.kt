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
import java.util.concurrent.atomic.AtomicReference
import org.apache.kafka.clients.ApiVersions
import org.apache.kafka.clients.NodeApiVersions
import org.apache.kafka.clients.consumer.OffsetAndTimestamp
import org.apache.kafka.clients.consumer.OffsetResetStrategy
import org.apache.kafka.clients.consumer.internals.SubscriptionState.FetchPosition
import org.apache.kafka.common.IsolationLevel
import org.apache.kafka.common.Node
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.RetriableException
import org.apache.kafka.common.errors.TopicAuthorizationException
import org.apache.kafka.common.message.ListOffsetsRequestData.ListOffsetsPartition
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.ListOffsetsRequest
import org.apache.kafka.common.requests.ListOffsetsResponse
import org.apache.kafka.common.requests.OffsetsForLeaderEpochRequest
import org.apache.kafka.common.utils.LogContext
import org.apache.kafka.common.utils.Time
import org.slf4j.Logger

/**
 * Utility functions for fetching offsets, validating and resetting positions.
 */
internal class OffsetFetcherUtils(
    logContext: LogContext,
    private val metadata: ConsumerMetadata,
    private val subscriptionState: SubscriptionState,
    private val time: Time,
    private val retryBackoffMs: Long,
    private val apiVersions: ApiVersions,
) {

    private val log: Logger = logContext.logger(javaClass)

    private val cachedOffsetForLeaderException = AtomicReference<RuntimeException?>()

    private val cachedListOffsetsException = AtomicReference<RuntimeException?>()

    private val metadataUpdateVersion = AtomicInteger(-1)

    /**
     * Callback for the response of the list offset call.
     *
     * @param listOffsetsResponse The response from the server.
     * @return [OffsetFetcherUtils.ListOffsetResult] extracted from the response, containing the fetched offsets
     * and partitions to retry.
     */
    fun handleListOffsetResponse(listOffsetsResponse: ListOffsetsResponse): ListOffsetResult {
        val fetchedOffsets = mutableMapOf<TopicPartition, ListOffsetData>()
        val partitionsToRetry = mutableSetOf<TopicPartition>()
        val unauthorizedTopics = mutableSetOf<String>()

        for (topic in listOffsetsResponse.topics) {
            for (partition in topic.partitions) {
                val topicPartition = TopicPartition(topic.name, partition.partitionIndex)
                when (val error = Errors.forCode(partition.errorCode)) {
                    Errors.NONE -> if (partition.oldStyleOffsets.isNotEmpty()) {
                        // Handle v0 response with offsets
                        check(partition.oldStyleOffsets.size == 1) {
                            "Unexpected partitionData response of length ${partition.oldStyleOffsets.size}"
                        }
                        val offset = partition.oldStyleOffsets[0]

                        log.debug(
                            "Handling v0 ListOffsetResponse response for {}. Fetched offset {}",
                            topicPartition, offset
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
                            topicPartition, partition.offset, partition.timestamp
                        )
                        if (partition.offset != ListOffsetsResponse.UNKNOWN_OFFSET) {
                            val leaderEpoch =
                                if (partition.leaderEpoch == ListOffsetsResponse.UNKNOWN_EPOCH) null
                                else partition.leaderEpoch
                            val offsetData = ListOffsetData(
                                offset = partition.offset,
                                timestamp = partition.timestamp,
                                leaderEpoch = leaderEpoch,
                            )
                            fetchedOffsets[topicPartition] = offsetData
                        }
                    }

                    Errors.UNSUPPORTED_FOR_MESSAGE_FORMAT ->
                        // The message format on the broker side is before 0.10.0, which means it does not
                        // support timestamps. We treat this case the same as if we weren't able to find an
                        // offset corresponding to the requested timestamp and leave it out of the result.
                        log.debug(
                            "Cannot search by timestamp for partition {} because the message format version " +
                                    "is before 0.10.0", topicPartition
                        )

                    Errors.NOT_LEADER_OR_FOLLOWER,
                    Errors.REPLICA_NOT_AVAILABLE,
                    Errors.KAFKA_STORAGE_ERROR,
                    Errors.OFFSET_NOT_AVAILABLE,
                    Errors.LEADER_NOT_AVAILABLE,
                    Errors.FENCED_LEADER_EPOCH,
                    Errors.UNKNOWN_LEADER_EPOCH,
                    -> {
                        log.debug(
                            "Attempt to fetch offsets for partition {} failed due to {}, retrying.",
                            topicPartition, error
                        )
                        partitionsToRetry.add(topicPartition)
                    }

                    Errors.UNKNOWN_TOPIC_OR_PARTITION -> {
                        log.warn(
                            "Received unknown topic or partition error in ListOffset request for partition {}",
                            topicPartition
                        )
                        partitionsToRetry.add(topicPartition)
                    }

                    Errors.TOPIC_AUTHORIZATION_FAILED -> unauthorizedTopics.add(topicPartition.topic)
                    else -> {
                        log.warn(
                            "Attempt to fetch offsets for partition {} failed due to unexpected exception: {}, retrying.",
                            topicPartition, error.message
                        )
                        partitionsToRetry.add(topicPartition)
                    }
                }
            }
        }
        return if (unauthorizedTopics.isNotEmpty()) throw TopicAuthorizationException(unauthorizedTopics)
        else ListOffsetResult(
            fetchedOffsets = fetchedOffsets,
            partitionsToRetry = partitionsToRetry,
        )
    }

    fun <T> regroupPartitionMapByNode(partitionMap: Map<TopicPartition, T>): Map<Node, Map<TopicPartition, T>> {
        return partitionMap.entries.groupBy { (tp, _) -> metadata.fetch().leaderFor(tp)!! }
            .mapValues { it.value.associate { (key, value) -> key to value } }
    }

    val partitionsToValidate: Map<TopicPartition, FetchPosition>
        get() {
            val exception = cachedOffsetForLeaderException.getAndSet(null)
            if (exception != null) throw exception

            // Validate each partition against the current leader and epoch
            // If we see a new metadata version, check all partitions
            validatePositionsOnMetadataChange()

            // Collect positions needing validation, with backoff
            return subscriptionState
                .partitionsNeedingValidation(time.milliseconds())
                .filter { tp -> subscriptionState.position(tp) != null }
                .associateWith { subscriptionState.position(it)!! }
        }

    fun maybeSetOffsetForLeaderException(e: RuntimeException?) {
        if (!cachedOffsetForLeaderException.compareAndSet(null, e)) {
            log.error("Discarding error in OffsetsForLeaderEpoch because another error is pending", e)
        }
    }

    /**
     * If we have seen new metadata (as tracked by [org.apache.kafka.clients.Metadata.updateVersion]), then
     * we should check that all the assignments have a valid position.
     */
    fun validatePositionsOnMetadataChange() {
        val newMetadataUpdateVersion = metadata.updateVersion()
        if (metadataUpdateVersion.getAndSet(newMetadataUpdateVersion) != newMetadataUpdateVersion) {
            subscriptionState.assignedPartitions().forEach { topicPartition ->
                val leaderAndEpoch = metadata.currentLeader(topicPartition)
                subscriptionState.maybeValidatePositionForCurrentLeader(
                    apiVersions = apiVersions,
                    topicPartition = topicPartition,
                    leaderAndEpoch = leaderAndEpoch,
                )
            }
        }
    }

    val offsetResetTimestamp: Map<TopicPartition, Long>
        get() {
            // Raise exception from previous offset fetch if there is one
            val exception = cachedListOffsetsException.getAndSet(null)
            if (exception != null) throw exception
            val partitions = subscriptionState.partitionsNeedingReset(time.milliseconds())
            val offsetResetTimestamps: MutableMap<TopicPartition, Long> = HashMap()
            for (partition in partitions) {
                val timestamp = offsetResetStrategyTimestamp(partition)
                if (timestamp != null) offsetResetTimestamps[partition] = timestamp
            }
            return offsetResetTimestamps
        }

    private fun offsetResetStrategyTimestamp(partition: TopicPartition): Long? {
        return when (subscriptionState.resetStrategy(partition)) {
            OffsetResetStrategy.EARLIEST -> ListOffsetsRequest.EARLIEST_TIMESTAMP
            OffsetResetStrategy.LATEST -> ListOffsetsRequest.LATEST_TIMESTAMP
            else -> null
        }
    }

    fun updateSubscriptionState(
        fetchedOffsets: Map<TopicPartition, ListOffsetData>,
        isolationLevel: IsolationLevel,
    ) {
        for ((partition, value) in fetchedOffsets) {

            // if the interested partitions are part of the subscriptions, use the returned offset to update
            // the subscription state as well:
            //   * with read-committed, the returned offset would be LSO;
            //   * with read-uncommitted, the returned offset would be HW;
            if (subscriptionState.isAssigned(partition)) {
                val offset = value.offset!!
                if (isolationLevel == IsolationLevel.READ_COMMITTED) {
                    log.trace("Updating last stable offset for partition {} to {}", partition, offset)
                    subscriptionState.updateLastStableOffset(partition, offset)
                } else {
                    log.trace("Updating high watermark for partition {} to {}", partition, offset)
                    subscriptionState.updateHighWatermark(partition, offset)
                }
            }
        }
    }

    fun onSuccessfulRequestForResettingPositions(
        resetTimestamps: Map<TopicPartition, ListOffsetsPartition?>,
        result: ListOffsetResult,
    ) {
        if (result.partitionsToRetry.isNotEmpty()) {
            subscriptionState.requestFailed(result.partitionsToRetry, time.milliseconds() + retryBackoffMs)
            metadata.requestUpdate()
        }
        for ((partition, offsetData) in result.fetchedOffsets) {
            val requestedReset = resetTimestamps[partition]
            resetPositionIfNeeded(
                partition = partition,
                requestedResetStrategy = timestampToOffsetResetStrategy(requestedReset!!.timestamp),
                offsetData = offsetData,
            )
        }
    }

    fun onFailedRequestForResettingPositions(
        resetTimestamps: Map<TopicPartition, ListOffsetsPartition>,
        error: RuntimeException,
    ) {
        subscriptionState.requestFailed(resetTimestamps.keys, time.milliseconds() + retryBackoffMs)
        metadata.requestUpdate()
        if (error !is RetriableException && !cachedListOffsetsException.compareAndSet(null, error))
            log.error("Discarding error in ListOffsetResponse because another error is pending", error)
    }

    // Visible for testing
    internal fun resetPositionIfNeeded(
        partition: TopicPartition,
        requestedResetStrategy: OffsetResetStrategy?,
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
        subscriptionState.maybeSeekUnvalidated(
            topicPartition = partition,
            position = position,
            requestedResetStrategy = requestedResetStrategy,
        )
    }

    internal data class ListOffsetResult(
        val fetchedOffsets: MutableMap<TopicPartition, ListOffsetData> = mutableMapOf(),
        val partitionsToRetry: Set<TopicPartition> = emptySet(),
    )

    /**
     * Represents data about an offset returned by a broker.
     */
    internal data class ListOffsetData(
        val offset: Long?, //  null if the broker does not support returning timestamps
        val timestamp: Long?, // empty if the leader epoch is not known
        val leaderEpoch: Int?,
    )

    companion object {

        fun buildOffsetsForTimesResult(
            timestampsToSearch: Map<TopicPartition, Long?>,
            fetchedOffsets: Map<TopicPartition, ListOffsetData>,
        ): Map<TopicPartition, OffsetAndTimestamp?> {
            val offsetsByTimes = HashMap<TopicPartition, OffsetAndTimestamp?>(timestampsToSearch.size)

            for ((key) in timestampsToSearch) offsetsByTimes[key] = null

            for ((key, offsetData) in fetchedOffsets) {
                // 'entry.getValue().timestamp' will not be null since we are guaranteed
                // to work with a v1 (or later) ListOffset request
                offsetsByTimes[key] = OffsetAndTimestamp(
                    offset = offsetData.offset!!,
                    timestamp = offsetData.timestamp!!,
                    leaderEpoch = offsetData.leaderEpoch,
                )
            }
            return offsetsByTimes
        }

        fun topicsForPartitions(partitions: Collection<TopicPartition>): Set<String> =
            partitions.map { it.topic }.toSet()

        fun timestampToOffsetResetStrategy(timestamp: Long): OffsetResetStrategy? = when (timestamp) {
            ListOffsetsRequest.EARLIEST_TIMESTAMP -> OffsetResetStrategy.EARLIEST
            ListOffsetsRequest.LATEST_TIMESTAMP -> OffsetResetStrategy.LATEST
            else -> null
        }

        fun regroupFetchPositionsByLeader(
            partitionMap: Map<TopicPartition, FetchPosition>,
        ): Map<Node, Map<TopicPartition, FetchPosition>> {
            return partitionMap.entries
                .filter { it.value.currentLeader.leader != null }
                .groupBy { it.value.currentLeader.leader!! }
                .mapValues { it.value.associate { (tp, fp) -> tp to fp } }
        }

        fun hasUsableOffsetForLeaderEpochVersion(nodeApiVersions: NodeApiVersions): Boolean {
            val apiVersion = nodeApiVersions.apiVersion(ApiKeys.OFFSET_FOR_LEADER_EPOCH) ?: return false
            return OffsetsForLeaderEpochRequest.supportsTopicPermission(apiVersion.maxVersion)
        }
    }
}
