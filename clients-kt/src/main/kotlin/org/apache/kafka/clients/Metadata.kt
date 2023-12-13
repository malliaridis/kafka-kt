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

package org.apache.kafka.clients

import java.io.Closeable
import java.net.InetSocketAddress
import org.apache.kafka.common.Cluster
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.Node
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.Uuid
import org.apache.kafka.common.errors.InvalidMetadataException
import org.apache.kafka.common.errors.InvalidTopicException
import org.apache.kafka.common.errors.TopicAuthorizationException
import org.apache.kafka.common.internals.ClusterResourceListeners
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.record.RecordBatch
import org.apache.kafka.common.requests.MetadataRequest
import org.apache.kafka.common.requests.MetadataResponse
import org.apache.kafka.common.requests.MetadataResponse.PartitionMetadata
import org.apache.kafka.common.utils.LogContext
import org.slf4j.Logger

/**
 * A class encapsulating some of the logic around metadata.
 *
 *
 * This class is shared by the client thread (for partitioning) and the background sender thread.
 *
 * Metadata is maintained for only a subset of topics, which can be added to over time. When we
 * request metadata for a topic we don't have any metadata for it will trigger a metadata update.
 *
 *
 * If topic expiry is enabled for the metadata, any topic that has not been used within the expiry
 * interval is removed from the metadata refresh set after an update. Consumers disable topic expiry
 * since they explicitly manage topics while producers rely on topic expiry to limit the refresh set.
 */
open class Metadata(
    private val refreshBackoffMs: Long,
    private val metadataExpireMs: Long,
    private val clusterResourceListeners: ClusterResourceListeners,
    private val log: Logger,
) : Closeable {

    private var updateVersion = 0 // bumped on every metadata response

    private var requestVersion = 0 // bumped on every new topic addition

    private var lastRefreshMs = 0L

    private var lastSuccessfulRefreshMs = 0L

    private var fatalException: KafkaException? = null

    private var invalidTopics: Set<String> = emptySet()

    private var unauthorizedTopics: Set<String> = emptySet()

    private var cache = MetadataCache.empty()

    private var needFullUpdate = false

    private var needPartialUpdate = false

    /**
     * Check if this metadata instance has been closed. See [close] for more information.
     *
     * @return True if this instance has been closed; false otherwise
     */
    @get:Synchronized
    var isClosed = false
        private set

    val lastSeenLeaderEpochs: MutableMap<TopicPartition, Int> = HashMap()

    /**
     * Create a new Metadata instance
     *
     * @param refreshBackoffMs The minimum amount of time that must expire between metadata
     * refreshes to avoid busy polling
     * @param metadataExpireMs The maximum amount of time that metadata can be retained without
     * refresh
     * @param logContext Log context corresponding to the containing client
     * @param clusterResourceListeners List of ClusterResourceListeners which will receive metadata
     * updates.
     */
    constructor(
        refreshBackoffMs: Long,
        metadataExpireMs: Long,
        logContext: LogContext,
        clusterResourceListeners: ClusterResourceListeners,
    ) : this(
        refreshBackoffMs = refreshBackoffMs,
        metadataExpireMs = metadataExpireMs,
        log = logContext.logger(Metadata::class.java),
        clusterResourceListeners = clusterResourceListeners
    )

    /**
     * Get the current cluster info without blocking
     */
    @Synchronized
    fun fetch(): Cluster {
        return cache.cluster()
    }

    /**
     * Return the next time when the current cluster info can be updated (i.e., backoff time has elapsed).
     *
     * @param nowMs current time in ms
     * @return remaining time in ms till the cluster info can be updated again
     */
    @Synchronized
    fun timeToAllowUpdate(nowMs: Long): Long {
        return (lastRefreshMs + refreshBackoffMs - nowMs).coerceAtLeast(0)
    }

    /**
     * The next time to update the cluster info is the maximum of the time the current info will expire and the time the
     * current info can be updated (i.e. backoff time has elapsed); If an update has been request then the expiry time
     * is now
     *
     * @param nowMs current time in ms
     * @return remaining time in ms till updating the cluster info
     */
    @Synchronized
    fun timeToNextUpdate(nowMs: Long): Long {
        val timeToExpire = if (updateRequested()) 0
        else (lastSuccessfulRefreshMs + metadataExpireMs - nowMs).coerceAtLeast(0)
        return timeToExpire.coerceAtLeast(timeToAllowUpdate(nowMs))
    }

    fun metadataExpireMs(): Long {
        return metadataExpireMs
    }

    /**
     * Request an update of the current cluster metadata info, return the current updateVersion before the update
     */
    @Synchronized
    fun requestUpdate(): Int {
        needFullUpdate = true
        return updateVersion
    }

    @Synchronized
    fun requestUpdateForNewTopics(): Int {
        // Override the timestamp of last refresh to let immediate update.
        lastRefreshMs = 0
        needPartialUpdate = true
        requestVersion++
        return updateVersion
    }

    /**
     * Request an update for the partition metadata iff we have seen a newer leader epoch. This is called by the client
     * any time it handles a response from the broker that includes leader epoch, except for UpdateMetadata which
     * follows a different code path ([update]).
     *
     * @param topicPartition
     * @param leaderEpoch
     * @return true if we updated the last seen epoch, false otherwise
     */
    @Synchronized
    fun updateLastSeenEpochIfNewer(topicPartition: TopicPartition, leaderEpoch: Int): Boolean {
        require(leaderEpoch >= 0) { "Invalid leader epoch $leaderEpoch (must be non-negative)" }
        val oldEpoch = lastSeenLeaderEpochs[topicPartition]
        log.trace(
            "Determining if we should replace existing epoch {} with new epoch {} for partition {}",
            oldEpoch, leaderEpoch, topicPartition
        )
        val updated: Boolean
        if (oldEpoch == null) {
            log.debug(
                "Not replacing null epoch with new epoch {} for partition {}",
                leaderEpoch, topicPartition
            )
            updated = false
        } else if (leaderEpoch > oldEpoch) {
            log.debug(
                "Updating last seen epoch from {} to {} for partition {}",
                oldEpoch, leaderEpoch, topicPartition
            )
            lastSeenLeaderEpochs[topicPartition] = leaderEpoch
            updated = true
        } else {
            log.debug(
                "Not replacing existing epoch {} with new epoch {} for partition {}",
                oldEpoch, leaderEpoch, topicPartition
            )
            updated = false
        }
        needFullUpdate = needFullUpdate || updated
        return updated
    }

    @Deprecated(
        message = "User property directly",
        replaceWith = ReplaceWith("lastSeenLeaderEpochs[topicPartition]")
    )
    fun lastSeenLeaderEpoch(topicPartition: TopicPartition): Int? =
        lastSeenLeaderEpochs[topicPartition]

    /**
     * Check whether an update has been explicitly requested.
     *
     * @return true if an update was requested, false otherwise
     */
    @Synchronized
    fun updateRequested(): Boolean = needFullUpdate || needPartialUpdate

    /**
     * Return the cached partition info if it exists and a newer leader epoch isn't known about.
     */
    @Synchronized
    fun partitionMetadataIfCurrent(topicPartition: TopicPartition): PartitionMetadata? {
        val epoch = lastSeenLeaderEpochs[topicPartition]
        val partitionMetadata = cache.partitionMetadata(topicPartition)
        return if (epoch == null) partitionMetadata // old cluster format (no epochs)
        else partitionMetadata?.takeIf { metadata: PartitionMetadata ->
            (metadata.leaderEpoch ?: RecordBatch.NO_PARTITION_LEADER_EPOCH) == epoch
        }
    }

    /**
     * @return a mapping from topic names to topic IDs for all topics with valid IDs in the cache
     */
    @Synchronized
    fun topicIds(): Map<String, Uuid> {
        return cache.topicIds()
    }

    @Synchronized
    fun currentLeader(topicPartition: TopicPartition): LeaderAndEpoch {
        val partitionMetadata = partitionMetadataIfCurrent(topicPartition) ?: return LeaderAndEpoch(
            leader = null,
            epoch = lastSeenLeaderEpochs[topicPartition]
        )

        val leaderEpochOpt = partitionMetadata.leaderEpoch
        val leaderNodeOpt = partitionMetadata.leaderId?.let { cache.nodeById(it) }
        return LeaderAndEpoch(
            leader = leaderNodeOpt,
            epoch = leaderEpochOpt,
        )
    }

    @Synchronized
    fun bootstrap(addresses: List<InetSocketAddress>) {
        needFullUpdate = true
        updateVersion += 1
        cache = MetadataCache.bootstrap(addresses)
    }

    /**
     * Update metadata assuming the current request version.
     *
     * For testing only.
     */
    @Synchronized
    fun updateWithCurrentRequestVersion(
        response: MetadataResponse,
        isPartialUpdate: Boolean,
        nowMs: Long,
    ) {
        update(requestVersion, response, isPartialUpdate, nowMs)
    }

    /**
     * Updates the cluster metadata. If topic expiry is enabled, expiry time
     * is set for topics if required and expired topics are removed from the metadata.
     *
     * @param requestVersion The request version corresponding to the update response, as provided by
     * [newMetadataRequestAndVersion].
     * @param response metadata response received from the broker
     * @param isPartialUpdate whether the metadata request was for a subset of the active topics
     * @param nowMs current time in milliseconds
     */
    @Synchronized
    open fun update(
        requestVersion: Int,
        response: MetadataResponse,
        isPartialUpdate: Boolean,
        nowMs: Long,
    ) {
        check(!isClosed) { "Update requested after metadata close" }
        needPartialUpdate = requestVersion < this.requestVersion
        lastRefreshMs = nowMs
        updateVersion += 1
        if (!isPartialUpdate) {
            needFullUpdate = false
            lastSuccessfulRefreshMs = nowMs
        }
        val previousClusterId = cache.clusterResource().clusterId
        cache = handleMetadataResponse(response, isPartialUpdate, nowMs)
        val cluster = cache.cluster()
        maybeSetMetadataError(cluster)
        lastSeenLeaderEpochs.keys.removeIf { tp: TopicPartition ->
            !retainTopic(
                topic = tp.topic,
                isInternal = false,
                nowMs = nowMs
            )
        }
        val newClusterId = cache.clusterResource().clusterId
        if (previousClusterId != newClusterId) {
            log.info("Cluster ID: {}", newClusterId.toString())
        }
        clusterResourceListeners.onUpdate(cache.clusterResource())
        log.debug("Updated cluster metadata updateVersion {} to {}", updateVersion, cache)
    }

    private fun maybeSetMetadataError(cluster: Cluster) {
        clearRecoverableErrors()
        checkInvalidTopics(cluster)
        checkUnauthorizedTopics(cluster)
    }

    private fun checkInvalidTopics(cluster: Cluster) {
        if (cluster.invalidTopics.isNotEmpty()) {
            log.error("Metadata response reported invalid topics {}", cluster.invalidTopics)
            invalidTopics = cluster.invalidTopics.toHashSet()
        }
    }

    private fun checkUnauthorizedTopics(cluster: Cluster) {
        if (cluster.unauthorizedTopics.isNotEmpty()) {
            log.error("Topic authorization failed for topics {}", cluster.unauthorizedTopics)
            unauthorizedTopics = cluster.unauthorizedTopics.toHashSet()
        }
    }

    /**
     * Transform a MetadataResponse into a new MetadataCache instance.
     */
    private fun handleMetadataResponse(
        metadataResponse: MetadataResponse,
        isPartialUpdate: Boolean,
        nowMs: Long,
    ): MetadataCache {
        // All encountered topics.
        val topics: MutableSet<String> = mutableSetOf()

        // Retained topics to be passed to the metadata cache.
        val internalTopics: MutableSet<String> = hashSetOf()
        val unauthorizedTopics: MutableSet<String> = hashSetOf()
        val invalidTopics: MutableSet<String> = hashSetOf()
        val partitions: MutableList<PartitionMetadata> = ArrayList()
        val topicIds: MutableMap<String, Uuid> = hashMapOf()
        val oldTopicIds = cache.topicIds()
        metadataResponse.topicMetadata().forEach { metadata ->
            val topicName = metadata.topic
            var topicId: Uuid? = metadata.topicId
            topics.add(topicName)
            // We can only reason about topic ID changes when both IDs are valid, so keep oldId null
            // unless the new metadata contains a topic ID
            var oldTopicId: Uuid? = null
            if (Uuid.ZERO_UUID != topicId) {
                topicIds[topicName] = topicId!!
                oldTopicId = oldTopicIds[topicName]
            } else topicId = null
            if (!retainTopic(topicName, metadata.isInternal, nowMs)) return@forEach
            if (metadata.isInternal) internalTopics.add(topicName)
            if (metadata.error == Errors.NONE) {
                for (partitionMetadata in metadata.partitionMetadata) {
                    // Even if the partition's metadata includes an error, we need to handle
                    // the update to catch new epochs
                    updateLatestMetadata(
                        partitionMetadata = partitionMetadata,
                        hasReliableLeaderEpoch = metadataResponse.hasReliableLeaderEpochs(),
                        topicId = topicId,
                        oldTopicId = oldTopicId,
                    )?.let { e: PartitionMetadata -> partitions.add(e) }

                    if (partitionMetadata.error.exception is InvalidMetadataException) {
                        log.debug(
                            "Requesting metadata update for partition {} due to error {}",
                            partitionMetadata.topicPartition,
                            partitionMetadata.error
                        )
                        requestUpdate()
                    }
                }
            } else {
                if (metadata.error.exception is InvalidMetadataException) {
                    log.debug(
                        "Requesting metadata update for topic {} due to error {}",
                        topicName, metadata.error
                    )
                    requestUpdate()
                }
                if (metadata.error == Errors.INVALID_TOPIC_EXCEPTION) invalidTopics.add(topicName)
                else if (metadata.error == Errors.TOPIC_AUTHORIZATION_FAILED)
                    unauthorizedTopics.add(topicName)
            }
        }
        val nodes = metadataResponse.brokersById()
        return if (isPartialUpdate) cache.mergeWith(
            newClusterId = metadataResponse.clusterId,
            newNodes = nodes,
            addPartitions = partitions,
            addUnauthorizedTopics = unauthorizedTopics,
            addInvalidTopics = invalidTopics,
            addInternalTopics = internalTopics,
            newController = metadataResponse.controller,
            topicIds = topicIds,
            retainTopic = { topic, isInternal ->
                !topics.contains(topic) && retainTopic(topic, isInternal, nowMs)
            }
        ) else MetadataCache(
            clusterId = metadataResponse.clusterId,
            nodes = nodes,
            partitions = partitions,
            unauthorizedTopics = unauthorizedTopics,
            invalidTopics = invalidTopics,
            internalTopics = internalTopics,
            controller = metadataResponse.controller,
            topicIds = topicIds
        )
    }

    /**
     * Compute the latest partition metadata to cache given ordering by leader epochs (if both
     * available and reliable) and whether the topic ID changed.
     */
    private fun updateLatestMetadata(
        partitionMetadata: PartitionMetadata,
        hasReliableLeaderEpoch: Boolean,
        topicId: Uuid?,
        oldTopicId: Uuid?,
    ): PartitionMetadata? {
        val tp = partitionMetadata.topicPartition
        return if (hasReliableLeaderEpoch && partitionMetadata.leaderEpoch != null) {
            val newEpoch = partitionMetadata.leaderEpoch
            val currentEpoch = lastSeenLeaderEpochs[tp]
            if (currentEpoch == null) {
                // We have no previous info, so we can just insert the new epoch info
                log.debug(
                    "Setting the last seen epoch of partition {} to {} since the last known epoch was undefined.",
                    tp, newEpoch
                )
                lastSeenLeaderEpochs[tp] = newEpoch
                return partitionMetadata
            } else if (topicId != null && topicId != oldTopicId) {
                // If the new topic ID is valid and different from the last seen topic ID, update the metadata.
                // Between the time that a topic is deleted and re-created, the client may lose track of the
                // corresponding topicId (i.e. `oldTopicId` will be null). In this case, when we discover the new
                // topicId, we allow the corresponding leader epoch to override the last seen value.
                log.info(
                    "Resetting the last seen epoch of partition {} to {} since the associated topicId changed from {} to {}",
                    tp, newEpoch, oldTopicId, topicId
                )
                lastSeenLeaderEpochs[tp] = newEpoch
                partitionMetadata
            } else if (newEpoch >= currentEpoch) {
                // If the received leader epoch is at least the same as the previous one, update the metadata
                log.debug(
                    "Updating last seen epoch for partition {} from {} to epoch {} from new metadata",
                    tp, currentEpoch, newEpoch
                )
                lastSeenLeaderEpochs[tp] = newEpoch
                partitionMetadata
            } else {
                // Otherwise ignore the new metadata and use the previously cached info
                log.debug(
                    "Got metadata for an older epoch {} (current is {}) for partition {}, not updating",
                    newEpoch, currentEpoch, tp
                )
                cache.partitionMetadata(tp)
            }
        } else {
            // Handle old cluster formats as well as error responses where leader and epoch are missing
            lastSeenLeaderEpochs.remove(tp)
            partitionMetadata.withoutLeaderEpoch()
        }
    }

    /**
     * If any non-retriable exceptions were encountered during metadata update, clear and throw the exception.
     * This is used by the consumer to propagate any fatal exceptions or topic exceptions for any of the topics
     * in the consumer's Metadata.
     */
    @Synchronized
    fun maybeThrowAnyException() {
        clearErrorsAndMaybeThrowException { recoverableException() }
    }

    /**
     * If any fatal exceptions were encountered during metadata update, throw the exception. This is used by
     * the producer to abort waiting for metadata if there were fatal exceptions (e.g. authentication failures)
     * in the last metadata update.
     */
    @Synchronized
    protected fun maybeThrowFatalException() {
        val metadataException = fatalException
        if (metadataException != null) {
            fatalException = null
            throw metadataException
        }
    }

    /**
     * If any non-retriable exceptions were encountered during metadata update, throw exception if the exception
     * is fatal or related to the specified topic. All exceptions from the last metadata update are cleared.
     * This is used by the producer to propagate topic metadata errors for send requests.
     */
    @Synchronized
    fun maybeThrowExceptionForTopic(topic: String) {
        clearErrorsAndMaybeThrowException {
            recoverableExceptionForTopic(topic)
        }
    }

    private fun clearErrorsAndMaybeThrowException(recoverableExceptionSupplier: () -> KafkaException?) {
        val metadataException = fatalException ?: recoverableExceptionSupplier()
        fatalException = null
        clearRecoverableErrors()
        if (metadataException != null) throw metadataException
    }

    // We may be able to recover from this exception if metadata for this topic is no longer needed
    private fun recoverableException(): KafkaException? {
        return if (unauthorizedTopics.isNotEmpty()) TopicAuthorizationException(unauthorizedTopics)
        else if (invalidTopics.isNotEmpty()) InvalidTopicException(invalidTopics)
        else null
    }

    private fun recoverableExceptionForTopic(topic: String): KafkaException? {
        return if (unauthorizedTopics.contains(topic)) TopicAuthorizationException(setOf(topic))
        else if (invalidTopics.contains(topic)) InvalidTopicException(setOf(topic))
        else null
    }

    private fun clearRecoverableErrors() {
        invalidTopics = emptySet()
        unauthorizedTopics = emptySet()
    }

    /**
     * Record an attempt to update the metadata that failed. We need to keep track of this
     * to avoid retrying immediately.
     */
    @Synchronized
    fun failedUpdate(now: Long) {
        lastRefreshMs = now
    }

    /**
     * Propagate a fatal error which affects the ability to fetch metadata for the cluster.
     * Two examples are authentication and unsupported version exceptions.
     *
     * @param exception The fatal exception
     */
    @Synchronized
    open fun fatalError(exception: KafkaException?) {
        fatalException = exception
    }

    /**
     * @return The current metadata updateVersion
     */
    @Synchronized
    fun updateVersion(): Int = updateVersion

    /**
     * The last time metadata was successfully updated.
     */
    @Synchronized
    fun lastSuccessfulUpdate(): Long = lastSuccessfulRefreshMs

    /**
     * Close this metadata instance to indicate that metadata updates are no longer possible.
     */
    @Synchronized
    override fun close() {
        isClosed = true
    }

    @Synchronized
    fun newMetadataRequestAndVersion(nowMs: Long): MetadataRequestAndVersion {
        var request: MetadataRequest.Builder? = null
        var isPartialUpdate = false

        // Perform a partial update only if a full update hasn't been requested, and the last successful
        // hasn't exceeded the metadata refresh time.
        if (!needFullUpdate && lastSuccessfulRefreshMs + metadataExpireMs > nowMs) {
            request = newMetadataRequestBuilderForNewTopics()
            isPartialUpdate = true
        }
        if (request == null) {
            request = newMetadataRequestBuilder()
            isPartialUpdate = false
        }
        return MetadataRequestAndVersion(request, requestVersion, isPartialUpdate)
    }

    /**
     * Constructs and returns a metadata request builder for fetching cluster data and all active topics.
     *
     * @return the constructed non-null metadata builder
     */
    open fun newMetadataRequestBuilder(): MetadataRequest.Builder {
        return MetadataRequest.Builder.allTopics()
    }

    /**
     * Constructs and returns a metadata request builder for fetching cluster data and any uncached topics,
     * otherwise null if the functionality is not supported.
     *
     * @return the constructed metadata builder, or null if not supported
     */
    internal open fun newMetadataRequestBuilderForNewTopics(): MetadataRequest.Builder? {
        return null
    }

    internal open fun retainTopic(
        topic: String,
        isInternal: Boolean,
        nowMs: Long,
    ): Boolean = true

    class MetadataRequestAndVersion internal constructor(
        val requestBuilder: MetadataRequest.Builder,
        val requestVersion: Int,
        val isPartialUpdate: Boolean,
    )

    /**
     * Represents current leader state known in metadata. It is possible that we know the leader,
     * but not the epoch if the metadata is received from a broker which does not support a
     * sufficient Metadata API version. It is also possible that we know of the leader epoch, but
     * not the leader when it is derived from an external source (e.g. a committed offset).
     */
    data class LeaderAndEpoch(
        val leader: Node?,
        val epoch: Int?,
    ) {

        override fun toString(): String {
            return "LeaderAndEpoch{" +
                    "leader=" + leader +
                    ", epoch=" + (epoch?.toString() ?: "absent") +
                    '}'
        }

        companion object {
            private val NO_LEADER_OR_EPOCH = LeaderAndEpoch(null, null)

            fun noLeaderOrEpoch(): LeaderAndEpoch {
                return NO_LEADER_OR_EPOCH
            }
        }
    }
}
