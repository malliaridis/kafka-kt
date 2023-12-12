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

import java.util.LinkedList
import java.util.Queue
import java.util.concurrent.CompletableFuture
import org.apache.kafka.clients.ClientResponse
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.clients.consumer.RetriableCommitFailedException
import org.apache.kafka.clients.consumer.internals.NetworkClientDelegate.PollResult
import org.apache.kafka.clients.consumer.internals.NetworkClientDelegate.UnsentRequest
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.GroupAuthorizationException
import org.apache.kafka.common.errors.TopicAuthorizationException
import org.apache.kafka.common.message.OffsetCommitRequestData
import org.apache.kafka.common.message.OffsetCommitRequestData.OffsetCommitRequestPartition
import org.apache.kafka.common.message.OffsetCommitRequestData.OffsetCommitRequestTopic
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.record.RecordBatch
import org.apache.kafka.common.requests.OffsetCommitRequest
import org.apache.kafka.common.requests.OffsetFetchRequest
import org.apache.kafka.common.requests.OffsetFetchResponse
import org.apache.kafka.common.utils.LogContext
import org.apache.kafka.common.utils.Time
import org.apache.kafka.common.utils.Timer
import org.slf4j.Logger

class CommitRequestManager(
    time: Time,
    logContext: LogContext,
    // TODO: We will need to refactor the subscriptionState
    private val subscriptionState: SubscriptionState,
    config: ConsumerConfig,
    private val coordinatorRequestManager: CoordinatorRequestManager,
    private val groupState: GroupState,
) : RequestManager {

    private val log: Logger = logContext.logger(javaClass)

    private var autoCommitState: AutoCommitState? = null

    private val retryBackoffMs: Long = (config.getLong(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG))!!

    private val throwOnFetchStableOffsetUnsupported: Boolean =
        config.getBoolean(THROW_ON_FETCH_STABLE_OFFSET_UNSUPPORTED)!!

    internal val pendingRequests: PendingRequests = PendingRequests()

    init {
        autoCommitState = if (config.getBoolean(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG)!!) {
            val autoCommitInterval =
                Integer.toUnsignedLong(config.getInt(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG)!!)
            AutoCommitState(time, autoCommitInterval)
        } else null
    }

    /**
     * Poll for the [OffsetFetchRequest] and [OffsetCommitRequest] request if there's any. The function will
     * also try to autocommit the offsets, if feature is enabled.
     */
    override fun poll(currentTimeMs: Long): PollResult {
        // poll only when the coordinator node is known.
        if (coordinatorRequestManager.coordinator == null) {
            return PollResult(Long.MAX_VALUE, emptyList())
        }
        maybeAutoCommit(subscriptionState.allConsumed())
        if (!pendingRequests.hasUnsentRequests()) {
            return PollResult(Long.MAX_VALUE, emptyList())
        }
        return PollResult(Long.MAX_VALUE, pendingRequests.drain(currentTimeMs))
    }

    fun maybeAutoCommit(offsets: Map<TopicPartition, OffsetAndMetadata>) {
        val autocommit = autoCommitState ?: return
        if (!autocommit.canSendAutocommit()) return

        sendAutoCommit(offsets)
        autocommit.resetTimer()
        autocommit.setInflightCommitStatus(true)
    }

    /**
     * Handles [org.apache.kafka.clients.consumer.internals.events.CommitApplicationEvent]. It creates an
     * [OffsetCommitRequestState] and enqueue it to send later.
     */
    fun addOffsetCommitRequest(
        offsets: Map<TopicPartition, OffsetAndMetadata>,
    ): CompletableFuture<ClientResponse> = pendingRequests.addOffsetCommitRequest(offsets)

    /**
     * Handles [org.apache.kafka.clients.consumer.internals.events.OffsetFetchApplicationEvent]. It creates an
     * [OffsetFetchRequestState] and enqueue it to send later.
     */
    fun addOffsetFetchRequest(
        partitions: Set<TopicPartition>,
    ): CompletableFuture<Map<TopicPartition, OffsetAndMetadata>> =
        pendingRequests.addOffsetFetchRequest(partitions)

    fun updateAutoCommitTimer(currentTimeMs: Long) {
        autoCommitState?.ack(currentTimeMs)
    }

    // Visible for testing
    internal fun unsentOffsetFetchRequests(): List<OffsetFetchRequestState> = pendingRequests.unsentOffsetFetches

    // Visible for testing
    internal fun unsentOffsetCommitRequests(): Queue<OffsetCommitRequestState> = pendingRequests.unsentOffsetCommits

    // Visible for testing
    internal fun sendAutoCommit(
        allConsumedOffsets: Map<TopicPartition, OffsetAndMetadata>,
    ): CompletableFuture<ClientResponse> {
        log.debug("Enqueuing autocommit offsets: {}", allConsumedOffsets)
        return addOffsetCommitRequest(allConsumedOffsets).whenComplete { _, throwable ->
            autoCommitState?.let { autoCommitState -> autoCommitState.setInflightCommitStatus(false) }

            if (throwable == null)
                log.debug("Completed asynchronous auto-commit of offsets {}", allConsumedOffsets)
        }.exceptionally { t ->
            if (t is RetriableCommitFailedException) {
                log.debug(
                    "Asynchronous auto-commit of offsets {} failed due to retriable error: {}",
                    allConsumedOffsets, t
                )
            } else log.warn("Asynchronous auto-commit of offsets {} failed: {}", allConsumedOffsets, t.message)
            null
        }
    }

    internal inner class OffsetCommitRequestState(
        private val offsets: Map<TopicPartition, OffsetAndMetadata>,
        private val groupId: String,
        private val groupInstanceId: String?,
        private val generation: GroupState.Generation,
    ) {

        private val future: NetworkClientDelegate.FutureCompletionHandler =
            NetworkClientDelegate.FutureCompletionHandler()

        fun future(): CompletableFuture<ClientResponse> = future.future()

        fun toUnsentRequest(): UnsentRequest {
            val requestTopicDataMap = mutableMapOf<String, OffsetCommitRequestTopic>()
            for ((topicPartition, offsetAndMetadata) in offsets) {
                val topic = requestTopicDataMap.getOrDefault(
                    key = topicPartition.topic,
                    defaultValue = OffsetCommitRequestTopic()
                        .setName(topicPartition.topic)
                )
                topic.partitions += OffsetCommitRequestPartition()
                    .setPartitionIndex(topicPartition.partition)
                    .setCommittedOffset(offsetAndMetadata.offset)
                    .setCommittedLeaderEpoch(offsetAndMetadata.leaderEpoch() ?: RecordBatch.NO_PARTITION_LEADER_EPOCH)
                    .setCommittedMetadata(offsetAndMetadata.metadata)
                requestTopicDataMap[topicPartition.topic] = topic
            }
            val builder = OffsetCommitRequest.Builder(
                OffsetCommitRequestData()
                    .setGroupId(groupId)
                    .setGenerationIdOrMemberEpoch(generation.generationId)
                    .setMemberId(generation.memberId)
                    .setGroupInstanceId(groupInstanceId)
                    .setTopics(requestTopicDataMap.values.toList())
            )
            return UnsentRequest(
                requestBuilder = builder,
                node = coordinatorRequestManager.coordinator,
                handler = future,
            )
        }
    }

    internal inner class OffsetFetchRequestState(
        val requestedPartitions: Set<TopicPartition>,
        val requestedGeneration: GroupState.Generation,
        retryBackoffMs: Long,
    ) : RequestState(retryBackoffMs) {

        var future: CompletableFuture<Map<TopicPartition, OffsetAndMetadata>> = CompletableFuture()

        fun sameRequest(request: OffsetFetchRequestState): Boolean {
            return requestedGeneration == request.requestedGeneration
                    && requestedPartitions == request.requestedPartitions
        }

        fun toUnsentRequest(currentTimeMs: Long): UnsentRequest {
            val builder = OffsetFetchRequest.Builder(
                groupId = groupState.groupId!!,
                requireStable = true,
                partitions = ArrayList(requestedPartitions),
                throwOnFetchStableOffsetsUnsupported = throwOnFetchStableOffsetUnsupported,
            )
            val unsentRequest = UnsentRequest(
                requestBuilder = builder,
                node = coordinatorRequestManager.coordinator,
            )
            unsentRequest.future.whenComplete { request, _ ->
                onResponse(currentTimeMs, request.responseBody as OffsetFetchResponse)
            }
            return unsentRequest
        }

        fun onResponse(
            currentTimeMs: Long,
            response: OffsetFetchResponse,
        ) {
            val responseError = response.groupLevelError(groupState.groupId!!)!!
            if (responseError != Errors.NONE) {
                onFailure(currentTimeMs, responseError)
                return
            }
            onSuccess(currentTimeMs, response)
        }

        fun onFailure(
            currentTimeMs: Long,
            responseError: Errors,
        ) {
            log.debug("Offset fetch failed: {}", responseError.message)

            // TODO: should we retry on COORDINATOR_NOT_AVAILABLE as well ?
            when (responseError) {
                Errors.COORDINATOR_LOAD_IN_PROGRESS -> retry(currentTimeMs)
                Errors.NOT_COORDINATOR -> {
                    // re-discover the coordinator and retry
                    coordinatorRequestManager.markCoordinatorUnknown(responseError.message, Time.SYSTEM.milliseconds())
                    retry(currentTimeMs)
                }
                Errors.GROUP_AUTHORIZATION_FAILED -> future.completeExceptionally(
                    GroupAuthorizationException(groupId = groupState.groupId)
                )
                else -> future.completeExceptionally(
                    KafkaException("Unexpected error in fetch offset response: " + responseError.message)
                )
            }
        }

        fun retry(currentTimeMs: Long) {
            onFailedAttempt(currentTimeMs)
            onSendAttempt(currentTimeMs)
            pendingRequests.addOffsetFetchRequest(this)
        }

        fun onSuccess(
            currentTimeMs: Long,
            response: OffsetFetchResponse,
        ) {
            var unauthorizedTopics: MutableSet<String>? = null
            val responseData = response.partitionDataMap(groupState.groupId!!)
            val offsets: MutableMap<TopicPartition, OffsetAndMetadata> = HashMap(responseData.size)
            val unstableTxnOffsetTopicPartitions: MutableSet<TopicPartition> = HashSet()
            for ((tp, partitionData) in responseData) {
                if (partitionData.hasError()) {
                    val error: Errors = partitionData.error
                    log.debug("Failed to fetch offset for partition {}: {}", tp, error.message)

                    when (error) {
                        Errors.UNKNOWN_TOPIC_OR_PARTITION -> {
                            future.completeExceptionally(KafkaException("Topic or Partition $tp does not exist"))
                            return
                        }
                        Errors.TOPIC_AUTHORIZATION_FAILED -> {
                            unauthorizedTopics = unauthorizedTopics ?: HashSet()
                            unauthorizedTopics.add(tp.topic)
                        }
                        Errors.UNSTABLE_OFFSET_COMMIT -> unstableTxnOffsetTopicPartitions.add(tp)
                        else -> {
                            future.completeExceptionally(
                                KafkaException(
                                    "Unexpected error in fetch offset response for partition $tp: ${error.message}"
                                )
                            )
                            return
                        }
                    }
                } else if (partitionData.offset >= 0) {
                    // record the position with the offset (-1 indicates no committed offset to fetch);
                    // if there's no committed offset, record as null
                    offsets[tp] = OffsetAndMetadata(
                        offset = partitionData.offset,
                        leaderEpoch = partitionData.leaderEpoch,
                        metadata = partitionData.metadata!!,
                    )
                } else log.info("Found no committed offset for partition {}", tp)
            }
            if (unauthorizedTopics != null) future.completeExceptionally(
                TopicAuthorizationException(unauthorizedTopics)
            )
            else if (unstableTxnOffsetTopicPartitions.isNotEmpty()) {
                // TODO: Optimization question: Do we need to retry all partitions upon a single partition error?
                log.info(
                    "The following partitions still have unstable offsets which are not cleared on " +
                            "the broker side: {}, this could be either transactional offsets waiting for completion, " +
                            "or normal offsets waiting for replication after appending to local log",
                    unstableTxnOffsetTopicPartitions
                )
                retry(currentTimeMs)
            } else future.complete(offsets)
        }

        fun chainFuture(
            future: CompletableFuture<Map<TopicPartition, OffsetAndMetadata>>,
        ): CompletableFuture<Map<TopicPartition, OffsetAndMetadata>> {
            return this.future.whenComplete { r, t ->
                if (t != null) future.completeExceptionally(t)
                else future.complete(r)
            }
        }
    }

    /**
     *
     * This is used to stage the unsent [OffsetCommitRequestState] and [OffsetFetchRequestState].
     * - unsentOffsetCommits holds the offset commit requests that have not been sent out>
     * - unsentOffsetFetches holds the offset fetch requests that have not been sent out
     * - inflightOffsetFetches holds the offset fetch requests that have been sent out but incompleted>.
     *
     * `addOffsetFetchRequest` dedupes the requests to avoid sending the same requests.
     */
    internal inner class PendingRequests {

        // Queue is used to ensure the sequence of commit
        var unsentOffsetCommits: Queue<OffsetCommitRequestState> = LinkedList()

        var unsentOffsetFetches = mutableListOf<OffsetFetchRequestState>()

        var inflightOffsetFetches = mutableListOf<OffsetFetchRequestState>()

        fun hasUnsentRequests(): Boolean {
            return !unsentOffsetCommits.isEmpty() || unsentOffsetFetches.isNotEmpty()
        }

        fun addOffsetCommitRequest(
            offsets: Map<TopicPartition, OffsetAndMetadata>,
        ): CompletableFuture<ClientResponse> {
            // TODO: Dedupe committing the same offsets to the same partitions
            val request = OffsetCommitRequestState(
                offsets = offsets,
                groupId = groupState.groupId!!,
                groupInstanceId = groupState.groupInstanceId,
                groupState.generation
            )
            unsentOffsetCommits.add(request)
            return request.future()
        }

        /**
         *
         * Adding an offset fetch request to the outgoing buffer.  If the same request was made, we chain the future
         * to the existing one.
         *
         *
         * If the request is new, it invokes a callback to remove itself from the `inflightOffsetFetches`
         * upon completion.>
         */
        fun addOffsetFetchRequest(
            request: OffsetFetchRequestState,
        ): CompletableFuture<Map<TopicPartition, OffsetAndMetadata>> {
            val dupe: OffsetFetchRequestState? = unsentOffsetFetches.firstOrNull { it.sameRequest(request) }
            val inflight: OffsetFetchRequestState? = inflightOffsetFetches.firstOrNull { it.sameRequest(request) }

            if (dupe != null || inflight != null) {
                log.info("Duplicated OffsetFetchRequest: {}", request.requestedPartitions)
                (dupe ?: inflight)!!.chainFuture(request.future)
            } else {
                // remove the request from the outbound buffer: inflightOffsetFetches
                request.future.whenComplete { _, _ ->
                    if (!inflightOffsetFetches.remove(request)) log.warn(
                        "A duplicated, inflight, request was identified, but unable to find it in the outbound buffer: {}",
                        request,
                    )
                }
                unsentOffsetFetches.add(request)
            }
            return request.future
        }

        fun addOffsetFetchRequest(
            partitions: Set<TopicPartition>,
        ): CompletableFuture<Map<TopicPartition, OffsetAndMetadata>> {
            val request = OffsetFetchRequestState(
                requestedPartitions = partitions,
                requestedGeneration = groupState.generation,
                retryBackoffMs = retryBackoffMs,
            )
            return addOffsetFetchRequest(request)
        }

        /**
         * Clear `unsentOffsetCommits` and moves all the sendable request in `unsentOffsetFetches` to the
         * `inflightOffsetFetches` to bookkeep all of the inflight requests.
         *
         * Note: Sendable requests are determined by their timer as we are expecting backoff on failed attempt. See
         * [RequestState].
         */
        fun drain(currentTimeMs: Long): List<UnsentRequest> {
            val unsentRequests = mutableListOf<UnsentRequest>()

            // Add all unsent offset commit requests to the unsentRequests list
            unsentRequests.addAll(unsentOffsetCommits.map { it.toUnsentRequest() })

            // Partition the unsent offset fetch requests into sendable and non-sendable lists
            val partitionedBySendability = unsentOffsetFetches.groupBy(
                keySelector = { request -> request.canSendRequest(currentTimeMs) },
            )

            // Add all sendable offset fetch requests to the unsentRequests list and to the inflightOffsetFetches list
            for (request in partitionedBySendability[true]!!) {
                request.onSendAttempt(currentTimeMs)
                unsentRequests.add(request.toUnsentRequest(currentTimeMs))
                inflightOffsetFetches.add(request)
            }

            // Clear the unsent offset commit and fetch lists and add all non-sendable offset fetch requests to the unsentOffsetFetches list
            unsentOffsetCommits.clear()
            unsentOffsetFetches.clear()
            unsentOffsetFetches.addAll(partitionedBySendability[false]!!)
            return unsentRequests
        }
    }

    /**
     * Encapsulates the state of auto-committing and manages the auto-commit timer.
     */
    private class AutoCommitState(
        time: Time,
        private val autoCommitInterval: Long,
    ) {

        private val timer: Timer = time.timer(autoCommitInterval)

        private var hasInflightCommit: Boolean = false

        fun canSendAutocommit(): Boolean = !hasInflightCommit && timer.isExpired

        fun resetTimer() = timer.reset(autoCommitInterval)

        fun ack(currentTimeMs: Long) = timer.update(currentTimeMs)

        fun setInflightCommitStatus(inflightCommitStatus: Boolean) {
            hasInflightCommit = inflightCommitStatus
        }
    }

    companion object {
        // TODO: current in ConsumerConfig but inaccessible in the internal package.
        private const val THROW_ON_FETCH_STABLE_OFFSET_UNSUPPORTED = "internal.throw.on.fetch.stable.offset.unsupported"
    }
}
