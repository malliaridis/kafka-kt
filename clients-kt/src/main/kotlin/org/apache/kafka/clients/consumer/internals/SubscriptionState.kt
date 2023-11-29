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
import org.apache.kafka.clients.Metadata.LeaderAndEpoch
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener
import org.apache.kafka.clients.consumer.NoOffsetForPartitionException
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.clients.consumer.OffsetResetStrategy
import org.apache.kafka.common.IsolationLevel
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.internals.PartitionStates
import org.apache.kafka.common.message.OffsetForLeaderEpochResponseData
import org.apache.kafka.common.requests.OffsetsForLeaderEpochResponse
import org.apache.kafka.common.utils.LogContext
import org.slf4j.Logger
import java.util.*
import java.util.function.Consumer
import java.util.function.LongSupplier
import java.util.function.Predicate
import java.util.regex.Pattern

/**
 * A class for tracking the topics, partitions, and offsets for the consumer. A partition is
 * "assigned" either directly with [assignFromUser] (manual assignment) or with
 * [assignFromSubscribed] (automatic assignment from subscription).
 *
 * Once assigned, the partition is not considered "fetchable" until its initial position has been
 * set with [seekValidated]. Fetchable partitions track a fetch position which is used to set the
 * offset of the next fetch, and a consumed position which is the last offset that has been returned
 * to the user. You can suspend fetching from a partition through [pause] without affecting the
 * fetched/consumed offsets. The partition will remain unfetchable until the [resume] is used. You
 * can also query the pause state independently with [isPaused].
 *
 * Note that pause state as well as fetch/consumed positions are not preserved when partition
 * assignment is changed whether directly by the user or through a group rebalance.
 *
 * Thread Safety: this class is thread-safe.
 */
class SubscriptionState(
    logContext: LogContext,
    /* Default offset reset strategy */
    private val defaultResetStrategy: OffsetResetStrategy
) {

    private val log: Logger = logContext.logger(this.javaClass)

    /**
     * The type of subscription
     */
    private var subscriptionType: SubscriptionType = SubscriptionType.NONE

    /**
     * The pattern user has requested
     */
    private var subscribedPattern: Pattern? = null

    /**
     * The list of topics the user has requested
     */
    private var subscription: Set<String> = TreeSet() // use a sorted set for better logging

    /**
     * The list of topics the group has subscribed to. This may include some topics which are not
     * part of `subscription` for the leader of a group since it is responsible for detecting
     * metadata changes which require a group rebalance.
     */
    private var groupSubscription = setOf<String>()

    /**
     * The partitions that are currently assigned, note that the order of partition matters (see
     * FetchBuilder for more details)
     */
    private val assignment: PartitionStates<TopicPartitionState> = PartitionStates()

    /**
     * User-provided listener to be invoked when assignment changes
     */
    @get:Synchronized
    var rebalanceListener: ConsumerRebalanceListener? = null
        private set

    private var assignmentId = 0

    @Synchronized
    override fun toString(): String {
        return "SubscriptionState{" +
                "type=$subscriptionType" +
                ", subscribedPattern=$subscribedPattern" +
                ", subscription=${subscription.joinToString(",")}" +
                ", groupSubscription=${groupSubscription.joinToString(",")}" +
                ", defaultResetStrategy=$defaultResetStrategy" +
                ", assignment=${assignment.partitionStateValues()} (id=$assignmentId)}"
    }

    @Synchronized
    fun prettyString(): String {
        return when (subscriptionType) {
            SubscriptionType.NONE -> "None"
            SubscriptionType.AUTO_TOPICS -> "Subscribe(${subscription.joinToString(",")})"
            SubscriptionType.AUTO_PATTERN -> "Subscribe($subscribedPattern)"
            SubscriptionType.USER_ASSIGNED -> "Assign(${assignedPartitions()} , id=$assignmentId)"
        }
    }

    /**
     * Monotonically increasing id which is incremented after every assignment change. This can be
     * used to check when an assignment has changed.
     *
     * @return The current assignment ID
     */
    @Synchronized
    fun assignmentId(): Int = assignmentId

    /**
     * This method sets the subscription type if it is not already set (i.e. when it is NONE), or
     * verifies that the subscription type is equal to the give type when it is set (i.e. when it is
     * not NONE)
     *
     * @param type The given subscription type
     */
    private fun setSubscriptionType(type: SubscriptionType) {
        if (subscriptionType == SubscriptionType.NONE) subscriptionType = type
        else check(subscriptionType == type) { SUBSCRIPTION_EXCEPTION_MESSAGE }
    }

    @Synchronized
    fun subscribe(topics: Set<String>, listener: ConsumerRebalanceListener): Boolean {
        registerRebalanceListener(listener)
        setSubscriptionType(SubscriptionType.AUTO_TOPICS)
        return changeSubscription(topics)
    }

    @Synchronized
    fun subscribe(pattern: Pattern, listener: ConsumerRebalanceListener) {
        registerRebalanceListener(listener)
        setSubscriptionType(SubscriptionType.AUTO_PATTERN)
        subscribedPattern = pattern
    }

    @Synchronized
    fun subscribeFromPattern(topics: Set<String>): Boolean {
        require(subscriptionType == SubscriptionType.AUTO_PATTERN) {
            "Attempt to subscribe from pattern while subscription type set to $subscriptionType"
        }
        return changeSubscription(topics)
    }

    private fun changeSubscription(topicsToSubscribe: Set<String>): Boolean {
        if (subscription == topicsToSubscribe) return false
        subscription = topicsToSubscribe
        return true
    }

    /**
     * Set the current group subscription. This is used by the group leader to ensure that it
     * receives metadata updates for all topics that the group is interested in.
     *
     * @param topics All topics from the group subscription
     * @return `true` if the group subscription contains topics which are not part of the local
     * subscription
     */
    @Synchronized
    fun groupSubscribe(topics: Collection<String>): Boolean {
        check(hasAutoAssignedPartitions()) { SUBSCRIPTION_EXCEPTION_MESSAGE }
        groupSubscription = topics.toHashSet()
        return !subscription.containsAll(groupSubscription)
    }

    /**
     * Reset the group's subscription to only contain topics subscribed by this consumer.
     */
    @Synchronized
    fun resetGroupSubscription() {
        groupSubscription = emptySet()
    }

    /**
     * Change the assignment to the specified partitions provided by the user, note this is
     * different from [assignFromSubscribed] whose input partitions are provided from the subscribed
     * topics.
     */
    @Synchronized
    fun assignFromUser(partitions: Set<TopicPartition>): Boolean {
        setSubscriptionType(SubscriptionType.USER_ASSIGNED)
        if (assignment.partitionSet() == partitions) return false
        assignmentId++

        // update the subscribed topics
        val manualSubscribedTopics = mutableSetOf<String>()
        val partitionToState = mutableMapOf<TopicPartition, TopicPartitionState>()
        for (partition in partitions) {
            var state = assignment.stateValue(partition)
            if (state == null) state = TopicPartitionState()
            partitionToState[partition] = state
            manualSubscribedTopics.add(partition.topic)
        }
        assignment.set(partitionToState)
        return changeSubscription(manualSubscribedTopics)
    }

    /**
     * @return true if assignments matches subscription, otherwise false
     */
    @Synchronized
    fun checkAssignmentMatchedSubscription(assignments: Collection<TopicPartition>): Boolean {
        for (topicPartition in assignments) {
            subscribedPattern?.let { subscribedPattern ->
                if (!subscribedPattern.matcher(topicPartition.topic).matches()) {
                    log.info(
                        "Assigned partition {} for non-subscribed topic regex pattern; " +
                                "subscription pattern is {}",
                        topicPartition,
                        subscribedPattern,
                    )
                    return false
                }
            } ?: run {
                if (!subscription.contains(topicPartition.topic)) {
                    log.info(
                        "Assigned partition {} for non-subscribed topic; subscription is {}",
                        topicPartition,
                        subscription,
                    )
                    return false
                }
            }
        }
        return true
    }

    /**
     * Change the assignment to the specified partitions returned from the coordinator, note this is
     * different from [assignFromUser] which directly set the assignment from user inputs.
     */
    @Synchronized
    fun assignFromSubscribed(assignments: Collection<TopicPartition>) {
        require(hasAutoAssignedPartitions()) {
            "Attempt to dynamically assign partitions while manual assignment in use"
        }
        val assignedPartitionStates: MutableMap<TopicPartition, TopicPartitionState> =
            HashMap(assignments.size)

        for (tp in assignments) {
            var state = assignment.stateValue(tp)
            if (state == null) state = TopicPartitionState()
            assignedPartitionStates[tp] = state
        }
        assignmentId++
        assignment.set(assignedPartitionStates)
    }

    private fun registerRebalanceListener(listener: ConsumerRebalanceListener) {
        rebalanceListener = listener
    }

    /**
     * Check whether pattern subscription is in use.
     */
    @Synchronized
    fun hasPatternSubscription(): Boolean = subscriptionType == SubscriptionType.AUTO_PATTERN

    @Synchronized
    fun hasNoSubscriptionOrUserAssignment(): Boolean = subscriptionType == SubscriptionType.NONE

    @Synchronized
    fun unsubscribe() {
        subscription = emptySet()
        groupSubscription = emptySet()
        assignment.clear()
        subscribedPattern = null
        subscriptionType = SubscriptionType.NONE
        assignmentId++
    }

    /**
     * Check whether a topic matches a subscribed pattern.
     *
     * @return true if pattern subscription is in use and the topic matches the subscribed pattern,
     * false otherwise
     */
    @Synchronized
    fun matchesSubscribedPattern(topic: String): Boolean {
        val pattern = subscribedPattern
        return if (hasPatternSubscription() && pattern != null) pattern.matcher(topic).matches()
        else false
    }

    @Synchronized
    fun subscription(): Set<String> = if (hasAutoAssignedPartitions()) subscription else emptySet()

    @Synchronized
    fun pausedPartitions(): Set<TopicPartition> = collectPartitions(TopicPartitionState::isPaused)

    /**
     * Get the subscription topics for which metadata is required. For the leader, this will include
     * the union of the subscriptions of all group members. For followers, it is just that member's
     * subscription. This is used when querying topic metadata to detect the metadata changes which
     * would require rebalancing. The leader fetches metadata for all topics in the group so that it
     * can do the partition assignment (which requires at least partition counts for all topics to
     * be assigned).
     *
     * @return The union of all subscribed topics in the group if this member is the leader of the
     * current generation; otherwise it returns the same set as [subscription]
     */
    @Synchronized
    fun metadataTopics(): Set<String> {
        return if (groupSubscription.isEmpty()) subscription
        else if (groupSubscription.containsAll(subscription)) groupSubscription
        else {
            // When subscription changes `groupSubscription` may be outdated, ensure that new
            // subscription topics are returned.
            val topics: MutableSet<String> = groupSubscription.toHashSet()
            topics.addAll(subscription)
            topics
        }
    }

    @Synchronized
    fun needsMetadata(topic: String): Boolean =
        subscription.contains(topic) || groupSubscription.contains(topic)

    private fun assignedState(tp: TopicPartition): TopicPartitionState =
        assignment.stateValue(tp) ?: error("No current assignment for partition $tp")

    private fun assignedStateOrNull(tp: TopicPartition): TopicPartitionState? =
        assignment.stateValue(tp)

    @Synchronized
    fun seekValidated(tp: TopicPartition, position: FetchPosition) =
        assignedState(tp).seekValidated(position)

    fun seek(tp: TopicPartition, offset: Long) {
        seekValidated(tp, FetchPosition(offset))
    }

    fun seekUnvalidated(tp: TopicPartition, position: FetchPosition) =
        assignedState(tp).seekUnvalidated(position)

    @Synchronized
    fun maybeSeekUnvalidated(
        tp: TopicPartition,
        position: FetchPosition,
        requestedResetStrategy: OffsetResetStrategy,
    ) {
        val state = assignedStateOrNull(tp)
        if (state == null)
            log.debug("Skipping reset of partition {} since it is no longer assigned", tp)
        else if (!state.awaitingReset())
            log.debug("Skipping reset of partition {} since reset is no longer needed", tp)
        else if (requestedResetStrategy !== state.resetStrategy) {
            log.debug(
                "Skipping reset of partition {} since an alternative reset has been requested",
                tp
            )
        } else {
            log.info("Resetting offset for partition {} to position {}.", tp, position)
            state.seekUnvalidated(position)
        }
    }

    /**
     * @return a modifiable copy of the currently assigned partitions
     */
    @Synchronized
    fun assignedPartitions(): Set<TopicPartition> {
        return assignment.partitionSet().toHashSet()
    }

    /**
     * @return a modifiable copy of the currently assigned partitions as a list
     */
    @Synchronized
    fun assignedPartitionsList(): List<TopicPartition> {
        return ArrayList(assignment.partitionSet())
    }

    /**
     * Provides the number of assigned partitions in a thread safe manner.
     * @return the number of assigned partitions.
     */
    @Synchronized
    fun numAssignedPartitions(): Int {
        return assignment.size()
    }

    // Visible for testing
    @Synchronized
    fun fetchablePartitions(isAvailable: Predicate<TopicPartition?>): List<TopicPartition> {
        // Since this is in the hot-path for fetching, we do this instead of using java.util.stream API
        val result: MutableList<TopicPartition> = ArrayList()
        assignment.forEach { topicPartition, topicPartitionState ->
            // Cheap check is first to avoid evaluating the predicate if possible
            if (topicPartitionState.isFetchable && isAvailable.test(topicPartition))
                result.add(topicPartition)
        }
        return result
    }

    @Synchronized
    fun hasAutoAssignedPartitions(): Boolean =
        subscriptionType == SubscriptionType.AUTO_TOPICS
                || subscriptionType == SubscriptionType.AUTO_PATTERN

    @Synchronized
    fun position(tp: TopicPartition, position: FetchPosition) = assignedState(tp).position(position)

    /**
     * Enter the offset validation state if the leader for this partition is known to support a usable version of the
     * OffsetsForLeaderEpoch API. If the leader node does not support the API, simply complete the offset validation.
     *
     * @param apiVersions supported API versions
     * @param tp topic partition to validate
     * @param leaderAndEpoch leader epoch of the topic partition
     * @return true if we enter the offset validation state
     */
    @Synchronized
    fun maybeValidatePositionForCurrentLeader(
        apiVersions: ApiVersions,
        tp: TopicPartition,
        leaderAndEpoch: LeaderAndEpoch
    ): Boolean {
        return if (leaderAndEpoch.leader != null) {
            val nodeApiVersions = apiVersions[leaderAndEpoch.leader.idString()]
            if (
                nodeApiVersions == null
                || Fetcher.hasUsableOffsetForLeaderEpochVersion(nodeApiVersions)
            ) assignedState(tp).maybeValidatePosition(leaderAndEpoch)
            else {
                // If the broker does not support a newer version of OffsetsForLeaderEpoch, we skip
                // validation
                assignedState(tp).updatePositionLeaderNoValidation(leaderAndEpoch)
                false
            }
        } else assignedState(tp).maybeValidatePosition(leaderAndEpoch)
    }

    /**
     * Attempt to complete validation with the end offset returned from the OffsetForLeaderEpoch
     * request.
     *
     * @return Log truncation details if detected and no reset policy is defined.
     */
    @Synchronized
    fun maybeCompleteValidation(
        tp: TopicPartition,
        requestPosition: FetchPosition,
        epochEndOffset: OffsetForLeaderEpochResponseData.EpochEndOffset,
    ): LogTruncation? {
        val state = assignedStateOrNull(tp)
        if (state == null) log.debug(
            "Skipping completed validation for partition {} which is not currently assigned.",
            tp
        )
        else if (!state.awaitingValidation()) log.debug(
            "Skipping completed validation for partition {} which is no longer expecting validation.",
            tp
        ) else {
            val currentPosition = state.position
            if (currentPosition != requestPosition) log.debug(
                "Skipping completed validation for partition {} since the current position {} " +
                        "no longer matches the position {} when the request was sent",
                tp,
                currentPosition,
                requestPosition,
            ) else if (
                epochEndOffset.endOffset == OffsetsForLeaderEpochResponse.UNDEFINED_EPOCH_OFFSET
                || epochEndOffset.leaderEpoch == OffsetsForLeaderEpochResponse.UNDEFINED_EPOCH
            ) {
                if (hasDefaultOffsetResetPolicy()) {
                    log.info(
                        "Truncation detected for partition {} at offset {}, resetting offset",
                        tp,
                        currentPosition,
                    )
                    requestOffsetReset(tp)
                } else {
                    log.warn(
                        "Truncation detected for partition {} at offset {}, but no reset policy is set",
                        tp,
                        currentPosition,
                    )
                    return LogTruncation(
                        topicPartition = tp,
                        fetchPosition = requestPosition,
                    )
                }
            } else if (epochEndOffset.endOffset < currentPosition.offset) {
                if (hasDefaultOffsetResetPolicy()) {
                    val newPosition = FetchPosition(
                        offset = epochEndOffset.endOffset,
                        offsetEpoch = epochEndOffset.leaderEpoch,
                        currentLeader = currentPosition.currentLeader,
                    )
                    log.info(
                        "Truncation detected for partition {} at offset {}, resetting offset to " +
                                "the first offset known to diverge {}",
                        tp,
                        currentPosition,
                        newPosition,
                    )
                    state.seekValidated(newPosition)
                } else {
                    val divergentOffset = OffsetAndMetadata(
                        offset = epochEndOffset.endOffset,
                        leaderEpoch = epochEndOffset.leaderEpoch,
                    )
                    log.warn(
                        "Truncation detected for partition {} at offset {} (the end offset from " +
                                "the broker is {}), but no reset policy is set",
                        tp,
                        currentPosition,
                        divergentOffset,
                    )
                    return LogTruncation(
                        topicPartition = tp,
                        fetchPosition = requestPosition,
                        divergentOffset = divergentOffset,
                    )
                }
            } else state.completeValidation()
        }
        return null
    }

    @Synchronized
    fun awaitingValidation(tp: TopicPartition): Boolean = assignedState(tp).awaitingValidation()

    @Synchronized
    fun completeValidation(tp: TopicPartition) = assignedState(tp).completeValidation()

    @Synchronized
    fun validPosition(tp: TopicPartition): FetchPosition? = assignedState(tp).validPosition()

    @Synchronized
    fun position(tp: TopicPartition): FetchPosition? = assignedState(tp).position

    @Synchronized
    fun partitionLag(tp: TopicPartition, isolationLevel: IsolationLevel): Long? {
        val topicPartitionState = assignedState(tp)
        return if (topicPartitionState.position == null) null
        else if (isolationLevel === IsolationLevel.READ_COMMITTED)
            topicPartitionState.lastStableOffset?.let { it - topicPartitionState.position!!.offset }
        else topicPartitionState.highWatermark?.let { it - topicPartitionState.position!!.offset }
    }

    @Synchronized
    fun partitionEndOffset(tp: TopicPartition, isolationLevel: IsolationLevel): Long? {
        val topicPartitionState = assignedState(tp)

        return if (isolationLevel === IsolationLevel.READ_COMMITTED)
            topicPartitionState.lastStableOffset
        else topicPartitionState.highWatermark
    }

    @Synchronized
    fun requestPartitionEndOffset(tp: TopicPartition) {
        val topicPartitionState = assignedState(tp)
        topicPartitionState.requestEndOffset()
    }

    @Synchronized
    fun partitionEndOffsetRequested(tp: TopicPartition): Boolean {
        val topicPartitionState = assignedState(tp)
        return topicPartitionState.endOffsetRequested
    }

    @Synchronized
    fun partitionLead(tp: TopicPartition): Long? {
        val topicPartitionState = assignedState(tp)
        return topicPartitionState.logStartOffset?.let { logStartOffset ->
            topicPartitionState.position!!.offset - logStartOffset
        }
    }

    @Synchronized
    fun updateHighWatermark(tp: TopicPartition, highWatermark: Long) {
        assignedState(tp).highWatermark(highWatermark)
    }

    @Synchronized
    fun updateLogStartOffset(tp: TopicPartition, logStartOffset: Long) {
        assignedState(tp).logStartOffset(logStartOffset)
    }

    @Synchronized
    fun updateLastStableOffset(tp: TopicPartition, lastStableOffset: Long) {
        assignedState(tp).lastStableOffset(lastStableOffset)
    }

    /**
     * Set the preferred read replica with a lease timeout. After this time, the replica will no
     * longer be valid and [preferredReadReplica] will return an empty result.
     *
     * @param tp The topic partition
     * @param preferredReadReplicaId The preferred read replica
     * @param timeMs The time at which this preferred replica is no longer valid
     */
    @Synchronized
    fun updatePreferredReadReplica(
        tp: TopicPartition,
        preferredReadReplicaId: Int,
        timeMs: LongSupplier,
    ) = assignedState(tp).updatePreferredReadReplica(
        preferredReadReplica = preferredReadReplicaId,
        timeMs = timeMs,
    )

    /**
     * Get the preferred read replica
     *
     * @param tp The topic partition
     * @param timeMs The current time
     * @return Returns the current preferred read replica, if it has been set and if it has not
     * expired.
     */
    @Synchronized
    fun preferredReadReplica(tp: TopicPartition, timeMs: Long): Int? {
        val topicPartitionState = assignedStateOrNull(tp)
        return topicPartitionState?.preferredReadReplica(timeMs)
    }

    /**
     * Unset the preferred read replica. This causes the fetcher to go back to the leader for fetches.
     *
     * @param tp The topic partition
     * @return the removed preferred read replica if set, None otherwise.
     */
    @Synchronized
    fun clearPreferredReadReplica(tp: TopicPartition): Int? {
        val topicPartitionState = assignedStateOrNull(tp)
        return topicPartitionState?.clearPreferredReadReplica()
    }

    @Synchronized
    fun allConsumed(): Map<TopicPartition, OffsetAndMetadata> {
        val allConsumed: MutableMap<TopicPartition, OffsetAndMetadata> = HashMap()
        assignment.forEach { topicPartition: TopicPartition, partitionState: TopicPartitionState ->
            if (partitionState.hasValidPosition()) allConsumed[topicPartition] = OffsetAndMetadata(
                partitionState.position!!.offset,
                partitionState.position!!.offsetEpoch, ""
            )
        }
        return allConsumed
    }

    @Synchronized
    fun requestOffsetReset(partition: TopicPartition, offsetResetStrategy: OffsetResetStrategy) =
        assignedState(partition).reset(offsetResetStrategy)

    @Synchronized
    fun requestOffsetReset(
        partitions: Collection<TopicPartition>,
        offsetResetStrategy: OffsetResetStrategy
    ) {
        partitions.forEach(Consumer { tp ->
            log.info("Seeking to {} offset of partition {}", offsetResetStrategy, tp)
            assignedState(tp).reset(offsetResetStrategy)
        })
    }

    fun requestOffsetReset(partition: TopicPartition) =
        requestOffsetReset(partition, defaultResetStrategy)

    @Synchronized
    fun setNextAllowedRetry(partitions: Set<TopicPartition>, nextAllowResetTimeMs: Long) {
        for (partition in partitions)
            assignedState(partition).setNextAllowedRetry(nextAllowResetTimeMs)
    }

    fun hasDefaultOffsetResetPolicy(): Boolean = defaultResetStrategy !== OffsetResetStrategy.NONE

    @Synchronized
    fun isOffsetResetNeeded(partition: TopicPartition): Boolean =
        assignedState(partition).awaitingReset()

    @Synchronized
    fun resetStrategy(partition: TopicPartition): OffsetResetStrategy? =
        assignedState(partition).resetStrategy

    @Synchronized
    fun hasAllFetchPositions(): Boolean {
        // Since this is in the hot-path for fetching, we do this instead of using java.util.stream API
        val it = assignment.stateIterator()
        while (it.hasNext()) {
            if (!it.next()!!.hasValidPosition()) {
                return false
            }
        }
        return true
    }

    @Synchronized
    fun initializingPartitions(): Set<TopicPartition> {
        return collectPartitions { state: TopicPartitionState -> state.fetchState == FetchStates.INITIALIZING }
    }

    private fun collectPartitions(filter: (TopicPartitionState) -> Boolean): Set<TopicPartition> {
        val result = mutableSetOf<TopicPartition>()

        assignment.forEach { topicPartition, topicPartitionState ->
            if (filter(topicPartitionState)) result.add(topicPartition)
        }
        return result
    }

    @Synchronized
    fun resetInitializingPositions() {
        val partitionsWithNoOffsets = mutableSetOf<TopicPartition>()

        assignment.forEach { tp, partitionState ->
            if (partitionState.fetchState == FetchStates.INITIALIZING) {
                if (defaultResetStrategy === OffsetResetStrategy.NONE)
                    partitionsWithNoOffsets.add(tp)
                else requestOffsetReset(tp)
            }
        }
        if (partitionsWithNoOffsets.isNotEmpty())
            throw NoOffsetForPartitionException(partitionsWithNoOffsets)
    }

    @Synchronized
    fun partitionsNeedingReset(nowMs: Long): Set<TopicPartition> {
        return collectPartitions { state ->
            state.awaitingReset() && !state.awaitingRetryBackoff(nowMs)
        }
    }

    @Synchronized
    fun partitionsNeedingValidation(nowMs: Long): Set<TopicPartition> {
        return collectPartitions { state ->
            state.awaitingValidation() && !state.awaitingRetryBackoff(nowMs)
        }
    }

    @Synchronized
    fun isAssigned(tp: TopicPartition): Boolean = assignment.contains(tp)

    @Synchronized
    fun isPaused(tp: TopicPartition): Boolean {
        val assignedOrNull = assignedStateOrNull(tp)
        return assignedOrNull != null && assignedOrNull.isPaused
    }

    @Synchronized
    fun isFetchable(tp: TopicPartition): Boolean {
        val assignedOrNull = assignedStateOrNull(tp)
        return assignedOrNull != null && assignedOrNull.isFetchable
    }

    @Synchronized
    fun hasValidPosition(tp: TopicPartition): Boolean {
        val assignedOrNull = assignedStateOrNull(tp)
        return assignedOrNull != null && assignedOrNull.hasValidPosition()
    }

    @Synchronized
    fun pause(tp: TopicPartition) = assignedState(tp).pause()

    @Synchronized
    fun markPendingRevocation(tps: Set<TopicPartition>) {
        tps.forEach { tp -> assignedState(tp).markPendingRevocation() }
    }

    @Synchronized
    fun resume(tp: TopicPartition) = assignedState(tp).resume()

    @Synchronized
    fun requestFailed(partitions: Set<TopicPartition>, nextRetryTimeMs: Long) {
        for (partition in partitions) {
            // by the time the request failed, the assignment may no longer
            // contain this partition any more, in which case we would just ignore.
            val state = assignedStateOrNull(partition)
            state?.requestFailed(nextRetryTimeMs)
        }
    }

    @Synchronized
    fun movePartitionToEnd(tp: TopicPartition) = assignment.moveToEnd(tp)

    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("rebalanceListener"),
    )
    @Synchronized
    fun rebalanceListener(): ConsumerRebalanceListener? = rebalanceListener

    private class TopicPartitionState {

        var fetchState: FetchState = FetchStates.INITIALIZING
            private set

        // last consumed position
        var position: FetchPosition? = null
            private set

        // the high watermark from last fetch
        var highWatermark: Long? = null
            private set

        // the log start offset
        var logStartOffset: Long? = null
            private set

        var lastStableOffset: Long? = null
            private set

        // whether this partition has been paused by the user
        var isPaused = false
            private set

        private var pendingRevocation = false

        // the strategy to use if the offset needs resetting
        var resetStrategy: OffsetResetStrategy? = null
            private set

        private var nextRetryTimeMs: Long? = null

        private var preferredReadReplica: Int? = null

        private var preferredReadReplicaExpireTimeMs: Long? = null

        var endOffsetRequested = false
            private set

        @Deprecated(
            message = "User property instead",
            replaceWith = ReplaceWith("endOffsetRequested"),
        )
        fun endOffsetRequested(): Boolean = endOffsetRequested

        fun requestEndOffset() {
            endOffsetRequested = true
        }

        private fun transitionState(newState: FetchState, runIfTransitioned: Runnable) {
            val nextState = fetchState.transitionTo(newState)
            if (nextState == newState) {
                fetchState = nextState
                runIfTransitioned.run()
                check(position != null || !nextState.requiresPosition()) {
                    "Transitioned subscription state to $nextState, but position is null"
                }
                if (!nextState.requiresPosition()) position = null
            }
        }

        fun preferredReadReplica(timeMs: Long): Int? {
            return if (
                preferredReadReplicaExpireTimeMs != null
                && timeMs > preferredReadReplicaExpireTimeMs!!
            ) {
                preferredReadReplica = null
                null
            } else preferredReadReplica
        }

        fun updatePreferredReadReplica(preferredReadReplica: Int, timeMs: LongSupplier) {
            if (
                this.preferredReadReplica == null
                || preferredReadReplica != this.preferredReadReplica
            ) {
                this.preferredReadReplica = preferredReadReplica
                preferredReadReplicaExpireTimeMs = timeMs.asLong
            }
        }

        fun clearPreferredReadReplica(): Int? {
            return if (preferredReadReplica != null) {
                val removedReplicaId = preferredReadReplica
                preferredReadReplica = null
                preferredReadReplicaExpireTimeMs = null
                removedReplicaId
            } else null
        }

        fun reset(strategy: OffsetResetStrategy) {
            transitionState(FetchStates.AWAIT_RESET) {
                resetStrategy = strategy
                nextRetryTimeMs = null
            }
        }

        /**
         * Check if the position exists and needs to be validated. If so, enter the AWAIT_VALIDATION
         * state. This method also will update the position with the current leader and epoch.
         *
         * @param currentLeaderAndEpoch leader and epoch to compare the offset with
         * @return true if the position is now awaiting validation
         */
        fun maybeValidatePosition(currentLeaderAndEpoch: LeaderAndEpoch): Boolean {
            if (fetchState == FetchStates.AWAIT_RESET) return false

            if (currentLeaderAndEpoch.leader == null) return false

            val position = position
            if (position != null && position.currentLeader != currentLeaderAndEpoch) {
                val newPosition = FetchPosition(
                    offset = position.offset,
                    offsetEpoch = position.offsetEpoch,
                    currentLeader = currentLeaderAndEpoch,
                )
                validatePosition(newPosition)
                preferredReadReplica = null
            }
            return fetchState == FetchStates.AWAIT_VALIDATION
        }

        /**
         * For older versions of the API, we cannot perform offset validation so we simply
         * transition directly to FETCHING
         */
        fun updatePositionLeaderNoValidation(currentLeaderAndEpoch: LeaderAndEpoch) {
            val position = position ?: return
            transitionState(FetchStates.FETCHING) {
                this.position = FetchPosition(
                    offset = position.offset,
                    offsetEpoch = position.offsetEpoch,
                    currentLeader = currentLeaderAndEpoch,
                )
                nextRetryTimeMs = null
            }
        }

        private fun validatePosition(position: FetchPosition) {
            if (position.offsetEpoch != null && position.currentLeader.epoch != null) {
                transitionState(FetchStates.AWAIT_VALIDATION) {
                    this.position = position
                    nextRetryTimeMs = null
                }
            } else {
                // If we have no epoch information for the current position, then we can skip
                // validation
                transitionState(FetchStates.FETCHING) {
                    this.position = position
                    nextRetryTimeMs = null
                }
            }
        }

        /**
         * Clear the awaiting validation state and enter fetching.
         */
        fun completeValidation() {
            if (hasPosition()) transitionState(FetchStates.FETCHING) { nextRetryTimeMs = null }
        }

        fun awaitingValidation(): Boolean = fetchState == FetchStates.AWAIT_VALIDATION

        fun awaitingRetryBackoff(nowMs: Long): Boolean =
            nextRetryTimeMs?.let { nowMs < it } == true

        fun awaitingReset(): Boolean = fetchState == FetchStates.AWAIT_RESET

        fun setNextAllowedRetry(nextAllowedRetryTimeMs: Long) {
            nextRetryTimeMs = nextAllowedRetryTimeMs
        }

        fun requestFailed(nextAllowedRetryTimeMs: Long) {
            nextRetryTimeMs = nextAllowedRetryTimeMs
        }

        fun hasValidPosition(): Boolean = fetchState.hasValidPosition()

        fun hasPosition(): Boolean = position != null

        fun seekValidated(position: FetchPosition) {
            transitionState(FetchStates.FETCHING) {
                this.position = position
                resetStrategy = null
                nextRetryTimeMs = null
            }
        }

        fun seekUnvalidated(fetchPosition: FetchPosition) {
            seekValidated(fetchPosition)
            validatePosition(fetchPosition)
        }

        fun position(position: FetchPosition) {
            check(hasValidPosition()) {
                "Cannot set a new position without a valid current position"
            }
            this.position = position
        }

        fun validPosition(): FetchPosition? = if (hasValidPosition()) position else null

        fun pause() {
            isPaused = true
        }

        fun markPendingRevocation() {
            pendingRevocation = true
        }

        fun resume() {
            isPaused = false
        }

        val isFetchable: Boolean
            get() = !isPaused && !pendingRevocation && hasValidPosition()

        fun highWatermark(highWatermark: Long) {
            this.highWatermark = highWatermark
            endOffsetRequested = false
        }

        fun logStartOffset(logStartOffset: Long) {
            this.logStartOffset = logStartOffset
        }

        fun lastStableOffset(lastStableOffset: Long) {
            this.lastStableOffset = lastStableOffset
            endOffsetRequested = false
        }

        @Deprecated(
            message = "User property instead",
            replaceWith = ReplaceWith("resetStrategy"),
        )
        fun resetStrategy(): OffsetResetStrategy? = resetStrategy
    }

    /**
     * The fetch state of a partition. This class is used to determine valid state transitions and
     * expose the some of the behavior of the current fetch state. Actual state variables are stored
     * in the [TopicPartitionState].
     */
    internal interface FetchState {

        fun transitionTo(newState: FetchState): FetchState {
            return if (validTransitions().contains(newState)) newState
            else this
        }

        /**
         * Return the valid states which this state can transition to
         */
        fun validTransitions(): Collection<FetchState>

        /**
         * Test if this state requires a position to be set
         */
        fun requiresPosition(): Boolean

        /**
         * Test if this state is considered to have a valid position which can be used for fetching
         */
        fun hasValidPosition(): Boolean
    }

    /**
     * An enumeration of all the possible fetch states. The state transitions are encoded in the
     * values returned by [FetchState.validTransitions].
     */
    internal enum class FetchStates : FetchState {

        INITIALIZING {
            override fun validTransitions(): Collection<FetchState> = listOf(
                FETCHING,
                AWAIT_RESET,
                AWAIT_VALIDATION,
            )

            override fun requiresPosition(): Boolean = false

            override fun hasValidPosition(): Boolean = false
        },

        FETCHING {
            override fun validTransitions(): Collection<FetchState> = listOf(
                FETCHING,
                AWAIT_RESET,
                AWAIT_VALIDATION,
            )

            override fun requiresPosition(): Boolean = true

            override fun hasValidPosition(): Boolean = true
        },

        AWAIT_RESET {
            override fun validTransitions(): Collection<FetchState> = listOf(
                FETCHING,
                AWAIT_RESET,
            )

            override fun requiresPosition(): Boolean = false

            override fun hasValidPosition(): Boolean = false
        },

        AWAIT_VALIDATION {
            override fun validTransitions(): Collection<FetchState> = listOf(
                FETCHING,
                AWAIT_RESET,
                AWAIT_VALIDATION,
            )

            override fun requiresPosition(): Boolean = true

            override fun hasValidPosition(): Boolean = false
        }
    }

    /**
     * Represents the position of a partition subscription.
     *
     * This includes the offset and epoch from the last record in the batch from a FetchResponse. It
     * also includes the leader epoch at the time the batch was consumed.
     */
    data class FetchPosition(
        val offset: Long,
        val offsetEpoch: Int?,
        val currentLeader: LeaderAndEpoch,
    ) {

        internal constructor(offset: Long) : this(
            offset = offset,
            offsetEpoch = null,
            currentLeader = LeaderAndEpoch.noLeaderOrEpoch(),
        )

        override fun toString(): String =
            "FetchPosition{offset=$offset, offsetEpoch=$offsetEpoch, currentLeader=$currentLeader}"
    }

    data class LogTruncation(
        val topicPartition: TopicPartition,
        val fetchPosition: FetchPosition,
        val divergentOffset: OffsetAndMetadata? = null,
    ) {
        override fun toString(): String {
            val bldr = StringBuilder()
                .append("(partition=")
                .append(topicPartition)
                .append(", fetchOffset=")
                .append(fetchPosition.offset)
                .append(", fetchEpoch=")
                .append(fetchPosition.offsetEpoch)
            if (divergentOffset != null) {
                bldr.append(", divergentOffset=")
                    .append(divergentOffset.offset)
                    .append(", divergentEpoch=")
                    .append(divergentOffset.leaderEpoch())
            } else {
                bldr.append(", divergentOffset=unknown")
                    .append(", divergentEpoch=unknown")
            }
            return bldr.append(")").toString()
        }
    }

    private enum class SubscriptionType {
        NONE,
        AUTO_TOPICS,
        AUTO_PATTERN,
        USER_ASSIGNED
    }

    companion object {
        private const val SUBSCRIPTION_EXCEPTION_MESSAGE =
            "Subscription to topics, partitions and pattern are mutually exclusive"
    }
}
