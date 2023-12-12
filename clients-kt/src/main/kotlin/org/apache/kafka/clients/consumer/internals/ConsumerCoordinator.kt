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

import java.nio.ByteBuffer
import java.util.SortedSet
import java.util.TreeSet
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference
import org.apache.kafka.clients.GroupRebalanceConfig
import org.apache.kafka.clients.consumer.CommitFailedException
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.GroupSubscription
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.RebalanceProtocol
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.clients.consumer.OffsetCommitCallback
import org.apache.kafka.clients.consumer.RetriableCommitFailedException
import org.apache.kafka.clients.consumer.internals.SubscriptionState.FetchPosition
import org.apache.kafka.clients.consumer.internals.Utils.TopicPartitionComparator
import org.apache.kafka.common.Cluster
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.PartitionInfo
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.FencedInstanceIdException
import org.apache.kafka.common.errors.GroupAuthorizationException
import org.apache.kafka.common.errors.InterruptException
import org.apache.kafka.common.errors.RebalanceInProgressException
import org.apache.kafka.common.errors.RetriableException
import org.apache.kafka.common.errors.TimeoutException
import org.apache.kafka.common.errors.TopicAuthorizationException
import org.apache.kafka.common.errors.UnstableOffsetCommitException
import org.apache.kafka.common.errors.WakeupException
import org.apache.kafka.common.message.JoinGroupRequestData.JoinGroupRequestProtocol
import org.apache.kafka.common.message.JoinGroupRequestData.JoinGroupRequestProtocolCollection
import org.apache.kafka.common.message.JoinGroupResponseData.JoinGroupResponseMember
import org.apache.kafka.common.message.OffsetCommitRequestData
import org.apache.kafka.common.message.OffsetCommitRequestData.OffsetCommitRequestPartition
import org.apache.kafka.common.message.OffsetCommitRequestData.OffsetCommitRequestTopic
import org.apache.kafka.common.metrics.Measurable
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.metrics.Sensor
import org.apache.kafka.common.metrics.stats.Avg
import org.apache.kafka.common.metrics.stats.Max
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.record.RecordBatch
import org.apache.kafka.common.requests.JoinGroupRequest
import org.apache.kafka.common.requests.OffsetCommitRequest
import org.apache.kafka.common.requests.OffsetCommitResponse
import org.apache.kafka.common.requests.OffsetFetchRequest
import org.apache.kafka.common.requests.OffsetFetchResponse
import org.apache.kafka.common.utils.LogContext
import org.apache.kafka.common.utils.Time
import org.apache.kafka.common.utils.Timer
import org.apache.kafka.common.utils.Utils
import org.slf4j.Logger
import kotlin.math.min

/**
 * This class manages the coordination process with the consumer coordinator.
 */
class ConsumerCoordinator(
    private val rebalanceConfig: GroupRebalanceConfig,
    logContext: LogContext,
    client: ConsumerNetworkClient?,
    private val assignors: List<ConsumerPartitionAssignor>,
    private val metadata: ConsumerMetadata,
    private val subscriptions: SubscriptionState,
    metrics: Metrics,
    metricGrpPrefix: String,
    time: Time,
    private val autoCommitEnabled: Boolean,
    private val autoCommitIntervalMs: Int,
    private val interceptors: ConsumerInterceptors<*, *>?,
    private val throwOnFetchStableOffsetsUnsupported: Boolean,
    rackId: String?,
) : AbstractCoordinator(
    rebalanceConfig = rebalanceConfig,
    logContext = logContext,
    client = client!!,
    metrics = metrics,
    metricGrpPrefix = metricGrpPrefix,
    time = time,
) {

    private val log: Logger = logContext.logger(ConsumerCoordinator::class.java)

    private val sensors = ConsumerCoordinatorMetrics(metrics, metricGrpPrefix)

    private val defaultOffsetCommitCallback: OffsetCommitCallback = DefaultOffsetCommitCallback()

    // track number of async commits for which callback must be called
    // package private for testing
    internal val inFlightAsyncCommits: AtomicInteger = AtomicInteger()

    // track the number of pending async commits waiting on the coordinator lookup to complete
    private val pendingAsyncCommits = AtomicInteger()

    // this collection must be thread-safe because it is modified from the response handler of
    // offset commit requests, which may be invoked from the heartbeat thread
    private val completedOffsetCommits = ConcurrentLinkedQueue<OffsetCommitCompletion>()

    // package private for testing
    internal var isLeader = false
        private set

    private lateinit var joinedSubscription: Set<String>

    private var metadataSnapshot: MetadataSnapshot

    private var assignmentSnapshot: MetadataSnapshot? = null

    private var nextAutoCommitTimer: Timer? = null

    private val asyncCommitFenced = AtomicBoolean(false)

    private var groupMetadata: ConsumerGroupMetadata = ConsumerGroupMetadata(
        groupId = rebalanceConfig.groupId!!,
        generationId = JoinGroupRequest.UNKNOWN_GENERATION_ID,
        memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID,
        groupInstanceId = rebalanceConfig.groupInstanceId,
    )

    private val rackId: String?

    // hold onto request&future for committed offset requests to enable async calls.
    private var pendingCommittedOffsetRequest: PendingCommittedOffsetRequest? = null

    /* test-only classes below */
    var protocol: RebalanceProtocol? = null

    // pending commit offset request in onJoinPrepare
    private var autoCommitOffsetRequestFuture: RequestFuture<Unit>? = null

    // a timer for join prepare to know when to stop.
    // it'll set to rebalance timeout so that the member can join the group successfully
    // even though offset commit failed.
    private var joinPrepareTimer: Timer? = null

    // package private for testing
    fun subscriptionState(): SubscriptionState = subscriptions

    public override fun protocolType(): String = ConsumerProtocol.PROTOCOL_TYPE

    /**
     * Initialize the coordination manager.
     */
    init {
        this.rackId = if (rackId.isNullOrEmpty()) null else rackId
        metadataSnapshot = MetadataSnapshot(
            clientRack = this.rackId,
            subscription = subscriptions,
            cluster = metadata.fetch(),
            version = metadata.updateVersion(),
        )
        if (autoCommitEnabled) nextAutoCommitTimer = time.timer(autoCommitIntervalMs.toLong())

        // Select the rebalance protocol such that:
        // 1. only consider protocols that are supported by all the assignors. If there is no common
        //    protocols supported across all the assignors, throw an exception.
        // 2. if there are multiple protocols that are commonly supported, select the one with the
        //    highest id (i.e. the id number indicates how advanced the protocol is).
        // We know there are at least one assignor in the list, no need to double check for NPE
        protocol = if (assignors.isNotEmpty()) {
            val supportedProtocols: MutableList<RebalanceProtocol> =
                assignors[0].supportedProtocols().toMutableList()

            for (assignor in assignors)
                supportedProtocols.retainAll(assignor.supportedProtocols().toSet())

            require(supportedProtocols.isNotEmpty()) {
                "Specified assignors ${assignors.map(ConsumerPartitionAssignor::name)} do not " +
                        "have commonly supported rebalance protocol"
            }
            supportedProtocols.sort()
            supportedProtocols[supportedProtocols.size - 1]
        } else null

        metadata.requestUpdate()
    }

    override fun metadata(): JoinGroupRequestProtocolCollection {
        log.debug("Joining group with current subscription: {}", subscriptions.subscription())
        joinedSubscription = subscriptions.subscription()
        val protocolSet = JoinGroupRequestProtocolCollection()
        val topics = joinedSubscription.toList()
        for (assignor in assignors) {
            val subscription = ConsumerPartitionAssignor.Subscription(
                topics = topics,
                userData = assignor.subscriptionUserData(joinedSubscription),
                ownedPartitions = subscriptions.assignedPartitionsList(),
                generationId = generation().generationId,
                rackId = rackId,
            )
            val metadata = ConsumerProtocol.serializeSubscription(subscription)
            protocolSet.add(
                JoinGroupRequestProtocol()
                    .setName(assignor.name())
                    .setMetadata(Utils.toArray(metadata))
            )
        }
        return protocolSet
    }

    fun updatePatternSubscription(cluster: Cluster) {
        val topicsToSubscribe = cluster.topics
            .filter { topic -> subscriptions.matchesSubscribedPattern(topic) }
            .toSet()
        if (subscriptions.subscribeFromPattern(topicsToSubscribe))
            metadata.requestUpdateForNewTopics()
    }

    private fun lookupAssignor(name: String?): ConsumerPartitionAssignor? =
        assignors.firstOrNull { it.name() == name }

    private fun maybeUpdateJoinedSubscription(assignedPartitions: Collection<TopicPartition>) {
        if (subscriptions.hasPatternSubscription()) {
            // Check if the assignment contains some topics that were not in the original
            // subscription, if yes we will obey what leader has decided and add these topics
            // into the subscriptions as long as they still match the subscribed pattern
            val addedTopics = assignedPartitions.filterNot { joinedSubscription.contains(it.topic) }
                .map { it.topic } // this is a copy because it's handed to listener below

            if (addedTopics.isNotEmpty()) {
                val newSubscription = subscriptions.subscription().toMutableSet()
                val newJoinedSubscription = joinedSubscription.toMutableSet()
                newSubscription.addAll(addedTopics)
                newJoinedSubscription.addAll(addedTopics)

                if (subscriptions.subscribeFromPattern(newSubscription))
                    metadata.requestUpdateForNewTopics()

                joinedSubscription = newJoinedSubscription
            }
        }
    }

    private fun invokeOnAssignment(
        assignor: ConsumerPartitionAssignor,
        assignment: ConsumerPartitionAssignor.Assignment,
    ): Exception? {
        log.info("Notifying assignor about the new {}", assignment)
        try {
            assignor.onAssignment(assignment, groupMetadata)
        } catch (e: Exception) {
            return e
        }
        return null
    }

    private fun invokePartitionsAssigned(assignedPartitions: SortedSet<TopicPartition>): Exception? {
        log.info("Adding newly assigned partitions: {}", assignedPartitions.joinToString(", "))
        val listener = subscriptions.rebalanceListener!!
        try {
            val startMs = time.milliseconds()
            listener.onPartitionsAssigned(assignedPartitions)
            sensors.assignCallbackSensor.record((time.milliseconds() - startMs).toDouble())
        } catch (e: WakeupException) {
            throw e
        } catch (e: InterruptException) {
            throw e
        } catch (e: Exception) {
            log.error(
                "User provided listener {} failed on invocation of onPartitionsAssigned for " +
                        "partitions {}",
                listener.javaClass.name,
                assignedPartitions,
                e,
            )
            return e
        }
        return null
    }

    private fun invokePartitionsRevoked(revokedPartitions: SortedSet<TopicPartition>): Exception? {
        log.info(
            "Revoke previously assigned partitions {}",
            revokedPartitions.joinToString(", "),
        )
        val revokePausedPartitions = subscriptions.pausedPartitions().toMutableSet()
        revokePausedPartitions.retainAll(revokedPartitions)
        if (revokePausedPartitions.isNotEmpty()) log.info(
            "The pause flag in partitions [{}] will be removed due to revocation.",
            revokePausedPartitions.joinToString(", "),
        )
        val listener = subscriptions.rebalanceListener!!
        try {
            val startMs = time.milliseconds()
            listener.onPartitionsRevoked(revokedPartitions)
            sensors.revokeCallbackSensor.record((time.milliseconds() - startMs).toDouble())
        } catch (e: WakeupException) {
            throw e
        } catch (e: InterruptException) {
            throw e
        } catch (e: Exception) {
            log.error(
                "User provided listener {} failed on invocation of onPartitionsRevoked for " +
                        "partitions {}",
                listener.javaClass.name, revokedPartitions, e
            )
            return e
        }
        return null
    }

    private fun invokePartitionsLost(lostPartitions: SortedSet<TopicPartition>): Exception? {
        log.info("Lost previously assigned partitions {}", lostPartitions.joinToString(", "))
        val lostPausedPartitions = subscriptions.pausedPartitions().toMutableSet()
        lostPausedPartitions.retainAll(lostPartitions)
        if (lostPausedPartitions.isNotEmpty()) log.info(
            "The pause flag in partitions [{}] will be removed due to partition lost.",
            lostPausedPartitions.joinToString(", "),
        )
        val listener = subscriptions.rebalanceListener!!
        try {
            val startMs = time.milliseconds()
            listener.onPartitionsLost(lostPartitions)
            sensors.loseCallbackSensor.record((time.milliseconds() - startMs).toDouble())
        } catch (e: WakeupException) {
            throw e
        } catch (e: InterruptException) {
            throw e
        } catch (e: Exception) {
            log.error(
                "User provided listener {} failed on invocation of onPartitionsLost for " +
                        "partitions {}",
                listener.javaClass.name,
                lostPartitions,
                e,
            )
            return e
        }
        return null
    }

    public override fun onJoinComplete(
        generation: Int,
        memberId: String,
        protocol: String?,
        memberAssignment: ByteBuffer,
    ) {
        log.debug(
            "Executing onJoinComplete with generation {} and memberId {}",
            generation,
            memberId,
        )

        // Only the leader is responsible for monitoring for metadata changes (i.e. partition changes)
        if (!isLeader) assignmentSnapshot = null
        val assignor = checkNotNull(lookupAssignor(protocol)) {
            "Coordinator selected invalid assignment protocol: $protocol"
        }

        // Give the assignor a chance to update internal state based on the received assignment
        groupMetadata = ConsumerGroupMetadata(
            groupId = rebalanceConfig.groupId!!,
            generationId = generation,
            memberId = memberId,
            groupInstanceId = rebalanceConfig.groupInstanceId,
        )
        val ownedPartitions: SortedSet<TopicPartition> = TreeSet(COMPARATOR)

        ownedPartitions.addAll(subscriptions.assignedPartitions())

        // should at least encode the short version
        check(memberAssignment.remaining() >= 2) {
            "There are insufficient bytes available to read assignment from the sync-group " +
                    "response (actual byte size ${memberAssignment.remaining()}) , this is not " +
                    "expected; it is possible that the leader's assign function is buggy and did " +
                    "not return any assignment for this member, or because static member is " +
                    "configured and the protocol is buggy hence did not get the assignment for " +
                    "this member"
        }
        val assignment = ConsumerProtocol.deserializeAssignment(memberAssignment)
        val assignedPartitions: SortedSet<TopicPartition> = TreeSet(COMPARATOR)

        assignedPartitions.addAll(assignment.partitions)
        if (!subscriptions.checkAssignmentMatchedSubscription(assignedPartitions)) {
            val fullReason =
                "received assignment ${assignment.partitions} does not match the current " +
                        "subscription ${subscriptions.prettyString()}; it is likely that the " +
                        "subscription has changed since we joined the group, will re-join with " +
                        "current subscription"
            requestRejoin(
                shortReason = "received assignment does not match the current subscription",
                fullReason = fullReason,
            )
            return
        }
        val firstException = AtomicReference<Exception?>(null)
        val addedPartitions: SortedSet<TopicPartition> = TreeSet(COMPARATOR)
        addedPartitions.addAll(assignedPartitions)
        addedPartitions.removeAll(ownedPartitions)

        if (this.protocol === RebalanceProtocol.COOPERATIVE) {
            val revokedPartitions: SortedSet<TopicPartition> = TreeSet(COMPARATOR)
            revokedPartitions.addAll(ownedPartitions)
            revokedPartitions.removeAll(assignedPartitions)
            log.info(
                ("Updating assignment with\n" +
                        "\tAssigned partitions:                       {}\n" +
                        "\tCurrent owned partitions:                  {}\n" +
                        "\tAdded partitions (assigned - owned):       {}\n" +
                        "\tRevoked partitions (owned - assigned):     {}\n"),
                assignedPartitions,
                ownedPartitions,
                addedPartitions,
                revokedPartitions,
            )
            if (!revokedPartitions.isEmpty()) {
                // Revoke partitions that were previously owned but no longer assigned;
                // note that we should only change the assignment (or update the assignor's state)
                // AFTER we've triggered  the revoke callback
                firstException.compareAndSet(null, invokePartitionsRevoked(revokedPartitions))

                // If revoked any partitions, need to re-join the group afterwards
                requestRejoin(
                    shortReason = "need to revoke partitions and re-join",
                    fullReason = "need to revoke partitions $revokedPartitions as indicated by " +
                            "the current assignment and re-join",
                )
            }
        }

        // The leader may have assigned partitions which match our subscription pattern, but which
        // were not explicitly requested, so we update the joined subscription here.
        maybeUpdateJoinedSubscription(assignedPartitions)

        // Catch any exception here to make sure we could complete the user callback.
        firstException.compareAndSet(null, invokeOnAssignment(assignor, assignment))

        // Reschedule the auto commit starting from now
        if (autoCommitEnabled) nextAutoCommitTimer!!.updateAndReset(autoCommitIntervalMs.toLong())
        subscriptions.assignFromSubscribed(assignedPartitions)

        // Add partitions that were not previously owned but are now assigned
        firstException.compareAndSet(null, invokePartitionsAssigned(addedPartitions))
        if (firstException.get() != null) {
            if (firstException.get() is KafkaException)
                throw firstException.get() as KafkaException
            else throw KafkaException(
                message = "User rebalance callback throws an error",
                cause = firstException.get()
            )
        }
    }

    fun maybeUpdateSubscriptionMetadata() {
        val version = metadata.updateVersion()
        if (version > metadataSnapshot.version) {
            val cluster = metadata.fetch()
            if (subscriptions.hasPatternSubscription()) updatePatternSubscription(cluster)

            // Update the current snapshot, which will be used to check for subscription
            // changes that would require a rebalance (e.g. new partitions).
            metadataSnapshot = MetadataSnapshot(
                clientRack = rackId,
                subscription = subscriptions,
                cluster = cluster,
                version = version,
            )
        }
    }

    private fun coordinatorUnknownAndUnreadySync(timer: Timer): Boolean =
        coordinatorUnknown() && !ensureCoordinatorReady(timer)

    private fun coordinatorUnknownAndUnreadyAsync(): Boolean =
        coordinatorUnknown() && !ensureCoordinatorReadyAsync()

    /**
     * Poll for coordinator events. This ensures that the coordinator is known and that the consumer
     * has joined the group (if it is using group management). This also handles periodic offset
     * commits if they are enabled.
     *
     * Returns early if the timeout expires or if waiting on rejoin is not required
     *
     * @param timer Timer bounding how long this method can block
     * @param waitForJoinGroup Boolean flag indicating if we should wait until re-join group
     * completes
     * @throws KafkaException if the rebalance callback throws an exception
     * @return `true` iff the operation succeeded
     */
    fun poll(timer: Timer, waitForJoinGroup: Boolean): Boolean {
        maybeUpdateSubscriptionMetadata()
        invokeCompletedOffsetCommitCallbacks()
        if (subscriptions.hasAutoAssignedPartitions()) {
            checkNotNull(protocol) {
                "User configured ${ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG} to empty " +
                        "while trying to subscribe for group protocol to auto assign partitions"
            }
            // Always update the heartbeat last poll time so that the heartbeat thread does not
            // leave the group proactively due to application inactivity even if (say) the
            // coordinator cannot be found.
            pollHeartbeat(timer.currentTimeMs)
            if (coordinatorUnknownAndUnreadySync(timer)) return false

            if (rejoinNeededOrPending()) {
                // due to a race condition between the initial metadata fetch and the initial
                // rebalance, we need to ensure that the metadata is fresh before joining initially.
                // This ensures that we have matched the pattern against the cluster's topics at
                // least once before joining.
                if (subscriptions.hasPatternSubscription()) {
                    // For consumer group that uses pattern-based subscription, after a topic is
                    // created, any consumer that discovers the topic after metadata refresh can
                    // trigger rebalance across the entire consumer group. Multiple rebalances can
                    // be triggered after one topic creation if consumers refresh metadata at vastly
                    // different times. We can significantly reduce the number of rebalances caused
                    // by single topic creation by asking consumer to refresh metadata before
                    // re-joining the group as long as the refresh backoff time has passed.
                    if (metadata.timeToAllowUpdate(timer.currentTimeMs) == 0L)
                        metadata.requestUpdate()

                    if (!client.ensureFreshMetadata(timer)) return false
                    maybeUpdateSubscriptionMetadata()
                }

                // if not wait for join group, we would just use a timer of 0
                if (!ensureActiveGroup(if (waitForJoinGroup) timer else time.timer(0L))) {
                    // since we may use a different timer in the callee, we'd still need to update
                    // the original timer's current time after the call
                    timer.update(time.milliseconds())
                    return false
                }
            }
        } else {
            // For manually assigned partitions, we do not try to pro-actively lookup coordinator;
            // instead we only try to refresh metadata when necessary.
            // If connections to all nodes fail, wakeups triggered while attempting to send fetch
            // requests result in polls returning immediately, causing a tight loop of polls.
            // Without the wakeup, poll() with no channels would block for the timeout, delaying
            // re-connection. awaitMetadataUpdate() in ensureCoordinatorReady initiates new
            // connections with configured backoff and avoids the busy loop.
            if (metadata.updateRequested() && !client.hasReadyNodes(timer.currentTimeMs))
                client.awaitMetadataUpdate(timer)

            // if there is pending coordinator requests, ensure they have a chance to be transmitted.
            client.pollNoWakeup()
        }
        maybeAutoCommitOffsetsAsync(timer.currentTimeMs)
        return true
    }

    /**
     * Return the time to the next needed invocation of [ConsumerNetworkClient.poll].
     * @param now current time in milliseconds
     * @return the maximum time in milliseconds the caller should wait before the next invocation of
     * poll()
     */
    fun timeToNextPoll(now: Long): Long {
        return if (!autoCommitEnabled) timeToNextHeartbeat(now)
        else min(nextAutoCommitTimer!!.remainingMs, timeToNextHeartbeat(now))
    }

    private fun updateGroupSubscription(topics: Set<String>) {
        // the leader will begin watching for changes to any of the topics the group is interested
        // in, which ensures that all metadata changes will eventually be seen
        if (subscriptions.groupSubscribe(topics)) metadata.requestUpdateForNewTopics()

        // update metadata (if needed) and keep track of the metadata used for assignment so that
        // we can check after rebalance completion whether anything has changed
        if (!client.ensureFreshMetadata(time.timer(Long.MAX_VALUE))) throw TimeoutException()
        maybeUpdateSubscriptionMetadata()
    }

    private fun isAssignFromSubscribedTopicsAssignor(name: String): Boolean =
        ConsumerConfig.ASSIGN_FROM_SUBSCRIBED_ASSIGNORS.contains(name)

    /**
     * user-customized assignor may have created some topics that are not in the subscription list
     * and assign their partitions to the members; in this case we would like to update the leader's
     * own metadata with the newly added topics so that it will not trigger a subsequent rebalance
     * when these topics gets updated from metadata refresh.
     *
     * We skip the check for in-product assignors since this will not happen in in-product assignors.
     *
     * TODO: this is a hack and not something we want to support long-term unless we push regex into
     *   the protocol
     * we may need to modify the ConsumerPartitionAssignor API to better support this case.
     *
     * @param assignorName The selected assignor name
     * @param assignments The assignments after assignor assigned
     * @param allSubscribedTopics All consumers' subscribed topics
     */
    private fun maybeUpdateGroupSubscription(
        assignorName: String,
        assignments: Map<String, ConsumerPartitionAssignor.Assignment>,
        allSubscribedTopics: MutableSet<String>,
    ) {
        if (!isAssignFromSubscribedTopicsAssignor(assignorName)) {
            val assignedTopics: MutableSet<String> = HashSet()
            for (assigned in assignments.values)
                for (tp in assigned.partitions) assignedTopics.add(tp.topic)

            if (!assignedTopics.containsAll(allSubscribedTopics)) {
                val notAssignedTopics: SortedSet<String> = TreeSet(allSubscribedTopics)
                notAssignedTopics.removeAll(assignedTopics)
                log.warn(
                    "The following subscribed topics are not assigned to any members: {} ",
                    notAssignedTopics,
                )
            }
            if (!allSubscribedTopics.containsAll(assignedTopics)) {
                val newlyAddedTopics: SortedSet<String> = TreeSet(assignedTopics)
                newlyAddedTopics.removeAll(allSubscribedTopics)
                log.info(
                    "The following not-subscribed topics are assigned, and their metadata will " +
                            "be fetched from the brokers: {}",
                    newlyAddedTopics,
                )
                allSubscribedTopics.addAll(newlyAddedTopics)
                updateGroupSubscription(allSubscribedTopics)
            }
        }
    }

    public override fun onLeaderElected(
        leaderId: String,
        protocol: String,
        allMemberMetadata: List<JoinGroupResponseMember>,
        skipAssignment: Boolean,
    ): Map<String, ByteBuffer> {
        val assignor = checkNotNull(lookupAssignor(protocol)) {
            "Coordinator selected invalid assignment protocol: $protocol"
        }
        val assignorName = assignor.name()
        val allSubscribedTopics: MutableSet<String> = HashSet()
        val subscriptions: MutableMap<String, ConsumerPartitionAssignor.Subscription> = HashMap()

        // collect all the owned partitions
        val ownedPartitions: MutableMap<String, List<TopicPartition>> = HashMap()
        for (memberSubscription: JoinGroupResponseMember in allMemberMetadata) {
            val subscription = ConsumerProtocol.deserializeSubscription(
                ByteBuffer.wrap(memberSubscription.metadata)
            )
            subscription.groupInstanceId = memberSubscription.groupInstanceId
            subscriptions[memberSubscription.memberId] = subscription
            allSubscribedTopics.addAll(subscription.topics)
            ownedPartitions[memberSubscription.memberId] = subscription.ownedPartitions
        }

        // the leader will begin watching for changes to any of the topics the group is interested
        // in, which ensures that all metadata changes will eventually be seen
        updateGroupSubscription(allSubscribedTopics)
        isLeader = true
        if (skipAssignment) {
            log.info(
                "Skipped assignment for returning static leader at generation {}. The static " +
                        "leader will continue with its existing assignment.",
                generation().generationId
            )
            assignmentSnapshot = metadataSnapshot
            return emptyMap()
        }
        log.debug(
            "Performing assignment using strategy {} with subscriptions {}",
            assignorName,
            subscriptions
        )
        val assignments = assignor.assign(
            metadata = metadata.fetch(),
            groupSubscription = GroupSubscription(subscriptions)
        ).assignments

        // skip the validation for built-in cooperative sticky assignor since we've considered
        // the "generation" of ownedPartition inside the assignor
        if (
            this.protocol === RebalanceProtocol.COOPERATIVE
            && assignorName != CooperativeStickyAssignor.COOPERATIVE_STICKY_ASSIGNOR_NAME
        ) validateCooperativeAssignment(ownedPartitions, assignments)

        maybeUpdateGroupSubscription(assignorName, assignments, allSubscribedTopics)

        // metadataSnapshot could be updated when the subscription is updated therefore
        // we must take the assignment snapshot after.
        assignmentSnapshot = metadataSnapshot
        log.info(
            "Finished assignment for group at generation {}: {}",
            generation().generationId,
            assignments
        )
        return assignments.mapValues { (_, value) -> ConsumerProtocol.serializeAssignment(value) }
    }

    /**
     * Used by COOPERATIVE rebalance protocol only.
     *
     * Validate the assignments returned by the assignor such that no owned partitions are going to
     * be reassigned to a different consumer directly: if the assignor wants to reassign an owned
     * partition, it must first remove it from the new assignment of the current owner so that it is
     * not assigned to any member, and then in the next rebalance it can finally reassign those
     * partitions not owned by anyone to consumers.
     */
    private fun validateCooperativeAssignment(
        ownedPartitions: Map<String, List<TopicPartition>>,
        assignments: Map<String, ConsumerPartitionAssignor.Assignment>,
    ) {
        val totalRevokedPartitions: MutableSet<TopicPartition> = HashSet()
        val totalAddedPartitions: SortedSet<TopicPartition> = TreeSet(COMPARATOR)
        for ((key, assignment) in assignments) {
            val addedPartitions = assignment.partitions.toMutableSet()
            addedPartitions.removeAll(ownedPartitions[key]!!.toSet())

            val revokedPartitions = ownedPartitions[key]!!.toMutableSet()
            revokedPartitions.removeAll(assignment.partitions.toSet())

            totalAddedPartitions.addAll(addedPartitions)
            totalRevokedPartitions.addAll(revokedPartitions)
        }

        // if there are overlap between revoked partitions and added partitions, it means some
        // partitions immediately gets re-assigned to another member while it is still claimed by
        // some member
        totalAddedPartitions.retainAll(totalRevokedPartitions)
        if (totalAddedPartitions.isNotEmpty()) {
            log.error(
                "With the COOPERATIVE protocol, owned partitions cannot be reassigned to other " +
                        "members; however the assignor has reassigned partitions {} which are " +
                        "still owned by some members",
                totalAddedPartitions,
            )
            error("Assignor supporting the COOPERATIVE protocol violates its requirements")
        }
    }

    public override fun onJoinPrepare(timer: Timer, generation: Int, memberId: String): Boolean {
        log.debug(
            "Executing onJoinPrepare with generation {} and memberId {}",
            generation,
            memberId,
        )

        joinPrepareTimer?.update() ?: run {
            // We should complete onJoinPrepare before rebalanceTimeoutMs,
            // and continue to join group to avoid member got kicked out from group
            joinPrepareTimer = time.timer(rebalanceConfig.rebalanceTimeoutMs!!.toLong())
        }

        // async commit offsets prior to rebalance if auto-commit enabled and there is no in-flight
        // offset commit request
        if (autoCommitEnabled && autoCommitOffsetRequestFuture == null) {
            maybeMarkPartitionsPendingRevocation()
            autoCommitOffsetRequestFuture = maybeAutoCommitOffsetsAsync()
        }

        // wait for commit offset response before timer expired
        autoCommitOffsetRequestFuture?.let { requestFuture ->
            val pollTimer =
                if (timer.remainingMs < joinPrepareTimer!!.remainingMs) timer
                else joinPrepareTimer!!

            client.poll(requestFuture, pollTimer)
            joinPrepareTimer!!.update()

            // Keep retrying/waiting the offset commit when:
            // 1. offset commit haven't done (and joinPrepareTimer not expired)
            // 2. failed with retriable exception (and joinPrepareTimer not expired)
            // Otherwise, continue to revoke partitions, ex:
            // 1. if joinPrepareTimer has expired
            // 2. if offset commit failed with non-retriable exception
            // 3. if offset commit success
            var onJoinPrepareAsyncCommitCompleted = true
            if (joinPrepareTimer!!.isExpired) log.error(
                "Asynchronous auto-commit of offsets failed: joinPrepare timeout. Will continue to join group"
            ) else if (!requestFuture.isDone) onJoinPrepareAsyncCommitCompleted = false
            else if (requestFuture.failed() && requestFuture.isRetriable) {
                log.debug(
                    "Asynchronous auto-commit of offsets failed with retryable error: {}. Will retry it.",
                    requestFuture.exception().message
                )
                onJoinPrepareAsyncCommitCompleted = false
            } else if (requestFuture.failed() && !requestFuture.isRetriable) log.error(
                "Asynchronous auto-commit of offsets failed: {}. Will continue to join group.",
                requestFuture.exception().message
            )
            if (requestFuture.isDone) this.autoCommitOffsetRequestFuture = null

            if (!onJoinPrepareAsyncCommitCompleted) {
                pollTimer.sleep(min(pollTimer.remainingMs, rebalanceConfig.retryBackoffMs!!))
                timer.update()
                return false
            }
        }

        // the generation / member-id can possibly be reset by the heartbeat thread upon getting
        // errors or heartbeat timeouts; in this case whatever is previously owned partitions would
        // be lost, we should trigger the callback and cleanup the assignment; otherwise we can
        // proceed normally and revoke the partitions depending on the protocol, and in that case we
        // should only change the assignment AFTER the revoke callback is triggered so that users
        // can still access the previously owned partitions to commit offsets etc.
        var exception: Exception? = null
        val revokedPartitions: SortedSet<TopicPartition> = TreeSet(COMPARATOR)
        if (
            generation == Generation.NO_GENERATION.generationId
            || memberId == Generation.NO_GENERATION.memberId
        ) {
            revokedPartitions.addAll(subscriptions.assignedPartitions())
            if (!revokedPartitions.isEmpty()) {
                log.info(
                    "Giving away all assigned partitions as lost since generation/memberID has " +
                            "been reset, indicating that consumer is in old state or no longer " +
                            "part of the group"
                )
                exception = invokePartitionsLost(revokedPartitions)
                subscriptions.assignFromSubscribed(emptySet())
            }
        } else when (protocol) {
            RebalanceProtocol.EAGER -> {
                // revoke all partitions
                revokedPartitions.addAll(subscriptions.assignedPartitions())
                exception = invokePartitionsRevoked(revokedPartitions)
                subscriptions.assignFromSubscribed(emptySet())
            }

            RebalanceProtocol.COOPERATIVE -> {
                // only revoke those partitions that are not in the subscription anymore.
                val ownedPartitions: MutableSet<TopicPartition> =
                    subscriptions.assignedPartitions().toMutableSet()

                revokedPartitions.addAll(
                    ownedPartitions.filter { tp ->
                        !subscriptions.subscription().contains(tp.topic)
                    }
                )
                if (!revokedPartitions.isEmpty()) {
                    exception = invokePartitionsRevoked(revokedPartitions)
                    ownedPartitions.removeAll(revokedPartitions)
                    subscriptions.assignFromSubscribed(ownedPartitions)
                }
            }

            else -> Unit
        }
        isLeader = false
        subscriptions.resetGroupSubscription()
        joinPrepareTimer = null
        autoCommitOffsetRequestFuture = null
        timer.update()
        if (exception != null)
            throw KafkaException("User rebalance callback throws an error", exception)

        return true
    }

    private fun maybeMarkPartitionsPendingRevocation() {
        if (protocol !== RebalanceProtocol.EAGER) return

        // When asynchronously committing offsets prior to the revocation of a set of partitions,
        // there will be a window of time between when the offset commit is sent and when it returns
        // and revocation completes. It is possible for pending fetches for these partitions to
        // return during this time, which means the application's position may get ahead of the
        // committed position prior to revocation. This can cause duplicate consumption. To prevent
        // this, we mark the partitions as "pending revocation," which stops the Fetcher from
        // sending new fetches or returning data from previous fetches to the user.
        val partitions = subscriptions.assignedPartitions()
        log.debug("Marking assigned partitions pending for revocation: {}", partitions)
        subscriptions.markPendingRevocation(partitions)
    }

    public override fun onLeavePrepare() {
        // Save the current Generation, as the hb thread can change it at any time
        val currentGeneration = generation()
        log.debug("Executing onLeavePrepare with generation {}", currentGeneration)

        // we should reset assignment and trigger the callback before leaving group
        val droppedPartitions: SortedSet<TopicPartition> = TreeSet(COMPARATOR)
        droppedPartitions.addAll(subscriptions.assignedPartitions())
        if (subscriptions.hasAutoAssignedPartitions() && !droppedPartitions.isEmpty()) {
            val e: Exception? = if (
                currentGeneration.generationId == Generation.NO_GENERATION.generationId
                || (currentGeneration.memberId == Generation.NO_GENERATION.memberId)
                || rebalanceInProgress
            ) invokePartitionsLost(droppedPartitions)
            else invokePartitionsRevoked(droppedPartitions)

            subscriptions.assignFromSubscribed(emptySet())
            if (e != null) throw KafkaException("User rebalance callback throws an error", e)
        }
    }

    /**
     * @throws KafkaException if the callback throws exception
     */
    public override fun rejoinNeededOrPending(): Boolean {
        if (!subscriptions.hasAutoAssignedPartitions()) return false

        // we need to rejoin if we performed the assignment and metadata has changed;
        // also for those owned-but-no-longer-existed partitions we should drop them as lost
        if (assignmentSnapshot != null && !assignmentSnapshot!!.matches(metadataSnapshot)) {
            requestRejoinIfNecessary(
                shortReason = "cached metadata has changed",
                fullReason = "cached metadata has changed from $assignmentSnapshot at the " +
                        "beginning of the rebalance to $metadataSnapshot",
            )
            return true
        }

        // we need to join if our subscription has changed since the last join
        if (::joinedSubscription.isInitialized && joinedSubscription != subscriptions.subscription()) {
            val fullReason = String.format(
                "subscription has changed from %s at the beginning of the rebalance to %s",
                joinedSubscription, subscriptions.subscription()
            )
            requestRejoinIfNecessary("subscription has changed", fullReason)
            return true
        }
        return super.rejoinNeededOrPending()
    }

    /**
     * Refresh the committed offsets for provided partitions.
     *
     * @param timer Timer bounding how long this method can block
     * @return true iff the operation completed within the timeout
     */
    fun refreshCommittedOffsetsIfNeeded(timer: Timer): Boolean {
        val initializingPartitions = subscriptions.initializingPartitions()
        val offsets = fetchCommittedOffsets(initializingPartitions, timer) ?: return false
        for ((tp, offsetAndMetadata) in offsets) {
            if (offsetAndMetadata != null) {
                // first update the epoch if necessary
                offsetAndMetadata.leaderEpoch()?.let { epoch ->
                    metadata.updateLastSeenEpochIfNewer(tp, epoch)
                }

                // it's possible that the partition is no longer assigned when the response is received,
                // so we need to ignore seeking if that's the case
                if (subscriptions.isAssigned(tp)) {
                    val leaderAndEpoch = metadata.currentLeader(tp)
                    val position = FetchPosition(
                        offsetAndMetadata.offset,
                        offsetAndMetadata.leaderEpoch(),
                        leaderAndEpoch,
                    )
                    subscriptions.seekUnvalidated(tp, position)
                    log.info(
                        "Setting offset for partition {} to the committed offset {}",
                        tp,
                        position,
                    )
                } else log.info(
                    "Ignoring the returned {} since its partition {} is no longer assigned",
                    offsetAndMetadata,
                    tp,
                )
            }
        }
        return true
    }

    /**
     * Fetch the current committed offsets from the coordinator for a set of partitions.
     *
     * @param partitions The partitions to fetch offsets for
     * @return A map from partition to the committed offset or null if the operation timed out
     */
    fun fetchCommittedOffsets(
        partitions: Set<TopicPartition>,
        timer: Timer,
    ): Map<TopicPartition, OffsetAndMetadata?>? {
        if (partitions.isEmpty()) return emptyMap()
        val generationForOffsetRequest = generationIfStable

        if (
            pendingCommittedOffsetRequest != null
            && !pendingCommittedOffsetRequest!!.sameRequest(partitions, generationForOffsetRequest)
        ) {
            // if we were waiting for a different request, then just clear it.
            pendingCommittedOffsetRequest = null
        }

        do {
            if (!ensureCoordinatorReady(timer)) return null

            // contact coordinator to fetch committed offsets
            val future = pendingCommittedOffsetRequest?.response ?: run {
                val fetchResponse = sendOffsetFetchRequest(partitions)
                pendingCommittedOffsetRequest = PendingCommittedOffsetRequest(
                    requestedPartitions = partitions,
                    requestedGeneration = generationForOffsetRequest,
                    response = fetchResponse,
                )
                fetchResponse
            }

            client.poll(future, timer)

            if (future.isDone) {
                pendingCommittedOffsetRequest = null
                if (future.succeeded()) return future.value()
                else if (!future.isRetriable) throw future.exception()
                else timer.sleep(rebalanceConfig.retryBackoffMs!!)
            } else return null

        } while (timer.isNotExpired)
        return null
    }

    /**
     * Return the consumer group metadata.
     *
     * @return the current consumer group metadata
     */
    fun groupMetadata(): ConsumerGroupMetadata = groupMetadata

    /**
     * @throws KafkaException if the rebalance callback throws exception
     */
    public override fun close(timer: Timer) {
        // we do not need to re-enable wakeups since we are closing already
        client.disableWakeups()
        try {
            maybeAutoCommitOffsetsSync(timer)
            while (pendingAsyncCommits.get() > 0 && timer.isNotExpired) {
                ensureCoordinatorReady(timer)
                client.poll(timer)
                invokeCompletedOffsetCommitCallbacks()
            }
        } finally {
            super.close(timer)
        }
    }

    // visible for testing
    fun invokeCompletedOffsetCommitCallbacks() {
        if (asyncCommitFenced.get()) throw FencedInstanceIdException(
            "Get fenced exception for group.instance.id " +
                    (rebalanceConfig.groupInstanceId ?: "unset_instance_id") +
                    ", current member.id is $memberId"
        )

        while (true) {
            val completion = completedOffsetCommits.poll() ?: break
            completion.invoke()
        }
    }

    fun commitOffsetsAsync(
        offsets: Map<TopicPartition, OffsetAndMetadata>,
        callback: OffsetCommitCallback?,
    ): RequestFuture<Unit>? {
        invokeCompletedOffsetCommitCallbacks()
        var future: RequestFuture<Unit>? = null
        if (offsets.isEmpty()) {
            // No need to check coordinator if offsets is empty since commit of empty offsets is
            // completed locally.
            future = doCommitOffsetsAsync(offsets, callback)
        } else if (!coordinatorUnknownAndUnreadyAsync()) {
            // we need to make sure coordinator is ready before committing, since this is for async
            // committing we do not try to block, but just try once to clear the previous
            // discover-coordinator future, resend, or get responses; if the coordinator is not
            // ready yet then we would just proceed and put that into the pending requests, and
            // future poll calls would still try to complete them.
            //
            // the key here though is that we have to try sending the discover-coordinator if it's
            // not known or ready, since this is the only place we can send such request under
            // manual assignment (there we would not have heartbeat thread trying to auto-rediscover
            // the coordinator).
            future = doCommitOffsetsAsync(offsets, callback)
        } else {
            // we don't know the current coordinator, so try to find it and then send the commit or
            // fail (we don't want recursive retries which can cause offset commits to arrive out of
            // order). Note that there may be multiple offset commits chained to the same
            // coordinator lookup request. This is fine because the listeners will be invoked in the
            // same order that they were added. Note also that AbstractCoordinator prevents multiple
            // concurrent coordinator lookup requests.
            pendingAsyncCommits.incrementAndGet()
            lookupCoordinator().addListener(object : RequestFutureListener<Unit> {
                override fun onSuccess(result: Unit) {
                    pendingAsyncCommits.decrementAndGet()
                    doCommitOffsetsAsync(offsets, callback)
                    client.pollNoWakeup()
                }

                override fun onFailure(exception: RuntimeException) {
                    pendingAsyncCommits.decrementAndGet()
                    completedOffsetCommits.add(
                        OffsetCommitCompletion(
                            callback = callback,
                            offsets = offsets,
                            exception = RetriableCommitFailedException(cause = exception),
                        )
                    )
                }
            })
        }

        // ensure the commit has a chance to be transmitted (without blocking on its completion).
        // Note that commits are treated as heartbeats by the coordinator, so there is no need to
        // explicitly allow heartbeats through delayed task execution.
        client.pollNoWakeup()
        return future
    }

    private fun doCommitOffsetsAsync(
        offsets: Map<TopicPartition, OffsetAndMetadata>,
        callback: OffsetCommitCallback?,
    ): RequestFuture<Unit> {
        val future = sendOffsetCommitRequest(offsets)
        inFlightAsyncCommits.incrementAndGet()
        val cb = callback ?: defaultOffsetCommitCallback
        future.addListener(object : RequestFutureListener<Unit> {
            override fun onSuccess(result: Unit) {
                inFlightAsyncCommits.decrementAndGet()

                interceptors?.onCommit(offsets)
                completedOffsetCommits.add(OffsetCommitCompletion(cb, offsets, null))
            }

            override fun onFailure(exception: RuntimeException) {
                inFlightAsyncCommits.decrementAndGet()

                var commitException: Exception = exception
                if (exception is RetriableException)
                    commitException = RetriableCommitFailedException(cause = exception)

                completedOffsetCommits.add(
                    OffsetCommitCompletion(
                        callback = cb,
                        offsets = offsets,
                        exception = commitException,
                    )
                )
                if (commitException is FencedInstanceIdException) asyncCommitFenced.set(true)
            }
        })
        return future
    }

    /**
     * Commit offsets synchronously. This method will retry until the commit completes successfully
     * or an unrecoverable error is encountered.
     *
     * @param offsets The offsets to be committed
     * @throws org.apache.kafka.common.errors.AuthorizationException if the consumer is not
     * authorized to the group or to any of the specified partitions. See the exception for more
     * details
     * @throws CommitFailedException if an unrecoverable error occurs before the commit can be
     * completed
     * @throws FencedInstanceIdException if a static member gets fenced
     * @return If the offset commit was successfully sent and a successful response was received
     * from the coordinator
     */
    fun commitOffsetsSync(offsets: Map<TopicPartition, OffsetAndMetadata>, timer: Timer): Boolean {
        invokeCompletedOffsetCommitCallbacks()

        if (offsets.isEmpty()) {
            // We guarantee that the callbacks for all commitAsync() will be invoked when
            // commitSync() completes, even if the user tries to commit empty offsets.
            return invokePendingAsyncCommits(timer)
        }

        do {
            if (coordinatorUnknownAndUnreadySync(timer)) return false

            val future = sendOffsetCommitRequest(offsets)
            client.poll(future, timer)

            // We may have had in-flight offset commits when the synchronous commit began. If so,
            // ensure that the corresponding callbacks are invoked prior to returning in order to
            // preserve the order that the offset commits were applied.
            invokeCompletedOffsetCommitCallbacks()

            if (future.succeeded()) {
                interceptors?.onCommit(offsets)
                return true
            }

            if (future.failed() && !future.isRetriable) throw future.exception()

            timer.sleep(rebalanceConfig.retryBackoffMs!!)
        } while (timer.isNotExpired)

        return false
    }

    private fun maybeAutoCommitOffsetsSync(timer: Timer) {
        if (autoCommitEnabled) {
            val allConsumedOffsets = subscriptions.allConsumed()
            try {
                log.debug("Sending synchronous auto-commit of offsets {}", allConsumedOffsets)
                if (!commitOffsetsSync(allConsumedOffsets, timer)) log.debug(
                    "Auto-commit of offsets {} timed out before completion",
                    allConsumedOffsets,
                )
            } catch (e: WakeupException) {
                log.debug(
                    "Auto-commit of offsets {} was interrupted before completion",
                    allConsumedOffsets,
                )
                // rethrow wakeups since they are triggered by the user
                throw e
            } catch (e: InterruptException) {
                log.debug(
                    "Auto-commit of offsets {} was interrupted before completion",
                    allConsumedOffsets,
                )
                throw e
            } catch (e: Exception) {
                // consistent with async auto-commit failures, we do not propagate the exception
                log.warn(
                    "Synchronous auto-commit of offsets {} failed: {}",
                    allConsumedOffsets,
                    e.message,
                )
            }
        }
    }

    fun maybeAutoCommitOffsetsAsync(now: Long) {
        if (autoCommitEnabled) {
            val timer = nextAutoCommitTimer!!
            timer.update(now)
            if (timer.isExpired) {
                timer.reset(autoCommitIntervalMs.toLong())
                autoCommitOffsetsAsync()
            }
        }
    }

    private fun invokePendingAsyncCommits(timer: Timer): Boolean {
        if (inFlightAsyncCommits.get() == 0) return true

        do {
            ensureCoordinatorReady(timer)
            client.poll(timer)
            invokeCompletedOffsetCommitCallbacks()
            if (inFlightAsyncCommits.get() == 0) return true
            timer.sleep(rebalanceConfig.retryBackoffMs!!)
        } while (timer.isNotExpired)
        return false
    }

    private fun autoCommitOffsetsAsync(): RequestFuture<Unit>? {
        val allConsumedOffsets = subscriptions.allConsumed()
        log.debug("Sending asynchronous auto-commit of offsets {}", allConsumedOffsets)
        return commitOffsetsAsync(
            allConsumedOffsets
        ) { offsets, exception ->
            if (exception != null) {
                if (exception is RetriableCommitFailedException) {
                    log.debug(
                        "Asynchronous auto-commit of offsets {} failed due to retriable error: {}",
                        offsets,
                        exception,
                    )
                    nextAutoCommitTimer!!.updateAndReset(rebalanceConfig.retryBackoffMs!!)
                } else {
                    log.warn(
                        "Asynchronous auto-commit of offsets {} failed: {}",
                        offsets,
                        exception.message,
                    )
                }
            } else log.debug("Completed asynchronous auto-commit of offsets {}", offsets)
        }
    }

    private fun maybeAutoCommitOffsetsAsync(): RequestFuture<Unit>? =
        if (autoCommitEnabled) autoCommitOffsetsAsync() else null

    private inner class DefaultOffsetCommitCallback : OffsetCommitCallback {

        override fun onComplete(
            offsets: Map<TopicPartition, OffsetAndMetadata>?,
            exception: Exception?,
        ) {
            if (exception != null) log.error("Offset commit with offsets {} failed", offsets, exception)
        }
    }

    /**
     * Commit offsets for the specified list of topics and partitions. This is a non-blocking call
     * which returns a request future that can be polled in the case of a synchronous commit or
     * ignored in the asynchronous case.
     *
     * NOTE: This is visible only for testing
     *
     * @param offsets The list of offsets per partition that should be committed.
     * @return A request future whose value indicates whether the commit was successful or not
     */
    fun sendOffsetCommitRequest(offsets: Map<TopicPartition, OffsetAndMetadata>): RequestFuture<Unit> {
        if (offsets.isEmpty()) return RequestFuture.voidSuccess()
        val coordinator = checkAndGetCoordinator() ?: return RequestFuture.coordinatorNotAvailable()

        // create the offset commit request
        val requestTopicDataMap = mutableMapOf<String, OffsetCommitRequestTopic>()
        for ((topicPartition, offsetAndMetadata) in offsets) {
            if (offsetAndMetadata.offset < 0) return RequestFuture.failure(
                IllegalArgumentException("Invalid offset: ${offsetAndMetadata.offset}")
            )

            val topic = requestTopicDataMap.getOrDefault(
                key = topicPartition.topic,
                defaultValue = OffsetCommitRequestTopic().setName(topicPartition.topic),
            )
            topic.partitions += OffsetCommitRequestPartition()
                .setPartitionIndex(topicPartition.partition)
                .setCommittedOffset(offsetAndMetadata.offset)
                .setCommittedLeaderEpoch(
                    offsetAndMetadata.leaderEpoch() ?: RecordBatch.NO_PARTITION_LEADER_EPOCH
                )
                .setCommittedMetadata(offsetAndMetadata.metadata)

            requestTopicDataMap[topicPartition.topic] = topic
        }
        val generation: Generation?
        val groupInstanceId: String?
        if (subscriptions.hasAutoAssignedPartitions()) {
            generation = generationIfStable
            groupInstanceId = rebalanceConfig.groupInstanceId
            // if the generation is null, we are not part of an active group (and we expect to be).
            // the only thing we can do is fail the commit and let the user rejoin the group in
            // poll().
            if (generation == null) {
                log.info("Failing OffsetCommit request since the consumer is not part of an active group")
                return if (rebalanceInProgress) {
                    // if the client knows it is already rebalancing, we can use
                    // RebalanceInProgressException instead of CommitFailedException to indicate
                    // this is not a fatal error
                    RequestFuture.failure(
                        RebalanceInProgressException(
                            "Offset commit cannot be completed since the consumer is undergoing " +
                                    "a rebalance for auto partition assignment. You can try " +
                                    "completing the rebalance by calling poll() and then retry " +
                                    "the operation."
                        )
                    )
                } else RequestFuture.failure(
                    CommitFailedException(
                        "Offset commit cannot be completed since the consumer is not part of an " +
                                "active group for auto partition assignment; it is likely that " +
                                "the consumer was kicked out of the group."
                    )
                )
            }
        } else {
            generation = Generation.NO_GENERATION
            groupInstanceId = null
        }
        val builder = OffsetCommitRequest.Builder(
            OffsetCommitRequestData()
                .setGroupId(rebalanceConfig.groupId!!)
                .setGenerationIdOrMemberEpoch(generation.generationId)
                .setMemberId(generation.memberId)
                .setGroupInstanceId(groupInstanceId)
                .setTopics(requestTopicDataMap.values.toList())
        )
        log.trace("Sending OffsetCommit request with {} to coordinator {}", offsets, coordinator)

        return client.send(coordinator, builder)
            .compose(OffsetCommitResponseHandler(offsets, generation))
    }

    private inner class OffsetCommitResponseHandler(
        private val offsets: Map<TopicPartition, OffsetAndMetadata>,
        generation: Generation,
    ) : CoordinatorResponseHandler<OffsetCommitResponse, Unit>(generation) {

        override fun handle(response: OffsetCommitResponse, future: RequestFuture<Unit>) {
            sensors.commitSensor.record(this.response!!.requestLatencyMs.toDouble())
            val unauthorizedTopics = mutableSetOf<String>()

            for (topic in response.data().topics) {
                for (partition in topic.partitions) {
                    val tp = TopicPartition(topic.name, partition.partitionIndex)
                    val offsetAndMetadata = offsets[tp]
                    val offset = offsetAndMetadata!!.offset
                    val error = Errors.forCode(partition.errorCode)
                    if (error === Errors.NONE)
                        log.debug("Committed offset {} for partition {}", offset, tp)
                    else {
                        if (error.exception is RetriableException) log.warn(
                            "Offset commit failed on partition {} at offset {}: {}",
                            tp,
                            offset,
                            error.message,
                        ) else log.error(
                            "Offset commit failed on partition {} at offset {}: {}",
                            tp,
                            offset,
                            error.message,
                        )

                        if (error === Errors.GROUP_AUTHORIZATION_FAILED) {
                            future.raise(
                                GroupAuthorizationException(groupId = rebalanceConfig.groupId)
                            )
                            return
                        } else if (error === Errors.TOPIC_AUTHORIZATION_FAILED)
                            unauthorizedTopics.add(tp.topic)
                        else if (
                            error === Errors.OFFSET_METADATA_TOO_LARGE
                            || error === Errors.INVALID_COMMIT_OFFSET_SIZE
                        ) {
                            // raise the error to the user
                            future.raise(error)
                            return
                        } else if (
                            error === Errors.COORDINATOR_LOAD_IN_PROGRESS
                            || error === Errors.UNKNOWN_TOPIC_OR_PARTITION
                        ) {
                            // just retry
                            future.raise(error)
                            return
                        } else if (
                            error === Errors.COORDINATOR_NOT_AVAILABLE
                            || error === Errors.NOT_COORDINATOR
                            || error === Errors.REQUEST_TIMED_OUT
                        ) {
                            markCoordinatorUnknown(error)
                            future.raise(error)
                            return
                        } else if (error === Errors.FENCED_INSTANCE_ID) {
                            log.info(
                                "OffsetCommit failed with {} due to group instance id {} fenced",
                                sentGeneration,
                                rebalanceConfig.groupInstanceId
                            )

                            // if the generation has changed or we are not in rebalancing, do not
                            // raise the fatal error but rebalance-in-progress
                            if (generationUnchanged()) future.raise(error)
                            else {
                                var exception: KafkaException
                                synchronized(this@ConsumerCoordinator) {
                                    exception = if (state === MemberState.PREPARING_REBALANCE)
                                        RebalanceInProgressException(
                                            "Offset commit cannot be completed since the " +
                                                    "consumer member's old generation is fenced " +
                                                    "by its group instance id, it is possible " +
                                                    "that this consumer has already participated " +
                                                    "another rebalance and got a new generation"
                                        )
                                    else CommitFailedException()

                                }
                                future.raise(exception)
                            }
                            return
                        } else if (error === Errors.REBALANCE_IN_PROGRESS) {
                            /* Consumer should not try to commit offset in between join-group and
                             * sync-group, and hence on broker-side it is not expected to see a
                             * commit offset request during CompletingRebalance phase; if it ever
                             * happens then broker would return this error to indicate that we are
                             *  still in the middle of a rebalance. In this case we would throw a
                             *  RebalanceInProgressException, request re-join but do not reset
                             *  generations. If the callers decide to retry they can go ahead and
                             *  call poll to finish up the rebalance first, and then try commit
                             *  again.
                             */
                            requestRejoin(
                                "offset commit failed since group is already rebalancing"
                            )
                            future.raise(
                                RebalanceInProgressException(
                                    "Offset commit cannot be completed since the consumer group " +
                                            "is executing a rebalance at the moment. You can try " +
                                            "completing the rebalance by calling poll() and then " +
                                            "retry commit again"
                                )
                            )
                            return
                        } else if (
                            error === Errors.UNKNOWN_MEMBER_ID
                            || error === Errors.ILLEGAL_GENERATION
                        ) {
                            log.info(
                                "OffsetCommit failed with {}: {}",
                                sentGeneration,
                                error.message,
                            )

                            // only need to reset generation and re-join group if generation has not
                            // changed or we are not in rebalancing; otherwise only raise
                            // rebalance-in-progress error
                            var exception: KafkaException
                            synchronized(this@ConsumerCoordinator) {
                                exception = if (
                                    !generationUnchanged()
                                    && state === MemberState.PREPARING_REBALANCE
                                ) RebalanceInProgressException(
                                    "Offset commit cannot be completed since the consumer " +
                                            "member's generation is already stale, meaning it " +
                                            "has already participated another rebalance and got " +
                                            "a new generation. You can try completing the " +
                                            "rebalance by calling poll() and then retry commit " +
                                            "again"
                                ) else {
                                    // don't reset generation member ID when ILLEGAL_GENERATION,
                                    // since the member might be still valid
                                    resetStateOnResponseError(
                                        api = ApiKeys.OFFSET_COMMIT,
                                        error = error,
                                        shouldResetMemberId = error !== Errors.ILLEGAL_GENERATION,
                                    )
                                    CommitFailedException()
                                }
                            }
                            future.raise(exception)
                            return
                        } else {
                            future.raise(
                                KafkaException("Unexpected error in commit: ${error.message}")
                            )
                            return
                        }
                    }
                }
            }
            if (unauthorizedTopics.isNotEmpty()) {
                log.error("Not authorized to commit to topics {}", unauthorizedTopics)
                future.raise(TopicAuthorizationException(unauthorizedTopics))
            } else future.complete(Unit)
        }
    }

    /**
     * Fetch the committed offsets for a set of partitions. This is a non-blocking call. The
     * returned future can be polled to get the actual offsets returned from the broker.
     *
     * @param partitions The set of partitions to get offsets for.
     * @return A request future containing the committed offsets.
     */
    private fun sendOffsetFetchRequest(
        partitions: Set<TopicPartition>,
    ): RequestFuture<Map<TopicPartition, OffsetAndMetadata?>> {
        val coordinator = checkAndGetCoordinator() ?: return RequestFuture.coordinatorNotAvailable()
        log.debug("Fetching committed offsets for partitions: {}", partitions)
        // construct the request
        val requestBuilder = OffsetFetchRequest.Builder(
            groupId = rebalanceConfig.groupId!!,
            requireStable = true,
            partitions = partitions.toList(),
            throwOnFetchStableOffsetsUnsupported = throwOnFetchStableOffsetsUnsupported
        )

        // send the request with a callback
        return client.send(coordinator, requestBuilder).compose(OffsetFetchResponseHandler())
    }

    private inner class OffsetFetchResponseHandler
        : CoordinatorResponseHandler<OffsetFetchResponse, Map<TopicPartition, OffsetAndMetadata?>>(
        Generation.NO_GENERATION
    ) {
        override fun handle(
            response: OffsetFetchResponse,
            future: RequestFuture<Map<TopicPartition, OffsetAndMetadata?>>,
        ) {
            val responseError = response.groupLevelError(rebalanceConfig.groupId!!)
            if (responseError !== Errors.NONE) {
                log.debug("Offset fetch failed: {}", responseError!!.message)
                if (responseError === Errors.COORDINATOR_LOAD_IN_PROGRESS) {
                    // just retry
                    future.raise(responseError)
                } else if (responseError === Errors.NOT_COORDINATOR) {
                    // re-discover the coordinator and retry
                    markCoordinatorUnknown(responseError)
                    future.raise(responseError)
                } else if (responseError === Errors.GROUP_AUTHORIZATION_FAILED)
                    future.raise(GroupAuthorizationException(groupId = rebalanceConfig.groupId))
                else future.raise(
                    KafkaException(
                        "Unexpected error in fetch offset response: ${responseError.message}"
                    )
                )

                return
            }
            var unauthorizedTopics: MutableSet<String>? = null
            val responseData = response.partitionDataMap(rebalanceConfig.groupId)
            val offsets: MutableMap<TopicPartition, OffsetAndMetadata?> = HashMap(responseData.size)
            val unstableTxnOffsetTopicPartitions = mutableSetOf<TopicPartition>()
            for ((tp, partitionData) in responseData) {
                if (partitionData.hasError()) {
                    val error = partitionData.error
                    log.debug("Failed to fetch offset for partition {}: {}", tp, error.message)
                    if (error === Errors.UNKNOWN_TOPIC_OR_PARTITION) {
                        future.raise(KafkaException("Topic or Partition $tp does not exist"))
                        return
                    } else if (error === Errors.TOPIC_AUTHORIZATION_FAILED) {
                        if (unauthorizedTopics == null) unauthorizedTopics = mutableSetOf()
                        unauthorizedTopics.add(tp.topic)
                    } else if (error === Errors.UNSTABLE_OFFSET_COMMIT)
                        unstableTxnOffsetTopicPartitions.add(tp)
                    else {
                        future.raise(
                            KafkaException(
                                "Unexpected error in fetch offset response for partition " +
                                        "$tp: ${error.message}"
                            )
                        )
                        return
                    }
                } else if (partitionData.offset >= 0) {
                    // record the position with the offset (-1 indicates no committed offset to fetch);
                    // if there's no committed offset, record as null
                    offsets[tp] = OffsetAndMetadata(
                        offset = partitionData.offset,
                        leaderEpoch = partitionData.leaderEpoch,
                        metadata = partitionData.metadata ?: "",
                    )
                } else {
                    log.info("Found no committed offset for partition {}", tp)
                    offsets[tp] = null
                }
            }
            if (unauthorizedTopics != null)
                future.raise(TopicAuthorizationException(unauthorizedTopics))
            else if (unstableTxnOffsetTopicPartitions.isNotEmpty()) {
                // just retry
                log.info(
                    "The following partitions still have unstable offsets which are not cleared " +
                            "on the broker side: {}, this could be either transactional offsets " +
                            "waiting for completion, or normal offsets waiting for replication " +
                            "after appending to local log",
                    unstableTxnOffsetTopicPartitions,
                )
                future.raise(
                    UnstableOffsetCommitException(
                        "There are unstable offsets for the requested topic partitions"
                    )
                )
            } else future.complete(offsets)
        }
    }

    private inner class ConsumerCoordinatorMetrics(metrics: Metrics, metricGrpPrefix: String) {

        private val metricGrpName: String = "$metricGrpPrefix-coordinator-metrics"

        val commitSensor: Sensor = metrics.sensor("commit-latency").apply {
            add(
                metricName = metrics.metricName(
                    name = "commit-latency-avg",
                    group = metricGrpName,
                    description = "The average time taken for a commit request",
                ),
                stat = Avg(),
            )
            add(
                metricName = metrics.metricName(
                    name = "commit-latency-max",
                    group = metricGrpName,
                    description = "The max time taken for a commit request",
                ),
                stat = Max(),
            )
            add(
                createMeter(
                    metrics = metrics,
                    groupName = metricGrpName,
                    baseName = "commit",
                    descriptiveName = "commit calls",
                )
            )
        }

        val revokeCallbackSensor: Sensor = metrics.sensor("partition-revoked-latency").apply {
            add(
                metricName = metrics.metricName(
                    name = "partition-revoked-latency-avg",
                    group = metricGrpName,
                    description =
                    "The average time taken for a partition-revoked rebalance listener callback",
                ),
                stat = Avg(),
            )
            add(
                metricName = metrics.metricName(
                    name = "partition-revoked-latency-max",
                    group = metricGrpName,
                    description =
                    "The max time taken for a partition-revoked rebalance listener callback",
                ),
                stat = Max(),
            )
        }

        val assignCallbackSensor: Sensor = metrics.sensor("partition-assigned-latency").apply {
            add(
                metricName = metrics.metricName(
                    name = "partition-assigned-latency-avg",
                    group = metricGrpName,
                    description =
                    "The average time taken for a partition-assigned rebalance listener callback",
                ),
                stat = Avg(),
            )
            add(
                metricName = metrics.metricName(
                    name = "partition-assigned-latency-max",
                    group = metricGrpName,
                    description =
                    "The max time taken for a partition-assigned rebalance listener callback",
                ),
                stat = Max(),
            )
        }

        val loseCallbackSensor: Sensor = metrics.sensor("partition-lost-latency").apply {
            add(
                metricName = metrics.metricName(
                    name = "partition-lost-latency-avg",
                    group = metricGrpName,
                    description =
                    "The average time taken for a partition-lost rebalance listener callback"
                ),
                stat = Avg(),
            )
            add(
                metricName = metrics.metricName(
                    name = "partition-lost-latency-max",
                    group = metricGrpName,
                    description =
                    "The max time taken for a partition-lost rebalance listener callback",
                ),
                stat = Max(),
            )
        }

        init {
            metrics.addMetric(
                metricName = metrics.metricName(
                    name = "assigned-partitions",
                    group = metricGrpName,
                    description = "The number of partitions currently assigned to this consumer",
                ),
                metricValueProvider = Measurable { _, _ ->
                    subscriptions.numAssignedPartitions().toDouble()
                },
            )
        }
    }

    private class MetadataSnapshot(
        clientRack: String?,
        subscription: SubscriptionState,
        cluster: Cluster,
        val version: Int,
    ) {
        private val partitionsPerTopic: Map<String, List<ConsumerCoordinator.PartitionRackInfo>>

        init {
            val partitionsPerTopic = mutableMapOf<String, List<PartitionRackInfo>>()
            for (topic in subscription.metadataTopics()) {
                val partitions = cluster.partitionsForTopic(topic)
                if (partitions.isNotEmpty()) {
                    val partitionRacks = partitions.map { p -> PartitionRackInfo(clientRack, p) }
                    partitionsPerTopic[topic] = partitionRacks
                }
            }
            this.partitionsPerTopic = partitionsPerTopic
        }

        fun matches(other: MetadataSnapshot): Boolean =
            version == other.version || (partitionsPerTopic == other.partitionsPerTopic)

        override fun toString(): String = "(version$version: $partitionsPerTopic)"
    }

    private class PartitionRackInfo(clientRack: String?, partition: PartitionInfo) {

        private val racks: Set<String?>

        init {
            racks = if (clientRack != null && partition.replicas.isNotEmpty()) {
                partition.replicas.map { node -> node.rack }.toSet()
            } else emptySet()
        }

        override fun equals(other: Any?): Boolean {
            if (this === other) return true
            if (other !is PartitionRackInfo) return false
            return racks == other.racks
        }

        override fun hashCode(): Int = racks.hashCode()

        override fun toString(): String = if (racks.isEmpty()) "NO_RACKS" else "racks=$racks"
    }

    private data class OffsetCommitCompletion(
        private val callback: OffsetCommitCallback?,
        private val offsets: Map<TopicPartition, OffsetAndMetadata>,
        private val exception: Exception?,
    ) {
        operator fun invoke() {
            callback?.onComplete(offsets, exception)
        }
    }

    fun poll(timer: Timer): Boolean = poll(timer, true)

    private data class PendingCommittedOffsetRequest(
        private val requestedPartitions: Set<TopicPartition>,
        private val requestedGeneration: Generation?,
        val response: RequestFuture<Map<TopicPartition, OffsetAndMetadata?>>,
    ) {
        fun sameRequest(
            currentRequest: Set<TopicPartition>,
            currentGeneration: Generation?,
        ): Boolean =
            requestedGeneration == currentGeneration && requestedPartitions == currentRequest
    }

    companion object {
        private val COMPARATOR = TopicPartitionComparator()
    }
}
