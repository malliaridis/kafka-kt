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
import java.nio.ByteBuffer
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference
import org.apache.kafka.clients.ClientResponse
import org.apache.kafka.clients.GroupRebalanceConfig
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.Node
import org.apache.kafka.common.errors.AuthenticationException
import org.apache.kafka.common.errors.DisconnectException
import org.apache.kafka.common.errors.FencedInstanceIdException
import org.apache.kafka.common.errors.GroupAuthorizationException
import org.apache.kafka.common.errors.GroupMaxSizeReachedException
import org.apache.kafka.common.errors.IllegalGenerationException
import org.apache.kafka.common.errors.InterruptException
import org.apache.kafka.common.errors.MemberIdRequiredException
import org.apache.kafka.common.errors.RebalanceInProgressException
import org.apache.kafka.common.errors.RetriableException
import org.apache.kafka.common.errors.UnknownMemberIdException
import org.apache.kafka.common.message.FindCoordinatorRequestData
import org.apache.kafka.common.message.HeartbeatRequestData
import org.apache.kafka.common.message.JoinGroupRequestData
import org.apache.kafka.common.message.JoinGroupRequestData.JoinGroupRequestProtocolCollection
import org.apache.kafka.common.message.JoinGroupResponseData.JoinGroupResponseMember
import org.apache.kafka.common.message.LeaveGroupRequestData.MemberIdentity
import org.apache.kafka.common.message.SyncGroupRequestData
import org.apache.kafka.common.message.SyncGroupRequestData.SyncGroupRequestAssignment
import org.apache.kafka.common.metrics.Measurable
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.metrics.Sensor
import org.apache.kafka.common.metrics.stats.Avg
import org.apache.kafka.common.metrics.stats.CumulativeCount
import org.apache.kafka.common.metrics.stats.CumulativeSum
import org.apache.kafka.common.metrics.stats.Max
import org.apache.kafka.common.metrics.stats.Meter
import org.apache.kafka.common.metrics.stats.Rate
import org.apache.kafka.common.metrics.stats.WindowedCount
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.FindCoordinatorRequest
import org.apache.kafka.common.requests.FindCoordinatorRequest.CoordinatorType
import org.apache.kafka.common.requests.FindCoordinatorResponse
import org.apache.kafka.common.requests.HeartbeatRequest
import org.apache.kafka.common.requests.HeartbeatResponse
import org.apache.kafka.common.requests.JoinGroupRequest
import org.apache.kafka.common.requests.JoinGroupResponse
import org.apache.kafka.common.requests.LeaveGroupRequest
import org.apache.kafka.common.requests.LeaveGroupResponse
import org.apache.kafka.common.requests.OffsetCommitRequest
import org.apache.kafka.common.requests.SyncGroupRequest
import org.apache.kafka.common.requests.SyncGroupResponse
import org.apache.kafka.common.utils.KafkaThread
import org.apache.kafka.common.utils.LogContext
import org.apache.kafka.common.utils.Time
import org.apache.kafka.common.utils.Timer
import org.apache.kafka.common.utils.Utils
import org.slf4j.Logger
import kotlin.math.max

/**
 * AbstractCoordinator implements group management for a single group member by interacting with a
 * designated Kafka broker (the coordinator). Group semantics are provided by extending this class.
 * See [ConsumerCoordinator] for example usage.
 *
 * From a high level, Kafka's group management protocol consists of the following sequence of
 * actions:
 *
 * 1. Group Registration: Group members register with the coordinator providing their own metadata
 *    (such as the set of topics they are interested in).
 * 2. Group/Leader Selection: The coordinator select the members of the group and chooses one member
 *    as the leader.
 * 3. State Assignment: The leader collects the metadata from all the members of the group and
 *    assigns state.
 * 4. Group Stabilization: Each member receives the state assigned by the leader and begins
 *    processing.
 *
 * To leverage this protocol, an implementation must define the format of metadata provided by each
 * member for group registration in [metadata] and the format of the state assignment provided by
 * the leader in [onLeaderElected] and becomes available to members in [onJoinComplete].
 *
 * Note on locking: this class shares state between the caller and a background thread which is
 * used for sending heartbeats after the client has joined the group. All mutable state as well as
 * state transitions are protected with the class's monitor. Generally this means acquiring the lock
 * before reading or writing the state of the group (e.g. generation, memberId) and holding the lock
 * when sending a request that affects the state of the group (e.g. JoinGroup, LeaveGroup).
 */
abstract class AbstractCoordinator(
    private val rebalanceConfig: GroupRebalanceConfig,
    logContext: LogContext,
    protected val client: ConsumerNetworkClient,
    metrics: Metrics,
    metricGrpPrefix: String,
    protected val time: Time,
) : Closeable {

    private val log: Logger = logContext.logger(this.javaClass)

    private val heartbeat: Heartbeat = Heartbeat(rebalanceConfig, time)

    private val sensors: GroupCoordinatorMetrics = GroupCoordinatorMetrics(metrics, metricGrpPrefix)

    private var coordinator: Node? = null

    private var rejoinReason = ""

    private var rejoinNeeded = true

    private var needsJoinPrepare = true

    private var heartbeatThread: HeartbeatThread? = null

    private var joinFuture: RequestFuture<ByteBuffer>? = null

    private var findCoordinatorFuture: RequestFuture<Unit>? = null

    @Volatile
    private var fatalFindCoordinatorException: RuntimeException? = null

    private var generation = Generation.NO_GENERATION

    private var lastRebalanceStartMs = -1L

    private var lastRebalanceEndMs = -1L

    // starting logging a warning only after unable to connect for a while
    private var lastTimeOfConnectionMs = -1L

    protected var state = MemberState.UNJOINED

    init {
        requireNotNull(rebalanceConfig.groupId) {
            "Expected a non-null group id for coordinator construction"
        }
    }

    /**
     * Unique identifier for the class of supported protocols (e.g. "consumer" or "connect").
     *
     * @return Non-null protocol type name
     */
    protected abstract fun protocolType(): String

    /**
     * Get the current list of protocols and their associated metadata supported by the local
     * member. The order of the protocols in the list indicates the preference of the protocol (the
     * first entry is the most preferred). The coordinator takes this preference into account when
     * selecting the generation protocol (generally more preferred protocols will be selected as
     * long as all members support them and there is no disagreement on the preference).
     *
     * @return Non-empty map of supported protocols and metadata
     */
    protected abstract fun metadata(): JoinGroupRequestProtocolCollection

    /**
     * Invoked prior to each group join or rejoin. This is typically used to perform any cleanup
     * from the previous generation (such as committing offsets for the consumer).
     *
     * @param timer Timer bounding how long this method can block
     * @param generation The previous generation or -1 if there was none
     * @param memberId The identifier of this member in the previous group or "" if there was none
     * @return `true` If `onJoinPrepare` async commit succeeded, `false` otherwise
     */
    protected abstract fun onJoinPrepare(
        timer: Timer,
        generation: Int = -1,
        memberId: String = "",
    ): Boolean

    /**
     * Invoked when the leader is elected. This is used by the leader to perform the assignment if
     * necessary and to push state to all the members of the group (e.g. to push partition
     * assignments in the case of the new consumer).
     *
     * @param leaderId The id of the leader (which is this member)
     * @param protocol The protocol selected by the coordinator
     * @param allMemberMetadata Metadata from all members of the group
     * @param skipAssignment `true` if leader must skip running the assignor
     * @return A map from each member to their state assignment
     */
    protected abstract fun onLeaderElected(
        leaderId: String,
        protocol: String,
        allMemberMetadata: List<JoinGroupResponseMember>,
        skipAssignment: Boolean,
    ): Map<String, ByteBuffer>

    /**
     * Invoked when a group member has successfully joined a group. If this call fails with an
     * exception, then it will be retried using the same assignment state on the next call to
     * [ensureActiveGroup].
     *
     * @param generation The generation that was joined
     * @param memberId The identifier for the local member in the group
     * @param protocol The protocol selected by the coordinator
     * @param memberAssignment The assignment propagated from the group leader
     */
    protected abstract fun onJoinComplete(
        generation: Int,
        memberId: String,
        protocol: String?,
        memberAssignment: ByteBuffer,
    )

    /**
     * Invoked prior to each leave group event. This is typically used to cleanup assigned
     * partitions; note it is triggered by the consumer's API caller thread (i.e. background
     * heartbeat thread would not trigger it even if it tries to force leaving group upon heartbeat
     * session expiration).
     */
    protected open fun onLeavePrepare() = Unit

    /**
     * Ensure that the coordinator is ready to receive requests.
     *
     * @param timer Timer bounding how long this method can block
     * @return `true` If coordinator discovery and initial connection succeeded, `false` otherwise
     */
    @Synchronized
    fun ensureCoordinatorReady(timer: Timer): Boolean {
        return ensureCoordinatorReady(timer, false)
    }

    /**
     * Ensure that the coordinator is ready to receive requests. This will return immediately
     * without blocking. It is intended to be called in an asynchronous context when wakeups are not
     * expected.
     *
     * @return `true` if coordinator discovery and initial connection succeeded, `false` otherwise
     */
    @Synchronized
    fun ensureCoordinatorReadyAsync(): Boolean {
        return ensureCoordinatorReady(time.timer(0), true)
    }

    @Synchronized
    private fun ensureCoordinatorReady(timer: Timer, disableWakeup: Boolean): Boolean {
        if (!coordinatorUnknown()) return true
        do {
            fatalFindCoordinatorException?.let { exception ->
                fatalFindCoordinatorException = null
                throw exception
            }

            val future = lookupCoordinator()
            client.poll(future, timer, disableWakeup)
            if (!future.isDone) {
                // ran out of time
                break
            }
            var fatalException: RuntimeException? = null
            if (future.failed()) {
                if (future.isRetriable) {
                    log.debug(
                        "Coordinator discovery failed, refreshing metadata",
                        future.exception()
                    )
                    client.awaitMetadataUpdate(timer)
                } else {
                    fatalException = future.exception()
                    log.info("FindCoordinator request hit fatal exception", fatalException)
                }
            } else if (coordinator?.let { client.isUnavailable(it) } == true) {
                // we found the coordinator, but the connection has failed, so mark it dead and
                // backoff before retrying discovery
                markCoordinatorUnknown("coordinator unavailable")
                timer.sleep(rebalanceConfig.retryBackoffMs!!)
            }
            clearFindCoordinatorFuture()
            if (fatalException != null) throw fatalException
        } while (coordinatorUnknown() && timer.isNotExpired)
        return !coordinatorUnknown()
    }

    @Synchronized
    fun lookupCoordinator(): RequestFuture<Unit> {
        if (findCoordinatorFuture == null) {
            // find a node to ask about the coordinator
            val node = client.leastLoadedNode()
            if (node == null) {
                log.debug("No broker available to send FindCoordinator request")
                return RequestFuture.noBrokersAvailable()
            } else findCoordinatorFuture = sendFindCoordinatorRequest(node)
        }
        return findCoordinatorFuture!!
    }

    @Synchronized
    private fun clearFindCoordinatorFuture() {
        findCoordinatorFuture = null
    }

    /**
     * Check whether the group should be rejoined (e.g. if metadata changes) or whether a rejoin
     * request is already in flight and needs to be completed.
     *
     * @return `true` if it should, `false` otherwise
     */
    @Synchronized
    open fun rejoinNeededOrPending(): Boolean {
        // if there's a pending joinFuture, we should try to complete handling it.
        return rejoinNeeded || joinFuture != null
    }

    /**
     * Check the status of the heartbeat thread (if it is active) and indicate the liveness of the
     * client. This must be called periodically after joining with [ensureActiveGroup] to ensure
     * that the member stays in the group. If an interval of time longer than the provided rebalance
     * timeout expires without calling this method, then the client will proactively leave the
     * group.
     *
     * @param now current time in milliseconds
     * @throws RuntimeException for unexpected errors raised from the heartbeat thread
     */
    @Synchronized
    fun pollHeartbeat(now: Long) {
        heartbeatThread?.let { thread ->
            if (thread.hasFailed()) {
                // set the heartbeat thread to null and raise an exception. If the user catches it,
                // the next call to ensureActiveGroup() will spawn a new heartbeat thread.
                heartbeatThread = null
                throw thread.failureCause()!!
            }
            // Awake the heartbeat thread if needed
            if (heartbeat.shouldHeartbeat(now)) (this as Object).notify()
            heartbeat.poll(now)
        }
    }

    @Synchronized
    protected fun timeToNextHeartbeat(now: Long): Long {
        // if we have not joined the group or we are preparing rebalance, we don't need to send
        // heartbeats
        return if (state.hasNotJoinedGroup()) Long.MAX_VALUE
        else heartbeat.timeToNextHeartbeat(now)
    }

    /**
     * Ensure that the group is active (i.e. joined and synced)
     */
    fun ensureActiveGroup() {
        while (!ensureActiveGroup(time.timer(Long.MAX_VALUE)))
            log.warn("still waiting to ensure active group")
    }

    /**
     * Ensure the group is active (i.e., joined and synced)
     *
     * @param timer Timer bounding how long this method can block
     * @throws KafkaException if the callback throws exception
     * @return true iff the group is active
     */
    fun ensureActiveGroup(timer: Timer): Boolean {
        // always ensure that the coordinator is ready because we may have been disconnected when
        // sending heartbeats and does not necessarily require us to rejoin the group.
        if (!ensureCoordinatorReady(timer)) return false

        startHeartbeatThreadIfNeeded()
        return joinGroupIfNeeded(timer)
    }

    @Synchronized
    private fun startHeartbeatThreadIfNeeded() {
        if (heartbeatThread == null) heartbeatThread = HeartbeatThread().apply { start() }
    }

    private fun closeHeartbeatThread() {
        var thread: HeartbeatThread
        synchronized(this) {
            val hbThread = heartbeatThread ?: return
            hbThread.close()
            thread = hbThread
            heartbeatThread = null
        }
        try {
            thread.join()
        } catch (e: InterruptedException) {
            log.warn("Interrupted while waiting for consumer heartbeat thread to close")
            throw InterruptException(cause = e)
        }
    }

    /**
     * Joins the group without starting the heartbeat thread.
     *
     * If this function returns `true`, the state must always be in `STABLE` and heartbeat enabled.
     *
     * If this function returns `false`, the state can be in one of the following:
     * - `UNJOINED`: got error response but times out before being able to re-join, heartbeat
     *   disabled
     * - `PREPARING_REBALANCE`: not yet received join-group response before timeout, heartbeat
     *   disabled
     * - `COMPLETING_REBALANCE`: not yet received sync-group response before timeout, heartbeat
     *   enabled
     *
     * Visible for testing.
     *
     * @param timer Timer bounding how long this method can block
     * @throws KafkaException if the callback throws exception
     * @return true iff the operation succeeded
     */
    fun joinGroupIfNeeded(timer: Timer): Boolean {
        while (rejoinNeededOrPending()) {
            if (!ensureCoordinatorReady(timer)) return false

            // call onJoinPrepare if needed. We set a flag to make sure that we do not call it a
            // second time if the client is woken up before a pending rebalance completes. This must
            // be called on each iteration of the loop because an event requiring a rebalance (such
            // as a metadata refresh which changes the matched subscription set) can occur while
            // another rebalance is still in progress.
            if (needsJoinPrepare) {
                // need to set the flag before calling onJoinPrepare since the user callback may
                // throw exception, in which case upon retry we should not retry onJoinPrepare
                // either.
                needsJoinPrepare = false
                // return false when onJoinPrepare is waiting for committing offset
                if (!onJoinPrepare(timer, generation.generationId, generation.memberId)) {
                    needsJoinPrepare = true
                    //should not initiateJoinGroup if needsJoinPrepare still is true
                    return false
                }
            }
            val future = initiateJoinGroup()
            client.poll(future, timer)
            if (!future.isDone) {
                // we ran out of time
                return false
            }
            if (future.succeeded()) {
                var generationSnapshot: Generation
                var stateSnapshot: MemberState

                // Generation data maybe concurrently cleared by Heartbeat thread. Can't use
                // synchronized for [onJoinComplete], because it can be long enough and
                // shouldn't block heartbeat thread.
                // See [PlaintextConsumerTest.testMaxPollIntervalMsDelayInAssignment]
                synchronized(this@AbstractCoordinator) {
                    generationSnapshot = generation
                    stateSnapshot = state
                }
                if (!hasGenerationReset(generationSnapshot) && stateSnapshot == MemberState.STABLE) {
                    // Duplicate the buffer in case `onJoinComplete` does not complete and needs to
                    // be retried.
                    val memberAssignment = future.value().duplicate()
                    onJoinComplete(
                        generationSnapshot.generationId,
                        generationSnapshot.memberId,
                        generationSnapshot.protocolName,
                        memberAssignment,
                    )

                    // Generally speaking we should always resetJoinGroupFuture once the future is
                    // done, but here we can only reset the join group future after the completion
                    // callback returns. This ensures that if the callback is woken up, we will
                    // retry it on the next joinGroupIfNeeded. And because of that we should
                    // explicitly trigger resetJoinGroupFuture in other conditions below.
                    resetJoinGroupFuture()
                    needsJoinPrepare = true
                } else {
                    val reason = "rebalance failed since the generation/state was modified by " +
                            "heartbeat thread to $generationSnapshot/$stateSnapshot before the " +
                            "rebalance callback triggered"
                    resetStateAndRejoin(reason, true)
                    resetJoinGroupFuture()
                }
            } else {
                val exception = future.exception()
                resetJoinGroupFuture()
                synchronized(this@AbstractCoordinator) {
                    val simpleName: String = exception.javaClass.simpleName
                    requestRejoin(
                        shortReason = "rebalance failed due to $$simpleName",
                        fullReason = "rebalance failed due to '${exception.message}' ($simpleName)",
                    )
                }
                if (
                    exception is UnknownMemberIdException
                    || exception is IllegalGenerationException
                    || exception is RebalanceInProgressException
                    || exception is MemberIdRequiredException
                ) continue
                else if (!future.isRetriable) throw exception

                timer.sleep(rebalanceConfig.retryBackoffMs!!)
            }
        }
        return true
    }

    @Synchronized
    private fun resetJoinGroupFuture() {
        joinFuture = null
    }

    @Synchronized
    private fun initiateJoinGroup(): RequestFuture<ByteBuffer> {
        // we store the join future in case we are woken up by the user after beginning the
        // rebalance in the call to poll below. This ensures that we do not mistakenly attempt to
        // rejoin before the pending rebalance has completed.
        return joinFuture ?: run {
            state = MemberState.PREPARING_REBALANCE
            // a rebalance can be triggered consecutively if the previous one failed, in this case
            // we would not update the start time.
            if (lastRebalanceStartMs == -1L) lastRebalanceStartMs = time.milliseconds()
            return sendJoinGroupRequest().apply {
                addListener(object : RequestFutureListener<ByteBuffer> {
                    // do nothing since all the handler logic are in SyncGroupResponseHandler
                    // already
                    override fun onSuccess(value: ByteBuffer) = Unit

                    override fun onFailure(e: RuntimeException) {
                        // we handle failures below after the request finishes. if the join
                        // completes after having been woken up, the exception is ignored and we
                        // will rejoin; this can be triggered when either join or sync request
                        // failed
                        synchronized(this@AbstractCoordinator) { sensors.failedRebalanceSensor.record() }
                    }
                })
                joinFuture = this
            }
        }
    }

    /**
     * Join the group and return the assignment for the next generation. This function handles both
     * JoinGroup and SyncGroup, delegating to [onLeaderElected] if elected leader by the coordinator.
     *
     * NOTE: This is visible only for testing
     *
     * @return A request future which wraps the assignment returned from the group leader
     */
    fun sendJoinGroupRequest(): RequestFuture<ByteBuffer> {
        if (coordinatorUnknown()) return RequestFuture.coordinatorNotAvailable()

        // send a join group request to the coordinator
        log.info("(Re-)joining group")
        val requestBuilder = JoinGroupRequest.Builder(
            JoinGroupRequestData()
                .setGroupId(rebalanceConfig.groupId!!)
                .setSessionTimeoutMs(rebalanceConfig.sessionTimeoutMs)
                .setMemberId(generation.memberId)
                .setGroupInstanceId(rebalanceConfig.groupInstanceId)
                .setProtocolType(protocolType())
                .setProtocols(metadata())
                .setRebalanceTimeoutMs(rebalanceConfig.rebalanceTimeoutMs!!)
                .setReason(JoinGroupRequest.maybeTruncateReason(rejoinReason))
        )
        log.debug("Sending JoinGroup ({}) to coordinator {}", requestBuilder, coordinator)

        // Note that we override the request timeout using the rebalance timeout since that is the
        // maximum time that it may block on the coordinator. We add an extra 5 seconds for small delays.
        val joinGroupTimeoutMs = max(
            client.defaultRequestTimeoutMs(),
            max(
                rebalanceConfig.rebalanceTimeoutMs + JOIN_GROUP_TIMEOUT_LAPSE,
                rebalanceConfig.rebalanceTimeoutMs
            ) // guard against overflow since rebalance timeout can be MAX_VALUE
        )
        return client.send(coordinator!!, requestBuilder, joinGroupTimeoutMs)
            .compose(JoinGroupResponseHandler(generation))
    }

    private inner class JoinGroupResponseHandler(generation: Generation) :
        CoordinatorResponseHandler<JoinGroupResponse, ByteBuffer>(generation) {
        override fun handle(response: JoinGroupResponse, future: RequestFuture<ByteBuffer>) {
            val error = response.error()
            if (error === Errors.NONE) {
                if (isProtocolTypeInconsistent(response.data().protocolType)) {
                    log.error(
                        "JoinGroup failed: Inconsistent Protocol Type, received {} but expected {}",
                        response.data().protocolType,
                        protocolType(),
                    )
                    future.raise(Errors.INCONSISTENT_GROUP_PROTOCOL)
                } else {
                    log.debug("Received successful JoinGroup response: {}", response)
                    sensors.joinSensor.record(this.response!!.requestLatencyMs.toDouble())

                    synchronized(this@AbstractCoordinator) {
                        if (state != MemberState.PREPARING_REBALANCE) {
                            // if the consumer was woken up before a rebalance completes, we may
                            // have already left the group. In this case, we do not want to continue
                            // with the sync group.
                            future.raise(UnjoinedGroupException())
                        } else {
                            state = MemberState.COMPLETING_REBALANCE

                            // we only need to enable heartbeat thread whenever we transit to
                            // COMPLETING_REBALANCE state since we always transit from this state to
                            // STABLE
                            if (heartbeatThread != null) heartbeatThread!!.enable()
                            generation = Generation(
                                generationId = response.data().generationId,
                                memberId = response.data().memberId,
                                protocolName = response.data().protocolName,
                            )
                            log.info(
                                "Successfully joined group with generation {}",
                                generation
                            )
                            if (response.isLeader) onLeaderElected(response).chain(future)
                            else onJoinFollower().chain(future)
                        }
                    }
                }
            } else if (error === Errors.COORDINATOR_LOAD_IN_PROGRESS) {
                log.info("JoinGroup failed: Coordinator {} is loading the group.", coordinator())
                // backoff and retry
                future.raise(error)
            } else if (error === Errors.UNKNOWN_MEMBER_ID) {
                log.info(
                    "JoinGroup failed: {} Need to re-join the group. Sent generation was {}",
                    error.message,
                    sentGeneration
                )
                // only need to reset the member id if generation has not been changed,
                // then retry immediately
                if (generationUnchanged()) resetStateOnResponseError(
                    ApiKeys.JOIN_GROUP,
                    error,
                    true
                )
                future.raise(error)
            } else if (
                error === Errors.COORDINATOR_NOT_AVAILABLE
                || error === Errors.NOT_COORDINATOR
            ) {
                // re-discover the coordinator and retry with backoff
                markCoordinatorUnknown(error)
                log.info(
                    "JoinGroup failed: {} Marking coordinator unknown. Sent generation was {}",
                    error.message, sentGeneration
                )
                future.raise(error)
            } else if (error === Errors.FENCED_INSTANCE_ID) {
                // for join-group request, even if the generation has changed we would not expect
                // the instance id gets fenced, and hence we always treat this as a fatal error
                log.error(
                    "JoinGroup failed: The group instance id {} has been fenced by another " +
                            "instance. Sent generation was {}",
                    rebalanceConfig.groupInstanceId,
                    sentGeneration
                )
                future.raise(error)
            } else if (
                error === Errors.INCONSISTENT_GROUP_PROTOCOL
                || error === Errors.INVALID_SESSION_TIMEOUT
                || error === Errors.INVALID_GROUP_ID
                || error === Errors.GROUP_AUTHORIZATION_FAILED
                || error === Errors.GROUP_MAX_SIZE_REACHED
            ) {
                // log the error and re-throw the exception
                log.error("JoinGroup failed due to fatal error: {}", error.message)
                if (error === Errors.GROUP_MAX_SIZE_REACHED) {
                    future.raise(
                        GroupMaxSizeReachedException(
                            "Consumer group ${rebalanceConfig.groupId} already has the " +
                                    "configured maximum number of members."
                        )
                    )
                } else if (error === Errors.GROUP_AUTHORIZATION_FAILED)
                    future.raise(GroupAuthorizationException(groupId = rebalanceConfig.groupId))
                else future.raise(error)
            } else if (error === Errors.UNSUPPORTED_VERSION) {
                log.error(
                    "JoinGroup failed due to unsupported version error. Please unset field " +
                            "group.instance.id and retry to see if the problem resolves"
                )
                future.raise(error)
            } else if (error === Errors.MEMBER_ID_REQUIRED) {
                // Broker requires a concrete member id to be allowed to join the group. Update
                // member id and send another join group request in next cycle.
                val memberId = response.data().memberId
                log.debug(
                    "JoinGroup failed due to non-fatal error: {}. Will set the member id as {} " +
                            "and then rejoin. Sent generation was {}",
                    error,
                    memberId,
                    sentGeneration,
                )
                synchronized(this@AbstractCoordinator) {
                    generation = Generation(
                        OffsetCommitRequest.DEFAULT_GENERATION_ID,
                        memberId,
                        null
                    )
                }
                requestRejoin("need to re-join with the given member-id: $memberId")
                future.raise(error)
            } else if (error === Errors.REBALANCE_IN_PROGRESS) {
                log.info(
                    "JoinGroup failed due to non-fatal error: REBALANCE_IN_PROGRESS, " +
                            "which could indicate a replication timeout on the broker. Will retry."
                )
                future.raise(error)
            } else {
                // unexpected error, throw the exception
                log.error("JoinGroup failed due to unexpected error: {}", error.message)
                future.raise(
                    KafkaException("Unexpected error in join group response: " + error.message)
                )
            }
        }
    }

    private fun onJoinFollower(): RequestFuture<ByteBuffer> {
        // send follower's sync group with an empty assignment
        val requestBuilder = SyncGroupRequest.Builder(
            SyncGroupRequestData()
                .setGroupId(rebalanceConfig.groupId!!)
                .setMemberId(generation.memberId)
                .setProtocolType(protocolType())
                .setProtocolName(generation.protocolName)
                .setGroupInstanceId(rebalanceConfig.groupInstanceId)
                .setGenerationId(generation.generationId)
                .setAssignments(emptyList<SyncGroupRequestAssignment>())
        )
        log.debug("Sending follower SyncGroup to coordinator {}: {}", coordinator, requestBuilder)
        return sendSyncGroupRequest(requestBuilder)
    }

    private fun onLeaderElected(joinResponse: JoinGroupResponse): RequestFuture<ByteBuffer> {
        try {
            // perform the leader synchronization and send back the assignment for the group
            val groupAssignment = onLeaderElected(
                joinResponse.data().leader,
                joinResponse.data().protocolName!!,
                joinResponse.data().members,
                joinResponse.data().skipAssignment,
            )

            val groupAssignmentList = groupAssignment.map { (key, value) ->
                SyncGroupRequestAssignment()
                    .setMemberId(key)
                    .setAssignment(Utils.toArray(value))
            }

            val requestBuilder = SyncGroupRequest.Builder(
                SyncGroupRequestData()
                    .setGroupId(rebalanceConfig.groupId!!)
                    .setMemberId(generation.memberId)
                    .setProtocolType(protocolType())
                    .setProtocolName(generation.protocolName)
                    .setGroupInstanceId(rebalanceConfig.groupInstanceId)
                    .setGenerationId(generation.generationId)
                    .setAssignments(groupAssignmentList)
            )
            log.debug("Sending leader SyncGroup to coordinator {}: {}", coordinator, requestBuilder)
            return sendSyncGroupRequest(requestBuilder)
        } catch (e: RuntimeException) {
            return RequestFuture.failure(e)
        }
    }

    private fun sendSyncGroupRequest(
        requestBuilder: SyncGroupRequest.Builder,
    ): RequestFuture<ByteBuffer> {
        return if (coordinatorUnknown()) RequestFuture.coordinatorNotAvailable()
        else client.send(coordinator!!, requestBuilder)
            .compose(SyncGroupResponseHandler(generation))
    }

    private fun hasGenerationReset(gen: Generation): Boolean {
        // the member ID might not be reset for ILLEGAL_GENERATION error, so only check generationID
        // and protocol name here
        return gen.generationId == Generation.NO_GENERATION.generationId && gen.protocolName == null
    }

    private inner class SyncGroupResponseHandler(
        generation: Generation,
    ) : CoordinatorResponseHandler<SyncGroupResponse, ByteBuffer>(generation) {

        override fun handle(response: SyncGroupResponse, future: RequestFuture<ByteBuffer>) {
            when (val error = response.error()) {
                Errors.NONE -> {
                    if (isProtocolTypeInconsistent(response.data().protocolType)) {
                        log.error(
                            "SyncGroup failed due to inconsistent Protocol Type, received {} but " +
                                    "expected {}",
                            response.data().protocolType,
                            protocolType(),
                        )
                        future.raise(Errors.INCONSISTENT_GROUP_PROTOCOL)
                    } else {
                        log.debug("Received successful SyncGroup response: {}", response)
                        sensors.syncSensor.record(this.response!!.requestLatencyMs.toDouble())
                        synchronized(this@AbstractCoordinator) {
                            if (
                                !hasGenerationReset(sentGeneration)
                                && state == MemberState.COMPLETING_REBALANCE
                            ) {
                                // check protocol name only if the generation is not reset
                                val protocolName = response.data().protocolName
                                val protocolNameInconsistent =
                                    protocolName != null && protocolName != sentGeneration.protocolName

                                if (protocolNameInconsistent) {
                                    log.error(
                                        "SyncGroup failed due to inconsistent Protocol Name, " +
                                                "received {} but expected {}",
                                        protocolName,
                                        sentGeneration.protocolName,
                                    )
                                    future.raise(Errors.INCONSISTENT_GROUP_PROTOCOL)
                                } else {
                                    log.info(
                                        "Successfully synced group in generation {}",
                                        sentGeneration,
                                    )
                                    state = MemberState.STABLE
                                    rejoinReason = ""
                                    rejoinNeeded = false
                                    // record rebalance latency
                                    lastRebalanceEndMs = time.milliseconds()
                                    sensors.successfulRebalanceSensor.record(
                                        (lastRebalanceEndMs - lastRebalanceStartMs).toDouble(),
                                    )
                                    lastRebalanceStartMs = -1L
                                    future.complete(ByteBuffer.wrap(response.data().assignment))
                                }
                            } else {
                                log.info(
                                    "Generation data was cleared by heartbeat thread to {} and " +
                                            "state is now {} before receiving SyncGroup " +
                                            "response, marking this rebalance as failed and retry",
                                    sentGeneration,
                                    state,
                                )
                                // use ILLEGAL_GENERATION error code to let it retry immediately
                                future.raise(Errors.ILLEGAL_GENERATION)
                            }
                        }
                    }
                }

                Errors.GROUP_AUTHORIZATION_FAILED -> {
                    future.raise(GroupAuthorizationException(groupId = rebalanceConfig.groupId))
                }

                Errors.REBALANCE_IN_PROGRESS -> {
                    log.info(
                        "SyncGroup failed: The group began another rebalance. Need to re-join " +
                                "the group. Sent generation was {}",
                        sentGeneration,
                    )
                    future.raise(error)
                }

                Errors.FENCED_INSTANCE_ID -> {
                    // for sync-group request, even if the generation has changed we would not
                    // expect the instance id gets fenced, and hence we always treat this as a fatal
                    // error
                    log.error(
                        "SyncGroup failed: The group instance id {} has been fenced by another " +
                                "instance. Sent generation was {}",
                        rebalanceConfig.groupInstanceId,
                        sentGeneration
                    )
                    future.raise(error)
                }

                Errors.UNKNOWN_MEMBER_ID, Errors.ILLEGAL_GENERATION -> {
                    log.info(
                        "SyncGroup failed: {} Need to re-join the group. Sent generation was {}",
                        error.message,
                        sentGeneration,
                    )
                    if (generationUnchanged()) {
                        // don't reset generation member ID when ILLEGAL_GENERATION, since the
                        // member ID might still be valid
                        resetStateOnResponseError(
                            api = ApiKeys.SYNC_GROUP,
                            error = error,
                            shouldResetMemberId = error !== Errors.ILLEGAL_GENERATION,
                        )
                    }
                    future.raise(error)
                }

                Errors.COORDINATOR_NOT_AVAILABLE, Errors.NOT_COORDINATOR -> {
                    log.info(
                        "SyncGroup failed: {} Marking coordinator unknown. Sent generation was {}",
                        error.message,
                        sentGeneration,
                    )
                    markCoordinatorUnknown(error)
                    future.raise(error)
                }

                else -> future.raise(
                    KafkaException("Unexpected error from SyncGroup: " + error.message)
                )
            }
        }
    }

    /**
     * Discover the current coordinator for the group. Sends a FindCoordinator request to the given
     * broker node. The returned future should be polled to get the result of the request.
     *
     * @return A request future which indicates the completion of the metadata request
     */
    private fun sendFindCoordinatorRequest(node: Node): RequestFuture<Unit> {
        log.debug("Sending FindCoordinator request to broker {}", node)
        val data = FindCoordinatorRequestData()
            .setKeyType(CoordinatorType.GROUP.id)
            .setKey(rebalanceConfig.groupId!!)

        val requestBuilder = FindCoordinatorRequest.Builder(data)
        return client.send(node, requestBuilder).compose(FindCoordinatorResponseHandler())
    }

    private inner class FindCoordinatorResponseHandler
        : RequestFutureAdapter<ClientResponse, Unit>() {

        override fun onSuccess(value: ClientResponse, future: RequestFuture<Unit>) {
            log.debug("Received FindCoordinator response {}", value)
            val coordinators = (value.responseBody as FindCoordinatorResponse).coordinators()
            if (coordinators.size != 1) {
                log.error(
                    "Group coordinator lookup failed: Invalid response containing more than a " +
                            "single coordinator"
                )
                future.raise(
                    IllegalStateException(
                        "Group coordinator lookup failed: Invalid response containing more than " +
                                "a single coordinator"
                    )
                )
            }
            val coordinatorData = coordinators[0]
            val error = Errors.forCode(coordinatorData.errorCode)
            if (error === Errors.NONE) {
                synchronized(this@AbstractCoordinator) {

                    // use MAX_VALUE - node.id as the coordinator id to allow separate connections
                    // for the coordinator in the underlying network client layer
                    val coordinatorConnectionId = Int.MAX_VALUE - coordinatorData.nodeId
                    coordinator = Node(
                        id = coordinatorConnectionId,
                        host = coordinatorData.host,
                        port = coordinatorData.port,
                    ).also {
                        log.info("Discovered group coordinator {}", it)
                        client.tryConnect(it)
                    }

                    heartbeat.resetSessionTimeout()
                }
                future.complete(Unit)
            } else if (error === Errors.GROUP_AUTHORIZATION_FAILED) {
                future.raise(GroupAuthorizationException(groupId = rebalanceConfig.groupId))
            } else {
                log.debug("Group coordinator lookup failed: {}", coordinatorData.errorMessage)
                future.raise(error)
            }
        }

        override fun onFailure(e: RuntimeException, future: RequestFuture<Unit>) {
            log.debug("FindCoordinator request failed due to {}", e.toString())
            if (e !is RetriableException) {
                // Remember the exception if fatal so we can ensure it gets thrown by the main
                // thread
                fatalFindCoordinatorException = e
            }
            super.onFailure(e, future)
        }
    }

    /**
     * Check if we know who the coordinator is and we have an active connection
     *
     * @return true if the coordinator is unknown
     */
    fun coordinatorUnknown(): Boolean = checkAndGetCoordinator() == null

    /**
     * Get the coordinator if its connection is still active. Otherwise mark it unknown and
     * return `null`.
     *
     * @return the current coordinator or `null` if it is unknown
     */
    @Synchronized
    fun checkAndGetCoordinator(): Node? {
        coordinator?.let {
            if (client.isUnavailable(it)) markCoordinatorUnknown(
                isDisconnected = true,
                cause = "coordinator unavailable",
            )
        }
        return coordinator
    }

    @Synchronized
    private fun coordinator(): Node? = coordinator

    @Synchronized
    protected fun markCoordinatorUnknown(error: Errors) {
        markCoordinatorUnknown(
            isDisconnected = false,
            cause = "error response " + error.name,
        )
    }

    @Synchronized
    fun markCoordinatorUnknown(cause: String?) {
        markCoordinatorUnknown(
            isDisconnected = false,
            cause = cause,
        )
    }

    protected fun markCoordinatorUnknown(isDisconnected: Boolean, cause: String?) =
        synchronized(this@AbstractCoordinator) {
            coordinator?.let { oldCoordinator ->
                log.info(
                    "Group coordinator {} is unavailable or invalid due to cause: {}. " +
                            "isDisconnected: {}. Rediscovery will be attempted.",
                    oldCoordinator,
                    cause,
                    isDisconnected,
                )

                // Mark the coordinator dead before disconnecting requests since the callbacks for any
                // pending requests may attempt to do likewise. This also prevents new requests from
                // being sent to the coordinator while the disconnect is in progress.
                this.coordinator = null

                // Disconnect from the coordinator to ensure that there are no in-flight requests
                // remaining. Pending callbacks will be invoked with a DisconnectException on the next
                // call to poll.
                if (!isDisconnected) {
                    log.info("Requesting disconnect from last known coordinator {}", oldCoordinator)
                    client.disconnectAsync(oldCoordinator)
                }
                lastTimeOfConnectionMs = time.milliseconds()
            } ?: run {
                val durationOfOngoingDisconnect = time.milliseconds() - lastTimeOfConnectionMs
                if (durationOfOngoingDisconnect > rebalanceConfig.rebalanceTimeoutMs!!) log.warn(
                    "Consumer has been disconnected from the group coordinator for {}ms",
                    durationOfOngoingDisconnect
                )
            }
        }

    /**
     * Get the current generation state, regardless of whether it is currently stable. Note that the
     * generation information can be updated while we are still in the middle of a rebalance, after
     * the join-group response is received.
     *
     * @return the current generation
     */
    @Synchronized
    fun generation(): Generation = generation

    /**
     * Get the current generation state if the group is stable, otherwise return `null`.
     *
     * @return the current generation or `null`
     */
    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("generationIfStable"),
    )
    @Synchronized
    fun generationIfStable(): Generation? {
        return if (state != MemberState.STABLE) null else generation
    }

    /**
     * The current generation state if the group is stable, otherwise `null`.
     */
    internal val generationIfStable: Generation?
        @Synchronized
        get() = if (state != MemberState.STABLE) null else generation

    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("rebalanceInProgress"),
    )
    @Synchronized
    protected fun rebalanceInProgress(): Boolean {
        return state == MemberState.PREPARING_REBALANCE || state == MemberState.COMPLETING_REBALANCE
    }

    internal val rebalanceInProgress: Boolean
        @Synchronized
        get() {
            return state == MemberState.PREPARING_REBALANCE || state == MemberState.COMPLETING_REBALANCE
        }


    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("memberId"),
    )
    @Synchronized
    protected fun memberId(): String = generation.memberId

    internal val memberId: String
        @Synchronized
        get() = generation.memberId

    private fun resetStateAndGeneration(reason: String, shouldResetMemberId: Boolean) =
        synchronized(this@AbstractCoordinator) {
            log.info(
                "Resetting generation {}due to: {}",
                if (shouldResetMemberId) "and member id " else "",
                reason
            )
            state = MemberState.UNJOINED
            generation = if (shouldResetMemberId) Generation.NO_GENERATION
            else {
                // keep member id since it might be still valid, to avoid to wait for the old member id
                // leaving group until rebalance timeout in next rebalance
                Generation(
                    generationId = Generation.NO_GENERATION.generationId,
                    memberId = generation.memberId,
                    protocolName = null,
                )
            }
        }

    private fun resetStateAndRejoin(reason: String, shouldResetMemberId: Boolean) =
        synchronized(this@AbstractCoordinator) {
            resetStateAndGeneration(reason, shouldResetMemberId)
            requestRejoin(reason)
            needsJoinPrepare = true
        }

    @Synchronized
    fun resetStateOnResponseError(
        api: ApiKeys?,
        error: Errors?,
        shouldResetMemberId: Boolean,
    ) {
        resetStateAndRejoin(
            reason = "encountered $error from $api response",
            shouldResetMemberId = shouldResetMemberId,
        )
    }

    @Synchronized
    fun resetGenerationOnLeaveGroup() {
        resetStateAndRejoin(
            reason = "consumer pro-actively leaving the group",
            shouldResetMemberId = true,
        )
    }

    @Synchronized
    fun requestRejoinIfNecessary(shortReason: String, fullReason: String) {
        if (!rejoinNeeded) requestRejoin(shortReason, fullReason)
    }

    /**
     * Request to rejoin the group.
     *
     * @param shortReason This is the reason passed up to the group coordinator. It must be
     * reasonably small.
     * @param fullReason This is the reason logged locally.
     */
    @Synchronized
    fun requestRejoin(shortReason: String, fullReason: String = shortReason) {
        log.info("Request joining group due to: {}", fullReason)
        rejoinReason = shortReason
        rejoinNeeded = true
    }

    private fun isProtocolTypeInconsistent(protocolType: String?): Boolean {
        return protocolType != null && protocolType != protocolType()
    }

    /**
     * Close the coordinator, waiting if needed to send LeaveGroup.
     */
    override fun close() = close(time.timer(timeoutMs = 0))

    /**
     * @throws KafkaException if the rebalance callback throws exception
     */
    protected open fun close(timer: Timer) {
        try {
            closeHeartbeatThread()
        } finally {
            // Synchronize after closing the heartbeat thread since heartbeat thread
            // needs this lock to complete and terminate after close flag is set.
            synchronized(this) {
                if (rebalanceConfig.leaveGroupOnClose == true) {
                    onLeavePrepare()
                    maybeLeaveGroup("the consumer is being closed")
                }

                // At this point, there may be pending commits (async commits or sync commits that
                // were interrupted using wakeup) and the leave group request which have been
                // queued, but not yet sent to the broker. Wait up to close timeout for these
                // pending requests to be processed. If coordinator is not known, requests are
                // aborted.
                val coordinator = checkAndGetCoordinator()
                if (coordinator != null && !client.awaitPendingRequests(coordinator, timer))
                    log.warn(
                        "Close timed out with {} pending requests to coordinator, terminating " +
                                "client connections",
                        client.pendingRequestCount(coordinator),
                    )
            }
        }
    }

    /**
     * Sends LeaveGroupRequest and logs the `leaveReason`, unless this member is using static
     * membership or is already not part of the group (ie does not have a valid member id, is in the
     * UNJOINED state, or the coordinator is unknown).
     *
     * @param leaveReason the reason to leave the group for logging
     * @throws KafkaException if the rebalance callback throws exception
     */
    @Synchronized
    fun maybeLeaveGroup(leaveReason: String): RequestFuture<Unit>? {
        var future: RequestFuture<Unit>? = null

        // Starting from 2.3, only dynamic members will send LeaveGroupRequest to the broker,
        // consumer with valid group.instance.id is viewed as static member that never sends LeaveGroup,
        // and the membership expiration is only controlled by session timeout.
        if (
            isDynamicMember
            && !coordinatorUnknown()
            && state != MemberState.UNJOINED
            && generation.hasMemberId()
        ) {
            // this is a minimal effort attempt to leave the group. we do not attempt any resending
            // if the request fails or times out.
            log.info(
                "Member {} sending LeaveGroup request to coordinator {} due to {}",
                generation.memberId,
                coordinator,
                leaveReason,
            )
            val request = LeaveGroupRequest.Builder(
                groupId = rebalanceConfig.groupId!!,
                members = listOf(
                    MemberIdentity()
                        .setMemberId(generation.memberId)
                        .setReason(JoinGroupRequest.maybeTruncateReason(leaveReason))
                )
            )
            future = client.send(coordinator!!, request)
                .compose(LeaveGroupResponseHandler(generation))

            client.pollNoWakeup()
        }
        resetGenerationOnLeaveGroup()
        return future
    }

    val isDynamicMember: Boolean
        get() = rebalanceConfig.groupInstanceId != null

    private inner class LeaveGroupResponseHandler(
        generation: Generation,
    ) : CoordinatorResponseHandler<LeaveGroupResponse, Unit>(generation) {

        override fun handle(response: LeaveGroupResponse, future: RequestFuture<Unit>) {
            val members = response.memberResponses()
            if (members.size > 1) {
                future.raise(
                    IllegalStateException(
                        "The expected leave group response should only contain no more than one " +
                                "member info, however get $members"
                    )
                )
            }
            val error = response.error()
            if (error === Errors.NONE) {
                log.debug(
                    "LeaveGroup response with {} returned successfully: {}",
                    sentGeneration,
                    this.response
                )
                future.complete(Unit)
            } else {
                log.error(
                    "LeaveGroup request with {} failed with error: {}",
                    sentGeneration,
                    error.message,
                )
                future.raise(error)
            }
        }
    }

    // visible for testing
    @Synchronized
    internal fun sendHeartbeatRequest(): RequestFuture<Unit> {
        log.debug(
            "Sending Heartbeat request with generation {} and member id {} to coordinator {}",
            generation.generationId,
            generation.memberId,
            coordinator,
        )
        val requestBuilder = HeartbeatRequest.Builder(
            HeartbeatRequestData()
                .setGroupId(rebalanceConfig.groupId!!)
                .setMemberId(generation.memberId)
                .setGroupInstanceId(rebalanceConfig.groupInstanceId)
                .setGenerationId(generation.generationId)
        )
        return client.send(coordinator!!, requestBuilder)
            .compose(HeartbeatResponseHandler(generation))
    }

    private inner class HeartbeatResponseHandler(
        generation: Generation,
    ) : CoordinatorResponseHandler<HeartbeatResponse, Unit>(generation) {

        override fun handle(response: HeartbeatResponse, future: RequestFuture<Unit>) {
            sensors.heartbeatSensor.record(this.response!!.requestLatencyMs.toDouble())
            val error = response.error()
            if (error === Errors.NONE) {
                log.debug("Received successful Heartbeat response")
                future.complete(Unit)
            } else if ((error === Errors.COORDINATOR_NOT_AVAILABLE
                        || error === Errors.NOT_COORDINATOR)
            ) {
                log.info(
                    "Attempt to heartbeat failed since coordinator {} is either not started or not valid",
                    coordinator()
                )
                markCoordinatorUnknown(error)
                future.raise(error)
            } else if (error === Errors.REBALANCE_IN_PROGRESS) {
                // since we may be sending the request during rebalance, we should check
                // this case and ignore the REBALANCE_IN_PROGRESS error
                synchronized(this@AbstractCoordinator) {
                    if (state == MemberState.STABLE) {
                        requestRejoin("group is already rebalancing")
                        future.raise(error)
                    } else {
                        log.debug(
                            "Ignoring heartbeat response with error {} during {} state",
                            error,
                            state
                        )
                        future.complete(Unit)
                    }
                }
            } else if ((error === Errors.ILLEGAL_GENERATION) || (
                        error === Errors.UNKNOWN_MEMBER_ID) || (
                        error === Errors.FENCED_INSTANCE_ID)
            ) {
                if (generationUnchanged()) {
                    log.info(
                        "Attempt to heartbeat with {} and group instance id {} failed due to {}, resetting generation",
                        sentGeneration, rebalanceConfig.groupInstanceId, error
                    )
                    // don't reset generation member ID when ILLEGAL_GENERATION, since the member ID is still valid
                    resetStateOnResponseError(
                        ApiKeys.HEARTBEAT,
                        error,
                        error !== Errors.ILLEGAL_GENERATION
                    )
                    future.raise(error)
                } else {
                    // if the generation has changed, then ignore this error
                    log.info(
                        "Attempt to heartbeat with stale {} and group instance id {} failed due to {}, ignoring the error",
                        sentGeneration, rebalanceConfig.groupInstanceId, error
                    )
                    future.complete(Unit)
                }
            } else if (error === Errors.GROUP_AUTHORIZATION_FAILED) future.raise(
                GroupAuthorizationException(groupId = rebalanceConfig.groupId)
            ) else future.raise(
                KafkaException("Unexpected error in heartbeat response: " + error.message)
            )
        }
    }

    protected abstract inner class CoordinatorResponseHandler<R, T> internal constructor(
        val sentGeneration: Generation,
    ) : RequestFutureAdapter<ClientResponse, T>() {

        var response: ClientResponse? = null

        abstract fun handle(response: R, future: RequestFuture<T>)

        override fun onFailure(exception: RuntimeException, future: RequestFuture<T>) {
            // mark the coordinator as dead
            if (exception is DisconnectException) {
                markCoordinatorUnknown(true, exception.message)
            }
            future.raise(exception)
        }

        override fun onSuccess(value: ClientResponse, future: RequestFuture<T>) {
            try {
                response = value
                val responseObj = value.responseBody as R
                handle(responseObj, future)
            } catch (e: RuntimeException) {
                if (!future.isDone) future.raise(e)
            }
        }

        fun generationUnchanged(): Boolean = synchronized(this@AbstractCoordinator) {
            return generation == sentGeneration
        }
    }

    protected fun createMeter(
        metrics: Metrics,
        groupName: String,
        baseName: String,
        descriptiveName: String?,
    ): Meter = Meter(
        rateStat = WindowedCount(),
        rateMetricName = metrics.metricName(
            name = "$baseName-rate",
            group = groupName,
            description = "The number of $descriptiveName per second"
        ),
        totalMetricName = metrics.metricName(
            name = "$baseName-total",
            group = groupName,
            description = "The total number of $descriptiveName",
        ),
    )

    private inner class GroupCoordinatorMetrics(metrics: Metrics, metricGrpPrefix: String) {

        val metricGrpName: String = "$metricGrpPrefix-coordinator-metrics"

        val heartbeatSensor: Sensor = metrics.sensor("heartbeat-latency").apply {
            add(
                metricName = metrics.metricName(
                    name = "heartbeat-response-time-max",
                    group = metricGrpName,
                    description = "The max time taken to receive a response to a heartbeat request",
                ),
                stat = Max(),
            )
            add(
                createMeter(
                    metrics = metrics,
                    groupName = metricGrpName,
                    baseName = "heartbeat",
                    descriptiveName = "heartbeats",
                )
            )
        }

        val joinSensor: Sensor = metrics.sensor("join-latency").apply {
            add(
                metricName = metrics.metricName(
                    name = "join-time-avg",
                    group = metricGrpName,
                    description = "The average time taken for a group rejoin",
                ),
                stat = Avg(),
            )
            add(
                metricName = metrics.metricName(
                    name = "join-time-max",
                    group = metricGrpName,
                    description = "The max time taken for a group rejoin",
                ),
                stat = Max(),
            )
            add(
                createMeter(
                    metrics = metrics,
                    groupName = metricGrpName,
                    baseName = "join",
                    descriptiveName = "group joins",
                )
            )
        }

        val syncSensor: Sensor = metrics.sensor("sync-latency").apply {
            add(
                metricName = metrics.metricName(
                    name = "sync-time-avg",
                    group = metricGrpName,
                    description = "The average time taken for a group sync",
                ),
                stat = Avg(),
            )
            add(
                metricName = metrics.metricName(
                    name = "sync-time-max",
                    group = metricGrpName,
                    description = "The max time taken for a group sync",
                ),
                stat = Max(),
            )
            add(
                createMeter(
                    metrics = metrics,
                    groupName = metricGrpName,
                    baseName = "sync",
                    descriptiveName = "group syncs",
                )
            )
        }

        val successfulRebalanceSensor: Sensor = metrics.sensor("rebalance-latency").apply {
            add(
                metricName = metrics.metricName(
                    name = "rebalance-latency-avg",
                    group = metricGrpName,
                    description = "The average time taken for a group to complete a successful " +
                            "rebalance, which may be composed of several failed re-trials until " +
                            "it succeeded"
                ),
                stat = Avg(),
            )
            add(
                metricName = metrics.metricName(
                    name = "rebalance-latency-max",
                    group = metricGrpName,
                    description = "The max time taken for a group to complete a successful " +
                            "rebalance, which may be composed of several failed re-trials until " +
                            "it succeeded",
                ),
                stat = Max(),
            )
            add(
                metricName = metrics.metricName(
                    name = "rebalance-latency-total",
                    group = metricGrpName,
                    description = "The total number of milliseconds this consumer has spent in " +
                            "successful rebalances since creation",
                ),
                stat = CumulativeSum(),
            )
            add(
                metricName = metrics.metricName(
                    name = "rebalance-total",
                    group = metricGrpName,
                    description = "The total number of successful rebalance events, each event " +
                            "is composed of several failed re-trials until it succeeded",
                ),
                stat = CumulativeCount(),
            )
            add(
                metricName = metrics.metricName(
                    name = "rebalance-rate-per-hour",
                    group = metricGrpName,
                    description = "The number of successful rebalance events per hour, each " +
                            "event is composed of several failed re-trials until it succeeded",
                ),
                stat = Rate(TimeUnit.HOURS, WindowedCount()),
            )
        }

        val failedRebalanceSensor: Sensor = metrics.sensor("failed-rebalance").apply {
            add(
                metricName = metrics.metricName(
                    name = "failed-rebalance-total",
                    group = metricGrpName,
                    description = "The total number of failed rebalance events",
                ),
                stat = CumulativeCount(),
            )
            add(
                metricName = metrics.metricName(
                    name = "failed-rebalance-rate-per-hour",
                    group = metricGrpName,
                    description = "The number of failed rebalance events per hour",
                ),
                stat = Rate(TimeUnit.HOURS, WindowedCount()),
            )
        }

        init {
            val lastRebalance = Measurable { _, now ->
                if (lastRebalanceEndMs == -1L)
                // if no rebalance is ever triggered, we just return -1.
                    return@Measurable -1.0
                else return@Measurable TimeUnit.SECONDS.convert(
                    now - lastRebalanceEndMs,
                    TimeUnit.MILLISECONDS,
                ).toDouble()
            }

            metrics.addMetric(
                metricName = metrics.metricName(
                    name = "last-rebalance-seconds-ago",
                    group = metricGrpName,
                    description = "The number of seconds since the last successful rebalance event",
                ),
                metricValueProvider = lastRebalance,
            )

            val lastHeartbeat = Measurable { _, now ->
                if (heartbeat.lastHeartbeatSend == 0L)
                // if no heartbeat is ever triggered, just return -1.
                    return@Measurable -1.0
                else return@Measurable TimeUnit.SECONDS.convert(
                    now - heartbeat.lastHeartbeatSend,
                    TimeUnit.MILLISECONDS
                ).toDouble()
            }

            metrics.addMetric(
                metricName = metrics.metricName(
                    name = "last-heartbeat-seconds-ago",
                    group = metricGrpName,
                    description = "The number of seconds since the last coordinator heartbeat " +
                            "was sent",
                ),
                metricValueProvider = lastHeartbeat
            )
        }
    }

    private inner class HeartbeatThread : KafkaThread(
        name = HEARTBEAT_THREAD_PREFIX +
                if (rebalanceConfig.groupId!!.isEmpty()) ""
                else " | " + rebalanceConfig.groupId,
        daemon = true,
    ), AutoCloseable {

        private var enabled = false

        private var closed = false

        private val failed = AtomicReference<RuntimeException?>(null)

        fun enable() = synchronized(this@AbstractCoordinator) {
            log.debug("Enabling heartbeat thread")
            enabled = true
            heartbeat.resetTimeouts()
            (this@AbstractCoordinator as Object).notify()
        }

        fun disable() = synchronized(this@AbstractCoordinator) {
            log.debug("Disabling heartbeat thread")
            enabled = false
        }

        override fun close() = synchronized(this@AbstractCoordinator) {
            closed = true
            (this@AbstractCoordinator as Object).notify()
        }

        fun hasFailed(): Boolean = failed.get() != null

        fun failureCause(): RuntimeException? = failed.get()

        override fun run() {
            try {
                log.debug("Heartbeat thread started")
                while (true) {
                    synchronized(this@AbstractCoordinator) {
                        if (closed) return
                        if (!enabled) {
                            (this@AbstractCoordinator as Object).wait()
                            return@synchronized
                        }

                        // we do not need to heartbeat we are not part of a group yet;
                        // also if we already have fatal error, the client will be
                        // crashed soon, hence we do not need to continue heartbeating either
                        // Kotlin Migration Note: state == State.NEW should be equivalent to state.hasNotJoinedGroup()
                        if (state == State.NEW || hasFailed()) {
                            disable()
                            return@synchronized
                        }
                        client.pollNoWakeup()
                        val now: Long = time.milliseconds()
                        if (coordinatorUnknown()) {
                            if (findCoordinatorFuture != null) {
                                // clear the future so that after the backoff, if the hb still sees
                                // coordinator unknown in the next iteration it will try to
                                // re-discover the coordinator in case the main thread cannot
                                clearFindCoordinatorFuture()
                            } else lookupCoordinator()

                            // backoff properly
                            (this@AbstractCoordinator as Object).wait(rebalanceConfig.retryBackoffMs!!)
                        } else if (heartbeat.sessionTimeoutExpired(now)) {
                            // the session timeout has expired without seeing a successful
                            // heartbeat, so we should probably make sure the coordinator is still
                            // healthy.
                            markCoordinatorUnknown(
                                "session timed out without receiving a heartbeat response"
                            )
                        } else if (heartbeat.pollTimeoutExpired(now)) {
                            // the poll timeout has expired, which means that the foreground thread
                            // has stalled in between calls to poll().
                            log.warn(
                                "consumer poll timeout has expired. This means the time between " +
                                        "subsequent calls to poll() was longer than the " +
                                        "configured max.poll.interval.ms, which typically " +
                                        "implies that the poll loop is spending too much time " +
                                        "processing messages. You can address this either by " +
                                        "increasing max.poll.interval.ms or by reducing the " +
                                        "maximum size of batches returned in poll() with " +
                                        "max.poll.records."
                            )
                            maybeLeaveGroup("consumer poll timeout has expired.")
                        } else if (!heartbeat.shouldHeartbeat(now)) {
                            // poll again after waiting for the retry backoff in case the heartbeat
                            // failed or the coordinator disconnected
                            (this@AbstractCoordinator as Object).wait(rebalanceConfig.retryBackoffMs!!)
                        } else {
                            heartbeat.sentHeartbeat(now)
                            val heartbeatFuture = sendHeartbeatRequest()
                            heartbeatFuture.addListener(object : RequestFutureListener<Unit> {

                                override fun onSuccess(value: Unit) = synchronized(this@AbstractCoordinator) {
                                    heartbeat.receiveHeartbeat()
                                }

                                override fun onFailure(e: RuntimeException) =
                                    synchronized(this@AbstractCoordinator) {
                                        when (e) {
                                            is RebalanceInProgressException -> {
                                                // it is valid to continue heartbeating while the
                                                // group is rebalancing. This ensures that the
                                                // coordinator keeps the member in the group for as
                                                // long as the duration of the rebalance timeout. If
                                                // we stop sending heartbeats, however, then the
                                                // session timeout may expire before we can rejoin.
                                                heartbeat.receiveHeartbeat()
                                            }

                                            is FencedInstanceIdException -> {
                                                log.error(
                                                    "Caught fenced group.instance.id {} error in " +
                                                            "heartbeat thread",
                                                    rebalanceConfig.groupInstanceId,
                                                )
                                                heartbeatThread!!.failed.set(e)
                                            }

                                            else -> {
                                                heartbeat.failHeartbeat()
                                                // wake up the thread if it's sleeping to reschedule
                                                // the heartbeat
                                                (this@AbstractCoordinator as Object).notify()
                                            }
                                        }
                                    }
                            })
                        }
                    }
                }
            } catch (e: AuthenticationException) {
                log.error("An authentication error occurred in the heartbeat thread", e)
                failed.set(e)
            } catch (e: GroupAuthorizationException) {
                log.error("A group authorization error occurred in the heartbeat thread", e)
                failed.set(e)
            } catch (e: InterruptedException) {
                interrupted()
                log.error("Unexpected interrupt received in heartbeat thread", e)
                failed.set(RuntimeException(e))
            } catch (e: InterruptException) {
                interrupted()
                log.error("Unexpected interrupt received in heartbeat thread", e)
                failed.set(RuntimeException(e))
            } catch (e: Throwable) {
                log.error("Heartbeat thread failed due to unexpected error", e)
                if (e is RuntimeException) failed.set(e) else failed.set(RuntimeException(e))
            } finally {
                log.debug("Heartbeat thread has closed")
            }
        }
    }

    data class Generation(
        val generationId: Int,
        val memberId: String,
        val protocolName: String?,
    ) {
        /**
         * @return true if this generation has a valid member id, false otherwise. A member might
         * have an id before it becomes part of a group generation.
         */
        fun hasMemberId(): Boolean = memberId.isNotEmpty()

        override fun toString(): String =
            "Generation{generationId=$generationId, memberId='$memberId', protocol='$protocolName}"

        companion object {

            val NO_GENERATION = Generation(
                generationId = OffsetCommitRequest.DEFAULT_GENERATION_ID,
                memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID,
                protocolName = null,
            )
        }
    }

    private class UnjoinedGroupException : RetriableException()

    // For testing only below
    fun heartbeat(): Heartbeat = heartbeat

    fun rejoinReason(): String = rejoinReason

    @Synchronized
    fun setLastRebalanceTime(timestamp: Long) {
        lastRebalanceEndMs = timestamp
    }

    /**
     * Check whether given generation id is matching the record within current generation.
     *
     * @param generationId generation id
     * @return true if the two ids are matching.
     */
    fun hasMatchingGenerationId(generationId: Int): Boolean =
        generation != Generation.NO_GENERATION && generation.generationId == generationId

    fun hasUnknownGeneration(): Boolean = generation == Generation.NO_GENERATION

    /**
     * @return true if the current generation's member ID is valid, false otherwise
     */
    fun hasValidMemberId(): Boolean = !hasUnknownGeneration() && generation.hasMemberId()

    @Synchronized
    fun setNewGeneration(generation: Generation) {
        this.generation = generation
    }

    @Synchronized
    fun setNewState(state: MemberState) {
        this.state = state
    }

    enum class MemberState {
        UNJOINED,

        // the client is not part of a group
        PREPARING_REBALANCE,

        // the client has sent the join group request, but have not received response
        COMPLETING_REBALANCE,

        // the client has received join group response, but have not received assignment
        STABLE;

        // the client has joined and is sending heartbeats
        fun hasNotJoinedGroup(): Boolean {
            return equals(UNJOINED) || equals(PREPARING_REBALANCE)
        }
    }

    companion object {

        val HEARTBEAT_THREAD_PREFIX = "kafka-coordinator-heartbeat-thread"

        val JOIN_GROUP_TIMEOUT_LAPSE = 5000
    }
}
