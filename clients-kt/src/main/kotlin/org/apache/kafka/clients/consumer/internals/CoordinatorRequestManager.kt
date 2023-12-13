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

import org.apache.kafka.clients.consumer.internals.NetworkClientDelegate.PollResult
import org.apache.kafka.clients.consumer.internals.NetworkClientDelegate.UnsentRequest
import org.apache.kafka.common.Node
import org.apache.kafka.common.errors.GroupAuthorizationException
import org.apache.kafka.common.errors.RetriableException
import org.apache.kafka.common.message.FindCoordinatorRequestData
import org.apache.kafka.common.message.FindCoordinatorResponseData
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.FindCoordinatorRequest
import org.apache.kafka.common.requests.FindCoordinatorResponse
import org.apache.kafka.common.utils.LogContext
import org.apache.kafka.common.utils.Time
import org.slf4j.Logger

/**
 * This is responsible for timing to send the next [FindCoordinatorRequest] based on the following criteria:
 *
 * Whether there is an existing coordinator.
 * Whether there is an inflight request.
 * Whether the backoff timer has expired.
 * The [org.apache.kafka.clients.consumer.internals.NetworkClientDelegate.PollResult] contains either a wait timer
 * or a singleton list of [org.apache.kafka.clients.consumer.internals.NetworkClientDelegate.UnsentRequest].
 *
 * The [FindCoordinatorRequest] will be handled by the [.onResponse]  callback, which
 * subsequently invokes `onResponse` to handle the exception and response. Note that the coordinator node will be
 * marked `null` upon receiving a failure.
 */
class CoordinatorRequestManager(
    private val time: Time,
    logContext: LogContext,
    retryBackoffMs: Long,
    private val nonRetriableErrorHandler: ErrorEventHandler,
    private val groupId: String,
) : RequestManager {

    private val log: Logger = logContext.logger(this.javaClass)

    private val coordinatorRequestState: RequestState = RequestState(retryBackoffMs)

    private var timeMarkedUnknownMs = -1L // starting logging a warning only after unable to connect for a while

    private var totalDisconnectedMin: Long = 0

    var coordinator: Node? = null

    /**
     * Poll for the FindCoordinator request.
     * If we don't need to discover a coordinator, this method will return a PollResult with Long.MAX_VALUE
     * backoff time and an empty list. If we are still backing off from a previous attempt, this method will return
     * a PollResult with the remaining backoff time and an empty list.
     *
     * Otherwise, this returns will return a PollResult with a singleton list of UnsentRequest and
     * Long.MAX_VALUE backoff time.
     *
     * Note that this method does not involve any actual network IO, and it only determines if we need to
     * send a new request or not.
     *
     * @param currentTimeMs current time in ms.
     * @return [org.apache.kafka.clients.consumer.internals.NetworkClientDelegate.PollResult]. This will not be `null`.
     */
    override fun poll(currentTimeMs: Long): PollResult {
        if (coordinator != null) return PollResult(Long.MAX_VALUE, emptyList())

        if (coordinatorRequestState.canSendRequest(currentTimeMs)) {
            val request: UnsentRequest = makeFindCoordinatorRequest(currentTimeMs)
            return PollResult(Long.MAX_VALUE, listOf(request))
        }
        return PollResult(coordinatorRequestState.remainingBackoffMs(currentTimeMs), emptyList())
    }

    private fun makeFindCoordinatorRequest(currentTimeMs: Long): UnsentRequest {
        coordinatorRequestState.onSendAttempt(currentTimeMs)
        val data = FindCoordinatorRequestData()
            .setKeyType(FindCoordinatorRequest.CoordinatorType.GROUP.id)
            .setKey(groupId)
        val unsentRequest = UnsentRequest(FindCoordinatorRequest.Builder(data), null)

        unsentRequest.future.whenComplete { clientResponse, throwable ->
            val responseTimeMs: Long = time.milliseconds()
            clientResponse.responseBody as FindCoordinatorResponse
            if (clientResponse != null) onResponse(responseTimeMs, clientResponse.responseBody)
            else onFailedResponse(responseTimeMs, throwable)
        }
        return unsentRequest
    }

    /**
     * Mark the current coordinator null.
     *
     * @param cause why the coordinator is marked unknown.
     * @param currentTimeMs the current time in ms.
     */
    fun markCoordinatorUnknown(cause: String?, currentTimeMs: Long) {
        if (coordinator != null) {
            log.info(
                "Group coordinator {} is unavailable or invalid due to cause: {}. Rediscovery will be attempted.",
                coordinator, cause
            )
            coordinator = null
            timeMarkedUnknownMs = currentTimeMs
            totalDisconnectedMin = 0
        } else {
            val durationOfOngoingDisconnectMs = (currentTimeMs - timeMarkedUnknownMs).coerceAtLeast(0)
            val currDisconnectMin = durationOfOngoingDisconnectMs / COORDINATOR_DISCONNECT_LOGGING_INTERVAL_MS
            if (currDisconnectMin > totalDisconnectedMin) {
                log.debug(
                    "Consumer has been disconnected from the group coordinator for {}ms",
                    durationOfOngoingDisconnectMs
                )
                totalDisconnectedMin = currDisconnectMin
            }
        }
    }

    private fun onSuccessfulResponse(
        currentTimeMs: Long,
        coordinator: FindCoordinatorResponseData.Coordinator,
    ) {
        // use MAX_VALUE - node.id as the coordinator id to allow separate connections
        // for the coordinator in the underlying network client layer
        val coordinatorConnectionId = Int.MAX_VALUE - coordinator.nodeId
        this.coordinator = Node(
            id = coordinatorConnectionId,
            host = coordinator.host,
            port = coordinator.port,
        )
        log.info("Discovered group coordinator {}", coordinator)
        coordinatorRequestState.onSuccessfulAttempt(currentTimeMs)
    }

    private fun onFailedResponse(
        currentTimeMs: Long,
        exception: Throwable,
    ) {
        coordinatorRequestState.onFailedAttempt(currentTimeMs)
        markCoordinatorUnknown("FindCoordinator failed with exception", currentTimeMs)
        if (exception is RetriableException) {
            log.debug("FindCoordinator request failed due to retriable exception", exception)
            return
        }
        if (exception === Errors.GROUP_AUTHORIZATION_FAILED.exception) {
            log.debug("FindCoordinator request failed due to authorization error {}", exception.message)
            nonRetriableErrorHandler.handle(GroupAuthorizationException(groupId = groupId))
            return
        }
        log.warn("FindCoordinator request failed due to fatal exception", exception)
        nonRetriableErrorHandler.handle(exception)
    }

    /**
     * Handles the response upon completing the [FindCoordinatorRequest] if the
     * future returned successfully. This method must still unwrap the response object
     * to check for protocol errors.
     *
     * @param currentTimeMs current time in ms.
     * @param response      the response for finding the coordinator. null if an exception is thrown.
     */
    private fun onResponse(
        currentTimeMs: Long,
        response: FindCoordinatorResponse,
    ) {
        // handles Runtime exception
        val coordinator = response.coordinatorByKey(groupId)
        if (coordinator == null) {
            val msg = "Response did not contain expected coordinator section for groupId: $groupId"
            onFailedResponse(currentTimeMs, IllegalStateException(msg))
            return
        }
        val node = coordinator
        if (node.errorCode != Errors.NONE.code) {
            onFailedResponse(currentTimeMs, Errors.forCode(node.errorCode).exception!!)
            return
        }
        onSuccessfulResponse(currentTimeMs, node)
    }

    /**
     * Returns the current coordinator node.
     *
     * @return the current coordinator node.
     */
    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("coordinator"),
    )
    fun coordinator(): Node? = coordinator

    companion object {
        private val COORDINATOR_DISCONNECT_LOGGING_INTERVAL_MS = (60 * 1000).toLong()
    }
}
