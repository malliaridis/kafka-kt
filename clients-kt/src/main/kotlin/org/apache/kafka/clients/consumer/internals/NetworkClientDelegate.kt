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

import java.io.IOException
import java.util.ArrayDeque
import java.util.Collections
import java.util.Objects
import java.util.Optional
import java.util.Queue
import java.util.concurrent.CompletableFuture
import java.util.function.Consumer
import org.apache.kafka.clients.ClientRequest
import org.apache.kafka.clients.ClientResponse
import org.apache.kafka.clients.KafkaClient
import org.apache.kafka.clients.RequestCompletionHandler
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.Node
import org.apache.kafka.common.errors.DisconnectException
import org.apache.kafka.common.errors.TimeoutException
import org.apache.kafka.common.requests.AbstractRequest
import org.apache.kafka.common.utils.LogContext
import org.apache.kafka.common.utils.Time
import org.apache.kafka.common.utils.Timer
import org.slf4j.Logger

/**
 * A wrapper around the [org.apache.kafka.clients.NetworkClient] to handle network poll and send operations.
 */
class NetworkClientDelegate(
    private val time: Time,
    config: ConsumerConfig,
    logContext: LogContext,
    private val client: KafkaClient,
) : AutoCloseable {

    private val log: Logger = logContext.logger(javaClass)

    private val requestTimeoutMs: Int = config.getInt(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG)!!

    private val unsentRequests: Queue<UnsentRequest> = ArrayDeque()

    private val retryBackoffMs: Long = config.getLong(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG)!!

    /**
     * Returns the responses of the sent requests. This method will try to send the unsent requests,
     * poll for responses, and check the disconnected nodes.
     *
     * @param timeoutMs timeout time
     * @param currentTimeMs current time
     * @return a list of client response
     */
    fun poll(timeoutMs: Long, currentTimeMs: Long) {
        trySend(currentTimeMs)
        var pollTimeoutMs = timeoutMs
        if (!unsentRequests.isEmpty()) {
            pollTimeoutMs = retryBackoffMs.coerceAtLeast(pollTimeoutMs)
        }
        client.poll(pollTimeoutMs, currentTimeMs)
        checkDisconnects()
    }

    /**
     * Tries to send the requests in the unsentRequest queue. If the request doesn't have an assigned node, it will
     * find the leastLoadedOne, and will be retried in the next `poll()`. If the request is expired, a
     * [TimeoutException] will be thrown.
     */
    private fun trySend(currentTimeMs: Long) {
        val iterator = unsentRequests.iterator()

        while (iterator.hasNext()) {
            val unsent = iterator.next()
            unsent.timer!!.update(currentTimeMs)
            if (unsent.timer!!.isExpired) {
                iterator.remove()
                unsent.handler.onFailure(
                    TimeoutException("Failed to send request after ${unsent.timer!!.timeoutMs} ms.")
                )
                continue
            }
            if (!doSend(unsent, currentTimeMs)) continue // continue to retry until timeout.
            iterator.remove()
        }
    }

    private fun doSend(
        request: UnsentRequest,
        currentTimeMs: Long,
    ): Boolean {
        val node = request.node ?: client.leastLoadedNode(currentTimeMs)
        if (node == null || nodeUnavailable(node)) {
            log.debug("No broker available to send the request: {}. Retrying.", request)
            return false
        }
        val req = makeClientRequest(request, node, currentTimeMs)
        if (!client.ready(node, currentTimeMs)) {
            // enqueue the request again if the node isn't ready yet. The request will be handled in the next iteration
            // of the event loop
            log.debug("Node is not ready, handle the request in the next event loop: node={}, request={}", node, req)
            return false
        }
        client.send(req, currentTimeMs)
        return true
    }

    private fun checkDisconnects() {
        // Check the connection of the unsent request. Disconnect the disconnected node if it is unable to be connected.
        val iter = unsentRequests.iterator()
        while (iter.hasNext()) {
            val unsent = iter.next()
            if (unsent.node != null && client.connectionFailed(unsent.node)) {
                iter.remove()
                val authenticationException = client.authenticationException(unsent.node)
                unsent.handler.onFailure(authenticationException)
            }
        }
    }

    private fun makeClientRequest(
        unsent: UnsentRequest,
        node: Node,
        currentTimeMs: Long,
    ): ClientRequest = client.newClientRequest(
        nodeId = node.idString(),
        requestBuilder = unsent.requestBuilder,
        createdTimeMs = currentTimeMs,
        expectResponse = true,
        requestTimeoutMs = unsent.timer!!.remainingMs.toInt(),
        callback = unsent.handler
    )

    fun leastLoadedNode(): Node? = client.leastLoadedNode(time.milliseconds())

    fun send(request: UnsentRequest) {
        request.setTimer(time, requestTimeoutMs.toLong())
        unsentRequests.add(request)
    }

    fun wakeup() = client.wakeup()

    /**
     * Check if the code is disconnected and unavailable for immediate reconnection (i.e. if it is in reconnect
     * backoff window following the disconnect).
     */
    fun nodeUnavailable(node: Node): Boolean =
        client.connectionFailed(node) && client.connectionDelay(node, time.milliseconds()) > 0

    @Throws(IOException::class)
    override fun close() = client.close()

    fun addAll(requests: List<UnsentRequest>) {
        requests.forEach { unsent -> unsent.setTimer(time, requestTimeoutMs.toLong()) }
        unsentRequests.addAll(requests)
    }

    data class PollResult(
        val timeUntilNextPollMs: Long,
        val unsentRequests: List<UnsentRequest>,
    )

    data class UnsentRequest(
        val requestBuilder: AbstractRequest.Builder<*>,
        val node: Node?, // empty if random node can be chosen
        val handler: FutureCompletionHandler = FutureCompletionHandler(),
    ) {
        var timer: Timer? = null

        fun setTimer(time: Time, requestTimeoutMs: Long) {
            timer = time.timer(requestTimeoutMs)
        }

        @Deprecated(
            message = "Use property instead",
            replaceWith = ReplaceWith("future"),
        )
        fun future(): CompletableFuture<ClientResponse> = handler.future

        val future: CompletableFuture<ClientResponse>
            get() = handler.future

        @Deprecated(
            message = "Use property instead",
            replaceWith = ReplaceWith("callback"),
        )
        fun callback(): RequestCompletionHandler = handler

        val callback: RequestCompletionHandler
            get() = handler

        @Deprecated(
            message = "Use property instead",
            replaceWith = ReplaceWith("requestBuilder"),
        )
        fun requestBuilder(): AbstractRequest.Builder<*> = requestBuilder

        override fun toString(): String = "UnsentRequest{" +
                "requestBuilder=$requestBuilder" +
                ", handler=$handler" +
                ", node=$node" +
                ", timer=$timer" +
                '}'
    }

    class FutureCompletionHandler internal constructor() : RequestCompletionHandler {

        val future: CompletableFuture<ClientResponse> = CompletableFuture()

        fun onFailure(e: RuntimeException?) = future.completeExceptionally(e)

        fun future(): CompletableFuture<ClientResponse> = future

        override fun onComplete(response: ClientResponse) {
            if (response.authenticationException != null) onFailure(response.authenticationException)
            else if (response.disconnected) onFailure(DisconnectException.INSTANCE)
            else if (response.versionMismatch != null) onFailure(response.versionMismatch)
            else future.complete(response)
        }
    }
}

