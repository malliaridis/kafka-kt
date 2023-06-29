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

import org.apache.kafka.clients.ClientRequest
import org.apache.kafka.clients.ClientResponse
import org.apache.kafka.clients.KafkaClient
import org.apache.kafka.clients.Metadata
import org.apache.kafka.clients.RequestCompletionHandler
import org.apache.kafka.common.Node
import org.apache.kafka.common.errors.DisconnectException
import org.apache.kafka.common.errors.InterruptException
import org.apache.kafka.common.errors.TimeoutException
import org.apache.kafka.common.errors.WakeupException
import org.apache.kafka.common.requests.AbstractRequest
import org.apache.kafka.common.utils.LogContext
import org.apache.kafka.common.utils.Time
import org.apache.kafka.common.utils.Timer
import org.slf4j.Logger
import java.io.Closeable
import java.io.IOException
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.ConcurrentMap
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.ReentrantLock
import kotlin.math.min

/**
 * Higher level consumer access to the network layer with basic support for request futures. This
 * class is thread-safe, but provides no synchronization for response callbacks. This guarantees
 * that no locks are held when they are invoked.
 */
class ConsumerNetworkClient(
    logContext: LogContext,
    private val client: KafkaClient,
    private val metadata: Metadata,
    private val time: Time,
    private val retryBackoffMs: Long,
    private val requestTimeoutMs: Int,
    maxPollTimeoutMs: Int,
) : Closeable {

    // the mutable state of this class is protected by the object's monitor (excluding the wakeup
    // flag and the request completion queue below).
    private val log: Logger = logContext.logger(ConsumerNetworkClient::class.java)

    private val unsent = UnsentRequests()

    private val maxPollTimeoutMs: Int = maxPollTimeoutMs.coerceAtMost(MAX_POLL_TIMEOUT_MS)

    private val wakeupDisabled = AtomicBoolean()

    // We do not need high throughput, so use a fair lock to try to avoid starvation
    private val lock = ReentrantLock(true)

    // when requests complete, they are transferred to this queue prior to invocation. The purpose
    // is to avoid invoking them while holding this object's monitor which can open the door for
    // deadlocks.
    private val pendingCompletion = ConcurrentLinkedQueue<RequestFutureCompletionHandler>()

    private val pendingDisconnects = ConcurrentLinkedQueue<Node>()

    // this flag allows the client to be safely woken up without waiting on the lock above. It is
    // atomic to avoid the need to acquire the lock above in order to enable it concurrently.
    private val wakeup = AtomicBoolean(false)

    fun defaultRequestTimeoutMs(): Int = requestTimeoutMs

    /**
     * Send a new request. Note that the request is not actually transmitted on the network until
     * one of the [poll] variants is invoked. At this point the request will either be transmitted
     * successfully or will fail. Use the returned future to obtain the result of the send. Note
     * that there is no need to check for disconnects explicitly on the [ClientResponse] object;
     * instead, the future will be failed with a [DisconnectException].
     *
     * @param node The destination of the request
     * @param requestBuilder A builder for the request payload
     * @param requestTimeoutMs Maximum time in milliseconds to await a response before disconnecting
     * the socket and cancelling the request. The request may be cancelled sooner if the socket
     * disconnects for any reason.
     * @return A future which indicates the result of the send.
     */
    fun send(
        node: Node,
        requestBuilder: AbstractRequest.Builder<*>?,
        requestTimeoutMs: Int = this.requestTimeoutMs,
    ): RequestFuture<ClientResponse> {
        val now = time.milliseconds()
        val completionHandler = RequestFutureCompletionHandler()
        val clientRequest = client.newClientRequest(
            nodeId = node.idString(),
            requestBuilder = requestBuilder,
            createdTimeMs = now,
            expectResponse = true,
            requestTimeoutMs = requestTimeoutMs,
            callback = completionHandler,
        )
        unsent.put(node, clientRequest)

        // wakeup the client in case it is blocking in poll so that we can send the queued request
        client.wakeup()
        return completionHandler.future
    }

    fun leastLoadedNode(): Node? {
        lock.lock()
        return try {
            client.leastLoadedNode(time.milliseconds())
        } finally {
            lock.unlock()
        }
    }

    fun hasReadyNodes(now: Long): Boolean {
        lock.lock()
        return try {
            client.hasReadyNodes(now)
        } finally {
            lock.unlock()
        }
    }

    /**
     * Block waiting on the metadata refresh with a timeout.
     *
     * @return true if update succeeded, false otherwise.
     */
    fun awaitMetadataUpdate(timer: Timer): Boolean {
        val version = metadata.requestUpdate()
        do {
            poll(timer)
        } while (metadata.updateVersion() == version && timer.isNotExpired)
        return metadata.updateVersion() > version
    }

    /**
     * Ensure our metadata is fresh (if an update is expected, this will block until it has
     * completed).
     */
    fun ensureFreshMetadata(timer: Timer): Boolean {
        return if (
            metadata.updateRequested()
            || metadata.timeToNextUpdate(timer.currentTimeMs) == 0L
        ) awaitMetadataUpdate(timer)
        else true // the metadata is already fresh
    }

    /**
     * Wakeup an active poll. This will cause the polling thread to throw an exception either on the
     * current poll if one is active, or the next poll.
     */
    fun wakeup() {
        // wakeup should be safe without holding the client lock since it simply delegates to
        // Selector's wakeup, which is thread-safe
        log.debug("Received user wakeup")
        wakeup.set(true)
        client.wakeup()
    }

    /**
     * Block indefinitely until the given request future has finished.
     *
     * @param future The request future to await.
     * @throws WakeupException if [wakeup] is called from another thread
     * @throws InterruptException if the calling thread is interrupted
     */
    fun poll(future: RequestFuture<*>) {
        while (!future.isDone) poll(time.timer(Long.MAX_VALUE), future)
    }

    /**
     * Block until the provided request future request has finished or the timeout has expired.
     *
     * @param future The request future to wait for
     * @param timer Timer bounding how long this method can block
     * @param disableWakeup true if we should not check for wakeups, false otherwise
     * @return `true` if the future is done, `false` otherwise
     * @throws WakeupException if [wakeup] is called from another thread and `disableWakeup` is
     * `false`
     * @throws InterruptException if the calling thread is interrupted
     */
    @JvmOverloads
    fun poll(future: RequestFuture<*>, timer: Timer, disableWakeup: Boolean = false): Boolean {
        do poll(timer, future, disableWakeup)
        while (!future.isDone && timer.isNotExpired)

        return future.isDone
    }

    /**
     * Poll for any network IO.
     *
     * @param timer Timer bounding how long this method can block
     * @param pollCondition Optional blocking condition
     * @param disableWakeup If `true` disable triggering wake-ups
     * @throws WakeupException if [wakeup] is called from another thread
     * @throws InterruptException if the calling thread is interrupted
     */
    @JvmOverloads
    fun poll(timer: Timer, pollCondition: PollCondition? = null, disableWakeup: Boolean = false) {
        // there may be handlers which need to be invoked if we woke up the previous call to poll
        firePendingCompletedRequests()
        lock.lock()
        try {
            // Handle async disconnects prior to attempting any sends
            handlePendingDisconnects()

            // send all the requests we can send now
            val pollDelayMs = trySend(timer.currentTimeMs)

            // check whether the poll is still needed by the caller. Note that if the expected
            // completion condition becomes satisfied after the call to shouldBlock() (because of a
            // fired completion handler), the client will be woken up.
            if (
                pendingCompletion.isEmpty()
                && (pollCondition == null || pollCondition.shouldBlock())
            ) {
                // if there are no requests in flight, do not block longer than the retry backoff
                var pollTimeout = min(timer.remainingMs, pollDelayMs)
                if (client.inFlightRequestCount() == 0)
                    pollTimeout = min(pollTimeout, retryBackoffMs)
                client.poll(pollTimeout, timer.currentTimeMs)
            } else client.poll(0, timer.currentTimeMs)

            timer.update()

            // handle any disconnects by failing the active requests. note that disconnects must be
            // checked immediately following poll since any subsequent call to client.ready() will
            // reset the disconnect status
            checkDisconnects(timer.currentTimeMs)
            if (!disableWakeup) {
                // trigger wakeups after checking for disconnects so that the callbacks will be
                // ready to be fired on the next call to poll()
                maybeTriggerWakeup()
            }
            // throw InterruptException if this thread is interrupted
            maybeThrowInterruptException()

            // try again to send requests since buffer space may have been cleared or a connect
            // finished in the poll
            trySend(timer.currentTimeMs)

            // fail requests that couldn't be sent if they have expired
            failExpiredRequests(timer.currentTimeMs)

            // clean unsent requests collection to keep the map from growing indefinitely
            unsent.clean()
        } finally {
            lock.unlock()
        }

        // called without the lock to avoid deadlock potential if handlers need to acquire locks
        firePendingCompletedRequests()
        metadata.maybeThrowAnyException()
    }

    /**
     * Poll for network IO and return immediately. This will not trigger wakeups.
     */
    fun pollNoWakeup() = poll(
        timer = time.timer(timeoutMs = 0),
        pollCondition = null,
        disableWakeup = true,
    )

    /**
     * Poll for network IO in best-effort only trying to transmit the ready-to-send request
     * Do not check any pending requests or metadata errors so that no exception should ever
     * be thrown, also no wakeups be triggered and no interrupted exception either.
     */
    fun transmitSends() {
        val timer = time.timer(0)

        // do not try to handle any disconnects, prev request failures, metadata exception etc;
        // just try once and return immediately
        lock.lock()
        try {
            // send all the requests we can send now
            trySend(timer.currentTimeMs)
            client.poll(0, timer.currentTimeMs)
        } finally {
            lock.unlock()
        }
    }

    /**
     * Block until all pending requests from the given node have finished.
     *
     * @param node The node to await requests from
     * @param timer Timer bounding how long this method can block
     * @return true If all requests finished, false if the timeout expired first
     */
    fun awaitPendingRequests(node: Node, timer: Timer): Boolean {
        while (hasPendingRequests(node) && timer.isNotExpired) poll(timer)
        return !hasPendingRequests(node)
    }

    /**
     * Get the count of pending requests to the given node. This includes both request that have
     * been transmitted (i.e. in-flight requests) and those which are awaiting transmission.
     *
     * @param node The node in question
     * @return The number of pending requests
     */
    fun pendingRequestCount(node: Node): Int {
        lock.lock()
        return try {
            unsent.requestCount(node) + client.inFlightRequestCount(node.idString())
        } finally {
            lock.unlock()
        }
    }

    /**
     * Check whether there is pending request to the given node. This includes both request that
     * have been transmitted (i.e. in-flight requests) and those which are awaiting transmission.
     *
     * @param node The node in question
     * @return A boolean indicating whether there is pending request
     */
    fun hasPendingRequests(node: Node): Boolean {
        if (unsent.hasRequests(node)) return true
        lock.lock()
        return try {
            client.hasInFlightRequests(node.idString())
        } finally {
            lock.unlock()
        }
    }

    /**
     * Get the total count of pending requests from all nodes. This includes both requests that
     * have been transmitted (i.e. in-flight requests) and those which are awaiting transmission.
     *
     * @return The total count of pending requests
     */
    fun pendingRequestCount(): Int {
        lock.lock()
        return try {
            unsent.requestCount() + client.inFlightRequestCount()
        } finally {
            lock.unlock()
        }
    }

    /**
     * Check whether there is pending request. This includes both requests that have been
     * transmitted (i.e. in-flight requests) and those which are awaiting transmission.
     *
     * @return A boolean indicating whether there is pending request
     */
    fun hasPendingRequests(): Boolean {
        if (unsent.hasRequests()) return true
        lock.lock()
        return try {
            client.hasInFlightRequests()
        } finally {
            lock.unlock()
        }
    }

    private fun firePendingCompletedRequests() {
        var completedRequestsFired = false
        while (true) {
            val completionHandler = pendingCompletion.poll() ?: break
            completionHandler.fireCompletion()
            completedRequestsFired = true
        }

        // wakeup the client in case it is blocking in poll for this future's completion
        if (completedRequestsFired) client.wakeup()
    }

    private fun checkDisconnects(now: Long) {
        // any disconnects affecting requests that have already been transmitted will be handled by
        // NetworkClient, so we just need to check whether connections for any of the unsent
        // requests have been disconnected; if they have, then we complete the corresponding future
        // and set the disconnect flag in the ClientResponse
        for (node in unsent.nodes()) {
            if (client.connectionFailed(node)) {
                // Remove entry before invoking request callback to avoid callbacks handling
                // coordinator failures traversing the unsent list again.
                val requests = unsent.remove(node)
                for (request in requests) {
                    val handler = request.callback as RequestFutureCompletionHandler
                    val authenticationException = client.authenticationException(node)
                    handler.onComplete(
                        ClientResponse(
                            requestHeader = request.makeHeader(
                                version = request.requestBuilder().latestAllowedVersion,
                            ),
                            callback = request.callback,
                            destination = request.destination,
                            createdTimeMs = request.createdTimeMs,
                            receivedTimeMs = now,
                            disconnected = true,
                            versionMismatch = null,
                            authenticationException = authenticationException!!,
                            responseBody = null
                        )
                    )
                }
            }
        }
    }

    private fun handlePendingDisconnects() {
        lock.lock()
        try {
            while (true) {
                val node = pendingDisconnects.poll() ?: break
                failUnsentRequests(node, DisconnectException.INSTANCE)
                client.disconnect(node.idString())
            }
        } finally {
            lock.unlock()
        }
    }

    fun disconnectAsync(node: Node) {
        pendingDisconnects.offer(node)
        client.wakeup()
    }

    private fun failExpiredRequests(now: Long) {
        // clear all expired unsent requests and fail their corresponding futures
        val expiredRequests = unsent.removeExpiredRequests(now)
        for (request in expiredRequests) {
            val handler = request.callback as RequestFutureCompletionHandler
            handler.onFailure(
                TimeoutException("Failed to send request after ${request.requestTimeoutMs} ms.")
            )
        }
    }

    private fun failUnsentRequests(node: Node, e: RuntimeException) {
        // clear unsent requests to node and fail their corresponding futures
        lock.lock()
        try {
            val unsentRequests = unsent.remove(node)
            unsentRequests.forEach { unsentRequest ->
                val handler = unsentRequest.callback as RequestFutureCompletionHandler
                handler.onFailure(e)
            }
        } finally {
            lock.unlock()
        }
    }

    // Visible for testing
    fun trySend(now: Long): Long {
        var pollDelayMs = maxPollTimeoutMs.toLong()

        // send any requests that can be sent now
        for (node in unsent.nodes()) {
            val iterator = unsent.requestIterator(node)
            if (iterator.hasNext()) pollDelayMs = min(pollDelayMs, client.pollDelayMs(node, now))

            while (iterator.hasNext()) {
                val request = iterator.next()
                if (client.ready(node, now)) {
                    client.send(request, now)
                    iterator.remove()
                } else {
                    // try next node when current node is not ready
                    break
                }
            }
        }
        return pollDelayMs
    }

    fun maybeTriggerWakeup() {
        if (!wakeupDisabled.get() && wakeup.get()) {
            log.debug("Raising WakeupException in response to user wakeup")
            wakeup.set(false)
            throw WakeupException()
        }
    }

    private fun maybeThrowInterruptException() {
        if (Thread.interrupted()) throw InterruptException(cause = InterruptedException())
    }

    fun disableWakeups() = wakeupDisabled.set(true)

    @Throws(IOException::class)
    override fun close() {
        lock.lock()
        try {
            client.close()
        } finally {
            lock.unlock()
        }
    }

    /**
     * Check if the code is disconnected and unavailable for immediate reconnection (i.e. if it is
     * in reconnect backoff window following the disconnect).
     */
    fun isUnavailable(node: Node): Boolean {
        lock.lock()
        return try {
            client.connectionFailed(node) && client.connectionDelay(node, time.milliseconds()) > 0
        } finally {
            lock.unlock()
        }
    }

    /**
     * Check for an authentication error on a given node and raise the exception if there is one.
     */
    fun maybeThrowAuthFailure(node: Node) {
        lock.lock()
        try {
            val exception = client.authenticationException(node)
            if (exception != null) throw exception
        } finally {
            lock.unlock()
        }
    }

    /**
     * Initiate a connection if currently possible. This is only really useful for resetting the
     * failed status of a socket. If there is an actual request to send, then [send] should be used.
     *
     * @param node The node to connect to
     */
    fun tryConnect(node: Node) {
        lock.lock()
        try {
            client.ready(node, time.milliseconds())
        } finally {
            lock.unlock()
        }
    }

    private inner class RequestFutureCompletionHandler : RequestCompletionHandler {

        val future: RequestFuture<ClientResponse> = RequestFuture()

        private lateinit var response: ClientResponse

        private var e: RuntimeException? = null

        fun fireCompletion() {
            if (e != null) future.raise(e)
            else if (response.authenticationException != null)
                future.raise(response.authenticationException)
            else if (response.disconnected) {
                log.debug(
                    "Cancelled request with header {} due to node {} being disconnected",
                    response.requestHeader,
                    response.destination,
                )
                future.raise(DisconnectException.INSTANCE)
            } else if (response.versionMismatch != null) future.raise(response.versionMismatch)
            else future.complete(response)
        }

        fun onFailure(e: RuntimeException?) {
            this.e = e
            pendingCompletion.add(this)
        }

        override fun onComplete(response: ClientResponse) {
            this.response = response
            pendingCompletion.add(this)
        }
    }

    /**
     * When invoking poll from a multi-threaded environment, it is possible that the condition that
     * the caller is awaiting has already been satisfied prior to the invocation of poll. We
     * therefore introduce this interface to push the condition checking as close as possible to the
     * invocation of poll. In particular, the check will be done while holding the lock used to
     * protect concurrent access to [org.apache.kafka.clients.NetworkClient], which means
     * implementations must be very careful about locking order if the callback must acquire
     * additional locks.
     */
    fun interface PollCondition {

        /**
         * Return whether the caller is still awaiting an IO event.
         * @return true if so, false otherwise.
         */
        fun shouldBlock(): Boolean
    }

    /*
     * A thread-safe helper class to hold requests per node that have not been sent yet
     */
    private class UnsentRequests {

        private val unsent: ConcurrentMap<Node, ConcurrentLinkedQueue<ClientRequest>> =
            ConcurrentHashMap()

        fun put(node: Node, request: ClientRequest) {
            // the lock protects the put from a concurrent removal of the queue for the node
            synchronized(unsent) {
                val requests = unsent.computeIfAbsent(node) { ConcurrentLinkedQueue() }
                requests.add(request)
            }
        }

        fun requestCount(node: Node): Int {
            val requests = unsent[node]
            return requests?.size ?: 0
        }

        fun requestCount(): Int {
            var total = 0
            for (requests in unsent.values) total += requests.size
            return total
        }

        fun hasRequests(node: Node): Boolean = !unsent[node].isNullOrEmpty()

        fun hasRequests(): Boolean = unsent.values.any { requests -> requests.isNotEmpty() }

        fun removeExpiredRequests(now: Long): Collection<ClientRequest> {
            val expiredRequests = mutableListOf<ClientRequest>()
            for (requests in unsent.values) {
                val requestIterator = requests.iterator()
                while (requestIterator.hasNext()) {
                    val request = requestIterator.next()
                    val elapsedMs = (now - request!!.createdTimeMs).coerceAtLeast(0)
                    if (elapsedMs > request.requestTimeoutMs) {
                        expiredRequests.add(request)
                        requestIterator.remove()
                    } else break
                }
            }
            return expiredRequests
        }

        fun clean() {
            // the lock protects removal from a concurrent put which could otherwise mutate the
            // queue after it has been removed from the map
            synchronized(unsent) {
                val iterator = unsent.values.iterator()
                while (iterator.hasNext()) {
                    val requests = iterator.next()
                    if (requests.isEmpty()) iterator.remove()
                }
            }
        }

        fun remove(node: Node): Collection<ClientRequest> {
            // the lock protects removal from a concurrent put which could otherwise mutate the
            // queue after it has been removed from the map
            synchronized(unsent) {
                val requests = unsent.remove(node)
                return requests ?: emptyList()
            }
        }

        fun requestIterator(node: Node): MutableIterator<ClientRequest> {
            val requests = unsent[node]
            return requests?.iterator() ?: Collections.emptyIterator()
        }

        fun nodes(): Collection<Node> = unsent.keys
    }

    companion object {
        private const val MAX_POLL_TIMEOUT_MS = 5000
    }
}
