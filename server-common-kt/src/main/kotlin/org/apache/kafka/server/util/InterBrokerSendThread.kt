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

package org.apache.kafka.server.util

import java.util.ArrayDeque
import org.apache.kafka.clients.ClientRequest
import org.apache.kafka.clients.ClientResponse
import org.apache.kafka.clients.KafkaClient
import org.apache.kafka.common.Node
import org.apache.kafka.common.errors.AuthenticationException
import org.apache.kafka.common.errors.DisconnectException
import org.apache.kafka.common.internals.FatalExitError
import org.apache.kafka.common.utils.Time
import org.apache.kafka.common.utils.Utils.closeQuietly
import kotlin.concurrent.Volatile

/**
 * An inter-broker send thread that utilizes a non-blocking network client.
 */
abstract class InterBrokerSendThread(
    name: String,
    @field:Volatile protected var networkClient: KafkaClient,
    private val requestTimeoutMs: Int,
    private val time: Time,
    isInterruptible: Boolean = true,
) : ShutdownableThread(name, isInterruptible) {

    private val unsentRequests: UnsentRequests = UnsentRequests()

    abstract fun generateRequests(): Collection<RequestAndCompletionHandler>

    fun hasUnsentRequests(): Boolean = unsentRequests.iterator().hasNext()

    @Throws(InterruptedException::class)
    override fun shutdown() {
        initiateShutdown()
        networkClient.initiateClose()
        awaitShutdown()
        closeQuietly(networkClient, "InterBrokerSendThread network client")
    }

    private fun drainGeneratedRequests() {
        generateRequests().forEach { request ->
            unsentRequests.put(
                node = request.destination,
                request = networkClient.newClientRequest(
                    nodeId = request.destination.idString(),
                    requestBuilder = request.request,
                    createdTimeMs = request.creationTimeMs,
                    expectResponse = true,
                    requestTimeoutMs = requestTimeoutMs,
                    callback = request.handler,
                )
            )
        }
    }

    open fun pollOnce(maxTimeoutMs: Long) {
        try {
            drainGeneratedRequests()
            var now = time.milliseconds()
            val timeout = sendRequests(now, maxTimeoutMs)
            networkClient.poll(timeout, now)
            now = time.milliseconds()
            checkDisconnects(now)
            failExpiredRequests(now)
            unsentRequests.clean()
        } catch (fee: FatalExitError) {
            throw fee
        } catch (t: Throwable) {
            // DisconnectException is expected when NetworkClient#initiateClose is called
            if (t is DisconnectException && !networkClient.active()) return

            // InterruptedException is expected when shutting down. Throw the error to ShutdownableThread to handle
            if (t is InterruptedException && !isRunning) throw t

            log.error("unhandled exception caught in InterBrokerSendThread", t)
            // rethrow any unhandled exceptions as FatalExitError so the JVM will be terminated
            // as we will be in an unknown state with potentially some requests dropped and not
            // being able to make progress. Known and expected Errors should have been appropriately
            // dealt with already.
            throw FatalExitError()
        }
    }

    override fun doWork() = pollOnce(Long.MAX_VALUE)

    private fun sendRequests(now: Long, maxTimeoutMs: Long): Long {
        var pollTimeout = maxTimeoutMs
        for (node in unsentRequests.nodes()) {
            val requestIterator = unsentRequests.requestIterator(node)
            while (requestIterator.hasNext()) {
                val request = requestIterator.next()
                if (networkClient.ready(node, now)) {
                    networkClient.send(request, now)
                    requestIterator.remove()
                } else pollTimeout = networkClient.connectionDelay(node, now).coerceAtMost(pollTimeout)
            }
        }
        return pollTimeout
    }

    private fun checkDisconnects(now: Long) {
        // any disconnects affecting requests that have already been transmitted will be handled
        // by NetworkClient, so we just need to check whether connections for any of the unsent
        // requests have been disconnected; if they have, then we complete the corresponding future
        // and set the disconnect flag in the ClientResponse
        val iterator = unsentRequests.iterator()
        while (iterator.hasNext()) {
            val (node, requests) = iterator.next()
            if (!requests.isEmpty() && networkClient.connectionFailed(node)) {
                iterator.remove()
                for (request in requests) {
                    val authenticationException = networkClient.authenticationException(node)
                    if (authenticationException != null) {
                        log.error("Failed to send the following request due to authentication error: {}", request)
                    }
                    completeWithDisconnect(request, now, authenticationException)
                }
            }
        }
    }

    private fun failExpiredRequests(now: Long) {
        // clear all expired unsent requests
        val timedOutRequests = unsentRequests.removeAllTimedOut(now)
        for (request in timedOutRequests) {
            log.debug("Failed to send the following request after {} ms: {}", request.requestTimeoutMs, request)
            completeWithDisconnect(request, now, null)
        }
    }

    fun wakeup() = networkClient.wakeup()

    private class UnsentRequests {

        private val unsent: MutableMap<Node, ArrayDeque<ClientRequest>> = HashMap()

        fun put(node: Node, request: ClientRequest) {
            val requests = unsent.computeIfAbsent(node) { ArrayDeque() }
            requests.add(request)
        }

        fun removeAllTimedOut(now: Long): Collection<ClientRequest> {
            val expiredRequests: MutableList<ClientRequest> = ArrayList()

            for (requests in unsent.values) {
                val requestIterator = requests.iterator()
                var foundExpiredRequest = false
                while (requestIterator.hasNext() && !foundExpiredRequest) {
                    val request = requestIterator.next()
                    val elapsedMs = (now - request.createdTimeMs).coerceAtLeast(0)
                    if (elapsedMs > request.requestTimeoutMs) {
                        expiredRequests.add(request)
                        requestIterator.remove()
                        foundExpiredRequest = true
                    }
                }
            }
            return expiredRequests
        }

        fun clean() = unsent.values.removeIf { it.isEmpty() }

        operator fun iterator(): MutableIterator<Map.Entry<Node, ArrayDeque<ClientRequest>>> =
            unsent.entries.iterator()

        fun requestIterator(node: Node): MutableIterator<ClientRequest> {
            val requests = unsent[node]
            return (requests ?: mutableListOf()).iterator()
        }

        fun nodes(): Set<Node> = unsent.keys
    }

    companion object {

        private fun completeWithDisconnect(
            request: ClientRequest,
            now: Long,
            authenticationException: AuthenticationException?,
        ) {
            val handler = request.callback!!
            handler.onComplete(
                ClientResponse(
                    requestHeader = request.makeHeader(request.requestBuilder().latestAllowedVersion),
                    callback = handler,
                    destination = request.destination,
                    createdTimeMs = now,
                    receivedTimeMs = now,
                    disconnected = true,
                    versionMismatch = null,
                    authenticationException = authenticationException,
                    responseBody = null,
                )
            )
        }
    }
}
