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

import java.util.ArrayDeque
import java.util.Deque
import java.util.concurrent.atomic.AtomicInteger
import org.apache.kafka.clients.NetworkClient.InFlightRequest

/**
 * The set of requests which have been sent or are being sent but haven't yet received a response
 */
internal class InFlightRequests(private val maxInFlightRequestsPerConnection: Int) {

    private val requests: MutableMap<String, Deque<InFlightRequest>> = HashMap()

    /**
     * Thread safe total number of in flight requests.
     */
    private val inFlightRequestCount = AtomicInteger(0)

    /**
     * Add the given request to the queue for the connection it was directed to
     */
    fun add(request: InFlightRequest) {
        val destination = request.destination
        var reqs = requests[destination]
        if (reqs == null) {
            reqs = ArrayDeque()
            requests[destination] = reqs
        }
        reqs.addFirst(request)
        inFlightRequestCount.incrementAndGet()
    }

    /**
     * Get the request queue for the given node
     */
    private fun requestQueue(node: String): Deque<InFlightRequest> {
        val reqs = requests[node]
        check(!reqs.isNullOrEmpty()) { "There are no in-flight requests for node $node" }
        return reqs
    }

    /**
     * Get the oldest request (the one that will be completed next) for the given node
     */
    fun completeNext(node: String): InFlightRequest {
        val inFlightRequest = requestQueue(node).pollLast()
        inFlightRequestCount.decrementAndGet()
        return inFlightRequest
    }

    /**
     * Get the last request we sent to the given node (but don't remove it from the queue)
     * @param node The node id
     */
    fun lastSent(node: String): InFlightRequest {
        return requestQueue(node).peekFirst()
    }

    /**
     * Complete the last request that was sent to a particular node.
     * @param node The node the request was sent to
     * @return The request
     */
    fun completeLastSent(node: String): InFlightRequest {
        val inFlightRequest = requestQueue(node).pollFirst()
        inFlightRequestCount.decrementAndGet()
        return inFlightRequest
    }

    /**
     * Can we send more requests to this node?
     *
     * @param node Node in question
     * @return true iff we have no requests still being sent to the given node
     */
    fun canSendMore(node: String): Boolean {
        val queue = requests[node]
        return queue.isNullOrEmpty()
                || queue.peekFirst().send?.completed() == true
                && queue.size < maxInFlightRequestsPerConnection
    }

    /**
     * Return the number of in-flight requests directed at the given node
     * @param node The node
     * @return The request count.
     */
    fun count(node: String): Int {
        val queue = requests[node]
        return queue?.size ?: 0
    }

    /**
     * Return true if there is no in-flight request directed at the given node and false otherwise
     */
    fun isEmpty(node: String): Boolean {
        val queue = requests[node]
        return queue.isNullOrEmpty()
    }

    /**
     * Count all in-flight requests for all nodes. This method is thread safe, but may lag the actual count.
     */
    fun count(): Int {
        return inFlightRequestCount.get()
    }

    val isEmpty: Boolean
        /**
         * Return true if there is no in-flight request and false otherwise
         */
        get() {
            for (deque in requests.values) {
                if (!deque.isEmpty()) return false
            }
            return true
        }

    /**
     * Clear out all the in-flight requests for the given node and return them
     *
     * @param node The node
     * @return All the in-flight requests for that node that have been removed
     */
    fun clearAll(node: String): Iterable<InFlightRequest> {
        val reqs = requests[node]
        return if (reqs == null) emptyList()
        else {
            val clearedRequests = requests.remove(node)!!
            inFlightRequestCount.getAndAdd(-clearedRequests.size)
            Iterable { clearedRequests.descendingIterator() }
        }
    }

    private fun hasExpiredRequest(now: Long, deque: Deque<InFlightRequest>): Boolean {
        return deque.any { request -> request.timeElapsedSinceSendMs(now) > request.requestTimeoutMs }
    }

    /**
     * Returns a list of nodes with pending in-flight request, that need to be timed out
     *
     * @param now current time in milliseconds
     * @return list of nodes
     */
    fun nodesWithTimedOutRequests(now: Long): List<String> {
        val nodeIds: MutableList<String> = mutableListOf()
        for ((nodeId, deque) in requests) {
            if (hasExpiredRequest(now, deque)) nodeIds.add(nodeId)
        }
        return nodeIds
    }
}
