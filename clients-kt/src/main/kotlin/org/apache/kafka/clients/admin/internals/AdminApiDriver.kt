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

package org.apache.kafka.clients.admin.internals

import java.util.*
import java.util.function.BiFunction
import java.util.function.Consumer
import org.apache.kafka.clients.admin.internals.AdminApiHandler.RequestAndKeys
import org.apache.kafka.common.Node
import org.apache.kafka.common.errors.DisconnectException
import org.apache.kafka.common.requests.AbstractRequest
import org.apache.kafka.common.requests.AbstractResponse
import org.apache.kafka.common.requests.FindCoordinatorRequest.NoBatchedFindCoordinatorsException
import org.apache.kafka.common.requests.OffsetFetchRequest.NoBatchedOffsetFetchRequestException
import org.apache.kafka.common.utils.LogContext
import org.slf4j.Logger

/**
 * The KafkaAdminClient`'s internal `Call` primitive is not a good fit for multi-stage
 * request workflows such as we see with the group coordinator APIs or any request which
 * needs to be sent to a partition leader. Typically these APIs have two concrete stages:
 *
 * 1. Lookup: Find the broker that can fulfill the request (e.g. partition leader or group
 * coordinator)
 * 2. Fulfillment: Send the request to the broker found in the first step
 *
 * This is complicated by the fact that `Admin` APIs are typically batched, which
 * means the Lookup stage may result in a set of brokers. For example, take a `ListOffsets`
 * request for a set of topic partitions. In the Lookup stage, we will find the partition
 * leaders for this set of partitions; in the Fulfillment stage, we will group together
 * partition according to the IDs of the discovered leaders.
 *
 * Additionally, the flow between these two stages is bi-directional. We may find after
 * sending a `ListOffsets` request to an expected leader that there was a leader change.
 * This would result in a topic partition being sent back to the Lookup stage.
 *
 * Managing this complexity by chaining together `Call` implementations is challenging
 * and messy, so instead we use this class to do the bookkeeping. It handles both the
 * batching aspect as well as the transitions between the Lookup and Fulfillment stages.
 *
 * Note that the interpretation of the `retries` configuration becomes ambiguous
 * for this kind of pipeline. We could treat it as an overall limit on the number
 * of requests that can be sent, but that is not very useful because each pipeline
 * has a minimum number of requests that need to be sent in order to satisfy the request.
 * Instead, we treat this number of retries independently at each stage so that each
 * stage has at least one opportunity to complete. So if a user sets `retries=1`, then
 * the full pipeline can still complete as long as there are no request failures.
 *
 * @param K The key type, which is also the granularity of the request routing (e.g.
 * this could be `TopicPartition` in the case of requests intended for a partition
 * leader or the `GroupId` in the case of consumer group requests intended for
 * the group coordinator)
 * @param V The fulfillment type for each key (e.g. this could be consumer group state
 * when the key type is a consumer `GroupId`)
 */
class AdminApiDriver<K, V>(
    private val handler: AdminApiHandler<K, V>,
    private val future: AdminApiFuture<K, V>,
    private val deadlineMs: Long,
    private val retryBackoffMs: Long,
    logContext: LogContext
) {

    private val log: Logger = logContext.logger(AdminApiDriver::class.java)

    private val lookupMap = BiMultimap<ApiRequestScope, K>()

    private val fulfillmentMap = BiMultimap<FulfillmentScope, K>()

    private val requestStates: MutableMap<ApiRequestScope, RequestState> =
        HashMap<ApiRequestScope, RequestState>()

    init {
        retryLookup(future.lookupKeys())
    }

    /**
     * Associate a key with a brokerId. This is called after a response in the Lookup
     * stage reveals the mapping (e.g. when the `FindCoordinator` tells us the group
     * coordinator for a specific consumer group).
     */
    private fun map(key: K, brokerId: Int) {
        lookupMap.remove(key)
        fulfillmentMap.put(FulfillmentScope(brokerId), key)
    }

    /**
     * Disassociate a key from the currently mapped brokerId. This will send the key
     * back to the Lookup stage, which will allow us to attempt lookup again.
     */
    private fun unmap(key: K) {
        fulfillmentMap.remove(key)

        handler.lookupStrategy()
            .lookupScope(key)
            .destinationBrokerId()
            ?.let { fulfillmentMap.put(FulfillmentScope(it), key) }
            ?: run { lookupMap.put(handler.lookupStrategy().lookupScope(key), key) }
    }

    private fun clear(keys: Collection<K>) {
        keys.forEach(Consumer { key: K ->
            lookupMap.remove(key)
            fulfillmentMap.remove(key)
        })
    }

    fun keyToBrokerId(key: K): Int? = fulfillmentMap.getKey(key)?.destinationBrokerId

    /**
     * Complete the future associated with the given key exceptionally. After is called,
     * the key will be taken out of both the Lookup and Fulfillment stages so that request
     * are not retried.
     */
    private fun completeExceptionally(errors: Map<K, Throwable>) {
        if (errors.isNotEmpty()) {
            future.completeExceptionally(errors)
            clear(errors.keys)
        }
    }

    private fun completeLookupExceptionally(errors: Map<K, Throwable>) {
        if (errors.isNotEmpty()) {
            future.completeLookupExceptionally(errors)
            clear(errors.keys)
        }
    }

    private fun retryLookup(keys: Collection<K>) {
        keys.forEach(Consumer { key: K -> unmap(key) })
    }

    /**
     * Complete the future associated with the given key. After this is called, all keys will
     * be taken out of both the Lookup and Fulfillment stages so that request are not retried.
     */
    private fun complete(values: Map<K, V>) {
        if (values.isNotEmpty()) {
            future.complete(values)
            clear(values.keys)
        }
    }

    private fun completeLookup(brokerIdMapping: Map<K, Int>) {
        if (brokerIdMapping.isNotEmpty()) {
            future.completeLookup(brokerIdMapping)
            brokerIdMapping.forEach { (key: K, brokerId: Int) ->
                this.map(
                    key,
                    brokerId
                )
            }
        }
    }

    /**
     * Check whether any requests need to be sent. This should be called immediately
     * after the driver is constructed and then again after each request returns
     * (i.e. after [onFailure] or
     * [onResponse]).
     *
     * @return A list of requests that need to be sent
     */
    fun poll(): List<RequestSpec<K>> {
        val requests: MutableList<RequestSpec<K>> = ArrayList()
        collectLookupRequests(requests)
        collectFulfillmentRequests(requests)
        return requests
    }

    /**
     * Callback that is invoked when a `Call` returns a response successfully.
     */
    fun onResponse(
        currentTimeMs: Long,
        spec: RequestSpec<K>,
        response: AbstractResponse,
        node: Node?
    ) {
        clearInflightRequest(currentTimeMs, spec)
        if (spec.scope is FulfillmentScope) handler.handleResponse(
            broker = node!!,
            keys = spec.keys,
            response = response,
        ).run {
            complete(completedKeys)
            completeExceptionally(failedKeys)
            retryLookup(unmappedKeys)
        }
        else handler.lookupStrategy().handleResponse(
            keys = spec.keys,
            response = response,
        ).run {
            completedKeys.forEach { value -> lookupMap.remove(value) }
            completeLookup(mappedKeys)
            completeLookupExceptionally(failedKeys)
        }
    }

    /**
     * Callback that is invoked when a `Call` is failed.
     */
    fun onFailure(
        currentTimeMs: Long,
        spec: RequestSpec<K>,
        t: Throwable
    ) {
        clearInflightRequest(currentTimeMs, spec)
        if (t is DisconnectException) {
            log.debug(
                "Node disconnected before response could be received for request {}. " +
                        "Will attempt retry", spec.request
            )

            // After a disconnect, we want the driver to attempt to lookup the key
            // again. This gives us a chance to find a new coordinator or partition
            // leader for example.
            val keysToUnmap = spec.keys.filter { o: K -> future.lookupKeys().contains(o) }
            retryLookup(keysToUnmap)
        } else if (t is NoBatchedFindCoordinatorsException || t is NoBatchedOffsetFetchRequestException) {
            (handler.lookupStrategy() as CoordinatorStrategy).disableBatch()
            val keysToUnmap = spec.keys.filter { o: K -> future.lookupKeys().contains(o) }
            retryLookup(keysToUnmap)
        } else {
            val errors = spec.keys.associateBy({ it }, { t })
            if (spec.scope is FulfillmentScope) completeExceptionally(errors)
            else completeLookupExceptionally(errors)
        }
    }

    private fun clearInflightRequest(currentTimeMs: Long, spec: RequestSpec<K>) {
        requestStates[spec.scope]?.let { requestState ->
            // Only apply backoff if it's not a retry of a lookup request
            if (spec.scope is FulfillmentScope) requestState.clearInflight(currentTimeMs + retryBackoffMs)
            else requestState.clearInflight(currentTimeMs)
        }
    }

    private fun <T : ApiRequestScope?> collectRequests(
        requests: MutableList<RequestSpec<K>>,
        multimap: BiMultimap<T, K>,
        buildRequest: BiFunction<Set<K>, T, Collection<RequestAndKeys<K>>>
    ) {
        multimap.entrySet().forEach { (scope, keys) ->
            if (keys.isEmpty()) return@forEach

            val requestState: RequestState = requestStates.computeIfAbsent(scope as ApiRequestScope) { RequestState() }

            if (requestState.hasInflight()) return@forEach

            // Copy the keys to avoid exposing the underlying mutable set
            val copyKeys = Collections.unmodifiableSet(keys.toHashSet())
            val newRequests = buildRequest.apply(copyKeys, scope)
            if (newRequests.isEmpty()) return

            // Only process the first request; all the remaining requests will be targeted at the same broker
            // and we don't want to issue more than one fulfillment request per broker at a time
            val newRequest = newRequests.first()
            val spec = RequestSpec(
                name = handler.apiName() + "(api=${newRequest.request.apiKey})",
                scope = scope,
                keys = newRequest.keys,
                request = newRequest.request,
                nextAllowedTryMs = requestState.nextAllowedRetryMs,
                deadlineMs = deadlineMs,
                tries = requestState.tries
            )
            requestState.setInflight(spec)
            requests.add(spec)
        }
    }

    private fun collectLookupRequests(requests: MutableList<RequestSpec<K>>) {
        collectRequests(requests, lookupMap) { keys: Set<K>, _: ApiRequestScope ->
            listOf(RequestAndKeys(handler.lookupStrategy().buildRequest(keys), keys))
        }
    }

    private fun collectFulfillmentRequests(requests: MutableList<RequestSpec<K>>) {
        collectRequests(requests, fulfillmentMap) { keys: Set<K>, scope: FulfillmentScope ->
            handler.buildRequest(scope.destinationBrokerId, keys)
        }
    }

    /**
     * This is a helper class which helps us to map requests that need to be sent
     * to the internal `Call` implementation that is used internally in
     * [org.apache.kafka.clients.admin.KafkaAdminClient].
     */
    data class RequestSpec<K>(
        val name: String,
        val scope: ApiRequestScope,
        val keys: Set<K>,
        val request: AbstractRequest.Builder<*>,
        val nextAllowedTryMs: Long,
        val deadlineMs: Long,
        val tries: Int
    ) {
        override fun toString(): String {
            return "RequestSpec(" +
                    "name=$name" +
                    ", scope=$scope" +
                    ", keys=$keys" +
                    ", request=$request" +
                    ", nextAllowedTryMs=$nextAllowedTryMs" +
                    ", deadlineMs=$deadlineMs" +
                    ", tries=$tries" +
                    ')'
        }
    }

    /**
     * Helper class used to track the request state within each request scope.
     * This class enforces a maximum number of inflight request and keeps track
     * of backoff/retry state.
     */
    private inner class RequestState {
        private var inflightRequest: RequestSpec<K>? = null
        var tries = 0
        var nextAllowedRetryMs: Long = 0

        fun hasInflight(): Boolean {
            return inflightRequest != null
        }

        fun clearInflight(nextAllowedRetryMs: Long) {
            inflightRequest = null
            this.nextAllowedRetryMs = nextAllowedRetryMs
        }

        fun setInflight(spec: RequestSpec<K>?) {
            inflightRequest = spec
            tries++
        }
    }

    /**
     * Completion of the Lookup stage results in a destination broker to send the
     * fulfillment request to. Each destination broker in the Fulfillment stage
     * gets its own request scope.
     */
    private data class FulfillmentScope(
        val destinationBrokerId: Int,
    ) : ApiRequestScope {

        @Deprecated(
            message = "Use property instead.",
            replaceWith = ReplaceWith("destinationBrokerId")
        )
        override fun destinationBrokerId(): Int {
            return destinationBrokerId
        }
    }

    /**
     * Helper class which maintains a bi-directional mapping from a key to a set of values.
     * Each value can map to one and only one key, but many values can be associated with
     * a single key.
     *
     * @param <K> The key type
     * @param <V> The value type
    </V></K> */
    private class BiMultimap<K, V> {
        private val reverseMap: MutableMap<V, K> = HashMap()
        private val map: MutableMap<K, MutableSet<V>> = HashMap()
        fun put(key: K, value: V) {
            remove(value)
            reverseMap[value] = key
            map.computeIfAbsent(key) { _: K -> hashSetOf() }.add(value)
        }

        fun remove(value: V) {
            val key = reverseMap.remove(value)
            if (key != null) {
                val set = map[key]
                if (set != null) {
                    set.remove(value)
                    if (set.isEmpty()) {
                        map.remove(key)
                    }
                }
            }
        }

        fun getKey(value: V): K? {
            return reverseMap[value]
        }

        fun entrySet(): Set<Map.Entry<K, Set<V>>> {
            return map.entries
        }
    }
}
