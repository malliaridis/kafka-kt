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

import java.util.stream.Collectors
import org.apache.kafka.common.Node
import org.apache.kafka.common.requests.AbstractRequest
import org.apache.kafka.common.requests.AbstractResponse

interface AdminApiHandler<K, V> {

    /**
     * Get a user-friendly name for the API this handler is implementing.
     */
    fun apiName(): String

    /**
     * Build the requests necessary for the given keys. The set of keys is derived by
     * [AdminApiDriver] during the lookup stage as the set of keys which all map
     * to the same destination broker. Handlers can choose to issue a single request for
     * all of the provided keys (see [Batched], issue one request per key (see
     * [Unbatched], or implement their own custom grouping logic if necessary.
     *
     * @param brokerId the target brokerId for the request
     * @param keys the set of keys that should be handled by this request
     *
     * @return a collection of [RequestAndKeys] for the requests containing the given keys
     */
    fun buildRequest(brokerId: Int, keys: Set<K>): Collection<RequestAndKeys<K>>

    /**
     * Callback that is invoked when a request returns successfully.
     * The handler should parse the response, check for errors, and return a
     * result which indicates which keys (if any) have either been completed or
     * failed with an unrecoverable error.
     *
     * It is also possible that the response indicates an incorrect target brokerId
     * (e.g. in the case of a NotLeader error when the request is bound for a partition
     * leader). In this case the key will be "unmapped" from the target brokerId
     * and lookup will be retried.
     *
     * Note that keys which received a retriable error should be left out of the
     * result. They will be retried automatically.
     *
     * @param broker the broker that the associated request was sent to
     * @param keys the set of keys from the associated request
     * @param response the response received from the broker
     *
     * @return result indicating key completion, failure, and unmapping
     */
    fun handleResponse(broker: Node, keys: Set<K>, response: AbstractResponse): ApiResult<K, V>

    /**
     * Get the lookup strategy that is responsible for finding the brokerId
     * which will handle each respective key.
     *
     * @return non-null lookup strategy
     */
    fun lookupStrategy(): AdminApiLookupStrategy<K>

    class ApiResult<K, V>(
        completedKeys: Map<K, V>,
        failedKeys: Map<K, Throwable>,
        unmappedKeys: List<K>
    ) {
        val completedKeys: Map<K, V>
        val failedKeys: Map<K, Throwable>
        val unmappedKeys: List<K>

        init {
            this.completedKeys = completedKeys
            this.failedKeys = failedKeys
            this.unmappedKeys = unmappedKeys
        }

        companion object {
            fun <K, V> completed(key: K, value: V): ApiResult<K, V> {
                return ApiResult(mapOf(key to value), emptyMap(), emptyList())
            }

            fun <K, V> failed(key: K, t: Throwable): ApiResult<K, V> {
                return ApiResult(emptyMap(), mapOf(key to t), emptyList())
            }

            fun <K, V> unmapped(keys: List<K>): ApiResult<K, V> {
                return ApiResult(emptyMap(), emptyMap(), keys)
            }

            fun <K, V> empty(): ApiResult<K, V> {
                return ApiResult(emptyMap(), emptyMap(), emptyList())
            }
        }
    }

    class RequestAndKeys<K>(val request: AbstractRequest.Builder<*>, val keys: Set<K>)

    /**
     * An [AdminApiHandler] that will group multiple keys into a single request when possible.
     * Keys will be grouped together whenever they target the same broker. This type of handler
     * should be used when interacting with broker APIs that can act on multiple keys at once, such
     * as describing or listing transactions.
     */
    abstract class Batched<K, V> : AdminApiHandler<K, V> {
        abstract fun buildBatchedRequest(brokerId: Int, keys: Set<K>): AbstractRequest.Builder<*>
        override fun buildRequest(brokerId: Int, keys: Set<K>): Collection<RequestAndKeys<K>> {
            return setOf(RequestAndKeys(buildBatchedRequest(brokerId, keys), keys))
        }
    }

    /**
     * An [AdminApiHandler] that will create one request per key, not performing any grouping based
     * on the targeted broker. This type of handler should only be used for broker APIs that do not accept
     * multiple keys at once, such as initializing a transactional producer.
     */
    abstract class Unbatched<K, V> : AdminApiHandler<K, V> {
        abstract fun buildSingleRequest(brokerId: Int, key: K): AbstractRequest.Builder<*>
        abstract fun handleSingleResponse(
            broker: Node,
            key: K,
            response: AbstractResponse
        ): ApiResult<K, V>

        override fun buildRequest(brokerId: Int, keys: Set<K>): Collection<RequestAndKeys<K>> {
            return keys.stream()
                .map { key: K ->
                    RequestAndKeys(
                        buildSingleRequest(brokerId, key),
                        setOf(key)
                    )
                }
                .collect(Collectors.toSet())
        }

        override fun handleResponse(
            broker: Node,
            keys: Set<K>,
            response: AbstractResponse
        ): ApiResult<K, V> {
            require(keys.size == 1) {
                "Unbatched admin handler should only be required to handle responses for a " +
                        "single key at a time"
            }
            val key = keys.first()
            return handleSingleResponse(broker, key, response)
        }
    }
}
