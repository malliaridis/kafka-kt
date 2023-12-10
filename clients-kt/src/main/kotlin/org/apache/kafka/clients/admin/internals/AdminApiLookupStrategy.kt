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

import java.util.function.Function
import java.util.stream.Collectors
import org.apache.kafka.common.errors.UnsupportedVersionException
import org.apache.kafka.common.requests.AbstractRequest
import org.apache.kafka.common.requests.AbstractResponse

interface AdminApiLookupStrategy<T> {

    /**
     * Define the scope of a given key for lookup. Key lookups are complicated
     * by the need to accommodate different batching mechanics. For example,
     * a `Metadata` request supports arbitrary batching of topic partitions in
     * order to discover partitions leaders. This can be supported by returning
     * a single scope object for all keys.
     *
     * On the other hand, `FindCoordinator` requests only support lookup of a
     * single key. This can be supported by returning a different scope object
     * for each lookup key.
     *
     * Note that if the [ApiRequestScope.destinationBrokerId] maps to
     * a specific brokerId, then lookup will be skipped. See the use of
     * [StaticBrokerStrategy] in [DescribeProducersHandler] for
     * an example of this usage.
     *
     * @param key the lookup key
     *
     * @return request scope indicating how lookup requests can be batched together
     */
    fun lookupScope(key: T): ApiRequestScope

    /**
     * Build the lookup request for a set of keys. The grouping of the keys is controlled
     * through [lookupScope]. In other words, each set of keys that map
     * to the same request scope object will be sent to this method.
     *
     * @param keys the set of keys that require lookup
     *
     * @return a builder for the lookup request
     */
    fun buildRequest(keys: Set<T>): AbstractRequest.Builder<*>

    /**
     * Callback that is invoked when a lookup request returns successfully. The handler
     * should parse the response, check for errors, and return a result indicating
     * which keys were mapped to a brokerId successfully and which keys received
     * a fatal error (e.g. a topic authorization failure).
     *
     * Note that keys which receive a retriable error should be left out of the
     * result. They will be retried automatically. For example, if the response of
     * `FindCoordinator` request indicates an unavailable coordinator, then the key
     * should be left out of the result so that the request will be retried.
     *
     * @param keys the set of keys from the associated request
     * @param response the response received from the broker
     *
     * @return a result indicating which keys mapped successfully to a brokerId and
     * which encountered a fatal error
     */
    fun handleResponse(keys: Set<T>, response: AbstractResponse): LookupResult<T>

    /**
     * Callback that is invoked when a lookup request hits an UnsupportedVersionException.
     * Keys for which the exception cannot be handled and the request shouldn't be retried must be mapped
     * to an error and returned. The remainder of the keys will then be unmapped and the lookup request will
     * be retried for them.
     *
     * @return The failure mappings for the keys for which the exception cannot be handled and the
     * request shouldn't be retried. If the exception cannot be handled all initial keys will be in
     * the returned map.
     */
    fun handleUnsupportedVersionException(
        exception: UnsupportedVersionException,
        keys: Set<T>,
    ): Map<T, Throwable> = keys.associateWith { exception }

    /**
     * @property completedKeys The set of keys that have been completed by the lookup phase itself.
     * The driver will not attempt lookup or fulfillment for completed keys.
     * @property failedKeys The set of keys that have been mapped to a specific broker for
     * fulfillment of the API request.
     * @property mappedKeys The set of keys that have encountered a fatal error during the lookup
     * phase. The driver will not attempt lookup or fulfillment for failed keys.
     */
    class LookupResult<K>(
        val completedKeys: List<K> = emptyList(),
        val failedKeys: Map<K, Throwable>,
        val mappedKeys: Map<K, Int>,
    ) {

        companion object {
            fun <K> empty(): LookupResult<K> {
                return LookupResult(
                    failedKeys = emptyMap(),
                    mappedKeys = emptyMap(),
                )
            }

            fun <K> failed(key: K, exception: Throwable): LookupResult<K> {
                return LookupResult(
                    failedKeys = mapOf(key to exception),
                    mappedKeys = emptyMap(),
                )
            }

            fun <K> mapped(key: K, brokerId: Int): LookupResult<K> {
                return LookupResult(
                    failedKeys = emptyMap(),
                    mappedKeys = mapOf(key to brokerId),
                )
            }
        }
    }
}
