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

import org.apache.kafka.clients.admin.internals.AllBrokersStrategy.BrokerKey
import org.apache.kafka.common.internals.KafkaFutureImpl
import org.apache.kafka.common.message.MetadataRequestData
import org.apache.kafka.common.message.MetadataResponseData.MetadataResponseBroker
import org.apache.kafka.common.requests.AbstractResponse
import org.apache.kafka.common.requests.MetadataRequest
import org.apache.kafka.common.requests.MetadataResponse
import org.apache.kafka.common.utils.LogContext
import org.slf4j.Logger

/**
 * This class is used for use cases which require requests to be sent to all
 * brokers in the cluster.
 *
 * This is a slightly degenerate case of a lookup strategy in the sense that
 * the broker IDs are used as both the keys and values. Also, unlike
 * [CoordinatorStrategy] and [PartitionLeaderStrategy], we do not
 * know the set of keys ahead of time: we require the initial lookup in order
 * to discover what the broker IDs are. This is represented with a more complex
 * type `Future<Map<Integer, Future<V>>` in the admin API result type.
 * For example, see [org.apache.kafka.clients.admin.ListTransactionsResult].
 */
class AllBrokersStrategy(
    logContext: LogContext
) : AdminApiLookupStrategy<BrokerKey> {
    private val log: Logger = logContext.logger(AllBrokersStrategy::class.java)

    override fun lookupScope(key: BrokerKey): ApiRequestScope {
        return SINGLE_REQUEST_SCOPE
    }

    override fun buildRequest(keys: Set<BrokerKey>): MetadataRequest.Builder {
        validateLookupKeys(keys)
        // Send empty `Metadata` request. We are only interested in the brokers from the response
        return MetadataRequest.Builder(MetadataRequestData())
    }

    override fun handleResponse(
        keys: Set<BrokerKey>,
        response: AbstractResponse
    ): AdminApiLookupStrategy.LookupResult<BrokerKey> {
        validateLookupKeys(keys)
        response as MetadataResponse
        val brokers = response.data().brokers()
        if (brokers.isEmpty()) {
            log.debug("Metadata response contained no brokers. Will backoff and retry")
            return AdminApiLookupStrategy.LookupResult.empty()
        } else log.debug("Discovered all brokers {} to send requests to", brokers)

        val brokerKeys = brokers.associateBy(
            keySelector = { BrokerKey(it.nodeId()) },
            valueTransform = { it.nodeId }
        )

        return AdminApiLookupStrategy.LookupResult(
            completedKeys = listOf(ANY_BROKER),
            failedKeys = emptyMap(),
            mappedKeys = brokerKeys,
        )
    }

    private fun validateLookupKeys(keys: Set<BrokerKey>) {
        require(keys.size == 1) { "Unexpected key set: $keys" }
        val key = keys.iterator().next()
        require(key === ANY_BROKER) { "Unexpected key set: $keys" }
    }

    data class BrokerKey(val brokerId: Int?) {
        override fun toString(): String = "BrokerKey(brokerId=$brokerId)"
    }

    class AllBrokersFuture<V> : AdminApiFuture<BrokerKey, V> {

        private val future = KafkaFutureImpl<Map<Int, KafkaFutureImpl<V>>>()
        private val brokerFutures: MutableMap<Int, KafkaFutureImpl<V>> = HashMap()

        override fun lookupKeys(): Set<BrokerKey> {
            return LOOKUP_KEYS
        }

        override fun completeLookup(brokerIdMapping: Map<BrokerKey, Int>) {
            brokerIdMapping.forEach { (brokerKey: BrokerKey, brokerId: Int) ->
                require(brokerId == (brokerKey.brokerId ?: -1)) {
                    "Invalid lookup mapping $brokerKey -> $brokerId"
                }
                brokerFutures[brokerId] = KafkaFutureImpl()
            }
            future.complete(brokerFutures)
        }

        fun completeLookupExceptionally(lookupErrors: Map<BrokerKey?, Throwable?>) {
            require(LOOKUP_KEYS == lookupErrors.keys) {
                "Unexpected keys among lookup errors: $lookupErrors"
            }
            future.completeExceptionally(lookupErrors[ANY_BROKER]!!)
        }

        override fun complete(values: Map<BrokerKey, V>) {
            values.forEach { (key: BrokerKey, value: V) -> complete(key, value) }
        }

        private fun complete(key: BrokerKey, value: V) {
            require(key !== ANY_BROKER) { "Invalid attempt to complete with lookup key sentinel" }
            futureOrThrow(key).complete(value)
        }

        override fun completeExceptionally(errors: Map<BrokerKey, Throwable>) {
            errors.forEach { (key: BrokerKey, t: Throwable) -> completeExceptionally(key, t) }
        }

        private fun completeExceptionally(key: BrokerKey, t: Throwable) {
            if (key === ANY_BROKER) {
                future.completeExceptionally(t)
            } else {
                futureOrThrow(key).completeExceptionally(t)
            }
        }

        fun all(): KafkaFutureImpl<Map<Int, KafkaFutureImpl<V>>> {
            return future
        }

        private fun futureOrThrow(key: BrokerKey): KafkaFutureImpl<V> {
            requireNotNull(key.brokerId) { "Attempt to complete with invalid key: $key" }
            return requireNotNull(brokerFutures[key.brokerId]) {
                "Attempt to complete with unknown broker id: ${key.brokerId}"
            }
        }
    }

    companion object {
        val ANY_BROKER = BrokerKey(brokerId = null)
        val LOOKUP_KEYS = setOf(ANY_BROKER)
        private val SINGLE_REQUEST_SCOPE: ApiRequestScope = object : ApiRequestScope {}
    }
}
