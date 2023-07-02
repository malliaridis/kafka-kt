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

import org.apache.kafka.common.KafkaFuture
import org.apache.kafka.common.internals.KafkaFutureImpl

interface AdminApiFuture<K, V> {

    /**
     * The initial set of lookup keys. Although this will usually match the fulfillment keys, it
     * does not necessarily have to. For example, in the case of
     * [AllBrokersStrategy.AllBrokersFuture], we use the lookup phase in order to discover the set
     * of keys that will be searched during the fulfillment phase.
     *
     * @return non-empty set of initial lookup keys
     */
    fun lookupKeys(): Set<K>

    /**
     * Complete the futures associated with the given keys.
     *
     * @param values the completed keys with their respective values
     */
    fun complete(values: Map<K, V>)

    /**
     * Invoked when lookup of a set of keys succeeds.
     *
     * @param brokerIdMapping the discovered mapping from key to the respective brokerId that will
     * handle the fulfillment request
     */
    fun completeLookup(brokerIdMapping: Map<K, Int>) {}

    /**
     * Invoked when lookup fails with a fatal error on a set of keys.
     *
     * @param lookupErrors the set of keys that failed lookup with their respective errors
     */
    fun completeLookupExceptionally(lookupErrors: Map<K, Throwable>) {
        completeExceptionally(lookupErrors)
    }

    /**
     * Complete the futures associated with the given keys exceptionally.
     *
     * @param errors the failed keys with their respective errors
     */
    fun completeExceptionally(errors: Map<K, Throwable>)

    /**
     * This class can be used when the set of keys is known ahead of time.
     */
    class SimpleAdminApiFuture<K, V>(keys: Set<K>) : AdminApiFuture<K, V> {
        private val futures: Map<K, KafkaFuture<V>>

        init {
            futures = keys.associateBy({it}, { KafkaFutureImpl() })
        }

        override fun lookupKeys(): Set<K> = futures.keys

        override fun complete(values: Map<K, V>) = values.forEach(::complete)

        private fun complete(key: K, value: V) {
            futureOrThrow(key).complete(value)
        }

        override fun completeExceptionally(errors: Map<K, Throwable>) =
            errors.forEach(::completeExceptionally)

        private fun completeExceptionally(key: K, t: Throwable) {
            futureOrThrow(key).completeExceptionally(t)
        }

        private fun futureOrThrow(key: K): KafkaFutureImpl<V> {
            // The below typecast is safe because we initialise futures using only KafkaFutureImpl.
            return requireNotNull(futures[key] as KafkaFutureImpl<V>) {
                "Attempt to complete future for $key, which was not requested"
            }
        }

        fun all(): Map<K, KafkaFuture<V>> = futures

        operator fun get(key: K): KafkaFuture<V> = futures[key]!!
    }

    companion object {
        fun <K, V> forKeys(keys: Set<K>): SimpleAdminApiFuture<K, V> = SimpleAdminApiFuture(keys)
    }
}
