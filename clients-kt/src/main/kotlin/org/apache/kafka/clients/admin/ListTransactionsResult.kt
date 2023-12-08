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

package org.apache.kafka.clients.admin

import org.apache.kafka.common.KafkaFuture
import org.apache.kafka.common.annotation.InterfaceStability.Evolving
import org.apache.kafka.common.internals.KafkaFutureImpl

/**
 * The result of the [Admin.listTransactions] call.
 *
 * The API of this class is evolving, see [Admin] for details.
 */
@Evolving
class ListTransactionsResult internal constructor(
    private val future: KafkaFuture<Map<Int, KafkaFutureImpl<Collection<TransactionListing>>>>,
) {
    /**
     * Get all transaction listings. If any of the underlying requests fail, then the future
     * returned from this method will also fail with the first encountered error.
     *
     * @return A future containing the collection of transaction listings. The future completes
     * when all transaction listings are available and fails after any non-retriable error.
     */
    fun all(): KafkaFuture<Collection<TransactionListing>> {
        return allByBrokerId().thenApply { map ->
            val allListings: MutableList<TransactionListing> = ArrayList()
            map.values.flatMap { it.toList() }
            for (listings in map.values) allListings.addAll(listings)

            allListings
        }
    }

    /**
     * Get a future which returns a map containing the underlying listing future for each broker in
     * the cluster. This is useful, for example, if a partial listing of transactions is sufficient,
     * or if you want more granular error details.
     *
     * @return A future containing a map of futures by broker which complete individually when their
     * respective transaction listings are available. The top-level future returned from this method
     * may fail if the admin client is unable to lookup the available brokers in the cluster.
     */
    fun byBrokerId(): KafkaFuture<Map<Int, KafkaFuture<Collection<TransactionListing>>>> {
        val result = KafkaFutureImpl<Map<Int, KafkaFuture<Collection<TransactionListing>>>>()
        future.whenComplete { brokerFutures, exception ->
            if (brokerFutures != null) result.complete(brokerFutures.toMap())
            else result.completeExceptionally(exception!!)
        }
        return result
    }

    /**
     * Get all transaction listings in a map which is keyed by the ID of respective broker that is
     * currently managing them. If any of the underlying requests fail, then the future returned
     * from this method will also fail with the first encountered error.
     *
     * @return A future containing a map from the broker ID to the transactions hosted by that
     * broker respectively. This future completes when all transaction listings are available and
     * fails after any non-retriable error.
     */
    fun allByBrokerId(): KafkaFuture<Map<Int, Collection<TransactionListing>>> {
        val allFuture = KafkaFutureImpl<Map<Int, Collection<TransactionListing>>>()
        val allListingsMap: MutableMap<Int, Collection<TransactionListing>> = HashMap()
        future.whenComplete { map, topLevelException ->
            if (topLevelException != null) {
                allFuture.completeExceptionally(topLevelException)
                return@whenComplete
            }
            val remainingResponses: MutableSet<Int> = map!!.keys.toHashSet()
            map.forEach { (brokerId, future) ->
                future.whenComplete { listings, brokerException ->
                    if (brokerException != null) allFuture.completeExceptionally(brokerException)
                    else if (!allFuture.isDone) {
                        allListingsMap[brokerId] = listings!!
                        remainingResponses.remove(brokerId)
                        if (remainingResponses.isEmpty()) allFuture.complete(allListingsMap)
                    }
                }
            }
        }
        return allFuture
    }
}
