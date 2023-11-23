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

import org.apache.kafka.clients.admin.ListTransactionsOptions
import org.apache.kafka.clients.admin.TransactionListing
import org.apache.kafka.clients.admin.TransactionState
import org.apache.kafka.clients.admin.internals.AdminApiHandler.ApiResult
import org.apache.kafka.clients.admin.internals.AdminApiHandler.Batched
import org.apache.kafka.clients.admin.internals.AllBrokersStrategy.AllBrokersFuture
import org.apache.kafka.clients.admin.internals.AllBrokersStrategy.BrokerKey
import org.apache.kafka.common.Node
import org.apache.kafka.common.errors.CoordinatorNotAvailableException
import org.apache.kafka.common.message.ListTransactionsRequestData
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.AbstractResponse
import org.apache.kafka.common.requests.ListTransactionsRequest
import org.apache.kafka.common.requests.ListTransactionsResponse
import org.apache.kafka.common.utils.LogContext
import org.slf4j.Logger

class ListTransactionsHandler(
    private val options: ListTransactionsOptions,
    logContext: LogContext
) : Batched<BrokerKey, Collection<TransactionListing>>() {

    private val log: Logger = logContext.logger(ListTransactionsHandler::class.java)

    private val lookupStrategy: AllBrokersStrategy = AllBrokersStrategy(logContext)

    override fun apiName(): String = "listTransactions"

    override fun lookupStrategy(): AdminApiLookupStrategy<BrokerKey> = lookupStrategy

    override fun buildBatchedRequest(
        brokerId: Int,
        keys: Set<BrokerKey>,
    ): ListTransactionsRequest.Builder {
        val request = ListTransactionsRequestData()
        request.setProducerIdFilters(options.filteredProducerIds().toLongArray())
        request.setStateFilters(options.filteredStates().map { obj -> obj.toString() })
        return ListTransactionsRequest.Builder(request)
    }

    override fun handleResponse(
        broker: Node,
        keys: Set<BrokerKey>,
        response: AbstractResponse,
    ): ApiResult<BrokerKey, Collection<TransactionListing>> {
        val brokerId = broker.id
        val key = requireSingleton(keys, brokerId)
        val res = response as ListTransactionsResponse
        val error = Errors.forCode(res.data().errorCode)
        if (error === Errors.COORDINATOR_LOAD_IN_PROGRESS) {
            log.debug(
                "The `ListTransactions` request sent to broker {} failed because the " +
                        "coordinator is still loading state. Will try again after backing off",
                brokerId
            )
            return ApiResult.empty()
        } else if (error === Errors.COORDINATOR_NOT_AVAILABLE) {
            log.debug(
                "The `ListTransactions` request sent to broker {} failed because the " +
                        "coordinator is shutting down",
                brokerId,
            )
            return ApiResult.failed(
                key,
                CoordinatorNotAvailableException(
                    "ListTransactions request sent to broker $brokerId failed because the " +
                            "coordinator is shutting down"
                ),
            )
        } else if (error !== Errors.NONE) {
            log.error(
                "The `ListTransactions` request sent to broker {} failed because of an " +
                        "unexpected error {}",
                brokerId,
                error,
            )
            return ApiResult.failed(
                key,
                error.exception(
                    "ListTransactions request sent to broker $brokerId failed with an unexpected " +
                            "exception"
                ),
            )
        } else {
            val listings = res.data().transactionStates.map { transactionState ->
                TransactionListing(
                    transactionState.transactionalId,
                    transactionState.producerId,
                    TransactionState.parse(transactionState.transactionState),
                )
            }

            return ApiResult.completed(key, listings)
        }
    }

    private fun requireSingleton(
        keys: Set<BrokerKey>,
        brokerId: Int,
    ): BrokerKey {
        require ( keys.size == 1) { "Unexpected key set: $keys" }

        val key = keys.iterator().next()
        require (key.brokerId != null && key.brokerId == brokerId) {
            "Unexpected broker key: $key"
        }

        return key
    }

    companion object {
        fun newFuture(): AllBrokersFuture<Collection<TransactionListing>> = AllBrokersFuture()
    }
}
