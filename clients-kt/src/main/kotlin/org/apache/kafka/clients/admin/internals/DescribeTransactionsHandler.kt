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

import org.apache.kafka.clients.admin.TransactionDescription
import org.apache.kafka.clients.admin.TransactionState
import org.apache.kafka.clients.admin.internals.AdminApiFuture.SimpleAdminApiFuture
import org.apache.kafka.clients.admin.internals.AdminApiHandler.ApiResult
import org.apache.kafka.clients.admin.internals.AdminApiHandler.Batched
import org.apache.kafka.common.Node
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.TransactionalIdAuthorizationException
import org.apache.kafka.common.errors.TransactionalIdNotFoundException
import org.apache.kafka.common.message.DescribeTransactionsRequestData
import org.apache.kafka.common.message.DescribeTransactionsResponseData
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.AbstractResponse
import org.apache.kafka.common.requests.DescribeTransactionsRequest
import org.apache.kafka.common.requests.DescribeTransactionsResponse
import org.apache.kafka.common.requests.FindCoordinatorRequest.CoordinatorType
import org.apache.kafka.common.utils.LogContext
import org.slf4j.Logger

class DescribeTransactionsHandler(
    logContext: LogContext
) : Batched<CoordinatorKey, TransactionDescription>() {

    private val log: Logger = logContext.logger(DescribeTransactionsHandler::class.java)

    private val lookupStrategy: AdminApiLookupStrategy<CoordinatorKey> =
        CoordinatorStrategy(CoordinatorType.TRANSACTION, logContext)


    override fun apiName(): String = "describeTransactions"

    override fun lookupStrategy(): AdminApiLookupStrategy<CoordinatorKey> = lookupStrategy

    override fun buildBatchedRequest(
        brokerId: Int,
        keys: Set<CoordinatorKey>
    ): DescribeTransactionsRequest.Builder {
        val request = DescribeTransactionsRequestData()
        val transactionalIds = keys.map { key: CoordinatorKey ->
            require(key.type == CoordinatorType.TRANSACTION) {
                "Invalid group coordinator key $key when building `DescribeTransaction` request"
            }

            key.idValue
        }
        request.setTransactionalIds(transactionalIds)
        return DescribeTransactionsRequest.Builder(request)
    }

    override fun handleResponse(
        broker: Node,
        keys: Set<CoordinatorKey>,
        response: AbstractResponse,
    ): ApiResult<CoordinatorKey, TransactionDescription> {
        response as DescribeTransactionsResponse
        val completed: MutableMap<CoordinatorKey, TransactionDescription> = HashMap()
        val failed: MutableMap<CoordinatorKey, Throwable> = HashMap()
        val unmapped: MutableList<CoordinatorKey> = ArrayList()

        response.data().transactionStates.forEach { transactionState ->

            val transactionalIdKey = CoordinatorKey
                .byTransactionalId(transactionState.transactionalId)

            if (!keys.contains(transactionalIdKey)) {
                log.warn(
                    "Response included transactionalId `{}`, which was not requested",
                    transactionState.transactionalId
                )
                return@forEach
            }

            val error = Errors.forCode(transactionState.errorCode)
            if (error !== Errors.NONE) {
                handleError(transactionalIdKey, error, failed, unmapped)
                return@forEach
            }

            val transactionStartTimeMs =
                if (transactionState.transactionStartTimeMs < 0) null
                else transactionState.transactionStartTimeMs

            completed[transactionalIdKey] = TransactionDescription(
                coordinatorId = broker.id,
                state = TransactionState.parse(transactionState.transactionState),
                producerId = transactionState.producerId,
                producerEpoch = transactionState.producerEpoch.toInt(),
                transactionTimeoutMs = transactionState.transactionTimeoutMs.toLong(),
                transactionStartTimeMs = transactionStartTimeMs,
                topicPartitions = collectTopicPartitions(transactionState)
            )
        }
        return ApiResult(completed, failed, unmapped)
    }

    private fun collectTopicPartitions(
        transactionState: DescribeTransactionsResponseData.TransactionState
    ): Set<TopicPartition> {
        val res: MutableSet<TopicPartition> = HashSet()

        transactionState.topics.forEach { topicData ->
            val topic = topicData.topic

            topicData.partitions.forEach { partitionId ->
                res.add(TopicPartition(topic, partitionId))
            }
        }
        return res
    }

    private fun handleError(
        transactionalIdKey: CoordinatorKey,
        error: Errors,
        failed: MutableMap<CoordinatorKey, Throwable>,
        unmapped: MutableList<CoordinatorKey>
    ) {
        when (error) {
            Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED -> failed[transactionalIdKey] =
                TransactionalIdAuthorizationException(
                    "DescribeTransactions request for transactionalId " +
                            "`${transactionalIdKey.idValue}` failed due to authorization failure"
                )

            Errors.TRANSACTIONAL_ID_NOT_FOUND -> failed[transactionalIdKey] =
                TransactionalIdNotFoundException(
                    "DescribeTransactions request for transactionalId " +
                            "`${transactionalIdKey.idValue}` failed because the ID could not be found"
                )

            Errors.COORDINATOR_LOAD_IN_PROGRESS ->
                // If the coordinator is in the middle of loading, then we just need to retry
                log.debug(
                    "DescribeTransactions request for transactionalId `{}` failed because the " +
                            "coordinator is still in the process of loading state. Will retry",
                    transactionalIdKey.idValue
                )

            Errors.NOT_COORDINATOR, Errors.COORDINATOR_NOT_AVAILABLE -> {
                // If the coordinator is unavailable or there was a coordinator change, then we unmap
                // the key so that we retry the `FindCoordinator` request
                unmapped.add(transactionalIdKey)
                log.debug(
                    "DescribeTransactions request for transactionalId `{}` returned error {}. " +
                            "Will attempt to find the coordinator again and retry",
                    transactionalIdKey.idValue,
                    error,
                )
            }

            else -> failed[transactionalIdKey] = error.exception(
                "DescribeTransactions request for transactionalId `${transactionalIdKey.idValue}`" +
                        " failed due to unexpected error"
            )!!
        }
    }

    companion object {
        fun newFuture(
            transactionalIds: Collection<String>,
        ): SimpleAdminApiFuture<CoordinatorKey, TransactionDescription> {
            return AdminApiFuture.forKeys(buildKeySet(transactionalIds))
        }

        private fun buildKeySet(transactionalIds: Collection<String>): Set<CoordinatorKey> {
            return transactionalIds.map(CoordinatorKey::byTransactionalId).toSet()
        }
    }
}
