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

import org.apache.kafka.clients.admin.internals.AdminApiFuture.SimpleAdminApiFuture
import org.apache.kafka.clients.admin.internals.AdminApiHandler.ApiResult
import org.apache.kafka.clients.admin.internals.AdminApiHandler.Unbatched
import org.apache.kafka.common.Node
import org.apache.kafka.common.errors.ClusterAuthorizationException
import org.apache.kafka.common.errors.TransactionalIdAuthorizationException
import org.apache.kafka.common.message.InitProducerIdRequestData
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.AbstractResponse
import org.apache.kafka.common.requests.FindCoordinatorRequest
import org.apache.kafka.common.requests.InitProducerIdRequest
import org.apache.kafka.common.requests.InitProducerIdResponse
import org.apache.kafka.common.utils.LogContext
import org.apache.kafka.common.utils.ProducerIdAndEpoch
import org.slf4j.Logger

class FenceProducersHandler(
    logContext: LogContext,
) : Unbatched<CoordinatorKey, ProducerIdAndEpoch>() {

    private val log: Logger = logContext.logger(FenceProducersHandler::class.java)

    private val lookupStrategy: AdminApiLookupStrategy<CoordinatorKey> =
        CoordinatorStrategy(FindCoordinatorRequest.CoordinatorType.TRANSACTION, logContext)

    override fun apiName(): String = "fenceProducer"

    override fun lookupStrategy(): AdminApiLookupStrategy<CoordinatorKey> = lookupStrategy

    override fun buildSingleRequest(
        brokerId: Int,
        key: CoordinatorKey
    ): InitProducerIdRequest.Builder {
        if (key.type != FindCoordinatorRequest.CoordinatorType.TRANSACTION) {
            throw IllegalArgumentException(
                "Invalid group coordinator key " + key +
                        " when building `InitProducerId` request"
            )
        }
        val data =
            InitProducerIdRequestData()
                // Because we never include a producer epoch or ID in this request, we expect that some errors
                // (such as PRODUCER_FENCED) will never be returned in the corresponding broker response.
                // If we ever modify this logic to include an epoch or producer ID, we will need to update the
                // error handling logic for this handler to accommodate these new errors.
                .setProducerEpoch(ProducerIdAndEpoch.NONE.epoch)
                .setProducerId(ProducerIdAndEpoch.NONE.producerId)
                .setTransactionalId(key.idValue)
                // Set transaction timeout to 1 since it's only being initialized to fence out
                // older producers with the same transactional ID, and shouldn't be used for any
                // actual record writes
                .setTransactionTimeoutMs(1)
        return InitProducerIdRequest.Builder(data)
    }

    override fun handleSingleResponse(
        broker: Node,
        key: CoordinatorKey,
        response: AbstractResponse
    ): ApiResult<CoordinatorKey, ProducerIdAndEpoch> {
        response as InitProducerIdResponse

        val error = Errors.forCode(response.data().errorCode)
        if (error !== Errors.NONE) return handleError(key, error)

        val completed = mapOf(
            key to ProducerIdAndEpoch(
                response.data().producerId,
                response.data().producerEpoch
            )
        )
        return ApiResult(completed, emptyMap(), emptyList())
    }

    private fun handleError(
        transactionalIdKey: CoordinatorKey,
        error: Errors
    ): ApiResult<CoordinatorKey, ProducerIdAndEpoch> {
        when (error) {
            Errors.CLUSTER_AUTHORIZATION_FAILED -> return ApiResult.failed(
                transactionalIdKey, ClusterAuthorizationException(
                    "InitProducerId request for transactionalId `${transactionalIdKey.idValue}` " +
                            "failed due to cluster authorization failure"
                )
            )

            Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED -> return ApiResult.failed(
                transactionalIdKey, TransactionalIdAuthorizationException(
                    "InitProducerId request for transactionalId `${transactionalIdKey.idValue}` " +
                            "failed due to transactional ID authorization failure"
                )
            )

            Errors.COORDINATOR_LOAD_IN_PROGRESS -> {
                // If the coordinator is in the middle of loading, then we just need to retry
                log.debug(
                    "InitProducerId request for transactionalId `{}` failed because the " +
                            "coordinator is still in the process of loading state. Will retry",
                    transactionalIdKey.idValue
                )
                return ApiResult.empty()
            }

            Errors.NOT_COORDINATOR, Errors.COORDINATOR_NOT_AVAILABLE -> {
                // If the coordinator is unavailable or there was a coordinator change, then we unmap
                // the key so that we retry the `FindCoordinator` request
                log.debug(
                    "InitProducerId request for transactionalId `{}` returned error {}. Will attempt " +
                            "to find the coordinator again and retry",
                    transactionalIdKey.idValue,
                    error
                )
                return ApiResult.unmapped(listOf(transactionalIdKey))
            }

            else -> return ApiResult.failed(
                transactionalIdKey, error.exception(
                    "InitProducerId request for transactionalId " +
                            "`${transactionalIdKey.idValue}` failed due to unexpected error"
                )
            )
        }
    }

    companion object {
        fun newFuture(
            transactionalIds: Collection<String>
        ): SimpleAdminApiFuture<CoordinatorKey, ProducerIdAndEpoch> {
            return AdminApiFuture.forKeys(buildKeyList(transactionalIds).toSet())
        }

        private fun buildKeyList(transactionalIds: Collection<String>): List<CoordinatorKey> {
            return transactionalIds.map(CoordinatorKey::byTransactionalId)
        }
    }
}
