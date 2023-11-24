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
import java.util.stream.Collectors
import org.apache.kafka.common.errors.GroupAuthorizationException
import org.apache.kafka.common.errors.InvalidGroupIdException
import org.apache.kafka.common.errors.TransactionalIdAuthorizationException
import org.apache.kafka.common.message.FindCoordinatorRequestData
import org.apache.kafka.common.message.FindCoordinatorResponseData
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.AbstractResponse
import org.apache.kafka.common.requests.FindCoordinatorRequest
import org.apache.kafka.common.requests.FindCoordinatorRequest.CoordinatorType
import org.apache.kafka.common.requests.FindCoordinatorResponse
import org.apache.kafka.common.utils.LogContext
import org.slf4j.Logger

class CoordinatorStrategy(
    private val type: CoordinatorType,
    logContext: LogContext
) : AdminApiLookupStrategy<CoordinatorKey> {

    private val log: Logger = logContext.logger(CoordinatorStrategy::class.java)

    private var unrepresentableKeys: List<CoordinatorKey> = emptyList()

    var batch: Boolean = true

    override fun lookupScope(key: CoordinatorKey): ApiRequestScope {
        return if (batch) BATCH_REQUEST_SCOPE
        else {
            // If the `FindCoordinator` API does not support batched lookups, we use a
            // separate lookup context for each coordinator key we need to lookup
            LookupRequestScope(key)
        }
    }

    override fun buildRequest(keys: Set<CoordinatorKey>): FindCoordinatorRequest.Builder {
        unrepresentableKeys = keys.filter { !isRepresentableKey(it.idValue) }
        val representableKeys = keys.filter { isRepresentableKey(it.idValue) }

        return if (batch) {
            ensureSameType(representableKeys)
            val data: FindCoordinatorRequestData = FindCoordinatorRequestData()
                .setKeyType(type.id)
                .setCoordinatorKeys(representableKeys.map { it.idValue })
            FindCoordinatorRequest.Builder(data)
        } else {
            val key: CoordinatorKey = requireSingletonAndType(representableKeys)
            FindCoordinatorRequest.Builder(
                FindCoordinatorRequestData()
                    .setKey(key.idValue)
                    .setKeyType(key.type.id)
            )
        }
    }

    override fun handleResponse(
        keys: Set<CoordinatorKey>,
        response: AbstractResponse
    ): AdminApiLookupStrategy.LookupResult<CoordinatorKey> {
        val mappedKeys: MutableMap<CoordinatorKey, Int> = HashMap()
        val failedKeys: MutableMap<CoordinatorKey, Throwable> = HashMap()

        unrepresentableKeys.forEach { key ->
            failedKeys[key] = InvalidGroupIdException(
                "The given group id '${key.idValue}' cannot be represented in a request."
            )
        }

        (response as FindCoordinatorResponse).coordinators().forEach { coordinator ->
            val key =
                if (coordinator.key.isEmpty()) requireSingletonAndType(keys)  // old version without batching
                else if ((type == CoordinatorType.GROUP)) CoordinatorKey.byGroupId(coordinator.key)
                else CoordinatorKey.byTransactionalId(coordinator.key)

            handleError(
                error = Errors.forCode(coordinator.errorCode),
                key = key,
                nodeId = coordinator.nodeId,
                mappedKeys = mappedKeys,
                failedKeys = failedKeys
            )
        }
        return AdminApiLookupStrategy.LookupResult(
            failedKeys = failedKeys,
            mappedKeys = mappedKeys,
        )
    }

    fun disableBatch() {
        batch = false
    }

    @Deprecated(
        message = "Use property instead.",
        replaceWith = ReplaceWith("batch"),
    )
    fun batch(): Boolean = batch

    private fun requireSingletonAndType(keys: Collection<CoordinatorKey>): CoordinatorKey {
        require(keys.size == 1) { "Unexpected size of key set: expected 1, but got ${keys.size}" }

        val key: CoordinatorKey = keys.iterator().next()
        require(key.type == type) {
            "Unexpected key type: expected key to be of type $type, but got ${key.type}"
        }
        return key
    }

    private fun ensureSameType(keys: List<CoordinatorKey>) {
        require(keys.isNotEmpty()) { "Unexpected size of key set: expected >= 1, but got ${keys.size}" }

        require(keys.filter { k: CoordinatorKey -> k.type == type }.size == keys.size) {
            "Unexpected key set: expected all key to be of type $type, but some key were not"
        }
    }

    private fun handleError(
        error: Errors,
        key: CoordinatorKey,
        nodeId: Int,
        mappedKeys: MutableMap<CoordinatorKey, Int>,
        failedKeys: MutableMap<CoordinatorKey, Throwable>
    ) {
        when (error) {
            Errors.NONE -> mappedKeys[key] = nodeId
            Errors.COORDINATOR_NOT_AVAILABLE, Errors.COORDINATOR_LOAD_IN_PROGRESS -> log.debug(
                "FindCoordinator request for key {} returned topic-level error {}. Will retry",
                key, error
            )

            Errors.GROUP_AUTHORIZATION_FAILED -> failedKeys[key] =
                GroupAuthorizationException(
                    ("FindCoordinator request for groupId " +
                            "`" + key + "` failed due to authorization failure"), key.idValue
                )

            Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED -> failedKeys.put(
                key, TransactionalIdAuthorizationException(
                    ("FindCoordinator request for " +
                            "transactionalId `" + key + "` failed due to authorization failure")
                )
            )

            else -> failedKeys.put(
                key, error.exception(
                    ("FindCoordinator request for key " +
                            "`" + key + "` failed due to an unexpected error")
                )
            )
        }
    }

    @JvmInline
    private value class LookupRequestScope(val key: CoordinatorKey) : ApiRequestScope

    companion object {
        private val BATCH_REQUEST_SCOPE: ApiRequestScope = object : ApiRequestScope {}
        private fun isRepresentableKey(groupId: String?): Boolean {
            return groupId != null
        }
    }
}
