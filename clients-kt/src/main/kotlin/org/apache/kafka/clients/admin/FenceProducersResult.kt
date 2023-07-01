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

import org.apache.kafka.clients.admin.internals.CoordinatorKey
import org.apache.kafka.common.KafkaFuture
import org.apache.kafka.common.KafkaFuture.BaseFunction
import org.apache.kafka.common.annotation.InterfaceStability.Evolving
import org.apache.kafka.common.utils.ProducerIdAndEpoch

/**
 * The result of the [Admin.fenceProducers] call.
 *
 * The API of this class is evolving, see [Admin] for details.
 */
@Evolving
class FenceProducersResult internal constructor(
    private val futures: Map<CoordinatorKey, KafkaFuture<ProducerIdAndEpoch>>,
) {

    /**
     * Return a map from transactional ID to futures which can be used to check the status of
     * individual fencings.
     */
    fun fencedProducers(): Map<String, KafkaFuture<Void?>> =
        futures.entries.associate {(key, value) ->
            key.idValue to value.thenApply { null }
        }

    /**
     * Returns a future that provides the producer ID generated while initializing the given
     * transaction when the request completes.
     */
    fun producerId(transactionalId: String): KafkaFuture<Long> =
        findAndApply(transactionalId) { (producerId): ProducerIdAndEpoch -> producerId }

    /**
     * Returns a future that provides the epoch ID generated while initializing the given
     * transaction when the request completes.
     */
    fun epochId(transactionalId: String): KafkaFuture<Short> =
        findAndApply(transactionalId) { (_, epoch): ProducerIdAndEpoch -> epoch }

    /**
     * Return a future which succeeds only if all the producer fencings succeed.
     */
    fun all(): KafkaFuture<Unit> = KafkaFuture.allOf(futures.values)

    private fun <T> findAndApply(
        transactionalId: String,
        followup: BaseFunction<ProducerIdAndEpoch, T>,
    ): KafkaFuture<T> {
        val key = CoordinatorKey.byTransactionalId(transactionalId)
        val future = requireNotNull(futures[key]) {
            "TransactionalId `$transactionalId` was not included in the request"
        }
        return future.thenApply(followup)
    }
}
