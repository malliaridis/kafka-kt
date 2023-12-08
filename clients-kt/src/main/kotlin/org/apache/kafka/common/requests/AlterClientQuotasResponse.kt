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

package org.apache.kafka.common.requests

import java.nio.ByteBuffer
import java.util.function.Consumer
import org.apache.kafka.common.internals.KafkaFutureImpl
import org.apache.kafka.common.message.AlterClientQuotasResponseData
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.ByteBufferAccessor
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.quota.ClientQuotaEntity

class AlterClientQuotasResponse(
    private val data: AlterClientQuotasResponseData,
) : AbstractResponse(ApiKeys.ALTER_CLIENT_QUOTAS) {

    fun complete(futures: Map<ClientQuotaEntity, KafkaFutureImpl<Unit>>) {

        data.entries.forEach { entryData ->
            val entityEntries: MutableMap<String, String?> = HashMap(entryData.entity.size)

            entryData.entity.forEach { entityData ->
                entityEntries[entityData.entityType] = entityData.entityName
            }

            val entity = ClientQuotaEntity(entityEntries)
            val future =
                requireNotNull(futures[entity]) { "Future map must contain entity $entity" }
            val error = Errors.forCode(entryData.errorCode)

            if (error === Errors.NONE) future.complete(Unit)
            else future.completeExceptionally(error.exception(entryData.errorMessage)!!)
        }
    }

    override fun throttleTimeMs(): Int = data.throttleTimeMs

    override fun maybeSetThrottleTimeMs(throttleTimeMs: Int) {
        data.setThrottleTimeMs(throttleTimeMs)
    }

    override fun errorCounts(): Map<Errors, Int> {
        val counts = mutableMapOf<Errors, Int>()

        data.entries.forEach { entry ->
            updateErrorCounts(
                errorCounts = counts,
                error = Errors.forCode(entry.errorCode)
            )
        }

        return counts
    }

    override fun data(): AlterClientQuotasResponseData = data

    companion object {

        private fun toEntityData(
            entity: ClientQuotaEntity,
        ): List<AlterClientQuotasResponseData.EntityData> {
            return entity.entries.map { (key, value) ->
                AlterClientQuotasResponseData.EntityData()
                    .setEntityType(key)
                    .setEntityName(value)
            }
        }

        fun parse(buffer: ByteBuffer?, version: Short): AlterClientQuotasResponse {
            return AlterClientQuotasResponse(
                AlterClientQuotasResponseData(
                    ByteBufferAccessor(buffer!!),
                    version
                )
            )
        }

        fun fromQuotaEntities(
            result: Map<ClientQuotaEntity, ApiError>,
            throttleTimeMs: Int
        ): AlterClientQuotasResponse {
            val entries: MutableList<AlterClientQuotasResponseData.EntryData> =
                ArrayList(result.size)

            result.forEach { (key, cause) ->
                entries.add(
                    AlterClientQuotasResponseData.EntryData()
                        .setErrorCode(cause.error.code)
                        .setErrorMessage(cause.message)
                        .setEntity(toEntityData(key))
                )
            }

            return AlterClientQuotasResponse(
                AlterClientQuotasResponseData()
                    .setThrottleTimeMs(throttleTimeMs)
                    .setEntries(entries)
            )
        }
    }
}
