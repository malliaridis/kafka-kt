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

import org.apache.kafka.common.message.AlterClientQuotasRequestData
import org.apache.kafka.common.message.AlterClientQuotasRequestData.OpData
import org.apache.kafka.common.message.AlterClientQuotasResponseData
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.ByteBufferAccessor
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.quota.ClientQuotaAlteration
import org.apache.kafka.common.quota.ClientQuotaEntity
import java.nio.ByteBuffer

class AlterClientQuotasRequest(
    private val data: AlterClientQuotasRequestData,
    version: Short,
) : AbstractRequest(ApiKeys.ALTER_CLIENT_QUOTAS, version) {

    fun entries(): List<ClientQuotaAlteration> {
        val entries = data.entries.map { entryData ->
            val entity = entryData.entity.associate { entityData ->
                entityData.entityType to entityData.entityName
            }

            val ops = entryData.ops.map { opData ->
                val value = if (opData.remove) null else opData.value
                ClientQuotaAlteration.Op(opData.key, value)
            }

            ClientQuotaAlteration(ClientQuotaEntity(entity), ops)
        }
        return entries
    }

    fun validateOnly(): Boolean = data.validateOnly

    override fun data(): AlterClientQuotasRequestData = data

    override fun getErrorResponse(throttleTimeMs: Int, e: Throwable): AlterClientQuotasResponse {
        val error = Errors.forException(e)
        val responseEntries = data.entries.map { entryData ->

            val responseEntities = entryData.entity.map { entityData ->

                AlterClientQuotasResponseData.EntityData()
                    .setEntityType(entityData.entityType)
                    .setEntityName(entityData.entityName)
            }

            AlterClientQuotasResponseData.EntryData()
                .setEntity(responseEntities)
                .setErrorCode(error.code)
                .setErrorMessage(error.message)
        }

        val responseData = AlterClientQuotasResponseData()
            .setThrottleTimeMs(throttleTimeMs)
            .setEntries(responseEntries)

        return AlterClientQuotasResponse(responseData)
    }

    class Builder(
        entries: Collection<ClientQuotaAlteration>,
        validateOnly: Boolean,
    ) : AbstractRequest.Builder<AlterClientQuotasRequest>(ApiKeys.ALTER_CLIENT_QUOTAS) {

        private val data: AlterClientQuotasRequestData

        init {
            val entryData = entries.map { entry ->
                val entityData = entry.entity.entries.map { (key, value) ->
                    AlterClientQuotasRequestData.EntityData()
                        .setEntityType(key)
                        .setEntityName(value)
                }

                val opData = entry.ops.map { op ->
                    OpData()
                        .setKey(op.key)
                        .setValue(op.value ?: 0.0)
                        .setRemove(op.value == null)
                }
                AlterClientQuotasRequestData.EntryData()
                    .setEntity(entityData)
                    .setOps(opData)
            }

            data = AlterClientQuotasRequestData()
                .setEntries(entryData)
                .setValidateOnly(validateOnly)
        }

        override fun build(version: Short): AlterClientQuotasRequest =
            AlterClientQuotasRequest(data, version)

        override fun toString(): String = data.toString()
    }

    companion object {

        fun parse(buffer: ByteBuffer, version: Short): AlterClientQuotasRequest =
            AlterClientQuotasRequest(
                AlterClientQuotasRequestData(ByteBufferAccessor(buffer), version),
                version,
            )
    }
}
