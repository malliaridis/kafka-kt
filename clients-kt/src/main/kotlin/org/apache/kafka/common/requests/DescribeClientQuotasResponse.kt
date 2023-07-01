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

import org.apache.kafka.common.internals.KafkaFutureImpl
import org.apache.kafka.common.message.DescribeClientQuotasResponseData
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.ByteBufferAccessor
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.quota.ClientQuotaEntity
import java.nio.ByteBuffer

class DescribeClientQuotasResponse(
    private val data: DescribeClientQuotasResponseData,
) : AbstractResponse(ApiKeys.DESCRIBE_CLIENT_QUOTAS) {

    fun complete(future: KafkaFutureImpl<Map<ClientQuotaEntity, Map<String, Double>>>) {
        val error = Errors.forCode(data.errorCode())

        if (error !== Errors.NONE) {
            future.completeExceptionally(error.exception(data.errorMessage()))
            return
        }

        val result: Map<ClientQuotaEntity, Map<String, Double>> =
            data.entries().associate { entry ->
                val entityMap = entry.entity().associateBy(
                    keySelector = DescribeClientQuotasResponseData.EntityData::entityType,
                    valueTransform = DescribeClientQuotasResponseData.EntityData::entityName,
                )

                val values = entry.values().associateBy(
                    keySelector = DescribeClientQuotasResponseData.ValueData::key,
                    valueTransform = DescribeClientQuotasResponseData.ValueData::value,
                )

                ClientQuotaEntity(entityMap) to values
            }

        future.complete(result)
    }

    override fun throttleTimeMs(): Int = data.throttleTimeMs()

    override fun maybeSetThrottleTimeMs(throttleTimeMs: Int) {
        data.setThrottleTimeMs(throttleTimeMs)
    }

    override fun data(): DescribeClientQuotasResponseData = data

    override fun errorCounts(): Map<Errors, Int> = errorCounts(Errors.forCode(data.errorCode()))

    companion object {

        fun parse(buffer: ByteBuffer, version: Short): DescribeClientQuotasResponse =
            DescribeClientQuotasResponse(
                DescribeClientQuotasResponseData(ByteBufferAccessor(buffer), version)
            )

        fun fromQuotaEntities(
            entities: Map<ClientQuotaEntity, Map<String, Double>>,
            throttleTimeMs: Int
        ): DescribeClientQuotasResponse {
            val entries: List<DescribeClientQuotasResponseData.EntryData> =
                entities.map { (quotaEntity, quotaValues) ->

                    val entityData = quotaEntity.entries.map { (key, value) ->
                        DescribeClientQuotasResponseData.EntityData()
                            .setEntityType(key)
                            .setEntityName(value)
                    }

                    val valueData = quotaValues.map { (key, value) ->
                        DescribeClientQuotasResponseData.ValueData()
                            .setKey(key)
                            .setValue(value)
                    }

                    DescribeClientQuotasResponseData.EntryData()
                        .setEntity(entityData)
                        .setValues(valueData)
                }

            return DescribeClientQuotasResponse(
                DescribeClientQuotasResponseData()
                    .setThrottleTimeMs(throttleTimeMs)
                    .setErrorCode(0.toShort())
                    .setErrorMessage(null)
                    .setEntries(entries)
            )
        }
    }
}
