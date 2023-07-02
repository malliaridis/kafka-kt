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

import org.apache.kafka.common.message.DescribeClientQuotasRequestData
import org.apache.kafka.common.message.DescribeClientQuotasResponseData
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.ByteBufferAccessor
import org.apache.kafka.common.quota.ClientQuotaFilter
import org.apache.kafka.common.quota.ClientQuotaFilterComponent
import java.nio.ByteBuffer

class DescribeClientQuotasRequest(
    private val data: DescribeClientQuotasRequestData,
    version: Short,
) : AbstractRequest(ApiKeys.DESCRIBE_CLIENT_QUOTAS, version) {

    fun filter(): ClientQuotaFilter {

        val components = data.components().map { data ->
            when (data.matchType()) {
                MATCH_TYPE_EXACT -> ClientQuotaFilterComponent(
                    entityType = data.entityType(),
                    match = data.match()
                )

                MATCH_TYPE_DEFAULT -> ClientQuotaFilterComponent(data.entityType())
                MATCH_TYPE_SPECIFIED -> ClientQuotaFilterComponent(data.entityType())
                else -> throw IllegalArgumentException("Unexpected match type: " + data.matchType())
            }
        }

        return if (data.strict()) ClientQuotaFilter.containsOnly(components)
        else ClientQuotaFilter.contains(components)
    }

    override fun data(): DescribeClientQuotasRequestData = data

    override fun getErrorResponse(
        throttleTimeMs: Int,
        e: Throwable
    ): DescribeClientQuotasResponse {
        val (error1, message) = ApiError.fromThrowable(e)
        return DescribeClientQuotasResponse(
            DescribeClientQuotasResponseData()
                .setThrottleTimeMs(throttleTimeMs)
                .setErrorCode(error1.code)
                .setErrorMessage(message)
                .setEntries(null)
        )
    }

    class Builder(
        filter: ClientQuotaFilter,
    ) : AbstractRequest.Builder<DescribeClientQuotasRequest>(ApiKeys.DESCRIBE_CLIENT_QUOTAS) {

        private val data: DescribeClientQuotasRequestData

        init {
            val componentData = filter.components().map { (entityType, match) ->
                DescribeClientQuotasRequestData.ComponentData()
                    .setEntityType(entityType)
                    .setMatchType(if (match == null) MATCH_TYPE_DEFAULT else MATCH_TYPE_EXACT)
                    .setMatch(match)
            }

            data = DescribeClientQuotasRequestData()
                .setComponents(componentData)
                .setStrict(filter.strict())
        }

        override fun build(version: Short): DescribeClientQuotasRequest =
            DescribeClientQuotasRequest(data, version)

        override fun toString(): String = data.toString()
    }

    companion object {

        // These values must not change.
        const val MATCH_TYPE_EXACT: Byte = 0

        const val MATCH_TYPE_DEFAULT: Byte = 1

        const val MATCH_TYPE_SPECIFIED: Byte = 2

        fun parse(buffer: ByteBuffer, version: Short): DescribeClientQuotasRequest =
            DescribeClientQuotasRequest(
                DescribeClientQuotasRequestData(ByteBufferAccessor(buffer), version),
                version,
            )
    }
}
