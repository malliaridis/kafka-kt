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

import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.message.IncrementalAlterConfigsResponseData
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.ByteBufferAccessor
import org.apache.kafka.common.protocol.Errors
import java.nio.ByteBuffer
import java.util.function.Consumer

class IncrementalAlterConfigsResponse : AbstractResponse {

    private val data: IncrementalAlterConfigsResponseData

    constructor(
        data: IncrementalAlterConfigsResponseData,
    ) : super(ApiKeys.INCREMENTAL_ALTER_CONFIGS) {
        this.data = data
    }

    constructor(
        requestThrottleMs: Int,
        results: Map<ConfigResource, ApiError>,
    ) : super(ApiKeys.INCREMENTAL_ALTER_CONFIGS) {
        val newResults = results.map { (resource, error) ->
            IncrementalAlterConfigsResponseData.AlterConfigsResourceResponse()
                .setErrorCode(error.error.code)
                .setErrorMessage(error.message)
                .setResourceName(resource.name)
                .setResourceType(resource.type.id)
        }

        data = IncrementalAlterConfigsResponseData()
            .setResponses(newResults)
            .setThrottleTimeMs(requestThrottleMs)
    }


    override fun data(): IncrementalAlterConfigsResponseData = data

    override fun errorCounts(): Map<Errors, Int> {
        val counts = mutableMapOf<Errors, Int>()
        data.responses.forEach { response ->
            updateErrorCounts(counts, Errors.forCode(response.errorCode))
        }

        return counts
    }

    override fun shouldClientThrottle(version: Short): Boolean = version >= 0

    override fun throttleTimeMs(): Int = data.throttleTimeMs

    override fun maybeSetThrottleTimeMs(throttleTimeMs: Int) {
        data.setThrottleTimeMs(throttleTimeMs)
    }

    companion object {

        fun fromResponseData(
            data: IncrementalAlterConfigsResponseData,
        ): Map<ConfigResource, ApiError> = data.responses.associate { response ->
            ConfigResource(
                ConfigResource.Type.forId(response.resourceType),
                response.resourceName
            ) to ApiError(
                Errors.forCode(response.errorCode), response.errorMessage
            )
        }

        fun parse(buffer: ByteBuffer, version: Short): IncrementalAlterConfigsResponse =
            IncrementalAlterConfigsResponse(
                IncrementalAlterConfigsResponseData(ByteBufferAccessor(buffer), version)
            )
    }
}
