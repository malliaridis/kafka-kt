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

import org.apache.kafka.clients.admin.AlterConfigOp
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.message.IncrementalAlterConfigsRequestData
import org.apache.kafka.common.message.IncrementalAlterConfigsResponseData
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.ByteBufferAccessor
import java.nio.ByteBuffer

class IncrementalAlterConfigsRequest(
    private val data: IncrementalAlterConfigsRequestData,
    version: Short,
) : AbstractRequest(
    ApiKeys.INCREMENTAL_ALTER_CONFIGS,
    version
) {

    override fun data(): IncrementalAlterConfigsRequestData = data

    override fun getErrorResponse(throttleTimeMs: Int, e: Throwable): AbstractResponse? {
        val (error, message) = ApiError.fromThrowable(e)

        val response = IncrementalAlterConfigsResponseData()
            .setResponses(
                data.resources().map { resource ->
                    IncrementalAlterConfigsResponseData.AlterConfigsResourceResponse()
                        .setResourceName(resource.resourceName())
                        .setResourceType(resource.resourceType())
                        .setErrorCode(error.code)
                        .setErrorMessage(message)
                }
            )

        return IncrementalAlterConfigsResponse(response)
    }

    class Builder : AbstractRequest.Builder<IncrementalAlterConfigsRequest> {

        private val data: IncrementalAlterConfigsRequestData

        constructor(
            data: IncrementalAlterConfigsRequestData,
        ) : super(ApiKeys.INCREMENTAL_ALTER_CONFIGS) {
            this.data = data
        }

        constructor(
            resources: Collection<ConfigResource>,
            configs: Map<ConfigResource, Collection<AlterConfigOp>>,
            validateOnly: Boolean,
        ) : super(ApiKeys.INCREMENTAL_ALTER_CONFIGS) {
            data = IncrementalAlterConfigsRequestData().setValidateOnly(validateOnly)

            for (resource in resources) {
                val alterableConfigSet =
                    IncrementalAlterConfigsRequestData.AlterableConfigCollection()

                alterableConfigSet.addAll(
                        configs[resource]!!.map { configEntry ->
                            IncrementalAlterConfigsRequestData.AlterableConfig()
                                .setName(configEntry.configEntry().name)
                                .setValue(configEntry.configEntry().value)
                                .setConfigOperation(configEntry.opType().id())
                        }
                    )

                val alterConfigsResource = IncrementalAlterConfigsRequestData.AlterConfigsResource()
                alterConfigsResource.setResourceType(resource.type.id)
                    .setResourceName(resource.name)
                    .setConfigs(alterableConfigSet)

                data.resources().add(alterConfigsResource)
            }
        }

        constructor(
            configs: Map<ConfigResource, Collection<AlterConfigOp>>,
            validateOnly: Boolean,
        ) : this(configs.keys, configs, validateOnly)

        override fun build(version: Short): IncrementalAlterConfigsRequest =
            IncrementalAlterConfigsRequest(data, version)

        override fun toString(): String = data.toString()
    }

    companion object {

        fun parse(buffer: ByteBuffer, version: Short): IncrementalAlterConfigsRequest =
            IncrementalAlterConfigsRequest(
                IncrementalAlterConfigsRequestData(ByteBufferAccessor(buffer), version),
                version,
            )
    }
}
