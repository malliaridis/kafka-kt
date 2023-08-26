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
import org.apache.kafka.common.message.AlterConfigsRequestData
import org.apache.kafka.common.message.AlterConfigsResponseData
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.ByteBufferAccessor
import java.nio.ByteBuffer
import java.util.*
import java.util.stream.Collectors

class AlterConfigsRequest(
    private val data: AlterConfigsRequestData,
    version: Short,
) : AbstractRequest(ApiKeys.ALTER_CONFIGS, version) {

    fun configs(): Map<ConfigResource, Config> = data.resources.associate { resource ->
        ConfigResource(
            ConfigResource.Type.forId(resource.resourceType),
            resource.resourceName
        ) to Config(
            resource.configs.map { entry -> ConfigEntry(entry.name, entry.value) }
        )
    }

    fun validateOnly(): Boolean = data.validateOnly

    override fun data(): AlterConfigsRequestData = data

    override fun getErrorResponse(throttleTimeMs: Int, e: Throwable): AbstractResponse {
        val (error1, message) = ApiError.fromThrowable(e)
        val data = AlterConfigsResponseData().setThrottleTimeMs(throttleTimeMs)

        data.responses += this.data.resources.map { resource ->
            AlterConfigsResponseData.AlterConfigsResourceResponse()
                .setResourceType(resource.resourceType)
                .setResourceName(resource.resourceName)
                .setErrorMessage(message)
                .setErrorCode(error1.code)
        }

        return AlterConfigsResponse(data)
    }

    class Config(val entries: Collection<ConfigEntry>) {

        @Deprecated(
            message = "User property instead",
            replaceWith = ReplaceWith("entries"),
        )
        fun entries(): Collection<ConfigEntry> = entries
    }

    class ConfigEntry(val name: String, val value: String?) {

        @Deprecated(
            message = "User property instead",
            replaceWith = ReplaceWith("name"),
        )
        fun name(): String = name

        @Deprecated(
            message = "User property instead",
            replaceWith = ReplaceWith("value"),
        )
        fun value(): String? = value
    }

    class Builder(
        configs: Map<ConfigResource, Config>,
        validateOnly: Boolean,
    ) : AbstractRequest.Builder<AlterConfigsRequest>(ApiKeys.ALTER_CONFIGS) {

        private val data = AlterConfigsRequestData()

        init {
            for ((key, value) in configs) {
                val resource = AlterConfigsRequestData.AlterConfigsResource()
                    .setResourceName(key.name)
                    .setResourceType(key.type.id)

                for (x in value.entries) {
                    resource.configs.add(
                        AlterConfigsRequestData.AlterableConfig()
                            .setName(x.name)
                            .setValue(x.value)
                    )
                }
                data.resources.add(resource)
            }
            data.setValidateOnly(validateOnly)
        }

        override fun build(version: Short): AlterConfigsRequest = AlterConfigsRequest(data, version)
    }

    companion object {

        fun parse(buffer: ByteBuffer, version: Short): AlterConfigsRequest =
            AlterConfigsRequest(
                AlterConfigsRequestData(ByteBufferAccessor(buffer), version),
                version,
            )
    }
}
