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
import org.apache.kafka.common.message.DescribeConfigsResponseData
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.ByteBufferAccessor
import org.apache.kafka.common.protocol.Errors
import java.nio.ByteBuffer
import org.apache.kafka.clients.admin.ConfigEntry.ConfigSource as AdminConfigSource
import org.apache.kafka.clients.admin.ConfigEntry.ConfigType as AdminConfigType

class DescribeConfigsResponse(
    private val data: DescribeConfigsResponseData,
) : AbstractResponse(ApiKeys.DESCRIBE_CONFIGS) {

    // This constructor should only be used after deserialization, it has special handling for
    // version 0
    private constructor(
        data: DescribeConfigsResponseData,
        version: Short
    ) : this(data = data) {
        if (version.toInt() == 0) {
            for (result in data.results) {
                for (config in result.configs) {
                    if (config.isDefault) config.setConfigSource(ConfigSource.DEFAULT_CONFIG.id)
                    else if (result.resourceType == ConfigResource.Type.BROKER.id)
                        config.setConfigSource(ConfigSource.STATIC_BROKER_CONFIG.id)
                    else if (result.resourceType == ConfigResource.Type.TOPIC.id)
                        config.setConfigSource(ConfigSource.TOPIC_CONFIG.id)
                    else config.setConfigSource(ConfigSource.UNKNOWN.id)
                }
            }
        }
    }

    fun resultMap(): Map<ConfigResource, DescribeConfigsResponseData.DescribeConfigsResult> =
        data().results.associateBy { configsResult ->
            ConfigResource(
                ConfigResource.Type.forId(configsResult.resourceType),
                configsResult.resourceName
            )
        }

    override fun data(): DescribeConfigsResponseData = data

    override fun throttleTimeMs(): Int = data.throttleTimeMs

    override fun maybeSetThrottleTimeMs(throttleTimeMs: Int) {
        data.setThrottleTimeMs(throttleTimeMs)
    }

    override fun errorCounts(): Map<Errors, Int> {
        val errorCounts = mutableMapOf<Errors, Int>()

        data.results.forEach { response ->
            updateErrorCounts(errorCounts, Errors.forCode(response.errorCode))
        }

        return errorCounts
    }

    override fun shouldClientThrottle(version: Short): Boolean = version >= 2

    data class Config(
        val error: ApiError,
        val entries: Collection<ConfigEntry>,
    ) {

        @Deprecated(
            message = "User property instead",
            replaceWith = ReplaceWith("error"),
        )
        fun error(): ApiError = error

        @Deprecated(
            message = "User property instead",
            replaceWith = ReplaceWith("entries"),
        )
        fun entries(): Collection<ConfigEntry> = entries
    }

    data class ConfigEntry(
        val name: String,
        val value: String,
        val source: ConfigSource,
        val isSensitive: Boolean,
        val isReadOnly: Boolean,
        val synonyms: Collection<ConfigSynonym>,
        val type: ConfigType = ConfigType.UNKNOWN,
        val documentation: String? = null
    ) {

        @Deprecated(
            message = "User property instead",
            replaceWith = ReplaceWith("name"),
        )
        fun name(): String = name

        @Deprecated(
            message = "User property instead",
            replaceWith = ReplaceWith("value"),
        )
        fun value(): String = value

        @Deprecated(
            message = "User property instead",
            replaceWith = ReplaceWith("source"),
        )
        fun source(): ConfigSource = source

        @Deprecated(
            message = "User property instead",
            replaceWith = ReplaceWith("synonyms"),
        )
        fun synonyms(): Collection<ConfigSynonym> = synonyms

        @Deprecated(
            message = "User property instead",
            replaceWith = ReplaceWith("type"),
        )
        fun type(): ConfigType = type

        @Deprecated(
            message = "User property instead",
            replaceWith = ReplaceWith("documentation"),
        )
        fun documentation(): String? = documentation
    }

    enum class ConfigSource(
        val id: Byte,
        val source: AdminConfigSource,
    ) {
        UNKNOWN(
            id = 0.toByte(),
            source = AdminConfigSource.UNKNOWN,
        ),
        TOPIC_CONFIG(
            id = 1.toByte(),
            source = AdminConfigSource.DYNAMIC_TOPIC_CONFIG,
        ),
        DYNAMIC_BROKER_CONFIG(
            id = 2.toByte(),
            source = AdminConfigSource.DYNAMIC_BROKER_CONFIG,
        ),
        DYNAMIC_DEFAULT_BROKER_CONFIG(
            id = 3.toByte(),
            source = AdminConfigSource.DYNAMIC_DEFAULT_BROKER_CONFIG,
        ),
        STATIC_BROKER_CONFIG(
            id = 4.toByte(),
            source = AdminConfigSource.STATIC_BROKER_CONFIG,
        ),
        DEFAULT_CONFIG(
            id = 5.toByte(),
            source = AdminConfigSource.DEFAULT_CONFIG,
        ),
        DYNAMIC_BROKER_LOGGER_CONFIG(
            id = 6.toByte(),
            source = AdminConfigSource.DYNAMIC_BROKER_LOGGER_CONFIG,
        );

        @Deprecated(
            message = "User property instead",
            replaceWith = ReplaceWith("id"),
        )
        fun id(): Byte = id

        @Deprecated(
            message = "User property instead",
            replaceWith = ReplaceWith("source"),
        )
        fun source(): AdminConfigSource = source

        companion object {

            fun forId(id: Byte): ConfigSource {
                require(id >= 0) { "id should be positive, id: $id" }

                return if (id >= values().size) UNKNOWN
                else values()[id.toInt()]
            }
        }
    }

    enum class ConfigType(
        val id: Byte,
        val type: AdminConfigType,
    ) {
        UNKNOWN(0.toByte(), AdminConfigType.UNKNOWN),
        BOOLEAN(1.toByte(), AdminConfigType.BOOLEAN),
        STRING(2.toByte(), AdminConfigType.STRING),
        INT(3.toByte(), AdminConfigType.INT),
        SHORT(4.toByte(), AdminConfigType.SHORT),
        LONG(5.toByte(), AdminConfigType.LONG),
        DOUBLE(6.toByte(), AdminConfigType.DOUBLE),
        LIST(7.toByte(), AdminConfigType.LIST),
        CLASS(8.toByte(), AdminConfigType.CLASS),
        PASSWORD(9.toByte(), AdminConfigType.PASSWORD);

        @Deprecated(
            message = "User property instead",
            replaceWith = ReplaceWith("id"),
        )
        fun id(): Byte = id

        @Deprecated(
            message = "User property instead",
            replaceWith = ReplaceWith("type"),
        )
        fun type(): AdminConfigType = type

        companion object {

            fun forId(id: Byte): ConfigType {
                require(id >= 0) { "id should be positive, id: $id" }

                return if (id >= values().size) UNKNOWN
                else values()[id.toInt()]
            }
        }
    }

    class ConfigSynonym(
        val name: String,
        val value: String,
        val source: ConfigSource,
    ) {

        @Deprecated(
            message = "User property instead",
            replaceWith = ReplaceWith("name"),
        )
        fun name(): String = name

        @Deprecated(
            message = "User property instead",
            replaceWith = ReplaceWith("value"),
        )
        fun value(): String = value

        @Deprecated(
            message = "User property instead",
            replaceWith = ReplaceWith("source"),
        )
        fun source(): ConfigSource = source
    }

    companion object {

        fun parse(buffer: ByteBuffer, version: Short): DescribeConfigsResponse =
            DescribeConfigsResponse(
                DescribeConfigsResponseData(ByteBufferAccessor(buffer), version),
                version,
            )
    }
}
