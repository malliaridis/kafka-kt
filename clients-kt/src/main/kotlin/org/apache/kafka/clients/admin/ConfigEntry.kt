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

import org.apache.kafka.common.annotation.InterfaceStability.Evolving

/**
 * A class representing a configuration entry containing name, value and additional metadata.
 *
 * The API of this class is evolving, see [Admin] for details.
 *
 * @property name the non-null config name
 * @property value the config value or null
 * @property source the source of this config entry
 * @property isSensitive whether the config value is sensitive, the broker never returns the value
 * if it is sensitive
 * @property isReadOnly whether the config is read-only and cannot be updated
 * @property synonyms Synonym configs in order of precedence
 */
@Evolving
data class ConfigEntry(
    val name: String,
    val value: String?,
    val source: ConfigSource = ConfigSource.UNKNOWN,
    val isSensitive: Boolean? = false,
    val isReadOnly: Boolean = false,
    val synonyms: List<ConfigSynonym> = emptyList(),
    val type: ConfigType = ConfigType.UNKNOWN,
    val documentation: String? = null
) {

    /**
     * Return the config name.
     */
    @Deprecated(
        message = "Use property instead.",
        replaceWith = ReplaceWith("name"),
    )
    fun name(): String = name

    /**
     * Return the value or null. Null is returned if the config is unset or if isSensitive is true.
     */
    @Deprecated(
        message = "Use property instead.",
        replaceWith = ReplaceWith("value"),
    )
    fun value(): String? = value

    /**
     * Return the source of this configuration entry.
     */
    @Deprecated(
        message = "Use property instead.",
        replaceWith = ReplaceWith("source"),
    )
    fun source(): ConfigSource = source

    /**
     * Return whether the config value is the default or if it's been explicitly set.
     */
    val isDefault: Boolean
        get() = source == ConfigSource.DEFAULT_CONFIG

    /**
     * Returns all config values that may be used as the value of this config along with their source,
     * in the order of precedence. The list starts with the value returned in this ConfigEntry.
     * The list is empty if synonyms were not requested using [DescribeConfigsOptions.includeSynonyms]
     */
    @Deprecated(
        message = "Use property instead.",
        replaceWith = ReplaceWith("synonyms"),
    )
    fun synonyms(): List<ConfigSynonym> = synonyms

    /**
     * Return the config data type.
     */
    @Deprecated(
        message = "Use property instead.",
        replaceWith = ReplaceWith("type"),
    )
    fun type(): ConfigType = type

    /**
     * Return the config documentation.
     */
    @Deprecated(
        message = "Use property instead.",
        replaceWith = ReplaceWith("documentation"),
    )
    fun documentation(): String? = documentation

    /**
     * Override toString to redact sensitive value.
     * WARNING, user should be responsible to set the correct "isSensitive" field for each config entry.
     */
    override fun toString(): String {
        return "ConfigEntry(" +
                "name=$name" +
                ", value=" + (if (isSensitive != false) "Redacted" else value) +
                ", source=$source" +
                ", isSensitive=$isSensitive" +
                ", isReadOnly=$isReadOnly" +
                ", synonyms=$synonyms" +
                ", type=$type" +
                ", documentation=$documentation" +
                ")"
    }

    /**
     * Data type of configuration entry.
     */
    enum class ConfigType {
        UNKNOWN,
        BOOLEAN,
        STRING,
        INT,
        SHORT,
        LONG,
        DOUBLE,
        LIST,
        CLASS,
        PASSWORD
    }

    /**
     * Source of configuration entries.
     */
    enum class ConfigSource {
        DYNAMIC_TOPIC_CONFIG,

        // dynamic topic config that is configured for a specific topic
        DYNAMIC_BROKER_LOGGER_CONFIG,

        // dynamic broker logger config that is configured for a specific broker
        DYNAMIC_BROKER_CONFIG,

        // dynamic broker config that is configured for a specific broker
        DYNAMIC_DEFAULT_BROKER_CONFIG,

        // dynamic broker config that is configured as default for all brokers in the cluster
        STATIC_BROKER_CONFIG,

        // static broker config provided as broker properties at start up (e.g. server.properties file)
        DEFAULT_CONFIG,

        // built-in default configuration for configs that have a default value
        UNKNOWN // source unknown e.g. in the ConfigEntry used for alter requests where source is not set
    }

    /**
     * Class representing a configuration synonym of a [ConfigEntry].
     *
     * @property name Configuration name (this may be different from the name of the associated
     * [ConfigEntry]).
     * @property value Configuration value.
     * @property source [ConfigSource] of this configuration.
     */
    data class ConfigSynonym internal constructor(
        val name: String,
        val value: String?,
        val source: ConfigSource,
    ) {
        /**
         * Returns the name of this configuration.
         */
        @Deprecated(
            message = "Use property instead.",
            replaceWith = ReplaceWith("name"),
        )
        fun name(): String = name

        /**
         * Returns the value of this configuration, which may be null if the configuration is sensitive.
         */
        @Deprecated(
            message = "Use property instead.",
            replaceWith = ReplaceWith("value"),
        )
        fun value(): String? = value

        /**
         * Returns the source of this configuration.
         */
        @Deprecated(
            message = "Use property instead.",
            replaceWith = ReplaceWith("source"),
        )
        fun source(): ConfigSource {
            return source
        }

        override fun toString(): String {
            return "ConfigSynonym(" +
                    "name=$name" +
                    ", value=$value" +
                    ", source=$source" +
                    ")"
        }
    }
}
