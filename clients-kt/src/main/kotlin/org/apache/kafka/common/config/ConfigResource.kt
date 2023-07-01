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

package org.apache.kafka.common.config

/**
 * A class representing resources that have configs.
 *
 * @constructor Create an instance of this class with the provided parameters.
 * @property type resource type
 * @property name resource name
 */
data class ConfigResource(
    val type: Type,
    val name: String
) {

    /**
     * Return the resource type.
     */
    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("type")
    )
    fun type(): Type = type

    /**
     * Return the resource name.
     */
    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("name")
    )
    fun name(): String = name

    /**
     * Whether this is the default resource of a resource type. Resource name is empty for the
     * default resource.
     */
    val isDefault: Boolean
        get() = name.isEmpty()

    override fun toString(): String = "ConfigResource(type=$type, name='$name')"

    /**
     * Type of resource.
     */
    enum class Type(val id: Byte) {

        BROKER_LOGGER(8.toByte()),

        BROKER(4.toByte()),

        TOPIC(2.toByte()),

        UNKNOWN(0.toByte());

        @Deprecated(
            message = "Use property instead",
            replaceWith = ReplaceWith("id")
        )
        fun id(): Byte = id

        companion object {

            private val TYPES = values().associateBy { it.id }

            fun forId(id: Byte): Type = TYPES.getOrDefault(id, UNKNOWN)
        }
    }
}
