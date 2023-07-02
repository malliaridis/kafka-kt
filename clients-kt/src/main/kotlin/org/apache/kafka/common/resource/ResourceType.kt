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

package org.apache.kafka.common.resource

import org.apache.kafka.common.annotation.InterfaceStability.Evolving

/**
 * Represents a type of resource which an ACL can be applied to.
 *
 * The API for this class is still evolving and we may break compatibility in minor releases, if
 * necessary.
 */
@Evolving
enum class ResourceType(val code: Byte) {

    /**
     * Represents any ResourceType which this client cannot understand, perhaps because this client
     * is too old.
     */
    UNKNOWN(0.toByte()),

    /**
     * In a filter, matches any ResourceType.
     */
    ANY(1.toByte()),

    /**
     * A Kafka topic.
     */
    TOPIC(2.toByte()),

    /**
     * A consumer group.
     */
    GROUP(3.toByte()),

    /**
     * The cluster as a whole.
     */
    CLUSTER(4.toByte()),

    /**
     * A transactional ID.
     */
    TRANSACTIONAL_ID(5.toByte()),

    /**
     * A token ID.
     */
    DELEGATION_TOKEN(6.toByte()),

    /**
     * A user principal.
     */
    USER(7.toByte());

    /**
     * Return the code of this resource.
     */
    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("code"),
    )
    fun code(): Byte {
        return code
    }

    /**
     * Whether this resource type is UNKNOWN.
     */
    val isUnknown: Boolean
        get() = this == UNKNOWN

    companion object {

        private val CODE_TO_VALUE = values().associateBy { it.code }

        /**
         * Parse the given string as an ACL resource type.
         *
         * @param str The string to parse.
         * @return The ResourceType, or UNKNOWN if the string could not be matched.
         */
        @Throws(IllegalArgumentException::class)
        fun fromString(str: String): ResourceType {
            return try {
                valueOf(str.uppercase())
            } catch (e: IllegalArgumentException) {
                UNKNOWN
            }
        }

        /**
         * Return the ResourceType with the provided code or `ResourceType.UNKNOWN` if one cannot be
         * found.
         */
        fun fromCode(code: Byte): ResourceType = CODE_TO_VALUE[code] ?: UNKNOWN
    }
}
