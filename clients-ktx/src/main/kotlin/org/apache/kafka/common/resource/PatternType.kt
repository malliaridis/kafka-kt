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
 * Resource pattern type.
 */
@Evolving
enum class PatternType(val code: Byte) {
    
    /**
     * Represents any PatternType which this client cannot understand, perhaps because this client
     * is too old.
     */
    UNKNOWN(0.toByte()),

    /**
     * In a filter, matches any resource pattern type.
     */
    ANY(1.toByte()),

    /**
     * In a filter, will perform pattern matching.
     *
     * e.g. Given a filter of `ResourcePatternFilter(TOPIC, "payments.received", MATCH)``, the
     * filter match any [ResourcePattern] that matches topic 'payments.received'. This might
     * include:
     *
     * - A Literal pattern with the same type and name, e.g. `ResourcePattern(TOPIC,
     *   "payments.received", LITERAL)`
     * - A Wildcard pattern with the same type, e.g. `ResourcePattern(TOPIC, "*", LITERAL)`
     * - A Prefixed pattern with the same type and where the name is a matching prefix, e.g.
     *   `ResourcePattern(TOPIC, "payments.", PREFIXED)`
     *
     */
    MATCH(2.toByte()),

    /**
     * A literal resource name.
     *
     * A literal name defines the full name of a resource, e.g. topic with name 'foo', or group with
     * name 'bob'.
     *
     * The special wildcard character `*` can be used to represent a resource with any name.
     */
    LITERAL(3.toByte()),

    /**
     * A prefixed resource name.
     *
     * A prefixed name defines a prefix for a resource, e.g. topics with names that start with 'foo'.
     */
    PREFIXED(4.toByte());

    /**
     * @return the code of this resource.
     */
    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("code")
    )
    fun code(): Byte = code

    /**
     * Whether this resource pattern type is UNKNOWN.
     */
    val isUnknown: Boolean
        get() = this == UNKNOWN

    /**
     * Whether this resource pattern type is a concrete type, rather than UNKNOWN or one of the
     * filter types.
     */
    val isSpecific: Boolean
        get() = this != UNKNOWN && this != ANY && this != MATCH

    companion object {

        private val CODE_TO_VALUE = values().associateBy { it.code }

        private val NAME_TO_VALUE = values().associateBy { it.name }

        /**
         * Return the PatternType with the provided code or [.UNKNOWN] if one cannot be found.
         */
        fun fromCode(code: Byte): PatternType = CODE_TO_VALUE.getOrDefault(code, UNKNOWN)

        /**
         * Return the PatternType with the provided name or [.UNKNOWN] if one cannot be found.
         */
        fun fromString(name: String): PatternType = NAME_TO_VALUE.getOrDefault(name, UNKNOWN)
    }
}
