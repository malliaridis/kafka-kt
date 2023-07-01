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
 * Represents a filter that can match [ResourcePattern].
 *
 * The API for this class is still evolving and we may break compatibility in minor releases, if
 * necessary.
 *
 * @constructor Create a filter using the supplied parameters.
 * @param resourceType resource type. If [ResourceType.ANY], the filter will ignore the resource
 * type of the pattern. If any other resource type, the filter will match only patterns with the
 * same type.
 * @param name resource name or `null`. If `null`, the filter will ignore the name of resources.
 * If [ResourcePattern.WILDCARD_RESOURCE], will match only wildcard patterns.
 * @param patternType resource pattern type. If [PatternType.ANY], the filter will match patterns
 * regardless of pattern type. If [PatternType.MATCH], the filter will match patterns that would
 * match the supplied `name`, including a matching prefixed and wildcards patterns. If any other
 * resource pattern type, the filter will match only patterns with the same type.
 */
@Evolving
data class ResourcePatternFilter(
    val resourceType: ResourceType,
    val name: String? = null,
    val patternType: PatternType,
) {

    /**
     * Whether this filter has any UNKNOWN components.
     */
    val isUnknown: Boolean
        get() = resourceType.isUnknown || patternType.isUnknown

    /**
     * @return the specific resource type this pattern matches
     */
    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("resourceType")
    )
    fun resourceType(): ResourceType = resourceType

    /**
     * @return the resource name.
     */
    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("name")
    )
    fun name(): String? = name

    /**
     * @return the resource pattern type.
     */
    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("patternType")
    )
    fun patternType(): PatternType = patternType

    /**
     * @return `true` if this filter matches the given pattern.
     */
    fun matches(pattern: ResourcePattern): Boolean {
        return if (resourceType != ResourceType.ANY && resourceType != pattern.resourceType) false
        else if (
            patternType != PatternType.ANY
            && patternType != PatternType.MATCH
            && patternType != pattern.patternType
        ) false
        else if (name == null) true
        else if (
            patternType == PatternType.ANY
            || patternType == pattern.patternType
        ) name == pattern.name
        else when (pattern.patternType) {
            PatternType.LITERAL ->
                name == pattern.name || pattern.name == ResourcePattern.WILDCARD_RESOURCE

            PatternType.PREFIXED -> name.startsWith(pattern.name)
            else -> throw IllegalArgumentException("Unsupported PatternType: " + pattern.patternType)
        }
    }

    /**
     * @return `true` if this filter could only match one pattern. In other words, if there are no
     * ANY or UNKNOWN fields.
     */
    fun matchesAtMostOne(): Boolean = findIndefiniteField() == null

    /**
     * @return a string describing any ANY or UNKNOWN field, or `null` if there is no such field.
     */
    fun findIndefiniteField(): String? {
        if (resourceType == ResourceType.ANY) return "Resource type is ANY."
        if (resourceType == ResourceType.UNKNOWN) return "Resource type is UNKNOWN."
        if (name == null) return "Resource name is NULL."
        if (patternType == PatternType.MATCH) return "Resource pattern type is MATCH."
        return if (patternType == PatternType.UNKNOWN) "Resource pattern type is UNKNOWN." else null
    }

    override fun toString(): String {
        return "ResourcePattern(resourceType=$resourceType" +
                ", name=${(name ?: "<any>")}" +
                ", patternType=$patternType)"
    }

    companion object {

        /**
         * Matches any resource pattern.
         */
        val ANY = ResourcePatternFilter(
            resourceType = ResourceType.ANY,
            patternType = PatternType.ANY,
        )
    }
}
