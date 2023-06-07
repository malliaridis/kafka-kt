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
 * Represents a pattern that is used by ACLs to match zero or more
 * [Resources][org.apache.kafka.common.resource.Resource].
 *
 * The API for this class is still evolving and we may break compatibility in minor releases, if
 * necessary.
 *
 * @property resourceType non-null, specific, resource type
 * @property name resource name, which can be the [WILDCARD_RESOURCE].
 * @property patternType specific, resource pattern type, which controls how the pattern will match
 * resource names.
 */
@Evolving
data class ResourcePattern(
    val resourceType: ResourceType,
    val name: String,
    val patternType: PatternType,
) {

    init {
        require(resourceType != ResourceType.ANY) { "resourceType must not be ANY" }
        require(!(patternType == PatternType.MATCH || patternType == PatternType.ANY)) {
            "patternType must not be $patternType"
        }
    }

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
    fun name(): String = name

    /**
     * @return the resource pattern type.
     */
    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("patternType")
    )
    fun patternType(): PatternType = patternType

    /**
     * @return a filter which matches only this pattern.
     */
    fun toFilter(): ResourcePatternFilter {
        return ResourcePatternFilter(resourceType, name, patternType)
    }

    override fun toString(): String {
        return "ResourcePattern(resourceType=$resourceType, name=$name, patternType=$patternType)"
    }

    /**
     * @return `true` if this Resource has any UNKNOWN components.
     */
    val isUnknown: Boolean
        get() = resourceType.isUnknown || patternType.isUnknown

    companion object {

        /**
         * A special literal resource name that corresponds to 'all resources of a certain type'.
         */
        const val WILDCARD_RESOURCE = "*"
    }
}
