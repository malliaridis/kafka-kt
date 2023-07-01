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

package org.apache.kafka.common.quota

/**
 * Describes a component for applying a client quota filter.
 *
 * @property entityType the entity type the filter component applies to
 * @property match if present, the name that's matched exactly, if empty, matches the default name
 * and if `null`, matches any specified name
 */
data class ClientQuotaFilterComponent(
    val entityType: String,
    val match: String? = null,
) {

    /**
     * @return the component's entity type
     */
    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("entityType"),
    )
    fun entityType(): String = entityType

    /**
     * @return the optional match string, where:
     * - if present, the name that's matched exactly
     * - if empty, matches the default name
     * - if null, matches any specified name
     */
    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("match"),
    )
    fun match(): String? = match

    override fun toString(): String {
        return "ClientQuotaFilterComponent(entityType=$entityType, match=$match)"
    }

    companion object {

        /**
         * Constructs and returns a filter component that exactly matches the provided entity
         * name for the entity type.
         *
         * @param entityType the entity type the filter component applies to
         * @param entityName the entity name that's matched exactly
         */
        @Deprecated("Use ClientQuotaFilterComponent constructor instead")
        fun ofEntity(entityType: String, entityName: String): ClientQuotaFilterComponent {
            return ClientQuotaFilterComponent(
                entityType,
                entityName
            )
        }

        /**
         * Constructs and returns a filter component that matches the built-in default entity name
         * for the entity type.
         *
         * @param entityType the entity type the filter component applies to
         */
        @Deprecated("Use ClientQuotaFilterComponent constructor instead with `null` for `match`.")
        fun ofDefaultEntity(entityType: String): ClientQuotaFilterComponent {
            return ClientQuotaFilterComponent(entityType, null)
        }

        /**
         * Constructs and returns a filter component that matches any specified name for the
         * entity type.
         *
         * @param entityType the entity type the filter component applies to
         */
        @Deprecated("Use ClientQuotaFilterComponent constructor instead.")
        fun ofEntityType(entityType: String): ClientQuotaFilterComponent {
            return ClientQuotaFilterComponent(entityType, null)
        }
    }
}
