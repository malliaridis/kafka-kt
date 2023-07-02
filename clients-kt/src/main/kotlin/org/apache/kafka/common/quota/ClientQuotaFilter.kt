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

import java.util.*

/**
 * Describes a client quota entity filter.
 *
 * @constructor Creates a filter to be applied to matching client quotas.
 * @property components the components to filter on
 * @property strict whether the filter only includes specified components
 */
class ClientQuotaFilter private constructor(
    private val components: Collection<ClientQuotaFilterComponent>,
    private val strict: Boolean
) {

    /**
     * @return the filter's components
     */
    fun components(): Collection<ClientQuotaFilterComponent> {
        return components
    }

    /**
     * @return whether the filter is strict, i.e. only includes specified components
     */
    fun strict(): Boolean {
        return strict
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other == null || javaClass != other.javaClass) return false
        val that = other as ClientQuotaFilter
        return components == that.components && strict == that.strict
    }

    override fun hashCode(): Int {
        return Objects.hash(components, strict)
    }

    override fun toString(): String {
        return "ClientQuotaFilter(components=$components, strict=$strict)"
    }

    companion object {

        /**
         * Constructs and returns a quota filter that matches all provided components. Matching
         * entities with entity types that are not specified by a component will also be included
         * in the result.
         *
         * @param components the components for the filter
         */
        fun contains(components: Collection<ClientQuotaFilterComponent>): ClientQuotaFilter {
            return ClientQuotaFilter(components, false)
        }

        /**
         * Constructs and returns a quota filter that matches all provided components. Matching
         * entities with entity types that are not specified by a component will *not* be included
         * in the result.
         *
         * @param components the components for the filter
         */
        fun containsOnly(components: Collection<ClientQuotaFilterComponent>): ClientQuotaFilter {
            return ClientQuotaFilter(components, true)
        }

        /**
         * Constructs and returns a quota filter that matches all configured entities.
         */
        fun all(): ClientQuotaFilter {
            return ClientQuotaFilter(emptyList(), false)
        }
    }
}
