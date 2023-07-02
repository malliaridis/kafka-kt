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

package org.apache.kafka.common.acl

import java.util.*
import org.apache.kafka.common.annotation.InterfaceStability.Evolving
import org.apache.kafka.common.resource.ResourcePatternFilter

/**
 * A filter which can match AclBinding objects.
 *
 * The API for this class is still evolving, and we may break compatibility in minor releases, i
 * necessary.
 *
 * @constructor Creates an instance of this filter with the provided parameters.
 * @property patternFilter Pattern filter
 * @property entryFilter Access control entry filter
 */
@Evolving
data class AclBindingFilter(
    val patternFilter: ResourcePatternFilter,
    val entryFilter: AccessControlEntryFilter,
) {

    /**
     * @return `true` if this filter has any UNKNOWN components.
     */
    val isUnknown: Boolean
        get() = patternFilter.isUnknown || entryFilter.isUnknown

    /**
     * @return the resource pattern filter.
     */
    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("patternFilter"),
    )
    fun patternFilter(): ResourcePatternFilter = patternFilter

    /**
     * @return the access control entry filter.
     */
    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("entryFilter"),
    )
    fun entryFilter(): AccessControlEntryFilter = entryFilter

    override fun toString(): String {
        return "(patternFilter=$patternFilter, entryFilter=$entryFilter)"
    }

    /**
     * Return true if the resource and entry filters can only match one ACE. In other words, if
     * there are no ANY or UNKNOWN fields.
     */
    fun matchesAtMostOne(): Boolean {
        return patternFilter.matchesAtMostOne() && entryFilter.matchesAtMostOne()
    }

    /**
     * Return a string describing an ANY or UNKNOWN field, or `null` if there is no such field.
     */
    fun findIndefiniteField(): String? {
        val indefinite = patternFilter.findIndefiniteField()
        return indefinite ?: entryFilter.findIndefiniteField()
    }

    /**
     * Return true if the resource filter matches the binding's resource and the entry filter matches binding's entry.
     */
    fun matches(binding: AclBinding): Boolean {
        return patternFilter.matches(binding.pattern) && entryFilter.matches(binding.entry)
    }

    companion object {

        /**
         * A filter which matches any ACL binding.
         */
        val ANY = AclBindingFilter(ResourcePatternFilter.ANY, AccessControlEntryFilter.ANY)
    }
}
