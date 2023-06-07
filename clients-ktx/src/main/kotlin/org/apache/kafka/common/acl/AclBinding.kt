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

import org.apache.kafka.common.annotation.InterfaceStability.Evolving
import org.apache.kafka.common.resource.ResourcePattern

/**
 * Represents a binding between a resource pattern and an access control entry.
 *
 * The API for this class is still evolving, and we may break compatibility in minor releases, if
 * necessary.
 *
 * Constructor creates an instance of this class with the provided parameters.
 *
 * @property pattern resource pattern.
 * @property entry entry
 */
@Evolving
data class AclBinding(
    val pattern: ResourcePattern,
    val entry: AccessControlEntry,
) {

    /**
     * @return true if this binding has any UNKNOWN components.
     */
    val isUnknown: Boolean
        get() = pattern.isUnknown || entry.isUnknown

    /**
     * @return the resource pattern for this binding.
     */
    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("pattern"),
    )
    fun pattern(): ResourcePattern = pattern

    /**
     * @return the access control entry for this binding.
     */
    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("entry"),
    )
    fun entry(): AccessControlEntry = entry

    /**
     * Create a filter which matches only this AclBinding.
     */
    fun toFilter(): AclBindingFilter {
        return AclBindingFilter(pattern.toFilter(), entry.toFilter())
    }

    override fun toString(): String {
        return "(pattern=$pattern, entry=$entry)"
    }
}
