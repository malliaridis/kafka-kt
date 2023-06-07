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

/**
 * Represents a filter which matches access control entries.
 *
 * The API for this class is still evolving, and we may break compatibility in minor releases, if
 * necessary.
 *
 * Constructor creates an instance of an access control entry filter with the provided parameters.
 *
 * @param principal the principal
 * @param host the host
 * @param operation operation
 * @param permissionType permission type
 */
@Evolving
data class AccessControlEntryFilter(
    override val principal: String?,
    override val host: String?,
    override val operation: AclOperation,
    override val permissionType: AclPermissionType,
) : AccessControlEntryData() {

    /**
     * Return the principal or null.
     */
    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("principal"),
    )
    fun principal(): String? = principal

    /**
     * Return the host or null. The value `*` means any host.
     */
    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("host"),
    )
    fun host(): String? = host

    /**
     * Return the AclOperation.
     */
    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("operation"),
    )
    fun operation(): AclOperation = operation

    /**
     * Return the AclPermissionType.
     */
    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("permissionType"),
    )
    fun permissionType(): AclPermissionType = permissionType

    /**
     * Returns true if this filter matches the given AccessControlEntry.
     */
    fun matches(other: AccessControlEntry): Boolean {
        return if (principal != null && principal != other.principal) false
        else if (host != null && host != other.host) false
        else if (operation != AclOperation.ANY
            && operation != other.operation) false
        else permissionType == AclPermissionType.ANY
                || permissionType == other.permissionType
    }

    /**
     * Returns true if this filter could only match one ACE -- in other words, if
     * there are no ANY or UNKNOWN fields.
     */
    fun matchesAtMostOne(): Boolean {
        return findIndefiniteField() == null
    }

    /**
     * Returns a string describing an ANY or UNKNOWN field, or null if there is
     * no such field.
     */
    fun findIndefiniteField(): String? {
        return if (principal == null) "Principal is NULL"
        else if (host == null) "Host is NULL"
        else if (operation == AclOperation.ANY) "Operation is ANY"
        else if (operation == AclOperation.UNKNOWN) "Operation is UNKNOWN"
        else if (permissionType == AclPermissionType.ANY) "Permission type is ANY"
        else if (permissionType == AclPermissionType.UNKNOWN) "Permission type is UNKNOWN"
        else null
    }

    companion object {

        /**
         * Matches any access control entry.
         */
        val ANY = AccessControlEntryFilter(
            principal = null,
            host = null,
            operation = AclOperation.ANY,
            permissionType = AclPermissionType.ANY)
    }
}
