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
 * Represents an access control entry. ACEs are a tuple of principal, host, operation, and
 * [permissionType].
 *
 * The API for this class is still evolving, and we may break compatibility in minor releases, if
 * necessary.
 *
 * Constructor creates an instance of an access control entry with the provided parameters.
 *
 * @property principal principal
 * @property host host
 * @property operation operation, ANY is not an allowed operation
 * @property permissionType permission type, ANY is not an allowed type
 * @see AccessControlEntryData
*/
@Evolving
data class AccessControlEntry(
    override val principal: String,
    override val host: String,
    override val operation: AclOperation,
    override val permissionType: AclPermissionType,
) : AccessControlEntryData() {

    init {
        require(operation != AclOperation.ANY) { "operation must not be ANY" }
        require(permissionType != AclPermissionType.ANY) { "permissionType must not be ANY" }
    }

    /**
     * Return the principal for this entry.
     */
    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("principal"),
    )
    fun principal(): String = principal

    /**
     * Return the host or `*` for all hosts.
     */
    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("host"),
    )
    fun host(): String = host

    /**
     * Return the AclOperation. This method will never return AclOperation.ANY.
     */
    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("operation"),
    )
    fun operation(): AclOperation = operation

    /**
     * Return the AclPermissionType. This method will never return AclPermissionType.ANY.
     */
    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("permissionType"),
    )
    fun permissionType(): AclPermissionType = permissionType

    /**
     * Create a filter which matches only this AccessControlEntry.
     */
    fun toFilter(): AccessControlEntryFilter {
        return AccessControlEntryFilter(
            principal = principal,
            host = host,
            operation = operation,
            permissionType = permissionType,
        )
    }
}
