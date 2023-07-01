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

/**
 * An internal, private class which contains the data stored in AccessControlEntry and
 * AccessControlEntryFilter objects.
 */
sealed class AccessControlEntryData {

    abstract val principal: String?
    abstract val host: String?
    abstract val operation: AclOperation
    abstract val permissionType: AclPermissionType

    override fun toString(): String {
        return "(principal=${principal ?: "<any>"}" +
                ", host=${host ?: "<any>"}" +
                ", operation=$operation" +
                ", permissionType=$permissionType)"
    }

    /**
     * Return true if there are any UNKNOWN components.
     */
    val isUnknown: Boolean
        get() = operation.isUnknown || permissionType.isUnknown
}
