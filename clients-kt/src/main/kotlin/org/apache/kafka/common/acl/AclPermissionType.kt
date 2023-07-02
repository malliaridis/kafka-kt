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
 * Represents whether an ACL grants or denies permissions.
 *
 * The API for this class is still evolving, and we may break compatibility in minor releases, if necessary.
 */
@Evolving
enum class AclPermissionType(val code: Byte) {

    /**
     * Represents any AclPermissionType which this client cannot understand,
     * perhaps because this client is too old.
     */
    UNKNOWN(0.toByte()),

    /**
     * In a filter, matches any AclPermissionType.
     */
    ANY(1.toByte()),

    /**
     * Disallows access.
     */
    DENY(2.toByte()),

    /**
     * Grants access.
     */
    ALLOW(3.toByte());

    /**
     * Return the code of this permission type.
     */
    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("code"),
    )
    fun code(): Byte {
        return code
    }

    /**
     * Return true if this permission type is UNKNOWN.
     */
    val isUnknown: Boolean
        get() = this == UNKNOWN

    companion object {

        private val CODE_TO_VALUE: Map<Byte, AclPermissionType> = values().associateBy { it.code }

        /**
         * Parse the given string as an ACL permission.
         *
         * @param str The string to parse.
         *
         * @return The [AclPermissionType], or [UNKNOWN] if the string could not be matched.
         */
        fun fromString(str: String): AclPermissionType {
            return try {
                valueOf(str.uppercase())
            } catch (e: IllegalArgumentException) {
                UNKNOWN
            }
        }

        /**
         * Return the [AclPermissionType] with the provided code or [UNKNOWN] if one cannot be found.
         */
        fun fromCode(code: Byte): AclPermissionType {
            return CODE_TO_VALUE[code] ?: return UNKNOWN
        }
    }
}
