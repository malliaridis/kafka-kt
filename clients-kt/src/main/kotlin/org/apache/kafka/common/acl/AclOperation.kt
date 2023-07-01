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
 * Represents an operation which an ACL grants or denies permission to perform.
 *
 * Some operations imply other operations:
 *
 * - `ALLOW ALL` implies `ALLOW` everything
 * - `DENY ALL` implies `DENY` everything
 * - `ALLOW READ` implies `ALLOW DESCRIBE`
 * - `ALLOW WRITE` implies `ALLOW DESCRIBE`
 * - `ALLOW DELETE` implies `ALLOW DESCRIBE`
 * - `ALLOW ALTER` implies `ALLOW DESCRIBE`
 * - `ALLOW ALTER_CONFIGS` implies `ALLOW DESCRIBE_CONFIGS`
 *
 * The API for this class is still evolving, and we may break compatibility in minor releases, if
 * necessary.
 */
@Evolving
enum class AclOperation(val code: Byte) {

    /**
     * Represents any AclOperation which this client cannot understand, perhaps because this
     * client is too old.
     */
    UNKNOWN(0.toByte()),

    /**
     * In a filter, matches any AclOperation.
     */
    ANY(1.toByte()),

    /**
     * ALL operation.
     */
    ALL(2.toByte()),

    /**
     * READ operation.
     */
    READ(3.toByte()),

    /**
     * WRITE operation.
     */
    WRITE(4.toByte()),

    /**
     * CREATE operation.
     */
    CREATE(5.toByte()),

    /**
     * DELETE operation.
     */
    DELETE(6.toByte()),

    /**
     * ALTER operation.
     */
    ALTER(7.toByte()),

    /**
     * DESCRIBE operation.
     */
    DESCRIBE(8.toByte()),

    /**
     * CLUSTER_ACTION operation.
     */
    CLUSTER_ACTION(9.toByte()),

    /**
     * DESCRIBE_CONFIGS operation.
     */
    DESCRIBE_CONFIGS(10.toByte()),

    /**
     * ALTER_CONFIGS operation.
     */
    ALTER_CONFIGS(11.toByte()),

    /**
     * IDEMPOTENT_WRITE operation.
     */
    IDEMPOTENT_WRITE(12.toByte()),

    /**
     * CREATE_TOKENS operation.
     */
    CREATE_TOKENS(13.toByte()),

    /**
     * DESCRIBE_TOKENS operation.
     */
    DESCRIBE_TOKENS(14.toByte());

    /**
     * Return the code of this operation.
     */
    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("code"),
    )
    fun code(): Byte = code

    /**
     * Return true if this operation is UNKNOWN.
     */
    val isUnknown: Boolean
        get() = this == UNKNOWN

    companion object {
        // Note: we cannot have more than 30 ACL operations without modifying the format used
        // to describe ACL operations in MetadataResponse.
        private val CODE_TO_VALUE: Map<Byte, AclOperation> = values().associateBy { it.code }

        /**
         * Parse the given string as an ACL operation.
         *
         * @param str The string to parse.
         *
         * @return The [AclOperation], or [UNKNOWN] if the string could not be matched.
         */
        @Throws(IllegalArgumentException::class)
        fun fromString(str: String): AclOperation {
            return try {
                valueOf(str.uppercase())
            } catch (e: IllegalArgumentException) {
                UNKNOWN
            }
        }

        /**
         * Return the [AclOperation] with the provided code or [UNKNOWN] if one cannot be
         * found.
         */
        fun fromCode(code: Byte): AclOperation {
            return CODE_TO_VALUE[code] ?: return UNKNOWN
        }
    }
}
