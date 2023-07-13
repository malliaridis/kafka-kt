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

package org.apache.kafka.clients.admin

import org.apache.kafka.common.annotation.InterfaceStability.Evolving

/**
 * A class representing a alter configuration entry containing name, value and operation type.
 *
 * The API of this class is evolving, see [Admin] for details.
 */
@Evolving
data class AlterConfigOp(
    val configEntry: ConfigEntry,
    val opType: OpType,
) {

    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("configEntry"),
    )
    fun configEntry(): ConfigEntry = configEntry

    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("opType"),
    )
    fun opType(): OpType = opType

    override fun toString(): String = "AlterConfigOp{opType=$opType, configEntry=$configEntry}"

    enum class OpType(val id: Byte) {

        /**
         * Set the value of the configuration entry.
         */
        SET(0.toByte()),

        /**
         * Revert the configuration entry to the default value (possibly `null`).
         */
        DELETE(1.toByte()),

        /**
         * (For list-type configuration entries only.) Add the specified values to the current value
         * of the configuration entry. If the configuration value has not been set, adds to the
         * default value.
         */
        APPEND(2.toByte()),

        /**
         * (For list-type configuration entries only.) Removes the specified values from the current
         * value of the configuration entry. It is legal to remove values that are not currently in
         * the configuration entry. Removing all entries from the current configuration value leaves
         * an empty list and does NOT revert to the default value of the entry.
         */
        SUBTRACT(3.toByte());

        companion object {

            private val OP_TYPES = values().associateBy(OpType::id)

            fun forId(id: Byte): OpType? = OP_TYPES[id]
        }
    }
}
