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
 * Describes a client quota entity, which is a mapping of entity types to their names.
 *
 * Constructs a quota entity for the given types and names. If a name is null,
 * then it is mapped to the built-in default entity name.
 *
 * @property entries maps entity type to its name
 */
data class ClientQuotaEntity(val entries: Map<String, String>) {

    /**
     * @return map of entity type to its name
     */
    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("entries"),
    )
    fun entries(): Map<String, String> = entries

    override fun toString(): String = "ClientQuotaEntity(entries=$entries)"

    companion object {

        /**
         * The type of an entity entry.
         */
        const val USER = "user"

        const val CLIENT_ID = "client-id"

        const val IP = "ip"

        fun isValidEntityType(entityType: String?): Boolean {
            return entityType == USER || entityType == CLIENT_ID || entityType == IP
        }
    }
}
