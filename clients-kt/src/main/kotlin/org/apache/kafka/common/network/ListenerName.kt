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

package org.apache.kafka.common.network

import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.common.security.auth.SecurityProtocol

data class ListenerName(val value: String) {

    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("value")
    )
    fun value(): String = value

    override fun toString(): String = "ListenerName($value)"

    fun configPrefix(): String = "$CONFIG_STATIC_PREFIX.${value.lowercase()}."

    fun saslMechanismConfigPrefix(saslMechanism: String): String =
        configPrefix() + saslMechanismPrefix(saslMechanism)

    companion object {

        private const val CONFIG_STATIC_PREFIX = "listener.name"

        /**
         * Create an instance with the security protocol name as the value.
         */
        fun forSecurityProtocol(securityProtocol: SecurityProtocol): ListenerName =
            ListenerName(securityProtocol.name)

        /**
         * Create an instance with the provided value converted to uppercase.
         */
        fun normalised(value: String): ListenerName {
            if (value.isBlank()) throw ConfigException("The provided listener name is null or empty string")
            return ListenerName(value.uppercase())
        }

        fun saslMechanismPrefix(saslMechanism: String): String = saslMechanism.lowercase() + "."
    }
}
