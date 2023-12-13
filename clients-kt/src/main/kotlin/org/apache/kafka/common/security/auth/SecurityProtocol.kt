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

package org.apache.kafka.common.security.auth

import java.util.Locale

/**
 * @property name Name of the security protocol. This may be used by client configuration.
 * @property id The permanent and immutable id of a security protocol -- this can't change, and must
 * match `kafka.cluster.SecurityProtocol`
 */
enum class SecurityProtocol(
    val id: Short,
) {

    /**
     * Un-authenticated, non-encrypted channel
     */
    PLAINTEXT(0),

    /**
     * SSL channel
     */
    SSL(1),

    /**
     * SASL authenticated, non-encrypted channel
     */
    SASL_PLAINTEXT(2),

    /**
     * SASL authenticated, SSL channel
     */
    SASL_SSL(3);

    companion object {

        private val CODE_TO_SECURITY_PROTOCOL: Map<Short, SecurityProtocol> = entries.associateBy { it.id }

        private val NAMES: List<String> = entries.map { it.name }

        fun names(): List<String> = NAMES

        fun forId(id: Short): SecurityProtocol? = CODE_TO_SECURITY_PROTOCOL[id]

        /**
         * Case-insensitive lookup by protocol name
         */
        fun forName(name: String): SecurityProtocol = valueOf(name.uppercase(Locale.ROOT))
    }
}
