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

package org.apache.kafka.common.security.scram.internals

/*
 * This code is duplicated in org.apache.kafka.clients.admin.ScramMechanism.
 * The type field in both files must match and must not change. The type field
 * is used both for passing ScramCredentialUpsertion and for the internal
 * UserScramCredentialRecord. Do not change the type field.
 */
enum class ScramMechanism(
    val type: Byte,
    val hashAlgorithm: String,
    val macAlgorithm: String,
    val minIterations: Int,
    val maxIterations: Int,
) {
    SCRAM_SHA_256(
        type = 1,
        hashAlgorithm = "SHA-256",
        macAlgorithm = "HmacSHA256",
        minIterations = 4096,
        maxIterations = 16384,
    ),
    SCRAM_SHA_512(
        type = 2,
        hashAlgorithm = "SHA-512",
        macAlgorithm = "HmacSHA512",
        minIterations = 4096,
        maxIterations = 16384,
    );

    val mechanismName: String = "SCRAM-$hashAlgorithm"

    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("mechanismName"),
    )
    fun mechanismName(): String = mechanismName

    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("hashAlgorithm"),
    )
    fun hashAlgorithm(): String = hashAlgorithm

    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("macAlgorithm"),
    )
    fun macAlgorithm(): String = macAlgorithm

    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("minIterations"),
    )
    fun minIterations(): Int = minIterations

    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("maxIterations"),
    )
    fun maxIterations(): Int = maxIterations

    /**
     * @return the type indicator for this SASL SCRAM mechanism
     */
    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("type"),
    )
    fun type(): Byte = this.type

    companion object {

        private val MECHANISMS_MAP: Map<String, ScramMechanism>

        init {
            val map: MutableMap<String, ScramMechanism> = HashMap()
            for (mech in values()) map[mech.mechanismName] = mech
            MECHANISMS_MAP = map
        }

        fun forMechanismName(mechanismName: String): ScramMechanism? = MECHANISMS_MAP[mechanismName]

        @Deprecated(
            message = "Use property instead",
            replaceWith = ReplaceWith("mechanismNames"),
        )
        fun mechanismNames(): Collection<String> = MECHANISMS_MAP.keys

        val mechanismNames: Collection<String> = MECHANISMS_MAP.keys

        fun isScram(mechanismName: String): Boolean = MECHANISMS_MAP.containsKey(mechanismName)
    }
}
