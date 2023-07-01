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

/**
 * Representation of a SASL/SCRAM Mechanism.
 *
 * @see [KIP-554: Add Broker-side SCRAM Config API](https://cwiki.apache.org/confluence/display/KAFKA/KIP-554%3A+Add+Broker-side+SCRAM+Config+API)
 */
enum class ScramMechanism(val type: Byte) {
    
    UNKNOWN(0.toByte()),
    
    SCRAM_SHA_256(1.toByte()),
    
    SCRAM_SHA_512(2.toByte());

    val mechanismName: String = toString().replace('_', '-')

    /**
     * @return the corresponding SASL SCRAM mechanism name
     * @see [Salted Challenge Response Authentication Mechanism](https://tools.ietf.org/html/rfc5802.section-4)
     */
    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("mechanismName")
    )
    fun mechanismName(): String = mechanismName

    /**
     * @return the type indicator for this SASL SCRAM mechanism
     */
    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("type")
    )
    fun type(): Byte = type

    companion object {

        /**
         * @param type the type indicator
         * @return the instance corresponding to the given type indicator, otherwise [UNKNOWN]
         */
        fun fromType(type: Byte): ScramMechanism {
            for (scramMechanism in values()) {
                if (scramMechanism.type == type) {
                    return scramMechanism
                }
            }
            return UNKNOWN
        }

        /**
         * @param mechanismName the SASL SCRAM mechanism name
         * @return the corresponding SASL SCRAM mechanism enum, otherwise [UNKNOWN]
         * @see [Salted Challenge Response Authentication Mechanism](https://tools.ietf.org/html/rfc5802.section-4)
         */
        fun fromMechanismName(mechanismName: String): ScramMechanism {
            return values().firstOrNull { mechanism: ScramMechanism ->
                mechanism.mechanismName == mechanismName
            } ?: UNKNOWN
        }
    }
}
