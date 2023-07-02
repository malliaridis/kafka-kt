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

package org.apache.kafka.common.security.kerberos

import java.util.regex.Pattern

/**
 * @constructor Creates an instance of `KerberosName` with the provided parameters.
 * @property serviceName The first section of the Kerberos principal name.
 * @property hostName The second section of the Kerberos principal name, may be `null`.
 * @property realm The realm of the name, may be `null`.
 */
data class KerberosName(
    val serviceName: String,
    val hostName: String? = null,
    val realm: String? = null,
) {

    /**
     * Put the name back together from the parts.
     */
    override fun toString(): String {
        val result = StringBuilder()
        result.append(serviceName)
        if (hostName != null) {
            result.append('/')
            result.append(hostName)
        }
        if (realm != null) {
            result.append('@')
            result.append(realm)
        }
        return result.toString()
    }

    /**
     * Get the first component of the name.
     * @return the first section of the Kerberos principal name
     */
    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("serviceName")
    )
    fun serviceName(): String = serviceName

    /**
     * Get the second component of the name.
     * @return the second section of the Kerberos principal name, and may be null
     */
    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("hostName")
    )
    fun hostName(): String? = hostName

    /**
     * Get the realm of the name.
     * @return the realm of the name, may be null
     */
    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("realm")
    )
    fun realm(): String? = realm

    companion object {

        /**
         * A pattern that matches a Kerberos name with at most 3 components.
         */
        private val NAME_PARSER = Pattern.compile("([^/@]*)(/([^/@]*))?@([^/@]*)")

        /**
         * Create a name from the full Kerberos principal name.
         */
        fun parse(principalName: String): KerberosName {
            val match = NAME_PARSER.matcher(principalName)

            return if (!match.matches()) {
                require(!principalName.contains("@")) {
                    "Malformed Kerberos name: $principalName"
                }
                KerberosName(principalName)
            } else KerberosName(match.group(1), match.group(3), match.group(4))
        }
    }
}
