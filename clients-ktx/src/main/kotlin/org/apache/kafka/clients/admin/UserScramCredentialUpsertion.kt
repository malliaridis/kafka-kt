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

import org.apache.kafka.common.security.scram.internals.ScramFormatter
import java.nio.charset.StandardCharsets
import java.security.SecureRandom
import java.util.*

/**
 * A request to update/insert a SASL/SCRAM credential for a user.
 *
 * @constructor Constructor that accepts an explicit salt
 *
 * @param user the user for which the credential is to be updated/inserted
 * @property credentialInfo the mechanism and iterations to be used
 * @property password the password
 * @property salt the salt to be used. Generates random salt by default.
 * @see [KIP-554: Add Broker-side SCRAM Config API](https://cwiki.apache.org/confluence/display/KAFKA/KIP-554%3A+Add+Broker-side+SCRAM+Config+API)
 */
class UserScramCredentialUpsertion(
    user: String,
    val credentialInfo: ScramCredentialInfo,
    val password: ByteArray,
    val salt: ByteArray = generateRandomSalt(),
) : UserScramCredentialAlteration(user) {

    /**
     * Constructor that generates a random salt
     *
     * @param user the user for which the credential is to be updated/inserted
     * @param credentialInfo the mechanism and iterations to be used
     * @param password the password
     */
    constructor(
        user: String,
        credentialInfo: ScramCredentialInfo,
        password: String
    ) : this(
        user = user,
        credentialInfo = credentialInfo,
        password = password.toByteArray(StandardCharsets.UTF_8),
    )

    /**
     *
     * @return the mechanism and iterations
     */
    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("credentialInfo"),
    )
    fun credentialInfo(): ScramCredentialInfo = credentialInfo

    /**
     *
     * @return the salt
     */
    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("salt"),
    )
    fun salt(): ByteArray = salt

    /**
     *
     * @return the password
     */
    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("password"),
    )
    fun password(): ByteArray = password

    companion object {
        private fun generateRandomSalt(): ByteArray {
            return ScramFormatter.secureRandomBytes(SecureRandom())
        }
    }
}
