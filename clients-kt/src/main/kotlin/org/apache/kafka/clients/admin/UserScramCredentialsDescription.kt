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
 * Representation of all SASL/SCRAM credentials associated with a user that can be retrieved, or an
 * exception indicating why credentials could not be retrieved.
 *
 * @property name the required user name
 * @property credentialInfos the required SASL/SCRAM credential representations for the user
 * @see [KIP-554: Add Broker-side SCRAM Config API](https://cwiki.apache.org/confluence/display/KAFKA/KIP-554%3A+Add+Broker-side+SCRAM+Config+API)
 */
data class UserScramCredentialsDescription(
    val name: String,
    val credentialInfos: List<ScramCredentialInfo>,
) {

    override fun toString(): String =
        "UserScramCredentialsDescription{name='$name', credentialInfos=$credentialInfos}"


    /**
     * @return the user name
     */
    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("name"),
    )
    fun name(): String = name

    /**
     * @return the always non-null/unmodifiable list of SASL/SCRAM credential representations for
     * the user
     */
    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("credentialInfos"),
    )
    fun credentialInfos(): List<ScramCredentialInfo> = credentialInfos
}
