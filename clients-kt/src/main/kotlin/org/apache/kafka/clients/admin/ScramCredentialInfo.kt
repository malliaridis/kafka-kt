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
 * Mechanism and iterations for a SASL/SCRAM credential associated with a user.
 *
 * @property mechanism the required mechanism
 * @property iterations the number of iterations used when creating the credential
 * @see [KIP-554: Add Broker-side SCRAM Config API](https://cwiki.apache.org/confluence/display/KAFKA/KIP-554%3A+Add+Broker-side+SCRAM+Config+API)
 */
data class ScramCredentialInfo(
    val mechanism: ScramMechanism,
    val iterations: Int,
) {

    /**
     * @return the mechanism
     */
    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("mechanism"),
    )
    fun mechanism(): ScramMechanism = mechanism

    /**
     *
     * @return the number of iterations used when creating the credential
     */
    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("iterations"),
    )
    fun iterations(): Int = iterations

    override fun toString(): String {
        return "ScramCredentialInfo{" +
                "mechanism=$mechanism" +
                ", iterations=$iterations" +
                '}'
    }
}
