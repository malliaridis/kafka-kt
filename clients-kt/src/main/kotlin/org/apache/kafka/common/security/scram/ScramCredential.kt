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

package org.apache.kafka.common.security.scram

/**
 * SCRAM credential class that encapsulates the credential data persisted for each user that is
 * accessible to the server. See [RFC rfc5802](https://tools.ietf.org/html/rfc5802#section-5)
 * for details.
 */
class ScramCredential(
    val salt: ByteArray,
    val storedKey: ByteArray,
    val serverKey: ByteArray,
    val iterations: Int,
) {

    /**
     * Returns the salt used to process this credential using the SCRAM algorithm.
     */
    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("salt"),
    )
    fun salt(): ByteArray = salt

    /**
     * Server key computed from the client password using the SCRAM algorithm.
     */
    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("serverKey"),
    )
    fun serverKey(): ByteArray = serverKey

    /**
     * Stored key computed from the client password using the SCRAM algorithm.
     */
    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("storedKey"),
    )
    fun storedKey(): ByteArray = storedKey

    /**
     * Number of iterations used to process this credential using the SCRAM algorithm.
     */
    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("iterations"),
    )
    fun iterations(): Int = iterations
}
