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

/**
 * Defines the context in which an [Authenticator] is to be created during a re-authentication.
 *
 * @property previousAuthenticator the mandatory [Authenticator] that was previously used to
 * authenticate the channel
 * @param networkReceive the applicable [NetworkReceive] instance, if any. For the client side this
 * may be a response that has been partially read, a non-null instance that has had no data read
 * into it yet, or null; if it is non-null then this is the instance that data should initially be
 * read into during re-authentication. For the server side this is mandatory and it must contain the
 * `SaslHandshakeRequest` that has been received on the server and that initiates re-authentication.
 *
 * @param reauthenticationBeginNanos the current time. The value is in nanoseconds as per
 * `System.nanoTime()` and is therefore only useful when compared to such a value -- it's absolute
 * value is meaningless. This defines the moment when re-authentication begins.
 */
data class ReauthenticationContext(
    val previousAuthenticator: Authenticator,
    val networkReceive: NetworkReceive?,
    val reauthenticationBeginNanos: Long
) {

    /**
     * Return the applicable [NetworkReceive] instance, if any. For the client
     * side this may be a response that has been partially read, a non-null instance
     * that has had no data read into it yet, or null; if it is non-null then this
     * is the instance that data should initially be read into during
     * re-authentication. For the server side this is mandatory and it must contain
     * the `SaslHandshakeRequest` that has been received on the server and
     * that initiates re-authentication.
     *
     * @return the applicable [NetworkReceive] instance, if any
     */
    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("networkReceive")
    )
    fun networkReceive(): NetworkReceive? = networkReceive

    /**
     * Return the always non-null [Authenticator] that was previously used to
     * authenticate the channel
     *
     * @return the always non-null [Authenticator] that was previously used to
     * authenticate the channel
     */
    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("previousAuthenticator")
    )
    fun previousAuthenticator(): Authenticator = previousAuthenticator

    /**
     * Return the time when re-authentication began. The value is in nanoseconds as
     * per `System.nanoTime()` and is therefore only useful when compared to
     * such a value -- it's absolute value is meaningless.
     *
     * @return the time when re-authentication began
     */
    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("reauthenticationBeginNanos")
    )
    fun reauthenticationBeginNanos(): Long = reauthenticationBeginNanos
}
