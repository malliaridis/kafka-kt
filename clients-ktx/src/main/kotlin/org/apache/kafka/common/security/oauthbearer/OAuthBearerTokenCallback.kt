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

package org.apache.kafka.common.security.oauthbearer

import javax.security.auth.callback.Callback
import org.apache.kafka.common.annotation.InterfaceStability.Evolving

/**
 * A [Callback] for use by the `SaslClient` and `Login` implementations when they require an OAuth 2
 * bearer token. Callback handlers should use the [error] method to communicate errors returned by
 * the authorization server as per
 * [RFC 6749: The OAuth 2.0 Authorization Framework](https://tools.ietf.org/html/rfc6749#section-5.2).
 * Callback handlers should communicate other problems by raising an `IOException`.
 *
 * This class was introduced in 2.0.0 and, while it feels stable, it could evolve. We will try to
 * evolve the API in a compatible manner, but we reserve the right to make breaking changes in minor
 * releases, if necessary. We will update the `InterfaceStability` annotation and this notice once
 * the API is considered stable.
 */
@Evolving
class OAuthBearerTokenCallback : Callback {

    /**
     * When setting the token all error-related values are cleared.
     */
    var token: OAuthBearerToken? = null
        set(value) {
            field = value
            errorCode = null
            errorDescription = null
            errorUri = null
        }

    var errorCode: String? = null
        private set

    var errorDescription: String? = null
        private set

    var errorUri: String? = null
        private set

    /**
     * Return the (potentially `null`) token.
     *
     * @return the (potentially `null`) token.
     */
    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("token")
    )
    fun token(): OAuthBearerToken? {
        return token
    }

    /**
     * Return the optional (but always non-empty if not null) error code as per
     * [RFC 6749: The OAuth 2.0 Authorization Framework](https://tools.ietf.org/html/rfc6749#section-5.2).
     *
     * @return the optional (but always non-empty if not null) error code
     */
    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("errorCode")
    )
    fun errorCode(): String? = errorCode

    /**
     * Return the (potentially null) error description as per
     * [RFC 6749: The OAuth 2.0 Authorization Framework](https://tools.ietf.org/html/rfc6749#section-5.2).
     *
     * @return the (potentially null) error description.
     */
    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("errorDescription")
    )
    fun errorDescription(): String? = errorDescription

    /**
     * Return the (potentially `null`) error URI as per
     * [RFC 6749: The OAuth 2.0 Authorization Framework](https://tools.ietf.org/html/rfc6749#section-5.2).
     *
     * @return the (potentially `null`) error URI.
     */
    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("errorUri")
    )
    fun errorUri(): String? = errorUri

    /**
     * Set the token. All error-related values are cleared.
     *
     * @param token the optional token to set
     */
    @Deprecated("Use property instead")
    fun token(token: OAuthBearerToken?) {
        this.token = token
    }

    /**
     * Set the error values as per
     * [RFC 6749: The OAuth 2.0 Authorization Framework](https://tools.ietf.org/html/rfc6749#section-5.2).
     * Any token is cleared.
     *
     * @param errorCode The mandatory error code to set.
     * @param errorDescription The optional error description to set.
     * @param errorUri The optional error URI to set.
     */
    fun error(
        errorCode: String,
        errorDescription: String? = null,
        errorUri: String? = null,
    ) {
        require(errorCode.isNotEmpty()) { "error code must not be empty" }
        token = null // order is important since token setter resets errors
        this.errorCode = errorCode
        this.errorDescription = errorDescription
        this.errorUri = errorUri
    }
}
