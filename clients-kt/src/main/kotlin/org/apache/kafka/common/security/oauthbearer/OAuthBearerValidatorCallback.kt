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
 * A `Callback` for use by the `SaslServer` implementation when it needs to provide an OAuth 2
 * bearer token compact serialization for validation. Callback handlers should use the [error]
 * method to communicate errors back to the SASL Client as per
 * [RFC 6749: The OAuth 2.0 Authorization Framework](https://tools.ietf.org/html/rfc6749#section-5.2)
 * and the
 * [IANA OAuth Extensions Error Registry](https://www.iana.org/assignments/oauth-parameters/oauth-parameters.xhtml#extensions-error).
 * Callback handlers should communicate other problems by raising an `IOException`.
 *
 * This class was introduced in 2.0.0 and, while it feels stable, it could evolve. We will try to
 * evolve the API in a compatible manner, but we reserve the right to make breaking changes in minor
 * releases, if necessary. We will update the `InterfaceStability` annotation and this notice once
 * the API is considered stable.
 *
 * @property tokenValue The mandatory/non-blank token value
 */
@Evolving
class OAuthBearerValidatorCallback(val tokenValue: String) : Callback {

    /**
     * When setting the token, the value is unchanged and is expected to match the provided token's
     * value. All error values are cleared. When setting the token, value is mandatory.
     */
    var token: OAuthBearerToken? = null
        private set

    var errorStatus: String? = null
        private set

    var errorScope: String? = null
        private set

    var errorOpenIDConfiguration: String? = null
        private set

    init {
        require(tokenValue.isNotEmpty()) { "token value must not be empty" }
    }

    /**
     * Return the (always non-null) token value
     *
     * @return the (always non-null) token value
     */
    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("tokenValue")
    )
    fun tokenValue(): String = tokenValue

    /**
     * Return the (potentially null) token
     *
     * @return the (potentially null) token
     */
    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("token")
    )
    fun token(): OAuthBearerToken? = token

    /**
     * Return the (potentially null) error status value as per
     * [RFC 7628: A Set of Simple Authentication and Security Layer (SASL) Mechanisms for OAuth](https://tools.ietf.org/html/rfc7628#section-3.2.2)
     * and the
     * [IANA OAuth Extensions Error Registry](https://www.iana.org/assignments/oauth-parameters/oauth-parameters.xhtml#extensions-error).
     *
     * @return the (potentially null) error status value
     */
    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("errorStatus")
    )
    fun errorStatus(): String? = errorStatus

    /**
     * Return the (potentially null) error scope value as per
     * [RFC 7628: A Set
 * of Simple Authentication and Security Layer (SASL) Mechanisms for OAuth](https://tools.ietf.org/html/rfc7628#section-3.2.2).
     *
     * @return the (potentially null) error scope value
     */
    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("errorScope")
    )
    fun errorScope(): String? = errorScope

    /**
     * Return the (potentially null) error openid-configuration value as per
     * [RFC 7628: A Set
 * of Simple Authentication and Security Layer (SASL) Mechanisms for OAuth](https://tools.ietf.org/html/rfc7628#section-3.2.2).
     *
     * @return the (potentially null) error openid-configuration value
     */
    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("errorOpenIDConfiguration")
    )
    fun errorOpenIDConfiguration(): String? = errorOpenIDConfiguration

    /**
     * Set the token. The token value is unchanged and is expected to match the provided token's
     * value. All error values are cleared.
     *
     * @param token the mandatory token to set
     */
    @Deprecated("Use property instead")
    fun token(token: OAuthBearerToken) {
        this.setToken(token)
    }

    fun setToken(token: OAuthBearerToken) {
        this.token = token
        errorStatus = null
        errorScope = null
        errorOpenIDConfiguration = null
    }

    /**
     * Set the error values as per
     * [RFC 7628: A Set of Simple Authentication and Security Layer (SASL) Mechanisms for OAuth](https://tools.ietf.org/html/rfc7628#section-3.2.2).
     * Any token is cleared.
     *
     * @param errorStatus the mandatory error status value from the
     * [IANA OAuth Extensions Error Registry](https://www.iana.org/assignments/oauth-parameters/oauth-parameters.xhtml#extensions-error) to set
     * @param errorScope the optional error scope value to set. Defaults to `null` if not provided.
     * @param errorOpenIDConfiguration the optional error openid-configuration value to set.
     * Defaults to `null` if not provided.
     */
    fun error(
        errorStatus: String,
        errorScope: String? = null,
        errorOpenIDConfiguration: String? = null
    ) {
        require(errorStatus.isNotEmpty()) { "error status must not be empty" }
        this.token = null // set token since error fields are reset by setter.
        this.errorStatus = errorStatus
        this.errorScope = errorScope
        this.errorOpenIDConfiguration = errorOpenIDConfiguration
    }
}
