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

package org.apache.kafka.common.security.oauthbearer.internals.unsecured

import java.io.Serializable

/**
 * The result of some kind of token validation
 *
 * @property success Whether this instance indicates success
 * @property failureDescription The (potentially null) descriptive message for the failure
 * @property failureScope The (potentially null) scope to be reported with the failure
 * @property failureOpenIdConfig The (potentially null) OpenID Connect configuration to be reported
 * with the failure
 */
class OAuthBearerValidationResult private constructor(
    val success: Boolean,
    val failureDescription: String?,
    val failureScope: String?,
    val failureOpenIdConfig: String?,
) : Serializable {

    init {
        require(!(success && (failureScope != null || failureOpenIdConfig != null))) {
            "success was indicated but failure scope/OpenIdConfig were provided"
        }
    }

    /**
     * Return true if this instance indicates success, otherwise false
     *
     * @return true if this instance indicates success, otherwise false
     */
    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("success")
    )
    fun success(): Boolean = success

    /**
     * Return the (potentially null) descriptive message for the failure
     *
     * @return the (potentially null) descriptive message for the failure
     */
    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("failureDescription")
    )
    fun failureDescription(): String? = failureDescription

    /**
     * Return the (potentially null) scope to be reported with the failure
     *
     * @return the (potentially null) scope to be reported with the failure
     */
    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("failureScope")
    )
    fun failureScope(): String? = failureScope

    /**
     * Return the (potentially null) OpenID Connect configuration to be reported
     * with the failure
     *
     * @return the (potentially null) OpenID Connect configuration to be reported
     * with the failure
     */
    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("failureOpenIdConfig")
    )
    fun failureOpenIdConfig(): String? = failureOpenIdConfig

    /**
     * Raise an exception if this instance indicates failure, otherwise do nothing
     *
     * @throws OAuthBearerIllegalTokenException
     * if this instance indicates failure
     */
    @Throws(OAuthBearerIllegalTokenException::class)
    fun throwExceptionIfFailed() {
        if (!success) throw OAuthBearerIllegalTokenException(this)
    }

    companion object {

        private const val serialVersionUID = 5774669940899777373L

        /**
         * Return an instance indicating success
         *
         * @return an instance indicating success
         */
        fun newSuccess(): OAuthBearerValidationResult {
            return OAuthBearerValidationResult(true, null, null, null)
        }

        /**
         * Return a new validation failure instance
         *
         * @param failureDescription optional description of the failure
         * @param failureScope optional scope to be reported with the failure
         * @param failureOpenIdConfig optional OpenID Connect configuration to be reported with the
         * failure
         * @return a new validation failure instance
         */
        fun newFailure(
            failureDescription: String?,
            failureScope: String? = null,
            failureOpenIdConfig: String? = null,
        ): OAuthBearerValidationResult {
            return OAuthBearerValidationResult(
                false,
                failureDescription,
                failureScope,
                failureOpenIdConfig
            )
        }
    }
}
