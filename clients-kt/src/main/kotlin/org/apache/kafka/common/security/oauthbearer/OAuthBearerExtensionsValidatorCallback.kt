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
import org.apache.kafka.common.security.auth.SaslExtensions

/**
 * A [Callback] for use by the `SaslServer` implementation when it needs to validate the SASL
 * extensions for the OAUTHBEARER mechanism. Callback handlers should use the [valid] method to
 * communicate valid extensions back to the SASL server. Callback handlers should use the [error]
 * method to communicate validation errors back to the SASL Server.
 *
 * As per [RFC-7628](https://tools.ietf.org/html/rfc7628#section-3.1), unknown extensions must be
 * ignored by the server. The callback handler implementation should simply ignore unknown
 * extensions, not calling [error] nor [valid]. Callback handlers should communicate other problems
 * by raising an `IOException`.
 *
 * The OAuth bearer token is provided in the callback for better context in extension validation.
 * It is very important that token validation is done in its own [OAuthBearerValidatorCallback]
 * irregardless of provided extensions, as they are inherently insecure.
 *
 * @property token [OAuthBearerToken] The OAuth bearer token of the client.
 * @property inputExtensions [SaslExtensions] consisting of the unvalidated extension names and
 * values that were sent by the client.
 */
class OAuthBearerExtensionsValidatorCallback(
    val token: OAuthBearerToken,
    val inputExtensions: SaslExtensions,
) : Callback {

    private val validatedExtensions: MutableMap<String, String> = HashMap()

    private val invalidExtensions: MutableMap<String, String?> = HashMap()

    /**
     * @return [OAuthBearerToken] the OAuth bearer token of the client
     */
    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("token")
    )
    fun token(): OAuthBearerToken = token

    /**
     * @return [SaslExtensions] consisting of the unvalidated extension names and values that were
     * sent by the client.
     */
    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("inputExtensions")
    )
    fun inputExtensions(): SaslExtensions = inputExtensions

    /**
     * @return an unmodifiable [Map] consisting of the validated and recognized by the server
     * extension names and values
     */
    fun validatedExtensions(): Map<String, String> = validatedExtensions.toMap()

    /**
     * @return An immutable [Map] consisting of the name->error messages of extensions which failed
     * validation.
     */
    fun invalidExtensions(): Map<String, String?> = invalidExtensions.toMap()

    /**
     * @return An immutable [Map] consisting of the extensions that have neither been validated nor
     * invalidated.
     */
    fun ignoredExtensions(): Map<String, String?> =
        inputExtensions.map() - (invalidExtensions.keys + validatedExtensions.keys)

    /**
     * Validates a specific extension in the original `inputExtensions` map.
     *
     * @param extensionName The name of the extension which was validated.
     */
    fun valid(extensionName: String) {
        validatedExtensions[extensionName] = requireNotNull(inputExtensions.map()[extensionName]) {
            "Extension $extensionName was not found in the original extensions"
        }
    }

    /**
     * Set the error value for a specific extension key-value pair if validation has failed.
     *
     * @param invalidExtensionName the mandatory extension name which caused the validation failure.
     * @param errorMessage error message describing why the validation failed.
     */
    fun error(invalidExtensionName: String, errorMessage: String?) {
        require(invalidExtensionName.isNotEmpty()) {"extension name must not be empty" }
        invalidExtensions[invalidExtensionName] = errorMessage
    }
}
