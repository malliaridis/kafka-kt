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

package org.apache.kafka.common.security.oauthbearer.internals

import java.nio.charset.StandardCharsets
import java.util.regex.Pattern
import javax.security.sasl.SaslException
import org.apache.kafka.common.security.auth.SaslExtensions
import org.apache.kafka.common.utils.Utils.mkString
import org.apache.kafka.common.utils.Utils.parseMap

class OAuthBearerClientInitialResponse {

    val tokenValue: String

    val authorizationId: String

    var saslExtensions: SaslExtensions
        private set

    /**
     * The SASLExtensions in an OAuth protocol-friendly string.
     */
    private val extensionsMessage: String
        get() = mkString(saslExtensions.map(), "", "", "=", SEPARATOR)

    constructor(response: ByteArray) {
        val responseMsg = String(response, StandardCharsets.UTF_8)

        val matcher = CLIENT_INITIAL_RESPONSE_PATTERN.matcher(responseMsg)
        if (!matcher.matches()) throw SaslException("Invalid OAUTHBEARER client first message")

        authorizationId = matcher.group("authzid") ?: ""

        val kvPairs = matcher.group("kvpairs")
        val properties = parseMap(kvPairs, "=", SEPARATOR).toMutableMap()
        val auth = properties[AUTH_KEY]
            ?: throw SaslException("Invalid OAUTHBEARER client first message: 'auth' not specified")
        properties.remove(AUTH_KEY)

        val extensions = SaslExtensions(properties)
        validateExtensions(extensions)
        saslExtensions = extensions

        val authMatcher = AUTH_PATTERN.matcher(auth)
        if (!authMatcher.matches())
            throw SaslException("Invalid OAUTHBEARER client first message: invalid 'auth' format")
        if (!"bearer".equals(authMatcher.group("scheme"), ignoreCase = true)) {
            val msg = String.format(
                "Invalid scheme in OAUTHBEARER client first message: %s",
                matcher.group("scheme"),
            )
            throw SaslException(msg)
        }
        tokenValue = authMatcher.group("token")
    }

    /**
     * Constructor
     *
     * @param tokenValue The mandatory token value.
     * @param authorizationId The optional authorization ID. Defaults to empty string if not
     * provided or `null`.
     * @param extensions The optional extensions.
     * @throws SaslException If any extension name or value fails to conform to the required regular
     * expression as defined by the specification, or if the reserved `auth` appears as a key.
     */
    constructor(
        tokenValue: String,
        authorizationId: String? = "",
        extensions: SaslExtensions? = SaslExtensions.empty(),
    ) {
        this.tokenValue = tokenValue
        this.authorizationId = authorizationId ?: ""
        validateExtensions(extensions)
        saslExtensions = extensions ?: SaslExtensions.empty()
    }

    /**
     * Return the always non-null extensions
     *
     * @return the always non-null extensions
     */
    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("saslExtensions")
    )
    fun extensions(): SaslExtensions = saslExtensions

    fun toBytes(): ByteArray {
        val authzid = if (authorizationId.isEmpty()) "" else "a=$authorizationId"

        var extensions = extensionsMessage
        if (extensions.isNotEmpty()) extensions = SEPARATOR + extensions

        return String.format(
            "n,%s,%sauth=Bearer %s%s%s%s",
            authzid,
            SEPARATOR,
            tokenValue,
            extensions,
            SEPARATOR,
            SEPARATOR
        ).toByteArray(StandardCharsets.UTF_8)
    }

    /**
     * Return the always non-null token value
     *
     * @return the always non-null toklen value
     */
    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("tokenValue")
    )
    fun tokenValue(): String = tokenValue

    /**
     * Return the always non-null authorization ID
     *
     * @return the always non-null authorization ID
     */
    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("authorizationId")
    )
    fun authorizationId(): String = authorizationId

    companion object {

        const val SEPARATOR = "\u0001"

        private const val SASLNAME = "(?:[\\x01-\\x7F&&[^=,]]|=2C|=3D)+"

        private const val KEY = "[A-Za-z]+"

        private const val VALUE = "[\\x21-\\x7E \t\r\n]+"

        private val KVPAIRS = String.format("(%s=%s%s)*", KEY, VALUE, SEPARATOR)

        private val AUTH_PATTERN =
            Pattern.compile("(?<scheme>[\\w]+)[ ]+(?<token>[-_\\.a-zA-Z0-9]+)")

        private val CLIENT_INITIAL_RESPONSE_PATTERN = Pattern.compile(
            String.format(
                "n,(a=(?<authzid>%s))?,%s(?<kvpairs>%s)%s",
                SASLNAME,
                SEPARATOR,
                KVPAIRS,
                SEPARATOR
            )
        )

        const val AUTH_KEY = "auth"

        val EXTENSION_KEY_PATTERN = Pattern.compile(KEY)

        val EXTENSION_VALUE_PATTERN = Pattern.compile(VALUE)

        /**
         * Validates that the given extensions conform to the standard. They should also not contain
         * the reserve key name [OAuthBearerClientInitialResponse.AUTH_KEY].
         *
         * @param extensions optional extensions to validate.
         * @throws SaslException if any extension name or value fails to conform to the required
         * regular expression as defined by the specification, or if the reserved `auth` appears as
         * a key.
         *
         * @see [RFC 7628, Section 3.1](https://tools.ietf.org/html/rfc7628.section-3.1)
         */
        @Throws(SaslException::class)
        fun validateExtensions(extensions: SaslExtensions?) {
            if (extensions == null) return
            if (extensions.map().containsKey(AUTH_KEY))
                throw SaslException("Extension name " + AUTH_KEY + " is invalid")

            extensions.map().forEach { (extensionName, extensionValue) ->
                if (!EXTENSION_KEY_PATTERN.matcher(extensionName).matches()) throw SaslException(
                    "Extension name $extensionName is invalid"
                )

                if (!EXTENSION_VALUE_PATTERN.matcher(extensionValue).matches()) throw SaslException(
                    "Extension value ($extensionValue) for extension $extensionName is invalid"
                )
            }
        }
    }
}
