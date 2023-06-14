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

import java.io.IOException
import java.nio.charset.StandardCharsets
import java.util.*
import java.util.regex.Matcher
import java.util.regex.Pattern
import javax.security.auth.callback.Callback
import javax.security.auth.callback.UnsupportedCallbackException
import javax.security.auth.login.AppConfigurationEntry
import javax.security.sasl.SaslException
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.common.security.auth.AuthenticateCallbackHandler
import org.apache.kafka.common.security.auth.SaslExtensions
import org.apache.kafka.common.security.auth.SaslExtensionsCallback
import org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule
import org.apache.kafka.common.security.oauthbearer.OAuthBearerTokenCallback
import org.apache.kafka.common.security.oauthbearer.internals.OAuthBearerClientInitialResponse
import org.apache.kafka.common.utils.Time
import org.apache.kafka.common.utils.Utils.isBlank
import org.slf4j.LoggerFactory

/**
 * A `CallbackHandler` that recognizes [OAuthBearerTokenCallback] to return an unsecured OAuth 2
 * bearer token and [SaslExtensionsCallback] to return SASL extensions.
 *
 * Claims and their values on the returned token can be specified using
 * `unsecuredLoginStringClaim_<claimname>`, `unsecuredLoginNumberClaim_<claimname>`, and
 * `unsecuredLoginListClaim_<claimname>` options. The first character of the value is taken as the
 * delimiter for list claims. You may define any claim name and value except '`iat`' and '`exp`',
 * both of which are calculated automatically.
 *
 * You can also add custom unsecured SASL extensions using
 * `unsecuredLoginExtension_<extensionname>`. Extension keys and values are subject to regex
 * validation. The extension key must also not be equal to the reserved key
 * [OAuthBearerClientInitialResponse.AUTH_KEY].
 *
 * This implementation also accepts the following options:
 *
 * - `unsecuredLoginPrincipalClaimName` set to a custom claim name if you wish the name of the
 *   String claim holding the principal name to be something other than '`sub`'.
 * - `unsecuredLoginLifetimeSeconds` set to an integer value if the token expiration is to be set to
 *   something other than the default value of 3600 seconds (which is 1 hour). The '`exp`' claim
 *   reflects the expiration time.
 * - `unsecuredLoginScopeClaimName` set to a custom claim name if you wish the name of the String or
 *   String List claim holding any token scope to be something other than '`scope`'
 *
 * For example:
 *
 * ```plaintext
 * KafkaClient {
 *   org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule Required
 *   unsecuredLoginStringClaim_sub="thePrincipalName"
 *   unsecuredLoginListClaim_scope="|scopeValue1|scopeValue2"
 *   unsecuredLoginLifetimeSeconds="60"
 *   unsecuredLoginExtension_traceId="123";
 * };
 * ```
 *
 * This class is the default when the SASL mechanism is OAUTHBEARER and no value is explicitly set
 * via either the `sasl.login.callback.handler.class` client configuration property or the
 * `listener.name.sasl_[plaintext|ssl].oauthbearer.sasl.login.callback.handler.class` broker
 * configuration property.
 */
class OAuthBearerUnsecuredLoginCallbackHandler : AuthenticateCallbackHandler {

    private var time = Time.SYSTEM

    private lateinit var moduleOptions: Map<String, String?>

    /**
     * Whether this instance is configured.
     */
    var configured = false
        private set

    /**
     * For testing
     *
     * @param time the mandatory time to set
     */
    fun time(time: Time) {
        this.time = time
    }

    /**
     * Return true if this instance has been configured, otherwise false
     *
     * @return true if this instance has been configured, otherwise false
     */
    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("configured")
    )
    fun configured(): Boolean = configured

    override fun configure(
        configs: Map<String, *>,
        saslMechanism: String,
        jaasConfigEntries: List<AppConfigurationEntry>
    ) {
        require(OAuthBearerLoginModule.OAUTHBEARER_MECHANISM == saslMechanism) {
            String.format("Unexpected SASL mechanism: %s", saslMechanism)
        }
        require(jaasConfigEntries.size == 1) {
            String.format(
                "Must supply exactly 1 non-null JAAS mechanism configuration (size was %d)",
                jaasConfigEntries.size
            )
        }
        moduleOptions = jaasConfigEntries[0].options as Map<String, String?>
        configured = true
    }

    @Throws(IOException::class, UnsupportedCallbackException::class)
    override fun handle(callbacks: Array<Callback>) {
        check(configured) { "Callback handler not configured" }

        callbacks.forEach { callback ->
            when (callback) {
                is OAuthBearerTokenCallback -> try {
                    handleTokenCallback(callback)
                } catch (e: KafkaException) {
                    throw IOException(e.message, e)
                }

                is SaslExtensionsCallback -> try {
                    handleExtensionsCallback(callback)
                } catch (e: KafkaException) {
                    throw IOException(e.message, e)
                }

                else -> throw UnsupportedCallbackException(callback)
            }
        }
    }

    override fun close() = Unit

    private fun handleTokenCallback(callback: OAuthBearerTokenCallback) {
        require(callback.token == null) { "Callback had a token already" }
        if (moduleOptions.isEmpty()) {
            log.debug("Token not provided, this login cannot be used to establish client connections")
            callback.token = null
            return
        }
        if (moduleOptions.keys.none { name -> !name.startsWith(EXTENSION_PREFIX) }) {
            throw OAuthBearerConfigException("Extensions provided in login context without a token")
        }

        val principalClaimNameValue = optionValue(PRINCIPAL_CLAIM_NAME_OPTION)
        val principalClaimName =
            if (isBlank(principalClaimNameValue)) DEFAULT_PRINCIPAL_CLAIM_NAME
            else principalClaimNameValue!!.trim { it <= ' ' } // isBlank does null-check

        val scopeClaimNameValue = optionValue(SCOPE_CLAIM_NAME_OPTION)
        val scopeClaimName =
            if (isBlank(scopeClaimNameValue)) DEFAULT_SCOPE_CLAIM_NAME
            else scopeClaimNameValue!!.trim { it <= ' ' } // isBlank does null-check

        val headerJson = "{${claimOrHeaderJsonText("alg", "none")}}"

        val lifetimeSecondsValueToUse =
            optionValue(LIFETIME_SECONDS_OPTION) ?: DEFAULT_LIFETIME_SECONDS_ONE_HOUR

        val claimsJson: String = try {
            String.format(
                "{%s,%s%s}", expClaimText(lifetimeSecondsValueToUse.toLong()),
                claimOrHeaderJsonText("iat", time.milliseconds() / 1000.0),
                commaPrependedStringNumberAndListClaimsJsonText()
            )
        } catch (e: NumberFormatException) {
            throw OAuthBearerConfigException(e.message)
        }

        try {
            val urlEncoderNoPadding = Base64.getUrlEncoder().withoutPadding()
            val jws = OAuthBearerUnsecuredJws(
                String.format(
                    "%s.%s.",
                    urlEncoderNoPadding.encodeToString(headerJson.toByteArray()),
                    urlEncoderNoPadding.encodeToString(claimsJson.toByteArray()),
                ),
                principalClaimName, scopeClaimName
            )
            log.info("Retrieved token with principal {}", jws.principalName())
            callback.token = jws
        } catch (e: OAuthBearerIllegalTokenException) {
            // occurs if the principal claim doesn't exist or has an empty value
            throw OAuthBearerConfigException(e.message, e)
        }
    }

    /**
     * Add and validate all the configured extensions.
     * Token keys, apart from passing regex validation, must not be equal to the reserved key [OAuthBearerClientInitialResponse.AUTH_KEY]
     */
    private fun handleExtensionsCallback(callback: SaslExtensionsCallback) {

        val extensions = moduleOptions.mapNotNull { (key, value) ->
            if (!key.startsWith(EXTENSION_PREFIX) || value == null) null
            else key.substring(EXTENSION_PREFIX.length) to value
        }.toMap()

        val saslExtensions = SaslExtensions(extensions)
        try {
            OAuthBearerClientInitialResponse.validateExtensions(saslExtensions)
        } catch (e: SaslException) {
            throw ConfigException(e.message)
        }
        callback.extensions = saslExtensions
    }

    @Throws(OAuthBearerConfigException::class)
    private fun commaPrependedStringNumberAndListClaimsJsonText(): String {
        check(configured) { "Callback handler not configured" }
        val sb = StringBuilder()

        moduleOptions.forEach { (key, value) ->
            if (key.startsWith(STRING_CLAIM_PREFIX) && key.length > STRING_CLAIM_PREFIX.length)
                sb.append(',').append(
                    claimOrHeaderJsonText(
                        confirmNotReservedClaimName(key.substring(STRING_CLAIM_PREFIX.length)),
                        value!!,
                    )
                )
            else if (key.startsWith(NUMBER_CLAIM_PREFIX) && key.length > NUMBER_CLAIM_PREFIX.length)
                sb.append(',').append(
                    claimOrHeaderJsonText(
                        confirmNotReservedClaimName(key.substring(NUMBER_CLAIM_PREFIX.length)),
                        value!!.toDouble(),
                    )
                )
            else if (key.startsWith(LIST_CLAIM_PREFIX) && key.length > LIST_CLAIM_PREFIX.length)
                sb.append(',').append(
                    claimOrHeaderJsonArrayText(
                        confirmNotReservedClaimName(key.substring(LIST_CLAIM_PREFIX.length)),
                        listJsonText(value!!),
                    )
                )
        }
        return sb.toString()
    }

    @Throws(OAuthBearerConfigException::class)
    private fun confirmNotReservedClaimName(claimName: String): String {
        if (RESERVED_CLAIMS.contains(claimName)) throw OAuthBearerConfigException(
            String.format("Cannot explicitly set the '%s' claim", claimName)
        )
        return claimName
    }

    private fun listJsonText(value: String): String {
        if (value.isEmpty() || value.length <= 1) return "[]"

        val unescapedDelimiterChar = value.substring(0, 1)

        val delimiter = when (unescapedDelimiterChar) {
            "\\", ".", "[", "(", "{", "|", "^", "$" -> "\\" + unescapedDelimiterChar
            else -> unescapedDelimiterChar
        }

        val listText = value.substring(1)
        val elements = listText.split(delimiter.toRegex())
            .dropLastWhile { it.isEmpty() }
            .toTypedArray()

        val sb = StringBuilder()

        elements.forEach { element ->
            sb.append(if (sb.isEmpty()) '[' else ',')
            sb.append('"').append(escape(element)).append('"')
        }

        if (
            listText.startsWith(unescapedDelimiterChar)
            || listText.endsWith(unescapedDelimiterChar)
            || listText.contains(unescapedDelimiterChar + unescapedDelimiterChar)
        ) sb.append(",\"\"")

        return sb.append(']').toString()
    }

    private fun optionValue(key: String, defaultValue: String? = null): String? {
        val explicitValue = option(key)
        return explicitValue ?: defaultValue
    }

    private fun option(key: String): String? {
        check(configured) { "Callback handler not configured" }
        return moduleOptions[key]
    }

    private fun claimOrHeaderJsonText(claimName: String, claimValue: Number): String {
        return QUOTE + escape(claimName) + QUOTE + ":" + claimValue
    }

    private fun claimOrHeaderJsonText(claimName: String, claimValue: String): String {
        return QUOTE + escape(claimName) + QUOTE + ":" + QUOTE + escape(claimValue) + QUOTE
    }

    private fun claimOrHeaderJsonArrayText(claimName: String, escapedClaimValue: String): String {
        require(
            escapedClaimValue.startsWith("[")
                    && escapedClaimValue.endsWith("]")
        ) { String.format("Illegal JSON array: %s", escapedClaimValue) }

        return QUOTE + escape(claimName) + QUOTE + ":" + escapedClaimValue
    }

    private fun escape(jsonStringValue: String): String {
        val replace1 = DOUBLEQUOTE.matcher(jsonStringValue)
            .replaceAll(Matcher.quoteReplacement("\\\""))

        return BACKSLASH.matcher(replace1).replaceAll(Matcher.quoteReplacement("\\\\"))
    }

    private fun expClaimText(lifetimeSeconds: Long): String {
        return claimOrHeaderJsonText("exp", time.milliseconds() / 1000.0 + lifetimeSeconds)
    }

    companion object {

        private val log =
            LoggerFactory.getLogger(OAuthBearerUnsecuredLoginCallbackHandler::class.java)

        private const val OPTION_PREFIX = "unsecuredLogin"

        private const val PRINCIPAL_CLAIM_NAME_OPTION = OPTION_PREFIX + "PrincipalClaimName"

        private const val LIFETIME_SECONDS_OPTION = OPTION_PREFIX + "LifetimeSeconds"

        private const val SCOPE_CLAIM_NAME_OPTION = OPTION_PREFIX + "ScopeClaimName"

        private val RESERVED_CLAIMS = setOf("iat", "exp")

        private const val DEFAULT_PRINCIPAL_CLAIM_NAME = "sub"

        private const val DEFAULT_LIFETIME_SECONDS_ONE_HOUR = "3600"

        private const val DEFAULT_SCOPE_CLAIM_NAME = "scope"

        private const val STRING_CLAIM_PREFIX = OPTION_PREFIX + "StringClaim_"

        private const val NUMBER_CLAIM_PREFIX = OPTION_PREFIX + "NumberClaim_"

        private const val LIST_CLAIM_PREFIX = OPTION_PREFIX + "ListClaim_"

        private const val EXTENSION_PREFIX = OPTION_PREFIX + "Extension_"

        private const val QUOTE = "\""

        private val DOUBLEQUOTE = Pattern.compile("\"", Pattern.LITERAL)

        private val BACKSLASH = Pattern.compile(Regex.escape("\\"), Pattern.LITERAL)
    }
}
