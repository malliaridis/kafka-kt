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

import java.io.IOException
import java.util.*
import javax.security.auth.callback.Callback
import javax.security.auth.callback.UnsupportedCallbackException
import javax.security.auth.login.AppConfigurationEntry
import javax.security.sasl.SaslException
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.common.config.SaslConfigs
import org.apache.kafka.common.security.auth.AuthenticateCallbackHandler
import org.apache.kafka.common.security.auth.SaslExtensions
import org.apache.kafka.common.security.auth.SaslExtensionsCallback
import org.apache.kafka.common.security.oauthbearer.internals.OAuthBearerClientInitialResponse
import org.apache.kafka.common.security.oauthbearer.internals.secured.AccessTokenRetriever
import org.apache.kafka.common.security.oauthbearer.internals.secured.AccessTokenRetrieverFactory
import org.apache.kafka.common.security.oauthbearer.internals.secured.AccessTokenValidator
import org.apache.kafka.common.security.oauthbearer.internals.secured.AccessTokenValidatorFactory
import org.apache.kafka.common.security.oauthbearer.internals.secured.JaasOptionsUtils
import org.apache.kafka.common.security.oauthbearer.internals.secured.ValidateException
import org.slf4j.LoggerFactory
import kotlin.collections.HashMap

/**
 * `OAuthBearerLoginCallbackHandler` is an [AuthenticateCallbackHandler] that accepts
 * [OAuthBearerTokenCallback] and [SaslExtensionsCallback] callbacks to perform the steps to request
 * a JWT from an OAuth/OIDC provider using the `clientcredentials`. This grant type is commonly used
 * for non-interactive "service accounts" where there is no user available to interactively supply
 * credentials.
 *
 * The `OAuthBearerLoginCallbackHandler` is used on the client side to retrieve a JWT and the
 * [OAuthBearerValidatorCallbackHandler] is used on the broker to validate the JWT that was sent to
 * it by the client to allow access. Both the brokers and clients will need to be configured with
 * their appropriate callback handlers and respective configuration for OAuth functionality to work.
 *
 * Note that while this callback handler class must be specified for a Kafka client that wants to
 * use OAuth functionality, in the case of OAuth-based inter-broker communication, the callback
 * handler must be used on the Kafka broker side as well.
 *
 * This [AuthenticateCallbackHandler] is enabled by specifying its class name in the Kafka
 * configuration. For client use, specify the class name in the
 * [org.apache.kafka.common.config.SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS] configuration like
 * so:
 *
 * ```plaintext
 * sasl.login.callback.handler.class=org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginCallbackHandler
 * ```
 *
 * If using OAuth login on the broker side (for inter-broker communication), the callback handler
 * class will be specified with a listener-based property:
 *
 * ```plaintext
 * listener.name.<listener name>.oauthbearer.sasl.login.callback.handler.class
 * ```
 * like so:
 * ```plaintext
 * listener.name.<listener name>.oauthbearer.sasl.login.callback.handler.class=org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginCallbackHandler
 * ```
 *
 * The Kafka configuration must also include JAAS configuration which includes the following
 * OAuth-specific options:
 *
 * - `clientId`OAuth client ID (required)
 * - `clientSecret`OAuth client secret (required)
 * - `scope`OAuth scope (optional)
 *
 * The JAAS configuration can also include any SSL options that are needed. The configuration
 * options are the same as those specified by the configuration in
 * [org.apache.kafka.common.config.SslConfigs.addClientSslSupport].
 *
 * Here's an example of the JAAS configuration for a Kafka client:
 *
 * ``` plaintext
 * sasl.jaas.config=org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required \
 * clientId="foo" \
 * clientSecret="bar" \
 * scope="baz" \
 * ssl.protocol="SSL" ;
 * ```
 *
 * The configuration option
 * [org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL] is also required
 * in order for the client to contact the OAuth/OIDC provider. For example:
 *
 * ```plaintext
 * sasl.oauthbearer.token.endpoint.url=https://example.com/oauth2/v1/token
 * ```
 *
 * Please see the OAuth/OIDC providers documentation for the token endpoint URL.
 *
 * The following is a list of all the configuration options that are available for the login
 * callback handler:
 *
 * - [org.apache.kafka.common.config.SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS]
 * - [org.apache.kafka.common.config.SaslConfigs.SASL_LOGIN_CONNECT_TIMEOUT_MS]
 * - [org.apache.kafka.common.config.SaslConfigs.SASL_LOGIN_READ_TIMEOUT_MS]
 * - [org.apache.kafka.common.config.SaslConfigs.SASL_LOGIN_RETRY_BACKOFF_MS]
 * - [org.apache.kafka.common.config.SaslConfigs.SASL_LOGIN_RETRY_BACKOFF_MAX_MS]
 * - [org.apache.kafka.common.config.SaslConfigs.SASL_JAAS_CONFIG]
 * - [org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL]
 * - [org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_SCOPE_CLAIM_NAME]
 * - [org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_SUB_CLAIM_NAME]
 */
class OAuthBearerLoginCallbackHandler : AuthenticateCallbackHandler {

    private lateinit var moduleOptions: Map<String, Any>

    // Package-visible for testing.
    var accessTokenRetriever: AccessTokenRetriever? = null
        private set

    private var accessTokenValidator: AccessTokenValidator? = null

    private var isInitialized = false

    override fun configure(
        configs: Map<String, *>,
        saslMechanism: String,
        jaasConfigEntries: List<AppConfigurationEntry>,
    ) {
        moduleOptions = JaasOptionsUtils.getOptions(saslMechanism, jaasConfigEntries)
        val accessTokenRetriever =
            AccessTokenRetrieverFactory.create(configs, saslMechanism, moduleOptions)
        val accessTokenValidator = AccessTokenValidatorFactory.create(
            configs = configs,
            saslMechanism = saslMechanism,
        )
        init(accessTokenRetriever, accessTokenValidator)
    }

    fun init(
        accessTokenRetriever: AccessTokenRetriever?,
        accessTokenValidator: AccessTokenValidator?,
    ) {
        this.accessTokenRetriever = accessTokenRetriever
        this.accessTokenValidator = accessTokenValidator
        try {
            this.accessTokenRetriever!!.init()
        } catch (exception: IOException) {
            throw KafkaException(
                "The OAuth login configuration encountered an error when initializing the " +
                        "AccessTokenRetriever",
                exception
            )
        }
        isInitialized = true
    }

    override fun close() {
        accessTokenRetriever?.let { retriever ->
            try {
                retriever.close()
            } catch (e: IOException) {
                log.warn(
                    "The OAuth login configuration encountered an error when closing the AccessTokenRetriever",
                    e
                )
            }
        }
    }

    @Throws(IOException::class, UnsupportedCallbackException::class)
    override fun handle(callbacks: Array<Callback>) {
        checkInitialized()
        callbacks.forEach { callback ->
            when (callback) {
                is OAuthBearerTokenCallback -> handleTokenCallback(callback)
                is SaslExtensionsCallback -> handleExtensionsCallback(callback)
                else -> throw UnsupportedCallbackException(callback)
            }
        }
    }

    @Throws(IOException::class)
    private fun handleTokenCallback(callback: OAuthBearerTokenCallback) {
        checkInitialized()
        val accessToken = accessTokenRetriever!!.retrieve()
        try {
            val token = accessTokenValidator!!.validate(accessToken)
            callback.token = token
        } catch (e: ValidateException) {
            log.warn(e.message, e)
            callback.error(
                errorCode = "invalid_token",
                errorDescription = e.message,
            )
        }
    }

    private fun handleExtensionsCallback(callback: SaslExtensionsCallback) {
        checkInitialized()
        val extensions: MutableMap<String, String> = HashMap()
        moduleOptions.forEach { (key, valueRaw) ->
            if (!key.startsWith(EXTENSION_PREFIX)) return@forEach

            extensions[key.substring(EXTENSION_PREFIX.length)] =
                if (valueRaw is String) valueRaw else valueRaw.toString()
        }

        val saslExtensions = SaslExtensions(extensions)
        try {
            OAuthBearerClientInitialResponse.validateExtensions(saslExtensions)
        } catch (exception: SaslException) {
            throw ConfigException(exception.message)
        }
        callback.extensions = saslExtensions
    }

    private fun checkInitialized() = check(isInitialized) {
        String.format("To use %s, first call the configure or init method", javaClass.simpleName)
    }

    companion object {

        private val log = LoggerFactory.getLogger(OAuthBearerLoginCallbackHandler::class.java)

        const val CLIENT_ID_CONFIG = "clientId"

        const val CLIENT_SECRET_CONFIG = "clientSecret"

        const val SCOPE_CONFIG = "scope"

        const val CLIENT_ID_DOC = "The OAuth/OIDC identity provider-issued client ID to uniquely " +
                "identify the service account to use for authentication for this client. The " +
                "value must be paired with a corresponding $CLIENT_SECRET_CONFIG value and is " +
                "provided to the OAuth provider using the OAuth clientcredentials grant type."

        const val CLIENT_SECRET_DOC = "The OAuth/OIDC identity provider-issued client secret " +
                "serves a similar function as a password to the $CLIENT_ID_CONFIG account and " +
                "identifies the service account to use for authentication for this client. The " +
                "value must be paired with a corresponding $CLIENT_ID_CONFIG value and is " +
                "provided to the OAuth provider using the OAuth clientcredentials grant type."

        const val SCOPE_DOC = "The (optional) HTTP/HTTPS login request to the token endpoint " +
                "(${SaslConfigs.SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL}) may need to specify an " +
                "OAuth \"scope\". If so, the " + SCOPE_CONFIG + " is used to provide the value " +
                "to include with the login request."

        private const val EXTENSION_PREFIX = "extension_"
    }
}
