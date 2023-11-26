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
import java.security.Key
import java.util.*
import java.util.concurrent.atomic.AtomicInteger
import javax.security.auth.callback.Callback
import javax.security.auth.callback.UnsupportedCallbackException
import javax.security.auth.login.AppConfigurationEntry
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.security.auth.AuthenticateCallbackHandler
import org.apache.kafka.common.security.oauthbearer.internals.secured.AccessTokenValidator
import org.apache.kafka.common.security.oauthbearer.internals.secured.AccessTokenValidatorFactory
import org.apache.kafka.common.security.oauthbearer.internals.secured.CloseableVerificationKeyResolver
import org.apache.kafka.common.security.oauthbearer.internals.secured.JaasOptionsUtils
import org.apache.kafka.common.security.oauthbearer.internals.secured.ValidateException
import org.apache.kafka.common.security.oauthbearer.internals.secured.VerificationKeyResolverFactory
import org.jose4j.jws.JsonWebSignature
import org.jose4j.jwx.JsonWebStructure
import org.jose4j.lang.UnresolvableKeyException
import org.slf4j.LoggerFactory

/**
 * `OAuthBearerValidatorCallbackHandler` is an [AuthenticateCallbackHandler] that accepts
 * [OAuthBearerValidatorCallback] and [OAuthBearerExtensionsValidatorCallback] callbacks to
 * implement OAuth/OIDC validation. This callback handler is intended only to be used on the Kafka
 * broker side as it will receive a [OAuthBearerValidatorCallback] that includes the JWT provided by
 * the Kafka client. That JWT is validated in terms of format, expiration, signature, and audience
 * and issuer (if desired). This callback handler is the broker side of the OAuth functionality,
 * whereas [OAuthBearerLoginCallbackHandler] is used by clients.
 *
 * This [AuthenticateCallbackHandler] is enabled in the broker configuration by setting the
 * [org.apache.kafka.common.config.internals.BrokerSecurityConfigs.SASL_SERVER_CALLBACK_HANDLER_CLASS]
 * like so:
 *
 * ```plaintext
 * listener.name.<listener name>.oauthbearer.sasl.server.callback.handler.class=org.apache.kafka.common.security.oauthbearer.OAuthBearerValidatorCallbackHandler
 * ```
 *
 * The JAAS configuration for OAuth is also needed. If using OAuth for inter-broker communication,
 * the options are those specified in [OAuthBearerLoginCallbackHandler].
 *
 * The configuration option
 * [org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_URL] is also required
 * in order to contact the OAuth/OIDC provider to retrieve the JWKS for use in JWT signature
 * validation. For example:
 *
 * ```plaintext
 * listener.name.<listener name>.oauthbearer.sasl.oauthbearer.jwks.endpoint.url=https://example.com/oauth2/v1/keys
 * ```
 *
 * Please see the OAuth/OIDC providers documentation for the JWKS endpoint URL.
 *
 * The following is a list of all the configuration options that are available for the broker
 * validation callback handler:
 *
 * - [org.apache.kafka.common.config.internals.BrokerSecurityConfigs.SASL_SERVER_CALLBACK_HANDLER_CLASS]
 * - [org.apache.kafka.common.config.SaslConfigs.SASL_JAAS_CONFIG]
 * - [org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_CLOCK_SKEW_SECONDS]
 * - [org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_EXPECTED_AUDIENCE]
 * - [org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_EXPECTED_ISSUER]
 * - [org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_REFRESH_MS]
 * - [org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MAX_MS]
 * - [org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MS]
 * - [org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_URL]
 * - [org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_SCOPE_CLAIM_NAME]
 * - [org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_SUB_CLAIM_NAME]
 */
class OAuthBearerValidatorCallbackHandler : AuthenticateCallbackHandler {

    private var verificationKeyResolver: CloseableVerificationKeyResolver? = null

    private var accessTokenValidator: AccessTokenValidator? = null

    private var isInitialized = false

    override fun configure(
        configs: Map<String, *>,
        saslMechanism: String,
        jaasConfigEntries: List<AppConfigurationEntry>
    ) {
        val moduleOptions = JaasOptionsUtils.getOptions(saslMechanism, jaasConfigEntries)
        var verificationKeyResolver: CloseableVerificationKeyResolver

        // Here's the logic which keeps our VerificationKeyResolvers down to a single instance.
        synchronized(VERIFICATION_KEY_RESOLVER_CACHE) {
            val key = VerificationKeyResolverKey(configs, moduleOptions)
            verificationKeyResolver = VERIFICATION_KEY_RESOLVER_CACHE.computeIfAbsent(key) {
                RefCountingVerificationKeyResolver(
                        VerificationKeyResolverFactory.create(
                            configs,
                            saslMechanism,
                            moduleOptions
                        )
                    )
                }
        }

        val accessTokenValidator = AccessTokenValidatorFactory.create(
            configs,
            saslMechanism,
            verificationKeyResolver,
        )
        init(verificationKeyResolver, accessTokenValidator)
    }

    fun init(
        verificationKeyResolver: CloseableVerificationKeyResolver,
        accessTokenValidator: AccessTokenValidator?,
    ) {
        this.verificationKeyResolver = verificationKeyResolver
        this.accessTokenValidator = accessTokenValidator
        try {
            verificationKeyResolver.init()
        } catch (e: Exception) {
            throw KafkaException(
                "The OAuth validator configuration encountered an error when initializing the " +
                        "VerificationKeyResolver",
                e
            )
        }
        isInitialized = true
    }

    override fun close() {
        try {
            verificationKeyResolver?.close()
        } catch (e: Exception) {
            log.error(e.message, e)
        }
    }

    @Throws(IOException::class, UnsupportedCallbackException::class)
    override fun handle(callbacks: Array<Callback>) {
        checkInitialized()
        callbacks.forEach { callback ->
            when (callback) {
                is OAuthBearerValidatorCallback -> handleValidatorCallback(callback)
                is OAuthBearerExtensionsValidatorCallback ->
                    handleExtensionsValidatorCallback(callback)
                else -> throw UnsupportedCallbackException(callback)
            }
        }
    }

    private fun handleValidatorCallback(callback: OAuthBearerValidatorCallback) {
        checkInitialized()
        try {
            callback.token = accessTokenValidator!!.validate(callback.tokenValue)
        } catch (e: ValidateException) {
            log.warn(e.message, e)
            callback.error(errorStatus = "invalid_token")
        }
    }

    private fun handleExtensionsValidatorCallback(
        extensionsValidatorCallback: OAuthBearerExtensionsValidatorCallback,
    ) {
        checkInitialized()
        extensionsValidatorCallback.inputExtensions().map().forEach { (extensionName, _) ->
            extensionsValidatorCallback.valid(extensionName)
        }
    }

    private fun checkInitialized() = check(isInitialized) {
        "To use ${javaClass.simpleName}, first call the configure or init method"
    }

    /**
     * `VkrKey` is a simple structure which encapsulates the criteria for different sets of
     * configuration. This will allow us to use this object as a key in a [Map] to keep a single
     * instance per key.
     */
    private data class VerificationKeyResolverKey(
        private val configs: Map<String, *>,
        private val moduleOptions: Map<String, Any>
    )

    /**
     * `RefCountingVerificationKeyResolver` allows us to share a single
     * [CloseableVerificationKeyResolver] instance between multiple [AuthenticateCallbackHandler]
     * instances and perform the lifecycle methods the appropriate number of times.
     */
    private class RefCountingVerificationKeyResolver(
        private val delegate: CloseableVerificationKeyResolver
    ) : CloseableVerificationKeyResolver {

        private val count = AtomicInteger(0)

        @Throws(UnresolvableKeyException::class)
        override fun resolveKey(
            jws: JsonWebSignature,
            nestingContext: List<JsonWebStructure>
        ): Key = delegate.resolveKey(jws, nestingContext)

        @Throws(IOException::class)
        override fun init() {
            if (count.incrementAndGet() == 1) delegate.init()
        }

        @Throws(IOException::class)
        override fun close() {
            if (count.decrementAndGet() == 0) delegate.close()
        }
    }

    companion object {

        private val log = LoggerFactory.getLogger(OAuthBearerValidatorCallbackHandler::class.java)

        /**
         * Because a [CloseableVerificationKeyResolver] instance can spawn threads and issue HTTP(S)
         * calls ([RefreshingHttpsJwksVerificationKeyResolver]), we only want to create a new
         * instance for each particular set of configuration. Because each set of configuration may
         * have multiple instances, we want to reuse the single instance.
         */
        private val VERIFICATION_KEY_RESOLVER_CACHE =
            mutableMapOf<VerificationKeyResolverKey, CloseableVerificationKeyResolver>()
    }
}
