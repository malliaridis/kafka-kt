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
import javax.security.auth.Subject
import javax.security.auth.callback.Callback
import javax.security.auth.callback.CallbackHandler
import javax.security.auth.callback.UnsupportedCallbackException
import javax.security.auth.login.LoginException
import javax.security.auth.spi.LoginModule
import org.apache.kafka.common.security.auth.AuthenticateCallbackHandler
import org.apache.kafka.common.security.auth.SaslExtensions
import org.apache.kafka.common.security.auth.SaslExtensionsCallback
import org.apache.kafka.common.security.oauthbearer.internals.OAuthBearerSaslClientProvider
import org.apache.kafka.common.security.oauthbearer.internals.OAuthBearerSaslServerProvider
import org.slf4j.LoggerFactory

/**
 * The `LoginModule` for the SASL/OAUTHBEARER mechanism. When a client (whether a non-broker client
 * or a broker when SASL/OAUTHBEARER is the inter-broker protocol) connects to Kafka the
 * `OAuthBearerLoginModule` instance asks its configured [AuthenticateCallbackHandler]
 * implementation to handle an instance of [OAuthBearerTokenCallback] and return an instance of
 * [OAuthBearerToken]. A default, builtin [AuthenticateCallbackHandler] implementation creates an
 * unsecured token as defined by these JAAS module options:
 *
 * | JAAS Module Option for Unsecured Token Retrieval | Documentation |
 * | ------------------------------------------------ | ------------- |
 * | `unsecuredLoginStringClaim_<claimname>="value"` | Creates a `String` claim with the given name and value. Any valid claim name can be specified except '`iat`' and '`exp`' (these are automatically generated). |
 * | `unsecuredLoginNumberClaim_<claimname>="value"` | Creates a `Number` claim with the given name and value. Any valid claim name can be specified except '`iat`' and '`exp`' (these are automatically generated). |
 * | `unsecuredLoginListClaim_<claimname>="value"` | Creates a `String List` claim with the given name and values parsed from the given value where the first character is taken as the delimiter. For example: `unsecuredLoginListClaim_fubar="|value1|value2"`. Any valid claim name can be specified except '`iat`' and '`exp`' (these are automatically generated). |
 * | `unsecuredLoginPrincipalClaimName` | Set to a custom claim name if you wish the name of the `String` claim holding the principal name to be something other than '`sub`'. |
 * | `unsecuredLoginLifetimeSeconds` | Set to an integer value if the token expiration is to be set to something other than the default value of 3600 seconds (which is 1 hour). The '`exp`' claim will be set to reflect the expiration time. |
 * | `unsecuredLoginScopeClaimName` | Set to a custom claim name if you wish the name of the `String` or `String List` claim holding any token scope to be something other than '`scope`'. |
 *
 * You can also add custom unsecured SASL extensions when using the default, builtin
 * [AuthenticateCallbackHandler] implementation through using the configurable option
 * `unsecuredLoginExtension_<extensionname>`. Note that there are validations for the key/values in
 * order to conform to the SASL/OAUTHBEARER standard
 * ([RFC 7628](https://tools.ietf.org/html/rfc7628#section-3.1)), including the reserved key at
 * [org.apache.kafka.common.security.oauthbearer.internals.OAuthBearerClientInitialResponse.AUTH_KEY].
 * The `OAuthBearerLoginModule` instance also asks its configured [AuthenticateCallbackHandler]
 * implementation to handle an instance of [SaslExtensionsCallback] and return an instance of
 * [SaslExtensions]. The configured callback handler does not need to handle this callback, though
 * -- any `UnsupportedCallbackException` that is thrown is ignored, and no SASL extensions will be
 * associated with the login.
 *
 * Production use cases will require writing an implementation of [AuthenticateCallbackHandler] that
 * can handle an instance of [OAuthBearerTokenCallback] and declaring it via either the
 * `sasl.login.callback.handler.class` configuration option for a non-broker client or via the
 * `listener.name.sasl_ssl.oauthbearer.sasl.login.callback.handler.class` configuration option for
 * brokers (when SASL/OAUTHBEARER is the inter-broker protocol).
 *
 * This class stores the retrieved [OAuthBearerToken] in the `Subject`'s private credentials where
 * the `SaslClient` can retrieve it. An appropriate, builtin `SaslClient` implementation is
 * automatically used and configured such that it can perform that retrieval.
 *
 * Here is a typical, basic JAAS configuration for a client leveraging unsecured SASL/OAUTHBEARER
 * authentication:
 *
 * ```plaintext
 * KafkaClient {
 *   org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule Required
 *   unsecuredLoginStringClaim_sub="thePrincipalName";
 * };
 * ```
 *
 * An implementation of the [Login] interface specific to the `OAUTHBEARER` mechanism is
 * automatically applied; it periodically refreshes any token before it expires so that the client
 * can continue to make connections to brokers. The parameters that impact how the refresh algorithm
 * operates are specified as part of the producer/consumer/broker configuration and are as follows.
 * See the documentation for these properties elsewhere for details.
 *
 * | Producer/Consumer/Broker Configuration Property |
 * | ----------------------------------------------- |
 * | `sasl.login.refresh.window.factor` |
 * | `sasl.login.refresh.window.jitter` |
 * | `sasl.login.refresh.min.period.seconds` |
 * | `sasl.login.refresh.min.buffer.seconds` |
 *
 * When a broker accepts a SASL/OAUTHBEARER connection the instance of the builtin `SaslServer`
 * implementation asks its configured [AuthenticateCallbackHandler] implementation to handle an
 * instance of [OAuthBearerValidatorCallback] constructed with the OAuth 2 Bearer Token's compact
 * serialization and return an instance of [OAuthBearerToken] if the value validates. A default,
 * builtin [AuthenticateCallbackHandler] implementation validates an unsecured token as defined by
 * these JAAS module options:
 *
 * | JAAS Module Option for Unsecured Token Validation | Documentation |
 * | ------------------------------------------------- | ------------- |
 * | `unsecuredValidatorPrincipalClaimName="value"` | Set to a non-empty value if you wish a particular `String` claim holding a principal name to be checked for existence; the default is to check for the existence of the '`sub`' claim. |
 * | `unsecuredValidatorScopeClaimName="value"` | Set to a custom claim name if you wish the name of the `String` or `String List` claim holding any token scope to be something other than '`scope`'. |
 * | `unsecuredValidatorRequiredScope="value"` | Set to a space-delimited list of scope values if you wish the `String/String List` claim holding the token scope to be checked to make sure it contains certain values. |
 * | `unsecuredValidatorAllowableClockSkewMs="value"` | Set to a positive integer value if you wish to allow up to some number of  positive milliseconds of clock skew (the default is 0). |
 *
 * Here is a typical, basic JAAS configuration for a broker leveraging unsecured SASL/OAUTHBEARER
 * validation:
 *
 * ```plaintext
 * KafkaServer {
 * org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule Required
 * unsecuredLoginStringClaim_sub="thePrincipalName";
 * };
 * ```
 *
 * Production use cases will require writing an implementation of [AuthenticateCallbackHandler] that
 * can handle an instance of [OAuthBearerValidatorCallback] and declaring it via the
 * `listener.name.sasl_ssl.oauthbearer.sasl.server.callback.handler.class` broker configuration
 * option.
 *
 * The builtin `SaslServer` implementation for SASL/OAUTHBEARER in Kafka makes the instance of
 * [OAuthBearerToken] available upon successful authentication via the negotiated property
 * "`OAUTHBEARER.token`"; the token could be used in a custom authorizer (to authorize based on JWT
 * claims rather than ACLs, for example).
 *
 * This implementation's `logout()` method will logout the specific token that this instance logged
 * in if it's `Subject` instance is shared across multiple `LoginContext`s and there happen to be
 * multiple tokens on the `Subject`. This functionality is useful because it means a new token with
 * a longer lifetime can be created before a soon-to-expire token is actually logged out. Otherwise,
 * if multiple simultaneous tokens were not supported like this, the soon-to-be expired token would
 * have to be logged out first, and then if the new token could not be retrieved (maybe the
 * authorization server is temporarily unavailable, for example) the client would be left without a
 * token and would be unable to create new connections. Better to mitigate this possibility by
 * leaving the existing token (which still has some lifetime left) in place until a new replacement
 * token is actually retrieved. This implementation supports this.
 *
 * @see SaslConfigs.SASL_LOGIN_REFRESH_WINDOW_FACTOR_DOC
 * @see SaslConfigs.SASL_LOGIN_REFRESH_WINDOW_JITTER_DOC
 * @see SaslConfigs.SASL_LOGIN_REFRESH_MIN_PERIOD_SECONDS_DOC
 * @see SaslConfigs.SASL_LOGIN_REFRESH_BUFFER_SECONDS_DOC
 */
class OAuthBearerLoginModule : LoginModule {

    private lateinit var subject: Subject

    private lateinit var callbackHandler: AuthenticateCallbackHandler

    private var tokenRequiringCommit: OAuthBearerToken? = null

    private var myCommittedToken: OAuthBearerToken? = null

    private var extensionsRequiringCommit: SaslExtensions? = null

    private var myCommittedExtensions: SaslExtensions? = null

    private var loginState: LoginState? = null

    override fun initialize(
        subject: Subject,
        callbackHandler: CallbackHandler,
        sharedState: Map<String, *>,
        options: Map<String, *>,
    ) {
        this.subject = subject
        require(callbackHandler is AuthenticateCallbackHandler) {
            String.format(
                "Callback handler must be castable to %s: %s",
                AuthenticateCallbackHandler::class.java.name,
                callbackHandler.javaClass.name,
            )
        }
        this.callbackHandler = callbackHandler
    }

    @Throws(LoginException::class)
    override fun login(): Boolean {
        if (loginState == LoginState.LOGGED_IN_NOT_COMMITTED) {
            check(tokenRequiringCommit == null) {
                String.format(
                    "Already have an uncommitted token with private credential token count=%d",
                    committedTokenCount
                )
            }
            error("Already logged in without a token")
        }
        if (loginState == LoginState.COMMITTED) {
            check(myCommittedToken == null) {
                String.format(
                    "Already have a committed token with private credential token count=%d; must " +
                            "login on another login context or logout here first before reusing " +
                            "the same login context",
                    committedTokenCount
                )
            }
            error("Login has already been committed without a token")
        }
        identifyToken()
        if (tokenRequiringCommit != null) identifyExtensions()
        else log.debug("Logged in without a token, this login cannot be used to establish client connections")
        loginState = LoginState.LOGGED_IN_NOT_COMMITTED
        log.debug(
            "Login succeeded; invoke commit() to commit it; current committed token count={}",
            committedTokenCount
        )
        return true
    }

    @Throws(LoginException::class)
    private fun identifyToken() {
        val tokenCallback = OAuthBearerTokenCallback()
        try {
            callbackHandler.handle(arrayOf<Callback>(tokenCallback))
        } catch (e: IOException) {
            log.error(e.message, e)
            throw LoginException("An internal error occurred while retrieving token from callback handler")
        } catch (e: UnsupportedCallbackException) {
            log.error(e.message, e)
            throw LoginException("An internal error occurred while retrieving token from callback handler")
        }
        tokenRequiringCommit = tokenCallback.token

        with(tokenCallback) {
            if (errorCode != null) {
                log.info(
                    "Login failed: {} : {} (URI={})",
                    errorCode,
                    errorDescription,
                    errorUri
                )
                throw LoginException(errorDescription)
            }
        }
    }

    /**
     * Attaches SASL extensions to the Subject
     */
    @Throws(LoginException::class)
    private fun identifyExtensions() {
        val extensionsCallback = SaslExtensionsCallback()

        try {
            callbackHandler.handle(arrayOf<Callback>(extensionsCallback))
            extensionsRequiringCommit = extensionsCallback.extensions
        } catch (e: IOException) {
            log.error(e.message, e)
            throw LoginException(
                "An internal error occurred while retrieving SASL extensions from callback handler"
            )
        } catch (e: UnsupportedCallbackException) {
            extensionsRequiringCommit = EMPTY_EXTENSIONS
            log.debug(
                "CallbackHandler {} does not support SASL extensions. No extensions will be added",
                callbackHandler.javaClass.name
            )
        }
        if (extensionsRequiringCommit == null) {
            log.error(
                "SASL Extensions cannot be null. Check whether your callback handler is " +
                        "explicitly setting them as null."
            )
            throw LoginException("Extensions cannot be null.")
        }
    }

    override fun logout(): Boolean {
        check(loginState != LoginState.LOGGED_IN_NOT_COMMITTED) {
            "Cannot call logout() immediately after login(); need to first invoke commit() or abort()"
        }

        if (loginState != LoginState.COMMITTED) {
            log.debug("Nothing here to log out")
            return false
        }

        if (myCommittedToken != null) {
            log.trace(
                "Logging out my token; current committed token count = {}",
                committedTokenCount
            )
            val iterator = subject.privateCredentials.iterator()
            while (iterator.hasNext()) {
                val privateCredential = iterator.next()
                if (privateCredential === myCommittedToken) {
                    iterator.remove()
                    myCommittedToken = null
                    break
                }
            }
            log.debug(
                "Done logging out my token; committed token count is now {}",
                committedTokenCount
            )
        } else log.debug("No tokens to logout for this login")

        if (myCommittedExtensions != null) {
            log.trace("Logging out my extensions")

            if (subject.publicCredentials.removeIf { e -> myCommittedExtensions === e })
                myCommittedExtensions = null

            log.debug("Done logging out my extensions")
        } else log.debug("No extensions to logout for this login")

        loginState = LoginState.NOT_LOGGED_IN
        return true
    }

    override fun commit(): Boolean {
        if (loginState != LoginState.LOGGED_IN_NOT_COMMITTED) {
            log.debug("Nothing here to commit")
            return false
        }

        if (tokenRequiringCommit != null) {
            log.trace(
                "Committing my token; current committed token count = {}",
                committedTokenCount
            )
            subject.privateCredentials.add(tokenRequiringCommit)
            myCommittedToken = tokenRequiringCommit
            tokenRequiringCommit = null
            log.debug(
                "Done committing my token; committed token count is now {}",
                committedTokenCount
            )
        } else log.debug("No tokens to commit, this login cannot be used to establish client connections")
        if (extensionsRequiringCommit != null) {
            subject.publicCredentials.add(extensionsRequiringCommit)
            myCommittedExtensions = extensionsRequiringCommit
            extensionsRequiringCommit = null
        }

        loginState = LoginState.COMMITTED
        return true
    }

    override fun abort(): Boolean {
        if (loginState == LoginState.LOGGED_IN_NOT_COMMITTED) {
            log.debug("Login aborted")
            tokenRequiringCommit = null
            extensionsRequiringCommit = null
            loginState = LoginState.NOT_LOGGED_IN
            return true
        }

        log.debug("Nothing here to abort")
        return false
    }

    private val committedTokenCount: Int
        get() = subject.getPrivateCredentials(OAuthBearerToken::class.java).size

    /**
     * Login state transitions:
     *
     * | Action          | Transition                                   |
     * | --------------- | -------------------------------------------- |
     * | (Initial state) | `NOT_LOGGED_IN`                              |
     * | [login]         | `NOT_LOGGED_IN` => `LOGGED_IN_NOT_COMMITTED` |
     * | [commit]        | `LOGGED_IN_NOT_COMMITTED` => `COMMITTED`     |
     * | [abort]         | `LOGGED_IN_NOT_COMMITTED` => `NOT_LOGGED_IN` |
     * | [logout]        | (Any state) => `NOT_LOGGED_IN`               |
     */
    private enum class LoginState {
        NOT_LOGGED_IN,
        LOGGED_IN_NOT_COMMITTED,
        COMMITTED
    }

    companion object {

        private val log = LoggerFactory.getLogger(OAuthBearerLoginModule::class.java)

        /**
         * The SASL Mechanism name for OAuth 2: `OAUTHBEARER`
         */
        const val OAUTHBEARER_MECHANISM = "OAUTHBEARER"

        private val EMPTY_EXTENSIONS = SaslExtensions(emptyMap())

        init {
            OAuthBearerSaslClientProvider.initialize() // not part of public API
            OAuthBearerSaslServerProvider.initialize() // not part of public API
        }
    }
}
