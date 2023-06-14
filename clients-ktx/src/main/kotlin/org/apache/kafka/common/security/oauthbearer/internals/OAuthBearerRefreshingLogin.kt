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

import javax.security.auth.Subject
import javax.security.auth.login.Configuration
import javax.security.auth.login.LoginContext
import javax.security.auth.login.LoginException
import org.apache.kafka.common.security.auth.AuthenticateCallbackHandler
import org.apache.kafka.common.security.auth.Login
import org.apache.kafka.common.security.oauthbearer.OAuthBearerToken
import org.apache.kafka.common.security.oauthbearer.internals.expiring.ExpiringCredential
import org.apache.kafka.common.security.oauthbearer.internals.expiring.ExpiringCredentialRefreshConfig
import org.apache.kafka.common.security.oauthbearer.internals.expiring.ExpiringCredentialRefreshingLogin
import org.slf4j.LoggerFactory

/**
 * This class is responsible for refreshing logins for both Kafka client and server when the
 * credential is an OAuth 2 bearer token communicated over SASL/OAUTHBEARER. An OAuth 2 bearer token
 * has a limited lifetime, and an instance of this class periodically refreshes it so that the
 * client can create new connections to brokers on an ongoing basis.
 *
 * This class does not need to be explicitly set via the `sasl.login.class` client configuration
 * property or the `listener.name.sasl_[plaintext|ssl].oauthbearer.sasl.login.class` broker
 * configuration property when the SASL mechanism is OAUTHBEARER; it is automatically set by default
 * in that case.
 *
 * The parameters that impact how the refresh algorithm operates are specified as part of the
 * producer/consumer/broker configuration and are as follows. See the documentation for these
 * properties elsewhere for details.
 *
 * | Producer/Consumer/Broker Configuration Property |
 * | ----------------------------------------------- |
 * | `sasl.login.refresh.window.factor` |
 * | `sasl.login.refresh.window.jitter` |
 * | `sasl.login.refresh.min.period.seconds` |
 * | `sasl.login.refresh.min.buffer.seconds` |
 *
 * @see org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule
 * @see SaslConfigs.SASL_LOGIN_REFRESH_WINDOW_FACTOR_DOC
 * @see SaslConfigs.SASL_LOGIN_REFRESH_WINDOW_JITTER_DOC
 * @see SaslConfigs.SASL_LOGIN_REFRESH_MIN_PERIOD_SECONDS_DOC
 * @see SaslConfigs.SASL_LOGIN_REFRESH_BUFFER_SECONDS_DOC
 */
class OAuthBearerRefreshingLogin : Login {

    private var expiringCredentialRefreshingLogin: ExpiringCredentialRefreshingLogin? = null

    override fun configure(
        configs: Map<String, *>,
        contextName: String,
        jaasConfiguration: Configuration,
        loginCallbackHandler: AuthenticateCallbackHandler,
    ) {
        /*
         * Specify this class as the one to synchronize on so that only one OAuth 2
         * Bearer Token is refreshed at a given time. Specify null if we don't mind
         * multiple simultaneously refreshes. Refreshes happen on the order of minutes
         * rather than seconds or milliseconds, and there are typically minutes of
         * lifetime remaining when the refresh occurs, so serializing them seems
         * reasonable.
         */
        val classToSynchronizeOnPriorToRefresh = OAuthBearerRefreshingLogin::class.java
        expiringCredentialRefreshingLogin = object : ExpiringCredentialRefreshingLogin(
            contextName, jaasConfiguration,
            ExpiringCredentialRefreshConfig(configs, true), loginCallbackHandler,
            classToSynchronizeOnPriorToRefresh
        ) {
            override fun expiringCredential(): ExpiringCredential? {
                val privateCredentialTokens: Set<OAuthBearerToken> =
                    expiringCredentialRefreshingLogin!!.subject!!
                        .getPrivateCredentials(OAuthBearerToken::class.java)

                if (privateCredentialTokens.isEmpty()) return null

                val token = privateCredentialTokens.iterator().next()
                if (log.isDebugEnabled) log.debug(
                    "Found expiring credential with principal '{}'.",
                    token.principalName()
                )

                return object : ExpiringCredential {
                    override fun principalName(): String = token.principalName()
                    override fun startTimeMs(): Long? = token.startTimeMs()
                    override fun expireTimeMs(): Long = token.lifetimeMs()
                    override fun absoluteLastRefreshTimeMs(): Long? = null
                }
            }
        }
    }

    override fun close() {
        expiringCredentialRefreshingLogin?.close()
    }

    override fun subject(): Subject? = expiringCredentialRefreshingLogin?.subject

    override fun serviceName(): String? = expiringCredentialRefreshingLogin?.serviceName

    @Synchronized
    @Throws(LoginException::class)
    override fun login(): LoginContext {
        return expiringCredentialRefreshingLogin?.login()
            ?: throw LoginException("Login was not configured properly")
    }

    companion object {
        private val log = LoggerFactory.getLogger(OAuthBearerRefreshingLogin::class.java)
    }
}
