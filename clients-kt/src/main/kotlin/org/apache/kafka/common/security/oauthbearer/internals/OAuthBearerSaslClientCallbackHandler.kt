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

import java.io.IOException
import java.security.AccessController
import java.util.*
import javax.security.auth.Subject
import javax.security.auth.callback.Callback
import javax.security.auth.callback.UnsupportedCallbackException
import javax.security.auth.login.AppConfigurationEntry
import org.apache.kafka.common.security.auth.AuthenticateCallbackHandler
import org.apache.kafka.common.security.auth.SaslExtensions
import org.apache.kafka.common.security.auth.SaslExtensionsCallback
import org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule
import org.apache.kafka.common.security.oauthbearer.OAuthBearerToken
import org.apache.kafka.common.security.oauthbearer.OAuthBearerTokenCallback
import org.slf4j.LoggerFactory

/**
 * An implementation of `AuthenticateCallbackHandler` that recognizes [OAuthBearerTokenCallback] and
 * retrieves OAuth 2 Bearer Token that was created when the `OAuthBearerLoginModule` logged in by
 * looking for an instance of [OAuthBearerToken] in the `Subject`'s private credentials. This class
 * also recognizes [SaslExtensionsCallback] and retrieves any SASL extensions that were created when
 * the `OAuthBearerLoginModule` logged in by looking for an instance of [SaslExtensions] in the
 * `Subject`'s public credentials.
 *
 * Use of this class is configured automatically and does not need to be explicitly set via the
 * `sasl.client.callback.handler.class` configuration property.
 */
class OAuthBearerSaslClientCallbackHandler : AuthenticateCallbackHandler {

    var configured = false

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
        configured = true
    }

    @Throws(IOException::class, UnsupportedCallbackException::class)
    override fun handle(callbacks: Array<Callback>) {
        check(configured) { "Callback handler not configured" }
        callbacks.forEach { callback ->
            when (callback) {
                is OAuthBearerTokenCallback -> handleCallback(callback)
                is SaslExtensionsCallback -> handleCallback(
                    callback,
                    Subject.getSubject(AccessController.getContext()),
                )

                else -> throw UnsupportedCallbackException(callback)
            }
        }
    }

    override fun close() = Unit

    @Throws(IOException::class)
    private fun handleCallback(callback: OAuthBearerTokenCallback) {
        require(callback.token == null) { "Callback had a token already" }

        val subject = Subject.getSubject(AccessController.getContext())
        val privateCredentials =
            if (subject != null) subject.getPrivateCredentials(OAuthBearerToken::class.java)
            else emptySet()

        if (privateCredentials.isEmpty()) throw IOException(
            "No OAuth Bearer tokens in Subject's private credentials"
        )

        if (privateCredentials.size == 1) callback.token = privateCredentials.first()
        else {
            /*
             * There a very small window of time upon token refresh (on the order of milliseconds)
             * where both an old and a new token appear on the Subject's private credentials.
             * Rather than implement a lock to eliminate this window, we will deal with it by
             * checking for the existence of multiple tokens and choosing the one that has the
             * longest lifetime. It is also possible that a bug could cause multiple tokens to
             * exist (e.g. KAFKA-7902), so dealing with the unlikely possibility that occurs
             * during normal operation also allows us to deal more robustly with potential bugs.
             */
            val sortedByLifetime: SortedSet<OAuthBearerToken> = TreeSet { o1, o2 ->
                o1.lifetimeMs().compareTo(o2.lifetimeMs())
            }

            sortedByLifetime.addAll(privateCredentials)
            log.warn(
                "Found {} OAuth Bearer tokens in Subject's private credentials; the oldest " +
                        "expires at {}, will use the newest, which expires at {}",
                sortedByLifetime.size,
                Date(sortedByLifetime.first().lifetimeMs()),
                Date(sortedByLifetime.last().lifetimeMs())
            )
            callback.token = sortedByLifetime.last()
        }
    }

    companion object {

        private val log = LoggerFactory.getLogger(OAuthBearerSaslClientCallbackHandler::class.java)

        /**
         * Attaches the first [SaslExtensions] found in the public credentials of the Subject
         */
        private fun handleCallback(extensionsCallback: SaslExtensionsCallback, subject: Subject?) {
            if (
                subject != null
                && subject.getPublicCredentials(SaslExtensions::class.java).isNotEmpty()
            ) extensionsCallback.extensions =
                subject.getPublicCredentials(SaslExtensions::class.java).first()
        }
    }
}
