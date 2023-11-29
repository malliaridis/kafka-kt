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

package org.apache.kafka.common.security.authenticator

import java.security.AccessController
import javax.security.auth.Subject
import javax.security.auth.callback.Callback
import javax.security.auth.callback.NameCallback
import javax.security.auth.callback.PasswordCallback
import javax.security.auth.callback.UnsupportedCallbackException
import javax.security.auth.login.AppConfigurationEntry
import javax.security.sasl.AuthorizeCallback
import javax.security.sasl.RealmCallback
import org.apache.kafka.common.config.SaslConfigs
import org.apache.kafka.common.security.auth.AuthenticateCallbackHandler
import org.apache.kafka.common.security.auth.SaslExtensions
import org.apache.kafka.common.security.auth.SaslExtensionsCallback
import org.apache.kafka.common.security.scram.ScramExtensionsCallback
import org.apache.kafka.common.security.scram.internals.ScramMechanism

/**
 * Default callback handler for Sasl clients. The callbacks required for the SASL mechanism
 * configured for the client should be supported by this callback handler. See
 * [Java SASL API](https://docs.oracle.com/javase/8/docs/technotes/guides/security/sasl/sasl-refguide.html)
 * for the list of SASL callback handlers required for each SASL mechanism.
 *
 * For adding custom SASL extensions, a [SaslExtensions] may be added to the subject's public
 * credentials
 */
class SaslClientCallbackHandler : AuthenticateCallbackHandler {

    private lateinit var mechanism: String

    override fun configure(
        configs: Map<String, *>,
        saslMechanism: String,
        jaasConfigEntries: List<AppConfigurationEntry>
    ) {
        this.mechanism = saslMechanism
    }

    @Throws(UnsupportedCallbackException::class)
    override fun handle(callbacks: Array<Callback>) {
        val subject = Subject.getSubject(AccessController.getContext())

        callbacks.forEach { callback ->
            when (callback) {
                is NameCallback -> {
                    callback.name = if (
                        subject != null
                        && subject.getPublicCredentials(String::class.java).isNotEmpty()
                    ) subject.getPublicCredentials(String::class.java).first()
                    else callback.defaultName
                }

                is PasswordCallback -> {
                    if (
                        subject != null
                        && subject.getPrivateCredentials(String::class.java).isNotEmpty()
                    ) {
                        val password = subject.getPrivateCredentials(String::class.java)
                            .first().toCharArray()

                        callback.password = password
                    } else throw UnsupportedCallbackException(
                        callback,
                        "Could not login: the client is being asked for a password, but the " +
                                "Kafka client code does not currently support obtaining a password " +
                                "from the user.",
                    )
                }

                is RealmCallback -> callback.text = callback.defaultText

                is AuthorizeCallback -> {
                    val authId = callback.authenticationID
                    val authZId = callback.authorizationID
                    callback.isAuthorized = authId == authZId
                    if (callback.isAuthorized) callback.authorizedID = authZId
                }

                is ScramExtensionsCallback -> {
                    if (
                        ScramMechanism.isScram(mechanism)
                        && subject != null
                        && subject.getPublicCredentials(Map::class.java).isNotEmpty()
                    ) {
                        val extensions = subject.getPublicCredentials(Map::class.java)
                            .first() as Map<String, String>

                        callback.extensions = extensions
                    }
                }

                is SaslExtensionsCallback -> {
                    if (
                        SaslConfigs.GSSAPI_MECHANISM != mechanism
                        && subject != null
                        && subject.getPublicCredentials(SaslExtensions::class.java).isNotEmpty()
                    ) {
                        val extensions =
                            subject.getPublicCredentials(SaslExtensions::class.java).first()

                        callback.extensions = extensions
                    }
                }

                else -> throw UnsupportedCallbackException(callback, "Unrecognized SASL ClientCallback")
            }
        }
    }

    override fun close() = Unit
}
