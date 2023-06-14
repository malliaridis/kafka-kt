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

package org.apache.kafka.common.security.kerberors

import javax.security.auth.callback.Callback
import javax.security.auth.callback.NameCallback
import javax.security.auth.callback.PasswordCallback
import javax.security.auth.callback.UnsupportedCallbackException
import javax.security.auth.login.AppConfigurationEntry
import javax.security.sasl.AuthorizeCallback
import javax.security.sasl.RealmCallback
import org.apache.kafka.common.config.SaslConfigs
import org.apache.kafka.common.security.auth.AuthenticateCallbackHandler

/**
 * Callback handler for SASL/GSSAPI clients.
 */
class KerberosClientCallbackHandler : AuthenticateCallbackHandler {

    override fun configure(
        configs: Map<String, *>,
        saslMechanism: String,
        jaasConfigEntries: List<AppConfigurationEntry>
    ) = check(saslMechanism == SaslConfigs.GSSAPI_MECHANISM) {
        "Kerberos callback handler should only be used with GSSAPI"
    }

    @Throws(UnsupportedCallbackException::class)
    override fun handle(callbacks: Array<Callback>) {
        callbacks.forEach { callback ->
            when (callback) {
                is NameCallback -> callback.name = callback.defaultName
                is PasswordCallback -> throw UnsupportedCallbackException(
                    callback,
                    "Could not login: the client is being asked for a password, but the Kafka " +
                            "client code does not currently support obtaining a password from " +
                            "the user. Make sure -Djava.security.auth.login.config property passed " +
                            "to JVM and the client is configured to use a ticket cache (using " +
                            "the JAAS configuration setting 'useTicketCache=true)'. Make sure " +
                            "you are using FQDN of the Kafka broker you are trying to connect to."
                )

                is RealmCallback -> callback.text = callback.defaultText
                is AuthorizeCallback -> callback.apply {
                    isAuthorized = authenticationID == authorizationID
                    if (isAuthorized) authorizedID = authorizationID
                }

                else -> throw UnsupportedCallbackException(
                    callback,
                    "Unrecognized SASL ClientCallback",
                )
            }
        }
    }

    override fun close() = Unit
}
