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

import javax.security.auth.Subject
import javax.security.auth.callback.Callback
import javax.security.auth.callback.NameCallback
import javax.security.auth.callback.PasswordCallback
import javax.security.auth.callback.UnsupportedCallbackException
import javax.security.auth.login.AppConfigurationEntry
import javax.security.auth.login.Configuration
import javax.security.auth.login.LoginContext
import javax.security.auth.login.LoginException
import javax.security.sasl.RealmCallback
import org.apache.kafka.common.security.auth.AuthenticateCallbackHandler
import org.apache.kafka.common.security.auth.Login
import org.slf4j.LoggerFactory

/**
 * Base login class that implements methods common to typical SASL mechanisms.
 */
abstract class AbstractLogin : Login {

    private var contextName: String? = null

    private var configuration: Configuration? = null

    private lateinit var loginContext: LoginContext

    private lateinit var loginCallbackHandler: AuthenticateCallbackHandler

    override fun configure(
        configs: Map<String, *>,
        contextName: String,
        jaasConfiguration: Configuration,
        loginCallbackHandler: AuthenticateCallbackHandler
    ) {
        this.contextName = contextName
        this.configuration = jaasConfiguration
        this.loginCallbackHandler = loginCallbackHandler
    }

    @Throws(LoginException::class)
    override fun login(): LoginContext {
        loginContext = LoginContext(contextName, null, loginCallbackHandler, configuration)
        loginContext.login()
        log.info("Successfully logged in.")
        return loginContext
    }

    override fun subject(): Subject? = loginContext.subject

    protected fun contextName(): String? = contextName

    protected fun configuration(): Configuration? = configuration

    /**
     * Callback handler for creating login context. Login callback handlers
     * should support the callbacks required for the login modules used by
     * the KafkaServer and KafkaClient contexts. Kafka does not support
     * callback handlers which require additional user input.
     */
    class DefaultLoginCallbackHandler : AuthenticateCallbackHandler {

        override fun configure(
            configs: Map<String, *>,
            saslMechanism: String,
            jaasConfigEntries: List<AppConfigurationEntry>
        ) = Unit

        @Throws(UnsupportedCallbackException::class)
        override fun handle(callbacks: Array<Callback>) {
            for (callback in callbacks) {
                when (callback) {
                    is NameCallback -> callback.name = callback.defaultName
                    is RealmCallback -> callback.text = callback.defaultText

                    is PasswordCallback -> throw UnsupportedCallbackException(
                        callback,
                        "Could not login: the client is being asked for a password, but the " +
                                "Kafka client code does not currently support obtaining a " +
                                "password from the user."
                    )

                    else -> throw UnsupportedCallbackException(callback, "Unrecognized SASL Login callback")
                }
            }
        }

        override fun close() = Unit
    }

    companion object {
        private val log = LoggerFactory.getLogger(AbstractLogin::class.java)
    }
}
