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

import javax.security.auth.callback.Callback
import javax.security.auth.callback.NameCallback
import javax.security.auth.callback.PasswordCallback
import javax.security.auth.login.AppConfigurationEntry
import javax.security.sasl.AuthorizeCallback
import javax.security.sasl.RealmCallback
import org.apache.kafka.common.security.auth.AuthenticateCallbackHandler
import org.apache.kafka.common.security.plain.PlainLoginModule

/**
 * Digest-MD5 login module for multi-mechanism tests.
 * This login module uses the same format as PlainLoginModule and hence simply reuses the same methods.
 *
 */
class TestDigestLoginModule : PlainLoginModule() {

    class DigestServerCallbackHandler : AuthenticateCallbackHandler {

        override fun configure(
            configs: Map<String, *>,
            saslMechanism: String,
            jaasConfigEntries: List<AppConfigurationEntry>
        ) = Unit

        override fun handle(callbacks: Array<Callback>) {
            var username: String? = null
            for (callback in callbacks) {
                when (callback) {
                    is NameCallback -> if (TestJaasConfig.USERNAME == callback.defaultName) {
                        callback.name = callback.defaultName
                        username = TestJaasConfig.USERNAME
                    }

                    is PasswordCallback -> if (TestJaasConfig.USERNAME == username)
                        callback.password = TestJaasConfig.PASSWORD.toCharArray()

                    is RealmCallback -> callback.text = callback.defaultText
                    is AuthorizeCallback -> if (TestJaasConfig.USERNAME == callback.authenticationID) {
                        callback.isAuthorized = true
                        callback.authorizedID = callback.authenticationID
                    }
                }
            }
        }

        override fun close() = Unit
    }
}
