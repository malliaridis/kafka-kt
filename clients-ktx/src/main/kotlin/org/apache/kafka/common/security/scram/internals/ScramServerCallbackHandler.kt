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

package org.apache.kafka.common.security.scram.internals

import javax.security.auth.callback.Callback
import javax.security.auth.callback.NameCallback
import javax.security.auth.callback.UnsupportedCallbackException
import javax.security.auth.login.AppConfigurationEntry
import org.apache.kafka.common.security.auth.AuthenticateCallbackHandler
import org.apache.kafka.common.security.authenticator.CredentialCache
import org.apache.kafka.common.security.scram.ScramCredential
import org.apache.kafka.common.security.scram.ScramCredentialCallback
import org.apache.kafka.common.security.token.delegation.internals.DelegationTokenCache
import org.apache.kafka.common.security.token.delegation.internals.DelegationTokenCredentialCallback

class ScramServerCallbackHandler(
    private val credentialCache: CredentialCache.Cache<ScramCredential>,
    private val tokenCache: DelegationTokenCache,
) : AuthenticateCallbackHandler {

    private lateinit var saslMechanism: String

    override fun configure(
        configs: Map<String, *>,
        saslMechanism: String,
        jaasConfigEntries: List<AppConfigurationEntry>
    ) {
        this.saslMechanism = saslMechanism
    }

    @Throws(UnsupportedCallbackException::class)
    override fun handle(callbacks: Array<Callback>) {
        lateinit var username: String

        callbacks.forEach { callback ->

            if (callback is NameCallback) username = callback.defaultName
            else if (callback is DelegationTokenCredentialCallback) {

                callback.scramCredential = tokenCache.credential(saslMechanism, username)
                callback.tokenOwner = tokenCache.owner(username)
                val tokenInfo = tokenCache.token(username)

                if (tokenInfo != null) callback.tokenExpiryTimestamp = tokenInfo.expiryTimestamp
            } else if (callback is ScramCredentialCallback) {
                callback.scramCredential = credentialCache[username]
            } else throw UnsupportedCallbackException(callback)
        }
    }

    override fun close() = Unit
}
