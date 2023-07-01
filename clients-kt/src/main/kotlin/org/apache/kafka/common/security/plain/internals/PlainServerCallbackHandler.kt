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

package org.apache.kafka.common.security.plain.internals

import java.io.IOException
import javax.security.auth.callback.Callback
import javax.security.auth.callback.NameCallback
import javax.security.auth.callback.UnsupportedCallbackException
import javax.security.auth.login.AppConfigurationEntry
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.security.JaasContext
import org.apache.kafka.common.security.auth.AuthenticateCallbackHandler
import org.apache.kafka.common.security.plain.PlainAuthenticateCallback
import org.apache.kafka.common.security.plain.PlainLoginModule
import org.apache.kafka.common.utils.Utils.isEqualConstantTime


open class PlainServerCallbackHandler : AuthenticateCallbackHandler {

    private lateinit var jaasConfigEntries: List<AppConfigurationEntry>

    override fun configure(
        configs: Map<String, *>,
        saslMechanism: String,
        jaasConfigEntries: List<AppConfigurationEntry>
    ) {
        this.jaasConfigEntries = jaasConfigEntries
    }

    @Throws(IOException::class, UnsupportedCallbackException::class)
    override fun handle(callbacks: Array<Callback>) {
        var username: String? = null

        callbacks.forEach { callback ->
            when (callback) {
                is NameCallback -> username = callback.defaultName

                is PlainAuthenticateCallback -> {
                    val authenticated = authenticate(username, callback.password)
                    callback.authenticated = authenticated
                }

                else -> throw UnsupportedCallbackException(callback)
            }
        }
    }

    @Throws(IOException::class)
    protected open fun authenticate(username: String?, password: CharArray?): Boolean {
        return if (username == null) false
        else {
            val expectedPassword = JaasContext.configEntryOption(
                configurationEntries = jaasConfigEntries,
                key = JAAS_USER_PREFIX + username,
                loginModuleName = PlainLoginModule::class.java.name
            )
            expectedPassword != null && isEqualConstantTime(
                first = password,
                second = expectedPassword.toCharArray()
            )
        }
    }

    @Throws(KafkaException::class)
    override fun close() = Unit

    companion object {
        private const val JAAS_USER_PREFIX = "user_"
    }
}
