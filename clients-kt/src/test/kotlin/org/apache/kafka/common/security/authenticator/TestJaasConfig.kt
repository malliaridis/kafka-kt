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

import javax.security.auth.login.AppConfigurationEntry
import javax.security.auth.login.AppConfigurationEntry.LoginModuleControlFlag
import javax.security.auth.login.Configuration
import org.apache.kafka.common.config.types.Password
import org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule
import org.apache.kafka.common.security.plain.PlainLoginModule
import org.apache.kafka.common.security.scram.ScramLoginModule
import org.apache.kafka.common.security.scram.internals.ScramMechanism

class TestJaasConfig : Configuration() {

    private val entryMap = mutableMapOf<String, Array<AppConfigurationEntry?>>()

    fun setClientOptions(saslMechanism: String, clientUsername: String?, clientPassword: String?) {
        val options = mutableMapOf<String, Any?>()
        if (clientUsername != null) options["username"] = clientUsername
        if (clientPassword != null) options["password"] = clientPassword
        val loginModuleClass =
            if (ScramMechanism.isScram(saslMechanism)) ScramLoginModule::class.java
            else PlainLoginModule::class.java
        createOrUpdateEntry(
            name = LOGIN_CONTEXT_CLIENT,
            loginModule = loginModuleClass.getName(),
            options = options,
        )
    }

    fun createOrUpdateEntry(name: String, loginModule: String?, options: Map<String, Any?>?) {
        val entry = AppConfigurationEntry(loginModule, LoginModuleControlFlag.REQUIRED, options)
        entryMap[name] = arrayOf(entry)
    }

    fun addEntry(name: String, loginModule: String?, options: Map<String?, Any?>?) {
        val entry = AppConfigurationEntry(loginModule, LoginModuleControlFlag.REQUIRED, options)
        val existing = entryMap[name]
        val newEntries = existing?.copyOf(existing.size + 1) ?: arrayOfNulls(1)
        newEntries[newEntries.size - 1] = entry
        entryMap[name] = newEntries
    }

    override fun getAppConfigurationEntry(name: String): Array<AppConfigurationEntry?>? = entryMap[name]

    companion object {

        const val LOGIN_CONTEXT_CLIENT = "KafkaClient"

        const val LOGIN_CONTEXT_SERVER = "KafkaServer"

        const val USERNAME = "myuser"

        const val PASSWORD = "mypassword"

        fun createConfiguration(clientMechanism: String, serverMechanisms: List<String>): TestJaasConfig {
            val config = TestJaasConfig()
            config.createOrUpdateEntry(
                name = LOGIN_CONTEXT_CLIENT,
                loginModule = loginModule(clientMechanism),
                options = defaultClientOptions(clientMechanism),
            )
            for (mechanism in serverMechanisms) {
                config.addEntry(
                    name = LOGIN_CONTEXT_SERVER,
                    loginModule = loginModule(mechanism),
                    options = defaultServerOptions(mechanism),
                )
            }
            setConfiguration(config)
            return config
        }

        fun jaasConfigProperty(mechanism: String, username: String, password: String): Password {
            return Password("${loginModule(mechanism)} required username=$username password=$password;")
        }

        fun jaasConfigProperty(mechanism: String, options: Map<String?, Any?>): Password {
            val builder = StringBuilder()
            builder.append(loginModule(mechanism))
            builder.append(" required")
            for ((key, value) in options) {
                builder.append(' ')
                builder.append(key)
                builder.append('=')
                builder.append(value)
            }
            builder.append(';')
            return Password(builder.toString())
        }

        private fun loginModule(mechanism: String): String = when (mechanism) {
            "PLAIN" -> PlainLoginModule::class.java.getName()
            "DIGEST-MD5" -> TestDigestLoginModule::class.java.getName()
            "OAUTHBEARER" -> OAuthBearerLoginModule::class.java.getName()
            else -> {
                require(ScramMechanism.isScram(mechanism)) { "Unsupported mechanism $mechanism" }
                ScramLoginModule::class.java.getName()
            }
        }

        fun defaultClientOptions(mechanism: String?): Map<String, Any?> {
            return when (mechanism) {
                "OAUTHBEARER" -> mapOf("unsecuredLoginStringClaim_sub" to USERNAME)
                else -> defaultClientOptions()
            }
        }

        fun defaultClientOptions(): Map<String, Any?> = mapOf(
            "username" to USERNAME,
            "password" to PASSWORD,
        )

        fun defaultServerOptions(mechanism: String): Map<String?, Any?> {
            val options = mutableMapOf<String?, Any?>()
            when (mechanism) {
                "PLAIN", "DIGEST-MD5" -> options["user_$USERNAME"] = PASSWORD
                "OAUTHBEARER" -> options["unsecuredLoginStringClaim_sub"] = USERNAME
                else -> require(ScramMechanism.isScram(mechanism)) { "Unsupported mechanism $mechanism" }
            }
            return options
        }
    }
}
