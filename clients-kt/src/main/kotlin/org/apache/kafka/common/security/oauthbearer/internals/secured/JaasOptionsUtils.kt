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

package org.apache.kafka.common.security.oauthbearer.internals.secured

import java.net.URL
import java.util.*
import javax.net.ssl.SSLSocketFactory
import javax.security.auth.login.AppConfigurationEntry
import org.apache.kafka.common.config.AbstractConfig
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.common.network.Mode
import org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule
import org.apache.kafka.common.security.ssl.DefaultSslEngineFactory
import org.apache.kafka.common.security.ssl.SslFactory
import org.slf4j.LoggerFactory

/**
 * `JaasOptionsUtils` is a utility class to perform logic for the JAAS options and
 * is separated out here for easier, more direct testing.
 */
class JaasOptionsUtils(private val options: Map<String, Any>) {

    fun shouldCreateSSLSocketFactory(url: URL): Boolean =
        url.protocol.equals("https", ignoreCase = true)

    val sslClientConfig: Map<String, *>
        get() {
            val sslConfigDef = ConfigDef()
            sslConfigDef.withClientSslSupport()
            val sslClientConfig = AbstractConfig(sslConfigDef, options)
            return sslClientConfig.values()
        }

    fun createSSLSocketFactory(): SSLSocketFactory {
        val sslClientConfig = sslClientConfig
        val sslFactory = SslFactory(Mode.CLIENT)
        sslFactory.configure(sslClientConfig)
        val socketFactory =
            (sslFactory.sslEngineFactory as DefaultSslEngineFactory).sslContext()!!.socketFactory
        log.debug("Created SSLSocketFactory: {}", sslClientConfig)
        return socketFactory
    }

    @Throws(ValidateException::class)
    @Deprecated("Replace with validateOptionalString or validateRequiredString")
    fun validateString(name: String, isRequired: Boolean = true): String? {
        var value = options[name] as String?
            ?: return if (isRequired) throw ConfigException(
                "The OAuth configuration option $name value must be non-null"
            ) else null
        value = value.trim { it <= ' ' }
        return value.ifEmpty {
            if (isRequired) throw ConfigException(
                "The OAuth configuration option $name value must not contain only whitespace"
            ) else null
        }
    }

    @Throws(ValidateException::class)
    fun validateRequiredString(name: String): String {
        var value = options[name] as String? ?: throw ConfigException(
            "The OAuth configuration option $name value must be non-null"
        )
        value = value.trim { it <= ' ' }
        return value.ifEmpty {
            throw ConfigException(
                "The OAuth configuration option $name value must not contain only whitespace"
            )
        }
    }

    fun validateOptionalString(name: String): String? {
        return (options[name] as String?)
            ?.trim { it <= ' ' }
            ?.ifEmpty { null }
    }

    companion object {

        private val log = LoggerFactory.getLogger(JaasOptionsUtils::class.java)

        fun getOptions(
            saslMechanism: String,
            jaasConfigEntries: List<AppConfigurationEntry>
        ): Map<String, Any> {
            require(OAuthBearerLoginModule.OAUTHBEARER_MECHANISM == saslMechanism) {
                "Unexpected SASL mechanism: $saslMechanism"
            }
            require(jaasConfigEntries.size == 1) {
                String.format(
                    "Must supply exactly 1 non-null JAAS mechanism configuration (size was %d)",
                    jaasConfigEntries.size
                )
            }
            return jaasConfigEntries[0].options as Map<String, Any>
        }
    }
}
