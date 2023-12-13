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
import javax.security.auth.login.LoginException
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.common.config.SaslConfigs
import org.apache.kafka.common.config.types.Password
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.security.JaasContext
import org.apache.kafka.common.security.auth.AuthenticateCallbackHandler
import org.apache.kafka.common.security.auth.Login
import org.apache.kafka.common.security.authenticator.AbstractLogin.DefaultLoginCallbackHandler
import org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule
import org.apache.kafka.common.security.oauthbearer.internals.unsecured.OAuthBearerUnsecuredLoginCallbackHandler
import org.apache.kafka.common.utils.SecurityUtils
import org.apache.kafka.common.utils.Utils.newInstance
import org.slf4j.LoggerFactory

class LoginManager private constructor(
    jaasContext: JaasContext,
    saslMechanism: String,
    configs: Map<String, *>,
    private val loginMetadata: LoginMetadata<*>,
) {

    private val login: Login = newInstance(loginMetadata.loginClass)

    private val loginCallbackHandler = newInstance(loginMetadata.loginCallbackClass)

    private var refCount = 0

    init {
        loginCallbackHandler.configure(configs, saslMechanism, jaasContext.configurationEntries)
        login.configure(
            configs,
            jaasContext.name,
            jaasContext.configuration,
            loginCallbackHandler
        )
        login.login()
    }

    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("subject")
    )
    fun subject(): Subject? = login.subject()

    val subject: Subject?
        get() = login.subject()

    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("serviceName")
    )
    fun serviceName(): String? = login.serviceName()

    val serviceName: String?
        get() = login.serviceName()

    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("cacheKey")
    )
    // Only for testing
    fun cacheKey(): Any? = loginMetadata.configInfo

    val cacheKey: Any?
        get() = loginMetadata.configInfo

    private fun acquire(): LoginManager {
        ++refCount
        LOGGER.trace("{} acquired", this)
        return this
    }

    /**
     * Decrease the reference count for this instance and release resources if it reaches 0.
     */
    fun release() {
        synchronized(LoginManager::class.java) {
            check(refCount != 0) { "release() called on disposed $this" }
            if (refCount == 1) {
                if (loginMetadata.configInfo is Password) DYNAMIC_INSTANCES.remove(loginMetadata)
                else STATIC_INSTANCES.remove(loginMetadata)

                login.close()
                loginCallbackHandler.close()
            }
            --refCount
            LOGGER.trace("{} released", this)
        }
    }

    override fun toString(): String {
        return "LoginManager(serviceName=$serviceName" +
                // subject.toString() exposes private credentials, so we can't use it
                ", publicCredentials=${subject!!.publicCredentials}" +
                ", refCount=$refCount)"
    }

    private class LoginMetadata<T>(
        val configInfo: T,
        val loginClass: Class<out Login>,
        val loginCallbackClass: Class<out AuthenticateCallbackHandler>,
        configs: Map<String, *>,
    ) {

        val saslConfigs: Map<String, Any?> = configs.filter { (key) -> key.startsWith("sasl.") }

        override fun equals(other: Any?): Boolean {
            if (this === other) return true
            if (javaClass != other?.javaClass) return false

            other as LoginMetadata<*>

            if (configInfo != other.configInfo) return false
            if (loginClass != other.loginClass) return false
            if (loginCallbackClass != other.loginCallbackClass) return false
            if (saslConfigs != other.saslConfigs) return false

            return true
        }

        override fun hashCode(): Int {
            var result = configInfo?.hashCode() ?: 0
            result = 31 * result + loginClass.hashCode()
            result = 31 * result + loginCallbackClass.hashCode()
            result = 31 * result + saslConfigs.hashCode()
            return result
        }
    }

    companion object {

        private val LOGGER = LoggerFactory.getLogger(LoginManager::class.java)

        // static configs (broker or client)
        private val STATIC_INSTANCES: MutableMap<LoginMetadata<String>, LoginManager> = HashMap()

        // dynamic configs (broker or client)
        private val DYNAMIC_INSTANCES: MutableMap<LoginMetadata<Password>, LoginManager> = HashMap()

        /**
         * Returns an instance of [LoginManager] and increases its reference count.
         *
         * [release] should be invoked when the `LoginManager` is no longer needed. This method will
         * try to reuse an existing `LoginManager` for the provided context type. If [jaasContext]
         * was loaded from a dynamic config, login managers are reused for the same dynamic config
         * value. For `jaasContext` loaded from static JAAS configuration, login managers are reused
         * for static contexts with the same login context name.
         *
         * This is a bit ugly and it would be nicer if we could pass the `LoginManager` to
         * `ChannelBuilders.create` and shut it down when the broker or clients are closed. It's
         * straightforward to do the former, but it's more complicated to do the latter without
         * making the consumer API more complex.
         *
         * @param jaasContext Static or dynamic JAAS context. `jaasContext.dynamicJaasConfig()` is
         * non-null for dynamic context. For static contexts, this may contain multiple login
         * modules if the context type is SERVER. For CLIENT static contexts and dynamic contexts of
         * CLIENT and SERVER, 'jaasContext` contains only one login module.
         * @param saslMechanism SASL mechanism for which login manager is being acquired. For
         * dynamic contexts, the single login module in `jaasContext` corresponds to this SASL
         * mechanism. Hence `Login` class is chosen based on this mechanism.
         * @param defaultLoginClass Default login class to use if an override is not specified in
         * `configs`.
         * @param configs Config options used to configure `Login` if a new login manager is created.
         */
        @Throws(LoginException::class)
        fun acquireLoginManager(
            jaasContext: JaasContext,
            saslMechanism: String,
            defaultLoginClass: Class<out Login>,
            configs: Map<String, *>,
        ): LoginManager {

            val loginClass = configuredClassOrDefault(
                configs = configs,
                jaasContext = jaasContext,
                saslMechanism = saslMechanism,
                configName = SaslConfigs.SASL_LOGIN_CLASS,
                defaultClass = defaultLoginClass
            )

            val defaultLoginCallbackHandlerClass =
                if (OAuthBearerLoginModule.OAUTHBEARER_MECHANISM == saslMechanism)
                    OAuthBearerUnsecuredLoginCallbackHandler::class.java
                else DefaultLoginCallbackHandler::class.java

            val loginCallbackClass = configuredClassOrDefault(
                configs = configs,
                jaasContext = jaasContext,
                saslMechanism = saslMechanism,
                configName = SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS,
                defaultClass = defaultLoginCallbackHandlerClass
            )

            synchronized(LoginManager::class.java) {
                var loginManager: LoginManager?
                val jaasConfigValue = jaasContext.dynamicJaasConfig

                if (jaasConfigValue != null) {
                    val loginMetadata =
                        LoginMetadata(jaasConfigValue, loginClass, loginCallbackClass, configs)

                    loginManager = DYNAMIC_INSTANCES[loginMetadata]

                    if (loginManager == null) {
                        loginManager =
                            LoginManager(jaasContext, saslMechanism, configs, loginMetadata)
                        DYNAMIC_INSTANCES[loginMetadata] = loginManager
                    }
                } else {
                    val loginMetadata =
                        LoginMetadata(jaasContext.name, loginClass, loginCallbackClass, configs)

                    loginManager = STATIC_INSTANCES.computeIfAbsent(loginMetadata) {
                        LoginManager(jaasContext, saslMechanism, configs, loginMetadata)
                    }
                }
                SecurityUtils.addConfiguredSecurityProviders(configs)

                return loginManager.acquire()
            }
        }

        /* Should only be used in tests. */
        fun closeAll() {
            synchronized(LoginManager::class.java) {
                STATIC_INSTANCES.keys.toSet()
                    .forEach { key -> STATIC_INSTANCES.remove(key)?.login?.close() }

                DYNAMIC_INSTANCES.keys.toSet()
                    .forEach { key -> DYNAMIC_INSTANCES.remove(key)?.login?.close() }
            }
        }

        private fun <T> configuredClassOrDefault(
            configs: Map<String, *>,
            jaasContext: JaasContext,
            saslMechanism: String,
            configName: String,
            defaultClass: Class<out T>
        ): Class<out T> {
            val prefix =
                if (jaasContext.type === JaasContext.Type.SERVER)
                    ListenerName.saslMechanismPrefix(saslMechanism)
                else ""

            val clazz = configs[prefix + configName] as Class<out T>? ?: return defaultClass

            if (jaasContext.configurationEntries.size != 1) throw ConfigException(
                "$configName cannot be specified with multiple login modules in the JAAS " +
                        "context. ${SaslConfigs.SASL_JAAS_CONFIG} must be configured to " +
                        "override mechanism-specific configs."
            )

            return clazz
        }
    }
}
