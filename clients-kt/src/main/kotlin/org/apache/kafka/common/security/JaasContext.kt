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

package org.apache.kafka.common.security

import javax.security.auth.login.AppConfigurationEntry
import javax.security.auth.login.Configuration
import org.apache.kafka.common.config.SaslConfigs
import org.apache.kafka.common.config.types.Password
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.security.JaasUtils.DISALLOWED_LOGIN_MODULES_CONFIG
import org.apache.kafka.common.security.JaasUtils.DISALLOWED_LOGIN_MODULES_DEFAULT
import org.slf4j.LoggerFactory

class JaasContext(
    val name: String,
    val type: Type,
    val configuration: Configuration,
    val dynamicJaasConfig: Password?
) {

    /**
     * The type of the SASL login context, it should be SERVER for the broker and CLIENT for the
     * clients (consumer, producer, etc.). This is used to validate behaviour (e.g. some
     * functionality is only available in the broker or clients).
     */
    enum class Type {
        CLIENT,
        SERVER,
    }

    val configurationEntries: List<AppConfigurationEntry> =
        requireNotNull(configuration.getAppConfigurationEntry(name)) {
            "Could not find a '$name' entry in this JAAS configuration."
        }.toList()

    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("name"),
    )
    fun name(): String {
        return name
    }

    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("type"),
    )
    fun type(): Type = type

    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("configuration"),
    )
    fun configuration(): Configuration = configuration

    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("configurationEntries"),
    )
    fun configurationEntries(): List<AppConfigurationEntry> = configurationEntries

    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("dynamicJaasConfig"),
    )
    fun dynamicJaasConfig(): Password? = dynamicJaasConfig

    companion object {
        
        private val log = LoggerFactory.getLogger(JaasContext::class.java)
        
        private const val GLOBAL_CONTEXT_NAME_SERVER = "KafkaServer"
        
        private const val GLOBAL_CONTEXT_NAME_CLIENT = "KafkaClient"

        /**
         * Returns an instance of this class.
         *
         * The context will contain the configuration specified by the JAAS configuration property
         * [SaslConfigs.SASL_JAAS_CONFIG] with prefix `listener.name.{listenerName}.{mechanism}.`
         * with listenerName and mechanism in lower case. The context `KafkaServer` will be returned
         * with a single login context entry loaded from the property.
         *
         *
         * If the property is not defined, the context will contain the default Configuration and
         * the context name will be one of:
         *
         *  1. Lowercased listener name followed by a period and the string `KafkaServer`
         *  1. The string `KafkaServer`
         *
         * If both are valid entries in the default JAAS configuration, the first option is chosen.
         *
         *
         * @throws IllegalArgumentException if listenerName or mechanism is not defined.
         */
        fun loadServerContext(
            listenerName: ListenerName,
            mechanism: String,
            configs: Map<String, *>,
        ): JaasContext {
            val listenerContextName = "${listenerName.value.lowercase()}.$GLOBAL_CONTEXT_NAME_SERVER"
            val dynamicJaasConfig =
                configs["${mechanism.lowercase()}.${SaslConfigs.SASL_JAAS_CONFIG}"] as Password?
            
            if (dynamicJaasConfig == null && configs[SaslConfigs.SASL_JAAS_CONFIG] != null)
                log.warn(
                    "Server config {} should be prefixed with SASL mechanism name, ignoring config",
                    SaslConfigs.SASL_JAAS_CONFIG
                )
            
            return load(
                contextType = Type.SERVER,
                listenerContextName = listenerContextName,
                globalContextName = GLOBAL_CONTEXT_NAME_SERVER,
                dynamicJaasConfig = dynamicJaasConfig,
            )
        }

        /**
         * Returns an instance of this class.
         *
         * If JAAS configuration property @link SaslConfigs#SASL_JAAS_CONFIG} is specified,
         * the configuration object is created by parsing the property value. Otherwise, the default
         * Configuration is returned. The context name is always `KafkaClient`.
         */
        fun loadClientContext(configs: Map<String, *>): JaasContext {
            val dynamicJaasConfig = configs[SaslConfigs.SASL_JAAS_CONFIG] as Password?
            return load(
                contextType = Type.CLIENT,
                listenerContextName = null,
                globalContextName = GLOBAL_CONTEXT_NAME_CLIENT,
                dynamicJaasConfig = dynamicJaasConfig,
            )
        }

        fun load(
            contextType: Type,
            listenerContextName: String?,
            globalContextName: String,
            dynamicJaasConfig: Password?
        ): JaasContext {
            return if (dynamicJaasConfig != null) {
                val jaasConfig = JaasConfig(globalContextName, dynamicJaasConfig.value)
                val contextModules = jaasConfig.getAppConfigurationEntry(globalContextName)

                require(!contextModules.isNullOrEmpty()) {
                    "JAAS config property does not contain any login modules"
                }

                require (contextModules.size == 1) {
                    "JAAS config property contains ${contextModules.size} login modules, should " +
                            "be 1 module"
                }
                
                throwIfLoginModuleIsNotAllowed(contextModules[0])
                JaasContext(globalContextName, contextType, jaasConfig, dynamicJaasConfig)
            } else defaultContext(contextType, listenerContextName, globalContextName)
        }

        private fun throwIfLoginModuleIsNotAllowed(appConfigurationEntry: AppConfigurationEntry) {
            val disallowedLoginModuleList: List<String> = System.getProperty(
                DISALLOWED_LOGIN_MODULES_CONFIG,
                DISALLOWED_LOGIN_MODULES_DEFAULT
            ).split(",".toRegex())
                .dropLastWhile { it.isEmpty() }
                .toTypedArray()
                .map { obj -> obj.trim { it <= ' ' } }

            val loginModuleName = appConfigurationEntry.loginModuleName.trim { it <= ' ' }

            require(!disallowedLoginModuleList.contains(loginModuleName)) {
                "$loginModuleName is not allowed. Update System property " +
                        "'$DISALLOWED_LOGIN_MODULES_CONFIG' to allow $loginModuleName"
            }
        }

        private fun defaultContext(
            contextType: Type,
            listenerContextName: String?,
            globalContextName: String
        ): JaasContext {

            val jaasConfigFile = System.getProperty(JaasUtils.JAVA_LOGIN_CONFIG_PARAM)

            if (jaasConfigFile == null) {
                if (contextType == Type.CLIENT) log.debug(
                    "System property '${JaasUtils.JAVA_LOGIN_CONFIG_PARAM}' and Kafka SASL " +
                            "property '${SaslConfigs.SASL_JAAS_CONFIG}' are not set, using " +
                            "default JAAS configuration."
                )
                else log.debug(
                    "System property '${JaasUtils.JAVA_LOGIN_CONFIG_PARAM}' is not set, using " +
                            "default JAAS configuration."
                )
            }

            val jaasConfig = Configuration.getConfiguration()
            var configEntries: Array<AppConfigurationEntry>? = null
            var contextName = globalContextName

            if (listenerContextName != null) {
                configEntries = jaasConfig.getAppConfigurationEntry(listenerContextName)
                if (configEntries != null) contextName = listenerContextName
            }

            if (configEntries == null) configEntries =
                jaasConfig.getAppConfigurationEntry(globalContextName)

            if (configEntries == null) {
                val listenerNameText =
                    if (listenerContextName == null) ""
                    else " or '$listenerContextName'"

                throw IllegalArgumentException(
                    "Could not find a '$globalContextName'$listenerNameText entry in the JAAS " +
                            "configuration. System property '${JaasUtils.JAVA_LOGIN_CONFIG_PARAM}' " +
                            "is ${jaasConfigFile ?: "not set"}"
                )
            }

            configEntries.forEach { appConfigurationEntry ->
                throwIfLoginModuleIsNotAllowed(appConfigurationEntry)
            }

            return JaasContext(contextName, contextType, jaasConfig, null)
        }

        /**
         * Returns the configuration option for `key` from this context.
         * If login module name is specified, return option value only from that module.
         */
        fun configEntryOption(
            configurationEntries: List<AppConfigurationEntry>,
            key: String?,
            loginModuleName: String? = null,
        ): String? {
            configurationEntries.forEach { entry: AppConfigurationEntry ->
                if (loginModuleName != null && loginModuleName != entry.loginModuleName) return@forEach
                val `val` = entry.options[key]
                if (`val` != null) return `val` as String?
            }
            return null
        }
    }
}
