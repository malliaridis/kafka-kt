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

package org.apache.kafka.common.network

import java.util.function.Supplier
import org.apache.kafka.common.Configurable
import org.apache.kafka.common.config.AbstractConfig
import org.apache.kafka.common.config.SslClientAuth
import org.apache.kafka.common.config.internals.BrokerSecurityConfigs
import org.apache.kafka.common.errors.InvalidConfigurationException
import org.apache.kafka.common.requests.ApiVersionsResponse
import org.apache.kafka.common.security.JaasContext
import org.apache.kafka.common.security.auth.KafkaPrincipalBuilder
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.security.authenticator.CredentialCache
import org.apache.kafka.common.security.authenticator.DefaultKafkaPrincipalBuilder
import org.apache.kafka.common.security.kerberos.KerberosShortNamer
import org.apache.kafka.common.security.ssl.SslPrincipalMapper
import org.apache.kafka.common.security.token.delegation.internals.DelegationTokenCache
import org.apache.kafka.common.utils.LogContext
import org.apache.kafka.common.utils.Time
import org.apache.kafka.common.utils.Utils.newInstance
import org.slf4j.LoggerFactory

object ChannelBuilders {

    private val log = LoggerFactory.getLogger(ChannelBuilders::class.java)

    /**
     * @param securityProtocol the securityProtocol
     * @param contextType the contextType, it must be non-null if `securityProtocol` is SASL_*; it
     * is ignored otherwise
     * @param config client config
     * @param listenerName the listenerName if contextType is SERVER or null otherwise
     * @param clientSaslMechanism SASL mechanism if mode is CLIENT, ignored otherwise
     * @param time the time instance
     * @param saslHandshakeRequestEnable flag to enable Sasl handshake requests; disabled only for
     * SASL inter-broker connections with inter-broker protocol version < 0.10
     * @param logContext the log context instance
     * @return the configured `ChannelBuilder`
     * @throws IllegalArgumentException if `mode` invariants described above is not maintained
     */
    fun clientChannelBuilder(
        securityProtocol: SecurityProtocol,
        contextType: JaasContext.Type?,
        config: AbstractConfig,
        listenerName: ListenerName? = null,
        clientSaslMechanism: String?,
        time: Time,
        saslHandshakeRequestEnable: Boolean = true,
        logContext: LogContext
    ): ChannelBuilder {
        if (
            securityProtocol === SecurityProtocol.SASL_PLAINTEXT
            || securityProtocol === SecurityProtocol.SASL_SSL
        ) {
            requireNotNull(contextType) {
                "`contextType` must be non-null if `securityProtocol` is `$securityProtocol`"
            }
            requireNotNull(clientSaslMechanism) {
                "`clientSaslMechanism` must be non-null in client mode if `securityProtocol` is `$securityProtocol`"
            }
        }

        return create(
            securityProtocol = securityProtocol,
            mode = Mode.CLIENT,
            contextType = contextType,
            config = config,
            listenerName = listenerName,
            isInterBrokerListener = false,
            clientSaslMechanism = clientSaslMechanism,
            saslHandshakeRequestEnable = saslHandshakeRequestEnable,
            time = time,
            logContext = logContext,
        )
    }

    /**
     * @param listenerName the listenerName
     * @param isInterBrokerListener whether or not this listener is used for inter-broker requests
     * @param securityProtocol the securityProtocol
     * @param config server config
     * @param credentialCache Credential cache for SASL/SCRAM if SCRAM is enabled
     * @param tokenCache Delegation token cache
     * @param time the time instance
     * @param logContext the log context instance
     * @param apiVersionSupplier supplier for ApiVersions responses sent prior to authentication
     * @return the configured `ChannelBuilder`
     */
    fun serverChannelBuilder(
        listenerName: ListenerName?,
        isInterBrokerListener: Boolean,
        securityProtocol: SecurityProtocol,
        config: AbstractConfig,
        credentialCache: CredentialCache?,
        tokenCache: DelegationTokenCache?,
        time: Time,
        logContext: LogContext,
        apiVersionSupplier: Supplier<ApiVersionsResponse>?
    ): ChannelBuilder {
        return create(
            securityProtocol = securityProtocol,
            mode = Mode.SERVER,
            contextType = JaasContext.Type.SERVER,
            config = config,
            listenerName = listenerName,
            isInterBrokerListener = isInterBrokerListener,
            saslHandshakeRequestEnable = true,
            credentialCache = credentialCache,
            tokenCache = tokenCache,
            time = time,
            logContext = logContext,
            apiVersionSupplier = apiVersionSupplier,
        )
    }

    private fun create(
        securityProtocol: SecurityProtocol,
        mode: Mode,
        contextType: JaasContext.Type?,
        config: AbstractConfig,
        listenerName: ListenerName?,
        isInterBrokerListener: Boolean,
        clientSaslMechanism: String? = null,
        saslHandshakeRequestEnable: Boolean = true,
        credentialCache: CredentialCache? = null,
        tokenCache: DelegationTokenCache? = null,
        time: Time,
        logContext: LogContext,
        apiVersionSupplier: Supplier<ApiVersionsResponse>? = null,
    ): ChannelBuilder {
        val configs = channelBuilderConfigs(config, listenerName)
        val channelBuilder: ChannelBuilder = when (securityProtocol) {
            SecurityProtocol.SSL -> SslChannelBuilder(
                mode = mode,
                listenerName = listenerName,
                isInterBrokerListener = isInterBrokerListener,
                logContext = logContext,
            )

            SecurityProtocol.SASL_SSL,
            SecurityProtocol.SASL_PLAINTEXT -> {
                val jaasContexts: Map<String, JaasContext>
                var sslClientAuthOverride: String? = null

                if (mode === Mode.SERVER) {
                    val enabledMechanisms =
                        configs[BrokerSecurityConfigs.SASL_ENABLED_MECHANISMS_CONFIG] as List<String>?

                    jaasContexts = enabledMechanisms!!.associateBy(
                        keySelector = { it },
                        valueTransform = { mechanism ->
                            JaasContext.loadServerContext(
                                listenerName = listenerName!!,
                                mechanism = mechanism,
                                configs = configs,
                            )
                        },
                    )

                    // SSL client authentication is enabled in brokers for SASL_SSL only if
                    // listener-prefixed config is specified.
                    if (listenerName != null && securityProtocol === SecurityProtocol.SASL_SSL) {
                        val configuredClientAuth =
                            configs[BrokerSecurityConfigs.SSL_CLIENT_AUTH_CONFIG] as String?

                        val listenerClientAuth = config.originalsWithPrefix(
                            prefix = listenerName.configPrefix(),
                            strip = true,
                        )[BrokerSecurityConfigs.SSL_CLIENT_AUTH_CONFIG] as String?

                        // If `ssl.client.auth` is configured at the listener-level, we don't set an
                        // override and SslFactory uses the value from `configs`. If not, we
                        // propagate `sslClientAuthOverride=NONE` to SslFactory and it applies the
                        // override to the latest configs when it is configured or reconfigured.
                        // `Note that ssl.client.auth` cannot be dynamically altered.
                        if (listenerClientAuth == null) {
                            sslClientAuthOverride = SslClientAuth.NONE.name.lowercase()

                            if (
                                configuredClientAuth != null
                                && !configuredClientAuth.equals(
                                    other = SslClientAuth.NONE.name,
                                    ignoreCase = true,
                                )
                            ) log.warn(
                                "Broker configuration '{}' is applied only to SSL listeners. " +
                                        "Listener-prefixed configuration can be used to enable " +
                                        "SSL client authentication for SASL_SSL listeners. In " +
                                        "future releases, broker-wide option without listener " +
                                        "prefix may be applied to SASL_SSL listeners as well. " +
                                        "All configuration options intended for specific " +
                                        "listeners should be listener-prefixed.",
                                BrokerSecurityConfigs.SSL_CLIENT_AUTH_CONFIG
                            )
                        }
                    }
                } else {
                    // Use server context for inter-broker client connections and client context for
                    // other clients
                    jaasContexts = mapOf(
                        clientSaslMechanism!! to
                                if (contextType === JaasContext.Type.CLIENT)
                                    JaasContext.loadClientContext(configs)
                                else JaasContext.loadServerContext(
                                    listenerName = listenerName!!,
                                    mechanism = clientSaslMechanism,
                                    configs = configs
                                )
                    )
                }
                SaslChannelBuilder(
                    mode = mode,
                    jaasContexts = jaasContexts,
                    securityProtocol = securityProtocol,
                    listenerName = listenerName,
                    isInterBrokerListener = isInterBrokerListener,
                    clientSaslMechanism = clientSaslMechanism,
                    handshakeRequestEnable = saslHandshakeRequestEnable,
                    credentialCache = credentialCache,
                    tokenCache = tokenCache,
                    sslClientAuthOverride = sslClientAuthOverride,
                    time = time,
                    logContext = logContext,
                    apiVersionSupplier = apiVersionSupplier
                )
            }

            SecurityProtocol.PLAINTEXT -> PlaintextChannelBuilder(listenerName)
        }

        channelBuilder.configure(configs)
        return channelBuilder
    }

    /**
     * @return a mutable RecordingMap. The elements got from RecordingMap are marked as "used".
     */
    fun channelBuilderConfigs(
        config: AbstractConfig,
        listenerName: ListenerName?
    ): Map<String, Any?> {
        val parsedConfigs = listenerName?.let {
            config.valuesWithPrefixOverride(listenerName.configPrefix())
        } ?: config.values()

        return parsedConfigs + config.originals().filter { (key, _) ->
            // exclude already parsed configs
            !parsedConfigs.containsKey(key)
        }.filterNot { e: Map.Entry<String, Any?> ->
            // exclude already parsed listener prefix configs
            (listenerName != null) && e.key.startsWith(listenerName.configPrefix())
                    && parsedConfigs.containsKey(e.key.substring(listenerName.configPrefix().length))
        }.filterNot { e: Map.Entry<String, Any?> ->
            // exclude keys like `{mechanism}.some.prop` if "listener.name." prefix is present and
            // key `some.prop` exists in parsed configs.
            listenerName != null && parsedConfigs
                .containsKey(e.key.substring(e.key.indexOf('.') + 1))
        }
    }

    @Deprecated("There is no need for null-check for this case in Kotlin")
    private fun requireNonNullMode(mode: Mode?, securityProtocol: SecurityProtocol) =
        requireNotNull(mode) { "`mode` must be non-null if `securityProtocol` is `$securityProtocol`" }

    fun createPrincipalBuilder(
        configs: Map<String, *>,
        kerberosShortNamer: KerberosShortNamer? = null,
        sslPrincipalMapper: SslPrincipalMapper? = null,
    ): KafkaPrincipalBuilder {
        val principalBuilderClass =
            configs[BrokerSecurityConfigs.PRINCIPAL_BUILDER_CLASS_CONFIG] as Class<*>?

        val builder: KafkaPrincipalBuilder = if (
            principalBuilderClass == null
            || principalBuilderClass == DefaultKafkaPrincipalBuilder::class.java
        ) DefaultKafkaPrincipalBuilder(kerberosShortNamer!!, sslPrincipalMapper!!)
        else if (KafkaPrincipalBuilder::class.java.isAssignableFrom(principalBuilderClass))
            newInstance(principalBuilderClass) as KafkaPrincipalBuilder
        else throw InvalidConfigurationException(
            "Type ${principalBuilderClass.name} is not an instance of " +
                    KafkaPrincipalBuilder::class.java.name
        )

        if (builder is Configurable) (builder as Configurable).configure(configs)
        return builder
    }
}
