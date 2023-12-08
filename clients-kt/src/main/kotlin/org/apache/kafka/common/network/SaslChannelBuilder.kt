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

import java.io.IOException
import java.nio.channels.SelectionKey
import java.nio.channels.SocketChannel
import java.util.*
import java.util.function.Supplier
import javax.security.auth.Subject
import javax.security.auth.kerberos.KerberosPrincipal
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.config.SaslConfigs
import org.apache.kafka.common.config.SslConfigs
import org.apache.kafka.common.config.internals.BrokerSecurityConfigs
import org.apache.kafka.common.memory.MemoryPool
import org.apache.kafka.common.requests.ApiVersionsResponse
import org.apache.kafka.common.security.JaasContext
import org.apache.kafka.common.security.auth.AuthenticateCallbackHandler
import org.apache.kafka.common.security.auth.Login
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.security.authenticator.CredentialCache
import org.apache.kafka.common.security.authenticator.DefaultLogin
import org.apache.kafka.common.security.authenticator.LoginManager
import org.apache.kafka.common.security.authenticator.SaslClientAuthenticator
import org.apache.kafka.common.security.authenticator.SaslClientCallbackHandler
import org.apache.kafka.common.security.authenticator.SaslServerAuthenticator
import org.apache.kafka.common.security.authenticator.SaslServerCallbackHandler
import org.apache.kafka.common.security.kerberos.KerberosClientCallbackHandler
import org.apache.kafka.common.security.kerberos.KerberosLogin
import org.apache.kafka.common.security.kerberos.KerberosName
import org.apache.kafka.common.security.kerberos.KerberosShortNamer
import org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule
import org.apache.kafka.common.security.oauthbearer.internals.OAuthBearerRefreshingLogin
import org.apache.kafka.common.security.oauthbearer.internals.OAuthBearerSaslClientCallbackHandler
import org.apache.kafka.common.security.oauthbearer.internals.unsecured.OAuthBearerUnsecuredValidatorCallbackHandler
import org.apache.kafka.common.security.plain.internals.PlainSaslServer
import org.apache.kafka.common.security.plain.internals.PlainServerCallbackHandler
import org.apache.kafka.common.security.scram.ScramCredential
import org.apache.kafka.common.security.scram.internals.ScramMechanism
import org.apache.kafka.common.security.scram.internals.ScramServerCallbackHandler
import org.apache.kafka.common.security.ssl.SslFactory
import org.apache.kafka.common.security.token.delegation.internals.DelegationTokenCache
import org.apache.kafka.common.utils.LogContext
import org.apache.kafka.common.utils.Time
import org.apache.kafka.common.utils.Utils.newInstance
import org.ietf.jgss.GSSContext
import org.ietf.jgss.GSSCredential
import org.ietf.jgss.GSSException
import org.ietf.jgss.GSSManager
import org.ietf.jgss.GSSName
import org.ietf.jgss.Oid
import org.slf4j.Logger

open class SaslChannelBuilder(
    private val mode: Mode,
    private val jaasContexts: Map<String, JaasContext>,
    private val securityProtocol: SecurityProtocol,
    private val listenerName: ListenerName?,
    private val isInterBrokerListener: Boolean,
    private val clientSaslMechanism: String?,
    private val handshakeRequestEnable: Boolean,
    private val credentialCache: CredentialCache?,
    private val tokenCache: DelegationTokenCache?,
    private val sslClientAuthOverride: String?,
    private val time: Time,
    private val logContext: LogContext,
    private val apiVersionSupplier: Supplier<ApiVersionsResponse>?,
) : ChannelBuilder, ListenerReconfigurable {

    private val loginManagers: MutableMap<String, LoginManager> = HashMap(jaasContexts.size)

    private lateinit var subjects: Map<String, Subject>

    private lateinit var sslFactory: SslFactory

    private lateinit var configs: Map<String, *>

    private var kerberosShortNamer: KerberosShortNamer? = null

    private val saslCallbackHandlers = mutableMapOf<String, AuthenticateCallbackHandler>()

    private val connectionsMaxReauthMsByMechanism = mutableMapOf<String, Long>()

    private val log: Logger = logContext.logger(javaClass)

    init {
        require(mode === Mode.CLIENT || apiVersionSupplier != null) {
            "Server channel builder must provide an ApiVersionResponse supplier"
        }
    }

    @Throws(KafkaException::class)
    override fun configure(configs: Map<String, *>) {
        try {
            this.configs = configs
            if (mode === Mode.SERVER) {
                createServerCallbackHandlers(configs)
                createConnectionsMaxReauthMsMap(configs)
            } else createClientCallbackHandler(configs)
            for ((mechanism, value) in saslCallbackHandlers) {
                value.configure(
                    configs,
                    mechanism,
                    jaasContexts[mechanism]!!.configurationEntries
                )
            }
            val defaultLoginClass = defaultLoginClass()
            if (mode === Mode.SERVER && jaasContexts.containsKey(SaslConfigs.GSSAPI_MECHANISM)) {
                val defaultRealm = try {
                    defaultKerberosRealm()
                } catch (_: Exception) {
                    ""
                }

                val principalToLocalRules =
                    configs[BrokerSecurityConfigs.SASL_KERBEROS_PRINCIPAL_TO_LOCAL_RULES_CONFIG] as? List<String>
                if (principalToLocalRules != null) kerberosShortNamer =
                    KerberosShortNamer.fromUnparsedRules(defaultRealm, principalToLocalRules)
            }
            subjects = jaasContexts.map { (mechanism, context) ->
                // With static JAAS configuration, use KerberosLogin if Kerberos is enabled. With
                // dynamic JAAS configuration, use KerberosLogin only for the LoginContext
                // corresponding to GSSAPI
                val loginManager = LoginManager.acquireLoginManager(
                    jaasContext = context,
                    saslMechanism = mechanism,
                    defaultLoginClass = defaultLoginClass,
                    configs = configs,
                )

                loginManagers[mechanism] = loginManager
                val subject = loginManager.subject!!
                if (mode === Mode.SERVER && mechanism == SaslConfigs.GSSAPI_MECHANISM)
                    maybeAddNativeGssapiCredentials(subject)

                mechanism to subject
            }.toMap()

            if (securityProtocol === SecurityProtocol.SASL_SSL) {
                // Disable SSL client authentication as we are using SASL authentication
                sslFactory = SslFactory(mode, sslClientAuthOverride, isInterBrokerListener)
                sslFactory.configure(configs)
            }
        } catch (e: Throwable) {
            close()
            throw KafkaException(cause = e)
        }
    }

    override fun reconfigurableConfigs(): Set<String> =
        if (securityProtocol === SecurityProtocol.SASL_SSL) SslConfigs.RECONFIGURABLE_CONFIGS
        else emptySet()

    override fun validateReconfiguration(configs: Map<String, *>) {
        if (securityProtocol === SecurityProtocol.SASL_SSL)
            sslFactory.validateReconfiguration(configs)
    }

    override fun reconfigure(configs: Map<String, *>) {
        if (securityProtocol === SecurityProtocol.SASL_SSL) sslFactory.reconfigure(configs)
    }

    override fun listenerName(): ListenerName? = listenerName

    @Throws(KafkaException::class)
    override fun buildChannel(
        id: String,
        key: SelectionKey,
        maxReceiveSize: Int,
        memoryPool: MemoryPool?,
        metadataRegistry: ChannelMetadataRegistry,
    ): KafkaChannel {
        return try {
            val socketChannel = key.channel() as SocketChannel
            val socket = socketChannel.socket()
            val transportLayer = buildTransportLayer(id, key, socketChannel, metadataRegistry)
            val authenticatorCreator: Supplier<Authenticator> = if (mode === Mode.SERVER) {
                Supplier {
                    buildServerAuthenticator(
                        configs = configs,
                        callbackHandlers = saslCallbackHandlers.toMap(),
                        id = id,
                        transportLayer = transportLayer,
                        subjects = subjects,
                        connectionsMaxReauthMsByMechanism = connectionsMaxReauthMsByMechanism.toMap(),
                        metadataRegistry = metadataRegistry
                    )
                }
            } else {
                val loginManager = loginManagers[clientSaslMechanism]!!
                Supplier {
                    buildClientAuthenticator(
                        configs = configs,
                        callbackHandler = saslCallbackHandlers[clientSaslMechanism]!!,
                        id = id,
                        serverHost = socket.inetAddress.hostName,
                        servicePrincipal = loginManager.serviceName!!,
                        transportLayer = transportLayer,
                        subject = subjects[clientSaslMechanism]!!,
                    )
                }
            }
            KafkaChannel(
                id = id,
                transportLayer = transportLayer,
                authenticatorCreator = authenticatorCreator,
                maxReceiveSize = maxReceiveSize,
                memoryPool = memoryPool ?: MemoryPool.NONE,
                metadataRegistry = metadataRegistry
            )
        } catch (e: Exception) {
            throw KafkaException(cause = e)
        }
    }

    override fun close() {
        for (loginManager in loginManagers.values) loginManager.release()
        loginManagers.clear()

        for (handler in saslCallbackHandlers.values) handler.close()
        if (::sslFactory.isInitialized) sslFactory.close()
    }

    // Visible to override for testing
    @Throws(IOException::class)
    internal open fun buildTransportLayer(
        id: String,
        key: SelectionKey,
        socketChannel: SocketChannel,
        metadataRegistry: ChannelMetadataRegistry,
    ): TransportLayer {
        return if (securityProtocol === SecurityProtocol.SASL_SSL) SslTransportLayer.create(
            channelId = id,
            key = key,
            sslEngine = sslFactory.createSslEngine(socketChannel.socket()),
            metadataRegistry = metadataRegistry,
        ) else PlaintextTransportLayer(key)
    }

    // Visible to override for testing
    internal open fun buildServerAuthenticator(
        configs: Map<String, *>,
        callbackHandlers: Map<String, AuthenticateCallbackHandler>,
        id: String,
        transportLayer: TransportLayer,
        subjects: Map<String, Subject>,
        connectionsMaxReauthMsByMechanism: Map<String, Long>,
        metadataRegistry: ChannelMetadataRegistry,
    ): SaslServerAuthenticator = SaslServerAuthenticator(
        configs = configs,
        callbackHandlers = callbackHandlers,
        connectionId = id,
        subjects = subjects,
        kerberosNameParser = kerberosShortNamer,
        listenerName = listenerName,
        securityProtocol = securityProtocol,
        transportLayer = transportLayer,
        connectionsMaxReauthMsByMechanism = connectionsMaxReauthMsByMechanism,
        metadataRegistry = metadataRegistry,
        time = time,
        apiVersionSupplier = apiVersionSupplier!!
    )

    // Visible to override for testing
    internal open fun buildClientAuthenticator(
        configs: Map<String, *>,
        callbackHandler: AuthenticateCallbackHandler,
        id: String,
        serverHost: String,
        servicePrincipal: String,
        transportLayer: TransportLayer,
        subject: Subject
    ): SaslClientAuthenticator = SaslClientAuthenticator(
        configs = configs,
        callbackHandler = callbackHandler,
        node = id,
        subject = subject,
        servicePrincipal = servicePrincipal,
        host = serverHost,
        mechanism = clientSaslMechanism!!,
        handshakeRequestEnable = handshakeRequestEnable,
        transportLayer = transportLayer,
        time = time,
        logContext = logContext
    )

    // Package private for testing
    internal fun loginManagers(): Map<String, LoginManager> = loginManagers

    private fun createClientCallbackHandler(configs: Map<String, *>) {
        val callbackHandlerClass =
            configs[SaslConfigs.SASL_CLIENT_CALLBACK_HANDLER_CLASS] as? Class<out AuthenticateCallbackHandler>
            ?: clientCallbackHandlerClass()

        saslCallbackHandlers[clientSaslMechanism!!] = newInstance(callbackHandlerClass)
    }

    private fun createServerCallbackHandlers(configs: Map<String, *>) {
        for (mechanism in jaasContexts.keys) {
            var callbackHandler: AuthenticateCallbackHandler
            val prefix = ListenerName.saslMechanismPrefix(mechanism)
            val key = prefix + BrokerSecurityConfigs.SASL_SERVER_CALLBACK_HANDLER_CLASS
            val clazz = configs[key] as? Class<out AuthenticateCallbackHandler>

            callbackHandler =
                if (clazz != null) newInstance(clazz)
                else if (mechanism == PlainSaslServer.PLAIN_MECHANISM) PlainServerCallbackHandler()
                else if (ScramMechanism.isScram(mechanism)) ScramServerCallbackHandler(
                    credentialCache = credentialCache!!.cache(mechanism, ScramCredential::class.java)!!,
                    tokenCache = tokenCache,
                ) else if (mechanism == OAuthBearerLoginModule.OAUTHBEARER_MECHANISM)
                    OAuthBearerUnsecuredValidatorCallbackHandler()
                else SaslServerCallbackHandler()

            saslCallbackHandlers[mechanism] = callbackHandler
        }
    }

    private fun createConnectionsMaxReauthMsMap(configs: Map<String, *>) {
        for (mechanism in jaasContexts.keys) {
            val prefix = ListenerName.saslMechanismPrefix(mechanism)
            var connectionsMaxReauthMs =
                configs[prefix + BrokerSecurityConfigs.CONNECTIONS_MAX_REAUTH_MS] as Long?
            if (connectionsMaxReauthMs == null)
                connectionsMaxReauthMs = configs[BrokerSecurityConfigs.CONNECTIONS_MAX_REAUTH_MS] as Long?
            if (connectionsMaxReauthMs != null)
                connectionsMaxReauthMsByMechanism[mechanism] = connectionsMaxReauthMs
        }
    }

    internal fun defaultLoginClass(): Class<out Login> {
        return if (jaasContexts.containsKey(SaslConfigs.GSSAPI_MECHANISM)) KerberosLogin::class.java
        else if (OAuthBearerLoginModule.OAUTHBEARER_MECHANISM == clientSaslMechanism)
            OAuthBearerRefreshingLogin::class.java
        else DefaultLogin::class.java
    }

    private fun clientCallbackHandlerClass(): Class<out AuthenticateCallbackHandler?> {
        return when (clientSaslMechanism) {
            SaslConfigs.GSSAPI_MECHANISM -> KerberosClientCallbackHandler::class.java
            OAuthBearerLoginModule.OAUTHBEARER_MECHANISM -> OAuthBearerSaslClientCallbackHandler::class.java
            else -> SaslClientCallbackHandler::class.java
        }
    }

    // As described in http://docs.oracle.com/javase/8/docs/technotes/guides/security/jgss/jgss-features.html:
    // "To enable Java GSS to delegate to the native GSS library and its list of native mechanisms,
    // set the system property "sun.security.jgss.native" to true"
    // "In addition, when performing operations as a particular Subject, for example, Subject.doAs(...)
    // or Subject.doAsPrivileged(...), the to-be-used GSSCredential should be added to Subject's
    // private credential set. Otherwise, the GSS operations will fail since no credential is found."
    private fun maybeAddNativeGssapiCredentials(subject: Subject?) {
        val usingNativeJgss = java.lang.Boolean.getBoolean(GSS_NATIVE_PROP)

        if (usingNativeJgss && subject!!.getPrivateCredentials(GSSCredential::class.java).isEmpty()) {
            val servicePrincipal = SaslClientAuthenticator.firstPrincipal(subject)

            val kerberosName: KerberosName = try {
                KerberosName.parse(servicePrincipal)
            } catch (e: IllegalArgumentException) {
                throw KafkaException("Principal has name with unexpected format $servicePrincipal")
            }
            val servicePrincipalName = kerberosName.serviceName
            val serviceHostname = kerberosName.hostName

            try {
                val manager = gssManager()
                // This Oid is used to represent the Kerberos version 5 GSS-API mechanism. It is defined in
                // RFC 1964.
                val krb5Mechanism = Oid("1.2.840.113554.1.2.2")
                val gssName = manager.createName(
                    "$servicePrincipalName@$serviceHostname",
                    GSSName.NT_HOSTBASED_SERVICE
                )
                val cred = manager.createCredential(
                    gssName,
                    GSSContext.INDEFINITE_LIFETIME, krb5Mechanism, GSSCredential.ACCEPT_ONLY
                )
                subject.privateCredentials.add(cred)
                log.info("Configured native GSSAPI private credentials for {}@{}", serviceHostname, serviceHostname)
            } catch (ex: GSSException) {
                log.warn("Cannot add private credential to subject; clients authentication may fail", ex)
            }
        }
    }

    // Visibility to override for testing
    internal open fun gssManager(): GSSManager {
        return GSSManager.getInstance()
    }

    // Visibility for testing
    internal fun subject(saslMechanism: String): Subject? {
        return subjects[saslMechanism]
    }

    companion object {
        const val GSS_NATIVE_PROP = "sun.security.jgss.native"
        private fun defaultKerberosRealm(): String {
            // see https://issues.apache.org/jira/browse/HADOOP-10848 for details
            return KerberosPrincipal("tmp", 1).realm
        }
    }
}
