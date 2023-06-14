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

import java.io.Closeable
import java.io.IOException
import java.net.InetAddress
import java.nio.ByteBuffer
import java.nio.channels.SelectionKey
import java.security.PrivilegedActionException
import java.security.PrivilegedExceptionAction
import java.util.*
import java.util.function.Supplier
import javax.security.auth.Subject
import javax.security.sasl.Sasl
import javax.security.sasl.SaslException
import javax.security.sasl.SaslServer
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.config.SaslConfigs
import org.apache.kafka.common.config.internals.BrokerSecurityConfigs
import org.apache.kafka.common.errors.AuthenticationException
import org.apache.kafka.common.errors.IllegalSaslStateException
import org.apache.kafka.common.errors.InvalidRequestException
import org.apache.kafka.common.errors.SaslAuthenticationException
import org.apache.kafka.common.errors.UnsupportedSaslMechanismException
import org.apache.kafka.common.errors.UnsupportedVersionException
import org.apache.kafka.common.message.SaslAuthenticateResponseData
import org.apache.kafka.common.message.SaslHandshakeResponseData
import org.apache.kafka.common.network.Authenticator
import org.apache.kafka.common.network.ByteBufferSend
import org.apache.kafka.common.network.ChannelBuilders
import org.apache.kafka.common.network.ChannelMetadataRegistry
import org.apache.kafka.common.network.ClientInformation
import org.apache.kafka.common.network.InvalidReceiveException
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.network.NetworkReceive
import org.apache.kafka.common.network.ReauthenticationContext
import org.apache.kafka.common.network.Send
import org.apache.kafka.common.network.SslTransportLayer
import org.apache.kafka.common.network.TransportLayer
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.AbstractResponse
import org.apache.kafka.common.requests.ApiVersionsRequest
import org.apache.kafka.common.requests.ApiVersionsResponse
import org.apache.kafka.common.requests.RequestContext
import org.apache.kafka.common.requests.RequestHeader
import org.apache.kafka.common.requests.SaslAuthenticateRequest
import org.apache.kafka.common.requests.SaslAuthenticateResponse
import org.apache.kafka.common.requests.SaslHandshakeRequest
import org.apache.kafka.common.requests.SaslHandshakeResponse
import org.apache.kafka.common.security.auth.AuthenticateCallbackHandler
import org.apache.kafka.common.security.auth.KafkaPrincipal
import org.apache.kafka.common.security.auth.KafkaPrincipalBuilder
import org.apache.kafka.common.security.auth.KafkaPrincipalSerde
import org.apache.kafka.common.security.auth.SaslAuthenticationContext
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.security.kerberos.KerberosError
import org.apache.kafka.common.security.kerberos.KerberosName
import org.apache.kafka.common.security.kerberos.KerberosShortNamer
import org.apache.kafka.common.security.scram.ScramLoginModule
import org.apache.kafka.common.security.scram.internals.ScramMechanism
import org.apache.kafka.common.utils.Time
import org.apache.kafka.common.utils.Utils.closeQuietly
import org.apache.kafka.common.utils.Utils.copyArray
import org.slf4j.LoggerFactory
import kotlin.math.roundToLong

open class SaslServerAuthenticator(
    private val configs: Map<String, *>,
    private val callbackHandlers: Map<String, AuthenticateCallbackHandler>,
    private val connectionId: String,
    private val subjects: Map<String, Subject>,
    kerberosNameParser: KerberosShortNamer?,
    private val listenerName: ListenerName,
    private val securityProtocol: SecurityProtocol,
    private val transportLayer: TransportLayer,
    private val connectionsMaxReauthMsByMechanism: Map<String?, Long>,
    private val metadataRegistry: ChannelMetadataRegistry,
    private val time: Time,
    private val apiVersionSupplier: Supplier<ApiVersionsResponse>
) : Authenticator {

    /**
     * The internal state transitions for initial authentication of a channel on the server side are
     * declared in order, starting with [INITIAL_REQUEST] and ending in either [COMPLETE] or
     * [FAILED].
     *
     * Re-authentication of a channel on the server side starts with the state
     * [REAUTH_PROCESS_HANDSHAKE]. It may then flow to [REAUTH_BAD_MECHANISM] before a transition to
     * [FAILED] if re-authentication is attempted with a mechanism different than the original one;
     * otherwise it joins the authentication flow at the [AUTHENTICATE] state and likewise ends at
     * either [COMPLETE] or [FAILED].
     */
    private enum class SaslState {

        // May be GSSAPI token, SaslHandshake or ApiVersions for authentication
        INITIAL_REQUEST,

        // May be SaslHandshake or ApiVersions
        HANDSHAKE_OR_VERSIONS_REQUEST,

        // After an ApiVersions request, next request must be SaslHandshake
        HANDSHAKE_REQUEST,

        // Authentication tokens (SaslHandshake v1 and above indicate SaslAuthenticate headers)
        AUTHENTICATE,

        // Authentication completed successfully
        COMPLETE,

        // Authentication failed
        FAILED,

        // Initial state for re-authentication, processes SASL handshake request
        REAUTH_PROCESS_HANDSHAKE,

        // When re-authentication requested with wrong mechanism, generate exception
        REAUTH_BAD_MECHANISM
    }

    private val enabledMechanisms: List<String>

    private val principalBuilder: KafkaPrincipalBuilder

    private val reauthInfo: ReauthInfo = ReauthInfo()

    // Current SASL state
    private var saslState = SaslState.INITIAL_REQUEST

    // Next SASL state to be set when outgoing writes associated with the current SASL state
    // complete
    private var pendingSaslState: SaslState? = null

    // Exception that will be thrown by `authenticate()` when SaslState is set to FAILED after
    // outbound writes complete
    private var pendingException: AuthenticationException? = null

    private var saslServer: SaslServer? = null

    private lateinit var saslMechanism: String

    // buffers used in `authenticate`
    private val saslAuthRequestMaxReceiveSize: Int

    private var netInBuffer: NetworkReceive? = null

    private lateinit var netOutBuffer: Send

    private var authenticationFailureSend: Send? = null

    // flag indicating if sasl tokens are sent as Kafka SaslAuthenticate request/responses
    private var enableKafkaSaslAuthenticateHeaders = false

    init {
        val enabledMechanisms =
            configs[BrokerSecurityConfigs.SASL_ENABLED_MECHANISMS_CONFIG] as List<String>?
        require(!enabledMechanisms.isNullOrEmpty()) { "No SASL mechanisms are enabled" }
        this.enabledMechanisms = enabledMechanisms

        this.enabledMechanisms.forEach { mechanism ->
            require(callbackHandlers.containsKey(mechanism)) {
                "Callback handler not specified for SASL mechanism $mechanism"
            }
            require(subjects.containsKey(mechanism)) {
                "Subject cannot be null for SASL mechanism $mechanism"
            }

            log.trace(
                "{} for mechanism={}: {}",
                BrokerSecurityConfigs.CONNECTIONS_MAX_REAUTH_MS,
                mechanism,
                connectionsMaxReauthMsByMechanism[mechanism]
            )
        }

        // Note that the old principal builder does not support SASL, so we do not need to pass the
        // authenticator or the transport layer
        principalBuilder = ChannelBuilders.createPrincipalBuilder(
            configs = configs,
            kerberosShortNamer = kerberosNameParser,
        )
        saslAuthRequestMaxReceiveSize =
            configs[BrokerSecurityConfigs.SASL_SERVER_MAX_RECEIVE_SIZE_CONFIG] as Int?
                ?: BrokerSecurityConfigs.DEFAULT_SASL_SERVER_MAX_RECEIVE_SIZE
    }

    @Throws(IOException::class)
    private fun createSaslServer(mechanism: String) {
        saslMechanism = mechanism
        val subject = subjects[mechanism]!!
        val callbackHandler = callbackHandlers[mechanism]

        if (mechanism == SaslConfigs.GSSAPI_MECHANISM)
            saslServer = createSaslKerberosServer(callbackHandler, configs, subject)
        else try {
            saslServer = Subject.doAs(
                subject,
                PrivilegedExceptionAction {
                    Sasl.createSaslServer(
                        saslMechanism,
                        "kafka",
                        serverAddress().hostName,
                        configs,
                        callbackHandler
                    )
                } as PrivilegedExceptionAction<SaslServer>,
            )

            if (saslServer == null) throw SaslException(
                "Kafka Server failed to create a SaslServer to interact with a client during " +
                        "session authentication with server mechanism $saslMechanism"
            )
        } catch (e: PrivilegedActionException) {
            throw SaslException(
                "Kafka Server failed to create a SaslServer to interact with a client during " +
                        "session authentication with server mechanism $saslMechanism",
                e.cause
            )
        }
    }

    @Throws(IOException::class)
    private fun createSaslKerberosServer(
        saslServerCallbackHandler: AuthenticateCallbackHandler?,
        configs: Map<String, *>,
        subject: Subject
    ): SaslServer {
        // server is using a JAAS-authenticated subject: determine service principal name and
        // hostname from kafka server's subject.
        val servicePrincipal = SaslClientAuthenticator.firstPrincipal(subject)

        val kerberosName: KerberosName = try {
            KerberosName.parse(servicePrincipal)
        } catch (e: IllegalArgumentException) {
            throw KafkaException("Principal has name with unexpected format $servicePrincipal")
        }

        val servicePrincipalName = kerberosName.serviceName()
        val serviceHostname = kerberosName.hostName()
        log.debug("Creating SaslServer for {} with mechanism {}", kerberosName, saslMechanism)

        return try {
            Subject.doAs(subject,
                PrivilegedExceptionAction {
                    Sasl.createSaslServer(
                        saslMechanism,
                        servicePrincipalName,
                        serviceHostname,
                        configs,
                        saslServerCallbackHandler
                    )
                } as PrivilegedExceptionAction<SaslServer>)
        } catch (e: PrivilegedActionException) {
            throw SaslException(
                "Kafka Server failed to create a SaslServer to interact with a client during " +
                        "session authentication",
                e.cause
            )
        }
    }

    /**
     * Evaluates client responses via `SaslServer.evaluateResponse` and returns the issued challenge
     * to the client until authentication succeeds or fails.
     *
     * The messages are sent and received as size delimited bytes that consists of a 4 byte
     * network-ordered size N followed by N bytes representing the opaque payload.
     */
    @Throws(IOException::class)
    override fun authenticate() {
        if (saslState != SaslState.REAUTH_PROCESS_HANDSHAKE) {
            if (::netOutBuffer.isInitialized && !flushNetOutBufferAndUpdateInterestOps()) return

            if (saslServer?.isComplete == true) {
                setSaslState(SaslState.COMPLETE)
                return
            }

            // allocate on heap (as opposed to any socket server memory pool)
            if (netInBuffer == null) netInBuffer = NetworkReceive(
                maxSize = saslAuthRequestMaxReceiveSize,
                source = connectionId
            )

            try {
                netInBuffer!!.readFrom(transportLayer)
            } catch (e: InvalidReceiveException) {
                throw SaslAuthenticationException(
                    "Failing SASL authentication due to invalid receive size",
                    e
                )
            }

            if (!netInBuffer!!.complete()) return
            netInBuffer!!.payload()!!.rewind()
        }

        val clientToken = ByteArray(netInBuffer!!.payload()!!.remaining())
        netInBuffer!!.payload()!![clientToken, 0, clientToken.size]
        this.netInBuffer = null // reset the networkReceive as we read all the data.

        try {
            when (saslState) {
                SaslState.REAUTH_PROCESS_HANDSHAKE,
                SaslState.HANDSHAKE_OR_VERSIONS_REQUEST,
                SaslState.HANDSHAKE_REQUEST -> handleKafkaRequest(clientToken)

                SaslState.REAUTH_BAD_MECHANISM ->
                    throw SaslAuthenticationException(reauthInfo.badMechanismErrorMessage)

                SaslState.INITIAL_REQUEST -> {
                    if (handleKafkaRequest(clientToken)) return

                    handleSaslToken(clientToken)
                    // When the authentication exchange is complete and no more tokens are expected
                    // from the client, update SASL state. Current SASL state will be updated when
                    // outgoing writes to the client complete.
                    if (saslServer!!.isComplete) setSaslState(SaslState.COMPLETE)
                }

                SaslState.AUTHENTICATE -> {
                    handleSaslToken(clientToken)
                    if (saslServer!!.isComplete) setSaslState(SaslState.COMPLETE)
                }

                else -> {}
            }
        } catch (e: AuthenticationException) {
            // Exception will be propagated after response is sent to client
            setSaslState(SaslState.FAILED, e)
        } catch (e: Exception) {
            // In the case of IOExceptions and other unexpected exceptions, fail immediately
            saslState = SaslState.FAILED
            log.debug(
                "Failed during {}: {}",
                reauthInfo.authenticationOrReauthenticationText(),
                e.message
            )
            throw e
        }
    }

    override fun principal(): KafkaPrincipal? {
        val sslSession =
            if (transportLayer is SslTransportLayer) transportLayer.sslSession()
            else null

        val context = SaslAuthenticationContext(
            saslServer!!,
            securityProtocol,
            clientAddress(),
            listenerName.value(),
            sslSession
        )
        val principal = principalBuilder.build(context)
        if (ScramMechanism.isScram(saslMechanism) && java.lang.Boolean.parseBoolean(
                saslServer!!.getNegotiatedProperty(ScramLoginModule.TOKEN_AUTH_CONFIG) as String
            )
        ) {
            principal.tokenAuthenticated(true)
        }
        return principal
    }

    override fun principalSerde(): KafkaPrincipalSerde? =
        principalBuilder as? KafkaPrincipalSerde

    override fun complete(): Boolean = saslState == SaslState.COMPLETE

    @Throws(IOException::class)
    override fun handleAuthenticationFailure() {
        // Send any authentication failure response that may have been previously built.
        authenticationFailureSend?.let {
            sendKafkaResponse(it)
            authenticationFailureSend = null
        }
    }

    @Throws(IOException::class)
    override fun close() {
        if (principalBuilder is Closeable) closeQuietly(
            principalBuilder as Closeable,
            "principal builder"
        )
        if (saslServer != null) saslServer!!.dispose()
    }

    @Throws(IOException::class)
    override fun reauthenticate(reauthenticationContext: ReauthenticationContext) {
        val saslHandshakeReceive = reauthenticationContext.networkReceive()
            ?: throw IllegalArgumentException(
                "Invalid saslHandshakeReceive in server-side re-authentication context: null"
            )
        val previousSaslServerAuthenticator =
            reauthenticationContext.previousAuthenticator() as SaslServerAuthenticator
        reauthInfo.reauthenticating(
            previousSaslServerAuthenticator.saslMechanism,
            previousSaslServerAuthenticator.principal(),
            reauthenticationContext.reauthenticationBeginNanos()
        )
        previousSaslServerAuthenticator.close()
        netInBuffer = saslHandshakeReceive
        log.debug("Beginning re-authentication: {}", this)
        netInBuffer!!.payload()!!.rewind()
        setSaslState(SaslState.REAUTH_PROCESS_HANDSHAKE)
        authenticate()
    }

    override fun serverSessionExpirationTimeNanos(): Long? {
        return reauthInfo.sessionExpirationTimeNanos
    }

    override fun reauthenticationLatencyMs(): Long? {
        return reauthInfo.reauthenticationLatencyMs()
    }

    override fun connectedClientSupportsReauthentication(): Boolean {
        return reauthInfo.connectedClientSupportsReauthentication
    }

    private fun setSaslState(saslState: SaslState) {
        setSaslState(saslState, null)
    }

    private fun setSaslState(saslState: SaslState, exception: AuthenticationException?) {
        if (::netOutBuffer.isInitialized && !netOutBuffer.completed()) {
            pendingSaslState = saslState
            pendingException = exception
        } else {
            this.saslState = saslState
            log.debug(
                "Set SASL server state to {} during {}",
                saslState,
                reauthInfo.authenticationOrReauthenticationText()
            )
            pendingSaslState = null
            pendingException = null
            if (exception != null) throw exception
        }
    }

    @Throws(IOException::class)
    private fun flushNetOutBufferAndUpdateInterestOps(): Boolean {
        val flushedCompletely = flushNetOutBuffer()
        if (flushedCompletely) {
            transportLayer.removeInterestOps(SelectionKey.OP_WRITE)
            if (pendingSaslState != null) setSaslState(pendingSaslState!!, pendingException)
        } else transportLayer.addInterestOps(SelectionKey.OP_WRITE)
        return flushedCompletely
    }

    @Throws(IOException::class)
    private fun flushNetOutBuffer(): Boolean {
        if (!netOutBuffer.completed()) netOutBuffer.writeTo(transportLayer)
        return netOutBuffer.completed()
    }

    private fun serverAddress(): InetAddress {
        return transportLayer.socketChannel()!!.socket().localAddress
    }

    private fun clientAddress(): InetAddress {
        return transportLayer.socketChannel()!!.socket().inetAddress
    }

    @Throws(IOException::class)
    private fun handleSaslToken(clientToken: ByteArray) {
        if (!enableKafkaSaslAuthenticateHeaders) {
            val response = saslServer!!.evaluateResponse(clientToken)
            if (saslServer!!.isComplete) {
                reauthInfo.calcCompletionTimesAndReturnSessionLifetimeMs()
                if (reauthInfo.reauthenticating()) reauthInfo.ensurePrincipalUnchanged(principal())
            }
            if (response != null) {
                netOutBuffer = ByteBufferSend.sizePrefixed(ByteBuffer.wrap(response))
                flushNetOutBufferAndUpdateInterestOps()
            }
        } else {
            val requestBuffer = ByteBuffer.wrap(clientToken)
            val header = RequestHeader.parse(requestBuffer)
            val apiKey = header.apiKey()
            val version = header.apiVersion()
            val requestContext = RequestContext(
                header,
                connectionId, clientAddress(),
                KafkaPrincipal.ANONYMOUS,
                listenerName,
                securityProtocol, ClientInformation.EMPTY, false
            )
            val requestAndSize = requestContext.parseRequest(requestBuffer)
            if (apiKey != ApiKeys.SASL_AUTHENTICATE) {
                val e = IllegalSaslStateException(
                    "Unexpected Kafka request of type $apiKey during SASL authentication."
                )

                buildResponseOnAuthenticateFailure(
                    requestContext,
                    requestAndSize.request.getErrorResponse(e)
                )
                throw e
            }
            if (!apiKey.isVersionSupported(version)) {
                // We cannot create an error response if the request version of SaslAuthenticate is
                // not supported. This should not normally occur since clients typically check
                // supported versions using ApiVersionsRequest
                throw UnsupportedVersionException("Version $version is not supported for apiKey $apiKey")
            }

            // The client sends multiple SASL_AUTHENTICATE requests, and the client is known
            // to support the required version if any one of them indicates it supports that
            // version.
            if (!reauthInfo.connectedClientSupportsReauthentication)
                 reauthInfo.connectedClientSupportsReauthentication = version > 0

            val saslAuthenticateRequest = requestAndSize.request as SaslAuthenticateRequest

            try {
                val saslServer = this.saslServer!!

                val responseToken = saslServer.evaluateResponse(
                    saslAuthenticateRequest.data().authBytes().copyOf()
                )

                if (reauthInfo.reauthenticating() && saslServer.isComplete)
                    reauthInfo.ensurePrincipalUnchanged(principal())

                // For versions with SASL_AUTHENTICATE header, send a response to SASL_AUTHENTICATE
                // request even if token is empty.
                val responseBytes = responseToken ?: ByteArray(0)
                val sessionLifetimeMs =
                    if (!saslServer.isComplete) 0L
                    else reauthInfo.calcCompletionTimesAndReturnSessionLifetimeMs()

                sendKafkaResponse(
                    requestContext, SaslAuthenticateResponse(
                        SaslAuthenticateResponseData()
                            .setErrorCode(Errors.NONE.code)
                            .setAuthBytes(responseBytes)
                            .setSessionLifetimeMs(sessionLifetimeMs)
                    )
                )
            } catch (e: SaslAuthenticationException) {
                buildResponseOnAuthenticateFailure(
                    requestContext,
                    SaslAuthenticateResponse(
                        SaslAuthenticateResponseData()
                            .setErrorCode(Errors.SASL_AUTHENTICATION_FAILED.code)
                            .setErrorMessage(e.message)
                    )
                )
                throw e
            } catch (e: SaslException) {
                val kerberosError = KerberosError.fromException(e)
                if (kerberosError != null && kerberosError.retriable()) {
                    // Handle retriable Kerberos exceptions as I/O exceptions rather than
                    // authentication exceptions
                    throw e
                } else {
                    // DO NOT include error message from the `SaslException` in the client response
                    // since it may contain sensitive data like the existence of the user.
                    val errorMessage = ("Authentication failed during " +
                            reauthInfo.authenticationOrReauthenticationText() +
                            " due to invalid credentials with SASL mechanism $saslMechanism")
                    buildResponseOnAuthenticateFailure(
                        requestContext, SaslAuthenticateResponse(
                            SaslAuthenticateResponseData()
                                .setErrorCode(Errors.SASL_AUTHENTICATION_FAILED.code)
                                .setErrorMessage(errorMessage)
                        )
                    )
                    throw SaslAuthenticationException(errorMessage, e)
                }
            }
        }
    }

    @Throws(IOException::class, AuthenticationException::class)
    private fun handleKafkaRequest(requestBytes: ByteArray): Boolean {
        var isKafkaRequest = false
        var clientMechanism: String? = null
        try {
            val requestBuffer = ByteBuffer.wrap(requestBytes)
            val header = RequestHeader.parse(requestBuffer)
            val apiKey = header.apiKey()

            // A valid Kafka request header was received. SASL authentication tokens are now
            // expected only following a SaslHandshakeRequest since this is not a GSSAPI client
            // token from a Kafka 0.9.0.x client.
            if (saslState == SaslState.INITIAL_REQUEST)
                setSaslState(SaslState.HANDSHAKE_OR_VERSIONS_REQUEST)

            isKafkaRequest = true

            // Raise an error prior to parsing if the api cannot be handled at this layer. This
            // avoids unnecessary exposure to some of the more complex schema types.
            if (apiKey != ApiKeys.API_VERSIONS && apiKey != ApiKeys.SASL_HANDSHAKE)
                throw IllegalSaslStateException(
                    "Unexpected Kafka request of type $apiKey during SASL handshake."
                )

            log.debug(
                "Handling Kafka request {} during {}",
                apiKey,
                reauthInfo.authenticationOrReauthenticationText()
            )
            val requestContext = RequestContext(
                header,
                connectionId, clientAddress(),
                KafkaPrincipal.ANONYMOUS,
                listenerName,
                securityProtocol, ClientInformation.EMPTY, false
            )
            val requestAndSize = requestContext.parseRequest(requestBuffer)
            if (apiKey == ApiKeys.API_VERSIONS) handleApiVersionsRequest(
                requestContext,
                requestAndSize.request as ApiVersionsRequest
            ) else clientMechanism = handleHandshakeRequest(
                requestContext,
                requestAndSize.request as SaslHandshakeRequest
            )
        } catch (e: InvalidRequestException) {
            clientMechanism = if (saslState == SaslState.INITIAL_REQUEST) {
                // InvalidRequestException is thrown if the request is not in Kafka format or if the
                // API key is invalid. For compatibility with 0.9.0.x where the first packet is a
                // GSSAPI token starting with 0x60, revert to GSSAPI for both these exceptions.
                if (log.isDebugEnabled) {
                    val tokenBuilder = StringBuilder()
                    for (b in requestBytes) {
                        tokenBuilder.append(String.format("%02x", b))
                        if (tokenBuilder.length >= 20) break
                    }
                    log.debug(
                        "Received client packet of length {} starting with bytes 0x{}, process " +
                                "as GSSAPI packet",
                        requestBytes.size,
                        tokenBuilder
                    )
                }
                if (enabledMechanisms.contains(SaslConfigs.GSSAPI_MECHANISM)) {
                    log.debug("First client packet is not a SASL mechanism request, using default " +
                            "mechanism GSSAPI")
                    SaslConfigs.GSSAPI_MECHANISM
                } else throw UnsupportedSaslMechanismException(
                    "Exception handling first SASL packet from client, GSSAPI is not supported " +
                            "by server",
                    e
                )
            } else throw e
        }
        if (
            clientMechanism != null
            && (!reauthInfo.reauthenticating() || reauthInfo.saslMechanismUnchanged(clientMechanism))
        ) {
            createSaslServer(clientMechanism)
            setSaslState(SaslState.AUTHENTICATE)
        }
        return isKafkaRequest
    }

    @Throws(IOException::class, UnsupportedSaslMechanismException::class)
    private fun handleHandshakeRequest(
        context: RequestContext,
        handshakeRequest: SaslHandshakeRequest
    ): String {
        val clientMechanism = handshakeRequest.data().mechanism()
        val version = context.header.apiVersion()
        if (version >= 1) enableKafkaSaslAuthenticateHeaders(true)
        return if (enabledMechanisms.contains(clientMechanism)) {
            log.debug("Using SASL mechanism '{}' provided by client", clientMechanism)
            sendKafkaResponse(
                context, SaslHandshakeResponse(
                    SaslHandshakeResponseData().setErrorCode(Errors.NONE.code)
                        .setMechanisms(enabledMechanisms)
                )
            )
            clientMechanism
        } else {
            log.debug("SASL mechanism '{}' requested by client is not supported", clientMechanism)
            buildResponseOnAuthenticateFailure(
                context, SaslHandshakeResponse(
                    SaslHandshakeResponseData().setErrorCode(Errors.UNSUPPORTED_SASL_MECHANISM.code)
                        .setMechanisms(enabledMechanisms)
                )
            )
            throw UnsupportedSaslMechanismException("Unsupported SASL mechanism $clientMechanism")
        }
    }

    // Visible to override for testing
    protected open fun enableKafkaSaslAuthenticateHeaders(flag: Boolean) {
        enableKafkaSaslAuthenticateHeaders = flag
    }

    @Throws(IOException::class)
    private fun handleApiVersionsRequest(
        context: RequestContext,
        apiVersionsRequest: ApiVersionsRequest
    ) {
        check(saslState == SaslState.HANDSHAKE_OR_VERSIONS_REQUEST) {
            "Unexpected ApiVersions request received during SASL authentication state $saslState"
        }

        if (apiVersionsRequest.hasUnsupportedRequestVersion()) sendKafkaResponse(
            context,
            apiVersionsRequest.getErrorResponse(0, Errors.UNSUPPORTED_VERSION.exception)
        ) else if (!apiVersionsRequest.isValid) sendKafkaResponse(
            context,
            apiVersionsRequest.getErrorResponse(0, Errors.INVALID_REQUEST.exception)
        ) else {
            metadataRegistry.registerClientInformation(
                ClientInformation(
                    apiVersionsRequest.data().clientSoftwareName(),
                    apiVersionsRequest.data().clientSoftwareVersion()
                )
            )
            sendKafkaResponse(context, apiVersionSupplier.get())
            setSaslState(SaslState.HANDSHAKE_REQUEST)
        }
    }

    /**
     * Build a [Send] response on [authenticate] failure. The actual response is sent out when
     * [handleAuthenticationFailure] is called.
     */
    private fun buildResponseOnAuthenticateFailure(
        context: RequestContext,
        response: AbstractResponse
    ) {
        authenticationFailureSend = context.buildResponseSend(response)
    }

    @Throws(IOException::class)
    private fun sendKafkaResponse(context: RequestContext, response: AbstractResponse) {
        sendKafkaResponse(context.buildResponseSend(response))
    }

    @Throws(IOException::class)
    private fun sendKafkaResponse(send: Send) {
        netOutBuffer = send
        flushNetOutBufferAndUpdateInterestOps()
    }

    /**
     * Information related to re-authentication
     */
    private inner class ReauthInfo {

        var previousSaslMechanism: String? = null

        var previousKafkaPrincipal: KafkaPrincipal? = null

        var reauthenticationBeginNanos: Long = 0

        var sessionExpirationTimeNanos: Long? = null

        var connectedClientSupportsReauthentication = false

        var authenticationEndNanos: Long = 0

        var badMechanismErrorMessage: String? = null

        fun reauthenticating(
            previousSaslMechanism: String?, previousKafkaPrincipal: KafkaPrincipal?,
            reauthenticationBeginNanos: Long
        ) {
            this.previousSaslMechanism = Objects.requireNonNull(previousSaslMechanism)
            this.previousKafkaPrincipal = Objects.requireNonNull(previousKafkaPrincipal)
            this.reauthenticationBeginNanos = reauthenticationBeginNanos
        }

        fun reauthenticating(): Boolean {
            return previousSaslMechanism != null
        }

        fun authenticationOrReauthenticationText(): String {
            return if (reauthenticating()) "re-authentication" else "authentication"
        }

        @Throws(SaslAuthenticationException::class)
        fun ensurePrincipalUnchanged(reauthenticatedKafkaPrincipal: KafkaPrincipal?) {
            if (previousKafkaPrincipal != reauthenticatedKafkaPrincipal) {
                throw SaslAuthenticationException(
                    String.format(
                        "Cannot change principals during re-authentication from %s.%s: %s.%s",
                        previousKafkaPrincipal!!.principalType,
                        previousKafkaPrincipal!!.name,
                        reauthenticatedKafkaPrincipal!!.principalType,
                        reauthenticatedKafkaPrincipal.name
                    )
                )
            }
        }

        /*
         * We define the REAUTH_BAD_MECHANISM state because the failed re-authentication
         * metric does not get updated if we send back an error immediately upon the
         * start of re-authentication.
         */
        fun saslMechanismUnchanged(clientMechanism: String): Boolean {
            if (previousSaslMechanism == clientMechanism) return true

            badMechanismErrorMessage = String.format(
                "SASL mechanism '%s' requested by client is not supported for re-authentication " +
                        "of mechanism '%s'",
                clientMechanism,
                previousSaslMechanism
            )
            log.debug(badMechanismErrorMessage)

            setSaslState(SaslState.REAUTH_BAD_MECHANISM)

            return false
        }

        fun calcCompletionTimesAndReturnSessionLifetimeMs(): Long {
            var retvalSessionLifetimeMs = 0L
            val authenticationEndMs = time.milliseconds()
            authenticationEndNanos = time.nanoseconds()

            val credentialExpirationMs = saslServer!!.getNegotiatedProperty(
                SaslInternalConfigs.CREDENTIAL_LIFETIME_MS_SASL_NEGOTIATED_PROPERTY_KEY
            ) as Long?

            val connectionsMaxReauthMs = connectionsMaxReauthMsByMechanism[saslMechanism]
            val maxReauthSet = connectionsMaxReauthMs != null && connectionsMaxReauthMs > 0

            if (credentialExpirationMs != null || maxReauthSet) {
                retvalSessionLifetimeMs = (
                        if (credentialExpirationMs == null) connectionsMaxReauthMs!!
                        else if (!maxReauthSet) credentialExpirationMs - authenticationEndMs
                        else (credentialExpirationMs - authenticationEndMs)
                            .coerceAtMost(connectionsMaxReauthMs!!)
                        ).coerceAtLeast(0)

                sessionExpirationTimeNanos =
                    authenticationEndNanos + 1000 * 1000 * retvalSessionLifetimeMs
            }

            if (credentialExpirationMs != null) {
                log.debug(
                    "Authentication complete; session max lifetime from broker config={} ms, " +
                            "credential expiration={} ({} ms); session expiration = {} ({} ms), " +
                            "sending {} ms to client",
                    connectionsMaxReauthMs,
                    Date(credentialExpirationMs),
                    credentialExpirationMs - authenticationEndMs,
                    Date(authenticationEndMs + retvalSessionLifetimeMs),
                    retvalSessionLifetimeMs,
                    retvalSessionLifetimeMs,
                )
            } else {
                if (sessionExpirationTimeNanos != null) log.debug(
                    "Authentication complete; session max lifetime from broker config={} ms, no " +
                            "credential expiration; session expiration = {} ({} ms), sending {} " +
                            "ms to client",
                    connectionsMaxReauthMs,
                    Date(authenticationEndMs + retvalSessionLifetimeMs),
                    retvalSessionLifetimeMs,
                    retvalSessionLifetimeMs,
                ) else log.debug(
                    "Authentication complete; session max lifetime from broker config={} ms, no " +
                            "credential expiration; no session expiration, sending 0 ms to client",
                    connectionsMaxReauthMs
                )
            }
            return retvalSessionLifetimeMs
        }

        fun reauthenticationLatencyMs(): Long? {
            if (!reauthenticating()) return null
            // record at least 1 ms if there is some latency
            val latencyNanos = authenticationEndNanos - reauthenticationBeginNanos
            return if (latencyNanos == 0L) 0L
            else (latencyNanos / 1000.0 / 1000.0).roundToLong().coerceAtLeast(1)
        }
    }

    companion object {
        private val log = LoggerFactory.getLogger(SaslServerAuthenticator::class.java)
    }
}
