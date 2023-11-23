package org.apache.kafka.common.security.authenticator

import java.io.IOException
import java.nio.BufferUnderflowException
import java.nio.ByteBuffer
import java.nio.channels.SelectionKey
import java.security.Principal
import java.security.PrivilegedActionException
import java.security.PrivilegedExceptionAction
import javax.security.auth.Subject
import javax.security.sasl.Sasl
import javax.security.sasl.SaslClient
import javax.security.sasl.SaslException
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.NetworkClient
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.config.SaslConfigs
import org.apache.kafka.common.errors.IllegalSaslStateException
import org.apache.kafka.common.errors.SaslAuthenticationException
import org.apache.kafka.common.errors.UnsupportedSaslMechanismException
import org.apache.kafka.common.message.RequestHeaderData
import org.apache.kafka.common.message.SaslAuthenticateRequestData
import org.apache.kafka.common.message.SaslHandshakeRequestData
import org.apache.kafka.common.network.Authenticator
import org.apache.kafka.common.network.ByteBufferSend
import org.apache.kafka.common.network.NetworkReceive
import org.apache.kafka.common.network.ReauthenticationContext
import org.apache.kafka.common.network.Send
import org.apache.kafka.common.network.TransportLayer
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.protocol.types.SchemaException
import org.apache.kafka.common.requests.AbstractResponse
import org.apache.kafka.common.requests.ApiVersionsRequest
import org.apache.kafka.common.requests.ApiVersionsResponse
import org.apache.kafka.common.requests.RequestHeader
import org.apache.kafka.common.requests.SaslAuthenticateRequest
import org.apache.kafka.common.requests.SaslAuthenticateResponse
import org.apache.kafka.common.requests.SaslHandshakeRequest
import org.apache.kafka.common.requests.SaslHandshakeResponse
import org.apache.kafka.common.security.auth.AuthenticateCallbackHandler
import org.apache.kafka.common.security.auth.KafkaPrincipal
import org.apache.kafka.common.security.auth.KafkaPrincipalSerde
import org.apache.kafka.common.security.kerberos.KerberosError
import org.apache.kafka.common.utils.LogContext
import org.apache.kafka.common.utils.Time
import org.apache.kafka.common.utils.Utils
import org.slf4j.Logger
import kotlin.math.roundToLong
import kotlin.random.Random

open class SaslClientAuthenticator(
    private val configs: Map<String, *>,
    private val callbackHandler: AuthenticateCallbackHandler,
    private val node: String,
    private val subject: Subject,
    private val servicePrincipal: String,
    private val host: String,
    private val mechanism: String,
    handshakeRequestEnable: Boolean,
    private val transportLayer: TransportLayer,
    private val time: Time,
    logContext: LogContext
) : Authenticator {

    private val saslClient: SaslClient

    private var clientPrincipalName: String? = null

    private val log: Logger = logContext.logger(javaClass)

    private val reauthInfo: ReauthInfo = ReauthInfo()

    /**
     * Buffers used in [authenticate].
     */
    private var netInBuffer: NetworkReceive? = null

    private var netOutBuffer: Send? = null

    /**
     * Current SASL state
     */
    private var saslState: SaslState? = null

    /**
     * Next SASL state to be set when outgoing writes associated with the current SASL state
     * complete.
     */
    private var pendingSaslState: SaslState? = null

    /**
     * Correlation ID for the next request.
     */
    private var correlationId = 0

    /**
     * Request header for which response from the server is pending.
     */
    private var currentRequestHeader: RequestHeader? = null

    /**
     * Version of SaslAuthenticate request/responses.
     */
    private var saslAuthenticateVersion: Short = DISABLE_KAFKA_SASL_AUTHENTICATE_HEADER

    /**
     * Version of SaslHandshake request/responses.
     */
    private var saslHandshakeVersion: Short = 0

    init {
        try {
            setSaslState(
                if (handshakeRequestEnable) SaslState.SEND_APIVERSIONS_REQUEST
                else SaslState.INITIAL
            )

            // determine client principal from subject for Kerberos to use as authorization id for
            // the SaslClient. For other mechanisms, the authenticated principal (username for PLAIN
            // and SCRAM) is used as authorization id. Hence the principal is not specified for
            // creating the SaslClient.
            clientPrincipalName =
                if (mechanism == SaslConfigs.GSSAPI_MECHANISM) firstPrincipal(subject)
                else null

            saslClient = createSaslClient()
        } catch (e: Exception) {
            throw SaslAuthenticationException("Failed to configure SaslClientAuthenticator", e)
        }
    }

    // visible for testing
    fun createSaslClient(): SaslClient = try {
        Subject.doAs(
            subject,
            PrivilegedExceptionAction {
                val mechs = arrayOf(mechanism)
                log.debug(
                    "Creating SaslClient: client={};service={};serviceHostname={};mechs={}",
                    clientPrincipalName,
                    servicePrincipal,
                    host,
                    mechs.contentToString(),
                )
                Sasl.createSaslClient(
                    mechs,
                    clientPrincipalName,
                    servicePrincipal,
                    host,
                    configs,
                    callbackHandler,
                ) ?: throw SaslAuthenticationException(
                    "Failed to create SaslClient with mechanism $mechanism"
                )
            } as PrivilegedExceptionAction<SaslClient>)
    } catch (e: PrivilegedActionException) {
        throw SaslAuthenticationException("Failed to create SaslClient with mechanism $mechanism", e.cause)
    }

    /**
     * Sends an empty message to the server to initiate the authentication process. It then
     * evaluates server challenges via `SaslClient.evaluateChallenge` and returns client responses
     * until authentication succeeds or fails.
     *
     * The messages are sent and received as size delimited bytes that consists of a 4 byte
     * network-ordered size N followed by N bytes representing the opaque payload.
     */
    @Throws(IOException::class)
    override fun authenticate() {
        if (netOutBuffer != null && !flushNetOutBufferAndUpdateInterestOps()) return

        when (saslState) {
            SaslState.SEND_APIVERSIONS_REQUEST -> {
                // Always use version 0 request since brokers treat requests with schema exceptions
                // as GSSAPI tokens
                val apiVersionsRequest = ApiVersionsRequest.Builder().build(0.toShort())
                send(apiVersionsRequest.toSend(
                    nextRequestHeader(
                        ApiKeys.API_VERSIONS,
                        apiVersionsRequest.version
                    )))
                setSaslState(SaslState.RECEIVE_APIVERSIONS_RESPONSE)
            }

            SaslState.RECEIVE_APIVERSIONS_RESPONSE -> {
                val apiVersionsResponse = receiveKafkaResponse() as ApiVersionsResponse? ?: return

                setSaslAuthenticateAndHandshakeVersions(apiVersionsResponse)
                reauthInfo.apiVersionsResponseFromBroker = apiVersionsResponse
                setSaslState(SaslState.SEND_HANDSHAKE_REQUEST)

                // Send handshake request with the latest supported version
                sendHandshakeRequest(saslHandshakeVersion)
                setSaslState(SaslState.RECEIVE_HANDSHAKE_RESPONSE)
            }

            SaslState.SEND_HANDSHAKE_REQUEST -> {
                sendHandshakeRequest(saslHandshakeVersion)
                setSaslState(SaslState.RECEIVE_HANDSHAKE_RESPONSE)
            }

            SaslState.RECEIVE_HANDSHAKE_RESPONSE -> {
                val handshakeResponse = receiveKafkaResponse() as SaslHandshakeResponse? ?: return

                handleSaslHandshakeResponse(handshakeResponse)
                setSaslState(SaslState.INITIAL)

                // Start SASL authentication using the configured client mechanism
                sendInitialToken()
                setSaslState(SaslState.INTERMEDIATE)
            }

            SaslState.INITIAL -> {
                sendInitialToken()
                setSaslState(SaslState.INTERMEDIATE)
            }

            SaslState.REAUTH_PROCESS_ORIG_APIVERSIONS_RESPONSE -> {
                setSaslAuthenticateAndHandshakeVersions(reauthInfo.apiVersionsResponseFromAuth!!)
                setSaslState(SaslState.REAUTH_SEND_HANDSHAKE_REQUEST) // Will set immediately

                // Send handshake request with the latest supported version
                sendHandshakeRequest(saslHandshakeVersion)
                setSaslState(SaslState.REAUTH_RECEIVE_HANDSHAKE_OR_OTHER_RESPONSE)
            }

            SaslState.REAUTH_SEND_HANDSHAKE_REQUEST -> {
                sendHandshakeRequest(saslHandshakeVersion)
                setSaslState(SaslState.REAUTH_RECEIVE_HANDSHAKE_OR_OTHER_RESPONSE)
            }

            SaslState.REAUTH_RECEIVE_HANDSHAKE_OR_OTHER_RESPONSE -> {
                val handshakeResponse = receiveKafkaResponse() as SaslHandshakeResponse? ?: return

                handleSaslHandshakeResponse(handshakeResponse)
                setSaslState(SaslState.REAUTH_INITIAL) // Will set immediately

                /*
                 * Start SASL authentication using the configured client mechanism. Note that we
                 * start SASL authentication to avoid adding a loop to enter the `when` statement
                 * again and therefore minimize the changes to authentication-related code due to
                 * the changes related to re-authentication.
                 */
                sendInitialToken()
                setSaslState(SaslState.INTERMEDIATE)
            }

            SaslState.REAUTH_INITIAL -> {
                sendInitialToken()
                setSaslState(SaslState.INTERMEDIATE)
            }

            SaslState.INTERMEDIATE -> {
                val serverToken = receiveToken()
                val noResponsesPending =
                    serverToken != null && !sendSaslClientToken(serverToken, false)

                // For versions without SASL_AUTHENTICATE header, SASL exchange may be complete
                // after a token is sent to server. For versions with SASL_AUTHENTICATE header,
                // server always sends a response to each SASL_AUTHENTICATE request.
                if (saslClient.isComplete) {
                    if (saslAuthenticateVersion == DISABLE_KAFKA_SASL_AUTHENTICATE_HEADER || noResponsesPending)
                        setSaslState(SaslState.COMPLETE)
                    else setSaslState(SaslState.CLIENT_COMPLETE)
                }
            }

            SaslState.CLIENT_COMPLETE -> {
                val serverResponse = receiveToken()
                if (serverResponse != null) setSaslState(SaslState.COMPLETE)
            }

            SaslState.COMPLETE -> {}
            SaslState.FAILED ->
                // Should never get here since exception would have been propagated earlier
                error("SASL handshake has already failed")
            null -> {}
        }
    }

    @Throws(IOException::class)
    private fun sendHandshakeRequest(version: Short) {
        val handshakeRequest = createSaslHandshakeRequest(version)
        send(
            handshakeRequest.toSend(
                nextRequestHeader(ApiKeys.SASL_HANDSHAKE, handshakeRequest.version)
            )
        )
    }

    @Throws(IOException::class)
    private fun sendInitialToken() {
        sendSaslClientToken(ByteArray(0), true)
    }

    @Throws(IOException::class)
    override fun reauthenticate(reauthenticationContext: ReauthenticationContext) {
        with(reauthenticationContext.previousAuthenticator() as SaslClientAuthenticator) {
            val apiVersionsResponseFromAuth = reauthInfo.apiVersionsResponse
            close()

            reauthInfo.reauthenticating(
                apiVersionsResponseFromAuth,
                reauthenticationContext.reauthenticationBeginNanos()
            )
        }
        val netInBufferFromChannel = reauthenticationContext.networkReceive()
        netInBuffer = netInBufferFromChannel
        setSaslState(SaslState.REAUTH_PROCESS_ORIG_APIVERSIONS_RESPONSE) // Will set immediately
        authenticate()
    }

    override fun pollResponseReceivedDuringReauthentication(): NetworkReceive? =
        reauthInfo.pollResponseReceivedDuringReauthentication()

    override fun clientSessionReauthenticationTimeNanos(): Long? =
        reauthInfo.clientSessionReauthenticationTime

    override fun reauthenticationLatencyMs(): Long? = reauthInfo.reauthenticationLatencyMs()

    // visible for testing
    fun nextCorrelationId(): Int {
        if (!isReserved(correlationId)) correlationId = MIN_RESERVED_CORRELATION_ID
        return correlationId++
    }

    private fun nextRequestHeader(apiKey: ApiKeys, version: Short): RequestHeader {
        val clientId = configs[CommonClientConfigs.CLIENT_ID_CONFIG] as String?
        val requestApiKey = apiKey.id
        return RequestHeader(
                RequestHeaderData()
                    .setRequestApiKey(requestApiKey)
                    .setRequestApiVersion(version)
                    .setClientId(clientId)
                    .setCorrelationId(nextCorrelationId()),
                apiKey.requestHeaderVersion(version)
            ).apply {
            currentRequestHeader = this
        }
    }

    // Visible to override for testing
    protected open fun createSaslHandshakeRequest(version: Short): SaslHandshakeRequest {
        return SaslHandshakeRequest.Builder(
            SaslHandshakeRequestData().setMechanism(mechanism)
        ).build(version)
    }

    // Visible to override for testing
    protected open fun setSaslAuthenticateAndHandshakeVersions(apiVersionsResponse: ApiVersionsResponse) {
        val authenticateVersion = apiVersionsResponse.apiVersion(ApiKeys.SASL_AUTHENTICATE.id)
        if (authenticateVersion != null) {
            saslAuthenticateVersion = authenticateVersion.maxVersion
                .coerceAtMost(ApiKeys.SASL_AUTHENTICATE.latestVersion())
        }
        val handshakeVersion = apiVersionsResponse.apiVersion(ApiKeys.SASL_HANDSHAKE.id)
        if (handshakeVersion != null) {
            saslHandshakeVersion = handshakeVersion.maxVersion
                .coerceAtMost(ApiKeys.SASL_HANDSHAKE.latestVersion())
        }
    }

    private fun setSaslState(saslState: SaslState) {
        if (netOutBuffer != null && !netOutBuffer!!.completed()) pendingSaslState = saslState else {
            pendingSaslState = null
            this.saslState = saslState
            log.debug("Set SASL client state to {}", saslState)
            if (saslState == SaslState.COMPLETE) {
                reauthInfo.setAuthenticationEndAndSessionReauthenticationTimes(time.nanoseconds())
                if (!reauthInfo.isReauthenticating) transportLayer.removeInterestOps(SelectionKey.OP_WRITE)
                /*
                 * Re-authentication is triggered by a write, so we have to make sure that
                 * pending write is actually sent.
                 */
                else transportLayer.addInterestOps(SelectionKey.OP_WRITE)
            }
        }
    }

    /**
     * Sends a SASL client token to server if required. This may be an initial token to start
     * SASL token exchange or response to a challenge from the server.
     * @return true if a token was sent to the server
     */
    @Throws(IOException::class)
    private fun sendSaslClientToken(serverToken: ByteArray, isInitial: Boolean): Boolean {
        return if (!saslClient.isComplete) {
            val saslToken = createSaslToken(serverToken, isInitial) ?: return false
            val tokenBuf = ByteBuffer.wrap(saslToken)

            if (saslAuthenticateVersion == DISABLE_KAFKA_SASL_AUTHENTICATE_HEADER)
                ByteBufferSend.sizePrefixed(tokenBuf)
            else {
                val data = SaslAuthenticateRequestData()
                    .setAuthBytes(tokenBuf.array())
                val request = SaslAuthenticateRequest.Builder(data).build(saslAuthenticateVersion)
                request.toSend(nextRequestHeader(ApiKeys.SASL_AUTHENTICATE, saslAuthenticateVersion))
            }.also { send(it) }
            true
        } else false
    }

    @Throws(IOException::class)
    private fun send(send: Send) {
        try {
            netOutBuffer = send
            flushNetOutBufferAndUpdateInterestOps()
        } catch (e: IOException) {
            setSaslState(SaslState.FAILED)
            throw e
        }
    }

    @Throws(IOException::class)
    private fun flushNetOutBufferAndUpdateInterestOps(): Boolean {
        val flushedCompletely = flushNetOutBuffer()
        if (flushedCompletely) {
            transportLayer.removeInterestOps(SelectionKey.OP_WRITE)
            if (pendingSaslState != null) setSaslState(pendingSaslState!!)
        } else transportLayer.addInterestOps(SelectionKey.OP_WRITE)
        return flushedCompletely
    }

    @Throws(IOException::class)
    private fun receiveResponseOrToken(): ByteArray? {
        val buffer =  netInBuffer ?: NetworkReceive(node)
        netInBuffer = buffer
        buffer.readFrom(transportLayer)
        var serverPacket: ByteArray? = null
        if (buffer.complete()) {
            buffer.payload()?.let { payload ->
                payload.rewind()
                val packet = ByteArray(payload.remaining())
                payload[packet, 0, packet.size]
                serverPacket = packet
            }
            netInBuffer = null // reset the networkReceive as we read all the data.
        }
        return serverPacket
    }

    override fun principal(): KafkaPrincipal {
        return KafkaPrincipal(KafkaPrincipal.USER_TYPE, requireNotNull(clientPrincipalName))
    }

    override fun principalSerde(): KafkaPrincipalSerde? = null

    override fun complete(): Boolean {
        return saslState == SaslState.COMPLETE
    }

    @Throws(IOException::class)
    override fun close() {
        saslClient.dispose()
    }

    @Throws(IOException::class)
    private fun receiveToken(): ByteArray? {
        if (saslAuthenticateVersion == DISABLE_KAFKA_SASL_AUTHENTICATE_HEADER)
            return receiveResponseOrToken()

        return (receiveKafkaResponse() as SaslAuthenticateResponse?)?.let { response ->
            val error = response.error()
            if (error != Errors.NONE) {
                setSaslState(SaslState.FAILED)
                val errMsg = response.errorMessage()
                throw if (errMsg == null) error.exception!! else error.exception(errMsg)
            }
            val sessionLifetimeMs = response.sessionLifetimeMs()
            if (sessionLifetimeMs > 0L) reauthInfo.positiveSessionLifetimeMs = sessionLifetimeMs
            Utils.copyArray(response.saslAuthBytes())
        } ?: run { null }
    }

    @Throws(SaslException::class)
    private fun createSaslToken(saslToken: ByteArray?, isInitial: Boolean): ByteArray? {
        if (saslToken == null) throw IllegalSaslStateException(
            "Error authenticating with the Kafka Broker: received a `null` saslToken."
        )

        return try {
            if (isInitial && !saslClient.hasInitialResponse()) saslToken else Subject.doAs(
                subject,
                PrivilegedExceptionAction {
                    saslClient.evaluateChallenge(saslToken)
                } as PrivilegedExceptionAction<ByteArray>)
        } catch (e: PrivilegedActionException) {
            var error = "An error: ($e) occurred when evaluating SASL token received from the Kafka Broker."

            val kerberosError = KerberosError.fromException(e)
            // Try to provide hints to use about what went wrong, so they can fix their configuration.
            if (kerberosError == KerberosError.SERVER_NOT_FOUND) {
                error += " This may be caused by Java's being unable to resolve the Kafka Broker's " +
                        "hostname correctly. You may want to try to adding " +
                        "'-Dsun.net.spi.nameservice.provider.1=dns,sun' to your client's JVMFLAGS " +
                        "environment. Users must configure FQDN of kafka brokers when " +
                        "authenticating using SASL and " +
                        "`socketChannel.socket().getInetAddress().getHostName()` must match the " +
                        "hostname in `principal/hostname@realm`"
            }
            //Unwrap the SaslException inside `PrivilegedActionException`
            val cause = e.cause
            // Treat transient Kerberos errors as non-fatal SaslExceptions that are processed as I/O exceptions
            // and all other failures as fatal SaslAuthenticationException.
            throw if (
                kerberosError != null
                && (kerberosError.retriable() || KerberosError.isRetriableClientGssException(e))
            ) SaslException("$error Kafka Client will retry.", cause)
            else SaslAuthenticationException(
                "$error Kafka Client will go to AUTHENTICATION_FAILED state.",
                cause,
            )
        }
    }

    @Throws(IOException::class)
    private fun flushNetOutBuffer(): Boolean {
        val buffer = netOutBuffer ?: return false
        if (!buffer.completed()) buffer.writeTo(transportLayer)
        return buffer.completed()
    }

    @Throws(IOException::class)
    private fun receiveKafkaResponse(): AbstractResponse? {
        val receive: NetworkReceive = netInBuffer ?: NetworkReceive(node)
        netInBuffer = receive
        return try {
            val responseBytes = receiveResponseOrToken()
            if (responseBytes == null) null else {
                val response = NetworkClient.parseResponse(
                    ByteBuffer.wrap(responseBytes),
                    currentRequestHeader!!
                )
                currentRequestHeader = null
                response
            }
        } catch (e: BufferUnderflowException) {
            handleKafkaResponseException(receive, e)
            return null
        } catch (e: SchemaException) {
            handleKafkaResponseException(receive, e)
            return null
        } catch (e: IllegalArgumentException) {
            handleKafkaResponseException(receive, e)
            return null
        }
    }

    private fun handleKafkaResponseException(receive: NetworkReceive, exception: Exception) {
        // Account for the fact that during re-authentication there may be responses
        // arriving for requests that were sent in the past.
        if (reauthInfo.isReauthenticating) {
            // It didn't match the current request header, so it must be unrelated to
            // re-authentication. Save it so it can be processed later.
            receive.payload()?.rewind()
            reauthInfo.pendingAuthenticatedReceives.add(receive)
            return
        }
        log.debug("Invalid SASL mechanism response, server may be expecting only GSSAPI tokens")
        setSaslState(SaslState.FAILED)
        throw IllegalSaslStateException(
            "Invalid SASL mechanism response, server may be expecting a different protocol",
            exception
        )
    }

    private fun handleSaslHandshakeResponse(response: SaslHandshakeResponse) {
        val error = response.error()
        if (error != Errors.NONE) setSaslState(SaslState.FAILED)
        when (error) {
            Errors.NONE -> {}
            Errors.UNSUPPORTED_SASL_MECHANISM -> throw UnsupportedSaslMechanismException(
                String.format(
                    "Client SASL mechanism '%s' not enabled in the server, enabled mechanisms are %s",
                    mechanism, response.enabledMechanisms()
                )
            )

            Errors.ILLEGAL_SASL_STATE -> throw IllegalSaslStateException(
                String.format(
                    "Unexpected handshake request with client mechanism %s, enabled mechanisms are %s",
                    mechanism,
                    response.enabledMechanisms(),
                )
            )

            else -> throw IllegalSaslStateException(
                String.format(
                    "Unknown error code %s, client mechanism is %s, enabled mechanisms are %s",
                    response.error(),
                    mechanism,
                    response.enabledMechanisms()
                )
            )
        }
    }

    /**
     * Information related to re-authentication
     */
    private inner class ReauthInfo {

        /**
         * API versions response from original authentication.
         */
        var apiVersionsResponseFromAuth: ApiVersionsResponse? = null

        var reauthenticationBeginNanos: Long = 0

        var pendingAuthenticatedReceives = mutableListOf<NetworkReceive>()

        /**
         * API versions response received from broker.
         */
        var apiVersionsResponseFromBroker: ApiVersionsResponse? = null

        var positiveSessionLifetimeMs: Long? = null

        var authenticationEndNanos: Long = 0

        /**
         * Client session re-authentication time in nanoseconds.
         */
        var clientSessionReauthenticationTime: Long? = null

        /**
         * @param apiVersionsResponseFromAuth API versions response from original authentication.
         */
        fun reauthenticating(
            apiVersionsResponseFromAuth: ApiVersionsResponse?,
            reauthenticationBeginNanos: Long
        ) {
            this.apiVersionsResponseFromAuth = apiVersionsResponseFromAuth
            this.reauthenticationBeginNanos = reauthenticationBeginNanos
        }

        @Deprecated(
            message = "Use property instead",
            replaceWith = ReplaceWith("isReauthenticating")
        )
        fun reauthenticating(): Boolean = isReauthenticating

        val isReauthenticating: Boolean
            get() = apiVersionsResponseFromAuth != null

        @Deprecated(
            message = "Use property instead",
            replaceWith = ReplaceWith("apiVersionsResponse")
        )
        fun apiVersionsResponse(): ApiVersionsResponse? =
            apiVersionsResponseFromAuth ?: apiVersionsResponseFromBroker

        val apiVersionsResponse: ApiVersionsResponse?
            get() = apiVersionsResponseFromAuth ?: apiVersionsResponseFromBroker

        /**
         * Return the (always non-null but possibly empty) NetworkReceive response that arrived
         * during re-authentication that is unrelated to re-authentication, if  any. This
         * corresponds to a request sent prior to the beginning of re-authentication; the request
         * was made when the channel was successfully authenticated, and the response arrived during
         * the re-authentication process.
         *
         * @return the (always non-null but possibly empty) NetworkReceive response that arrived
         * during re-authentication that is unrelated to re-authentication, if any.
         */
        fun pollResponseReceivedDuringReauthentication(): NetworkReceive? =
            pendingAuthenticatedReceives.removeFirstOrNull()

        fun setAuthenticationEndAndSessionReauthenticationTimes(nowNanos: Long) {
            authenticationEndNanos = nowNanos
            if (positiveSessionLifetimeMs != null) {

                // pick a random percentage between WINDOW_FACTOR (85%) and WINDOW_FACTOR + JITTER
                // (95%) for session re-authentication
                val pctToUse = WINDOW_FACTOR_PERCENTAGE + Random.nextDouble() * WINDOW_JITTER_PERCENTAGE
                val sessionLifetimeMsToUse = (positiveSessionLifetimeMs!! * pctToUse).toLong()
                clientSessionReauthenticationTime = authenticationEndNanos + 1000 * 1000 * sessionLifetimeMsToUse

                log.debug(
                    "Finished {} with session expiration in {} ms and session re-authentication on or after {} ms",
                    authenticationOrReauthenticationText(),
                    positiveSessionLifetimeMs,
                    sessionLifetimeMsToUse
                )
            } else log.debug(
                "Finished {} with no session expiration and no session re-authentication",
                authenticationOrReauthenticationText()
            )
        }

        fun reauthenticationLatencyMs(): Long? =
            if (isReauthenticating)
                    ((authenticationEndNanos - reauthenticationBeginNanos) / 1000.0 / 1000.0)
                        .roundToLong()
            else null

        private fun authenticationOrReauthenticationText(): String {
            return if (isReauthenticating) "re-authentication" else "authentication"
        }
    }

    /**
     * The internal state transitions for initial authentication of a channel are declared in order,
     * starting with [SEND_APIVERSIONS_REQUEST] and ending in either [COMPLETE] or [FAILED].
     *
     * Re-authentication of a channel starts with the state
     * - [REAUTH_PROCESS_ORIG_APIVERSIONS_RESPONSE] and then flows to
     * - [REAUTH_SEND_HANDSHAKE_REQUEST] followed by
     * - [REAUTH_RECEIVE_HANDSHAKE_OR_OTHER_RESPONSE] and then
     * - [REAUTH_INITIAL];
     *
     * after that the flow joins the authentication flow at the [INTERMEDIATE] state and ends at
     * either [COMPLETE] or [FAILED].
     */
    enum class SaslState {

        /**
         * Initial state for authentication: client sends ApiVersionsRequest in this state when
         * authenticating.
         */
        SEND_APIVERSIONS_REQUEST,

        /**
         * Awaiting ApiVersionsResponse from server.
         */
        RECEIVE_APIVERSIONS_RESPONSE,

        /**
         *
         * Received ApiVersionsResponse, send SaslHandshake request.
         */
        SEND_HANDSHAKE_REQUEST,

        /**
         * Awaiting SaslHandshake response from server when authenticating.
         */
        RECEIVE_HANDSHAKE_RESPONSE,

        /**
         * Initial authentication state starting SASL token exchange for configured mechanism, send
         * first token.
         */
        INITIAL,

        /**
         * Intermediate state during SASL token exchange, process challenges and send responses.
         */
        INTERMEDIATE,

        /**
         * Sent response to last challenge. If using SaslAuthenticate, wait for authentication
         * status from server, else COMPLETE.
         */
        CLIENT_COMPLETE,

        /**
         * Authentication sequence complete. If using SaslAuthenticate, this state implies
         * successful authentication.
         */
        COMPLETE,

        /**
         * Failed authentication due to an error at some stage.
         */
        FAILED,

        /**
         * Initial state for re-authentication: process ApiVersionsResponse from original
         * authentication.
         */
        REAUTH_PROCESS_ORIG_APIVERSIONS_RESPONSE,

        /**
         * Processed original ApiVersionsResponse, send SaslHandshake request as part of
         * re-authentication.
         */
        REAUTH_SEND_HANDSHAKE_REQUEST,

        /**
         * Awaiting SaslHandshake response from server when re-authenticating, and may receive
         * other, in-flight responses sent prior to start of re-authentication as well.
         */
        REAUTH_RECEIVE_HANDSHAKE_OR_OTHER_RESPONSE,

        /**
         * Initial re-authentication state starting SASL token exchange for configured mechanism,
         * send first token.
         */
        REAUTH_INITIAL
    }

    companion object {

        private const val DISABLE_KAFKA_SASL_AUTHENTICATE_HEADER: Short = -1

        /**
         * Minimum percentage value for window factor to take network latency and clock drift into
         * account.
         *
         * This value is used for session re-authentication.
         * @see WINDOW_JITTER_PERCENTAGE
         */
        private const val WINDOW_FACTOR_PERCENTAGE = 0.85

        /**
         * Percentage value for window jitter to avoid re-authentication storm across many channels
         * simultaneously.
         *
         * This value is used for session re-authentication.
         * @see WINDOW_FACTOR_PERCENTAGE
         */
        private const val WINDOW_JITTER_PERCENTAGE = 0.10

        /**
         * the reserved range of correlation id for Sasl requests.
         *
         * Noted: there is a story about reserved range. The response of LIST_OFFSET is compatible
         * to response of SASL_HANDSHAKE. Hence, we could miss the schema error when using schema of
         * SASL_HANDSHAKE to parse response of LIST_OFFSET. For example: the IllegalStateException
         * caused by mismatched correlation id is thrown if following steps happens.
         *
         * 1) sent LIST_OFFSET
         * 2) sent SASL_HANDSHAKE
         * 3) receive response of LIST_OFFSET
         * 4) succeed to use schema of SASL_HANDSHAKE to parse response of LIST_OFFSET
         * 5) throw IllegalStateException due to mismatched correlation id
         *
         * As a simple approach, we force Sasl requests to use a reserved correlation id which is
         * separated from those used in NetworkClient for Kafka requests. Hence, we can guarantee
         * that every SASL request will throw SchemaException due to correlation id mismatch during
         * reauthentication.
         */
        const val MAX_RESERVED_CORRELATION_ID = Int.MAX_VALUE

        /**
         * We only expect one request in-flight a time during authentication so the small range is
         * fine.
         */
        const val MIN_RESERVED_CORRELATION_ID = MAX_RESERVED_CORRELATION_ID - 7

        /**
         * @return true if the correlation id is reserved for SASL request. otherwise, false
         */
        fun isReserved(correlationId: Int): Boolean =
            correlationId >= MIN_RESERVED_CORRELATION_ID

        /**
         * Returns the first Principal from Subject.
         * @throws KafkaException if there are no Principals in the Subject. During Kerberos
         * re-login, principal is reset on Subject. An exception is thrown so that the connection is
         * retried after any configured backoff.
         */
        fun firstPrincipal(subject: Subject): String {
            val principals = subject.principals

            synchronized(principals) {
                val iterator: Iterator<Principal> = principals.iterator()

                return if (iterator.hasNext()) iterator.next().name
                else throw KafkaException(
                    "Principal could not be determined from Subject, this may be a transient " +
                    "failure due to Kerberos re-login",
                )
            }
        }
    }
}
