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

import java.security.InvalidKeyException
import java.security.MessageDigest
import java.security.NoSuchAlgorithmException
import javax.security.auth.callback.CallbackHandler
import javax.security.auth.callback.NameCallback
import javax.security.sasl.SaslException
import javax.security.sasl.SaslServer
import javax.security.sasl.SaslServerFactory
import org.apache.kafka.common.errors.AuthenticationException
import org.apache.kafka.common.errors.IllegalSaslStateException
import org.apache.kafka.common.errors.SaslAuthenticationException
import org.apache.kafka.common.security.authenticator.SaslInternalConfigs
import org.apache.kafka.common.security.scram.ScramCredential
import org.apache.kafka.common.security.scram.ScramCredentialCallback
import org.apache.kafka.common.security.scram.ScramLoginModule
import org.apache.kafka.common.security.scram.internals.ScramMessages.ClientFinalMessage
import org.apache.kafka.common.security.scram.internals.ScramMessages.ClientFirstMessage
import org.apache.kafka.common.security.scram.internals.ScramMessages.ServerFinalMessage
import org.apache.kafka.common.security.scram.internals.ScramMessages.ServerFirstMessage
import org.apache.kafka.common.security.token.delegation.internals.DelegationTokenCredentialCallback
import org.apache.kafka.common.utils.Utils.mkSet
import org.slf4j.LoggerFactory

/**
 * SaslServer implementation for SASL/SCRAM. This server is configured with a callback handler for
 * integration with a credential manager. Kafka brokers provide callbacks based on a Zookeeper-based
 * password store.
 *
 * @see [RFC 5802](https://tools.ietf.org/html/rfc5802)
 */
class ScramSaslServer(
    private val mechanism: ScramMechanism,
    props: Map<String, *>,
    private val callbackHandler: CallbackHandler,
) : SaslServer {

    internal enum class State {
        RECEIVE_CLIENT_FIRST_MESSAGE,
        RECEIVE_CLIENT_FINAL_MESSAGE,
        COMPLETE,
        FAILED
    }

    private val formatter: ScramFormatter = ScramFormatter(mechanism)

    private var state: State? = null

    private var username: String? = null

    private var clientFirstMessage: ClientFirstMessage? = null

    private var serverFirstMessage: ServerFirstMessage? = null

    private lateinit var scramExtensions: ScramExtensions

    private var scramCredential: ScramCredential? = null

    private var authorizationId: String? = null

    private var tokenExpiryTimestamp: Long? = null

    init {
        setState(State.RECEIVE_CLIENT_FIRST_MESSAGE)
    }

    /**
     * @throws SaslAuthenticationException if the requested authorization id is not the same as
     * username.
     *
     * **Note:** This method may throw [SaslAuthenticationException] to provide custom error
     * messages to clients. But care should be taken to avoid including any information in the
     * exception message that should not be leaked to unauthenticated clients. It may be safer to
     * throw [SaslException] in most cases so that a standard error message is returned to clients.
     */
    @Throws(SaslException::class, SaslAuthenticationException::class)
    override fun evaluateResponse(response: ByteArray): ByteArray {
        return try {
            when (state) {
                State.RECEIVE_CLIENT_FIRST_MESSAGE -> {
                    clientFirstMessage = ClientFirstMessage(response).also { scramExtensions = it.extensions }

                    if (!SUPPORTED_EXTENSIONS.containsAll(scramExtensions.map().keys)) {
                        log.debug(
                            "Unsupported extensions will be ignored, supported {}, provided {}",
                            SUPPORTED_EXTENSIONS, scramExtensions.map().keys
                        )
                    }

                    val serverNonce = formatter.secureRandomString()
                    try {
                        val saslName = clientFirstMessage!!.saslName
                        username = ScramFormatter.username(saslName)
                        val nameCallback = NameCallback("username", username)
                        val credentialCallback: ScramCredentialCallback

                        if (scramExtensions.tokenAuthenticated()) {
                            val tokenCallback = DelegationTokenCredentialCallback()
                            credentialCallback = tokenCallback
                            callbackHandler.handle(arrayOf(nameCallback, tokenCallback))
                            if (tokenCallback.tokenOwner == null) throw SaslException(
                                "Token Authentication failed: Invalid tokenId : $username"
                            )
                            authorizationId = tokenCallback.tokenOwner
                            tokenExpiryTimestamp = tokenCallback.tokenExpiryTimestamp
                        } else {
                            credentialCallback = ScramCredentialCallback()
                            callbackHandler.handle(arrayOf(nameCallback, credentialCallback))
                            authorizationId = username
                            tokenExpiryTimestamp = null
                        }

                        scramCredential = credentialCallback.scramCredential ?: throw SaslException(
                            "Authentication failed: Invalid user credentials"
                        )

                        val authorizationIdFromClient = clientFirstMessage!!.authorizationId

                        if (authorizationIdFromClient.isNotEmpty() && authorizationIdFromClient != username)
                            throw SaslAuthenticationException(
                                "Authentication failed: Client requested an authorization id that is different from username"
                            )

                        if (scramCredential!!.iterations < mechanism.minIterations)
                            throw SaslException(
                                "Iterations ${scramCredential!!.iterations} is less than the " +
                                        "minimum ${mechanism.minIterations} for $mechanism"
                            )

                        serverFirstMessage = ServerFirstMessage(
                            clientNonce = clientFirstMessage!!.nonce,
                            serverNonce = serverNonce,
                            salt = scramCredential!!.salt,
                            iterations = scramCredential!!.iterations
                        )

                        setState(State.RECEIVE_CLIENT_FINAL_MESSAGE)
                        serverFirstMessage!!.toBytes()
                    } catch (e: SaslException) {
                        throw e
                    } catch (e: AuthenticationException) {
                        throw e
                    } catch (e: Throwable) {
                        throw SaslException(
                            "Authentication failed: Credentials could not be obtained",
                            e
                        )
                    }
                }

                State.RECEIVE_CLIENT_FINAL_MESSAGE -> try {
                    val clientFinalMessage = ClientFinalMessage(response)
                    verifyClientProof(clientFinalMessage)
                    val serverKey = scramCredential!!.serverKey
                    val serverSignature = formatter.serverSignature(
                        serverKey = serverKey,
                        clientFirstMessage = clientFirstMessage!!,
                        serverFirstMessage = serverFirstMessage!!,
                        clientFinalMessage = clientFinalMessage
                    )
                    val serverFinalMessage = ServerFinalMessage(null, serverSignature)
                    clearCredentials()
                    setState(State.COMPLETE)
                    serverFinalMessage.toBytes()
                } catch (e: InvalidKeyException) {
                    throw SaslException("Authentication failed: Invalid client final message", e)
                }

                else -> throw IllegalSaslStateException("Unexpected challenge in Sasl server state $state")
            }
        } catch (e: SaslException) {
            clearCredentials()
            setState(State.FAILED)
            throw e
        } catch (e: AuthenticationException) {
            clearCredentials()
            setState(State.FAILED)
            throw e
        }
    }

    override fun getAuthorizationID(): String? {
        check(isComplete) { "Authentication exchange has not completed" }
        return authorizationId
    }

    override fun getMechanismName(): String = mechanism.mechanismName

    override fun getNegotiatedProperty(propName: String): Any? {
        check(isComplete) { "Authentication exchange has not completed" }

        return if (SaslInternalConfigs.CREDENTIAL_LIFETIME_MS_SASL_NEGOTIATED_PROPERTY_KEY == propName)
            tokenExpiryTimestamp // will be null if token not used
        else if (SUPPORTED_EXTENSIONS.contains(propName)) scramExtensions.map()[propName]
        else null
    }

    override fun isComplete(): Boolean {
        return state == State.COMPLETE
    }

    override fun unwrap(incoming: ByteArray, offset: Int, len: Int): ByteArray {
        check(isComplete) { "Authentication exchange has not completed" }
        return incoming.copyOfRange(offset, offset + len)
    }

    override fun wrap(outgoing: ByteArray, offset: Int, len: Int): ByteArray {
        check(isComplete) { "Authentication exchange has not completed" }
        return outgoing.copyOfRange(offset, offset + len)
    }

    override fun dispose() = Unit

    private fun setState(state: State) {
        log.debug("Setting SASL/{} server state to {}", mechanism, state)
        this.state = state
    }

    @Throws(SaslException::class)
    private fun verifyClientProof(clientFinalMessage: ClientFinalMessage) {
        try {
            val expectedStoredKey = scramCredential!!.storedKey
            val clientSignature = formatter.clientSignature(
                storedKey = expectedStoredKey,
                clientFirstMessage = clientFirstMessage!!,
                serverFirstMessage = serverFirstMessage!!,
                clientFinalMessage = clientFinalMessage
            )
            val computedStoredKey = formatter.storedKey(clientSignature, clientFinalMessage.proof!!)

            if (!MessageDigest.isEqual(computedStoredKey, expectedStoredKey))
                throw SaslException("Invalid client credentials")
        } catch (e: InvalidKeyException) {
            throw SaslException("Sasl client verification failed", e)
        }
    }

    private fun clearCredentials() {
        scramCredential = null
        clientFirstMessage = null
        serverFirstMessage = null
    }

    class ScramSaslServerFactory : SaslServerFactory {
        @Throws(SaslException::class)
        override fun createSaslServer(
            mechanism: String,
            protocol: String,
            serverName: String,
            props: Map<String, *>,
            cbh: CallbackHandler
        ): SaslServer {
            if (!ScramMechanism.isScram(mechanism)) {
                throw SaslException(
                    String.format(
                        "Requested mechanism '%s' is not supported. Supported mechanisms are '%s'.",
                        mechanism, ScramMechanism.mechanismNames
                    )
                )
            }
            return try {
                ScramSaslServer(ScramMechanism.forMechanismName(mechanism)!!, props, cbh)
            } catch (e: NoSuchAlgorithmException) {
                throw SaslException("Hash algorithm not supported for mechanism $mechanism", e)
            }
        }

        override fun getMechanismNames(props: Map<String?, *>?): Array<String> {
            val mechanisms = ScramMechanism.mechanismNames
            return mechanisms.toTypedArray<String>()
        }
    }

    companion object {

        private val log = LoggerFactory.getLogger(ScramSaslServer::class.java)

        private val SUPPORTED_EXTENSIONS = setOf(ScramLoginModule.TOKEN_AUTH_CONFIG)
    }
}
