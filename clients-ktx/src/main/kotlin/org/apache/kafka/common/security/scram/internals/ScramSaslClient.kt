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

import java.nio.charset.StandardCharsets
import java.security.InvalidKeyException
import java.security.MessageDigest
import java.security.NoSuchAlgorithmException
import java.util.*
import javax.security.auth.callback.Callback
import javax.security.auth.callback.CallbackHandler
import javax.security.auth.callback.NameCallback
import javax.security.auth.callback.PasswordCallback
import javax.security.auth.callback.UnsupportedCallbackException
import javax.security.sasl.SaslClient
import javax.security.sasl.SaslClientFactory
import javax.security.sasl.SaslException
import org.apache.kafka.common.errors.IllegalSaslStateException
import org.apache.kafka.common.security.scram.ScramExtensionsCallback
import org.apache.kafka.common.security.scram.internals.ScramMessages.ClientFinalMessage
import org.apache.kafka.common.security.scram.internals.ScramMessages.ClientFirstMessage
import org.apache.kafka.common.security.scram.internals.ScramMessages.ServerFinalMessage
import org.apache.kafka.common.security.scram.internals.ScramMessages.ServerFirstMessage
import org.slf4j.LoggerFactory

/**
 * SaslClient implementation for SASL/SCRAM.
 *
 * This implementation expects a login module that populates username as the Subject's public
 * credential and password as the private credential.
 *
 * @see [RFC 5802](https://tools.ietf.org/html/rfc5802)
 */
class ScramSaslClient(
    private val mechanism: ScramMechanism,
    private val callbackHandler: CallbackHandler
) : SaslClient {

    internal enum class State {
        SEND_CLIENT_FIRST_MESSAGE,
        RECEIVE_SERVER_FIRST_MESSAGE,
        RECEIVE_SERVER_FINAL_MESSAGE,
        COMPLETE,
        FAILED
    }

    private val formatter: ScramFormatter = ScramFormatter(mechanism)

    private lateinit var clientNonce: String

    private var state: State? = null

    private lateinit var saltedPassword: ByteArray

    private lateinit var clientFirstMessage: ClientFirstMessage

    private lateinit var serverFirstMessage: ServerFirstMessage

    private lateinit var clientFinalMessage: ClientFinalMessage

    init {
        setState(State.SEND_CLIENT_FIRST_MESSAGE)
    }

    override fun getMechanismName(): String = mechanism.mechanismName

    override fun hasInitialResponse(): Boolean = true

    @Throws(SaslException::class)
    override fun evaluateChallenge(challenge: ByteArray?): ByteArray? {
        return try {
            when (state) {
                State.SEND_CLIENT_FIRST_MESSAGE -> {
                    if (challenge != null && challenge.isNotEmpty()) throw SaslException("Expected empty challenge")
                    clientNonce = formatter.secureRandomString()
                    val nameCallback = NameCallback("Name:")
                    val extensionsCallback = ScramExtensionsCallback()
                    try {
                        callbackHandler.handle(arrayOf<Callback>(nameCallback))
                        try {
                            callbackHandler.handle(arrayOf<Callback>(extensionsCallback))
                        } catch (e: UnsupportedCallbackException) {
                            log.debug(
                                "Extensions callback is not supported by client callback handler " +
                                        "{}, no extensions will be added",
                                callbackHandler
                            )
                        }
                    } catch (e: Throwable) {
                        throw SaslException("User name or extensions could not be obtained", e)
                    }
                    val username = nameCallback.name
                    val saslName = ScramFormatter.saslName(username)
                    val extensions = extensionsCallback.extensions
                    clientFirstMessage = ClientFirstMessage(saslName, clientNonce, extensions)
                    setState(State.RECEIVE_SERVER_FIRST_MESSAGE)
                    clientFirstMessage.toBytes()
                }

                State.RECEIVE_SERVER_FIRST_MESSAGE -> {
                    serverFirstMessage = ServerFirstMessage(challenge)

                    if (!serverFirstMessage.nonce.startsWith(clientNonce))
                        throw SaslException("Invalid server nonce: does not start with client nonce")

                    if (serverFirstMessage.iterations < mechanism.minIterations)
                        throw SaslException(
                        "Requested iterations ${serverFirstMessage.iterations} is less than the " +
                                "minimum ${mechanism.minIterations} for $mechanism"
                    )

                    val passwordCallback = PasswordCallback("Password:", false)
                    try {
                        callbackHandler.handle(arrayOf<Callback>(passwordCallback))
                    } catch (e: Throwable) {
                        throw SaslException("User name could not be obtained", e)
                    }

                    clientFinalMessage = handleServerFirstMessage(passwordCallback.password)
                    setState(State.RECEIVE_SERVER_FINAL_MESSAGE)
                    clientFinalMessage.toBytes()
                }

                State.RECEIVE_SERVER_FINAL_MESSAGE -> {
                    val serverFinalMessage = ServerFinalMessage(challenge)
                    if (serverFinalMessage.error != null) throw SaslException(
                        "Sasl authentication using $mechanism failed with error: " +
                                serverFinalMessage.error
                    )

                    handleServerFinalMessage(serverFinalMessage.serverSignature)
                    setState(State.COMPLETE)
                    null
                }

                else -> throw IllegalSaslStateException("Unexpected challenge in Sasl client state $state")
            }
        } catch (e: SaslException) {
            setState(State.FAILED)
            throw e
        }
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

    override fun getNegotiatedProperty(propName: String): Any? {
        check(isComplete) { "Authentication exchange has not completed" }
        return null
    }

    override fun dispose() = Unit

    private fun setState(state: State) {
        log.debug("Setting SASL/{} client state to {}", mechanism, state)
        this.state = state
    }

    @Throws(SaslException::class)
    private fun handleServerFirstMessage(password: CharArray): ClientFinalMessage {
        return try {
            val passwordBytes = ScramFormatter.normalize(String(password))
            saltedPassword = formatter.hi(
                passwordBytes,
                serverFirstMessage.salt,
                serverFirstMessage.iterations
            )
            val clientFinalMessage = ClientFinalMessage(
                channelBinding = "n,,".toByteArray(StandardCharsets.UTF_8),
                nonce = serverFirstMessage.nonce
            )
            val clientProof = formatter.clientProof(
                saltedPassword = saltedPassword,
                clientFirstMessage = clientFirstMessage,
                serverFirstMessage = serverFirstMessage,
                clientFinalMessage = clientFinalMessage,
            )
            clientFinalMessage.proof = clientProof
            clientFinalMessage
        } catch (e: InvalidKeyException) {
            throw SaslException("Client final message could not be created", e)
        }
    }

    @Throws(SaslException::class)
    private fun handleServerFinalMessage(signature: ByteArray?) {
        try {
            val serverSignature = formatter.serverSignature(
                serverKey = formatter.serverKey(saltedPassword),
                clientFirstMessage = clientFirstMessage,
                serverFirstMessage = serverFirstMessage,
                clientFinalMessage = clientFinalMessage
            )
            if (!MessageDigest.isEqual(signature, serverSignature))
                throw SaslException("Invalid server signature in server final message")
        } catch (e: InvalidKeyException) {
            throw SaslException("Sasl server signature verification failed", e)
        }
    }

    class ScramSaslClientFactory : SaslClientFactory {
        @Throws(SaslException::class)
        override fun createSaslClient(
            mechanisms: Array<String>,
            authorizationId: String,
            protocol: String,
            serverName: String,
            props: Map<String, *>,
            cbh: CallbackHandler
        ): SaslClient {
            var mechanism: ScramMechanism? = null
            for (mech in mechanisms) {
                mechanism = ScramMechanism.forMechanismName(mech)
                if (mechanism != null) break
            }

            if (mechanism == null) throw SaslException(
                String.format(
                    "Requested mechanisms '%s' not supported. Supported mechanisms are '%s'.",
                    mechanisms.toList(),
                    ScramMechanism.mechanismNames
                )
            )
            return try {
                ScramSaslClient(mechanism, cbh)
            } catch (e: NoSuchAlgorithmException) {
                throw SaslException("Hash algorithm not supported for mechanism $mechanism", e)
            }
        }

        override fun getMechanismNames(props: Map<String, *>): Array<String> =
            ScramMechanism.mechanismNames.toTypedArray<String>()
    }

    companion object {
        private val log = LoggerFactory.getLogger(ScramSaslClient::class.java)
    }
}
