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

package org.apache.kafka.common.security.oauthbearer.internals

import java.io.IOException
import java.nio.charset.StandardCharsets
import javax.security.auth.callback.Callback
import javax.security.auth.callback.CallbackHandler
import javax.security.auth.callback.UnsupportedCallbackException
import javax.security.sasl.SaslClient
import javax.security.sasl.SaslClientFactory
import javax.security.sasl.SaslException
import org.apache.kafka.common.errors.IllegalSaslStateException
import org.apache.kafka.common.security.auth.AuthenticateCallbackHandler
import org.apache.kafka.common.security.auth.SaslExtensions
import org.apache.kafka.common.security.auth.SaslExtensionsCallback
import org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule
import org.apache.kafka.common.security.oauthbearer.OAuthBearerTokenCallback
import org.slf4j.LoggerFactory

/**
 * `SaslClient` implementation for SASL/OAUTHBEARER in Kafka. This
 * implementation requires an instance of `AuthenticateCallbackHandler`
 * that can handle an instance of [OAuthBearerTokenCallback] and return
 * the [OAuthBearerToken] generated by the `login()` event on the
 * `LoginContext`. Said handler can also optionally handle an instance of [SaslExtensionsCallback]
 * to return any extensions generated by the `login()` event on the `LoginContext`.
 *
 * @see [RFC 6750,
 * Section 2.1](https://tools.ietf.org/html/rfc6750.section-2.1)
 */
class OAuthBearerSaslClient(private val callbackHandler: AuthenticateCallbackHandler) : SaslClient {

    internal enum class State {
        SEND_CLIENT_FIRST_MESSAGE,
        RECEIVE_SERVER_FIRST_MESSAGE,
        RECEIVE_SERVER_MESSAGE_AFTER_FAILURE,
        COMPLETE,
        FAILED
    }

    private var state: State? = null

    init {
        setState(State.SEND_CLIENT_FIRST_MESSAGE)
    }

    fun callbackHandler(): CallbackHandler = callbackHandler

    override fun getMechanismName(): String = OAuthBearerLoginModule.OAUTHBEARER_MECHANISM

    override fun hasInitialResponse(): Boolean = true

    @Throws(SaslException::class)
    override fun evaluateChallenge(challenge: ByteArray): ByteArray? {
        return try {
            val callback = OAuthBearerTokenCallback()
            when (state) {
                State.SEND_CLIENT_FIRST_MESSAGE -> {
                    if (challenge.isNotEmpty()) throw SaslException("Expected empty challenge")
                    callbackHandler().handle(arrayOf(callback))

                    val extensions = retrieveCustomExtensions()
                    setState(State.RECEIVE_SERVER_FIRST_MESSAGE)
                    OAuthBearerClientInitialResponse(
                        tokenValue = callback.token!!.value()!!,
                        extensions = extensions,
                    ).toBytes()
                }

                State.RECEIVE_SERVER_FIRST_MESSAGE -> {
                    if (challenge.isNotEmpty()) {
                        val jsonErrorResponse = String(challenge, StandardCharsets.UTF_8)
                        if (log.isDebugEnabled) log.debug(
                            "Sending %%x01 response to server after receiving an error: {}",
                            jsonErrorResponse
                        )
                        setState(State.RECEIVE_SERVER_MESSAGE_AFTER_FAILURE)
                        return byteArrayOf(BYTE_CONTROL_A)
                    }

                    callbackHandler().handle(arrayOf(callback))
                    if (log.isDebugEnabled) log.debug(
                        "Successfully authenticated as {}",
                        callback.token!!.principalName()
                    )

                    setState(State.COMPLETE)
                    null
                }

                else -> throw IllegalSaslStateException("Unexpected challenge in Sasl client state $state")
            }
        } catch (e: SaslException) {
            setState(State.FAILED)
            throw e
        } catch (e: IOException) {
            setState(State.FAILED)
            throw SaslException(e.message, e)
        } catch (e: UnsupportedCallbackException) {
            setState(State.FAILED)
            throw SaslException(e.message, e)
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
        log.debug(
            "Setting SASL/{} client state to {}",
            OAuthBearerLoginModule.OAUTHBEARER_MECHANISM,
            state
        )
        this.state = state
    }

    @Throws(SaslException::class)
    private fun retrieveCustomExtensions(): SaslExtensions {
        val extensionsCallback = SaslExtensionsCallback()
        try {
            callbackHandler().handle(arrayOf<Callback>(extensionsCallback))
        } catch (e: UnsupportedCallbackException) {
            log.debug(
                "Extensions callback is not supported by client callback handler {}, no extensions will be added",
                callbackHandler()
            )
        } catch (e: Exception) {
            throw SaslException("SASL extensions could not be obtained", e)
        }
        return extensionsCallback.extensions
    }

    class OAuthBearerSaslClientFactory : SaslClientFactory {
        override fun createSaslClient(
            mechanisms: Array<String>,
            authorizationId: String?,
            protocol: String,
            serverName: String,
            props: Map<String?, *>?,
            callbackHandler: CallbackHandler,
        ): SaslClient? {
            val mechanismNamesCompatibleWithPolicy = getMechanismNames(props)
            mechanisms.forEach { mechanism ->
                mechanismNamesCompatibleWithPolicy.forEach { compatibleMechanism ->
                    if (compatibleMechanism == mechanism) {
                        require(callbackHandler is AuthenticateCallbackHandler) {
                            String.format(
                                "Callback handler must be castable to %s: %s",
                                AuthenticateCallbackHandler::class.java.name,
                                callbackHandler.javaClass.name
                            )
                        }
                        return OAuthBearerSaslClient(callbackHandler)
                    }
                }
            }
            return null
        }

        override fun getMechanismNames(props: Map<String?, *>?): Array<String> {
            return OAuthBearerSaslServer.mechanismNamesCompatibleWithPolicy(props)
        }
    }

    companion object {

        const val BYTE_CONTROL_A = 0x01.toByte()

        private val log = LoggerFactory.getLogger(OAuthBearerSaslClient::class.java)
    }
}
