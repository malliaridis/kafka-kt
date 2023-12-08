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
import java.util.*
import javax.security.auth.callback.Callback
import javax.security.auth.callback.CallbackHandler
import javax.security.auth.callback.UnsupportedCallbackException
import javax.security.sasl.Sasl
import javax.security.sasl.SaslException
import javax.security.sasl.SaslServer
import javax.security.sasl.SaslServerFactory
import org.apache.kafka.common.errors.SaslAuthenticationException
import org.apache.kafka.common.security.auth.AuthenticateCallbackHandler
import org.apache.kafka.common.security.auth.SaslExtensions
import org.apache.kafka.common.security.authenticator.SaslInternalConfigs
import org.apache.kafka.common.security.oauthbearer.OAuthBearerExtensionsValidatorCallback
import org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule
import org.apache.kafka.common.security.oauthbearer.OAuthBearerToken
import org.apache.kafka.common.security.oauthbearer.OAuthBearerValidatorCallback
import org.apache.kafka.common.utils.Utils.mkString
import org.slf4j.LoggerFactory

/**
 * `SaslServer` implementation for SASL/OAUTHBEARER in Kafka. An instance of [OAuthBearerToken] is
 * available upon successful authentication via the negotiated property "`OAUTHBEARER.token`"; the
 * token could be used in a custom authorizer (to authorize based on JWT claims rather than ACLs,
 * for example).
 */
class OAuthBearerSaslServer(private val callbackHandler: CallbackHandler) : SaslServer {

    private var complete = false

    private var _tokenForNegotiatedProperty: OAuthBearerToken? = null
    private val tokenForNegotiatedProperty: OAuthBearerToken
        get() = _tokenForNegotiatedProperty!!

    private var errorMessage: String? = null

    private var extensions: SaslExtensions? = null

    init {
        require(callbackHandler is AuthenticateCallbackHandler) {
            String.format(
                "Callback handler must be castable to %s: %s",
                AuthenticateCallbackHandler::class.java.name,
                callbackHandler.javaClass.name
            )
        }
    }

    /**
     * @throws SaslAuthenticationException if access token cannot be validated
     *
     * **Note:** This method may throw [SaslAuthenticationException] to provide custom error
     * messages to clients. But care should be taken to avoid including any information in the
     * exception message that should not be leaked to unauthenticated clients. It may be safer to
     * throw [SaslException] in some cases so that a standard error message is returned to clients.
     */
    @Throws(SaslException::class, SaslAuthenticationException::class)
    override fun evaluateResponse(response: ByteArray): ByteArray {

        if (
            response.size == 1
            && response[0] == OAuthBearerSaslClient.BYTE_CONTROL_A
            && errorMessage != null
        ) {
            log.debug("Received %x01 response from client after it received our error")
            throw SaslAuthenticationException(errorMessage)
        }

        errorMessage = null
        val clientResponse: OAuthBearerClientInitialResponse = try {
            OAuthBearerClientInitialResponse(response)
        } catch (e: SaslException) {
            log.debug(e.message)
            throw e
        }

        return process(
            clientResponse.tokenValue,
            clientResponse.authorizationId,
            clientResponse.saslExtensions
        )
    }

    override fun getAuthorizationID(): String {
        check(complete) { "Authentication exchange has not completed" }
        return tokenForNegotiatedProperty.principalName()
    }

    override fun getMechanismName(): String = OAuthBearerLoginModule.OAUTHBEARER_MECHANISM

    override fun getNegotiatedProperty(propName: String): Any? {
        check(complete) { "Authentication exchange has not completed" }

        return if (NEGOTIATED_PROPERTY_KEY_TOKEN == propName) tokenForNegotiatedProperty
        else if (SaslInternalConfigs.CREDENTIAL_LIFETIME_MS_SASL_NEGOTIATED_PROPERTY_KEY == propName)
            tokenForNegotiatedProperty.lifetimeMs()
        else extensions!!.map()[propName]
    }

    override fun isComplete(): Boolean = complete

    override fun unwrap(incoming: ByteArray, offset: Int, len: Int): ByteArray {
        check(complete) { "Authentication exchange has not completed" }
        return incoming.copyOfRange(offset, offset + len)
    }

    override fun wrap(outgoing: ByteArray, offset: Int, len: Int): ByteArray {
        check(complete) { "Authentication exchange has not completed" }
        return outgoing.copyOfRange(offset, offset + len)
    }

    override fun dispose() {
        complete = false
        _tokenForNegotiatedProperty = null
        extensions = null
    }

    @Throws(SaslException::class)
    private fun process(
        tokenValue: String,
        authorizationId: String,
        extensions: SaslExtensions
    ): ByteArray {
        val callback = OAuthBearerValidatorCallback(tokenValue)

        try {
            callbackHandler.handle(arrayOf<Callback>(callback))
        } catch (e: IOException) {
            handleCallbackError(e)
        } catch (e: UnsupportedCallbackException) {
            handleCallbackError(e)
        }

        val token = callback.token
        if (token == null) {
            val errorMessage = jsonErrorResponse(
                callback.errorStatus,
                callback.errorScope,
                callback.errorOpenIDConfiguration
            ).also {
                log.debug(it)
                this.errorMessage = it
            }

            return errorMessage.toByteArray(StandardCharsets.UTF_8)
        }

        // We support the client specifying an authorization ID as per the SASL specification, but
        // it must match the principal name if it is specified.
        if (authorizationId.isNotEmpty() && authorizationId != token.principalName())
            throw SaslAuthenticationException(
            String.format(
                "Authentication failed: Client requested an authorization id (%s) that is " +
                        "different from the token's principal name (%s)",
                authorizationId,
                token.principalName()
            )
        )

        val validExtensions = processExtensions(token, extensions)
        _tokenForNegotiatedProperty = token
        this.extensions = SaslExtensions(validExtensions)
        complete = true
        log.debug("Successfully authenticate User={}", token.principalName())

        return ByteArray(0)
    }

    @Throws(SaslException::class)
    private fun processExtensions(
        token: OAuthBearerToken,
        extensions: SaslExtensions
    ): Map<String, String> {
        val extensionsCallback = OAuthBearerExtensionsValidatorCallback(token, extensions)

        try {
            callbackHandler.handle(arrayOf(extensionsCallback))
        } catch (e: UnsupportedCallbackException) {
            // backwards compatibility - no extensions will be added
        } catch (e: IOException) {
            handleCallbackError(e)
        }

        if (extensionsCallback.invalidExtensions().isNotEmpty()) {
            val errorMessage = String.format(
                "Authentication failed: %d extensions are invalid! They are: %s",
                extensionsCallback.invalidExtensions().size,
                mkString(
                    map = extensionsCallback.invalidExtensions(),
                    keyValueSeparator = ": ",
                    elementSeparator = "; ",
                )
            )
            log.debug(errorMessage)
            throw SaslAuthenticationException(errorMessage)
        }
        return extensionsCallback.validatedExtensions()
    }

    @Throws(SaslException::class)
    private fun handleCallbackError(e: Exception) {
        val msg = String.format("%s: %s", INTERNAL_ERROR_ON_SERVER, e.message)
        log.debug(msg, e)
        throw SaslException(msg)
    }

    class OAuthBearerSaslServerFactory : SaslServerFactory {
        override fun createSaslServer(
            mechanism: String,
            protocol: String,
            serverName: String,
            props: Map<String?, *>?,
            callbackHandler: CallbackHandler
        ): SaslServer? {
            val mechanismNamesCompatibleWithPolicy = getMechanismNames(props)
            mechanismNamesCompatibleWithPolicy.forEach { compatibleMechanism ->
                if (compatibleMechanism == mechanism) return OAuthBearerSaslServer(callbackHandler)
            }
            return null
        }

        override fun getMechanismNames(props: Map<String?, *>?): Array<String> =
            mechanismNamesCompatibleWithPolicy(props)
    }

    companion object {

        private val log = LoggerFactory.getLogger(OAuthBearerSaslServer::class.java)

        private const val NEGOTIATED_PROPERTY_KEY_TOKEN =
            "${OAuthBearerLoginModule.OAUTHBEARER_MECHANISM}.token"

        private const val INTERNAL_ERROR_ON_SERVER =
            "Authentication could not be performed due to an internal error on the server"

        private fun jsonErrorResponse(
            errorStatus: String?,
            errorScope: String?,
            errorOpenIDConfiguration: String?
        ): String {
            var jsonErrorResponse = String.format("{\"status\":\"%s\"", errorStatus)

            if (errorScope != null) jsonErrorResponse =
                String.format("%s, \"scope\":\"%s\"",
                    jsonErrorResponse,
                    errorScope,
                )

            if (errorOpenIDConfiguration != null) jsonErrorResponse = String.format(
                "%s, \"openid-configuration\":\"%s\"",
                jsonErrorResponse,
                errorOpenIDConfiguration,
            )

            jsonErrorResponse = String.format("%s}", jsonErrorResponse)
            return jsonErrorResponse
        }

        fun mechanismNamesCompatibleWithPolicy(props: Map<String?, *>?): Array<String> =
            if (props != null && "true" == props[Sasl.POLICY_NOPLAINTEXT].toString()) emptyArray()
            else arrayOf(OAuthBearerLoginModule.OAUTHBEARER_MECHANISM)
    }
}
