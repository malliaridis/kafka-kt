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

package org.apache.kafka.common.security.plain.internals

import javax.security.auth.callback.CallbackHandler
import javax.security.auth.callback.NameCallback
import javax.security.sasl.Sasl
import javax.security.sasl.SaslException
import javax.security.sasl.SaslServer
import javax.security.sasl.SaslServerFactory
import org.apache.kafka.common.errors.SaslAuthenticationException
import org.apache.kafka.common.security.plain.PlainAuthenticateCallback

/**
 * Simple SaslServer implementation for SASL/PLAIN. In order to make this implementation fully
 * pluggable, authentication of username/password is fully contained within the server
 * implementation.
 *
 * Valid users with passwords are specified in the Jaas configuration file. Each user is specified
 * with `user_<username>` as key and `<password>` as value. This is consistent with Zookeeper
 * Digest-MD5 implementation.
 *
 * To avoid storing clear passwords on disk or to integrate with external authentication servers in
 * production systems, this module can be replaced with a different implementation.
 */
class PlainSaslServer(private val callbackHandler: CallbackHandler) : SaslServer {

    private var complete = false

    private lateinit var authorizationId: String

    /**
     * @throws SaslAuthenticationException if username/password combination is invalid or if the
     * requested authorization id is not the same as username.
     *
     * **Note:** This method may throw [SaslAuthenticationException] to provide custom error
     * messages to clients. But care should be taken to avoid including any information in the
     * exception message that should not be leaked to unauthenticated clients. It may be safer to
     * throw [SaslException] in some cases so that a standard error message is returned to clients.
     */
    @Throws(SaslAuthenticationException::class)
    override fun evaluateResponse(responseBytes: ByteArray): ByteArray {
        /*
         * Message format (from https://tools.ietf.org/html/rfc4616):
         *
         * message   = [authzid] UTF8NUL authcid UTF8NUL passwd
         * authcid   = 1*SAFE ; MUST accept up to 255 octets
         * authzid   = 1*SAFE ; MUST accept up to 255 octets
         * passwd    = 1*SAFE ; MUST accept up to 255 octets
         * UTF8NUL   = %x00 ; UTF-8 encoded NUL character
         *
         * SAFE      = UTF1 / UTF2 / UTF3 / UTF4
         *                ;; any UTF-8 encoded Unicode character except NUL
         */
        val response = String(responseBytes)
        val tokens = extractTokens(response)
        val authorizationIdFromClient = tokens[0]
        val username = tokens[1]
        val password = tokens[2]

        if (username.isEmpty()) throw SaslAuthenticationException(
            "Authentication failed: username not specified"
        )

        if (password.isEmpty()) throw SaslAuthenticationException(
            "Authentication failed: password not specified"
        )

        val nameCallback = NameCallback("username", username)
        val authenticateCallback = PlainAuthenticateCallback(password.toCharArray())
        try {
            callbackHandler.handle(arrayOf(nameCallback, authenticateCallback))
        } catch (e: Throwable) {
            throw SaslAuthenticationException(
                "Authentication failed: credentials for user could not be verified",
                e
            )
        }
        if (!authenticateCallback.authenticated) throw SaslAuthenticationException(
            "Authentication failed: Invalid username or password"
        )
        if (authorizationIdFromClient.isNotEmpty() && authorizationIdFromClient != username)
            throw SaslAuthenticationException(
                "Authentication failed: Client requested an authorization id that is different from username"
            )

        authorizationId = username
        complete = true
        return ByteArray(0)
    }

    private fun extractTokens(string: String): List<String> {
        val tokens: MutableList<String> = ArrayList()
        var startIndex = 0

        for (i in 0..3) {
            val endIndex = string.indexOf("\u0000", startIndex)
            if (endIndex == -1) {
                tokens.add(string.substring(startIndex))
                break
            }
            tokens.add(string.substring(startIndex, endIndex))
            startIndex = endIndex + 1
        }

        if (tokens.size != 3) throw SaslAuthenticationException(
            "Invalid SASL/PLAIN response: expected 3 tokens, got ${tokens.size}"
        )
        return tokens
    }

    override fun getAuthorizationID(): String {
        check(complete) { "Authentication exchange has not completed" }
        return authorizationId
    }

    override fun getMechanismName(): String = PLAIN_MECHANISM

    override fun getNegotiatedProperty(propName: String): Any? {
        check(complete) { "Authentication exchange has not completed" }
        return null
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

    override fun dispose() = Unit

    class PlainSaslServerFactory : SaslServerFactory {

        @Throws(SaslException::class)
        override fun createSaslServer(
            mechanism: String,
            protocol: String,
            serverName: String,
            props: Map<String?, *>?,
            cbh: CallbackHandler
        ): SaslServer {
            if (PLAIN_MECHANISM != mechanism) throw SaslException(
                String.format(
                    "Mechanism \'%s\' is not supported. Only PLAIN is supported.",
                    mechanism
                )
            )
            return PlainSaslServer(cbh)
        }

        override fun getMechanismNames(props: Map<String, *>?): Array<String> {
            if (props == null) return arrayOf(PLAIN_MECHANISM)
            val noPlainText = props[Sasl.POLICY_NOPLAINTEXT] as String?
            return if ("true" == noPlainText) arrayOf() else arrayOf(PLAIN_MECHANISM)
        }
    }

    companion object {
        const val PLAIN_MECHANISM = "PLAIN"
    }
}
