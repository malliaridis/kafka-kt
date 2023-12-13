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
import java.util.Base64
import java.util.regex.Pattern
import javax.security.sasl.SaslException
import org.apache.kafka.common.utils.Utils.mkString

/**
 * SCRAM request/response message creation and parsing based on
 * [RFC 5802](https://tools.ietf.org/html/rfc5802).
 */
class ScramMessages {
    
    abstract class AbstractScramMessage {
        
        abstract fun toMessage(): String
        
        fun toBytes(): ByteArray {
            return toMessage().toByteArray(StandardCharsets.UTF_8)
        }

        protected fun toMessage(messageBytes: ByteArray?): String {
            return String(messageBytes!!, StandardCharsets.UTF_8)
        }

        companion object {
            
            const val ALPHA = "[A-Za-z]+"
            
            const val VALUE_SAFE = "[\\x01-\\x7F&&[^=,]]+"
            
            const val VALUE = "[\\x01-\\x7F&&[^,]]+"
            
            const val PRINTABLE = "[\\x21-\\x7E&&[^,]]+"
            
            const val SASLNAME = "(?:[\\x01-\\x7F&&[^=,]]|=2C|=3D)+"
            
            const val BASE64_CHAR = "[a-zA-Z0-9/+]"
            
            val BASE64 = "(?:$BASE64_CHAR{4})*(?:$BASE64_CHAR{3}=|$BASE64_CHAR{2}==)?"
            
            val RESERVED = "(m=$VALUE,)?"
            
            val EXTENSIONS = "(,$ALPHA=$VALUE)*"
        }
    }

    /**
     * Format:
     * ```
     * gs2-header [reserved-mext ","] username "," nonce ["," extensions]
     * ```
     * 
     * Limitations:
     * - Only gs2-header "n" is supported.
     * - Extensions are ignored.
     */
    class ClientFirstMessage : AbstractScramMessage {
        
        val saslName: String
        
        val nonce: String
        
        val authorizationId: String
        
        val extensions: ScramExtensions

        constructor(messageBytes: ByteArray?) {
            val message = toMessage(messageBytes)
            val matcher = PATTERN.matcher(message)
            if (!matcher.matches()) throw SaslException("Invalid SCRAM client first message format: $message")
            authorizationId = matcher.group("authzid") ?: ""
            saslName = matcher.group("saslname")
            nonce = matcher.group("nonce")
            val extString = matcher.group("extensions")
            extensions = if (extString.startsWith(",")) ScramExtensions(extString.substring(1))
            else ScramExtensions()
        }

        constructor(
            saslName: String,
            nonce: String,
            extensions: Map<String, String>,
        ) {
            this.saslName = saslName
            this.nonce = nonce
            this.extensions = ScramExtensions(extensions)
            authorizationId = "" // Optional authzid not specified in gs2-header
        }

        @Deprecated(
            message = "Use property instead",
            replaceWith = ReplaceWith("saslName"),
        )
        fun saslName(): String = saslName

        @Deprecated(
            message = "Use property instead",
            replaceWith = ReplaceWith("nonce"),
        )
        fun nonce(): String = nonce

        @Deprecated(
            message = "Use property instead",
            replaceWith = ReplaceWith("authorizationId"),
        )
        fun authorizationId(): String = authorizationId

        @Deprecated(
            message = "Use property instead",
            replaceWith = ReplaceWith("gs2Header"),
        )
        fun gs2Header(): String = "n,$authorizationId,"
        
        val gs2Header: String
            get() = "n,$authorizationId,"

        @Deprecated(
            message = "Use property instead",
            replaceWith = ReplaceWith("extensions"),
        )
        fun extensions(): ScramExtensions = extensions

        fun clientFirstMessageBare(): String {
            val extensionStr = mkString(
                map = extensions.map(), 
                begin = "", 
                end = "", 
                keyValueSeparator = "=", 
                elementSeparator = ","
            )
            
            return if (extensionStr.isEmpty()) "n=$saslName,r=$nonce"
            else "n=$saslName,r=$nonce,$extensionStr"
        }

        override fun toMessage(): String {
            return gs2Header + clientFirstMessageBare()
        }

        companion object {
            private val PATTERN = Pattern.compile(
                "n,(a=(?<authzid>$SASLNAME))?,${RESERVED}n=(?<saslname>$SASLNAME),r=(?<nonce>$PRINTABLE)(?<extensions>$EXTENSIONS)"
            )
        }
    }

    /**
     * Format:
     * ```
     * [reserved-mext ","] nonce "," salt "," iteration-count ["," extensions]
     * ```
     * 
     * Limitations:
     * - Extensions are ignored.
     */
    class ServerFirstMessage : AbstractScramMessage {
        
        val nonce: String
        
        val salt: ByteArray
        
        val iterations: Int

        constructor(messageBytes: ByteArray?) {
            val message = toMessage(messageBytes)
            val matcher = PATTERN.matcher(message)
            
            if (!matcher.matches()) throw SaslException("Invalid SCRAM server first message format: $message")
            
            try {
                iterations = matcher.group("iterations").toInt()
                if (iterations <= 0)
                    throw SaslException("Invalid SCRAM server first message format: invalid iterations $iterations")
            } catch (e: NumberFormatException) {
                throw SaslException("Invalid SCRAM server first message format: invalid iterations", e)
            }
            
            nonce = matcher.group("nonce")
            val salt = matcher.group("salt")
            this.salt = Base64.getDecoder().decode(salt)
        }

        constructor(clientNonce: String, serverNonce: String, salt: ByteArray, iterations: Int) {
            nonce = clientNonce + serverNonce
            this.salt = salt
            this.iterations = iterations
        }

        @Deprecated(
            message = "Use property instead",
            replaceWith = ReplaceWith("nonce"),
        )
        fun nonce(): String = nonce

        @Deprecated(
            message = "Use property instead",
            replaceWith = ReplaceWith("salt"),
        )
        fun salt(): ByteArray = salt

        @Deprecated(
            message = "Use property instead",
            replaceWith = ReplaceWith("iterations"),
        )
        fun iterations(): Int = iterations

        override fun toMessage(): String {
            return String.format(
                "r=%s,s=%s,i=%d",
                nonce,
                Base64.getEncoder().encodeToString(salt),
                iterations
            )
        }

        companion object {
            private val PATTERN = Pattern.compile(
                "${RESERVED}r=(?<nonce>$PRINTABLE),s=(?<salt>$BASE64),i=(?<iterations>[0-9]+)$EXTENSIONS"
            )
        }
    }

    /**
     * Format:
     * channel-binding "," nonce ["," extensions]"," proof
     * Limitations:
     * Extensions are ignored.
     *
     */
    class ClientFinalMessage : AbstractScramMessage {
        
        val channelBinding: ByteArray
        
        val nonce: String
        
        var proof: ByteArray? = null

        constructor(messageBytes: ByteArray?) {
            val message = toMessage(messageBytes)
            val matcher = PATTERN.matcher(message)
            if (!matcher.matches()) throw SaslException("Invalid SCRAM client final message format: $message")
            channelBinding = Base64.getDecoder().decode(matcher.group("channel"))
            nonce = matcher.group("nonce")
            proof = Base64.getDecoder().decode(matcher.group("proof"))
        }

        constructor(channelBinding: ByteArray, nonce: String) {
            this.channelBinding = channelBinding
            this.nonce = nonce
        }

        @Deprecated(
            message = "Use property instead",
            replaceWith = ReplaceWith("channelBinding"),
        )
        fun channelBinding(): ByteArray = channelBinding

        @Deprecated(
            message = "Use property instead",
            replaceWith = ReplaceWith("nonce"),
        )
        fun nonce(): String = nonce

        @Deprecated(
            message = "Use property instead",
            replaceWith = ReplaceWith("proof"),
        )
        fun proof(): ByteArray? = proof

        @Deprecated("Use property instead")
        fun proof(proof: ByteArray) {
            this.proof = proof
        }

        fun clientFinalMessageWithoutProof(): String =
            "c=${Base64.getEncoder().encodeToString(channelBinding)},r=$nonce"

        override fun toMessage(): String =
            "${clientFinalMessageWithoutProof()},p=${Base64.getEncoder().encodeToString(proof)}"

        companion object {
            
            private val PATTERN = Pattern.compile(
                "c=(?<channel>$BASE64),r=(?<nonce>$PRINTABLE)$EXTENSIONS,p=(?<proof>$BASE64)"
            )
        }
    }

    /**
     * Format:
     * ```
     * ("e=" server-error-value | "v=" base64_server_signature) ["," extensions]
     * ```
     * 
     * Limitations:
     * - Extensions are ignored.
     */
    class ServerFinalMessage : AbstractScramMessage {
        
        val error: String?
        
        val serverSignature: ByteArray?

        constructor(messageBytes: ByteArray?) {
            val message = toMessage(messageBytes)
            val matcher = PATTERN.matcher(message)
            if (!matcher.matches()) throw SaslException("Invalid SCRAM server final message format: $message")
            var error: String? = null
            
            try {
                error = matcher.group("error")
            } catch (_: IllegalArgumentException) {
                // ignore
            }
            
            if (error == null) {
                serverSignature = Base64.getDecoder().decode(matcher.group("signature"))
                this.error = null
            } else {
                serverSignature = null
                this.error = error
            }
        }

        constructor(error: String?, serverSignature: ByteArray?) {
            this.error = error
            this.serverSignature = serverSignature
        }

        @Deprecated(
            message = "Use property instead",
            replaceWith = ReplaceWith("error"),
        )
        fun error(): String? = error

        @Deprecated(
            message = "Use property instead",
            replaceWith = ReplaceWith("serverSignature"),
        )
        fun serverSignature(): ByteArray? = serverSignature

        override fun toMessage(): String {
            return if (error != null) "e=$error" else "v=" + Base64.getEncoder()
                .encodeToString(serverSignature)
        }

        companion object {
            
            private val PATTERN = Pattern.compile(
                "(?:e=(?<error>$VALUE_SAFE))|(?:v=(?<signature>$BASE64))$EXTENSIONS"
            )
        }
    }
}
