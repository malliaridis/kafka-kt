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

import java.math.BigInteger
import java.nio.charset.StandardCharsets
import java.security.InvalidKeyException
import java.security.MessageDigest
import java.security.SecureRandom
import java.util.regex.Matcher
import java.util.regex.Pattern
import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.security.scram.ScramCredential
import org.apache.kafka.common.security.scram.internals.ScramMessages.ClientFinalMessage
import org.apache.kafka.common.security.scram.internals.ScramMessages.ClientFirstMessage
import org.apache.kafka.common.security.scram.internals.ScramMessages.ServerFirstMessage

/**
 * Scram message salt and hash functions defined in [RFC 5802](https://tools.ietf.org/html/rfc5802).
 */
class ScramFormatter(mechanism: ScramMechanism) {

    private val messageDigest: MessageDigest

    private val mac: Mac

    private val random: SecureRandom

    init {
        messageDigest = MessageDigest.getInstance(mechanism.hashAlgorithm)
        mac = Mac.getInstance(mechanism.macAlgorithm)
        random = SecureRandom()
    }

    @Throws(InvalidKeyException::class)
    fun hmac(key: ByteArray, bytes: ByteArray?): ByteArray {
        mac.init(SecretKeySpec(key, mac.algorithm))
        return mac.doFinal(bytes)
    }

    fun hash(str: ByteArray): ByteArray {
        return messageDigest.digest(str)
    }

    @Throws(InvalidKeyException::class)
    fun hi(str: ByteArray, salt: ByteArray?, iterations: Int): ByteArray {
        mac.init(SecretKeySpec(str, mac.algorithm))
        mac.update(salt)

        val u1 = mac.doFinal(byteArrayOf(0, 0, 0, 1))
        var prev = u1
        var result = u1

        for (i in 2..iterations) {
            val ui = hmac(str, prev)
            result = xor(result, ui)
            prev = ui
        }
        return result
    }

    @Throws(InvalidKeyException::class)
    fun saltedPassword(password: String, salt: ByteArray?, iterations: Int): ByteArray {
        return hi(normalize(password), salt, iterations)
    }

    @Throws(InvalidKeyException::class)
    fun clientKey(saltedPassword: ByteArray): ByteArray =
        hmac(saltedPassword, toBytes("Client Key"))

    fun storedKey(clientKey: ByteArray): ByteArray = hash(clientKey)

    @Throws(InvalidKeyException::class)
    fun clientSignature(
        storedKey: ByteArray,
        clientFirstMessage: ClientFirstMessage,
        serverFirstMessage: ServerFirstMessage,
        clientFinalMessage: ClientFinalMessage,
    ): ByteArray {
        val authMessage = authMessage(clientFirstMessage, serverFirstMessage, clientFinalMessage)
        return hmac(storedKey, authMessage)
    }

    @Throws(InvalidKeyException::class)
    fun clientProof(
        saltedPassword: ByteArray,
        clientFirstMessage: ClientFirstMessage,
        serverFirstMessage: ServerFirstMessage,
        clientFinalMessage: ClientFinalMessage
    ): ByteArray {
        val clientKey = clientKey(saltedPassword)
        val storedKey = hash(clientKey)
        val clientSignature = hmac(
            key = storedKey,
            bytes = authMessage(
                clientFirstMessage = clientFirstMessage,
                serverFirstMessage = serverFirstMessage,
                clientFinalMessage = clientFinalMessage,
            )
        )

        return xor(clientKey, clientSignature)
    }

    private fun authMessage(
        clientFirstMessage: ClientFirstMessage,
        serverFirstMessage: ServerFirstMessage,
        clientFinalMessage: ClientFinalMessage
    ): ByteArray {
        return toBytes(
            authMessage(
                clientFirstMessageBare = clientFirstMessage.clientFirstMessageBare(),
                serverFirstMessage = serverFirstMessage.toMessage(),
                clientFinalMessageWithoutProof = clientFinalMessage.clientFinalMessageWithoutProof()
            )
        )
    }

    fun storedKey(clientSignature: ByteArray, clientProof: ByteArray): ByteArray {
        return hash(xor(clientSignature, clientProof))
    }

    @Throws(InvalidKeyException::class)
    fun serverKey(saltedPassword: ByteArray): ByteArray {
        return hmac(saltedPassword, toBytes("Server Key"))
    }

    @Throws(InvalidKeyException::class)
    fun serverSignature(
        serverKey: ByteArray,
        clientFirstMessage: ClientFirstMessage,
        serverFirstMessage: ServerFirstMessage,
        clientFinalMessage: ClientFinalMessage
    ): ByteArray {
        return hmac(
            key = serverKey,
            bytes = authMessage(
                clientFirstMessage = clientFirstMessage,
                serverFirstMessage = serverFirstMessage,
                clientFinalMessage = clientFinalMessage,
            )
        )
    }

    fun generateCredential(password: String, iterations: Int): ScramCredential {
        return try {
            val salt = secureRandomBytes()
            val saltedPassword = saltedPassword(password, salt, iterations)
            generateCredential(salt, saltedPassword, iterations)
        } catch (e: InvalidKeyException) {
            throw KafkaException("Could not create credential", e)
        }
    }

    fun generateCredential(
        salt: ByteArray,
        saltedPassword: ByteArray,
        iterations: Int
    ): ScramCredential {
        return try {
            val clientKey = clientKey(saltedPassword)
            val storedKey = storedKey(clientKey)
            val serverKey = serverKey(saltedPassword)
            ScramCredential(salt, storedKey, serverKey, iterations)
        } catch (e: InvalidKeyException) {
            throw KafkaException("Could not create credential", e)
        }
    }

    fun secureRandomString(): String = BigInteger(130, random).toString(Character.MAX_RADIX)

    fun secureRandomBytes(): ByteArray = toBytes(secureRandomString(random))

    companion object {

        private val EQUAL = Pattern.compile("=", Pattern.LITERAL)

        private val COMMA = Pattern.compile(",", Pattern.LITERAL)

        private val EQUAL_TWO_C = Pattern.compile("=2C", Pattern.LITERAL)

        private val EQUAL_THREE_D = Pattern.compile("=3D", Pattern.LITERAL)

        fun xor(first: ByteArray, second: ByteArray): ByteArray {
            require(first.size == second.size) { "Argument arrays must be of the same length" }
            val result = ByteArray(first.size)
            for (i in result.indices) result[i] = (first[i].toInt() xor second[i].toInt()).toByte()
            return result
        }

        fun normalize(str: String): ByteArray = toBytes(str)

        fun saslName(username: String): String {
            val replace1 = EQUAL.matcher(username).replaceAll(Matcher.quoteReplacement("=3D"))
            return COMMA.matcher(replace1).replaceAll(Matcher.quoteReplacement("=2C"))
        }

        fun username(saslName: String): String {
            val username = EQUAL_TWO_C.matcher(saslName)
                .replaceAll(Matcher.quoteReplacement(","))

            require(
                EQUAL_THREE_D.matcher(username)
                    .replaceAll(Matcher.quoteReplacement(""))
                    .indexOf('=') < 0
            ) { "Invalid username: $saslName" }

            return EQUAL_THREE_D.matcher(username).replaceAll(Matcher.quoteReplacement("="))
        }

        fun authMessage(
            clientFirstMessageBare: String,
            serverFirstMessage: String,
            clientFinalMessageWithoutProof: String
        ): String = "$clientFirstMessageBare,$serverFirstMessage,$clientFinalMessageWithoutProof"

        fun secureRandomString(random: SecureRandom): String =
            BigInteger(130, random).toString(Character.MAX_RADIX)

        fun secureRandomBytes(random: SecureRandom): ByteArray = toBytes(secureRandomString(random))

        fun toBytes(str: String): ByteArray = str.toByteArray(StandardCharsets.UTF_8)
    }
}
