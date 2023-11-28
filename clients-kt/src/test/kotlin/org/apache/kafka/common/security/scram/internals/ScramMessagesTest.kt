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

import java.util.Base64
import javax.security.sasl.SaslException
import org.apache.kafka.common.security.scram.internals.ScramMessages.AbstractScramMessage
import org.apache.kafka.common.security.scram.internals.ScramMessages.ClientFinalMessage
import org.apache.kafka.common.security.scram.internals.ScramMessages.ClientFirstMessage
import org.apache.kafka.common.security.scram.internals.ScramMessages.ServerFinalMessage
import org.apache.kafka.common.security.scram.internals.ScramMessages.ServerFirstMessage
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull
import kotlin.test.assertTrue

class ScramMessagesTest {

    private lateinit var formatter: ScramFormatter

    @BeforeEach
    @Throws(Exception::class)
    fun setUp() {
        formatter = ScramFormatter(ScramMechanism.SCRAM_SHA_256)
    }

    @Test
    @Throws(SaslException::class)
    fun validClientFirstMessage() {
        val nonce = formatter.secureRandomString()
        var m = ClientFirstMessage(
            saslName = "someuser",
            nonce = nonce,
            extensions = emptyMap(),
        )
        checkClientFirstMessage(
            message = m,
            saslName = "someuser",
            nonce = nonce,
            authzid = "",
        )

        // Default format used by Kafka client: only user and nonce are specified
        var str = "n,,n=testuser,r=$nonce"
        m = createScramMessage<ClientFirstMessage>(str)
        checkClientFirstMessage(
            message = m,
            saslName = "testuser",
            nonce = nonce,
            authzid = "",
        )
        m = ClientFirstMessage(m.toBytes())
        checkClientFirstMessage(
            message = m,
            saslName = "testuser",
            nonce = nonce,
            authzid = "",
        )

        // Username containing comma, encoded as =2C
        str = "n,,n=test=2Cuser,r=$nonce"
        m = createScramMessage<ClientFirstMessage>(str)
        checkClientFirstMessage(
            message = m,
            saslName = "test=2Cuser",
            nonce = nonce,
            authzid = "",
        )
        assertEquals("test,user", ScramFormatter.username(m.saslName))

        // Username containing equals, encoded as =3D
        str = "n,,n=test=3Duser,r=$nonce"
        m = createScramMessage<ClientFirstMessage>(str)
        checkClientFirstMessage(
            message = m,
            saslName = "test=3Duser",
            nonce = nonce,
            authzid = "",
        )
        assertEquals("test=user", ScramFormatter.username(m.saslName))

        // Optional authorization id specified
        str = "n,a=testauthzid,n=testuser,r=$nonce"
        checkClientFirstMessage(
            message = createScramMessage<ClientFirstMessage>(str),
            saslName = "testuser",
            nonce = nonce,
            authzid = "testauthzid",
        )

        // Optional reserved value specified
        for (reserved in VALID_RESERVED) {
            str = "n,,$reserved,n=testuser,r=$nonce"
            checkClientFirstMessage(
                message = createScramMessage<ClientFirstMessage>(str),
                saslName = "testuser",
                nonce = nonce,
                authzid = "",
            )
        }

        // Optional extension specified
        for (extension in VALID_EXTENSIONS) {
            str = "n,,n=testuser,r=$nonce,$extension"
            checkClientFirstMessage(
                message = createScramMessage<ClientFirstMessage>(str),
                saslName = "testuser",
                nonce = nonce,
                authzid = "",
            )
        }

        //optional tokenauth specified as extensions
        str = "n,,n=testuser,r=$nonce,tokenauth=true"
        m = createScramMessage<ClientFirstMessage>(str)
        assertTrue(m.extensions.tokenAuthenticated(), "Token authentication not set from extensions")
    }

    @Test
    fun invalidClientFirstMessage() {
        val nonce = formatter.secureRandomString()
        // Invalid entry in gs2-header
        var invalid = "n,x=something,n=testuser,r=$nonce"
        checkInvalidScramMessage<ClientFirstMessage>(invalid)

        // Invalid reserved entry
        for (reserved in INVALID_RESERVED) {
            invalid = "n,,$reserved,n=testuser,r=$nonce"
            checkInvalidScramMessage<ClientFirstMessage>(invalid)
        }

        // Invalid extension
        for (extension in INVALID_EXTENSIONS) {
            invalid = "n,,n=testuser,r=$nonce,$extension"
            checkInvalidScramMessage<ClientFirstMessage>(invalid)
        }
    }

    @Test
    @Throws(SaslException::class)
    fun validServerFirstMessage() {
        val clientNonce = formatter.secureRandomString()
        val serverNonce = formatter.secureRandomString()
        val nonce = clientNonce + serverNonce
        val salt = randomBytesAsString()
        var m = ServerFirstMessage(
            clientNonce = clientNonce,
            serverNonce = serverNonce,
            salt = toBytes(salt),
            iterations = 8192,
        )
        checkServerFirstMessage(
            message = m,
            nonce = nonce,
            salt = salt,
            iterations = 8192,
        )

        // Default format used by Kafka clients, only nonce, salt and iterations are specified
        var str = "r=$nonce,s=$salt,i=4096"
        m = createScramMessage<ServerFirstMessage>(str)
        checkServerFirstMessage(
            message = m,
            nonce = nonce,
            salt = salt,
            iterations = 4096,
        )
        m = ServerFirstMessage(m.toBytes())
        checkServerFirstMessage(
            message = m,
            nonce = nonce,
            salt = salt,
            iterations = 4096,
        )

        // Optional reserved value
        for (reserved in VALID_RESERVED) {
            str = "$reserved,r=$nonce,s=$salt,i=4096"
            checkServerFirstMessage(
                message = createScramMessage<ServerFirstMessage>(str),
                nonce = nonce,
                salt = salt,
                iterations = 4096,
            )
        }

        // Optional extension
        for (extension in VALID_EXTENSIONS) {
            str = "r=$nonce,s=$salt,i=4096,$extension"
            checkServerFirstMessage(
                message = createScramMessage<ServerFirstMessage>(str),
                nonce = nonce,
                salt = salt,
                iterations = 4096,
            )
        }
    }

    @Test
    fun invalidServerFirstMessage() {
        val nonce = formatter.secureRandomString()
        val salt = randomBytesAsString()

        // Invalid iterations
        var invalid = "r=$nonce,s=$salt,i=0"
        checkInvalidScramMessage<ServerFirstMessage>(invalid)

        // Invalid salt
        invalid = "r=$nonce,s==123,i=4096"
        checkInvalidScramMessage<ServerFirstMessage>(invalid)

        // Invalid format
        invalid = "r=$nonce,invalid,s=$salt,i=4096"
        checkInvalidScramMessage<ServerFirstMessage>(invalid)

        // Invalid reserved entry
        for (reserved in INVALID_RESERVED) {
            invalid = "$reserved,r=$nonce,s=$salt,i=4096"
            checkInvalidScramMessage<ServerFirstMessage>(invalid)
        }

        // Invalid extension
        for (extension in INVALID_EXTENSIONS) {
            invalid = "r=$nonce,s=$salt,i=4096,$extension"
            checkInvalidScramMessage<ServerFirstMessage>(invalid)
        }
    }

    @Test
    @Throws(SaslException::class)
    fun validClientFinalMessage() {
        val nonce = formatter.secureRandomString()
        val channelBinding = randomBytesAsString()
        val proof = randomBytesAsString()
        var m = ClientFinalMessage(toBytes(channelBinding), nonce)
        assertNull(m.proof, "Invalid proof")
        m.proof = toBytes(proof)
        checkClientFinalMessage(
            message = m,
            channelBinding = channelBinding,
            nonce = nonce,
            proof = proof,
        )

        // Default format used by Kafka client: channel-binding, nonce and proof are specified
        var str = "c=$channelBinding,r=$nonce,p=$proof"
        m = createScramMessage<ClientFinalMessage>(str)
        checkClientFinalMessage(
            message = m,
            channelBinding = channelBinding,
            nonce = nonce,
            proof = proof,
        )
        m = ClientFinalMessage(m.toBytes())
        checkClientFinalMessage(
            message = m,
            channelBinding = channelBinding,
            nonce = nonce,
            proof = proof,
        )

        // Optional extension specified
        for (extension in VALID_EXTENSIONS) {
            str = "c=$channelBinding,r=$nonce,$extension,p=$proof"
            checkClientFinalMessage(
                message = createScramMessage<ClientFinalMessage>(str),
                channelBinding = channelBinding,
                nonce = nonce,
                proof = proof,
            )
        }
    }

    @Test
    fun invalidClientFinalMessage() {
        val nonce = formatter.secureRandomString()
        val channelBinding = randomBytesAsString()
        val proof = randomBytesAsString()

        // Invalid channel binding
        var invalid = "c=ab,r=$nonce,p=$proof"
        checkInvalidScramMessage<ClientFirstMessage>(invalid)

        // Invalid proof
        invalid = "c=$channelBinding,r=$nonce,p=123"
        checkInvalidScramMessage<ClientFirstMessage>(invalid)

        // Invalid extensions
        for (extension in INVALID_EXTENSIONS) {
            invalid = "c=$channelBinding,r=$nonce,$extension,p=$proof"
            checkInvalidScramMessage<ClientFinalMessage>(invalid)
        }
    }

    @Test
    @Throws(SaslException::class)
    fun validServerFinalMessage() {
        val serverSignature = randomBytesAsString()
        var m = ServerFinalMessage(error = "unknown-user", serverSignature = null)
        checkServerFinalMessage(
            message = m,
            error = "unknown-user",
            serverSignature = null,
        )
        m = ServerFinalMessage(error = null, serverSignature = toBytes(serverSignature))
        checkServerFinalMessage(
            message = m,
            error = null,
            serverSignature = serverSignature,
        )

        // Default format used by Kafka clients for successful final message
        var str = "v=$serverSignature"
        m = createScramMessage<ServerFinalMessage>(str)
        checkServerFinalMessage(
            message = m,
            error = null,
            serverSignature = serverSignature,
        )
        m = ServerFinalMessage(m.toBytes())
        checkServerFinalMessage(
            message = m,
            error = null,
            serverSignature = serverSignature,
        )

        // Default format used by Kafka clients for final message with error
        str = "e=other-error"
        m = createScramMessage<ServerFinalMessage>(str)
        checkServerFinalMessage(
            message = m,
            error = "other-error",
            serverSignature = null,
        )
        m = ServerFinalMessage(m.toBytes())
        checkServerFinalMessage(
            message = m,
            error = "other-error",
            serverSignature = null,
        )

        // Optional extension
        for (extension in VALID_EXTENSIONS) {
            str = "v=$serverSignature,$extension"
            checkServerFinalMessage(
                message = createScramMessage<ServerFinalMessage>(str),
                error = null,
                serverSignature = serverSignature,
            )
        }
    }

    @Test
    fun invalidServerFinalMessage() {
        val serverSignature = randomBytesAsString()

        // Invalid error
        var invalid = "e=error1,error2"
        checkInvalidScramMessage<ServerFinalMessage>(invalid)

        // Invalid server signature
        invalid = "v=1=23"
        checkInvalidScramMessage<ServerFinalMessage>(invalid)

        // Invalid extensions
        for (extension in INVALID_EXTENSIONS) {
            invalid = "v=$serverSignature,$extension"
            checkInvalidScramMessage<ServerFinalMessage>(invalid)
            invalid = "e=unknown-user,$extension"
            checkInvalidScramMessage<ServerFinalMessage>(invalid)
        }
    }

    private fun randomBytesAsString(): String {
        return Base64.getEncoder().encodeToString(formatter.secureRandomBytes())
    }

    private fun toBytes(base64Str: String): ByteArray {
        return Base64.getDecoder().decode(base64Str)
    }

    private fun checkClientFirstMessage(
        message: ClientFirstMessage,
        saslName: String,
        nonce: String,
        authzid: String,
    ) {
        assertEquals(saslName, message.saslName)
        assertEquals(nonce, message.nonce)
        assertEquals(authzid, message.authorizationId)
    }

    private fun checkServerFirstMessage(
        message: ServerFirstMessage,
        nonce: String,
        salt: String,
        iterations: Int,
    ) {
        assertEquals(nonce, message.nonce)
        assertContentEquals(Base64.getDecoder().decode(salt), message.salt)
        assertEquals(iterations, message.iterations)
    }

    private fun checkClientFinalMessage(
        message: ClientFinalMessage,
        channelBinding: String,
        nonce: String,
        proof: String,
    ) {
        assertContentEquals(Base64.getDecoder().decode(channelBinding), message.channelBinding)
        assertEquals(nonce, message.nonce)
        assertContentEquals(Base64.getDecoder().decode(proof), message.proof)
    }

    private fun checkServerFinalMessage(
        message: ServerFinalMessage,
        error: String?,
        serverSignature: String?,
    ) {
        assertEquals(error, message.error)
        if (serverSignature == null) assertNull(message.serverSignature, "Unexpected server signature")
        else assertContentEquals(Base64.getDecoder().decode(serverSignature), message.serverSignature)
    }

    @Throws(SaslException::class)
    private inline fun <reified T : AbstractScramMessage?> createScramMessage(message: String): T {
        val bytes = message.toByteArray()
        return when (T::class.java) {
            ClientFirstMessage::class.java -> ClientFirstMessage(bytes) as T
            ServerFirstMessage::class.java -> ServerFirstMessage(bytes) as T
            ClientFinalMessage::class.java -> ClientFinalMessage(bytes) as T
            ServerFinalMessage::class.java -> ServerFinalMessage(bytes) as T
            else -> throw IllegalArgumentException("Unknown message type: ${T::class}")
        }
    }

    private inline fun <reified T : AbstractScramMessage?> checkInvalidScramMessage(message: String) {
        assertFailsWith<SaslException>(
            message = "Exception not throws for invalid message of type ${T::class} : $message",
        ) { createScramMessage<T>(message) }
    }

    companion object {

        private val VALID_EXTENSIONS = arrayOf(
            "ext=val1",
            "anotherext=name1=value1 name2=another test value \"\'!$[]()",
            "first=val1,second=name1 = value ,third=123",
        )

        private val INVALID_EXTENSIONS = arrayOf(
            "ext1=value",
            "ext",
            "ext=value1,value2",
            "ext=,",
            "ext =value",
        )

        private val VALID_RESERVED = arrayOf(
            "m=reserved-value",
            "m=name1=value1 name2=another test value \"\'!$[]()",
        )

        private val INVALID_RESERVED = arrayOf(
            "m",
            "m=name,value",
            "m=,",
        )
    }
}
