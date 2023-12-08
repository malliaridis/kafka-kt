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
import org.apache.kafka.common.security.scram.internals.ScramMessages.ClientFinalMessage
import org.apache.kafka.common.security.scram.internals.ScramMessages.ClientFirstMessage
import org.apache.kafka.common.security.scram.internals.ScramMessages.ServerFinalMessage
import org.apache.kafka.common.security.scram.internals.ScramMessages.ServerFirstMessage
import org.junit.jupiter.api.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals

class ScramFormatterTest {

    /**
     * Tests that the formatter implementation produces the same values for the
     * example included in [RFC 7677](https://tools.ietf.org/html/rfc5802#section-5)
     */
    @Test
    @Throws(Exception::class)
    fun rfc7677Example() {
        val formatter = ScramFormatter(ScramMechanism.SCRAM_SHA_256)
        val password = "pencil"
        val c1 = "n,,n=user,r=rOprNGfwEbeRWgbNEkqO"
        val s1 = "r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF\$k0,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096"
        val c2 =
            "c=biws,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF\$k0,p=dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ="
        val s2 = "v=6rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4="
        val clientFirst = ClientFirstMessage(ScramFormatter.toBytes(c1))
        val serverFirst = ServerFirstMessage(ScramFormatter.toBytes(s1))
        val clientFinal = ClientFinalMessage(ScramFormatter.toBytes(c2))
        val serverFinal = ServerFinalMessage(ScramFormatter.toBytes(s2))
        val username = clientFirst.saslName
        assertEquals("user", username)
        val clientNonce = clientFirst.nonce
        assertEquals("rOprNGfwEbeRWgbNEkqO", clientNonce)
        val serverNonce = serverFirst.nonce.substring(clientNonce.length)
        assertEquals("%hvYDpWUa2RaTCAfuxFIlj)hNlF\$k0", serverNonce)
        val salt = serverFirst.salt
        assertContentEquals(Base64.getDecoder().decode("W22ZaJ0SNY7soEsUEjb6gQ=="), salt)
        val iterations = serverFirst.iterations
        assertEquals(4096, iterations)
        val channelBinding = clientFinal.channelBinding
        assertContentEquals(Base64.getDecoder().decode("biws"), channelBinding)
        val serverSignature = serverFinal.serverSignature
        assertContentEquals(
            expected = Base64.getDecoder().decode("6rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4="),
            actual = serverSignature,
        )
        val saltedPassword = formatter.saltedPassword(
            password = password,
            salt = salt,
            iterations = iterations,
        )
        val serverKey = formatter.serverKey(saltedPassword)
        val computedProof = formatter.clientProof(
            saltedPassword = saltedPassword,
            clientFirstMessage = clientFirst,
            serverFirstMessage = serverFirst,
            clientFinalMessage = clientFinal,
        )
        assertContentEquals(clientFinal.proof, computedProof)
        val computedSignature = formatter.serverSignature(
            serverKey = serverKey,
            clientFirstMessage = clientFirst,
            serverFirstMessage = serverFirst,
            clientFinalMessage = clientFinal,
        )
        assertContentEquals(serverFinal.serverSignature, computedSignature)

        // Minimum iterations defined in RFC-7677
        assertEquals(4096, ScramMechanism.SCRAM_SHA_256.minIterations())
    }

    /**
     * Tests encoding of username
     */
    @Test
    @Throws(Exception::class)
    fun saslName() {
        val usernames = arrayOf("user1", "123", "1,2", "user=A", "user==B", "user,1", "user 1", ",", "=", ",=", "==")
        val formatter = ScramFormatter(ScramMechanism.SCRAM_SHA_256)
        for (username in usernames) {
            val saslName = ScramFormatter.saslName(username)
            // There should be no commas in saslName (comma is used as field separator in SASL messages)
            assertEquals(-1, saslName.indexOf(','))
            // There should be no "=" in the saslName apart from those used in encoding (comma is =2C and equals is =3D)
            assertEquals(-1, saslName.replace("=2C", "")
                .replace("=3D", "")
                .indexOf('='))
            assertEquals(username, ScramFormatter.username(saslName))
        }
    }
}
