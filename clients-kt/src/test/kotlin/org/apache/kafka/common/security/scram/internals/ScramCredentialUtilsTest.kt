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

import java.security.NoSuchAlgorithmException
import org.apache.kafka.common.security.authenticator.CredentialCache
import org.apache.kafka.common.security.scram.ScramCredential
import org.apache.kafka.common.security.scram.internals.ScramCredentialUtils.createCache
import org.apache.kafka.common.security.scram.internals.ScramCredentialUtils.credentialFromString
import org.apache.kafka.common.security.scram.internals.ScramCredentialUtils.credentialToString
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

class ScramCredentialUtilsTest {
    
    private lateinit var formatter: ScramFormatter
    
    @BeforeEach
    @Throws(NoSuchAlgorithmException::class)
    fun setUp() {
        formatter = ScramFormatter(ScramMechanism.SCRAM_SHA_256)
    }

    @Test
    fun stringConversion() {
        val credential = formatter.generateCredential(password = "password", iterations = 1024)
        assertTrue(credential.salt.isNotEmpty(), "Salt must not be empty")
        assertTrue(credential.storedKey.isNotEmpty(), "Stored key must not be empty")
        assertTrue(credential.serverKey.isNotEmpty(), "Server key must not be empty")
        val credential2 = credentialFromString(credentialToString(credential))
        assertContentEquals(credential.salt, credential2.salt)
        assertContentEquals(credential.storedKey, credential2.storedKey)
        assertContentEquals(credential.serverKey, credential2.serverKey)
        assertEquals(credential.iterations, credential2.iterations)
    }

    @Test
    fun generateCredential() {
        val credential1 = formatter.generateCredential(password = "password", iterations = 4096)
        val credential2 = formatter.generateCredential(password = "password", iterations = 4096)
        // Random salt should ensure that the credentials persisted are different every time
        assertNotEquals(credentialToString(credential1), credentialToString(credential2))
    }

    @Test
    fun invalidCredential() {
        assertFailsWith<IllegalArgumentException> { credentialFromString("abc") }
    }

    @Test
    fun missingFields() {
        val cred = credentialToString(
            formatter.generateCredential(password = "password", iterations = 2048),
        )
        assertFailsWith<IllegalArgumentException> {
            credentialFromString(cred.substring(cred.indexOf(',')))
        }
    }

    @Test
    fun extraneousFields() {
        val cred = credentialToString(
            formatter.generateCredential(password = "password", iterations = 2048)
        )
        assertFailsWith<IllegalArgumentException> { credentialFromString("$cred,a=test") }
    }

    @Test
    @Throws(Exception::class)
    fun scramCredentialCache() {
        val cache = CredentialCache()
        createCache(cache, listOf("SCRAM-SHA-512", "PLAIN"))
        assertNotNull(
            actual = cache.cache(
                mechanism = ScramMechanism.SCRAM_SHA_512.mechanismName,
                credentialClass = ScramCredential::class.java
            ),
            message = "Cache not created for enabled mechanism",
        )
        assertNull(
            actual = cache.cache(
                mechanism = ScramMechanism.SCRAM_SHA_256.mechanismName,
                credentialClass = ScramCredential::class.java,
            ),
            message = "Cache created for disabled mechanism",
        )
        val sha512Cache = assertNotNull(
            cache.cache(
                mechanism = ScramMechanism.SCRAM_SHA_512.mechanismName,
                credentialClass = ScramCredential::class.java,
            )
        )
        val formatter = ScramFormatter(ScramMechanism.SCRAM_SHA_512)
        val credentialA = formatter.generateCredential(password = "password", iterations = 4096)
        sha512Cache.put(username = "userA", credential = credentialA)
        assertEquals(credentialA, sha512Cache["userA"])
        assertNull(sha512Cache["userB"], "Invalid user credential")
    }
}
