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

package org.apache.kafka.common.security.oauthbearer.internals.secured

import org.jose4j.jws.AlgorithmIdentifiers
import org.jose4j.jwx.HeaderParameterNames
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import kotlin.test.assertFailsWith

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
abstract class AccessTokenValidatorTest : OAuthBearerTest() {
    
    @Throws(Exception::class)
    protected abstract fun createAccessTokenValidator(accessTokenBuilder: AccessTokenBuilder): AccessTokenValidator
    
    @Throws(Exception::class)
    protected fun createAccessTokenValidator(): AccessTokenValidator =
        createAccessTokenValidator(AccessTokenBuilder())

    @Test
    @Disabled("Kotlin migration: Validator validate does not accept nullable values.")
    @Throws(Exception::class)
    fun testNull() {
//        val validator = createAccessTokenValidator()
//        assertThrowsWithMessage(
//            clazz = ValidateException::class.java,
//            executable = { validator.validate(null) },
//            substring = "Empty JWT provided",
//        )
    }

    @Test
    @Throws(Exception::class)
    fun testEmptyString() {
        val validator = createAccessTokenValidator()
        assertThrowsWithMessage(
            clazz = ValidateException::class.java,
            executable = { validator.validate("") },
            substring = "Empty JWT provided",
        )
    }

    @Test
    @Throws(Exception::class)
    fun testWhitespace() {
        val validator = createAccessTokenValidator()
        assertThrowsWithMessage(
            clazz = ValidateException::class.java,
            executable = { validator.validate("    ") },
            substring = "Empty JWT provided",
        )
    }

    @Test
    @Throws(Exception::class)
    fun testEmptySections() {
        val validator = createAccessTokenValidator()
        assertThrowsWithMessage(
            clazz = ValidateException::class.java,
            executable = { validator.validate("..") },
            substring = "Malformed JWT provided",
        )
    }

    @Test
    @Throws(Exception::class)
    fun testMissingHeader() {
        val validator = createAccessTokenValidator()
        val header = ""
        val payload = createBase64JsonJwtSection { }
        val signature = ""
        val accessToken = "$header.$payload.$signature"
        assertFailsWith<ValidateException> { validator.validate(accessToken) }
    }

    @Test
    @Throws(Exception::class)
    fun testMissingPayload() {
        val validator = createAccessTokenValidator()
        val header = createBase64JsonJwtSection { node ->
            node.put(HeaderParameterNames.ALGORITHM, AlgorithmIdentifiers.NONE)
        }
        val payload = ""
        val signature = ""
        val accessToken = "$header.$payload.$signature"
        assertFailsWith<ValidateException> { validator.validate(accessToken) }
    }

    @Test
    @Throws(Exception::class)
    fun testMissingSignature() {
        val validator = createAccessTokenValidator()
        val header = createBase64JsonJwtSection { node ->
            node.put(HeaderParameterNames.ALGORITHM, AlgorithmIdentifiers.NONE)
        }
        val payload = createBase64JsonJwtSection { }
        val signature = ""
        val accessToken = "$header.$payload.$signature"
        assertFailsWith<ValidateException> { validator.validate(accessToken) }
    }
}
