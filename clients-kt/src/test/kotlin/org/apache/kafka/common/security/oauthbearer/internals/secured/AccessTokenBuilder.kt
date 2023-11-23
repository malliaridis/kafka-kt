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

import com.fasterxml.jackson.databind.ObjectMapper
import java.io.IOException
import org.apache.kafka.common.security.oauthbearer.internals.secured.ClaimValidationUtils.validateClaimNameOverride
import org.apache.kafka.common.utils.MockTime
import org.apache.kafka.common.utils.Time
import org.jose4j.jwk.PublicJsonWebKey
import org.jose4j.jws.JsonWebSignature
import org.jose4j.jwt.ReservedClaimNames
import org.jose4j.lang.JoseException

class AccessTokenBuilder(time: Time = MockTime()) {

    private val objectMapper = ObjectMapper()

    var alg: String? = null

    var audience: String? = null

    var subject: String? = "jdoe"

    var subjectClaimName = ReservedClaimNames.SUBJECT

    private var scope: Any = "engineering"
        set(value) {
            require(value is String || value is Collection<*>) {
                "$scopeClaimName parameter must be a ${String::class.java.getName()} or " +
                        "a ${MutableCollection::class.java.getName()} containing ${String::class.java.getName()}"
            }
            field = value
        }

    val scopeClaimName = "scope"

    val issuedAtSeconds: Long = time.milliseconds() / 1000

    var expirationSeconds: Long? = issuedAtSeconds + 60

    var jwk: PublicJsonWebKey? = null

    private val customClaims: MutableMap<String, String> = HashMap()

    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("alg"),
    )
    fun alg(): String? = alg

    @Deprecated("Use property instead")
    fun alg(alg: String?): AccessTokenBuilder {
        this.alg = alg
        return this
    }

    @Deprecated("Use property instead")
    fun audience(audience: String?): AccessTokenBuilder {
        this.audience = audience
        return this
    }

    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("subject"),
    )
    fun subject(): String? = subject

    @Deprecated("Use property instead")
    fun subject(subject: String?): AccessTokenBuilder {
        this.subject = subject
        return this
    }

    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("subjectClaimName"),
    )
    fun subjectClaimName(): String {
        return subjectClaimName
    }

    @Deprecated("Use property instead")
    fun subjectClaimName(subjectClaimName: String): AccessTokenBuilder {
        this.subjectClaimName = subjectClaimName
        return this
    }

    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("scope"),
    )
    fun scope(): Any = scope

    fun scope(scope: Any): AccessTokenBuilder {
        this.scope = scope
        return when (scope) {
            is String -> this
            is Collection<*> -> this
            else -> throw IllegalArgumentException(
                "$scopeClaimName parameter must be a ${String::class.java.getName()} or " +
                        "a ${MutableCollection::class.java.getName()} containing ${String::class.java.getName()}"
            )
        }
    }

    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("scopeClaimName"),
    )
    fun scopeClaimName(): String = scopeClaimName

    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("issuedAtSeconds"),
    )
    fun issuedAtSeconds(): Long = issuedAtSeconds

    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("expirationSeconds"),
    )
    fun expirationSeconds(): Long? = expirationSeconds

    @Deprecated("Use property instead")
    fun expirationSeconds(expirationSeconds: Long?): AccessTokenBuilder {
        this.expirationSeconds = expirationSeconds
        return this
    }

    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("jwk"),
    )
    fun jwk(): PublicJsonWebKey? = jwk

    @Deprecated("Use property instead")
    fun jwk(jwk: PublicJsonWebKey?): AccessTokenBuilder {
        this.jwk = jwk
        return this
    }

    fun addCustomClaim(name: String?, value: String?): AccessTokenBuilder {
        val validatedName = validateClaimNameOverride("claim name", name!!)
        val validatedValue = validateClaimNameOverride(validatedName, value!!)
        customClaims[validatedName] = validatedValue
        return this
    }

    @Throws(JoseException::class, IOException::class)
    fun build(): String {
        val node = objectMapper.createObjectNode()
        if (audience != null) node.put(ReservedClaimNames.AUDIENCE, audience)
        if (subject != null) node.put(subjectClaimName, subject)
        when (scope) {
            is String -> node.put(scopeClaimName, scope as String)

            is Collection<*> -> {
                val child = node.putArray(scopeClaimName)
                (scope as Collection<*>).forEach { v -> child.add(v as? String) }
            }

            else -> throw IllegalArgumentException(
                "$scopeClaimName claim must be a ${String::class.java.getName()} or " +
                        "a ${MutableCollection::class.java.getName()} containing ${String::class.java.getName()}"
            )
        }
        node.put(ReservedClaimNames.ISSUED_AT, issuedAtSeconds)
        node.put(ReservedClaimNames.EXPIRATION_TIME, expirationSeconds)
        for ((key, value) in customClaims) node.put(key, value)
        val json = objectMapper.writeValueAsString(node)
        val jws = JsonWebSignature()
        jws.payload = json
        if (jwk != null) {
            jws.setKey(jwk!!.privateKey)
            jws.keyIdHeaderValue = jwk!!.keyId
        }
        if (alg != null) jws.algorithmHeaderValue = alg

        return jws.getCompactSerialization()
    }
}
