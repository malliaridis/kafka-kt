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

package org.apache.kafka.common.security.oauthbearer.internals.unsecured

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.JsonNodeType
import java.io.IOException
import java.util.*
import org.apache.kafka.common.security.oauthbearer.OAuthBearerToken
import kotlin.math.roundToLong

/**
 * A simple unsecured JWS implementation. The '`nbf`' claim is ignored if
 * it is given because the related logic is not required for Kafka testing and
 * development purposes.
 *
 * @constructor Constructor with the given principal and scope claim names
 * @property compactSerialization the compact serialization to parse as an unsecured JWS
 * @param principalClaimName the required principal claim name
 * @param scopeClaimName the required scope claim name
 * @throws OAuthBearerIllegalTokenException if the compact serialization is not a valid unsecured
 * JWS (meaning it did not have 3 dot-separated Base64URL sections without an empty digital
 * signature; or the header or claims either are not valid Base 64 URL encoded values or are not
 * JSON after decoding; or the mandatory '`alg`' header value is not "`none`")
 * @see [RFC 7515](https://tools.ietf.org/html/rfc7515)
 */
class OAuthBearerUnsecuredJws(
    private val compactSerialization: String,
    principalClaimName: String,
    scopeClaimName: String
) : OAuthBearerToken {

    /**
     * 3 or 5 dot-separated sections of the JWT compact serialization.
     */
    val splits: List<String> = extractCompactSerializationSplits()

    val header: Map<String, Any>

    val principalClaimName: String

    val scopeClaimName: String

    val claims: Map<String, Any>

    private val scope: Set<String>

    private val lifetime: Long

    private val principalName: String

    private val startTimeMs: Long?

    init {
        if (compactSerialization.contains("..")) throw OAuthBearerIllegalTokenException(
            OAuthBearerValidationResult.newFailure("Malformed compact serialization contains '..'")
        )

        header = toMap(splits[0])
        val claimsSplit = splits[1]
        claims = toMap(claimsSplit)

        val alg = header["alg"]?.toString()
            ?: throw NullPointerException("JWS header must have an Algorithm value")

        if ("none" != alg) throw OAuthBearerIllegalTokenException(
            OAuthBearerValidationResult.newFailure("Unsecured JWS must have 'none' for an algorithm")
        )

        val digitalSignatureSplit = splits[2]
        if (digitalSignatureSplit.isNotEmpty()) throw OAuthBearerIllegalTokenException(
            OAuthBearerValidationResult.newFailure("Unsecured JWS must not contain a digital signature")
        )

        this.principalClaimName = principalClaimName.trim { it <= ' ' }
        require(this.principalClaimName.isNotEmpty()) {
            "Must specify a non-blank principal claim name"
        }

        this.scopeClaimName = scopeClaimName.trim { it <= ' ' }
        require(this.scopeClaimName.isNotEmpty()) { "Must specify a non-blank scope claim name" }

        scope = calculateScope()
        val expirationTimeSeconds = expirationTime
            ?: throw OAuthBearerIllegalTokenException(
                OAuthBearerValidationResult.newFailure("No expiration time in JWT")
            )
        lifetime = convertClaimTimeInSecondsToMs(expirationTimeSeconds)
        val principalName = claim(this.principalClaimName, String::class.java)
        if (principalName.isNullOrBlank()) throw OAuthBearerIllegalTokenException(
            OAuthBearerValidationResult
                .newFailure("No principal name in JWT claim: " + this.principalClaimName)
        )
        this.principalName = principalName // isBlank() checks for null value
        startTimeMs = calculateStartTimeMs()
    }

    override fun value(): String = compactSerialization

    /**
     * Return the 3 or 5 dot-separated sections of the JWT compact serialization
     *
     * @return the 3 or 5 dot-separated sections of the JWT compact serialization
     */
    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("splits")
    )
    fun splits(): List<String> = splits

    /**
     * Return the JOSE Header as a `Map`
     *
     * @return the JOSE header
     */
    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("header")
    )
    fun header(): Map<String, Any> = header

    override fun principalName(): String = principalName

    override fun startTimeMs(): Long? = startTimeMs

    override fun lifetimeMs(): Long = lifetime

    @Throws(OAuthBearerIllegalTokenException::class)
    override fun scope(): Set<String> = scope

    /**
     * Return the JWT Claim Set as a `Map`
     *
     * @return the (always non-null but possibly empty) claims
     */
    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("claims")
    )
    fun claims(): Map<String, Any> = claims

    /**
     * Return the (always non-null/non-empty) principal claim name
     *
     * @return the (always non-null/non-empty) principal claim name
     */
    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("principalClaimName")
    )
    fun principalClaimName(): String = principalClaimName

    /**
     * Return the (always non-null/non-empty) scope claim name
     *
     * @return the (always non-null/non-empty) scope claim name
     */
    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("scopeClaimName")
    )
    fun scopeClaimName(): String = scopeClaimName

    /**
     * Indicate if the claim exists and is the given type.
     *
     * @param claimName the mandatory JWT claim name
     * @param type the mandatory type, which should either be String.class, Number.class, or
     * List.class
     * @return `true` if the claim exists and is the given type, otherwise `false`
     */
    fun isClaimType(claimName: String, type: Class<*>): Boolean {
        val value = rawClaim(claimName) ?: return false

        if (type == String::class.java && value is String) return true
        return if (type == Number::class.java && value is Number) true
        else type == MutableList::class.java && value is List<*>
    }

    /**
     * Extract a claim of the given type
     *
     * @param claimName the mandatory JWT claim name
     * @param type the mandatory type, which must either be String.class, Number.class, or
     * List.class
     * @return the claim if it exists, otherwise null?
     * @throws OAuthBearerIllegalTokenException if the claim exists but is not the given type
     */
    @Throws(OAuthBearerIllegalTokenException::class)
    fun <T> claim(claimName: String, type: Class<T>): T? {
        val value = rawClaim(claimName)
        return try {
            type.cast(value)
        } catch (e: ClassCastException) {
            throw OAuthBearerIllegalTokenException(
                OAuthBearerValidationResult.newFailure(
                    String.format(
                        "The '%s' claim was not of type %s: %s",
                        claimName,
                        type.simpleName,
                        value!!.javaClass.simpleName
                    )
                )
            )
        }
    }

    /**
     * Extract a claim in its raw form
     *
     * @param claimName the mandatory JWT claim name
     * @return the raw claim value, if it exists, otherwise `null`
     */
    fun rawClaim(claimName: String): Any? = claims[claimName]

    /**
     * Return the [Expiration Time](https://tools.ietf.org/html/rfc7519#section-4.1.4) claim.
     *
     * @return the [Expiration Time](https://tools.ietf.org/html/rfc7519#section-4.1.4) claim if
     * available, otherwise `null`
     * @throws OAuthBearerIllegalTokenException if the claim value is the incorrect type
     */
    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("expirationTime")
    )
    @Throws(OAuthBearerIllegalTokenException::class)
    fun expirationTime(): Number? {
        return claim("exp", Number::class.java)
    }

    /**
     * The [Expiration Time](https://tools.ietf.org/html/rfc7519#section-4.1.4) claim if available,
     * otherwise `null`
     *
     * @throws OAuthBearerIllegalTokenException if the claim value is the incorrect type.
     */
    val expirationTime: Number?
        @Throws(OAuthBearerIllegalTokenException::class)
        get() = claim("exp", Number::class.java)

    /**
     * Return the [Issued At](https://tools.ietf.org/html/rfc7519#section-4.1.6) claim.
     *
     * @return the [Issued At](https://tools.ietf.org/html/rfc7519#section-4.1.6) claim if
     * available, otherwise `null`
     * @throws OAuthBearerIllegalTokenException if the claim value is the incorrect type
     */
    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("issuedAt")
    )
    @Throws(OAuthBearerIllegalTokenException::class)
    fun issuedAt(): Number? = claim("iat", Number::class.java)

    /**
     * The [Issued At](https://tools.ietf.org/html/rfc7519#section-4.1.6) claim if available,
     * otherwise `null`.
     *
     * @throws OAuthBearerIllegalTokenException if the claim value is the incorrect type
     */
    val issuedAt: Number?
        @Throws(OAuthBearerIllegalTokenException::class)
        get() = claim("iat", Number::class.java)

    /**
     * Return the [Subject](https://tools.ietf.org/html/rfc7519#section-4.1.2) claim.
     *
     * @return the [Subject](https://tools.ietf.org/html/rfc7519#section-4.1.2) claim if available,
     * otherwise `null`
     * @throws OAuthBearerIllegalTokenException if the claim value is the incorrect type
     */
    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("subject")
    )
    @Throws(OAuthBearerIllegalTokenException::class)
    fun subject(): String? = claim("sub", String::class.java)

    /**
     * The [Subject](https://tools.ietf.org/html/rfc7519#section-4.1.2) claim if available,
     * otherwise `null`.
     *
     * @throws OAuthBearerIllegalTokenException if the claim value is the incorrect type
     */
    val subject: String?
        @Throws(OAuthBearerIllegalTokenException::class)
        get() = claim("sub", String::class.java)

    private fun extractCompactSerializationSplits(): List<String> {
        val tmpSplits = compactSerialization.split("\\.".toRegex())
            .dropLastWhile { it.isEmpty() }
            .toMutableList()

        if (compactSerialization.endsWith(".")) tmpSplits.add("")
        if (tmpSplits.size != 3) throw OAuthBearerIllegalTokenException(
            OAuthBearerValidationResult.newFailure(
                "Unsecured JWS compact serializations must have 3 dot-separated " +
                        "Base64URL-encoded values"
            )
        )
        return tmpSplits.toList()
    }

    @Throws(OAuthBearerIllegalTokenException::class)
    private fun calculateStartTimeMs(): Long? {
        return claim("iat", Number::class.java)?.let { convertClaimTimeInSecondsToMs(it) }
    }

    private fun calculateScope(): Set<String> {
        if (isClaimType(scopeClaimName, String::class.java)) {
            val scopeClaimValue = claim(scopeClaimName, String::class.java)

            return if (scopeClaimValue.isNullOrBlank()) emptySet() else {
                val retval: MutableSet<String> = HashSet()
                retval.add(scopeClaimValue.trim { it <= ' ' }) // isBlank() checks for null
                retval.toSet()
            }
        }
        val scopeClaimValue = claim(scopeClaimName, List::class.java)

        if (scopeClaimValue.isNullOrEmpty()) return emptySet()

        return (scopeClaimValue as List<String>).mapNotNull { scope ->
            if (!scope.isNullOrBlank()) scope.trim { it <= ' ' }
            else null
        }.toSet()
    }

    companion object {

        /**
         * Decode the given Base64URL-encoded value, parse the resulting JSON as a JSON object, and
         * return the map of member names to their values (each value being represented as either a
         * String, a Number, or a List of Strings).
         *
         * @param split the value to decode and parse
         * @return the map of JSON member names to their String, Number, or String List value
         * @throws OAuthBearerIllegalTokenException if the given Base64URL-encoded value cannot be
         * decoded or parsed
         */
        @Throws(OAuthBearerIllegalTokenException::class)
        fun toMap(split: String?): Map<String, Any> {
            val retval: MutableMap<String, Any> = HashMap()

            return try {
                val decode = Base64.getDecoder().decode(split)
                val jsonNode = ObjectMapper().readTree(decode)
                    ?: throw OAuthBearerIllegalTokenException(
                        OAuthBearerValidationResult.newFailure("malformed JSON")
                    )

                jsonNode.fields().forEach { (key, node) ->
                    retval[key] = convert(node)
                }
                retval.toMap()
            } catch (e: IllegalArgumentException) {
                // potentially thrown by java.util.Base64.Decoder implementations
                throw OAuthBearerIllegalTokenException(
                    OAuthBearerValidationResult.newFailure("malformed Base64 URL encoded value")
                )
            } catch (e: IOException) {
                throw OAuthBearerIllegalTokenException(
                    OAuthBearerValidationResult.newFailure("malformed JSON")
                )
            }
        }

        private fun convert(value: JsonNode): Any {
            if (value.isArray) {
                val retvalList: MutableList<String> = ArrayList()
                for (arrayElement in value) retvalList.add(arrayElement.asText())
                return retvalList
            }
            return if (value.nodeType == JsonNodeType.NUMBER) value.numberValue()
            else value.asText()
        }

        private fun convertClaimTimeInSecondsToMs(claimValue: Number): Long =
            (claimValue.toDouble() * 1000).roundToLong()
    }
}
