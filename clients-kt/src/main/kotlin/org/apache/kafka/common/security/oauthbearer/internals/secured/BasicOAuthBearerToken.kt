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

import java.util.*
import org.apache.kafka.common.security.oauthbearer.OAuthBearerToken

/**
 * An implementation of the [OAuthBearerToken] that fairly straightforwardly stores the values
 * given to its constructor (except the scope set which is copied to avoid modifications).
 *
 * Very little validation is applied here with respect to the validity of the given values. All
 * validation is assumed to happen by users of this class.
 *
 * @constructor Creates a new OAuthBearerToken instance around the given values.
 * @property token Value containing the compact serialization as a base 64 string that can be
 * parsed, decoded, and validated as a well-formed JWS. Must be non-blank, and non-whitespace only.
 * @property scopes Set of scopes. May contain case-sensitive "duplicates". The given set is copied
 * and made unmodifiable so neither the caller of this constructor nor any downstream users can
 * modify it.
 * @property lifetimeMs The token's lifetime, expressed as the number of milliseconds since the
 * epoch. Must be non-negative.
 * @property principalName The name of the principal to which this credential applies. Must be
 * non-blank, and non-whitespace only.
 * @property startTimeMs The token's start time, expressed as the number of milliseconds since the
 * epoch, if available, otherwise `null`. Must be non-negative if a non-`null` value is provided.
 * @see [RFC 7515: JSON Web Signature](https://tools.ietf.org/html/rfc7515)
 */
class BasicOAuthBearerToken(
    private val token: String,
    private val scopes: Set<String>,
    private val lifetimeMs: Long,
    private val principalName: String,
    private val startTimeMs: Long? = null,
) : OAuthBearerToken {

    /**
     * The `b64token` value as defined in
     * [RFC 6750 Section 2.1](https://tools.ietf.org/html/rfc6750#section-2.1)
     *
     * @return `b64token` value as defined in
     * [RFC 6750 Section 2.1](https://tools.ietf.org/html/rfc6750#section-2.1)
     */
    override fun value(): String = token

    /**
     * The token's scope of access, as per
     * [RFC 6749 Section 1.4](https://tools.ietf.org/html/rfc6749#section-1.4)
     *
     * @return the token's (always non-null but potentially empty) scope of access, as per
     * [RFC 6749 Section 1.4](https://tools.ietf.org/html/rfc6749#section-1.4). Note that all values
     * in the returned set will be trimmed of preceding and trailing whitespace, and the result will
     * never contain the empty string.
     */
    override fun scope(): Set<String> = scopes

    /**
     * The token's lifetime, expressed as the number of milliseconds since the epoch, as per
     * [RFC 6749 Section 1.4](https://tools.ietf.org/html/rfc6749#section-1.4)
     *
     * @return the token's lifetime, expressed as the number of milliseconds since the epoch, as per
     * [RFC 6749 Section 1.4](https://tools.ietf.org/html/rfc6749#section-1.4).
     */
    override fun lifetimeMs(): Long = lifetimeMs

    /**
     * The name of the principal to which this credential applies
     *
     * @return the always non-null/non-empty principal name
     */
    override fun principalName(): String = principalName

    /**
     * When the credential became valid, in terms of the number of milliseconds since the epoch, if
     * known, otherwise null. An expiring credential may not necessarily indicate when it was
     * created -- just when it expires -- so we need to support a null return value here.
     *
     * @return the time when the credential became valid, in terms of the number of milliseconds
     * since the epoch, if known, otherwise `null`
     */
    override fun startTimeMs(): Long? = startTimeMs

    override fun toString(): String {
        return StringJoiner(
            ", ",
            BasicOAuthBearerToken::class.java.simpleName + "[", "]"
        )
            .add("token='$token'")
            .add("scopes=$scopes")
            .add("lifetimeMs=$lifetimeMs")
            .add("principalName='$principalName'")
            .add("startTimeMs=$startTimeMs")
            .toString()
    }
}
