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

package org.apache.kafka.common.security.token.delegation

import java.util.*
import org.apache.kafka.common.annotation.InterfaceStability.Evolving

/**
 * A class representing a delegation token.
 */
@Evolving
data class DelegationToken(
    val tokenInfo: TokenInformation,
    val hmac: ByteArray
) {

    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("tokenInfo"),
    )
    fun tokenInfo(): TokenInformation = tokenInfo

    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("hmac"),
    )
    fun hmac(): ByteArray = hmac

    fun hmacAsBase64String(): String {
        return Base64.getEncoder().encodeToString(hmac)
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as DelegationToken

        if (tokenInfo != other.tokenInfo) return false
        return hmac.contentEquals(other.hmac)
    }

    override fun hashCode(): Int {
        var result = tokenInfo.hashCode()
        result = 31 * result + hmac.contentHashCode()
        return result
    }

    override fun toString(): String {
        return "DelegationToken{" +
                "tokenInformation=$tokenInfo" +
                ", hmac=[*******]" +
                '}'
    }
}
