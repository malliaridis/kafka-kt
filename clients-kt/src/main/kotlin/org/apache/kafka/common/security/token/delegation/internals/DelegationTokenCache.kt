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

package org.apache.kafka.common.security.token.delegation.internals

import java.util.concurrent.ConcurrentHashMap
import org.apache.kafka.common.security.authenticator.CredentialCache
import org.apache.kafka.common.security.scram.ScramCredential
import org.apache.kafka.common.security.scram.internals.ScramCredentialUtils
import org.apache.kafka.common.security.scram.internals.ScramMechanism
import org.apache.kafka.common.security.token.delegation.DelegationToken
import org.apache.kafka.common.security.token.delegation.TokenInformation

open class DelegationTokenCache(scramMechanisms: Collection<String>) {

    private val credentialCache = CredentialCache()

    //Cache to hold all the tokens
    private val tokenCache: MutableMap<String, TokenInformation> = ConcurrentHashMap()

    //Cache to hold hmac->tokenId mapping. This is required for renew, expire requests
    private val hmacTokenIdCache: MutableMap<String, String> = ConcurrentHashMap()

    //Cache to hold tokenId->hmac mapping. This is required for removing entry from hmacTokenIdCache using tokenId.
    private val tokenIdHmacCache: MutableMap<String, String> = ConcurrentHashMap()

    init {
        //Create caches for scramMechanisms
        ScramCredentialUtils.createCache(credentialCache, scramMechanisms)
    }

    fun credential(mechanism: String, tokenId: String): ScramCredential? {
        val cache = credentialCache.cache(
            mechanism,
            ScramCredential::class.java
        )
        return cache?.get(tokenId)
    }

    fun owner(tokenId: String): String? {
        return tokenCache[tokenId]?.owner?.name
    }

    fun updateCache(token: DelegationToken, scramCredentialMap: Map<String, ScramCredential>) {
        //Update TokenCache
        val tokenId = token.tokenInfo.tokenId
        addToken(tokenId, token.tokenInfo)
        val hmac = token.hmacAsBase64String()

        //Update Scram Credentials
        updateCredentials(tokenId, scramCredentialMap)

        //Update hmac-id cache
        hmacTokenIdCache[hmac] = tokenId
        tokenIdHmacCache[tokenId] = hmac
    }

    fun removeCache(tokenId: String) {
        removeToken(tokenId)
        updateCredentials(tokenId, HashMap())
    }

    fun tokenIdForHmac(base64hmac: String): String? = hmacTokenIdCache[base64hmac]

    fun tokenForHmac(base64hmac: String): TokenInformation? {
        return hmacTokenIdCache[base64hmac]?.let { tokenCache[it] }
    }

    fun addToken(tokenId: String, tokenInfo: TokenInformation): TokenInformation? {
        return tokenCache.put(tokenId, tokenInfo)
    }

    fun removeToken(tokenId: String) {
        val tokenInfo = tokenCache.remove(tokenId)

        if (tokenInfo != null) {
            val hmac = tokenIdHmacCache.remove(tokenInfo.tokenId)
            if (hmac != null) hmacTokenIdCache.remove(hmac)
        }
    }

    fun tokens(): Collection<TokenInformation> = tokenCache.values

    open fun token(tokenId: String): TokenInformation? = tokenCache[tokenId]

    fun credentialCache(mechanism: String?): CredentialCache.Cache<ScramCredential>? {
        return credentialCache.cache(mechanism, ScramCredential::class.java)
    }

    private fun updateCredentials(
        tokenId: String,
        scramCredentialMap: Map<String, ScramCredential>
    ) {
        ScramMechanism.mechanismNames.forEach { mechanism ->
            val cache = credentialCache.cache(
                mechanism,
                ScramCredential::class.java
            ) ?: return@forEach

            val credential = scramCredentialMap[mechanism]
            credential?.let { cache.put(tokenId, it) } ?: cache.remove(tokenId)
        }
    }
}