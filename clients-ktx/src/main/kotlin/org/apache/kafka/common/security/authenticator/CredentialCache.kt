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

package org.apache.kafka.common.security.authenticator

import java.util.concurrent.ConcurrentHashMap

class CredentialCache {

    private val cacheMap = ConcurrentHashMap<String, Cache<out Any>>()

    fun <C : Any> createCache(mechanism: String, credentialClass: Class<C>): Cache<C> {
        val cache = Cache(credentialClass)
        val oldCache = cacheMap.putIfAbsent(mechanism, cache) as Cache<C>?
        return oldCache ?: cache
    }

    fun <C : Any> cache(mechanism: String?, credentialClass: Class<C>): Cache<C>? {
        val cache = cacheMap[mechanism]

        return if (cache != null) {
            require(cache.credentialClass == credentialClass) {
                "Invalid credential class $credentialClass, expected ${cache.credentialClass}"
            }
            cache as Cache<C>?
        } else null
    }

    class Cache<C : Any>(val credentialClass: Class<C>) {

        private val credentials: ConcurrentHashMap<String, C> = ConcurrentHashMap()

        operator fun get(username: String): C? = credentials[username]

        fun put(username: String, credential: C): C? = credentials.put(username, credential)

        fun remove(username: String): C? = credentials.remove(username)

        @Deprecated(
            message = "Use property instead",
            replaceWith = ReplaceWith("credentialClass")
        )
        fun credentialClass(): Class<C> = credentialClass
    }
}
