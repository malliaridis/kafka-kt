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

import javax.net.ssl.SSLSocketFactory
import org.apache.kafka.common.config.SaslConfigs
import org.apache.kafka.common.utils.Time
import org.jose4j.http.Get
import org.jose4j.jwk.HttpsJwks

object VerificationKeyResolverFactory {

    /**
     * Create an [AccessTokenRetriever] from the given [org.apache.kafka.common.config.SaslConfigs].
     *
     * **Note**: the returned `CloseableVerificationKeyResolver` is not initialized here and must be
     * done by the caller.
     *
     * Primarily exposed here for unit testing.
     *
     * @param configs SASL configuration
     * @param saslMechanism optional SASL mechanism
     * @param jaasConfig JAAS configuration
     * @return Non-`null` [CloseableVerificationKeyResolver]
     */
    fun create(
        configs: Map<String, *>,
        saslMechanism: String? = null,
        jaasConfig: Map<String, Any>
    ): CloseableVerificationKeyResolver {
        val cu = ConfigurationUtils(configs, saslMechanism)
        val jwksEndpointUrl = cu.validateUrl(SaslConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_URL)

        return if (jwksEndpointUrl.protocol.lowercase() == "file") {
            val p = cu.validateFile(SaslConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_URL)
            JwksFileVerificationKeyResolver(p)
        } else {
            val refreshIntervalMs =
                cu.validateRequiredLong(SaslConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_REFRESH_MS, 0L)
            val jou = JaasOptionsUtils(jaasConfig)
            var sslSocketFactory: SSLSocketFactory? = null
            if (jou.shouldCreateSSLSocketFactory(jwksEndpointUrl)) sslSocketFactory =
                jou.createSSLSocketFactory()
            val httpsJwks = HttpsJwks(jwksEndpointUrl.toString())
            httpsJwks.setDefaultCacheDuration(refreshIntervalMs)
            if (sslSocketFactory != null) {
                val get = Get()
                get.setSslSocketFactory(sslSocketFactory)
                httpsJwks.setSimpleHttpGet(get)
            }
            val refreshingHttpsJwks = RefreshingHttpsJwks(
                Time.SYSTEM,
                httpsJwks,
                refreshIntervalMs,
                cu.validateRequiredLong(SaslConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MS),
                cu.validateRequiredLong(SaslConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MAX_MS)
            )
            RefreshingHttpsJwksVerificationKeyResolver(refreshingHttpsJwks)
        }
    }
}