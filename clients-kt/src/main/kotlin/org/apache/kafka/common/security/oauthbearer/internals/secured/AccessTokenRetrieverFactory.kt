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
import org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginCallbackHandler

object AccessTokenRetrieverFactory {

    /**
     * Create an [AccessTokenRetriever] from the given SASL and JAAS configuration.
     *
     * **Note**: the returned `AccessTokenRetriever` is *not* initialized here and must be done by
     * the caller prior to use.
     *
     * @param configs SASL configuration
     * @param saslMechanism SASL mechanism, defaults to `null`
     * @param jaasConfig JAAS configuration
     *
     * @return [AccessTokenRetriever]
     */
    fun create(
        configs: Map<String, *>,
        saslMechanism: String? = null,
        jaasConfig: Map<String, Any>
    ): AccessTokenRetriever {
        val cu = ConfigurationUtils(configs, saslMechanism)
        val tokenEndpointUrl = cu.validateUrl(SaslConfigs.SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL)

        return if (tokenEndpointUrl.protocol.lowercase() == "file") {
            FileTokenRetriever(cu.validateFile(SaslConfigs.SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL))
        } else {
            val jou = JaasOptionsUtils(jaasConfig)

            val clientId =
                jou.validateRequiredString(OAuthBearerLoginCallbackHandler.CLIENT_ID_CONFIG)
            val clientSecret =
                jou.validateRequiredString(OAuthBearerLoginCallbackHandler.CLIENT_SECRET_CONFIG)
            val scope = jou.validateOptionalString(OAuthBearerLoginCallbackHandler.SCOPE_CONFIG)

            var sslSocketFactory: SSLSocketFactory? = null
            if (jou.shouldCreateSSLSocketFactory(tokenEndpointUrl))
                sslSocketFactory = jou.createSSLSocketFactory()

            HttpAccessTokenRetriever(
                clientId = clientId,
                clientSecret = clientSecret,
                scope = scope,
                sslSocketFactory = sslSocketFactory,
                tokenEndpointUrl = tokenEndpointUrl.toString(),
                loginRetryBackoffMs = cu.validateRequiredLong(SaslConfigs.SASL_LOGIN_RETRY_BACKOFF_MS),
                loginRetryBackoffMaxMs = cu.validateRequiredLong(SaslConfigs.SASL_LOGIN_RETRY_BACKOFF_MAX_MS),
                loginConnectTimeoutMs = cu.validateOptionalInt(SaslConfigs.SASL_LOGIN_CONNECT_TIMEOUT_MS),
                loginReadTimeoutMs = cu.validateOptionalInt(SaslConfigs.SASL_LOGIN_READ_TIMEOUT_MS)
            )
        }
    }
}