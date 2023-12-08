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

import java.net.URL
import org.apache.kafka.common.config.SslConfigs
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

class JaasOptionsUtilsTest : OAuthBearerTest() {

    @Test
    fun testSSLClientConfig() {
        val sslKeystore = "test.keystore.jks"
        val sslTruststore = "test.truststore.jks"
        val options = mapOf(
            SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG to sslKeystore,
            SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG to "$3cr3+",
            SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG to sslTruststore,
        )
        val jou = JaasOptionsUtils(options)
        val sslClientConfig = jou.sslClientConfig

        assertNotNull(sslClientConfig)
        assertEquals(
            expected = sslKeystore,
            actual = sslClientConfig[SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG],
        )
        assertEquals(
            expected = sslTruststore,
            actual = sslClientConfig[SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG],
        )
        assertEquals(
            expected = SslConfigs.DEFAULT_SSL_PROTOCOL,
            actual = sslClientConfig[SslConfigs.SSL_PROTOCOL_CONFIG],
        )
    }

    @Test
    @Throws(Exception::class)
    fun testShouldUseSslClientConfig() {
        val jou = JaasOptionsUtils(emptyMap())
        assertFalse(jou.shouldCreateSSLSocketFactory(URL("http://example.com")))
        assertTrue(jou.shouldCreateSSLSocketFactory(URL("https://example.com")))
        assertFalse(jou.shouldCreateSSLSocketFactory(URL("file:///tmp/test.txt")))
    }
}
