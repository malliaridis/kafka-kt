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

package org.apache.kafka.common.network

import java.io.File
import java.io.IOException
import java.net.InetSocketAddress
import java.security.GeneralSecurityException
import org.apache.kafka.common.config.SslConfigs
import org.apache.kafka.common.network.NetworkTestUtils.waitForChannelReady
import org.apache.kafka.test.TestSslUtils.createSslConfig
import org.junit.jupiter.api.Test
import kotlin.test.assertTrue

class Tls12SelectorTest : SslSelectorTest() {

    @Throws(GeneralSecurityException::class, IOException::class)
    override fun createSslClientConfigs(trustStoreFile: File?): Map<String, Any?> {
        val configs = createSslConfig(
            useClientCert = false,
            trustStore = false,
            mode = Mode.CLIENT,
            trustStoreFile = trustStoreFile!!,
            certAlias = "client",
        ).toMutableMap()
        configs[SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG] = listOf("TLSv1.2")
        return configs
    }

    /**
     * Renegotiation is not supported when TLS 1.2 is used (renegotiation was removed from TLS 1.3)
     */
    @Test
    @Throws(Exception::class)
    fun testRenegotiationFails() {
        val node = "0"
        // create connections
        val addr = InetSocketAddress("localhost", server.port)
        selector.connect(node, addr, BUFFER_SIZE, BUFFER_SIZE)
        waitForChannelReady(selector, node)

        // send echo requests and receive responses
        selector.send(createSend(node, "$node-0"))
        selector.poll(0L)
        server.renegotiate()
        selector.send(createSend(node, "$node-1"))
        val expiryTime = System.currentTimeMillis() + 2000
        val disconnected = mutableListOf<String>()
        while (!disconnected.contains(node) && System.currentTimeMillis() < expiryTime) {
            selector.poll(10)
            disconnected.addAll(selector.disconnected().keys)
        }
        assertTrue(disconnected.contains(node), "Renegotiation should cause disconnection")
    }
}
