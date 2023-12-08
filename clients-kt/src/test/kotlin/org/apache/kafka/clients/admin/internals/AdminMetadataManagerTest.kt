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

package org.apache.kafka.clients.admin.internals

import org.apache.kafka.common.Cluster
import org.apache.kafka.common.Node
import org.apache.kafka.common.errors.AuthenticationException
import org.apache.kafka.common.utils.LogContext
import org.apache.kafka.common.utils.MockTime
import org.junit.jupiter.api.Test
import java.net.InetSocketAddress
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class AdminMetadataManagerTest {
    
    private val time = MockTime()
    
    private val logContext = LogContext()
    
    private val refreshBackoffMs: Long = 100
    
    private val metadataExpireMs: Long = 60000
    
    private val mgr = AdminMetadataManager(logContext, refreshBackoffMs, metadataExpireMs)

    @Test
    fun testMetadataReady() {
        // Metadata is not ready on initialization
        assertFalse(mgr.isReady)
        assertEquals(0, mgr.metadataFetchDelayMs(time.milliseconds()))

        // Metadata is not ready when bootstrap servers are set
        mgr.update(
            Cluster.bootstrap(listOf(InetSocketAddress("localhost", 9999))),
            time.milliseconds()
        )
        assertFalse(mgr.isReady)
        assertEquals(0, mgr.metadataFetchDelayMs(time.milliseconds()))
        mgr.update(mockCluster(), time.milliseconds())
        assertTrue(mgr.isReady)
        assertEquals(metadataExpireMs, mgr.metadataFetchDelayMs(time.milliseconds()))
        time.sleep(metadataExpireMs)
        assertEquals(0, mgr.metadataFetchDelayMs(time.milliseconds()))
    }

    @Test
    fun testMetadataRefreshBackoff() {
        mgr.transitionToUpdatePending(time.milliseconds())
        assertEquals(Long.MAX_VALUE, mgr.metadataFetchDelayMs(time.milliseconds()))
        mgr.updateFailed(RuntimeException())
        assertEquals(refreshBackoffMs, mgr.metadataFetchDelayMs(time.milliseconds()))

        // Even if we explicitly request an update, the backoff should be respected
        mgr.requestUpdate()
        assertEquals(refreshBackoffMs, mgr.metadataFetchDelayMs(time.milliseconds()))
        time.sleep(refreshBackoffMs)
        assertEquals(0, mgr.metadataFetchDelayMs(time.milliseconds()))
    }

    @Test
    fun testAuthenticationFailure() {
        mgr.transitionToUpdatePending(time.milliseconds())
        mgr.updateFailed(AuthenticationException("Authentication failed"))
        assertEquals(refreshBackoffMs, mgr.metadataFetchDelayMs(time.milliseconds()))
        assertFailsWith<AuthenticationException> { mgr.isReady }
        mgr.update(mockCluster(), time.milliseconds())
        assertTrue(mgr.isReady)
    }

    companion object {
        private fun mockCluster(): Cluster {
            val nodes = HashMap<Int, Node>()
            nodes[0] = Node(0, "localhost", 8121)
            nodes[1] = Node(1, "localhost", 8122)
            nodes[2] = Node(2, "localhost", 8123)
            return Cluster(
                clusterId = "mockClusterId",
                nodes = nodes.values,
                partitions = emptySet(),
                unauthorizedTopics = emptySet(),
                invalidTopics = emptySet(),
                controller = nodes[0],
            )
        }
    }
}
