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

package org.apache.kafka.clients.admin

import org.apache.kafka.clients.MockClient
import org.apache.kafka.clients.MockClient.MetadataUpdate
import org.apache.kafka.clients.MockClient.MockMetadataUpdater
import org.apache.kafka.clients.admin.internals.AdminMetadataManager
import org.apache.kafka.common.Cluster
import org.apache.kafka.common.Node
import org.apache.kafka.common.utils.LogContext
import org.apache.kafka.common.utils.Time
import java.time.Duration

/**
 * Simple utility for setting up a mock [KafkaAdminClient] that uses a [MockClient] for a supplied
 * [Cluster]. Create a [Cluster] manually or use [org.apache.kafka.test.TestUtils] methods to
 * easily create a simple cluster.
 *
 *
 * To use in a test, create an instance and prepare its [MockClient][.kafkaClient] with the expected
 * responses for the [Admin]. Then, use the [AdminClient][.adminClient] in the test, which will then
 * use the MockClient and receive the responses you provided.
 *
 * Since [MockClient.kafkaClient] is not thread-safe, users should be wary of calling its methods
 * after the [AdminClient.adminClient] is instantiated.
 *
 * When finished, be sure to [close][.close] the environment object.
 */
class AdminClientUnitTestEnv(
    val time: Time,
    val cluster: Cluster,
    config: Map<String, Any?> = clientConfigs(),
    unreachableNodes: Map<Node, Long> = emptyMap(),
) : AutoCloseable {

    val mockClient: MockClient

    val adminClient: KafkaAdminClient

    constructor(cluster: Cluster, vararg vals: String) : this(
        time = Time.SYSTEM,
        cluster = cluster,
        config = clientConfigs(*vals),
    )
    constructor(time: Time = Time.SYSTEM, cluster: Cluster, vararg vals: String) : this(
        time = time,
        cluster = cluster,
        config = clientConfigs(*vals),
    )

    init {
        val adminClientConfig = AdminClientConfig(config)
        val metadataManager = AdminMetadataManager(
            logContext = LogContext(),
            refreshBackoffMs = adminClientConfig.getLong(AdminClientConfig.RETRY_BACKOFF_MS_CONFIG)!!,
            metadataExpireMs = adminClientConfig.getLong(AdminClientConfig.METADATA_MAX_AGE_CONFIG)!!,
        )
        mockClient = MockClient(time, object : MockMetadataUpdater {
            override fun fetchNodes(): List<Node> = cluster.nodes

            override val isUpdateNeeded: Boolean = false

            override fun update(time: Time, update: MetadataUpdate) =
                throw UnsupportedOperationException()
        })
        metadataManager.update(cluster, time.milliseconds())
        unreachableNodes.forEach { (node, durationMs) ->
            mockClient.setUnreachable(
                node = node,
                durationMs = durationMs,
            )
        }
        adminClient = KafkaAdminClient.createInternal(
            config = adminClientConfig,
            metadataManager = metadataManager,
            client = mockClient,
            time = time,
        )
    }

    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("time"),
    )
    fun time(): Time {
        return time
    }

    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("cluster"),
    )
    fun cluster(): Cluster = cluster

    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("adminClient"),
    )
    fun adminClient(): Admin = adminClient

    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("mockClient"),
    )
    fun kafkaClient(): MockClient = mockClient

    override fun close() {
        // tell the admin client to close now
        adminClient.close(Duration.ZERO)
        // block for up to a minute until the internal threads shut down.
        adminClient.close(Duration.ofMinutes(1))
    }

    companion object {
        fun clientConfigs(vararg overrides: String): Map<String, Any?> {
            val map: MutableMap<String, Any?> = HashMap()
            map[AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG] = "localhost:8121"
            map[AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG] = "1000"
            check(overrides.size % 2 == 0)
            var i = 0
            while (i < overrides.size) {
                map[overrides[i]] = overrides[i + 1]
                i += 2
            }
            return map
        }

        fun kafkaAdminClientNetworkThreadPrefix(): String {
            return KafkaAdminClient.NETWORK_THREAD_PREFIX
        }
    }
}
