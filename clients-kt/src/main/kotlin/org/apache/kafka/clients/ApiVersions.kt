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

package org.apache.kafka.clients

import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.record.RecordBatch
import org.apache.kafka.common.requests.ProduceRequest

/**
 * Maintains node api versions for access outside NetworkClient (which is where the information is
 * derived). The pattern is akin to the use of [Metadata] for topic metadata.
 *
 * NOTE: This class is intended for INTERNAL usage only within Kafka.
 */
class ApiVersions {

    private val nodeApiVersions: MutableMap<String, NodeApiVersions> = HashMap()

    private var maxUsableProduceMagic = RecordBatch.CURRENT_MAGIC_VALUE

    @Synchronized
    fun update(nodeId: String, nodeApiVersions: NodeApiVersions) {
        this.nodeApiVersions[nodeId] = nodeApiVersions
        maxUsableProduceMagic = computeMaxUsableProduceMagic()
    }

    @Synchronized
    fun remove(nodeId: String) {
        nodeApiVersions.remove(nodeId)
        maxUsableProduceMagic = computeMaxUsableProduceMagic()
    }

    @Synchronized
    operator fun get(nodeId: String): NodeApiVersions? = nodeApiVersions[nodeId]

    private fun computeMaxUsableProduceMagic(): Byte {
        // use a magic version which is supported by all brokers to reduce the chance that
        // we will need to convert the messages when they are ready to be sent.
        val knownBrokerNodesMinRequiredMagicForProduce: Byte? =
            nodeApiVersions.values.filter { versions ->
                // filter out Raft controller nodes
                versions.apiVersion(ApiKeys.PRODUCE) != null
            }.minOfOrNull { versions ->
                ProduceRequest.requiredMagicForVersion(versions.latestUsableVersion(ApiKeys.PRODUCE))
            }
        return (knownBrokerNodesMinRequiredMagicForProduce ?: RecordBatch.CURRENT_MAGIC_VALUE)
            .coerceAtMost(RecordBatch.CURRENT_MAGIC_VALUE)
    }

    @Synchronized
    fun maxUsableProduceMagic(): Byte = maxUsableProduceMagic
}
