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

package org.apache.kafka.common.requests

import org.apache.kafka.common.Node
import org.apache.kafka.common.TopicIdPartition
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.Uuid
import org.apache.kafka.common.internals.Topic.isInternal
import org.apache.kafka.common.message.MetadataResponseData.MetadataResponsePartition
import org.apache.kafka.common.message.MetadataResponseData.MetadataResponseTopic
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.protocol.ObjectSerializationCache
import org.apache.kafka.common.record.RecordBatch
import org.apache.kafka.common.requests.MetadataResponse.PartitionMetadata
import org.apache.kafka.common.requests.RequestTestUtils.PartitionMetadataSupplier
import org.apache.kafka.common.requests.RequestUtils.flag
import java.nio.ByteBuffer
import java.util.*
import java.util.function.Consumer
import java.util.function.Function
import java.util.stream.Collectors

object RequestTestUtils {

    fun hasIdempotentRecords(request: ProduceRequest?): Boolean {
        return flag(request!!) { obj: RecordBatch -> obj.hasProducerId() }
    }

    fun serializeRequestHeader(header: RequestHeader): ByteBuffer {
        val serializationCache = ObjectSerializationCache()
        val buffer = ByteBuffer.allocate(header.size(serializationCache))
        header.write(buffer, serializationCache)
        buffer.flip()
        return buffer
    }

    fun serializeResponseWithHeader(
        response: AbstractResponse,
        version: Short,
        correlationId: Int
    ): ByteBuffer {
        return response.serializeWithHeader(
            header = ResponseHeader(
                correlationId = correlationId,
                headerVersion = response.apiKey.responseHeaderVersion(version),
            ),
            version = version,
        )
    }

    @JvmOverloads
    @Deprecated("This function overload will be removed")
    fun metadataResponse(
        brokers: Collection<Node>?,
        clusterId: String?,
        controllerId: Int,
        topicMetadataList: List<MetadataResponse.TopicMetadata>,
        responseVersion: Short = ApiKeys.METADATA.latestVersion()
    ): MetadataResponse = metadataResponse(
        throttleTimeMs = AbstractResponse.DEFAULT_THROTTLE_TIME,
        brokers = brokers,
        clusterId = clusterId,
        controllerId = controllerId,
        topicMetadatas = topicMetadataList,
        clusterAuthorizedOperations = MetadataResponse.AUTHORIZED_OPERATIONS_OMITTED,
        responseVersion = responseVersion
    )

    fun metadataResponse(
        throttleTimeMs: Int = AbstractResponse.DEFAULT_THROTTLE_TIME,
        brokers: Collection<Node>?,
        clusterId: String?, controllerId: Int,
        topicMetadatas: List<MetadataResponse.TopicMetadata>,
        clusterAuthorizedOperations: Int = MetadataResponse.AUTHORIZED_OPERATIONS_OMITTED,
        responseVersion: Short = ApiKeys.METADATA.latestVersion(),
    ): MetadataResponse {
        val topics: MutableList<MetadataResponseTopic> = ArrayList()
        topicMetadatas.forEach { (error, topic, topicId, isInternal, partitionMetadata1, authorizedOperations): MetadataResponse.TopicMetadata ->
            val metadataResponseTopic = MetadataResponseTopic()
            metadataResponseTopic
                .setErrorCode(error.code)
                .setName(topic)
                .setTopicId(topicId)
                .setIsInternal(isInternal)
                .setTopicAuthorizedOperations(authorizedOperations)
            for (partitionMetadata in partitionMetadata1) {
                metadataResponseTopic.partitions += MetadataResponsePartition()
                    .setErrorCode(partitionMetadata.error.code)
                    .setPartitionIndex(partitionMetadata.partition)
                    .setLeaderId(partitionMetadata.leaderId ?: MetadataResponse.NO_LEADER_ID)
                    .setLeaderEpoch(
                        partitionMetadata.leaderEpoch ?: RecordBatch.NO_PARTITION_LEADER_EPOCH
                    )
                    .setReplicaNodes(partitionMetadata.replicaIds.toIntArray())
                    .setIsrNodes(partitionMetadata.inSyncReplicaIds.toIntArray())
                    .setOfflineReplicas(partitionMetadata.offlineReplicaIds.toIntArray())
            }
            topics.add(metadataResponseTopic)
        }
        return MetadataResponse.prepareResponse(
            responseVersion, throttleTimeMs,
            brokers!!, clusterId, controllerId,
            topics, clusterAuthorizedOperations
        )
    }

    @Deprecated("This overload will be removed")
    fun metadataUpdateWith(
        numNodes: Int,
        topicPartitionCounts: Map<String, Int>
    ): MetadataResponse {
        return metadataUpdateWith(
            clusterId = "kafka-cluster",
            numNodes = numNodes,
            topicPartitionCounts = topicPartitionCounts,
        )
    }
/*
TODO Remove deprecated overloads

    @Deprecated("This overload will be removed")
    fun metadataUpdateWith(
        numNodes: Int,
        topicPartitionCounts: Map<String, Int>,
        epochSupplier: Function<TopicPartition?, Int?>
    ): MetadataResponse {
        return metadataUpdateWith(
            clusterId = "kafka-cluster",
            numNodes = numNodes,
            topicErrors = emptyMap(),
            topicPartitionCounts = topicPartitionCounts,
            epochSupplier = epochSupplier,
            partitionSupplier = SimplePartitionMetadataSupplier,
            responseVersion = ApiKeys.METADATA.latestVersion(),
            topicIds = emptyMap()
        )
    }

    @Deprecated("This overload will be removed")
    fun metadataUpdateWith(
        clusterId: String = "kafka-cluster",
        numNodes: Int,
        topicPartitionCounts: Map<String, Int>
    ): MetadataResponse {
        return metadataUpdateWith(
            clusterId = clusterId,
            numNodes = numNodes,
            topicErrors = emptyMap(),
            topicPartitionCounts = topicPartitionCounts,
            epochSupplier = { null },
            partitionSupplier = SimplePartitionMetadataSupplier,
            responseVersion = ApiKeys.METADATA.latestVersion(),
            topicIds = emptyMap(),
        )
    }

    @Deprecated("This overload will be removed")
    fun metadataUpdateWith(
        clusterId: String,
        numNodes: Int,
        topicErrors: Map<String, Errors>,
        topicPartitionCounts: Map<String, Int>,
        responseVersion: Short,
    ): MetadataResponse {
        return metadataUpdateWith(
            clusterId = clusterId,
            numNodes = numNodes,
            topicErrors = topicErrors,
            topicPartitionCounts = topicPartitionCounts,
            epochSupplier = { null },
            partitionSupplier = SimplePartitionMetadataSupplier,
            responseVersion = responseVersion,
            topicIds = emptyMap(),
        )
    }
*/
    @Deprecated("This overload will be removed")
    fun metadataUpdateWithIds(
        numNodes: Int,
        topicPartitionCounts: Map<String, Int>,
        topicIds: Map<String, Uuid>
    ): MetadataResponse {
        return metadataUpdateWith(
            clusterId = "kafka-cluster",
            numNodes = numNodes,
            topicErrors = emptyMap(),
            topicPartitionCounts = topicPartitionCounts,
            epochSupplier = { null },
            partitionSupplier = SimplePartitionMetadataSupplier,
            responseVersion = ApiKeys.METADATA.latestVersion(),
            topicIds = topicIds,
        )
    }

    @Deprecated("This overload will be removed")
    fun metadataUpdateWithIds(
        numNodes: Int,
        partitions: Set<TopicIdPartition>,
        epochSupplier: Function<TopicPartition?, Int?>
    ): MetadataResponse {
        val topicPartitionCounts: MutableMap<String, Int> = HashMap()
        val topicIds: MutableMap<String, Uuid> = HashMap()
        partitions.forEach { partition ->
            topicPartitionCounts.compute(partition.topic) { _, value -> if (value == null) 1 else value + 1 }
            topicIds.putIfAbsent(partition.topic, partition.topicId)
        }
        return metadataUpdateWithIds(
            numNodes = numNodes,
            topicPartitionCounts = topicPartitionCounts,
            epochSupplier = epochSupplier,
            topicIds = topicIds,
        )
    }

    @Deprecated("This overload will be removed")
    fun metadataUpdateWithIds(
        numNodes: Int,
        topicPartitionCounts: Map<String, Int>,
        epochSupplier: Function<TopicPartition?, Int?>,
        topicIds: Map<String, Uuid>
    ): MetadataResponse {
        return metadataUpdateWith(
            clusterId = "kafka-cluster",
            numNodes = numNodes,
            topicErrors = emptyMap(),
            topicPartitionCounts = topicPartitionCounts,
            epochSupplier = epochSupplier,
            partitionSupplier = SimplePartitionMetadataSupplier,
            responseVersion = ApiKeys.METADATA.latestVersion(),
            topicIds = topicIds,
        )
    }

    @Deprecated("This overload will be removed")
    fun metadataUpdateWithIds(
        clusterId: String?,
        numNodes: Int,
        topicErrors: Map<String, Errors>,
        topicPartitionCounts: Map<String, Int>,
        epochSupplier: Function<TopicPartition?, Int?>,
        topicIds: Map<String, Uuid>
    ): MetadataResponse {
        return metadataUpdateWith(
            clusterId = clusterId,
            numNodes = numNodes,
            topicErrors = topicErrors,
            topicPartitionCounts = topicPartitionCounts,
            epochSupplier = epochSupplier,
            partitionSupplier = SimplePartitionMetadataSupplier,
            responseVersion = ApiKeys.METADATA.latestVersion(),
            topicIds = topicIds,
        )
    }

    @JvmOverloads
    fun metadataUpdateWith(
        clusterId: String?,
        numNodes: Int,
        topicErrors: Map<String, Errors> = emptyMap(),
        topicPartitionCounts: Map<String, Int>,
        epochSupplier: Function<TopicPartition?, Int?> = Function { null },
        partitionSupplier: PartitionMetadataSupplier = PartitionMetadataSupplier { error, topicPartition, leaderId, leaderEpoch, replicaIds, inSyncReplicaIds, offlineReplicaIds ->
            PartitionMetadata(
                error = error,
                topicPartition = topicPartition,
                leaderId = leaderId,
                leaderEpoch = leaderEpoch,
                replicaIds = replicaIds,
                inSyncReplicaIds = inSyncReplicaIds,
                offlineReplicaIds = offlineReplicaIds
            )
        },
        responseVersion: Short = ApiKeys.METADATA.latestVersion(),
        topicIds: Map<String, Uuid> = emptyMap(),
        leaderOnly: Boolean = true
    ): MetadataResponse {
        val nodes: MutableList<Node> = ArrayList(numNodes)
        for (i in 0 until numNodes) nodes.add(Node(i, "localhost", 1969 + i))
        val topicMetadata: MutableList<MetadataResponse.TopicMetadata> = ArrayList()
        for ((topic, numPartitions) in topicPartitionCounts) {
            val partitionMetadata: MutableList<PartitionMetadata> = ArrayList(numPartitions)
            for (i in 0 until numPartitions) {
                val tp = TopicPartition(topic, i)
                val (id) = nodes[i % nodes.size]
                val replicaIds = if (leaderOnly) listOf(id) else nodes.map { (id1): Node -> id1 }
                partitionMetadata.add(
                    partitionSupplier.supply(
                        error = Errors.NONE,
                        partition = tp,
                        leaderId = id,
                        leaderEpoch = epochSupplier.apply(tp),
                        replicas = replicaIds,
                        isr = replicaIds,
                        offlineReplicas = emptyList(),
                    )
                )
            }
            topicMetadata.add(
                MetadataResponse.TopicMetadata(
                    Errors.NONE,
                    topic,
                    topicIds.getOrDefault(topic, Uuid.ZERO_UUID),
                    isInternal(topic),
                    partitionMetadata,
                    MetadataResponse.AUTHORIZED_OPERATIONS_OMITTED
                )
            )
        }
        for ((topic, value) in topicErrors) {
            topicMetadata.add(
                MetadataResponse.TopicMetadata(
                    error = value!!,
                    topic = topic!!,
                    isInternal = isInternal(topic),
                    partitionMetadata = emptyList()
                )
            )
        }
        return metadataResponse(
            brokers = nodes,
            clusterId = clusterId,
            controllerId = 0,
            topicMetadataList = topicMetadata,
            responseVersion = responseVersion,
        )
    }

    fun interface PartitionMetadataSupplier {
        fun supply(
            error: Errors,
            partition: TopicPartition,
            leaderId: Int?,
            leaderEpoch: Int?,
            replicas: List<Int>,
            isr: List<Int>,
            offlineReplicas: List<Int>,
        ): PartitionMetadata
    }

    // TODO Remove once no longer in use
    private val SimplePartitionMetadataSupplier = PartitionMetadataSupplier { error, topicPartition, leaderId, leaderEpoch, replicaIds, inSyncReplicaIds, offlineReplicaIds ->
        PartitionMetadata(
            error = error,
            topicPartition = topicPartition,
            leaderId = leaderId,
            leaderEpoch = leaderEpoch,
            replicaIds = replicaIds,
            inSyncReplicaIds = inSyncReplicaIds,
            offlineReplicaIds = offlineReplicaIds
        )
    }
}

