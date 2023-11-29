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

import java.nio.ByteBuffer
import java.util.*
import org.apache.kafka.common.Cluster
import org.apache.kafka.common.Node
import org.apache.kafka.common.PartitionInfo
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.Uuid
import org.apache.kafka.common.message.MetadataResponseData
import org.apache.kafka.common.message.MetadataResponseData.MetadataResponseBroker
import org.apache.kafka.common.message.MetadataResponseData.MetadataResponseTopic
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.ByteBufferAccessor
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.utils.Utils

/**
 * Possible topic-level error codes:
 * - UnknownTopic (3)
 * - LeaderNotAvailable (5)
 * - InvalidTopic (17)
 * - TopicAuthorizationFailed (29)
 *
 * Possible partition-level error codes:
 * - LeaderNotAvailable (5)
 * - ReplicaNotAvailable (9)
 */
class MetadataResponse internal constructor(
    private val data: MetadataResponseData,
    private val hasReliableLeaderEpochs: Boolean,
) : AbstractResponse(ApiKeys.METADATA) {

    @Volatile
    private var _holder: Holder? = null

    constructor(
        data: MetadataResponseData,
        version: Short,
    ) : this(
        data = data,
        hasReliableLeaderEpochs = hasReliableLeaderEpochs(version),
    )

    override fun data(): MetadataResponseData = data

    override fun throttleTimeMs(): Int = data.throttleTimeMs

    override fun maybeSetThrottleTimeMs(throttleTimeMs: Int) {
        data.setThrottleTimeMs(throttleTimeMs)
    }

    /**
     * Get a map of the topics which had metadata errors
     * @return the map
     */
    fun errors(): Map<String, Errors> {
        val errors: MutableMap<String, Errors> = HashMap()
        for (metadata in data.topics) {
            checkNotNull(metadata.name) {
                "Use errorsByTopicId() when managing topic using topic id"
            }
            if (metadata.errorCode != Errors.NONE.code) errors[metadata.name!!] =
                Errors.forCode(metadata.errorCode)
        }
        return errors
    }

    /**
     * Get a map of the topicIds which had metadata errors
     * @return the map
     */
    fun errorsByTopicId(): Map<Uuid, Errors> {
        val errors: MutableMap<Uuid, Errors> = HashMap()
        for (metadata in data.topics) {
            check(metadata.topicId !== Uuid.ZERO_UUID) {
                "Use errors() when managing topic using topic name"
            }
            if (metadata.errorCode != Errors.NONE.code)
                errors[metadata.topicId] = Errors.forCode(metadata.errorCode)
        }
        return errors
    }

    override fun errorCounts(): Map<Errors, Int> {
        val errorCounts = mutableMapOf<Errors, Int>()
        data.topics.forEach { metadata ->
            metadata.partitions.forEach { partition ->
                updateErrorCounts(errorCounts, Errors.forCode(partition.errorCode))
            }
            updateErrorCounts(errorCounts, Errors.forCode(metadata.errorCode))
        }
        return errorCounts
    }

    /**
     * Returns the set of topics with the specified error
     */
    fun topicsByError(error: Errors): Set<String> {
        val errorTopics: MutableSet<String> = hashSetOf()
        for (metadata in data.topics)
            if (metadata.errorCode == error.code) errorTopics.add(metadata.name!!)

        return errorTopics
    }

    /**
     * Get a snapshot of the cluster metadata from this response
     * @return the cluster snapshot
     */
    fun buildCluster(): Cluster {
        val internalTopics: MutableSet<String> = hashSetOf()
        val partitions: MutableList<PartitionInfo> = ArrayList()
        val topicIds: MutableMap<String, Uuid> = mutableMapOf()
        for (metadata in topicMetadata())
            if (metadata.error == Errors.NONE) {
                if (metadata.isInternal) internalTopics.add(metadata.topic)
                if (Uuid.ZERO_UUID != metadata.topicId) topicIds[metadata.topic] = metadata.topicId
                metadata.partitionMetadata.forEach { partitionMetadata ->
                    partitions.add(toPartitionInfo(partitionMetadata, holder.brokers))
                }
            }

        return Cluster(
            clusterId = data.clusterId,
            nodes = brokers(),
            partitions = partitions,
            unauthorizedTopics = topicsByError(Errors.TOPIC_AUTHORIZATION_FAILED),
            invalidTopics = topicsByError(Errors.INVALID_TOPIC_EXCEPTION),
            internalTopics = internalTopics,
            controller = controller,
            topicIds = topicIds
        )
    }

    /**
     * Returns a 32-bit bitfield to represent authorized operations for this topic.
     */
    fun topicAuthorizedOperations(topicName: String): Int =
        data.topics.find(topicName)!!.topicAuthorizedOperations

    /**
     * Returns a 32-bit bitfield to represent authorized operations for this cluster.
     */
    fun clusterAuthorizedOperations(): Int = data.clusterAuthorizedOperations

    @Deprecated(
        message = "Use property access instead",
        replaceWith = ReplaceWith("holder")
    )
    private fun holder(): Holder = holder

    private val holder: Holder by lazy { Holder(data) }

    /**
     * Get all brokers returned in metadata response
     * @return the brokers
     */
    fun brokers(): Collection<Node> = holder.brokers.values

    fun brokersById(): Map<Int, Node> = holder.brokers

    /**
     * Get all topic metadata returned in the metadata response
     * @return the topicMetadata
     */
    fun topicMetadata(): Collection<TopicMetadata> = holder.topicMetadata

    /**
     * The controller node returned in metadata response
     * @return the controller node or null if it doesn't exist
     */
    @Deprecated(
        message = "User property instead.",
        replaceWith = ReplaceWith("controller")
    )
    fun controller(): Node? = holder.controller

    val controller: Node? = holder.controller

    /**
     * The cluster identifier returned in the metadata response.
     * @return cluster identifier if it is present in the response, null otherwise.
     */
    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("clusterId"),
    )
    fun clusterId(): String? = data.clusterId

    /**
     * The cluster identifier returned in the metadata response or `null` if it is not present in
     * the response.
     */
    val clusterId: String?
        get() = data.clusterId

    /**
     * Check whether the leader epochs returned from the response can be relied on
     * for epoch validation in Fetch, ListOffsets, and OffsetsForLeaderEpoch requests.
     * If not, then the client will not retain the leader epochs and hence will not
     * forward them in requests.
     *
     * @return true if the epoch can be used for validation
     */
    fun hasReliableLeaderEpochs(): Boolean = hasReliableLeaderEpochs

    data class TopicMetadata(
        val error: Errors,
        val topic: String,
        val topicId: Uuid = Uuid.ZERO_UUID,
        val isInternal: Boolean,
        val partitionMetadata: List<PartitionMetadata>,
        var authorizedOperations: Int = AUTHORIZED_OPERATIONS_OMITTED
    ) {

        @Deprecated(
            message = "Use property instead.",
            replaceWith = ReplaceWith("error")
        )
        fun error(): Errors = error

        @Deprecated(
            message = "Use property instead.",
            replaceWith = ReplaceWith("topic")
        )
        fun topic(): String = topic

        @Deprecated(
            message = "Use property instead.",
            replaceWith = ReplaceWith("topicId")
        )
        fun topicId(): Uuid = topicId

        @Deprecated(
            message = "Use property instead.",
            replaceWith = ReplaceWith("partitionMetadata")
        )
        fun partitionMetadata(): List<PartitionMetadata> = partitionMetadata

        @Deprecated(message = "Use property instead.")
        fun authorizedOperations(authorizedOperations: Int) {
            this.authorizedOperations = authorizedOperations
        }

        @Deprecated(
            message = "Use property instead.",
            replaceWith = ReplaceWith("authorizedOperations")
        )
        fun authorizedOperations(): Int = authorizedOperations

        override fun toString(): String {
            return "TopicMetadata{" +
                    "error=error" +
                    ", topic='topic'" +
                    ", topicId='$topicId'" +
                    ", isInternal=$isInternal" +
                    ", partitionMetadata=$partitionMetadata" +
                    ", authorizedOperations=$authorizedOperations" +
                    '}'
        }
    }

    // This is used to describe per-partition state in the MetadataResponse
    data class PartitionMetadata(
        val error: Errors,
        val topicPartition: TopicPartition,
        val leaderId: Int?,
        val leaderEpoch: Int?,
        val replicaIds: List<Int>,
        val inSyncReplicaIds: List<Int>,
        val offlineReplicaIds: List<Int>
    ) {

        val partition: Int
            get() = topicPartition.partition

        val topic: String
            get() = topicPartition.topic

        @Deprecated(
            message = "Use `partition` instead.",
            replaceWith = ReplaceWith("partition"),
        )
        fun partition(): Int {
            return topicPartition.partition
        }

        @Deprecated(
            message = "Use `topic` instead.",
            replaceWith = ReplaceWith("topic"),
        )
        fun topic(): String? {
            return topicPartition.topic
        }

        fun withoutLeaderEpoch(): PartitionMetadata {
            return PartitionMetadata(
                error = error,
                topicPartition = topicPartition,
                leaderId = leaderId,
                leaderEpoch = null,
                replicaIds = replicaIds,
                inSyncReplicaIds = inSyncReplicaIds,
                offlineReplicaIds = offlineReplicaIds
            )
        }

        override fun toString(): String {
            return "PartitionMetadata(" +
                    "error=" + error +
                    ", partition=" + topicPartition +
                    ", leader=" + leaderId +
                    ", leaderEpoch=" + leaderEpoch +
                    ", replicas=" + replicaIds.joinToString(",") +
                    ", isr=" + inSyncReplicaIds.joinToString(",") +
                    ", offlineReplicas=" + offlineReplicaIds.joinToString(",") + ')'
        }
    }

    private class Holder(
        data: MetadataResponseData
    ) {
        val brokers: Map<Int, Node>
        val controller: Node?
        val topicMetadata: Collection<TopicMetadata>

        init {
            brokers = Collections.unmodifiableMap(createBrokers(data))
            topicMetadata = createTopicMetadata(data)
            controller = brokers[data.controllerId]
        }

        private fun createBrokers(data: MetadataResponseData): Map<Int?, Node> {
            return data.brokers.valuesList().map { broker: MetadataResponseBroker ->
                Node(
                    id = broker.nodeId,
                    host = broker.host,
                    port = broker.port,
                    rack = broker.rack
                )
            }.associateBy { it.id }
        }

        private fun createTopicMetadata(data: MetadataResponseData): Collection<TopicMetadata> {
            val topicMetadataList: MutableList<TopicMetadata> = ArrayList()
            data.topics.forEach { topicMetadata ->
                val topicError = Errors.forCode(topicMetadata.errorCode)
                val topic = topicMetadata.name!!
                val topicId = topicMetadata.topicId
                val isInternal = topicMetadata.isInternal
                val partitionMetadataList: MutableList<PartitionMetadata> = ArrayList()
                topicMetadata.partitions.forEach { partitionMetadata ->
                    val partitionError = Errors.forCode(partitionMetadata.errorCode)
                    val partitionIndex = partitionMetadata.partitionIndex
                    val leaderId = partitionMetadata.leaderId
                    val leaderIdOpt = leaderId.takeUnless { leaderId < 0 }
                    val leaderEpoch = RequestUtils.getLeaderEpoch(partitionMetadata.leaderEpoch)
                    val topicPartition = TopicPartition(topic, partitionIndex)
                    partitionMetadataList.add(
                        PartitionMetadata(
                            error = partitionError,
                            topicPartition = topicPartition,
                            leaderId = leaderIdOpt,
                            leaderEpoch = leaderEpoch,
                            replicaIds = partitionMetadata.replicaNodes.toList(),
                            inSyncReplicaIds = partitionMetadata.isrNodes.toList(),
                            offlineReplicaIds = partitionMetadata.offlineReplicas.toList(),
                        )
                    )
                }
                topicMetadataList.add(
                    TopicMetadata(
                        topicError, topic, topicId, isInternal, partitionMetadataList,
                        topicMetadata.topicAuthorizedOperations
                    )
                )
            }
            return topicMetadataList
        }
    }

    override fun shouldClientThrottle(version: Short): Boolean = version >= 6

    companion object {

        const val NO_CONTROLLER_ID = -1

        const val NO_LEADER_ID = -1

        const val AUTHORIZED_OPERATIONS_OMITTED = Int.MIN_VALUE

        fun toPartitionInfo(
            metadata: PartitionMetadata,
            nodesById: Map<Int, Node>
        ): PartitionInfo {
            return PartitionInfo(
                topic = metadata.topic,
                partition = metadata.partition,
                leader = metadata.leaderId?.let { nodesById[it] },
                replicas = convertToNodeList(metadata.replicaIds, nodesById),
                inSyncReplicas = convertToNodeList(metadata.inSyncReplicaIds, nodesById),
                offlineReplicas = convertToNodeList(metadata.offlineReplicaIds, nodesById)
            )
        }

        private fun convertToNodeList(
            replicaIds: List<Int>,
            nodesById: Map<Int, Node>
        ): List<Node> = replicaIds.map { replicaId: Int ->
            nodesById[replicaId] ?: Node(replicaId, "", -1)
        }

        // Prior to Kafka version 2.4 (which coincides with Metadata version 9), the broker
        // does not propagate leader epoch information accurately while a reassignment is in
        // progress. Relying on a stale epoch can lead to FENCED_LEADER_EPOCH errors which
        // can prevent consumption throughout the course of a reassignment. It is safer in
        // this case to revert to the behavior in previous protocol versions which checks
        // leader status only.
        private fun hasReliableLeaderEpochs(version: Short): Boolean {
            return version >= 9
        }

        fun parse(buffer: ByteBuffer?, version: Short): MetadataResponse {
            return MetadataResponse(
                MetadataResponseData(ByteBufferAccessor(buffer!!), version),
                hasReliableLeaderEpochs(version)
            )
        }

        fun prepareResponse(
            version: Short,
            throttleTimeMs: Int,
            brokers: Collection<Node>,
            clusterId: String?,
            controllerId: Int,
            topics: List<MetadataResponseTopic>,
            clusterAuthorizedOperations: Int
        ): MetadataResponse = prepareResponse(
            hasReliableEpoch = hasReliableLeaderEpochs(version),
            throttleTimeMs = throttleTimeMs,
            brokers = brokers,
            clusterId = clusterId,
            controllerId = controllerId,
            topics = topics,
            clusterAuthorizedOperations = clusterAuthorizedOperations
        )

        // Visible for testing
        internal fun prepareResponse(
            hasReliableEpoch: Boolean,
            throttleTimeMs: Int,
            brokers: Collection<Node>,
            clusterId: String?,
            controllerId: Int,
            topics: List<MetadataResponseTopic>,
            clusterAuthorizedOperations: Int
        ): MetadataResponse {
            val responseData = MetadataResponseData()
            responseData.setThrottleTimeMs(throttleTimeMs)
            brokers.forEach { (id, host, port, rack) ->
                responseData.brokers.add(
                    MetadataResponseBroker()
                        .setNodeId(id)
                        .setHost(host)
                        .setPort(port)
                        .setRack(rack)
                )
            }
            responseData.setClusterId(clusterId)
            responseData.setControllerId(controllerId)
            responseData.setClusterAuthorizedOperations(clusterAuthorizedOperations)
            topics.forEach { topicMetadata -> responseData.topics.add(topicMetadata) }
            return MetadataResponse(responseData, hasReliableEpoch)
        }
    }
}
