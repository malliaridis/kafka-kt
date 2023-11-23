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

import java.net.InetSocketAddress
import java.util.*
import java.util.stream.Collectors
import org.apache.kafka.common.Cluster
import org.apache.kafka.common.ClusterResource
import org.apache.kafka.common.Node
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.Uuid
import org.apache.kafka.common.requests.MetadataResponse
import org.apache.kafka.common.requests.MetadataResponse.PartitionMetadata

/**
 * An internal mutable cache of nodes, topics, and partitions in the Kafka cluster. This keeps an up-to-date Cluster
 * instance which is optimized for read access.
 */
class MetadataCache private constructor(
    private val clusterId: String?,
    private val nodes: Map<Int, Node>,
    partitions: Collection<PartitionMetadata>,
    private val unauthorizedTopics: Set<String>,
    private val invalidTopics: Set<String>,
    private val internalTopics: Set<String>,
    private val controller: Node?,
    private val topicIds: Map<String, Uuid>,
    clusterInstance: Cluster?
) {
    private val metadataByPartition: MutableMap<TopicPartition, PartitionMetadata>
    private val clusterInstance: Cluster

    internal constructor(
        clusterId: String?,
        nodes: Map<Int, Node>,
        partitions: Collection<PartitionMetadata>,
        unauthorizedTopics: Set<String>,
        invalidTopics: Set<String>,
        internalTopics: Set<String>,
        controller: Node?,
        topicIds: Map<String, Uuid>
    ) : this(
        clusterId = clusterId,
        nodes = nodes,
        partitions = partitions,
        unauthorizedTopics = unauthorizedTopics,
        invalidTopics = invalidTopics,
        internalTopics = internalTopics,
        controller = controller,
        topicIds = topicIds,
        clusterInstance = null
    )

    init {
        metadataByPartition = HashMap(partitions.size)
        partitions.forEach { partition ->
            metadataByPartition[partition.topicPartition] = partition
        }
        this.clusterInstance = clusterInstance ?: computeClusterView()
    }

    fun partitionMetadata(topicPartition: TopicPartition): PartitionMetadata? {
        return metadataByPartition[topicPartition]
    }

    fun topicIds(): Map<String, Uuid> {
        return topicIds
    }

    fun nodeById(id: Int): Node? {
        return nodes[id]
    }

    fun cluster(): Cluster = clusterInstance

    fun clusterResource(): ClusterResource {
        return ClusterResource(clusterId)
    }

    /**
     * Merges the metadata cache's contents with the provided metadata, returning a new metadata
     * cache. The provided metadata is presumed to be more recent than the cache's metadata, and
     * therefore all overlapping metadata will be overridden.
     *
     * @param newClusterId the new cluster ID
     * @param newNodes the new set of nodes
     * @param addPartitions partitions to add
     * @param addUnauthorizedTopics unauthorized topics to add
     * @param addInternalTopics internal topics to add
     * @param newController the new controller node
     * @param topicIds the mapping from topic name to topic ID from the MetadataResponse
     * @param retainTopic returns whether a topic's metadata should be retained
     * @return the merged metadata cache
     */
    fun mergeWith(
        newClusterId: String?,
        newNodes: Map<Int, Node>,
        addPartitions: Collection<PartitionMetadata>,
        addUnauthorizedTopics: Set<String>,
        addInvalidTopics: Set<String>,
        addInternalTopics: Set<String>,
        newController: Node?,
        topicIds: Map<String, Uuid?>,
        retainTopic: (String, Boolean) -> Boolean
    ): MetadataCache {

        val shouldRetainTopic: (String) -> Boolean = { topic: String ->
            retainTopic(topic, internalTopics.contains(topic))
        }
        val newMetadataByPartition: MutableMap<TopicPartition, PartitionMetadata> = HashMap(addPartitions.size)

        // We want the most recent topic ID. We start with the previous ID stored for retained topics and then
        // update with newest information from the MetadataResponse. We always take the latest state, removing existing
        // topic IDs if the latest state contains the topic name but not a topic ID.
        val tempTopicIds = topicIds.filterKeys { shouldRetainTopic(it) }.toMutableMap()

        addPartitions.forEach { partitionMetadata ->
            newMetadataByPartition[partitionMetadata.topicPartition] = partitionMetadata
            val id = topicIds[partitionMetadata.topic]
            if (id != null) tempTopicIds[partitionMetadata.topic] = id
            else tempTopicIds.remove(partitionMetadata.topic) // Remove if the latest metadata does not have a topic ID
        }

        // Remove nullability (since topics with nullable UUID are already removed
        val newTopicIds = tempTopicIds.mapNotNull { (key, value) -> value?.let { key to value } }
            .toMap()

        metadataByPartition.forEach { (key, value) ->
            if (shouldRetainTopic(key.topic)) newMetadataByPartition.putIfAbsent(key, value)
        }
        val newUnauthorizedTopics = fillSet(
            baseSet = addUnauthorizedTopics,
            fillSet = unauthorizedTopics,
            predicate = shouldRetainTopic,
        )
        val newInvalidTopics = fillSet(
            baseSet = addInvalidTopics,
            fillSet = invalidTopics,
            predicate = shouldRetainTopic
        )
        val newInternalTopics = fillSet(
            baseSet = addInternalTopics,
            fillSet = internalTopics,
            predicate = shouldRetainTopic
        )
        return MetadataCache(
            newClusterId,
            newNodes,
            newMetadataByPartition.values,
            newUnauthorizedTopics,
            newInvalidTopics,
            newInternalTopics,
            newController,
            newTopicIds,
        )
    }

    private fun computeClusterView() : Cluster {
        val partitionInfos = metadataByPartition.values.map { metadata ->
            MetadataResponse.toPartitionInfo(metadata, nodes)
        }
        return Cluster(
            clusterId = clusterId,
            nodes = nodes.values,
            partitions = partitionInfos,
            unauthorizedTopics = unauthorizedTopics,
            invalidTopics = invalidTopics,
            internalTopics = internalTopics,
            controller = controller,
            topicIds = topicIds
        )
    }

    override fun toString(): String {
        return "MetadataCache{" +
                "clusterId='" + clusterId + '\'' +
                ", nodes=" + nodes +
                ", partitions=" + metadataByPartition.values +
                ", controller=" + controller +
                '}'
    }

    companion object {
        /**
         * Copies `baseSet` and adds all non-existent elements in `fillSet` such that `predicate` is true.
         * In other words, all elements of `baseSet` will be contained in the result, with additional non-overlapping
         * elements in `fillSet` where the predicate is true.
         *
         * @param baseSet the base elements for the resulting set
         * @param fillSet elements to be filled into the resulting set
         * @param predicate tested against the fill set to determine whether elements should be added to the base set
         */
        private fun <T> fillSet(baseSet: Set<T>, fillSet: Set<T>, predicate: (T) -> Boolean): Set<T> {
            val result: MutableSet<T> = HashSet(baseSet)
            fillSet.forEach { element -> if (predicate(element)) result.add(element) }
            return result
        }

        fun bootstrap(addresses: List<InetSocketAddress>): MetadataCache {
            val nodes: MutableMap<Int, Node> = HashMap()
            var nodeId = -1
            for (address in addresses) {
                nodes[nodeId] = Node(nodeId, address.hostString, address.port)
                nodeId--
            }
            return MetadataCache(
                null, nodes, emptyList(), emptySet(), emptySet(), emptySet(),
                null, emptyMap(), Cluster.bootstrap(addresses)
            )
        }

        fun empty(): MetadataCache {
            return MetadataCache(
                null,
                emptyMap(),
                emptyList(),
                emptySet(),
                emptySet(),
                emptySet(),
                null,
                emptyMap(),
                Cluster.empty()
            )
        }
    }
}
