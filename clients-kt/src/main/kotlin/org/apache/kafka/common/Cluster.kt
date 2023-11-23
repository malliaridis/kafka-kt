package org.apache.kafka.common

import java.net.InetSocketAddress
import java.util.*

/**
 * An immutable representation of a subset of the nodes, topics, and partitions in the Kafka
 * cluster.
 *
 * @property nodes A randomized, unmodifiable copy of the nodes
 */
class Cluster private constructor(
    val clusterResource: ClusterResource,
    val isBootstrapConfigured: Boolean = false,
    val nodes: List<Node>,
    partitions: Collection<PartitionInfo>,
    val unauthorizedTopics: Set<String> = emptySet(),
    val invalidTopics: Set<String> = emptySet(),
    val internalTopics: Set<String> = emptySet(),
    val controller: Node? = null,
    private val topicIds: Map<String, Uuid>,
) {

    /**
     * Partitions with [TopicPartition] as their index for quick lookups.
     */
    private val partitionsByTopicPartition: Map<TopicPartition, PartitionInfo>

    /**
     * Partitions with their topic as index for quick lookups.
     */
    private val partitionsByTopic: Map<String, List<PartitionInfo>>

    /**
     * Available partitions with their topic as index for quick lookups.
     */
    private val availablePartitionsByTopic: Map<String, List<PartitionInfo>>

    /**
     * Partitions with their associated nodes as index for quick lookups.
     */
    private val partitionsByNode: Map<Int, List<PartitionInfo>>

    /**
     * Nodes with their IDs as index for quick lookups.
     */
    private val nodesById: Map<Int, Node> = nodes.associateBy { node -> node.id }

    /**
     * Flipped representation of [topicIds].
     */
    private val topicNames: Map<Uuid, String> =
        topicIds.entries.associateBy({ it.value }) { it.key }

    /**
     * Create a new cluster with the given id, nodes and partitions.
     *
     * @param nodes The nodes in the cluster.
     * @param partitions Information about a subset of the topic-partitions this cluster hosts.
     */
    constructor(
        clusterId: String?,
        nodes: Collection<Node> = emptyList(),
        partitions: Collection<PartitionInfo> = emptyList(),
        unauthorizedTopics: Set<String> = emptySet(),
        invalidTopics: Set<String> = emptySet(),
        internalTopics: Set<String> = emptySet(),
        controller: Node? = null,
        topicIds: Map<String, Uuid> = emptyMap(),
    ) : this(
        clusterResource = ClusterResource(clusterId),
        isBootstrapConfigured = false,
        nodes = nodes.shuffled(),
        partitions = partitions,
        unauthorizedTopics = unauthorizedTopics,
        invalidTopics = invalidTopics,
        internalTopics = internalTopics,
        controller = controller,
        topicIds = topicIds,
    )

    init {

        partitionsByTopicPartition = partitions.associateBy { partitionInfo ->
            TopicPartition(partitionInfo.topic, partitionInfo.partition)
        }

        partitionsByTopic = partitions.groupBy { partitionInfo -> partitionInfo.topic }

        availablePartitionsByTopic = partitionsByTopic.mapValues { (_, partitions) ->
            // Optimise for the common case where all partitions are available
            val hasUnavailablePartitions = partitions.any { partitionInfo ->
                partitionInfo.leader == null
            }

            if (hasUnavailablePartitions) partitions.filter { it.leader != null }
            else partitions
        }

        // Create a temporary grouped list of partitions
        val tmpPartitionsByNode = partitions.groupBy { partitionInfo ->
            partitionInfo.leader?.let { node ->
                // If the leader is known, its node information should be available
                if (!node.isEmpty) node.id
                else null
            }
        }

        // and assign the partitions to the nodes.
        partitionsByNode = nodes.map { it.id }
            .associateWith { tmpPartitionsByNode[it] ?: emptyList() }
    }

    /**
     * Return a copy of this cluster combined with `partitions`.
     */
    fun withPartitions(partitions: Map<TopicPartition, PartitionInfo>): Cluster {
        return Cluster(
            clusterId = clusterResource.clusterId,
            nodes = nodes,
            partitions = (partitionsByTopicPartition + partitions).values,
            unauthorizedTopics = unauthorizedTopics.toSet(),
            invalidTopics = invalidTopics.toSet(),
            internalTopics = internalTopics.toSet(),
            controller = controller
        )
    }

    /**
     * @return The known set of nodes
     */
    @Deprecated(
        message = "Use property instaed.",
        replaceWith = ReplaceWith("nodes")
    )
    fun nodes(): List<Node?> {
        return nodes
    }

    /**
     * Get the node by the node id (or null if the node is not online or does not exist)
     * @param id The id of the node
     * @return The node, or `null` if the node is not online or does not exist
     */
    fun nodeById(id: Int): Node? {
        return nodesById[id]
    }

    /**
     * Get the node by node id if the replica for the given partition is online. If the [partition]
     * was not found `null` is returned.
     *
     * @param partition
     * @param id
     * @return the node from [partition] with the [id] if online, `null` otherwise.
     */
    fun nodeIfOnline(partition: TopicPartition, id: Int): Node? {
        val node = nodeById(id) ?: return null
        val partitionInfo = partition(partition) ?: return null

        return if (!partitionInfo.offlineReplicas.contains(node)
            && partitionInfo.replicas.contains(node)
        ) node
        else null
    }

    /**
     * Get the current leader for the given topic-partition.
     *
     * @param topicPartition The topic and partition we want to know the leader for.
     * @return The node that is the leader for this topic-partition, or null if there is currently
     * no leader.
     */
    fun leaderFor(topicPartition: TopicPartition): Node? {
        return partitionsByTopicPartition[topicPartition]?.leader
    }

    /**
     * Get the metadata for the specified partition
     * @param topicPartition The topic and partition to fetch info for
     * @return The metadata about the given topic and partition, or null if none is found
     */
    fun partition(topicPartition: TopicPartition): PartitionInfo? {
        return partitionsByTopicPartition[topicPartition]
    }

    /**
     * Get the list of partitions for this topic.
     *
     * @param topic The topic name
     * @return A list of partitions
     */
    fun partitionsForTopic(topic: String?): List<PartitionInfo> {
        return partitionsByTopic.getOrDefault(topic, emptyList())
    }

    /**
     * Get the number of partitions for the given topic.
     * @param topic The topic to get the number of partitions for
     * @return The number of partitions or `null` if there is no corresponding metadata
     */
    fun partitionCountForTopic(topic: String): Int? {
        return partitionsByTopic[topic]?.size
    }

    /**
     * Get the list of available partitions for this topic
     * @param topic The topic name
     * @return A list of partitions
     */
    fun availablePartitionsForTopic(topic: String?): List<PartitionInfo> {
        return availablePartitionsByTopic.getOrDefault(topic, emptyList())
    }

    /**
     * Get the list of partitions whose leader is this node
     * @param nodeId The node id
     * @return A list of partitions
     */
    fun partitionsForNode(nodeId: Int): List<PartitionInfo> {
        return partitionsByNode.getOrDefault(nodeId, emptyList())
    }

    /**
     * Get all topics.
     * @return a set of all topics
     */
    fun topics(): Set<String> {
        return partitionsByTopic.keys
    }

    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("unauthorizedTopics")
    )
    fun unauthorizedTopics(): Set<String> {
        return unauthorizedTopics
    }

    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("invalidTopics")
    )
    fun invalidTopics(): Set<String> {
        return invalidTopics
    }

    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("internalTopics")
    )
    fun internalTopics(): Set<String> {
        return internalTopics
    }

    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("clusterResource")
    )
    fun clusterResource(): ClusterResource {
        return clusterResource
    }

    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("controller")
    )
    fun controller(): Node? {
        return controller
    }

    fun topicIds(): Collection<Uuid> {
        return topicIds.values
    }

    fun topicId(topic: String): Uuid {
        return topicIds[topic] ?: Uuid.ZERO_UUID
    }

    fun topicName(topicId: Uuid): String? {
        return topicNames[topicId]
    }

    override fun toString(): String {
        return "Cluster(id = ${clusterResource.clusterId}" + ", nodes = $nodes" +
                ", partitions = ${partitionsByTopicPartition.values}, controller = $controller)"
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other == null || javaClass != other.javaClass) return false
        val cluster = other as Cluster
        return isBootstrapConfigured == cluster.isBootstrapConfigured
                && nodes == cluster.nodes
                && unauthorizedTopics == cluster.unauthorizedTopics
                && invalidTopics == cluster.invalidTopics
                && internalTopics == cluster.internalTopics
                && controller == cluster.controller
                && partitionsByTopicPartition == cluster.partitionsByTopicPartition
                && clusterResource == cluster.clusterResource
    }

    override fun hashCode(): Int {
        return Objects.hash(
            isBootstrapConfigured,
            nodes,
            unauthorizedTopics,
            invalidTopics,
            internalTopics,
            controller,
            partitionsByTopicPartition,
            clusterResource
        )
    }

    companion object {
        /**
         * Create an empty cluster instance with no nodes and no topic-partitions.
         */
        fun empty(): Cluster {
            return Cluster(
                clusterId = null,
                nodes = emptyList(),
                partitions = emptyList(),
                unauthorizedTopics = emptySet(),
                internalTopics = emptySet(),
            )
        }

        /**
         * Create a "bootstrap" cluster using the given list of host/ports
         * @param addresses The addresses
         * @return A cluster for these hosts/ports
         */
        fun bootstrap(addresses: List<InetSocketAddress>): Cluster {
            val nodes: List<Node> = addresses.mapIndexed { index, address ->
                Node(-index - 1, address.hostString, address.port)
            }
            return Cluster(
                clusterResource = ClusterResource(null),
                isBootstrapConfigured = true,
                nodes = nodes,
                partitions = emptyList(),
                unauthorizedTopics = emptySet(),
                invalidTopics = emptySet(),
                internalTopics = emptySet(),
                controller = null,
                topicIds = emptyMap()
            )
        }
    }
}
