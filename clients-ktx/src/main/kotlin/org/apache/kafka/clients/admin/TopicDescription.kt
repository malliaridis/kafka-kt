package org.apache.kafka.clients.admin

import org.apache.kafka.common.TopicPartitionInfo
import org.apache.kafka.common.Uuid
import org.apache.kafka.common.acl.AclOperation
import org.apache.kafka.common.utils.Utils

/**
 * A detailed description of a single topic in the cluster.
 *
 * @property name The topic name
 * @property internal Whether the topic is internal to Kafka. An example of an internal topic is the offsets and group
 * management topic: __consumer_offsets.
 * @property partitions A list of partitions where the index represents the partition id and the element contains
 * leadership and replica information for that partition.
 * @property authorizedOperations authorized operations for this topic, or empty set if this is not known.
 */
data class TopicDescription(
    val name: String,
    val internal: Boolean,
    val partitions: List<TopicPartitionInfo>,
    val authorizedOperations: Set<AclOperation>? = null,
) {

    // Do not include topicId in primary constructor to exclude from generated functions
    private var _topicId: Uuid= Uuid.ZERO_UUID
    val topicId: Uuid
        get() = _topicId

    constructor(
        name: String,
        internal: Boolean,
        partitions: List<TopicPartitionInfo>,
        authorizedOperations: Set<AclOperation>? = null,
        topicId: Uuid = Uuid.ZERO_UUID,
    ) : this(
        name = name,
        internal = internal,
        partitions = partitions,
        authorizedOperations = authorizedOperations,
    ) {
        this._topicId = topicId
    }

    override fun toString(): String {
        return "(name=$name, " +
                "internal=$internal, " +
                "partitions=${Utils.join(partitions, ",")}, " +
                "authorizedOperations=$authorizedOperations)"
    }
}
