package org.apache.kafka.clients.admin

import org.apache.kafka.common.ConsumerGroupState

/**
 * A listing of a consumer group in the cluster.
 *
 * @property groupId Group ID
 * @property isSimpleConsumerGroup If Consumer Group is simple or not.
 * @property state The state of the consumer group
 */
data class ConsumerGroupListing(
    val groupId: String,
    val isSimpleConsumerGroup: Boolean,
    val state: ConsumerGroupState? = null
) {

    override fun toString(): String {
        return "(" +
                "groupId='$groupId'" +
                ", isSimpleConsumerGroup=$isSimpleConsumerGroup" +
                ", state=$state" +
                ')'
    }
}
