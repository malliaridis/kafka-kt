package org.apache.kafka.clients.admin

import org.apache.kafka.common.KafkaFuture
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.annotation.InterfaceStability.Evolving
import org.apache.kafka.common.internals.KafkaFutureImpl

/**
 * The result of [Admin.electLeaders]
 *
 * The API of this class is evolving, see [Admin] for details.
 */
@Evolving
class ElectLeadersResult internal constructor(
    private val electionFuture: KafkaFuture<Map<TopicPartition, Throwable?>>,
) {

    /**
     *
     * Get a future for the topic partitions for which a leader election was attempted.
     * If the election succeeded then the value for a topic partition will be the empty Optional.
     * Otherwise the election failed and the Optional will be set with the error.
     */
    fun partitions(): KafkaFuture<Map<TopicPartition, Throwable?>> = electionFuture

    /**
     * Return a future which succeeds if all the topic elections succeed.
     */
    fun all(): KafkaFuture<Void?> {
        val result = KafkaFutureImpl<Void?>()
        partitions().whenComplete(
            object : KafkaFuture.BiConsumer<Map<TopicPartition, Throwable?>, Throwable?> {
                override fun accept(topicPartitions: Map<TopicPartition, Throwable?>, throwable: Throwable?) {
                    if (throwable != null) result.completeExceptionally(throwable)
                    else {
                        for (exception in topicPartitions.values) {
                            exception?.let {
                                result.completeExceptionally(it)
                                return
                            }
                        }
                        result.complete(null)
                    }
                }
            })
        return result
    }
}
