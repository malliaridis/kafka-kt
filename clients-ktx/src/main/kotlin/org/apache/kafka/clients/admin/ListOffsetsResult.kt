package org.apache.kafka.clients.admin

import java.util.*
import java.util.concurrent.ExecutionException
import org.apache.kafka.common.KafkaFuture
import org.apache.kafka.common.KafkaFuture.BaseFunction
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.annotation.InterfaceStability.Evolving

/**
 * The result of the [AdminClient.listOffsets] call.
 *
 * The API of this class is evolving, see [AdminClient] for details.
 */
@Evolving
data class ListOffsetsResult(
    private val futures: Map<TopicPartition, KafkaFuture<ListOffsetsResultInfo>>
) {
    /**
     * Return a future which can be used to check the result for a given partition.
     */
    fun partitionResult(partition: TopicPartition): KafkaFuture<ListOffsetsResultInfo> {
        return futures[partition] ?: throw IllegalArgumentException(
            "List Offsets for partition \"$partition\" was not attempted"
        )
    }

    /**
     * Return a future which succeeds only if offsets for all specified partitions have been successfully
     * retrieved.
     */
    fun all(): KafkaFuture<Map<TopicPartition, ListOffsetsResultInfo>> {
        return KafkaFuture.allOf(*futures.values.toTypedArray<KafkaFuture<*>>())
            .thenApply {
                val offsets: MutableMap<TopicPartition, ListOffsetsResultInfo> = HashMap(futures.size)
                for (entry: Map.Entry<TopicPartition, KafkaFuture<ListOffsetsResultInfo>> in futures.entries) {
                    try {
                        offsets[entry.key] = entry.value.get()
                    } catch (e: InterruptedException) {
                        // This should be unreachable, because allOf ensured that all the futures completed successfully.
                        throw RuntimeException(e)
                    } catch (e: ExecutionException) {
                        throw RuntimeException(e)
                    }
                }
                offsets
            }
    }

    data class ListOffsetsResultInfo(
        private val offset: Long,
        private val timestamp: Long,
        private val leaderEpoch: Int?
    ) {
        override fun toString(): String {
            return ("ListOffsetsResultInfo(offset=" + offset + ", timestamp=" + timestamp + ", leaderEpoch="
                    + leaderEpoch + ")")
        }
    }
}
