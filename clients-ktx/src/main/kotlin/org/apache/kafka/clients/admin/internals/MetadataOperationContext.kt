package org.apache.kafka.clients.admin.internals

import org.apache.kafka.clients.admin.AbstractOptions
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.InvalidMetadataException
import org.apache.kafka.common.internals.KafkaFutureImpl
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.MetadataResponse

/**
 * Context class to encapsulate parameters of a call to fetch and use cluster metadata.
 * Some of the parameters are provided at construction and are immutable whereas others are provided
 * as "Call" are completed and values are available.
 *
 * @param T The type of return value of the KafkaFuture
 * @param O The type of configuration option.
 */
class MetadataOperationContext<T, O : AbstractOptions<O>?>(
    val topics: Collection<String>,
    val options: O,
    val deadline: Long,
    val futures: Map<TopicPartition, KafkaFutureImpl<T>>
) {
    var response: MetadataResponse? = null

    @Deprecated(message = "Use class property instead.")
    fun setResponse(response: MetadataResponse?) {
        this.response = response
    }

    @Deprecated(
        message = "Use class property instead.",
        replaceWith = ReplaceWith("response"),
    )
    fun response(): MetadataResponse? {
        return response
    }

    @Deprecated(message = "Use class property instead.")
    fun options(): O {
        return options
    }

    @Deprecated(
        message = "Use class property instead.",
        replaceWith = ReplaceWith("deadline"),
    )
    fun deadline(): Long {
        return deadline
    }

    @Deprecated(
        message = "Use class property instead.",
        replaceWith = ReplaceWith("futures"),
    )
    fun futures(): Map<TopicPartition, KafkaFutureImpl<T>> {
        return futures
    }

    @Deprecated(
        message = "Use class property instead.",
        replaceWith = ReplaceWith("topics"),
    )
    fun topics(): Collection<String> {
        return topics
    }

    companion object {
        fun handleMetadataErrors(response: MetadataResponse) {
            for (tm in response.topicMetadata()) {
                for (pm in tm.partitionMetadata) {
                    if (shouldRefreshMetadata(pm.error)) {
                        throw pm.error.exception
                    }
                }
            }
        }

        fun shouldRefreshMetadata(error: Errors): Boolean {
            return error.exception is InvalidMetadataException
        }
    }
}
