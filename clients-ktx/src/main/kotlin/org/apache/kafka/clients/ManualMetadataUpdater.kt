package org.apache.kafka.clients

import java.util.*
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.Node
import org.apache.kafka.common.errors.AuthenticationException
import org.apache.kafka.common.requests.MetadataResponse
import org.apache.kafka.common.requests.RequestHeader

/**
 * A simple implementation of `MetadataUpdater` that returns the cluster nodes set via the constructor or via
 * `setNodes`.
 *
 * This is useful in cases where automatic metadata updates are not required. An example is controller/broker
 * communication.
 *
 * This class is not thread-safe!
 */
open class ManualMetadataUpdater(
    private var nodes: List<Node> = emptyList(),
) : MetadataUpdater {

    fun setNodes(nodes: List<Node>) {
        this.nodes = nodes
    }

    override fun fetchNodes(): List<Node> = ArrayList(nodes)

    override fun isUpdateDue(now: Long): Boolean = false

    override fun maybeUpdate(now: Long): Long = Long.MAX_VALUE

    override fun handleServerDisconnect(
        now: Long,
        nodeId: String?,
        maybeAuthException: AuthenticationException?
    ) {
        // We don't fail the broker on failures. There should be sufficient information from
        // the NetworkClient logs to indicate the reason for the failure.
    }

    override fun handleFailedRequest(now: Long, maybeFatalException: KafkaException?) = Unit

    override fun handleSuccessfulResponse(requestHeader: RequestHeader?, now: Long, response: MetadataResponse) = Unit

    override fun close() = Unit
}
