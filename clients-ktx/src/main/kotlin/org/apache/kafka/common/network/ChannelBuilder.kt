package org.apache.kafka.common.network

import java.nio.channels.SelectionKey
import org.apache.kafka.common.Configurable
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.memory.MemoryPool

/**
 * A ChannelBuilder interface to build Channel based on configs
 */
interface ChannelBuilder : AutoCloseable, Configurable {

    /**
     * returns a Channel with TransportLayer and Authenticator configured.
     * @param  id  channel id
     * @param  key SelectionKey
     * @param  maxReceiveSize max size of a single receive buffer to allocate
     * @param  memoryPool memory pool from which to allocate buffers, or null for none
     * @return KafkaChannel
     */
    @Throws(KafkaException::class)
    fun buildChannel(
        id: String,
        key: SelectionKey,
        maxReceiveSize: Int,
        memoryPool: MemoryPool?,
        metadataRegistry: ChannelMetadataRegistry?,
    ): KafkaChannel

    /**
     * Closes ChannelBuilder
     */
    override fun close()
}
