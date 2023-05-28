package org.apache.kafka.common.network

import java.io.IOException

/**
 * This interface models the in-progress sending of data.
 */
interface Send {
    /**
     * Is this send complete?
     */
    fun completed(): Boolean

    /**
     * Write some as-yet unwritten bytes from this send to the provided channel. It may take multiple calls for the send
     * to be completely written
     * @param channel The Channel to write to
     * @return The number of bytes written
     * @throws IOException If the write fails
     */
    @Throws(IOException::class)
    fun writeTo(channel: TransferableChannel?): Long

    /**
     * Size of the send
     */
    fun size(): Long
}
