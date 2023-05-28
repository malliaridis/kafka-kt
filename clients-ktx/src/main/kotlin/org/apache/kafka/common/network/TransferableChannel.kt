package org.apache.kafka.common.network

import java.io.IOException
import java.nio.channels.FileChannel
import java.nio.channels.GatheringByteChannel

/**
 * Extends GatheringByteChannel with the minimal set of methods required by the Send interface. Supporting TLS and
 * efficient zero copy transfers are the main reasons for the additional methods.
 *
 * @see SslTransportLayer
 */
interface TransferableChannel : GatheringByteChannel {

    /**
     * @return true if there are any pending writes. false if the implementation directly write all data to output.
     */
    fun hasPendingWrites(): Boolean

    /**
     * Transfers bytes from `fileChannel` to this `TransferableChannel`.
     *
     * This method will delegate to [FileChannel.transferTo],
     * but it will unwrap the destination channel, if possible, in order to benefit from zero copy. This is required
     * because the fast path of `transferTo` is only executed if the destination buffer inherits from an internal JDK
     * class.
     *
     * @param fileChannel The source channel
     * @param position The position within the file at which the transfer is to begin; must be non-negative
     * @param count The maximum number of bytes to be transferred; must be non-negative
     * @return The number of bytes, possibly zero, that were actually transferred
     * @see FileChannel.transferTo
     */
    @Throws(IOException::class)
    fun transferFrom(
        fileChannel: FileChannel?,
        position: Long,
        count: Long,
    ): Long
}
