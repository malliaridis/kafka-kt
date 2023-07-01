package org.apache.kafka.common.network

import java.io.Closeable
import java.io.IOException
import java.nio.channels.ScatteringByteChannel

/**
 * This interface models the in-progress reading of data from a channel to a source identified by an integer id
 */
interface Receive : Closeable {

    /**
     * The numeric id of the source from which we are receiving data.
     */
    fun source(): String?

    /**
     * Are we done receiving data?
     */
    fun complete(): Boolean

    /**
     * Read bytes into this receive from the given channel
     * @param channel The channel to read from
     * @return The number of bytes read
     * @throws IOException If the reading fails
     */
    @Throws(IOException::class)
    fun readFrom(channel: ScatteringByteChannel): Long

    /**
     * Do we know yet how much memory we require to fully read this
     */
    fun requiredMemoryAmountKnown(): Boolean

    /**
     * Has the underlying memory required to complete reading been allocated yet?
     */
    fun memoryAllocated(): Boolean
}
