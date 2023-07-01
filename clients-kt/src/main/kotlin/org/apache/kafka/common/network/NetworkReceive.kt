package org.apache.kafka.common.network

import java.io.EOFException
import java.io.IOException
import java.nio.ByteBuffer
import java.nio.channels.ScatteringByteChannel
import org.apache.kafka.common.memory.MemoryPool
import org.slf4j.LoggerFactory


/**
 * The default buffer capacity, in bytes
 */
private const val DEFAULT_BUFFER_CAPACITY = 4

/**
 * A size delimited Receive that consists of a 4 byte network-ordered size N followed by N bytes of
 * content.
 */
class NetworkReceive(
    private val source: String = UNKNOWN_SOURCE,
    private val maxSize: Int = UNLIMITED,
    private val memoryPool: MemoryPool = MemoryPool.NONE,
    private var buffer: ByteBuffer? = null
) : Receive {

    private val size: ByteBuffer = ByteBuffer.allocate(DEFAULT_BUFFER_CAPACITY)

    private var requestedBufferSize = -1

    override fun source(): String = source

    override fun complete(): Boolean {
        return !size.hasRemaining() && buffer?.hasRemaining() != true
    }

    @Throws(IOException::class)
    override fun readFrom(channel: ScatteringByteChannel): Long {
        var read = 0
        if (size.hasRemaining()) {
            val bytesRead = channel.read(size)
            if (bytesRead < 0) throw EOFException()
            read += bytesRead
            if (!size.hasRemaining()) {
                size.rewind()
                val receiveSize = size.getInt()
                if (receiveSize < 0) throw InvalidReceiveException("Invalid receive (size = $receiveSize)")
                if (maxSize != UNLIMITED && receiveSize > maxSize) throw InvalidReceiveException(
                    "Invalid receive (size = $receiveSize larger than $maxSize)"
                )
                requestedBufferSize = receiveSize //may be 0 for some payloads (SASL)
                if (receiveSize == 0) {
                    buffer = EMPTY_BUFFER
                }
            }
        }
        if (buffer == null && requestedBufferSize != -1) {
            // we know the size we want but haven't been able to allocate it yet
            buffer = memoryPool.tryAllocate(requestedBufferSize)
            if (buffer == null) log.trace(
                "Broker low on memory - could not allocate buffer of size {} for source {}", requestedBufferSize,
                source
            )
        }
        if (buffer != null) {
            val bytesRead = channel.read(buffer)
            if (bytesRead < 0) throw EOFException()
            read += bytesRead
        }
        return read.toLong()
    }

    override fun requiredMemoryAmountKnown(): Boolean {
        return requestedBufferSize != -1
    }

    override fun memoryAllocated(): Boolean {
        return buffer != null
    }

    @Throws(IOException::class)
    override fun close() {
        val buffer = this.buffer ?: return

        if (buffer !== EMPTY_BUFFER) {
            memoryPool.release(buffer)
            this.buffer = null
        }
    }

    fun payload(): ByteBuffer? = buffer

    fun bytesRead(): Int {
        return buffer?.let { it.position() + size. position() }
            ?: run { size.position() }
    }

    /**
     * Returns the total size of the receive including payload and size buffer
     * for use in metrics. This is consistent with [NetworkSend.size]
     */
    fun size(): Int {
        return payload()!!.limit() + size.limit()
    }

    companion object {
        const val UNKNOWN_SOURCE = ""
        const val UNLIMITED = -1
        private val log = LoggerFactory.getLogger(NetworkReceive::class.java)
        private val EMPTY_BUFFER = ByteBuffer.allocate(0)
    }
}
