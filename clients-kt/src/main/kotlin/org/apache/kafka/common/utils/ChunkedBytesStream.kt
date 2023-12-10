/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.common.utils

import java.io.FilterInputStream
import java.io.IOException
import java.io.InputStream
import java.nio.ByteBuffer
import kotlin.math.min

/**
 * ChunkedBytesStream is a copy of [BufferedInputStream] with the following differences:
 * - Unlike [java.io.BufferedInputStream.skip] this class could be configured to not push skip() to
 * input stream. We may want to avoid pushing this to input stream because it's implementation maybe inefficient,
 * e.g. the case of ZstdInputStream which allocates a new buffer from buffer pool, per skip call.
 * - Unlike [java.io.BufferedInputStream], which allocates an intermediate buffer, this uses a buffer supplier to
 * create the intermediate buffer.
 * - Unlike [java.io.BufferedInputStream], this implementation does not support [InputStream.mark] and
 * [InputStream.markSupported] will return false.
 *
 * Note that:
 * - this class is not thread safe and shouldn't be used in scenarios where multiple threads access this.
 * - the implementation of this class is performance sensitive. Minor changes such as usage of ByteBuffer instead of byte[]
 * can significantly impact performance, hence, proceed with caution.
 *
 * @property bufferSupplier Supplies the ByteBuffer which is used as intermediate buffer to store the chunk of
 * output data.
 */
class ChunkedBytesStream(
    inputStream: InputStream?,
    private val bufferSupplier: BufferSupplier,
    intermediateBufSize: Int, delegateSkipToSourceStream: Boolean,
) : FilterInputStream(inputStream) {

    /**
     * Intermediate buffer to store the chunk of output data. The ChunkedBytesStream is considered closed if
     * this buffer is null.
     */
    private var intermediateBuf: ByteArray?

    /**
     * The index one greater than the index of the last valid byte in
     * the buffer.
     * This value is always in the range `0` through `intermediateBuf.length`;
     * elements `intermediateBuf[0]`  through `intermediateBuf[count-1]
    ` * contain buffered input data obtained
     * from the underlying  input stream.
     */
    internal var count = 0

    /**
     * The current position in the buffer. This is the index of the next
     * character to be read from the `buf` array.
     *
     *
     * This value is always in the range `0`
     * through `count`. If it is less
     * than `count`, then  `intermediateBuf[pos]`
     * is the next byte to be supplied as input;
     * if it is equal to `count`, then
     * the  next `read` or `skip`
     * operation will require more bytes to be
     * read from the contained  input stream.
     */
    internal var pos = 0

    /**
     * Reference for the intermediate buffer. This reference is only kept for releasing the buffer from the
     * buffer supplier.
     */
    private val intermediateBufRef: ByteBuffer?

    /**
     * If this flag is true, we will delegate the responsibility of skipping to the sourceStream.
     * This is an alternative to reading the data from source stream, storing in an intermediate buffer
     * and skipping the values using this implementation.
     */
    private val delegateSkipToSourceStream: Boolean

    init {
        intermediateBufRef = bufferSupplier[intermediateBufSize]!!
        require(intermediateBufRef.hasArray() && intermediateBufRef.arrayOffset() == 0) {
            "provided ByteBuffer lacks array or has non-zero arrayOffset"
        }
        intermediateBuf = intermediateBufRef.array()
        this.delegateSkipToSourceStream = delegateSkipToSourceStream
    }

    /**
     * Check to make sure that buffer has not been nulled out due to close; if not return it;
     */
    @Throws(IOException::class)
    private fun getBufferIfOpen(): ByteArray = intermediateBuf ?: throw IOException("Stream closed")

    /**
     * See the general contract of the `read` method of `InputStream`.
     *
     * @return the next byte of data, or `-1` if the end of the stream is reached.
     * @throws IOException if this input stream has been closed by invoking its [.close] method,
     * or an I/O error occurs.
     *
     * @see BufferedInputStream.read
     */
    @Throws(IOException::class)
    override fun read(): Int {
        if (pos >= count) {
            fill()
            if (pos >= count) return -1
        }
        return getBufferIfOpen()[pos++].toInt() and 0xff
    }

    /**
     * Check to make sure that underlying input stream has not been nulled out due to close; if not return it;
     */
    @Throws(IOException::class)
    fun getInIfOpen(): InputStream = `in` ?: throw IOException("Stream closed")

    /**
     * Fills the intermediate buffer with more data. The amount of new data read is equal to the remaining empty space
     * in the buffer. For optimal performance, read as much data as possible in this call.
     * This method also assumes that all data has already been read in, hence pos > count.
     */
    @Throws(IOException::class)
    fun fill(): Int {
        val buffer = getBufferIfOpen()
        pos = 0
        count = pos
        val n = getInIfOpen().read(buffer, pos, buffer.size - pos)
        if (n > 0) count = n + pos
        return n
    }

    @Throws(IOException::class)
    override fun close() {
        val mybuf = intermediateBuf
        intermediateBuf = null
        val input = `in`
        `in` = null
        if (mybuf != null) bufferSupplier.release(intermediateBufRef!!)
        input?.close()
    }

    /**
     * Reads bytes from this byte-input stream into the specified byte array,
     * starting at the given offset.
     *
     *  This method implements the general contract of the corresponding
     * `[read][InputStream.read]` method of
     * the `[InputStream]` class.  As an additional
     * convenience, it attempts to read as many bytes as possible by repeatedly
     * invoking the `read` method of the underlying stream.  This
     * iterated `read` continues until one of the following
     * conditions becomes true:
     *
     * - The specified number of bytes have been read,
     * - The `read` method of the underlying stream returns `-1`, indicating end-of-file, or
     * - The `available` method of the underlying stream returns zero, indicating that further input requests
     *   would block.
     *
     *  If the first `read` on the underlying stream returns `-1` to indicate end-of-file then this method returns `-1`.
     *  Otherwise this method returns the number of bytes actually read.
     *
     * Subclasses of this class are encouraged, but not required, to attempt to read as many bytes as possible
     * in the same fashion.
     *
     * @param b destination buffer.
     * @param off offset at which to start storing bytes.
     * @param len maximum number of bytes to read.
     * @return the number of bytes read, or `-1` if the end of
     * the stream has been reached.
     * @throws IOException if this input stream has been closed by
     * invoking its [.close] method,
     * or an I/O error occurs.
     *
     * @see BufferedInputStream.read
     */
    @Throws(IOException::class)
    override fun read(b: ByteArray, off: Int, len: Int): Int {
        getBufferIfOpen() // Check for closed stream

        if (off or len or off + len or b.size - (off + len) < 0) throw IndexOutOfBoundsException()
        else if (len == 0) return 0

        var n = 0
        while (true) {
            val nread = read1(b, off + n, len - n)
            if (nread <= 0) return if (n == 0) nread else n
            n += nread
            if (n >= len) return n
            // if not closed but no bytes available, return
            val input = `in`
            if (input != null && input.available() <= 0) return n
        }
    }

    /**
     * Read characters into a portion of an array, reading from the underlying stream at most once if necessary.
     *
     * Note - Implementation copied from [BufferedInputStream]. Slight modification done to remove the mark position.
     */
    @Throws(IOException::class)
    private fun read1(b: ByteArray, off: Int, len: Int): Int {
        var avail = count - pos
        if (avail <= 0) {
            /* If the requested length is at least as large as the buffer, and
               if there is no mark/reset activity, do not bother to copy the
               bytes into the local buffer.  In this way buffered streams will
               cascade harmlessly. */
            if (len >= getBufferIfOpen().size) return getInIfOpen().read(b, off, len)

            fill()
            avail = count - pos
            if (avail <= 0) return -1
        }
        val cnt = if (avail < len) avail else len
        System.arraycopy(getBufferIfOpen(), pos, b, off, cnt)
        pos += cnt

        return cnt
    }

    /**
     * Skips over and discards exactly `n` bytes of data from this input stream.
     * If `n` is zero, then no bytes are skipped.
     * If `n` is negative, then no bytes are skipped.
     *
     * This method blocks until the requested number of bytes has been skipped, end of file is reached, or an
     * exception is thrown.
     *
     * If end of stream is reached before the stream is at the desired position, then the bytes skipped till
     * than pointed are returned.
     *
     * If an I/O error occurs, then the input stream may be in an inconsistent state. It is strongly recommended
     * that the stream be promptly closed if an I/O error occurs.
     *
     * This method first skips and discards bytes in the intermediate buffer.
     * After that, depending on the value of [.delegateSkipToSourceStream], it either delegates the skipping of
     * bytes to the sourceStream or it reads the data from input stream in chunks, copies the data into intermediate
     * buffer and skips it.
     *
     * Starting JDK 12, a new method was introduced in InputStream, skipNBytes which has a similar behaviour as
     * this method.
     *
     * @param toSkip the number of bytes to be skipped.
     * @return the actual number of bytes skipped which might be zero.
     * @throws IOException if this input stream has been closed by invoking its [.close] method,
     * `in.skip(n)` throws an IOException, or an I/O error occurs.
     */
    @Throws(IOException::class)
    override fun skip(toSkip: Long): Long {
        getBufferIfOpen() // Check for closed stream
        if (toSkip <= 0) return 0

        var remaining = toSkip

        // Skip bytes stored in intermediate buffer first
        var avail = count - pos
        var bytesSkipped = min(avail.toDouble(), remaining.toDouble()).toInt()
        pos += bytesSkipped
        remaining -= bytesSkipped.toLong()
        while (remaining > 0) {
            if (delegateSkipToSourceStream) {
                // Use sourceStream's skip() to skip the rest.
                // conversion to int is acceptable because toSkip and remaining are int.
                val delegateBytesSkipped = getInIfOpen().skip(remaining)
                if (delegateBytesSkipped == 0L) {
                    // read one byte to check for EOS
                    if (read() == -1) break

                    // one byte read so decrement number to skip
                    remaining--
                } else if (delegateBytesSkipped > remaining || delegateBytesSkipped < 0) {
                    // skipped negative or too many bytes
                    throw IOException("Unable to skip exactly")
                }
                remaining -= delegateBytesSkipped
            } else {
                // skip from intermediate buffer, filling it first (if required)
                if (pos >= count) {
                    fill()
                    // if we don't have data in intermediate buffer after fill, then stop skipping
                    if (pos >= count) break
                }
                avail = count - pos
                bytesSkipped = min(avail.toDouble(), remaining.toDouble()).toInt()
                pos += bytesSkipped
                remaining -= bytesSkipped.toLong()
            }
        }
        return toSkip - remaining
    }

    // visible for testing
    internal fun sourceStream(): InputStream = `in`

    /**
     * Returns an estimate of the number of bytes that can be read (or skipped over) from this input stream
     * without blocking by the next invocation of a method for this input stream. The next invocation might be
     * the same thread or another thread.  A single read or skip of this many bytes will not block,
     * but may read or skip fewer bytes.
     *
     * This method returns the sum of the number of bytes remaining to be read in the buffer (`count - pos`)
     * and the result of calling the [in][java.io.FilterInputStream. in].available().
     *
     * @return an estimate of the number of bytes that can be read (or skipped over) from this input stream
     * without blocking.
     * @throws IOException if this input stream has been closed by invoking its [.close] method, or an I/O error occurs.
     * @see BufferedInputStream.available
     */
    @Synchronized
    @Throws(IOException::class)
    override fun available(): Int {
        val n = count - pos
        val avail = getInIfOpen().available()
        return if (n > Int.MAX_VALUE - avail) Int.MAX_VALUE else n + avail
    }
}
