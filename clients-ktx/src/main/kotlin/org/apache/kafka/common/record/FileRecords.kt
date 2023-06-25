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

package org.apache.kafka.common.record

import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.network.TransferableChannel
import org.apache.kafka.common.record.FileLogInputStream.FileChannelRecordBatch
import org.apache.kafka.common.utils.AbstractIterator
import org.apache.kafka.common.utils.Time
import org.apache.kafka.common.utils.Utils.atomicMoveWithFallback
import org.apache.kafka.common.utils.Utils.closeQuietly
import org.apache.kafka.common.utils.Utils.readFully
import java.io.Closeable
import java.io.File
import java.io.IOException
import java.io.RandomAccessFile
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.Files
import java.nio.file.StandardOpenOption
import java.util.*
import java.util.concurrent.atomic.AtomicInteger
import kotlin.math.min

/**
 * A [Records] implementation backed by a file. An optional start and end position can be applied to
 * this instance to enable slicing a range of the log records.
 */
class FileRecords internal constructor(
    @field:Volatile var file: File,
    val channel: FileChannel,
    private val start: Int,
    private val end: Int,
    private val isSlice: Boolean
) : AbstractRecords(), Closeable {

    private val batches: Iterable<FileChannelRecordBatch>

    // mutable state
    private val size: AtomicInteger = AtomicInteger()

    /**
     * The `FileRecords.open` methods should be used instead of this constructor whenever possible.
     * The constructor is visible for tests.
     */
    init {
        if (isSlice) {
            // don't check the file size if this is just a slice view
            size.set(end - start)
        } else {
            if (channel.size() > Int.MAX_VALUE) throw KafkaException(
                "The size of segment $file (${channel.size()}) is larger than the maximum " +
                        "allowed segment size of ${Int.MAX_VALUE}"
            )
            val limit = channel.size().toInt().coerceAtMost(end)
            size.set(limit - start)

            // if this is not a slice, update the file pointer to the end of the file
            // set the file position to the last byte in the file
            channel.position(limit.toLong())
        }
        batches = batchesFrom(start)
    }

    override fun sizeInBytes(): Int = size.get()

    /**
     * Get the underlying file.
     * @return The file
     */
    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("file"),
    )
    fun file(): File = file

    /**
     * Get the underlying file channel.
     * @return The file channel
     */
    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("channel"),
    )
    fun channel(): FileChannel = channel

    /**
     * Read log batches into the given buffer until there are no bytes remaining in the buffer or
     * the end of the file is reached.
     *
     * @param buffer The buffer to write the batches to
     * @param position Position in the buffer to read from
     * @throws IOException If an I/O error occurs, see [FileChannel.read] for details on the
     * possible exceptions
     */
    @Throws(IOException::class)
    fun readInto(buffer: ByteBuffer, position: Int) {
        readFully(channel, buffer, (position + start).toLong())
        buffer.flip()
    }

    /**
     * Return a slice of records from this instance, which is a view into this set starting from the
     * given position and with the given size limit.
     *
     * If the size is beyond the end of the file, the end will be based on the size of the file at
     * the time of the read.
     *
     * If this message set is already sliced, the position will be taken relative to that slicing.
     *
     * @param position The start position to begin the read from
     * @param size The number of bytes after the start position to include
     * @return A sliced wrapper on this message set limited based on the given position and size
     */
    @Throws(IOException::class)
    fun slice(position: Int, size: Int): FileRecords {
        val availableBytes = availableBytes(position, size)
        val startPosition = start + position

        return FileRecords(
            file = file,
            channel = channel,
            start = startPosition,
            end = startPosition + availableBytes,
            isSlice = true
        )
    }

    /**
     * Return a slice of records from this instance, the difference with [FileRecords.slice] is
     * that the position is not necessarily on an offset boundary.
     *
     * This method is reserved for cases where offset alignment is not necessary, such as in the
     * replication of raft snapshots.
     *
     * @param position The start position to begin the read from
     * @param size The number of bytes after the start position to include
     * @return An unaligned slice of records on this message set limited based on the given position
     * and size
     */
    fun sliceUnaligned(position: Int, size: Int): UnalignedFileRecords {
        val availableBytes = availableBytes(position, size)
        return UnalignedFileRecords(channel, (start + position).toLong(), availableBytes)
    }

    private fun availableBytes(position: Int, size: Int): Int {
        // Cache current size in case concurrent write changes it
        val currentSizeInBytes = sizeInBytes()
        require(position >= 0) { "Invalid position: $position in read from $this" }
        require(position <= currentSizeInBytes - start) {
            "Slice from position $position exceeds end position of $this"
        }
        require(size >= 0) { "Invalid size: $size in read from $this" }

        var end = start + position + size
        // Handle integer overflow or if end is beyond the end of the file
        if (end < 0 || end > start + currentSizeInBytes) end = start + currentSizeInBytes
        return end - (start + position)
    }

    /**
     * Append a set of records to the file. This method is not thread-safe and must be protected
     * with a lock.
     *
     * @param records The records to append
     * @return the number of bytes written to the underlying file
     */
    @Throws(IOException::class)
    fun append(records: MemoryRecords): Int {
        require(records.sizeInBytes() <= Int.MAX_VALUE - size.get()) {
            "Append of size ${records.sizeInBytes()} bytes is too large for segment with current " +
                    "file position at ${size.get()}"
        }
        val written = records.writeFullyTo(channel)
        size.getAndAdd(written)
        return written
    }

    /**
     * Commit all written data to the physical disk
     */
    @Throws(IOException::class)
    fun flush() {
        channel.force(true)
    }

    /**
     * Close this record set
     */
    @Throws(IOException::class)
    override fun close() {
        flush()
        trim()
        channel.close()
    }

    /**
     * Close file handlers used by the FileChannel but don't write to disk. This is used when the
     * disk may have failed
     */
    @Throws(IOException::class)
    fun closeHandlers() = channel.close()

    /**
     * Delete this message set from the filesystem
     *
     * @throws IOException if deletion fails due to an I/O error
     * @return  `true` if the file was deleted by this method; `false` if the file could not be
     * deleted because it did not exist
     */
    @Throws(IOException::class)
    fun deleteIfExists(): Boolean {
        closeQuietly(channel, "FileChannel")
        return Files.deleteIfExists(file.toPath())
    }

    /**
     * Trim file when close or roll to next file
     */
    @Throws(IOException::class)
    fun trim() = truncateTo(sizeInBytes())

    /**
     * Update the parent directory (to be used with caution since this does not reopen the file
     * channel)
     *
     * @param parentDir The new parent directory
     */
    fun updateParentDir(parentDir: File?) {
        file = File(parentDir, file.name)
    }

    /**
     * Rename the file that backs this message set
     * @throws IOException if rename fails.
     */
    @Throws(IOException::class)
    fun renameTo(f: File) {
        try {
            atomicMoveWithFallback(file.toPath(), f.toPath(), false)
        } finally {
            file = f
        }
    }

    /**
     * Truncate this file message set to the given size in bytes. Note that this API does no checking that the
     * given size falls on a valid message boundary.
     * In some versions of the JDK truncating to the same size as the file message set will cause an
     * update of the files mtime, so truncate is only performed if the targetSize is smaller than the
     * size of the underlying FileChannel.
     * It is expected that no other threads will do writes to the log when this function is called.
     * @param targetSize The size to truncate to. Must be between 0 and sizeInBytes.
     * @return The number of bytes truncated off
     */
    @Throws(IOException::class)
    fun truncateTo(targetSize: Int): Int {
        val originalSize = sizeInBytes()
        if (targetSize > originalSize || targetSize < 0) throw KafkaException(
            "Attempt to truncate log segment $file to $targetSize bytes failed, " +
                    " size of this log segment is $originalSize bytes."
        )
        if (targetSize < channel.size().toInt()) {
            channel.truncate(targetSize.toLong())
            size.set(targetSize)
        }
        return originalSize - targetSize
    }

    override fun downConvert(
        toMagic: Byte,
        firstOffset: Long,
        time: Time,
    ): ConvertedRecords<out Records> {
        val convertedRecords: ConvertedRecords<MemoryRecords> = RecordsUtil.downConvert(
            batches = batches,
            toMagic = toMagic,
            firstOffset = firstOffset,
            time = time,
        )

        return if (convertedRecords.recordConversionStats.numRecordsConverted == 0) {
            // This indicates that the message is too large, which means that the buffer is not
            // large enough to hold a full record batch. We just return all the bytes in this
            // instance. Even though the record batch does not have the right format version, we
            // expect old clients to raise an error to the user after reading the record batch size
            // and seeing that there are not enough available bytes in the response to read it
            // fully. Note that this is only possible prior to KIP-74, after which the broker was
            // changed to always return at least one full record batch, even if it requires
            // exceeding the max fetch size requested by the client.
            ConvertedRecords(this, RecordConversionStats.EMPTY)
        } else convertedRecords
    }

    @Throws(IOException::class)
    override fun writeTo(channel: TransferableChannel, position: Long, length: Int): Long {
        val newSize = min(this.channel.size(), end.toLong()) - start
        val oldSize = sizeInBytes()
        if (newSize < oldSize) throw KafkaException(
            String.format(
                "Size of FileRecords %s has been truncated during write: old size %d, new size %d",
                file.absolutePath, oldSize, newSize
            )
        )
        val position = start + position
        val count = min(length.toLong(), oldSize - position)
        return channel.transferFrom(this.channel, position, count)
    }

    /**
     * Search forward for the file position of the last offset that is greater than or equal to the
     * target offset and return its physical position and the size of the message (including log
     * overhead) at the returned offset. If no such offsets are found, return null.
     *
     * @param targetOffset The offset to search for.
     * @param startingPosition The starting position in the file to begin searching from.
     */
    fun searchForOffsetWithSize(targetOffset: Long, startingPosition: Int): LogOffsetPosition? {
        for (batch: FileChannelRecordBatch in batchesFrom(startingPosition)) {
            val offset = batch.lastOffset()
            if (offset >= targetOffset) return LogOffsetPosition(
                offset = offset,
                position = batch.position,
                size = batch.sizeInBytes(),
            )
        }
        return null
    }

    /**
     * Search forward for the first message that meets the following requirements:
     * - Message's timestamp is greater than or equals to the targetTimestamp.
     * - Message's position in the log file is greater than or equals to the startingPosition.
     * - Message's offset is greater than or equals to the startingOffset.
     *
     * @param targetTimestamp The timestamp to search for.
     * @param startingPosition The starting position to search.
     * @param startingOffset The starting offset to search.
     * @return The timestamp and offset of the message found. Null if no message is found.
     */
    fun searchForTimestamp(
        targetTimestamp: Long,
        startingPosition: Int,
        startingOffset: Long
    ): TimestampAndOffset? {
        for (batch: RecordBatch in batchesFrom(startingPosition)) {
            if (batch.maxTimestamp() >= targetTimestamp) {
                // We found a message
                for (record: Record in batch) {
                    val timestamp = record.timestamp()
                    if (timestamp >= targetTimestamp && record.offset() >= startingOffset)
                        return TimestampAndOffset(
                            timestamp = timestamp,
                            offset = record.offset(),
                            leaderEpoch = maybeLeaderEpoch(batch.partitionLeaderEpoch()),
                    )
                }
            }
        }
        return null
    }

    /**
     * Return the largest timestamp of the messages after a given position in this file message set.
     * @param startingPosition The starting position.
     * @return The largest timestamp of the messages after the given position.
     */
    fun largestTimestampAfter(startingPosition: Int): TimestampAndOffset {
        var maxTimestamp = RecordBatch.NO_TIMESTAMP
        var offsetOfMaxTimestamp = -1L
        var leaderEpochOfMaxTimestamp = RecordBatch.NO_PARTITION_LEADER_EPOCH
        for (batch: RecordBatch in batchesFrom(startingPosition)) {
            val timestamp = batch.maxTimestamp()
            if (timestamp > maxTimestamp) {
                maxTimestamp = timestamp
                offsetOfMaxTimestamp = batch.lastOffset()
                leaderEpochOfMaxTimestamp = batch.partitionLeaderEpoch()
            }
        }
        return TimestampAndOffset(
            maxTimestamp, offsetOfMaxTimestamp,
            maybeLeaderEpoch(leaderEpochOfMaxTimestamp)
        )
    }

    private fun maybeLeaderEpoch(leaderEpoch: Int): Int? =
        if (leaderEpoch == RecordBatch.NO_PARTITION_LEADER_EPOCH) null
        else leaderEpoch

    /**
     * Get an iterator over the record batches in the file. Note that the batches are
     * backed by the open file channel. When the channel is closed (i.e. when this instance
     * is closed), the batches will generally no longer be readable.
     * @return An iterator over the batches
     */
    override fun batches(): Iterable<FileChannelRecordBatch> {
        return batches
    }

    override fun toString(): String {
        return ("FileRecords(size=${sizeInBytes()}" +
                ", file=$file" +
                ", start=$start" +
                ", end=$end" +
                ")")
    }

    /**
     * Get an iterator over the record batches in the file, starting at a specific position. This is
     * similar to [batches] except that callers specify a particular position to start reading the
     * batches from. This method must be used with caution: the start position passed in must be a
     * known start of a batch.
     *
     * @param start The position to start record iteration from; must be a known position for start of a batch
     * @return An iterator over batches starting from `start`
     */
    fun batchesFrom(start: Int): Iterable<FileChannelRecordBatch> =
        Iterable { batchIterator(start) }

    override fun batchIterator(): AbstractIterator<FileChannelRecordBatch> = batchIterator(start)

    private fun batchIterator(start: Int): AbstractIterator<FileChannelRecordBatch> {
        val end: Int = if (isSlice) this.end else sizeInBytes()
        val inputStream = FileLogInputStream(this, start, end)
        return RecordBatchIterator(inputStream)
    }

    data class LogOffsetPosition(val offset: Long, val position: Int, val size: Int) {
        override fun toString(): String {
            return "LogOffsetPosition(" +
                    "offset=$offset" +
                    ", position=$position" +
                    ", size=$size" +
                    ')'
        }
    }

    data class TimestampAndOffset(
        val timestamp: Long,
        val offset: Long,
        val leaderEpoch: Int?
    ) {
        override fun toString(): String {
            return "TimestampAndOffset(" +
                    "timestamp=$timestamp" +
                    ", offset=$offset" +
                    ", leaderEpoch=$leaderEpoch" +
                    ')'
        }
    }

    companion object {

        @Throws(IOException::class)
        fun open(
            file: File,
            mutable: Boolean = true,
            fileAlreadyExists: Boolean = false,
            initFileSize: Int = 0,
            preallocate: Boolean = false
        ): FileRecords {
            val channel = openChannel(file, mutable, fileAlreadyExists, initFileSize, preallocate)
            val end = if ((!fileAlreadyExists && preallocate)) 0 else Int.MAX_VALUE
            return FileRecords(
                file = file,
                channel = channel,
                start = 0,
                end = end,
                isSlice = false
            )
        }

        @Throws(IOException::class)
        fun open(
            file: File,
            fileAlreadyExists: Boolean,
            initFileSize: Int,
            preallocate: Boolean
        ): FileRecords {
            return open(
                file = file,
                mutable = true,
                fileAlreadyExists = fileAlreadyExists,
                initFileSize = initFileSize,
                preallocate = preallocate
            )
        }

        /**
         * Open a channel for the given file
         * For windows NTFS and some old LINUX file system, set preallocate to true and initFileSize
         * with one value (for example 512 * 1025 *1024 ) can improve the kafka produce performance.
         *
         * @param file File path
         * @param mutable mutable
         * @param fileAlreadyExists File already exists or not
         * @param initFileSize The size used for pre allocate file, for example 512 * 1025 *1024
         * @param preallocate Pre-allocate file or not, gotten from configuration.
         */
        @Throws(IOException::class)
        private fun openChannel(
            file: File,
            mutable: Boolean,
            fileAlreadyExists: Boolean,
            initFileSize: Int,
            preallocate: Boolean
        ): FileChannel {
            return if (mutable) {
                if (fileAlreadyExists || !preallocate) {
                    FileChannel.open(
                        file.toPath(),
                        StandardOpenOption.CREATE,
                        StandardOpenOption.READ,
                        StandardOpenOption.WRITE
                    )
                } else {
                    val randomAccessFile = RandomAccessFile(file, "rw")
                    randomAccessFile.setLength(initFileSize.toLong())
                    randomAccessFile.channel
                }
            } else FileChannel.open(file.toPath())
        }
    }
}
