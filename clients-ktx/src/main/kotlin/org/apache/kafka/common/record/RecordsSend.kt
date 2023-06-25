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

import org.apache.kafka.common.network.Send
import org.apache.kafka.common.network.TransferableChannel
import java.io.EOFException
import java.io.IOException
import java.nio.ByteBuffer

abstract class RecordsSend<T : BaseRecords?> protected constructor(
    private val records: T,
    private val maxBytesToWrite: Int
) : Send {
    
    private var remaining: Int = maxBytesToWrite

    private var pending = false

    override fun completed(): Boolean = remaining <= 0 && !pending

    @Throws(IOException::class)
    override fun writeTo(channel: TransferableChannel): Long {
        var written: Long = 0
        if (remaining > 0) {
            written = writeTo(channel, size() - remaining, remaining)
            if (written < 0)
                throw EOFException("Wrote negative bytes to channel. This shouldn't happen.")

            remaining -= written.toInt()
        }
        pending = channel.hasPendingWrites()
        if (remaining <= 0 && pending) channel.write(EMPTY_BYTE_BUFFER)
        return written
    }

    override fun size(): Long = maxBytesToWrite.toLong()

    protected fun records(): T = records

    /**
     * Write records up to `remaining` bytes to `channel`. The implementation is allowed to be
     * stateful. The contract from the caller is that the first invocation will be with
     * `previouslyWritten` equal to 0, and `remaining` equal to the to maximum bytes we want to
     * write the to `channel`. `previouslyWritten` and `remaining` will be adjusted appropriately
     * for every subsequent invocation. See [writeTo] for example expected usage.
     * 
     * @param channel The channel to write to
     * @param previouslyWritten Bytes written in previous calls to [writeTo]; 0 if being called for
     * the first time
     * @param remaining Number of bytes remaining to be written
     * @return The number of bytes actually written
     * @throws IOException For any IO errors
     */
    @Throws(IOException::class)
    protected abstract fun writeTo(
        channel: TransferableChannel,
        previouslyWritten: Long,
        remaining: Int
    ): Long

    companion object {
        private val EMPTY_BYTE_BUFFER = ByteBuffer.allocate(0)
    }
}
