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

import org.apache.kafka.common.network.TransferableChannel
import java.io.IOException
import java.nio.channels.FileChannel
import kotlin.math.min

/**
 * Represents a file record set which is not necessarily offset-aligned.
 */
class UnalignedFileRecords(
    private val channel: FileChannel,
    private val position: Long,
    private val size: Int,
) : UnalignedRecords {

    override fun sizeInBytes(): Int = size

    @Throws(IOException::class)
    override fun writeTo(
        destChannel: TransferableChannel,
        previouslyWritten: Int,
        remaining: Int,
    ): Int {
        val position = position + previouslyWritten
        val count = min(remaining, sizeInBytes() - previouslyWritten)
        return destChannel.transferFrom(channel, position, count.toLong()).toInt()
    }
}
