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
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.network.Send
import org.apache.kafka.common.network.TransferableChannel
import org.slf4j.LoggerFactory
import java.io.IOException
import java.util.*

/**
 * A set of composite sends with nested [RecordsSend], sent one after another
 */
class MultiRecordsSend : Send {

    private val sendQueue: Queue<Send>

    private val size: Long

    var recordConversionStats: MutableMap<TopicPartition, RecordConversionStats>? = null
        private set

    private var totalWritten: Long = 0

    private var current: Send?

    /**
     * Construct a MultiRecordsSend from a queue of Send objects. The queue will be consumed as the
     * MultiRecordsSend progresses (on completion, it will be empty).
     */
    constructor(sends: Queue<Send>) {
        sendQueue = sends
        var size: Long = 0
        for (send in sends) size += send.size()
        this.size = size
        current = sendQueue.poll()
    }

    constructor(sends: Queue<Send>, size: Long) {
        sendQueue = sends
        this.size = size
        current = sendQueue.poll()
    }

    override fun size(): Long = size

    override fun completed(): Boolean = current == null

    // Visible for testing
    fun numResidentSends(): Int {
        var count = 0
        if (current != null) count += 1
        count += sendQueue.size
        return count
    }

    @Throws(IOException::class)
    override fun writeTo(channel: TransferableChannel): Long {
        if (completed())
            throw KafkaException("This operation cannot be invoked on a complete request.")

        var totalWrittenPerCall = 0L
        var sendComplete: Boolean
        do {
            val written = current!!.writeTo(channel)
            totalWrittenPerCall += written.toInt()
            sendComplete = current!!.completed()
            if (sendComplete) {
                updateRecordConversionStats(current)
                current = sendQueue.poll()
            }
        } while (!completed() && sendComplete)
        totalWritten += totalWrittenPerCall
        if (completed() && totalWritten != size) log.error(
            "mismatch in sending bytes over socket; expected: {} actual: {}",
            size,
            totalWritten
        )
        log.trace(
            "Bytes written as part of multi-send call: {}, total bytes written so far: {}, " +
                    "expected bytes to write: {}",
            totalWrittenPerCall,
            totalWritten,
            size
        )
        return totalWrittenPerCall.toLong()
    }

    /**
     * Get any statistics that were recorded as part of executing this [MultiRecordsSend].
     * @return Records processing statistics (could be null if no statistics were collected)
     */
    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("recordConversionStats"),
    )
    fun recordConversionStats(): Map<TopicPartition, RecordConversionStats>? = recordConversionStats

    override fun toString(): String = "MultiRecordsSend(" +
            "size=$size" +
            ", totalWritten=$totalWritten" +
            ')'

    private fun updateRecordConversionStats(completedSend: Send?) {
        // The underlying send might have accumulated statistics that need to be recorded. For
        // example, LazyDownConversionRecordsSend accumulates statistics related to the number of
        // bytes down-converted, the amount of temporary memory used for down-conversion, etc. Pull
        // out any such statistics from the underlying send and fold it up appropriately.
        if (completedSend is LazyDownConversionRecordsSend) {

            if (recordConversionStats == null) recordConversionStats = HashMap()
            recordConversionStats!![completedSend.topicPartition] =
                completedSend.recordConversionStats
        }
    }

    companion object {
        private val log = LoggerFactory.getLogger(MultiRecordsSend::class.java)
    }
}
