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

package org.apache.kafka.trogdor.workload

import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonProperty
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.util.Random
import org.apache.kafka.common.utils.Time

/**
 * This class behaves identically to TimestampRandomPayloadGenerator, except the message size follows a gaussian
 * distribution.
 *
 * This should be used in conjunction with TimestampRecordProcessor in the Consumer to measure true end-to-end latency
 * of a system.
 *
 * `messageSizeAverage` - The average size in bytes of each message.
 * `messageSizeDeviation` - The standard deviation to use when calculating message size.
 * `messagesUntilSizeChange` - The number of messages to keep at the same size.
 * `seed` - Used to initialize Random() to remove some non-determinism.
 *
 * Here is an example spec:
 *
 * ```json
 * {
 *     "type": "gaussianTimestampRandom",
 *     "messageSizeAverage": 512,
 *     "messageSizeDeviation": 100,
 *     "messagesUntilSizeChange": 100
 * }
 * ```
 *
 * This will generate messages on a gaussian distribution with an average size each 512-bytes. The message sizes will
 * have a standard deviation of 100 bytes, and the size will only change every 100 messages. The distribution of
 * messages will be as follows:
 *
 *     The average size of the messages are 512 bytes.
 *     ~68% of the messages are between 412 and 612 bytes
 *     ~95% of the messages are between 312 and 712 bytes
 *     ~99% of the messages are between 212 and 812 bytes
 */
class GaussianTimestampRandomPayloadGenerator @JsonCreator constructor(
    @param:JsonProperty("messageSizeAverage") private val messageSizeAverage: Int,
    @param:JsonProperty("messageSizeDeviation") private val messageSizeDeviation: Double,
    @param:JsonProperty("messagesUntilSizeChange") private val messagesUntilSizeChange: Int,
    @param:JsonProperty("seed") private val seed: Long,
) : PayloadGenerator {

    private val random = Random()

    private val buffer: ByteBuffer = ByteBuffer.allocate(java.lang.Long.BYTES).apply {
        order(ByteOrder.LITTLE_ENDIAN)
    }

    private var messageTracker = 0

    private var messageSize = 0

    @JsonProperty
    fun messageSizeAverage(): Int = messageSizeAverage

    @JsonProperty
    fun messageSizeDeviation(): Double = messageSizeDeviation

    @JsonProperty
    fun messagesUntilSizeChange(): Int = messagesUntilSizeChange

    @JsonProperty
    fun seed(): Long = seed

    @Synchronized
    override fun generate(position: Long): ByteArray {
        // Make the random number generator deterministic for unit tests.
        random.setSeed(seed + position)

        // Calculate the next message size based on a gaussian distribution.
        if (messageSize == 0 || messageTracker >= messagesUntilSizeChange) {
            messageTracker = 0
            messageSize = (random.nextGaussian() * messageSizeDeviation + messageSizeAverage)
                .toInt()
                .coerceAtMost(Long.SIZE_BYTES)
        }
        messageTracker += 1

        // Generate out of order to prevent inclusion of random number generation in latency numbers.
        val result = ByteArray(messageSize)
        random.nextBytes(result)

        // Do the timestamp generation as the very last task.
        buffer.clear()
        buffer.putLong(Time.SYSTEM.milliseconds())
        buffer.rewind()
        System.arraycopy(buffer.array(), 0, result, 0, Long.SIZE_BYTES)
        return result
    }
}
