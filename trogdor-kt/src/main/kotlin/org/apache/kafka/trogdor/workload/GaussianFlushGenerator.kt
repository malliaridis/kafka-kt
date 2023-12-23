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
import java.util.Random
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.common.errors.InterruptException

/**
 * This generator will flush the producer after a specific number of messages, determined by a gaussian distribution.
 * This is useful to simulate a specific number of messages in a batch regardless of the message size, since batch
 * flushing is not exposed in the KafkaProducer.
 *
 * WARNING: This does not directly control when KafkaProducer will batch, this only makes the best effort.
 * This also cannot tell when a KafkaProducer batch is closed. If the KafkaProducer sends a batch before this executes,
 * this will continue to execute on its own cadence. To alleviate this, make sure to set `linger.ms` to allow
 * for messages to be generated up to your upper limit threshold, and make sure to set `batch.size` to allow
 * for all these messages.
 *
 * Here is an example spec:
 *
 * ```json
 * {
 *     "type": "gaussian",
 *     "messagesPerFlushAverage": 16,
 *     "messagesPerFlushDeviation": 4
 * }
 * ```
 *
 * This example will flush the producer on average every 16 messages, assuming `linger.ms` and `batch.size`
 * allow for it. That average changes based on a normal distribution after each flush:
 *
 *     An average of the flushes will be at 16 messages.
 *     ~68% of the flushes are at between 12 and 20 messages.
 *     ~95% of the flushes are at between 8 and 24 messages.
 *     ~99% of the flushes are at between 4 and 28 messages.
 */
class GaussianFlushGenerator @JsonCreator constructor(
    @param:JsonProperty("messagesPerFlushAverage") private val messagesPerFlushAverage: Int,
    @param:JsonProperty("messagesPerFlushDeviation") private val messagesPerFlushDeviation: Double,
) : FlushGenerator {

    private val random = Random()

    private var messageTracker = 0

    private var flushSize = 0

    init {
        calculateFlushSize()
    }

    @JsonProperty
    fun messagesPerFlushAverage(): Int = messagesPerFlushAverage

    @JsonProperty
    fun messagesPerFlushDeviation(): Double = messagesPerFlushDeviation

    @Synchronized
    private fun calculateFlushSize() {
        flushSize = (random.nextGaussian() * messagesPerFlushDeviation + messagesPerFlushAverage)
            .coerceAtMost(1.0)
            .toInt()
        messageTracker = 0
    }

    @Synchronized
    override fun <K, V> increment(producer: KafkaProducer<K, V>) {
        // Increment the message tracker.
        messageTracker += 1

        // Compare the tracked message count with the throttle limits.
        if (messageTracker >= flushSize) {
            try {
                producer.flush()
            } catch (_: InterruptException) {
                // Ignore flush interruption exceptions.
            }
            calculateFlushSize()
        }
    }
}
