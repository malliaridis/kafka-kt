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

package org.apache.kafka.clients.producer.internals

import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.requests.ProduceResponse
import org.apache.kafka.common.utils.PrimitiveRef.ofInt
import org.apache.kafka.common.utils.ProducerIdAndEpoch

internal class TxnPartitionMap {

    val topicPartitions: MutableMap<TopicPartition, TxnPartitionEntry> = HashMap()

    operator fun get(topicPartition: TopicPartition): TxnPartitionEntry {
        return checkNotNull(topicPartitions[topicPartition]) {
            "Trying to get the sequence number for $topicPartition, but the sequence number " +
                    "was never set for this partition."
        }
    }

    fun getOrCreate(topicPartition: TopicPartition): TxnPartitionEntry =
        topicPartitions.computeIfAbsent(topicPartition) { TxnPartitionEntry() }

    operator fun contains(topicPartition: TopicPartition): Boolean =
        topicPartitions.containsKey(topicPartition)

    fun reset() = topicPartitions.clear()

    fun lastAckedOffset(topicPartition: TopicPartition): Long? {
        val entry = topicPartitions[topicPartition]
        return if (
            entry != null
            && entry.lastAckedOffset != ProduceResponse.INVALID_OFFSET
        ) entry.lastAckedOffset
        else null
    }

    fun lastAckedSequence(topicPartition: TopicPartition): Int? {
        val entry = topicPartitions[topicPartition]
        return if (
            entry != null
            && entry.lastAckedSequence != TransactionManager.NO_LAST_ACKED_SEQUENCE_NUMBER
        ) entry.lastAckedSequence
        else null
    }

    fun startSequencesAtBeginning(
        topicPartition: TopicPartition,
        newProducerIdAndEpoch: ProducerIdAndEpoch,
    ) {
        val sequence = ofInt(0)
        val topicPartitionEntry = get(topicPartition)
        topicPartitionEntry.resetSequenceNumbers { inFlightBatch ->
            inFlightBatch.resetProducerState(
                newProducerIdAndEpoch,
                sequence.value,
                inFlightBatch.isTransactional
            )
            sequence.value += inFlightBatch.recordCount
        }
        topicPartitionEntry.producerIdAndEpoch = newProducerIdAndEpoch
        topicPartitionEntry.nextSequence = sequence.value
        topicPartitionEntry.lastAckedSequence = TransactionManager.NO_LAST_ACKED_SEQUENCE_NUMBER
    }
}
