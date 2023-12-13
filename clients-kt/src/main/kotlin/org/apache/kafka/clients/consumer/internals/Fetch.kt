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

package org.apache.kafka.clients.consumer.internals

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.utils.Utils.mkEntry
import org.apache.kafka.common.utils.Utils.mkMap

class Fetch<K, V> private constructor(
    private val records: MutableMap<TopicPartition, List<ConsumerRecord<K, V>>>,
    private var positionAdvanced: Boolean,
    numRecords: Int,
) {

    var numRecords: Int = numRecords
        private set

    /**
     * Add another [Fetch] to this one; all of its records will be added to this fetch's [records],
     * and if the other fetch [advanced the consume position for any topicpartition]
     * [positionAdvanced], this fetch will be marked as having advanced the consume position as
     * well.
     *
     * @param fetch the other fetch to add; may not be null
     */
    fun add(fetch: Fetch<K, V>) {
        addRecords(fetch.records)
        positionAdvanced = positionAdvanced or fetch.positionAdvanced
    }

    /**
     * @return all of the non-control messages for this fetch, grouped by partition
     */
    fun records(): Map<TopicPartition, List<ConsumerRecord<K, V>>> = records.toMap()

    /**
     * @return whether the fetch caused the consumer's
     * [position][org.apache.kafka.clients.consumer.KafkaConsumer.position] to advance for at
     * least one of the topic partitions in this fetch
     */
    fun positionAdvanced(): Boolean = positionAdvanced

    /**
     * @return the total number of non-control messages for this fetch, across all partitions
     */
    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("numRecords"),
    )
    fun numRecords(): Int = numRecords

    /**
     * `true` if and only if this fetch did not return any user-visible (i.e., non-control) records, and
     * did not cause the consumer position to advance for any topic partitions
     */
    val isEmpty: Boolean
        get() = numRecords == 0 && !positionAdvanced

    private fun addRecords(records: Map<TopicPartition, List<ConsumerRecord<K, V>>>) {
        records.forEach { (partition, partRecords) ->
            numRecords += partRecords.size
            val currentRecords = this.records[partition]
            if (currentRecords == null) {
                this.records[partition] = partRecords
            } else {
                // this case shouldn't usually happen because we only send one fetch at a time per partition,
                // but it might conceivably happen in some rare cases (such as partition leader changes).
                // we have to copy to a new list because the old one may be immutable
                this.records[partition] = currentRecords + partRecords
            }
        }
    }

    companion object {
        fun <K, V> empty(): Fetch<K, V> = Fetch(
            records = HashMap(),
            positionAdvanced = false,
            numRecords = 0
        )

        fun <K, V> forPartition(
            partition: TopicPartition,
            records: List<ConsumerRecord<K, V>>,
            positionAdvanced: Boolean,
        ): Fetch<K, V> {
            val recordsMap =
                if (records.isEmpty()) mutableMapOf()
                else mutableMapOf(partition to records)

            return Fetch(
                records = recordsMap,
                positionAdvanced = positionAdvanced,
                numRecords = records.size,
            )
        }
    }
}
