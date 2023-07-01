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

package org.apache.kafka.clients.consumer

import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.utils.AbstractIterator

/**
 * A container that holds the list [ConsumerRecord] per partition for a particular topic. There is
 * one [ConsumerRecord] list for every topic partition returned by a [Consumer.poll] operation.
 */
class ConsumerRecords<K, V>(
    private val records: Map<TopicPartition, List<ConsumerRecord<K, V>>>,
) : Iterable<ConsumerRecord<K, V>> {

    /**
     * Get just the records for the given partition
     *
     * @param partition The partition to get records for
     */
    fun records(partition: TopicPartition): List<ConsumerRecord<K, V>> {
        return records[partition]?.toList() ?: emptyList()
    }

    /**
     * Get just the records for the given topic
     */
    fun records(topic: String): Iterable<ConsumerRecord<K, V>> = ConcatenatedIterable(
        records.filterKeys { key -> key.topic == topic }
            .map { (_, value) -> value }
    )

    /**
     * Get the partitions which have records contained in this record set.
     *
     * @return the set of partitions with data in this record set (may be empty if no data was
     * returned)
     */
    fun partitions(): Set<TopicPartition> = records.keys

    override fun iterator(): Iterator<ConsumerRecord<K, V>> =
        ConcatenatedIterable(records.values).iterator()

    /**
     * The number of records for all topics
     */
    fun count(): Int {
        var count = 0
        for (recs in records.values) count += recs.size
        return count
    }

    private class ConcatenatedIterable<K, V>(
        private val iterables: Iterable<Iterable<ConsumerRecord<K, V>>>,
    ) : Iterable<ConsumerRecord<K, V>> {

        override fun iterator(): Iterator<ConsumerRecord<K, V>> {

            return object : AbstractIterator<ConsumerRecord<K, V>>() {
                var iters = iterables.iterator()
                var current: Iterator<ConsumerRecord<K, V>>? = null

                public override fun makeNext(): ConsumerRecord<K, V>? {
                    while (current?.hasNext() != true) {
                        current =
                            if (iters.hasNext()) iters.next().iterator()
                            else return allDone()
                    }
                    return current!!.next()
                }
            }
        }
    }

    val isEmpty: Boolean
        get() = records.isEmpty()

    companion object {

        val EMPTY = ConsumerRecords<Any, Any>(emptyMap())

        fun <K, V> empty(): ConsumerRecords<K, V> = EMPTY as ConsumerRecords<K, V>
    }
}
