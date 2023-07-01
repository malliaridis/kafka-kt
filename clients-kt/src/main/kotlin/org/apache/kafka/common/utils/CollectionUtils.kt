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

import java.util.function.BiConsumer
import java.util.function.Function
import java.util.stream.Collectors
import org.apache.kafka.common.TopicPartition

object CollectionUtils {

    /**
     * Given two maps (A, B), returns all the key-value pairs in A whose keys are not contained in B
     */
    @Deprecated("Use Kotlin operator '-' with Map.keys")
    fun <K, V> subtractMap(minuend: Map<out K, V>, subtrahend: Map<out K, V?>): Map<K, V> =
        minuend - subtrahend.keys

    /**
     * group data by topic
     *
     * @param data Data to be partitioned
     * @param T Partition data type
     * @return partitioned data
     */
    fun <T> groupPartitionDataByTopic(
        data: Map<TopicPartition, T>,
    ): Map<String, MutableMap<Int, T>> {
        val dataByTopic: MutableMap<String, MutableMap<Int, T>> = HashMap()

        data.forEach { (key, value) ->
            val topic = key.topic
            val partition = key.partition
            val topicData = dataByTopic.computeIfAbsent(topic) { HashMap() }
            topicData[partition] = value
        }

        return dataByTopic
    }

    /**
     * Group a list of partitions by the topic name.
     *
     * @param partitions The partitions to collect
     * @return partitions per topic
     */
    fun groupPartitionsByTopic(
        partitions: Collection<TopicPartition>,
    ): Map<String, List<Int>> {
        return groupPartitionsByTopic<MutableList<Int>>(
            partitions = partitions,
            buildGroup = { ArrayList() },
            addToGroup = { obj, e -> obj.add(e) }
        )
    }

    /**
     * Group a collection of partitions by topic
     *
     * @return The map used to group the partitions
     */
    fun <T> groupPartitionsByTopic(
        partitions: Collection<TopicPartition>,
        buildGroup: Function<String, T>,
        addToGroup: BiConsumer<T, Int>
    ): Map<String, T> {
        val dataByTopic: MutableMap<String, T> = HashMap()
        for ((topic, partition) in partitions) {
            val topicData = dataByTopic.computeIfAbsent(topic, buildGroup)
            addToGroup.accept(topicData, partition)
        }
        return dataByTopic
    }
}
