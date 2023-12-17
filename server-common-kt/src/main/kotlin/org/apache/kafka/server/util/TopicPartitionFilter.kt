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

package org.apache.kafka.server.util

import org.apache.kafka.common.TopicPartition
import org.apache.kafka.server.util.TopicFilter.IncludeList


interface TopicPartitionFilter {

    /**
     * Used to filter topics based on a certain criteria, for example, a set of topic names or a regular expression.
     */
    fun isTopicAllowed(topic: String): Boolean

    /**
     * Used to filter topic-partitions based on a certain criteria, for example, a topic pattern and a set
     * of partition ids.
     */
    fun isTopicPartitionAllowed(partition: TopicPartition): Boolean

    class TopicFilterAndPartitionFilter(
        private val topicFilter: IncludeList,
        private val partitionFilter: PartitionFilter,
    ) : TopicPartitionFilter {

        override fun isTopicAllowed(topic: String): Boolean = topicFilter.isTopicAllowed(topic, false)

        override fun isTopicPartitionAllowed(partition: TopicPartition): Boolean {
            return isTopicAllowed(partition.topic) && partitionFilter.isPartitionAllowed(partition.partition)
        }
    }

    class CompositeTopicPartitionFilter(private val filters: List<TopicPartitionFilter>) : TopicPartitionFilter {

        override fun isTopicAllowed(topic: String): Boolean = filters.any { tp -> tp.isTopicAllowed(topic) }

        override fun isTopicPartitionAllowed(partition: TopicPartition): Boolean =
            filters.any { tp -> tp.isTopicPartitionAllowed(partition) }
    }
}
