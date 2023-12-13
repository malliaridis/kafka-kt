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

import org.apache.kafka.common.TopicPartition
import java.io.Serializable

class Utils {

    internal class PartitionComparator(
        private val map: Map<String, List<String>>,
    ) : Comparator<TopicPartition>, Serializable {

        override fun compare(o1: TopicPartition, o2: TopicPartition): Int {
            var ret = map[o1.topic]!!.size - map[o2.topic]!!.size
            if (ret == 0) {
                ret = o1.topic.compareTo(o2.topic)
                if (ret == 0) ret = o1.partition - o2.partition
            }
            return ret
        }

        companion object {
            private const val serialVersionUID = 1L
        }
    }

    class TopicPartitionComparator : Comparator<TopicPartition>, Serializable {

        override fun compare(
            topicPartition1: TopicPartition,
            topicPartition2: TopicPartition,
        ): Int {
            val topic1 = topicPartition1.topic
            val topic2 = topicPartition2.topic

            return if (topic1 == topic2) topicPartition1.partition - topicPartition2.partition
            else topic1.compareTo(topic2)
        }

        companion object {
            private const val serialVersionUID = 1L
        }
    }
}
