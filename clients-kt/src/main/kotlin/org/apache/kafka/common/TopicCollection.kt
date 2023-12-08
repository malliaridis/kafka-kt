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

package org.apache.kafka.common

/**
 * A class used to represent a collection of topics. This collection may define topics by name or
 * ID.
 */
sealed class TopicCollection {

    /**
     * A class used to represent a collection of topics defined by their topic ID.
     * Subclassing this class beyond the classes provided here is not supported.
     */
    data class TopicIdCollection internal constructor(
        val topicIds: List<Uuid> = emptyList()
    ) : TopicCollection() {

        /**
         * @return A collection of topic IDs
         */
        @Deprecated(
            message = "Use property instead.",
            replaceWith = ReplaceWith("topicIds")
        )
        fun topicIds(): List<Uuid> {
            return topicIds
        }
    }

    /**
     * A class used to represent a collection of topics defined by their topic name.
     * Subclassing this class beyond the classes provided here is not supported.
     */
    data class TopicNameCollection internal constructor(
        val topicNames: List<String>
    ) : TopicCollection() {

        /**
         * @return A collection of topic names
         */
        @Deprecated(
            message = "Use property instead.",
            replaceWith = ReplaceWith("topicNames"),
        )
        fun topicNames(): Collection<String> {
            return topicNames
        }
    }

    companion object {
        /**
         * @return a collection of topics defined by topic ID
         */
        @Deprecated("Use function with List param.")
        fun ofTopicIds(topics: Collection<Uuid>): TopicIdCollection {
            return TopicIdCollection(topics.toList())
        }

        /**
         * @return a collection of topics defined by topic ID
         */
        fun ofTopicIds(topics: List<Uuid>): TopicIdCollection {
            return TopicIdCollection(topics.toList())
        }

        /**
         * @return a collection of topics defined by topic name
         */
        fun ofTopicNames(topics: Collection<String>): TopicNameCollection {
            return TopicNameCollection(topics.toList())
        }
    }
}
