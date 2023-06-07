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

package org.apache.kafka.common.internals

import java.util.function.Consumer
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.InvalidTopicException


object Topic {

    const val GROUP_METADATA_TOPIC_NAME = "__consumer_offsets"

    const val TRANSACTION_STATE_TOPIC_NAME = "__transaction_state"

    const val CLUSTER_METADATA_TOPIC_NAME = "__cluster_metadata"

    val CLUSTER_METADATA_TOPIC_PARTITION = TopicPartition(
        CLUSTER_METADATA_TOPIC_NAME,
        0
    )

    const val LEGAL_CHARS = "[a-zA-Z0-9._-]"

    private val INTERNAL_TOPICS = setOf(
        GROUP_METADATA_TOPIC_NAME,
        TRANSACTION_STATE_TOPIC_NAME
    )

    private const val MAX_NAME_LENGTH = 249

    fun validate(topic: String) {
        validate(
            topic, "Topic name"
        ) { message: String? ->
            throw InvalidTopicException(
                message
            )
        }
    }

    private fun detectInvalidTopic(name: String): String? {
        return if (name.isEmpty()) "the empty string is not allowed"
        else if ("." == name) "'.' is not allowed"
        else if (".." == name) "'..' is not allowed"
        else if (name.length > MAX_NAME_LENGTH)
            "the length of '$name' is longer than the max allowed length $MAX_NAME_LENGTH"
        else if (!containsValidPattern(name))
            "'$name' contains one or more characters other than ASCII alphanumerics, '.', '_' and '-'"
        else null
    }

    fun isValid(name: String): Boolean {
        val reasonInvalid = detectInvalidTopic(name)
        return reasonInvalid == null
    }

    fun validate(name: String, logPrefix: String, throwableConsumer: Consumer<String?>) {
        val reasonInvalid = detectInvalidTopic(name)
        if (reasonInvalid != null) {
            throwableConsumer.accept("$logPrefix is invalid: $reasonInvalid")
        }
    }

    fun isInternal(topic: String): Boolean = INTERNAL_TOPICS.contains(topic)

    /**
     * Due to limitations in metric names, topics with a period ('.') or underscore ('_') could collide.
     *
     * @param topic The topic to check for colliding character
     * @return true if the topic has collision characters
     */
    fun hasCollisionChars(topic: String): Boolean {
        return topic.contains("_") || topic.contains(".")
    }

    /**
     * Unify topic name with a period ('.') or underscore ('_'), this is only used to check collision and will not
     * be used to really change topic name.
     *
     * @param topic A topic to unify
     * @return A unified topic name
     */
    fun unifyCollisionChars(topic: String): String {
        return topic.replace('.', '_')
    }

    /**
     * Returns true if the topicNames collide due to a period ('.') or underscore ('_') in the same position.
     *
     * @param topicA A topic to check for collision
     * @param topicB A topic to check for collision
     * @return true if the topics collide
     */
    fun hasCollision(topicA: String, topicB: String): Boolean {
        return unifyCollisionChars(topicA) == unifyCollisionChars(topicB)
    }

    /**
     * Valid characters for Kafka topics are the ASCII alphanumerics, '.', '_', and '-'
     */
    fun containsValidPattern(topic: String): Boolean {
        topic.forEach { element ->
            // We don't use Character.isLetterOrDigit(c) because it's slower
            val validChar = element in 'a'..'z'
                        || element in '0'..'9'
                        || element in 'A'..'Z'
                        || element == '.'
                        || element == '_'
                        || element == '-'

            if (!validChar) return false
        }
        return true
    }
}
