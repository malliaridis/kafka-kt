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

import org.apache.kafka.common.errors.InvalidTopicException
import org.apache.kafka.common.internals.Topic.containsValidPattern
import org.apache.kafka.common.internals.Topic.hasCollision
import org.apache.kafka.common.internals.Topic.hasCollisionChars
import org.apache.kafka.common.internals.Topic.unifyCollisionChars
import org.apache.kafka.common.internals.Topic.validate
import org.apache.kafka.test.TestUtils.randomString
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue
import kotlin.test.fail

class TopicTest {

    @Test
    fun shouldAcceptValidTopicNames() {
        val maxLengthString = randomString(249)
        val validTopicNames = arrayOf("valid", "TOPIC", "nAmEs", "ar6", "VaL1d", "_0-9_.", "...", maxLengthString)
        for (topicName in validTopicNames) {
            validate(topicName)
        }
    }

    @Test
    fun shouldThrowOnInvalidTopicNames() {
        val longString = CharArray(250) { 'a' }
        val invalidTopicNames = arrayOf("", "foo bar", "..", "foo:bar", "foo=bar", ".", String(longString))
        for (topicName in invalidTopicNames) {
            try {
                validate(topicName)
                fail("No exception was thrown for topic with invalid name: $topicName")
            } catch (_: InvalidTopicException) {
                // Good
            }
        }
    }

    @Test
    fun shouldRecognizeInvalidCharactersInTopicNames() {
        val invalidChars =
            charArrayOf('/', '\\', ',', '\u0000', ':', '"', '\'', ';', '*', '?', ' ', '\t', '\r', '\n', '=')
        for (c in invalidChars) {
            val topicName = "Is " + c + "illegal"
            assertFalse(containsValidPattern(topicName))
        }
    }

    @Test
    fun testTopicHasCollisionChars() {
        val falseTopics = listOf("start", "end", "middle", "many")
        val trueTopics = listOf(".start", "end.", "mid.dle", ".ma.ny.", "_start", "end_", "mid_dle", "_ma_ny.")
        for (topic in falseTopics) assertFalse(hasCollisionChars(topic))
        for (topic in trueTopics) assertTrue(hasCollisionChars(topic))
    }

    @Test
    fun testUnifyCollisionChars() {
        assertEquals("topic", unifyCollisionChars("topic"))
        assertEquals("_topic", unifyCollisionChars(".topic"))
        assertEquals("_topic", unifyCollisionChars("_topic"))
        assertEquals("__topic", unifyCollisionChars("_.topic"))
    }

    @Test
    fun testTopicHasCollision() {
        val periodFirstMiddleLastNone = listOf(".topic", "to.pic", "topic.", "topic")
        val underscoreFirstMiddleLastNone = mutableListOf<String?>("_topic", "to_pic", "topic_", "topic")

        // Self
        for (topic in periodFirstMiddleLastNone) assertTrue(hasCollision(topic, topic))
        for (topic in underscoreFirstMiddleLastNone) assertTrue(hasCollision(topic!!, topic))

        // Same Position
        for (i in periodFirstMiddleLastNone.indices)
            assertTrue(hasCollision(periodFirstMiddleLastNone[i], underscoreFirstMiddleLastNone[i]!!))

        // Different Position
        underscoreFirstMiddleLastNone.reverse()
        for (i in periodFirstMiddleLastNone.indices)
            assertFalse(hasCollision(periodFirstMiddleLastNone[i], underscoreFirstMiddleLastNone[i]!!))
    }
}
