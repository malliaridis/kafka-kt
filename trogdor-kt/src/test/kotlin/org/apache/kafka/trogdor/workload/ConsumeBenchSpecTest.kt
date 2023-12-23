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

import org.apache.kafka.common.TopicPartition
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class ConsumeBenchSpecTest {
    
    @Test
    fun testMaterializeTopicsWithNoPartitions() {
        val materializedTopics: Map<String, List<TopicPartition>> = consumeBenchSpec(
            mutableListOf("topic[1-3]", "secondTopic")
        ).materializeTopics()
        val expected = mapOf<String, List<TopicPartition>>(
            "topic1" to emptyList(),
            "topic2" to emptyList(),
            "topic3" to emptyList(),
            "secondTopic" to emptyList(),
        )
        assertEquals(expected, materializedTopics)
    }

    @Test
    fun testMaterializeTopicsWithSomePartitions() {
        val materializedTopics = consumeBenchSpec(
            listOf("topic[1-3]:[1-5]", "secondTopic", "thirdTopic:1")
        ).materializeTopics()
        val expected = mapOf(
            "topic1" to (1..<6).map { i -> TopicPartition("topic1", i) },
            "topic2" to (1..<6).map { i -> TopicPartition("topic2", i) },
            "topic3" to (1..<6).map { i -> TopicPartition("topic3", i) },
            "secondTopic" to emptyList(),
            "thirdTopic" to listOf(TopicPartition("thirdTopic", 1)),
        )
        assertEquals(expected, materializedTopics)
    }

    @Test
    fun testInvalidTopicNameRaisesExceptionInMaterialize() {
        val invalidNames = listOf(
            "In:valid",
            "invalid:",
            ":invalid",
            "in:valid:1",
            "invalid:2:2",
            "invalid::1",
            "invalid[1-3]:",
        )
        for (invalidName in invalidNames) {
            assertFailsWith<IllegalArgumentException> {
                consumeBenchSpec(listOf(invalidName)).materializeTopics()
            }
        }
    }

    private fun consumeBenchSpec(activeTopics: List<String>): ConsumeBenchSpec = ConsumeBenchSpec(
        startMs = 0,
        durationMs = 0,
        consumerNode = "node",
        bootstrapServers = "localhost",
        targetMessagesPerSec = 123,
        maxMessages = 1234,
        consumerGroup = "cg-1",
        consumerConf = emptyMap(),
        commonClientConf = emptyMap(),
        adminClientConf = emptyMap(),
        threadsPerWorker = 1,
        recordProcessor = null,
        activeTopics = activeTopics,
    )
}
