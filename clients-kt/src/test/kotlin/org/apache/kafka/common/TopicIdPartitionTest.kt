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

import org.junit.jupiter.api.Test
import java.util.*
import kotlin.test.assertEquals
import kotlin.test.assertNotEquals

internal class TopicIdPartitionTest {
    
    private val topicId0 = Uuid(-4883993789924556279L, -5960309683534398572L)
    
    private val topicName0 = "a_topic_name"
    
    private val partition1 = 1
    
    private val topicPartition0 = TopicPartition(topicName0, partition1)
    
    private val topicIdPartition0 = TopicIdPartition(topicId0, topicPartition0)
    
    private val topicIdPartition1 = TopicIdPartition(topicId0, partition1, topicName0)
    
    private val topicIdPartitionWithEmptyTopic0 = TopicIdPartition(topicId0, partition1, "")
    
    private val topicIdPartitionWithEmptyTopic1 = TopicIdPartition(topicId0, TopicPartition("", partition1))
    
    private val topicId1 = Uuid(7759286116672424028L, -5081215629859775948L)
    
    private val topicName1 = "another_topic_name"
    
    private val topicIdPartition2 = TopicIdPartition(topicId1, partition1, topicName1)
    
    private val topicIdPartitionWithEmptyTopic2 = TopicIdPartition(topicId1, TopicPartition("", partition1))

    @Test
    fun testEquals() {
        assertEquals(topicIdPartition0, topicIdPartition1)
        assertEquals(topicIdPartition1, topicIdPartition0)
        assertEquals(topicIdPartitionWithEmptyTopic0, topicIdPartitionWithEmptyTopic1)
        assertNotEquals(topicIdPartition0, topicIdPartition2)
        assertNotEquals(topicIdPartition2, topicIdPartition0)
        assertNotEquals(topicIdPartition0, topicIdPartitionWithEmptyTopic0)
        assertNotEquals(topicIdPartitionWithEmptyTopic0, topicIdPartitionWithEmptyTopic2)
    }

    @Test
    fun testHashCode() {
        assertEquals(
            Objects.hash(topicIdPartition0.topicId, topicIdPartition0.topicPartition),
            topicIdPartition0.hashCode()
        )
        assertEquals(topicIdPartition0.hashCode(), topicIdPartition1.hashCode())
        assertEquals(
            Objects.hash(
                topicIdPartitionWithEmptyTopic0.topicId,
                TopicPartition("", partition1)
            ), topicIdPartitionWithEmptyTopic0.hashCode()
        )
        assertEquals(topicIdPartitionWithEmptyTopic0.hashCode(), topicIdPartitionWithEmptyTopic1.hashCode())
        assertNotEquals(topicIdPartition0.hashCode(), topicIdPartition2.hashCode())
        assertNotEquals(topicIdPartition0.hashCode(), topicIdPartitionWithEmptyTopic0.hashCode())
        assertNotEquals(topicIdPartitionWithEmptyTopic0.hashCode(), topicIdPartitionWithEmptyTopic2.hashCode())
    }

    @Test
    fun testToString() {
        assertEquals("vDiRhkpVQgmtSLnsAZx7lA:a_topic_name-1", topicIdPartition0.toString())
        assertEquals("vDiRhkpVQgmtSLnsAZx7lA:-1", topicIdPartitionWithEmptyTopic0.toString())
    }
}
