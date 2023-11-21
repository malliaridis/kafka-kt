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

package org.apache.kafka.common.requests

import org.apache.kafka.common.Uuid
import org.apache.kafka.common.errors.UnsupportedVersionException
import org.apache.kafka.common.message.DeleteTopicsRequestData
import org.apache.kafka.common.message.DeleteTopicsRequestData.DeleteTopicState
import org.apache.kafka.common.protocol.ApiKeys
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull

class DeleteTopicsRequestTest {
    
    @Test
    fun testTopicNormalization() {
        for (version in ApiKeys.DELETE_TOPICS.allVersions()) {
            // Check topic names are in the correct place when using topicNames.
            val topic1 = "topic1"
            val topic2 = "topic2"
            val topics = listOf(topic1, topic2)
            val requestWithNames = DeleteTopicsRequest.Builder(DeleteTopicsRequestData().setTopicNames(topics))
                .build(version)
            val requestWithNamesSerialized = DeleteTopicsRequest.parse(requestWithNames.serialize(), version)
            assertEquals(topics, requestWithNames.topicNames())
            assertEquals(topics, requestWithNamesSerialized.topicNames())
            if (version < 6) {
                assertEquals(topics, requestWithNames.data().topicNames)
                assertEquals(topics, requestWithNamesSerialized.data().topicNames)
            } else {
                // topics in TopicNames are moved to new topics field
                assertEquals(topics, requestWithNames.data().topics.map(DeleteTopicState::name))
                assertEquals(topics, requestWithNamesSerialized.data().topics.map(DeleteTopicState::name))
            }
        }
    }

    @Test
    fun testNewTopicsField() {
        for (version in ApiKeys.DELETE_TOPICS.allVersions()) {
            val topic1 = "topic1"
            val topic2 = "topic2"
            val topics = listOf(topic1, topic2)
            val requestWithNames = DeleteTopicsRequest.Builder(
                DeleteTopicsRequestData().setTopics(
                    listOf(
                        DeleteTopicState().setName(topic1),
                        DeleteTopicState().setName(topic2),
                    )
                )
            ).build(version)
            // Ensure we only use new topics field on versions 6+.
            if (version >= 6) {
                val requestWithNamesSerialized = DeleteTopicsRequest.parse(requestWithNames.serialize(), version)
                assertEquals(topics, requestWithNames.topicNames())
                assertEquals(topics, requestWithNamesSerialized.topicNames())
            } else {
                // We should fail if version is less than 6.
                assertFailsWith<UnsupportedVersionException> { requestWithNames.serialize() }
            }
        }
    }

    @Test
    fun testTopicIdsField() {
        for (version in ApiKeys.DELETE_TOPICS.allVersions()) {
            // Check topic IDs are handled correctly. We should only use this field on versions 6+.
            val topicId1 = Uuid.randomUuid()
            val topicId2 = Uuid.randomUuid()
            val topicIds = listOf(topicId1, topicId2)
            val requestWithIds = DeleteTopicsRequest.Builder(
                DeleteTopicsRequestData().setTopics(
                    listOf(
                        DeleteTopicState().setTopicId(topicId1),
                        DeleteTopicState().setTopicId(topicId2),
                    )
                )
            ).build(version)
            if (version >= 6) {
                val requestWithIdsSerialized = DeleteTopicsRequest.parse(requestWithIds.serialize(), version)
                assertEquals(topicIds, requestWithIds.topicIds())
                assertEquals(topicIds, requestWithIdsSerialized.topicIds())

                // All topic names should be replaced with null
                requestWithIds.data().topics.forEach { topic -> assertNull(topic.name) }
                requestWithIdsSerialized.data().topics.forEach { topic ->
                    assertNull(topic.name)
                }
            } else {
                // We should fail if version is less than 6.
                assertFailsWith<UnsupportedVersionException> { requestWithIds.serialize() }
            }
        }
    }

    @Test
    fun testDeleteTopicsRequestNumTopics() {
        for (version in ApiKeys.DELETE_TOPICS.allVersions()) {
            val request = DeleteTopicsRequest.Builder(
                DeleteTopicsRequestData()
                    .setTopicNames(mutableListOf("topic1", "topic2"))
                    .setTimeoutMs(1000)
            ).build(version)
            val serializedRequest = DeleteTopicsRequest.parse(request.serialize(), version)
            // createDeleteTopicsRequest sets 2 topics
            assertEquals(2, request.numberOfTopics())
            assertEquals(2, serializedRequest.numberOfTopics())

            // Test using IDs
            if (version >= 6) {
                val requestWithIds = DeleteTopicsRequest.Builder(
                    DeleteTopicsRequestData().setTopics(
                        listOf(
                            DeleteTopicState().setTopicId(Uuid.randomUuid()),
                            DeleteTopicState().setTopicId(Uuid.randomUuid()),
                        )
                    )
                ).build(version)
                val serializedRequestWithIds = DeleteTopicsRequest.parse(requestWithIds.serialize(), version)
                assertEquals(2, requestWithIds.numberOfTopics())
                assertEquals(2, serializedRequestWithIds.numberOfTopics())
            }
        }
    }
}
