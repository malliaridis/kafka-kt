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
import org.apache.kafka.common.message.MetadataRequestData
import org.apache.kafka.common.message.MetadataRequestData.MetadataRequestTopic
import org.apache.kafka.common.protocol.ApiKeys
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlin.test.fail

class MetadataRequestTest {

    @Test
    fun testEmptyMeansAllTopicsV0() {
        val data = MetadataRequestData()
        val parsedRequest = MetadataRequest(data = data, version = 0)
        assertTrue(parsedRequest.isAllTopics)
        assertNull(parsedRequest.topics())
    }

    @Test
    fun testEmptyMeansEmptyForVersionsAboveV0() {
        for (i in 1 until MetadataRequestData.SCHEMAS.size) {
            val data = MetadataRequestData()
            data.setAllowAutoTopicCreation(true)
            val parsedRequest = MetadataRequest(data = data, version = i.toShort())
            assertFalse(parsedRequest.isAllTopics)
            assertEquals(emptyList(), parsedRequest.topics())
        }
    }

    @Test
    fun testMetadataRequestVersion() {
        val builder = MetadataRequest.Builder(
            topics = listOf("topic"),
            allowAutoTopicCreation = false,
        )
        assertEquals(ApiKeys.METADATA.oldestVersion(), builder.oldestAllowedVersion)
        assertEquals(ApiKeys.METADATA.latestVersion(), builder.latestAllowedVersion)
        val version: Short = 5
        val builder2 = MetadataRequest.Builder(
            topics = listOf("topic"),
            allowAutoTopicCreation = false,
            allowedVersion = version,
        )
        assertEquals(version, builder2.oldestAllowedVersion)
        assertEquals(version, builder2.latestAllowedVersion)
        val minVersion: Short = 1
        val maxVersion: Short = 6
        val builder3 = MetadataRequest.Builder(
            topics = listOf("topic"),
            allowAutoTopicCreation = false,
            minVersion = minVersion,
            maxVersion = maxVersion,
        )
        assertEquals(minVersion, builder3.oldestAllowedVersion)
        assertEquals(maxVersion, builder3.latestAllowedVersion)
    }

    @Test
    fun testTopicIdAndNullTopicNameRequests() {
        // Construct invalid MetadataRequestTopics. We will build each one separately and ensure the error is thrown.
        val topics = listOf(
            MetadataRequestTopic().setName(null).setTopicId(Uuid.randomUuid()),
            MetadataRequestTopic().setName(null),
            MetadataRequestTopic().setTopicId(Uuid.randomUuid()),
            MetadataRequestTopic().setName("topic").setTopicId(Uuid.randomUuid())
        )

        // if version is 10 or 11, the invalid topic metadata should return an error
        val invalidVersions = listOf<Short>(10, 11)
        invalidVersions.forEach { version ->
            topics.forEach { topic ->
                val metadataRequestData = MetadataRequestData().setTopics(listOf(topic))
                val builder = MetadataRequest.Builder(metadataRequestData)
                assertFailsWith<UnsupportedVersionException> { builder.build(version = version) }
            }
        }
    }

    @Test
    fun testTopicIdWithZeroUuid() {
        val topics = listOf(
            MetadataRequestTopic().setName("topic").setTopicId(Uuid.ZERO_UUID),
            MetadataRequestTopic().setName("topic")
                .setTopicId(Uuid(mostSignificantBits = 0L, leastSignificantBits = 0L)),
            MetadataRequestTopic().setName("topic")
        )
        val invalidVersions = listOf<Short>(10, 11)
        invalidVersions.forEach { version ->
            topics.forEach { topic ->
                val metadataRequestData = MetadataRequestData().setTopics(listOf(topic))
                val builder = MetadataRequest.Builder(metadataRequestData)
                try {
                    builder.build(version)
                } catch (_: Exception) {
                    fail("Expected not to throw exception.")
                }
            }
        }
    }
}
