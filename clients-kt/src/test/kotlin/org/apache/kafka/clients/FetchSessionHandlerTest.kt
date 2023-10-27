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

package org.apache.kafka.clients

import java.util.*
import java.util.function.Consumer
import java.util.stream.Stream
import org.apache.kafka.clients.FetchSessionHandler.FetchRequestData
import org.apache.kafka.common.TopicIdPartition
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.Uuid
import org.apache.kafka.common.message.FetchResponseData
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.FetchMetadata
import org.apache.kafka.common.requests.FetchRequest
import org.apache.kafka.common.requests.FetchResponse
import org.apache.kafka.common.utils.LogContext
import org.junit.jupiter.api.Timeout
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import org.junit.jupiter.params.provider.ValueSource
import kotlin.collections.HashMap
import kotlin.collections.LinkedHashMap
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlin.test.fail

/**
 * A unit test for FetchSessionHandler.
 */
@Timeout(120)
class FetchSessionHandlerTest {

    @Test
    fun testFindMissing() {
        val foo0 = TopicPartition(topic = "foo", partition = 0)
        val foo1 = TopicPartition(topic = "foo", partition = 1)
        val bar0 = TopicPartition(topic = "bar", partition = 0)
        val bar1 = TopicPartition(topic = "bar", partition = 1)
        val baz0 = TopicPartition(topic = "baz", partition = 0)
        val baz1 = TopicPartition(topic = "baz", partition = 1)
        assertEquals(
            expected = toSet(),
            actual = FetchSessionHandler.findMissing(
                toFind = toSet(foo0),
                toSearch = toSet(foo0),
            ),
        )
        assertEquals(
            expected = toSet(foo0),
            actual = FetchSessionHandler.findMissing(
                toFind = toSet(foo0),
                toSearch = toSet(foo1),
            ),
        )
        assertEquals(
            expected = toSet(foo0, foo1),
            actual = FetchSessionHandler.findMissing(
                toFind = toSet(foo0, foo1),
                toSearch = toSet(baz0),
            ),
        )
        assertEquals(
            expected = toSet(bar1, foo0, foo1),
            actual = FetchSessionHandler.findMissing(
                toFind = toSet(foo0, foo1, bar0, bar1),
                toSearch = toSet(bar0, baz0, baz1),
            )
        )
        assertEquals(
            expected = toSet(),
            actual = FetchSessionHandler.findMissing(
                toFind = toSet(foo0, foo1, bar0, bar1, baz1),
                toSearch = toSet(foo0, foo1, bar0, bar1, baz0, baz1),
            )
        )
    }

    private class ReqEntry(
        topic: String,
        topicId: Uuid,
        partition: Int,
        fetchOffset: Long,
        logStartOffset: Long,
        maxBytes: Int,
    ) {

        val part: TopicPartition = TopicPartition(topic, partition)

        val data: FetchRequest.PartitionData = FetchRequest.PartitionData(
            topicId = topicId,
            fetchOffset = fetchOffset,
            logStartOffset = logStartOffset,
            maxBytes = maxBytes,
            currentLeaderEpoch = null,
        )
    }

    private class RespEntry {

        val part: TopicIdPartition

        val data: FetchResponseData.PartitionData

        constructor(
            topic: String,
            partition: Int,
            topicId: Uuid,
            highWatermark: Long,
            lastStableOffset: Long,
        ) {
            part = TopicIdPartition(topicId, TopicPartition(topic, partition))
            data = FetchResponseData.PartitionData()
                .setPartitionIndex(partition)
                .setHighWatermark(highWatermark)
                .setLastStableOffset(lastStableOffset)
                .setLogStartOffset(0)
        }

        constructor(
            topic: String,
            partition: Int,
            topicId: Uuid,
            error: Errors,
        ) {
            part = TopicIdPartition(topicId, TopicPartition(topic, partition))
            data = FetchResponseData.PartitionData()
                .setPartitionIndex(partition)
                .setErrorCode(error.code)
                .setHighWatermark(FetchResponse.INVALID_HIGH_WATERMARK)
        }
    }

    /**
     * Test the handling of SESSIONLESS responses.
     * Pre-KIP-227 brokers always supply this kind of response.
     */
    @Test
    fun testSessionless() {
        val topicIds = mutableMapOf<String, Uuid>()
        val topicNames = mutableMapOf<Uuid, String>()
        // We want to test both on older versions that do not use topic IDs and on newer versions that do.
        val versions = listOf(12.toShort(), ApiKeys.FETCH.latestVersion())
        versions.forEach(Consumer<Short> { version: Short ->
            val handler = FetchSessionHandler(LOG_CONTEXT, 1)
            val builder = handler.newBuilder()
            addTopicId(topicIds, topicNames, "foo", version)
            val fooId = topicIds.getOrDefault("foo", Uuid.ZERO_UUID)
            builder.add(
                topicPartition = TopicPartition(topic = "foo", partition = 0),
                data = FetchRequest.PartitionData(
                    topicId = fooId,
                    fetchOffset = 0,
                    logStartOffset = 100,
                    maxBytes = 200,
                    currentLeaderEpoch = null,
                ),
            )
            builder.add(
                topicPartition = TopicPartition(topic = "foo", partition = 1),
                data = FetchRequest.PartitionData(
                    topicId = fooId,
                    fetchOffset = 10,
                    logStartOffset = 110,
                    maxBytes = 210,
                    currentLeaderEpoch = null,
                ),
            )
            val data: FetchRequestData = builder.build()
            assertMapsEqual(
                reqMap(
                    ReqEntry(
                        topic = "foo",
                        topicId = fooId,
                        partition = 0,
                        fetchOffset = 0,
                        logStartOffset = 100,
                        maxBytes = 200,
                    ),
                    ReqEntry(
                        topic = "foo",
                        topicId = fooId,
                        partition = 1,
                        fetchOffset = 10,
                        logStartOffset = 110,
                        maxBytes = 210,
                    ),
                ),
                data.toSend,
                data.sessionPartitions,
            )
            assertEquals(
                expected = FetchMetadata.INVALID_SESSION_ID,
                actual = data.metadata.sessionId,
            )
            assertEquals(
                expected = FetchMetadata.INITIAL_EPOCH,
                actual = data.metadata.epoch,
            )
            val resp: FetchResponse = FetchResponse.of(
                error = Errors.NONE,
                throttleTimeMs = 0,
                sessionId = FetchMetadata.INVALID_SESSION_ID,
                responseData = respMap(
                    RespEntry(
                        topic = "foo",
                        partition = 0,
                        topicId = fooId,
                        highWatermark = 0,
                        lastStableOffset = 0,
                    ),
                    RespEntry(
                        topic = "foo",
                        partition = 1,
                        topicId = fooId,
                        highWatermark = 0,
                        lastStableOffset = 0,
                    ),
                ),
            )
            handler.handleResponse(resp, version)
            val builder2 = handler.newBuilder()
            builder2.add(
                TopicPartition(topic = "foo", partition = 0),
                FetchRequest.PartitionData(
                    topicId = fooId,
                    fetchOffset = 0,
                    logStartOffset = 100,
                    maxBytes = 200,
                    currentLeaderEpoch = null,
                ),
            )
            val data2: FetchRequestData = builder2.build()
            assertEquals(
                expected = FetchMetadata.INVALID_SESSION_ID,
                actual = data2.metadata.sessionId,
            )
            assertEquals(
                expected = FetchMetadata.INITIAL_EPOCH,
                actual = data2.metadata.epoch,
            )
            assertMapsEqual(
                reqMap(
                    ReqEntry(
                        topic = "foo",
                        topicId = fooId,
                        partition = 0,
                        fetchOffset = 0,
                        logStartOffset = 100,
                        maxBytes = 200,
                    ),
                ),
                data2.toSend,
                data2.sessionPartitions,
            )
        })
    }

    /**
     * Test handling an incremental fetch session.
     */
    @Test
    fun testIncrementals() {
        val topicIds = mutableMapOf<String, Uuid>()
        val topicNames = mutableMapOf<Uuid, String>()
        // We want to test both on older versions that do not use topic IDs and on newer versions that do.
        val versions = listOf(12.toShort(), ApiKeys.FETCH.latestVersion())
        versions.forEach { version ->
            val handler = FetchSessionHandler(LOG_CONTEXT, 1)
            val builder = handler.newBuilder()
            addTopicId(topicIds, topicNames, "foo", version)
            val fooId = topicIds.getOrDefault("foo", Uuid.ZERO_UUID)
            val foo0 = TopicPartition(topic = "foo", partition = 0)
            val foo1 = TopicPartition(topic = "foo", partition = 1)
            builder.add(
                topicPartition = foo0,
                data = FetchRequest.PartitionData(
                    topicId = fooId,
                    fetchOffset = 0,
                    logStartOffset = 100,
                    maxBytes = 200,
                    currentLeaderEpoch = null,
                ),
            )
            builder.add(
                topicPartition = foo1,
                data = FetchRequest.PartitionData(
                    topicId = fooId,
                    fetchOffset = 10,
                    logStartOffset = 110,
                    maxBytes = 210,
                    currentLeaderEpoch = null,
                ),
            )
            val data = builder.build()
            assertMapsEqual(
                reqMap(
                    ReqEntry(
                        topic = "foo",
                        topicId = fooId,
                        partition = 0,
                        fetchOffset = 0,
                        logStartOffset = 100,
                        maxBytes = 200,
                    ),
                    ReqEntry(
                        topic = "foo",
                        topicId = fooId,
                        partition = 1,
                        fetchOffset = 10,
                        logStartOffset = 110,
                        maxBytes = 210,
                    ),
                ),
                data.toSend,
                data.sessionPartitions,
            )
            assertEquals(
                expected = FetchMetadata.INVALID_SESSION_ID,
                actual = data.metadata.sessionId,
            )
            assertEquals(
                FetchMetadata.INITIAL_EPOCH,
                data.metadata.epoch,
            )
            val resp: FetchResponse = FetchResponse.of(
                error = Errors.NONE,
                throttleTimeMs = 0,
                sessionId = 123,
                responseData = respMap(
                    RespEntry(
                        topic = "foo",
                        partition = 0,
                        topicId = fooId,
                        highWatermark = 10,
                        lastStableOffset = 20,
                    ),
                    RespEntry(
                        topic = "foo",
                        partition = 1,
                        topicId = fooId,
                        highWatermark = 10,
                        lastStableOffset = 20,
                    ),
                )
            )
            handler.handleResponse(resp, version)

            // Test an incremental fetch request which adds one partition and modifies another.
            val builder2 = handler.newBuilder()
            addTopicId(topicIds, topicNames, "bar", version)
            val barId = topicIds.getOrDefault("bar", Uuid.ZERO_UUID)
            val bar0 = TopicPartition(topic = "bar", partition = 0)
            builder2.add(
                topicPartition = foo0,
                data = FetchRequest.PartitionData(
                    topicId = fooId,
                    fetchOffset = 0,
                    logStartOffset = 100,
                    maxBytes = 200,
                    currentLeaderEpoch = null,
                ),
            )
            builder2.add(
                topicPartition = foo1,
                data = FetchRequest.PartitionData(
                    topicId = fooId,
                    fetchOffset = 10,
                    logStartOffset = 120,
                    maxBytes = 210,
                    currentLeaderEpoch = null,
                ),
            )
            builder2.add(
                topicPartition = bar0,
                data = FetchRequest.PartitionData(
                    topicId = barId,
                    fetchOffset = 20,
                    logStartOffset = 200,
                    maxBytes = 200,
                    currentLeaderEpoch = null,
                ),
            )
            val data2 = builder2.build()
            assertFalse(data2.metadata.isFull)
            assertMapEquals(
                expected = reqMap(
                    ReqEntry(
                        topic = "foo",
                        topicId = fooId,
                        partition = 0,
                        fetchOffset = 0,
                        logStartOffset = 100,
                        maxBytes = 200,
                    ),
                    ReqEntry(
                        topic = "foo",
                        topicId = fooId,
                        partition = 1,
                        fetchOffset = 10,
                        logStartOffset = 120,
                        maxBytes = 210,
                    ),
                    ReqEntry(
                        topic = "bar",
                        topicId = barId,
                        partition = 0,
                        fetchOffset = 20,
                        logStartOffset = 200,
                        maxBytes = 200,
                    ),
                ),
                actual = data2.sessionPartitions,
            )
            assertMapEquals(
                expected = reqMap(
                    ReqEntry(
                        topic = "bar",
                        topicId = barId,
                        partition = 0,
                        fetchOffset = 20,
                        logStartOffset = 200,
                        maxBytes = 200,
                    ),
                    ReqEntry(
                        topic = "foo",
                        topicId = fooId,
                        partition = 1,
                        fetchOffset = 10,
                        logStartOffset = 120,
                        maxBytes = 210,
                    ),
                ),
                actual = data2.toSend,
            )
            val resp2 = FetchResponse.of(
                error = Errors.NONE,
                throttleTimeMs = 0,
                sessionId = 123,
                responseData = respMap(
                    RespEntry(
                        topic = "foo",
                        partition = 1,
                        topicId = fooId,
                        highWatermark = 20,
                        lastStableOffset = 20,
                    ),
                ),
            )
            handler.handleResponse(resp2, version)

            // Skip building a new request.  Test that handling an invalid fetch session epoch
            // response results in a request which closes the session.
            val resp3 = FetchResponse.of(
                error = Errors.INVALID_FETCH_SESSION_EPOCH,
                throttleTimeMs = 0,
                sessionId = FetchMetadata.INVALID_SESSION_ID,
                responseData = respMap(),
            )
            handler.handleResponse(resp3, version)
            val builder4 = handler.newBuilder()
            builder4.add(
                topicPartition = foo0,
                data = FetchRequest.PartitionData(
                    topicId = fooId,
                    fetchOffset = 0,
                    logStartOffset = 100,
                    maxBytes = 200,
                    currentLeaderEpoch = null,
                ),
            )
            builder4.add(
                topicPartition = foo1,
                data = FetchRequest.PartitionData(
                    topicId = fooId,
                    fetchOffset = 10,
                    logStartOffset = 120,
                    maxBytes = 210,
                    currentLeaderEpoch = null,
                ),
            )
            builder4.add(
                topicPartition = bar0,
                data = FetchRequest.PartitionData(
                    topicId = barId,
                    fetchOffset = 20,
                    logStartOffset = 200,
                    maxBytes = 200,
                    currentLeaderEpoch = null,
                ),
            )
            val data4 = builder4.build()
            assertTrue(data4.metadata.isFull)
            assertEquals(
                expected = data2.metadata.sessionId,
                actual = data4.metadata.sessionId,
            )
            assertEquals(
                expected = FetchMetadata.INITIAL_EPOCH,
                actual = data4.metadata.epoch,
            )
            assertMapsEqual(
                reqMap(
                    ReqEntry(
                        topic = "foo",
                        topicId = fooId,
                        partition = 0,
                        fetchOffset = 0,
                        logStartOffset = 100,
                        maxBytes = 200,
                    ),
                    ReqEntry(
                        topic = "foo",
                        topicId = fooId,
                        partition = 1,
                        fetchOffset = 10,
                        logStartOffset = 120,
                        maxBytes = 210,
                    ),
                    ReqEntry(
                        topic = "bar",
                        topicId = barId,
                        partition = 0,
                        fetchOffset = 20,
                        logStartOffset = 200,
                        maxBytes = 200,
                    ),
                ),
                data4.sessionPartitions,
                data4.toSend,
            )
        }
    }

    /**
     * Test that calling FetchSessionHandler#Builder#build twice fails.
     */
    @Test
    fun testDoubleBuild() {
        val handler = FetchSessionHandler(logContext = LOG_CONTEXT, node = 1)
        val builder = handler.newBuilder()
        builder.add(
            topicPartition = TopicPartition(topic = "foo", partition = 0),
            data = FetchRequest.PartitionData(
                topicId = Uuid.randomUuid(),
                fetchOffset = 0,
                logStartOffset = 100,
                maxBytes = 200,
                currentLeaderEpoch = null,
            ),
        )
        builder.build()
        try {
            builder.build()
            fail("Expected calling build twice to fail.")
        } catch (t: Throwable) {
            // expected
        }
    }

    @Test
    fun testIncrementalPartitionRemoval() {
        val topicIds = mutableMapOf<String, Uuid>()
        val topicNames = mutableMapOf<Uuid, String>()
        // We want to test both on older versions that do not use topic IDs and on
        // newer versions that do.
        val versions = listOf(12.toShort(), ApiKeys.FETCH.latestVersion())
        versions.forEach { version ->
            val handler = FetchSessionHandler(LOG_CONTEXT, 1)
            val builder: FetchSessionHandler.Builder = handler.newBuilder()
            addTopicId(
                topicIds = topicIds,
                topicNames = topicNames,
                name = "foo",
                version = version,
            )
            addTopicId(
                topicIds = topicIds,
                topicNames = topicNames,
                name = "bar",
                version = version,
            )
            val fooId = topicIds.getOrDefault("foo", Uuid.ZERO_UUID)
            val barId = topicIds.getOrDefault("bar", Uuid.ZERO_UUID)
            val foo0 = TopicPartition(topic = "foo", partition = 0)
            val foo1 = TopicPartition(topic = "foo", partition = 1)
            val bar0 = TopicPartition(topic = "bar", partition = 0)
            builder.add(
                topicPartition = foo0,
                data = FetchRequest.PartitionData(
                    topicId = fooId,
                    fetchOffset = 0,
                    logStartOffset = 100,
                    maxBytes = 200,
                    currentLeaderEpoch = null,
                ),
            )
            builder.add(
                topicPartition = foo1,
                data = FetchRequest.PartitionData(
                    topicId = fooId,
                    fetchOffset = 10,
                    logStartOffset = 110,
                    maxBytes = 210,
                    currentLeaderEpoch = null,
                ),
            )
            builder.add(
                topicPartition = bar0,
                data = FetchRequest.PartitionData(
                    topicId = barId,
                    fetchOffset = 20,
                    logStartOffset = 120,
                    maxBytes = 220,
                    currentLeaderEpoch = null,
                ),
            )
            val data = builder.build()
            assertMapsEqual(
                reqMap(
                    ReqEntry(
                        topic = "foo",
                        topicId = fooId,
                        partition = 0,
                        fetchOffset = 0,
                        logStartOffset = 100,
                        maxBytes = 200,
                    ),
                    ReqEntry(
                        topic = "foo",
                        topicId = fooId,
                        partition = 1,
                        fetchOffset = 10,
                        logStartOffset = 110,
                        maxBytes = 210,
                    ),
                    ReqEntry(
                        topic = "bar",
                        topicId = barId,
                        partition = 0,
                        fetchOffset = 20,
                        logStartOffset = 120,
                        maxBytes = 220,
                    ),
                ),
                data.toSend,
                data.sessionPartitions,
            )
            assertTrue(data.metadata.isFull)
            val resp: FetchResponse = FetchResponse.of(
                error = Errors.NONE,
                throttleTimeMs = 0,
                sessionId = 123,
                responseData = respMap(
                    RespEntry(
                        topic = "foo",
                        partition = 0,
                        topicId = fooId,
                        highWatermark = 10,
                        lastStableOffset = 20,
                    ),
                    RespEntry(
                        topic = "foo",
                        partition = 1,
                        topicId = fooId,
                        highWatermark = 10,
                        lastStableOffset = 20,
                    ),
                    RespEntry(
                        topic = "bar",
                        partition = 0,
                        topicId = barId,
                        highWatermark = 10,
                        lastStableOffset = 20,
                    ),
                ),
            )
            handler.handleResponse(resp, version)

            // Test an incremental fetch request which removes two partitions.
            val builder2 = handler.newBuilder()
            builder2.add(
                topicPartition = foo1,
                data = FetchRequest.PartitionData(
                    topicId = fooId,
                    fetchOffset = 10,
                    logStartOffset = 110,
                    maxBytes = 210,
                    currentLeaderEpoch = null,
                ),
            )
            val data2 = builder2.build()
            assertFalse(data2.metadata.isFull)
            assertEquals(expected = 123, actual = data2.metadata.sessionId)
            assertEquals(expected = 1, actual = data2.metadata.epoch)
            assertMapEquals(
                expected = reqMap(
                    ReqEntry(
                        topic = "foo",
                        topicId = fooId,
                        partition = 1,
                        fetchOffset = 10,
                        logStartOffset = 110,
                        maxBytes = 210,
                    ),
                ),
                actual = data2.sessionPartitions,
            )
            assertMapEquals(
                expected = reqMap(),
                actual = data2.toSend,
            )
            val expectedToForget2 = listOf(
                TopicIdPartition(topicId = fooId, topicPartition = foo0),
                TopicIdPartition(topicId = barId, topicPartition = bar0)
            )
            assertListEquals(expected = expectedToForget2, actual = data2.toForget)

            // A FETCH_SESSION_ID_NOT_FOUND response triggers us to close the session.
            // The next request is a session establishing FULL request.
            val resp2 = FetchResponse.of(
                error = Errors.FETCH_SESSION_ID_NOT_FOUND,
                throttleTimeMs = 0,
                sessionId = FetchMetadata.INVALID_SESSION_ID,
                responseData = respMap(),
            )
            handler.handleResponse(resp2, version)
            val builder3 = handler.newBuilder()
            builder3.add(
                topicPartition = foo0,
                data = FetchRequest.PartitionData(
                    topicId = fooId,
                    fetchOffset = 0,
                    logStartOffset = 100,
                    maxBytes = 200,
                    currentLeaderEpoch = null,
                ),
            )
            val data3 = builder3.build()
            assertTrue(data3.metadata.isFull)
            assertEquals(
                expected = FetchMetadata.INVALID_SESSION_ID,
                actual = data3.metadata.sessionId,
            )
            assertEquals(
                expected = FetchMetadata.INITIAL_EPOCH,
                actual = data3.metadata.epoch,
            )
            assertMapsEqual(
                reqMap(
                    ReqEntry(
                        topic = "foo",
                        topicId = fooId,
                        partition = 0,
                        fetchOffset = 0,
                        logStartOffset = 100,
                        maxBytes = 200,
                    ),
                ),
                data3.sessionPartitions,
                data3.toSend,
            )
        }
    }

    @Test
    fun testTopicIdUsageGrantedOnIdUpgrade() {
        // We want to test adding a topic ID to an existing partition and a new partition
        // in the incremental request. 0 is the existing partition and 1 is the new one.
        val partitions = listOf(0, 1)
        partitions.forEach { partition ->
            val testType = if (partition == 0) "updating a partition" else "adding a new partition"
            val handler = FetchSessionHandler(LOG_CONTEXT, 1)
            val builder = handler.newBuilder()
            builder.add(
                topicPartition = TopicPartition(topic = "foo", partition = 0),
                data = FetchRequest.PartitionData(
                    topicId = Uuid.ZERO_UUID,
                    fetchOffset = 0,
                    logStartOffset = 100,
                    maxBytes = 200,
                    currentLeaderEpoch = null,
                ),
            )
            val data = builder.build()
            assertMapsEqual(
                reqMap(
                    ReqEntry(
                        topic = "foo",
                        topicId = Uuid.ZERO_UUID,
                        partition = 0,
                        fetchOffset = 0,
                        logStartOffset = 100,
                        maxBytes = 200,
                    ),
                ),
                data.toSend,
                data.sessionPartitions,
            )
            assertTrue(data.metadata.isFull)
            assertFalse(data.canUseTopicIds)
            val resp = FetchResponse.of(
                error = Errors.NONE,
                throttleTimeMs = 0,
                sessionId = 123,
                responseData = respMap(
                    RespEntry(
                        topic = "foo",
                        partition = 0,
                        topicId = Uuid.ZERO_UUID,
                        highWatermark = 10,
                        lastStableOffset = 20,
                    ),
                ),
            )
            handler.handleResponse(resp, 12.toShort())

            // Try to add a topic ID to an already existing topic partition (0) or
            // a new partition (1) in the session.
            val topicId = Uuid.randomUuid()
            val builder2 = handler.newBuilder()
            builder2.add(
                topicPartition = TopicPartition(topic = "foo", partition = partition),
                data = FetchRequest.PartitionData(
                    topicId = topicId,
                    fetchOffset = 10,
                    logStartOffset = 110,
                    maxBytes = 210,
                    currentLeaderEpoch = null,
                ),
            )
            val data2 = builder2.build()
            // Should have the same session ID, and next epoch and can only use topic IDs if
            // the partition was updated.
            val updated = partition == 0
            // The receiving broker will handle closing the session.
            assertEquals(
                expected = 123,
                actual = data2.metadata.sessionId,
                message = "Did not use same session when $testType",
            )
            assertEquals(
                expected = 1,
                actual = data2.metadata.epoch,
                message = "Did not have correct epoch when $testType"
            )
            assertEquals(expected = updated, actual = data2.canUseTopicIds)
        }
    }

    @Test
    fun testIdUsageRevokedOnIdDowngrade() {
        // We want to test removing topic ID from an existing partition and adding a
        // new partition without an ID in the incremental request.
        // 0 is the existing partition and 1 is the new one.
        val partitions: List<Int> = mutableListOf(0, 1)
        partitions.forEach(Consumer { partition: Int ->
            val testType = if (partition == 0) "updating a partition" else "adding a new partition"
            val fooId = Uuid.randomUuid()
            val handler = FetchSessionHandler(LOG_CONTEXT, 1)
            val builder: FetchSessionHandler.Builder = handler.newBuilder()
            builder.add(
                topicPartition = TopicPartition(topic = "foo", partition = 0),
                data = FetchRequest.PartitionData(
                    topicId = fooId,
                    fetchOffset = 0,
                    logStartOffset = 100,
                    maxBytes = 200,
                    currentLeaderEpoch = null,
                ),
            )
            val data = builder.build()
            assertMapsEqual(
                reqMap(
                    ReqEntry(
                        topic = "foo",
                        topicId = fooId,
                        partition = 0,
                        fetchOffset = 0,
                        logStartOffset = 100,
                        maxBytes = 200,
                    )
                ),
                data.toSend,
                data.sessionPartitions,
            )
            assertTrue(data.metadata.isFull)
            assertTrue(data.canUseTopicIds)
            val resp: FetchResponse = FetchResponse.of(
                error = Errors.NONE,
                throttleTimeMs = 0,
                sessionId = 123,
                responseData = respMap(
                    RespEntry(
                        topic = "foo",
                        partition = 0,
                        topicId = fooId,
                        highWatermark = 10,
                        lastStableOffset = 20,
                    ),
                ),
            )
            handler.handleResponse(resp, ApiKeys.FETCH.latestVersion())

            // Try to remove a topic ID from an existing topic partition (0) or
            // add a new topic partition (1) without an ID.
            val builder2 = handler.newBuilder()
            builder2.add(
                TopicPartition(topic = "foo", partition = partition),
                FetchRequest.PartitionData(
                    topicId = Uuid.ZERO_UUID,
                    fetchOffset = 10,
                    logStartOffset = 110,
                    maxBytes = 210,
                    currentLeaderEpoch = null,
                ),
            )
            val data2: FetchRequestData = builder2.build()
            // Should have the same session ID, and next epoch and can no longer use topic IDs.
            // The receiving broker will handle closing the session.
            assertEquals(
                expected = 123,
                actual = data2.metadata.sessionId,
                message = "Did not use same session when $testType"
            )
            assertEquals(
                expected = 1,
                actual = data2.metadata.epoch,
                message = "Did not have correct epoch when $testType"
            )
            assertFalse(data2.canUseTopicIds)
        })
    }

    @ParameterizedTest
    @MethodSource("ΙdUsageCombinations")
    fun testTopicIdReplaced(startsWithTopicIds: Boolean, endsWithTopicIds: Boolean) {
        val tp = TopicPartition("foo", 0)
        val handler = FetchSessionHandler(LOG_CONTEXT, 1)
        val builder = handler.newBuilder()
        val topicId1 = if (startsWithTopicIds) Uuid.randomUuid() else Uuid.ZERO_UUID
        builder.add(
            topicPartition = tp,
            data = FetchRequest.PartitionData(
                topicId = topicId1,
                fetchOffset = 0,
                logStartOffset = 100,
                maxBytes = 200,
                currentLeaderEpoch = null,
            )
        )
        val data = builder.build()
        assertMapsEqual(
            reqMap(
                ReqEntry(
                    topic = "foo",
                    topicId = topicId1,
                    partition = 0,
                    fetchOffset = 0,
                    logStartOffset = 100,
                    maxBytes = 200,
                ),
            ),
            data.toSend,
            data.sessionPartitions,
        )
        assertTrue(data.metadata.isFull)
        assertEquals(startsWithTopicIds, data.canUseTopicIds)
        val resp = FetchResponse.of(
            error = Errors.NONE,
            throttleTimeMs = 0,
            sessionId = 123,
            responseData = respMap(
                RespEntry(
                    topic = "foo",
                    partition = 0,
                    topicId = topicId1,
                    highWatermark = 10,
                    lastStableOffset = 20,
                ),
            )
        )
        val version = if (startsWithTopicIds) ApiKeys.FETCH.latestVersion() else 12
        handler.handleResponse(resp, version)

        // Try to add a new topic ID.
        val builder2 = handler.newBuilder()
        val topicId2 = if (endsWithTopicIds) Uuid.randomUuid() else Uuid.ZERO_UUID
        // Use the same data besides the topic ID.
        val partitionData = FetchRequest.PartitionData(
            topicId = topicId2,
            fetchOffset = 0,
            logStartOffset = 100,
            maxBytes = 200,
            currentLeaderEpoch = null,
        )
        builder2.add(tp, partitionData)
        val data2 = builder2.build()
        if (startsWithTopicIds && endsWithTopicIds) {
            // If we started with an ID, both a only a new ID will count towards replaced.
            // The old topic ID partition should be in toReplace, and the new one should be in toSend.
            assertEquals(
                expected = listOf(TopicIdPartition(topicId1, tp)),
                actual = data2.toReplace,
            )
            assertMapsEqual(
                reqMap(
                    ReqEntry(
                        topic = "foo",
                        topicId = topicId2,
                        partition = 0,
                        fetchOffset = 0,
                        logStartOffset = 100,
                        maxBytes = 200,
                    ),
                ),
                data2.toSend,
                data2.sessionPartitions,
            )

            // sessionTopicNames should contain only the second topic ID.
            assertEquals(
                expected = mapOf(topicId2 to tp.topic),
                actual = handler.sessionTopicNames(),
            )
        } else if (startsWithTopicIds || endsWithTopicIds) {
            // If we downgraded to not using topic IDs we will want to send this data.
            // However, we will not mark the partition as one replaced. In this scenario,
            // we should see the session close due to changing request types.
            // We will have the new topic ID in the session partition map
            assertEquals(expected = emptyList(), actual = data2.toReplace)
            assertMapsEqual(
                reqMap(
                    ReqEntry(
                        topic = "foo",
                        topicId = topicId2,
                        partition = 0,
                        fetchOffset = 0,
                        logStartOffset = 100,
                        maxBytes = 200,
                    ),
                ),
                data2.toSend,
                data2.sessionPartitions,
            )
            // The topicNames map will have the new topic ID if it is valid.
            // The old topic ID should be removed as the map will be empty if the request
            // doesn't use topic IDs.
            if (endsWithTopicIds) assertEquals(
                expected = mapOf(topicId2 to tp.topic),
                actual = handler.sessionTopicNames()
            ) else assertEquals(
                expected = emptyMap(),
                actual = handler.sessionTopicNames(),
            )
        } else {
            // Otherwise, we have no partition in toReplace and since the partition and
            // topic ID was not updated, there is no data to send.
            assertEquals(expected = emptyList(), actual = data2.toReplace)
            assertEquals(expected = emptyMap(), actual = data2.toSend)
            assertMapsEqual(
                reqMap(
                    ReqEntry(
                        topic = "foo",
                        topicId = topicId2,
                        partition = 0,
                        fetchOffset = 0,
                        logStartOffset = 100,
                        maxBytes = 200,
                    ),
                ),
                data2.sessionPartitions,
            )
            // There is also nothing in the sessionTopicNames map, as there are no topic IDs used.
            assertEquals(expected = emptyMap(), actual = handler.sessionTopicNames())
        }

        // Should have the same session ID, and next epoch and can use topic IDs if it ended with topic IDs.
        assertEquals(
            expected = 123,
            actual = data2.metadata.sessionId,
            message = "Did not use same session",
        )
        assertEquals(
            expected = 1,
            actual = data2.metadata.epoch,
            message = "Did not have correct epoch",
        )
        assertEquals(expected = endsWithTopicIds, actual = data2.canUseTopicIds)
    }

    @ParameterizedTest
    @ValueSource(booleans = [true, false])
    fun testSessionEpochWhenMixedUsageOfTopicIDs(startsWithTopicIds: Boolean) {
        val fooId = if (startsWithTopicIds) Uuid.randomUuid() else Uuid.ZERO_UUID
        val barId = if (startsWithTopicIds) Uuid.ZERO_UUID else Uuid.randomUuid()
        val responseVersion = if (startsWithTopicIds) ApiKeys.FETCH.latestVersion() else 12
        val tp0 = TopicPartition("foo", 0)
        val tp1 = TopicPartition("bar", 1)
        val handler = FetchSessionHandler(LOG_CONTEXT, 1)
        val builder = handler.newBuilder()
        builder.add(
            topicPartition = tp0,
            data = FetchRequest.PartitionData(
                topicId = fooId,
                fetchOffset = 0,
                logStartOffset = 100,
                maxBytes = 200,
                currentLeaderEpoch = null,
            )
        )
        val data = builder.build()
        assertMapsEqual(
            reqMap(
                ReqEntry(
                    topic = "foo",
                    topicId = fooId,
                    partition = 0,
                    fetchOffset = 0,
                    logStartOffset = 100,
                    maxBytes = 200,
                ),
            ),
            data.toSend,
            data.sessionPartitions,
        )
        assertTrue(data.metadata.isFull)
        assertEquals(expected = startsWithTopicIds, actual = data.canUseTopicIds)
        val resp = FetchResponse.of(
            error = Errors.NONE,
            throttleTimeMs = 0,
            sessionId = 123,
            responseData = respMap(
                RespEntry(
                    topic = "foo",
                    partition = 0,
                    topicId = fooId,
                    highWatermark = 10,
                    lastStableOffset = 20,
                ),
            ),
        )
        handler.handleResponse(resp, responseVersion)

        // Re-add the first partition. Then add a partition with opposite ID usage.
        val builder2 = handler.newBuilder()
        builder2.add(
            topicPartition = tp0,
            data = FetchRequest.PartitionData(
                topicId = fooId,
                fetchOffset = 10,
                logStartOffset = 110,
                maxBytes = 210,
                currentLeaderEpoch = null,
            ),
        )
        builder2.add(
            topicPartition = tp1,
            data = FetchRequest.PartitionData(
                topicId = barId,
                fetchOffset = 0,
                logStartOffset = 100,
                maxBytes = 200,
                currentLeaderEpoch = null,
            ),
        )
        val data2 = builder2.build()
        // Should have the same session ID, and the next epoch and can not use topic IDs.
        // The receiving broker will handle closing the session.
        assertEquals(
            expected = 123,
            actual = data2.metadata.sessionId,
            message = "Did not use same session",
        )
        assertEquals(
            expected = 1,
            actual = data2.metadata.epoch,
            message = "Did not have final epoch",
        )
        assertFalse(data2.canUseTopicIds)
    }

    @ParameterizedTest
    @ValueSource(booleans = [true, false])
    fun testIdUsageWithAllForgottenPartitions(useTopicIds: Boolean) {
        // We want to test when all topics are removed from the session
        val foo0 = TopicPartition("foo", 0)
        val topicId = if (useTopicIds) Uuid.randomUuid() else Uuid.ZERO_UUID
        val responseVersion = if (useTopicIds) ApiKeys.FETCH.latestVersion() else 12
        val handler = FetchSessionHandler(LOG_CONTEXT, 1)

        // Add topic foo to the session
        val builder = handler.newBuilder()
        builder.add(
            topicPartition = foo0,
            data = FetchRequest.PartitionData(
                topicId = topicId,
                fetchOffset = 0,
                logStartOffset = 100,
                maxBytes = 200,
                currentLeaderEpoch = null,
            ),
        )
        val data = builder.build()
        assertMapsEqual(
            reqMap(
                ReqEntry(
                    topic = "foo",
                    topicId = topicId,
                    partition = 0,
                    fetchOffset = 0,
                    logStartOffset = 100,
                    maxBytes = 200,
                ),
            ),
            data.toSend,
            data.sessionPartitions,
        )
        assertTrue(data.metadata.isFull)
        assertEquals(expected = useTopicIds, actual = data.canUseTopicIds)
        val resp = FetchResponse.of(
            error = Errors.NONE,
            throttleTimeMs = 0,
            sessionId = 123,
            responseData = respMap(
                RespEntry(
                    topic = "foo",
                    partition = 0,
                    topicId = topicId,
                    highWatermark = 10,
                    lastStableOffset = 20
                ),
            )
        )
        handler.handleResponse(resp, responseVersion)

        // Remove the topic from the session
        val builder2 = handler.newBuilder()
        val data2 = builder2.build()
        assertEquals(listOf(TopicIdPartition(topicId, foo0)), data2.toForget)
        // Should have the same session ID, next epoch, and same ID usage.
        assertEquals(
            expected = 123, actual = data2.metadata.sessionId,
            message = "Did not use same session when useTopicIds was $useTopicIds",
        )
        assertEquals(
            expected = 1, actual = data2.metadata.epoch,
            message = "Did not have correct epoch when useTopicIds was $useTopicIds",
        )
        assertEquals(expected = useTopicIds, actual = data2.canUseTopicIds)
    }

    @Test
    fun testOkToAddNewIdAfterTopicRemovedFromSession() {
        val topicId = Uuid.randomUuid()
        val handler = FetchSessionHandler(LOG_CONTEXT, 1)
        val builder = handler.newBuilder()
        builder.add(
            TopicPartition("foo", 0),
            FetchRequest.PartitionData(topicId, 0, 100, 200, null)
        )
        val data = builder.build()
        assertMapsEqual(
            reqMap(
                ReqEntry(
                    topic = "foo",
                    topicId = topicId,
                    partition = 0,
                    fetchOffset = 0,
                    logStartOffset = 100,
                    maxBytes = 200,
                ),
            ),
            data.toSend,
            data.sessionPartitions,
        )
        assertTrue(data.metadata.isFull)
        assertTrue(data.canUseTopicIds)
        val resp = FetchResponse.of(
            error = Errors.NONE,
            throttleTimeMs = 0,
            sessionId = 123,
            responseData = respMap(
                RespEntry(
                    topic = "foo",
                    partition = 0,
                    topicId = topicId,
                    highWatermark = 10,
                    lastStableOffset = 20
                ),
            ),
        )
        handler.handleResponse(resp, ApiKeys.FETCH.latestVersion())

        // Remove the partition from the session. Return a session ID as though the session is still open.
        val builder2 = handler.newBuilder()
        val data2 = builder2.build()
        assertMapsEqual(
            LinkedHashMap(),
            data2.toSend,
            data2.sessionPartitions,
        )
        val resp2 = FetchResponse.of(
            error = Errors.NONE,
            throttleTimeMs = 0,
            sessionId = 123,
            responseData = LinkedHashMap(),
        )
        handler.handleResponse(resp2, ApiKeys.FETCH.latestVersion())

        // After the topic is removed, add a recreated topic with a new ID.
        val builder3 = handler.newBuilder()
        builder3.add(
            topicPartition = TopicPartition(topic = "foo", partition = 0),
            data = FetchRequest.PartitionData(
                topicId = Uuid.randomUuid(),
                fetchOffset = 0,
                logStartOffset = 100,
                maxBytes = 200,
                currentLeaderEpoch = null,
            ),
        )
        val data3 = builder3.build()
        // Should have the same session ID and epoch 2.
        assertEquals(
            expected = 123,
            actual = data3.metadata.sessionId,
            message = "Did not use same session",
        )
        assertEquals(
            expected = 2,
            actual = data3.metadata.epoch,
            message = "Did not have the correct session epoch",
        )
        assertTrue(data.canUseTopicIds)
    }

    @Test
    fun testVerifyFullFetchResponsePartitions() {
        val topicIds: MutableMap<String, Uuid> = HashMap()
        val topicNames: MutableMap<Uuid, String> = HashMap()
        // We want to test both on older versions that do not use topic IDs and on newer versions that do.
        val versions = listOf(12.toShort(), ApiKeys.FETCH.latestVersion())
        versions.forEach(Consumer { version: Short ->
            val handler = FetchSessionHandler(LOG_CONTEXT, 1)
            addTopicId(
                topicIds = topicIds,
                topicNames = topicNames,
                name = "foo",
                version = version,
            )
            addTopicId(
                topicIds = topicIds,
                topicNames = topicNames,
                name = "bar",
                version = version,
            )
            val fooId = topicIds.getOrDefault("foo", Uuid.ZERO_UUID)
            val barId = topicIds.getOrDefault("bar", Uuid.ZERO_UUID)
            val foo0 = TopicPartition(topic = "foo", partition = 0)
            val foo1 = TopicPartition(topic = "foo", partition = 1)
            val bar0 = TopicPartition(topic = "bar", partition = 0)
            val resp1 = FetchResponse.of(
                error = Errors.NONE,
                throttleTimeMs = 0,
                sessionId = FetchMetadata.INVALID_SESSION_ID,
                responseData = respMap(
                    RespEntry(
                        topic = "foo",
                        partition = 0,
                        topicId = fooId,
                        highWatermark = 10,
                        lastStableOffset = 20,
                    ),
                    RespEntry(
                        topic = "foo",
                        partition = 1,
                        topicId = fooId,
                        highWatermark = 10,
                        lastStableOffset = 20,
                    ),
                    RespEntry(
                        topic = "bar",
                        partition = 0,
                        topicId = barId,
                        highWatermark = 10,
                        lastStableOffset = 20,
                    ),
                )
            )
            val issue = handler.verifyFullFetchResponsePartitions(
                topicPartitions = resp1.responseData(topicNames, version).keys,
                ids = resp1.topicIds(),
                version = version,
            )
            assertTrue(issue!!.contains("extraPartitions="))
            assertFalse(issue.contains("omittedPartitions="))
            val builder = handler.newBuilder()
            builder.add(
                topicPartition = foo0,
                data = FetchRequest.PartitionData(
                    topicId = fooId,
                    fetchOffset = 0,
                    logStartOffset = 100,
                    maxBytes = 200,
                    currentLeaderEpoch = null,
                ),
            )
            builder.add(
                topicPartition = foo1,
                data = FetchRequest.PartitionData(
                    topicId = fooId,
                    fetchOffset = 10,
                    logStartOffset = 110,
                    maxBytes = 210,
                    currentLeaderEpoch = null,
                ),
            )
            builder.add(
                topicPartition = bar0,
                data = FetchRequest.PartitionData(
                    topicId = barId,
                    fetchOffset = 20,
                    logStartOffset = 120,
                    maxBytes = 220,
                    currentLeaderEpoch = null,
                ),
            )
            builder.build()
            val resp2: FetchResponse = FetchResponse.of(
                error = Errors.NONE,
                throttleTimeMs = 0,
                sessionId = FetchMetadata.INVALID_SESSION_ID,
                responseData = respMap(
                    RespEntry(
                        topic = "foo",
                        partition = 0,
                        topicId = fooId,
                        highWatermark = 10,
                        lastStableOffset = 20,
                    ),
                    RespEntry(
                        topic = "foo",
                        partition = 1,
                        topicId = fooId,
                        highWatermark = 10,
                        lastStableOffset = 20,
                    ),
                    RespEntry(
                        topic = "bar",
                        partition = 0,
                        topicId = barId,
                        highWatermark = 10,
                        lastStableOffset = 20,
                    ),
                )
            )
            val issue2: String? = handler.verifyFullFetchResponsePartitions(
                topicPartitions = resp2.responseData(topicNames, version).keys,
                ids = resp2.topicIds(),
                version = version,
            )
            assertNull(issue2)
            val resp3 = FetchResponse.of(
                error = Errors.NONE,
                throttleTimeMs = 0,
                sessionId = FetchMetadata.INVALID_SESSION_ID,
                responseData = respMap(
                    RespEntry(
                        topic = "foo",
                        partition = 0,
                        topicId = fooId,
                        highWatermark = 10,
                        lastStableOffset = 20,
                    ),
                    RespEntry(
                        topic = "foo",
                        partition = 1,
                        topicId = fooId,
                        highWatermark = 10,
                        lastStableOffset = 20,
                    ),
                )
            )
            val issue3 = handler.verifyFullFetchResponsePartitions(
                topicPartitions = resp3.responseData(topicNames, version).keys,
                ids = resp3.topicIds(),
                version = version,
            )
            assertFalse(issue3!!.contains("extraPartitions="))
            assertTrue(issue3.contains("omittedPartitions="))
        })
    }

    @Test
    fun testVerifyFullFetchResponsePartitionsWithTopicIds() {
        val topicIds = mutableMapOf<String, Uuid>()
        val topicNames = mutableMapOf<Uuid, String>()
        val handler = FetchSessionHandler(logContext = LOG_CONTEXT, node = 1)
        addTopicId(
            topicIds = topicIds,
            topicNames = topicNames,
            name = "foo",
            version = ApiKeys.FETCH.latestVersion(),
        )
        addTopicId(
            topicIds = topicIds,
            topicNames = topicNames,
            name = "bar",
            version = ApiKeys.FETCH.latestVersion(),
        )
        addTopicId(
            topicIds = topicIds,
            topicNames = topicNames,
            name = "extra2",
            version = ApiKeys.FETCH.latestVersion(),
        )
        val resp1 = FetchResponse.of(
            error = Errors.NONE,
            throttleTimeMs = 0,
            sessionId = FetchMetadata.INVALID_SESSION_ID,
            responseData = respMap(
                RespEntry(
                    topic = "foo",
                    partition = 0,
                    topicId = topicIds["foo"]!!,
                    highWatermark = 10,
                    lastStableOffset = 20,
                ),
                RespEntry(
                    topic = "extra2",
                    partition = 1,
                    topicId = topicIds["extra2"]!!,
                    highWatermark = 10,
                    lastStableOffset = 20,
                ),
                RespEntry(
                    topic = "bar",
                    partition = 0,
                    topicId = topicIds["bar"]!!,
                    highWatermark = 10,
                    lastStableOffset = 20,
                ),
            )
        )
        val issue = handler.verifyFullFetchResponsePartitions(
            topicPartitions = resp1.responseData(topicNames, ApiKeys.FETCH.latestVersion()).keys,
            ids = resp1.topicIds(),
            version = ApiKeys.FETCH.latestVersion(),
        )
        assertTrue(issue!!.contains("extraPartitions="))
        assertFalse(issue.contains("omittedPartitions="))
        val builder = handler.newBuilder()
        builder.add(
            topicPartition = TopicPartition(topic = "foo", partition = 0),
            data = FetchRequest.PartitionData(
                topicId = topicIds["foo"]!!,
                fetchOffset = 0,
                logStartOffset = 100,
                maxBytes = 200,
                currentLeaderEpoch = null,
            ),
        )
        builder.add(
            topicPartition = TopicPartition(topic = "bar", partition = 0),
            data = FetchRequest.PartitionData(
                topicId = topicIds["bar"]!!,
                fetchOffset = 20,
                logStartOffset = 120,
                maxBytes = 220,
                currentLeaderEpoch = null,
            ),
        )
        builder.build()
        val resp2 = FetchResponse.of(
            error = Errors.NONE,
            throttleTimeMs = 0,
            sessionId = FetchMetadata.INVALID_SESSION_ID,
            responseData = respMap(
                RespEntry(
                    topic = "foo",
                    partition = 0,
                    topicId = topicIds["foo"]!!,
                    highWatermark = 10,
                    lastStableOffset = 20,
                ),
                RespEntry(
                    topic = "extra2",
                    partition = 1,
                    topicId = topicIds["extra2"]!!,
                    highWatermark = 10,
                    lastStableOffset = 20,
                ),
                RespEntry(
                    topic = "bar",
                    partition = 0,
                    topicId = topicIds["bar"]!!,
                    highWatermark = 10,
                    lastStableOffset = 20,
                ),
            )
        )
        val issue2 = handler.verifyFullFetchResponsePartitions(
            topicPartitions = resp2.responseData(topicNames, ApiKeys.FETCH.latestVersion()).keys,
            ids = resp2.topicIds(),
            version = ApiKeys.FETCH.latestVersion(),
        )
        assertTrue(issue2!!.contains("extraPartitions="))
        assertFalse(issue2.contains("omittedPartitions="))
        val resp3 = FetchResponse.of(
            error = Errors.NONE,
            throttleTimeMs = 0,
            sessionId = FetchMetadata.INVALID_SESSION_ID,
            responseData = respMap(
                RespEntry(
                    topic = "foo",
                    partition = 0,
                    topicId = topicIds["foo"]!!,
                    highWatermark = 10,
                    lastStableOffset = 20,
                ),
                RespEntry(
                    topic = "bar",
                    partition = 0,
                    topicId = topicIds["bar"]!!,
                    highWatermark = 10,
                    lastStableOffset = 20,
                ),
            )
        )
        val issue3 = handler.verifyFullFetchResponsePartitions(
            topicPartitions = resp3.responseData(topicNames, ApiKeys.FETCH.latestVersion()).keys,
            ids = resp3.topicIds(),
            version = ApiKeys.FETCH.latestVersion(),
        )
        assertNull(issue3)
    }

    @Test
    fun testTopLevelErrorResetsMetadata() {
        val topicIds = mutableMapOf<String, Uuid>()
        val topicNames = mutableMapOf<Uuid, String>()
        val handler = FetchSessionHandler(LOG_CONTEXT, 1)
        val builder = handler.newBuilder()
        addTopicId(
            topicIds = topicIds,
            topicNames = topicNames,
            name = "foo",
            version = ApiKeys.FETCH.latestVersion(),
        )
        val fooId = topicIds.getOrDefault("foo", Uuid.ZERO_UUID)
        builder.add(
            topicPartition = TopicPartition(topic = "foo", partition = 0),
            data = FetchRequest.PartitionData(
                topicId = fooId,
                fetchOffset = 0,
                logStartOffset = 100,
                maxBytes = 200,
                currentLeaderEpoch = null,
            ),
        )
        builder.add(
            topicPartition = TopicPartition("foo", 1),
            data = FetchRequest.PartitionData(
                topicId = fooId,
                fetchOffset = 10,
                logStartOffset = 110,
                maxBytes = 210,
                currentLeaderEpoch = null,
            ),
        )
        val data = builder.build()
        assertEquals(
            expected = FetchMetadata.INVALID_SESSION_ID,
            actual = data.metadata.sessionId,
        )
        assertEquals(
            expected = FetchMetadata.INITIAL_EPOCH,
            actual = data.metadata.epoch,
        )
        val resp = FetchResponse.of(
            error = Errors.NONE,
            throttleTimeMs = 0,
            sessionId = 123,
            responseData = respMap(
                RespEntry(
                    topic = "foo",
                    partition = 0,
                    topicId = topicIds["foo"]!!,
                    highWatermark = 10,
                    lastStableOffset = 20,
                ),
                RespEntry(
                    topic = "foo",
                    partition = 1,
                    topicId = topicIds["foo"]!!,
                    highWatermark = 10,
                    lastStableOffset = 20,
                ),
            )
        )
        handler.handleResponse(resp, ApiKeys.FETCH.latestVersion())

        // Test an incremental fetch request which adds an ID unknown to the broker.
        val builder2 = handler.newBuilder()
        addTopicId(
            topicIds = topicIds,
            topicNames = topicNames,
            name = "unknown",
            version = ApiKeys.FETCH.latestVersion(),
        )
        builder2.add(
            topicPartition = TopicPartition(topic = "unknown", partition = 0),
            data = FetchRequest.PartitionData(
                topicId = topicIds.getOrDefault("unknown", Uuid.ZERO_UUID),
                fetchOffset = 0,
                logStartOffset = 100,
                maxBytes = 200,
                currentLeaderEpoch = null,
            ),
        )
        val data2 = builder2.build()
        assertFalse(data2.metadata.isFull)
        assertEquals(expected = 123, actual = data2.metadata.sessionId)
        assertEquals(
            expected = FetchMetadata.nextEpoch(FetchMetadata.INITIAL_EPOCH),
            actual = data2.metadata.epoch,
        )

        // Return and handle a response with a top level error
        val resp2 = FetchResponse.of(
            error = Errors.UNKNOWN_TOPIC_ID,
            throttleTimeMs = 0,
            sessionId = 123,
            responseData = respMap(
                RespEntry(
                    topic = "unknown",
                    partition = 0,
                    topicId = Uuid.randomUuid(),
                    error = Errors.UNKNOWN_TOPIC_ID,
                ),
            ),
        )
        assertFalse(handler.handleResponse(resp2, ApiKeys.FETCH.latestVersion()))

        // Ensure we start with a new epoch. This will close the session in the next request.
        val builder3 = handler.newBuilder()
        val data3 = builder3.build()
        assertEquals(expected = 123, actual = data3.metadata.sessionId)
        assertEquals(expected = FetchMetadata.INITIAL_EPOCH, actual = data3.metadata.epoch)
    }

    private fun addTopicId(
        topicIds: MutableMap<String, Uuid>,
        topicNames: MutableMap<Uuid, String>,
        name: String,
        version: Short,
    ) {
        if (version >= 13) {
            val id = Uuid.randomUuid()
            topicIds[name] = id
            topicNames[id] = name
        }
    }

    companion object {
        private val LOG_CONTEXT = LogContext("[FetchSessionHandler]=")

        /**
         * Create a set of TopicPartitions.  We use a TreeSet, in order to get a deterministic
         * ordering for test purposes.
         */
        private fun toSet(vararg arr: TopicPartition): Set<TopicPartition> {
            val set = TreeSet<TopicPartition> { o1, o2 ->
                o1.toString().compareTo(o2.toString())
            }
            set.addAll(arr)
            return set
        }

        private fun reqMap(
            vararg entries: ReqEntry,
        ): LinkedHashMap<TopicPartition, FetchRequest.PartitionData> {
            val map = LinkedHashMap<TopicPartition, FetchRequest.PartitionData>()
            for (entry in entries) {
                map[entry.part] = entry.data
            }
            return map
        }

        private fun assertMapEquals(
            expected: Map<TopicPartition, FetchRequest.PartitionData>,
            actual: Map<TopicPartition, FetchRequest.PartitionData>,
        ) {
            val expectedIter = expected.entries.iterator()
            val actualIter = actual.entries.iterator()
            var i = 1
            while (expectedIter.hasNext()) {
                val expectedEntry = expectedIter.next()
                if (!actualIter.hasNext()) fail("Element $i not found.")

                val actualEntry = actualIter.next()
                assertEquals(
                    expected = expectedEntry.key,
                    actual = actualEntry.key,
                    message = "Element $i had a different TopicPartition than expected.",
                )
                assertEquals(
                    expected = expectedEntry.value,
                    actual = actualEntry.value,
                    message = "Element $i had different PartitionData than expected.",
                )
                i++
            }
            if (actualIter.hasNext()) fail("Unexpected element $i found.")
        }

        @SafeVarargs
        private fun assertMapsEqual(
            expected: Map<TopicPartition, FetchRequest.PartitionData>,
            vararg actuals: Map<TopicPartition, FetchRequest.PartitionData>,
        ) {
            for (actual in actuals) assertMapEquals(expected, actual)
        }

        private fun assertListEquals(
            expected: List<TopicIdPartition>,
            actual: List<TopicIdPartition>,
        ) {
            for (expectedPart: TopicIdPartition in expected) {
                if (!actual.contains(expectedPart))
                    fail("Failed to find expected partition $expectedPart")
            }
            for (actualPart: TopicIdPartition in actual) {
                if (!expected.contains(actualPart))
                    fail("Found unexpected partition $actualPart")
            }
        }

        private fun respMap(
            vararg entries: RespEntry,
        ): LinkedHashMap<TopicIdPartition, FetchResponseData.PartitionData> {
            val map: LinkedHashMap<TopicIdPartition, FetchResponseData.PartitionData> = LinkedHashMap()
            entries.forEach { entry -> map[entry.part] = entry.data }
            return map
        }

        private fun idUsageCombinations(): Stream<Arguments> {
            return Stream.of(
                Arguments.of(true, true),
                Arguments.of(true, false),
                Arguments.of(false, true),
                Arguments.of(false, false)
            )
        }
    }
}
