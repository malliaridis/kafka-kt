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

package org.apache.kafka.clients.producer.internals

import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.errors.AuthenticationException
import org.apache.kafka.common.errors.TimeoutException
import org.apache.kafka.common.internals.ClusterResourceListeners
import org.apache.kafka.common.requests.MetadataResponse
import org.apache.kafka.common.requests.RequestTestUtils.metadataUpdateWith
import org.apache.kafka.common.utils.LogContext
import org.apache.kafka.common.utils.Time
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import java.util.concurrent.atomic.AtomicReference
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertNull
import kotlin.test.assertTrue

class ProducerMetadataTest {
    
    private val refreshBackoffMs: Long = 100
    
    private val metadataExpireMs: Long = 1000
    
    private val metadata = ProducerMetadata(
        refreshBackoffMs = refreshBackoffMs,
        metadataExpireMs = metadataExpireMs,
        metadataIdleMs = METADATA_IDLE_MS,
        logContext = LogContext(),
        clusterResourceListeners = ClusterResourceListeners(),
        time = Time.SYSTEM,
    )
    
    private val backgroundError = AtomicReference<Exception?>()
    
    @AfterEach
    fun tearDown() {
        assertNull(
            actual = backgroundError.get(),
            message = "Exception in background thread : ${backgroundError.get()}",
        )
    }

    @Test
    @Throws(Exception::class)
    fun testMetadata() {
        var time = Time.SYSTEM.milliseconds()
        val topic = "my-topic"
        metadata.add(topic, time)
        metadata.updateWithCurrentRequestVersion(
            response = responseWithTopics(emptySet()),
            isPartialUpdate = false,
            nowMs = time,
        )

        assertTrue(metadata.timeToNextUpdate(time) > 0, "No update needed.")

        metadata.requestUpdate()

        assertTrue(metadata.timeToNextUpdate(time) > 0, "Still no updated needed due to backoff")

        time += refreshBackoffMs

        assertEquals(0, metadata.timeToNextUpdate(time), "Update needed now that backoff time expired")

        val t1 = asyncFetch(topic, 500)
        val t2 = asyncFetch(topic, 500)

        assertTrue(t1.isAlive, "Awaiting update")
        assertTrue(t2.isAlive, "Awaiting update")

        // Perform metadata update when an update is requested on the async fetch thread
        // This simulates the metadata update sequence in KafkaProducer
        while (t1.isAlive || t2.isAlive) {
            if (metadata.timeToNextUpdate(time) == 0L) {
                metadata.updateWithCurrentRequestVersion(responseWithCurrentTopics(), false, time)
                time += refreshBackoffMs
            }
            Thread.sleep(1)
        }
        t1.join()
        t2.join()
        assertTrue(metadata.timeToNextUpdate(time) > 0, "No update needed.")
        time += metadataExpireMs
        assertEquals(0, metadata.timeToNextUpdate(time), "Update needed due to stale metadata.")
    }

    @Test
    @Throws(InterruptedException::class)
    fun testMetadataAwaitAfterClose() {
        var time: Long = 0
        metadata.updateWithCurrentRequestVersion(
            response = responseWithCurrentTopics(),
            isPartialUpdate = false,
            nowMs = time,
        )

        assertTrue(metadata.timeToNextUpdate(time) > 0, "No update needed.")

        metadata.requestUpdate()

        assertTrue(metadata.timeToNextUpdate(time) > 0, "Still no updated needed due to backoff")

        time += refreshBackoffMs

        assertEquals(0, metadata.timeToNextUpdate(time), "Update needed now that backoff time expired")

        val topic = "my-topic"
        metadata.close()
        val t1 = asyncFetch(topic, 500)
        t1.join()

        val error = backgroundError.get()
        assertIs<KafkaException>(error)
        assertTrue(error.toString().contains("Requested metadata update after close"))

        clearBackgroundError()
    }

    /**
     * Tests that [org.apache.kafka.clients.producer.internals.ProducerMetadata.awaitUpdate] doesn't
     * wait forever with a max timeout value of 0
     *
     * @throws Exception
     * @see [KAFKA-1836](https://issues.apache.org/jira/browse/KAFKA-1836)
     */
    @Test
    @Throws(Exception::class)
    fun testMetadataUpdateWaitTime() {
        val time: Long = 0
        metadata.updateWithCurrentRequestVersion(
            response = responseWithCurrentTopics(),
            isPartialUpdate = false,
            nowMs = time,
        )

        assertTrue(metadata.timeToNextUpdate(time) > 0, "No update needed.")
        // first try with a max wait time of 0 and ensure that this returns back without waiting forever
        assertFailsWith<TimeoutException>(
            message = "Wait on metadata update was expected to timeout, but it didn't",
        ) { metadata.awaitUpdate(metadata.requestUpdate(), 0) }

        // now try with a higher timeout value once
        val twoSecondWait: Long = 2000
        assertFailsWith<TimeoutException>(
            message = "Wait on metadata update was expected to timeout, but it didn't",
        ) { metadata.awaitUpdate(metadata.requestUpdate(), twoSecondWait) }
    }

    @Test
    fun testTimeToNextUpdateOverwriteBackoff() {
        val now: Long = 10000

        // New topic added to fetch set and update requested. It should allow immediate update.
        metadata.updateWithCurrentRequestVersion(
            response = responseWithCurrentTopics(),
            isPartialUpdate = false,
            nowMs = now,
        )
        metadata.add("new-topic", now)
        assertEquals(0, metadata.timeToNextUpdate(now))

        // Even though add is called, immediate update isn't necessary if the new topic set isn't
        // containing a new topic,
        metadata.updateWithCurrentRequestVersion(
            response = responseWithCurrentTopics(),
            isPartialUpdate = false,
            nowMs = now,
        )
        metadata.add("new-topic", now)
        assertEquals(metadataExpireMs, metadata.timeToNextUpdate(now))

        // If the new set of topics containing a new topic then it should allow immediate update.
        metadata.add("another-new-topic", now)
        assertEquals(0, metadata.timeToNextUpdate(now))
    }

    @Test
    fun testTopicExpiry() {
        // Test that topic is expired if not used within the expiry interval
        var time: Long = 0
        val topic1 = "topic1"
        metadata.add(topic1, time)
        metadata.updateWithCurrentRequestVersion(
            response = responseWithCurrentTopics(),
            isPartialUpdate = false,
            nowMs = time,
        )
        assertTrue(metadata.containsTopic(topic1))
        time += METADATA_IDLE_MS
        metadata.updateWithCurrentRequestVersion(responseWithCurrentTopics(), false, time)
        assertFalse(metadata.containsTopic(topic1), "Unused topic not expired")

        // Test that topic is not expired if used within the expiry interval
        val topic2 = "topic2"
        metadata.add(topic2, time)
        metadata.updateWithCurrentRequestVersion(
            response = responseWithCurrentTopics(),
            isPartialUpdate = false,
            nowMs = time,
        )
        repeat(3) {
            time += METADATA_IDLE_MS / 2
            metadata.updateWithCurrentRequestVersion(
                response = responseWithCurrentTopics(),
                isPartialUpdate = false,
                nowMs = time,
            )
            assertTrue(metadata.containsTopic(topic2), "Topic expired even though in use")
            metadata.add(topic2, time)
        }

        // Add a new topic, but update its metadata after the expiry would have occurred.
        // The topic should still be retained.
        val topic3 = "topic3"
        metadata.add(topic3, time)
        time += METADATA_IDLE_MS * 2
        metadata.updateWithCurrentRequestVersion(
            response = responseWithCurrentTopics(),
            isPartialUpdate = false,
            nowMs = time,
        )
        assertTrue(metadata.containsTopic(topic3), "Topic expired while awaiting metadata")
    }

    @Test
    fun testMetadataWaitAbortedOnFatalException() {
        metadata.fatalError(AuthenticationException("Fatal exception from test"))
        assertFailsWith<AuthenticationException> { metadata.awaitUpdate(lastVersion = 0, timeoutMs = 1000) }
    }

    @Test
    fun testMetadataPartialUpdate() {
        var now: Long = 10000

        // Add a new topic and fetch its metadata in a partial update.
        val topic1 = "topic-one"
        metadata.add(topic1, now)
        assertTrue(metadata.updateRequested())
        assertEquals(0, metadata.timeToNextUpdate(now))
        assertEquals(metadata.topics(), setOf(topic1))
        assertEquals(metadata.newTopics(), setOf(topic1))

        // Perform the partial update. Verify the topic is no longer considered "new".
        now += 1000
        metadata.updateWithCurrentRequestVersion(
            response = responseWithTopics(setOf(topic1)),
            isPartialUpdate = true,
            nowMs = now,
        )
        assertFalse(metadata.updateRequested())
        assertEquals(metadata.topics(), setOf(topic1))
        assertEquals(metadata.newTopics(), emptySet<Any>())

        // Add the topic again. It should not be considered "new".
        metadata.add(topic1, now)
        assertFalse(metadata.updateRequested())
        assertTrue(metadata.timeToNextUpdate(now) > 0)
        assertEquals(metadata.topics(), setOf(topic1))
        assertEquals(metadata.newTopics(), emptySet<Any>())

        // Add two new topics. However, we'll only apply a partial update for one of them.
        now += 1000
        val topic2 = "topic-two"
        metadata.add(topic2, now)
        now += 1000
        val topic3 = "topic-three"
        metadata.add(topic3, now)
        assertTrue(metadata.updateRequested())
        assertEquals(0, metadata.timeToNextUpdate(now))
        assertEquals(metadata.topics(), setOf(topic1, topic2, topic3))
        assertEquals(metadata.newTopics(), setOf(topic2, topic3))

        // Perform the partial update for a subset of the new topics.
        now += 1000
        assertTrue(metadata.updateRequested())
        metadata.updateWithCurrentRequestVersion(
            response = responseWithTopics(setOf(topic2)),
            isPartialUpdate = true,
            nowMs = now,
        )
        assertEquals(metadata.topics(), setOf(topic1, topic2, topic3))
        assertEquals(metadata.newTopics(), setOf(topic3))
    }

    @Test
    fun testRequestUpdateForTopic() {
        var now: Long = 10000
        val topic1 = "topic-1"
        val topic2 = "topic-2"

        // Add the topics to the metadata.
        metadata.add(topic1, now)
        metadata.add(topic2, now)
        assertTrue(metadata.updateRequested())

        // Request an update for topic1. Since the topic is considered new, it should not trigger
        // the metadata to require a full update.
        metadata.requestUpdateForTopic(topic1)
        assertTrue(metadata.updateRequested())

        // Perform the partial update. Verify no additional (full) updates are requested.
        now += 1000
        metadata.updateWithCurrentRequestVersion(
            response = responseWithTopics(setOf(topic1)),
            isPartialUpdate = true,
            nowMs = now,
        )
        assertFalse(metadata.updateRequested())

        // Request an update for topic1 again. Such a request may occur when the leader
        // changes, which may affect many topics, and should therefore request a full update.
        metadata.requestUpdateForTopic(topic1)
        assertTrue(metadata.updateRequested())

        // Perform a partial update for the topic. This should not clear the full update.
        now += 1000
        metadata.updateWithCurrentRequestVersion(
            response = responseWithTopics(setOf(topic1)),
            isPartialUpdate = true,
            nowMs = now,
        )
        assertTrue(metadata.updateRequested())

        // Perform the full update. This should clear the update request.
        now += 1000
        metadata.updateWithCurrentRequestVersion(
            response = responseWithTopics(setOf(topic1, topic2)),
            isPartialUpdate = false,
            nowMs = now,
        )
        assertFalse(metadata.updateRequested())
    }

    private fun responseWithCurrentTopics(): MetadataResponse =  responseWithTopics(metadata.topics())

    private fun responseWithTopics(topics: Set<String>): MetadataResponse {
        val partitionCounts = topics.associateWith { 1 }
        return metadataUpdateWith(numNodes = 1, topicPartitionCounts = partitionCounts)
    }

    private fun clearBackgroundError() {
        backgroundError.set(null)
    }

    private fun asyncFetch(topic: String, maxWaitMs: Long): Thread {
        val thread = Thread {
            try {
                while (metadata.fetch().partitionsForTopic(topic).isEmpty())
                    metadata.awaitUpdate(metadata.requestUpdate(), maxWaitMs)
            } catch (e: Exception) {
                backgroundError.set(e)
            }
        }
        thread.start()
        return thread
    }

    companion object {

        private const val METADATA_IDLE_MS = (60 * 1000).toLong()
    }
}
