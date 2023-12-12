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

import org.apache.kafka.clients.Metadata
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.internals.ClusterResourceListeners
import org.apache.kafka.common.requests.MetadataRequest
import org.apache.kafka.common.requests.MetadataResponse
import org.apache.kafka.common.utils.LogContext
import org.apache.kafka.common.utils.Time
import org.slf4j.Logger

class ProducerMetadata(
    refreshBackoffMs: Long,
    metadataExpireMs: Long,
    // If a topic hasn't been accessed for this many milliseconds, it is removed from the cache.
    private val metadataIdleMs: Long,
    logContext: LogContext,
    clusterResourceListeners: ClusterResourceListeners?,
    private val time: Time
) : Metadata(
    refreshBackoffMs = refreshBackoffMs,
    metadataExpireMs = metadataExpireMs,
    logContext = logContext,
    clusterResourceListeners = clusterResourceListeners!!,
) {

    /**
     * Topics with expiry time
     */
    private val topics: MutableMap<String, Long?> = HashMap()

    private val newTopics: MutableSet<String> = hashSetOf()

    private val log: Logger = logContext.logger(ProducerMetadata::class.java)

    @Synchronized
    override fun newMetadataRequestBuilder(): MetadataRequest.Builder {
        return MetadataRequest.Builder(ArrayList(topics.keys), true)
    }

    @Synchronized
    override fun newMetadataRequestBuilderForNewTopics(): MetadataRequest.Builder {
        return MetadataRequest.Builder(ArrayList(newTopics), true)
    }

    @Synchronized
    fun add(topic: String, nowMs: Long) {
        if (topics.put(topic, nowMs + metadataIdleMs) == null) {
            newTopics.add(topic)
            requestUpdateForNewTopics()
        }
    }

    @Synchronized
    fun requestUpdateForTopic(topic: String): Int {
        return if (newTopics.contains(topic)) requestUpdateForNewTopics()
        else requestUpdate()
    }

    // Visible for testing
    @Synchronized
    fun topics(): Set<String> = topics.keys

    // Visible for testing
    @Synchronized
    fun newTopics(): Set<String> = newTopics

    @Synchronized
    fun containsTopic(topic: String): Boolean = topics.containsKey(topic)

    @Synchronized
    override fun retainTopic(
        topic: String,
        isInternal: Boolean,
        nowMs: Long,
    ): Boolean {
        val expireMs = topics[topic]

        return if (expireMs == null) false
        else if (newTopics.contains(topic)) true
        else if (expireMs <= nowMs) {
            log.debug(
                "Removing unused topic {} from the metadata list, expiryMs {} now {}",
                topic,
                expireMs,
                nowMs
            )
            topics.remove(topic)
            false
        } else true
    }

    /**
     * Wait for metadata update until the current version is larger than the last version we know of
     */
    @Synchronized
    @Throws(InterruptedException::class)
    fun awaitUpdate(lastVersion: Int, timeoutMs: Long) {
        val currentTimeMs = time.milliseconds()
        val deadlineMs =
            if (currentTimeMs + timeoutMs < 0) Long.MAX_VALUE
            else currentTimeMs + timeoutMs

        time.waitObject(
            obj = this as Object,
            condition = {
                // Throw fatal exceptions, if there are any. Recoverable topic errors will be handled by the caller.
                maybeThrowFatalException()
                updateVersion() > lastVersion || isClosed
            },
            deadlineMs = deadlineMs
        )

        if (isClosed) throw KafkaException("Requested metadata update after close")
    }

    @Synchronized
    override fun update(
        requestVersion: Int,
        response: MetadataResponse,
        isPartialUpdate: Boolean,
        nowMs: Long
    ) {
        super.update(requestVersion, response, isPartialUpdate, nowMs)

        // Remove all topics in the response that are in the new topic set. Note that if an error was encountered for a
        // new topic's metadata, then any work to resolve the error will include the topic in a full metadata update.
        if (newTopics.isNotEmpty()) response.topicMetadata().forEach { (_, topic) ->
            newTopics.remove(topic)
        }

        (this as Object).notifyAll()
    }

    @Synchronized
    override fun fatalError(exception: KafkaException?) {
        super.fatalError(exception)
        (this as Object).notifyAll()
    }

    /**
     * Close this instance and notify any awaiting threads.
     */
    @Synchronized
    override fun close() {
        super.close()
        (this as Object).notifyAll()
    }
}
