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

package org.apache.kafka.clients.consumer.internals

import org.apache.kafka.clients.Metadata
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.internals.ClusterResourceListeners
import org.apache.kafka.common.requests.MetadataRequest
import org.apache.kafka.common.utils.LogContext

class ConsumerMetadata(
    refreshBackoffMs: Long,
    metadataExpireMs: Long,
    private val includeInternalTopics: Boolean,
    val allowAutoTopicCreation: Boolean,
    private val subscription: SubscriptionState,
    logContext: LogContext,
    clusterResourceListeners: ClusterResourceListeners,
) : Metadata(
    refreshBackoffMs = refreshBackoffMs,
    metadataExpireMs = metadataExpireMs,
    logContext = logContext,
    clusterResourceListeners = clusterResourceListeners,
) {

    private val transientTopics = mutableSetOf<String>()

    constructor(
        config: ConsumerConfig,
        subscriptions: SubscriptionState,
        logContext: LogContext,
        clusterResourceListeners: ClusterResourceListeners,
    ) : this(
        refreshBackoffMs = config.getLong(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG)!!,
        metadataExpireMs = config.getLong(ConsumerConfig.METADATA_MAX_AGE_CONFIG)!!,
        includeInternalTopics = !config.getBoolean(ConsumerConfig.EXCLUDE_INTERNAL_TOPICS_CONFIG)!!,
        allowAutoTopicCreation = config.getBoolean(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG)!!,
        subscription = subscriptions,
        logContext = logContext,
        clusterResourceListeners = clusterResourceListeners,
    )

    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("allowAutoTopicCreation"),
    )
    fun allowAutoTopicCreation(): Boolean = allowAutoTopicCreation

    @Synchronized
    override fun newMetadataRequestBuilder(): MetadataRequest.Builder {
        if (subscription.hasPatternSubscription()) return MetadataRequest.Builder.allTopics()
        val topics = subscription.metadataTopics() + transientTopics

        return MetadataRequest.Builder(topics.toList(), allowAutoTopicCreation)
    }

    @Synchronized
    fun addTransientTopics(topics: Set<String>) {
        transientTopics.addAll(topics)
        if (!fetch().topics.containsAll(topics)) requestUpdateForNewTopics()
    }

    fun clearTransientTopics() = transientTopics.clear()

    @Synchronized
    override fun retainTopic(
        topic: String,
        isInternal: Boolean,
        nowMs: Long,
    ): Boolean = if (
        transientTopics.contains(topic)
        || subscription.needsMetadata(topic)
    ) true
    else if (isInternal && !includeInternalTopics) false
    else subscription.matchesSubscribedPattern(topic)
}
