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

package org.apache.kafka.server.config

import org.apache.kafka.common.config.TopicConfig

object ServerTopicConfigSynonyms {

    private const val LOG_PREFIX = "log."

    private const val LOG_CLEANER_PREFIX = LOG_PREFIX + "cleaner."

    /**
     * Maps topic configurations to their equivalent broker configurations.
     *
     * Topics can be configured either by setting their dynamic topic configurations, or by
     * setting equivalent broker configurations. For historical reasons, the equivalent broker
     * configurations have different names. This table maps each topic configuration to its
     * equivalent broker configurations.
     *
     * In some cases, the equivalent broker configurations must be transformed before they
     * can be used. For example, log.roll.hours must be converted to milliseconds before it
     * can be used as the value of segment.ms.
     *
     * The broker configurations will be used in the order specified here. In other words, if
     * both the first and the second synonyms are configured, we will use only the value of
     * the first synonym and ignore the second.
     */
    // Topic configs with no mapping to a server config can be found in `LogConfig.CONFIGS_WITH_NO_SERVER_DEFAULTS`
    @Suppress("Deprecation")
    val ALL_TOPIC_CONFIG_SYNONYMS = mapOf(
        TopicConfig.SEGMENT_BYTES_CONFIG to listOf(
            ConfigSynonym(LOG_PREFIX + TopicConfig.SEGMENT_BYTES_CONFIG),
        ),
        TopicConfig.SEGMENT_MS_CONFIG to listOf(
            ConfigSynonym(LOG_PREFIX + "roll.ms"),
            ConfigSynonym(LOG_PREFIX + "roll.hours", ConfigSynonym.HOURS_TO_MILLISECONDS),
        ),
        TopicConfig.SEGMENT_JITTER_MS_CONFIG to listOf(
            ConfigSynonym(LOG_PREFIX + "roll.jitter.ms"),
            ConfigSynonym(LOG_PREFIX + "roll.jitter.hours", ConfigSynonym.HOURS_TO_MILLISECONDS),
        ),
        TopicConfig.SEGMENT_INDEX_BYTES_CONFIG to listOf(
            ConfigSynonym(LOG_PREFIX + "index.size.max.bytes"),
        ),
        TopicConfig.SEGMENT_INDEX_BYTES_CONFIG to listOf(
            ConfigSynonym(LOG_PREFIX + "index.size.max.bytes"),
        ),
        TopicConfig.FLUSH_MESSAGES_INTERVAL_CONFIG to listOf(
            ConfigSynonym(LOG_PREFIX + "flush.interval.messages"),
        ),
        TopicConfig.FLUSH_MS_CONFIG to listOf(
            ConfigSynonym(LOG_PREFIX + "flush.interval.ms"),
            ConfigSynonym(LOG_PREFIX + "flush.scheduler.interval.ms")
        ),
        TopicConfig.RETENTION_BYTES_CONFIG to listOf(
            ConfigSynonym(LOG_PREFIX + TopicConfig.RETENTION_BYTES_CONFIG)
        ),
        TopicConfig.RETENTION_MS_CONFIG to listOf(
            ConfigSynonym(LOG_PREFIX + "retention.ms"),
            ConfigSynonym(LOG_PREFIX + "retention.minutes", ConfigSynonym.MINUTES_TO_MILLISECONDS),
            ConfigSynonym(LOG_PREFIX + "retention.hours", ConfigSynonym.HOURS_TO_MILLISECONDS),
        ),
        TopicConfig.MAX_MESSAGE_BYTES_CONFIG to listOf(
            ConfigSynonym("message.max.bytes"),
        ),
        TopicConfig.INDEX_INTERVAL_BYTES_CONFIG to listOf(
            ConfigSynonym(LOG_PREFIX + TopicConfig.INDEX_INTERVAL_BYTES_CONFIG),
        ),
        TopicConfig.DELETE_RETENTION_MS_CONFIG to listOf(
            ConfigSynonym(LOG_CLEANER_PREFIX + TopicConfig.DELETE_RETENTION_MS_CONFIG),
        ),
        TopicConfig.MIN_COMPACTION_LAG_MS_CONFIG to listOf(
            ConfigSynonym(LOG_CLEANER_PREFIX + TopicConfig.MIN_COMPACTION_LAG_MS_CONFIG),
        ),
        TopicConfig.MAX_COMPACTION_LAG_MS_CONFIG to listOf(
            ConfigSynonym(LOG_CLEANER_PREFIX + TopicConfig.MAX_COMPACTION_LAG_MS_CONFIG),
        ),
        TopicConfig.FILE_DELETE_DELAY_MS_CONFIG to listOf(
            ConfigSynonym(LOG_PREFIX + "segment.delete.delay.ms"),
        ),
        TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_CONFIG to listOf(
            ConfigSynonym(LOG_CLEANER_PREFIX + "min.cleanable.ratio"),
        ),
        TopicConfig.CLEANUP_POLICY_CONFIG to listOf(
            ConfigSynonym(LOG_PREFIX + TopicConfig.CLEANUP_POLICY_CONFIG),
        ),
        TopicConfig.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG to listOf(
            ConfigSynonym(TopicConfig.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG),
        ),
        TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG to listOf(
            ConfigSynonym(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG),
        ),
        TopicConfig.COMPRESSION_TYPE_CONFIG to listOf(
            ConfigSynonym(TopicConfig.COMPRESSION_TYPE_CONFIG),
        ),
        TopicConfig.PREALLOCATE_CONFIG to listOf(
            ConfigSynonym(LOG_PREFIX + TopicConfig.PREALLOCATE_CONFIG),
        ),
        TopicConfig.MESSAGE_FORMAT_VERSION_CONFIG to listOf(
            ConfigSynonym(LOG_PREFIX + TopicConfig.MESSAGE_FORMAT_VERSION_CONFIG),
        ),
        TopicConfig.MESSAGE_TIMESTAMP_TYPE_CONFIG to listOf(
            ConfigSynonym(LOG_PREFIX + TopicConfig.MESSAGE_TIMESTAMP_TYPE_CONFIG),
        ),
        TopicConfig.MESSAGE_TIMESTAMP_DIFFERENCE_MAX_MS_CONFIG to listOf(
            ConfigSynonym(LOG_PREFIX + TopicConfig.MESSAGE_TIMESTAMP_DIFFERENCE_MAX_MS_CONFIG),
        ),
        TopicConfig.MESSAGE_TIMESTAMP_BEFORE_MAX_MS_CONFIG to listOf(
            ConfigSynonym(LOG_PREFIX + TopicConfig.MESSAGE_TIMESTAMP_BEFORE_MAX_MS_CONFIG),
        ),
        TopicConfig.MESSAGE_TIMESTAMP_AFTER_MAX_MS_CONFIG to listOf(
            ConfigSynonym(LOG_PREFIX + TopicConfig.MESSAGE_TIMESTAMP_AFTER_MAX_MS_CONFIG),
        ),
        TopicConfig.MESSAGE_DOWNCONVERSION_ENABLE_CONFIG to listOf(
            ConfigSynonym(LOG_PREFIX + TopicConfig.MESSAGE_DOWNCONVERSION_ENABLE_CONFIG),
        ),
        TopicConfig.LOCAL_LOG_RETENTION_MS_CONFIG to listOf(
            ConfigSynonym(LOG_PREFIX + TopicConfig.LOCAL_LOG_RETENTION_MS_CONFIG),
        ),
        TopicConfig.LOCAL_LOG_RETENTION_BYTES_CONFIG to listOf(
            ConfigSynonym(LOG_PREFIX + TopicConfig.LOCAL_LOG_RETENTION_BYTES_CONFIG),
        ),
    )

    /**
     * Map topic config to the server config with the highest priority. Some of these have additional
     * synonyms that can be obtained using `kafka.server.DynamicBrokerConfig.brokerConfigSynonyms`
     * or using `AllTopicConfigSynonyms`.
     */
    val TOPIC_CONFIG_SYNONYMS = ALL_TOPIC_CONFIG_SYNONYMS
        .mapValues { (_, value) -> value[0].name }

    /**
     * Return the server config with the highest priority for `topicConfigName` if it exists. Otherwise,
     * throw NoSuchElementException.
     */
    fun serverSynonym(topicConfigName: String): String {
        return TOPIC_CONFIG_SYNONYMS[topicConfigName]
            ?: throw NoSuchElementException("No server synonym found for $topicConfigName")
    }
}
