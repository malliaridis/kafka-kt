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

package org.apache.kafka.common.config

/**
 *
 * Keys that can be used to configure a topic. These keys are useful when creating or reconfiguring
 * a topic using the AdminClient.
 *
 * The intended pattern is for broker configs to include a `` `log.` `` prefix. For example, to set
 * the default broker cleanup policy, one would set `log.cleanup.policy` instead of
 * `cleanup.policy`. Unfortunately, there are many cases where this pattern is not followed.
 */
object TopicConfig {

    // This is a public API, so we should not remove or alter keys without a discussion and a
    // deprecation period. Eventually this should replace LogConfig.scala.

    const val SEGMENT_BYTES_CONFIG = "segment.bytes"

    const val SEGMENT_BYTES_DOC = "This configuration controls the segment file size for " +
            "the log. Retention and cleaning is always done a file at a time so a larger segment size means " +
            "fewer files but less granular control over retention."

    const val SEGMENT_MS_CONFIG = "segment.ms"

    const val SEGMENT_MS_DOC = "This configuration controls the period of time after " +
            "which Kafka will force the log to roll even if the segment file isn't full to ensure that retention " +
            "can delete or compact old data."

    const val SEGMENT_JITTER_MS_CONFIG = "segment.jitter.ms"

    const val SEGMENT_JITTER_MS_DOC = "The maximum random jitter subtracted from the scheduled " +
            "segment roll time to avoid thundering herds of segment rolling"

    const val SEGMENT_INDEX_BYTES_CONFIG = "segment.index.bytes"

    const val SEGMENT_INDEX_BYTES_DOC = "This configuration controls the size of the index that " +
            "maps offsets to file positions. We preallocate this index file and shrink it only after log " +
            "rolls. You generally should not need to change this setting."

    const val FLUSH_MESSAGES_INTERVAL_CONFIG = "flush.messages"

    const val FLUSH_MESSAGES_INTERVAL_DOC = "This setting allows specifying an interval at " +
            "which we will force an fsync of data written to the log. For example if this was set to 1 " +
            "we would fsync after every message; if it were 5 we would fsync after every five messages. " +
            "In general we recommend you not set this and use replication for durability and allow the " +
            "operating system's background flush capabilities as it is more efficient. This setting can " +
            "be overridden on a per-topic basis (see <a href=\"#topicconfigs\">the per-topic configuration " +
            "section</a>)."

    const val FLUSH_MS_CONFIG = "flush.ms"

    const val FLUSH_MS_DOC = "This setting allows specifying a time interval at which we will " +
            "force an fsync of data written to the log. For example if this was set to 1000 " +
            "we would fsync after 1000 ms had passed. In general we recommend you not set " +
            "this and use replication for durability and allow the operating system's background " +
            "flush capabilities as it is more efficient."

    const val RETENTION_BYTES_CONFIG = "retention.bytes"

    const val RETENTION_BYTES_DOC = "This configuration controls the maximum size a partition " +
            "(which consists of log segments) can grow to before we will discard old log segments to free up space " +
            "if we are using the \"delete\" retention policy. By default there is no size limit only a time limit. " +
            "Since this limit is enforced at the partition level, multiply it by the number of partitions to compute " +
            "the topic retention in bytes."

    const val RETENTION_MS_CONFIG = "retention.ms"

    const val RETENTION_MS_DOC = "This configuration controls the maximum time we will retain a " +
            "log before we will discard old log segments to free up space if we are using the " +
            "\"delete\" retention policy. This represents an SLA on how soon consumers must read " +
            "their data. If set to -1, no time limit is applied."

    const val REMOTE_LOG_STORAGE_ENABLE_CONFIG = "remote.storage.enable"

    const val REMOTE_LOG_STORAGE_ENABLE_DOC =
        "To enable tiered storage for a topic, set this configuration as true. " +
                "You can not disable this config once it is enabled. It will be provided in future versions."

    const val LOCAL_LOG_RETENTION_MS_CONFIG = "local.retention.ms"

    const val LOCAL_LOG_RETENTION_MS_DOC =
        "The number of milli seconds to keep the local log segment before it gets deleted. " +
                "Default value is -2, it represents `retention.ms` value is to be used. The effective value " +
                "should always be less than or equal to `retention.ms` value."

    const val LOCAL_LOG_RETENTION_BYTES_CONFIG = "local.retention.bytes"

    const val LOCAL_LOG_RETENTION_BYTES_DOC =
        "The maximum size of local log segments that can grow for a partition before it " +
                "deletes the old segments. Default value is -2, it represents `retention.bytes` value to be used. " +
                "The effective value should always be less than or equal to `retention.bytes` value."

    const val MAX_MESSAGE_BYTES_CONFIG = "max.message.bytes"

    const val MAX_MESSAGE_BYTES_DOC =
        "The largest record batch size allowed by Kafka (after compression if compression is enabled). " +
                "If this is increased and there are consumers older than 0.10.2, the consumers' fetch " +
                "size must also be increased so that they can fetch record batches this large. " +
                "In the latest message format version, records are always grouped into batches for efficiency. " +
                "In previous message format versions, uncompressed records are not grouped into batches and this " +
                "limit only applies to a single record in that case."

    const val INDEX_INTERVAL_BYTES_CONFIG = "index.interval.bytes"

    const val INDEX_INTERVAL_BYTES_DOC = "This setting controls how frequently " +
            "Kafka adds an index entry to its offset index. The default setting ensures that we index a " +
            "message roughly every 4096 bytes. More indexing allows reads to jump closer to the exact " +
            "position in the log but makes the index larger. You probably don't need to change this."

    const val FILE_DELETE_DELAY_MS_CONFIG = "file.delete.delay.ms"

    const val FILE_DELETE_DELAY_MS_DOC = "The time to wait before deleting a file from the " +
            "filesystem"

    const val DELETE_RETENTION_MS_CONFIG = "delete.retention.ms"

    const val DELETE_RETENTION_MS_DOC = "The amount of time to retain delete tombstone markers " +
            "for <a href=\"#compaction\">log compacted</a> topics. This setting also gives a bound " +
            "on the time in which a consumer must complete a read if they begin from offset 0 " +
            "to ensure that they get a valid snapshot of the final stage (otherwise delete " +
            "tombstones may be collected before they complete their scan)."

    const val MIN_COMPACTION_LAG_MS_CONFIG = "min.compaction.lag.ms"

    const val MIN_COMPACTION_LAG_MS_DOC = "The minimum time a message will remain " +
            "uncompacted in the log. Only applicable for logs that are being compacted."

    const val MAX_COMPACTION_LAG_MS_CONFIG = "max.compaction.lag.ms"

    const val MAX_COMPACTION_LAG_MS_DOC = "The maximum time a message will remain " +
            "ineligible for compaction in the log. Only applicable for logs that are being compacted."

    const val MIN_CLEANABLE_DIRTY_RATIO_CONFIG = "min.cleanable.dirty.ratio"

    const val MIN_CLEANABLE_DIRTY_RATIO_DOC = "This configuration controls how frequently " +
            "the log compactor will attempt to clean the log (assuming <a href=\"#compaction\">log " +
            "compaction</a> is enabled). By default we will avoid cleaning a log where more than " +
            "50% of the log has been compacted. This ratio bounds the maximum space wasted in " +
            "the log by duplicates (at 50% at most 50% of the log could be duplicates). A " +
            "higher ratio will mean fewer, more efficient cleanings but will mean more wasted " +
            "space in the log. If the $MAX_COMPACTION_LAG_MS_CONFIG or the $MIN_COMPACTION_LAG_MS_CONFIG" +
            " configurations are also specified, then the log compactor considers the log to be eligible for compaction " +
            "as soon as either: (i) the dirty ratio threshold has been met and the log has had dirty (uncompacted) " +
            "records for at least the $MIN_COMPACTION_LAG_MS_CONFIG duration, or (ii) if the log has had " +
            "dirty (uncompacted) records for at most the $MAX_COMPACTION_LAG_MS_CONFIG period."

    const val CLEANUP_POLICY_CONFIG = "cleanup.policy"

    const val CLEANUP_POLICY_COMPACT = "compact"

    const val CLEANUP_POLICY_DELETE = "delete"

    const val CLEANUP_POLICY_DOC = "This config designates the retention policy to " +
            "use on log segments. The \"delete\" policy (which is the default) will discard old segments " +
            "when their retention time or size limit has been reached. The \"compact\" policy will enable " +
            "<a href=\"#compaction\">log compaction</a>, which retains the latest value for each key. " +
            "It is also possible to specify both policies in a comma-separated list (e.g. \"delete,compact\"). " +
            "In this case, old segments will be discarded per the retention time and size configuration, " +
            "while retained segments will be compacted."

    const val UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG = "unclean.leader.election.enable"

    const val UNCLEAN_LEADER_ELECTION_ENABLE_DOC = "Indicates whether to enable replicas " +
            "not in the ISR set to be elected as leader as a last resort, even though doing so may result in data " +
            "loss."

    const val MIN_IN_SYNC_REPLICAS_CONFIG = "min.insync.replicas"

    const val MIN_IN_SYNC_REPLICAS_DOC = "When a producer sets acks to \"all\" (or \"-1\"), " +
            "this configuration specifies the minimum number of replicas that must acknowledge " +
            "a write for the write to be considered successful. If this minimum cannot be met, " +
            "then the producer will raise an exception (either NotEnoughReplicas or NotEnoughReplicasAfterAppend)." +
            "<br>When used together, <code>min.insync.replicas</code> and <code>acks</code> " +
            "allow you to enforce greater durability guarantees. A typical scenario would be to " +
            "create a topic with a replication factor of 3, set <code>min.insync.replicas</code> to 2, and " +
            "produce with <code>acks</code> of \"all\". This will ensure that the producer raises an exception " +
            "if a majority of replicas do not receive a write."

    const val COMPRESSION_TYPE_CONFIG = "compression.type"

    const val COMPRESSION_TYPE_DOC = "Specify the final compression type for a given topic. " +
            "This configuration accepts the standard compression codecs ('gzip', 'snappy', 'lz4', 'zstd'). " +
            "It additionally accepts 'uncompressed' which is equivalent to no compression; and 'producer' " +
            "which means retain the original compression codec set by the producer."

    const val PREALLOCATE_CONFIG = "preallocate"

    const val PREALLOCATE_DOC = "True if we should preallocate the file on disk when " +
            "creating a new log segment."

    @Deprecated(
        """since 3.0, removal planned in 4.0. The default value for this config is appropriate
      for most situations."""
    )
    val MESSAGE_FORMAT_VERSION_CONFIG = "message.format.version"

    @Deprecated(
        """since 3.0, removal planned in 4.0. The default value for this config is appropriate
      for most situations."""
    )
    val MESSAGE_FORMAT_VERSION_DOC = "[DEPRECATED] Specify the message format version the broker " +
            "will use to append messages to the logs. The value of this config is always assumed to be `3.0` if " +
            "`inter.broker.protocol.version` is 3.0 or higher (the actual config value is ignored). " +
            "Otherwise, the value should be a valid ApiVersion. Some examples are: 0.10.0, 1.1, 2.8, 3.0. " +
            "By setting a particular message format version, the user is certifying that all the existing messages " +
            "on disk are smaller or equal than the specified version. Setting this value incorrectly " +
            "will cause consumers with older versions to break as they will receive messages with a format " +
            "that they don't understand."

    const val MESSAGE_TIMESTAMP_TYPE_CONFIG = "message.timestamp.type"

    const val MESSAGE_TIMESTAMP_TYPE_DOC = "Define whether the timestamp in the message is " +
            "message create time or log append time. The value should be either `CreateTime` or `LogAppendTime`"

    @Deprecated(
        message = "since 3.6, removal planned in 4.0. Use message.timestamp.before.max.ms and " +
                "message.timestamp.after.max.ms instead",
    )
    const val MESSAGE_TIMESTAMP_DIFFERENCE_MAX_MS_CONFIG = "message.timestamp.difference.max.ms"

    @Deprecated(
        message = "since 3.6, removal planned in 4.0. Use message.timestamp.before.max.ms and " +
                "message.timestamp.after.max.ms instead",
    )
    const val MESSAGE_TIMESTAMP_DIFFERENCE_MAX_MS_DOC = "The maximum difference allowed between " +
            "the timestamp when a broker receives a message and the timestamp specified in the message. If " +
            "message.timestamp.type=CreateTime, a message will be rejected if the difference in timestamp " +
            "exceeds this threshold. This configuration is ignored if message.timestamp.type=LogAppendTime."

    const val MESSAGE_TIMESTAMP_BEFORE_MAX_MS_CONFIG = "message.timestamp.before.max.ms"

    const val MESSAGE_TIMESTAMP_BEFORE_MAX_MS_DOC = "This configuration sets the allowable timestamp " +
            "difference between the broker's timestamp and the message timestamp. The message timestamp " +
            "can be earlier than or equal to the broker's timestamp, with the maximum allowable difference " +
            "determined by the value set in this configuration. If message.timestamp.type=CreateTime, " +
            "the message will be rejected if the difference in timestamps exceeds this specified threshold. " +
            "This configuration is ignored if message.timestamp.type=LogAppendTime."

    const val MESSAGE_TIMESTAMP_AFTER_MAX_MS_CONFIG = "message.timestamp.after.max.ms"

    const val MESSAGE_TIMESTAMP_AFTER_MAX_MS_DOC = "This configuration sets the allowable timestamp " +
            "difference between the message timestamp and the broker's timestamp. The message timestamp can " +
            "be later than or equal to the broker's timestamp, with the maximum allowable difference determined by " +
            "the value set in this configuration. If message.timestamp.type=CreateTime, the message will be rejected " +
            "if the difference in timestamps exceeds this specified threshold. This configuration is ignored " +
            "if message.timestamp.type=LogAppendTime."

    const val MESSAGE_DOWNCONVERSION_ENABLE_CONFIG = "message.downconversion.enable"

    const val MESSAGE_DOWNCONVERSION_ENABLE_DOC = "This configuration controls whether " +
            "down-conversion of message formats is enabled to satisfy consume requests. When set to " +
            "<code>false</code>, broker will not perform down-conversion for consumers expecting an older message " +
            "format. The broker responds with <code>UNSUPPORTED_VERSION</code> error for consume requests " +
            "from such older clients. This configuration does not apply to any message format conversion " +
            "that might be required for replication to followers."
}
