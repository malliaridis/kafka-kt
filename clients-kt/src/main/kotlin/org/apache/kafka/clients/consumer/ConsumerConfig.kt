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

package org.apache.kafka.clients.consumer

import org.apache.kafka.clients.ClientDnsLookup
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.CommonClientConfigs.postProcessReconnectBackoffConfigs
import org.apache.kafka.clients.CommonClientConfigs.postValidateSaslMechanismConfig
import org.apache.kafka.common.IsolationLevel
import org.apache.kafka.common.config.AbstractConfig
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigDef.Importance
import org.apache.kafka.common.config.ConfigDef.NonEmptyString
import org.apache.kafka.common.config.ConfigDef.NonNullValidator
import org.apache.kafka.common.config.ConfigDef.Range.Companion.atLeast
import org.apache.kafka.common.config.ConfigDef.ValidString.Companion.`in`
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.common.config.SecurityConfig
import org.apache.kafka.common.errors.InvalidConfigurationException
import org.apache.kafka.common.metrics.Sensor
import org.apache.kafka.common.requests.JoinGroupRequest
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.utils.Utils
import org.apache.kafka.common.utils.Utils.enumOptions
import org.apache.kafka.common.utils.Utils.propsToMap
import java.util.*
import java.util.concurrent.atomic.AtomicInteger

/**
 * The consumer configuration keys
 */
class ConsumerConfig : AbstractConfig {

    override fun postProcessParsedConfig(parsedValues: Map<String, Any?>): Map<String, Any?> {
        postValidateSaslMechanismConfig(this)
        val refinedConfigs = postProcessReconnectBackoffConfigs(this, parsedValues)
            .toMutableMap()

        maybeOverrideClientId(refinedConfigs)
        return refinedConfigs
    }

    private fun maybeOverrideClientId(configs: MutableMap<String, Any?>) {
        val clientId = getString(CLIENT_ID_CONFIG)
        if (clientId.isNullOrEmpty()) {
            val groupId = getString(GROUP_ID_CONFIG)
            val groupInstanceId = getString(GROUP_INSTANCE_ID_CONFIG)

            if (groupInstanceId != null)
                JoinGroupRequest.validateGroupInstanceId(groupInstanceId)

            val groupInstanceIdPart = groupInstanceId
                ?: (CONSUMER_CLIENT_ID_SEQUENCE.getAndIncrement().toString() + "")
            val generatedClientId = "consumer-$groupId-$groupInstanceIdPart"

            configs[CLIENT_ID_CONFIG] = generatedClientId
        }
    }

    fun maybeOverrideEnableAutoCommit(): Boolean {
        val groupId = getString(CommonClientConfigs.GROUP_ID_CONFIG)
        var enableAutoCommit = getBoolean(ENABLE_AUTO_COMMIT_CONFIG) == true
        if (groupId == null) {
            // overwrite in case of default group id where the config is not explicitly provided
            if (!originals().containsKey(ENABLE_AUTO_COMMIT_CONFIG)) enableAutoCommit = false
            else if (enableAutoCommit) throw InvalidConfigurationException(
                "$ENABLE_AUTO_COMMIT_CONFIG cannot be set to true when default group id (null) " +
                        "is used."
            )
        }
        return enableAutoCommit
    }

    constructor(props: Properties) : super(
        definition = CONFIG,
        originals = propsToMap(props),
    )

    constructor(props: Map<String, Any?>) : super(
        definition = CONFIG,
        originals = props,
    )

    internal constructor(props: Map<String, Any>, doLog: Boolean) : super(
        definition = CONFIG,
        originals = props,
        doLog = doLog,
    )

    companion object {

        private val CONFIG: ConfigDef

        // a list contains all the assignor names that only assign subscribed topics to consumer.
        // Should be updated when new assignor added. This is to help optimize
        // ConsumerCoordinator#performAssignment method
        val ASSIGN_FROM_SUBSCRIBED_ASSIGNORS = listOf(
            RangeAssignor.RANGE_ASSIGNOR_NAME,
            RoundRobinAssignor.ROUNDROBIN_ASSIGNOR_NAME,
            StickyAssignor.STICKY_ASSIGNOR_NAME,
            CooperativeStickyAssignor.COOPERATIVE_STICKY_ASSIGNOR_NAME,
        )

        /*
         * NOTE: DO NOT CHANGE EITHER CONFIG STRINGS OR THEIR JAVA VARIABLE NAMES AS
         * THESE ARE PART OF THE PUBLIC API AND CHANGE WILL BREAK USER CODE.
         */

        /**
         * `group.id`
         */
        val GROUP_ID_CONFIG = CommonClientConfigs.GROUP_ID_CONFIG
        private val GROUP_ID_DOC = CommonClientConfigs.GROUP_ID_DOC

        /**
         * `group.instance.id`
         */
        val GROUP_INSTANCE_ID_CONFIG = CommonClientConfigs.GROUP_INSTANCE_ID_CONFIG
        private val GROUP_INSTANCE_ID_DOC = CommonClientConfigs.GROUP_INSTANCE_ID_DOC

        /**
         * `max.poll.records`
         */
        const val MAX_POLL_RECORDS_CONFIG = "max.poll.records"
        private const val MAX_POLL_RECORDS_DOC =
            "The maximum number of records returned in a single call to poll(). Note, that " +
                    "<code>$MAX_POLL_RECORDS_CONFIG</code> does not impact the underlying " +
                    "fetching behavior. The consumer will cache the records from each fetch " +
                    "request and returns them incrementally from each poll."

        /**
         * `max.poll.interval.ms`
         */
        val MAX_POLL_INTERVAL_MS_CONFIG = CommonClientConfigs.MAX_POLL_INTERVAL_MS_CONFIG
        private val MAX_POLL_INTERVAL_MS_DOC = CommonClientConfigs.MAX_POLL_INTERVAL_MS_DOC

        /**
         * `session.timeout.ms`
         */
        val SESSION_TIMEOUT_MS_CONFIG = CommonClientConfigs.SESSION_TIMEOUT_MS_CONFIG
        private val SESSION_TIMEOUT_MS_DOC = CommonClientConfigs.SESSION_TIMEOUT_MS_DOC

        /**
         * `heartbeat.interval.ms`
         */
        val HEARTBEAT_INTERVAL_MS_CONFIG = CommonClientConfigs.HEARTBEAT_INTERVAL_MS_CONFIG
        private val HEARTBEAT_INTERVAL_MS_DOC = CommonClientConfigs.HEARTBEAT_INTERVAL_MS_DOC

        /**
         * `bootstrap.servers`
         */
        val BOOTSTRAP_SERVERS_CONFIG = CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG

        /**
         * `client.dns.lookup`
         */
        val CLIENT_DNS_LOOKUP_CONFIG = CommonClientConfigs.CLIENT_DNS_LOOKUP_CONFIG

        /**
         * `enable.auto.commit`
         */
        const val ENABLE_AUTO_COMMIT_CONFIG = "enable.auto.commit"
        private const val ENABLE_AUTO_COMMIT_DOC =
            "If true the consumer's offset will be periodically committed in the background."

        /**
         * `auto.commit.interval.ms`
         */
        const val AUTO_COMMIT_INTERVAL_MS_CONFIG = "auto.commit.interval.ms"
        private const val AUTO_COMMIT_INTERVAL_MS_DOC =
            "The frequency in milliseconds that the consumer offsets are auto-committed to Kafka " +
                    "if <code>enable.auto.commit</code> is set to <code>true</code>."

        /**
         * `partition.assignment.strategy`
         */
        const val PARTITION_ASSIGNMENT_STRATEGY_CONFIG = "partition.assignment.strategy"
        private const val PARTITION_ASSIGNMENT_STRATEGY_DOC =
            "A list of class names or class types, ordered by preference, of supported partition " +
                    "assignment strategies that the client will use to distribute partition " +
                    "ownership amongst consumer instances when group management is used. " +
                    "Available options are:" +
                    "<ul>" +
                    "<li><code>org.apache.kafka.clients.consumer.RangeAssignor</code>: Assigns " +
                    "partitions on a per-topic basis.</li>" +
                    "<li><code>org.apache.kafka.clients.consumer.RoundRobinAssignor</code>: " +
                    "Assigns partitions to consumers in a round-robin fashion.</li>" +
                    "<li><code>org.apache.kafka.clients.consumer.StickyAssignor</code>: " +
                    "Guarantees an assignment that is " +
                    "maximally balanced while preserving as many existing partition assignments " +
                    "as possible.</li>" +
                    "<li><code>org.apache.kafka.clients.consumer.CooperativeStickyAssignor</code>: " +
                    "Follows the same StickyAssignor logic, but allows for cooperative " +
                    "rebalancing.</li>" +
                    "</ul>" +
                    "<p>The default assignor is [RangeAssignor, CooperativeStickyAssignor], " +
                    "which will use the RangeAssignor by default, but allows upgrading to the " +
                    "CooperativeStickyAssignor with just a single rolling bounce that removes " +
                    "the RangeAssignor from the list.</p>" +
                    "<p>Implementing the <code>org.apache.kafka.clients.consumer.ConsumerPartitionAssignor</code> " +
                    "interface allows you to plug in a custom assignment strategy.</p>"

        /**
         * `auto.offset.reset`
         */
        const val AUTO_OFFSET_RESET_CONFIG = "auto.offset.reset"
        const val AUTO_OFFSET_RESET_DOC =
            "What to do when there is no initial offset in Kafka or if the current offset does " +
                    "not exist any more on the server (e.g. because that data has been deleted): " +
                    "<ul><li>earliest: automatically reset the offset to the earliest offset" +
                    "<li>latest: automatically reset the offset to the latest offset</li>" +
                    "<li>none: throw exception to the consumer if no previous offset is found " +
                    "for the consumer's group</li>" +
                    "<li>anything else: throw exception to the consumer.</li></ul>"

        /**
         * `fetch.min.bytes`
         */
        const val FETCH_MIN_BYTES_CONFIG = "fetch.min.bytes"
        private const val FETCH_MIN_BYTES_DOC =
            "The minimum amount of data the server should return for a fetch request. If " +
                    "insufficient data is available the request will wait for that much data " +
                    "to accumulate before answering the request. The default setting of 1 byte " +
                    "means that fetch requests are answered as soon as a single byte of data is " +
                    "available or the fetch request times out waiting for data to arrive. " +
                    "Setting this to something greater than 1 will cause the server to wait for " +
                    "larger amounts of data to accumulate which can improve server throughput a " +
                    "bit at the cost of some additional latency."

        /**
         * `fetch.max.bytes`
         */
        const val FETCH_MAX_BYTES_CONFIG = "fetch.max.bytes"
        private const val FETCH_MAX_BYTES_DOC =
            "The maximum amount of data the server should return for a fetch request. Records " +
                    "are fetched in batches by the consumer, and if the first record batch in " +
                    "the first non-empty partition of the fetch is larger than this value, the " +
                    "record batch will still be returned to ensure that the consumer can make " +
                    "progress. As such, this is not a absolute maximum. The maximum record batch " +
                    "size accepted by the broker is defined via <code>message.max.bytes</code> " +
                    "(broker config) or <code>max.message.bytes</code> (topic config). Note that " +
                    "the consumer performs multiple fetches in parallel."
        const val DEFAULT_FETCH_MAX_BYTES = 50 * 1024 * 1024

        /**
         * `fetch.max.wait.ms`
         */
        const val FETCH_MAX_WAIT_MS_CONFIG = "fetch.max.wait.ms"
        private const val FETCH_MAX_WAIT_MS_DOC =
            "The maximum amount of time the server will block before answering the fetch request " +
                    "if there isn't sufficient data to immediately satisfy the requirement given " +
                    "by fetch.min.bytes."

        /**
         * `metadata.max.age.ms`
         */
        val METADATA_MAX_AGE_CONFIG = CommonClientConfigs.METADATA_MAX_AGE_CONFIG

        /**
         * `max.partition.fetch.bytes`
         */
        const val MAX_PARTITION_FETCH_BYTES_CONFIG = "max.partition.fetch.bytes"
        private const val MAX_PARTITION_FETCH_BYTES_DOC =
            "The maximum amount of data per-partition the server will return. Records are " +
                    "fetched in batches by the consumer. If the first record batch in the first " +
                    "non-empty partition of the fetch is larger than this limit, the batch will " +
                    "still be returned to ensure that the consumer can make progress. The " +
                    "maximum record batch size accepted by the broker is defined via " +
                    "<code>message.max.bytes</code> (broker config) or " +
                    "<code>max.message.bytes</code> (topic config). See $FETCH_MAX_BYTES_CONFIG " +
                    "for limiting the consumer request size."
        const val DEFAULT_MAX_PARTITION_FETCH_BYTES = 1 * 1024 * 1024

        /**
         * `send.buffer.bytes`
         */
        val SEND_BUFFER_CONFIG = CommonClientConfigs.SEND_BUFFER_CONFIG

        /**
         * `receive.buffer.bytes`
         */
        val RECEIVE_BUFFER_CONFIG = CommonClientConfigs.RECEIVE_BUFFER_CONFIG

        /**
         * `client.id`
         */
        val CLIENT_ID_CONFIG = CommonClientConfigs.CLIENT_ID_CONFIG

        /**
         * `client.rack`
         */
        val CLIENT_RACK_CONFIG = CommonClientConfigs.CLIENT_RACK_CONFIG

        /**
         * `reconnect.backoff.ms`
         */
        val RECONNECT_BACKOFF_MS_CONFIG = CommonClientConfigs.RECONNECT_BACKOFF_MS_CONFIG

        /**
         * `reconnect.backoff.max.ms`
         */
        val RECONNECT_BACKOFF_MAX_MS_CONFIG = CommonClientConfigs.RECONNECT_BACKOFF_MAX_MS_CONFIG

        /**
         * `retry.backoff.ms`
         */
        val RETRY_BACKOFF_MS_CONFIG = CommonClientConfigs.RETRY_BACKOFF_MS_CONFIG

        /**
         * `metrics.sample.window.ms`
         */
        val METRICS_SAMPLE_WINDOW_MS_CONFIG = CommonClientConfigs.METRICS_SAMPLE_WINDOW_MS_CONFIG

        /**
         * `metrics.num.samples`
         */
        val METRICS_NUM_SAMPLES_CONFIG = CommonClientConfigs.METRICS_NUM_SAMPLES_CONFIG

        /**
         * `metrics.log.level`
         */
        val METRICS_RECORDING_LEVEL_CONFIG = CommonClientConfigs.METRICS_RECORDING_LEVEL_CONFIG

        /**
         * `metric.reporters`
         */
        val METRIC_REPORTER_CLASSES_CONFIG = CommonClientConfigs.METRIC_REPORTER_CLASSES_CONFIG

        /**
         * `auto.include.jmx.reporter`
         */
        @Deprecated("")
        val AUTO_INCLUDE_JMX_REPORTER_CONFIG = CommonClientConfigs.AUTO_INCLUDE_JMX_REPORTER_CONFIG

        /**
         * `check.crcs`
         */
        const val CHECK_CRCS_CONFIG = "check.crcs"
        private const val CHECK_CRCS_DOC =
            "Automatically check the CRC32 of the records consumed. This ensures no on-the-wire " +
                    "or on-disk corruption to the messages occurred. This check adds some " +
                    "overhead, so it may be disabled in cases seeking extreme performance."

        /**
         * `key.deserializer`
         */
        const val KEY_DESERIALIZER_CLASS_CONFIG = "key.deserializer"
        const val KEY_DESERIALIZER_CLASS_DOC =
            "Deserializer class for key that implements the " +
                    "<code>org.apache.kafka.common.serialization.Deserializer</code> interface."

        /**
         * `value.deserializer`
         */
        const val VALUE_DESERIALIZER_CLASS_CONFIG = "value.deserializer"
        const val VALUE_DESERIALIZER_CLASS_DOC =
            "Deserializer class for value that implements the " +
                    "<code>org.apache.kafka.common.serialization.Deserializer</code> interface."

        /**
         * `socket.connection.setup.timeout.ms`
         */
        val SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG =
            CommonClientConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG

        /**
         * `socket.connection.setup.timeout.max.ms`
         */
        val SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG =
            CommonClientConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG

        /**
         * `connections.max.idle.ms`
         */
        val CONNECTIONS_MAX_IDLE_MS_CONFIG = CommonClientConfigs.CONNECTIONS_MAX_IDLE_MS_CONFIG

        /**
         * `request.timeout.ms`
         */
        val REQUEST_TIMEOUT_MS_CONFIG = CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG
        private val REQUEST_TIMEOUT_MS_DOC = CommonClientConfigs.REQUEST_TIMEOUT_MS_DOC

        /**
         * `default.api.timeout.ms`
         */
        val DEFAULT_API_TIMEOUT_MS_CONFIG = CommonClientConfigs.DEFAULT_API_TIMEOUT_MS_CONFIG

        /**
         * `interceptor.classes`
         */
        const val INTERCEPTOR_CLASSES_CONFIG = "interceptor.classes"
        const val INTERCEPTOR_CLASSES_DOC =
            "A list of classes to use as interceptors. Implementing the " +
                    "<code>org.apache.kafka.clients.consumer.ConsumerInterceptor</code> " +
                    "interface allows you to intercept (and possibly mutate) records received " +
                    "by the consumer. By default, there are no interceptors."

        /**
         * `exclude.internal.topics`
         */
        const val EXCLUDE_INTERNAL_TOPICS_CONFIG = "exclude.internal.topics"
        private const val EXCLUDE_INTERNAL_TOPICS_DOC =
            "Whether internal topics matching a subscribed pattern should be excluded from the " +
                    "subscription. It is always possible to explicitly subscribe to an internal " +
                    "topic."
        const val DEFAULT_EXCLUDE_INTERNAL_TOPICS = true

        /**
         * `internal.leave.group.on.close`
         * Whether or not the consumer should leave the group on close. If set to `false` then a
         * rebalance won't occur until `session.timeout.ms` expires.
         *
         * Note: this is an internal configuration and could be changed in the future in a backward
         * incompatible way.
         */
        const val LEAVE_GROUP_ON_CLOSE_CONFIG = "internal.leave.group.on.close"

        /**
         * `internal.throw.on.fetch.stable.offset.unsupported`
         *
         *  Whether or not the consumer should throw when the new stable offset feature is supported.
         * If set to `true` then the client shall crash upon hitting it. The purpose of this flag is
         * to prevent unexpected broker downgrade which makes the offset fetch protection against
         * pending commit invalid. The safest approach is to fail fast to avoid introducing
         * correctness issue.
         *
         * Note: this is an internal configuration and could be changed in the future in a backward
         * incompatible way
         */
        const val THROW_ON_FETCH_STABLE_OFFSET_UNSUPPORTED =
            "internal.throw.on.fetch.stable.offset.unsupported"

        /**
         * `isolation.level`
         */
        const val ISOLATION_LEVEL_CONFIG = "isolation.level"
        const val ISOLATION_LEVEL_DOC =
            "Controls how to read messages written transactionally. If set to " +
                    "<code>read_committed</code>, consumer.poll() will only return transactional " +
                    "messages which have been committed. If set to <code>read_uncommitted</code> " +
                    "(the default), consumer.poll() will return all messages, even transactional " +
                    "messages which have been aborted. Non-transactional messages will be " +
                    "returned unconditionally in either mode. <p>Messages will always be " +
                    "returned in offset order. Hence, in  <code>read_committed</code> mode, " +
                    "consumer.poll() will only return messages up to the last stable offset " +
                    "(LSO), which is the one less than the offset of the first open transaction. " +
                    "In particular any messages appearing after messages belonging to ongoing " +
                    "transactions will be withheld until the relevant transaction has been " +
                    "completed. As a result, <code>read_committed</code> consumers will not be " +
                    "able to read up to the high watermark when there are in flight " +
                    "transactions.</p><p> Further, when in <code>read_committed</code> the " +
                    "seekToEnd method will return the LSO</p>"
        val DEFAULT_ISOLATION_LEVEL = IsolationLevel.READ_UNCOMMITTED.toString().lowercase()

        /** `allow.auto.create.topics`  */
        const val ALLOW_AUTO_CREATE_TOPICS_CONFIG = "allow.auto.create.topics"
        private const val ALLOW_AUTO_CREATE_TOPICS_DOC =
            "Allow automatic topic creation on the broker when subscribing to or assigning a " +
                    "topic. A topic being subscribed to will be automatically created only if the" +
                    " broker allows for it using `auto.create.topics.enable` broker " +
                    "configuration. This configuration must be set to `false` when using brokers " +
                    "older than 0.11.0"
        const val DEFAULT_ALLOW_AUTO_CREATE_TOPICS = true

        /**
         * `security.providers`
         */
        const val SECURITY_PROVIDERS_CONFIG = SecurityConfig.SECURITY_PROVIDERS_CONFIG
        private const val SECURITY_PROVIDERS_DOC = SecurityConfig.SECURITY_PROVIDERS_DOC
        private val CONSUMER_CLIENT_ID_SEQUENCE = AtomicInteger(1)

        init {
            CONFIG = ConfigDef().define(
                name = BOOTSTRAP_SERVERS_CONFIG,
                type = ConfigDef.Type.LIST,
                defaultValue = emptyList<Any>(),
                validator = NonNullValidator(),
                importance = Importance.HIGH,
                documentation = CommonClientConfigs.BOOTSTRAP_SERVERS_DOC,
            ).define(
                name = CLIENT_DNS_LOOKUP_CONFIG,
                type = ConfigDef.Type.STRING,
                defaultValue = ClientDnsLookup.USE_ALL_DNS_IPS.toString(),
                validator = `in`(
                    ClientDnsLookup.USE_ALL_DNS_IPS.toString(),
                    ClientDnsLookup.RESOLVE_CANONICAL_BOOTSTRAP_SERVERS_ONLY.toString(),
                ),
                importance = Importance.MEDIUM,
                documentation = CommonClientConfigs.CLIENT_DNS_LOOKUP_DOC,
            ).define(
                name = GROUP_ID_CONFIG,
                type = ConfigDef.Type.STRING,
                defaultValue = null,
                importance = Importance.HIGH,
                documentation = GROUP_ID_DOC,
            ).define(
                name = GROUP_INSTANCE_ID_CONFIG,
                type = ConfigDef.Type.STRING,
                defaultValue = null,
                validator = NonEmptyString(),
                importance = Importance.MEDIUM,
                documentation = GROUP_INSTANCE_ID_DOC,
            ).define(
                name = SESSION_TIMEOUT_MS_CONFIG,
                type = ConfigDef.Type.INT,
                defaultValue = 45000,
                importance = Importance.HIGH,
                documentation = SESSION_TIMEOUT_MS_DOC,
            ).define(
                name = HEARTBEAT_INTERVAL_MS_CONFIG,
                type = ConfigDef.Type.INT,
                defaultValue = 3000,
                importance = Importance.HIGH,
                documentation = HEARTBEAT_INTERVAL_MS_DOC,
            ).define(
                name = PARTITION_ASSIGNMENT_STRATEGY_CONFIG,
                type = ConfigDef.Type.LIST,
                defaultValue = listOf(
                    RangeAssignor::class.java,
                    CooperativeStickyAssignor::class.java,
                ),
                validator = NonNullValidator(),
                importance = Importance.MEDIUM,
                documentation = PARTITION_ASSIGNMENT_STRATEGY_DOC,
            ).define(
                name = METADATA_MAX_AGE_CONFIG,
                type = ConfigDef.Type.LONG,
                defaultValue = 5 * 60 * 1000,
                validator = atLeast(0),
                importance = Importance.LOW,
                documentation = CommonClientConfigs.METADATA_MAX_AGE_DOC,
            ).define(
                name = ENABLE_AUTO_COMMIT_CONFIG,
                type = ConfigDef.Type.BOOLEAN,
                defaultValue = true,
                importance = Importance.MEDIUM,
                documentation = ENABLE_AUTO_COMMIT_DOC,
            ).define(
                name = AUTO_COMMIT_INTERVAL_MS_CONFIG,
                type = ConfigDef.Type.INT,
                defaultValue = 5000,
                validator = atLeast(0),
                importance = Importance.LOW,
                documentation = AUTO_COMMIT_INTERVAL_MS_DOC,
            ).define(
                name = CLIENT_ID_CONFIG,
                type = ConfigDef.Type.STRING,
                defaultValue = "",
                importance = Importance.LOW,
                documentation = CommonClientConfigs.CLIENT_ID_DOC,
            ).define(
                name = CLIENT_RACK_CONFIG,
                type = ConfigDef.Type.STRING,
                defaultValue = "",
                importance = Importance.LOW,
                documentation = CommonClientConfigs.CLIENT_RACK_DOC,
            ).define(
                name = MAX_PARTITION_FETCH_BYTES_CONFIG,
                type = ConfigDef.Type.INT,
                defaultValue = DEFAULT_MAX_PARTITION_FETCH_BYTES,
                validator = atLeast(0),
                importance = Importance.HIGH,
                documentation = MAX_PARTITION_FETCH_BYTES_DOC,
            ).define(
                name = SEND_BUFFER_CONFIG,
                type = ConfigDef.Type.INT,
                defaultValue = 128 * 1024,
                validator = atLeast(CommonClientConfigs.SEND_BUFFER_LOWER_BOUND),
                importance = Importance.MEDIUM,
                documentation = CommonClientConfigs.SEND_BUFFER_DOC,
            ).define(
                name = RECEIVE_BUFFER_CONFIG,
                type = ConfigDef.Type.INT,
                defaultValue = 64 * 1024,
                validator = atLeast(CommonClientConfigs.RECEIVE_BUFFER_LOWER_BOUND),
                importance = Importance.MEDIUM,
                documentation = CommonClientConfigs.RECEIVE_BUFFER_DOC,
            ).define(
                name = FETCH_MIN_BYTES_CONFIG,
                type = ConfigDef.Type.INT,
                defaultValue = 1,
                validator = atLeast(0),
                importance = Importance.HIGH,
                documentation = FETCH_MIN_BYTES_DOC,
            ).define(
                name = FETCH_MAX_BYTES_CONFIG,
                type = ConfigDef.Type.INT,
                defaultValue = DEFAULT_FETCH_MAX_BYTES,
                validator = atLeast(0),
                importance = Importance.MEDIUM,
                documentation = FETCH_MAX_BYTES_DOC,
            ).define(
                name = FETCH_MAX_WAIT_MS_CONFIG,
                type = ConfigDef.Type.INT,
                defaultValue = 500,
                validator = atLeast(0),
                importance = Importance.LOW,
                documentation = FETCH_MAX_WAIT_MS_DOC,
            ).define(
                name = RECONNECT_BACKOFF_MS_CONFIG,
                type = ConfigDef.Type.LONG,
                defaultValue = 50L,
                validator = atLeast(0L),
                importance = Importance.LOW,
                documentation = CommonClientConfigs.RECONNECT_BACKOFF_MS_DOC,
            ).define(
                name = RECONNECT_BACKOFF_MAX_MS_CONFIG,
                type = ConfigDef.Type.LONG,
                defaultValue = 1000L,
                validator = atLeast(0L),
                importance = Importance.LOW,
                documentation = CommonClientConfigs.RECONNECT_BACKOFF_MAX_MS_DOC,
            ).define(
                name = RETRY_BACKOFF_MS_CONFIG,
                type = ConfigDef.Type.LONG,
                defaultValue = 100L,
                validator = atLeast(0L),
                importance = Importance.LOW,
                documentation = CommonClientConfigs.RETRY_BACKOFF_MS_DOC,
            ).define(
                name = AUTO_OFFSET_RESET_CONFIG,
                type = ConfigDef.Type.STRING,
                defaultValue = OffsetResetStrategy.LATEST.toString(),
                validator = `in`(*enumOptions(OffsetResetStrategy::class.java)),
                importance = Importance.MEDIUM,
                documentation = AUTO_OFFSET_RESET_DOC,
            ).define(
                name = CHECK_CRCS_CONFIG,
                type = ConfigDef.Type.BOOLEAN,
                defaultValue = true,
                importance = Importance.LOW,
                documentation = CHECK_CRCS_DOC,
            ).define(
                name = METRICS_SAMPLE_WINDOW_MS_CONFIG,
                type = ConfigDef.Type.LONG,
                defaultValue = 30000,
                validator = atLeast(0),
                importance = Importance.LOW,
                documentation = CommonClientConfigs.METRICS_SAMPLE_WINDOW_MS_DOC,
            ).define(
                name = METRICS_NUM_SAMPLES_CONFIG,
                type = ConfigDef.Type.INT,
                defaultValue = 2,
                validator = atLeast(1),
                importance = Importance.LOW,
                documentation = CommonClientConfigs.METRICS_NUM_SAMPLES_DOC,
            ).define(
                name = METRICS_RECORDING_LEVEL_CONFIG,
                type = ConfigDef.Type.STRING,
                defaultValue = Sensor.RecordingLevel.INFO.toString(),
                validator = `in`(
                    Sensor.RecordingLevel.INFO.toString(),
                    Sensor.RecordingLevel.DEBUG.toString(),
                    Sensor.RecordingLevel.TRACE.toString(),
                ),
                importance = Importance.LOW,
                documentation = CommonClientConfigs.METRICS_RECORDING_LEVEL_DOC,
            ).define(
                name = METRIC_REPORTER_CLASSES_CONFIG,
                type = ConfigDef.Type.LIST,
                defaultValue = emptyList<Any>(),
                validator = NonNullValidator(),
                importance = Importance.LOW,
                documentation = CommonClientConfigs.METRIC_REPORTER_CLASSES_DOC,
            ).define(
                name = AUTO_INCLUDE_JMX_REPORTER_CONFIG,
                type = ConfigDef.Type.BOOLEAN,
                defaultValue = true,
                importance = Importance.LOW,
                documentation = CommonClientConfigs.AUTO_INCLUDE_JMX_REPORTER_DOC,
            ).define(
                name = KEY_DESERIALIZER_CLASS_CONFIG,
                type = ConfigDef.Type.CLASS,
                importance = Importance.HIGH,
                documentation = KEY_DESERIALIZER_CLASS_DOC,
            ).define(
                name = VALUE_DESERIALIZER_CLASS_CONFIG,
                type = ConfigDef.Type.CLASS,
                importance = Importance.HIGH,
                documentation = VALUE_DESERIALIZER_CLASS_DOC,
            ).define(
                name = REQUEST_TIMEOUT_MS_CONFIG,
                type = ConfigDef.Type.INT,
                defaultValue = 30000,
                validator = atLeast(0),
                importance = Importance.MEDIUM,
                documentation = REQUEST_TIMEOUT_MS_DOC,
            ).define(
                name = DEFAULT_API_TIMEOUT_MS_CONFIG,
                type = ConfigDef.Type.INT,
                defaultValue = 60 * 1000,
                validator = atLeast(0),
                importance = Importance.MEDIUM,
                documentation = CommonClientConfigs.DEFAULT_API_TIMEOUT_MS_DOC,
            ).define(
                name = SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG,
                type = ConfigDef.Type.LONG,
                defaultValue = CommonClientConfigs.DEFAULT_SOCKET_CONNECTION_SETUP_TIMEOUT_MS,
                importance = Importance.MEDIUM,
                documentation = CommonClientConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MS_DOC,
            ).define(
                name = SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG,
                type = ConfigDef.Type.LONG,
                defaultValue = CommonClientConfigs.DEFAULT_SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS,
                importance = Importance.MEDIUM,
                documentation = CommonClientConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_DOC,
            ).define(
                name = CONNECTIONS_MAX_IDLE_MS_CONFIG,
                type = ConfigDef.Type.LONG,
                // default is set to be a bit lower than the server default (10 min), to avoid both
                // client and server closing connection at same time
                defaultValue = 9 * 60 * 1000,
                importance = Importance.MEDIUM,
                documentation = CommonClientConfigs.CONNECTIONS_MAX_IDLE_MS_DOC,
            ).define(
                name = INTERCEPTOR_CLASSES_CONFIG,
                type = ConfigDef.Type.LIST,
                defaultValue = emptyList<Any>(),
                validator = NonNullValidator(),
                importance = Importance.LOW,
                documentation = INTERCEPTOR_CLASSES_DOC,
            ).define(
                name = MAX_POLL_RECORDS_CONFIG,
                type = ConfigDef.Type.INT,
                defaultValue = 500,
                validator = atLeast(1),
                importance = Importance.MEDIUM,
                documentation = MAX_POLL_RECORDS_DOC,
            ).define(
                name = MAX_POLL_INTERVAL_MS_CONFIG,
                type = ConfigDef.Type.INT,
                defaultValue = 300000,
                validator = atLeast(1),
                importance = Importance.MEDIUM,
                documentation = MAX_POLL_INTERVAL_MS_DOC,
            ).define(
                name = EXCLUDE_INTERNAL_TOPICS_CONFIG,
                type = ConfigDef.Type.BOOLEAN,
                defaultValue = DEFAULT_EXCLUDE_INTERNAL_TOPICS,
                importance = Importance.MEDIUM,
                documentation = EXCLUDE_INTERNAL_TOPICS_DOC,
            ).defineInternal(
                name = LEAVE_GROUP_ON_CLOSE_CONFIG,
                type = ConfigDef.Type.BOOLEAN,
                defaultValue = true,
                importance = Importance.LOW,
            ).defineInternal(
                name = THROW_ON_FETCH_STABLE_OFFSET_UNSUPPORTED,
                type = ConfigDef.Type.BOOLEAN,
                defaultValue = false,
                importance = Importance.LOW,
            ).define(
                name = ISOLATION_LEVEL_CONFIG,
                type = ConfigDef.Type.STRING,
                defaultValue = DEFAULT_ISOLATION_LEVEL,
                validator = `in`(
                    IsolationLevel.READ_COMMITTED.toString().lowercase(),
                    IsolationLevel.READ_UNCOMMITTED.toString().lowercase(),
                ),
                importance = Importance.MEDIUM,
                documentation = ISOLATION_LEVEL_DOC,
            ).define(
                name = ALLOW_AUTO_CREATE_TOPICS_CONFIG,
                type = ConfigDef.Type.BOOLEAN,
                defaultValue = DEFAULT_ALLOW_AUTO_CREATE_TOPICS,
                importance = Importance.MEDIUM,
                documentation = ALLOW_AUTO_CREATE_TOPICS_DOC,
            ).define(
                // security support
                name = SECURITY_PROVIDERS_CONFIG,
                type = ConfigDef.Type.STRING,
                defaultValue = null,
                importance = Importance.LOW,
                documentation = SECURITY_PROVIDERS_DOC,
            ).define(
                name = CommonClientConfigs.SECURITY_PROTOCOL_CONFIG,
                type = ConfigDef.Type.STRING,
                defaultValue = CommonClientConfigs.DEFAULT_SECURITY_PROTOCOL,
                validator = `in`(*enumOptions(SecurityProtocol::class.java)),
                importance = Importance.MEDIUM,
                documentation = CommonClientConfigs.SECURITY_PROTOCOL_DOC,
            )
                .withClientSslSupport()
                .withClientSaslSupport()
        }

        internal fun appendDeserializerToConfig(
            configs: Map<String, Any?>,
            keyDeserializer: Deserializer<*>?,
            valueDeserializer: Deserializer<*>?
        ): Map<String, Any?> {
            // validate deserializer configuration, if the passed deserializer instance is null, the user must explicitly set a valid deserializer configuration value
            val newConfigs = configs.toMutableMap()
            if (keyDeserializer != null)
                newConfigs[KEY_DESERIALIZER_CLASS_CONFIG] = keyDeserializer.javaClass
            else if (newConfigs[KEY_DESERIALIZER_CLASS_CONFIG] == null) throw ConfigException(
                KEY_DESERIALIZER_CLASS_CONFIG, null, "must be non-null."
            )
            if (valueDeserializer != null)
                newConfigs[VALUE_DESERIALIZER_CLASS_CONFIG] = valueDeserializer.javaClass
            else if (newConfigs[VALUE_DESERIALIZER_CLASS_CONFIG] == null) throw ConfigException(
                VALUE_DESERIALIZER_CLASS_CONFIG, null, "must be non-null."
            )
            return newConfigs
        }

        fun configNames(): Set<String> = CONFIG.names()

        fun configDef(): ConfigDef = ConfigDef(CONFIG)

        @JvmStatic
        fun main(args: Array<String>) {
            println(CONFIG.toHtml(4, { config -> "consumerconfigs_$config" }))
        }
    }
}
