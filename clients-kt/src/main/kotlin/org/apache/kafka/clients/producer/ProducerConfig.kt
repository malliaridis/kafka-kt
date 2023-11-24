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

package org.apache.kafka.clients.producer

import org.apache.kafka.clients.ClientDnsLookup
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.CommonClientConfigs.postProcessReconnectBackoffConfigs
import org.apache.kafka.clients.CommonClientConfigs.postValidateSaslMechanismConfig
import org.apache.kafka.common.config.AbstractConfig
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigDef.Importance
import org.apache.kafka.common.config.ConfigDef.NonEmptyString
import org.apache.kafka.common.config.ConfigDef.NonNullValidator
import org.apache.kafka.common.config.ConfigDef.Range.Companion.atLeast
import org.apache.kafka.common.config.ConfigDef.Range.Companion.between
import org.apache.kafka.common.config.ConfigDef.ValidString.Companion.`in`
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.common.config.SecurityConfig
import org.apache.kafka.common.metrics.Sensor
import org.apache.kafka.common.record.CompressionType
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.serialization.Serializer
import org.apache.kafka.common.utils.Utils.enumOptions
import org.apache.kafka.common.utils.Utils.propsToMap
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.util.*
import java.util.concurrent.atomic.AtomicInteger

/**
 * Configuration for the Kafka Producer. Documentation for these configurations can be found in the
 * [Kafka documentation](http://kafka.apache.org/documentation.html#producerconfigs)
 */
class ProducerConfig : AbstractConfig {

    constructor(props: Properties) : super(
        definition = CONFIG,
        originals = propsToMap(props),
    )

    constructor(props: Map<String, Any?>) : super(
        definition = CONFIG,
        originals = props,
    )

    internal constructor(props: Map<String, Any?>, doLog: Boolean) : super(
        definition = CONFIG,
        originals = props,
        doLog = doLog,
    )

    override fun postProcessParsedConfig(parsedValues: Map<String, Any?>): Map<String, Any?> {
        postValidateSaslMechanismConfig(this)
        val refinedConfigs = postProcessReconnectBackoffConfigs(
            config = this,
            parsedValues = parsedValues,
        ).toMutableMap()

        postProcessAndValidateIdempotenceConfigs(refinedConfigs)
        maybeOverrideClientId(refinedConfigs)
        return refinedConfigs
    }

    private fun maybeOverrideClientId(configs: MutableMap<String, Any?>) {
        val refinedClientId: String = getString(CLIENT_ID_CONFIG) ?: run {
            val transactionalId: String = getString(TRANSACTIONAL_ID_CONFIG)!!
            "producer-$transactionalId"
        }
        configs[CLIENT_ID_CONFIG] = refinedClientId
    }

    private fun postProcessAndValidateIdempotenceConfigs(configs: MutableMap<String, Any?>) {
        val originalConfigs: Map<String, Any?> = originals()
        val acksStr = parseAcks(getString(ACKS_CONFIG)!!)
        configs[ACKS_CONFIG] = acksStr
        val userConfiguredIdempotence = originals().containsKey(ENABLE_IDEMPOTENCE_CONFIG)
        var idempotenceEnabled = getBoolean(ENABLE_IDEMPOTENCE_CONFIG)!!
        var shouldDisableIdempotence = false

        // For idempotence producers, values for `retries` and `acks` and
        // `max.in.flight.requests.per.connection` need validation
        if (idempotenceEnabled) {
            val retries = getInt(RETRIES_CONFIG)
            if (retries == 0) {
                if (userConfiguredIdempotence) throw ConfigException(
                    "Must set $RETRIES_CONFIG to non-zero when using the idempotent producer."
                )
                log.info("Idempotence will be disabled because {} is set to 0.", RETRIES_CONFIG)
                shouldDisableIdempotence = true
            }
            val acks = acksStr.toShort()
            if (acks != (-1).toShort()) {
                if (userConfiguredIdempotence) throw ConfigException(
                    "Must set $ACKS_CONFIG to all in order to use the idempotent producer. " +
                            "Otherwise we cannot guarantee idempotence."
                )

                log.info(
                    "Idempotence will be disabled because {} is set to {}, not set to 'all'.",
                    ACKS_CONFIG,
                    acks
                )
                shouldDisableIdempotence = true
            }
            val inFlightConnection = getInt(MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION)!!
            if (MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION_FOR_IDEMPOTENCE < inFlightConnection) {
                if (userConfiguredIdempotence) throw ConfigException(
                    "Must set $MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION to at most 5 to use the " +
                            "idempotent producer."
                )

                log.warn(
                    "Idempotence will be disabled because {} is set to {}, which is greater than " +
                            "5. Please note that in v4.0.0 and onward, this will become an error.",
                    MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,
                    inFlightConnection,
                )
                shouldDisableIdempotence = true
            }
        }
        if (shouldDisableIdempotence) {
            configs[ENABLE_IDEMPOTENCE_CONFIG] = false
            idempotenceEnabled = false
        }

        // validate `transaction.id` after validating idempotence dependant configs because
        // `enable.idempotence` config might be overridden
        val userConfiguredTransactions = originalConfigs.containsKey(TRANSACTIONAL_ID_CONFIG)
        if (!idempotenceEnabled && userConfiguredTransactions) throw ConfigException(
            "Cannot set a $TRANSACTIONAL_ID_CONFIG without also enabling idempotence."
        )
    }

    @Suppress("LargeClass")
    companion object {

        private val log: Logger = LoggerFactory.getLogger(ProducerConfig::class.java)

        /*
         * NOTE: DO NOT CHANGE EITHER CONFIG STRINGS OR THEIR JAVA VARIABLE NAMES AS THESE ARE PART
         * OF THE PUBLIC API AND CHANGE WILL BREAK USER CODE.
         */
        private val CONFIG: ConfigDef

        /**
         * `bootstrap.servers`
         */
        val BOOTSTRAP_SERVERS_CONFIG = CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG

        /**
         * `client.dns.lookup`
         */
        val CLIENT_DNS_LOOKUP_CONFIG = CommonClientConfigs.CLIENT_DNS_LOOKUP_CONFIG

        /**
         * `metadata.max.age.ms`
         */
        val METADATA_MAX_AGE_CONFIG = CommonClientConfigs.METADATA_MAX_AGE_CONFIG
        private val METADATA_MAX_AGE_DOC = CommonClientConfigs.METADATA_MAX_AGE_DOC

        /**
         * `metadata.max.idle.ms`
         */
        val METADATA_MAX_IDLE_CONFIG = "metadata.max.idle.ms"
        private val METADATA_MAX_IDLE_DOC =
            "Controls how long the producer will cache metadata for a topic that's idle. If the " +
                    "elapsed time since a topic was last produced to exceeds the metadata idle " +
                    "duration, then the topic's metadata is forgotten and the next access to it " +
                    "will force a metadata fetch request."

        /**
         * `batch.size`
         */
        val BATCH_SIZE_CONFIG = "batch.size"
        private val BATCH_SIZE_DOC =
            "The producer will attempt to batch records together into fewer requests whenever " +
                    "multiple records are being sent to the same partition. This helps " +
                    "performance on both the client and the server. This configuration controls " +
                    "the default batch size in bytes. " +
                    "<p>No attempt will be made to batch records larger than this size. " +
                    "<p>Requests sent to brokers will contain multiple batches, one for each " +
                    "partition with data available to be sent. " +
                    "<p>A small batch size will make batching less common and may reduce " +
                    "throughput (a batch size of zero will disable batching entirely). A very " +
                    "large batch size may use memory a bit more wastefully as we will always " +
                    "allocate a buffer of the specified batch size in anticipation of additional " +
                    "records." +
                    "<p>Note: This setting gives the upper bound of the batch size to be sent. " +
                    "If we have fewer than this many bytes accumulated for this partition, we " +
                    "will 'linger' for the <code>linger.ms</code> time waiting for more records " +
                    "to show up. This <code>linger.ms</code> setting defaults to 0, which means " +
                    "we'll immediately send out a record even the accumulated batch size is " +
                    "under this <code>batch.size</code> setting."

        /**
         * `partitioner.adaptive.partitioning.enable`
         */
        val PARTITIONER_ADPATIVE_PARTITIONING_ENABLE_CONFIG =
            "partitioner.adaptive.partitioning.enable"
        private val PARTITIONER_ADPATIVE_PARTITIONING_ENABLE_DOC =
            "When set to 'true', the producer will try to adapt to broker performance and " +
                    "produce more messages to partitions hosted on faster brokers. If 'false', " +
                    "producer will try to distribute messages uniformly. Note: this setting has " +
                    "no effect if a custom partitioner is used"

        /**
         * `partitioner.availability.timeout.ms`
         */
        val PARTITIONER_AVAILABILITY_TIMEOUT_MS_CONFIG = "partitioner.availability.timeout.ms"
        private val PARTITIONER_AVAILABILITY_TIMEOUT_MS_DOC =
            "If a broker cannot process produce requests from a partition for " +
                    "<code>$PARTITIONER_AVAILABILITY_TIMEOUT_MS_CONFIG</code> time, the " +
                    "partitioner treats that partition as not available. If the value is 0, " +
                    "this logic is disabled. Note: this setting has no effect if a custom " +
                    "partitioner is used or " +
                    "<code>$PARTITIONER_ADPATIVE_PARTITIONING_ENABLE_CONFIG</code> is set to " +
                    "'false'"

        /**
         * `partitioner.ignore.keys`
         */
        val PARTITIONER_IGNORE_KEYS_CONFIG = "partitioner.ignore.keys"
        private val PARTITIONER_IGNORE_KEYS_DOC =
            "When set to 'true' the producer won't use record keys to choose a partition. If " +
                    "'false', producer would choose a partition based on a hash of the key when " +
                    "a key is present. Note: this setting has no effect if a custom partitioner " +
                    "is used."

        /**
         * `acks`
         */
        val ACKS_CONFIG = "acks"
        private val ACKS_DOC =
            "The number of acknowledgments the producer requires the leader to have received " +
                    "before considering a request complete. This controls the durability of " +
                    "records that are sent. The following settings are allowed:" +
                    "<ul> " +
                    "<li><code>acks=0</code> If set to zero then the producer will not wait for " +
                    "any acknowledgment from the server at all. The record will be immediately " +
                    "added to the socket buffer and considered sent. No guarantee can be made " +
                    "that the server has received the record in this case, and the " +
                    "<code>retries</code> configuration will not take effect (as the client " +
                    "won't generally know of any failures). The offset given back for each " +
                    "record will always be set to <code>-1</code>. " +
                    "<li><code>acks=1</code> This will mean the leader will write the record to " +
                    "its local log but will respond without awaiting full acknowledgement from " +
                    "all followers. In this case should the leader fail immediately after " +
                    "acknowledging the record but before the followers have replicated it then " +
                    "the record will be lost. " +
                    "<li><code>acks=all</code> This means the leader will wait for the full set " +
                    "of in-sync replicas to acknowledge the record. This guarantees that the " +
                    "record will not be lost as long as at least one in-sync replica remains " +
                    "alive. This is the strongest available guarantee. This is equivalent to the " +
                    "acks=-1 setting.</ul>" +
                    "<p>Note that enabling idempotence requires this config value to be 'all'. " +
                    "If conflicting configurations are set and idempotence is not explicitly " +
                    "enabled, idempotence is disabled."

        /**
         * `linger.ms`
         */
        val LINGER_MS_CONFIG = "linger.ms"
        private val LINGER_MS_DOC =
            "The producer groups together any records that arrive in between request " +
                    "transmissions into a single batched request. Normally this occurs only " +
                    "under load when records arrive faster than they can be sent out. However " +
                    "in some circumstances the client may want to reduce the number of requests " +
                    "even under moderate load. This setting accomplishes this by adding a small " +
                    "amount of artificial delay&mdash;that is, rather than immediately sending " +
                    "out a record, the producer will wait for up to the given delay to allow " +
                    "other records to be sent so that the sends can be batched together. This " +
                    "can be thought of as analogous to Nagle's algorithm in TCP. This setting " +
                    "gives the upper bound on the delay for batching: once we get " +
                    "<code>$BATCH_SIZE_CONFIG</code> worth of records for a partition it will " +
                    "be sent immediately regardless of this setting, however if we have fewer " +
                    "than this many bytes accumulated for this partition we will 'linger' for " +
                    "the specified time waiting for more records to show up. This setting " +
                    "defaults to 0 (i.e. no delay). Setting <code>$LINGER_MS_CONFIG=5</code>, " +
                    "for example, would have the effect of reducing the number of requests sent " +
                    "but would add up to 5ms of latency to records sent in the absence of load."

        /**
         * `request.timeout.ms`
         */
        val REQUEST_TIMEOUT_MS_CONFIG = CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG
        private val REQUEST_TIMEOUT_MS_DOC =
            "${CommonClientConfigs.REQUEST_TIMEOUT_MS_DOC} This should be larger than " +
                    "<code>replica.lag.time.max.ms</code> (a broker configuration) to reduce the " +
                    "possibility of message duplication due to unnecessary producer retries."

        /**
         * `delivery.timeout.ms`
         */
        val DELIVERY_TIMEOUT_MS_CONFIG = "delivery.timeout.ms"
        private val DELIVERY_TIMEOUT_MS_DOC =
            "An upper bound on the time to report success or failure after a call to " +
                    "<code>send()</code> returns. This limits the total time that a record will " +
                    "be delayed prior to sending, the time to await acknowledgement from the " +
                    "broker (if expected), and the time allowed for retriable send failures. The " +
                    "producer may report failure to send a record earlier than this config if " +
                    "either an unrecoverable error is encountered, the retries have been " +
                    "exhausted, or the record is added to a batch which reached an earlier " +
                    "delivery expiration deadline. The value of this config should be greater " +
                    "than or equal to the sum of <code>$REQUEST_TIMEOUT_MS_CONFIG</code> and " +
                    "<code>$LINGER_MS_CONFIG</code>."

        /**
         * `client.id`
         */
        val CLIENT_ID_CONFIG = CommonClientConfigs.CLIENT_ID_CONFIG

        /**
         * `send.buffer.bytes`
         */
        val SEND_BUFFER_CONFIG = CommonClientConfigs.SEND_BUFFER_CONFIG

        /**
         * `receive.buffer.bytes`
         */
        val RECEIVE_BUFFER_CONFIG = CommonClientConfigs.RECEIVE_BUFFER_CONFIG

        /**
         * `max.request.size`
         */
        val MAX_REQUEST_SIZE_CONFIG = "max.request.size"
        private val MAX_REQUEST_SIZE_DOC =
            "The maximum size of a request in bytes. This setting will limit the number of " +
                    "record batches the producer will send in a single request to avoid sending " +
                    "huge requests. This is also effectively a cap on the maximum uncompressed " +
                    "record batch size. Note that the server has its own cap on the record batch " +
                    "size (after compression if compression is enabled) which may be different " +
                    "from this."

        /**
         * `reconnect.backoff.ms`
         */
        val RECONNECT_BACKOFF_MS_CONFIG = CommonClientConfigs.RECONNECT_BACKOFF_MS_CONFIG

        /**
         * `reconnect.backoff.max.ms`
         */
        val RECONNECT_BACKOFF_MAX_MS_CONFIG = CommonClientConfigs.RECONNECT_BACKOFF_MAX_MS_CONFIG

        /**
         * `max.block.ms`
         */
        val MAX_BLOCK_MS_CONFIG = "max.block.ms"
        private val MAX_BLOCK_MS_DOC =
            "The configuration controls how long the <code>KafkaProducer</code>'s " +
                    "<code>send()</code>, <code>partitionsFor()</code>, " +
                    "<code>initTransactions()</code>, <code>sendOffsetsToTransaction()</code>, " +
                    "<code>commitTransaction()</code> and <code>abortTransaction()</code> " +
                    "methods will block. For <code>send()</code> this timeout bounds the total " +
                    "time waiting for both metadata fetch and buffer allocation (blocking in " +
                    "the user-supplied serializers or partitioner is not counted against this " +
                    "timeout). For <code>partitionsFor()</code> this timeout bounds the time " +
                    "spent waiting for metadata if it is unavailable. The transaction-related " +
                    "methods always block, but may timeout if the transaction coordinator could " +
                    "not be discovered or did not respond within the timeout."

        /**
         * `buffer.memory`
         */
        val BUFFER_MEMORY_CONFIG = "buffer.memory"
        private val BUFFER_MEMORY_DOC =
            "The total bytes of memory the producer can use to buffer records waiting to be sent " +
                    "to the server. If records are sent faster than they can be delivered to the " +
                    "server the producer will block for <code>$MAX_BLOCK_MS_CONFIG</code> after " +
                    "which it will throw an exception." +
                    "<p>This setting should correspond roughly to the total memory the producer " +
                    "will use, but is not a hard bound since not all memory the producer uses is " +
                    "used for buffering. Some additional memory will be used for compression (if " +
                    "compression is enabled) as well as for maintaining in-flight requests."

        /**
         * `retry.backoff.ms`
         */
        val RETRY_BACKOFF_MS_CONFIG = CommonClientConfigs.RETRY_BACKOFF_MS_CONFIG

        /**
         * `compression.type`
         */
        val COMPRESSION_TYPE_CONFIG = "compression.type"
        private val COMPRESSION_TYPE_DOC =
            "The compression type for all data generated by the producer. The default is none " +
                    "(i.e. no compression). Valid values are <code>none</code>, " +
                    "<code>gzip</code>, <code>snappy</code>, <code>lz4</code>, or " +
                    "<code>zstd</code>. Compression is of full batches of data, so the efficacy " +
                    "of batching will also impact the compression ratio (more batching means " +
                    "better compression)."

        /**
         * `metrics.sample.window.ms`
         */
        val METRICS_SAMPLE_WINDOW_MS_CONFIG = CommonClientConfigs.METRICS_SAMPLE_WINDOW_MS_CONFIG

        /**
         * `metrics.num.samples`
         */
        val METRICS_NUM_SAMPLES_CONFIG = CommonClientConfigs.METRICS_NUM_SAMPLES_CONFIG

        /**
         * `metrics.recording.level`
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

        // max.in.flight.requests.per.connection should be less than or equal to 5 when idempotence
        // producer enabled to ensure message ordering
        private val MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION_FOR_IDEMPOTENCE = 5

        /**
         * `max.in.flight.requests.per.connection`
         */
        val MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION = "max.in.flight.requests.per.connection"
        private val MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION_DOC =
            "The maximum number of unacknowledged requests the client will send on a single " +
                    "connection before blocking. Note that if this configuration is set to be " +
                    "greater than 1 and <code>enable.idempotence</code> is set to false, there " +
                    "is a risk of message reordering after a failed send due to retries (i.e., " +
                    "if retries are enabled);  if retries are disabled or if " +
                    "<code>enable.idempotence</code> is set to true, ordering will be preserved. " +
                    "Additionally, enabling idempotence requires the value of this configuration " +
                    "to be less than or equal to " +
                    "$MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION_FOR_IDEMPOTENCE. If conflicting " +
                    "configurations are set and idempotence is not explicitly enabled, " +
                    "idempotence is disabled. "

        /**
         * `retries`
         */
        val RETRIES_CONFIG = CommonClientConfigs.RETRIES_CONFIG
        private val RETRIES_DOC =
            "Setting a value greater than zero will cause the client to resend any record whose " +
                    "send fails with a potentially transient error. Note that this retry is no " +
                    "different than if the client resent the record upon receiving the error. " +
                    "Produce requests will be failed before the number of retries has been " +
                    "exhausted if the timeout configured by " +
                    "<code>$DELIVERY_TIMEOUT_MS_CONFIG</code> expires first before successful " +
                    "acknowledgement. Users should generally prefer to leave this config unset " +
                    "and instead use <code>$DELIVERY_TIMEOUT_MS_CONFIG</code> to control retry " +
                    "behavior." +
                    "<p>Enabling idempotence requires this config value to be greater than 0. If " +
                    "conflicting configurations are set and idempotence is not explicitly " +
                    "enabled, idempotence is disabled." +
                    "<p>Allowing retries while setting <code>enable.idempotence</code> to " +
                    "<code>false</code> and <code>$MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION</code> " +
                    "to 1 will potentially change the ordering of records because if two batches " +
                    "are sent to a single partition, and the first fails and is retried but the " +
                    "second succeeds, then the records in the second batch may appear first."

        /**
         * `key.serializer`
         */
        val KEY_SERIALIZER_CLASS_CONFIG = "key.serializer"
        val KEY_SERIALIZER_CLASS_DOC =
            "Serializer class for key that implements the " +
                    "<code>org.apache.kafka.common.serialization.Serializer</code> interface."

        /**
         * `value.serializer`
         */
        val VALUE_SERIALIZER_CLASS_CONFIG = "value.serializer"
        val VALUE_SERIALIZER_CLASS_DOC =
            "Serializer class for value that implements the " +
                    "<code>org.apache.kafka.common.serialization.Serializer</code> interface."

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
         * `partitioner.class`
         */
        val PARTITIONER_CLASS_CONFIG = "partitioner.class"
        private val PARTITIONER_CLASS_DOC =
            "A class to use to determine which partition to be send to when produce the records. " +
                    "Available options are:" +
                    "<ul>" +
                    "<li>If not set, the default partitioning logic is used. This strategy will " +
                    "try sticking to a partition until at least $BATCH_SIZE_CONFIG bytes is " +
                    "produced to the partition. It works with the strategy:" +
                    "<ul>" +
                    "<li>If no partition is specified but a key is present, choose a partition " +
                    "based on a hash of the key</li>" +
                    "<li>If no partition or key is present, choose the sticky partition that " +
                    "changes when at least $BATCH_SIZE_CONFIG bytes are produced to the " +
                    "partition.</li>" +
                    "</ul>" +
                    "</li>" +
                    "<li><code>org.apache.kafka.clients.producer.RoundRobinPartitioner</code>: " +
                    "This partitioning strategy is that each record in a series of consecutive " +
                    "records will be sent to a different partition(no matter if the 'key' is " +
                    "provided or not), until we run out of partitions and start over again. " +
                    "Note: There's a known issue that will cause uneven distribution when new " +
                    "batch is created. Please check KAFKA-9965 for more detail." +
                    "</li>" +
                    "</ul>" +
                    "<p>Implementing the <code>org.apache.kafka.clients.producer.Partitioner</code> " +
                    "interface allows you to plug in a custom partitioner."

        /**
         * `interceptor.classes`
         */
        val INTERCEPTOR_CLASSES_CONFIG = "interceptor.classes"
        val INTERCEPTOR_CLASSES_DOC =
            "A list of classes to use as interceptors. Implementing the " +
                    "<code>org.apache.kafka.clients.producer.ProducerInterceptor</code> " +
                    "interface allows you to intercept (and possibly mutate) the records " +
                    "received by the producer before they are published to the Kafka cluster. By " +
                    "default, there are no interceptors."

        /**
         * `enable.idempotence`
         */
        val ENABLE_IDEMPOTENCE_CONFIG = "enable.idempotence"
        val ENABLE_IDEMPOTENCE_DOC =
            "When set to 'true', the producer will ensure that exactly one copy of each message " +
                    "is written in the stream. If 'false', producer retries due to broker " +
                    "failures, etc., may write duplicates of the retried message in the stream. " +
                    "Note that enabling idempotence requires " +
                    "<code>$MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION</code> to be less than or " +
                    "equal to $MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION_FOR_IDEMPOTENCE (with " +
                    "message ordering preserved for any allowable value), " +
                    "<code>$RETRIES_CONFIG</code> to be greater than 0, and " +
                    "<code>$ACKS_CONFIG</code> must be 'all'. " +
                    "<p>Idempotence is enabled by default if no conflicting configurations are " +
                    "set. If conflicting configurations are set and idempotence is not " +
                    "explicitly enabled, idempotence is disabled. If idempotence is explicitly " +
                    "enabled and conflicting configurations are set, a " +
                    "<code>ConfigException</code> is thrown."

        /**
         * ` transaction.timeout.ms `
         */
        val TRANSACTION_TIMEOUT_CONFIG = "transaction.timeout.ms"
        val TRANSACTION_TIMEOUT_DOC =
            "The maximum amount of time in ms that the transaction coordinator will wait for a " +
                    "transaction status update from the producer before proactively aborting the " +
                    "ongoing transaction. If this value is larger than the " +
                    "transaction.max.timeout.ms setting in the broker, the request will fail " +
                    "with a <code>InvalidTxnTimeoutException</code> error."

        /**
         * ` transactional.id `
         */
        val TRANSACTIONAL_ID_CONFIG = "transactional.id"
        val TRANSACTIONAL_ID_DOC =
            "The TransactionalId to use for transactional delivery. This enables reliability " +
                    "semantics which span multiple producer sessions since it allows the client " +
                    "to guarantee that transactions using the same TransactionalId have been " +
                    "completed prior to starting any new transactions. If no TransactionalId is " +
                    "provided, then the producer is limited to idempotent delivery. If a " +
                    "TransactionalId is configured, <code>enable.idempotence</code> is implied. " +
                    "By default the TransactionId is not configured, which means transactions " +
                    "cannot be used. Note that, by default, transactions require a cluster of at " +
                    "least three brokers which is the recommended setting for production; for " +
                    "development you can change this, by adjusting broker setting " +
                    "<code>transaction.state.log.replication.factor</code>."

        /**
         * `security.providers`
         */
        val SECURITY_PROVIDERS_CONFIG = SecurityConfig.SECURITY_PROVIDERS_CONFIG
        private val SECURITY_PROVIDERS_DOC = SecurityConfig.SECURITY_PROVIDERS_DOC
        private val PRODUCER_CLIENT_ID_SEQUENCE = AtomicInteger(1)

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
                name = BUFFER_MEMORY_CONFIG,
                type = ConfigDef.Type.LONG,
                defaultValue = 32 * 1024 * 1024L,
                validator = atLeast(0L),
                importance = Importance.HIGH,
                documentation = BUFFER_MEMORY_DOC,
            ).define(
                name = RETRIES_CONFIG,
                type = ConfigDef.Type.INT,
                defaultValue = Int.MAX_VALUE,
                validator = between(0, Int.MAX_VALUE),
                importance = Importance.HIGH,
                documentation = RETRIES_DOC,
            ).define(
                name = ACKS_CONFIG,
                type = ConfigDef.Type.STRING,
                defaultValue = "all",
                validator = `in`("all", "-1", "0", "1"),
                importance = Importance.LOW,
                documentation = ACKS_DOC,
            ).define(
                name = COMPRESSION_TYPE_CONFIG,
                type = ConfigDef.Type.STRING,
                defaultValue = CompressionType.NONE.name,
                validator = `in`(*enumOptions(CompressionType::class.java)),
                importance = Importance.HIGH,
                documentation = COMPRESSION_TYPE_DOC,
            ).define(
                name = BATCH_SIZE_CONFIG,
                type = ConfigDef.Type.INT,
                defaultValue = 16384,
                validator = atLeast(0),
                importance = Importance.MEDIUM,
                documentation = BATCH_SIZE_DOC,
            ).define(
                name = PARTITIONER_ADPATIVE_PARTITIONING_ENABLE_CONFIG,
                type = ConfigDef.Type.BOOLEAN,
                defaultValue = true,
                importance = Importance.LOW,
                documentation = PARTITIONER_ADPATIVE_PARTITIONING_ENABLE_DOC,
            ).define(
                name = PARTITIONER_AVAILABILITY_TIMEOUT_MS_CONFIG,
                type = ConfigDef.Type.LONG,
                defaultValue = 0,
                validator = atLeast(0),
                importance = Importance.LOW,
                documentation = PARTITIONER_AVAILABILITY_TIMEOUT_MS_DOC,
            ).define(
                name = PARTITIONER_IGNORE_KEYS_CONFIG,
                type = ConfigDef.Type.BOOLEAN,
                defaultValue = false,
                importance = Importance.MEDIUM,
                documentation = PARTITIONER_IGNORE_KEYS_DOC,
            ).define(
                name = LINGER_MS_CONFIG,
                type = ConfigDef.Type.LONG,
                defaultValue = 0,
                validator = atLeast(0),
                importance = Importance.MEDIUM,
                documentation = LINGER_MS_DOC,
            ).define(
                name = DELIVERY_TIMEOUT_MS_CONFIG,
                type = ConfigDef.Type.INT,
                defaultValue = 120 * 1000,
                validator = atLeast(0),
                importance = Importance.MEDIUM,
                documentation = DELIVERY_TIMEOUT_MS_DOC,
            ).define(
                name = CLIENT_ID_CONFIG,
                type = ConfigDef.Type.STRING,
                defaultValue = "",
                importance = Importance.MEDIUM,
                documentation = CommonClientConfigs.CLIENT_ID_DOC,
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
                defaultValue = 32 * 1024,
                validator = atLeast(CommonClientConfigs.RECEIVE_BUFFER_LOWER_BOUND),
                importance = Importance.MEDIUM,
                documentation = CommonClientConfigs.RECEIVE_BUFFER_DOC,
            ).define(
                name = MAX_REQUEST_SIZE_CONFIG,
                type = ConfigDef.Type.INT,
                defaultValue = 1024 * 1024,
                validator = atLeast(0),
                importance = Importance.MEDIUM,
                documentation = MAX_REQUEST_SIZE_DOC,
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
                name = MAX_BLOCK_MS_CONFIG,
                type = ConfigDef.Type.LONG,
                defaultValue = 60 * 1000,
                validator = atLeast(0),
                importance = Importance.MEDIUM,
                documentation = MAX_BLOCK_MS_DOC,
            ).define(
                name = REQUEST_TIMEOUT_MS_CONFIG,
                type = ConfigDef.Type.INT,
                defaultValue = 30 * 1000,
                validator = atLeast(0),
                importance = Importance.MEDIUM,
                documentation = REQUEST_TIMEOUT_MS_DOC,
            ).define(
                name = METADATA_MAX_AGE_CONFIG,
                type = ConfigDef.Type.LONG,
                defaultValue = 5 * 60 * 1000,
                validator = atLeast(0),
                importance = Importance.LOW,
                documentation = METADATA_MAX_AGE_DOC,
            ).define(
                name = METADATA_MAX_IDLE_CONFIG,
                type = ConfigDef.Type.LONG,
                defaultValue = 5 * 60 * 1000,
                validator = atLeast(5000),
                importance = Importance.LOW,
                documentation = METADATA_MAX_IDLE_DOC,
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
                name = MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,
                type = ConfigDef.Type.INT,
                defaultValue = 5,
                validator = atLeast(1),
                importance = Importance.LOW,
                documentation = MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION_DOC,
            ).define(
                name = KEY_SERIALIZER_CLASS_CONFIG,
                type = ConfigDef.Type.CLASS,
                importance = Importance.HIGH,
                documentation = KEY_SERIALIZER_CLASS_DOC,
            ).define(
                name = VALUE_SERIALIZER_CLASS_CONFIG,
                type = ConfigDef.Type.CLASS,
                importance = Importance.HIGH,
                documentation = VALUE_SERIALIZER_CLASS_DOC,
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
                // default is set to be a bit lower than the server default (10 min), to avoid both
                // client and server closing connection at same time
                name = CONNECTIONS_MAX_IDLE_MS_CONFIG,
                type = ConfigDef.Type.LONG,
                defaultValue = 9 * 60 * 1000,
                importance = Importance.MEDIUM,
                documentation = CommonClientConfigs.CONNECTIONS_MAX_IDLE_MS_DOC,
            ).define(
                name = PARTITIONER_CLASS_CONFIG,
                type = ConfigDef.Type.CLASS,
                defaultValue = null,
                importance = Importance.MEDIUM,
                documentation = PARTITIONER_CLASS_DOC,
            ).define(
                name = INTERCEPTOR_CLASSES_CONFIG,
                type = ConfigDef.Type.LIST, defaultValue = emptyList<Any>(),
                validator = NonNullValidator(),
                importance = Importance.LOW,
                documentation = INTERCEPTOR_CLASSES_DOC,
            ).define(
                name = CommonClientConfigs.SECURITY_PROTOCOL_CONFIG,
                type = ConfigDef.Type.STRING,
                defaultValue = CommonClientConfigs.DEFAULT_SECURITY_PROTOCOL,
                validator = `in`(*enumOptions(SecurityProtocol::class.java)),
                importance = Importance.MEDIUM,
                documentation = CommonClientConfigs.SECURITY_PROTOCOL_DOC,
            ).define(
                name = SECURITY_PROVIDERS_CONFIG,
                type = ConfigDef.Type.STRING,
                defaultValue = null,
                importance = Importance.LOW,
                documentation = SECURITY_PROVIDERS_DOC,
            ).withClientSslSupport()
                .withClientSaslSupport()
                .define(
                    name = ENABLE_IDEMPOTENCE_CONFIG,
                    type = ConfigDef.Type.BOOLEAN,
                    defaultValue = true,
                    importance = Importance.LOW,
                    documentation = ENABLE_IDEMPOTENCE_DOC,
                ).define(
                    name = TRANSACTION_TIMEOUT_CONFIG,
                    type = ConfigDef.Type.INT,
                    defaultValue = 60000,
                    importance = Importance.LOW,
                    documentation = TRANSACTION_TIMEOUT_DOC,
                ).define(
                    name = TRANSACTIONAL_ID_CONFIG,
                    type = ConfigDef.Type.STRING,
                    defaultValue = null,
                    validator = NonEmptyString(),
                    importance = Importance.LOW,
                    documentation = TRANSACTIONAL_ID_DOC,
                )
        }

        private fun parseAcks(acksString: String): String {
            try {
                return if (acksString.trim { it <= ' ' }.equals("all", ignoreCase = true)) "-1"
                else acksString.trim { it <= ' ' }.toShort().toString()
            } catch (e: NumberFormatException) {
                throw ConfigException("Invalid configuration value for 'acks': $acksString")
            }
        }

        fun appendSerializerToConfig(
            configs: Map<String, Any?>?,
            keySerializer: Serializer<*>?,
            valueSerializer: Serializer<*>?,
        ): Map<String, Any?> {
            // validate serializer configuration, if the passed serializer instance is null, the
            // user must explicitly set a valid serializer configuration value
            val newConfigs: MutableMap<String, Any?> = HashMap(configs)
            if (keySerializer != null)
                newConfigs[KEY_SERIALIZER_CLASS_CONFIG] = keySerializer.javaClass
            else if (newConfigs[KEY_SERIALIZER_CLASS_CONFIG] == null)
                throw ConfigException(name = KEY_SERIALIZER_CLASS_CONFIG, message = "must be non-null.")

            if (valueSerializer != null)
                newConfigs[VALUE_SERIALIZER_CLASS_CONFIG] = valueSerializer.javaClass
            else if (newConfigs[VALUE_SERIALIZER_CLASS_CONFIG] == null)
                throw ConfigException(name = VALUE_SERIALIZER_CLASS_CONFIG, message = "must be non-null.")

            return newConfigs
        }

        fun configNames(): Set<String> = CONFIG.names()

        fun configDef(): ConfigDef = ConfigDef(CONFIG)

        @JvmStatic
        fun main(args: Array<String>) {
            println(CONFIG.toHtml(4, { config -> "producerconfigs_$config" }))
        }
    }
}
