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

import java.time.Duration
import java.util.*
import java.util.concurrent.ExecutionException
import java.util.concurrent.Future
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference
import org.apache.kafka.clients.ApiVersions
import org.apache.kafka.clients.ClientUtils
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.KafkaClient
import org.apache.kafka.clients.NetworkClient
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.clients.producer.internals.BufferPool
import org.apache.kafka.clients.producer.internals.BuiltInPartitioner
import org.apache.kafka.clients.producer.internals.DefaultPartitioner
import org.apache.kafka.clients.producer.internals.KafkaProducerMetrics
import org.apache.kafka.clients.producer.internals.ProducerInterceptors
import org.apache.kafka.clients.producer.internals.ProducerMetadata
import org.apache.kafka.clients.producer.internals.ProducerMetrics
import org.apache.kafka.clients.producer.internals.RecordAccumulator
import org.apache.kafka.clients.producer.internals.RecordAccumulator.PartitionerConfig
import org.apache.kafka.clients.producer.internals.Sender
import org.apache.kafka.clients.producer.internals.TransactionManager
import org.apache.kafka.common.Cluster
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.Metric
import org.apache.kafka.common.MetricName
import org.apache.kafka.common.PartitionInfo
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.common.errors.ApiException
import org.apache.kafka.common.errors.InterruptException
import org.apache.kafka.common.errors.InvalidTopicException
import org.apache.kafka.common.errors.ProducerFencedException
import org.apache.kafka.common.errors.RecordTooLargeException
import org.apache.kafka.common.errors.SerializationException
import org.apache.kafka.common.errors.TimeoutException
import org.apache.kafka.common.header.Headers
import org.apache.kafka.common.header.internals.RecordHeaders
import org.apache.kafka.common.internals.ClusterResourceListeners
import org.apache.kafka.common.metrics.KafkaMetricsContext
import org.apache.kafka.common.metrics.MetricConfig
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.metrics.MetricsContext
import org.apache.kafka.common.metrics.Sensor
import org.apache.kafka.common.network.Selector
import org.apache.kafka.common.record.AbstractRecords
import org.apache.kafka.common.record.CompressionType
import org.apache.kafka.common.record.RecordBatch
import org.apache.kafka.common.requests.JoinGroupRequest
import org.apache.kafka.common.serialization.Serializer
import org.apache.kafka.common.utils.AppInfoParser
import org.apache.kafka.common.utils.KafkaThread
import org.apache.kafka.common.utils.LogContext
import org.apache.kafka.common.utils.Time
import org.apache.kafka.common.utils.Utils
import org.slf4j.Logger

/**
 * A Kafka client that publishes records to the Kafka cluster.
 *
 * The producer is *thread safe* and sharing a single producer instance across threads will
 * generally be faster than having multiple instances.
 *
 * Here is a simple example of using the producer to send records with strings containing sequential
 * numbers as the key/value pairs.
 *
 * ```java
 * `Properties props = new Properties();
 * props.put("bootstrap.servers", "localhost:9092");
 * props.put("linger.ms", 1);
 * props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
 * props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
 *
 * Producer<String, String> producer = new KafkaProducer<>(props);
 * for (int i = 0; i < 100; i++)
 * producer.send(new ProducerRecord<String, String>("my-topic", Integer.toString(i), Integer.toString(i)));
 *
 * producer.close();
 * ```
 *
 * The producer consists of a pool of buffer space that holds records that haven't yet been
 * transmitted to the server as well as a background I/O thread that is responsible for turning
 * these records into requests and transmitting them to the cluster. Failure to close the producer
 * after use will leak these resources.
 *
 * The [send] method is asynchronous. When called, it adds the record to a buffer of pending record
 * sends and immediately returns. This allows the producer to batch together individual records for
 * efficiency.
 *
 * The `acks` config controls the criteria under which requests are considered complete. The default
 * setting "all" will result in blocking on the full commit of the record, the slowest but most
 * durable setting.
 *
 * If the request fails, the producer can automatically retry. The `retries` setting defaults to
 * `Integer.MAX_VALUE`, and it's recommended to use `delivery.timeout.ms` to control retry behavior,
 * instead of `retries`.
 *
 * The producer maintains buffers of unsent records for each partition. These buffers are of a size
 * specified by
 * the `batch.size` config. Making this larger can result in more batching, but requires more memory
 * (since we will generally have one of these buffers for each active partition).
 *
 * By default a buffer is available to send immediately even if there is additional unused space in
 * the buffer. However if you want to reduce the number of requests you can set `linger.ms` to
 * something greater than 0. This will instruct the producer to wait up to that number of
 * milliseconds before sending a request in hope that more records will arrive to fill up the same
 * batch. This is analogous to Nagle's algorithm in TCP. For example, in the code snippet above,
 * likely all 100 records would be sent in a single request since we set our linger time to 1
 * millisecond. However this setting would add 1 millisecond of latency to our request waiting for
 * more records to arrive if we didn't fill up the buffer. Note that records that arrive close
 * together in time will generally batch together even with `linger.ms=0`. So, under heavy load,
 * batching will occur regardless of the linger configuration; however setting this to something
 * larger than 0 can lead to fewer, more efficient requests when not under maximal load at the cost
 * of a small amount of latency.
 *
 * The `buffer.memory` controls the total amount of memory available to the producer for buffering.
 * If records are sent faster than they can be transmitted to the server then this buffer space will
 * be exhausted. When the buffer space is exhausted additional send calls will block. The threshold
 * for time to block is determined by `max.block.ms` after which it throws a [TimeoutException].
 *
 * The `key.serializer` and `value.serializer` instruct how to turn the key and value objects the
 * user provides with their `ProducerRecord` into bytes. You can use the included
 * [org.apache.kafka.common.serialization.ByteArraySerializer] or
 * [org.apache.kafka.common.serialization.StringSerializer] for simple string or byte types.
 *
 * From Kafka 0.11, the KafkaProducer supports two additional modes: the idempotent producer and the
 * transactional producer. The idempotent producer strengthens Kafka's delivery semantics from at
 * least once to exactly once delivery. In particular producer retries will no longer introduce
 * duplicates. The transactional producer allows an application to send messages to multiple
 * partitions (and topics!) atomically.
 *
 * From Kafka 3.0, the `enable.idempotence` configuration defaults to true. When enabling
 * idempotence, `retries` config will default to `Integer.MAX_VALUE` and the `acks` config will
 * default to `all`. There are no API changes for the idempotent producer, so existing applications
 * will not need to be modified to take advantage of this feature.
 *
 * To take advantage of the idempotent producer, it is imperative to avoid application level
 * re-sends since these cannot be de-duplicated. As such, if an application enables idempotence, it
 * is recommended to leave the `retries` config unset, as it will be defaulted to
 * `Integer.MAX_VALUE`. Additionally, if a [send] returns an error even with infinite retries (for
 * instance if the message expires in the buffer before being sent), then it is recommended to shut
 * down the producer and check the contents of the last produced message to ensure that it is not
 * duplicated. Finally, the producer can only guarantee idempotence for messages sent within a
 * single session.
 *
 * To use the transactional producer and the attendant APIs, you must set the `transactional.id`
 * configuration property. If the `transactional.id` is set, idempotence is automatically enabled
 * along with the producer configs which idempotence depends on. Further, topics which are included
 * in transactions should be configured for durability. In particular, the `replication.factor`
 * should be at least `3`, and the `min.insync.replicas` for these topics should be set to 2.
 * Finally, in order for transactional guarantees to be realized from end-to-end, the consumers must
 * be configured to read only committed messages as well.
 *
 * The purpose of the `transactional.id` is to enable transaction recovery across multiple sessions
 * of a single producer instance. It would typically be derived from the shard identifier in a
 * partitioned, stateful, application. As such, it should be unique to each producer instance
 * running within a partitioned application.
 *
 * All the new transactional APIs are blocking and will throw exceptions on failure. The example
 * below illustrates how the new APIs are meant to be used. It is similar to the example above,
 * except that all 100 messages are part of a single transaction.
 *
 * ```java
 * `Properties props = new Properties();
 * props.put("bootstrap.servers", "localhost:9092");
 * props.put("transactional.id", "my-transactional-id");
 * Producer<String, String> producer = new KafkaProducer<>(props, new StringSerializer(), new StringSerializer());
 *
 * producer.initTransactions();
 *
 * try {
 *   producer.beginTransaction();
 *   for (int i = 0; i < 100; i++)
 *     producer.send(new ProducerRecord<>("my-topic", Integer.toString(i), Integer.toString(i)));
 *     producer.commitTransaction();
 *   } catch (ProducerFencedException | OutOfOrderSequenceException | AuthorizationException e) {
 *   // We can't recover from these exceptions, so our only option is to close the producer and exit.
 *   producer.close();
 * } catch (KafkaException e) {
 *   // For all other exceptions, just abort the transaction and try again.
 *   producer.abortTransaction();
 * }
 * producer.close();
 * ```
 *
 * As is hinted at in the example, there can be only one open transaction per producer. All messages
 * sent between the [beginTransaction] and [commitTransaction] calls will be part of a single
 * transaction. When the `transactional.id` is specified, all messages sent by the producer must be
 * part of a transaction.
 *
 * The transactional producer uses exceptions to communicate error states. In particular, it is not
 * required to specify callbacks for `producer.send()` or to call `.get()` on the returned Future: a
 * `KafkaException` would be thrown if any of the `producer.send()` or transactional calls hit an
 * irrecoverable error during a transaction. See the [send] documentation for more details about
 * detecting errors from a transactional send.
 *
 * By calling `producer.abortTransaction()` upon receiving a `KafkaException` we can ensure that any
 * successful writes are marked as aborted, hence keeping the transactional guarantees.
 *
 * This client can communicate with brokers that are version 0.10.0 or newer. Older or newer brokers
 * may not support certain client features. For instance, the transactional APIs need broker
 * versions 0.11.0 or later. You will receive an `UnsupportedVersionException` when invoking an API
 * that is not available in the running broker version.
 */
open class KafkaProducer<K, V> : Producer<K, V> {

    private val log: Logger

    // Visible for testing
    internal val clientId: String

    // Visible for testing
    internal val metrics: Metrics

    private val producerMetrics: KafkaProducerMetrics

    private val partitioner: Partitioner?

    private val maxRequestSize: Int

    private val totalMemorySize: Long

    private val metadata: ProducerMetadata

    private val accumulator: RecordAccumulator

    private val sender: Sender?

    private val ioThread: Thread?

    private val compressionType: CompressionType

    private val errors: Sensor

    private val time: Time

    private val keySerializer: Serializer<K>

    private val valueSerializer: Serializer<V>

    private val producerConfig: ProducerConfig

    private val maxBlockTimeMs: Long

    private val partitionerIgnoreKeys: Boolean

    private val interceptors: ProducerInterceptors<K, V>

    private val apiVersions: ApiVersions

    private val transactionManager: TransactionManager?

    /**
     * A producer is instantiated by providing a set of key-value pairs as configuration, a key and
     * a value [Serializer]. Valid configuration strings are documented
     * [here](http://kafka.apache.org/documentation.html#producerconfigs). Values can be either
     * strings or Objects of the appropriate type (for example a numeric configuration would accept
     * either the string "42" or the integer 42).
     *
     * Note: after creating a [KafkaProducer] you must always [close] it to avoid resource leaks.
     *
     * @param configs The producer configs
     * @param keySerializer The serializer for key that implements [Serializer]. The configure()
     * method won't be called in the producer when the serializer is passed in directly.
     * @param valueSerializer The serializer for value that implements [Serializer]. The configure()
     * method won't be called in the producer when the serializer is passed in directly.
     */
    constructor(
        configs: Map<String, Any?>,
        keySerializer: Serializer<K>? = null,
        valueSerializer: Serializer<V>? = null
    ) : this(
        config = ProducerConfig(
            props = ProducerConfig.appendSerializerToConfig(configs, keySerializer, valueSerializer)
        ),
        keySerializer = keySerializer,
        valueSerializer = valueSerializer,
    )

    /**
     * A producer is instantiated by providing a set of key-value pairs as configuration, a key and
     * a value [Serializer]. Valid configuration strings are documented
     * [here](http://kafka.apache.org/documentation.html#producerconfigs).
     *
     * Note: after creating a `KafkaProducer` you must always [close] it to avoid resource leaks.
     *
     * @param properties The producer configs
     * @param keySerializer The serializer for key that implements [Serializer]. The configure()
     * method won't be called in the producer when the serializer is passed in directly.
     * @param valueSerializer The serializer for value that implements [Serializer]. The configure()
     * method won't be called in the producer when the serializer is passed in directly.
     */
    constructor(
        properties: Properties,
        keySerializer: Serializer<K>? = null,
        valueSerializer: Serializer<V>? = null
    ) : this(
        Utils.propsToMap(properties),
        keySerializer,
        valueSerializer,
    )

    // visible for testing
    internal constructor(
        config: ProducerConfig,
        keySerializer: Serializer<K>? = null,
        valueSerializer: Serializer<V>? = null,
        metadata: ProducerMetadata? = null,
        kafkaClient: KafkaClient? = null,
        interceptors: ProducerInterceptors<K, V>? = null,
        time: Time = Time.SYSTEM,
    ) {
        try {
            producerConfig = config
            this.time = time
            val transactionalId = config.getString(ProducerConfig.TRANSACTIONAL_ID_CONFIG)
            clientId = config.getString(ProducerConfig.CLIENT_ID_CONFIG)!!
            val logContext: LogContext = if (transactionalId == null) LogContext(
                String.format("[Producer clientId=%s] ", clientId)
            ) else LogContext(
                String.format(
                    "[Producer clientId=%s, transactionalId=%s] ",
                    clientId,
                    transactionalId
                )
            )
            log = logContext.logger(KafkaProducer::class.java)
            log.trace("Starting the Kafka producer")

            val metricConfig = MetricConfig().apply {
                samples = config.getInt(ProducerConfig.METRICS_NUM_SAMPLES_CONFIG)!!
                timeWindowMs = config.getLong(ProducerConfig.METRICS_SAMPLE_WINDOW_MS_CONFIG)!!
                recordingLevel = Sensor.RecordingLevel.forName(
                    config.getString(ProducerConfig.METRICS_RECORDING_LEVEL_CONFIG)!!
                )

                tags = Collections.singletonMap("client-id", clientId)
            }

            val reporters = CommonClientConfigs.metricsReporters(clientId, config)
            val metricsContext: MetricsContext = KafkaMetricsContext(
                JMX_PREFIX,
                config.originalsWithPrefix(CommonClientConfigs.METRICS_CONTEXT_PREFIX)
            )
            metrics = Metrics(
                config = metricConfig,
                reporters = reporters.toMutableList(),
                time = time,
                metricsContext = metricsContext,
            )
            producerMetrics = KafkaProducerMetrics(metrics)

            partitioner = config.getConfiguredInstance(
                ProducerConfig.PARTITIONER_CLASS_CONFIG,
                Partitioner::class.java,
                mapOf(ProducerConfig.CLIENT_ID_CONFIG to clientId)
            )
            warnIfPartitionerDeprecated()

            partitionerIgnoreKeys = config.getBoolean(ProducerConfig.PARTITIONER_IGNORE_KEYS_CONFIG)!!
            val retryBackoffMs = config.getLong(ProducerConfig.RETRY_BACKOFF_MS_CONFIG)!!

            if (keySerializer == null) {
                this.keySerializer = config.getConfiguredInstance(
                    ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                    Serializer::class.java
                ) as Serializer<K>
                this.keySerializer.configure(
                    config.originals(
                        Collections.singletonMap<String, Any>(
                            ProducerConfig.CLIENT_ID_CONFIG,
                            clientId
                        )
                    ), true
                )
            } else {
                config.ignore(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG)
                this.keySerializer = keySerializer
            }
            if (valueSerializer == null) {
                this.valueSerializer = config.getConfiguredInstance(
                    ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                    Serializer::class.java
                ) as Serializer<V>
                this.valueSerializer.configure(
                    config.originals(
                        Collections.singletonMap<String, Any>(
                            ProducerConfig.CLIENT_ID_CONFIG,
                            clientId
                        )
                    ), false
                )
            } else {
                config.ignore(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG)
                this.valueSerializer = valueSerializer
            }
            val interceptorList: List<ProducerInterceptor<K, V>> =
                config.getConfiguredInstances(
                    ProducerConfig.INTERCEPTOR_CLASSES_CONFIG,
                    ProducerInterceptor::class.java,
                    Collections.singletonMap<String, Any>(ProducerConfig.CLIENT_ID_CONFIG, clientId)
                ) as List<ProducerInterceptor<K, V>>
            if (interceptors != null) this.interceptors = interceptors else this.interceptors =
                ProducerInterceptors(interceptorList)
            val clusterResourceListeners = configureClusterResourceListeners(
                keySerializer,
                valueSerializer, interceptorList, reporters
            )
            maxRequestSize = config.getInt(ProducerConfig.MAX_REQUEST_SIZE_CONFIG)!!
            totalMemorySize = config.getLong(ProducerConfig.BUFFER_MEMORY_CONFIG)!!
            compressionType =
                CompressionType.forName(config.getString(ProducerConfig.COMPRESSION_TYPE_CONFIG)!!)
            maxBlockTimeMs = config.getLong(ProducerConfig.MAX_BLOCK_MS_CONFIG)!!
            val deliveryTimeoutMs = configureDeliveryTimeout(config, log)
            apiVersions = ApiVersions()
            transactionManager = configureTransactionState(config, logContext)
            // There is no need to do work required for adaptive partitioning, if we use a custom partitioner.
            val enableAdaptivePartitioning = partitioner == null &&
                    config.getBoolean(ProducerConfig.PARTITIONER_ADPATIVE_PARTITIONING_ENABLE_CONFIG)!!
            val partitionerConfig = PartitionerConfig(
                enableAdaptivePartitioning,
                config.getLong(ProducerConfig.PARTITIONER_AVAILABILITY_TIMEOUT_MS_CONFIG)!!
            )
            // As per Kafka producer configuration documentation batch.size may be set to 0 to explicitly disable
            // batching which in practice actually means using a batch size of 1.
            val batchSize = config.getInt(ProducerConfig.BATCH_SIZE_CONFIG)!!.coerceAtLeast(1)
            accumulator = RecordAccumulator(
                logContext = logContext,
                batchSize = batchSize,
                compression = compressionType,
                lingerMs = lingerMs(config),
                retryBackoffMs = retryBackoffMs,
                deliveryTimeoutMs = deliveryTimeoutMs,
                partitionerConfig = partitionerConfig,
                metrics = metrics,
                metricGrpName = PRODUCER_METRIC_GROUP_NAME,
                time = time,
                apiVersions = apiVersions,
                transactionManager = transactionManager,
                free = BufferPool(
                    totalMemory = totalMemorySize,
                    poolableSize = batchSize,
                    metrics = metrics,
                    time = time,
                    metricGrpName = PRODUCER_METRIC_GROUP_NAME,
                )
            )
            val addresses = ClientUtils.parseAndValidateAddresses(
                config.getList(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG)!!,
                config.getString(ProducerConfig.CLIENT_DNS_LOOKUP_CONFIG)!!
            )
            if (metadata != null) this.metadata = metadata
            else {
                this.metadata = ProducerMetadata(
                    retryBackoffMs,
                    config.getLong(ProducerConfig.METADATA_MAX_AGE_CONFIG)!!,
                    config.getLong(ProducerConfig.METADATA_MAX_IDLE_CONFIG)!!,
                    logContext,
                    clusterResourceListeners,
                    Time.SYSTEM
                )
                this.metadata.bootstrap(addresses)
            }
            errors = metrics.sensor("errors")
            sender = newSender(logContext, kafkaClient, this.metadata)
            val ioThreadName = "$NETWORK_THREAD_PREFIX | $clientId"
            ioThread = KafkaThread(ioThreadName, sender, true)
            ioThread.start()
            config.logUnused()
            AppInfoParser.registerAppInfo(JMX_PREFIX, clientId, metrics, time.milliseconds())
            log.debug("Kafka producer started")
        } catch (t: Throwable) {
            // call close methods if internal objects are already constructed this is to prevent resource leak. see KAFKA-2121
            close(Duration.ofMillis(0), true)
            // now propagate the exception
            throw KafkaException("Failed to construct kafka producer", t)
        }
    }

    // visible for testing
    internal constructor(
        config: ProducerConfig,
        logContext: LogContext,
        metrics: Metrics,
        keySerializer: Serializer<K>,
        valueSerializer: Serializer<V>,
        metadata: ProducerMetadata,
        accumulator: RecordAccumulator,
        transactionManager: TransactionManager?,
        sender: Sender?,
        interceptors: ProducerInterceptors<K, V>,
        partitioner: Partitioner?,
        time: Time,
        ioThread: KafkaThread?
    ) {
        producerConfig = config
        this.time = time
        clientId = config.getString(ProducerConfig.CLIENT_ID_CONFIG)!!
        log = logContext.logger(KafkaProducer::class.java)
        this.metrics = metrics
        producerMetrics = KafkaProducerMetrics(metrics)
        this.partitioner = partitioner
        this.keySerializer = keySerializer
        this.valueSerializer = valueSerializer
        this.interceptors = interceptors
        maxRequestSize = config.getInt(ProducerConfig.MAX_REQUEST_SIZE_CONFIG)!!
        totalMemorySize = config.getLong(ProducerConfig.BUFFER_MEMORY_CONFIG)!!
        compressionType =
            CompressionType.forName(config.getString(ProducerConfig.COMPRESSION_TYPE_CONFIG)!!)
        maxBlockTimeMs = config.getLong(ProducerConfig.MAX_BLOCK_MS_CONFIG)!!
        partitionerIgnoreKeys = config.getBoolean(ProducerConfig.PARTITIONER_IGNORE_KEYS_CONFIG)!!
        apiVersions = ApiVersions()
        this.transactionManager = transactionManager
        this.accumulator = accumulator
        errors = this.metrics.sensor("errors")
        this.metadata = metadata
        this.sender = sender
        this.ioThread = ioThread
    }

    /**
     * Check if partitioner is deprecated and log a warning if it is.
     */
    @Suppress("deprecation")
    private fun warnIfPartitionerDeprecated() {
        // Using DefaultPartitioner and UniformStickyPartitioner is deprecated, see KIP-794.
        if (partitioner is DefaultPartitioner) {
            log.warn(
                "DefaultPartitioner is deprecated. Please clear " + ProducerConfig.PARTITIONER_CLASS_CONFIG
                        + " configuration setting to get the default partitioning behavior"
            )
        }
        if (partitioner is UniformStickyPartitioner) {
            log.warn(
                ("UniformStickyPartitioner is deprecated. Please clear " + ProducerConfig.PARTITIONER_CLASS_CONFIG
                        + " configuration setting and set " + ProducerConfig.PARTITIONER_IGNORE_KEYS_CONFIG
                        + " to 'true' to get the uniform sticky partitioning behavior")
            )
        }
    }

    // visible for testing
    private fun newSender(
        logContext: LogContext,
        kafkaClient: KafkaClient?,
        metadata: ProducerMetadata,
    ): Sender {
        val maxInflightRequests =
            producerConfig.getInt(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION)!!
        val requestTimeoutMs = producerConfig.getInt(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG)!!
        val channelBuilder = ClientUtils.createChannelBuilder(producerConfig, time, logContext)
        val metricsRegistry = ProducerMetrics(metrics)
        val throttleTimeSensor = Sender.throttleTimeSensor(metricsRegistry.senderMetrics)
        val client = kafkaClient
            ?: NetworkClient(
                selector = Selector(
                    connectionMaxIdleMs =
                    producerConfig.getLong(ProducerConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG)!!,
                    metrics = metrics,
                    time = time,
                    metricGrpPrefix = "producer",
                    channelBuilder = channelBuilder,
                    logContext = logContext
                ),
                metadata = metadata,
                clientId = clientId,
                maxInFlightRequestsPerConnection = maxInflightRequests,
                reconnectBackoffMs =
                producerConfig.getLong(ProducerConfig.RECONNECT_BACKOFF_MS_CONFIG)!!,
                reconnectBackoffMax =
                producerConfig.getLong(ProducerConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG)!!,
                socketSendBuffer = producerConfig.getInt(ProducerConfig.SEND_BUFFER_CONFIG)!!,
                socketReceiveBuffer = producerConfig.getInt(ProducerConfig.RECEIVE_BUFFER_CONFIG)!!,
                defaultRequestTimeoutMs = requestTimeoutMs,
                connectionSetupTimeoutMs =
                producerConfig.getLong(ProducerConfig.SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG)!!,
                connectionSetupTimeoutMaxMs =
                producerConfig.getLong(ProducerConfig.SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG)!!,
                time = time,
                discoverBrokerVersions = true,
                apiVersions = apiVersions,
                throttleTimeSensor = throttleTimeSensor,
                logContext = logContext
            )
        val acks = producerConfig.getString(ProducerConfig.ACKS_CONFIG)!!.toShort()
        return Sender(
            logContext = logContext,
            client = client,
            metadata = metadata,
            accumulator = accumulator,
            guaranteeMessageOrder = maxInflightRequests == 1,
            maxRequestSize = producerConfig.getInt(ProducerConfig.MAX_REQUEST_SIZE_CONFIG)!!,
            acks = acks,
            retries = producerConfig.getInt(ProducerConfig.RETRIES_CONFIG)!!,
            metricsRegistry = metricsRegistry.senderMetrics,
            time = time,
            requestTimeoutMs = requestTimeoutMs,
            retryBackoffMs = producerConfig.getLong(ProducerConfig.RETRY_BACKOFF_MS_CONFIG)!!,
            transactionManager = transactionManager,
            apiVersions = apiVersions
        )
    }

    private fun configureTransactionState(
        config: ProducerConfig,
        logContext: LogContext
    ): TransactionManager? {
        var transactionManager: TransactionManager? = null
        if (config.getBoolean(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG)!!) {
            val transactionalId = config.getString(ProducerConfig.TRANSACTIONAL_ID_CONFIG)!!
            val transactionTimeoutMs = config.getInt(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG)!!
            val retryBackoffMs = config.getLong(ProducerConfig.RETRY_BACKOFF_MS_CONFIG)!!
            transactionManager = TransactionManager(
                logContext = logContext,
                transactionalId = transactionalId,
                transactionTimeoutMs = transactionTimeoutMs,
                retryBackoffMs = retryBackoffMs,
                apiVersions = apiVersions
            )
            if (transactionManager.isTransactional) log.info("Instantiated a transactional producer.") else log.info(
                "Instantiated an idempotent producer."
            )
        } else {
            // ignore unretrieved configurations related to producer transaction
            config.ignore(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG)
        }
        return transactionManager
    }

    /**
     * Needs to be called before any other methods when the transactional.id is set in the
     * configuration.
     *
     * This method does the following:
     *
     * 1. Ensures any transactions initiated by previous instances of the producer with the same
     *    transactional.id are completed. If the previous instance had failed with a transaction in
     *    progress, it will be aborted. If the last transaction had begun completion, but not yet
     *    finished, this method awaits its completion.
     * 2. Gets the internal producer id and epoch, used in all future transactional messages issued
     *    by the producer.
     *
     * Note that this method will raise [TimeoutException] if the transactional state cannot be
     * initialized before expiration of `max.block.ms`. Additionally, it will raise
     * [InterruptException] if interrupted. It is safe to retry in either case, but once the
     * transactional state has been successfully initialized, this method should no longer be used.
     *
     * @throws IllegalStateException if no transactional.id has been configured
     * @throws org.apache.kafka.common.errors.UnsupportedVersionException fatal error indicating the
     * broker does not support transactions (i.e. if its version is lower than 0.11.0.0)
     * @throws org.apache.kafka.common.errors.AuthorizationException fatal error indicating that the
     * configured transactional.id is not authorized. See the exception for more details
     * @throws KafkaException if the producer has encountered a previous fatal error or for any
     * other unexpected error
     * @throws TimeoutException if the time taken for initialize the transaction has surpassed
     * `max.block.ms`.
     * @throws InterruptException if the thread is interrupted while blocked
     */
    override fun initTransactions() {
        throwIfNoTransactionManager()
        throwIfProducerClosed()
        val now = time.nanoseconds()
        val result = transactionManager!!.initializeTransactions()
        sender!!.wakeup()
        result.await(maxBlockTimeMs, TimeUnit.MILLISECONDS)
        producerMetrics.recordInit(time.nanoseconds() - now)
    }

    /**
     * Should be called before the start of each new transaction. Note that prior to the first invocation
     * of this method, you must invoke [initTransactions] exactly one time.
     *
     * @throws IllegalStateException if no transactional.id has been configured or if [initTransactions]
     * has not yet been invoked
     * @throws ProducerFencedException if another producer with the same transactional.id is active
     * @throws org.apache.kafka.common.errors.InvalidProducerEpochException if the producer has attempted to produce with an old epoch
     * to the partition leader. See the exception for more details
     * @throws org.apache.kafka.common.errors.UnsupportedVersionException fatal error indicating the broker
     * does not support transactions (i.e. if its version is lower than 0.11.0.0)
     * @throws org.apache.kafka.common.errors.AuthorizationException fatal error indicating that the configured
     * transactional.id is not authorized. See the exception for more details
     * @throws KafkaException if the producer has encountered a previous fatal error or for any other unexpected error
     */
    @Throws(ProducerFencedException::class)
    override fun beginTransaction() {
        throwIfNoTransactionManager()
        throwIfProducerClosed()
        val now = time.nanoseconds()
        transactionManager!!.beginTransaction()
        producerMetrics.recordBeginTxn(time.nanoseconds() - now)
    }

    /**
     * Sends a list of specified offsets to the consumer group coordinator, and also marks those
     * offsets as part of the current transaction. These offsets will be considered committed only
     * if the transaction is committed successfully. The committed offset should be the next message
     * your application will consume, i.e. lastProcessedMessageOffset + 1.
     *
     * This method should be used when you need to batch consumed and produced messages together,
     * typically in a consume-transform-produce pattern. Thus, the specified `consumerGroupId`
     * should be the same as config parameter `group.id` of the used [consumer][KafkaConsumer].
     * Note, that the consumer should have `enable.auto.commit=false` and should also not commit
     * offsets manually (via [sync][KafkaConsumer.commitSync] or [async][KafkaConsumer.commitAsync]
     * commits).
     *
     * This method is a blocking call that waits until the request has been received and
     * acknowledged by the consumer group coordinator; but the offsets are not considered as
     * committed until the transaction itself is successfully committed later (via the
     * [commitTransaction] call).
     *
     * @throws IllegalStateException if no transactional.id has been configured, no transaction has
     * been started
     * @throws ProducerFencedException fatal error indicating another producer with the same
     * transactional.id is active
     * @throws org.apache.kafka.common.errors.UnsupportedVersionException fatal error indicating the
     * broker does not support transactions (i.e. if its version is lower than 0.11.0.0)
     * @throws org.apache.kafka.common.errors.UnsupportedForMessageFormatException fatal error
     * indicating the message format used for the offsets topic on the broker does not support
     * transactions
     * @throws org.apache.kafka.common.errors.AuthorizationException fatal error indicating that the
     * configured transactional.id is not authorized, or the consumer group id is not authorized.
     * @throws org.apache.kafka.common.errors.InvalidProducerEpochException if the producer has
     * attempted to produce with an old epoch to the partition leader. See the exception for more
     * details
     * @throws TimeoutException if the time taken for sending the offsets has surpassed
     * `max.block.ms`.
     * @throws KafkaException if the producer has encountered a previous fatal or abortable error,
     * or for any other unexpected error
     */
    @Deprecated("Since 3.0.0, please use {@link #sendOffsetsToTransaction(Map, ConsumerGroupMetadata)} instead.")
    @Throws(
        ProducerFencedException::class
    )
    override fun sendOffsetsToTransaction(
        offsets: Map<TopicPartition, OffsetAndMetadata>,
        consumerGroupId: String
    ) = sendOffsetsToTransaction(offsets, ConsumerGroupMetadata(consumerGroupId))

    /**
     * Sends a list of specified offsets to the consumer group coordinator, and also marks those
     * offsets as part of the current transaction. These offsets will be considered committed only
     * if the transaction is committed successfully. The committed offset should be the next message
     * your application will consume, i.e. lastProcessedMessageOffset + 1.
     *
     * This method should be used when you need to batch consumed and produced messages together,
     * typically in a consume-transform-produce pattern. Thus, the specified `groupMetadata` should
     * be extracted from the used [consumer][KafkaConsumer] via [KafkaConsumer.groupMetadata] to
     * leverage consumer group metadata. This will provide stronger fencing than just supplying the
     * `consumerGroupId` and passing in `new ConsumerGroupMetadata(consumerGroupId)`, however note
     * that the full set of consumer group metadata returned by [KafkaConsumer.groupMetadata]
     * requires the brokers to be on version 2.5 or newer to understand.
     *
     * This method is a blocking call that waits until the request has been received and
     * acknowledged by the consumer group coordinator; but the offsets are not considered as
     * committed until the transaction itself is successfully committed later (via the
     * [commitTransaction] call).
     *
     * Note, that the consumer should have `enable.auto.commit=false` and should also not commit
     * offsets manually (via [sync][KafkaConsumer.commitSync] or [async][KafkaConsumer.commitAsync]
     * commits). This method will raise [TimeoutException] if the producer cannot send offsets
     * before expiration of `max.block.ms`. Additionally, it will raise [InterruptException] if
     * interrupted.
     *
     * @throws IllegalStateException if no transactional.id has been configured or no transaction
     * has been started.
     * @throws ProducerFencedException fatal error indicating another producer with the same
     * transactional.id is active
     * @throws org.apache.kafka.common.errors.UnsupportedVersionException fatal error indicating the
     * broker does not support transactions (i.e. if its version is lower than 0.11.0.0) or the
     * broker doesn't support latest version of transactional API with all consumer group metadata
     * (i.e. if its version is lower than 2.5.0).
     * @throws org.apache.kafka.common.errors.UnsupportedForMessageFormatException fatal error
     * indicating the message format used for the offsets topic on the broker does not support
     * transactions
     * @throws org.apache.kafka.common.errors.AuthorizationException fatal error indicating that the
     * configured transactional.id is not authorized, or the consumer group id is not authorized.
     * @throws org.apache.kafka.clients.consumer.CommitFailedException if the commit failed and
     * cannot be retried (e.g. if the consumer has been kicked out of the group). Users should
     * handle this by aborting the transaction.
     * @throws org.apache.kafka.common.errors.FencedInstanceIdException if this producer instance
     * gets fenced by broker due to a mis-configured consumer instance id within group metadata.
     * @throws org.apache.kafka.common.errors.InvalidProducerEpochException if the producer has
     * attempted to produce with an old epoch to the partition leader. See the exception for more
     * details
     * @throws KafkaException if the producer has encountered a previous fatal or abortable error,
     * or for any other unexpected error
     * @throws TimeoutException if the time taken for sending the offsets has surpassed
     * `max.block.ms`.
     * @throws InterruptException if the thread is interrupted while blocked
     */
    @Throws(ProducerFencedException::class)
    override fun sendOffsetsToTransaction(
        offsets: Map<TopicPartition, OffsetAndMetadata>,
        groupMetadata: ConsumerGroupMetadata,
    ) {
        throwIfInvalidGroupMetadata(groupMetadata)
        throwIfNoTransactionManager()
        throwIfProducerClosed()
        if (offsets.isNotEmpty()) {
            val start = time.nanoseconds()
            val result = transactionManager!!.sendOffsetsToTransaction(offsets, groupMetadata)
            sender!!.wakeup()
            result.await(maxBlockTimeMs, TimeUnit.MILLISECONDS)
            producerMetrics.recordSendOffsets(time.nanoseconds() - start)
        }
    }

    /**
     * Commits the ongoing transaction. This method will flush any unsent records before actually
     * committing the transaction.
     *
     * Further, if any of the [send] calls which were part of the transaction hit irrecoverable
     * errors, this method will throw the last received exception immediately and the transaction
     * will not be committed. So all [send] calls in a transaction must succeed in order for this
     * method to succeed.
     *
     * If the transaction is committed successfully and this method returns without throwing an
     * exception, it is guaranteed that all [callbacks][Callback] for records in the transaction
     * will have been invoked and completed. Note that exceptions thrown by callbacks are ignored;
     * the producer proceeds to commit the transaction in any case.
     *
     * Note that this method will raise [TimeoutException] if the transaction cannot be committed
     * before expiration of `max.block.ms`, but this does not mean the request did not actually
     * reach the broker. In fact, it only indicates that we cannot get the acknowledgement response
     * in time, so it's up to the application's logic to decide how to handle time outs.
     * Additionally, it will raise [InterruptException] if interrupted. It is safe to retry in
     * either case, but it is not possible to attempt a different operation (such as
     * abortTransaction) since the commit may already be in the progress of completing. If not
     * retrying, the only option is to close the producer.
     *
     * @throws IllegalStateException if no transactional.id has been configured or no transaction
     * has been started
     * @throws ProducerFencedException fatal error indicating another producer with the same
     * transactional.id is active
     * @throws org.apache.kafka.common.errors.UnsupportedVersionException fatal error indicating the
     * broker does not support transactions (i.e. if its version is lower than 0.11.0.0)
     * @throws org.apache.kafka.common.errors.AuthorizationException fatal error indicating that the
     * configured transactional.id is not authorized. See the exception for more details
     * @throws org.apache.kafka.common.errors.InvalidProducerEpochException if the producer has
     * attempted to produce with an old epoch to the partition leader. See the exception for more
     * details
     * @throws KafkaException if the producer has encountered a previous fatal or abortable error,
     * or for any other unexpected error
     * @throws TimeoutException if the time taken for committing the transaction has surpassed
     * `max.block.ms`.
     * @throws InterruptException if the thread is interrupted while blocked
     */
    @Throws(ProducerFencedException::class)
    override fun commitTransaction() {
        throwIfNoTransactionManager()
        throwIfProducerClosed()
        val commitStart = time.nanoseconds()
        val result = transactionManager!!.beginCommit()
        sender!!.wakeup()
        result.await(maxBlockTimeMs, TimeUnit.MILLISECONDS)
        producerMetrics.recordCommitTxn(time.nanoseconds() - commitStart)
    }

    /**
     * Aborts the ongoing transaction. Any unflushed produce messages will be aborted when this call
     * is made. This call will throw an exception immediately if any prior [send] calls failed with
     * a [ProducerFencedException] or an instance of
     * [org.apache.kafka.common.errors.AuthorizationException].
     *
     * Note that this method will raise [TimeoutException] if the transaction cannot be aborted
     * before expiration of `max.block.ms`, but this does not mean the request did not actually
     * reach the broker. In fact, it only indicates that we cannot get the acknowledgement response
     * in time, so it's up to the application's logic to decide how to handle time outs.
     * Additionally, it will raise [InterruptException] if interrupted. It is safe to retry in
     * either case, but it is not possible to attempt a different operation (such as
     * commitTransaction) since the abort may already be in the progress of completing. If not
     * retrying, the only option is to close the producer.
     *
     * @throws IllegalStateException if no transactional.id has been configured or no transaction
     * has been started
     * @throws ProducerFencedException fatal error indicating another producer with the same
     * transactional.id is active
     * @throws org.apache.kafka.common.errors.InvalidProducerEpochException if the producer has
     * attempted to produce with an old epoch to the partition leader. See the exception for more
     * details
     * @throws org.apache.kafka.common.errors.UnsupportedVersionException fatal error indicating the
     * broker does not support transactions (i.e. if its version is lower than 0.11.0.0)
     * @throws org.apache.kafka.common.errors.AuthorizationException fatal error indicating that the
     * configured transactional.id is not authorized. See the exception for more details
     * @throws KafkaException if the producer has encountered a previous fatal error or for any
     * other unexpected error
     * @throws TimeoutException if the time taken for aborting the transaction has surpassed
     * `max.block.ms`.
     * @throws InterruptException if the thread is interrupted while blocked
     */
    @Throws(ProducerFencedException::class)
    override fun abortTransaction() {
        throwIfNoTransactionManager()
        throwIfProducerClosed()
        log.info("Aborting incomplete transaction")
        val abortStart = time.nanoseconds()
        val result = transactionManager!!.beginAbort()
        sender!!.wakeup()
        result.await(maxBlockTimeMs, TimeUnit.MILLISECONDS)
        producerMetrics.recordAbortTxn(time.nanoseconds() - abortStart)
    }

    /**
     * Asynchronously send a record to a topic. Equivalent to `send(record, null)`.
     * See [send] for details.
     */
    override fun send(record: ProducerRecord<K, V>): Future<RecordMetadata> {
        return send(record, null)
    }

    /**
     * Asynchronously send a record to a topic and invoke the provided callback when the send has
     * been acknowledged.
     *
     * The send is asynchronous and this method will return immediately once the record has been
     * stored in the buffer of records waiting to be sent. This allows sending many records in
     * parallel without blocking to wait for the response after each one.
     *
     * The result of the send is a [RecordMetadata] specifying the partition the record was sent to,
     * the offset it was assigned and the timestamp of the record. If the producer is configured
     * with acks = 0, the [RecordMetadata] will have offset = -1 because the producer does not wait
     * for the acknowledgement from the broker. If
     * [CreateTime][org.apache.kafka.common.record.TimestampType.CREATE_TIME] is used by the topic,
     * the timestamp will be the user provided timestamp or the record send time if the user did not
     * specify a timestamp for the record. If
     * [LogAppendTime][org.apache.kafka.common.record.TimestampType.LOG_APPEND_TIME] is used for the
     * topic, the timestamp will be the Kafka broker local time when the message is appended.
     *
     * Since the send call is asynchronous it returns a [Future][java.util.concurrent.Future] for
     * the [RecordMetadata] that will be assigned to this record. Invoking
     * [get()][java.util.concurrent.Future.get] on this future will block until the associated
     * request completes and then return the metadata for the record or throw any exception that
     * occurred while sending the record.
     *
     * If you want to simulate a simple blocking call you can call the `get()` method immediately:
     *
     * ```java
     * byte[] key = "key".getBytes();
     * byte[] value = "value".getBytes();
     * ProducerRecord<byte[],byte[]> record = new ProducerRecord<byte[],byte[]>("my-topic", key, value)
     * producer.send(record).get();
     * ```
     *
     * Fully non-blocking usage can make use of the [Callback] parameter to provide a callback that
     * will be invoked when the request is complete.
     *
     * ```java
     * ProducerRecord<byte[],byte[]> record = new ProducerRecord<byte[],byte[]>("the-topic", key, value);
     * producer.send(myRecord,
     * new Callback() {
     * public void onCompletion(RecordMetadata metadata, Exception e) {
     * if(e != null) {
     * e.printStackTrace();
     * } else {
     * System.out.println("The offset of the record we just sent is: " + metadata.offset());
     * }
     * }
     * });
     * ```
     *
     * Callbacks for records being sent to the same partition are guaranteed to execute in order.
     * That is, in the following example `callback1` is guaranteed to execute before `callback2`:
     *
     * ```java
     * producer.send(new ProducerRecord<byte[],byte[]>(topic, partition, key1, value1), callback1);
     * producer.send(new ProducerRecord<byte[],byte[]>(topic, partition, key2, value2), callback2);
     * ```
     *
     * When used as part of a transaction, it is not necessary to define a callback or check the
     * result of the future in order to detect errors from `send`. If any of the send calls failed
     * with an irrecoverable error, the final [commitTransaction] call will fail and throw the
     * exception from the last failed send. When this happens, your application should call
     * [abortTransaction] to reset the state and continue to send data.
     *
     * Some transactional send errors cannot be resolved with a call to [abortTransaction]. In
     * particular, if a transactional send finishes with a [ProducerFencedException], a
     * [org.apache.kafka.common.errors.OutOfOrderSequenceException], a
     * [org.apache.kafka.common.errors.UnsupportedVersionException], or an
     * [org.apache.kafka.common.errors.AuthorizationException], then the only option left is to call
     * [close]. Fatal errors cause the producer to enter a defunct state in which future API calls
     * will continue to raise the same underlying error wrapped in a new [KafkaException].
     *
     * It is a similar picture when idempotence is enabled, but no `transactional.id` has been
     * configured. In this case, [org.apache.kafka.common.errors.UnsupportedVersionException] and
     * [org.apache.kafka.common.errors.AuthorizationException] are considered fatal errors. However,
     * [ProducerFencedException] does not need to be handled. Additionally, it is possible to
     * continue sending after receiving an
     * [org.apache.kafka.common.errors.OutOfOrderSequenceException], but doing so can result in out
     * of order delivery of pending messages. To ensure proper ordering, you should close the
     * producer and create a new instance.
     *
     * If the message format of the destination topic is not upgraded to 0.11.0.0, idempotent and
     * transactional produce requests will fail with an
     * [org.apache.kafka.common.errors.UnsupportedForMessageFormatException] error. If this is
     * encountered during a transaction, it is possible to abort and continue. But note that future
     * sends to the same topic will continue receiving the same exception until the topic is
     * upgraded.
     *
     * Note that callbacks will generally execute in the I/O thread of the producer and so should be
     * reasonably fast or they will delay the sending of messages from other threads. If you want to
     * execute blocking or computationally expensive callbacks it is recommended to use your own
     * [java.util.concurrent.Executor] in the callback body to parallelize processing.
     *
     * @param record The record to send
     * @param callback A user-supplied callback to execute when the record has been acknowledged by
     * the server (null indicates no callback)
     *
     * @throws AuthenticationException if authentication fails. See the exception for more details
     * @throws AuthorizationException fatal error indicating that the producer is not allowed to
     * write
     * @throws IllegalStateException if a transactional. id has been configured and no transaction
     * has been started, or when send is invoked after producer has been closed.
     * @throws InterruptException If the thread is interrupted while blocked
     * @throws SerializationException If the key or value are not valid objects given the configured
     * serializers
     * @throws TimeoutException If the record could not be appended to the send buffer due to memory
     * unavailable or missing metadata within `max.block.ms`.
     * @throws KafkaException If a Kafka related error occurs that does not belong to the public API
     * exceptions.
     */
    override fun send(record: ProducerRecord<K, V>, callback: Callback?): Future<RecordMetadata> {
        // intercept the record, which can be potentially modified; this method does not throw exceptions
        val interceptedRecord = interceptors.onSend(record)
        return doSend(interceptedRecord, callback)
    }

    // Verify that this producer instance has not been closed. This method throws
    // IllegalStateException if the producer has already been closed.
    private fun throwIfProducerClosed() = check(sender != null && sender.isRunning) {
        "Cannot perform operation after producer has been closed"
    }

    /**
     * Call deprecated [Partitioner.onNewBatch]
     */
    @Suppress("deprecation")
    private fun onNewBatch(topic: String, cluster: Cluster, prevPartition: Int) {
        assert(partitioner != null)
        partitioner!!.onNewBatch(topic, cluster, prevPartition)
    }

    /**
     * Implementation of asynchronously send a record to a topic.
     */
    private fun doSend(
        record: ProducerRecord<K, V>,
        callback: Callback?,
    ): Future<RecordMetadata> {
        // Append callback takes care of the following:
        //  - call interceptors and user callback on completion
        //  - remember partition that is calculated in RecordAccumulator.append
        val appendCallbacks: AppendCallbacks<K, V> = AppendCallbacks(
            userCallback = callback,
            interceptors = interceptors,
            record = record,
        )
        try {
            throwIfProducerClosed()
            // first make sure the metadata for the topic is available
            var nowMs = time.milliseconds()
            val clusterAndWaitTime: ClusterAndWaitTime
            try {
                clusterAndWaitTime = waitOnMetadata(
                    topic = record.topic,
                    partition = record.partition,
                    nowMs = nowMs,
                    maxWaitMs = maxBlockTimeMs
                )
            } catch (e: KafkaException) {
                if (metadata.isClosed) throw KafkaException(
                    "Producer closed while send in progress",
                    e
                )
                throw e
            }
            nowMs += clusterAndWaitTime.waitedOnMetadataMs
            val remainingWaitMs = (maxBlockTimeMs - clusterAndWaitTime.waitedOnMetadataMs)
                .coerceAtLeast(0)
            val cluster = clusterAndWaitTime.cluster
            val serializedKey: ByteArray?
            try {
                serializedKey = keySerializer.serialize(
                    record.topic,
                    record.headers,
                    record.key
                )
            } catch (cce: ClassCastException) {
                throw SerializationException(
                    message = "Can't convert key of class ${record.key?:Nothing::class.java.name}" +
                            " to class ${producerConfig.getClass(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG)!!.name}" +
                            " specified in key.serializer",
                    cause = cce,
                )
            }
            val serializedValue: ByteArray?
            try {
                serializedValue = valueSerializer.serialize(
                    topic = record.topic,
                    headers = record.headers,
                    data = record.value,
                )
            } catch (cce: ClassCastException) {
                throw SerializationException(
                    message = "Can't convert value of class ${record.value?:Nothing::class.java.name}" +
                            " to class ${producerConfig.getClass(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG)!!.name}" +
                            " specified in value.serializer",
                    cause = cce,
                )
            }

            // Try to calculate partition, but note that after this call it can be
            // RecordMetadata.UNKNOWN_PARTITION, which means that the RecordAccumulator would pick a
            // partition using built-in logic (which may take into account broker load, the amount
            // of data produced to each partition, etc.).
            var partition = partition(record, serializedKey, serializedValue, cluster)
            setReadOnly(record.headers)
            val headers = record.headers.toArray()
            val serializedSize = AbstractRecords.estimateSizeInBytesUpperBound(
                magic = apiVersions.maxUsableProduceMagic(),
                compressionType = compressionType,
                key = serializedKey,
                value = serializedValue,
                headers = headers,
            )
            ensureValidRecordSize(serializedSize)
            val timestamp = record.timestamp ?: nowMs

            // A custom partitioner may take advantage on the onNewBatch callback.
            val abortOnNewBatch = partitioner != null

            // Append the record to the accumulator. Note, that the actual partition may be
            // calculated there and can be accessed via appendCallbacks.topicPartition.
            var result = accumulator.append(
                topic = record.topic,
                partition = partition,
                timestamp = timestamp,
                key = serializedKey,
                value = serializedValue,
                headers = headers,
                callbacks = appendCallbacks,
                maxTimeToBlock = remainingWaitMs,
                abortOnNewBatch = abortOnNewBatch,
                nowMs = nowMs,
                cluster = cluster
            )
            assert(appendCallbacks.partition != RecordMetadata.UNKNOWN_PARTITION)
            if (result.abortForNewBatch) {
                val prevPartition = partition
                onNewBatch(record.topic, cluster, prevPartition)
                partition = partition(record, serializedKey, serializedValue, cluster)
                if (log.isTraceEnabled) {
                    log.trace(
                        "Retrying append due to new batch creation for topic {} partition {}. The old partition was {}",
                        record.topic,
                        partition,
                        prevPartition
                    )
                }
                result = accumulator.append(
                    topic = record.topic,
                    partition = partition,
                    timestamp = timestamp,
                    key = serializedKey,
                    value = serializedValue,
                    headers = headers,
                    callbacks = appendCallbacks,
                    maxTimeToBlock = remainingWaitMs,
                    abortOnNewBatch = false,
                    nowMs = nowMs,
                    cluster = cluster
                )
            }

            // Add the partition to the transaction (if in progress) after it has been successfully
            // appended to the accumulator. We cannot do it before because the partition may be
            // unknown or the initially selected partition may be changed when the batch is closed
            // (as indicated by `abortForNewBatch`). Note that the `Sender` will refuse to dequeue
            // batches from the accumulator until they have been added to the transaction.
            transactionManager?.maybeAddPartition(appendCallbacks.topicPartition()!!)
            if (result.batchIsFull || result.newBatchCreated) {
                log.trace(
                    "Waking up the sender since topic {} partition {} is either full or getting a new batch",
                    record.topic,
                    appendCallbacks.partition
                )
                sender!!.wakeup()
            }
            return result.future!!
            // handling exceptions and record the errors;
            // for API exceptions return them in the future,
            // for other exceptions throw directly
        } catch (e: ApiException) {
            log.debug("Exception occurred during message send:", e)
            if (callback != null) {
                val tp: TopicPartition? = appendCallbacks.topicPartition()
                val nullMetadata = RecordMetadata(
                    topicPartition = tp!!,
                    baseOffset = -1,
                    batchIndex = -1,
                    timestamp = RecordBatch.NO_TIMESTAMP,
                    serializedKeySize = -1,
                    serializedValueSize = -1
                )
                callback.onCompletion(nullMetadata, e)
            }
            errors.record()
            interceptors.onSendError(record, appendCallbacks.topicPartition(), e)
            transactionManager?.maybeTransitionToErrorState(e)
            return FutureFailure(e)
        } catch (e: InterruptedException) {
            errors.record()
            interceptors.onSendError(record, appendCallbacks.topicPartition(), e)
            throw InterruptException(cause = e)
        } catch (e: KafkaException) {
            errors.record()
            interceptors.onSendError(record, appendCallbacks.topicPartition(), e)
            throw e
        } catch (e: Exception) {
            // we notify interceptor about all exceptions, since onSend is called before anything
            // else in this method
            interceptors.onSendError(record, appendCallbacks.topicPartition(), e)
            throw e
        }
    }

    private fun setReadOnly(headers: Headers) {
        if (headers is RecordHeaders) {
            headers.setReadOnly()
        }
    }

    /**
     * Wait for cluster metadata including partitions for the given topic to be available.
     * @param topic The topic we want metadata for
     * @param partition A specific partition expected to exist in metadata, or null if there's no
     * preference
     * @param nowMs The current time in ms
     * @param maxWaitMs The maximum time in ms for waiting on the metadata
     * @return The cluster containing topic metadata and the amount of time we waited in ms
     * @throws TimeoutException if metadata could not be refreshed within `max.block.ms`
     * @throws KafkaException for all Kafka-related exceptions, including the case where this method
     * is called after producer close
     */
    @Throws(InterruptedException::class)
    private fun waitOnMetadata(
        topic: String,
        partition: Int?,
        nowMs: Long,
        maxWaitMs: Long
    ): ClusterAndWaitTime {
        // add topic to metadata topic list if it is not there already and reset expiry
        var cluster = metadata.fetch()
        if (cluster.invalidTopics.contains(topic)) throw InvalidTopicException(topic)
        metadata.add(topic, nowMs)
        var partitionsCount = cluster.partitionCountForTopic(topic)
        // Return cached metadata if we have it, and if the record's partition is either undefined
        // or within the known partition range
        if (partitionsCount != null && (partition == null || partition < partitionsCount)) return ClusterAndWaitTime(
            cluster,
            0
        )
        var remainingWaitMs = maxWaitMs
        var elapsed: Long = 0
        // Issue metadata requests until we have metadata for the topic and the requested partition,
        // or until maxWaitTimeMs is exceeded. This is necessary in case the metadata
        // is stale and the number of partitions for this topic has increased in the meantime.
        val nowNanos = time.nanoseconds()
        do {
            if (partition != null) {
                log.trace(
                    "Requesting metadata update for partition {} of topic {}.",
                    partition,
                    topic
                )
            } else {
                log.trace("Requesting metadata update for topic {}.", topic)
            }
            metadata.add(topic, nowMs + elapsed)
            val version = metadata.requestUpdateForTopic(topic)
            sender!!.wakeup()
            try {
                metadata.awaitUpdate(version, remainingWaitMs)
            } catch (ex: TimeoutException) {
                // Rethrow with original maxWaitMs to prevent logging exception with remainingWaitMs
                throw TimeoutException(
                    String.format(
                        "Topic %s not present in metadata after %d ms.",
                        topic, maxWaitMs
                    )
                )
            }
            cluster = metadata.fetch()
            elapsed = time.milliseconds() - nowMs
            if (elapsed >= maxWaitMs) {
                throw TimeoutException(
                    if (partitionsCount == null) String.format(
                        "Topic %s not present in metadata after %d ms.",
                        topic,
                        maxWaitMs
                    ) else String.format(
                        "Partition %d of topic %s with partition count %d is not present in" +
                                " metadata after %d ms.",
                        partition,
                        topic,
                        partitionsCount,
                        maxWaitMs
                    )
                )
            }
            metadata.maybeThrowExceptionForTopic(topic)
            remainingWaitMs = maxWaitMs - elapsed
            partitionsCount = cluster.partitionCountForTopic(topic)
        } while (partitionsCount == null || (partition != null && partition >= partitionsCount))
        producerMetrics.recordMetadataWait(time.nanoseconds() - nowNanos)
        return ClusterAndWaitTime(cluster, elapsed)
    }

    /**
     * Validate that the record size isn't too large
     */
    private fun ensureValidRecordSize(size: Int) {
        if (size > maxRequestSize) throw RecordTooLargeException(
            "The message is $size bytes when serialized which is larger than $maxRequestSize, which" +
                    " is the value of the ${ProducerConfig.MAX_REQUEST_SIZE_CONFIG} configuration."
        )
        if (size > totalMemorySize) throw RecordTooLargeException(
            "The message is $size bytes when serialized which is larger than the total memory" +
                    " buffer you have configured with the ${ProducerConfig.BUFFER_MEMORY_CONFIG}" +
                    " configuration."

        )
    }

    /**
     * Invoking this method makes all buffered records immediately available to send (even if
     * `linger.ms` is greater than 0) and blocks on the completion of the requests associated with
     * these records. The post-condition of `flush()` is that any previously sent record will have
     * completed (e.g. `Future.isDone() == true`). A request is considered completed when it is
     * successfully acknowledged according to the `acks` configuration you have specified or else
     * it results in an error.
     *
     * Other threads can continue sending records while one thread is blocked waiting for a flush
     * call to complete, however no guarantee is made about the completion of records sent after the
     * flush call begins.
     *
     * This method can be useful when consuming from some input system and producing into Kafka. The
     * `flush()` call gives a convenient way to ensure all previously sent messages have actually
     * completed.
     *
     * This example shows how to consume from one Kafka topic and produce to another Kafka topic:
     * ```java
     * for(ConsumerRecord<String, String> record: consumer.poll(100))
     * producer.send(new ProducerRecord("my-topic", record.key(), record.value());
     * producer.flush();
     * consumer.commitSync();
     * ```
     *
     * Note that the above example may drop records if the produce request fails. If we want to
     * ensure that this does not occur we need to set `retries=<large_number>` in our config.
     *
     * Applications don't need to call this method for transactional producers, since the
     * [commitTransaction] will flush all buffered records before performing the commit. This
     * ensures that all the [send] calls made since the previous [beginTransaction] are completed
     * before the commit.
     *
     * @throws InterruptException If the thread is interrupted while blocked
     */
    override fun flush() {
        log.trace("Flushing accumulated records in producer.")
        val start = time.nanoseconds()
        accumulator.beginFlush()
        sender!!.wakeup()
        try {
            accumulator.awaitFlushCompletion()
        } catch (e: InterruptedException) {
            throw InterruptException("Flush interrupted.", e)
        } finally {
            producerMetrics.recordFlush(time.nanoseconds() - start)
        }
    }

    /**
     * Get the partition metadata for the given topic. This can be used for custom partitioning.
     * @throws AuthenticationException if authentication fails. See the exception for more details
     * @throws AuthorizationException if not authorized to the specified topic. See the exception
     * for more details
     * @throws InterruptException if the thread is interrupted while blocked
     * @throws TimeoutException if metadata could not be refreshed within `max.block.ms`
     * @throws KafkaException for all Kafka-related exceptions, including the case where this method
     * is called after producer close
     */
    override fun partitionsFor(topic: String): List<PartitionInfo> {
        Objects.requireNonNull(topic, "topic cannot be null")
        try {
            return waitOnMetadata(
                topic = topic,
                partition = null,
                nowMs = time.milliseconds(),
                maxWaitMs = maxBlockTimeMs,
            ).cluster.partitionsForTopic(topic)
        } catch (e: InterruptedException) {
            throw InterruptException(cause = e)
        }
    }

    /**
     * Get the full set of internal metrics maintained by the producer.
     */
    override fun metrics(): Map<MetricName, Metric> = metrics.metrics.toMap()

    /**
     * Close this producer. This method blocks until all previously sent requests complete.
     * This method is equivalent to `close(Long.MAX_VALUE, TimeUnit.MILLISECONDS)`.
     *
     * **If [close] is called from [Callback], a warning message will be logged and
     * `close(0, TimeUnit.MILLISECONDS)` will be called instead. We do this because the sender
     * thread would otherwise try to join itself and block forever.**
     *
     * @throws InterruptException If the thread is interrupted while blocked.
     * @throws KafkaException If a unexpected error occurs while trying to close the client, this
     * error should be treated as fatal and indicate the client is no longer functionable.
     */
    override fun close() = close(Duration.ofMillis(Long.MAX_VALUE))

    /**
     * This method waits up to `timeout` for the producer to complete the sending of all incomplete
     * requests.
     *
     * If the producer is unable to complete all requests before the timeout expires, this method
     * will fail any unsent and unacknowledged records immediately. It will also abort the ongoing
     * transaction if it's not already completing.
     *
     * If invoked from within a [Callback] this method will not block and will be equivalent to
     * `close(Duration.ofMillis(0))`. This is done since no further sending will happen while
     * blocking the I/O thread of the producer.
     *
     * @param timeout The maximum time to wait for producer to complete any pending requests. The
     * value should be non-negative. Specifying a timeout of zero means do not wait for pending send
     * requests to complete.
     * @throws InterruptException If the thread is interrupted while blocked.
     * @throws KafkaException If a unexpected error occurs while trying to close the client, this
     * error should be treated as fatal and indicate the client is no longer functionable.
     * @throws IllegalArgumentException If the `timeout` is negative.
     */
    override fun close(timeout: Duration) {
        close(timeout, false)
    }

    private fun close(timeout: Duration, swallowException: Boolean) {
        val timeoutMs = timeout.toMillis()
        require(timeoutMs >= 0) { "The timeout cannot be negative." }
        log.info("Closing the Kafka producer with timeoutMillis = {} ms.", timeoutMs)

        // this will keep track of the first encountered exception
        val firstException = AtomicReference<Throwable?>()
        val invokedFromCallback = Thread.currentThread() === ioThread
        if (timeoutMs > 0) {
            if (invokedFromCallback) {
                log.warn(
                    "Overriding close timeout {} ms to 0 ms in order to prevent useless" +
                            " blocking due to self-join. This means you have incorrectly" +
                            " invoked close with a non-zero timeout from the producer call-back.",
                    timeoutMs
                )
            } else {
                // Try to close gracefully.
                sender?.initiateClose()
                if (ioThread != null) {
                    try {
                        ioThread.join(timeoutMs)
                    } catch (exception: InterruptedException) {
                        firstException.compareAndSet(
                            null,
                            InterruptException(cause = exception)
                        )
                        log.error("Interrupted while joining ioThread", exception)
                    }
                }
            }
        }
        if ((sender != null) && (ioThread != null) && ioThread.isAlive) {
            log.info(
                "Proceeding to force close the producer since pending requests could not be" +
                        " completed within timeout {} ms.", timeoutMs
            )
            sender.forceClose()
            // Only join the sender thread when not calling from callback.
            if (!invokedFromCallback) {
                try {
                    ioThread.join()
                } catch (e: InterruptedException) {
                    firstException.compareAndSet(null, InterruptException(cause = e))
                }
            }
        }
        Utils.closeQuietly(interceptors, "producer interceptors", firstException)
        Utils.closeQuietly(producerMetrics, "producer metrics wrapper", firstException)
        Utils.closeQuietly(metrics, "producer metrics", firstException)
        Utils.closeQuietly(keySerializer, "producer keySerializer", firstException)
        Utils.closeQuietly(valueSerializer, "producer valueSerializer", firstException)
        Utils.closeQuietly(partitioner, "producer partitioner", firstException)
        AppInfoParser.unregisterAppInfo(JMX_PREFIX, clientId, metrics)
        val exception = firstException.get()
        if (exception != null && !swallowException) {
            if (exception is InterruptException) {
                throw (exception as InterruptException?)!!
            }
            throw KafkaException("Failed to close kafka producer", exception)
        }
        log.debug("Kafka producer has been closed")
    }

    private fun configureClusterResourceListeners(
        keySerializer: Serializer<K>?,
        valueSerializer: Serializer<V>?,
        vararg candidateLists: List<*>
    ): ClusterResourceListeners {
        val clusterResourceListeners = ClusterResourceListeners()
        for (candidateList: List<*> in candidateLists) clusterResourceListeners.maybeAddAll(
            candidateList
        )
        clusterResourceListeners.maybeAdd(keySerializer)
        clusterResourceListeners.maybeAdd(valueSerializer)
        return clusterResourceListeners
    }

    /**
     * computes partition for given record.
     * if the record has partition returns the value otherwise
     * if custom partitioner is specified, call it to compute partition
     * otherwise try to calculate partition based on key.
     * If there is no key or key should be ignored return
     * RecordMetadata.UNKNOWN_PARTITION to indicate any partition
     * can be used (the partition is then calculated by built-in
     * partitioning logic).
     */
    private fun partition(
        record: ProducerRecord<K, V>,
        serializedKey: ByteArray?,
        serializedValue: ByteArray?,
        cluster: Cluster
    ): Int {
        if (record.partition != null) return record.partition
        if (partitioner != null) {
            val customPartition = partitioner.partition(
                record.topic,
                record.key,
                serializedKey,
                record.value,
                serializedValue,
                cluster
            )
            if (customPartition < 0) throw IllegalArgumentException(
                String.format(
                    "The partitioner generated an invalid partition number: %d. Partition number should always be non-negative.",
                    customPartition
                )
            )
            return customPartition
        }
        return if (serializedKey != null && !partitionerIgnoreKeys) {
            // hash the keyBytes to choose a partition
            BuiltInPartitioner.partitionForKey(
                serializedKey,
                cluster.partitionsForTopic(record.topic).size
            )
        } else RecordMetadata.UNKNOWN_PARTITION
    }

    private fun throwIfInvalidGroupMetadata(groupMetadata: ConsumerGroupMetadata?) {
        if (groupMetadata == null) {
            throw IllegalArgumentException("Consumer group metadata could not be null")
        } else if ((groupMetadata.generationId() > 0
                    && (JoinGroupRequest.UNKNOWN_MEMBER_ID == groupMetadata.memberId()))
        ) {
            throw IllegalArgumentException("Passed in group metadata $groupMetadata has generationId > 0 but member.id ")
        }
    }

    private fun throwIfNoTransactionManager() {
        if (transactionManager == null) throw IllegalStateException(
            ("Cannot use transactional methods without enabling transactions " +
                    "by setting the " + ProducerConfig.TRANSACTIONAL_ID_CONFIG + " configuration property")
        )
    }

    class ClusterAndWaitTime internal constructor(
        val cluster: Cluster,
        val waitedOnMetadataMs: Long
    )

    class FutureFailure(exception: Exception?) :
        Future<RecordMetadata> {
        private val exception: ExecutionException

        init {
            this.exception = ExecutionException(exception)
        }

        override fun cancel(interrupt: Boolean): Boolean {
            return false
        }

        @Throws(ExecutionException::class)
        override fun get(): RecordMetadata {
            throw exception
        }

        @Throws(ExecutionException::class)
        override fun get(timeout: Long, unit: TimeUnit): RecordMetadata {
            throw exception
        }

        override fun isCancelled(): Boolean {
            return false
        }

        override fun isDone(): Boolean {
            return true
        }
    }

    /**
     * Callbacks that are called by the RecordAccumulator append functions:
     * - user callback
     * - interceptor callbacks
     * - partition callback
     */
    private inner class AppendCallbacks<K, V> private constructor(
        private val userCallback: Callback?,
        private val interceptors: ProducerInterceptors<K, V>,
        private val topic: String?,
        private val recordPartition: Int?,
        private val recordLogString: String,
    ) : RecordAccumulator.AppendCallbacks {

        @Volatile
        var partition = RecordMetadata.UNKNOWN_PARTITION
            private set

        @Volatile
        private var topicPartition: TopicPartition? = null

        constructor(
            userCallback: Callback?,
            interceptors: ProducerInterceptors<K, V>,
            record: ProducerRecord<K, V>?,
        ) : this(
            userCallback = userCallback,
            interceptors = interceptors,
            // Extract record info as we don't want to keep a reference to the record during
            // whole lifetime of the batch.
            // We don't want to have an NPE here, because the interceptors would not be notified
            // (see [doSend]).
            topic = record?.topic,
            recordPartition = record?.partition,
            recordLogString = if (log.isTraceEnabled && record != null) record.toString() else ""
        )

        override fun onCompletion(metadata: RecordMetadata?, exception: Exception?) {
            val recordMetadata: RecordMetadata = metadata ?: RecordMetadata(
                topicPartition = topicPartition()!!,
                baseOffset = -1,
                batchIndex = -1,
                timestamp = RecordBatch.NO_TIMESTAMP,
                serializedKeySize = -1,
                serializedValueSize = -1,
            )
            this.interceptors.onAcknowledgement(recordMetadata, exception)
            userCallback?.onCompletion(recordMetadata, exception)
        }

        override fun setPartition(partition: Int) {
            assert(partition != RecordMetadata.UNKNOWN_PARTITION)
            this.partition = partition
            if (log.isTraceEnabled) {
                // Log the message here, because we don't know the partition before that.
                log.trace(
                    "Attempting to append record {} with callback {} to topic {} partition {}",
                    recordLogString,
                    userCallback,
                    topic,
                    partition
                )
            }
        }

        fun topicPartition(): TopicPartition? {
            if (topicPartition == null && topic != null) {
                topicPartition =
                    if (partition != RecordMetadata.UNKNOWN_PARTITION)
                        TopicPartition(topic, partition)
                    else if (recordPartition != null) TopicPartition(topic, recordPartition)
                    else TopicPartition(topic, RecordMetadata.UNKNOWN_PARTITION)
            }
            return topicPartition
        }
    }

    companion object {

        val JMX_PREFIX = "kafka.producer"

        val NETWORK_THREAD_PREFIX = "kafka-producer-network-thread"

        val PRODUCER_METRIC_GROUP_NAME = "producer-metrics"

        fun lingerMs(config: ProducerConfig): Int {
            return config.getLong(ProducerConfig.LINGER_MS_CONFIG)!!
                .coerceAtMost(Int.MAX_VALUE.toLong())
                .toInt()
        }

        fun configureDeliveryTimeout(config: ProducerConfig, log: Logger): Int {
            var deliveryTimeoutMs = config.getInt(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG)!!
            val lingerMs = lingerMs(config)
            val requestTimeoutMs = config.getInt(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG)!!
            val lingerAndRequestTimeoutMs = (lingerMs.toLong() + requestTimeoutMs)
                .coerceAtMost(Int.MAX_VALUE.toLong())
                .toInt()

            if (deliveryTimeoutMs < lingerAndRequestTimeoutMs) {
                if (config.originals().containsKey(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG)) {
                    // throw an exception if the user explicitly set an inconsistent value
                    throw ConfigException(
                        (ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG
                                + " should be equal to or larger than " + ProducerConfig.LINGER_MS_CONFIG
                                + " + " + ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG)
                    )
                } else {
                    // override deliveryTimeoutMs default value to lingerMs + requestTimeoutMs for backward compatibility
                    deliveryTimeoutMs = lingerAndRequestTimeoutMs
                    log.warn(
                        "{} should be equal to or larger than {} + {}. Setting it to {}.",
                        ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, ProducerConfig.LINGER_MS_CONFIG,
                        ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, deliveryTimeoutMs
                    )
                }
            }
            return deliveryTimeoutMs
        }
    }
}
