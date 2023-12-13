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

import java.util.Collections
import org.apache.kafka.clients.ApiVersions
import org.apache.kafka.clients.ClientUtils.createConfiguredInterceptors
import org.apache.kafka.clients.ClientUtils.createNetworkClient
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.CommonClientConfigs.metricsReporters
import org.apache.kafka.clients.GroupRebalanceConfig
import org.apache.kafka.clients.Metadata
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerInterceptor
import org.apache.kafka.clients.consumer.OffsetResetStrategy
import org.apache.kafka.common.IsolationLevel
import org.apache.kafka.common.metrics.KafkaMetricsContext
import org.apache.kafka.common.metrics.MetricConfig
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.metrics.MetricsContext
import org.apache.kafka.common.metrics.Sensor
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.utils.LogContext
import org.apache.kafka.common.utils.Time

object ConsumerUtils {

    const val CONSUMER_JMX_PREFIX = "kafka.consumer"

    const val CONSUMER_METRIC_GROUP_PREFIX = "consumer"

    /**
     * A fixed, large enough value will suffice for max.
     */
    const val CONSUMER_MAX_INFLIGHT_REQUESTS_PER_CONNECTION = 100

    private const val CONSUMER_CLIENT_ID_METRIC_TAG = "client-id"

    fun createConsumerNetworkClient(
        config: ConsumerConfig,
        metrics: Metrics,
        logContext: LogContext,
        apiVersions: ApiVersions,
        time: Time,
        metadata: Metadata,
        throttleTimeSensor: Sensor?,
        retryBackoffMs: Long,
    ): ConsumerNetworkClient {
        val netClient = createNetworkClient(
            config = config,
            metrics = metrics,
            metricsGroupPrefix = CONSUMER_METRIC_GROUP_PREFIX,
            logContext = logContext,
            apiVersions = apiVersions,
            time = time,
            maxInFlightRequestsPerConnection = CONSUMER_MAX_INFLIGHT_REQUESTS_PER_CONNECTION,
            metadata = metadata,
            throttleTimeSensor = throttleTimeSensor,
        )

        // Will avoid blocking an extended period of time to prevent heartbeat thread starvation
        val heartbeatIntervalMs = config.getInt(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG)!!
        return ConsumerNetworkClient(
            logContext = logContext,
            client = netClient,
            metadata = metadata,
            time = time,
            retryBackoffMs = retryBackoffMs,
            requestTimeoutMs = config.getInt(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG)!!,
            maxPollTimeoutMs = heartbeatIntervalMs,
        )
    }

    fun createLogContext(config: ConsumerConfig, groupRebalanceConfig: GroupRebalanceConfig): LogContext {
        val groupId = groupRebalanceConfig.groupId.toString()
        val clientId = config.getString(CommonClientConfigs.CLIENT_ID_CONFIG)
        val logPrefix: String
        val groupInstanceId = groupRebalanceConfig.groupInstanceId
        logPrefix = if (groupInstanceId != null) {
            // If group.instance.id is set, we will append it to the log context.
            "[Consumer instanceId=$groupInstanceId, clientId=$clientId, groupId=$groupId] "
        } else "[Consumer clientId=$clientId, groupId=$groupId] "
        return LogContext(logPrefix)
    }

    fun createIsolationLevel(config: ConsumerConfig): IsolationLevel {
        val s = config.getString(ConsumerConfig.ISOLATION_LEVEL_CONFIG)!!.uppercase()
        return IsolationLevel.valueOf(s)
    }

    fun createSubscriptionState(config: ConsumerConfig, logContext: LogContext): SubscriptionState {
        val s = config.getString(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG)!!.uppercase()
        val strategy = OffsetResetStrategy.valueOf(s)
        return SubscriptionState(logContext, strategy)
    }

    fun createMetrics(config: ConsumerConfig, time: Time): Metrics {
        val clientId = config.getString(ConsumerConfig.CLIENT_ID_CONFIG)
        val metricsTags = mapOf(CONSUMER_CLIENT_ID_METRIC_TAG to clientId)
        val metricConfig = MetricConfig().apply {
            samples = config.getInt(ConsumerConfig.METRICS_NUM_SAMPLES_CONFIG)!!
            timeWindowMs = config.getLong(ConsumerConfig.METRICS_SAMPLE_WINDOW_MS_CONFIG)!!
            recordingLevel = Sensor.RecordingLevel.forName(
                config.getString(ConsumerConfig.METRICS_RECORDING_LEVEL_CONFIG)!!
            )
            tags = metricsTags
        }
        val reporters = metricsReporters(clientId, config).toMutableList()
        val metricsContext: MetricsContext = KafkaMetricsContext(
            CONSUMER_JMX_PREFIX,
            config.originalsWithPrefix(CommonClientConfigs.METRICS_CONTEXT_PREFIX)
        )
        return Metrics(
            config = metricConfig,
            reporters = reporters,
            time = time,
            metricsContext = metricsContext,
        )
    }

    fun createFetchMetricsManager(metrics: Metrics): FetchMetricsManager {
        val metricsTags = setOf(CONSUMER_CLIENT_ID_METRIC_TAG)
        val metricsRegistry = FetchMetricsRegistry(metricsTags, CONSUMER_METRIC_GROUP_PREFIX)
        return FetchMetricsManager(metrics, metricsRegistry)
    }

    fun <K, V> createFetchConfig(
        config: ConsumerConfig,
        keyDeserializer: Deserializer<K>,
        valueDeserializer: Deserializer<V>,
    ): FetchConfig<K, V> = FetchConfig(
        config = config,
        keyDeserializer = keyDeserializer,
        valueDeserializer = valueDeserializer,
        isolationLevel = createIsolationLevel(config),
    )

    fun <K, V> createConsumerInterceptors(config: ConsumerConfig): List<ConsumerInterceptor<K, V>> =
        createConfiguredInterceptors<ConsumerInterceptor<K, V>>(
            config = config,
            interceptorClassesConfigName = ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG
        )

    fun <K> createKeyDeserializer(
        config: ConsumerConfig,
        keyDeserializer: Deserializer<K>?,
    ): Deserializer<K> {
        var deserializer = keyDeserializer
        val clientId = config.getString(ConsumerConfig.CLIENT_ID_CONFIG)

        if (deserializer == null) {
            deserializer = config.getConfiguredInstance<Deserializer<K>>(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG)!!
            deserializer.configure(
                configs = config.originals(mapOf(ConsumerConfig.CLIENT_ID_CONFIG to clientId)),
                isKey = true,
            )
        } else config.ignore(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG)

        return deserializer
    }

    fun <V> createValueDeserializer(config: ConsumerConfig, valueDeserializer: Deserializer<V>?): Deserializer<V> {
        var deserializer = valueDeserializer
        val clientId = config.getString(ConsumerConfig.CLIENT_ID_CONFIG)

        if (deserializer == null) {
            deserializer =
                config.getConfiguredInstance<Deserializer<V>>(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG)!!
            deserializer.configure(
                configs = config.originals(mapOf(ConsumerConfig.CLIENT_ID_CONFIG to clientId)),
                isKey = false
            )
        } else config.ignore(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG)

        return deserializer
    }
}
