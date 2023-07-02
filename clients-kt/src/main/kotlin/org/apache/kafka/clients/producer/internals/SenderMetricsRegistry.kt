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

import org.apache.kafka.common.MetricName
import org.apache.kafka.common.MetricNameTemplate
import org.apache.kafka.common.metrics.Measurable
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.metrics.Sensor

class SenderMetricsRegistry(private val metrics: Metrics) {

    private val allTemplates = mutableListOf<MetricNameTemplate>()

    /* * * * * * * * */
    /* Client level  */
    /* * * * * * * * */

    val batchSizeAvg: MetricName = createMetricName(
        name = "batch-size-avg",
        description = "The average number of bytes sent per partition per-request.",
    )

    val batchSizeMax: MetricName = createMetricName(
        name = "batch-size-max",
        description = "The max number of bytes sent per partition per-request.",
    )

    val compressionRateAvg: MetricName = createMetricName(
        name = "compression-rate-avg",
        description = "The average compression rate of record batches, defined as the average ratio " +
                "of the compressed batch size over the uncompressed size.",
    )

    val recordQueueTimeAvg: MetricName = createMetricName(
        name = "record-queue-time-avg",
        description = "The average time in ms record batches spent in the send buffer.",
    )

    val recordQueueTimeMax: MetricName = createMetricName(
        name = "record-queue-time-max",
        description = "The maximum time in ms record batches spent in the send buffer.",
    )

    val requestLatencyAvg: MetricName = createMetricName(
        name = "request-latency-avg",
        description = "The average request latency in ms",
    )

    val requestLatencyMax: MetricName = createMetricName(
        name = "request-latency-max",
        description = "The maximum request latency in ms",
    )

    val produceThrottleTimeAvg: MetricName = createMetricName(
        name = "produce-throttle-time-avg",
        description = "The average time in ms a request was throttled by a broker",
    )

    val produceThrottleTimeMax: MetricName = createMetricName(
        name = "produce-throttle-time-max",
        description = "The maximum time in ms a request was throttled by a broker",
    )

    val recordSendRate: MetricName = createMetricName(
        name = "record-send-rate",
        description = "The average number of records sent per second.",
    )

    val recordSendTotal: MetricName = createMetricName(
        name = "record-send-total",
        description = "The total number of records sent.",
    )

    val recordsPerRequestAvg: MetricName = createMetricName(
        name = "records-per-request-avg",
        description = "The average number of records per request.",
    )

    val recordRetryRate: MetricName = createMetricName(
        name = "record-retry-rate",
        description = "The average per-second number of retried record sends",
    )

    val recordRetryTotal: MetricName = createMetricName(
        name = "record-retry-total",
        description = "The total number of retried record sends",
    )

    val recordErrorRate: MetricName = createMetricName(
        name = "record-error-rate",
        description = "The average per-second number of record sends that resulted in errors",
    )

    val recordErrorTotal: MetricName = createMetricName(
        name = "record-error-total",
        description = "The total number of record sends that resulted in errors",
    )

    val recordSizeMax: MetricName = createMetricName(
        name = "record-size-max",
        description = "The maximum record size",
    )

    val recordSizeAvg: MetricName = createMetricName(
        name = "record-size-avg",
        description = "The average record size",
    )

    val requestsInFlight: MetricName = createMetricName(
        name = "requests-in-flight",
        description = "The current number of in-flight requests awaiting a response.",
    )

    val metadataAge: MetricName = createMetricName(
        name = "metadata-age",
        description = "The age in seconds of the current producer metadata being used.",
    )

    val batchSplitRate: MetricName = createMetricName(
        name = "batch-split-rate",
        description = "The average number of batch splits per second",
    )

    val batchSplitTotal: MetricName = createMetricName(
        name = "batch-split-total",
        description = "The total number of batch splits",
    )

    private val tags: Set<String> = metrics.config.tags.keys

    /* * * * * * * */
    /* Topic level */
    /* * * * * * * */
    // We can't create the MetricName up front for these, because we don't know the topic name yet.

    private val topicTags: LinkedHashSet<String> = LinkedHashSet(tags).apply {
        add("topic")
    }

    private val topicRecordSendRate: MetricNameTemplate = createTopicTemplate(
        name = "record-send-rate",
        description = "The average number of records sent per second for a topic.",
    )

    private val topicRecordSendTotal: MetricNameTemplate = createTopicTemplate(
        name = "record-send-total",
        description = "The total number of records sent for a topic.",
    )

    private val topicByteRate: MetricNameTemplate = createTopicTemplate(
        name = "byte-rate",
        description = "The average number of bytes sent per second for a topic.",
    )

    private val topicByteTotal: MetricNameTemplate = createTopicTemplate(
        name = "byte-total",
        description = "The total number of bytes sent for a topic.",
    )

    private val topicCompressionRate: MetricNameTemplate = createTopicTemplate(
        name = "compression-rate",
        description = "The average compression rate of record batches for a topic, defined as " +
                "the average ratio of the compressed batch size over the uncompressed size.",
    )

    private val topicRecordRetryRate: MetricNameTemplate = createTopicTemplate(
        name = "record-retry-rate",
        description = "The average per-second number of retried record sends for a topic",
    )

    private val topicRecordRetryTotal: MetricNameTemplate = createTopicTemplate(
        name = "record-retry-total",
        description = "The total number of retried record sends for a topic",
    )

    private val topicRecordErrorRate: MetricNameTemplate = createTopicTemplate(
        name = "record-error-rate",
        description = "The average per-second number of record sends that resulted in errors for " +
                "a topic",
    )

    private val topicRecordErrorTotal: MetricNameTemplate = createTopicTemplate(
        name = "record-error-total",
        description = "The total number of record sends that resulted in errors for a topic",
    )

    private fun createMetricName(name: String, description: String): MetricName =
        metrics.metricInstance(
            createTemplate(
                name = name,
                group = KafkaProducerMetrics.GROUP,
                description = description,
                tags = tags,
            )
        )

    private fun createTopicTemplate(name: String, description: String): MetricNameTemplate =
        createTemplate(
            name = name,
            group = TOPIC_METRIC_GROUP_NAME,
            description = description,
            tags = topicTags,
        )

    /* topic level metrics  */
    fun topicRecordSendRate(tags: Map<String, String>): MetricName =
        metrics.metricInstance(topicRecordSendRate, tags)

    fun topicRecordSendTotal(tags: Map<String, String>): MetricName =
        metrics.metricInstance(topicRecordSendTotal, tags)

    fun topicByteRate(tags: Map<String, String>): MetricName =
        metrics.metricInstance(topicByteRate, tags)

    fun topicByteTotal(tags: Map<String, String>): MetricName =
        metrics.metricInstance(topicByteTotal, tags)

    fun topicCompressionRate(tags: Map<String, String>): MetricName =
        metrics.metricInstance(topicCompressionRate, tags)

    fun topicRecordRetryRate(tags: Map<String, String>): MetricName =
        metrics.metricInstance(topicRecordRetryRate, tags)

    fun topicRecordRetryTotal(tags: Map<String, String>): MetricName =
        metrics.metricInstance(topicRecordRetryTotal, tags)

    fun topicRecordErrorRate(tags: Map<String, String>): MetricName =
        metrics.metricInstance(topicRecordErrorRate, tags)

    fun topicRecordErrorTotal(tags: Map<String, String>): MetricName =
        metrics.metricInstance(topicRecordErrorTotal, tags)

    fun allTemplates(): List<MetricNameTemplate> = allTemplates

    fun sensor(name: String): Sensor = metrics.sensor(name)

    fun addMetric(metricName: MetricName, measurable: Measurable) = metrics.addMetric(
        metricName = metricName,
        metricValueProvider = measurable,
    )

    fun getSensor(name: String): Sensor? = metrics.getSensor(name)

    private fun createTemplate(
        name: String,
        group: String,
        description: String,
        tags: Set<String>,
    ): MetricNameTemplate {
        val template = MetricNameTemplate(name, group, description, tags)
        allTemplates.add(template)
        return template
    }

    companion object {
        const val TOPIC_METRIC_GROUP_NAME = "producer-topic-metrics"
    }
}
