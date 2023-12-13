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

import org.apache.kafka.common.MetricName
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.metrics.Gauge
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.metrics.Sensor
import org.apache.kafka.common.metrics.stats.WindowedCount

/**
 * The [FetchMetricsManager] class provides wrapper methods to record lag, lead, latency, and fetch metrics.
 * It keeps an internal ID of the assigned set of partitions which is updated to ensure the set of metrics it
 * records matches up with the topic-partitions in use.
 */
class FetchMetricsManager(
    private val metrics: Metrics,
    metricsRegistry: FetchMetricsRegistry,
) {

    private val metricsRegistry: FetchMetricsRegistry

    private val throttleTime: Sensor

    private val bytesFetched: Sensor

    private val recordsFetched: Sensor

    private val fetchLatency: Sensor

    private val recordsLag: Sensor

    private val recordsLead: Sensor

    private var assignmentId = 0

    private var assignedPartitions = emptySet<TopicPartition>()

    init {
        this.metricsRegistry = metricsRegistry
        throttleTime = SensorBuilder(metrics, "fetch-throttle-time")
            .withAvg(metricsRegistry.fetchThrottleTimeAvg)
            .withMax(metricsRegistry.fetchThrottleTimeMax)
            .build()
        bytesFetched = SensorBuilder(metrics, "bytes-fetched")
            .withAvg(metricsRegistry.fetchSizeAvg)
            .withMax(metricsRegistry.fetchSizeMax)
            .withMeter(metricsRegistry.bytesConsumedRate, metricsRegistry.bytesConsumedTotal)
            .build()
        recordsFetched = SensorBuilder(metrics, "records-fetched")
            .withAvg(metricsRegistry.recordsPerRequestAvg)
            .withMeter(metricsRegistry.recordsConsumedRate, metricsRegistry.recordsConsumedTotal)
            .build()
        fetchLatency = SensorBuilder(metrics, "fetch-latency")
            .withAvg(metricsRegistry.fetchLatencyAvg)
            .withMax(metricsRegistry.fetchLatencyMax)
            .withMeter(WindowedCount(), metricsRegistry.fetchRequestRate, metricsRegistry.fetchRequestTotal)
            .build()
        recordsLag = SensorBuilder(metrics, "records-lag")
            .withMax(metricsRegistry.recordsLagMax)
            .build()
        recordsLead = SensorBuilder(metrics, "records-lead")
            .withMin(metricsRegistry.recordsLeadMin)
            .build()
    }

    fun throttleTimeSensor(): Sensor = throttleTime

    fun recordLatency(requestLatencyMs: Long) {
        fetchLatency.record(requestLatencyMs.toDouble())
    }

    fun recordBytesFetched(bytes: Int) {
        bytesFetched.record(bytes.toDouble())
    }

    fun recordRecordsFetched(records: Int) {
        recordsFetched.record(records.toDouble())
    }

    fun recordBytesFetched(topic: String, bytes: Int) {
        val name = topicBytesFetchedMetricName(topic)
        val bytesFetched: Sensor = SensorBuilder(metrics, name) { topicTags(topic) }
            .withAvg(metricsRegistry.topicFetchSizeAvg)
            .withMax(metricsRegistry.topicFetchSizeMax)
            .withMeter(metricsRegistry.topicBytesConsumedRate, metricsRegistry.topicBytesConsumedTotal)
            .build()
        bytesFetched.record(bytes.toDouble())
    }

    fun recordRecordsFetched(topic: String, records: Int) {
        val name = topicRecordsFetchedMetricName(topic)
        val recordsFetched: Sensor = SensorBuilder(metrics, name) { topicTags(topic) }
            .withAvg(metricsRegistry.topicRecordsPerRequestAvg)
            .withMeter(metricsRegistry.topicRecordsConsumedRate, metricsRegistry.topicRecordsConsumedTotal)
            .build()
        recordsFetched.record(records.toDouble())
    }

    fun recordPartitionLag(tp: TopicPartition, lag: Long) {
        recordsLag.record(lag.toDouble())
        val name = partitionRecordsLagMetricName(tp)
        val recordsLag: Sensor = SensorBuilder(metrics, name) { topicPartitionTags(tp) }
            .withValue(metricsRegistry.partitionRecordsLag)
            .withMax(metricsRegistry.partitionRecordsLagMax)
            .withAvg(metricsRegistry.partitionRecordsLagAvg)
            .build()
        recordsLag.record(lag.toDouble())
    }

    fun recordPartitionLead(tp: TopicPartition, lead: Long) {
        recordsLead.record(lead.toDouble())
        val name = partitionRecordsLeadMetricName(tp)
        val recordsLead: Sensor = SensorBuilder(metrics, name) { topicPartitionTags(tp) }
            .withValue(metricsRegistry.partitionRecordsLead)
            .withMin(metricsRegistry.partitionRecordsLeadMin)
            .withAvg(metricsRegistry.partitionRecordsLeadAvg)
            .build()
        recordsLead.record(lead.toDouble())
    }

    /**
     * This method is called by the [fetch][Fetch] logic before it requests fetches in order to update the
     * internal set of metrics that are tracked.
     *
     * @param subscription [SubscriptionState] that contains the set of assigned partitions
     * @see SubscriptionState.assignmentId
     */
    fun maybeUpdateAssignment(subscription: SubscriptionState) {
        val newAssignmentId = subscription.assignmentId()
        if (assignmentId != newAssignmentId) {
            val newAssignedPartitions = subscription.assignedPartitions()
            for (tp in assignedPartitions) {
                if (!newAssignedPartitions.contains(tp)) {
                    metrics.removeSensor(partitionRecordsLagMetricName(tp))
                    metrics.removeSensor(partitionRecordsLeadMetricName(tp))
                    metrics.removeMetric(partitionPreferredReadReplicaMetricName(tp))
                }
            }
            for (tp in newAssignedPartitions) {
                if (!assignedPartitions.contains(tp)) {
                    val metricName = partitionPreferredReadReplicaMetricName(tp)
                    metrics.addMetricIfAbsent(
                        metricName = metricName,
                        config = null,
                        metricValueProvider = Gauge { _, _ -> subscription.preferredReadReplica(tp, 0L) ?: -1 },
                    )
                }
            }
            assignedPartitions = newAssignedPartitions
            assignmentId = newAssignmentId
        }
    }

    private fun partitionPreferredReadReplicaMetricName(tp: TopicPartition): MetricName {
        val metricTags = topicPartitionTags(tp)
        return metrics.metricInstance(metricsRegistry.partitionPreferredReadReplica, metricTags)
    }

    companion object {

        private fun topicBytesFetchedMetricName(topic: String): String = "topic.$topic.bytes-fetched"

        private fun topicRecordsFetchedMetricName(topic: String): String = "topic.$topic.records-fetched"

        private fun partitionRecordsLeadMetricName(tp: TopicPartition): String = "$tp.records-lead"

        private fun partitionRecordsLagMetricName(tp: TopicPartition): String = "$tp.records-lag"

        fun topicTags(topic: String): Map<String, String> =
            mapOf("topic" to topic.replace('.', '_'))

        fun topicPartitionTags(tp: TopicPartition): Map<String, String> = mapOf(
            "topic" to tp.topic.replace('.', '_'),
            "partition" to tp.partition.toString(),
        )
    }
}