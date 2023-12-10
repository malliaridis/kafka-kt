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

import org.apache.kafka.common.TopicPartition

/**
 * Since we parse the message data for each partition from each fetch response lazily, fetch-level
 * metrics need to be aggregated as the messages from each partition are parsed. This class is used
 * to facilitate this incremental aggregation.
 */
internal class FetchMetricsAggregator(
    private val metricsManager: FetchMetricsManager,
    partitions: Set<TopicPartition>,
) {

    private val unrecordedPartitions: MutableSet<TopicPartition>

    private val fetchFetchMetrics = FetchMetrics()

    private val perTopicFetchMetrics: MutableMap<String, FetchMetrics> = HashMap()

    init {
        unrecordedPartitions = partitions.toMutableSet()
    }

    /**
     * After each partition is parsed, we update the current metric totals with the total bytes
     * and number of records parsed. After all partitions have reported, we write the metric.
     */
    fun record(partition: TopicPartition, bytes: Int, records: Int) {
        // Aggregate the metrics at the fetch level
        fetchFetchMetrics.increment(bytes, records)

        // Also aggregate the metrics on a per-topic basis.
        perTopicFetchMetrics.computeIfAbsent(partition.topic) { FetchMetrics() }
            .increment(bytes, records)

        maybeRecordMetrics(partition)
    }

    /**
     * Once we've detected that all of the [partitions][TopicPartition] for the fetch have been handled, we
     * can then record the aggregated metrics values. This is done at the fetch level and on a per-topic basis.
     *
     * @param partition [TopicPartition]
     */
    private fun maybeRecordMetrics(partition: TopicPartition) {
        unrecordedPartitions.remove(partition)
        if (unrecordedPartitions.isNotEmpty()) return

        // Record the metrics aggregated at the fetch level.
        metricsManager.recordBytesFetched(fetchFetchMetrics.bytes)
        metricsManager.recordRecordsFetched(fetchFetchMetrics.records)

        // Also record the metrics aggregated on a per-topic basis.
        for ((topic, fetchMetrics) in perTopicFetchMetrics) {
            metricsManager.recordBytesFetched(topic, fetchMetrics.bytes)
            metricsManager.recordRecordsFetched(topic, fetchMetrics.records)
        }
    }

    private class FetchMetrics {

        var bytes = 0

        var records = 0

        fun increment(bytes: Int, records: Int) {
            this.bytes += bytes
            this.records += records
        }
    }
}