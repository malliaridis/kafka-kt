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

import org.apache.kafka.common.MetricNameTemplate

class FetcherMetricsRegistry(
    tags: Set<String> = emptySet(),
    metricGrpPrefix: String = "",
) {
    val fetchSizeAvg: MetricNameTemplate
    val fetchSizeMax: MetricNameTemplate
    val bytesConsumedRate: MetricNameTemplate
    val bytesConsumedTotal: MetricNameTemplate
    val recordsPerRequestAvg: MetricNameTemplate
    val recordsConsumedRate: MetricNameTemplate
    val recordsConsumedTotal: MetricNameTemplate
    val fetchLatencyAvg: MetricNameTemplate
    val fetchLatencyMax: MetricNameTemplate
    val fetchRequestRate: MetricNameTemplate
    val fetchRequestTotal: MetricNameTemplate
    val recordsLagMax: MetricNameTemplate
    val recordsLeadMin: MetricNameTemplate
    val fetchThrottleTimeAvg: MetricNameTemplate
    val fetchThrottleTimeMax: MetricNameTemplate

    val topicFetchSizeAvg: MetricNameTemplate
    val topicFetchSizeMax: MetricNameTemplate
    val topicBytesConsumedRate: MetricNameTemplate
    val topicBytesConsumedTotal: MetricNameTemplate
    val topicRecordsPerRequestAvg: MetricNameTemplate
    val topicRecordsConsumedRate: MetricNameTemplate
    val topicRecordsConsumedTotal: MetricNameTemplate
    val partitionRecordsLag: MetricNameTemplate
    val partitionRecordsLagMax: MetricNameTemplate
    val partitionRecordsLagAvg: MetricNameTemplate
    val partitionRecordsLead: MetricNameTemplate
    val partitionRecordsLeadMin: MetricNameTemplate
    val partitionRecordsLeadAvg: MetricNameTemplate
    val partitionPreferredReadReplica: MetricNameTemplate

    init {
        /***** Client level  */
        val groupName = "$metricGrpPrefix-fetch-manager-metrics"
        fetchSizeAvg = MetricNameTemplate(
            name = "fetch-size-avg",
            group = groupName,
            description = "The average number of bytes fetched per request",
            tagsNames = tags,
        )
        fetchSizeMax = MetricNameTemplate(
            name = "fetch-size-max",
            group = groupName,
            description = "The maximum number of bytes fetched per request",
            tagsNames = tags
        )
        bytesConsumedRate = MetricNameTemplate(
            name = "bytes-consumed-rate",
            group = groupName,
            description = "The average number of bytes consumed per second",
            tagsNames = tags
        )
        bytesConsumedTotal = MetricNameTemplate(
            name = "bytes-consumed-total",
            group = groupName,
            description = "The total number of bytes consumed",
            tagsNames = tags
        )
        recordsPerRequestAvg = MetricNameTemplate(
            name = "records-per-request-avg",
            group = groupName,
            description = "The average number of records in each request",
            tagsNames = tags
        )
        recordsConsumedRate = MetricNameTemplate(
            name = "records-consumed-rate",
            group = groupName,
            description = "The average number of records consumed per second",
            tagsNames = tags
        )
        recordsConsumedTotal = MetricNameTemplate(
            name = "records-consumed-total",
            group = groupName,
            description = "The total number of records consumed",
            tagsNames = tags
        )
        fetchLatencyAvg = MetricNameTemplate(
            name = "fetch-latency-avg",
            group = groupName,
            description = "The average time taken for a fetch request.",
            tagsNames = tags
        )
        fetchLatencyMax = MetricNameTemplate(
            name = "fetch-latency-max",
            group = groupName,
            description = "The max time taken for any fetch request.",
            tagsNames = tags
        )
        fetchRequestRate = MetricNameTemplate(
            name = "fetch-rate",
            group = groupName,
            description = "The number of fetch requests per second.",
            tagsNames = tags
        )
        fetchRequestTotal = MetricNameTemplate(
            name = "fetch-total",
            group = groupName,
            description = "The total number of fetch requests.",
            tagsNames = tags
        )
        recordsLagMax = MetricNameTemplate(
            name = "records-lag-max",
            group = groupName,
            description = "The maximum lag in terms of number of records for any partition in this window. NOTE: This is based on current offset and not committed offset",
            tagsNames = tags
        )
        recordsLeadMin = MetricNameTemplate(
            name = "records-lead-min",
            group = groupName,
            description = "The minimum lead in terms of number of records for any partition in this window",
            tagsNames = tags
        )
        fetchThrottleTimeAvg = MetricNameTemplate(
            name = "fetch-throttle-time-avg",
            group = groupName,
            description = "The average throttle time in ms",
            tagsNames = tags
        )
        fetchThrottleTimeMax = MetricNameTemplate(
            name = "fetch-throttle-time-max",
            group = groupName,
            description = "The maximum throttle time in ms",
            tagsNames = tags
        )
        /***** Topic level  */
        val topicTags: Set<String> = tags + "topic"

        topicFetchSizeAvg = MetricNameTemplate(
            name = "fetch-size-avg",
            group = groupName,
            description = "The average number of bytes fetched per request for a topic",
            tagsNames = topicTags
        )
        topicFetchSizeMax = MetricNameTemplate(
            name = "fetch-size-max",
            group = groupName,
            description = "The maximum number of bytes fetched per request for a topic",
            tagsNames = topicTags
        )
        topicBytesConsumedRate = MetricNameTemplate(
            name = "bytes-consumed-rate",
            group = groupName,
            description = "The average number of bytes consumed per second for a topic",
            tagsNames = topicTags
        )
        topicBytesConsumedTotal = MetricNameTemplate(
            name = "bytes-consumed-total",
            group = groupName,
            description = "The total number of bytes consumed for a topic",
            tagsNames = topicTags
        )
        topicRecordsPerRequestAvg = MetricNameTemplate(
            name = "records-per-request-avg",
            group = groupName,
            description = "The average number of records in each request for a topic",
            tagsNames = topicTags
        )
        topicRecordsConsumedRate = MetricNameTemplate(
            name = "records-consumed-rate",
            group = groupName,
            description = "The average number of records consumed per second for a topic",
            tagsNames = topicTags
        )
        topicRecordsConsumedTotal = MetricNameTemplate(
            name = "records-consumed-total",
            group = groupName,
            description = "The total number of records consumed for a topic",
            tagsNames = topicTags
        )
        /***** Partition level  */
        val partitionTags: Set<String> = topicTags + "partition"

        partitionRecordsLag = MetricNameTemplate(
            name = "records-lag",
            group = groupName,
            description = "The latest lag of the partition",
            tagsNames = partitionTags
        )
        partitionRecordsLagMax = MetricNameTemplate(
            name = "records-lag-max",
            group = groupName,
            description = "The max lag of the partition",
            tagsNames = partitionTags
        )
        partitionRecordsLagAvg = MetricNameTemplate(
            name = "records-lag-avg",
            group = groupName,
            description = "The average lag of the partition",
            tagsNames = partitionTags
        )
        partitionRecordsLead = MetricNameTemplate(
            name = "records-lead",
            group = groupName,
            description = "The latest lead of the partition",
            tagsNames = partitionTags
        )
        partitionRecordsLeadMin = MetricNameTemplate(
            name = "records-lead-min",
            group = groupName,
            description = "The min lead of the partition",
            tagsNames = partitionTags
        )
        partitionRecordsLeadAvg = MetricNameTemplate(
            name = "records-lead-avg",
            group = groupName,
            description = "The average lead of the partition",
            tagsNames = partitionTags
        )
        partitionPreferredReadReplica = MetricNameTemplate(
            name = "preferred-read-replica",
            group = "consumer-fetch-manager-metrics",
            description = "The current read replica for the partition, or -1 if reading from leader",
            tagsNames = partitionTags
        )
    }

    val allTemplates: List<MetricNameTemplate>
        get() = listOf(
            fetchSizeAvg,
            fetchSizeMax,
            bytesConsumedRate,
            bytesConsumedTotal,
            recordsPerRequestAvg,
            recordsConsumedRate,
            recordsConsumedTotal,
            fetchLatencyAvg,
            fetchLatencyMax,
            fetchRequestRate,
            fetchRequestTotal,
            recordsLagMax,
            recordsLeadMin,
            fetchThrottleTimeAvg,
            fetchThrottleTimeMax,
            topicFetchSizeAvg,
            topicFetchSizeMax,
            topicBytesConsumedRate,
            topicBytesConsumedTotal,
            topicRecordsPerRequestAvg,
            topicRecordsConsumedRate,
            topicRecordsConsumedTotal,
            partitionRecordsLag,
            partitionRecordsLagAvg,
            partitionRecordsLagMax,
            partitionRecordsLead,
            partitionRecordsLeadMin,
            partitionRecordsLeadAvg,
            partitionPreferredReadReplica
        )
}
