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

import org.apache.kafka.clients.consumer.internals.FetchMetricsManager.Companion.topicPartitionTags
import org.apache.kafka.clients.consumer.internals.FetchMetricsManager.Companion.topicTags
import org.apache.kafka.common.MetricNameTemplate
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.utils.MockTime
import org.apache.kafka.common.utils.Time
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

class FetchMetricsManagerTest {
    
    private val time: Time = MockTime(
        autoTickMs = 1,
        currentTimeMs = 0,
        currentHighResTimeNs = 0,
    )
    
    private lateinit var metrics: Metrics
    
    private lateinit var metricsRegistry: FetchMetricsRegistry
    
    private lateinit var metricsManager: FetchMetricsManager
    
    @BeforeEach
    fun setup() {
        metrics = Metrics(time = time)
        metricsRegistry = FetchMetricsRegistry(
            tags = metrics.config.tags.keys,
            metricGrpPrefix = "test",
        )
        metricsManager = FetchMetricsManager(
            metrics = metrics,
            metricsRegistry = metricsRegistry,
        )
    }

    @AfterEach
    fun tearDown() {
        metrics.close()
    }

    @Test
    fun testLatency() {
        metricsManager.recordLatency(123)
        time.sleep(metrics.config.timeWindowMs + 1)
        metricsManager.recordLatency(456)
        assertEquals(289.5, metricValue(metricsRegistry.fetchLatencyAvg), EPSILON)
        assertEquals(456.0, metricValue(metricsRegistry.fetchLatencyMax), EPSILON)
    }

    @Test
    fun testBytesFetched() {
        metricsManager.recordBytesFetched(2)
        time.sleep(metrics.config.timeWindowMs + 1)
        metricsManager.recordBytesFetched(10)

        assertEquals(6.0, metricValue(metricsRegistry.fetchSizeAvg), EPSILON)
        assertEquals(10.0, metricValue(metricsRegistry.fetchSizeMax), EPSILON)
    }

    @Test
    fun testBytesFetchedTopic() {
        val topicName1 = TOPIC_NAME
        val topicName2 = "another-topic"
        val tags1 = topicTags(topicName1)
        val tags2 = topicTags(topicName2)

        metricsManager.recordBytesFetched(topicName1, 2)
        metricsManager.recordBytesFetched(topicName2, 1)
        time.sleep(metrics.config.timeWindowMs + 1)
        metricsManager.recordBytesFetched(topicName1, 10)
        metricsManager.recordBytesFetched(topicName2, 5)

        assertEquals(6.0, metricValue(metricsRegistry.topicFetchSizeAvg, tags1), EPSILON)
        assertEquals(10.0, metricValue(metricsRegistry.topicFetchSizeMax, tags1), EPSILON)
        assertEquals(3.0, metricValue(metricsRegistry.topicFetchSizeAvg, tags2), EPSILON)
        assertEquals(5.0, metricValue(metricsRegistry.topicFetchSizeMax, tags2), EPSILON)
    }

    @Test
    fun testRecordsFetched() {
        metricsManager.recordRecordsFetched(3)
        time.sleep(metrics.config.timeWindowMs + 1)
        metricsManager.recordRecordsFetched(15)

        assertEquals(9.0, metricValue(metricsRegistry.recordsPerRequestAvg), EPSILON)
    }

    @Test
    fun testRecordsFetchedTopic() {
        val topicName1 = TOPIC_NAME
        val topicName2 = "another-topic"
        val tags1 = topicTags(topicName1)
        val tags2 = topicTags(topicName2)

        metricsManager.recordRecordsFetched(topicName1, 2)
        metricsManager.recordRecordsFetched(topicName2, 1)
        time.sleep(metrics.config.timeWindowMs + 1)
        metricsManager.recordRecordsFetched(topicName1, 10)
        metricsManager.recordRecordsFetched(topicName2, 5)

        assertEquals(6.0, metricValue(metricsRegistry.topicRecordsPerRequestAvg, tags1), EPSILON)
        assertEquals(3.0, metricValue(metricsRegistry.topicRecordsPerRequestAvg, tags2), EPSILON)
    }

    @Test
    fun testPartitionLag() {
        val tags = topicPartitionTags(TP)
        metricsManager.recordPartitionLag(TP, 14)
        metricsManager.recordPartitionLag(TP, 8)
        time.sleep(metrics.config.timeWindowMs + 1)
        metricsManager.recordPartitionLag(TP, 5)

        assertEquals(14.0, metricValue(metricsRegistry.recordsLagMax), EPSILON)
        assertEquals(5.0, metricValue(metricsRegistry.partitionRecordsLag, tags), EPSILON)
        assertEquals(14.0, metricValue(metricsRegistry.partitionRecordsLagMax, tags), EPSILON)
        assertEquals(9.0, metricValue(metricsRegistry.partitionRecordsLagAvg, tags), EPSILON)
    }

    @Test
    fun testPartitionLead() {
        val tags: Map<String, String> = topicPartitionTags(TP)
        metricsManager.recordPartitionLead(TP, 15)
        metricsManager.recordPartitionLead(TP, 11)
        time.sleep(metrics.config.timeWindowMs + 1)
        metricsManager.recordPartitionLead(TP, 13)

        assertEquals(11.0, metricValue(metricsRegistry.recordsLeadMin), EPSILON)
        assertEquals(13.0, metricValue(metricsRegistry.partitionRecordsLead, tags), EPSILON)
        assertEquals(11.0, metricValue(metricsRegistry.partitionRecordsLeadMin, tags), EPSILON)
        assertEquals(13.0, metricValue(metricsRegistry.partitionRecordsLeadAvg, tags), EPSILON)
    }

    private fun metricValue(name: MetricNameTemplate): Double {
        val metricName = metrics.metricInstance(name)
        val metric = metrics.metric(metricName)!!
        return metric.metricValue() as Double
    }

    private fun metricValue(name: MetricNameTemplate, tags: Map<String, String>): Double {
        val metricName = metrics.metricInstance(name, tags)
        val metric = metrics.metric(metricName)!!
        return metric.metricValue() as Double
    }

    companion object {
        
        private const val EPSILON = 0.0001
        
        private const val TOPIC_NAME = "test"
        
        private val TP = TopicPartition(TOPIC_NAME, 0)
    }
}

