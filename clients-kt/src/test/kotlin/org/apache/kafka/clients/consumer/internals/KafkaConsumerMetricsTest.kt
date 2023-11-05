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

import org.apache.kafka.common.metrics.Metrics
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull

internal class KafkaConsumerMetricsTest {
    
    private val metrics = Metrics()
    
    private val consumerMetrics = KafkaConsumerMetrics(metrics = metrics, metricGrpPrefix = CONSUMER_GROUP_PREFIX)
    
    @Test
    fun shouldRecordCommitSyncTime() {
        // When:
        consumerMetrics.recordCommitSync(METRIC_VALUE)

        // Then:
        assertMetricValue(COMMIT_SYNC_TIME_TOTAL)
    }

    @Test
    fun shouldRecordCommittedTime() {
        // When:
        consumerMetrics.recordCommitted(METRIC_VALUE)

        // Then:
        assertMetricValue(COMMITTED_TIME_TOTAL)
    }

    @Test
    fun shouldRemoveMetricsOnClose() {
        // When:
        consumerMetrics.close()

        // Then:
        assertMetricRemoved(COMMIT_SYNC_TIME_TOTAL)
        assertMetricRemoved(COMMITTED_TIME_TOTAL)
    }

    private fun assertMetricRemoved(name: String) {
        assertNull(metrics.metric(metrics.metricName(name, CONSUMER_METRIC_GROUP)))
    }

    private fun assertMetricValue(name: String) {
        assertEquals(
            expected = metrics.metric(
                metricName = metrics.metricName(name = name, group = CONSUMER_METRIC_GROUP),
            )!!.metricValue(),
            actual = METRIC_VALUE.toDouble(),
        )
    }

    companion object {

        private const val METRIC_VALUE = 123L

        private const val CONSUMER_GROUP_PREFIX = "consumer"

        private const val CONSUMER_METRIC_GROUP = "consumer-metrics"

        private const val COMMIT_SYNC_TIME_TOTAL = "commit-sync-time-ns-total"

        private const val COMMITTED_TIME_TOTAL = "committed-time-ns-total"
    }
}