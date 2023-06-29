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
import org.apache.kafka.common.metrics.Measurable
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.metrics.Sensor
import org.apache.kafka.common.metrics.stats.Avg
import org.apache.kafka.common.metrics.stats.CumulativeSum
import org.apache.kafka.common.metrics.stats.Max
import java.util.concurrent.TimeUnit

class KafkaConsumerMetrics(
    private val metrics: Metrics,
    metricGrpPrefix: String,
) : AutoCloseable {

    private val lastPollMetricName: MetricName

    private val timeBetweenPollSensor: Sensor

    private val pollIdleSensor: Sensor

    private val committedSensor: Sensor

    private val commitSyncSensor: Sensor

    private var lastPollMs: Long = 0

    private var pollStartMs: Long = 0

    private var timeSinceLastPollMs: Long = 0

    init {
        val metricGroupName = "$metricGrpPrefix-metrics"
        val lastPoll = Measurable { _, now ->
            if (lastPollMs == 0L)
                // if no poll is ever triggered, just return -1.
                return@Measurable -1.0
            else return@Measurable TimeUnit.SECONDS.convert(
                now - lastPollMs,
                TimeUnit.MILLISECONDS
            ).toDouble()
        }
        lastPollMetricName = metrics.metricName(
            name = "last-poll-seconds-ago",
            group = metricGroupName,
            description = "The number of seconds since the last poll() invocation.",
        )
        metrics.addMetric(
            metricName = lastPollMetricName,
            metricValueProvider = lastPoll,
        )
        timeBetweenPollSensor = metrics.sensor("time-between-poll")
        timeBetweenPollSensor.add(
            metricName = metrics.metricName(
                name = "time-between-poll-avg",
                group = metricGroupName,
                description = "The average delay between invocations of poll() in milliseconds.",
            ),
            stat = Avg(),
        )
        timeBetweenPollSensor.add(
            metricName = metrics.metricName(
                name = "time-between-poll-max",
                group = metricGroupName,
                description = "The max delay between invocations of poll() in milliseconds.",
            ),
            stat = Max(),
        )
        pollIdleSensor = metrics.sensor("poll-idle-ratio-avg")
        pollIdleSensor.add(
            metricName = metrics.metricName(
                name = "poll-idle-ratio-avg",
                group = metricGroupName,
                description = "The average fraction of time the consumer's poll() is idle as " +
                        "opposed to waiting for the user code to process records.",
            ),
            stat = Avg()
        )
        commitSyncSensor = metrics.sensor("commit-sync-time-ns-total")
        commitSyncSensor.add(
            metricName = metrics.metricName(
                name = "commit-sync-time-ns-total",
                group = metricGroupName,
                description = "The total time the consumer has spent in commitSync in nanoseconds",
            ),
            stat = CumulativeSum(),
        )
        committedSensor = metrics.sensor("committed-time-ns-total")
        committedSensor.add(
            metricName = metrics.metricName(
                name = "committed-time-ns-total",
                group = metricGroupName,
                description = "The total time the consumer has spent in committed in nanoseconds",
            ),
            stat = CumulativeSum(),
        )
    }

    fun recordPollStart(pollStartMs: Long) {
        this.pollStartMs = pollStartMs
        timeSinceLastPollMs = if (lastPollMs != 0L) pollStartMs - lastPollMs else 0
        timeBetweenPollSensor.record(timeSinceLastPollMs.toDouble())
        lastPollMs = pollStartMs
    }

    fun recordPollEnd(pollEndMs: Long) {
        val pollTimeMs = pollEndMs - pollStartMs
        val pollIdleRatio = pollTimeMs * 1.0 / (pollTimeMs + timeSinceLastPollMs)
        pollIdleSensor.record(pollIdleRatio)
    }

    fun recordCommitSync(duration: Long) = commitSyncSensor.record(duration.toDouble())

    fun recordCommitted(duration: Long) = committedSensor.record(duration.toDouble())

    override fun close() = with(metrics) {
        removeMetric(lastPollMetricName)
        removeSensor(timeBetweenPollSensor.name)
        removeSensor(pollIdleSensor.name)
        removeSensor(commitSyncSensor.name)
        removeSensor(committedSensor.name)
    }
}
