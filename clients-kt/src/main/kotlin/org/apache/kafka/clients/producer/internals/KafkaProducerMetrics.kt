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
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.metrics.Sensor
import org.apache.kafka.common.metrics.stats.CumulativeSum

class KafkaProducerMetrics(private val metrics: Metrics) : AutoCloseable {

    private val tags: Map<String, String?> = metrics.config.tags

    private val flushTimeSensor: Sensor = newLatencySensor(
        name = FLUSH,
        description = "Total time producer has spent in flush in nanoseconds."
    )

    private val initTimeSensor: Sensor = newLatencySensor(
        name = TXN_INIT,
        description = "Total time producer has spent in initTransactions in nanoseconds."
    )

    private val beginTxnTimeSensor: Sensor = newLatencySensor(
        name = TXN_BEGIN,
        description = "Total time producer has spent in beginTransaction in nanoseconds."
    )

    private val sendOffsetsSensor: Sensor = newLatencySensor(
        name = TXN_SEND_OFFSETS,
        description = "Total time producer has spent in sendOffsetsToTransaction in nanoseconds."
    )

    private val commitTxnSensor: Sensor = newLatencySensor(
        name = TXN_COMMIT,
        description = "Total time producer has spent in commitTransaction in nanoseconds."
    )

    private val abortTxnSensor: Sensor = newLatencySensor(
        name = TXN_ABORT,
        description = "Total time producer has spent in abortTransaction in nanoseconds."
    )

    private val metadataWaitSensor: Sensor = newLatencySensor(
        name = METADATA_WAIT,
        description = "Total time producer has spent waiting on topic metadata in nanoseconds."
    )

    override fun close() {
        removeMetric(FLUSH)
        removeMetric(TXN_INIT)
        removeMetric(TXN_BEGIN)
        removeMetric(TXN_SEND_OFFSETS)
        removeMetric(TXN_COMMIT)
        removeMetric(TXN_ABORT)
        removeMetric(METADATA_WAIT)
    }

    fun recordFlush(duration: Long) = flushTimeSensor.record(duration.toDouble())

    fun recordInit(duration: Long) = initTimeSensor.record(duration.toDouble())

    fun recordBeginTxn(duration: Long) = beginTxnTimeSensor.record(duration.toDouble())

    fun recordSendOffsets(duration: Long) = sendOffsetsSensor.record(duration.toDouble())

    fun recordCommitTxn(duration: Long) = commitTxnSensor.record(duration.toDouble())

    fun recordAbortTxn(duration: Long) = abortTxnSensor.record(duration.toDouble())

    fun recordMetadataWait(duration: Long) = metadataWaitSensor.record(duration.toDouble())

    private fun newLatencySensor(name: String, description: String): Sensor {
        val sensor = metrics.sensor(name + TOTAL_TIME_SUFFIX)
        sensor.add(metricName(name, description), CumulativeSum())
        return sensor
    }

    private fun metricName(name: String, description: String): MetricName =
        metrics.metricName(name + TOTAL_TIME_SUFFIX, GROUP, description, tags)

    private fun removeMetric(name: String) = metrics.removeSensor(name + TOTAL_TIME_SUFFIX)

    companion object {

        const val GROUP = "producer-metrics"

        private const val FLUSH = "flush"

        private const val TXN_INIT = "txn-init"

        private const val TXN_BEGIN = "txn-begin"

        private const val TXN_SEND_OFFSETS = "txn-send-offsets"

        private const val TXN_COMMIT = "txn-commit"

        private const val TXN_ABORT = "txn-abort"

        private const val TOTAL_TIME_SUFFIX = "-time-ns-total"

        private const val METADATA_WAIT = "metadata-wait"
    }
}
