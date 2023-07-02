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

package org.apache.kafka.common.metrics.internals

import java.util.concurrent.ConcurrentLinkedDeque
import java.util.concurrent.locks.Lock
import java.util.concurrent.locks.ReentrantLock
import java.util.function.Function
import org.apache.kafka.common.MetricName
import org.apache.kafka.common.metrics.Gauge
import org.apache.kafka.common.metrics.MetricConfig
import org.apache.kafka.common.metrics.MetricValueProvider
import org.apache.kafka.common.metrics.Metrics
import org.slf4j.Logger

/**
 * Manages a suite of integer Gauges.
 *
 * @property log The log4j logger.
 * @property suiteName The name of this suite.
 * @property metrics The metrics object to use.
 * @property metricNameCalculator A user-supplied callback which translates keys into unique metric
 * names.
 * @property maxEntries The maximum number of gauges that we will ever create at once.
 */
class IntGaugeSuite<K>(
    private val log: Logger,
    private val suiteName: String,
    val metrics: Metrics,
    private val metricNameCalculator: (K) -> MetricName,
    val maxEntries: Int
) : AutoCloseable {

    /**
     * A map from keys to gauges. Protected by the object monitor.
     */
    private val gauges: MutableMap<K, StoredIntGauge> = HashMap(1)

    /**
     * The keys of gauges that can be removed, since their value is zero.
     * Protected by the object monitor.
     */
    private val removable: MutableSet<K> = HashSet()

    /**
     * A lockless list of pending metrics additions and removals.
     */
    private val pending: ConcurrentLinkedDeque<PendingMetricsChange> = ConcurrentLinkedDeque()

    /**
     * A lock which serializes modifications to metrics. This lock is not
     * required to create a new pending operation.
     */
    private val modifyMetricsLock: Lock = ReentrantLock()

    /**
     * True if this suite is closed. Protected by the object monitor.
     */
    private var closed: Boolean = false

    init {
        log.trace(
            "{}: created new gauge suite with maxEntries = {}.",
            suiteName,
            maxEntries
        )
    }

    fun increment(key: K) {
        synchronized(this) {
            if (closed) {
                log.warn(
                    "{}: Attempted to increment {}, but the GaugeSuite was closed.",
                    suiteName,
                    key.toString()
                )
                return
            }

            var gauge = gauges[key]
            if (gauge != null) {
                // Fast path: increment the existing counter.
                if (gauge.increment() > 0) {
                    removable.remove(key)
                }
                return
            }

            if (gauges.size == maxEntries) {
                if (removable.isEmpty()) {
                    log.debug(
                        "{}: Attempted to increment {}, but there are already {} entries.",
                        suiteName,
                        key.toString(),
                        maxEntries
                    )
                    return
                }

                val iter = removable.iterator()
                val keyToRemove = iter.next()
                iter.remove()
                val metricNameToRemove = gauges[keyToRemove]!!.metricName
                gauges.remove(keyToRemove)
                pending.push(PendingMetricsChange(metricNameToRemove, null))
                log.trace(
                    "{}: Removing the metric {}, which has a value of 0.",
                    suiteName, keyToRemove.toString()
                )
            }

            val metricNameToAdd = metricNameCalculator(key)
            gauge = StoredIntGauge(metricNameToAdd)
            gauges[key] = gauge
            pending.push(PendingMetricsChange(metricNameToAdd, gauge))
            log.trace("{}: Adding a new metric {}.", suiteName, key.toString())
        }

        // Drop the object monitor and perform any pending metrics additions or removals.
        performPendingMetricsOperations()
    }

    /**
     * Perform pending metrics additions or removals. It is important to perform them in order. For
     * example, we don't want to try to remove a metric that we haven't finished adding yet.
     */
    private fun performPendingMetricsOperations() {
        modifyMetricsLock.lock()
        try {
            log.trace("{}: entering performPendingMetricsOperations", suiteName)
            var change = pending.pollLast()

            while (change != null) {
                if (change.provider == null) {
                    if (log.isTraceEnabled)
                        log.trace("{}: removing metric {}", suiteName, change.metricName)

                    metrics.removeMetric(change.metricName)
                } else {
                    if (log.isTraceEnabled)
                        log.trace("{}: adding metric {}", suiteName, change.metricName)

                    metrics.addMetric(
                        metricName = change.metricName,
                        metricValueProvider = change.provider!!,
                    )
                }
                change = pending.pollLast()
            }

            log.trace("{}: leaving performPendingMetricsOperations", suiteName)
        } finally {
            modifyMetricsLock.unlock()
        }
    }

    @Synchronized
    fun decrement(key: K) {
        if (closed) {
            log.warn(
                "{}: Attempted to decrement {}, but the gauge suite was closed.",
                suiteName, key.toString()
            )
            return
        }
        val gauge = gauges[key]
        if (gauge == null) {
            log.debug(
                "{}: Attempted to decrement {}, but no such metric was registered.",
                suiteName, key.toString()
            )
        } else {
            val cur = gauge.decrement()
            log.trace(
                "{}: Removed a reference to {}. {} reference(s) remaining.",
                suiteName, key.toString(), cur
            )
            if (cur <= 0) {
                removable.add(key)
            }
        }
    }

    @Synchronized
    override fun close() {
        if (closed) {
            log.trace("{}: gauge suite is already closed.", suiteName)
            return
        }
        closed = true
        var prevSize = 0
        val iter = gauges.values.iterator()
        while (iter.hasNext()) {
            pending.push(PendingMetricsChange(iter.next().metricName, null))
            prevSize++
            iter.remove()
        }
        performPendingMetricsOperations()
        log.trace("{}: closed {} metric(s).", suiteName, prevSize)
    }

    /**
     * Get the maximum number of metrics this suite can create.
     */
    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("maxEntries")
    )
    fun maxEntries(): Int = maxEntries

    // Visible for testing only.
    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("metrics")
    )
    fun metrics(): Metrics = metrics

    /**
     * Return a map from keys to current reference counts.
     * Visible for testing only.
     */
    @Synchronized
    fun values(): Map<K, Int> = gauges.mapValues { it.value.value() }


    /**
     * A pending metrics addition or removal.
     */
    private data class PendingMetricsChange(

        /**
         * The name of the metric to add or remove.
         */
        val metricName: MetricName,

        /**
         * In an addition, this field is the MetricValueProvider to add.
         * In a removal, this field is null.
         */
        val provider: MetricValueProvider<*>?
    )

    /**
     * The gauge object which we register with the metrics system.
     */
    private class StoredIntGauge(val metricName: MetricName) : Gauge<Int?> {

        private var value: Int = 1

        /**
         * This callback is invoked when the metrics system retrieves the value of this gauge.
         */
        @Synchronized
        override fun value(config: MetricConfig, now: Long): Int = value

        @Synchronized
        fun increment(): Int = ++value

        @Synchronized
        fun decrement(): Int = --value

        @Synchronized
        fun value(): Int = value
    }
}
