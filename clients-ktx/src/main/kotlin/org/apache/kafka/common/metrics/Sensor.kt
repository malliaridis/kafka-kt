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

package org.apache.kafka.common.metrics

import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.Lock
import java.util.concurrent.locks.ReentrantLock
import java.util.function.Supplier
import org.apache.kafka.common.MetricName
import org.apache.kafka.common.metrics.stats.TokenBucket
import org.apache.kafka.common.utils.Time
import kotlin.concurrent.withLock
import kotlin.math.max

/**
 * A sensor applies a continuous sequence of numerical values to a set of associated metrics. For
 * example a sensor on message size would record a sequence of message sizes using the [record] api
 * and would maintain a set of metrics about request sizes such as the average or max.
 */
class Sensor internal constructor(
    private val registry: Metrics,
    val name: String,
    val parents: List<Sensor> = emptyList(),
    private val config: MetricConfig,
    private val time: Time,
    inactiveSensorExpirationTimeSeconds: Long,
    private val recordingLevel: RecordingLevel
) {

    private val stats = mutableListOf<StatAndConfig>()

    private val metrics = mutableMapOf<MetricName, KafkaMetric>()

    @Volatile
    private var lastRecordTime: Long = time.milliseconds()

    private val inactiveSensorExpirationTimeMs: Long = TimeUnit.MILLISECONDS.convert(
        inactiveSensorExpirationTimeSeconds,
        TimeUnit.SECONDS
    )

    /**
     * KafkaMetrics of sensors which use SampledStat should be synchronized on the same lock for
     * sensor record and metric value read to allow concurrent reads and updates. For simplicity,
     * all sensors are synchronized on this object.
     *
     * Sensor object is not used as a lock for reading metric value since metrics reporter is
     * invoked while holding Sensor and Metrics locks to report addition and removal of metrics and
     * synchronized reporters may deadlock if Sensor lock is used for reading metrics values. Note
     * that Sensor object itself is used as a lock to protect the access to stats and metrics while
     * recording metric values, adding and deleting sensors.
     *
     * Locking order (assume all MetricsReporter methods may be synchronized):
     *
     * - Sensor#add: Sensor -> Metrics -> MetricsReporter
     * - Metrics#removeSensor: Sensor -> Metrics -> MetricsReporter
     * - KafkaMetric#metricValue: MetricsReporter -> Sensor#metricLock
     * - Sensor#record: Sensor -> Sensor#metricLock
     */
    private val metricLock: Lock = ReentrantLock()

    init {
        checkForest(mutableSetOf())
    }

    /**
     * Validate that this sensor doesn't end up referencing itself.
     */
    private fun checkForest(sensors: MutableSet<Sensor>) {
        require(sensors.add(this)) { "Circular dependency in sensors: $name is its own parent." }
        for (parent in parents) parent.checkForest(sensors)
    }

    /**
     * The name this sensor is registered with. This name will be unique among all registered
     * sensors.
     */
    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("name")
    )
    fun name(): String = name

    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("parents")
    )
    fun parents(): List<Sensor> = parents

    /**
     * @return `true` if the sensor's record level indicates that the metric will be recorded,
     * `false` otherwise
     */
    fun shouldRecord(): Boolean = recordingLevel.shouldRecord(config.recordingLevel.id.toInt())


    /**
     * Record a value at a known time. performance is slightly better when reusing a timestamp via
     * [timeMs].
     *
     * @param value The value we are recording
     * @param timeMs The current POSIX time in milliseconds
     * @param checkQuotas Indicate if quota must be enforced or not
     * @throws QuotaViolationException if recording this value moves a metric beyond its configured
     * maximum or minimum bound
     */
    fun record(
        value: Double = 1.0,
        timeMs: Long = time.milliseconds(),
        checkQuotas: Boolean = true,
    ) {
        if (shouldRecord()) recordInternal(
            value = value,
            timeMs = timeMs,
            checkQuotas = checkQuotas,
        )
    }

    private fun recordInternal(value: Double, timeMs: Long, checkQuotas: Boolean) {
        lastRecordTime = timeMs
        synchronized(this) {
            metricLock.withLock {
                // increment all the stats
                stats.forEach { statAndConfig ->
                    statAndConfig.stat.record(statAndConfig.config(), value, timeMs)
                }
            }
            if (checkQuotas) checkQuotas(timeMs)
        }

        for (parent in parents) parent.record(value, timeMs, checkQuotas)
    }

    /**
     * Check if we have violated our quota for any metric that has a configured quota.
     */
    fun checkQuotas(timeMs: Long = time.milliseconds()) {
        metrics.values.forEach { metric ->
            val config = metric.config
            val quota = config.quota ?: return@forEach

            val value = metric.measurableValue(timeMs)
            if (metric.measurable() is TokenBucket) {
                if (value < 0) throw QuotaViolationException(metric, value, quota.bound())
            } else if (!quota.acceptable(value))
                throw QuotaViolationException(metric, value, quota.bound())
        }
    }

    /**
     * Register a compound statistic with this sensor which yields multiple measurable quantities
     * (like a histogram).
     *
     * @param stat The stat to register
     * @param config The configuration for this stat. the stat will use the default configuration
     * for this sensor if not provided.
     * @return `true` if stat is added to sensor, `false` if sensor is expired
     */
    @Synchronized
    fun add(stat: CompoundStat, config: MetricConfig = this.config): Boolean {
        if (hasExpired()) return false
        stats.add(StatAndConfig(stat) { config })

        stat.stats().forEach { measurable ->
            val metric = KafkaMetric(
                lock = metricLock,
                metricName = measurable.name,
                metricValueProvider = measurable.stat,
                config = config,
                time = time,
            )

            metrics.computeIfAbsent(metric.metricName()) {
                val existingMetric = registry.registerMetric(metric)
                require(existingMetric == null) {
                    "A metric named '${metric.metricName()}' already exists, can't register another one."
                }
                metric
            }
        }
        return true
    }

    /**
     * Register a metric with this sensor.
     *
     * @param metricName The name of the metric
     * @param stat The statistic to keep
     * @param config A special configuration for this metric. If not provided use the sensor default
     * configuration.
     * @return `true` if metric is added to sensor, `false` if sensor is expired
     */
    @Synchronized
    fun add(
        metricName: MetricName,
        stat: MeasurableStat,
        config: MetricConfig = this.config,
    ): Boolean {
        return if (hasExpired()) false
        else if (metrics.containsKey(metricName)) true
        else {
            val metric = KafkaMetric(
                lock = metricLock,
                metricName = metricName,
                metricValueProvider = stat,
                config = config,
                time = time
            )
            val existingMetric = registry.registerMetric(metric)
            require(existingMetric == null) {
                "A metric named '$metricName' already exists, can't register another one."
            }

            metrics[metric.metricName()] = metric
            stats.add(StatAndConfig(stat) { metric.config })
            true
        }
    }

    /**
     * Return if metrics were registered with this sensor.
     *
     * @return true if metrics were registered, false otherwise
     */
    @Synchronized
    fun hasMetrics(): Boolean = metrics.isNotEmpty()

    /**
     * Return true if the Sensor is eligible for removal due to inactivity.
     * false otherwise
     */
    fun hasExpired(): Boolean =
        time.milliseconds() - lastRecordTime > inactiveSensorExpirationTimeMs

    @Synchronized
    fun metrics(): List<KafkaMetric> = metrics.values.toList()

    /**
     * KafkaMetrics of sensors which use SampledStat should be synchronized on the same lock for
     * sensor record and metric value read to allow concurrent reads and updates. For simplicity,
     * all sensors are synchronized on this object.
     *
     * Sensor object is not used as a lock for reading metric value since metrics reporter is
     * invoked while holding Sensor and Metrics locks to report addition and removal of metrics and
     * synchronized reporters may deadlock if Sensor lock is used for reading metrics values. Note
     * that Sensor object itself is used as a lock to protect the access to stats and metrics while
     * recording metric values, adding and deleting sensors.
     *
     * Locking order (assume all MetricsReporter methods may be synchronized):
     *
     * - Sensor#add: Sensor -> Metrics -> MetricsReporter
     * - Metrics#removeSensor: Sensor -> Metrics -> MetricsReporter
     * - KafkaMetric#metricValue: MetricsReporter -> Sensor#metricLock
     * - Sensor#record: Sensor -> Sensor#metricLock
     */
    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("metricLock")
    )
    private fun metricLock(): Lock = metricLock

    private class StatAndConfig(
        val stat: Stat,
        private val configSupplier: Supplier<MetricConfig>
    ) {
        @Deprecated(
            message = "Use property instead",
            replaceWith = ReplaceWith("stat")
        )
        fun stat(): Stat = stat

        fun config(): MetricConfig = configSupplier.get()

        override fun toString(): String = "StatAndConfig(stat=$stat)"
    }

    /**
     * The permanent and immutable id of an API--this can't change ever
     */
    enum class RecordingLevel(val id: Short) {

        INFO(0),

        DEBUG(1),

        TRACE(2);

        fun shouldRecord(configId: Int): Boolean = when (configId.toShort()) {
            INFO.id -> id == INFO.id
            DEBUG.id -> id == INFO.id || id == DEBUG.id
            TRACE.id -> true
            else -> error("Did not recognize recording level $configId")
        }

        companion object {

            private val ID_TO_TYPE: Array<RecordingLevel?>

            private const val MIN_RECORDING_LEVEL_KEY = 0

            val MAX_RECORDING_LEVEL_KEY: Short

            init {
                val maxRL = maxOf((-1).toShort(), values().maxOf(RecordingLevel::id))

                val idToName = arrayOfNulls<RecordingLevel>(maxRL + 1)

                for (level in values()) idToName[level.id.toInt()] = level

                ID_TO_TYPE = idToName
                MAX_RECORDING_LEVEL_KEY = maxRL
            }

            fun forId(id: Int): RecordingLevel? {
                require(id in MIN_RECORDING_LEVEL_KEY .. MAX_RECORDING_LEVEL_KEY) {
                    String.format(
                        "Unexpected RecordLevel id `%d`, it should be between `%d` " +
                                "and `%d` (inclusive)",
                        id,
                        MIN_RECORDING_LEVEL_KEY,
                        MAX_RECORDING_LEVEL_KEY
                    )
                }

                return ID_TO_TYPE[id]
            }

            /**
             * Case-insensitive lookup by protocol name.
             */
            fun forName(name: String): RecordingLevel = valueOf(name.uppercase())
        }
    }
}
