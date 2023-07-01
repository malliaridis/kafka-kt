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

import java.io.Closeable
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentMap
import java.util.concurrent.ScheduledThreadPoolExecutor
import java.util.concurrent.ThreadFactory
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock
import org.apache.kafka.common.MetricName
import org.apache.kafka.common.MetricNameTemplate
import org.apache.kafka.common.metrics.Sensor.RecordingLevel
import org.apache.kafka.common.metrics.internals.MetricsUtils
import org.apache.kafka.common.utils.KafkaThread
import org.apache.kafka.common.utils.Time
import org.slf4j.LoggerFactory

/**
 * A registry of sensors and metrics.
 *
 * A metric is a named, numerical measurement. A sensor is a handle to record numerical measurements
 * as they occur. Each Sensor has zero or more associated metrics. For example a Sensor might
 * represent message sizes and we might associate with this sensor a metric for the average,
 * maximum, or other statistics computed off the sequence of message sizes that are recorded by the
 * sensor.
 *
 * Usage looks something like this:
 *
 * ```java
 * // set up metrics:
 * Metrics metrics = new Metrics(); // this is the global repository of metrics and sensors
 * Sensor sensor = metrics.sensor("message-sizes");
 * MetricName metricName = new MetricName("message-size-avg", "producer-metrics");
 * sensor.add(metricName, new Avg());
 * metricName = new MetricName("message-size-max", "producer-metrics");
 * sensor.add(metricName, new Max());
 *
 * // as messages are sent we record the sizes
 * sensor.record(messageSize);
 * ```
 *
 * @constructor Create a metrics repository with a config, metric reporters and the ability to
 * expire eligible sensors
 * @property config The default config to use for all metrics that don't override their config
 * @property reporters The metrics reporters
 * @property time The time instance to use with the metrics
 * @param enableExpiration `true` if the metrics instance can garbage collect inactive sensors,
 * `false` otherwise
 * @param metricsContext The metricsContext to initialize metrics reporter with
 */
class Metrics(
    val config: MetricConfig = MetricConfig(),
    val reporters: MutableList<MetricsReporter> = ArrayList(0),
    private val time: Time = Time.SYSTEM,
    enableExpiration: Boolean = false,
    metricsContext: MetricsContext = KafkaMetricsContext("")
) : Closeable {

    val metrics = ConcurrentHashMap<MetricName, KafkaMetric>()

    private val sensors: ConcurrentMap<String, Sensor> = ConcurrentHashMap()

    private val childrenSensors: ConcurrentMap<Sensor, MutableList<Sensor>> = ConcurrentHashMap()

    private var metricsScheduler: ScheduledThreadPoolExecutor? = null

    init {
        reporters.forEach { reporter ->
            reporter.contextChange(metricsContext)
            reporter.init(emptyList())
        }

        // Create the ThreadPoolExecutor only if expiration of Sensors is enabled.
        metricsScheduler = if (enableExpiration) {
            ScheduledThreadPoolExecutor(1).apply {
                // Creating a daemon thread to not block shutdown
                threadFactory = ThreadFactory { runnable: Runnable? ->
                    KafkaThread.daemon("SensorExpiryThread", runnable)
                }
                scheduleAtFixedRate(ExpireSensorTask(), 30, 30, TimeUnit.SECONDS)
            }
        } else null

        addMetric(
            metricName(
                name = "count",
                group = "kafka-metrics-count",
                description = "total number of registered metrics"
            ),
            metricValueProvider = Measurable { _, _ -> metrics.size.toDouble() }
        )
    }

    /**
     * Create a MetricName with the given name, group, description and tags, plus default tags
     * specified in the metric configuration. Tag in tags takes precedence if the same tag key is
     * specified in the default metric configuration.
     *
     * @param name The name of the metric
     * @param group logical group name of the metrics to which this metric belongs
     * @param description A human-readable description to include in the metric
     * @param tags additional key/value attributes of the metric
     */
    fun metricName(
        name: String,
        group: String,
        description: String = "",
        tags: Map<String, String> = emptyMap()
    ): MetricName {
        val combinedTag: MutableMap<String, String> = LinkedHashMap(config.tags)
        combinedTag.putAll(tags)
        return MetricName(name, group, description, combinedTag.toMap())
    }

    /**
     * Create a MetricName with the given name, group, description, and keyValue as tags, plus
     * default tags specified in the metric configuration. Tag in keyValue takes precedence if the
     * same tag key is specified in the default metric configuration.
     *
     * @param name The name of the metric
     * @param group logical group name of the metrics to which this metric belongs
     * @param description A human-readable description to include in the metric
     * @param keyValue additional key/value attributes of the metric (must come in pairs)
     */
    fun metricName(
        name: String,
        group: String,
        description: String,
        vararg keyValue: String
    ): MetricName {
        return metricName(name, group, description, MetricsUtils.getTags(*keyValue))
    }

    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("config")
    )
    fun config(): MetricConfig = config

    /**
     * Get the sensor with the given name if it exists.
     *
     * @param name The name of the sensor
     * @return Return the sensor or `null` if no such sensor exists
     */
    fun getSensor(name: String): Sensor? = sensors[name]

    /**
     * Get or create a sensor with the given unique name and zero or more parent sensors. All parent
     * sensors will receive every value recorded with this sensor.
     *
     * @param name The name of the sensor
     * @param config A default configuration to use for this sensor for metrics that don't have
     * their own config
     * @param inactiveSensorExpirationTimeSeconds If no value is recorded on the Sensor for this
     * duration of time, it is eligible for removal
     * @param parents The parent sensors
     * @param recordingLevel The recording level
     * @return The sensor that is created
     */
    @Synchronized
    fun sensor(
        name: String,
        config: MetricConfig? = null,
        inactiveSensorExpirationTimeSeconds: Long = Long.MAX_VALUE,
        recordingLevel: RecordingLevel = RecordingLevel.INFO,
        vararg parents: Sensor
    ): Sensor {
        return sensors.computeIfAbsent(name) {
            val sensor = Sensor(
                registry = this,
                name = name,
                parents = parents.toList(),
                config = config ?: this.config,
                time = time,
                inactiveSensorExpirationTimeSeconds = inactiveSensorExpirationTimeSeconds,
                recordingLevel = recordingLevel,
            )

            parents.forEach { parent ->
                val children = childrenSensors.computeIfAbsent(parent) { mutableListOf() }
                children.add(sensor)
            }

            log.trace("Added sensor with name {}", name)
            sensor
        }
    }

    /**
     * Remove a sensor (if it exists), associated metrics and its children.
     *
     * @param name The name of the sensor to be removed
     */
    fun removeSensor(name: String) {
        val sensor = sensors[name] ?: return

        var childSensors: List<Sensor>? = null

        synchronized(sensor) {
            synchronized(this) {
                if (sensors.remove(name, sensor)) {
                    sensor.metrics().forEach { metric -> removeMetric(metric.metricName()) }
                    log.trace("Removed sensor with name {}", name)

                    childSensors = childrenSensors.remove(sensor)
                    sensor.parents.forEach { parent -> childrenSensors[parent]?.remove(sensor) }
                }
            }
        }

        childSensors?.forEach { childSensor ->  removeSensor(childSensor.name()) }
    }

    /**
     * Add a metric to monitor an object that implements Measurable or MetricValueProvider. This
     * metric won't be associated with any sensor. This is a way to expose existing values as
     * metrics. User is expected to add any additional synchronization to update and access metric
     * values, if required.
     *
     * @param metricName The name of the metric
     * @param config The configuration to use when measuring with [metricValueProvider]
     * @param metricValueProvider The metric value provider associated with this metric
     * @throws IllegalArgumentException if a metric with same name already exists.
     */
    fun addMetric(
        metricName: MetricName,
        config: MetricConfig = this.config,
        metricValueProvider: MetricValueProvider<*>
    ) {
        val metric = KafkaMetric(
            lock = ReentrantLock(),
            metricName = Objects.requireNonNull(metricName),
            metricValueProvider = Objects.requireNonNull(metricValueProvider),
            config = config,
            time = time,
        )

        require(registerMetric(metric) == null) {
            "A metric named '$metricName' already exists, can't register another one."
        }
    }

    /**
     * Create or get an existing metric to monitor an object that implements MetricValueProvider.
     * This metric won't be associated with any sensor. This is a way to expose existing values as
     * metrics. This method takes care of synchronisation while updating/accessing metrics by
     * concurrent threads.
     *
     * @param metricName The name of the metric
     * @param config The configuration to use when measuring with [metricValueProvider]
     * @param metricValueProvider The metric value provider associated with this metric
     * @return Existing KafkaMetric if already registered or else a newly created one
     */
    fun addMetricIfAbsent(
        metricName: MetricName,
        config: MetricConfig = this.config,
        metricValueProvider: MetricValueProvider<*>
    ): KafkaMetric {
        val metric = KafkaMetric(
            lock = ReentrantLock(),
            metricName = metricName,
            metricValueProvider = metricValueProvider,
            config = config,
            time = time
        )

        val existingMetric = registerMetric(metric)
        return existingMetric ?: metric
    }

    /**
     * Remove a metric if it exists and return it. Return null otherwise. If a metric is removed,
     * `metricRemoval` will be invoked for each reporter.
     *
     * @param metricName The name of the metric
     * @return the removed `KafkaMetric` or `null` if no such metric exists
     */
    @Synchronized
    fun removeMetric(metricName: MetricName): KafkaMetric? {
        val metric = metrics.remove(metricName) ?: return null

        reporters.forEach { reporter ->
            try {
                reporter.metricRemoval(metric)
            } catch (e: Exception) {
                log.error("Error when removing metric from ${reporter.javaClass.name}", e)
            }
        }
        log.trace("Removed metric named {}", metricName)

        return metric
    }

    /**
     * Add a MetricReporter
     */
    @Synchronized
    fun addReporter(reporter: MetricsReporter) {
        reporter.init(ArrayList(metrics.values))
        reporters.add(reporter)
    }

    /**
     * Remove a MetricReporter
     */
    @Synchronized
    fun removeReporter(reporter: MetricsReporter) {
        if (reporters.remove(reporter)) reporter.close()
    }

    /**
     * Register a metric if not present or return the already existing metric with the same name.
     * When a metric is newly registered, this method returns null
     *
     * @param metric The KafkaMetric to register
     * @return the existing metric with the same name or `null` if no metric exist.
     */
    @Synchronized
    fun registerMetric(metric: KafkaMetric): KafkaMetric? {
        val metricName = metric.metricName()
        val existingMetric = metrics.putIfAbsent(metricName, metric)
        if (existingMetric != null) return existingMetric

        // newly added metric
        reporters.forEach { reporter ->
            try {
                reporter.metricChange(metric)
            } catch (e: Exception) {
                log.error("Error when registering metric on " + reporter.javaClass.name, e)
            }
        }
        log.trace("Registered metric named {}", metricName)
        return null
    }

    /**
     * Get all the metrics currently maintained indexed by metricName
     */
    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("metrics")
    )
    fun metrics(): Map<MetricName, KafkaMetric> = metrics

    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("reporters")
    )
    fun reporters(): List<MetricsReporter> = reporters

    @Deprecated("Use property metrics directly instead")
    fun metric(metricName: MetricName): KafkaMetric? = metrics[metricName]

    /**
     * This iterates over every Sensor and triggers a removeSensor if it has expired Package private
     * for testing.
     */
    internal inner class ExpireSensorTask : Runnable {
        override fun run() {
            sensors.forEach { (key, value) ->
                // removeSensor also locks the sensor object. This is fine because synchronized is
                // reentrant. There is however a minor race condition here. Assume we have a parent
                // sensor P and child sensor C. Calling record on C would cause a record on P as
                // well. So expiration time for P == expiration time for C. If the record on P
                // happens via C just after P is removed, that will cause C to also get removed.
                // Since the expiration time is typically high it is not expected to be a
                // significant concern and thus not necessary to optimize
                synchronized(value) {
                    if (value.hasExpired()) {
                        log.debug("Removing expired sensor {}", key)
                        removeSensor(key)
                    }
                }
            }
        }
    }

    /* For testing use only. */
    fun childrenSensors(): Map<Sensor, MutableList<Sensor>> = childrenSensors.toMap()

    fun metricInstance(template: MetricNameTemplate, vararg keyValue: String): MetricName =
        metricInstance(template, MetricsUtils.getTags(*keyValue))

    fun metricInstance(template: MetricNameTemplate, tags: Map<String, String>): MetricName {
        // check to make sure that the runtime defined tags contain all the template tags.
        val runtimeTagKeys = config.tags.keys + tags.keys

        require(runtimeTagKeys == template.tags) {
            "For '${template.name}', runtime-defined metric tags do not match the tags in the " +
                    "template. Runtime = $runtimeTagKeys Template = ${template.tags}"
        }

        return this.metricName(template.name, template.group, template.description, tags)
    }

    /**
     * Close this metrics repository.
     */
    override fun close() {
        metricsScheduler?.let {
            it.shutdown()
            try {
                it.awaitTermination(30, TimeUnit.SECONDS)
            } catch (ex: InterruptedException) {
                // ignore and continue shutdown
                Thread.currentThread().interrupt()
            }
        }

        log.info("Metrics scheduler closed")
        reporters.forEach { reporter ->
            try {
                log.info("Closing reporter {}", reporter.javaClass.name)
                reporter.close()
            } catch (e: Exception) {
                log.error("Error when closing ${reporter.javaClass.name}", e)
            }
        }
        log.info("Metrics reporters closed")
    }

    companion object {

        private val log = LoggerFactory.getLogger(Metrics::class.java)

        /**
         * Use the specified domain and metric name templates to generate an HTML table documenting
         * the metrics. A separate table section will be generated for each of the MBeans and the
         * associated attributes. The MBean names are lexicographically sorted to determine the
         * order of these sections. This order is therefore dependent upon the order of the tags in
         * each [MetricNameTemplate].
         *
         * @param domain the domain or prefix for the JMX MBean names
         * @param allMetrics the collection of all [MetricNameTemplate] instances each describing
         * one metric
         * @return the string containing the HTML table
         */
        fun toHtmlTable(domain: String, allMetrics: Iterable<MetricNameTemplate>): String {
            val beansAndAttributes: MutableMap<String, MutableMap<String, String>> = TreeMap()
            Metrics().use { metrics ->

                allMetrics.forEach { template ->
                    val tags = template.tags.associateWith { "{$it}" }

                    val metricName = metrics.metricName(
                        name = template.name,
                        group = template.group,
                        description = template.description,
                        tags = tags
                    )

                    val mBeanName = JmxReporter.getMBeanName(domain, metricName)
                    val attrAndDesc = beansAndAttributes.computeIfAbsent(mBeanName) { TreeMap() }

                    require(attrAndDesc.putIfAbsent(template.name, template.description) == null) {
                        "mBean '$mBeanName' attribute '${template.name}' is defined twice."
                    }
                }
            }

            val b = StringBuilder()
            b.append("<table class=\"data-table\"><tbody>\n")
            beansAndAttributes.forEach { (key, value) ->
                b.append("<tr>\n")
                b.append("<td colspan=3 class=\"mbeanName\" style=\"background-color:#ccc; font-weight: bold;\">")
                b.append(key)
                b.append("</td>")
                b.append("</tr>\n")
                b.append("<tr>\n")
                b.append("<th style=\"width: 90px\"></th>\n")
                b.append("<th>Attribute name</th>\n")
                b.append("<th>Description</th>\n")
                b.append("</tr>\n")
                value.forEach { (key1, value1) ->
                    b.append("<tr>\n")
                    b.append("<td></td>")
                    b.append("<td>")
                    b.append(key1)
                    b.append("</td>")
                    b.append("<td>")
                    b.append(value1)
                    b.append("</td>")
                    b.append("</tr>\n")
                }
            }
            b.append("</tbody></table>")

            return b.toString()
        }
    }
}
