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

package org.apache.kafka.common.utils

import java.lang.management.ManagementFactory
import java.util.*
import javax.management.JMException
import javax.management.ObjectName
import org.apache.kafka.common.MetricName
import org.apache.kafka.common.metrics.Gauge
import org.apache.kafka.common.metrics.MetricConfig
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.utils.Sanitizer.jmxSanitize
import org.slf4j.LoggerFactory

object AppInfoParser {

    private val log = LoggerFactory.getLogger(AppInfoParser::class.java)

    val version: String

    val commitId: String

    internal const val DEFAULT_VALUE = "unknown"

    init {
        val props = Properties()
        try {
            AppInfoParser::class.java.getResourceAsStream("/kafka/kafka-version.properties")
                .use { resourceStream -> props.load(resourceStream) }
        } catch (e: Exception) {
            log.warn("Error while loading kafka-version.properties: {}", e.message)
        }
        version = props.getProperty("version", DEFAULT_VALUE).trim { it <= ' ' }
        commitId = props.getProperty("commitId", DEFAULT_VALUE).trim { it <= ' ' }
    }

    @Synchronized
    fun registerAppInfo(prefix: String, id: String?, metrics: Metrics?, nowMs: Long) {
        try {
            val name = ObjectName("$prefix:type=app-info,id=" + jmxSanitize(id))
            val mBean = AppInfo(nowMs)

            ManagementFactory.getPlatformMBeanServer().registerMBean(mBean, name)
            registerMetrics(metrics, mBean) // prefix will be added later by JmxReporter
        } catch (e: JMException) {
            log.warn("Error registering AppInfo mbean", e)
        }
    }

    @Synchronized
    fun unregisterAppInfo(prefix: String, id: String?, metrics: Metrics?) {
        val server = ManagementFactory.getPlatformMBeanServer()
        try {
            val name = ObjectName("$prefix:type=app-info,id=" + jmxSanitize(id))

            if (server.isRegistered(name)) server.unregisterMBean(name)
            unregisterMetrics(metrics)
        } catch (e: JMException) {
            log.warn("Error unregistering AppInfo mbean", e)
        } finally {
            log.info("App info {} for {} unregistered", prefix, id)
        }
    }

    private fun metricName(metrics: Metrics, name: String): MetricName =
        metrics.metricName(name, "app-info", "Metric indicating $name")

    private fun registerMetrics(metrics: Metrics?, appInfo: AppInfo) {
        if (metrics == null) return

        metrics.addMetric(
            metricName = metricName(metrics, "version"),
            metricValueProvider = ImmutableValue(appInfo.version),
        )

        metrics.addMetric(
            metricName = metricName(metrics, "commit-id"),
            metricValueProvider = ImmutableValue(appInfo.commitId),
        )

        metrics.addMetric(
            metricName = metricName(metrics, "start-time-ms"),
            metricValueProvider = ImmutableValue(appInfo.startTimeMs),
        )
    }

    private fun unregisterMetrics(metrics: Metrics?) {
        if (metrics == null) return

        metrics.removeMetric(metricName(metrics, "version"))
        metrics.removeMetric(metricName(metrics, "commit-id"))
        metrics.removeMetric(metricName(metrics, "start-time-ms"))
    }

    interface AppInfoMBean {

        val version: String

        val commitId: String

        val startTimeMs: Long
    }

    class AppInfo(override val startTimeMs: Long) : AppInfoMBean {

        init {
            log.info("Kafka version: {}", AppInfoParser.version)
            log.info("Kafka commitId: {}", AppInfoParser.commitId)
            log.info("Kafka startTimeMs: {}", startTimeMs)
        }

        override val version: String
            get() = AppInfoParser.version

        override val commitId: String
            get() = AppInfoParser.commitId
    }

    internal class ImmutableValue<T>(private val value: T) : Gauge<T> {

        override fun value(config: MetricConfig, now: Long): T = value
    }
}
