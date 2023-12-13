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

import java.lang.management.ManagementFactory
import java.util.concurrent.locks.ReentrantLock
import java.util.function.Predicate
import java.util.regex.Pattern
import java.util.regex.PatternSyntaxException
import javax.management.Attribute
import javax.management.AttributeList
import javax.management.AttributeNotFoundException
import javax.management.DynamicMBean
import javax.management.JMException
import javax.management.MBeanAttributeInfo
import javax.management.MBeanInfo
import javax.management.ObjectName
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.MetricName
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.common.utils.ConfigUtils.translateDeprecatedConfigs
import org.apache.kafka.common.utils.Sanitizer.jmxSanitize
import org.apache.kafka.common.utils.Utils.mkSet
import org.slf4j.LoggerFactory
import kotlin.concurrent.withLock

/**
 * Register metrics in JMX as dynamic mbeans based on the metric names
 *
 * @constructor Create a JMX reporter that prefixes all metrics with the given string.
 */
open class JmxReporter : MetricsReporter {

    private var prefix: String = ""

    private val mbeans: MutableMap<String, KafkaMbean> = HashMap()

    private var mbeanPredicate = Predicate { _: String -> true }

    constructor()

    /**
     * Create a JMX reporter that prefixes all metrics with the given string.
     * @deprecated Since 2.6.0. Use {@link JmxReporter#JmxReporter()}
     * Initialize JmxReporter with {@link JmxReporter#contextChange(MetricsContext)}
     * Populate prefix by adding _namespace/prefix key value pair to {@link MetricsContext}
     */
    @Deprecated("Since 2.6.0. Use {@link JmxReporter#JmxReporter()}.\n" +
            "- Initialize JmxReporter with {@link JmxReporter#contextChange(MetricsContext)}\n" +
            "- Populate prefix by adding _namespace/prefix key value pair to {@link MetricsContext}"
    )
    constructor(prefix: String) {
        this.prefix = prefix
    }

    override fun configure(configs: Map<String, *>) = reconfigure(configs)

    override fun reconfigurableConfigs(): Set<String> = RECONFIGURABLE_CONFIGS

    @Throws(ConfigException::class)
    override fun validateReconfiguration(configs: Map<String, *>) {
        compilePredicate(configs)
    }

    override fun reconfigure(configs: Map<String, *>) {
        lock.withLock {
            mbeanPredicate = compilePredicate(configs)
            mbeans.forEach { (name: String, mbean: KafkaMbean?) ->
                if (mbeanPredicate.test(name)) {
                    reregister(mbean)
                } else {
                    unregister(mbean)
                }
            }
        }
    }

    override fun init(metrics: List<KafkaMetric>) {
        lock.withLock {
            metrics.forEach { metric -> addAttribute(metric) }

            mbeans.forEach { (name, mbean) ->
                if (mbeanPredicate.test(name)) reregister(mbean)
            }
        }
    }

    fun containsMbean(mbeanName: String): Boolean = mbeans.containsKey(mbeanName)

    override fun metricChange(metric: KafkaMetric) {
        lock.withLock {
            val mbeanName = addAttribute(metric)
            if (mbeanName != null && mbeanPredicate.test(mbeanName)) {
                reregister(mbeans[mbeanName]!!)
            }
        }
    }

    override fun metricRemoval(metric: KafkaMetric) {
        lock.withLock {
            val metricName = metric.metricName()
            val mBeanName = getMBeanName(prefix, metricName)
            val mbean = removeAttribute(metric, mBeanName) ?: return

            if (mbean.metrics.isEmpty()) {
                unregister(mbean)
                mbeans.remove(mBeanName)
            } else if (mbeanPredicate.test(mBeanName)) reregister(mbean)
            else Unit
        }
    }

    private fun removeAttribute(metric: KafkaMetric, mBeanName: String): KafkaMbean? {
        val metricName = metric.metricName()
        val mbean = mbeans[mBeanName]
        mbean?.removeAttribute(metricName.name)
        return mbean
    }

    private fun addAttribute(metric: KafkaMetric): String? {
        return try {
            val metricName = metric.metricName()
            val mBeanName = getMBeanName(prefix, metricName)

            val mbean = mbeans.computeIfAbsent(mBeanName) { KafkaMbean(mBeanName) }
            mbean.setAttribute(metricName.name, metric)
            mBeanName
        } catch (e: JMException) {
            throw KafkaException(
                "Error creating mbean attribute for metricName :${metric.metricName()}",
                e
            )
        }
    }

    override fun close() {
        lock.withLock {
            for (mbean in mbeans.values) unregister(mbean)
        }
    }

    private fun unregister(mbean: KafkaMbean) {
        val server = ManagementFactory.getPlatformMBeanServer()
        try {
            if (server.isRegistered(mbean.name)) server.unregisterMBean(mbean.name)
        } catch (e: JMException) {
            throw KafkaException("Error unregistering mbean", e)
        }
    }

    private fun reregister(mbean: KafkaMbean) {
        unregister(mbean)
        try {
            ManagementFactory.getPlatformMBeanServer().registerMBean(mbean, mbean.name)
        } catch (e: JMException) {
            throw KafkaException("Error registering mbean " + mbean.name, e)
        }
    }

    private class KafkaMbean(mbeanName: String?) : DynamicMBean {

        val name: ObjectName = ObjectName(mbeanName)

        val metrics = mutableMapOf<String, KafkaMetric>()

        @Deprecated(
            message = "Use property instead",
            replaceWith = ReplaceWith("objectName")
        )
        fun name(): ObjectName = name

        fun setAttribute(name: String, metric: KafkaMetric) {
            metrics[name] = metric
        }

        @Throws(AttributeNotFoundException::class)
        override fun getAttribute(name: String): Any {
            return metrics[name]?.metricValue()
                ?: throw AttributeNotFoundException("Could not find attribute $name")
        }

        override fun getAttributes(names: Array<String>): AttributeList {
            return AttributeList(
                names.mapNotNull { name ->
                    try {
                        Attribute(name, getAttribute(name))
                    } catch (e: Exception) {
                        log.warn("Error getting JMX attribute '{}'", name, e)
                        null
                    }
                }
            )
        }

        fun removeAttribute(name: String): KafkaMetric? = metrics.remove(name)

        override fun getMBeanInfo(): MBeanInfo {
            val attrs = arrayOfNulls<MBeanAttributeInfo>(metrics.size)
            var i = 0

            metrics.forEach { (attribute, metric) ->
                attrs[i] = MBeanAttributeInfo(
                    attribute,
                    Double::class.javaPrimitiveType!!.name,
                    metric.metricName().description,
                    true,
                    false,
                    false
                )
                i += 1
            }

            return MBeanInfo(
                this.javaClass.name,
                "",
                attrs,
                null,
                null,
                null
            )
        }

        override fun invoke(name: String, params: Array<Any>?, sig: Array<String>?): Any =
            throw UnsupportedOperationException("Set not allowed.")

        override fun setAttribute(attribute: Attribute) =
            throw UnsupportedOperationException("Set not allowed.")

        override fun setAttributes(list: AttributeList): AttributeList =
            throw UnsupportedOperationException("Set not allowed.")
    }

    override fun contextChange(metricsContext: MetricsContext) {
        val namespace = metricsContext.contextLabels()[MetricsContext.NAMESPACE]!!

        lock.withLock {
            check(mbeans.isEmpty()) {
                "JMX MetricsContext can only be updated before JMX metrics are created"
            }

            // prevent prefix from getting reset back to empty for backwards compatibility with the
            // deprecated JmxReporter(String prefix) constructor, in case contextChange gets called
            // via one of the Metrics() constructor with a default empty MetricsContext()
            if (namespace.isEmpty()) return

            prefix = namespace
        }
    }

    companion object {

        const val METRICS_CONFIG_PREFIX = "metrics.jmx."

        const val EXCLUDE_CONFIG = METRICS_CONFIG_PREFIX + "exclude"

        const val EXCLUDE_CONFIG_ALIAS = METRICS_CONFIG_PREFIX + "blacklist"

        const val INCLUDE_CONFIG = METRICS_CONFIG_PREFIX + "include"

        const val INCLUDE_CONFIG_ALIAS = METRICS_CONFIG_PREFIX + "whitelist"

        val RECONFIGURABLE_CONFIGS = mkSet(
            INCLUDE_CONFIG,
            INCLUDE_CONFIG_ALIAS,
            EXCLUDE_CONFIG,
            EXCLUDE_CONFIG_ALIAS
        )

        const val DEFAULT_INCLUDE = ".*"

        const val DEFAULT_EXCLUDE = ""

        private val log = LoggerFactory.getLogger(JmxReporter::class.java)

        private val lock = ReentrantLock()

        private val lockCondition = lock.newCondition()

        /**
         * @param metricName
         * @return standard JMX MBean name in the following format domainName:type=metricType,key1=val1,key2=val2
         */
        fun getMBeanName(prefix: String, metricName: MetricName): String {
            val mBeanName = StringBuilder()
            mBeanName.append(prefix)
            mBeanName.append(":type=")
            mBeanName.append(metricName.group)
            for ((key, value) in metricName.tags) {
                if (key.isEmpty() || value.isNullOrEmpty()) continue
                mBeanName.append(",")
                mBeanName.append(key)
                mBeanName.append("=")
                mBeanName.append(jmxSanitize(value))
            }
            return mBeanName.toString()
        }

        fun compilePredicate(originalConfig: Map<String, *>): Predicate<String> {
            val configs: Map<String, *> = translateDeprecatedConfigs(
                originalConfig,
                arrayOf(
                    arrayOf(INCLUDE_CONFIG, INCLUDE_CONFIG_ALIAS),
                    arrayOf(EXCLUDE_CONFIG, EXCLUDE_CONFIG_ALIAS),
                )
            )

            val include = configs[INCLUDE_CONFIG] as String? ?: DEFAULT_INCLUDE
            val exclude = configs[EXCLUDE_CONFIG] as String? ?: DEFAULT_EXCLUDE

            return try {
                val includePattern = Pattern.compile(include)
                val excludePattern = Pattern.compile(exclude)
                Predicate { s ->
                    (includePattern.matcher(s).matches()
                            && !excludePattern.matcher(s).matches())
                }
            } catch (e: PatternSyntaxException) {
                throw ConfigException(
                    "JMX filter for configuration$METRICS_CONFIG_PREFIX" +
                            ".(include/exclude) is not a valid regular expression"
                )
            }
        }
    }
}
