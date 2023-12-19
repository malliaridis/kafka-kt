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

package org.apache.kafka.server.metrics

import com.yammer.metrics.core.MetricName
import com.yammer.metrics.core.MetricsRegistry
import java.util.SortedMap
import java.util.TreeMap
import org.apache.kafka.common.Reconfigurable
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.common.metrics.JmxReporter
import org.apache.kafka.common.utils.Exit.addShutdownHook
import org.apache.kafka.common.utils.Sanitizer.jmxSanitize

/**
 * This class encapsulates the default yammer metrics registry for Kafka server,
 * and configures the set of exported JMX metrics for Yammer metrics.
 *
 * KafkaYammerMetrics.defaultRegistry() should always be used instead of Metrics.defaultRegistry()
 */
class KafkaYammerMetrics private constructor() : Reconfigurable {
    
    private val metricsRegistry = MetricsRegistry()
    
    private val jmxReporter = FilteringJmxReporter(metricsRegistry) { true }

    init {
        jmxReporter.start()
        addShutdownHook("kafka-jmx-shutdown-hook") { jmxReporter.shutdown() }
    }

    override fun configure(configs: Map<String, *>) = reconfigure(configs)

    override fun reconfigurableConfigs(): Set<String> = JmxReporter.RECONFIGURABLE_CONFIGS

    @Throws(ConfigException::class)
    override fun validateReconfiguration(configs: Map<String, *>) {
        JmxReporter.compilePredicate(configs)
    }

    override fun reconfigure(configs: Map<String, *>) {
        val mBeanPredicate = JmxReporter.compilePredicate(configs)
        jmxReporter.updatePredicate { metricName -> mBeanPredicate.test(metricName.mBeanName) }
    }

    companion object {
        
        val INSTANCE = KafkaYammerMetrics()

        /**
         * convenience method to replace [com.yammer.metrics.Metrics.defaultRegistry]
         */
        fun defaultRegistry(): MetricsRegistry = INSTANCE.metricsRegistry

        fun getMetricName(
            group: String?,
            typeName: String?,
            name: String,
        ): MetricName = getMetricName(
            group = group,
            typeName = typeName,
            name = name,
            tags = null,
        )

        fun getMetricName(
            group: String?,
            typeName: String?,
            name: String,
            tags: LinkedHashMap<String, String>?,
        ): MetricName {
            val nameBuilder = StringBuilder()
            nameBuilder.append(group)
            nameBuilder.append(":type=")
            nameBuilder.append(typeName)
            if (name.length > 0) {
                nameBuilder.append(",name=")
                nameBuilder.append(name)
            }
            val scope = toScope(tags)
            val tagsName = toMBeanName(tags)
            tagsName?.let { nameBuilder.append(it) }

            return MetricName(group, typeName, name, scope, nameBuilder.toString())
        }

        private fun toMBeanName(tags: LinkedHashMap<String, String>?): String? {
            if (tags == null) return null
            val nonEmptyTags = collectNonEmptyTags(tags) { LinkedHashMap() }
            return if (nonEmptyTags.isEmpty()) null
            else {
                val tagsString = StringBuilder()
                for ((key, value) in nonEmptyTags)  {
                    val sanitizedValue = jmxSanitize(value)
                    tagsString.append(",")
                    tagsString.append(key)
                    tagsString.append("=")
                    tagsString.append(sanitizedValue)
                }
                tagsString.toString()
            }
        }

        private fun <T : MutableMap<String, String>> collectNonEmptyTags(
            tags: Map<String, String>,
            mapSupplier: () -> T,
        ): T {
            val result = mapSupplier()
            for ((key, tagValue) in tags) {
                if ("" != tagValue) result.put(key, tagValue)
            }
            return result
        }

        private fun toScope(tags: Map<String, String>?): String? {
            if (tags == null) return null
            val nonEmptyTags: SortedMap<String, String> = collectNonEmptyTags(tags) { TreeMap() }
            return if (nonEmptyTags.isEmpty()) null
            else {
                val tagsString = StringBuilder()
                val iterator = nonEmptyTags.entries.iterator()
                while (iterator.hasNext()) {

                    // convert dot to _ since reporters like Graphite typically use dot to represent hierarchy
                    val (key, value) = iterator.next()
                    val convertedValue = value.replace("\\.".toRegex(), "_")
                    tagsString.append(key)
                    tagsString.append(".")
                    tagsString.append(convertedValue)
                    if (iterator.hasNext()) tagsString.append(".")
                }
                tagsString.toString()
            }
        }
    }
}
