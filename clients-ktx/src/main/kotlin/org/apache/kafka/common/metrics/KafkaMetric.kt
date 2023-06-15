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

import java.util.concurrent.locks.Lock
import java.util.concurrent.locks.ReentrantLock
import org.apache.kafka.common.Metric
import org.apache.kafka.common.MetricName
import org.apache.kafka.common.utils.Time
import kotlin.concurrent.withLock

// public for testing
class KafkaMetric(
    private val lock: Lock,
    private val metricName: MetricName,
    private val metricValueProvider: MetricValueProvider<*>,
    config: MetricConfig,
    private val time: Time
) : Metric {

    var config: MetricConfig = config
        private set

    init {
        require(metricValueProvider is Measurable || metricValueProvider is Gauge<*>) {
            "Unsupported metric value provider of class ${metricValueProvider.javaClass}"
        }
    }

    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("config")
    )
    fun config(): MetricConfig = config

    override fun metricName(): MetricName = metricName

    override fun metricValue(): Any {
        val now = time.milliseconds()
        lock.withLock {
            return when (metricValueProvider) {
                is Measurable -> metricValueProvider.measure(config, now)
                is Gauge<*> -> metricValueProvider.value(config, now)!!
                else -> error("Not a valid metric: ${metricValueProvider.javaClass}")
            }
        }
    }

    fun measurable(): Measurable = metricValueProvider as? Measurable
        ?: error("Not a measurable: ${metricValueProvider.javaClass}")

    fun measurableValue(timeMs: Long): Double {
        lock.withLock {
            return if (metricValueProvider is Measurable)
                metricValueProvider.measure(config, timeMs)
            else 0.0
        }
    }

    fun config(config: MetricConfig) {
        lock.withLock { this.config = config }
    }
}
