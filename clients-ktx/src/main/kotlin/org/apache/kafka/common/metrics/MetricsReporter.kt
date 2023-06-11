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

import org.apache.kafka.common.Reconfigurable
import org.apache.kafka.common.annotation.InterfaceStability.Evolving
import org.apache.kafka.common.config.ConfigException

/**
 * A plugin interface to allow things to listen as new metrics are created, so they can be reported.
 *
 * Implement [org.apache.kafka.common.ClusterResourceListener] to receive cluster metadata once it's
 * available. Please see the class documentation for ClusterResourceListener for more information.
 */
interface MetricsReporter : Reconfigurable, AutoCloseable {

    /**
     * This is called when the reporter is first registered to initially register all existing
     * metrics.
     *
     * @param metrics All currently existing metrics
     */
    fun init(metrics: List<KafkaMetric>)

    /**
     * This is called whenever a metric is updated or added.
     *
     * @param metric
     */
    fun metricChange(metric: KafkaMetric)

    /**
     * This is called whenever a metric is removed.
     *
     * @param metric
     */
    fun metricRemoval(metric: KafkaMetric)

    /**
     * Called when the metrics repository is closed.
     */
    override fun close()

    // default methods for backwards compatibility with reporters that only implement Configurable
    override fun reconfigurableConfigs(): Set<String> {
        return emptySet()
    }

    @Throws(ConfigException::class)
    override fun validateReconfiguration(configs: Map<String, *>) {
    }

    override fun reconfigure(configs: Map<String, *>) {}

    /**
     * Sets the context labels for the service or library exposing metrics. This will be called
     * before [init] and may be called anytime after that.
     *
     * @param metricsContext the metric context
     */
    @Evolving
    fun contextChange(metricsContext: MetricsContext) {
    }
}
