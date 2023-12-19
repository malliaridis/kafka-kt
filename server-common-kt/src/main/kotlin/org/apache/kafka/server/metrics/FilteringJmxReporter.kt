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

import com.yammer.metrics.core.Metric
import com.yammer.metrics.core.MetricName
import com.yammer.metrics.core.MetricsRegistry
import com.yammer.metrics.reporting.JmxReporter
import java.util.function.Predicate
import kotlin.concurrent.Volatile

class FilteringJmxReporter(
    registry: MetricsRegistry,
    @field:Volatile private var metricPredicate: Predicate<MetricName>,
) : JmxReporter(registry) {

    override fun onMetricAdded(name: MetricName, metric: Metric) {
        if (metricPredicate.test(name)) super.onMetricAdded(name, metric)
    }

    fun updatePredicate(predicate: Predicate<MetricName>) {
        metricPredicate = predicate
        // re-register metrics on update
        metricsRegistry.allMetrics().forEach { (name, metric) ->
            if (metricPredicate.test(name)) super.onMetricAdded(name, metric)
            else super.onMetricRemoved(name)
        }
    }
}
