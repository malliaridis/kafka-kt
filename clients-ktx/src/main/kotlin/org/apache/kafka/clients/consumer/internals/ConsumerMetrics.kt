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

package org.apache.kafka.clients.consumer.internals

import org.apache.kafka.common.MetricNameTemplate
import org.apache.kafka.common.metrics.Metrics

class ConsumerMetrics(metricsTags: Set<String> = emptySet(), metricGrpPrefix: String) {

    var fetcherMetrics: FetcherMetricsRegistry = FetcherMetricsRegistry(metricsTags, metricGrpPrefix)

    private val allTemplates: List<MetricNameTemplate>
        get() = fetcherMetrics.allTemplates.toList()

    companion object {
        @JvmStatic
        fun main(args: Array<String>) {
            val tags = setOf("client-id")
            val metrics = ConsumerMetrics(tags, "consumer")
            println(
                Metrics.toHtmlTable(
                    "kafka.consumer",
                    metrics.allTemplates
                )
            )
        }
    }
}
