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

package org.apache.kafka.test

import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.common.metrics.KafkaMetric
import org.apache.kafka.common.metrics.MetricsReporter
import java.util.concurrent.atomic.AtomicInteger

class MockMetricsReporter : MetricsReporter {

    var clientId: String? = null

    override fun init(metrics: List<KafkaMetric>) {
        INIT_COUNT.incrementAndGet()
    }

    override fun metricChange(metric: KafkaMetric) = Unit

    override fun metricRemoval(metric: KafkaMetric) = Unit

    override fun close() {
        CLOSE_COUNT.incrementAndGet()
    }

    override fun configure(configs: Map<String, *>) {
        clientId = configs[CommonClientConfigs.CLIENT_ID_CONFIG] as String?
    }

    companion object {

        val INIT_COUNT = AtomicInteger(0)

        val CLOSE_COUNT = AtomicInteger(0)
    }
}
