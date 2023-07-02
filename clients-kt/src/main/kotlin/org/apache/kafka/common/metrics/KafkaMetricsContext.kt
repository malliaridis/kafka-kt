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

/**
 * A implementation of MetricsContext, it encapsulates required metrics context properties for Kafka
 * services and clients.
 *
 * @constructor Create a MetricsContext with namespace, service or client properties.
 * @param namespace value for _namespace key
 * @property contextLabels contextLabels additional entries to add to the context.
 * values will be converted to string using Object.toString()
 */
class KafkaMetricsContext(
    namespace: String?,
    contextLabels: Map<String, Any?> = emptyMap(),
) : MetricsContext {

    /**
     * Client or Service's contextLabels map.
     */
    private val contextLabels: Map<String, String?>

    init {
        this.contextLabels = mapOf(MetricsContext.NAMESPACE to namespace) +
                contextLabels.mapValues { it.value?.toString() }
    }

    override fun contextLabels(): Map<String, String?> = contextLabels
}
