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

import org.apache.kafka.common.annotation.InterfaceStability.Evolving

/**
 * MetricsContext encapsulates additional contextLabels about metrics exposed via a
 * [org.apache.kafka.common.metrics.MetricsReporter]
 *
 * The [contextLabels] map provides following information:
 *
 * in all components
 * - a `_namespace` field indicating the component exposing metrics e.g. kafka.server,
 *   kafka.consumer. The [JmxReporter] uses this as prefix for MBean names
 *
 * for clients and streams libraries
 * - any freeform fields passed in via client properties in the form of
 *   `metrics.context.<key>=<value>`
 *
 * for kafka brokers
 * - kafka.broker.id, kafka.cluster.id
 *
 * for connect workers
 * - connect.kafka.cluster.id, connect.group.id
 *
 */
@Evolving
interface MetricsContext {

    /**
     * Returns the labels for this metrics context.
     *
     * @return the map of label keys and values; never `null` but possibly empty
     */
    fun contextLabels(): Map<String, String?>

    companion object {

        /* predefined fields */
        const val NAMESPACE = "_namespace" // metrics namespace, formerly jmx prefix
    }
}
