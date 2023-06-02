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

package org.apache.kafka.common

import java.util.*

/**
 * The [MetricName] class encapsulates a metric's name, logical group and its related attributes.
 * It should be constructed using `metrics.MetricName(...)`.
 *
 * This class captures the following parameters:
 *
 * - **name** - The name of the metric
 * - **group** - logical group name of the metrics to which this metric belongs.
 * - **description** - A human-readable description to include in the metric. This is optional.
 * - **tags** - Additional key/value attributes of the metric. This is optional.
 *
 * group, tags parameters can be used to create unique metric names while reporting in JMX or any
 * custom reporting.

 * Ex: standard JMX MBean can be constructed like **domainName:type=group,key1=val1,key2=val2**
 *
 * Usage looks something like this:
 * ```java
 * // set up metrics:
 *
 * Map<String, String> metricTags = new LinkedHashMap<String, String>();
 * metricTags.put("client-id", "producer-1");
 * metricTags.put("topic", "topic");
 *
 * MetricConfig metricConfig = new MetricConfig().tags(metricTags);
 * Metrics metrics = new Metrics(metricConfig); // this is the global repository of metrics and sensors
 *
 * Sensor sensor = metrics.sensor("message-sizes");
 *
 * MetricName metricName = metrics.metricName("message-size-avg", "producer-metrics", "average message size");
 * sensor.add(metricName, new Avg());
 *
 * metricName = metrics.metricName("message-size-max", "producer-metrics");
 * sensor.add(metricName, new Max());
 *
 * metricName = metrics.metricName(
 *   "message-size-min",
 *   "producer-metrics",
 *   "message minimum size",
 *   "client-id",
 *   "my-client",
 *   "topic",
 *   "my-topic"
 * );
 * sensor.add(metricName, new Min());
 *
 * // as messages are sent we record the sizes
 * sensor.record(messageSize);
 * ```
 *
 * Please create MetricName by method [org.apache.kafka.common.metrics.Metrics.metricName]
 *
 * @param name The name of the metric
 * @param group logical group name of the metrics to which this metric belongs
 * @param description A human-readable description to include in the metric
 * @param tags additional key/value attributes of the metric
 */
data class MetricName(
    val name: String,
    val group: String,
    val description: String,
    val tags: Map<String, String>
) {

    @Deprecated(
        message = "Use property instead.",
        replaceWith = ReplaceWith("name")
    )
    fun name(): String {
        return name
    }

    @Deprecated(
        message = "Use property instead.",
        replaceWith = ReplaceWith("group")
    )
    fun group(): String {
        return group
    }

    @Deprecated(
        message = "Use property instead.",
        replaceWith = ReplaceWith("tags")
    )
    fun tags(): Map<String, String> {
        return tags
    }

    @Deprecated(
        message = "Use property instead.",
        replaceWith = ReplaceWith("description")
    )
    fun description(): String {
        return description
    }

    override fun toString(): String {
        return "MetricName [" +
                "name=$name" +
                ", group=$group" +
                ", description=$description" +
                ", tags=$tags" +
                "]"

    }
}

