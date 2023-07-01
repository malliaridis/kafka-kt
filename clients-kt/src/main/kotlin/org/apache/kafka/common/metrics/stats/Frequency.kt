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

package org.apache.kafka.common.metrics.stats

import org.apache.kafka.common.MetricName

/**
 * Definition of a frequency metric used in a [Frequencies] compound statistic.
 *
 * @constructor Create an instance with the given name and center point value.
 * @property name the name of the frequency metric
 * @property centerValue the value identifying the [Frequencies] bucket to be reported
 */
data class Frequency(
    val name: MetricName,
    val centerValue: Double,
) {

    /**
     * Get the name of this metric.
     *
     * @return the metric name; never null
     */
    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("name")
    )
    fun name(): MetricName = name

    /**
     * Get the value of this metrics center point.
     *
     * @return the center point value
     */
    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("centerValue")
    )
    fun centerValue(): Double = centerValue

    override fun toString(): String {
        return "Frequency(" +
                "name=$name" +
                ", centerValue=$centerValue" +
                ')'
    }
}
