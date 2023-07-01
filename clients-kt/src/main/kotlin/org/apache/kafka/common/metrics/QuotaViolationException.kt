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

import org.apache.kafka.common.KafkaException

/**
 * Thrown when a sensor records a value that causes a metric to go outside the bounds configured as
 * its quota.
 */
class QuotaViolationException(
    val metric: KafkaMetric,
    val value: Double,
    val bound: Double,
) : KafkaException() {

    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("metric")
    )
    fun metric(): KafkaMetric = metric

    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("value")
    )
    fun value(): Double = value

    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("bound")
    )
    fun bound(): Double = bound

    override fun toString(): String =
        "${javaClass.name}: '${metric.metricName()}' violated quota. Actual: $value, Threshold: $bound"

    /* avoid the expensive and stack trace for quota violation exceptions */
    override fun fillInStackTrace(): Throwable = this

    companion object {
        private const val serialVersionUID = 1L
    }
}
