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
 * An upper or lower bound for metrics.
 */
data class Quota(
    val bound: Double,
    val isUpperBound: Boolean,
) {

    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("bound")
    )
    fun bound(): Double = bound

    fun acceptable(value: Double): Boolean {
        return isUpperBound && value <= bound || !isUpperBound && value >= bound
    }

    override fun toString(): String {
        return (if (isUpperBound) "upper=" else "lower=") + bound
    }

    companion object {

        fun upperBound(upperBound: Double): Quota = Quota(upperBound, true)

        fun lowerBound(lowerBound: Double): Quota = Quota(lowerBound, false)
    }
}
