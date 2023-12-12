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

package org.apache.kafka.clients.admin

import org.apache.kafka.common.annotation.InterfaceStability.Evolving

/**
 * Options for [Admin.describeProducers].
 *
 * The API of this class is evolving, see [Admin] for details.
 */
@Evolving
class DescribeProducersOptions : AbstractOptions<DescribeProducersOptions>() {

    var brokerId: Int? = null

    @Deprecated("Use property instead.")
    fun brokerId(brokerId: Int): DescribeProducersOptions {
        this.brokerId = brokerId
        return this
    }

    @Deprecated(
        message = "Use property instead.",
        replaceWith = ReplaceWith("brokerId")
    )
    fun brokerId(): Int? = brokerId

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as DescribeProducersOptions

        if (brokerId != other.brokerId) return false
        if (timeoutMs != other.timeoutMs) return false

        return true
    }

    override fun hashCode(): Int {
        var result = brokerId ?: 0
        result = 31 * result + (timeoutMs ?: 0)
        return result
    }

    override fun toString(): String = "DescribeProducersOptions(brokerId=$brokerId, timeoutMs=$timeoutMs)"
}
