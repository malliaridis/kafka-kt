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

package org.apache.kafka.server.util

import java.math.BigInteger
import java.util.concurrent.TimeUnit
import org.apache.kafka.common.utils.Time

class Deadline private constructor(val nanoseconds: Long) {

    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("nanoseconds"),
    )
    fun nanoseconds(): Long = nanoseconds

    override fun hashCode(): Int = nanoseconds.hashCode()

    override fun equals(other: Any?): Boolean {
        if (other == null || other.javaClass != this.javaClass) return false
        return nanoseconds == (other as Deadline).nanoseconds
    }

    companion object {

        fun fromMonotonicNanoseconds(nanoseconds: Long): Deadline = Deadline(nanoseconds)

        fun fromDelay(
            time: Time,
            delay: Long,
            timeUnit: TimeUnit,
        ): Deadline {
            if (delay < 0) throw RuntimeException("Negative delays are not allowed.")

            val nowNs = time.nanoseconds()
            val deadlineNs = BigInteger.valueOf(nowNs).add(BigInteger.valueOf(timeUnit.toNanos(delay)))

            return if (deadlineNs >= BigInteger.valueOf(Long.MAX_VALUE)) Deadline(Long.MAX_VALUE)
            else Deadline(deadlineNs.toLong())
        }
    }
}
