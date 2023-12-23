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

package org.apache.kafka.trogdor.workload

import org.apache.kafka.common.utils.Time

open class Throttle internal constructor(
    private val maxPerPeriod: Int,
    private val periodMs: Int,
) {

    private var count: Int = maxPerPeriod

    private var prevPeriod: Long = -1

    private var lastTimeMs: Long = 0

    @Synchronized
    @Throws(InterruptedException::class)
    fun increment(): Boolean {
        var throttled = false
        while (true) {
            if (count < maxPerPeriod) {
                count++
                return throttled
            }
            lastTimeMs = time.milliseconds()
            val curPeriod = lastTimeMs / periodMs
            if (curPeriod <= prevPeriod) {
                val nextPeriodMs = (curPeriod + 1) * periodMs
                delay(nextPeriodMs - lastTimeMs)
                throttled = true
            } else {
                prevPeriod = curPeriod
                count = 0
            }
        }
    }

    @Synchronized
    fun lastTimeMs(): Long = lastTimeMs

    protected val time: Time
        get() = Time.SYSTEM

    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("time"),
    )
    protected fun time(): Time = Time.SYSTEM

    @Synchronized
    @Throws(InterruptedException::class)
    protected open fun delay(amount: Long) {
        if (amount > 0) (this as Object).wait(amount)
    }
}
