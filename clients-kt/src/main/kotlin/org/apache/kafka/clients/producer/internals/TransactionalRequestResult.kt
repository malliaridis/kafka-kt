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

package org.apache.kafka.clients.producer.internals

import org.apache.kafka.common.errors.InterruptException
import org.apache.kafka.common.errors.TimeoutException
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

class TransactionalRequestResult private constructor(
    private val latch: CountDownLatch,
    private val operation: String,
) {

    @Volatile
    private var error: RuntimeException? = null

    @Volatile
    var isAcked = false
        private set

    constructor(operation: String) : this(CountDownLatch(1), operation)

    fun fail(error: RuntimeException?) {
        this.error = error
        latch.countDown()
    }

    fun done() = latch.countDown()

    @JvmOverloads
    fun await(timeout: Long = Long.MAX_VALUE, unit: TimeUnit = TimeUnit.MILLISECONDS) {
        try {
            val success = latch.await(timeout, unit)
            if (!success) throw TimeoutException(
                "Timeout expired after ${unit.toMillis(timeout)} ms while awaiting $operation"
            )

            isAcked = true
            error?.let { throw it }
        } catch (e: InterruptedException) {
            throw InterruptException("Received interrupt while awaiting $operation", e)
        }
    }

    fun error(): RuntimeException? = error

    val isSuccessful: Boolean
        get() = isCompleted && error == null

    val isCompleted: Boolean
        get() = latch.count == 0L
}
