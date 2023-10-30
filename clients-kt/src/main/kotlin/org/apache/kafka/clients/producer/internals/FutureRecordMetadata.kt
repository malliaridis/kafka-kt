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

import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.utils.Time
import java.util.concurrent.ExecutionException
import java.util.concurrent.Future
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException

/**
 * The future result of a record send
 */
class FutureRecordMetadata(
    private val result: ProduceRequestResult,
    private val batchIndex: Int,
    private val createTimestamp: Long,
    private val serializedKeySize: Int,
    private val serializedValueSize: Int,
    private val time: Time,
) : Future<RecordMetadata> {

    @Volatile
    private var nextRecordMetadata: FutureRecordMetadata? = null

    override fun cancel(interrupt: Boolean): Boolean = false

    override fun isCancelled(): Boolean = false

    @Throws(InterruptedException::class, ExecutionException::class)
    override fun get(): RecordMetadata {
        result.await()
        return nextRecordMetadata?.get() ?: valueOrError()
    }

    @Throws(
        InterruptedException::class,
        ExecutionException::class,
        TimeoutException::class
    )
    override fun get(timeout: Long, unit: TimeUnit): RecordMetadata {
        // Handle overflow.
        val now = time.milliseconds()
        val timeoutMillis = unit.toMillis(timeout)
        val deadline =
            if (Long.MAX_VALUE - timeoutMillis < now) Long.MAX_VALUE
            else now + timeoutMillis

        val occurred = result.await(timeout, unit)
        if (!occurred) throw TimeoutException("Timeout after waiting for $timeoutMillis ms.")
        return nextRecordMetadata?.get(deadline - time.milliseconds(), TimeUnit.MILLISECONDS)
            ?: valueOrError()
    }

    /**
     * This method is used when we have to split a large batch in smaller ones. A chained metadata
     * will allow the future that has already returned to the users to wait on the newly created
     * split batches even after the old big batch has been deemed as done.
     */
    fun chain(futureRecordMetadata: FutureRecordMetadata?) {
        nextRecordMetadata?.chain(futureRecordMetadata) ?: run {
            nextRecordMetadata = futureRecordMetadata
        }
    }

    @Throws(ExecutionException::class)
    fun valueOrError(): RecordMetadata {
        val exception = result.error(batchIndex) ?: return value()
        throw ExecutionException(exception)
    }

    fun value(): RecordMetadata = nextRecordMetadata?.value() ?: RecordMetadata(
        topicPartition = result.topicPartition,
        baseOffset = result.baseOffset(),
        batchIndex = batchIndex,
        timestamp = timestamp(),
        serializedKeySize = serializedKeySize,
        serializedValueSize = serializedValueSize,
    )

    private fun timestamp(): Long =
        if (result.hasLogAppendTime()) result.logAppendTime()
        else createTimestamp

    override fun isDone(): Boolean =
        if (nextRecordMetadata != null) nextRecordMetadata!!.isDone
        else result.completed
}
