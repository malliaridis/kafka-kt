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

import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.record.RecordBatch
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

/**
 * A class that models the future completion of a produce request for a single partition. There is
 * one of these per partition in a produce request and it is shared by all the [RecordMetadata]
 * instances that are batched together for the same partition in the request.
 *
 * @constructor Create an instance of this class.
 * @property topicPartition The topic and partition to which this record set was sent
 */
class ProduceRequestResult(val topicPartition: TopicPartition) {

    private val latch = CountDownLatch(1)

    @Volatile
    var baseOffset: Long? = null
        private set

    @Volatile
    var logAppendTime = RecordBatch.NO_TIMESTAMP
        private set

    @Volatile
    private var errorsByIndex: ((Int) -> RuntimeException?)? = null

    /**
     * Set the result of the produce request.
     *
     * @param baseOffset The base offset assigned to the record
     * @param logAppendTime The log append time or -1 if CreateTime is being used
     * @param errorsByIndex Function mapping the batch index to the exception, or `null` if the
     * response was successful
     */
    operator fun set(
        baseOffset: Long?,
        logAppendTime: Long,
        errorsByIndex: ((Int) -> RuntimeException?)?,
    ) {
        this.baseOffset = baseOffset
        this.logAppendTime = logAppendTime
        this.errorsByIndex = errorsByIndex
    }

    /**
     * Mark this request as complete and unblock any threads waiting on its completion.
     */
    fun done() {
        checkNotNull(baseOffset) { "The method `set` must be invoked before this method." }
        latch.countDown()
    }

    /**
     * Await the completion of this request
     */
    @Throws(InterruptedException::class)
    fun await() = latch.await()

    /**
     * Await the completion of this request (up to the given time interval)
     * @param timeout The maximum time to wait
     * @param unit The unit for the max time
     * @return `true` if the request completed, `false` if we timed out
     */
    @Throws(InterruptedException::class)
    fun await(timeout: Long, unit: TimeUnit): Boolean = latch.await(timeout, unit)

    /**
     * The base offset for the request (the first offset in the record set)
     */
    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("baseOffset"),
    )
    fun baseOffset(): Long? = baseOffset!!

    /**
     * Return true if log append time is being used for this topic
     */
    fun hasLogAppendTime(): Boolean = logAppendTime != RecordBatch.NO_TIMESTAMP

    /**
     * The log append time or -1 if CreateTime is being used
     */
    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("logAppendTime"),
    )
    fun logAppendTime(): Long = logAppendTime

    /**
     * The error thrown (generally on the server) while processing this request
     */
    fun error(batchIndex: Int): RuntimeException? = errorsByIndex?.invoke(batchIndex)

    /**
     * The topic and partition to which the record was appended
     */
    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("topicPartition"),
    )
    fun topicPartition(): TopicPartition = topicPartition

    /**
     * Has the request completed?
     */
    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("completed"),
    )
    fun completed(): Boolean = latch.count == 0L

    val completed: Boolean
        get() = latch.count == 0L
}
