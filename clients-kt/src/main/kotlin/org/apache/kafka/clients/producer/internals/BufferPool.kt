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

import org.apache.kafka.clients.producer.BufferExhaustedException
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.metrics.Sensor
import org.apache.kafka.common.metrics.stats.Meter
import org.apache.kafka.common.utils.Time
import java.nio.ByteBuffer
import java.util.Deque
import java.util.ArrayDeque
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.Condition
import java.util.concurrent.locks.ReentrantLock
import kotlin.math.min

/**
 * A pool of ByteBuffers kept under a given memory limit. This class is fairly specific to the needs
 * of the producer. In particular, it has the following properties:
 *
 * 1. There is a special "poolable size" and buffers of this size are kept in a free list and
 *    recycled
 * 2. It is fair. That is all memory is given to the longest waiting thread until it has sufficient
 *    memory. This prevents starvation or deadlock when a thread asks for a large chunk of memory
 *    and needs to block until multiple buffers are deallocated.
 *
 * @constructor Create a new buffer pool
 * @property totalMemory The maximum amount of memory that this buffer pool can allocate
 * @property poolableSize The buffer size to cache in the free list rather than deallocating
 * @param metrics instance of Metrics
 * @param time time instance
 * @param metricGrpName logical group name for metrics
 */
open class BufferPool(
    val totalMemory: Long,
    val poolableSize: Int,
    private val metrics: Metrics,
    private val time: Time,
    metricGrpName: String,
) {

    private val lock: ReentrantLock = ReentrantLock()

    private val free: Deque<ByteBuffer> = ArrayDeque()

    val waiters: Deque<Condition> = ArrayDeque()

    /**
     * Total available memory is the sum of nonPooledAvailableMemory and the number of byte buffers
     * in free times poolableSize.
     */
    private var nonPooledAvailableMemory: Long = totalMemory

    private val waitTime: Sensor = metrics.sensor(WAIT_TIME_SENSOR_NAME)

    private var closed: Boolean

    init {
        val rateMetricName = metrics.metricName(
            name = "bufferpool-wait-ratio",
            group = metricGrpName,
            description = "The fraction of time an appender waits for space allocation.",
        )
        val totalMetricName = metrics.metricName(
            name = "bufferpool-wait-time-total",
            group = metricGrpName,
            description = "*Deprecated* The total time an appender waits for space allocation.",
        )
        val totalNsMetricName = metrics.metricName(
            name = "bufferpool-wait-time-ns-total",
            group = metricGrpName,
            description = "The total time in nanoseconds an appender waits for space allocation.",
        )
        val bufferExhaustedRecordSensor = metrics.sensor("buffer-exhausted-records")
        val bufferExhaustedRateMetricName = metrics.metricName(
            name = "buffer-exhausted-rate",
            group = metricGrpName,
            description = "The average per-second number of record sends that are dropped due to " +
                    "buffer exhaustion",
        )
        val bufferExhaustedTotalMetricName = metrics.metricName(
            name = "buffer-exhausted-total",
            group = metricGrpName,
            description = "The total number of record sends that are dropped due to buffer " +
                    "exhaustion",
        )
        bufferExhaustedRecordSensor.add(
            Meter(
                bufferExhaustedRateMetricName,
                bufferExhaustedTotalMetricName,
            )
        )
        waitTime.add(
            Meter(
                unit = TimeUnit.NANOSECONDS,
                rateMetricName = rateMetricName,
                totalMetricName = totalMetricName,
            )
        )
        waitTime.add(
            Meter(
                unit = TimeUnit.NANOSECONDS,
                rateMetricName = rateMetricName,
                totalMetricName = totalNsMetricName,
            )
        )
        closed = false
    }

    /**
     * Allocate a buffer of the given size. This method blocks if there is not enough memory and the
     * buffer pool is configured with blocking mode.
     *
     * @param size The buffer size to allocate in bytes
     * @param maxTimeToBlockMs The maximum time in milliseconds to block for buffer memory to be
     * available
     * @return The buffer
     * @throws InterruptedException If the thread is interrupted while blocked
     * @throws IllegalArgumentException if size is larger than the total memory controlled by the
     * pool (and hence we would block forever)
     */
    @Throws(InterruptedException::class)
    open fun allocate(size: Int, maxTimeToBlockMs: Long): ByteBuffer {
        require(size <= totalMemory) {
            "Attempt to allocate $size bytes, but there is a hard limit of $totalMemory on " +
                    "memory allocations."
        }
        var buffer: ByteBuffer? = null
        lock.lock()
        if (closed) {
            lock.unlock()
            throw KafkaException("Producer closed while allocating memory")
        }

        try {
            // check if we have a free buffer of the right size pooled
            if (size == poolableSize && !free.isEmpty()) return free.pollFirst()

            // now check if the request is immediately satisfiable with the
            // memory on hand or if we need to block
            val freeListSize = freeSize * poolableSize
            if (nonPooledAvailableMemory + freeListSize >= size) {
                // we have enough unallocated or pooled memory to immediately
                // satisfy the request, but need to allocate the buffer
                freeUp(size)
                nonPooledAvailableMemory -= size.toLong()
            } else {
                // we are out of memory and will have to block
                var accumulated = 0
                val moreMemory = lock.newCondition()
                try {
                    var remainingTimeToBlockNs = TimeUnit.MILLISECONDS.toNanos(maxTimeToBlockMs)
                    waiters.addLast(moreMemory)
                    // loop over and over until we have a buffer or have reserved
                    // enough memory to allocate one
                    while (accumulated < size) {
                        val startWaitNs = time.nanoseconds()
                        var timeNs: Long
                        var waitingTimeElapsed: Boolean
                        try {
                            waitingTimeElapsed =
                                !moreMemory.await(remainingTimeToBlockNs, TimeUnit.NANOSECONDS)
                        } finally {
                            val endWaitNs = time.nanoseconds()
                            timeNs = (endWaitNs - startWaitNs).coerceAtLeast(0L)
                            recordWaitTime(timeNs)
                        }

                        if (closed) throw KafkaException("Producer closed while allocating memory")

                        if (waitingTimeElapsed) {
                            metrics.sensor("buffer-exhausted-records").record()
                            throw BufferExhaustedException(
                                "Failed to allocate $size bytes within the configured max " +
                                        "blocking time $maxTimeToBlockMs ms. " +
                                        "Total memory: $totalMemory bytes. " +
                                        "Available memory: ${availableMemory()} bytes. " +
                                        "Poolable size: $poolableSize bytes"
                            )
                        }
                        remainingTimeToBlockNs -= timeNs

                        // check if we can satisfy this request from the free list,
                        // otherwise allocate memory
                        if ((accumulated == 0) && (size == poolableSize) && !free.isEmpty()) {
                            // just grab a buffer from the free list
                            buffer = free.pollFirst()
                            accumulated = size
                        } else {
                            // we'll need to allocate memory, but we may only get
                            // part of what we need on this iteration
                            freeUp(size - accumulated)
                            val got = min(
                                (size - accumulated).toLong(),
                                nonPooledAvailableMemory,
                            ).toInt()
                            nonPooledAvailableMemory -= got
                            accumulated += got
                        }
                    }
                    // Don't reclaim memory on throwable since nothing was thrown
                    accumulated = 0
                } finally {
                    // When this loop was not able to successfully terminate don't loose available
                    // memory
                    nonPooledAvailableMemory += accumulated
                    waiters.remove(moreMemory)
                }
            }
        } finally {
            // signal any additional waiters if there is more memory left over for them
            try {
                if (!(nonPooledAvailableMemory == 0L && free.isEmpty()) && !waiters.isEmpty())
                    waiters.peekFirst().signal()
            } finally {
                // Another finally... otherwise find bugs complains
                lock.unlock()
            }
        }
        return buffer ?: safeAllocateByteBuffer(size)
    }

    // Protected for testing
    internal fun recordWaitTime(timeNs: Long) {
        waitTime.record(value = timeNs.toDouble(), timeMs = time.milliseconds())
    }

    /**
     * Allocate a buffer. If buffer allocation fails (e.g. because of OOM) then return the size
     * count back to available memory and signal the next waiter if it exists.
     */
    private fun safeAllocateByteBuffer(size: Int): ByteBuffer {
        var error = true
        try {
            val buffer = allocateByteBuffer(size)
            error = false
            return buffer
        } finally {
            if (error) {
                lock.lock()
                try {
                    nonPooledAvailableMemory += size
                    if (!waiters.isEmpty()) waiters.peekFirst().signal()
                } finally {
                    lock.unlock()
                }
            }
        }
    }

    // Protected for testing.
    internal open fun allocateByteBuffer(size: Int): ByteBuffer {
        return ByteBuffer.allocate(size)
    }

    /**
     * Attempt to ensure we have at least the requested number of bytes of memory for allocation by
     * deallocating pooled buffers (if needed)
     */
    private fun freeUp(size: Int) {
        while (!free.isEmpty() && nonPooledAvailableMemory < size)
            nonPooledAvailableMemory += free.pollLast().capacity()
    }

    /**
     * Return buffers to the pool. If they are of the poolable size add them to the free list,
     * otherwise just mark the memory as free.
     *
     * @param buffer The buffer to return
     * @param size The size of the buffer to mark as deallocated, note that this may be smaller than
     * `buffer.capacity` since the buffer may re-allocate itself during in-place compression
     */
    open fun deallocate(buffer: ByteBuffer, size: Int) {
        lock.lock()
        try {
            if (size == poolableSize && size == buffer.capacity()) {
                buffer.clear()
                free.add(buffer)
            } else nonPooledAvailableMemory += size.toLong()

            val moreMem: Condition? = waiters.peekFirst()
            moreMem?.signal()
        } finally {
            lock.unlock()
        }
    }

    fun deallocate(buffer: ByteBuffer?) {
        buffer?.let { deallocate(it, it.capacity()) }
    }

    /**
     * the total free memory both unallocated and in the free list
     */
    fun availableMemory(): Long {
        lock.lock()
        try {
            return nonPooledAvailableMemory + freeSize * poolableSize.toLong()
        } finally {
            lock.unlock()
        }
    }

    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("freeSize"),
    )
    // Protected for testing.
    protected open fun freeSize(): Int = free.size

    internal open val freeSize: Int
        get() = free.size

    /**
     * Get the unallocated memory (not in the free list or in use)
     */
    fun unallocatedMemory(): Long {
        lock.lock()
        try {
            return nonPooledAvailableMemory
        } finally {
            lock.unlock()
        }
    }

    /**
     * The number of threads blocked waiting on memory
     */
    fun queued(): Int {
        lock.lock()
        try {
            return waiters.size
        } finally {
            lock.unlock()
        }
    }

    /**
     * The buffer size that will be retained in the free list after use
     */
    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("poolableSize"),
    )
    fun poolableSize(): Int = poolableSize

    /**
     * The total memory managed by this pool
     */
    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("totalMemory"),
    )
    fun totalMemory(): Long = totalMemory

    // package-private method used only for testing
    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("waiters"),
    )
    fun waiters(): Deque<Condition> = waiters

    /**
     * Closes the buffer pool. Memory will be prevented from being allocated, but may be
     * deallocated. All allocations awaiting available memory will be notified to abort.
     */
    fun close() {
        lock.lock()
        closed = true
        try {
            for (waiter in waiters) waiter.signal()
        } finally {
            lock.unlock()
        }
    }

    companion object {
        const val WAIT_TIME_SENSOR_NAME = "bufferpool-wait-time"
    }
}
