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

package org.apache.kafka.common.memory

import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicLong
import org.apache.kafka.common.metrics.Sensor
import org.apache.kafka.common.utils.Utils.formatBytes
import org.slf4j.LoggerFactory

/**
 * a simple pool implementation. this implementation just provides a limit on the total outstanding
 * memory. Any buffer allocated must be release()ed always otherwise memory is not marked as
 * reclaimed (and "leak"s).
 */
open class SimpleMemoryPool(
    sizeInBytes: Long,
    maxSingleAllocationBytes: Int,
    strict: Boolean,
    oomPeriodSensor: Sensor?
) : MemoryPool {

    protected val log = LoggerFactory.getLogger(javaClass) //subclass-friendly
    protected val sizeBytes: Long
    protected val strict: Boolean
    protected val availableMemory: AtomicLong
    protected val maxSingleAllocationSize: Int
    protected val startOfNoMemPeriod = AtomicLong() //nanoseconds

    @Volatile
    protected var oomTimeSensor: Sensor?

    init {
        require(sizeInBytes > 0 && maxSingleAllocationBytes > 0 && maxSingleAllocationBytes <= sizeInBytes) {
            "must provide a positive size and max single allocation size smaller than size. " +
                    "provided $sizeInBytes and $maxSingleAllocationBytes respectively"
        }
        sizeBytes = sizeInBytes
        this.strict = strict
        availableMemory = AtomicLong(sizeInBytes)
        maxSingleAllocationSize = maxSingleAllocationBytes
        oomTimeSensor = oomPeriodSensor
    }

    override fun tryAllocate(sizeBytes: Int): ByteBuffer? {
        require(sizeBytes >= 1) { "requested size $sizeBytes<=0" }
        require(sizeBytes <= maxSingleAllocationSize) {
            "requested size $sizeBytes is larger than maxSingleAllocationSize $maxSingleAllocationSize"
        }

        var available: Long
        var success = false
        //in strict mode we will only allocate memory if we have at least the size required.
        //in non-strict mode we will allocate memory if we have _any_ memory available (so available
        // memory can dip into the negative and max allocated memory would be
        // sizeBytes + maxSingleAllocationSize)
        val threshold = (if (strict) sizeBytes else 1).toLong()

        while ((availableMemory.get().also { available = it }) >= threshold) {
            success = availableMemory.compareAndSet(available, available - sizeBytes)
            if (success) break
        }

        if (success) maybeRecordEndOfDrySpell()
        else {
            if (oomTimeSensor != null) startOfNoMemPeriod.compareAndSet(0, System.nanoTime())
            log.trace("refused to allocate buffer of size {}", sizeBytes)
            return null
        }
        val allocated = ByteBuffer.allocate(sizeBytes)
        bufferToBeReturned(allocated)
        return allocated
    }

    override fun release(previouslyAllocated: ByteBuffer) {
        bufferToBeReleased(previouslyAllocated)
        availableMemory.addAndGet(previouslyAllocated.capacity().toLong())
        maybeRecordEndOfDrySpell()
    }

    override fun size(): Long = sizeBytes

    override fun availableMemory(): Long = availableMemory.get()

    override val isOutOfMemory: Boolean
        get() = availableMemory.get() <= 0

    //allows subclasses to do their own bookkeeping (and validation) _before_ memory is returned to client code.
    protected open fun bufferToBeReturned(justAllocated: ByteBuffer) {
        log.trace("allocated buffer of size {} ", justAllocated.capacity())
    }

    //allows subclasses to do their own bookkeeping (and validation) _before_ memory is marked as reclaimed.
    protected open fun bufferToBeReleased(justReleased: ByteBuffer) {
        log.trace("released buffer of size {}", justReleased.capacity())
    }

    override fun toString(): String {
        val allocated = sizeBytes - availableMemory.get()
        return "SimpleMemoryPool{${formatBytes(allocated)}/${formatBytes(sizeBytes)} used}"
    }

    protected fun maybeRecordEndOfDrySpell() {
        if (oomTimeSensor != null) {
            val startOfDrySpell = startOfNoMemPeriod.getAndSet(0)
            if (startOfDrySpell != 0L) {
                //how long were we refusing allocation requests for
                oomTimeSensor!!.record((System.nanoTime() - startOfDrySpell) / 1000000.0) //fractional (double) millis
            }
        }
    }
}
