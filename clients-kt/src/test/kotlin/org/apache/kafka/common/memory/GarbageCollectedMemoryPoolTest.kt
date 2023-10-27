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

import org.apache.kafka.common.utils.Utils.formatBytes
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Disabled
import java.nio.ByteBuffer
import java.util.concurrent.TimeUnit
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

class GarbageCollectedMemoryPoolTest {

    private var pool: GarbageCollectedMemoryPool? = null

    @AfterEach
    fun releasePool() {
        if (pool != null) pool!!.close()
    }

    @Test
    fun testZeroSize() {
        assertFailsWith<IllegalArgumentException> {
            GarbageCollectedMemoryPool(
                sizeBytes = 0,
                maxSingleAllocationSize = 7,
                strict = true,
                oomPeriodSensor = null,
            )
        }
    }

    @Test
    fun testNegativeSize() {
        assertFailsWith<IllegalArgumentException> {
            GarbageCollectedMemoryPool(
                sizeBytes = -1,
                maxSingleAllocationSize = 7,
                strict = false,
                oomPeriodSensor = null,
            )
        }
    }

    @Test
    fun testZeroMaxAllocation() {
        assertFailsWith<IllegalArgumentException> {
            GarbageCollectedMemoryPool(
                sizeBytes = 100,
                maxSingleAllocationSize = 0,
                strict = true,
                oomPeriodSensor = null,
            )
        }
    }

    @Test
    fun testNegativeMaxAllocation() {
        assertFailsWith<IllegalArgumentException> {
            GarbageCollectedMemoryPool(
                sizeBytes = 100,
                maxSingleAllocationSize = -1,
                strict = false,
                oomPeriodSensor = null,
            )
        }
    }

    @Test
    fun testMaxAllocationLargerThanSize() {
        assertFailsWith<IllegalArgumentException> {
            GarbageCollectedMemoryPool(
                sizeBytes = 100,
                maxSingleAllocationSize = 101,
                strict = true,
                oomPeriodSensor = null,
            )
        }
    }

    @Test
    fun testAllocationOverMaxAllocation() {
        pool = GarbageCollectedMemoryPool(
            sizeBytes = 1000,
            maxSingleAllocationSize = 10,
            strict = false,
            oomPeriodSensor = null,
        )
        assertFailsWith<IllegalArgumentException> { pool!!.tryAllocate(11) }
    }

    @Test
    fun testAllocationZero() {
        pool = GarbageCollectedMemoryPool(
            sizeBytes = 1000,
            maxSingleAllocationSize = 10,
            strict = true,
            oomPeriodSensor = null,
        )
        assertFailsWith<IllegalArgumentException> { pool!!.tryAllocate(0) }
    }

    @Test
    fun testAllocationNegative() {
        pool = GarbageCollectedMemoryPool(
            sizeBytes = 1000,
            maxSingleAllocationSize = 10,
            strict = false,
            oomPeriodSensor = null,
        )
        assertFailsWith<IllegalArgumentException> { pool!!.tryAllocate(-1) }
    }

    @Test
    @Disabled("Kotlin Migration: releasing null is not possible, since parameter is non-nullable")
    fun testReleaseNull() {
//        pool = GarbageCollectedMemoryPool(
//            sizeBytes = 1000,
//            maxSingleAllocationSize = 10,
//            strict = true,
//            oomPeriodSensor = null,
//        )
//        assertFailsWith<IllegalArgumentException> { pool!!.release(null) }
    }

    @Test
    fun testReleaseForeignBuffer() {
        pool = GarbageCollectedMemoryPool(
            sizeBytes = 1000,
            maxSingleAllocationSize = 10,
            strict = true,
            oomPeriodSensor = null,
        )
        val fellOffATruck = ByteBuffer.allocate(1)
        assertFailsWith<IllegalArgumentException> { pool!!.release(fellOffATruck) }
    }

    @Test
    fun testDoubleFree() {
        pool = GarbageCollectedMemoryPool(1000, 10, false, null)
        val buffer = pool!!.tryAllocate(5)
        assertNotNull(buffer)
        pool!!.release(buffer)
        assertFailsWith<IllegalArgumentException> { pool!!.release(buffer) }
    }

    @Test
    fun testAllocationBound() {
        pool = GarbageCollectedMemoryPool(21, 10, false, null)
        val buf1 = pool!!.tryAllocate(10)
        assertNotNull(buf1)
        assertEquals(10, buf1.capacity())
        val buf2 = pool!!.tryAllocate(10)
        assertNotNull(buf2)
        assertEquals(10, buf2.capacity())
        val buf3 = pool!!.tryAllocate(10)
        assertNotNull(buf3)
        assertEquals(10, buf3.capacity())
        //no more allocations
        assertNull(pool!!.tryAllocate(1))
        //release a buffer
        pool!!.release(buf3)
        //now we can have more
        val buf4 = pool!!.tryAllocate(10)
        assertNotNull(buf4)
        assertEquals(10, buf4.capacity())
        //no more allocations
        assertNull(pool!!.tryAllocate(1))
    }

    @Test
    @Throws(Exception::class)
    fun testBuffersGarbageCollected() {
        val runtime = Runtime.getRuntime()
        val maxHeap = runtime.maxMemory() //in bytes
        val maxPool = maxHeap / 2
        val maxSingleAllocation = maxPool / 10

        //test JVM running with too much memory for this test logic (?)
        assertTrue(maxSingleAllocation < Int.MAX_VALUE / 2)
        pool = GarbageCollectedMemoryPool(maxPool, maxSingleAllocation.toInt(), false, null)

        //we will allocate 30 buffers from this pool, which is sized such that at-most
        //11 should coexist and 30 do not fit in the JVM memory, proving that:
        // 1. buffers were reclaimed and
        // 2. the pool registered the reclamation.
        val timeoutSeconds = 30
        val giveUp = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(timeoutSeconds.toLong())
        var success = false
        var buffersAllocated = 0
        while (System.currentTimeMillis() < giveUp) {
            val buffer = pool!!.tryAllocate(maxSingleAllocation.toInt())
            if (buffer == null) {
                System.gc()
                Thread.sleep(10)
                continue
            }
            buffersAllocated++
            if (buffersAllocated >= 30) {
                success = true
                break
            }
        }
        assertTrue(
            success,
            "failed to allocate 30 buffers in $timeoutSeconds seconds. " +
                    "buffers allocated: $buffersAllocated " +
                    "heap ${formatBytes(maxHeap)} " +
                    "pool ${formatBytes(maxPool)} " +
                    "single allocation ${formatBytes(maxSingleAllocation)}"
        )
    }
}
