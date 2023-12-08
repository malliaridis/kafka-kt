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
import org.apache.kafka.common.utils.MockTime
import org.apache.kafka.common.utils.Time
import org.apache.kafka.test.TestUtils
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import java.nio.ByteBuffer
import java.util.concurrent.Callable
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import org.mockito.kotlin.any
import org.mockito.kotlin.doThrow
import org.mockito.kotlin.spy
import org.mockito.kotlin.verify
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotEquals
import kotlin.test.assertTrue
import kotlin.test.fail

class BufferPoolTest {
    
    private val time = MockTime()
    
    private val metrics = Metrics(time = time)
    
    private val maxBlockTimeMs: Long = 10
    
    private val metricGroup = "TestMetrics"
    
    @AfterEach
    fun teardown() {
        metrics.close()
    }

    /**
     * Test the simple non-blocking allocation paths
     */
    @Test
    @Throws(Exception::class)
    fun testSimple() {
        val totalMemory = (64 * 1024).toLong()
        val size = 1024
        val pool = BufferPool(
            totalMemory = totalMemory,
            poolableSize = size,
            metrics = metrics,
            time = time,
            metricGrpName = metricGroup,
        )
        var buffer = pool.allocate(size, maxBlockTimeMs)

        assertEquals(size, buffer.limit(), "Buffer size should equal requested size.")
        assertEquals(totalMemory - size, pool.unallocatedMemory(), "Unallocated memory should have shrunk")
        assertEquals(totalMemory - size, pool.availableMemory(), "Available memory should have shrunk")

        buffer.putInt(1)
        buffer.flip()
        pool.deallocate(buffer)

        assertEquals(totalMemory, pool.availableMemory(), "All memory should be available")
        assertEquals(totalMemory - size, pool.unallocatedMemory(), "But now some is on the free list")

        buffer = pool.allocate(size, maxBlockTimeMs)

        assertEquals(0, buffer.position(), "Recycled buffer should be cleared.")
        assertEquals(buffer.capacity(), buffer.limit(), "Recycled buffer should be cleared.")

        pool.deallocate(buffer)

        assertEquals(totalMemory, pool.availableMemory(), "All memory should be available")
        assertEquals(totalMemory - size, pool.unallocatedMemory(), "Still a single buffer on the free list")

        buffer = pool.allocate(2 * size, maxBlockTimeMs)
        pool.deallocate(buffer)

        assertEquals(totalMemory, pool.availableMemory(), "All memory should be available")
        assertEquals(
            totalMemory - size,
            pool.unallocatedMemory(),
            "Non-standard size didn't go to the free list."
        )
    }

    /**
     * Test that we cannot try to allocate more memory then we have in the whole pool
     */
    @Test
    @Throws(Exception::class)
    fun testCantAllocateMoreMemoryThanWeHave() {
        val pool = BufferPool(
            totalMemory = 1024,
            poolableSize = 512,
            metrics = metrics,
            time = time,
            metricGrpName = metricGroup,
        )
        val buffer = pool.allocate(1024, maxBlockTimeMs)

        assertEquals(1024, buffer.limit())

        pool.deallocate(buffer)

        assertFailsWith<IllegalArgumentException> { pool.allocate(1025, maxBlockTimeMs) }
    }

    /**
     * Test that delayed allocation blocks
     */
    @Test
    @Throws(Exception::class)
    fun testDelayedAllocation() {
        val pool = BufferPool(
            totalMemory = (5 * 1024).toLong(),
            poolableSize = 1024,
            metrics = metrics,
            time = time,
            metricGrpName = metricGroup,
        )
        val buffer = pool.allocate(1024, maxBlockTimeMs)
        val doDealloc = asyncDeallocate(pool, buffer)
        val allocation = asyncAllocate(pool, 5 * 1024)

        assertEquals(1L, allocation.count, "Allocation shouldn't have happened yet, waiting on memory.")

        doDealloc.countDown() // return the memory

        assertTrue(
            allocation.await(1, TimeUnit.SECONDS),
            "Allocation should succeed soon after de-allocation"
        )
    }

    private fun asyncDeallocate(pool: BufferPool, buffer: ByteBuffer): CountDownLatch {
        val latch = CountDownLatch(1)
        val thread = Thread {
            try {
                latch.await()
            } catch (e: InterruptedException) {
                e.printStackTrace()
            }
            pool.deallocate(buffer)
        }
        thread.start()
        return latch
    }

    private fun delayedDeallocate(pool: BufferPool, buffer: ByteBuffer, delayMs: Long) {
        val thread = Thread {
            Time.SYSTEM.sleep(delayMs)
            pool.deallocate(buffer)
        }
        thread.start()
    }

    private fun asyncAllocate(pool: BufferPool, size: Int): CountDownLatch {
        val completed = CountDownLatch(1)
        val thread = Thread {
            try {
                pool.allocate(size, maxBlockTimeMs)
            } catch (e: InterruptedException) {
                e.printStackTrace()
            } finally {
                completed.countDown()
            }
        }
        thread.start()
        return completed
    }

    /**
     * Test if BufferExhausted exception is thrown when there is not enough memory to allocate and the elapsed
     * time is greater than the max specified block time.
     */
    @Test
    @Throws(Exception::class)
    fun testBufferExhaustedExceptionIsThrown() {
        val pool = BufferPool(
            totalMemory = 2,
            poolableSize = 1,
            metrics = metrics,
            time = time,
            metricGrpName = metricGroup,
        )
        pool.allocate(1, maxBlockTimeMs)

        assertFailsWith<BufferExhaustedException> { pool.allocate(2, maxBlockTimeMs) }
    }

    /**
     * Verify that a failed allocation attempt due to not enough memory finishes soon after the maxBlockTimeMs.
     */
    @Test
    @Throws(Exception::class)
    fun testBlockTimeout() {
        val pool = BufferPool(
            totalMemory = 10,
            poolableSize = 1,
            metrics = metrics,
            time = Time.SYSTEM,
            metricGrpName = metricGroup,
        )
        val buffer1 = pool.allocate(1, maxBlockTimeMs)
        val buffer2 = pool.allocate(1, maxBlockTimeMs)
        val buffer3 = pool.allocate(1, maxBlockTimeMs)
        // The first two buffers will be de-allocated within maxBlockTimeMs since the most recent allocation
        delayedDeallocate(pool, buffer1, maxBlockTimeMs / 2)
        delayedDeallocate(pool, buffer2, maxBlockTimeMs)
        // The third buffer will be de-allocated after maxBlockTimeMs since the most recent allocation
        delayedDeallocate(pool, buffer3, maxBlockTimeMs / 2 * 5)
        val beginTimeMs = Time.SYSTEM.milliseconds()
        assertFailsWith<BufferExhaustedException>(
            message = "The buffer allocated more memory than its maximum value 10",
        ) { pool.allocate(10, maxBlockTimeMs) }

        // Thread scheduling sometimes means that deallocation varies by this point
        assertTrue(pool.availableMemory() in 7..10, "available memory ${pool.availableMemory()}")
        val durationMs = Time.SYSTEM.milliseconds() - beginTimeMs
        assertTrue(
            actual = durationMs >= maxBlockTimeMs,
            message = "BufferExhaustedException should not throw before maxBlockTimeMs"
        )
        assertTrue(
            actual = durationMs < maxBlockTimeMs + 1000,
            message = "BufferExhaustedException should throw soon after maxBlockTimeMs"
        )
    }

    /**
     * Test if the  waiter that is waiting on availability of more memory is cleaned up when a timeout occurs
     */
    @Test
    @Throws(Exception::class)
    fun testCleanupMemoryAvailabilityWaiterOnBlockTimeout() {
        val pool = BufferPool(
            totalMemory = 2,
            poolableSize = 1,
            metrics = metrics,
            time = time,
            metricGrpName = metricGroup,
        )
        pool.allocate(1, maxBlockTimeMs)

        assertFailsWith<BufferExhaustedException>(
            message = "The buffer allocated more memory than its maximum value 2",
        ) { pool.allocate(2, maxBlockTimeMs) }
        assertEquals(0, pool.queued())
        assertEquals(1, pool.availableMemory())
    }

    /**
     * Test if the  waiter that is waiting on availability of more memory is cleaned up when an interruption occurs
     */
    @Test
    @Throws(Exception::class)
    fun testCleanupMemoryAvailabilityWaiterOnInterruption() {
        val pool = BufferPool(
            totalMemory = 2,
            poolableSize = 1,
            metrics = metrics,
            time = time,
            metricGrpName = metricGroup,
        )
        val blockTime: Long = 5000
        pool.allocate(1, maxBlockTimeMs)
        val t1 = Thread(BufferPoolAllocator(pool, blockTime))
        val t2 = Thread(BufferPoolAllocator(pool, blockTime))
        // start thread t1 which will try to allocate more memory on to the Buffer pool
        t1.start()
        // sleep for 500ms. Condition variable c1 associated with pool.allocate() by thread t1 will be inserted in
        // the waiters queue.
        Thread.sleep(500)
        val waiters = pool.waiters
        // get the condition object associated with pool.allocate() by thread t1
        val c1 = waiters.first
        // start thread t2 which will try to allocate more memory on to the Buffer pool
        t2.start()
        // sleep for 500ms. Condition variable c2 associated with pool.allocate() by thread t2 will be inserted in
        // the waiters queue. The waiters queue will have 2 entries c1 and c2.
        Thread.sleep(500)
        t1.interrupt()
        // sleep for 500ms.
        Thread.sleep(500)
        // get the condition object associated with allocate() by thread t2
        val c2 = waiters.last
        t2.interrupt()

        assertNotEquals(c1, c2)

        t1.join()
        t2.join()

        // both the allocate() called by threads t1 and t2 should have been interrupted and the waiters queue
        // should be empty
        assertEquals(pool.queued(), 0)
    }

    @Test
    @Throws(Exception::class)
    fun testCleanupMemoryAvailabilityOnMetricsException() {
        val bufferPool = spy(
            BufferPool(
                totalMemory = 2,
                poolableSize = 1,
                metrics = Metrics(),
                time = time,
                metricGrpName = metricGroup,
            )
        )

        doThrow(OutOfMemoryError())
            .`when`(bufferPool)
            .recordWaitTime(any())

        bufferPool.allocate(1, 0)

        assertFailsWith<OutOfMemoryError>(message = "Expected oom.") {
            bufferPool.allocate(2, 1000)
        }
        assertEquals(1, bufferPool.availableMemory())
        assertEquals(0, bufferPool.queued())
        assertEquals(1, bufferPool.unallocatedMemory())

        //This shouldn't timeout
        bufferPool.allocate(1, 0)

        verify(bufferPool).recordWaitTime(any())
    }

    private class BufferPoolAllocator internal constructor(var pool: BufferPool, var maxBlockTimeMs: Long) :
        Runnable {
        override fun run() {
            try {
                pool.allocate(2, maxBlockTimeMs)
                fail("The buffer allocated more memory than its maximum value 2")
            } catch (_: BufferExhaustedException) {
                // this is good
            } catch (_: InterruptedException) {
                // this can be neglected
            }
        }
    }

    /**
     * This test creates lots of threads that hammer on the pool
     */
    @Test
    @Throws(Exception::class)
    fun testStressfulSituation() {
        val numThreads = 10
        val iterations = 50000
        val poolableSize = 1024
        val totalMemory = (numThreads / 2 * poolableSize).toLong()
        val pool = BufferPool(totalMemory, poolableSize, metrics, time, metricGroup)
        val threads: MutableList<StressTestThread> = ArrayList()
        for (i in 0 until numThreads) threads.add(StressTestThread(pool, iterations))
        for (thread in threads) thread.start()
        for (thread in threads) thread.join()
        for (thread in threads) assertTrue(
            thread.success.get(),
            "Thread should have completed all iterations successfully."
        )
        assertEquals(totalMemory, pool.availableMemory())
    }

    @Test
    @Throws(Exception::class)
    fun testLargeAvailableMemory() {
        val memory = 20000000000L
        val poolableSize = 2000000000
        val freeSize = AtomicInteger(0)
        val pool = object : BufferPool(memory, poolableSize, metrics, time, metricGroup) {

            override fun allocateByteBuffer(size: Int): ByteBuffer {
                // Ignore size to avoid OOM due to large buffers
                return ByteBuffer.allocate(0)
            }

            @Deprecated("User property instead", replaceWith = ReplaceWith("freeSize"))
            override fun freeSize(): Int = freeSize.get()

            override val freeSize: Int
                get() = freeSize.get()
        }
        pool.allocate(poolableSize, 0)

        assertEquals(18000000000L, pool.availableMemory())

        pool.allocate(poolableSize, 0)

        assertEquals(16000000000L, pool.availableMemory())

        // Emulate `deallocate` by increasing `freeSize`
        freeSize.incrementAndGet()

        assertEquals(18000000000L, pool.availableMemory())

        freeSize.incrementAndGet()

        assertEquals(20000000000L, pool.availableMemory())
    }

    @Test
    fun outOfMemoryOnAllocation() {
        val bufferPool: BufferPool = object : BufferPool(
            totalMemory = 1024,
            poolableSize = 1024,
            metrics = metrics,
            time = time,
            metricGrpName = metricGroup,
        ) {
            override fun allocateByteBuffer(size: Int): ByteBuffer = throw OutOfMemoryError()
        }

        assertFailsWith<OutOfMemoryError>(
            message = "Should have thrown OutOfMemoryError",
        ) { bufferPool.allocateByteBuffer(1024) }
        assertEquals(1024, bufferPool.availableMemory())
    }

    class StressTestThread(
        private val pool: BufferPool,
        private val iterations: Int,
    ) : Thread() {

        private val maxBlockTimeMs: Long = 20000

        val success = AtomicBoolean(false)

        override fun run() {
            try {
                repeat(iterations) {
                    val size =
                        if (TestUtils.RANDOM.nextBoolean()) // allocate poolable size
                            pool.poolableSize
                        else // allocate a random size
                            TestUtils.RANDOM.nextInt(pool.totalMemory.toInt())
                    val buffer = pool.allocate(size, maxBlockTimeMs)
                    pool.deallocate(buffer)
                }
                success.set(true)
            } catch (e: Exception) {
                e.printStackTrace()
            }
        }
    }

    @Test
    @Throws(Exception::class)
    fun testCloseAllocations() {
        val pool = BufferPool(
            totalMemory = 10,
            poolableSize = 1,
            metrics = metrics,
            time = Time.SYSTEM,
            metricGrpName = metricGroup,
        )
        val buffer = pool.allocate(1, maxBlockTimeMs)

        // Close the buffer pool. This should prevent any further allocations.
        pool.close()
        assertFailsWith<KafkaException> { pool.allocate(1, maxBlockTimeMs) }

        // Ensure deallocation still works.
        pool.deallocate(buffer)
    }

    @Test
    @Throws(Exception::class)
    fun testCloseNotifyWaiters() {
        val numWorkers = 2
        val pool = BufferPool(1, 1, metrics, Time.SYSTEM, metricGroup)
        val buffer = pool.allocate(1, Long.MAX_VALUE)
        val executor = Executors.newFixedThreadPool(numWorkers)
        val work = Callable {
            assertFailsWith<KafkaException> { pool.allocate(1, Long.MAX_VALUE) }
        }
        for (i in 0 until numWorkers) executor.submit(work)

        TestUtils.waitForCondition(
            testCondition = { pool.queued() == numWorkers },
            conditionDetails = "Awaiting $numWorkers workers to be blocked on allocation",
        )

        // Close the buffer pool. This should notify all waiters.
        pool.close()
        TestUtils.waitForCondition(
            testCondition = { pool.queued() == 0 },
            conditionDetails = "Awaiting $numWorkers workers to be interrupted from allocation",
        )
        pool.deallocate(buffer)
    }
}
