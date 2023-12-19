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

package org.apache.kafka.server.common

import java.util.concurrent.CompletableFuture
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

internal class ProducerIdsBlockTest {

    @Test
    fun testEmptyBlock() {
        assertEquals(-1, ProducerIdsBlock.EMPTY.lastProducerId)
        assertEquals(0, ProducerIdsBlock.EMPTY.nextBlockFirstId)
        assertEquals(0, ProducerIdsBlock.EMPTY.size)
    }

    @Test
    fun testDynamicBlock() {
        val firstId = 1309418324L
        val blockSize = 5391
        val brokerId = 5
        val block = ProducerIdsBlock(brokerId, firstId, blockSize)
        assertEquals(firstId, block.firstProducerId)
        assertEquals(firstId + blockSize - 1, block.lastProducerId)
        assertEquals(firstId + blockSize, block.nextBlockFirstId)
        assertEquals(blockSize, block.size)
        assertEquals(brokerId, block.assignedBrokerId)
    }

    @Test
    @Throws(Exception::class)
    fun testClaimNextId() {
        repeat(50) {
            val block = ProducerIdsBlock(assignedBrokerId = 0, firstProducerId = 1, blockSize = 1)
            val latch = CountDownLatch(1)
            val counter = AtomicLong(0)
            CompletableFuture.runAsync {
                val pid = block.claimNextId()
                counter.addAndGet(pid ?: 0L)
                latch.countDown()
            }
            val pid = block.claimNextId()
            counter.addAndGet(pid ?: 0L)
            assertTrue(latch.await(1, TimeUnit.SECONDS))
            assertEquals(1, counter.get())
        }
    }
}
