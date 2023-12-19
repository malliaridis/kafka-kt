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

import java.util.Objects
import java.util.concurrent.atomic.AtomicLong

/**
 * Holds a range of Producer IDs used for Transactional and EOS producers.
 *
 * The start and end of the ID block are inclusive.
 *
 * @property assignedBrokerId The ID of the broker that this block was assigned to.
 * @property firstProducerId The first ID (inclusive) to be assigned from this block.
 * @property blockSize The number of IDs contained in this block.
 */
data class ProducerIdsBlock(
    val assignedBrokerId: Int,
    val firstProducerId: Long,
    private val blockSize: Int,
) {

    private val producerIdCounter: AtomicLong = AtomicLong(firstProducerId)

    /**
     * Claim the next available producer id from the block.
     * Returns an `null` if there are no more available producer ids in the block.
     */
    fun claimNextId(): Long? {
        val nextId = producerIdCounter.getAndIncrement()
        return if (nextId > lastProducerId) null else nextId
    }

    /**
     * Get the ID of the broker that this block was assigned to.
     */
    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("assignedBrokerId"),
    )
    fun assignedBrokerId(): Int = assignedBrokerId

    /**
     * Get the first ID (inclusive) to be assigned from this block.
     */
    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("firstProducerId"),
    )
    fun firstProducerId(): Long = firstProducerId

    /**
     * Get the number of IDs contained in this block.
     */
    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("size"),
    )
    fun size(): Int = blockSize

    /**
     * The number of IDs contained in this block.
     */
    val size: Int
        get() = blockSize

    /**
     * Get the last ID (inclusive) to be assigned from this block.
     */
    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("lastProducerId"),
    )
    fun lastProducerId(): Long = lastProducerId

    /**
     * The last ID (inclusive) to be assigned from this block.
     */
    val lastProducerId: Long
        get() = firstProducerId + blockSize - 1

    /**
     * Get the first ID of the next block following this one.
     */
    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("nextBlockFirstId"),
    )
    fun nextBlockFirstId(): Long = nextBlockFirstId

    /**
     * The first ID of the next block following this one.
     */
    val nextBlockFirstId: Long
        get() = firstProducerId + blockSize

    override fun toString(): String = "ProducerIdsBlock(" +
            "assignedBrokerId=$assignedBrokerId" +
            ", firstProducerId=$firstProducerId" +
            ", size=$blockSize" +
            ')'

    companion object {

        const val PRODUCER_ID_BLOCK_SIZE = 1000

        val EMPTY = ProducerIdsBlock(assignedBrokerId = -1, firstProducerId = 0, blockSize = 0)
    }
}
