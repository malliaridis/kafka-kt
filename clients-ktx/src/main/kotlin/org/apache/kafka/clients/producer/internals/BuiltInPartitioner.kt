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

import org.apache.kafka.common.Cluster
import org.apache.kafka.common.utils.LogContext
import org.apache.kafka.common.utils.Utils.murmur2
import org.apache.kafka.common.utils.Utils.toPositive
import org.slf4j.Logger
import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference
import java.util.function.Supplier

/**
 * Built-in default partitioner. Note, that this is just a utility class that is used directly from
 * RecordAccumulator, it does not implement the Partitioner interface.
 *
 * The class keeps track of various bookkeeping information required for adaptive sticky
 * partitioning (described in detail in KIP-794). There is one partitioner object per topic.
 *
 * @property topic The topic
 * @property stickyBatchSize How much to produce to partition before switch
 */
class BuiltInPartitioner(
    logContext: LogContext,
    private val topic: String,
    private val stickyBatchSize: Int,
) {
    
    private val log: Logger = logContext.logger(BuiltInPartitioner::class.java)

    @Volatile
    private var partitionLoadStats: PartitionLoadStats? = null
    
    private val stickyPartitionInfo = AtomicReference<StickyPartitionInfo?>()

    init {
        require(stickyBatchSize >= 1) {
            "stickyBatchSize must be >= 1 but got $stickyBatchSize"
        }
    }

    /**
     * Calculate the next partition for the topic based on the partition load stats.
     */
    private fun nextPartition(cluster: Cluster): Int {
        val random = if (mockRandom != null) mockRandom!!.get() else toPositive(
            ThreadLocalRandom.current().nextInt()
        )

        // Cache volatile variable in local variable.
        val partitionLoadStats = partitionLoadStats
        val partition: Int = if (partitionLoadStats == null) {
            // We don't have stats to do adaptive partitioning (or it's disabled), just switch to
            // the next partition based on uniform distribution.
            val availablePartitions = cluster.availablePartitionsForTopic(topic)
            if (availablePartitions.isNotEmpty())
                availablePartitions[random % availablePartitions.size].partition
            else {
                // We don't have available partitions, just pick one among all partitions.
                val partitions = cluster.partitionsForTopic(topic)
                random % partitions.size
            }
        } else {
            // Calculate next partition based on load distribution.
            // Note that partitions without leader are excluded from the partitionLoadStats.
            assert(partitionLoadStats.length > 0)
            val cumulativeFrequencyTable = partitionLoadStats.cumulativeFrequencyTable
            val weightedRandom = random % cumulativeFrequencyTable[partitionLoadStats.length - 1]

            // By construction, the cumulative frequency table is sorted, so we can use binary
            // search to find the desired index.
            val searchResult = cumulativeFrequencyTable.binarySearch(
                element = weightedRandom,
                fromIndex = 0,
                toIndex = partitionLoadStats.length,
            )

            // binarySearch results the index of the found element, or -(insertion_point) - 1
            // (where insertion_point is the index of the first element greater than the key).
            // We need to get the index of the first value that is strictly greater, which
            // would be the insertion point, except if we found the element that's equal to
            // the searched value (in this case we need to get next). For example, if we have
            //  4 5 8
            // and we're looking for 3, then we'd get the insertion_point = 0, and the function
            // would return -0 - 1 = -1, by adding 1 we'd get 0. If we're looking for 4, we'd
            // get 0, and we need the next one, so adding 1 works here as well.
            val partitionIndex = Math.abs(searchResult + 1)
            assert(partitionIndex < partitionLoadStats.length)
            partitionLoadStats.partitionIds[partitionIndex]
        }
        log.trace("Switching to partition {} in topic {}", partition, topic)
        return partition
    }

    /**
     * Test-only function. When partition load stats are defined, return the end of range for the
     * random number.
     */
    fun loadStatsRangeEnd(): Int {
        assert(partitionLoadStats != null)
        assert(partitionLoadStats!!.length > 0)
        return partitionLoadStats!!.cumulativeFrequencyTable[partitionLoadStats!!.length - 1]
    }

    /**
     * Peek currently chosen sticky partition. This method works in conjunction with
     * [isPartitionChanged] and [updatePartitionInfo]. The workflow is the following:
     *
     * 1. peekCurrentPartitionInfo is called to know which partition to lock.
     * 2. Lock partition's batch queue.
     * 3. isPartitionChanged under lock to make sure that nobody raced us.
     * 4. Append data to buffer.
     * 5. updatePartitionInfo to update produced bytes and maybe switch partition.
     *
     * It's important that steps 3-5 are under partition's batch queue lock.
     *
     * @param cluster The cluster information (needed if there is no current partition)
     * @return sticky partition info object
     */
    fun peekCurrentPartitionInfo(cluster: Cluster): StickyPartitionInfo? {
        var partitionInfo = stickyPartitionInfo.get()
        if (partitionInfo != null) return partitionInfo

        // We're the first to create it.
        partitionInfo = StickyPartitionInfo(nextPartition(cluster))
        return if (stickyPartitionInfo.compareAndSet(null, partitionInfo)) partitionInfo
        else stickyPartitionInfo.get()

        // Someone has raced us.
    }

    /**
     * Check if partition is changed by a concurrent thread. NOTE this function needs to be called
     * under the partition's batch queue lock.
     *
     * @param partitionInfo The sticky partition info object returned by peekCurrentPartitionInfo
     * @return true if sticky partition object is changed (race condition)
     */
    fun isPartitionChanged(partitionInfo: StickyPartitionInfo?): Boolean {
        // partitionInfo may be null if the caller didn't use built-in partitioner.
        return partitionInfo != null && stickyPartitionInfo.get() !== partitionInfo
    }
    /**
     * Update partition info with the number of bytes appended and maybe switch partition.
     *
     * NOTE: this function needs to be called under the partition's batch queue lock.
     *
     * @param partitionInfo The sticky partition info object returned by peekCurrentPartitionInfo
     * @param appendedBytes The number of bytes appended to this partition
     * @param cluster The cluster information
     * @param enableSwitch If true, switch partition once produced enough bytes
     *
     * Update partition info with the number of bytes appended and maybe switch partition.
     *
     * NOTE: this function needs to be called under the partition's batch queue lock.
     */
    fun updatePartitionInfo(
        partitionInfo: StickyPartitionInfo?,
        appendedBytes: Int,
        cluster: Cluster,
        enableSwitch: Boolean = true,
    ) {
        // partitionInfo may be null if the caller didn't use built-in partitioner.
        if (partitionInfo == null) return
        assert(partitionInfo === stickyPartitionInfo.get())
        val producedBytes = partitionInfo.producedBytes.addAndGet(appendedBytes)

        // We're trying to switch partition once we produce stickyBatchSize bytes to a partition
        // but doing so may hinder batching because partition switch may happen while batch isn't
        // ready to send. This situation is especially likely with high linger.ms setting.
        // Consider the following example:
        //   linger.ms=500, producer produces 12KB in 500ms, batch.size=16KB
        //     - first batch collects 12KB in 500ms, gets sent
        //     - second batch collects 4KB, then we switch partition, so 4KB gets eventually sent
        //     - ... and so on - we'd get 12KB and 4KB batches
        // To get more optimal batching and avoid 4KB fractional batches, the caller may disallow
        // partition switch if batch is not ready to send, so with the example above we'd avoid
        // fractional 4KB batches: in that case the scenario would look like this:
        //     - first batch collects 12KB in 500ms, gets sent
        //     - second batch collects 4KB, but partition switch doesn't happen because batch in not
        //       ready
        //     - second batch collects 12KB in 500ms, gets sent and now we switch partition.
        //     - ... and so on - we'd just send 12KB batches
        // We cap the produced bytes to not exceed 2x of the batch size to avoid pathological cases
        // (e.g. if we have a mix of keyed and unkeyed messages, key messages may create an
        // unready batch after the batch that disabled partition switch becomes ready).
        // As a result, with high latency.ms setting we end up switching partitions after producing
        // between stickyBatchSize and stickyBatchSize * 2 bytes, to better align with batch
        // boundary.
        if (producedBytes >= stickyBatchSize * 2) log.trace(
            "Produced {} bytes, exceeding twice the batch size of {} bytes, with switching " +
                    "set to {}",
            producedBytes,
            stickyBatchSize,
            enableSwitch,
        )
        if (
            producedBytes >= stickyBatchSize
            && enableSwitch
            || producedBytes >= stickyBatchSize * 2
        ) {
            // We've produced enough to this partition, switch to next.
            val newPartitionInfo = StickyPartitionInfo(nextPartition(cluster))
            stickyPartitionInfo.set(newPartitionInfo)
        }
    }

    /**
     * Update partition load stats from the queue sizes of each partition
     *
     * NOTE: queueSizes are modified in place to avoid allocations
     *
     * @param queueSizes The queue sizes, partitions without leaders are excluded
     * @param partitionIds The partition ids for the queues, partitions without leaders are excluded
     * @param length The logical length of the arrays (could be less): we may eliminate some
     * partitions based on latency, but to avoid reallocation of the arrays, we just decrement
     * logical length
     *
     * Visible for testing
     */
    fun updatePartitionLoadStats(queueSizes: IntArray?, partitionIds: IntArray, length: Int) {
        if (queueSizes == null) {
            log.trace("No load stats for topic {}, not using adaptive", topic)
            partitionLoadStats = null
            return
        }
        assert(queueSizes.size == partitionIds.size)
        assert(length <= queueSizes.size)

        // The queueSizes.length represents the number of all partitions in the topic and if we have
        // less than 2 partitions, there is no need to do adaptive logic.
        // If partitioner.availability.timeout.ms != 0, then partitions that experience high
        // latencies (greater than partitioner.availability.timeout.ms) may be excluded, the length
        // represents partitions that are not excluded. If some partitions were excluded, we'd still
        // want to go through adaptive logic, even if we have one partition.
        // See also RecordAccumulator#partitionReady where the queueSizes are built.
        if (length < 1 || queueSizes.size < 2) {
            log.trace(
                "The number of partitions is too small: available={}, all={}, not using adaptive " +
                        "for topic {}",
                length,
                queueSizes.size,
                topic,
            )
            partitionLoadStats = null
            return
        }

        // We build cumulative frequency table from the queue sizes in place. At the beginning each
        // entry contains queue size, then we invert it (so it represents the frequency) and convert
        // to a running sum. Then a uniformly distributed random variable in the range [0..last)
        // would map to a partition with weighted probability.
        // Example: suppose we have 3 partitions with the corresponding queue sizes:
        //  0 3 1
        // Then we can invert them by subtracting the queue size from the max queue size + 1 = 4:
        //  4 1 3
        // Then we can convert it into a running sum (next value adds previous value):
        //  4 5 8
        // Now if we get a random number in the range [0..8) and find the first value that is
        // strictly greater than the number (e.g. for 4 it would be 5), then the index of the value
        // is the index of the partition we're looking for. In this example random numbers
        // 0, 1, 2, 3 would map to partition[0], 4 would map to partition[1] and 5, 6, 7 would map
        // to partition[2].

        // Calculate max queue size + 1 and check if all sizes are the same.
        var maxSizePlus1 = queueSizes[0]
        var allEqual = true
        for (i in 1 until length) {
            if (queueSizes[i] != maxSizePlus1) allEqual = false
            if (queueSizes[i] > maxSizePlus1) maxSizePlus1 = queueSizes[i]
        }
        ++maxSizePlus1
        if (allEqual && length == queueSizes.size) {
            // No need to have complex probability logic when all queue sizes are the same, and we
            // didn't exclude partitions that experience high latencies (greater than
            // partitioner.availability.timeout.ms).
            log.trace("All queue lengths are the same, not using adaptive for topic {}", topic)
            partitionLoadStats = null
            return
        }

        // Invert and fold the queue size, so that they become separator values in the CFT.
        queueSizes[0] = maxSizePlus1 - queueSizes[0]
        for (i in 1 until length) {
            queueSizes[i] = maxSizePlus1 - queueSizes[i] + queueSizes[i - 1]
        }
        log.trace(
            "Partition load stats for topic {}: CFT={}, IDs={}, length={}",
            topic, queueSizes, partitionIds, length
        )
        partitionLoadStats = PartitionLoadStats(queueSizes, partitionIds, length)
    }

    /**
     * Info for the current sticky partition.
     */
    class StickyPartitionInfo internal constructor(private val index: Int) {
        internal val producedBytes = AtomicInteger()
        fun partition(): Int {
            return index
        }
    }

    /**
     * The partition load stats for each topic that are used for adaptive partition distribution.
     */
    private class PartitionLoadStats(
        val cumulativeFrequencyTable: IntArray,
        val partitionIds: IntArray,
        val length: Int,
    ) {
        init {
            assert(cumulativeFrequencyTable.size == partitionIds.size)
            assert(length <= cumulativeFrequencyTable.size)
        }
    }

    companion object {

        // Visible and used for testing only.
        @Volatile
        var mockRandom: Supplier<Int>? = null

        /**
         * Default hashing function to choose a partition from the serialized key bytes
         */
        fun partitionForKey(serializedKey: ByteArray, numPartitions: Int): Int =
            toPositive(murmur2(serializedKey)) % numPartitions
    }
}
