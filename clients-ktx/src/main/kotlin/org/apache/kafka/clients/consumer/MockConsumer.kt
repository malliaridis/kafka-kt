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

package org.apache.kafka.clients.consumer

import org.apache.kafka.clients.Metadata.LeaderAndEpoch
import org.apache.kafka.clients.consumer.internals.NoOpConsumerRebalanceListener
import org.apache.kafka.clients.consumer.internals.SubscriptionState
import org.apache.kafka.clients.consumer.internals.SubscriptionState.FetchPosition
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.Metric
import org.apache.kafka.common.MetricName
import org.apache.kafka.common.PartitionInfo
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.WakeupException
import org.apache.kafka.common.utils.LogContext
import java.time.Duration
import java.util.*
import java.util.concurrent.atomic.AtomicBoolean
import java.util.regex.Pattern

/**
 * A mock of the [Consumer] interface you can use for testing code that uses Kafka. **This class is 
 * not threadsafe**. However, you can use the [schedulePollTask] method to write multithreaded
 * tests where a driver thread waits for [poll] to be called by a background thread and then can
 * safely perform operations during a callback.
 */
class MockConsumer<K, V>(offsetResetStrategy: OffsetResetStrategy) : Consumer<K, V> {
    
    private val partitions: MutableMap<String, List<PartitionInfo>> = HashMap()

    private val subscriptions: SubscriptionState =
        SubscriptionState(LogContext(), offsetResetStrategy)

    private val beginningOffsets: MutableMap<TopicPartition, Long?> = HashMap()

    private val endOffsets: MutableMap<TopicPartition, Long> = HashMap()

    private val committed: MutableMap<TopicPartition, OffsetAndMetadata> = HashMap()

    private val pollTasks: Queue<Runnable> = LinkedList()

    private val paused: MutableSet<TopicPartition> = HashSet()

    private val records: MutableMap<TopicPartition, MutableList<ConsumerRecord<K?, V?>>> = HashMap()

    private var pollException: KafkaException? = null

    private var offsetsException: KafkaException? = null

    private val wakeup: AtomicBoolean = AtomicBoolean(false)

    var lastPollTimeout: Duration? = null
        private set

    private var closed: Boolean = false

    var shouldRebalance: Boolean = false
        private set

    @Synchronized
    override fun assignment(): Set<TopicPartition> = subscriptions.assignedPartitions()

    /**
     * Simulate a rebalance event.
     */
    @Synchronized
    fun rebalance(newAssignment: Collection<TopicPartition>) {
        // TODO: Rebalance callbacks
        records.clear()
        subscriptions.assignFromSubscribed(newAssignment)
    }

    @Synchronized
    override fun subscription(): Set<String> = subscriptions.subscription()

    @Synchronized
    override fun subscribe(topics: Collection<String>) {
        subscribe(topics, NoOpConsumerRebalanceListener())
    }

    @Synchronized
    override fun subscribe(pattern: Pattern, callback: ConsumerRebalanceListener) {
        ensureNotClosed()
        committed.clear()
        subscriptions.subscribe(pattern, callback)
        val topicsToSubscribe: MutableSet<String> = HashSet()
        for (topic in partitions.keys) if (
            pattern.matcher(topic).matches()
            && !subscriptions.subscription().contains(topic)
        ) topicsToSubscribe.add(topic)

        ensureNotClosed()
        subscriptions.subscribeFromPattern(topicsToSubscribe)
        val assignedPartitions: MutableSet<TopicPartition> = HashSet()

        for (topic in topicsToSubscribe)
            for ((_, partition) in partitions[topic]!!)
                assignedPartitions.add(TopicPartition(topic, partition))

        subscriptions.assignFromSubscribed(assignedPartitions)
    }

    @Synchronized
    override fun subscribe(pattern: Pattern) {
        subscribe(pattern, NoOpConsumerRebalanceListener())
    }

    @Synchronized
    override fun subscribe(topics: Collection<String>, callback: ConsumerRebalanceListener) {
        ensureNotClosed()
        committed.clear()
        subscriptions.subscribe(HashSet(topics), callback)
    }

    @Synchronized
    override fun assign(partitions: Collection<TopicPartition>) {
        ensureNotClosed()
        committed.clear()
        subscriptions.assignFromUser(HashSet(partitions))
    }

    @Synchronized
    override fun unsubscribe() {
        ensureNotClosed()
        committed.clear()
        subscriptions.unsubscribe()
    }

    @Deprecated("")
    @Synchronized
    override fun poll(timeout: Long): ConsumerRecords<K?, V?> = poll(Duration.ofMillis(timeout))

    @Synchronized
    override fun poll(timeout: Duration): ConsumerRecords<K?, V?> {
        ensureNotClosed()
        lastPollTimeout = timeout

        // Synchronize around the entire execution so new tasks to be triggered on subsequent poll
        // calls can be added in the callback
        synchronized(pollTasks) {
            val task = pollTasks.poll()
            task?.run()
        }
        if (wakeup.get()) {
            wakeup.set(false)
            throw WakeupException()
        }
        if (pollException != null) {
            val exception: RuntimeException? = pollException
            pollException = null
            throw exception!!
        }

        // Handle seeks that need to wait for a poll() call to be processed
        for (tp in subscriptions.assignedPartitions())
            if (!subscriptions.hasValidPosition(tp)) updateFetchPosition(tp)

        // update the consumed offset
        val results: MutableMap<TopicPartition, MutableList<ConsumerRecord<K?, V?>>> = HashMap()
        val toClear: MutableList<TopicPartition> = ArrayList()
        for ((key, recs) in records) {
            if (!subscriptions.isPaused(key)) {
                for (rec in recs) {
                    val position = subscriptions.position(key)!!.offset
                    if (beginningOffsets[key] != null && beginningOffsets[key]!! > position)
                        throw OffsetOutOfRangeException(mapOf(key to position))

                    if (assignment().contains(key) && rec.offset >= position) {
                        results.computeIfAbsent(key) { ArrayList() }.add(rec)
                        val leaderAndEpoch = LeaderAndEpoch(null, rec.leaderEpoch)
                        val newPosition = FetchPosition(
                            offset = rec.offset + 1,
                            offsetEpoch = rec.leaderEpoch,
                            currentLeader = leaderAndEpoch,
                        )
                        subscriptions.position(key, newPosition)
                    }
                }
                toClear.add(key)
            }
        }
        toClear.forEach { p -> records.remove(p) }
        return ConsumerRecords(results)
    }

    @Synchronized
    fun addRecord(record: ConsumerRecord<K?, V?>) {
        ensureNotClosed()
        val tp = TopicPartition(record.topic, record.partition)
        val currentAssigned = subscriptions.assignedPartitions()
        check(currentAssigned.contains(tp)) {
            "Cannot add records for a partition that is not assigned to the consumer"
        }
        val recs = records.computeIfAbsent(tp) { ArrayList() }
        recs.add(record)
    }

    @Deprecated("Use setPollException(KafkaException) instead")
    @Synchronized
    fun setException(exception: KafkaException?) = setPollException(exception)

    @Synchronized
    fun setPollException(exception: KafkaException?) {
        pollException = exception
    }

    @Synchronized
    fun setOffsetsException(exception: KafkaException?) {
        offsetsException = exception
    }

    @Synchronized
    override fun commitAsync(
        offsets: Map<TopicPartition, OffsetAndMetadata>,
        callback: OffsetCommitCallback?,
    ) {
        ensureNotClosed()
        committed.putAll(offsets)
        callback?.onComplete(offsets, null)
    }

    @Synchronized
    override fun commitSync(offsets: Map<TopicPartition, OffsetAndMetadata>) =
        commitAsync(offsets, null)

    @Synchronized
    override fun commitAsync() = commitAsync(null)

    @Synchronized
    override fun commitAsync(callback: OffsetCommitCallback?) {
        ensureNotClosed()
        commitAsync(subscriptions.allConsumed(), callback)
    }

    @Synchronized
    override fun commitSync() = commitSync(subscriptions.allConsumed())

    @Synchronized
    override fun commitSync(timeout: Duration) = commitSync(subscriptions.allConsumed())

    override fun commitSync(offsets: Map<TopicPartition, OffsetAndMetadata>, timeout: Duration) =
        commitSync(offsets)

    @Synchronized
    override fun seek(partition: TopicPartition, offset: Long) {
        ensureNotClosed()
        subscriptions.seek(partition, offset)
    }

    override fun seek(partition: TopicPartition, offsetAndMetadata: OffsetAndMetadata) {
        ensureNotClosed()
        subscriptions.seek(partition, offsetAndMetadata.offset)
    }

    @Deprecated("")
    @Synchronized
    override fun committed(partition: TopicPartition): OffsetAndMetadata =
        committed(setOf(partition))[partition]!!

    @Deprecated("")
    override fun committed(partition: TopicPartition, timeout: Duration): OffsetAndMetadata =
        committed(partition)

    @Synchronized
    override fun committed(
        partitions: Set<TopicPartition>,
    ): Map<TopicPartition, OffsetAndMetadata?> {
        ensureNotClosed()
        return partitions.filter { committed.containsKey(it) }
            .associateWith { tp ->
                if (subscriptions.isAssigned(tp)) committed[tp]
                else OffsetAndMetadata(0)
            }
    }

    @Synchronized
    override fun committed(
        partitions: Set<TopicPartition>,
        timeout: Duration
    ): Map<TopicPartition, OffsetAndMetadata?> = committed(partitions)

    @Synchronized
    override fun position(partition: TopicPartition): Long {
        ensureNotClosed()
        require(subscriptions.isAssigned(partition)) {
            "You can only check the position for partitions assigned to this consumer."
        }
        var position = subscriptions.position(partition)
        if (position == null) {
            updateFetchPosition(partition)
            position = subscriptions.position(partition)
        }
        return position!!.offset
    }

    @Synchronized
    override fun position(partition: TopicPartition, timeout: Duration): Long = position(partition)

    @Synchronized
    override fun seekToBeginning(partitions: Collection<TopicPartition>) {
        ensureNotClosed()
        subscriptions.requestOffsetReset(partitions, OffsetResetStrategy.EARLIEST)
    }

    @Synchronized
    fun updateBeginningOffsets(newOffsets: Map<TopicPartition, Long?>) {
        beginningOffsets.putAll(newOffsets)
    }

    @Synchronized
    override fun seekToEnd(partitions: Collection<TopicPartition>) {
        ensureNotClosed()
        subscriptions.requestOffsetReset(partitions, OffsetResetStrategy.LATEST)
    }

    @Synchronized
    fun updateEndOffsets(newOffsets: Map<TopicPartition, Long>) = endOffsets.putAll(newOffsets)

    @Synchronized
    override fun metrics(): Map<MetricName, Metric> {
        ensureNotClosed()
        return emptyMap()
    }

    @Synchronized
    override fun partitionsFor(topic: String): List<PartitionInfo> {
        ensureNotClosed()
        return partitions.getOrDefault(topic, emptyList())
    }

    @Synchronized
    override fun listTopics(): Map<String, List<PartitionInfo>> {
        ensureNotClosed()
        return partitions
    }

    @Synchronized
    fun updatePartitions(topic: String, partitions: List<PartitionInfo>) {
        ensureNotClosed()
        this.partitions[topic] = partitions
    }

    @Synchronized
    override fun pause(partitions: Collection<TopicPartition>) {
        for (partition in partitions) {
            subscriptions.pause(partition)
            paused.add(partition)
        }
    }

    @Synchronized
    override fun resume(partitions: Collection<TopicPartition>) {
        for (partition in partitions) {
            subscriptions.resume(partition)
            paused.remove(partition)
        }
    }

    @Synchronized
    override fun offsetsForTimes(
        timestampsToSearch: Map<TopicPartition, Long>,
    ): Map<TopicPartition, OffsetAndTimestamp?> =
        throw UnsupportedOperationException("Not implemented yet.")

    @Synchronized
    override fun beginningOffsets(
        partitions: Collection<TopicPartition>,
    ): Map<TopicPartition, Long> {
        offsetsException?.let {
            offsetsException = null
            throw it
        }
        return partitions.associateWith { tp ->
            beginningOffsets[tp] ?: error("The partition $tp does not have a beginning offset.")
        }
    }

    @Synchronized
    override fun endOffsets(partitions: Collection<TopicPartition>): Map<TopicPartition, Long> {
        offsetsException?.let {
            offsetsException = null
            throw it
        }
        return partitions.associateWith { tp ->
            endOffsets[tp] ?: error("The partition $tp does not have an end offset.")
        }
    }

    override fun close() = close(Duration.ofMillis(KafkaConsumer.DEFAULT_CLOSE_TIMEOUT_MS))

    @Synchronized
    override fun close(timeout: Duration) {
        closed = true
    }

    @Synchronized
    fun closed(): Boolean = closed

    @Synchronized
    override fun wakeup() = wakeup.set(true)

    /**
     * Schedule a task to be executed during a poll(). One enqueued task will be executed per [poll]
     * invocation. You can use this repeatedly to mock out multiple responses to poll invocations.
     * @param task the task to be executed
     */
    @Synchronized
    fun schedulePollTask(task: Runnable) = synchronized(pollTasks) { pollTasks.add(task) }

    @Synchronized
    fun scheduleNopPollTask() = schedulePollTask {}

    @Synchronized
    override fun paused(): Set<TopicPartition> = paused.toSet()

    private fun ensureNotClosed() = check(!closed) { "This consumer has already been closed." }

    private fun updateFetchPosition(tp: TopicPartition) {
        if (subscriptions.isOffsetResetNeeded(tp)) resetOffsetPosition(tp)
        else if (!committed.containsKey(tp)) {
            subscriptions.requestOffsetReset(tp)
            resetOffsetPosition(tp)
        } else subscriptions.seek(tp, committed[tp]!!.offset)
    }

    private fun resetOffsetPosition(tp: TopicPartition) {
        val offset: Long?
        when(subscriptions.resetStrategy(tp)) {
            OffsetResetStrategy.LATEST -> {
                offset = endOffsets[tp]
                checkNotNull(offset) {
                    "MockConsumer didn't have end offset specified, but tried to seek to end"
                }
            }
            OffsetResetStrategy.EARLIEST -> {
                offset = beginningOffsets[tp]
                checkNotNull(offset) {
                    "MockConsumer didn't have beginning offset specified, but tried to seek to " +
                            "beginning"
                }
            }
            else -> throw NoOffsetForPartitionException(tp)
        }
        seek(tp, offset)
    }

    override fun partitionsFor(topic: String, timeout: Duration): List<PartitionInfo> =
        partitionsFor(topic)

    override fun listTopics(timeout: Duration): Map<String, List<PartitionInfo>> = listTopics()

    override fun offsetsForTimes(
        timestampsToSearch: Map<TopicPartition, Long>,
        timeout: Duration,
    ): Map<TopicPartition, OffsetAndTimestamp?> = offsetsForTimes(timestampsToSearch)

    override fun beginningOffsets(
        partitions: Collection<TopicPartition>,
        timeout: Duration,
    ): Map<TopicPartition, Long> = beginningOffsets(partitions)

    override fun endOffsets(
        partitions: Collection<TopicPartition>,
        timeout: Duration,
    ): Map<TopicPartition, Long> = endOffsets(partitions)

    override fun currentLag(topicPartition: TopicPartition): Long {
        return if (endOffsets.containsKey(topicPartition))
            endOffsets[topicPartition]!! - position(topicPartition)
        else {
            // if the test doesn't bother to set an end offset, we assume it wants to model being
            // caught up.
            0L
        }
    }

    override fun groupMetadata(): ConsumerGroupMetadata = ConsumerGroupMetadata(
        groupId = "dummy.group.id",
        generationId = 1,
        memberId = "1",
    )

    override fun enforceRebalance() = enforceRebalance(null)

    override fun enforceRebalance(reason: String?) {
        shouldRebalance = true
    }

    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("shouldRebalance"),
    )
    fun shouldRebalance(): Boolean = shouldRebalance

    fun resetShouldRebalance() {
        shouldRebalance = false
    }

    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("lastPollTimeout"),
    )
    fun lastPollTimeout(): Duration? = lastPollTimeout
}
