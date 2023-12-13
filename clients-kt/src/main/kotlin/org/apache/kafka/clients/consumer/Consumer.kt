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

import java.io.Closeable
import java.time.Duration
import java.util.regex.Pattern
import org.apache.kafka.common.Metric
import org.apache.kafka.common.MetricName
import org.apache.kafka.common.PartitionInfo
import org.apache.kafka.common.TopicPartition

/**
 * @see KafkaConsumer
 *
 * @see MockConsumer
 */
interface Consumer<K, V> : Closeable {

    /**
     * @see KafkaConsumer.assignment
     */
    fun assignment(): Set<TopicPartition>

    /**
     * @see KafkaConsumer.subscription
     */
    fun subscription(): Set<String>

    /**
     * @see KafkaConsumer.subscribe
     */
    fun subscribe(topics: Collection<String>)

    /**
     * @see KafkaConsumer.subscribe
     */
    fun subscribe(topics: Collection<String>, callback: ConsumerRebalanceListener)

    /**
     * @see KafkaConsumer.assign
     */
    fun assign(partitions: Collection<TopicPartition>)

    /**
     * @see KafkaConsumer.subscribe
     */
    fun subscribe(pattern: Pattern, callback: ConsumerRebalanceListener)

    /**
     * @see KafkaConsumer.subscribe
     */
    fun subscribe(pattern: Pattern)

    /**
     * @see KafkaConsumer.unsubscribe
     */
    fun unsubscribe()

    /**
     * @see KafkaConsumer.poll
     */
    @Deprecated("")
    fun poll(timeout: Long): ConsumerRecords<K, V>

    /**
     * @see KafkaConsumer.poll
     */
    fun poll(timeout: Duration): ConsumerRecords<K, V>

    /**
     * @see KafkaConsumer.commitSync
     */
    fun commitSync()

    /**
     * @see KafkaConsumer.commitSync
     */
    fun commitSync(timeout: Duration)

    /**
     * @see KafkaConsumer.commitSync
     */
    fun commitSync(offsets: Map<TopicPartition, OffsetAndMetadata>)

    /**
     * @see KafkaConsumer.commitSync
     */
    fun commitSync(offsets: Map<TopicPartition, OffsetAndMetadata>, timeout: Duration)

    /**
     * @see KafkaConsumer.commitAsync
     */
    fun commitAsync()

    /**
     * @see KafkaConsumer.commitAsync
     */
    fun commitAsync(callback: OffsetCommitCallback?)

    /**
     * @see KafkaConsumer.commitAsync
     */
    fun commitAsync(
        offsets: Map<TopicPartition, OffsetAndMetadata>,
        callback: OffsetCommitCallback?,
    )

    /**
     * @see KafkaConsumer.seek
     */
    fun seek(partition: TopicPartition, offset: Long)

    /**
     * @see KafkaConsumer.seek
     */
    fun seek(partition: TopicPartition, offsetAndMetadata: OffsetAndMetadata)

    /**
     * @see KafkaConsumer.seekToBeginning
     */
    fun seekToBeginning(partitions: Collection<TopicPartition>)

    /**
     * @see KafkaConsumer.seekToEnd
     */
    fun seekToEnd(partitions: Collection<TopicPartition>)

    /**
     * @see KafkaConsumer.position
     */
    fun position(partition: TopicPartition): Long

    /**
     * @see KafkaConsumer.position
     */
    fun position(partition: TopicPartition, timeout: Duration): Long

    /**
     * @see KafkaConsumer.committed
     */
    @Deprecated("")
    fun committed(partition: TopicPartition): OffsetAndMetadata

    /**
     * @see KafkaConsumer.committed
     */
    @Deprecated("")
    fun committed(partition: TopicPartition, timeout: Duration): OffsetAndMetadata

    /**
     * @see KafkaConsumer.committed
     */
    fun committed(partitions: Set<TopicPartition>): Map<TopicPartition, OffsetAndMetadata?>

    /**
     * @see KafkaConsumer.committed
     */
    fun committed(
        partitions: Set<TopicPartition>,
        timeout: Duration
    ): Map<TopicPartition, OffsetAndMetadata?>

    /**
     * @see KafkaConsumer.metrics
     */
    fun metrics(): Map<MetricName, Metric>

    /**
     * @see KafkaConsumer.partitionsFor
     */
    fun partitionsFor(topic: String): List<PartitionInfo>

    /**
     * @see KafkaConsumer.partitionsFor
     */
    fun partitionsFor(topic: String, timeout: Duration): List<PartitionInfo>

    /**
     * @see KafkaConsumer.listTopics
     */
    fun listTopics(): Map<String, List<PartitionInfo>>

    /**
     * @see KafkaConsumer.listTopics
     */
    fun listTopics(timeout: Duration): Map<String, List<PartitionInfo>>

    /**
     * @see KafkaConsumer.paused
     */
    fun paused(): Set<TopicPartition>

    /**
     * @see KafkaConsumer.pause
     */
    fun pause(partitions: Collection<TopicPartition>)

    /**
     * @see KafkaConsumer.resume
     */
    fun resume(partitions: Collection<TopicPartition>)

    /**
     * @see KafkaConsumer.offsetsForTimes
     */
    fun offsetsForTimes(
        timestampsToSearch: Map<TopicPartition, Long>,
    ): Map<TopicPartition, OffsetAndTimestamp?>

    /**
     * @see KafkaConsumer.offsetsForTimes
     */
    fun offsetsForTimes(
        timestampsToSearch: Map<TopicPartition, Long>,
        timeout: Duration,
    ): Map<TopicPartition, OffsetAndTimestamp?>

    /**
     * @see KafkaConsumer.beginningOffsets
     */
    fun beginningOffsets(partitions: Collection<TopicPartition>): Map<TopicPartition, Long>

    /**
     * @see KafkaConsumer.beginningOffsets
     */
    fun beginningOffsets(
        partitions: Collection<TopicPartition>,
        timeout: Duration
    ): Map<TopicPartition, Long>

    /**
     * @see KafkaConsumer.endOffsets
     */
    fun endOffsets(partitions: Collection<TopicPartition>): Map<TopicPartition, Long>

    /**
     * @see KafkaConsumer.endOffsets
     */
    fun endOffsets(
        partitions: Collection<TopicPartition>,
        timeout: Duration
    ): Map<TopicPartition, Long>

    /**
     * @see KafkaConsumer.currentLag
     */
    fun currentLag(topicPartition: TopicPartition): Long?

    /**
     * @see KafkaConsumer.groupMetadata
     */
    fun groupMetadata(): ConsumerGroupMetadata

    /**
     * @see KafkaConsumer.enforceRebalance
     */
    fun enforceRebalance()

    /**
     * @see KafkaConsumer.enforceRebalance
     */
    fun enforceRebalance(reason: String? = null)

    /**
     * @see KafkaConsumer.close
     */
    override fun close()

    /**
     * @see KafkaConsumer.close
     */
    fun close(timeout: Duration)

    /**
     * @see KafkaConsumer.wakeup
     */
    fun wakeup()
}
