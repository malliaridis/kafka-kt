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

package org.apache.kafka.clients.producer

import org.apache.kafka.clients.consumer.ConsumerGroupMetadata
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.Metric
import org.apache.kafka.common.MetricName
import org.apache.kafka.common.PartitionInfo
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.ProducerFencedException
import java.io.Closeable
import java.time.Duration
import java.util.concurrent.Future

/**
 * The interface for the [KafkaProducer]
 *
 * @see KafkaProducer
 * @see MockProducer
 */
interface Producer<K, V> : Closeable {

    /**
     * See [KafkaProducer.initTransactions]
     */
    fun initTransactions()

    /**
     * See [KafkaProducer.beginTransaction]
     */
    @Throws(ProducerFencedException::class)
    fun beginTransaction()

    /**
     * See [KafkaProducer.sendOffsetsToTransaction]
     */
    @Deprecated("")
    @Throws(ProducerFencedException::class)
    fun sendOffsetsToTransaction(
        offsets: Map<TopicPartition, OffsetAndMetadata>,
        consumerGroupId: String,
    )

    /**
     * See [KafkaProducer.sendOffsetsToTransaction]
     */
    @Throws(ProducerFencedException::class)
    fun sendOffsetsToTransaction(
        offsets: Map<TopicPartition, OffsetAndMetadata>,
        groupMetadata: ConsumerGroupMetadata,
    )

    /**
     * See [KafkaProducer.commitTransaction]
     */
    @Throws(ProducerFencedException::class)
    fun commitTransaction()

    /**
     * See [KafkaProducer.abortTransaction]
     */
    @Throws(ProducerFencedException::class)
    fun abortTransaction()

    /**
     * See [KafkaProducer.send]
     */
    fun send(record: ProducerRecord<K, V>): Future<RecordMetadata>

    /**
     * See [KafkaProducer.send]
     */
    fun send(record: ProducerRecord<K, V>, callback: Callback?): Future<RecordMetadata>

    /**
     * See [KafkaProducer.flush]
     */
    fun flush()

    /**
     * See [KafkaProducer.partitionsFor]
     */
    fun partitionsFor(topic: String): List<PartitionInfo>

    /**
     * See [KafkaProducer.metrics]
     */
    fun metrics(): Map<MetricName, Metric>

    /**
     * See [KafkaProducer.close]
     */
    override fun close()

    /**
     * See [KafkaProducer.close]
     */
    fun close(timeout: Duration)
}
