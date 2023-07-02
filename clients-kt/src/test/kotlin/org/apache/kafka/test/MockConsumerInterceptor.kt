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

package org.apache.kafka.test

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerInterceptor
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.ClusterResource
import org.apache.kafka.common.ClusterResourceListener
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.config.ConfigException
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference

class MockConsumerInterceptor : ClusterResourceListener, ConsumerInterceptor<String, String> {

    init {
        INIT_COUNT.incrementAndGet()
    }

    override fun configure(configs: Map<String, *>) {
        // clientId must be in configs
        configs[ConsumerConfig.CLIENT_ID_CONFIG] ?: throw ConfigException(
            "Mock consumer interceptor expects configuration ${ProducerConfig.CLIENT_ID_CONFIG}"
        )
        CONFIG_COUNT.incrementAndGet()

        if (CONFIG_COUNT.get() == THROW_ON_CONFIG_EXCEPTION_THRESHOLD.get()) throw ConfigException(
            "Failed to instantiate interceptor. Reached configuration exception threshold."
        )
    }

    override fun onConsume(
        records: ConsumerRecords<String?, String?>,
    ): ConsumerRecords<String?, String?> {

        // This will ensure that we get the cluster metadata when onConsume is called for the first
        // time as subsequent compareAndSet operations will fail.
        CLUSTER_ID_BEFORE_ON_CONSUME.compareAndSet(NO_CLUSTER_ID, CLUSTER_META.get())
        val recordMap = mutableMapOf<TopicPartition, List<ConsumerRecord<String?, String?>>>()

        for (tp in records.partitions()) {
            val lst: List<ConsumerRecord<String?, String?>> = records.records(tp).map { record ->
                with(record) {
                    ConsumerRecord(
                        topic = topic,
                        partition = partition,
                        offset = offset,
                        timestamp = timestamp,
                        timestampType = timestampType,
                        serializedKeySize = serializedKeySize,
                        serializedValueSize = serializedValueSize,
                        key = key,
                        value = value!!.uppercase(),
                    )
                }
            }
            recordMap[tp] = lst
        }
        return ConsumerRecords(recordMap)
    }

    override fun onCommit(offsets: Map<TopicPartition, OffsetAndMetadata>) {
        ON_COMMIT_COUNT.incrementAndGet()
    }

    override fun close() {
        CLOSE_COUNT.incrementAndGet()
    }

    override fun onUpdate(clusterResource: ClusterResource?) {
        CLUSTER_META.set(clusterResource)
    }

    companion object {

        val INIT_COUNT = AtomicInteger(0)

        val CLOSE_COUNT = AtomicInteger(0)

        val ON_COMMIT_COUNT = AtomicInteger(0)

        val CONFIG_COUNT = AtomicInteger(0)

        val THROW_CONFIG_EXCEPTION = AtomicInteger(0)

        val THROW_ON_CONFIG_EXCEPTION_THRESHOLD = AtomicInteger(0)

        val CLUSTER_META = AtomicReference<ClusterResource?>()

        val NO_CLUSTER_ID = ClusterResource("no_cluster_id")

        val CLUSTER_ID_BEFORE_ON_CONSUME = AtomicReference(NO_CLUSTER_ID)

        fun setThrowOnConfigExceptionThreshold(value: Int) {
            THROW_ON_CONFIG_EXCEPTION_THRESHOLD.set(value)
        }

        fun resetCounters() {
            INIT_COUNT.set(0)
            CLOSE_COUNT.set(0)
            ON_COMMIT_COUNT.set(0)
            CONFIG_COUNT.set(0)
            THROW_CONFIG_EXCEPTION.set(0)
            CLUSTER_META.set(null)
            CLUSTER_ID_BEFORE_ON_CONSUME.set(NO_CLUSTER_ID)
        }
    }
}
