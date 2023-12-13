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

import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerInterceptor
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.ClusterResource
import org.apache.kafka.common.ClusterResourceListener
import org.apache.kafka.common.config.ConfigException
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference

class MockProducerInterceptor : ClusterResourceListener, ProducerInterceptor<String?, String?> {

    private var appendStr: String? = null

    init {
        INIT_COUNT.incrementAndGet()
    }

    override fun configure(configs: Map<String, *>) {
        // ensure this method is called and expected configs are passed in
        val o = configs[APPEND_STRING_PROP] ?: throw ConfigException(
            "Mock producer interceptor expects configuration $APPEND_STRING_PROP"
        )
        if (o is String) appendStr = o

        // clientId also must be in configs
        configs[ProducerConfig.CLIENT_ID_CONFIG] ?: throw ConfigException(
            "Mock producer interceptor expects configuration ${ProducerConfig.CLIENT_ID_CONFIG}"
        )

        CONFIG_COUNT.incrementAndGet()
        if (CONFIG_COUNT.get() == THROW_ON_CONFIG_EXCEPTION_THRESHOLD.get()) throw ConfigException(
            "Failed to instantiate interceptor. Reached configuration exception threshold."
        )
    }

    override fun onSend(record: ProducerRecord<String?, String?>): ProducerRecord<String?, String?> {
        ONSEND_COUNT.incrementAndGet()
        return ProducerRecord(
            topic = record.topic,
            partition = record.partition,
            key = record.key,
            value = record.value + appendStr,
        )
    }

    override fun onAcknowledgement(metadata: RecordMetadata?, exception: Exception?) {
        ON_ACKNOWLEDGEMENT_COUNT.incrementAndGet()
        // This will ensure that we get the cluster metadata when onAcknowledgement is called for
        // the first time as subsequent compareAndSet operations will fail.
        CLUSTER_ID_BEFORE_ON_ACKNOWLEDGEMENT.compareAndSet(NO_CLUSTER_ID, CLUSTER_META.get())
        if (exception != null) {
            ON_ERROR_COUNT.incrementAndGet()
            if (metadata != null) ON_ERROR_WITH_METADATA_COUNT.incrementAndGet()
        } else if (metadata != null) ON_SUCCESS_COUNT.incrementAndGet()
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

        val ONSEND_COUNT = AtomicInteger(0)

        val CONFIG_COUNT = AtomicInteger(0)

        val THROW_CONFIG_EXCEPTION = AtomicInteger(0)

        val THROW_ON_CONFIG_EXCEPTION_THRESHOLD = AtomicInteger(0)

        val ON_SUCCESS_COUNT = AtomicInteger(0)

        val ON_ERROR_COUNT = AtomicInteger(0)

        val ON_ERROR_WITH_METADATA_COUNT = AtomicInteger(0)

        val ON_ACKNOWLEDGEMENT_COUNT = AtomicInteger(0)

        val CLUSTER_META = AtomicReference<ClusterResource?>()

        val NO_CLUSTER_ID = ClusterResource("no_cluster_id")

        val CLUSTER_ID_BEFORE_ON_ACKNOWLEDGEMENT = AtomicReference(
            NO_CLUSTER_ID
        )

        const val APPEND_STRING_PROP = "mock.interceptor.append"

        fun setThrowOnConfigExceptionThreshold(value: Int) {
            THROW_ON_CONFIG_EXCEPTION_THRESHOLD.set(value)
        }

        fun resetCounters() {
            INIT_COUNT.set(0)
            CLOSE_COUNT.set(0)
            ONSEND_COUNT.set(0)
            CONFIG_COUNT.set(0)
            THROW_CONFIG_EXCEPTION.set(0)
            ON_SUCCESS_COUNT.set(0)
            ON_ERROR_COUNT.set(0)
            ON_ERROR_WITH_METADATA_COUNT.set(0)
            CLUSTER_META.set(null)
            CLUSTER_ID_BEFORE_ON_ACKNOWLEDGEMENT.set(NO_CLUSTER_ID)
        }
    }
}
