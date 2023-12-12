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

package org.apache.kafka.clients.consumer.internals

import java.util.Properties
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.IsolationLevel
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test

class FetchConfigTest {

    /**
     * Verify correctness if both the key and value [deserializers][Deserializer] provided to the
     * [FetchConfig] constructors are `nonnull`.
     */
    @Test
    fun testBasicFromConsumerConfig() {
        StringDeserializer().use { keyDeserializer ->
            StringDeserializer().use { valueDeserializer ->
                newFetchConfigFromConsumerConfig(keyDeserializer, valueDeserializer)
                newFetchConfigFromValues(keyDeserializer, valueDeserializer)
            }
        }
    }

    /**
     * Verify an exception is thrown if the key [deserializer][Deserializer] provided to the
     * [FetchConfig] constructors is `null`.
     */
    @Test
    @Disabled("Kotlin Migration - Key deserializer are not nullable.")
    fun testPreventNullKeyDeserializer() {
//        StringDeserializer().use { valueDeserializer ->
//            assertFailsWith<NullPointerException> {
//                newFetchConfigFromConsumerConfig(
//                    keyDeserializer = null,
//                    valueDeserializer = valueDeserializer,
//                )
//            }
//            assertFailsWith<NullPointerException> {
//                newFetchConfigFromValues(
//                    keyDeserializer = null,
//                    valueDeserializer = valueDeserializer,
//                )
//            }
//        }
    }

    /**
     * Verify an exception is thrown if the value [deserializer][Deserializer] provided to the
     * [FetchConfig] constructors is `null`.
     */
    @Test
    @Disabled("Kotlin Migration - Value deserializer are not nullable.")
    fun testPreventNullValueDeserializer() {
//        StringDeserializer().use { keyDeserializer ->
//            assertFailsWith<NullPointerException> {
//                newFetchConfigFromConsumerConfig(
//                    keyDeserializer = keyDeserializer,
//                    valueDeserializer = null,
//                )
//            }
//            assertFailsWith<NullPointerException> {
//                newFetchConfigFromValues(
//                    keyDeserializer = keyDeserializer,
//                    valueDeserializer = null,
//                )
//            }
//        }
    }

    private fun newFetchConfigFromConsumerConfig(
        keyDeserializer: Deserializer<String>,
        valueDeserializer: Deserializer<String>,
    ) {
        val properties = Properties()
        properties[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] = "localhost:9092"
        properties[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = StringSerializer::class.java.name
        properties[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = StringSerializer::class.java.name
        val config = ConsumerConfig(properties)
        FetchConfig(
            config = config,
            keyDeserializer = keyDeserializer,
            valueDeserializer = valueDeserializer,
            isolationLevel = IsolationLevel.READ_UNCOMMITTED,
        )
    }

    private fun newFetchConfigFromValues(
        keyDeserializer: Deserializer<String>,
        valueDeserializer: Deserializer<String>,
    ) {
        FetchConfig(
            minBytes = ConsumerConfig.DEFAULT_FETCH_MIN_BYTES,
            maxBytes = ConsumerConfig.DEFAULT_FETCH_MAX_BYTES,
            maxWaitMs = ConsumerConfig.DEFAULT_FETCH_MAX_WAIT_MS,
            fetchSize = ConsumerConfig.DEFAULT_MAX_PARTITION_FETCH_BYTES,
            maxPollRecords = ConsumerConfig.DEFAULT_MAX_POLL_RECORDS,
            checkCrcs = true,
            clientRackId = ConsumerConfig.DEFAULT_CLIENT_RACK,
            keyDeserializer = keyDeserializer,
            valueDeserializer = valueDeserializer,
            isolationLevel = IsolationLevel.READ_UNCOMMITTED,
        )
    }
}

