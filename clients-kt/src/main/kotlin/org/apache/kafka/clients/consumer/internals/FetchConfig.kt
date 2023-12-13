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

import java.util.Objects
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.IsolationLevel
import org.apache.kafka.common.serialization.Deserializer

/**
 * [FetchConfig] represents the static configuration for fetching records from Kafka. It is simply a way
 * to bundle the immutable settings that were presented at the time the [Consumer] was created for later use by
 * classes like [Fetcher], [CompletedFetch], etc.
 *
 * In most cases, the values stored and returned by [FetchConfig] will be those stored in the following
 * [consumer configuration][ConsumerConfig] settings:
 *
 * - [minBytes]: [ConsumerConfig.FETCH_MIN_BYTES_CONFIG]
 * - [maxBytes]: [ConsumerConfig.FETCH_MAX_BYTES_CONFIG]
 * - [maxWaitMs]: [ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG]
 * - [fetchSize]: [ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG]
 * - [maxPollRecords]: [ConsumerConfig.MAX_POLL_RECORDS_CONFIG]
 * - [checkCrcs]: [ConsumerConfig.CHECK_CRCS_CONFIG]
 * - [clientRackId]: [ConsumerConfig.CLIENT_RACK_CONFIG]
 * - [keyDeserializer]: [ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG]
 * - [valueDeserializer]: [ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG]
 * - [isolationLevel]: [ConsumerConfig.ISOLATION_LEVEL_CONFIG]
 *
 * However, there are places in the code where additional logic is used to determine these fetch-related configuration
 * values. In those cases, the values are calculated outside of this class and simply passed in when constructed.
 *
 * Note: the [deserializers][Deserializer] used for the key and value are not closed by this class. They should be
 * closed by the creator of the [FetchConfig].
 *
 * @param K Type used to [deserialize][Deserializer] the message/record key
 * @param V Type used to [deserialize][Deserializer] the message/record value
 */
data class FetchConfig<K, V>(
    val minBytes: Int,
    val maxBytes: Int,
    val maxWaitMs: Int,
    val fetchSize: Int,
    val maxPollRecords: Int,
    val checkCrcs: Boolean,
    val clientRackId: String,
    val keyDeserializer: Deserializer<K>,
    val valueDeserializer: Deserializer<V>,
    val isolationLevel: IsolationLevel,
) {

    constructor(
        config: ConsumerConfig,
        keyDeserializer: Deserializer<K>,
        valueDeserializer: Deserializer<V>,
        isolationLevel: IsolationLevel,
    ) : this(
        minBytes = config.getInt(ConsumerConfig.FETCH_MIN_BYTES_CONFIG)!!,
        maxBytes = config.getInt(ConsumerConfig.FETCH_MAX_BYTES_CONFIG)!!,
        maxWaitMs = config.getInt(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG)!!,
        fetchSize = config.getInt(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG)!!,
        maxPollRecords = config.getInt(ConsumerConfig.MAX_POLL_RECORDS_CONFIG)!!,
        checkCrcs = config.getBoolean(ConsumerConfig.CHECK_CRCS_CONFIG)!!,
        clientRackId = config.getString(ConsumerConfig.CLIENT_RACK_CONFIG)!!,
        keyDeserializer = keyDeserializer,
        valueDeserializer = valueDeserializer,
        isolationLevel = isolationLevel,
    )

    override fun toString(): String {
        return "FetchConfig{" +
                "minBytes=$minBytes" +
                ", maxBytes=$maxBytes" +
                ", maxWaitMs=$maxWaitMs" +
                ", fetchSize=$fetchSize" +
                ", maxPollRecords=$maxPollRecords" +
                ", checkCrcs=$checkCrcs" +
                ", clientRackId='$clientRackId'" +
                ", keyDeserializer=$keyDeserializer" +
                ", valueDeserializer=$valueDeserializer" +
                ", isolationLevel=$isolationLevel" +
                '}'
    }
}
