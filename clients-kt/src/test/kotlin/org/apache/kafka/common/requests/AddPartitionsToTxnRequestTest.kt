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

package org.apache.kafka.common.requests

import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.utils.annotation.ApiKeyVersionsSource
import org.junit.jupiter.params.ParameterizedTest
import kotlin.test.assertEquals

class AddPartitionsToTxnRequestTest {

    @ParameterizedTest
    @ApiKeyVersionsSource(apiKey = ApiKeys.ADD_PARTITIONS_TO_TXN)
    fun testConstructor(version: Short) {
        val partitions: MutableList<TopicPartition> = ArrayList()
        partitions.add(TopicPartition(topic = "topic", partition = 0))
        partitions.add(TopicPartition(topic = "topic", partition = 1))
        val builder = AddPartitionsToTxnRequest.Builder(
            transactionalId = TRANSACTIONAL_ID,
            producerId = PRODUCER_ID,
            producerEpoch = PRODUCER_EPOCH,
            partitions = partitions,
        )
        val request = builder.build(version)
        assertEquals(TRANSACTIONAL_ID, request.data().transactionalId)
        assertEquals(PRODUCER_ID, request.data().producerId)
        assertEquals(PRODUCER_EPOCH, request.data().producerEpoch)
        assertEquals(partitions, request.partitions())
        val response = request.getErrorResponse(
            throttleTimeMs = THROTTLE_TIME_MS,
            e = Errors.UNKNOWN_TOPIC_OR_PARTITION.exception!!
        )
        assertEquals(mapOf(Errors.UNKNOWN_TOPIC_OR_PARTITION to 2), response.errorCounts())
        assertEquals(THROTTLE_TIME_MS, response.throttleTimeMs())
    }

    companion object {

        private const val TRANSACTIONAL_ID = "transactionalId"

        private const val PRODUCER_ID = 10L

        private const val PRODUCER_EPOCH: Short = 1

        private const val THROTTLE_TIME_MS = 10
    }
}
