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

import org.apache.kafka.common.Uuid
import org.apache.kafka.common.message.AlterPartitionRequestData
import org.apache.kafka.common.message.AlterPartitionRequestData.BrokerState
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.utils.annotation.ApiKeyVersionsSource
import org.junit.jupiter.params.ParameterizedTest
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertTrue

internal class AlterPartitionRequestTest {

    var topic = "test-topic"

    var topicId = Uuid.randomUuid()

    @ParameterizedTest
    @ApiKeyVersionsSource(apiKey = ApiKeys.ALTER_PARTITION)
    fun testBuildAlterPartitionRequest(version: Short) {
        val request = AlterPartitionRequestData()
            .setBrokerId(1)
            .setBrokerEpoch(1)

        val topicData = AlterPartitionRequestData.TopicData()
            .setTopicId(topicId)
            .setTopicName(topic)

        val newIsrWithBrokerEpoch = mutableListOf<BrokerState>()
        newIsrWithBrokerEpoch.add(BrokerState().setBrokerId(1).setBrokerEpoch(1001))
        newIsrWithBrokerEpoch.add(BrokerState().setBrokerId(2).setBrokerEpoch(1002))
        newIsrWithBrokerEpoch.add(BrokerState().setBrokerId(3).setBrokerEpoch(1003))

        topicData.partitions += AlterPartitionRequestData.PartitionData()
            .setPartitionIndex(0)
            .setLeaderEpoch(1)
            .setPartitionEpoch(10)
            .setNewIsrWithEpochs(newIsrWithBrokerEpoch)

        request.topics += topicData

        val builder = AlterPartitionRequest.Builder(data = request, canUseTopicIds = version > 1)
        var alterPartitionRequest = builder.build(version)

        assertEquals(1, alterPartitionRequest.data().topics.size)
        assertEquals(1, alterPartitionRequest.data().topics[0].partitions.size)

        val partitionData = alterPartitionRequest.data().topics[0].partitions[0]
        if (version < 3) {
            assertContentEquals(intArrayOf(1, 2, 3), partitionData.newIsr)
            assertTrue(partitionData.newIsrWithEpochs.isEmpty())
        } else {
            assertEquals(newIsrWithBrokerEpoch, partitionData.newIsrWithEpochs)
            assertTrue(partitionData.newIsr.isEmpty())
        }

        // Build the request again to make sure build() is idempotent.
        alterPartitionRequest = builder.build(version)
        assertEquals(1, alterPartitionRequest.data().topics.size)
        assertEquals(1, alterPartitionRequest.data().topics[0].partitions.size)
        alterPartitionRequest.data().topics[0].partitions[0]
        if (version < 3) {
            assertEquals(intArrayOf(1, 2, 3), partitionData.newIsr)
            assertTrue(partitionData.newIsrWithEpochs.isEmpty())
        } else {
            assertEquals(newIsrWithBrokerEpoch, partitionData.newIsrWithEpochs)
            assertTrue(partitionData.newIsr.isEmpty())
        }
    }
}
