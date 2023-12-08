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
import org.apache.kafka.common.message.TxnOffsetCommitRequestData.TxnOffsetCommitRequestPartition
import org.apache.kafka.common.message.TxnOffsetCommitRequestData.TxnOffsetCommitRequestTopic
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.TxnOffsetCommitRequest.CommittedOffset
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

class TxnOffsetCommitRequestTest : OffsetCommitRequestTest() {

    private lateinit var offsets: Map<TopicPartition, CommittedOffset>

    private lateinit var builder: TxnOffsetCommitRequest.Builder

    private lateinit var builderWithGroupMetadata: TxnOffsetCommitRequest.Builder
    
    @BeforeEach
    override fun setUp() {
        super.setUp()
        offsets = mapOf(
            TopicPartition(TOPIC_ONE, PARTITION_ONE) to CommittedOffset(
                offset = OFFSET,
                metadata = METADATA,
                leaderEpoch = LEADER_EPOCH.toInt(),
            ),
            TopicPartition(TOPIC_TWO, PARTITION_TWO) to CommittedOffset(
                offset = OFFSET,
                metadata = METADATA,
                leaderEpoch = LEADER_EPOCH.toInt(),
            ),
        )
        builder = TxnOffsetCommitRequest.Builder(
            transactionalId = TRANSACTIONAL_ID,
            consumerGroupId = GROUP_ID,
            producerId = PRODUCER_ID.toLong(),
            producerEpoch = PRODUCER_EPOCH,
            pendingTxnOffsetCommits = offsets,
        )
        builderWithGroupMetadata = TxnOffsetCommitRequest.Builder(
            transactionalId = TRANSACTIONAL_ID,
            consumerGroupId = GROUP_ID,
            producerId = PRODUCER_ID.toLong(),
            producerEpoch = PRODUCER_EPOCH,
            pendingTxnOffsetCommits = offsets,
            memberId = MEMBER_ID,
            generationId = GENERATION_ID,
            groupInstanceId = GROUP_INSTANCE_ID,
        )
    }

    @Test
    override fun testConstructor() {
        val errorsMap = mapOf(
            TopicPartition(TOPIC_ONE, PARTITION_ONE) to Errors.NOT_COORDINATOR,
            TopicPartition(TOPIC_TWO, PARTITION_TWO) to Errors.NOT_COORDINATOR,
        )
        val expectedTopics = listOf(
            TxnOffsetCommitRequestTopic()
                .setName(TOPIC_ONE)
                .setPartitions(
                    listOf(
                        TxnOffsetCommitRequestPartition()
                            .setPartitionIndex(PARTITION_ONE)
                            .setCommittedOffset(OFFSET)
                            .setCommittedLeaderEpoch(LEADER_EPOCH.toInt())
                            .setCommittedMetadata(METADATA)
                    )
                ),
            TxnOffsetCommitRequestTopic()
                .setName(TOPIC_TWO)
                .setPartitions(
                    listOf(
                        TxnOffsetCommitRequestPartition()
                            .setPartitionIndex(PARTITION_TWO)
                            .setCommittedOffset(OFFSET)
                            .setCommittedLeaderEpoch(LEADER_EPOCH.toInt())
                            .setCommittedMetadata(METADATA),
                    )
                )
        )
        for (version in ApiKeys.TXN_OFFSET_COMMIT.allVersions()) {
            val request = if (version < 3) builder.build(version) else builderWithGroupMetadata.build(version)

            assertEquals(offsets, request.offsets())
            assertEquals(expectedTopics, TxnOffsetCommitRequest.getTopics(request.offsets()))

            val response = request.getErrorResponse(
                throttleTimeMs = THROTTLE_TIME_MS,
                e = Errors.NOT_COORDINATOR.exception!!,
            )
            assertEquals(errorsMap, response.errors())
            assertEquals(mapOf(Errors.NOT_COORDINATOR to 2), response.errorCounts())
            assertEquals(THROTTLE_TIME_MS, response.throttleTimeMs())
        }
    }

    companion object {
        
        private const val TRANSACTIONAL_ID = "transactionalId"
        
        private const val PRODUCER_ID = 10
        
        private const val PRODUCER_EPOCH: Short = 1
        
        private const val GENERATION_ID = 5
    }
}
