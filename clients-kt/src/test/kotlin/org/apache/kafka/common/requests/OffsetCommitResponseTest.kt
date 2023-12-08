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
import org.apache.kafka.common.message.OffsetCommitResponseData
import org.apache.kafka.common.message.OffsetCommitResponseData.OffsetCommitResponsePartition
import org.apache.kafka.common.message.OffsetCommitResponseData.OffsetCommitResponseTopic
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.protocol.MessageUtil.toByteBuffer
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

open class OffsetCommitResponseTest {

    internal lateinit var expectedErrorCounts: Map<Errors, Int>
    
    internal lateinit var errorsMap: Map<TopicPartition, Errors>
    
    @BeforeEach
    fun setUp() {
        expectedErrorCounts = mapOf(
            ERROR_ONE to 1,
            ERROR_TWO to 1,
        )
        errorsMap = mapOf(
            TOPIC_PARTITION_ONE to ERROR_ONE,
            TOPIC_PARTITION_TWO to ERROR_TWO,
        )
    }

    @Test
    open fun testConstructorWithErrorResponse() {
        val response = OffsetCommitResponse(THROTTLE_TIME_MS, errorsMap)
        assertEquals(expectedErrorCounts, response.errorCounts())
        assertEquals(THROTTLE_TIME_MS, response.throttleTimeMs())
    }

    @Test
    open fun testParse() {
        val data = OffsetCommitResponseData()
            .setTopics(
                listOf(
                    OffsetCommitResponseTopic().setPartitions(
                        listOf(
                            OffsetCommitResponsePartition()
                                .setPartitionIndex(PARTITION_ONE)
                                .setErrorCode(ERROR_ONE.code)
                        )
                    ),
                    OffsetCommitResponseTopic().setPartitions(
                        listOf(
                            OffsetCommitResponsePartition()
                                .setPartitionIndex(PARTITION_TWO)
                                .setErrorCode(ERROR_TWO.code)
                        )
                    )
                )
            )
            .setThrottleTimeMs(THROTTLE_TIME_MS)
        for (version in ApiKeys.OFFSET_COMMIT.allVersions()) {
            val buffer = toByteBuffer(data, version)
            val response = OffsetCommitResponse.parse(buffer, version)
            assertEquals(expectedErrorCounts, response.errorCounts())
            if (version >= 3) assertEquals(THROTTLE_TIME_MS, response.throttleTimeMs())
            else assertEquals(AbstractResponse.DEFAULT_THROTTLE_TIME, response.throttleTimeMs())
            assertEquals(version >= 4, response.shouldClientThrottle(version))
        }
    }

    companion object {

        internal const val THROTTLE_TIME_MS = 10

        internal const val TOPIC_ONE = "topic1"

        internal const val TOPIC_TWO = "topic2"

        internal const val PARTITION_ONE = 1

        internal const val PARTITION_TWO = 2

        internal val ERROR_ONE = Errors.COORDINATOR_NOT_AVAILABLE

        internal val ERROR_TWO = Errors.NOT_COORDINATOR

        internal val TOPIC_PARTITION_ONE = TopicPartition(TOPIC_ONE, PARTITION_ONE)

        internal val TOPIC_PARTITION_TWO = TopicPartition(TOPIC_TWO, PARTITION_TWO)
    }
}
