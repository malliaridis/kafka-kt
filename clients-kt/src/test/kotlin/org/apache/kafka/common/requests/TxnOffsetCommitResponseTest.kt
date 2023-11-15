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

import org.apache.kafka.common.message.TxnOffsetCommitResponseData
import org.apache.kafka.common.message.TxnOffsetCommitResponseData.TxnOffsetCommitResponsePartition
import org.apache.kafka.common.message.TxnOffsetCommitResponseData.TxnOffsetCommitResponseTopic
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.MessageUtil.toByteBuffer
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

class TxnOffsetCommitResponseTest : OffsetCommitResponseTest() {

    @Test
    override fun testConstructorWithErrorResponse() {
        val response = TxnOffsetCommitResponse(throttleTimeMs, errorsMap)
        assertEquals(errorsMap, response.errors())
        assertEquals(expectedErrorCounts, response.errorCounts())
        assertEquals(throttleTimeMs, response.throttleTimeMs())
    }

    @Test
    override fun testParse() {
        val data = TxnOffsetCommitResponseData()
            .setThrottleTimeMs(throttleTimeMs)
            .setTopics(
                listOf(
                    TxnOffsetCommitResponseTopic().setPartitions(
                        listOf(
                            TxnOffsetCommitResponsePartition()
                                .setPartitionIndex(partitionOne)
                                .setErrorCode(errorOne.code)
                        )
                    ),
                    TxnOffsetCommitResponseTopic().setPartitions(
                        listOf(
                            TxnOffsetCommitResponsePartition()
                                .setPartitionIndex(partitionTwo)
                                .setErrorCode(errorTwo.code),
                        )
                    )
                )
            )
        for (version in ApiKeys.TXN_OFFSET_COMMIT.allVersions()) {
            val response = TxnOffsetCommitResponse.parse(
                buffer = toByteBuffer(data, version),
                version = version
            )
            assertEquals(expectedErrorCounts, response.errorCounts())
            assertEquals(throttleTimeMs, response.throttleTimeMs())
            assertEquals(version >= 1, response.shouldClientThrottle(version))
        }
    }
}
