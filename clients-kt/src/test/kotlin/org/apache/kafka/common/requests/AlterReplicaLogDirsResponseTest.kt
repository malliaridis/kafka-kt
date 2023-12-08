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

import org.apache.kafka.common.message.AlterReplicaLogDirsResponseData
import org.apache.kafka.common.message.AlterReplicaLogDirsResponseData.AlterReplicaLogDirPartitionResult
import org.apache.kafka.common.message.AlterReplicaLogDirsResponseData.AlterReplicaLogDirTopicResult
import org.apache.kafka.common.protocol.Errors
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

class AlterReplicaLogDirsResponseTest {

    @Test
    fun testErrorCounts() {
        val data = AlterReplicaLogDirsResponseData().setResults(
            listOf(
                AlterReplicaLogDirTopicResult()
                    .setTopicName("t0")
                    .setPartitions(
                        listOf(
                            AlterReplicaLogDirPartitionResult()
                                .setPartitionIndex(0)
                                .setErrorCode(Errors.LOG_DIR_NOT_FOUND.code),
                            AlterReplicaLogDirPartitionResult()
                                .setPartitionIndex(1)
                                .setErrorCode(Errors.NONE.code),
                        )
                    ),
                AlterReplicaLogDirTopicResult()
                    .setTopicName("t1")
                    .setPartitions(
                        listOf(
                            AlterReplicaLogDirPartitionResult()
                                .setPartitionIndex(0)
                                .setErrorCode(Errors.LOG_DIR_NOT_FOUND.code)
                        )
                    )
            )
        )
        val counts = AlterReplicaLogDirsResponse(data).errorCounts()
        assertEquals(2, counts.size)
        assertEquals(2, counts[Errors.LOG_DIR_NOT_FOUND])
        assertEquals(1, counts[Errors.NONE])
    }
}
