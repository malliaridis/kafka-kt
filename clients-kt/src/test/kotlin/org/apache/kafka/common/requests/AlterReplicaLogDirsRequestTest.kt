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
import org.apache.kafka.common.errors.LogDirNotFoundException
import org.apache.kafka.common.message.AlterReplicaLogDirsRequestData
import org.apache.kafka.common.message.AlterReplicaLogDirsRequestData.AlterReplicaLogDir
import org.apache.kafka.common.message.AlterReplicaLogDirsRequestData.AlterReplicaLogDirCollection
import org.apache.kafka.common.message.AlterReplicaLogDirsRequestData.AlterReplicaLogDirTopic
import org.apache.kafka.common.message.AlterReplicaLogDirsRequestData.AlterReplicaLogDirTopicCollection
import org.apache.kafka.common.protocol.Errors
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

class AlterReplicaLogDirsRequestTest {

    @Test
    fun testErrorResponse() {
        val data = AlterReplicaLogDirsRequestData().setDirs(
            AlterReplicaLogDirCollection(
                listOf(
                    AlterReplicaLogDir()
                        .setPath("/data0")
                        .setTopics(
                            AlterReplicaLogDirTopicCollection(
                                listOf(
                                    AlterReplicaLogDirTopic()
                                        .setName("topic")
                                        .setPartitions(intArrayOf(0, 1, 2))
                                ).iterator()
                            )
                        )
                ).iterator()
            )
        )
        val errorResponse = AlterReplicaLogDirsRequest.Builder(data).build()
            .getErrorResponse(throttleTimeMs = 123, e = LogDirNotFoundException("/data0"))
        assertEquals(1, errorResponse.data().results.size)
        val topicResponse = errorResponse.data().results[0]
        assertEquals("topic", topicResponse.topicName)
        assertEquals(3, topicResponse.partitions.size)
        for (i in 0..2) {
            assertEquals(i, topicResponse.partitions[i].partitionIndex)
            assertEquals(Errors.LOG_DIR_NOT_FOUND.code, topicResponse.partitions[i].errorCode)
        }
    }

    @Test
    fun testPartitionDir() {
        val data = AlterReplicaLogDirsRequestData().setDirs(
            AlterReplicaLogDirCollection(
                listOf(
                    AlterReplicaLogDir()
                        .setPath("/data0")
                        .setTopics(
                            AlterReplicaLogDirTopicCollection(
                                listOf(
                                    AlterReplicaLogDirTopic()
                                        .setName("topic")
                                        .setPartitions(intArrayOf(0, 1)),
                                    AlterReplicaLogDirTopic()
                                        .setName("topic2")
                                        .setPartitions(intArrayOf(7))
                                ).iterator()
                            )
                        ),
                    AlterReplicaLogDir()
                        .setPath("/data1")
                        .setTopics(
                            AlterReplicaLogDirTopicCollection(
                                listOf(
                                    AlterReplicaLogDirTopic()
                                        .setName("topic3")
                                        .setPartitions(intArrayOf(12))
                                ).iterator()
                            )
                        )
                ).iterator()
            )
        )
        val request = AlterReplicaLogDirsRequest.Builder(data).build()
        val expect = mapOf(
            TopicPartition("topic", 0) to "/data0",
            TopicPartition("topic", 1) to "/data0",
            TopicPartition("topic2", 7) to "/data0",
            TopicPartition("topic3", 12) to "/data1",
        )
        assertEquals(expect, request.partitionDirs())
    }
}
