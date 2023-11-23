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
import org.apache.kafka.common.message.AlterReplicaLogDirsRequestData
import org.apache.kafka.common.message.AlterReplicaLogDirsRequestData.AlterReplicaLogDir
import org.apache.kafka.common.message.AlterReplicaLogDirsRequestData.AlterReplicaLogDirTopic
import org.apache.kafka.common.message.AlterReplicaLogDirsResponseData
import org.apache.kafka.common.message.AlterReplicaLogDirsResponseData.AlterReplicaLogDirPartitionResult
import org.apache.kafka.common.message.AlterReplicaLogDirsResponseData.AlterReplicaLogDirTopicResult
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.ByteBufferAccessor
import org.apache.kafka.common.protocol.Errors
import java.nio.ByteBuffer
import java.util.function.Consumer
import java.util.stream.Collectors

class AlterReplicaLogDirsRequest(
    private val data: AlterReplicaLogDirsRequestData,
    version: Short,
) : AbstractRequest(ApiKeys.ALTER_REPLICA_LOG_DIRS, version) {

    override fun data(): AlterReplicaLogDirsRequestData = data

    override fun getErrorResponse(throttleTimeMs: Int, e: Throwable): AlterReplicaLogDirsResponse {
        val data = AlterReplicaLogDirsResponseData()
        data.setResults(this.data.dirs.flatMap { alterDir ->
            alterDir.topics.map { topic ->
                AlterReplicaLogDirTopicResult()
                    .setTopicName(topic.name)
                    .setPartitions(
                        topic.partitions
                            .map { partitionId ->
                                AlterReplicaLogDirPartitionResult()
                                    .setErrorCode(Errors.forException(e).code)
                                    .setPartitionIndex(partitionId)
                            }
                    )
            }
        })

        return AlterReplicaLogDirsResponse(data.setThrottleTimeMs(throttleTimeMs))
    }

    fun partitionDirs(): Map<TopicPartition, String> {
        val result = mutableMapOf<TopicPartition, String>()
        data.dirs.forEach { alterDir: AlterReplicaLogDir ->
            alterDir.topics.forEach { topic: AlterReplicaLogDirTopic ->
                topic.partitions.forEach { partition ->
                    result[TopicPartition(topic.name, partition)] = alterDir.path
                }
            }
        }

        return result
    }

    class Builder(
        private val data: AlterReplicaLogDirsRequestData,
    ) : AbstractRequest.Builder<AlterReplicaLogDirsRequest>(ApiKeys.ALTER_REPLICA_LOG_DIRS) {

        override fun build(version: Short): AlterReplicaLogDirsRequest =
            AlterReplicaLogDirsRequest(data, version)

        override fun toString(): String = data.toString()
    }

    companion object {

        fun parse(buffer: ByteBuffer, version: Short): AlterReplicaLogDirsRequest =
            AlterReplicaLogDirsRequest(
                AlterReplicaLogDirsRequestData(ByteBufferAccessor(buffer), version),
                version,
            )
    }
}
