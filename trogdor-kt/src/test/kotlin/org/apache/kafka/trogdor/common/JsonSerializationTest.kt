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

package org.apache.kafka.trogdor.common

import org.apache.kafka.trogdor.fault.FilesUnreadableFaultSpec
import org.apache.kafka.trogdor.fault.Kibosh.KiboshControlFile
import org.apache.kafka.trogdor.fault.NetworkPartitionFaultSpec
import org.apache.kafka.trogdor.fault.ProcessStopFaultSpec
import org.apache.kafka.trogdor.rest.AgentStatusResponse
import org.apache.kafka.trogdor.rest.TasksResponse
import org.apache.kafka.trogdor.rest.WorkerDone
import org.apache.kafka.trogdor.rest.WorkerRunning
import org.apache.kafka.trogdor.rest.WorkerStopping
import org.apache.kafka.trogdor.workload.PartitionsSpec
import org.apache.kafka.trogdor.workload.ProduceBenchSpec
import org.apache.kafka.trogdor.workload.RoundTripWorkloadSpec
import org.apache.kafka.trogdor.workload.TopicsSpec
import org.junit.jupiter.api.Test
import kotlin.test.assertNotNull

class JsonSerializationTest {

    @Test
    @Throws(Exception::class)
    fun testDeserializationDoesNotProduceNulls() {
        verify(
            FilesUnreadableFaultSpec(
                startMs = 0,
                durationMs = 0,
                nodeNames = null,
                mountPath = null,
                prefix = null,
                errorCode = 0,
            )
        )
        verify(KiboshControlFile(faults = null))
        verify(NetworkPartitionFaultSpec(startMs = 0, durationMs = 0, partitions = null))
        verify(ProcessStopFaultSpec(startMs = 0, durationMs = 0, nodeNames = null, javaProcessName = null))
        verify(AgentStatusResponse(serverStartMs = 0, workers = null))
        verify(TasksResponse(tasks = null))
        verify(WorkerDone(taskId = null, spec = null, startedMs = 0, doneMs = 0, status = null, error = null))
        verify(WorkerRunning(taskId = null, spec = null, startedMs = 0, status = null))
        verify(WorkerStopping(taskId = null, spec = null, startedMs = 0, status = null))
        verify(
            ProduceBenchSpec(
                startMs = 0,
                durationMs = 0,
                producerNode = null,
                bootstrapServers = null,
                targetMessagesPerSec = 0,
                maxMessages = 0,
                keyGenerator = null,
                valueGenerator = null,
                txGenerator = null,
                producerConf = null,
                commonClientConf = null,
                adminClientConf = null,
                activeTopics = null,
                inactiveTopics = null,
                useConfiguredPartitioner = false,
                skipFlush = false,
            )
        )
        verify(
            RoundTripWorkloadSpec(
                startMs = 0,
                durationMs = 0,
                clientNode = null,
                bootstrapServers = null,
                commonClientConf = null,
                adminClientConf = null,
                consumerConf = null,
                producerConf = null,
                targetMessagesPerSec = 0,
                valueGenerator = null,
                activeTopics = null,
                maxMessages = 0,
            )
        )
        verify(TopicsSpec())
        verify(
            PartitionsSpec(
                numPartitions = 0,
                replicationFactor = 0.toShort(),
                partitionAssignments = null,
                configs = null,
            )
        )
        val partitionAssignments = mapOf<Int?, List<Int?>?>(
            0 to listOf(1, 2, 3),
            1 to listOf(1, 2, 3),
        )
        verify(PartitionsSpec(0, 0.toShort(), partitionAssignments, null))
        verify(PartitionsSpec(0, 0.toShort(), null, null))
    }

    @Throws(Exception::class)
    private fun <T : Any> verify(value: T) {
        val bytes = JsonUtil.JSON_SERDE.writeValueAsBytes(value)
        val clazz = value::class.java as Class<T>
        val value2 = JsonUtil.JSON_SERDE.readValue(bytes, clazz)
        for (field in clazz.getDeclaredFields()) {
            val wasAccessible = field.isAccessible
            field.setAccessible(true)
            assertNotNull(field[value2], "Field $field was null.")
            field.setAccessible(wasAccessible)
        }
    }
}
