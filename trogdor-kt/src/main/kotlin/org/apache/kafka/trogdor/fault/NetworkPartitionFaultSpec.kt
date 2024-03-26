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

package org.apache.kafka.trogdor.fault

import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.kafka.trogdor.task.TaskController
import org.apache.kafka.trogdor.task.TaskSpec
import org.apache.kafka.trogdor.task.TaskWorker

/**
 * The specification for a fault that creates a network partition.
 */
class NetworkPartitionFaultSpec @JsonCreator constructor(
    @JsonProperty("startMs") startMs: Long,
    @JsonProperty("durationMs") durationMs: Long,
    @JsonProperty("partitions") partitions: List<List<String>>?,
) : TaskSpec(startMs, durationMs) {

    private val partitions: List<List<String>> = partitions ?: emptyList()

    @JsonProperty
    fun partitions(): List<List<String>> = partitions

    override fun newController(id: String?): TaskController = NetworkPartitionFaultController(partitionSets())

    override fun newTaskWorker(id: String): TaskWorker = NetworkPartitionFaultWorker(id, partitionSets())

    private fun partitionSets(): List<Set<String>> {
        val partitionSets = mutableListOf<Set<String>>()
        val prevNodes = mutableSetOf<String>()
        for (partition in partitions()) {
            for (nodeName in partition) {
                if (prevNodes.contains(nodeName)) {
                    throw RuntimeException("Node $nodeName appears in more than one partition.")
                }
                prevNodes.add(nodeName)
                partitionSets.add(partition.toSet())
            }
        }
        return partitionSets
    }
}
