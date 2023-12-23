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

package org.apache.kafka.trogdor.workload

import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.trogdor.rest.Message

/**
 * Describes some partitions.
 */
class PartitionsSpec @JsonCreator constructor(
    @param:JsonProperty("numPartitions") private val numPartitions: Int,
    @param:JsonProperty("replicationFactor") private val replicationFactor: Short,
    @JsonProperty("partitionAssignments") partitionAssignments: Map<Int?, List<Int?>?>?,
    @JsonProperty("configs") configs: Map<String, String>?,
) : Message() {

    private val partitionAssignments: Map<Int, List<Int>>

    private val configs: Map<String, String>

    init {
        val partMap = mutableMapOf<Int, List<Int>>()
        if (partitionAssignments != null) {
            for ((key, value) in partitionAssignments) {
                partMap[key ?: 0] = value?.map { it ?: 0 } ?: emptyList()
            }
        }
        this.partitionAssignments = partMap.toMap()
        this.configs = configs?.toMap() ?: emptyMap()
    }

    @JsonProperty
    fun numPartitions(): Int = numPartitions

    fun partitionNumbers(): List<Int> {
        return if (partitionAssignments.isEmpty()) {
            val partitionNumbers = ArrayList<Int>()
            val effectiveNumPartitions = if (numPartitions <= 0) DEFAULT_NUM_PARTITIONS.toInt() else numPartitions
            for (i in 0..<effectiveNumPartitions) partitionNumbers.add(i)

            partitionNumbers
        } else partitionAssignments.keys.toList()
    }

    @JsonProperty
    fun replicationFactor(): Short = replicationFactor

    @JsonProperty
    fun partitionAssignments(): Map<Int, List<Int>> = partitionAssignments

    @JsonProperty
    fun configs(): Map<String, String> = configs

    fun newTopic(topicName: String): NewTopic {
        val newTopic = if (partitionAssignments.isEmpty()) {
            val effectiveNumPartitions = if (numPartitions <= 0) DEFAULT_NUM_PARTITIONS.toInt() else numPartitions
            val effectiveReplicationFactor =
                if (replicationFactor <= 0) DEFAULT_REPLICATION_FACTOR
                else replicationFactor

            NewTopic(
                name = topicName,
                numPartitions = effectiveNumPartitions,
                replicationFactor = effectiveReplicationFactor,
            )
        } else NewTopic(
            name = topicName,
            replicasAssignments = partitionAssignments,
        )
        if (configs.isNotEmpty()) newTopic.configs = configs
        return newTopic
    }

    companion object {

        private const val DEFAULT_REPLICATION_FACTOR: Short = 3

        private const val DEFAULT_NUM_PARTITIONS: Short = 1
    }
}
