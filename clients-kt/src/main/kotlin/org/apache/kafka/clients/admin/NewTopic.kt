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

package org.apache.kafka.clients.admin

import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableReplicaAssignment
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableTopic
import org.apache.kafka.common.message.CreateTopicsRequestData.CreateableTopicConfig
import org.apache.kafka.common.requests.CreateTopicsRequest

/**
 * A new topic to be created via [Admin.createTopics]. The new topic optionally defaults
 * [numPartitions] and [replicationFactor] to the broker configurations for `num.partitions` and
 * `default.replication.factor` respectively.
 *
 * @property name The topic name.
 * @param replicasAssignments a map from partition id to replica ids (i.e. broker ids). Although
 * not enforced, it is generally a good idea for all partitions to have the same number of replicas.
 */
data class NewTopic(
    val name: String,
    val numPartitions: Int = CreateTopicsRequest.NO_NUM_PARTITIONS,
    val replicationFactor: Short = CreateTopicsRequest.NO_REPLICATION_FACTOR,
    val replicasAssignments: Map<Int, List<Int>> = emptyMap(),
    var configs: Map<String, String> = emptyMap()
) {

    /**
     * The name of the topic to be created.
     */
    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("name"),
    )
    fun name(): String {
        return name
    }

    /**
     * The number of partitions for the new topic or -1 if a replica assignment has been specified.
     */
    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("numPartitions"),
    )
    fun numPartitions(): Int {
        return numPartitions
    }

    /**
     * The replication factor for the new topic or -1 if a replica assignment has been specified.
     */
    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("replicationFactor"),
    )
    fun replicationFactor(): Short {
        return replicationFactor
    }

    /**
     * A map from partition id to replica ids (i.e. broker ids) or null if the number of partitions and replication
     * factor have been specified instead.
     */
    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("replicasAssignments"),
    )
    fun replicasAssignments(): Map<Int, List<Int>> {
        return replicasAssignments
    }

    /**
     * Set the configuration to use on the new topic.
     *
     * @param configs               The configuration map.
     * @return                      This NewTopic object.
     */
    @Deprecated(message = "Use property instead")
    fun configs(configs: Map<String, String>): NewTopic {
        this.configs = configs
        return this
    }

    /**
     * The configuration for the new topic or null if no configs ever specified.
     */
    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("configs"),
    )
    fun configs(): Map<String, String> {
        return configs
    }

    fun convertToCreatableTopic(): CreatableTopic {
        val creatableTopic = CreatableTopic().setName(name)
            .setNumPartitions(numPartitions)
            .setReplicationFactor(replicationFactor)
        replicasAssignments.forEach { (key, value) ->
            creatableTopic.assignments.add(
                CreatableReplicaAssignment()
                    .setPartitionIndex(key)
                    .setBrokerIds(value.toIntArray())
            )
        }

        configs.forEach { (key, value) ->
            creatableTopic.configs.add(CreateableTopicConfig().setName(key).setValue(value))
        }
        return creatableTopic
    }

    override fun toString(): String {
        return "(name=$name" +
                ", numPartitions=${
                    if (numPartitions != CreateTopicsRequest.NO_NUM_PARTITIONS) numPartitions
                    else "default"
                }" +
                ", replicationFactor=${
                    if (replicationFactor != CreateTopicsRequest.NO_REPLICATION_FACTOR) replicationFactor
                    else "default"
                }" +
                ", replicasAssignments=$replicasAssignments" +
                ", configs=$configs" +
                ")"
    }
}
