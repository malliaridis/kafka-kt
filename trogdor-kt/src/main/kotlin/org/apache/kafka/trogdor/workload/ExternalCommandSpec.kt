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
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.NullNode
import org.apache.kafka.trogdor.task.TaskController
import org.apache.kafka.trogdor.task.TaskSpec
import org.apache.kafka.trogdor.task.TaskWorker

/**
 * ExternalCommandSpec describes a task that executes Trogdor tasks with the command.
 *
 * An example uses the python runner to execute the ProduceBenchSpec task.
 *
 * ```json
 * {
 *     "class": "org.apache.kafka.trogdor.workload.ExternalCommandSpec",
 *     "command": ["python", "/path/to/trogdor/python/runner"],
 *     "durationMs": 10000000,
 *     "producerNode": "node0",
 *     "workload": {
 *         "class": "org.apache.kafka.trogdor.workload.ProduceBenchSpec",
 *         "bootstrapServers": "localhost:9092",
 *         "targetMessagesPerSec": 10,
 *         "maxMessages": 100,
 *         "activeTopics": {
 *             "foo[1-3]": {
 *                 "numPartitions": 3,
 *                 "replicationFactor": 1
 *             }
 *         },
 *         "inactiveTopics": {
 *             "foo[4-5]": {
 *                 "numPartitions": 3,
 *                 "replicationFactor": 1
 *             }
 *         }
 *     }
 * }
 * ```
 */
class ExternalCommandSpec @JsonCreator constructor(
    @JsonProperty("startMs") startMs: Long,
    @JsonProperty("durationMs") durationMs: Long,
    @JsonProperty("commandNode") commandNode: String?,
    @JsonProperty("command") command: List<String>?,
    @JsonProperty("workload") workload: JsonNode?,
    @param:JsonProperty("shutdownGracePeriodMs") private val shutdownGracePeriodMs: Int?,
) : TaskSpec(startMs, durationMs) {

    private val commandNode: String

    private val command: List<String>

    private val workload: JsonNode

    init {
        this.commandNode = commandNode ?: ""
        this.command = command ?: emptyList()
        this.workload = workload ?: NullNode.instance
    }

    @JsonProperty
    fun commandNode(): String = commandNode

    @JsonProperty
    fun command(): List<String> = command

    @JsonProperty
    fun workload(): JsonNode = workload

    @JsonProperty
    fun shutdownGracePeriodMs(): Int? = shutdownGracePeriodMs

    override fun newController(id: String?): TaskController = TaskController { setOf(commandNode) }

    override fun newTaskWorker(id: String): TaskWorker = ExternalCommandWorker(id, this)
}
