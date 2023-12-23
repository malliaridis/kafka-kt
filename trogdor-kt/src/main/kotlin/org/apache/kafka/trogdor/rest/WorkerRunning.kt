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

package org.apache.kafka.trogdor.rest

import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.NullNode
import org.apache.kafka.trogdor.task.TaskSpec

/**
 * The state for a task which is being run by the agent.
 *
 * @property startedMs The time on the agent when the task was started.
 */
class WorkerRunning @JsonCreator constructor(
    @JsonProperty("taskId") taskId: String,
    @JsonProperty("spec") spec: TaskSpec,
    @param:JsonProperty("startedMs") private val startedMs: Long,
    @JsonProperty("status") status: JsonNode?,
) : WorkerState(taskId, spec) {

    /**
     * The task status. The format will depend on the type of task that is being run.
     */
    private val status: JsonNode

    init {
        this.status = status ?: NullNode.instance
    }

    @JsonProperty
    override fun startedMs(): Long = startedMs

    @JsonProperty
    override fun status(): JsonNode = status

    override fun running(): Boolean = true
}
