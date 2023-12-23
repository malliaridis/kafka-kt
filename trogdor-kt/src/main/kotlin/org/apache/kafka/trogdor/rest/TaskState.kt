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

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonTypeInfo
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.NullNode
import org.apache.kafka.trogdor.task.TaskSpec

/**
 * The state which a task is in on the Coordinator.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "state")
@JsonSubTypes(
    JsonSubTypes.Type(value = TaskPending::class, name = TaskStateType.Constants.PENDING_VALUE),
    JsonSubTypes.Type(value = TaskRunning::class, name = TaskStateType.Constants.RUNNING_VALUE),
    JsonSubTypes.Type(value = TaskStopping::class, name = TaskStateType.Constants.STOPPING_VALUE),
    JsonSubTypes.Type(value = TaskDone::class, name = TaskStateType.Constants.DONE_VALUE),
)
abstract class TaskState(
    private val spec: TaskSpec,
    status: JsonNode?,
) : Message() {

    private val status: JsonNode

    init {
        this.status = status ?: NullNode.instance
    }

    @JsonProperty
    fun spec(): TaskSpec = spec

    @JsonProperty
    fun status(): JsonNode = status

    abstract fun stateType(): TaskStateType?
}
