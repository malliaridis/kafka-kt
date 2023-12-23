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
import org.apache.kafka.trogdor.task.TaskSpec

/**
 * The state a task is in once it's done.
 */
/**
 * @property startedMs The time on the coordinator when the task was started.
 * @property doneMs The time on the coordinator when the task was completed.
 * @property error Empty if the task completed without error; the error message otherwise.
 * @property cancelled `true` if the task was manually cancelled, rather than terminating itself.
 */
class TaskDone @JsonCreator constructor(
    @JsonProperty("spec") spec: TaskSpec,
    @param:JsonProperty("startedMs") private val startedMs: Long,
    @param:JsonProperty("doneMs") private val doneMs: Long,
    @param:JsonProperty("error") private val error: String?,
    @param:JsonProperty("cancelled") private val cancelled: Boolean,
    @JsonProperty("status") status: JsonNode?,
) : TaskState(spec, status) {

    @JsonProperty
    fun startedMs(): Long = startedMs

    @JsonProperty
    fun doneMs(): Long = doneMs

    @JsonProperty
    fun error(): String? = error

    @JsonProperty
    fun cancelled(): Boolean = cancelled

    override fun stateType(): TaskStateType = TaskStateType.DONE
}
