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
import java.util.Collections
import java.util.Optional
import kotlin.math.max

/**
 * The request to /coordinator/tasks
 */
class TasksRequest @JsonCreator constructor(
    @JsonProperty("taskIds") taskIds: Collection<String>?,
    @JsonProperty("firstStartMs") firstStartMs: Long,
    @JsonProperty("lastStartMs") lastStartMs: Long,
    @JsonProperty("firstEndMs") firstEndMs: Long,
    @JsonProperty("lastEndMs") lastEndMs: Long,
    @JsonProperty("state") state: TaskStateType?,
) : Message() {

    /**
     * The task IDs to list.
     * An empty set of task IDs indicates that we should list all task IDs.
     */
    private val taskIds: Set<String>

    /**
     * If this is non-zero, only tasks with a startMs at or after this time will be listed.
     */
    private val firstStartMs: Long

    /**
     * If this is non-zero, only tasks with a startMs at or before this time will be listed.
     */
    private val lastStartMs: Long

    /**
     * If this is non-zero, only tasks with an endMs at or after this time will be listed.
     */
    private val firstEndMs: Long

    /**
     * If this is non-zero, only tasks with an endMs at or before this time will be listed.
     */
    private val lastEndMs: Long

    /**
     * The desired state of the tasks.
     * An empty string will match all states.
     */
    private val state: TaskStateType?

    init {
        this.taskIds = taskIds?.toSet() ?: emptySet()
        this.firstStartMs = firstStartMs.coerceAtLeast(0)
        this.lastStartMs = lastStartMs.coerceAtLeast(0)
        this.firstEndMs = firstEndMs.coerceAtLeast(0)
        this.lastEndMs = lastEndMs.coerceAtLeast(0)
        this.state = state
    }

    @JsonProperty
    fun taskIds(): Collection<String> = taskIds

    @JsonProperty
    fun firstStartMs(): Long = firstStartMs

    @JsonProperty
    fun lastStartMs(): Long = lastStartMs

    @JsonProperty
    fun firstEndMs(): Long = firstEndMs

    @JsonProperty
    fun lastEndMs(): Long = lastEndMs

    @JsonProperty
    fun state(): TaskStateType? = state

    /**
     * Determine if this TaskRequest should return a particular task.
     *
     * @param taskId The task ID.
     * @param startMs The task start time, or -1 if the task hasn't started.
     * @param endMs The task end time, or -1 if the task hasn't ended.
     * @return `true` if information about the task should be returned.
     */
    fun matches(taskId: String, startMs: Long, endMs: Long, state: TaskStateType?): Boolean {
        if (taskIds.isNotEmpty() && !taskIds.contains(taskId)) return false
        if (firstStartMs > 0 && startMs < firstStartMs) return false
        if (lastStartMs > 0 && (startMs < 0 || startMs > lastStartMs)) return false
        if (firstEndMs > 0 && endMs < firstEndMs) return false
        if (lastEndMs > 0 && (endMs < 0 || endMs > lastEndMs)) return false
        return this.state == null || this.state == state
    }
}
