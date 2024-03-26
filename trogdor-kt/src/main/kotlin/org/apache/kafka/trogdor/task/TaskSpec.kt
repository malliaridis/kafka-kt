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

package org.apache.kafka.trogdor.task

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.annotation.JsonTypeInfo
import org.apache.kafka.trogdor.common.JsonUtil.toJsonString

/**
 * The specification for a task. This should be immutable and suitable for serializing and sending over the wire.
 *
 * @property startMs When the time should start in milliseconds.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "class")
abstract class TaskSpec protected constructor(
    @param:JsonProperty("startMs") private val startMs: Long,
    @JsonProperty("durationMs") durationMs: Long,
) {

    /**
     * How long the task should run in milliseconds.
     */
    private val durationMs: Long

    init {
        this.durationMs = durationMs.coerceAtMost(MAX_TASK_DURATION_MS)
            .coerceAtLeast(0)
    }

    /**
     * Get the target start time of this task in ms.
     */
    @JsonProperty
    fun startMs(): Long = startMs

    /**
     * Get the deadline time of this task in ms
     */
    fun endMs(): Long = startMs + durationMs

    /**
     * Get the duration of this task in ms.
     */
    @JsonProperty
    fun durationMs(): Long = durationMs

    /**
     * Hydrate this task on the coordinator.
     *
     * @param id The task id.
     */
    abstract fun newController(id: String?): TaskController

    /**
     * Hydrate this task on the agent.
     *
     * @param id The worker id.
     */
    abstract fun newTaskWorker(id: String): TaskWorker

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        return if (other == null || javaClass != other.javaClass) false else toString() == other.toString()
    }

    override fun hashCode(): Int = toString().hashCode()

    override fun toString(): String = toJsonString(this)

    protected fun configOrEmptyMap(
        config: Map<String, String>?,
    ): Map<String, String> = config ?: emptyMap()

    companion object {

        /**
         * The maximum task duration.
         *
         * We cap the task duration at this value to avoid worrying about 64-bit overflow or floating
         * point rounding.  (Objects serialized as JSON canonically contain only floating point numbers,
         * because JavaScript did not support integers.)
         */
        const val MAX_TASK_DURATION_MS = 1000000000000000L
    }
}
