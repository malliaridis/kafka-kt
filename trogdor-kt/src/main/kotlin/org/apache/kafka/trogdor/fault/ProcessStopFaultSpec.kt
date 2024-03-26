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
class ProcessStopFaultSpec @JsonCreator constructor(
    @JsonProperty("startMs") startMs: Long,
    @JsonProperty("durationMs") durationMs: Long,
    @JsonProperty("nodeNames") nodeNames: List<String>?,
    @JsonProperty("javaProcessName") javaProcessName: String?,
) : TaskSpec(startMs, durationMs) {

    private val nodeNames: Set<String> = nodeNames?.toSet() ?: emptySet()

    private val javaProcessName: String = javaProcessName ?: ""

    @JsonProperty
    fun nodeNames(): Set<String> = nodeNames

    @JsonProperty
    fun javaProcessName(): String = javaProcessName

    override fun newController(id: String?): TaskController = ProcessStopFaultController(nodeNames)

    override fun newTaskWorker(id: String): TaskWorker = ProcessStopFaultWorker(id, javaProcessName)
}
