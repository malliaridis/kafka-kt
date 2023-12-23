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

import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonProperty

class SampleTaskSpec @JsonCreator constructor(
    @JsonProperty("startMs") startMs: Long,
    @JsonProperty("durationMs") durationMs: Long,
    @JsonProperty("nodeToExitMs") nodeToExitMs: Map<String, Long>?,
    @JsonProperty("error") error: String?,
) : TaskSpec(startMs, durationMs) {

    private val nodeToExitMs: Map<String, Long> = nodeToExitMs?.toMap() ?: emptyMap()

    private val error: String = error ?: ""

    @JsonProperty
    fun nodeToExitMs(): Map<String, Long> = nodeToExitMs

    @JsonProperty
    fun error(): String = error

    override fun newController(id: String?): TaskController = SampleTaskController()

    override fun newTaskWorker(id: String): TaskWorker = SampleTaskWorker(this)
}
