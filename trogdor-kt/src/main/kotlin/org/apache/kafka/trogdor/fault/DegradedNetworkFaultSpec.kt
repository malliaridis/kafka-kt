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

class DegradedNetworkFaultSpec @JsonCreator constructor(
    @JsonProperty("startMs") startMs: Long,
    @JsonProperty("durationMs") durationMs: Long,
    @JsonProperty("nodeSpecs") nodeSpecs: Map<String, NodeDegradeSpec>?,
) : TaskSpec(startMs, durationMs) {

    private val nodeSpecs: Map<String, NodeDegradeSpec> = nodeSpecs?.toMap() ?: emptyMap()

    override fun newController(id: String?): TaskController = TaskController { nodeSpecs.keys }

    override fun newTaskWorker(id: String): TaskWorker = DegradedNetworkFaultWorker(id, nodeSpecs)

    @JsonProperty("nodeSpecs")
    fun nodeSpecs(): Map<String, NodeDegradeSpec> = nodeSpecs

    class NodeDegradeSpec(
        @JsonProperty("networkDevice") networkDevice: String?,
        @JsonProperty("latencyMs") latencyMs: Int?,
        @JsonProperty("rateLimitKbit") rateLimitKbit: Int?,
    ) {

        private val networkDevice: String = networkDevice ?: ""
        private val latencyMs: Int = latencyMs ?: 0
        private val rateLimitKbit: Int = rateLimitKbit ?: 0

        @JsonProperty("networkDevice")
        fun networkDevice(): String = networkDevice

        @JsonProperty("latencyMs")
        fun latencyMs(): Int = latencyMs

        @JsonProperty("rateLimitKbit")
        fun rateLimitKbit(): Int = rateLimitKbit

        override fun toString(): String =
            "NodeDegradeSpec{networkDevice='$networkDevice', latencyMs=$latencyMs, rateLimitKbit=$rateLimitKbit}"
    }
}
