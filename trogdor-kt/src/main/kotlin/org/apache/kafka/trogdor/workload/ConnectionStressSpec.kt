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
import java.util.TreeSet
import org.apache.kafka.trogdor.task.TaskController
import org.apache.kafka.trogdor.task.TaskSpec
import org.apache.kafka.trogdor.task.TaskWorker

/**
 * The specification for a task which connects and disconnects many times a second to stress the broker.
 */
class ConnectionStressSpec @JsonCreator constructor(
    @JsonProperty("startMs") startMs: Long,
    @JsonProperty("durationMs") durationMs: Long,
    @JsonProperty("clientNode") clientNodes: List<String>?,
    @JsonProperty("bootstrapServers") bootstrapServers: String?,
    @JsonProperty("commonClientConf") commonClientConf: Map<String, String>?,
    @JsonProperty("targetConnectionsPerSec") targetConnectionsPerSec: Int,
    @JsonProperty("numThreads") numThreads: Int,
    @JsonProperty("action") action: ConnectionStressAction?,
) : TaskSpec(startMs, durationMs) {

    private val clientNodes: List<String>

    private val bootstrapServers: String

    private val commonClientConf: Map<String, String>

    private val targetConnectionsPerSec: Int

    private val numThreads: Int

    private val action: ConnectionStressAction

    init {
        this.clientNodes = clientNodes?.toList() ?: emptyList()
        this.bootstrapServers = bootstrapServers ?: ""
        this.commonClientConf = commonClientConf ?: emptyMap()
        this.targetConnectionsPerSec = targetConnectionsPerSec
        this.numThreads = if (numThreads < 1) 1 else numThreads
        this.action = action ?: ConnectionStressAction.CONNECT
    }

    @JsonProperty
    fun clientNode(): List<String> = clientNodes

    @JsonProperty
    fun bootstrapServers(): String = bootstrapServers

    @JsonProperty
    fun commonClientConf(): Map<String, String> = commonClientConf

    @JsonProperty
    fun targetConnectionsPerSec(): Int = targetConnectionsPerSec

    @JsonProperty
    fun numThreads(): Int = numThreads

    @JsonProperty
    fun action(): ConnectionStressAction = action

    override fun newController(id: String?): TaskController = TaskController { TreeSet(clientNodes) }

    override fun newTaskWorker(id: String): TaskWorker = ConnectionStressWorker(id, this)

    enum class ConnectionStressAction {
        CONNECT,
        FETCH_METADATA
    }
}
