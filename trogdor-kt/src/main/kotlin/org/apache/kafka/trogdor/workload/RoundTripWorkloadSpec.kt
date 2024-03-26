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
import org.apache.kafka.trogdor.task.TaskController
import org.apache.kafka.trogdor.task.TaskSpec
import org.apache.kafka.trogdor.task.TaskWorker

/**
 * The specification for a workload that sends messages to a broker and then
 * reads them back.
 */
class RoundTripWorkloadSpec @JsonCreator constructor(
    @JsonProperty("startMs") startMs: Long,
    @JsonProperty("durationMs") durationMs: Long,
    @JsonProperty("clientNode") clientNode: String?,
    @JsonProperty("bootstrapServers") bootstrapServers: String?,
    @JsonProperty("commonClientConf") commonClientConf: Map<String, String>?,
    @JsonProperty("adminClientConf") adminClientConf: Map<String, String>?,
    @JsonProperty("consumerConf") consumerConf: Map<String, String>?,
    @JsonProperty("producerConf") producerConf: Map<String, String>?,
    @param:JsonProperty("targetMessagesPerSec") private val targetMessagesPerSec: Int,
    @JsonProperty("valueGenerator") valueGenerator: PayloadGenerator?,
    @JsonProperty("activeTopics") activeTopics: TopicsSpec?,
    @param:JsonProperty("maxMessages") private val maxMessages: Long,
) : TaskSpec(startMs, durationMs) {
    
    private val clientNode: String = clientNode ?: ""
    
    private val bootstrapServers: String = bootstrapServers ?: ""
    
    private val valueGenerator: PayloadGenerator =
        valueGenerator ?: UniformRandomPayloadGenerator(32, 123, 10)
    
    private val activeTopics: TopicsSpec = activeTopics?.immutableCopy() ?: TopicsSpec.EMPTY
    
    private val commonClientConf: Map<String, String> = commonClientConf ?: emptyMap()
    
    private val producerConf: Map<String, String> = producerConf ?: emptyMap()
    
    private val consumerConf: Map<String, String> = consumerConf ?: emptyMap()
    
    private val adminClientConf: Map<String, String> = adminClientConf ?: emptyMap()

    @JsonProperty
    fun clientNode(): String = clientNode

    @JsonProperty
    fun bootstrapServers(): String = bootstrapServers

    @JsonProperty
    fun targetMessagesPerSec(): Int = targetMessagesPerSec

    @JsonProperty
    fun activeTopics(): TopicsSpec = activeTopics

    @JsonProperty
    fun valueGenerator(): PayloadGenerator = valueGenerator

    @JsonProperty
    fun maxMessages(): Long = maxMessages

    @JsonProperty
    fun commonClientConf(): Map<String, String> = commonClientConf

    @JsonProperty
    fun adminClientConf(): Map<String, String> = adminClientConf

    @JsonProperty
    fun producerConf(): Map<String, String> = producerConf

    @JsonProperty
    fun consumerConf(): Map<String, String> = consumerConf

    override fun newController(id: String?): TaskController = TaskController { setOf(clientNode) }

    override fun newTaskWorker(id: String): TaskWorker = RoundTripWorker(id, this)
}
