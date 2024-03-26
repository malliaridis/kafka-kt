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
import org.apache.kafka.trogdor.common.Topology
import org.apache.kafka.trogdor.task.TaskController
import org.apache.kafka.trogdor.task.TaskSpec
import org.apache.kafka.trogdor.task.TaskWorker

/**
 * The specification for a benchmark that creates sustained connections.
 *
 * An example JSON representation which will result in a test that creates 27 connections (9 of each),
 * refreshes them every 10 seconds using 2 threads, running against topic `topic1`, for a duration of 1 hour,
 * and with various other options set:
 *
 * ```json
 * {
 *     "class": "org.apache.kafka.trogdor.workload.SustainedConnectionSpec",
 *     "durationMs": 3600000,
 *     "clientNode": "node0",
 *     "bootstrapServers": "localhost:9092",
 *     "commonClientConf": {
 *         "compression.type": "lz4",
 *         "auto.offset.reset": "earliest",
 *         "linger.ms": "100"
 *     },
 *     "keyGenerator": {
 *         "type": "sequential",
 *         "size": 4,
 *         "startOffset": 0
 *     },
 *     "valueGenerator": {
 *         "type": "uniformRandom",
 *         "size": 512,
 *         "seed": 0,
 *         "padding": 0
 *     },
 *     "producerConnectionCount": 9,
 *     "consumerConnectionCount": 9,
 *     "metadataConnectionCount": 9,
 *     "topicName": "test-topic1-1",
 *     "numThreads": 2,
 *     "refreshRateMs": 10000
 * }
 * ```
 */
class SustainedConnectionSpec @JsonCreator constructor(
    @JsonProperty("startMs") startMs: Long,
    @JsonProperty("durationMs") durationMs: Long,
    @JsonProperty("clientNode") clientNode: String?,
    @JsonProperty("bootstrapServers") bootstrapServers: String?,
    @JsonProperty("producerConf") producerConf: Map<String, String>?,
    @JsonProperty("consumerConf") consumerConf: Map<String, String>?,
    @JsonProperty("adminClientConf") adminClientConf: Map<String, String>?,
    @JsonProperty("commonClientConf") commonClientConf: Map<String, String>?,
    @param:JsonProperty("keyGenerator") private val keyGenerator: PayloadGenerator,
    @param:JsonProperty("valueGenerator") private val valueGenerator: PayloadGenerator,
    @param:JsonProperty("producerConnectionCount") private val producerConnectionCount: Int,
    @param:JsonProperty("consumerConnectionCount") private val consumerConnectionCount: Int,
    @param:JsonProperty("metadataConnectionCount") private val metadataConnectionCount: Int,
    @param:JsonProperty("topicName") private val topicName: String,
    @JsonProperty("numThreads") numThreads: Int,
    @JsonProperty("refreshRateMs") refreshRateMs: Int,
) : TaskSpec(startMs, durationMs) {

    private val clientNode: String = clientNode ?: ""

    private val bootstrapServers: String = bootstrapServers ?: ""

    private val producerConf: Map<String, String> = producerConf ?: emptyMap()

    private val consumerConf: Map<String, String> = consumerConf ?: emptyMap()

    private val adminClientConf: Map<String, String> = adminClientConf ?: emptyMap()

    private val commonClientConf: Map<String, String> = commonClientConf ?: emptyMap()

    private val numThreads: Int = if (numThreads < 1) 1 else numThreads

    private val refreshRateMs: Int = if (refreshRateMs < 1) 1 else refreshRateMs

    @JsonProperty
    fun clientNode(): String = clientNode

    @JsonProperty
    fun bootstrapServers(): String = bootstrapServers

    @JsonProperty
    fun producerConf(): Map<String, String> = producerConf

    @JsonProperty
    fun consumerConf(): Map<String, String> = consumerConf

    @JsonProperty
    fun adminClientConf(): Map<String, String> = adminClientConf

    @JsonProperty
    fun commonClientConf(): Map<String, String> = commonClientConf

    @JsonProperty
    fun keyGenerator(): PayloadGenerator = keyGenerator

    @JsonProperty
    fun valueGenerator(): PayloadGenerator = valueGenerator

    @JsonProperty
    fun producerConnectionCount(): Int = producerConnectionCount

    @JsonProperty
    fun consumerConnectionCount(): Int = consumerConnectionCount

    @JsonProperty
    fun metadataConnectionCount(): Int = metadataConnectionCount

    @JsonProperty
    fun topicName(): String = topicName

    @JsonProperty
    fun numThreads(): Int = numThreads

    @JsonProperty
    fun refreshRateMs(): Int = refreshRateMs

    override fun newController(id: String?): TaskController = TaskController { setOf(clientNode) }

    override fun newTaskWorker(id: String): TaskWorker = SustainedConnectionWorker(id, this)
}
