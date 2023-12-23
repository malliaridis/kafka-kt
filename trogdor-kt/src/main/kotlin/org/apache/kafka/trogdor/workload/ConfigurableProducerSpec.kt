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
import java.util.Optional
import org.apache.kafka.trogdor.common.Topology
import org.apache.kafka.trogdor.task.TaskController
import org.apache.kafka.trogdor.task.TaskSpec
import org.apache.kafka.trogdor.task.TaskWorker

/**
 * This is the spec to pass in to be able to run the `ConfigurableProducerWorker` workload. This allows
 * for customized and even variable configurations in terms of messages per second, message size, batch size,
 * key size, and even the ability to target a specific partition out of a topic.
 *
 * This has several notable differences from the ProduceBench classes, namely the ability to dynamically control
 * flushing and throughput through configurable classes, but also the ability to run against specific partitions within
 * a topic directly. This workload can only run against one topic at a time, unlike the ProduceBench workload.
 *
 * The parameters that differ from ProduceBenchSpec:
 *
 * - `flushGenerator` - Used to instruct the KafkaProducer when to issue flushes. This allows us to simulate
 * variable batching since batch flushing is not currently exposed within the KafkaProducer
 * class. See the `FlushGenerator` interface for more information.
 *
 * - `throughputGenerator` - Used to throttle the ConfigurableProducerWorker based on a calculated number of messages
 * within a window. See the `ThroughputGenerator` interface for more information.
 *
 * - `activeTopic` - This class only supports execution against a single topic at a time. If more than one
 * topic is specified, the ConfigurableProducerWorker will throw an error.
 *
 * - `activePartition` - Specify a specific partition number within the activeTopic to run load against, or
 * specify `-1` to allow use of all partitions.
 *
 * Here is an example spec:
 *
 * ```json
 * {
 *     "startMs": 1606949497662,
 *     "durationMs": 3600000,
 *     "producerNode": "trogdor-agent-0",
 *     "bootstrapServers": "some.example.kafka.server:9091",
 *     "flushGenerator": {
 *         "type": "gaussian",
 *         "messagesPerFlushAverage": 16,
 *         "messagesPerFlushDeviation": 4
 *     },
 *     "throughputGenerator": {
 *         "type": "gaussian",
 *         "messagesPerSecondAverage": 500,
 *         "messagesPerSecondDeviation": 50,
 *         "windowsUntilRateChange": 100,
 *         "windowSizeMs": 100
 *     },
 *     "keyGenerator": {
 *       "type": "constant",
 *       "size": 8
 *     },
 *     "valueGenerator": {
 *         "type": "gaussianTimestampRandom",
 *         "messageSizeAverage": 512,
 *         "messageSizeDeviation": 100,
 *         "messagesUntilSizeChange": 100
 *     },
 *     "producerConf": {
 *         "acks": "all"
 *     },
 *     "commonClientConf": {},
 *     "adminClientConf": {},
 *     "activeTopic": {
 *         "topic0": {
 *             "numPartitions": 100,
 *             "replicationFactor": 3,
 *             "configs": {
 *                 "retention.ms": "1800000"
 *             }
 *         }
 *     },
 *     "activePartition": 5
 * }
 *```
 *
 * This example spec performed the following:
 *
 * - Ran on `trogdor-agent-0` for 1 hour starting at 2020-12-02 22:51:37.662 GMT
 * - Produced with acks=all to Partition 5 of `topic0` on kafka server `some.example.kafka.server:9091`.
 * - The average batch had 16 messages, with a standard deviation of 4 messages.
 * - The messages had 8-byte constant keys with an average size of 512 bytes and a standard deviation of 100 bytes.
 * - The messages had millisecond timestamps embedded in the first several bytes of the value.
 * - The average throughput was 500 messages/second, with a window of 100ms and a deviation of 50 messages/second.
 */
class ConfigurableProducerSpec @JsonCreator constructor(
    @JsonProperty("startMs") startMs: Long,
    @JsonProperty("durationMs") durationMs: Long,
    @JsonProperty("producerNode") producerNode: String?,
    @JsonProperty("bootstrapServers") bootstrapServers: String?,
    @param:JsonProperty("flushGenerator") private val flushGenerator: FlushGenerator?,
    @param:JsonProperty("throughputGenerator") private val throughputGenerator: ThroughputGenerator,
    @param:JsonProperty("keyGenerator") private val keyGenerator: PayloadGenerator,
    @param:JsonProperty("valueGenerator") private val valueGenerator: PayloadGenerator,
    @JsonProperty("producerConf") producerConf: Map<String, String>?,
    @JsonProperty("commonClientConf") commonClientConf: Map<String, String>?,
    @JsonProperty("adminClientConf") adminClientConf: Map<String, String>?,
    @JsonProperty("activeTopic") activeTopic: TopicsSpec,
    @JsonProperty("activePartition") activePartition: Int,
) : TaskSpec(startMs, durationMs) {

    private val producerNode: String

    private val bootstrapServers: String

    private val producerConf: Map<String, String>

    private val adminClientConf: Map<String, String>

    private val commonClientConf: Map<String, String>

    private val activeTopic: TopicsSpec

    private val activePartition: Int

    init {
        this.producerNode = producerNode ?: ""
        this.bootstrapServers = bootstrapServers ?: ""
        this.producerConf = producerConf ?: emptyMap()
        this.commonClientConf = commonClientConf ?: emptyMap()
        this.adminClientConf = adminClientConf ?: emptyMap()
        this.activeTopic = activeTopic.immutableCopy()
        this.activePartition = activePartition
    }

    @JsonProperty
    fun producerNode(): String = producerNode

    @JsonProperty
    fun bootstrapServers(): String = bootstrapServers

    @JsonProperty
    fun flushGenerator(): FlushGenerator? = flushGenerator

    @JsonProperty
    fun keyGenerator(): PayloadGenerator = keyGenerator

    @JsonProperty
    fun valueGenerator(): PayloadGenerator = valueGenerator

    @JsonProperty
    fun throughputGenerator(): ThroughputGenerator = throughputGenerator

    @JsonProperty
    fun producerConf(): Map<String, String> = producerConf

    @JsonProperty
    fun commonClientConf(): Map<String, String> = commonClientConf

    @JsonProperty
    fun adminClientConf(): Map<String, String> = adminClientConf

    @JsonProperty
    fun activeTopic(): TopicsSpec = activeTopic

    @JsonProperty
    fun activePartition(): Int = activePartition

    override fun newController(id: String?): TaskController = TaskController { setOf(producerNode) }

    override fun newTaskWorker(id: String): TaskWorker = ConfigurableProducerWorker(id, this)
}
