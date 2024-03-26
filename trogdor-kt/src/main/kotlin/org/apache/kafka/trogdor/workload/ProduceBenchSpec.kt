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
 * The specification for a benchmark that produces messages to a set of topics.
 *
 * To configure a transactional producer, a #[TransactionGenerator] must be passed in.
 * Said generator works in lockstep with the producer by instructing it what action to take next to regards to
 * a transaction.
 *
 * An example JSON representation which will result in a producer that creates three topics (foo1, foo2, foo3)
 * with three partitions each and produces to them:
 *
 * ```json
 * {
 *     "class": "org.apache.kafka.trogdor.workload.ProduceBenchSpec",
 *     "durationMs": 10000000,
 *     "producerNode": "node0",
 *     "bootstrapServers": "localhost:9092",
 *     "targetMessagesPerSec": 10,
 *     "maxMessages": 100,
 *     "activeTopics": {
 *         "foo[1-3]": {
 *             "numPartitions": 3,
 *             "replicationFactor": 1
 *         }
 *     },
 *     "inactiveTopics": {
 *         "foo[4-5]": {
 *              "numPartitions": 3,
 *              "replicationFactor": 1
 *          }
 *     }
 * }
 * ```
 */
class ProduceBenchSpec @JsonCreator constructor(
    @JsonProperty("startMs") startMs: Long,
    @JsonProperty("durationMs") durationMs: Long,
    @JsonProperty("producerNode") producerNode: String?,
    @JsonProperty("bootstrapServers") bootstrapServers: String?,
    @param:JsonProperty("targetMessagesPerSec") private val targetMessagesPerSec: Int,
    @param:JsonProperty("maxMessages") private val maxMessages: Long,
    @JsonProperty("keyGenerator") keyGenerator: PayloadGenerator?,
    @JsonProperty("valueGenerator") valueGenerator: PayloadGenerator?,
    @JsonProperty("transactionGenerator") txGenerator: TransactionGenerator?,
    @JsonProperty("producerConf") producerConf: Map<String, String>?,
    @JsonProperty("commonClientConf") commonClientConf: Map<String, String>?,
    @JsonProperty("adminClientConf") adminClientConf: Map<String, String>?,
    @JsonProperty("activeTopics") activeTopics: TopicsSpec?,
    @JsonProperty("inactiveTopics") inactiveTopics: TopicsSpec?,
    @param:JsonProperty("useConfiguredPartitioner") private val useConfiguredPartitioner: Boolean,
    @param:JsonProperty("skipFlush") private val skipFlush: Boolean,
) : TaskSpec(startMs, durationMs) {

    private val producerNode: String = producerNode ?: ""

    private val bootstrapServers: String = bootstrapServers ?: ""

    private val keyGenerator: PayloadGenerator = keyGenerator ?: SequentialPayloadGenerator(4, 0)

    private val valueGenerator: PayloadGenerator =
        valueGenerator ?: ConstantPayloadGenerator(512, ByteArray(0))

    private val transactionGenerator: TransactionGenerator? = txGenerator

    private val producerConf: Map<String, String> = producerConf ?: emptyMap()

    private val adminClientConf: Map<String, String> = adminClientConf ?: emptyMap()

    private val commonClientConf: Map<String, String> = commonClientConf ?: emptyMap()

    private val activeTopics: TopicsSpec = activeTopics?.immutableCopy() ?: TopicsSpec.EMPTY

    private val inactiveTopics: TopicsSpec = inactiveTopics?.immutableCopy() ?: TopicsSpec.EMPTY

    @JsonProperty
    fun producerNode(): String = producerNode

    @JsonProperty
    fun bootstrapServers(): String = bootstrapServers

    @JsonProperty
    fun targetMessagesPerSec(): Int = targetMessagesPerSec

    @JsonProperty
    fun maxMessages(): Long = maxMessages

    @JsonProperty
    fun keyGenerator(): PayloadGenerator = keyGenerator

    @JsonProperty
    fun valueGenerator(): PayloadGenerator = valueGenerator

    @JsonProperty
    fun transactionGenerator(): TransactionGenerator? = transactionGenerator

    @JsonProperty
    fun producerConf(): Map<String, String> = producerConf

    @JsonProperty
    fun commonClientConf(): Map<String, String> = commonClientConf

    @JsonProperty
    fun adminClientConf(): Map<String, String> = adminClientConf

    @JsonProperty
    fun activeTopics(): TopicsSpec = activeTopics

    @JsonProperty
    fun inactiveTopics(): TopicsSpec = inactiveTopics

    @JsonProperty
    fun useConfiguredPartitioner(): Boolean = useConfiguredPartitioner

    @JsonProperty
    fun skipFlush(): Boolean = skipFlush

    override fun newController(id: String?): TaskController = TaskController { setOf(producerNode) }

    override fun newTaskWorker(id: String): TaskWorker = ProduceBenchWorker(id, this)
}
