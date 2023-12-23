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
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.trogdor.common.StringExpander
import org.apache.kafka.trogdor.common.StringExpander.expand
import org.apache.kafka.trogdor.task.TaskController
import org.apache.kafka.trogdor.task.TaskSpec
import org.apache.kafka.trogdor.task.TaskWorker

/**
 * The specification for a benchmark that consumer messages from a set of topic/partitions.
 *
 * If a consumer group is not given to the specification, a random one will be generated and used to track
 * offsets/subscribe to topics.
 *
 * This specification uses a specific way to represent a topic partition via its "activeTopics" field.
 * The notation for that is topic_name:partition_number (e.g "foo:1" represents partition-1 of topic "foo")
 * Note that a topic name cannot have more than one colon.
 *
 * The "activeTopics" field also supports ranges that get expanded. See #[StringExpander].
 *
 * There now exists a clever and succinct way to represent multiple partitions of multiple topics.
 * Example:
 * Given "activeTopics": ["foo[1-3]:[1-3]"], "foo[1-3]:[1-3]" will get
 * expanded to [foo1:1, foo1:2, foo1:3, foo2:1, ..., foo3:3].
 * This represents all partitions 1-3 for the three topics foo1, foo2 and foo3.
 *
 * If there is at least one topic:partition pair, the consumer will be manually assigned partitions via
 * [org.apache.kafka.clients.consumer.KafkaConsumer.assign].
 * Note that in this case the consumer will fetch and assign all partitions for a topic if no partition is given
 * for it (e.g ["foo:1", "bar"]).
 *
 * If there are no topic:partition pairs given, the consumer will subscribe to the topics via
 * #[org.apache.kafka.clients.consumer.KafkaConsumer.subscribe].
 * It will be assigned partitions dynamically from the consumer group.
 *
 * This specification supports the spawning of multiple consumers in the single Trogdor worker agent.
 * The "threadsPerWorker" field denotes how many consumers should be spawned for this spec.
 * It is worth noting that the "targetMessagesPerSec", "maxMessages" and "activeTopics" fields apply
 * for every consumer individually.
 *
 * If a consumer group is not specified, every consumer is assigned a different, random group. When specified,
 * all consumers use the same group. Since no two consumers in the same group can be assigned the same partition,
 * explicitly specifying partitions in "activeTopics" when there are multiple "threadsPerWorker"
 * and a particular "consumerGroup" will result in an [ConfigException], aborting the task.
 *
 * The "recordProcessor" field allows the specification of tasks to run on records that are consumed.
 * This is run immediately after the messages are polled. See the `RecordProcessor` interface for more information.
 *
 * An example JSON representation which will result in a consumer that is part of the consumer group "cg" and
 * subscribed to topics foo1, foo2, foo3 and bar.
 * ```json
 * {
 *     "class": "org.apache.kafka.trogdor.workload.ConsumeBenchSpec",
 *     "durationMs": 10000000,
 *     "consumerNode": "node0",
 *     "bootstrapServers": "localhost:9092",
 *     "maxMessages": 100,
 *     "consumerGroup": "cg",
 *     "activeTopics": ["foo[1-3]", "bar"]
 * }
 * ```
 */
class ConsumeBenchSpec @JsonCreator constructor(
    @JsonProperty("startMs") startMs: Long,
    @JsonProperty("durationMs") durationMs: Long,
    @JsonProperty("consumerNode") consumerNode: String?,
    @JsonProperty("bootstrapServers") bootstrapServers: String?,
    @param:JsonProperty("targetMessagesPerSec") private val targetMessagesPerSec: Int,
    @param:JsonProperty("maxMessages") private val maxMessages: Long,
    @JsonProperty("consumerGroup") consumerGroup: String?,
    @JsonProperty("consumerConf") consumerConf: Map<String, String>?,
    @JsonProperty("commonClientConf") commonClientConf: Map<String, String>?,
    @JsonProperty("adminClientConf") adminClientConf: Map<String, String>?,
    @JsonProperty("threadsPerWorker") threadsPerWorker: Int?,
    @JsonProperty("recordProcessor") recordProcessor: RecordProcessor?,
    @JsonProperty("activeTopics") activeTopics: List<String>?,
) : TaskSpec(startMs, durationMs) {

    private val consumerNode: String

    private val bootstrapServers: String

    private val consumerConf: Map<String, String>

    private val adminClientConf: Map<String, String>

    private val commonClientConf: Map<String, String>

    private val activeTopics: List<String>

    private val consumerGroup: String

    private val threadsPerWorker: Int

    private val recordProcessor: RecordProcessor?

    init {
        this.consumerNode = consumerNode ?: ""
        this.bootstrapServers = bootstrapServers ?: ""
        this.consumerConf = consumerConf ?: emptyMap()
        this.commonClientConf = commonClientConf ?: emptyMap()
        this.adminClientConf = adminClientConf ?: emptyMap()
        this.activeTopics = activeTopics ?: ArrayList()
        this.consumerGroup = consumerGroup ?: ""
        this.threadsPerWorker = threadsPerWorker ?: 1
        this.recordProcessor = recordProcessor
    }

    @JsonProperty
    fun consumerNode(): String = consumerNode

    @JsonProperty
    fun consumerGroup(): String = consumerGroup

    @JsonProperty
    fun bootstrapServers(): String = bootstrapServers

    @JsonProperty
    fun targetMessagesPerSec(): Int = targetMessagesPerSec

    @JsonProperty
    fun maxMessages(): Long = maxMessages

    @JsonProperty
    fun threadsPerWorker(): Int = threadsPerWorker

    @JsonProperty
    fun recordProcessor(): RecordProcessor? = recordProcessor

    @JsonProperty
    fun consumerConf(): Map<String, String> = consumerConf

    @JsonProperty
    fun commonClientConf(): Map<String, String> = commonClientConf

    @JsonProperty
    fun adminClientConf(): Map<String, String> = adminClientConf

    @JsonProperty
    fun activeTopics(): List<String> = activeTopics

    override fun newController(id: String?): TaskController = TaskController { setOf(consumerNode) }

    override fun newTaskWorker(id: String): TaskWorker = ConsumeBenchWorker(id, this)

    /**
     * Materializes a list of topic names (optionally with ranges) into a map of the topics and their partitions
     *
     * Example:
     * ['foo[1-3]', 'foobar:2', 'bar[1-2]:[1-2]'] => {'foo1': [], 'foo2': [], 'foo3': [], 'foobar': [2],
     * 'bar1': [1, 2], 'bar2': [1, 2] }
     */
    fun materializeTopics(): MutableMap<String, MutableList<TopicPartition>> {
        val partitionsByTopics = mutableMapOf<String, MutableList<TopicPartition>>()
        for (rawTopicName in activeTopics) {
            val expandedNames = expandTopicName(rawTopicName)
            require(expandedNames.first().matches(VALID_EXPANDED_TOPIC_NAME_PATTERN.toRegex())) {
                "Expanded topic name $rawTopicName is invalid"
            }
            for (topicName in expandedNames) {
                var actualTopicName = topicName
                var partition: TopicPartition? = null
                if (actualTopicName.contains(":")) {
                    val topicAndPartition = actualTopicName.split(":")
                    actualTopicName = topicAndPartition[0]
                    partition = TopicPartition(actualTopicName, topicAndPartition[1].toInt())
                }

                if (!partitionsByTopics.containsKey(actualTopicName))
                    partitionsByTopics[actualTopicName] = mutableListOf()

                if (partition != null) partitionsByTopics[actualTopicName]!!.add(partition)
            }
        }
        return partitionsByTopics
    }

    /**
     * Expands a topic name until there are no more ranges in it
     */
    private fun expandTopicName(topicName: String): Set<String> {
        val expandedNames: Set<String> = expand(topicName)
        if (expandedNames.size == 1) return expandedNames

        return expandedNames.flatMap { expandTopicName(it) }.toSet()
    }

    companion object {
        private const val VALID_EXPANDED_TOPIC_NAME_PATTERN = "^[^:]+(:[\\d]+|[^:]*)$"
    }
}
