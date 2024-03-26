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

package org.apache.kafka.trogdor.common

import java.util.Properties
import org.apache.kafka.clients.admin.MockAdminClient
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.admin.TopicDescription
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.Node
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.TopicPartitionInfo
import org.apache.kafka.common.errors.TopicExistsException
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException
import org.apache.kafka.trogdor.common.WorkerUtils.addConfigsToProperties
import org.apache.kafka.trogdor.common.WorkerUtils.createTopics
import org.apache.kafka.trogdor.common.WorkerUtils.getMatchingTopicPartitions
import org.apache.kafka.trogdor.common.WorkerUtils.verifyTopics
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.slf4j.LoggerFactory
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class WorkerUtilsTest {
    
    private val broker1 = Node(0, "testHost-1", 1234)
    
    private val broker2 = Node(1, "testHost-2", 1234)
    
    private val broker3 = Node(1, "testHost-3", 1234)
    
    private val cluster = listOf(broker1, broker2, broker3)
    
    private val singleReplica = listOf(broker1)
    
    private lateinit var adminClient: MockAdminClient
    
    @BeforeEach
    fun setUp() {
        adminClient = MockAdminClient(cluster, broker1)
    }

    @Test
    @Throws(Throwable::class)
    fun testCreateOneTopic() {
        val newTopics = mapOf(TEST_TOPIC to NEW_TEST_TOPIC)
        createTopics(
            log = log,
            adminClient = adminClient,
            topics = newTopics,
            failOnExisting = true,
        )
        assertEquals(setOf(TEST_TOPIC), adminClient.listTopics().names.get())
        assertEquals(
            TopicDescription(
                name = TEST_TOPIC,
                internal = false,
                partitions = listOf(
                    TopicPartitionInfo(
                        partition = 0,
                        leader = broker1,
                        replicas = singleReplica,
                        inSyncReplicas = emptyList(),
                    ),
                )
            ),
            adminClient.describeTopics(setOf(TEST_TOPIC)).topicNameValues()!![TEST_TOPIC]!!.get()
        )
    }

    @Test
    @Throws(Throwable::class)
    fun testCreateRetriesOnTimeout() {
        adminClient.timeoutNextRequest(1)
        createTopics(
            log = log,
            adminClient = adminClient,
            topics = mapOf(TEST_TOPIC to NEW_TEST_TOPIC),
            failOnExisting = true,
        )
        assertEquals(
            TopicDescription(
                name = TEST_TOPIC,
                internal = false,
                partitions = listOf(
                    TopicPartitionInfo(
                        partition = 0,
                        leader = broker1,
                        replicas = singleReplica,
                        inSyncReplicas = emptyList(),
                    ),
                ),
            ),
            adminClient.describeTopics(setOf(TEST_TOPIC)).topicNameValues()!![TEST_TOPIC]!!.get()
        )
    }

    @Test
    @Throws(Throwable::class)
    fun testCreateZeroTopicsDoesNothing() {
        createTopics(log, adminClient, emptyMap(), true)
        assertEquals(0, adminClient.listTopics().names.get().size)
    }

    @Test
    @Throws(Throwable::class)
    fun testCreateTopicsFailsIfAtLeastOneTopicExists() {
        adminClient.addTopic(
            internal = false,
            name = TEST_TOPIC,
            partitions = listOf(
                TopicPartitionInfo(
                    partition = 0,
                    leader = broker1,
                    replicas = singleReplica,
                    inSyncReplicas = emptyList(),
                ),
            ),
            configs = null
        )
        val newTopics = mapOf(
            TEST_TOPIC to NEW_TEST_TOPIC,
            "another-topic" to NewTopic("another-topic", TEST_PARTITIONS, TEST_REPLICATION_FACTOR),
            "one-more-topic" to NewTopic("one-more-topic", TEST_PARTITIONS, TEST_REPLICATION_FACTOR),
        )
        assertFailsWith<TopicExistsException> {
            createTopics(
                log = log,
                adminClient = adminClient,
                topics = newTopics,
                failOnExisting = true,
            )
        }
    }

    @Test
    @Throws(Throwable::class)
    fun testExistingTopicsMustHaveRequestedNumberOfPartitions() {
        val tpInfo: MutableList<TopicPartitionInfo> = ArrayList()
        tpInfo.add(TopicPartitionInfo(0, broker1, singleReplica, emptyList()))
        tpInfo.add(TopicPartitionInfo(1, broker2, singleReplica, emptyList()))
        adminClient.addTopic(
            internal = false,
            name = TEST_TOPIC,
            partitions = tpInfo,
            configs = null,
        )
        assertFailsWith<RuntimeException> {
            createTopics(
                log = log,
                adminClient = adminClient,
                topics = mapOf(TEST_TOPIC to NEW_TEST_TOPIC),
                failOnExisting = false,
            )
        }
    }

    @Test
    @Throws(Throwable::class)
    fun testExistingTopicsNotCreated() {
        val existingTopic = "existing-topic"
        val tpInfo = listOf(
            TopicPartitionInfo(
                partition = 0,
                leader = broker1,
                replicas = singleReplica,
                inSyncReplicas = emptyList()
            ),
            TopicPartitionInfo(
                partition = 1,
                leader = broker2,
                replicas = singleReplica,
                inSyncReplicas = emptyList()
            ),
            TopicPartitionInfo(
                partition = 2,
                leader = broker3,
                replicas = singleReplica,
                inSyncReplicas = emptyList()
            ),
        )
        adminClient.addTopic(
            internal = false,
            name = existingTopic,
            partitions = tpInfo,
            configs = null,
        )
        createTopics(
            log = log,
            adminClient = adminClient,
            topics = mapOf(existingTopic to NewTopic(existingTopic, tpInfo.size, TEST_REPLICATION_FACTOR)),
            failOnExisting = false,
        )
        assertEquals(setOf(existingTopic), adminClient.listTopics().names.get())
    }

    @Test
    @Throws(Throwable::class)
    fun testCreatesNotExistingTopics() {
        // should be no topics before the call
        assertEquals(0, adminClient.listTopics().names.get().size)
        createTopics(
            log = log,
            adminClient = adminClient,
            topics = mapOf(TEST_TOPIC to NEW_TEST_TOPIC),
            failOnExisting = false,
        )
        assertEquals(setOf(TEST_TOPIC), adminClient.listTopics().names.get())
        assertEquals(
            TopicDescription(
                name = TEST_TOPIC,
                internal = false,
                partitions = listOf(
                    TopicPartitionInfo(
                        partition = 0,
                        leader = broker1,
                        replicas = singleReplica,
                        inSyncReplicas = emptyList(),
                    ),
                )
            ),
            adminClient.describeTopics(setOf(TEST_TOPIC)).topicNameValues()!![TEST_TOPIC]!!.get()
        )
    }

    @Test
    @Throws(Throwable::class)
    fun testCreatesOneTopicVerifiesOneTopic() {
        val existingTopic = "existing-topic"
        val tpInfo = listOf(
            TopicPartitionInfo(
                partition = 0,
                leader = broker1,
                replicas = singleReplica,
                inSyncReplicas = emptyList(),
            ),
            TopicPartitionInfo(
                partition = 1,
                leader = broker2,
                replicas = singleReplica,
                inSyncReplicas = emptyList(),
            ),
        )
        adminClient.addTopic(
            internal = false,
            name = existingTopic,
            partitions = tpInfo,
            configs = null,
        )
        val topics = mapOf(
            existingTopic to NewTopic(
                name = existingTopic,
                numPartitions = tpInfo.size,
                replicationFactor = TEST_REPLICATION_FACTOR,
            ),
            TEST_TOPIC to NEW_TEST_TOPIC,
        )
        createTopics(log, adminClient, topics, false)
        assertEquals(setOf(existingTopic, TEST_TOPIC), adminClient.listTopics().names.get())
    }

    @Test
    @Throws(Throwable::class)
    fun testCreateNonExistingTopicsWithZeroTopicsDoesNothing() {
        createTopics(
            log = log,
            adminClient = adminClient,
            topics = emptyMap(),
            failOnExisting = false,
        )
        assertEquals(0, adminClient.listTopics().names.get().size)
    }

    @Test
    fun testAddConfigsToPropertiesAddsAllConfigs() {
        val props = Properties()
        props[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = "localhost:9092"
        props[ProducerConfig.ACKS_CONFIG] = "all"
        val resultProps = Properties()
        resultProps.putAll(props)
        resultProps[ProducerConfig.CLIENT_ID_CONFIG] = "test-client"
        resultProps[ProducerConfig.LINGER_MS_CONFIG] = "1000"
        addConfigsToProperties(
            props = props,
            commonConf = mapOf(ProducerConfig.CLIENT_ID_CONFIG to "test-client"),
            clientConf = mapOf(ProducerConfig.LINGER_MS_CONFIG to "1000"),
        )
        assertEquals(resultProps, props)
    }

    @Test
    fun testCommonConfigOverwritesDefaultProps() {
        val props = Properties()
        props[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = "localhost:9092"
        props[ProducerConfig.ACKS_CONFIG] = "all"
        val resultProps = Properties()
        resultProps[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = "localhost:9092"
        resultProps[ProducerConfig.ACKS_CONFIG] = "1"
        resultProps[ProducerConfig.LINGER_MS_CONFIG] = "1000"
        addConfigsToProperties(
            props = props,
            commonConf = mapOf(ProducerConfig.ACKS_CONFIG to "1"),
            clientConf = mapOf(ProducerConfig.LINGER_MS_CONFIG to "1000"),
        )
        assertEquals(resultProps, props)
    }

    @Test
    fun testClientConfigOverwritesBothDefaultAndCommonConfigs() {
        val props = Properties()
        props[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = "localhost:9092"
        props[ProducerConfig.ACKS_CONFIG] = "all"
        val resultProps = Properties()
        resultProps[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = "localhost:9092"
        resultProps[ProducerConfig.ACKS_CONFIG] = "0"
        addConfigsToProperties(
            props = props,
            commonConf = mapOf(ProducerConfig.ACKS_CONFIG to "1"),
            clientConf = mapOf(ProducerConfig.ACKS_CONFIG to "0"),
        )
        assertEquals(resultProps, props)
    }

    @Test
    @Throws(Throwable::class)
    fun testGetMatchingTopicPartitionsCorrectlyMatchesExactTopicName() {
        val topic1 = "existing-topic"
        val topic2 = "another-topic"
        makeExistingTopicWithOneReplica(topic1, 10)
        makeExistingTopicWithOneReplica(topic2, 20)
        val topicPartitions = getMatchingTopicPartitions(
            adminClient = adminClient,
            topicRegex = topic2,
            startPartition = 0,
            endPartition = 2,
        )
        assertEquals(
            setOf(
                TopicPartition(topic2, 0),
                TopicPartition(topic2, 1),
                TopicPartition(topic2, 2),
            ),
            topicPartitions.toSet(),
        )
    }

    @Test
    @Throws(Throwable::class)
    fun testGetMatchingTopicPartitionsCorrectlyMatchesTopics() {
        val topic1 = "test-topic"
        val topic2 = "another-test-topic"
        val topic3 = "one-more"
        makeExistingTopicWithOneReplica(topic1, 10)
        makeExistingTopicWithOneReplica(topic2, 20)
        makeExistingTopicWithOneReplica(topic3, 30)
        val topicPartitions = getMatchingTopicPartitions(
            adminClient = adminClient,
            topicRegex = ".*-topic$",
            startPartition = 0,
            endPartition = 1,
        )
        assertEquals(
            setOf(
                TopicPartition(topic1, 0),
                TopicPartition(topic1, 1),
                TopicPartition(topic2, 0),
                TopicPartition(topic2, 1),
            ),
            topicPartitions.toSet(),
        )
    }

    private fun makeExistingTopicWithOneReplica(topicName: String, numPartitions: Int) {
        val tpInfo = mutableListOf<TopicPartitionInfo>()
        var brokerIndex = 0
        for (i in 0..<numPartitions) {
            val broker = cluster[brokerIndex]
            tpInfo.add(
                TopicPartitionInfo(
                    partition = i,
                    leader = broker,
                    replicas = singleReplica,
                    inSyncReplicas = emptyList(),
                )
            )
            brokerIndex = (brokerIndex + 1) % cluster.size
        }
        adminClient.addTopic(
            internal = false,
            name = topicName,
            partitions = tpInfo,
            configs = null,
        )
    }

    @Test
    @Throws(Throwable::class)
    fun testVerifyTopics() {
        val newTopics = mapOf(TEST_TOPIC to NEW_TEST_TOPIC)
        createTopics(log, adminClient, newTopics, true)
        adminClient.setFetchesRemainingUntilVisible(topicName = TEST_TOPIC, fetchesRemainingUntilVisible = 2)
        verifyTopics(
            log = log,
            adminClient = adminClient,
            topicsToVerify = setOf(TEST_TOPIC),
            topicsInfo = mapOf(TEST_TOPIC to NEW_TEST_TOPIC),
            retryCount = 3,
            retryBackoffMs = 1,
        )
        adminClient.setFetchesRemainingUntilVisible(topicName = TEST_TOPIC, fetchesRemainingUntilVisible = 100)
        assertFailsWith<UnknownTopicOrPartitionException> {
            verifyTopics(
                log = log,
                adminClient = adminClient,
                topicsToVerify = setOf(TEST_TOPIC),
                topicsInfo = mapOf(TEST_TOPIC to NEW_TEST_TOPIC),
                retryCount = 2,
                retryBackoffMs = 1,
            )
        }
    }

    companion object {
        
        private val log = LoggerFactory.getLogger(WorkerUtilsTest::class.java)
        
        private const val TEST_TOPIC = "test-topic-1"
        
        private const val TEST_REPLICATION_FACTOR: Short = 1
        
        private const val TEST_PARTITIONS = 1
        
        private val NEW_TEST_TOPIC = NewTopic(TEST_TOPIC, TEST_PARTITIONS, TEST_REPLICATION_FACTOR)
    }
}
