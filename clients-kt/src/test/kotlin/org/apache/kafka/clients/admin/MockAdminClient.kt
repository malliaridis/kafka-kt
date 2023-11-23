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

package org.apache.kafka.clients.admin

import org.apache.kafka.clients.admin.AlterConfigOp.OpType
import org.apache.kafka.clients.admin.CreateTopicsResult.TopicMetadataAndConfig
import org.apache.kafka.clients.admin.DescribeReplicaLogDirsResult.ReplicaLogDirInfo
import org.apache.kafka.clients.admin.FeatureUpdate.UpgradeType
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo
import org.apache.kafka.clients.admin.OffsetSpec.EarliestSpec
import org.apache.kafka.clients.admin.OffsetSpec.TimestampSpec
import org.apache.kafka.clients.admin.internals.CoordinatorKey
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.ElectionType
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.KafkaFuture
import org.apache.kafka.common.Metric
import org.apache.kafka.common.MetricName
import org.apache.kafka.common.Node
import org.apache.kafka.common.TopicCollection
import org.apache.kafka.common.TopicCollection.TopicIdCollection
import org.apache.kafka.common.TopicCollection.TopicNameCollection
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.TopicPartitionInfo
import org.apache.kafka.common.TopicPartitionReplica
import org.apache.kafka.common.Uuid
import org.apache.kafka.common.acl.AclBinding
import org.apache.kafka.common.acl.AclBindingFilter
import org.apache.kafka.common.acl.AclOperation
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.errors.InvalidReplicationFactorException
import org.apache.kafka.common.errors.InvalidRequestException
import org.apache.kafka.common.errors.InvalidUpdateVersionException
import org.apache.kafka.common.errors.KafkaStorageException
import org.apache.kafka.common.errors.ReplicaNotAvailableException
import org.apache.kafka.common.errors.TimeoutException
import org.apache.kafka.common.errors.TopicExistsException
import org.apache.kafka.common.errors.UnknownTopicIdException
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException
import org.apache.kafka.common.errors.UnsupportedVersionException
import org.apache.kafka.common.internals.KafkaFutureImpl
import org.apache.kafka.common.quota.ClientQuotaAlteration
import org.apache.kafka.common.quota.ClientQuotaFilter
import org.apache.kafka.common.requests.DescribeLogDirsResponse
import java.time.Duration
import java.util.*
import java.util.function.Function
import java.util.stream.Collectors
import kotlin.math.max
import kotlin.math.min

class MockAdminClient private constructor(
    private val brokers: List<Node> = listOf(Node.noNode()),
    controller: Node = Node.noNode(),
    private val clusterId: String = DEFAULT_CLUSTER_ID,
    private val defaultPartitions: Int = 1,
    private val defaultReplicationFactor: Int = brokers.size,
    private val brokerLogDirs: List<List<String>> = List(brokers.size) { DEFAULT_LOG_DIRS },
    private val usingRaftController: Boolean = false,
    featureLevels: Map<String, Short> = emptyMap(),
    private val minSupportedFeatureLevels: Map<String, Short> = emptyMap(),
    private val maxSupportedFeatureLevels: Map<String, Short> = emptyMap(),
) : AdminClient() {

    private val allTopics: MutableMap<String, TopicMetadata> = HashMap()

    private val topicIds: MutableMap<String, Uuid> = HashMap()

    private val topicNames: MutableMap<Uuid, String> = HashMap()

    private val reassignments: MutableMap<TopicPartition, NewPartitionReassignment> = HashMap()

    private val replicaMoves: MutableMap<TopicPartitionReplica, ReplicaLogDirInfo> = HashMap()

    private val beginningOffsets: MutableMap<TopicPartition, Long> = HashMap()

    private val endOffsets: MutableMap<TopicPartition, Long> = HashMap()

    private val committedOffsets: MutableMap<TopicPartition, Long> = HashMap()

    private val featureLevels: MutableMap<String, Short>

    private val brokerConfigs: MutableList<Map<String, String?>> = ArrayList()

    private var controller: Node? = null
        @Synchronized
        set(value) {
            require(brokers.contains(value)) { "The controller node must be in the list of brokers" }
            field = value
        }

    private var timeoutNextRequests = 0

    private var listConsumerGroupOffsetsException: KafkaException? = null

    private val mockMetrics: MutableMap<MetricName, Metric> = HashMap()

    init {
        this.controller = controller

        for (i in brokers.indices) {
            val config: MutableMap<String, String?> = HashMap()
            config["default.replication.factor"] = defaultReplicationFactor.toString()
            brokerConfigs.add(config)
        }
        this.featureLevels = HashMap(featureLevels)
    }

    @Deprecated("Use property instead")
    @Synchronized
    fun controller(controller: Node?) {
        if (!brokers.contains(controller)) throw IllegalArgumentException("The controller node must be in the list of brokers")
        this.controller = controller
    }

    @Synchronized
    fun addTopic(
        internal: Boolean,
        name: String,
        partitions: List<TopicPartitionInfo>,
        configs: Map<String, String?>?,
        usesTopicId: Boolean = true,
    ) {
        require(!allTopics.containsKey(name)) {
            String.format("Topic %s was already added.", name)
        }
        partitions.forEach { partition ->
            require(brokers.contains(partition.leader)) { "Leader broker unknown" }
            require(brokers.containsAll(partition.replicas)) { "Unknown brokers in replica list" }
            require(brokers.containsAll(partition.inSyncReplicas)) { "Unknown brokers in isr list" }
        }
        val logDirs = mutableListOf<String>()
        partitions.forEach { partition ->
            if (partition.leader != null) logDirs.add(brokerLogDirs[partition.leader!!.id][0])
        }

        val topicId: Uuid
        if (usesTopicId) {
            topicId = Uuid.randomUuid()
            topicIds[name] = topicId
            topicNames[topicId] = name
        } else {
            topicId = Uuid.ZERO_UUID
        }
        allTopics[name] = TopicMetadata(
            topicId = topicId,
            isInternalTopic = internal,
            partitions = partitions,
            partitionLogDirs = logDirs,
            configs = configs,
        )
    }

    @Synchronized
    fun markTopicForDeletion(name: String) {
        require(allTopics.containsKey(name)) { String.format("Topic %s did not exist.", name) }
        allTopics[name]!!.markedForDeletion = true
    }

    @Synchronized
    fun timeoutNextRequest(numberOfRequest: Int) {
        timeoutNextRequests = numberOfRequest
    }

    @Synchronized
    override fun describeCluster(options: DescribeClusterOptions): DescribeClusterResult {
        val nodesFuture = KafkaFutureImpl<Collection<Node>>()
        val controllerFuture = KafkaFutureImpl<Node?>()
        val brokerIdFuture = KafkaFutureImpl<String>()
        val authorizedOperationsFuture = KafkaFutureImpl<Set<AclOperation>?>()
        if (timeoutNextRequests > 0) {
            nodesFuture.completeExceptionally(TimeoutException())
            controllerFuture.completeExceptionally(TimeoutException())
            brokerIdFuture.completeExceptionally(TimeoutException())
            authorizedOperationsFuture.completeExceptionally(TimeoutException())
            --timeoutNextRequests
        } else {
            nodesFuture.complete(brokers)
            controllerFuture.complete(controller)
            brokerIdFuture.complete(clusterId)
            authorizedOperationsFuture.complete(emptySet())
        }
        return DescribeClusterResult(
            nodes = nodesFuture,
            controller = controllerFuture,
            clusterId = brokerIdFuture,
            authorizedOperations = authorizedOperationsFuture,
        )
    }

    @Synchronized
    override fun createTopics(
        newTopics: Collection<NewTopic>,
        options: CreateTopicsOptions,
    ): CreateTopicsResult {
        val createTopicResult = mutableMapOf<String, KafkaFuture<TopicMetadataAndConfig>>()
        if (timeoutNextRequests > 0) {
            for (newTopic in newTopics) {
                val topicName = newTopic.name
                val future = KafkaFutureImpl<TopicMetadataAndConfig>()
                future.completeExceptionally(TimeoutException())
                createTopicResult[topicName] = future
            }
            --timeoutNextRequests
            return CreateTopicsResult(createTopicResult)
        }
        for (newTopic in newTopics) {
            val future = KafkaFutureImpl<TopicMetadataAndConfig>()
            val topicName = newTopic.name
            if (allTopics.containsKey(topicName)) {
                future.completeExceptionally(TopicExistsException(String.format("Topic %s exists already.", topicName)))
                createTopicResult[topicName] = future
                continue
            }
            var replicationFactor = newTopic.replicationFactor.toInt()
            if (replicationFactor == -1) {
                replicationFactor = defaultReplicationFactor
            }
            if (replicationFactor > brokers.size) {
                future.completeExceptionally(
                    InvalidReplicationFactorException(
                        String.format(
                            "Replication factor: %d is larger than brokers: %d",
                            newTopic.replicationFactor,
                            brokers.size
                        )
                    )
                )
                createTopicResult[topicName] = future
                continue
            }
            val replicas: MutableList<Node> = ArrayList(replicationFactor)
            for (i in 0 until replicationFactor) replicas.add(brokers[i])

            var numberOfPartitions = newTopic.numPartitions
            if (numberOfPartitions == -1) {
                numberOfPartitions = defaultPartitions
            }
            val partitions: MutableList<TopicPartitionInfo> = ArrayList(numberOfPartitions)
            // Partitions start off on the first log directory of each broker, for now.
            val logDirs: MutableList<String> = ArrayList(numberOfPartitions)
            for (i in 0 until numberOfPartitions) {
                partitions.add(TopicPartitionInfo(i, brokers[0], replicas, emptyList()))
                logDirs.add(brokerLogDirs[partitions[i].leader!!.id][0])
            }
            val topicId = Uuid.randomUuid()
            topicIds[topicName] = topicId
            topicNames[topicId] = topicName
            allTopics[topicName] = TopicMetadata(
                topicId = topicId,
                isInternalTopic = false,
                partitions = partitions,
                partitionLogDirs = logDirs,
                configs = newTopic.configs,
            )
            future.complete(
                TopicMetadataAndConfig(
                    topicId = topicId,
                    numPartitions = numberOfPartitions,
                    replicationFactor = replicationFactor,
                    config = Config(emptyMap()),
                )
            )
            createTopicResult[topicName] = future
        }
        return CreateTopicsResult(createTopicResult)
    }

    @Synchronized
    override fun listTopics(options: ListTopicsOptions): ListTopicsResult {
        val topicListings = mutableMapOf<String, TopicListing>()
        if (timeoutNextRequests > 0) {
            val future = KafkaFutureImpl<Map<String, TopicListing>>()
            future.completeExceptionally(TimeoutException())
            --timeoutNextRequests
            return ListTopicsResult(future)
        }
        for ((topicName, metadata) in allTopics) {
            if (metadata.fetchesRemainingUntilVisible > 0) {
                metadata.fetchesRemainingUntilVisible--
            } else {
                topicListings[topicName] = TopicListing(
                    name = topicName,
                    topicId = metadata.topicId,
                    isInternal = metadata.isInternalTopic
                )
            }
        }
        val future = KafkaFutureImpl<Map<String, TopicListing>>()
        future.complete(topicListings)
        return ListTopicsResult(future)
    }

    @Synchronized
    override fun describeTopics(
        topics: TopicCollection,
        options: DescribeTopicsOptions,
    ): DescribeTopicsResult {
        return when (topics) {
            is TopicIdCollection -> DescribeTopicsResult.ofTopicIds(
                handleDescribeTopicsUsingIds(topics.topicIds, options).toMap()
            )

            is TopicNameCollection -> DescribeTopicsResult.ofTopicNames(
                handleDescribeTopicsByNames(topics.topicNames, options).toMap()
            )

            else -> throw IllegalArgumentException(
                "The TopicCollection provided did not match any supported classes for describeTopics."
            )
        }
    }

    private fun handleDescribeTopicsByNames(
        topicNames: Collection<String>,
        options: DescribeTopicsOptions,
    ): Map<String, KafkaFuture<TopicDescription>> {
        val topicDescriptions = mutableMapOf<String, KafkaFuture<TopicDescription>>()
        if (timeoutNextRequests > 0) {
            for (requestedTopic in topicNames) {
                val future = KafkaFutureImpl<TopicDescription>()
                future.completeExceptionally(TimeoutException())
                topicDescriptions[requestedTopic] = future
            }
            --timeoutNextRequests
            return topicDescriptions
        }
        for (requestedTopic in topicNames) {
            for ((topicName, topicMetadata) in allTopics) {
                val topicId = topicIds.getOrDefault(topicName, Uuid.ZERO_UUID)
                if (topicName == requestedTopic && !topicMetadata.markedForDeletion) {
                    if (topicMetadata.fetchesRemainingUntilVisible > 0) {
                        topicMetadata.fetchesRemainingUntilVisible--
                    } else {
                        val future = KafkaFutureImpl<TopicDescription>()
                        future.complete(
                            TopicDescription(
                                name = topicName,
                                internal = topicMetadata.isInternalTopic,
                                partitions = topicMetadata.partitions,
                                authorizedOperations = emptySet(),
                                topicId = topicId,
                            )
                        )
                        topicDescriptions[topicName] = future
                        break
                    }
                }
            }
            if (!topicDescriptions.containsKey(requestedTopic)) {
                val future = KafkaFutureImpl<TopicDescription>()
                future.completeExceptionally(UnknownTopicOrPartitionException("Topic $requestedTopic not found."))
                topicDescriptions[requestedTopic] = future
            }
        }
        return topicDescriptions
    }

    @Synchronized
    fun handleDescribeTopicsUsingIds(
        topicIds: Collection<Uuid>,
        options: DescribeTopicsOptions,
    ): Map<Uuid, KafkaFuture<TopicDescription>> {
        val topicDescriptions = mutableMapOf<Uuid, KafkaFuture<TopicDescription>>()
        if (timeoutNextRequests > 0) {
            for (requestedTopicId in topicIds) {
                val future = KafkaFutureImpl<TopicDescription>()
                future.completeExceptionally(TimeoutException())
                topicDescriptions[requestedTopicId] = future
            }
            --timeoutNextRequests
            return topicDescriptions
        }
        for (requestedTopicId in topicIds) {
            for ((topicName, topicMetadata) in allTopics) {
                val topicId = this.topicIds[topicName]
                if (topicId != null && topicId == requestedTopicId && !topicMetadata.markedForDeletion) {
                    if (topicMetadata.fetchesRemainingUntilVisible > 0) {
                        topicMetadata.fetchesRemainingUntilVisible--
                    } else {
                        val future = KafkaFutureImpl<TopicDescription>()
                        future.complete(
                            TopicDescription(
                                name = topicName,
                                internal = topicMetadata.isInternalTopic,
                                partitions = topicMetadata.partitions,
                                authorizedOperations = emptySet(),
                                topicId = topicId
                            )
                        )
                        topicDescriptions[requestedTopicId] = future
                        break
                    }
                }
            }
            if (!topicDescriptions.containsKey(requestedTopicId)) {
                val future = KafkaFutureImpl<TopicDescription>()
                future.completeExceptionally(UnknownTopicIdException("Topic id$requestedTopicId not found."))
                topicDescriptions[requestedTopicId] = future
            }
        }
        return topicDescriptions
    }

    @Synchronized
    override fun deleteTopics(
        topics: TopicCollection,
        options: DeleteTopicsOptions,
    ): DeleteTopicsResult {
        val result = when (topics) {
            is TopicIdCollection -> DeleteTopicsResult.ofTopicIds(
                HashMap(handleDeleteTopicsUsingIds(topics.topicIds, options))
            )

            is TopicNameCollection -> DeleteTopicsResult.ofTopicNames(
                HashMap(handleDeleteTopicsUsingNames(topics.topicNames, options))
            )

            else -> throw IllegalArgumentException(
                "The TopicCollection provided did not match any supported classes for deleteTopics."
            )
        }
        return result
    }

    private fun handleDeleteTopicsUsingNames(
        topicNameCollection: Collection<String>,
        options: DeleteTopicsOptions,
    ): Map<String, KafkaFuture<Unit>> {
        val deleteTopicsResult = mutableMapOf<String, KafkaFuture<Unit>>()
        val topicNames = topicNameCollection.toMutableList()
        if (timeoutNextRequests > 0) {
            for (topicName in topicNames) {
                val future = KafkaFutureImpl<Unit>()
                future.completeExceptionally(TimeoutException())
                deleteTopicsResult[topicName] = future
            }
            --timeoutNextRequests
            return deleteTopicsResult
        }
        for (topicName in topicNames) {
            val future = KafkaFutureImpl<Unit>()
            if (allTopics.remove(topicName) == null) {
                future.completeExceptionally(
                    UnknownTopicOrPartitionException("Topic $topicName does not exist.")
                )
            } else {
                topicNames.remove(topicIds.remove(topicName).toString())
                future.complete(Unit)
            }
            deleteTopicsResult[topicName] = future
        }
        return deleteTopicsResult
    }

    private fun handleDeleteTopicsUsingIds(
        topicIdCollection: Collection<Uuid>,
        options: DeleteTopicsOptions,
    ): Map<Uuid, KafkaFuture<Unit>> {
        val deleteTopicsResult = mutableMapOf<Uuid, KafkaFuture<Unit>>()
        val topicIds = topicIdCollection.toMutableList()
        if (timeoutNextRequests > 0) {
            for (topicId: Uuid in topicIds) {
                val future = KafkaFutureImpl<Unit>()
                future.completeExceptionally(TimeoutException())
                deleteTopicsResult[topicId] = future
            }
            --timeoutNextRequests
            return deleteTopicsResult
        }
        for (topicId: Uuid in topicIds) {
            val future = KafkaFutureImpl<Unit>()
            val name = topicNames.remove(topicId)
            if (name == null || allTopics.remove(name) == null) {
                future.completeExceptionally(UnknownTopicOrPartitionException("Topic $topicId does not exist."))
            } else {
                topicIds.remove(Uuid.fromString(name))
                future.complete(Unit)
            }
            deleteTopicsResult[topicId] = future
        }
        return deleteTopicsResult
    }

    @Synchronized
    override fun createPartitions(
        newPartitions: Map<String, NewPartitions>,
        options: CreatePartitionsOptions,
    ): CreatePartitionsResult {
        throw UnsupportedOperationException("Not implemented yet")
    }

    @Synchronized
    override fun deleteRecords(
        recordsToDelete: Map<TopicPartition, RecordsToDelete>,
        options: DeleteRecordsOptions,
    ): DeleteRecordsResult {
        val deletedRecordsResult: Map<TopicPartition, KafkaFuture<DeletedRecords>> = HashMap()
        return if (recordsToDelete.isEmpty()) DeleteRecordsResult(deletedRecordsResult)
        else throw UnsupportedOperationException("Not implemented yet")
    }

    @Synchronized
    override fun createDelegationToken(options: CreateDelegationTokenOptions): CreateDelegationTokenResult {
        throw UnsupportedOperationException("Not implemented yet")
    }

    @Synchronized
    override fun renewDelegationToken(
        hmac: ByteArray,
        options: RenewDelegationTokenOptions,
    ): RenewDelegationTokenResult {
        throw UnsupportedOperationException("Not implemented yet")
    }

    @Synchronized
    override fun expireDelegationToken(
        hmac: ByteArray,
        options: ExpireDelegationTokenOptions,
    ): ExpireDelegationTokenResult {
        throw UnsupportedOperationException("Not implemented yet")
    }

    @Synchronized
    override fun describeDelegationToken(options: DescribeDelegationTokenOptions): DescribeDelegationTokenResult {
        throw UnsupportedOperationException("Not implemented yet")
    }

    @Synchronized
    override fun describeConsumerGroups(
        groupIds: Collection<String>,
        options: DescribeConsumerGroupsOptions,
    ): DescribeConsumerGroupsResult {
        throw UnsupportedOperationException("Not implemented yet")
    }

    @Synchronized
    override fun listConsumerGroups(options: ListConsumerGroupsOptions): ListConsumerGroupsResult {
        throw UnsupportedOperationException("Not implemented yet")
    }

    @Synchronized
    override fun listConsumerGroupOffsets(
        groupSpecs: Map<String, ListConsumerGroupOffsetsSpec>,
        options: ListConsumerGroupOffsetsOptions,
    ): ListConsumerGroupOffsetsResult {
        // ignoring the groups and assume one test would only work on one group only
        if (groupSpecs.size != 1) throw UnsupportedOperationException("Not implemented yet")
        val group = groupSpecs.keys.iterator().next()
        val topicPartitions = groupSpecs[group]!!.topicPartitions
        val future = KafkaFutureImpl<Map<TopicPartition, OffsetAndMetadata?>>()
        if (listConsumerGroupOffsetsException != null) {
            future.completeExceptionally(listConsumerGroupOffsetsException!!)
        } else {
            if (topicPartitions!!.isEmpty()) future.complete(
                committedOffsets.mapValues { OffsetAndMetadata(it.value) }
            )
            else future.complete(
                committedOffsets.filter { (key, _) -> topicPartitions.contains(key) }
                    .mapValues { OffsetAndMetadata(it.value) }
            )
        }
        return ListConsumerGroupOffsetsResult(mapOf(CoordinatorKey.byGroupId(group) to future))
    }

    @Synchronized
    override fun deleteConsumerGroups(
        groupIds: Collection<String>,
        options: DeleteConsumerGroupsOptions,
    ): DeleteConsumerGroupsResult {
        throw UnsupportedOperationException("Not implemented yet")
    }

    @Synchronized
    override fun deleteConsumerGroupOffsets(
        groupId: String,
        partitions: Set<TopicPartition>,
        options: DeleteConsumerGroupOffsetsOptions,
    ): DeleteConsumerGroupOffsetsResult {
        throw UnsupportedOperationException("Not implemented yet")
    }

    @Synchronized
    override fun electLeaders(
        electionType: ElectionType,
        partitions: Set<TopicPartition>?,
        options: ElectLeadersOptions,
    ): ElectLeadersResult {
        throw UnsupportedOperationException("Not implemented yet")
    }

    @Synchronized
    override fun removeMembersFromConsumerGroup(
        groupId: String,
        options: RemoveMembersFromConsumerGroupOptions,
    ): RemoveMembersFromConsumerGroupResult {
        throw UnsupportedOperationException("Not implemented yet")
    }

    @Synchronized
    override fun createAcls(acls: Collection<AclBinding>, options: CreateAclsOptions): CreateAclsResult {
        throw UnsupportedOperationException("Not implemented yet")
    }

    @Synchronized
    override fun describeAcls(filter: AclBindingFilter, options: DescribeAclsOptions): DescribeAclsResult {
        throw UnsupportedOperationException("Not implemented yet")
    }

    @Synchronized
    override fun deleteAcls(filters: Collection<AclBindingFilter>, options: DeleteAclsOptions): DeleteAclsResult {
        throw UnsupportedOperationException("Not implemented yet")
    }

    @Synchronized
    override fun describeConfigs(
        resources: Collection<ConfigResource>,
        options: DescribeConfigsOptions,
    ): DescribeConfigsResult {
        if (timeoutNextRequests > 0) {
            val configs = mutableMapOf<ConfigResource, KafkaFuture<Config>>()
            for (requestedResource in resources) {
                val future = KafkaFutureImpl<Config>()
                future.completeExceptionally(TimeoutException())
                configs[requestedResource] = future
            }
            --timeoutNextRequests
            return DescribeConfigsResult(configs)
        }
        val results = mutableMapOf<ConfigResource, KafkaFuture<Config>>()
        for (resource in resources) {
            val future = KafkaFutureImpl<Config>()
            results[resource] = future
            try {
                future.complete(getResourceDescription(resource))
            } catch (e: Throwable) {
                future.completeExceptionally(e)
            }
        }
        return DescribeConfigsResult(results)
    }

    @Synchronized
    private fun getResourceDescription(resource: ConfigResource): Config {
        when (resource.type) {
            ConfigResource.Type.BROKER -> {
                val brokerId = resource.name.toInt()
                if (brokerId >= brokerConfigs.size) throw InvalidRequestException("Broker ${resource.name} not found.")
                return toConfigObject(brokerConfigs[brokerId])
            }

            ConfigResource.Type.TOPIC -> {
                val topicMetadata = allTopics[resource.name]
                if (topicMetadata != null && !topicMetadata.markedForDeletion) {
                    if (topicMetadata.fetchesRemainingUntilVisible > 0)
                        topicMetadata.fetchesRemainingUntilVisible =
                            max(0.0, (topicMetadata.fetchesRemainingUntilVisible - 1).toDouble()).toInt()
                    else return toConfigObject(topicMetadata.configs)
                }
                throw UnknownTopicOrPartitionException("Resource $resource not found.")
            }

            else -> throw UnsupportedOperationException("Not implemented yet")
        }
    }

    @Deprecated("")
    @Synchronized
    override fun alterConfigs(configs: Map<ConfigResource, Config>, options: AlterConfigsOptions): AlterConfigsResult {
        throw UnsupportedOperationException("Not implemented yet")
    }

    @Synchronized
    override fun incrementalAlterConfigs(
        configs: Map<ConfigResource, Collection<AlterConfigOp>>,
        options: AlterConfigsOptions,
    ): AlterConfigsResult {
        val futures: MutableMap<ConfigResource, KafkaFuture<Unit>> = HashMap()
        for ((resource, config) in configs) {
            val future = KafkaFutureImpl<Unit>()
            futures[resource] = future
            val throwable = handleIncrementalResourceAlteration(resource, config)

            if (throwable == null) future.complete(Unit)
            else future.completeExceptionally(throwable)
        }
        return AlterConfigsResult(futures)
    }

    @Synchronized
    private fun handleIncrementalResourceAlteration(
        resource: ConfigResource,
        ops: Collection<AlterConfigOp>,
    ): Throwable? {
        when (resource.type) {
            ConfigResource.Type.BROKER -> {
                val brokerId: Int
                try {
                    brokerId = resource.name.toInt()
                } catch (e: NumberFormatException) {
                    return e
                }
                if (brokerId >= brokerConfigs.size) {
                    return InvalidRequestException("no such broker as $brokerId")
                }
                val newMap = HashMap(brokerConfigs[brokerId])
                for (op in ops) {
                    when (op.opType) {
                        OpType.SET -> newMap[op.configEntry.name] = op.configEntry.value
                        OpType.DELETE -> newMap.remove(op.configEntry.name)
                        else -> return InvalidRequestException("Unsupported op type ${op.opType}")
                    }
                }
                brokerConfigs[brokerId] = newMap
                return null
            }

            ConfigResource.Type.TOPIC -> {
                val topicMetadata = allTopics[resource.name]
                    ?: return UnknownTopicOrPartitionException("No such topic as ${resource.name}")

                val newMap = HashMap(topicMetadata.configs)
                for (op in ops) {
                    when (op.opType) {
                        OpType.SET -> newMap[op.configEntry.name] = op.configEntry.value
                        OpType.DELETE -> newMap.remove(op.configEntry.name)
                        else -> return InvalidRequestException("Unsupported op type ${op.opType}")
                    }
                }
                topicMetadata.configs = newMap
                return null
            }

            else -> return UnsupportedOperationException()
        }
    }

    @Synchronized
    override fun alterReplicaLogDirs(
        replicaAssignment: Map<TopicPartitionReplica, String>,
        options: AlterReplicaLogDirsOptions,
    ): AlterReplicaLogDirsResult {
        val results: MutableMap<TopicPartitionReplica, KafkaFuture<Unit>> = HashMap()
        for ((replica, newLogDir) in replicaAssignment) {
            val future = KafkaFutureImpl<Unit>()
            results[replica] = future
            val dirs = brokerLogDirs.getOrNull(replica.brokerId)
            if (dirs == null) future.completeExceptionally(
                ReplicaNotAvailableException("Can't find $replica")
            )
            else if (!dirs.contains(newLogDir)) future.completeExceptionally(
                KafkaStorageException("Log directory $newLogDir is offline")
            )
            else {
                val metadata = allTopics[replica.topic]
                if (metadata == null || metadata.partitions.size <= replica.partition)
                    future.completeExceptionally(
                        ReplicaNotAvailableException("Can't find $replica")
                    )
                else {
                    val currentLogDir = metadata.partitionLogDirs[replica.partition]
                    replicaMoves[replica] = ReplicaLogDirInfo(
                        currentReplicaLogDir = currentLogDir,
                        currentReplicaOffsetLag = 0,
                        futureReplicaLogDir = newLogDir,
                        futureReplicaOffsetLag = 0,
                    )
                    future.complete(Unit)
                }
            }
        }
        return AlterReplicaLogDirsResult(results)
    }

    @Synchronized
    override fun describeLogDirs(
        brokers: Collection<Int>,
        options: DescribeLogDirsOptions,
    ): DescribeLogDirsResult {
        throw UnsupportedOperationException("Not implemented yet")
    }

    @Synchronized
    override fun describeReplicaLogDirs(
        replicas: Collection<TopicPartitionReplica>,
        options: DescribeReplicaLogDirsOptions,
    ): DescribeReplicaLogDirsResult {
        val results = mutableMapOf<TopicPartitionReplica, KafkaFuture<ReplicaLogDirInfo>>()
        for (replica in replicas) {
            val topicMetadata = allTopics[replica.topic]
            if (topicMetadata != null) {
                val future = KafkaFutureImpl<ReplicaLogDirInfo>()
                results[replica] = future
                val currentLogDir = currentLogDir(replica)
                if (currentLogDir == null) {
                    future.complete(
                        ReplicaLogDirInfo(
                            currentReplicaLogDir = null,
                            currentReplicaOffsetLag = DescribeLogDirsResponse.INVALID_OFFSET_LAG,
                            futureReplicaLogDir = null,
                            futureReplicaOffsetLag = DescribeLogDirsResponse.INVALID_OFFSET_LAG,
                        )
                    )
                } else {
                    val info = replicaMoves[replica]
                    if (info == null) future.complete(
                        ReplicaLogDirInfo(
                            currentReplicaLogDir = currentLogDir,
                            currentReplicaOffsetLag = 0,
                            futureReplicaLogDir = null,
                            futureReplicaOffsetLag = 0,
                        )
                    )
                    else future.complete(info)
                }
            }
        }
        return DescribeReplicaLogDirsResult(results)
    }

    @Synchronized
    private fun currentLogDir(replica: TopicPartitionReplica): String? {
        val topicMetadata = allTopics[replica.topic] ?: return null

        return if (topicMetadata.partitionLogDirs.size <= replica.partition) null
        else topicMetadata.partitionLogDirs[replica.partition]
    }

    @Synchronized
    override fun alterPartitionReassignments(
        newReassignments: Map<TopicPartition, NewPartitionReassignment?>,
        options: AlterPartitionReassignmentsOptions,
    ): AlterPartitionReassignmentsResult {
        val futures = mutableMapOf<TopicPartition, KafkaFuture<Unit>>()
        newReassignments.forEach { (partition, newReassignment) ->
            val future = KafkaFutureImpl<Unit>()
            futures[partition] = future
            val topicMetadata = allTopics[partition.topic]
            if (
                partition.partition < 0
                || topicMetadata == null
                || topicMetadata.partitions.size <= partition.partition
            ) future.completeExceptionally(UnknownTopicOrPartitionException())
            else if (newReassignment != null) {
                reassignments[partition] = newReassignment
                future.complete(Unit)
            } else {
                reassignments.remove(partition)
                future.complete(Unit)
            }
        }
        return AlterPartitionReassignmentsResult(futures)
    }

    @Synchronized
    override fun listPartitionReassignments(
        partitions: Set<TopicPartition>,
        options: ListPartitionReassignmentsOptions,
    ): ListPartitionReassignmentsResult {
        val map = mutableMapOf<TopicPartition, PartitionReassignment>()
        partitions.forEach { partition ->
            val reassignment = findPartitionReassignment(partition)
            if (reassignment != null) map[partition] = reassignment
        }
        return ListPartitionReassignmentsResult(KafkaFuture.completedFuture(map))
    }

    @Synchronized
    private fun findPartitionReassignment(partition: TopicPartition): PartitionReassignment? {
        val reassignment = reassignments[partition] ?: return null
        val metadata = allTopics[partition.topic] ?: throw RuntimeException(
            "Internal MockAdminClient logic error: found reassignment for $partition, but no TopicMetadata"
        )
        val info = metadata.partitions.getOrNull(partition.partition) ?: throw RuntimeException(
            "Internal MockAdminClient logic error: found reassignment for $partition, but no TopicPartitionInfo"
        )
        val replicas = mutableListOf<Int>()
        val removingReplicas = mutableListOf<Int>()
        val addingReplicas = ArrayList(reassignment.targetReplicas)
        info.replicas.forEach { node ->
            replicas.add(node.id)
            if (!reassignment.targetReplicas.contains(node.id)) removingReplicas.add(node.id)
            addingReplicas.remove(node.id)
        }
        return PartitionReassignment(replicas, addingReplicas, removingReplicas)
    }

    @Synchronized
    override fun alterConsumerGroupOffsets(
        groupId: String,
        offsets: Map<TopicPartition, OffsetAndMetadata>,
        options: AlterConsumerGroupOffsetsOptions,
    ): AlterConsumerGroupOffsetsResult {
        throw UnsupportedOperationException("Not implement yet")
    }

    @Synchronized
    override fun listOffsets(
        topicPartitionOffsets: Map<TopicPartition, OffsetSpec>,
        options: ListOffsetsOptions,
    ): ListOffsetsResult {
        val futures = mutableMapOf<TopicPartition, KafkaFuture<ListOffsetsResultInfo>>()
        topicPartitionOffsets.forEach { (tp, spec) ->
            val future = KafkaFutureImpl<ListOffsetsResultInfo>()
            when (spec) {
                is TimestampSpec -> throw UnsupportedOperationException("Not implement yet")
                is EarliestSpec -> future.complete(
                    ListOffsetsResultInfo(
                        offset = beginningOffsets[tp]!!,
                        timestamp = -1,
                        leaderEpoch = null,
                    )
                )

                else -> future.complete(
                    ListOffsetsResultInfo(
                        offset = endOffsets[tp]!!,
                        timestamp = -1,
                        leaderEpoch = null,
                    )
                )
            }
            futures[tp] = future
        }
        return ListOffsetsResult(futures)
    }

    override fun describeClientQuotas(
        filter: ClientQuotaFilter,
        options: DescribeClientQuotasOptions,
    ): DescribeClientQuotasResult {
        throw UnsupportedOperationException("Not implement yet")
    }

    override fun alterClientQuotas(
        entries: Collection<ClientQuotaAlteration>,
        options: AlterClientQuotasOptions,
    ): AlterClientQuotasResult {
        throw UnsupportedOperationException("Not implement yet")
    }

    override fun describeUserScramCredentials(
        users: List<String?>?,
        options: DescribeUserScramCredentialsOptions,
    ): DescribeUserScramCredentialsResult {
        throw UnsupportedOperationException("Not implemented yet")
    }

    override fun alterUserScramCredentials(
        alterations: List<UserScramCredentialAlteration>,
        options: AlterUserScramCredentialsOptions,
    ): AlterUserScramCredentialsResult {
        throw UnsupportedOperationException("Not implemented yet")
    }

    override fun describeMetadataQuorum(options: DescribeMetadataQuorumOptions): DescribeMetadataQuorumResult {
        throw UnsupportedOperationException("Not implemented yet")
    }

    override fun describeFeatures(options: DescribeFeaturesOptions): DescribeFeaturesResult {
        val finalizedFeatures = mutableMapOf<String, FinalizedVersionRange>()
        val supportedFeatures = mutableMapOf<String, SupportedVersionRange>()
        for ((key, value) in featureLevels) {
            finalizedFeatures[key] = FinalizedVersionRange(value, value)
            supportedFeatures[key] = SupportedVersionRange(
                minVersion = minSupportedFeatureLevels[key]!!,
                maxVersion = maxSupportedFeatureLevels[key]!!,
            )
        }
        return DescribeFeaturesResult(
            KafkaFuture.completedFuture(
                FeatureMetadata(
                    finalizedFeatures = finalizedFeatures,
                    finalizedFeaturesEpoch = 123L,
                    supportedFeatures = supportedFeatures,
                )
            )
        )
    }

    override fun updateFeatures(
        featureUpdates: Map<String, FeatureUpdate>,
        options: UpdateFeaturesOptions,
    ): UpdateFeaturesResult {
        val results: MutableMap<String, KafkaFuture<Unit>> = HashMap()
        for ((feature, value) in featureUpdates) {
            val future = KafkaFutureImpl<Unit>()
            try {
                var cur = featureLevels.getOrDefault(feature, 0)
                val next = value.maxVersionLevel
                val min = minSupportedFeatureLevels.getOrDefault(feature, 0)
                val max = maxSupportedFeatureLevels.getOrDefault(feature, 0)
                when (value.upgradeType) {
                    UpgradeType.UNKNOWN -> throw InvalidRequestException("Invalid upgrade type.")
                    UpgradeType.UPGRADE ->
                        if (cur > next) throw InvalidUpdateVersionException("Can't upgrade to lower version.")

                    UpgradeType.SAFE_DOWNGRADE ->
                        if (cur < next) throw InvalidUpdateVersionException("Can't downgrade to newer version.")

                    UpgradeType.UNSAFE_DOWNGRADE -> {
                        if (cur < next) throw InvalidUpdateVersionException("Can't downgrade to newer version.")

                        while (next != cur) {
                            // Simulate a scenario where all the even feature levels unsafe to downgrade from.
                            if (cur % 2 == 0) {
                                if (value.upgradeType === UpgradeType.SAFE_DOWNGRADE)
                                    throw InvalidUpdateVersionException("Unable to perform a safe downgrade.")
                            }
                            cur--
                        }
                    }
                }
                if (next < min) throw InvalidUpdateVersionException("Can't downgrade below $min")
                if (next > max) throw InvalidUpdateVersionException("Can't upgrade above $max")
                if (!options.validateOnly()) featureLevels[feature] = next
                future.complete(Unit)
            } catch (e: Exception) {
                future.completeExceptionally(e)
            }
            results[feature] = future
        }
        return UpdateFeaturesResult(results)
    }

    override fun unregisterBroker(brokerId: Int, options: UnregisterBrokerOptions): UnregisterBrokerResult {
        return if (usingRaftController) UnregisterBrokerResult((KafkaFuture.completedFuture(Unit)))
        else {
            val future = KafkaFutureImpl<Unit>()
            future.completeExceptionally(UnsupportedVersionException(""))
            UnregisterBrokerResult(future)
        }
    }

    override fun describeProducers(
        partitions: Collection<TopicPartition>,
        options: DescribeProducersOptions,
    ): DescribeProducersResult {
        throw UnsupportedOperationException("Not implemented yet")
    }

    override fun describeTransactions(
        transactionalIds: Collection<String>,
        options: DescribeTransactionsOptions,
    ): DescribeTransactionsResult {
        throw UnsupportedOperationException("Not implemented yet")
    }

    override fun abortTransaction(
        spec: AbortTransactionSpec,
        options: AbortTransactionOptions,
    ): AbortTransactionResult {
        throw UnsupportedOperationException("Not implemented yet")
    }

    override fun listTransactions(options: ListTransactionsOptions): ListTransactionsResult {
        throw UnsupportedOperationException("Not implemented yet")
    }

    override fun fenceProducers(
        transactionalIds: Collection<String>,
        options: FenceProducersOptions,
    ): FenceProducersResult {
        throw UnsupportedOperationException("Not implemented yet")
    }

    @Synchronized
    override fun close(timeout: Duration) {
    }

    @Synchronized
    fun updateBeginningOffsets(newOffsets: Map<TopicPartition, Long>) {
        beginningOffsets.putAll(newOffsets)
    }

    @Synchronized
    fun updateEndOffsets(newOffsets: Map<TopicPartition, Long>) {
        endOffsets.putAll(newOffsets)
    }

    @Synchronized
    fun updateConsumerGroupOffsets(newOffsets: Map<TopicPartition, Long>) {
        committedOffsets.putAll(newOffsets)
    }

    @Synchronized
    fun throwOnListConsumerGroupOffsets(exception: KafkaException?) {
        listConsumerGroupOffsetsException = exception
    }

    class Builder {

        private var clusterId = DEFAULT_CLUSTER_ID

        private var brokers: MutableList<Node> = ArrayList()

        private var controller: Node? = null

        private var brokerLogDirs: MutableList<List<String>> = ArrayList()

        private var defaultPartitions: Short? = null

        private var usingRaftController = false

        private var defaultReplicationFactor: Int? = null

        private var featureLevels = emptyMap<String, Short>()

        private var minSupportedFeatureLevels = emptyMap<String, Short>()

        private var maxSupportedFeatureLevels = emptyMap<String, Short>()

        init {
            numBrokers(1)
        }

        fun clusterId(clusterId: String): Builder {
            this.clusterId = clusterId
            return this
        }

        fun brokers(brokers: MutableList<Node>): Builder {
            numBrokers(brokers.size)
            this.brokers = brokers
            return this
        }

        fun numBrokers(numBrokers: Int): Builder {
            if (brokers.size >= numBrokers) {
                brokers = brokers.subList(0, numBrokers)
                brokerLogDirs = brokerLogDirs.subList(0, numBrokers)
            } else {
                for (id in brokers.size until numBrokers) {
                    brokers.add(Node(id, "localhost", 1000 + id))
                    brokerLogDirs.add(DEFAULT_LOG_DIRS)
                }
            }
            return this
        }

        fun controller(index: Int): Builder {
            controller = brokers[index]
            return this
        }

        fun brokerLogDirs(brokerLogDirs: MutableList<List<String>>): Builder {
            this.brokerLogDirs = brokerLogDirs
            return this
        }

        fun defaultReplicationFactor(defaultReplicationFactor: Int): Builder {
            this.defaultReplicationFactor = defaultReplicationFactor
            return this
        }

        fun usingRaftController(usingRaftController: Boolean): Builder {
            this.usingRaftController = usingRaftController
            return this
        }

        fun defaultPartitions(numPartitions: Short): Builder {
            defaultPartitions = numPartitions
            return this
        }

        fun featureLevels(featureLevels: Map<String, Short>): Builder {
            this.featureLevels = featureLevels
            return this
        }

        fun minSupportedFeatureLevels(minSupportedFeatureLevels: Map<String, Short>): Builder {
            this.minSupportedFeatureLevels = minSupportedFeatureLevels
            return this
        }

        fun maxSupportedFeatureLevels(maxSupportedFeatureLevels: Map<String, Short>): Builder {
            this.maxSupportedFeatureLevels = maxSupportedFeatureLevels
            return this
        }

        fun build(): MockAdminClient {
            return MockAdminClient(
                brokers = brokers,
                controller = controller ?: brokers[0],
                clusterId = clusterId,
                defaultPartitions = (defaultPartitions?.toInt() ?: 1).toInt(),
                defaultReplicationFactor = defaultReplicationFactor ?: min(brokers.size.toDouble(), 3.0).toInt(),
                brokerLogDirs = brokerLogDirs,
                usingRaftController = usingRaftController,
                featureLevels = featureLevels,
                minSupportedFeatureLevels = minSupportedFeatureLevels,
                maxSupportedFeatureLevels = maxSupportedFeatureLevels,
            )
        }
    }

    private class TopicMetadata(
        val topicId: Uuid,
        val isInternalTopic: Boolean,
        val partitions: List<TopicPartitionInfo>,
        val partitionLogDirs: List<String>,
        configs: Map<String, String?>?,
    ) {
        var configs: Map<String, String?>
        var fetchesRemainingUntilVisible = 0
        var markedForDeletion = false

        init {
            this.configs = configs ?: emptyMap<String, String>()
        }
    }

    @Synchronized
    fun setMockMetrics(name: MetricName, metric: Metric) {
        mockMetrics[name] = metric
    }

    @Synchronized
    override fun metrics(): Map<MetricName, Metric> {
        return mockMetrics
    }

    @Synchronized
    fun setFetchesRemainingUntilVisible(topicName: String, fetchesRemainingUntilVisible: Int) {
        val metadata = allTopics[topicName] ?: throw RuntimeException("No such topic as $topicName")
        metadata.fetchesRemainingUntilVisible = fetchesRemainingUntilVisible
    }

    @Synchronized
    fun brokers(): List<Node?> = brokers.toList()

    @Synchronized
    fun broker(index: Int): Node = brokers[index]

    companion object {

        val DEFAULT_CLUSTER_ID = "I4ZmrWqfT2e-upky_4fdPA"

        val DEFAULT_LOG_DIRS = listOf("/tmp/kafka-logs")

        fun create(): Builder = Builder()

        private fun toConfigObject(map: Map<String, String?>): Config =
            Config(map.map { (key, value) -> ConfigEntry(key, value) })
    }
}
