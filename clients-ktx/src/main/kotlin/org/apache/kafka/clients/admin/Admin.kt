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

import java.time.Duration
import java.util.*
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.ElectionType
import org.apache.kafka.common.Metric
import org.apache.kafka.common.MetricName
import org.apache.kafka.common.TopicCollection
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.TopicPartitionReplica
import org.apache.kafka.common.acl.AclBinding
import org.apache.kafka.common.acl.AclBindingFilter
import org.apache.kafka.common.annotation.InterfaceStability.Evolving
import org.apache.kafka.common.annotation.InterfaceStability.Unstable
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.quota.ClientQuotaAlteration
import org.apache.kafka.common.quota.ClientQuotaFilter

/**
 * The administrative client for Kafka, which supports managing and inspecting topics, brokers,
 * configurations and ACLs.
 *
 * Instances returned from the `create` methods of this interface are guaranteed to be thread safe.
 * However, the [KafkaFutures][KafkaFuture] returned from request methods are executed by a single
 * thread, so it is important that any code which executes on that thread when they complete (using
 * [KafkaFuture.thenApply], for example) doesn't block for too long. If necessary, processing of
 * results should be passed to another thread.
 *
 * The operations exposed by Admin follow a consistent pattern:
 *
 *  * Admin instances should be created using [Admin.create] or [Admin.create]
 *  * Each operation typically has two overloaded methods, one which uses a default set of options
 *  and an overloaded method where the last parameter is an explicit options object.
 *  * The operation method's first parameter is a `Collection` of items to perform the operation on.
 *  Batching multiple requests into a single call is more efficient and should be preferred over
 *  multiple calls to the same method.
 *  * The operation methods execute asynchronously.
 *  * Each `xxx` operation method returns an `XxxResult` class with methods which expose
 *  [KafkaFuture] for accessing the result(s) of the operation.
 *  * Typically an `all()` method is provided for getting the overall success/failure of the batch
 *  and a `values()` method provided access to each item in a request batch.
 * Other methods may also be provided.
 *  * For synchronous behaviour use [KafkaFuture.get]
 *
 * Here is a simple example of using an Admin client instance to create a new topic:
 * <pre>
 * `Properties props = new Properties();
 * props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
 *
 * try (Admin admin = Admin.create(props)) {
 * String topicName = "my-topic";
 * int partitions = 12;
 * short replicationFactor = 3;
 * // Create a compacted topic
 * CreateTopicsResult result = admin.createTopics(Collections.singleton(
 * new NewTopic(topicName, partitions, replicationFactor)
 * .configs(Collections.singletonMap(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT))));
 *
 * // Call values() to get the result for a specific topic
 * KafkaFuture<Void> future = result.values().get(topicName);
 *
 * // Call get() to block until the topic creation is complete or has failed
 * // if creation failed the ExecutionException wraps the underlying cause.
 * future.get();
 * }
 * </pre>
 *
 * <h3>Bootstrap and balancing</h3>
 *
 * The `bootstrap.servers` config in the `Map` or `Properties` passed
 * to [Admin.create] is only used for discovering the brokers in the cluster,
 * which the client will then connect to as needed.
 * As such, it is sufficient to include only two or three broker addresses to cope with the possibility of brokers
 * being unavailable.
 *
 * Different operations necessitate requests being sent to different nodes in the cluster. For example
 * [.createTopics] communicates with the controller, but [.describeTopics]
 * can talk to any broker. When the recipient does not matter the instance will try to use the broker with the
 * fewest outstanding requests.
 *
 * The client will transparently retry certain errors which are usually transient.
 * For example if the request for `createTopics()` get sent to a node which was not the controller
 * the metadata would be refreshed and the request re-sent to the controller.
 *
 * <h3>Broker Compatibility</h3>
 *
 * The minimum broker version required is 0.10.0.0. Methods with stricter requirements will specify the minimum broker
 * version required.
 *
 * This client was introduced in 0.11.0.0 and the API is still evolving. We will try to evolve the API in a compatible
 * manner, but we reserve the right to make breaking changes in minor releases, if necessary. We will update the
 * `InterfaceStability` annotation and this notice once the API is considered stable.
 */
@Evolving
interface Admin : AutoCloseable {

    /**
     * Close the Admin and release all associated resources.
     */
    override fun close() = close(Duration.ofMillis(Long.MAX_VALUE))

    /**
     * Close the Admin client and release all associated resources.
     *
     * The close operation has a grace period during which current operations will be allowed to
     * complete, specified by the given duration.
     * New operations will not be accepted during the grace period. Once the grace period is over,
     * all operations that have not yet been completed will be aborted with a
     * [org.apache.kafka.common.errors.TimeoutException].
     *
     * @param timeout The time to use for the wait time.
     */
    fun close(timeout: Duration)

    /**
     * Create a batch of new topics with the default options.
     *
     * This is a convenience method for [.createTopics] with default options.
     * See the overload for more details.
     *
     * This operation is supported by brokers with version 0.10.1.0 or higher.
     *
     * @param newTopics The new topics to create.
     * @return The CreateTopicsResult.
     */
    fun createTopics(newTopics: Collection<NewTopic>): CreateTopicsResult {
        return createTopics(newTopics, CreateTopicsOptions())
    }

    /**
     * Create a batch of new topics.
     *
     * This operation is not transactional so it may succeed for some topics while fail for others.
     *
     * It may take several seconds after [CreateTopicsResult] returns
     * success for all the brokers to become aware that the topics have been created.
     * During this time, [.listTopics] and [.describeTopics]
     * may not return information about the new topics.
     *
     * This operation is supported by brokers with version 0.10.1.0 or higher. The validateOnly
     * option is supported from version 0.10.2.0.
     *
     * @param newTopics The new topics to create.
     * @param options   The options to use when creating the new topics.
     * @return The CreateTopicsResult.
     */
    fun createTopics(newTopics: Collection<NewTopic>, options: CreateTopicsOptions): CreateTopicsResult

    /**
     * This is a convenience method for [.deleteTopics]
     * with default options. See the overload for more details.
     *
     * This operation is supported by brokers with version 0.10.1.0 or higher.
     *
     * @param topics The topic names to delete.
     * @return The DeleteTopicsResult.
     */
    fun deleteTopics(topics: Collection<String>): DeleteTopicsResult {
        return deleteTopics(TopicCollection.ofTopicNames(topics), DeleteTopicsOptions())
    }

    /**
     * This is a convenience method for [.deleteTopics]
     * with default options. See the overload for more details.
     *
     * This operation is supported by brokers with version 0.10.1.0 or higher.
     *
     * @param topics  The topic names to delete.
     * @param options The options to use when deleting the topics.
     * @return The DeleteTopicsResult.
     */
    fun deleteTopics(topics: Collection<String>, options: DeleteTopicsOptions): DeleteTopicsResult {
        return deleteTopics(TopicCollection.ofTopicNames(topics), options)
    }

    /**
     * This is a convenience method for [.deleteTopics]
     * with default options. See the overload for more details.
     *
     * When using topic IDs, this operation is supported by brokers with inter-broker protocol 2.8
     * or higher. When using topic names, this operation is supported by brokers with version
     * 0.10.1.0 or higher.
     *
     * @param topics The topics to delete.
     * @return The DeleteTopicsResult.
     */
    fun deleteTopics(topics: TopicCollection): DeleteTopicsResult {
        return deleteTopics(topics, DeleteTopicsOptions())
    }

    /**
     * Delete a batch of topics.
     *
     * This operation is not transactional so it may succeed for some topics while fail for others.
     *
     * It may take several seconds after the [DeleteTopicsResult] returns
     * success for all the brokers to become aware that the topics are gone.
     * During this time, [.listTopics] and [.describeTopics]
     * may continue to return information about the deleted topics.
     *
     * If delete.topic.enable is false on the brokers, deleteTopics will mark
     * the topics for deletion, but not actually delete them. The futures will
     * return successfully in this case.
     *
     * When using topic IDs, this operation is supported by brokers with inter-broker protocol 2.8
     * or higher. When using topic names, this operation is supported by brokers with version
     * 0.10.1.0 or higher.
     *
     * @param topics  The topics to delete.
     * @param options The options to use when deleting the topics.
     * @return The DeleteTopicsResult.
     */
    fun deleteTopics(topics: TopicCollection, options: DeleteTopicsOptions): DeleteTopicsResult

    /**
     * List the topics available in the cluster with the default options.
     *
     * This is a convenience method for [.listTopics] with default options.
     * See the overload for more details.
     *
     * @return The ListTopicsResult.
     */
    fun listTopics(): ListTopicsResult {
        return listTopics(ListTopicsOptions())
    }

    /**
     * List the topics available in the cluster.
     *
     * @param options The options to use when listing the topics.
     * @return The ListTopicsResult.
     */
    fun listTopics(options: ListTopicsOptions): ListTopicsResult

    /**
     * Describe some topics in the cluster.
     *
     * @param topicNames The names of the topics to describe.
     * @param options    The options to use when describing the topic.
     * @return The DescribeTopicsResult.
     */
    fun describeTopics(
        topicNames: Collection<String>,
        options: DescribeTopicsOptions = DescribeTopicsOptions(),
    ): DescribeTopicsResult {
        return describeTopics(TopicCollection.ofTopicNames(topicNames), options)
    }

    /**
     * This is a convenience method for [.describeTopics]
     * with default options. See the overload for more details.
     *
     * When using topic IDs, this operation is supported by brokers with version 3.1.0 or higher.
     *
     * @param topics The topics to describe.
     * @return The DescribeTopicsResult.
     */
    fun describeTopics(topics: TopicCollection): DescribeTopicsResult {
        return describeTopics(topics, DescribeTopicsOptions())
    }

    /**
     * Describe some topics in the cluster.
     *
     * When using topic IDs, this operation is supported by brokers with version 3.1.0 or higher.
     *
     * @param topics  The topics to describe.
     * @param options The options to use when describing the topics.
     * @return The DescribeTopicsResult.
     */
    fun describeTopics(
        topics: TopicCollection,
        options: DescribeTopicsOptions,
    ): DescribeTopicsResult

    /**
     * Get information about the nodes in the cluster, using the default options.
     *
     * This is a convenience method for [.describeCluster] with default options.
     * See the overload for more details.
     *
     * @return The DescribeClusterResult.
     */
    fun describeCluster(): DescribeClusterResult {
        return describeCluster(DescribeClusterOptions())
    }

    /**
     * Get information about the nodes in the cluster.
     *
     * @param options The options to use when getting information about the cluster.
     * @return The DescribeClusterResult.
     */
    fun describeCluster(options: DescribeClusterOptions): DescribeClusterResult

    /**
     * This is a convenience method for [.describeAcls] with
     * default options. See the overload for more details.
     *
     * This operation is supported by brokers with version 0.11.0.0 or higher.
     *
     * @param filter The filter to use.
     * @return The DescribeAclsResult.
     */
    fun describeAcls(filter: AclBindingFilter): DescribeAclsResult {
        return describeAcls(filter, DescribeAclsOptions())
    }

    /**
     * Lists access control lists (ACLs) according to the supplied filter.
     *
     * Note: it may take some time for changes made by `createAcls` or `deleteAcls` to be reflected
     * in the output of `describeAcls`.
     *
     * This operation is supported by brokers with version 0.11.0.0 or higher.
     *
     * @param filter  The filter to use.
     * @param options The options to use when listing the ACLs.
     * @return The DescribeAclsResult.
     */
    fun describeAcls(filter: AclBindingFilter, options: DescribeAclsOptions): DescribeAclsResult

    /**
     * This is a convenience method for [.createAcls] with
     * default options. See the overload for more details.
     *
     * This operation is supported by brokers with version 0.11.0.0 or higher.
     *
     * @param acls The ACLs to create
     * @return The CreateAclsResult.
     */
    fun createAcls(acls: Collection<AclBinding>): CreateAclsResult {
        return createAcls(acls, CreateAclsOptions())
    }

    /**
     * Creates access control lists (ACLs) which are bound to specific resources.
     *
     * This operation is not transactional so it may succeed for some ACLs while fail for others.
     *
     * If you attempt to add an ACL that duplicates an existing ACL, no error will be raised, but
     * no changes will be made.
     *
     * This operation is supported by brokers with version 0.11.0.0 or higher.
     *
     * @param acls    The ACLs to create
     * @param options The options to use when creating the ACLs.
     * @return The CreateAclsResult.
     */
    fun createAcls(acls: Collection<AclBinding>, options: CreateAclsOptions): CreateAclsResult

    /**
     * This is a convenience method for [.deleteAcls] with default options.
     * See the overload for more details.
     *
     * This operation is supported by brokers with version 0.11.0.0 or higher.
     *
     * @param filters The filters to use.
     * @return The DeleteAclsResult.
     */
    fun deleteAcls(filters: Collection<AclBindingFilter>): DeleteAclsResult {
        return deleteAcls(filters, DeleteAclsOptions())
    }

    /**
     * Deletes access control lists (ACLs) according to the supplied filters.
     *
     * This operation is not transactional so it may succeed for some ACLs while fail for others.
     *
     * This operation is supported by brokers with version 0.11.0.0 or higher.
     *
     * @param filters The filters to use.
     * @param options The options to use when deleting the ACLs.
     * @return The DeleteAclsResult.
     */
    fun deleteAcls(filters: Collection<AclBindingFilter>, options: DeleteAclsOptions): DeleteAclsResult

    /**
     * Get the configuration for the specified resources with the default options.
     *
     * This is a convenience method for [.describeConfigs] with default options.
     * See the overload for more details.
     *
     * This operation is supported by brokers with version 0.11.0.0 or higher.
     *
     * @param resources The resources (topic and broker resource types are currently supported)
     * @return The DescribeConfigsResult
     */
    fun describeConfigs(resources: Collection<ConfigResource>): DescribeConfigsResult {
        return describeConfigs(resources, DescribeConfigsOptions())
    }

    /**
     * Get the configuration for the specified resources.
     *
     * The returned configuration includes default values and the isDefault() method can be used to
     * distinguish them from user supplied values.
     *
     * The value of config entries where isSensitive() is true is always `null` so that sensitive
     * information is not disclosed.
     *
     * Config entries where isReadOnly() is true cannot be updated.
     *
     * This operation is supported by brokers with version 0.11.0.0 or higher.
     *
     * @param resources The resources (topic and broker resource types are currently supported)
     * @param options   The options to use when describing configs
     * @return The DescribeConfigsResult
     */
    fun describeConfigs(
        resources: Collection<ConfigResource>,
        options: DescribeConfigsOptions
    ): DescribeConfigsResult

    /**
     * Update the configuration for the specified resources with the default options.
     *
     * This is a convenience method for [.alterConfigs] with default options.
     * See the overload for more details.
     *
     * This operation is supported by brokers with version 0.11.0.0 or higher.
     *
     * @param configs The resources with their configs (topic is the only resource type with configs
     * that can be updated currently)
     *
     * @return The AlterConfigsResult
     */
    @Deprecated("Since 2.3. Use {@link #incrementalAlterConfigs(Map)}.")
    fun alterConfigs(configs: Map<ConfigResource, Config>): AlterConfigsResult {
        return alterConfigs(configs, AlterConfigsOptions())
    }

    /**
     * Update the configuration for the specified resources with the default options.
     *
     * Updates are not transactional, so they may succeed for some resources while fail for others.
     * The configs for a particular resource are updated atomically.
     *
     * This operation is supported by brokers with version 0.11.0.0 or higher.
     *
     * @param configs The resources with their configs (topic is the only resource type with configs
     * that can be updated currently)
     *
     * @param options The options to use when describing configs
     * @return The AlterConfigsResult
     */
    @Deprecated("Since 2.3. Use {@link #incrementalAlterConfigs(Map, AlterConfigsOptions)}.")
    fun alterConfigs(configs: Map<ConfigResource, Config>, options: AlterConfigsOptions): AlterConfigsResult

    /**
     * Incrementally updates the configuration for the specified resources with default options.
     *
     * This is a convenience method for [.incrementalAlterConfigs] with default options.
     * See the overload for more details.
     *
     * This operation is supported by brokers with version 2.3.0 or higher.
     *
     * @param configs The resources with their configs
     * @return The AlterConfigsResult
     */
    fun incrementalAlterConfigs(
        configs: Map<ConfigResource, Collection<AlterConfigOp>>,
    ): AlterConfigsResult {
        return incrementalAlterConfigs(configs, AlterConfigsOptions())
    }

    /**
     * Incrementally update the configuration for the specified resources.
     *
     * Updates are not transactional so they may succeed for some resources while fail for others.
     * The configs for a particular resource are updated atomically.
     *
     * The following exceptions can be anticipated when calling `get()` on the futures obtained from
     * the returned [AlterConfigsResult]:
     *
     *  * [org.apache.kafka.common.errors.ClusterAuthorizationException]
     * if the authenticated user didn't have alter access to the cluster.
     *  * [org.apache.kafka.common.errors.TopicAuthorizationException]
     * if the authenticated user didn't have alter access to the Topic.
     *  * [org.apache.kafka.common.errors.UnknownTopicOrPartitionException]
     * if the Topic doesn't exist.
     *  * [org.apache.kafka.common.errors.InvalidRequestException]
     * if the request details are invalid. e.g., a configuration key was specified more than once
     * for a resource
     *
     * This operation is supported by brokers with version 2.3.0 or higher.
     *
     * @param configs The resources with their configs
     * @param options The options to use when altering configs
     * @return The AlterConfigsResult
     */
    fun incrementalAlterConfigs(
        configs: Map<ConfigResource, Collection<AlterConfigOp>>,
        options: AlterConfigsOptions
    ): AlterConfigsResult

    /**
     * Change the log directory for the specified replicas. If the replica does not exist on the
     * broker, the result shows REPLICA_NOT_AVAILABLE for the given replica and the replica will be
     * created in the given log directory on the broker when it is created later. If the replica
     * already exists on the broker, the replica will be moved to the given log directory if it is
     * not already there. For detailed result, inspect the returned [AlterReplicaLogDirsResult]
     * instance.
     *
     * This operation is not transactional so it may succeed for some replicas while fail for others.
     *
     * This is a convenience method for [.alterReplicaLogDirs] with default options.
     * See the overload for more details.
     *
     * This operation is supported by brokers with version 1.1.0 or higher.
     *
     * @param replicaAssignment     The replicas with their log directory absolute path
     * @return                      The AlterReplicaLogDirsResult
     */
    fun alterReplicaLogDirs(
        replicaAssignment: Map<TopicPartitionReplica, String>,
    ): AlterReplicaLogDirsResult {
        return alterReplicaLogDirs(replicaAssignment, AlterReplicaLogDirsOptions())
    }

    /**
     * Change the log directory for the specified replicas. If the replica does not exist on the
     * broker, the result shows REPLICA_NOT_AVAILABLE for the given replica and the replica will be
     * created in the given log directory on the broker when it is created later. If the replica
     * already exists on the broker, the replica will be moved to the given log directory if it is
     * not already there. For detailed result, inspect the returned [AlterReplicaLogDirsResult]
     * instance.
     *
     * This operation is not transactional so it may succeed for some replicas while fail for others.
     *
     * This operation is supported by brokers with version 1.1.0 or higher.
     *
     * @param replicaAssignment     The replicas with their log directory absolute path
     * @param options               The options to use when changing replica dir
     * @return                      The AlterReplicaLogDirsResult
     */
    fun alterReplicaLogDirs(
        replicaAssignment: Map<TopicPartitionReplica, String>,
        options: AlterReplicaLogDirsOptions
    ): AlterReplicaLogDirsResult

    /**
     * Query the information of all log directories on the given set of brokers
     *
     * This is a convenience method for [.describeLogDirs] with default options.
     * See the overload for more details.
     *
     * This operation is supported by brokers with version 1.0.0 or higher.
     *
     * @param brokers A list of brokers
     * @return The DescribeLogDirsResult
     */
    fun describeLogDirs(brokers: Collection<Int>): DescribeLogDirsResult {
        return describeLogDirs(brokers, DescribeLogDirsOptions())
    }

    /**
     * Query the information of all log directories on the given set of brokers
     *
     * This operation is supported by brokers with version 1.0.0 or higher.
     *
     * @param brokers A list of brokers
     * @param options The options to use when querying log dir info
     * @return The DescribeLogDirsResult
     */
    fun describeLogDirs(
        brokers: Collection<Int>,
        options: DescribeLogDirsOptions,
    ): DescribeLogDirsResult

    /**
     * Query the replica log directory information for the specified replicas.
     *
     * This is a convenience method for [.describeReplicaLogDirs]
     * with default options. See the overload for more details.
     *
     * This operation is supported by brokers with version 1.0.0 or higher.
     *
     * @param replicas The replicas to query
     * @return The DescribeReplicaLogDirsResult
     */
    fun describeReplicaLogDirs(
        replicas: Collection<TopicPartitionReplica>,
    ): DescribeReplicaLogDirsResult {
        return describeReplicaLogDirs(replicas, DescribeReplicaLogDirsOptions())
    }

    /**
     * Query the replica log directory information for the specified replicas.
     *
     * This operation is supported by brokers with version 1.0.0 or higher.
     *
     * @param replicas The replicas to query
     * @param options  The options to use when querying replica log dir info
     * @return The DescribeReplicaLogDirsResult
     */
    fun describeReplicaLogDirs(
        replicas: Collection<TopicPartitionReplica>,
        options: DescribeReplicaLogDirsOptions
    ): DescribeReplicaLogDirsResult

    /**
     * Increase the number of partitions of the topics given as the keys of `newPartitions`
     * according to the corresponding values. **If partitions are increased for a topic that has a
     * key, the partition logic or ordering of the messages will be affected.**
     *
     * This is a convenience method for [.createPartitions] with default options.
     * See the overload for more details.
     *
     * @param newPartitions The topics which should have new partitions created, and corresponding
     * parameters for the created partitions.
     *
     * @return The CreatePartitionsResult.
     */
    fun createPartitions(newPartitions: Map<String, NewPartitions>): CreatePartitionsResult {
        return createPartitions(newPartitions, CreatePartitionsOptions())
    }

    /**
     * Increase the number of partitions of the topics given as the keys of `newPartitions`
     * according to the corresponding values. **If partitions are increased for a topic that has a
     * key, the partition logic or ordering of the messages will be affected.**
     *
     * This operation is not transactional so it may succeed for some topics while fail for others.
     *
     * It may take several seconds after this method returns
     * success for all the brokers to become aware that the partitions have been created.
     * During this time, [.describeTopics]
     * may not return information about the new partitions.
     *
     * This operation is supported by brokers with version 1.0.0 or higher.
     *
     * The following exceptions can be anticipated when calling `get()` on the futures obtained from
     * the [values()][CreatePartitionsResult.values] method of the returned [CreatePartitionsResult]
     *
     *  * [org.apache.kafka.common.errors.AuthorizationException]
     * if the authenticated user is not authorized to alter the topic
     *  * [org.apache.kafka.common.errors.TimeoutException]
     * if the request was not completed in within the given [CreatePartitionsOptions.timeoutMs].
     *  * [org.apache.kafka.common.errors.ReassignmentInProgressException]
     * if a partition reassignment is currently in progress
     *  * [org.apache.kafka.common.errors.BrokerNotAvailableException]
     * if the requested [NewPartitions.assignments] contain a broker that is currently unavailable.
     *  * [org.apache.kafka.common.errors.InvalidReplicationFactorException]
     * if no [NewPartitions.assignments] are given and it is impossible for the broker to assign
     * replicas with the topics replication factor.
     *  * Subclasses of [org.apache.kafka.common.KafkaException]
     * if the request is invalid in some way.
     *
     * @param newPartitions The topics which should have new partitions created, and corresponding
     * parameters for the created partitions.
     * @param options       The options to use when creating the new partitions.
     * @return The CreatePartitionsResult.
     */
    fun createPartitions(
        newPartitions: Map<String, NewPartitions>,
        options: CreatePartitionsOptions
    ): CreatePartitionsResult

    /**
     * Delete records whose offset is smaller than the given offset of the corresponding partition.
     *
     * This is a convenience method for [.deleteRecords] with default options.
     * See the overload for more details.
     *
     * This operation is supported by brokers with version 0.11.0.0 or higher.
     *
     * @param recordsToDelete The topic partitions and related offsets from which records deletion
     * starts.
     * @return The DeleteRecordsResult.
     */
    fun deleteRecords(recordsToDelete: Map<TopicPartition, RecordsToDelete>): DeleteRecordsResult {
        return deleteRecords(recordsToDelete, DeleteRecordsOptions())
    }

    /**
     * Delete records whose offset is smaller than the given offset of the corresponding partition.
     *
     * This operation is supported by brokers with version 0.11.0.0 or higher.
     *
     * @param recordsToDelete The topic partitions and related offsets from which records deletion
     * starts.
     * @param options The options to use when deleting records.
     * @return The DeleteRecordsResult.
     */
    fun deleteRecords(
        recordsToDelete: Map<TopicPartition, RecordsToDelete>,
        options: DeleteRecordsOptions
    ): DeleteRecordsResult

    /**
     * Create a Delegation Token.
     *
     * This is a convenience method for [.createDelegationToken] with default options.
     * See the overload for more details.
     *
     * @return The CreateDelegationTokenResult.
     */
    fun createDelegationToken(): CreateDelegationTokenResult {
        return createDelegationToken(CreateDelegationTokenOptions())
    }

    /**
     * Create a Delegation Token.
     *
     * This operation is supported by brokers with version 1.1.0 or higher.
     *
     * The following exceptions can be anticipated when calling `get()` on the futures obtained from
     * the [delegationToken()][CreateDelegationTokenResult.delegationToken] method of the returned
     * [CreateDelegationTokenResult]
     *
     *  * [org.apache.kafka.common.errors.UnsupportedByAuthenticationException]
     * If the request sent on PLAINTEXT/1-way SSL channels or delegation token authenticated
     * channels.
     *  * [org.apache.kafka.common.errors.InvalidPrincipalTypeException]
     * if the renewers principal type is not supported.
     *  * [org.apache.kafka.common.errors.DelegationTokenDisabledException]
     * if the delegation token feature is disabled.
     *  * [org.apache.kafka.common.errors.TimeoutException]
     * if the request was not completed in within the given [CreateDelegationTokenOptions.timeoutMs].
     *
     *
     * @param options The options to use when creating delegation token.
     * @return The DeleteRecordsResult.
     */
    fun createDelegationToken(options: CreateDelegationTokenOptions): CreateDelegationTokenResult

    /**
     * Renew a Delegation Token.
     *
     * This is a convenience method for [.renewDelegationToken] with default options.
     * See the overload for more details.
     *
     * @param hmac HMAC of the Delegation token
     * @return The RenewDelegationTokenResult.
     */
    fun renewDelegationToken(hmac: ByteArray): RenewDelegationTokenResult {
        return renewDelegationToken(hmac, RenewDelegationTokenOptions())
    }

    /**
     * Renew a Delegation Token.
     *
     * This operation is supported by brokers with version 1.1.0 or higher.
     *
     * The following exceptions can be anticipated when calling `get()` on the futures obtained from
     * the [expiryTimestamp()][RenewDelegationTokenResult.expiryTimestamp] method of the returned
     * [RenewDelegationTokenResult]
     *
     *  * [org.apache.kafka.common.errors.UnsupportedByAuthenticationException]
     * If the request sent on PLAINTEXT/1-way SSL channels or delegation token authenticated channels.
     *  * [org.apache.kafka.common.errors.DelegationTokenDisabledException]
     * if the delegation token feature is disabled.
     *  * [org.apache.kafka.common.errors.DelegationTokenNotFoundException]
     * if the delegation token is not found on server.
     *  * [org.apache.kafka.common.errors.DelegationTokenOwnerMismatchException]
     * if the authenticated user is not owner/renewer of the token.
     *  * [org.apache.kafka.common.errors.DelegationTokenExpiredException]
     * if the delegation token is expired.
     *  * [org.apache.kafka.common.errors.TimeoutException]
     * if the request was not completed in within the given [RenewDelegationTokenOptions.timeoutMs].
     *
     * @param hmac    HMAC of the Delegation token
     * @param options The options to use when renewing delegation token.
     * @return The RenewDelegationTokenResult.
     */
    fun renewDelegationToken(hmac: ByteArray, options: RenewDelegationTokenOptions): RenewDelegationTokenResult

    /**
     * Expire a Delegation Token.
     *
     * This is a convenience method for [.expireDelegationToken] with default options.
     * This will expire the token immediately. See the overload for more details.
     *
     * @param hmac HMAC of the Delegation token
     * @return The ExpireDelegationTokenResult.
     */
    fun expireDelegationToken(hmac: ByteArray): ExpireDelegationTokenResult {
        return expireDelegationToken(hmac, ExpireDelegationTokenOptions())
    }

    /**
     * Expire a Delegation Token.
     *
     * This operation is supported by brokers with version 1.1.0 or higher.
     *
     * The following exceptions can be anticipated when calling `get()` on the futures obtained from
     * the [expiryTimestamp()][ExpireDelegationTokenResult.expiryTimestamp] method of the returned
     * [ExpireDelegationTokenResult]
     *
     *  * [org.apache.kafka.common.errors.UnsupportedByAuthenticationException]
     * If the request sent on PLAINTEXT/1-way SSL channels or delegation token authenticated channels.
     *  * [org.apache.kafka.common.errors.DelegationTokenDisabledException]
     * if the delegation token feature is disabled.
     *  * [org.apache.kafka.common.errors.DelegationTokenNotFoundException]
     * if the delegation token is not found on server.
     *  * [org.apache.kafka.common.errors.DelegationTokenOwnerMismatchException]
     * if the authenticated user is not owner/renewer of the requested token.
     *  * [org.apache.kafka.common.errors.DelegationTokenExpiredException]
     * if the delegation token is expired.
     *  * [org.apache.kafka.common.errors.TimeoutException]
     * if the request was not completed in within the given [ExpireDelegationTokenOptions.timeoutMs].
     *
     * @param hmac    HMAC of the Delegation token
     * @param options The options to use when expiring delegation token.
     * @return The ExpireDelegationTokenResult.
     */
    fun expireDelegationToken(
        hmac: ByteArray,
        options: ExpireDelegationTokenOptions,
    ): ExpireDelegationTokenResult

    /**
     * Describe the Delegation Tokens.
     *
     * This is a convenience method for [.describeDelegationToken] with default options.
     * This will return all the user owned tokens and tokens where user have Describe permission.
     * See the overload for more details.
     *
     * @return The DescribeDelegationTokenResult.
     */
    fun describeDelegationToken(): DescribeDelegationTokenResult {
        return describeDelegationToken(DescribeDelegationTokenOptions())
    }

    /**
     * Describe the Delegation Tokens.
     *
     * This operation is supported by brokers with version 1.1.0 or higher.
     *
     * The following exceptions can be anticipated when calling `get()` on the futures obtained from
     * the [delegationTokens()][DescribeDelegationTokenResult.delegationTokens] method of the
     * returned [DescribeDelegationTokenResult]
     *
     *  * [org.apache.kafka.common.errors.UnsupportedByAuthenticationException]
     * If the request sent on PLAINTEXT/1-way SSL channels or delegation token authenticated channels.
     *  * [org.apache.kafka.common.errors.DelegationTokenDisabledException]
     * if the delegation token feature is disabled.
     *  * [org.apache.kafka.common.errors.TimeoutException]
     * if the request was not completed in within the given [DescribeDelegationTokenOptions.timeoutMs].
     *
     *
     * @param options The options to use when describing delegation tokens.
     * @return The DescribeDelegationTokenResult.
     */
    fun describeDelegationToken(options: DescribeDelegationTokenOptions): DescribeDelegationTokenResult

    /**
     * Describe some group IDs in the cluster.
     *
     * @param groupIds The IDs of the groups to describe.
     * @param options  The options to use when describing the groups.
     * @return The DescribeConsumerGroupResult.
     */
    fun describeConsumerGroups(
        groupIds: Collection<String>,
        options: DescribeConsumerGroupsOptions
    ): DescribeConsumerGroupsResult

    /**
     * Describe some group IDs in the cluster, with the default options.
     *
     * This is a convenience method for [.describeConsumerGroups]
     * with default options. See the overload for more details.
     *
     * @param groupIds The IDs of the groups to describe.
     * @return The DescribeConsumerGroupResult.
     */
    fun describeConsumerGroups(groupIds: Collection<String>): DescribeConsumerGroupsResult {
        return describeConsumerGroups(groupIds, DescribeConsumerGroupsOptions())
    }

    /**
     * List the consumer groups available in the cluster.
     *
     * @param options The options to use when listing the consumer groups.
     * @return The ListGroupsResult.
     */
    fun listConsumerGroups(options: ListConsumerGroupsOptions): ListConsumerGroupsResult

    /**
     * List the consumer groups available in the cluster with the default options.
     *
     * This is a convenience method for [.listConsumerGroups] with default options.
     * See the overload for more details.
     *
     * @return The ListGroupsResult.
     */
    fun listConsumerGroups(): ListConsumerGroupsResult {
        return listConsumerGroups(ListConsumerGroupsOptions())
    }

    /**
     * List the consumer group offsets available in the cluster.
     *
     * @param options The options to use when listing the consumer group offsets.
     * @return The ListGroupOffsetsResult
     */
    fun listConsumerGroupOffsets(
        groupId: String,
        options: ListConsumerGroupOffsetsOptions = ListConsumerGroupOffsetsOptions()
    ): ListConsumerGroupOffsetsResult {
        @Suppress("deprecation")
        val groupSpec = ListConsumerGroupOffsetsSpec().topicPartitions(options.topicPartitions())

        // We can use the provided options with the batched API, which uses topic partitions from
        // the group spec and ignores any topic partitions set in the options.
        return listConsumerGroupOffsets(Collections.singletonMap(groupId, groupSpec), options)
    }

    /**
     * List the consumer group offsets available in the cluster for the specified consumer groups.
     *
     * @param groupSpecs Map of consumer group ids to a spec that specifies the topic partitions of the group to list
     * offsets for.
     *
     * @param options The options to use when listing the consumer group offsets.
     * @return The ListConsumerGroupOffsetsResult
     */
    fun listConsumerGroupOffsets(
        groupSpecs: Map<String, ListConsumerGroupOffsetsSpec>,
        options: ListConsumerGroupOffsetsOptions
    ): ListConsumerGroupOffsetsResult

    /**
     * List the consumer group offsets available in the cluster for the specified groups with the default options.
     *
     * This is a convenience method for
     * [.listConsumerGroupOffsets] with default options.
     *
     * @param groupSpecs Map of consumer group ids to a spec that specifies the topic partitions of the group to list
     * offsets for.
     * @return The ListConsumerGroupOffsetsResult.
     */
    fun listConsumerGroupOffsets(
        groupSpecs: Map<String, ListConsumerGroupOffsetsSpec>,
    ): ListConsumerGroupOffsetsResult {
        return listConsumerGroupOffsets(groupSpecs, ListConsumerGroupOffsetsOptions())
    }

    /**
     * Delete consumer groups from the cluster.
     *
     * @param options The options to use when deleting a consumer group.
     * @return The DeletConsumerGroupResult.
     */
    fun deleteConsumerGroups(
        groupIds: Collection<String>,
        options: DeleteConsumerGroupsOptions
    ): DeleteConsumerGroupsResult

    /**
     * Delete consumer groups from the cluster with the default options.
     *
     * @return The DeleteConsumerGroupResult.
     */
    fun deleteConsumerGroups(groupIds: Collection<String>): DeleteConsumerGroupsResult {
        return deleteConsumerGroups(groupIds, DeleteConsumerGroupsOptions())
    }

    /**
     * Delete committed offsets for a set of partitions in a consumer group. This will
     * succeed at the partition level only if the group is not actively subscribed
     * to the corresponding topic.
     *
     * @param options The options to use when deleting offsets in a consumer group.
     * @return The DeleteConsumerGroupOffsetsResult.
     */
    fun deleteConsumerGroupOffsets(
        groupId: String,
        partitions: Set<TopicPartition>,
        options: DeleteConsumerGroupOffsetsOptions
    ): DeleteConsumerGroupOffsetsResult

    /**
     * Delete committed offsets for a set of partitions in a consumer group with the default
     * options. This will succeed at the partition level only if the group is not actively
     * subscribed to the corresponding topic.
     *
     * @return The DeleteConsumerGroupOffsetsResult.
     */
    fun deleteConsumerGroupOffsets(
        groupId: String,
        partitions: Set<TopicPartition>
    ): DeleteConsumerGroupOffsetsResult {
        return deleteConsumerGroupOffsets(groupId, partitions, DeleteConsumerGroupOffsetsOptions())
    }

    /**
     * Elect a replica as leader for topic partitions.
     *
     * This is a convenience method for [.electLeaders]
     * with default options.
     *
     * @param electionType The type of election to conduct.
     * @param partitions   The topics and partitions for which to conduct elections.
     * @return The ElectLeadersResult.
     */
    fun electLeaders(electionType: ElectionType, partitions: Set<TopicPartition>): ElectLeadersResult {
        return electLeaders(electionType, partitions, ElectLeadersOptions())
    }

    /**
     * Elect a replica as leader for the given `partitions`, or for all partitions if the argument
     * to `partitions` is null.
     *
     * This operation is not transactional so it may succeed for some partitions while fail for others.
     *
     * It may take several seconds after this method returns success for all the brokers in the cluster
     * to become aware that the partitions have new leaders. During this time,
     * [.describeTopics] may not return information about the partitions'
     * new leaders.
     *
     * This operation is supported by brokers with version 2.2.0 or later if preferred election is use;
     * otherwise the brokers most be 2.4.0 or higher.
     *
     * The following exceptions can be anticipated when calling `get()` on the future obtained
     * from the returned [ElectLeadersResult]:
     *
     *  * [org.apache.kafka.common.errors.ClusterAuthorizationException]
     * if the authenticated user didn't have alter access to the cluster.
     *  * [org.apache.kafka.common.errors.UnknownTopicOrPartitionException]
     * if the topic or partition did not exist within the cluster.
     *  * [org.apache.kafka.common.errors.InvalidTopicException]
     * if the topic was already queued for deletion.
     *  * [org.apache.kafka.common.errors.NotControllerException]
     * if the request was sent to a broker that was not the controller for the cluster.
     *  * [org.apache.kafka.common.errors.TimeoutException]
     * if the request timed out before the election was complete.
     *  * [org.apache.kafka.common.errors.LeaderNotAvailableException]
     * if the preferred leader was not alive or not in the ISR.
     *
     * @param electionType The type of election to conduct.
     * @param partitions   The topics and partitions for which to conduct elections.
     * @param options      The options to use when electing the leaders.
     * @return The ElectLeadersResult.
     */
    fun electLeaders(
        electionType: ElectionType,
        partitions: Set<TopicPartition>?,
        options: ElectLeadersOptions
    ): ElectLeadersResult

    /**
     * Change the reassignments for one or more partitions.
     * Providing `null` will <bold>revert</bold> the reassignment for the associated partition.
     *
     * This is a convenience method for [.alterPartitionReassignments]
     * with default options.  See the overload for more details.
     */
    fun alterPartitionReassignments(
        reassignments: Map<TopicPartition, NewPartitionReassignment?>
    ): AlterPartitionReassignmentsResult {
        return alterPartitionReassignments(reassignments, AlterPartitionReassignmentsOptions())
    }

    /**
     * Change the reassignments for one or more partitions.
     * Providing `null` will <bold>revert</bold> the reassignment for the associated partition.
     *
     * The following exceptions can be anticipated when calling `get()` on the futures obtained from
     * the returned `AlterPartitionReassignmentsResult`:
     *
     *  * [org.apache.kafka.common.errors.ClusterAuthorizationException]
     * If the authenticated user didn't have alter access to the cluster.
     *  * [org.apache.kafka.common.errors.UnknownTopicOrPartitionException]
     * If the topic or partition does not exist within the cluster.
     *  * [org.apache.kafka.common.errors.TimeoutException]
     * if the request timed out before the controller could record the new assignments.
     *  * [org.apache.kafka.common.errors.InvalidReplicaAssignmentException]
     * If the specified assignment was not valid.
     *  * [org.apache.kafka.common.errors.NoReassignmentInProgressException]
     * If there was an attempt to cancel a reassignment for a partition which was not being
     * reassigned.
     *
     * @param reassignments The reassignments to add, modify, or remove. See
     * [NewPartitionReassignment].
     * @param options The options to use.
     * @return The result.
     */
    fun alterPartitionReassignments(
        reassignments: Map<TopicPartition, NewPartitionReassignment?>,
        options: AlterPartitionReassignmentsOptions
    ): AlterPartitionReassignmentsResult
    /**
     * List the current reassignments for the given partitions
     *
     * The following exceptions can be anticipated when calling `get()` on the futures obtained from
     * the returned `ListPartitionReassignmentsResult`:
     *
     *  * [org.apache.kafka.common.errors.ClusterAuthorizationException]
     * If the authenticated user doesn't have alter access to the cluster.
     *  * [org.apache.kafka.common.errors.UnknownTopicOrPartitionException]
     * If a given topic or partition does not exist.
     *  * [org.apache.kafka.common.errors.TimeoutException]
     * If the request timed out before the controller could list the current reassignments.
     *
     *
     * @param partitions      The topic partitions to list reassignments for.
     * @param options         The options to use.
     * @return                The result.
     */
    fun listPartitionReassignments(
        partitions: Set<TopicPartition>,
        options: ListPartitionReassignmentsOptions = ListPartitionReassignmentsOptions()
    ): ListPartitionReassignmentsResult {
        return listPartitionReassignments(partitions, options)
    }

    /**
     * List all of the current partition reassignments
     *
     * The following exceptions can be anticipated when calling `get()` on the futures obtained from
     * the returned `ListPartitionReassignmentsResult`:
     *
     *  * [org.apache.kafka.common.errors.ClusterAuthorizationException]
     * If the authenticated user doesn't have alter access to the cluster.
     *  * [org.apache.kafka.common.errors.UnknownTopicOrPartitionException]
     * If a given topic or partition does not exist.
     *  * [org.apache.kafka.common.errors.TimeoutException]
     * If the request timed out before the controller could list the current reassignments.
     *
     * @param options         The options to use.
     * @return                The result.
     */
    fun listPartitionReassignments(
        options: ListPartitionReassignmentsOptions = ListPartitionReassignmentsOptions()
    ): ListPartitionReassignmentsResult {
        return listPartitionReassignments(null, options)
    }

    /**
     * @param partitions the partitions we want to get reassignment for, or `null` if we want to get
     * the reassignments for all partitions in the cluster.
     *
     * @param options The options to use.
     * @return The result.
     */
    fun listPartitionReassignments(
        partitions: Set<TopicPartition>?,
        options: ListPartitionReassignmentsOptions
    ): ListPartitionReassignmentsResult

    /**
     * Remove members from the consumer group by given member identities.
     *
     * For possible error codes, refer to [LeaveGroupResponse].
     *
     * @param groupId The ID of the group to remove member from.
     * @param options The options to carry removing members' information.
     * @return The MembershipChangeResult.
     */
    fun removeMembersFromConsumerGroup(
        groupId: String,
        options: RemoveMembersFromConsumerGroupOptions
    ): RemoveMembersFromConsumerGroupResult

    /**
     * Alters offsets for the specified group. In order to succeed, the group must be empty.
     *
     * This is a convenience method for [.alterConsumerGroupOffsets] with default options.
     * See the overload for more details.
     *
     * @param groupId The group for which to alter offsets.
     * @param offsets A map of offsets by partition with associated metadata.
     * @return The AlterOffsetsResult.
     */
    fun alterConsumerGroupOffsets(
        groupId: String,
        offsets: Map<TopicPartition, OffsetAndMetadata>
    ): AlterConsumerGroupOffsetsResult {
        return alterConsumerGroupOffsets(groupId, offsets, AlterConsumerGroupOffsetsOptions())
    }

    /**
     * Alters offsets for the specified group. In order to succeed, the group must be empty.
     *
     * This operation is not transactional so it may succeed for some partitions while fail for others.
     *
     * @param groupId The group for which to alter offsets.
     * @param offsets A map of offsets by partition with associated metadata. Partitions not
     * specified in the map are ignored.
     * @param options The options to use when altering the offsets.
     * @return The AlterOffsetsResult.
     */
    fun alterConsumerGroupOffsets(
        groupId: String,
        offsets: Map<TopicPartition, OffsetAndMetadata>,
        options: AlterConsumerGroupOffsetsOptions
    ): AlterConsumerGroupOffsetsResult

    /**
     * List offset for the specified partitions and OffsetSpec. This operation enables to find
     * the beginning offset, end offset as well as the offset matching a timestamp in partitions.
     *
     * This is a convenience method for [.listOffsets]
     *
     * @param topicPartitionOffsets The mapping from partition to the OffsetSpec to look up.
     * @return The ListOffsetsResult.
     */
    fun listOffsets(topicPartitionOffsets: Map<TopicPartition, OffsetSpec>): ListOffsetsResult {
        return listOffsets(topicPartitionOffsets, ListOffsetsOptions())
    }

    /**
     * List offset for the specified partitions. This operation enables to find
     * the beginning offset, end offset as well as the offset matching a timestamp in partitions.
     *
     * @param topicPartitionOffsets The mapping from partition to the OffsetSpec to look up.
     * @param options The options to use when retrieving the offsets
     * @return The ListOffsetsResult.
     */
    fun listOffsets(
        topicPartitionOffsets: Map<TopicPartition, OffsetSpec>,
        options: ListOffsetsOptions
    ): ListOffsetsResult

    /**
     * Describes all entities matching the provided filter that have at least one client quota configuration
     * value defined.
     *
     * This is a convenience method for [.describeClientQuotas]
     * with default options. See the overload for more details.
     *
     * This operation is supported by brokers with version 2.6.0 or higher.
     *
     * @param filter the filter to apply to match entities
     * @return the DescribeClientQuotasResult containing the result
     */
    fun describeClientQuotas(filter: ClientQuotaFilter): DescribeClientQuotasResult {
        return describeClientQuotas(filter, DescribeClientQuotasOptions())
    }

    /**
     * Describes all entities matching the provided filter that have at least one client quota configuration
     * value defined.
     *
     * The following exceptions can be anticipated when calling `get()` on the future from the
     * returned [DescribeClientQuotasResult]:
     *
     *  * [org.apache.kafka.common.errors.ClusterAuthorizationException]
     * If the authenticated user didn't have describe access to the cluster.
     *  * [org.apache.kafka.common.errors.InvalidRequestException]
     * If the request details are invalid. e.g., an invalid entity type was specified.
     *  * [org.apache.kafka.common.errors.TimeoutException]
     * If the request timed out before the describe could finish.
     *
     * This operation is supported by brokers with version 2.6.0 or higher.
     *
     * @param filter the filter to apply to match entities
     * @param options the options to use
     * @return the DescribeClientQuotasResult containing the result
     */
    fun describeClientQuotas(
        filter: ClientQuotaFilter,
        options: DescribeClientQuotasOptions
    ): DescribeClientQuotasResult

    /**
     * Alters client quota configurations with the specified alterations.
     *
     * This is a convenience method for [.alterClientQuotas]
     * with default options. See the overload for more details.
     *
     * This operation is supported by brokers with version 2.6.0 or higher.
     *
     * @param entries the alterations to perform
     * @return the AlterClientQuotasResult containing the result
     */
    fun alterClientQuotas(entries: Collection<ClientQuotaAlteration>): AlterClientQuotasResult {
        return alterClientQuotas(entries, AlterClientQuotasOptions())
    }

    /**
     * Alters client quota configurations with the specified alterations.
     *
     * Alterations for a single entity are atomic, but across entities is not guaranteed. The resulting
     * per-entity error code should be evaluated to resolve the success or failure of all updates.
     *
     * The following exceptions can be anticipated when calling `get()` on the futures obtained from
     * the returned [AlterClientQuotasResult]:
     *
     *  * [org.apache.kafka.common.errors.ClusterAuthorizationException]
     * If the authenticated user didn't have alter access to the cluster.
     *  * [org.apache.kafka.common.errors.InvalidRequestException]
     * If the request details are invalid. e.g., a configuration key was specified more than once for an entity.
     *  * [org.apache.kafka.common.errors.TimeoutException]
     * If the request timed out before the alterations could finish. It cannot be guaranteed whether the update
     * succeed or not.
     *
     * This operation is supported by brokers with version 2.6.0 or higher.
     *
     * @param entries the alterations to perform
     * @return the AlterClientQuotasResult containing the result
     */
    fun alterClientQuotas(
        entries: Collection<ClientQuotaAlteration>,
        options: AlterClientQuotasOptions
    ): AlterClientQuotasResult

    /**
     * Describe all SASL/SCRAM credentials.
     *
     * This is a convenience method for [.describeUserScramCredentials]
     *
     * @return The DescribeUserScramCredentialsResult.
     */
    fun describeUserScramCredentials(): DescribeUserScramCredentialsResult {
        return describeUserScramCredentials(null, DescribeUserScramCredentialsOptions())
    }

    /**
     * Describe SASL/SCRAM credentials for the given users.
     *
     * This is a convenience method for [.describeUserScramCredentials]
     *
     * @param users the users for which credentials are to be described; all users' credentials are described if null
     * or empty.
     * @return The DescribeUserScramCredentialsResult.
     */
    fun describeUserScramCredentials(users: List<String>): DescribeUserScramCredentialsResult {
        return describeUserScramCredentials(users, DescribeUserScramCredentialsOptions())
    }

    /**
     * Describe SASL/SCRAM credentials.
     *
     * The following exceptions can be anticipated when calling `get()` on the futures from the
     * returned [DescribeUserScramCredentialsResult]:
     *
     *  * [org.apache.kafka.common.errors.ClusterAuthorizationException]
     * If the authenticated user didn't have describe access to the cluster.
     *  * [org.apache.kafka.common.errors.ResourceNotFoundException]
     * If the user did not exist/had no SCRAM credentials.
     *  * [org.apache.kafka.common.errors.DuplicateResourceException]
     * If the user was requested to be described more than once in the original request.
     *  * [org.apache.kafka.common.errors.TimeoutException]
     * If the request timed out before the describe operation could finish.
     *
     * This operation is supported by brokers with version 2.7.0 or higher.
     *
     * @param users the users for which credentials are to be described; all users' credentials are described if null
     * or empty.
     * @param options The options to use when describing the credentials
     * @return The DescribeUserScramCredentialsResult.
     */
    fun describeUserScramCredentials(
        users: List<String>?,
        options: DescribeUserScramCredentialsOptions
    ): DescribeUserScramCredentialsResult

    /**
     * Alter SASL/SCRAM credentials for the given users.
     *
     * This is a convenience method for [.alterUserScramCredentials]
     *
     * @param alterations the alterations to be applied
     * @return The AlterUserScramCredentialsResult.
     */
    fun alterUserScramCredentials(alterations: List<UserScramCredentialAlteration>): AlterUserScramCredentialsResult {
        return alterUserScramCredentials(alterations, AlterUserScramCredentialsOptions())
    }

    /**
     * Alter SASL/SCRAM credentials.
     *
     * The following exceptions can be anticipated when calling `get()` any of the futures from the
     * returned [AlterUserScramCredentialsResult]:
     *
     *  * [org.apache.kafka.common.errors.NotControllerException]
     * If the request is not sent to the Controller broker.
     *  * [org.apache.kafka.common.errors.ClusterAuthorizationException]
     * If the authenticated user didn't have alter access to the cluster.
     *  * [org.apache.kafka.common.errors.UnsupportedByAuthenticationException]
     * If the user authenticated with a delegation token.
     *  * [org.apache.kafka.common.errors.UnsupportedSaslMechanismException]
     * If the requested SCRAM mechanism is unrecognized or otherwise unsupported.
     *  * [org.apache.kafka.common.errors.UnacceptableCredentialException]
     * If the username is empty or the requested number of iterations is too small or too large.
     *  * [org.apache.kafka.common.errors.TimeoutException]
     * If the request timed out before the describe could finish.
     *
     * This operation is supported by brokers with version 2.7.0 or higher.
     *
     * @param alterations the alterations to be applied
     * @param options The options to use when altering the credentials
     * @return The AlterUserScramCredentialsResult.
     */
    fun alterUserScramCredentials(
        alterations: List<UserScramCredentialAlteration>,
        options: AlterUserScramCredentialsOptions
    ): AlterUserScramCredentialsResult

    /**
     * Describes finalized as well as supported features.
     *
     * This is a convenience method for [.describeFeatures] with default options.
     * See the overload for more details.
     *
     * @return the [DescribeFeaturesResult] containing the result
     */
    fun describeFeatures(): DescribeFeaturesResult {
        return describeFeatures(DescribeFeaturesOptions())
    }

    /**
     * Describes finalized as well as supported features. The request is issued to any random
     * broker.
     *
     * The following exceptions can be anticipated when calling `get()` on the future from the
     * returned [DescribeFeaturesResult]:
     *
     *  * [org.apache.kafka.common.errors.TimeoutException]
     * If the request timed out before the describe operation could finish.
     *
     * @param options the options to use
     * @return the [DescribeFeaturesResult] containing the result
     */
    fun describeFeatures(options: DescribeFeaturesOptions): DescribeFeaturesResult

    /**
     * Applies specified updates to finalized features. This operation is not transactional so some
     * updates may succeed while the rest may fail.
     *
     * The API takes in a map of finalized feature names to [FeatureUpdate] that needs to be
     * applied. Each entry in the map specifies the finalized feature to be added or updated or
     * deleted, along with the new max feature version level value. This request is issued only to
     * the controller since the API is only served by the controller. The return value contains an
     * error code for each supplied [FeatureUpdate], and the code indicates if the update
     * succeeded or failed in the controller.
     *
     *  * Downgrade of feature version level is not a regular operation/intent. It is only allowed
     * in the controller if the [FeatureUpdate] has the allowDowngrade flag set. Setting this
     * flag conveys user intent to attempt downgrade of a feature max version level. Note that
     * despite the allowDowngrade flag being set, certain downgrades may be rejected by the
     * controller if it is deemed impossible.
     *  * Deletion of a finalized feature version is not a regular operation/intent. It could be
     * done by setting the allowDowngrade flag to true in the [FeatureUpdate], and, setting
     * the max version level to a value less than 1.
     *
     * The following exceptions can be anticipated when calling `get()` on the futures
     * obtained from the returned [UpdateFeaturesResult]:
     *
     *  * [org.apache.kafka.common.errors.ClusterAuthorizationException]
     * If the authenticated user didn't have alter access to the cluster.
     *  * [org.apache.kafka.common.errors.InvalidRequestException]
     * If the request details are invalid. e.g., a non-existing finalized feature is attempted
     * to be deleted or downgraded.
     *  * [org.apache.kafka.common.errors.TimeoutException]
     * If the request timed out before the updates could finish. It cannot be guaranteed whether
     * the updates succeeded or not.
     *  * [FeatureUpdateFailedException]
     * This means there was an unexpected error encountered when the update was applied on
     * the controller. There is no guarantee on whether the update succeeded or failed. The best
     * way to find out is to issue a [Admin.describeFeatures]
     * request.
     *
     * This operation is supported by brokers with version 2.7.0 or higher.
     *
     * @param featureUpdates the map of finalized feature name to [FeatureUpdate]
     * @param options the options to use
     * @return the [UpdateFeaturesResult] containing the result
     */
    fun updateFeatures(
        featureUpdates: Map<String, FeatureUpdate>,
        options: UpdateFeaturesOptions
    ): UpdateFeaturesResult

    /**
     * Describes the state of the metadata quorum.
     *
     * This is a convenience method for [.describeMetadataQuorum] with default options.
     * See the overload for more details.
     *
     * @return the [DescribeMetadataQuorumResult] containing the result
     */
    fun describeMetadataQuorum(): DescribeMetadataQuorumResult {
        return describeMetadataQuorum(DescribeMetadataQuorumOptions())
    }

    /**
     * Describes the state of the metadata quorum.
     *
     * The following exceptions can be anticipated when calling `get()` on the futures obtained from
     * the returned `DescribeMetadataQuorumResult`:
     *
     *  * [org.apache.kafka.common.errors.ClusterAuthorizationException]
     * If the authenticated user didn't have `DESCRIBE` access to the cluster.
     *  * [org.apache.kafka.common.errors.TimeoutException]
     * If the request timed out before the controller could list the cluster links.
     *
     * @param options The [DescribeMetadataQuorumOptions] to use when describing the quorum.
     * @return the [DescribeMetadataQuorumResult] containing the result
     */
    fun describeMetadataQuorum(options: DescribeMetadataQuorumOptions): DescribeMetadataQuorumResult

    /**
     * Unregister a broker.
     *
     * This operation does not have any effect on partition assignments. It is supported
     * only on Kafka clusters which use Raft to store metadata, rather than ZooKeeper.
     *
     * This is a convenience method for [.unregisterBroker]
     *
     * @param brokerId  the broker id to unregister.
     *
     * @return the [UnregisterBrokerResult] containing the result
     */
    @Unstable
    fun unregisterBroker(brokerId: Int): UnregisterBrokerResult {
        return unregisterBroker(brokerId, UnregisterBrokerOptions())
    }

    /**
     * Unregister a broker.
     *
     * This operation does not have any effect on partition assignments. It is supported
     * only on Kafka clusters which use Raft to store metadata, rather than ZooKeeper.
     *
     * The following exceptions can be anticipated when calling `get()` on the future from the
     * returned [UnregisterBrokerResult]:
     *
     *  * [org.apache.kafka.common.errors.TimeoutException]
     * If the request timed out before the describe operation could finish.
     *  * [org.apache.kafka.common.errors.UnsupportedVersionException]
     * If the software is too old to support the unregistration API, or if the
     * cluster is not using Raft to store metadata.
     *
     * @param brokerId  the broker id to unregister.
     * @param options   the options to use.
     *
     * @return the [UnregisterBrokerResult] containing the result
     */
    @Unstable
    fun unregisterBroker(brokerId: Int, options: UnregisterBrokerOptions): UnregisterBrokerResult

    /**
     * Describe producer state on a set of topic partitions. See
     * [.describeProducers] for more details.
     *
     * @param partitions The set of partitions to query
     * @return The result
     */
    fun describeProducers(partitions: Collection<TopicPartition>): DescribeProducersResult {
        return describeProducers(partitions, DescribeProducersOptions())
    }

    /**
     * Describe active producer state on a set of topic partitions. Unless a specific broker
     * is requested through [DescribeProducersOptions.brokerId], this will
     * query the partition leader to find the producer state.
     *
     * @param partitions The set of partitions to query
     * @param options Options to control the method behavior
     * @return The result
     */
    fun describeProducers(
        partitions: Collection<TopicPartition>,
        options: DescribeProducersOptions
    ): DescribeProducersResult

    /**
     * Describe the state of a set of transactional IDs. See
     * [.describeTransactions] for more details.
     *
     * @param transactionalIds The set of transactional IDs to query
     * @return The result
     */
    fun describeTransactions(transactionalIds: Collection<String>): DescribeTransactionsResult {
        return describeTransactions(transactionalIds, DescribeTransactionsOptions())
    }

    /**
     * Describe the state of a set of transactional IDs from the respective transaction coordinators,
     * which are dynamically discovered.
     *
     * @param transactionalIds The set of transactional IDs to query
     * @param options Options to control the method behavior
     * @return The result
     */
    fun describeTransactions(
        transactionalIds: Collection<String>,
        options: DescribeTransactionsOptions
    ): DescribeTransactionsResult

    /**
     * Forcefully abort a transaction which is open on a topic partition. See
     * [.abortTransaction] for more details.
     *
     * @param spec The transaction specification including topic partition and producer details
     * @return The result
     */
    fun abortTransaction(spec: AbortTransactionSpec): AbortTransactionResult {
        return abortTransaction(spec, AbortTransactionOptions())
    }

    /**
     * Forcefully abort a transaction which is open on a topic partition. This will
     * send a `WriteTxnMarkers` request to the partition leader in order to abort the
     * transaction. This requires administrative privileges.
     *
     * @param spec The transaction specification including topic partition and producer details
     * @param options Options to control the method behavior (including filters)
     * @return The result
     */
    fun abortTransaction(spec: AbortTransactionSpec, options: AbortTransactionOptions): AbortTransactionResult

    /**
     * List active transactions in the cluster. See
     * [.listTransactions] for more details.
     *
     * @return The result
     */
    fun listTransactions(): ListTransactionsResult {
        return listTransactions(ListTransactionsOptions())
    }

    /**
     * List active transactions in the cluster. This will query all potential transaction
     * coordinators in the cluster and collect the state of all transactions. Users
     * should typically attempt to reduce the size of the result set using
     * [ListTransactionsOptions.filterProducerIds] or
     * [ListTransactionsOptions.filterStates]
     *
     * @param options Options to control the method behavior (including filters)
     * @return The result
     */
    fun listTransactions(options: ListTransactionsOptions): ListTransactionsResult

    /**
     * Fence out all active producers that use any of the provided transactional IDs, with the default options.
     *
     * This is a convenience method for [.fenceProducers]
     * with default options. See the overload for more details.
     *
     * @param transactionalIds The IDs of the producers to fence.
     * @return The FenceProducersResult.
     */
    fun fenceProducers(transactionalIds: Collection<String>): FenceProducersResult {
        return fenceProducers(transactionalIds, FenceProducersOptions())
    }

    /**
     * Fence out all active producers that use any of the provided transactional IDs.
     *
     * @param transactionalIds The IDs of the producers to fence.
     * @param options          The options to use when fencing the producers.
     * @return The FenceProducersResult.
     */
    fun fenceProducers(
        transactionalIds: Collection<String>,
        options: FenceProducersOptions
    ): FenceProducersResult

    /**
     * Get the metrics kept by the adminClient
     */
    fun metrics(): Map<MetricName, Metric>

    companion object {

        /**
         * Create a new Admin with the given configuration.
         *
         * @param props The configuration.
         * @return The new KafkaAdminClient.
         */
        fun create(props: Properties): Admin {
            return KafkaAdminClient.createInternal(config = AdminClientConfig(props, true))
        }

        /**
         * Create a new Admin with the given configuration.
         *
         * @param conf The configuration.
         * @return The new KafkaAdminClient.
         */
        fun create(conf: Map<String, Any?>): Admin {
            return KafkaAdminClient.createInternal(config = AdminClientConfig(conf, true))
        }
    }
}
