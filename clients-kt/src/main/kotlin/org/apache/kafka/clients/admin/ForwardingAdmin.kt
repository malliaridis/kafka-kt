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

import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.ElectionType
import org.apache.kafka.common.Metric
import org.apache.kafka.common.MetricName
import org.apache.kafka.common.TopicCollection
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.TopicPartitionReplica
import org.apache.kafka.common.acl.AclBinding
import org.apache.kafka.common.acl.AclBindingFilter
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.quota.ClientQuotaAlteration
import org.apache.kafka.common.quota.ClientQuotaFilter
import java.time.Duration
import java.util.*

/**
 * `ForwardingAdmin` is the default value of `forwarding.admin.class` in MirrorMaker. Users who wish
 * to customize the MirrorMaker behaviour for the creation of topics and access control lists can
 * extend this class without needing to provide a whole implementation of `Admin`.
 *
 * The class must have a constructor with signature `(Map<String, Object> config)` for configuring
 * a decorated [KafkaAdminClient] and any other clients needed for external resource management.
 */
open class ForwardingAdmin(configs: Map<String, Any?>) : Admin {
    private val delegate: Admin = Admin.create(configs)

    override fun close(timeout: Duration) = delegate.close(timeout)

    override fun createTopics(
        newTopics: Collection<NewTopic>,
        options: CreateTopicsOptions,
    ): CreateTopicsResult = delegate.createTopics(newTopics, options)

    override fun deleteTopics(
        topics: TopicCollection,
        options: DeleteTopicsOptions,
    ): DeleteTopicsResult = delegate.deleteTopics(topics, options)

    override fun listTopics(options: ListTopicsOptions): ListTopicsResult =
        delegate.listTopics(options)

    override fun describeTopics(
        topics: TopicCollection,
        options: DescribeTopicsOptions,
    ): DescribeTopicsResult = delegate.describeTopics(topics, options)

    override fun describeCluster(options: DescribeClusterOptions): DescribeClusterResult =
        delegate.describeCluster(options)

    override fun describeAcls(
        filter: AclBindingFilter,
        options: DescribeAclsOptions,
    ): DescribeAclsResult = delegate.describeAcls(filter, options)

    override fun createAcls(
        acls: Collection<AclBinding>,
        options: CreateAclsOptions,
    ): CreateAclsResult = delegate.createAcls(acls, options)

    override fun deleteAcls(
        filters: Collection<AclBindingFilter>,
        options: DeleteAclsOptions,
    ): DeleteAclsResult = delegate.deleteAcls(filters, options)

    override fun describeConfigs(
        resources: Collection<ConfigResource>,
        options: DescribeConfigsOptions,
    ): DescribeConfigsResult = delegate.describeConfigs(resources, options)

    @Deprecated("")
    override fun alterConfigs(
        configs: Map<ConfigResource, Config>,
        options: AlterConfigsOptions,
    ): AlterConfigsResult = delegate.alterConfigs(configs, options)

    override fun incrementalAlterConfigs(
        configs: Map<ConfigResource, Collection<AlterConfigOp>>,
        options: AlterConfigsOptions,
    ): AlterConfigsResult = delegate.incrementalAlterConfigs(configs, options)

    override fun alterReplicaLogDirs(
        replicaAssignment: Map<TopicPartitionReplica, String>,
        options: AlterReplicaLogDirsOptions,
    ): AlterReplicaLogDirsResult = delegate.alterReplicaLogDirs(replicaAssignment, options)

    override fun describeLogDirs(
        brokers: Collection<Int>,
        options: DescribeLogDirsOptions,
    ): DescribeLogDirsResult = delegate.describeLogDirs(brokers, options)

    override fun describeReplicaLogDirs(
        replicas: Collection<TopicPartitionReplica>,
        options: DescribeReplicaLogDirsOptions,
    ): DescribeReplicaLogDirsResult = delegate.describeReplicaLogDirs(replicas, options)

    override fun createPartitions(
        newPartitions: Map<String, NewPartitions>,
        options: CreatePartitionsOptions,
    ): CreatePartitionsResult = delegate.createPartitions(newPartitions, options)

    override fun deleteRecords(
        recordsToDelete: Map<TopicPartition, RecordsToDelete>,
        options: DeleteRecordsOptions,
    ): DeleteRecordsResult = delegate.deleteRecords(recordsToDelete, options)

    override fun createDelegationToken(
        options: CreateDelegationTokenOptions,
    ): CreateDelegationTokenResult = delegate.createDelegationToken(options)

    override fun renewDelegationToken(
        hmac: ByteArray,
        options: RenewDelegationTokenOptions,
    ): RenewDelegationTokenResult = delegate.renewDelegationToken(hmac, options)

    override fun expireDelegationToken(
        hmac: ByteArray,
        options: ExpireDelegationTokenOptions,
    ): ExpireDelegationTokenResult = delegate.expireDelegationToken(hmac, options)

    override fun describeDelegationToken(
        options: DescribeDelegationTokenOptions,
    ): DescribeDelegationTokenResult = delegate.describeDelegationToken(options)

    override fun describeConsumerGroups(
        groupIds: Collection<String>,
        options: DescribeConsumerGroupsOptions,
    ): DescribeConsumerGroupsResult = delegate.describeConsumerGroups(groupIds, options)

    override fun listConsumerGroups(options: ListConsumerGroupsOptions): ListConsumerGroupsResult =
        delegate.listConsumerGroups(options)

    override fun listConsumerGroupOffsets(
        groupSpecs: Map<String, ListConsumerGroupOffsetsSpec>,
        options: ListConsumerGroupOffsetsOptions,
    ): ListConsumerGroupOffsetsResult = delegate.listConsumerGroupOffsets(groupSpecs, options)

    override fun deleteConsumerGroups(
        groupIds: Collection<String>,
        options: DeleteConsumerGroupsOptions,
    ): DeleteConsumerGroupsResult = delegate.deleteConsumerGroups(groupIds, options)

    override fun deleteConsumerGroupOffsets(
        groupId: String,
        partitions: Set<TopicPartition>,
        options: DeleteConsumerGroupOffsetsOptions,
    ): DeleteConsumerGroupOffsetsResult =
        delegate.deleteConsumerGroupOffsets(groupId, partitions, options)

    override fun electLeaders(
        electionType: ElectionType,
        partitions: Set<TopicPartition>?,
        options: ElectLeadersOptions,
    ): ElectLeadersResult = delegate.electLeaders(electionType, partitions, options)

    override fun alterPartitionReassignments(
        reassignments: Map<TopicPartition, NewPartitionReassignment?>,
        options: AlterPartitionReassignmentsOptions,
    ): AlterPartitionReassignmentsResult =
        delegate.alterPartitionReassignments(reassignments, options)

    override fun listPartitionReassignments(
        partitions: Set<TopicPartition>,
        options: ListPartitionReassignmentsOptions,
    ): ListPartitionReassignmentsResult = delegate.listPartitionReassignments(partitions, options)

    override fun removeMembersFromConsumerGroup(
        groupId: String,
        options: RemoveMembersFromConsumerGroupOptions,
    ): RemoveMembersFromConsumerGroupResult =
        delegate.removeMembersFromConsumerGroup(groupId, options)

    override fun alterConsumerGroupOffsets(
        groupId: String,
        offsets: Map<TopicPartition, OffsetAndMetadata>,
        options: AlterConsumerGroupOffsetsOptions,
    ): AlterConsumerGroupOffsetsResult =
        delegate.alterConsumerGroupOffsets(groupId, offsets, options)

    override fun listOffsets(
        topicPartitionOffsets: Map<TopicPartition, OffsetSpec>,
        options: ListOffsetsOptions,
    ): ListOffsetsResult = delegate.listOffsets(topicPartitionOffsets, options)

    override fun describeClientQuotas(
        filter: ClientQuotaFilter,
        options: DescribeClientQuotasOptions,
    ): DescribeClientQuotasResult = delegate.describeClientQuotas(filter, options)

    override fun alterClientQuotas(
        entries: Collection<ClientQuotaAlteration>,
        options: AlterClientQuotasOptions,
    ): AlterClientQuotasResult = delegate.alterClientQuotas(entries, options)

    override fun describeUserScramCredentials(
        users: List<String?>?,
        options: DescribeUserScramCredentialsOptions,
    ): DescribeUserScramCredentialsResult = delegate.describeUserScramCredentials(users, options)

    override fun alterUserScramCredentials(
        alterations: List<UserScramCredentialAlteration>,
        options: AlterUserScramCredentialsOptions,
    ): AlterUserScramCredentialsResult = delegate.alterUserScramCredentials(alterations, options)

    override fun describeFeatures(options: DescribeFeaturesOptions): DescribeFeaturesResult =
        delegate.describeFeatures(options)

    override fun updateFeatures(
        featureUpdates: Map<String, FeatureUpdate>,
        options: UpdateFeaturesOptions,
    ): UpdateFeaturesResult = delegate.updateFeatures(featureUpdates, options)

    override fun describeMetadataQuorum(
        options: DescribeMetadataQuorumOptions,
    ): DescribeMetadataQuorumResult = delegate.describeMetadataQuorum(options)

    override fun unregisterBroker(
        brokerId: Int,
        options: UnregisterBrokerOptions,
    ): UnregisterBrokerResult = delegate.unregisterBroker(brokerId, options)

    override fun describeProducers(
        partitions: Collection<TopicPartition>,
        options: DescribeProducersOptions,
    ): DescribeProducersResult = delegate.describeProducers(partitions, options)

    override fun describeTransactions(
        transactionalIds: Collection<String>,
        options: DescribeTransactionsOptions,
    ): DescribeTransactionsResult = delegate.describeTransactions(transactionalIds, options)

    override fun abortTransaction(
        spec: AbortTransactionSpec,
        options: AbortTransactionOptions,
    ): AbortTransactionResult = delegate.abortTransaction(spec, options)

    override fun listTransactions(options: ListTransactionsOptions): ListTransactionsResult =
        delegate.listTransactions(options)

    override fun fenceProducers(
        transactionalIds: Collection<String>,
        options: FenceProducersOptions,
    ): FenceProducersResult = delegate.fenceProducers(transactionalIds, options)

    override fun metrics(): Map<MetricName, Metric> = delegate.metrics()
}
