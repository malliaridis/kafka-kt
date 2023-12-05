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

package org.apache.kafka.common.requests

import java.nio.BufferUnderflowException
import java.nio.ByteBuffer
import org.apache.kafka.common.ConsumerGroupState
import org.apache.kafka.common.ElectionType
import org.apache.kafka.common.IsolationLevel
import org.apache.kafka.common.Node
import org.apache.kafka.common.TopicIdPartition
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.Uuid
import org.apache.kafka.common.acl.AccessControlEntry
import org.apache.kafka.common.acl.AccessControlEntryFilter
import org.apache.kafka.common.acl.AclBinding
import org.apache.kafka.common.acl.AclBindingFilter
import org.apache.kafka.common.acl.AclOperation
import org.apache.kafka.common.acl.AclPermissionType
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.errors.NotCoordinatorException
import org.apache.kafka.common.errors.NotEnoughReplicasException
import org.apache.kafka.common.errors.SecurityDisabledException
import org.apache.kafka.common.errors.UnknownServerException
import org.apache.kafka.common.errors.UnsupportedVersionException
import org.apache.kafka.common.message.AddOffsetsToTxnRequestData
import org.apache.kafka.common.message.AddOffsetsToTxnResponseData
import org.apache.kafka.common.message.AllocateProducerIdsRequestData
import org.apache.kafka.common.message.AllocateProducerIdsResponseData
import org.apache.kafka.common.message.AlterClientQuotasResponseData
import org.apache.kafka.common.message.AlterConfigsResponseData
import org.apache.kafka.common.message.AlterPartitionReassignmentsRequestData
import org.apache.kafka.common.message.AlterPartitionReassignmentsRequestData.ReassignablePartition
import org.apache.kafka.common.message.AlterPartitionReassignmentsRequestData.ReassignableTopic
import org.apache.kafka.common.message.AlterPartitionReassignmentsResponseData
import org.apache.kafka.common.message.AlterPartitionReassignmentsResponseData.ReassignablePartitionResponse
import org.apache.kafka.common.message.AlterPartitionReassignmentsResponseData.ReassignableTopicResponse
import org.apache.kafka.common.message.AlterPartitionRequestData
import org.apache.kafka.common.message.AlterPartitionResponseData
import org.apache.kafka.common.message.AlterReplicaLogDirsRequestData
import org.apache.kafka.common.message.AlterReplicaLogDirsRequestData.AlterReplicaLogDir
import org.apache.kafka.common.message.AlterReplicaLogDirsRequestData.AlterReplicaLogDirTopic
import org.apache.kafka.common.message.AlterReplicaLogDirsRequestData.AlterReplicaLogDirTopicCollection
import org.apache.kafka.common.message.AlterReplicaLogDirsResponseData
import org.apache.kafka.common.message.AlterReplicaLogDirsResponseData.AlterReplicaLogDirPartitionResult
import org.apache.kafka.common.message.AlterReplicaLogDirsResponseData.AlterReplicaLogDirTopicResult
import org.apache.kafka.common.message.AlterUserScramCredentialsRequestData
import org.apache.kafka.common.message.AlterUserScramCredentialsRequestData.ScramCredentialDeletion
import org.apache.kafka.common.message.AlterUserScramCredentialsRequestData.ScramCredentialUpsertion
import org.apache.kafka.common.message.AlterUserScramCredentialsResponseData
import org.apache.kafka.common.message.ApiMessageType
import org.apache.kafka.common.message.ApiVersionsRequestData
import org.apache.kafka.common.message.ApiVersionsResponseData
import org.apache.kafka.common.message.ApiVersionsResponseData.ApiVersionCollection
import org.apache.kafka.common.message.BeginQuorumEpochRequestData
import org.apache.kafka.common.message.BeginQuorumEpochResponseData
import org.apache.kafka.common.message.BrokerHeartbeatRequestData
import org.apache.kafka.common.message.BrokerHeartbeatResponseData
import org.apache.kafka.common.message.BrokerRegistrationRequestData
import org.apache.kafka.common.message.BrokerRegistrationRequestData.FeatureCollection
import org.apache.kafka.common.message.BrokerRegistrationRequestData.ListenerCollection
import org.apache.kafka.common.message.BrokerRegistrationResponseData
import org.apache.kafka.common.message.ControlledShutdownRequestData
import org.apache.kafka.common.message.ControlledShutdownResponseData
import org.apache.kafka.common.message.ControlledShutdownResponseData.RemainingPartition
import org.apache.kafka.common.message.ControlledShutdownResponseData.RemainingPartitionCollection
import org.apache.kafka.common.message.CreateAclsRequestData
import org.apache.kafka.common.message.CreateAclsRequestData.AclCreation
import org.apache.kafka.common.message.CreateAclsResponseData
import org.apache.kafka.common.message.CreateAclsResponseData.AclCreationResult
import org.apache.kafka.common.message.CreateDelegationTokenRequestData
import org.apache.kafka.common.message.CreateDelegationTokenRequestData.CreatableRenewers
import org.apache.kafka.common.message.CreateDelegationTokenResponseData
import org.apache.kafka.common.message.CreatePartitionsRequestData
import org.apache.kafka.common.message.CreatePartitionsRequestData.CreatePartitionsAssignment
import org.apache.kafka.common.message.CreatePartitionsRequestData.CreatePartitionsTopic
import org.apache.kafka.common.message.CreatePartitionsRequestData.CreatePartitionsTopicCollection
import org.apache.kafka.common.message.CreatePartitionsResponseData
import org.apache.kafka.common.message.CreatePartitionsResponseData.CreatePartitionsTopicResult
import org.apache.kafka.common.message.CreateTopicsRequestData
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableReplicaAssignment
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableTopic
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableTopicCollection
import org.apache.kafka.common.message.CreateTopicsRequestData.CreateableTopicConfig
import org.apache.kafka.common.message.CreateTopicsResponseData
import org.apache.kafka.common.message.CreateTopicsResponseData.CreatableTopicConfigs
import org.apache.kafka.common.message.CreateTopicsResponseData.CreatableTopicResult
import org.apache.kafka.common.message.DeleteAclsRequestData
import org.apache.kafka.common.message.DeleteAclsRequestData.DeleteAclsFilter
import org.apache.kafka.common.message.DeleteAclsResponseData
import org.apache.kafka.common.message.DeleteAclsResponseData.DeleteAclsFilterResult
import org.apache.kafka.common.message.DeleteAclsResponseData.DeleteAclsMatchingAcl
import org.apache.kafka.common.message.DeleteGroupsRequestData
import org.apache.kafka.common.message.DeleteGroupsResponseData
import org.apache.kafka.common.message.DeleteGroupsResponseData.DeletableGroupResult
import org.apache.kafka.common.message.DeleteGroupsResponseData.DeletableGroupResultCollection
import org.apache.kafka.common.message.DeleteRecordsRequestData
import org.apache.kafka.common.message.DeleteRecordsRequestData.DeleteRecordsPartition
import org.apache.kafka.common.message.DeleteRecordsRequestData.DeleteRecordsTopic
import org.apache.kafka.common.message.DeleteRecordsResponseData
import org.apache.kafka.common.message.DeleteRecordsResponseData.DeleteRecordsPartitionResult
import org.apache.kafka.common.message.DeleteRecordsResponseData.DeleteRecordsPartitionResultCollection
import org.apache.kafka.common.message.DeleteRecordsResponseData.DeleteRecordsTopicResult
import org.apache.kafka.common.message.DeleteRecordsResponseData.DeleteRecordsTopicResultCollection
import org.apache.kafka.common.message.DeleteTopicsRequestData
import org.apache.kafka.common.message.DeleteTopicsResponseData
import org.apache.kafka.common.message.DeleteTopicsResponseData.DeletableTopicResult
import org.apache.kafka.common.message.DescribeAclsResponseData
import org.apache.kafka.common.message.DescribeAclsResponseData.AclDescription
import org.apache.kafka.common.message.DescribeAclsResponseData.DescribeAclsResource
import org.apache.kafka.common.message.DescribeClientQuotasResponseData
import org.apache.kafka.common.message.DescribeClusterRequestData
import org.apache.kafka.common.message.DescribeClusterResponseData
import org.apache.kafka.common.message.DescribeClusterResponseData.DescribeClusterBroker
import org.apache.kafka.common.message.DescribeClusterResponseData.DescribeClusterBrokerCollection
import org.apache.kafka.common.message.DescribeConfigsRequestData
import org.apache.kafka.common.message.DescribeConfigsRequestData.DescribeConfigsResource
import org.apache.kafka.common.message.DescribeConfigsResponseData
import org.apache.kafka.common.message.DescribeConfigsResponseData.DescribeConfigsResourceResult
import org.apache.kafka.common.message.DescribeGroupsRequestData
import org.apache.kafka.common.message.DescribeGroupsResponseData
import org.apache.kafka.common.message.DescribeLogDirsRequestData
import org.apache.kafka.common.message.DescribeLogDirsRequestData.DescribableLogDirTopic
import org.apache.kafka.common.message.DescribeLogDirsRequestData.DescribableLogDirTopicCollection
import org.apache.kafka.common.message.DescribeLogDirsResponseData
import org.apache.kafka.common.message.DescribeLogDirsResponseData.DescribeLogDirsPartition
import org.apache.kafka.common.message.DescribeLogDirsResponseData.DescribeLogDirsTopic
import org.apache.kafka.common.message.DescribeProducersRequestData
import org.apache.kafka.common.message.DescribeProducersRequestData.TopicRequest
import org.apache.kafka.common.message.DescribeProducersResponseData
import org.apache.kafka.common.message.DescribeProducersResponseData.TopicResponse
import org.apache.kafka.common.message.DescribeQuorumRequestData
import org.apache.kafka.common.message.DescribeQuorumResponseData
import org.apache.kafka.common.message.DescribeTransactionsRequestData
import org.apache.kafka.common.message.DescribeTransactionsResponseData
import org.apache.kafka.common.message.DescribeTransactionsResponseData.TopicDataCollection
import org.apache.kafka.common.message.DescribeUserScramCredentialsRequestData
import org.apache.kafka.common.message.DescribeUserScramCredentialsRequestData.UserName
import org.apache.kafka.common.message.DescribeUserScramCredentialsResponseData
import org.apache.kafka.common.message.DescribeUserScramCredentialsResponseData.CredentialInfo
import org.apache.kafka.common.message.ElectLeadersResponseData.PartitionResult
import org.apache.kafka.common.message.ElectLeadersResponseData.ReplicaElectionResult
import org.apache.kafka.common.message.EndQuorumEpochRequestData
import org.apache.kafka.common.message.EndQuorumEpochResponseData
import org.apache.kafka.common.message.EndTxnRequestData
import org.apache.kafka.common.message.EndTxnResponseData
import org.apache.kafka.common.message.EnvelopeResponseData
import org.apache.kafka.common.message.ExpireDelegationTokenRequestData
import org.apache.kafka.common.message.ExpireDelegationTokenResponseData
import org.apache.kafka.common.message.FetchResponseData
import org.apache.kafka.common.message.FetchResponseData.AbortedTransaction
import org.apache.kafka.common.message.FetchResponseData.FetchableTopicResponse
import org.apache.kafka.common.message.FetchSnapshotRequestData
import org.apache.kafka.common.message.FetchSnapshotResponseData
import org.apache.kafka.common.message.FindCoordinatorRequestData
import org.apache.kafka.common.message.HeartbeatRequestData
import org.apache.kafka.common.message.HeartbeatResponseData
import org.apache.kafka.common.message.IncrementalAlterConfigsRequestData
import org.apache.kafka.common.message.IncrementalAlterConfigsResponseData
import org.apache.kafka.common.message.InitProducerIdRequestData
import org.apache.kafka.common.message.InitProducerIdResponseData
import org.apache.kafka.common.message.JoinGroupRequestData
import org.apache.kafka.common.message.JoinGroupRequestData.JoinGroupRequestProtocol
import org.apache.kafka.common.message.JoinGroupRequestData.JoinGroupRequestProtocolCollection
import org.apache.kafka.common.message.JoinGroupResponseData
import org.apache.kafka.common.message.JoinGroupResponseData.JoinGroupResponseMember
import org.apache.kafka.common.message.LeaderAndIsrRequestData.LeaderAndIsrPartitionState
import org.apache.kafka.common.message.LeaderAndIsrResponseData
import org.apache.kafka.common.message.LeaderAndIsrResponseData.LeaderAndIsrPartitionError
import org.apache.kafka.common.message.LeaderAndIsrResponseData.LeaderAndIsrTopicError
import org.apache.kafka.common.message.LeaderAndIsrResponseData.LeaderAndIsrTopicErrorCollection
import org.apache.kafka.common.message.LeaveGroupRequestData.MemberIdentity
import org.apache.kafka.common.message.LeaveGroupResponseData
import org.apache.kafka.common.message.ListGroupsRequestData
import org.apache.kafka.common.message.ListGroupsResponseData
import org.apache.kafka.common.message.ListGroupsResponseData.ListedGroup
import org.apache.kafka.common.message.ListOffsetsRequestData.ListOffsetsPartition
import org.apache.kafka.common.message.ListOffsetsRequestData.ListOffsetsTopic
import org.apache.kafka.common.message.ListOffsetsResponseData
import org.apache.kafka.common.message.ListOffsetsResponseData.ListOffsetsPartitionResponse
import org.apache.kafka.common.message.ListOffsetsResponseData.ListOffsetsTopicResponse
import org.apache.kafka.common.message.ListPartitionReassignmentsRequestData
import org.apache.kafka.common.message.ListPartitionReassignmentsRequestData.ListPartitionReassignmentsTopics
import org.apache.kafka.common.message.ListPartitionReassignmentsResponseData
import org.apache.kafka.common.message.ListPartitionReassignmentsResponseData.OngoingPartitionReassignment
import org.apache.kafka.common.message.ListPartitionReassignmentsResponseData.OngoingTopicReassignment
import org.apache.kafka.common.message.ListTransactionsRequestData
import org.apache.kafka.common.message.ListTransactionsResponseData
import org.apache.kafka.common.message.OffsetCommitRequestData
import org.apache.kafka.common.message.OffsetCommitRequestData.OffsetCommitRequestPartition
import org.apache.kafka.common.message.OffsetCommitRequestData.OffsetCommitRequestTopic
import org.apache.kafka.common.message.OffsetCommitResponseData
import org.apache.kafka.common.message.OffsetCommitResponseData.OffsetCommitResponsePartition
import org.apache.kafka.common.message.OffsetCommitResponseData.OffsetCommitResponseTopic
import org.apache.kafka.common.message.OffsetDeleteRequestData
import org.apache.kafka.common.message.OffsetDeleteRequestData.OffsetDeleteRequestPartition
import org.apache.kafka.common.message.OffsetDeleteRequestData.OffsetDeleteRequestTopic
import org.apache.kafka.common.message.OffsetDeleteRequestData.OffsetDeleteRequestTopicCollection
import org.apache.kafka.common.message.OffsetDeleteResponseData
import org.apache.kafka.common.message.OffsetDeleteResponseData.OffsetDeleteResponsePartition
import org.apache.kafka.common.message.OffsetDeleteResponseData.OffsetDeleteResponsePartitionCollection
import org.apache.kafka.common.message.OffsetDeleteResponseData.OffsetDeleteResponseTopic
import org.apache.kafka.common.message.OffsetDeleteResponseData.OffsetDeleteResponseTopicCollection
import org.apache.kafka.common.message.OffsetForLeaderEpochRequestData.OffsetForLeaderPartition
import org.apache.kafka.common.message.OffsetForLeaderEpochRequestData.OffsetForLeaderTopic
import org.apache.kafka.common.message.OffsetForLeaderEpochRequestData.OffsetForLeaderTopicCollection
import org.apache.kafka.common.message.OffsetForLeaderEpochResponseData
import org.apache.kafka.common.message.OffsetForLeaderEpochResponseData.OffsetForLeaderTopicResult
import org.apache.kafka.common.message.ProduceRequestData
import org.apache.kafka.common.message.ProduceRequestData.PartitionProduceData
import org.apache.kafka.common.message.ProduceRequestData.TopicProduceData
import org.apache.kafka.common.message.ProduceRequestData.TopicProduceDataCollection
import org.apache.kafka.common.message.ProduceResponseData.PartitionProduceResponse
import org.apache.kafka.common.message.ProduceResponseData.TopicProduceResponse
import org.apache.kafka.common.message.RenewDelegationTokenRequestData
import org.apache.kafka.common.message.RenewDelegationTokenResponseData
import org.apache.kafka.common.message.SaslAuthenticateRequestData
import org.apache.kafka.common.message.SaslAuthenticateResponseData
import org.apache.kafka.common.message.SaslHandshakeRequestData
import org.apache.kafka.common.message.SaslHandshakeResponseData
import org.apache.kafka.common.message.StopReplicaRequestData.StopReplicaPartitionState
import org.apache.kafka.common.message.StopReplicaRequestData.StopReplicaTopicState
import org.apache.kafka.common.message.StopReplicaResponseData
import org.apache.kafka.common.message.StopReplicaResponseData.StopReplicaPartitionError
import org.apache.kafka.common.message.SyncGroupRequestData
import org.apache.kafka.common.message.SyncGroupRequestData.SyncGroupRequestAssignment
import org.apache.kafka.common.message.SyncGroupResponseData
import org.apache.kafka.common.message.UnregisterBrokerRequestData
import org.apache.kafka.common.message.UnregisterBrokerResponseData
import org.apache.kafka.common.message.UpdateFeaturesRequestData
import org.apache.kafka.common.message.UpdateFeaturesRequestData.FeatureUpdateKey
import org.apache.kafka.common.message.UpdateFeaturesRequestData.FeatureUpdateKeyCollection
import org.apache.kafka.common.message.UpdateFeaturesResponseData
import org.apache.kafka.common.message.UpdateFeaturesResponseData.UpdatableFeatureResult
import org.apache.kafka.common.message.UpdateFeaturesResponseData.UpdatableFeatureResultCollection
import org.apache.kafka.common.message.UpdateMetadataRequestData.UpdateMetadataBroker
import org.apache.kafka.common.message.UpdateMetadataRequestData.UpdateMetadataEndpoint
import org.apache.kafka.common.message.UpdateMetadataRequestData.UpdateMetadataPartitionState
import org.apache.kafka.common.message.UpdateMetadataResponseData
import org.apache.kafka.common.message.VoteRequestData
import org.apache.kafka.common.message.VoteResponseData
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.ByteBufferAccessor
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.protocol.ObjectSerializationCache
import org.apache.kafka.common.protocol.types.RawTaggedField
import org.apache.kafka.common.quota.ClientQuotaAlteration
import org.apache.kafka.common.quota.ClientQuotaEntity
import org.apache.kafka.common.quota.ClientQuotaFilter
import org.apache.kafka.common.record.CompressionType
import org.apache.kafka.common.record.MemoryRecords
import org.apache.kafka.common.record.RecordBatch
import org.apache.kafka.common.record.SimpleRecord
import org.apache.kafka.common.requests.DescribeConfigsResponse.ConfigSource
import org.apache.kafka.common.requests.FindCoordinatorRequest.CoordinatorType
import org.apache.kafka.common.requests.FindCoordinatorRequest.NoBatchedFindCoordinatorsException
import org.apache.kafka.common.requests.MetadataResponse.PartitionMetadata
import org.apache.kafka.common.requests.ProduceResponse.RecordError
import org.apache.kafka.common.requests.RequestTestUtils.metadataResponse
import org.apache.kafka.common.requests.TxnOffsetCommitRequest.CommittedOffset
import org.apache.kafka.common.requests.WriteTxnMarkersRequest.TxnMarkerEntry
import org.apache.kafka.common.resource.PatternType
import org.apache.kafka.common.resource.ResourcePattern
import org.apache.kafka.common.resource.ResourcePatternFilter
import org.apache.kafka.common.resource.ResourceType
import org.apache.kafka.common.security.auth.KafkaPrincipal
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.security.token.delegation.DelegationToken
import org.apache.kafka.common.security.token.delegation.TokenInformation
import org.apache.kafka.common.utils.SecurityUtils.parseKafkaPrincipal
import org.junit.jupiter.api.Test
import kotlin.test.assertContains
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlin.test.fail

// This class performs tests requests and responses for all API keys
class RequestResponseTest {

    // Exception includes a message that we verify is not included in error responses
    private val unknownServerException = UnknownServerException("secret")

    @Test
    fun testSerialization() {
        val toSkip = mapOf(
            // It's not possible to create a MetadataRequest v0 via the builder
            ApiKeys.METADATA to listOf<Short>(0),
            // DescribeLogDirsResponse v0, v1 and v2 don't have a top level error field
            ApiKeys.DESCRIBE_LOG_DIRS to listOf<Short>(0, 1, 2),
            // ElectLeaders v0 does not have a top level error field, when accessing it, it defaults to NONE
            ApiKeys.ELECT_LEADERS to listOf<Short>(0),
        )
        for (apikey in ApiKeys.values()) {
            for (version in apikey.allVersions()) {
                if (toSkip.containsKey(apikey) && toSkip[apikey]!!.contains(version)) continue
                val request = getRequest(apikey, version)
                checkRequest(request)
                checkErrorResponse(request, unknownServerException)
                checkResponse(getResponse(apikey, version), version)
            }
        }
    }

    // This test validates special cases that are not checked in testSerialization
    @Test
    fun testSerializationSpecialCases() {
        // Produce
        checkResponse(response = createProduceResponseWithErrorMessage(), version = 8)
        // Fetch
        checkResponse(response = createFetchResponse(true), version = 4)
        val toForgetTopics = listOf(
            TopicIdPartition(Uuid.ZERO_UUID, TopicPartition("foo", 0)),
            TopicIdPartition(Uuid.ZERO_UUID, TopicPartition("foo", 2)),
            TopicIdPartition(Uuid.ZERO_UUID, TopicPartition("bar", 0)),
        )
        checkRequest(
            createFetchRequest(
                version = 7,
                metadata = FetchMetadata(sessionId = 123, epoch = 456),
                toForget = toForgetTopics,
            )
        )
        checkResponse(
            response = createFetchResponse(sessionId = 123),
            version = 7,
        )
        checkResponse(
            response = createFetchResponse(
                error = Errors.FETCH_SESSION_ID_NOT_FOUND,
                sessionId = 123,
            ),
            version = 7,
        )
        checkOlderFetchVersions()
        // Metadata
        checkRequest(MetadataRequest.Builder.allTopics().build(2))
        // OffsetFetch
        checkRequest(createOffsetFetchRequestWithMultipleGroups(version = 8, requireStable = true))
        checkRequest(createOffsetFetchRequestWithMultipleGroups(version = 8, requireStable = false))
        checkRequest(createOffsetFetchRequestForAllPartition(version = 7, requireStable = true))
        checkRequest(createOffsetFetchRequestForAllPartition(version = 8, requireStable = true))
        checkErrorResponse(
            req = createOffsetFetchRequestWithMultipleGroups(version = 8, requireStable = true),
            e = unknownServerException,
        )
        checkErrorResponse(
            req = createOffsetFetchRequestForAllPartition(version = 7, requireStable = true),
            e = NotCoordinatorException("Not Coordinator")
        )
        checkErrorResponse(
            req = createOffsetFetchRequestForAllPartition(version = 8, requireStable = true),
            e = NotCoordinatorException("Not Coordinator")
        )
        checkErrorResponse(
            req = createOffsetFetchRequestWithMultipleGroups(version = 8, requireStable = true),
            e = NotCoordinatorException("Not Coordinator"),
        )
        // StopReplica
        for (version in ApiKeys.STOP_REPLICA.allVersions()) {
            checkRequest(createStopReplicaRequest(version = version, deletePartitions = false))
            checkErrorResponse(
                req = createStopReplicaRequest(version = version, deletePartitions = false),
                e = unknownServerException,
            )
        }
        // CreatePartitions
        for (version in ApiKeys.CREATE_PARTITIONS.allVersions()) {
            checkRequest(createCreatePartitionsRequestWithAssignments(version))
        }
        // UpdateMetadata
        for (version in ApiKeys.UPDATE_METADATA.allVersions()) {
            checkRequest(createUpdateMetadataRequest(version = version, rack = null))
            checkErrorResponse(
                req = createUpdateMetadataRequest(version = version, rack = null),
                e = unknownServerException,
            )
        }
        // LeaderForEpoch
        checkRequest(createLeaderEpochRequestForConsumer())
        checkErrorResponse(
            req = createLeaderEpochRequestForConsumer(),
            e = unknownServerException,
        )
        // TxnOffsetCommit
        checkRequest(createTxnOffsetCommitRequestWithAutoDowngrade())
        checkErrorResponse(
            req = createTxnOffsetCommitRequestWithAutoDowngrade(),
            e = unknownServerException,
        )
        // DescribeAcls
        checkErrorResponse(
            req = createDescribeAclsRequest(version = 0),
            e = SecurityDisabledException("Security is not enabled."),
        )
        checkErrorResponse(
            req = createCreateAclsRequest(version = 0),
            e = SecurityDisabledException("Security is not enabled."),
        )
        // DeleteAcls
        checkErrorResponse(
            req = createDeleteAclsRequest(version = 0),
            e = SecurityDisabledException("Security is not enabled."),
        )
        // DescribeConfigs
        checkRequest(createDescribeConfigsRequestWithConfigEntries(version = 0))
        checkRequest(createDescribeConfigsRequestWithConfigEntries(version = 1))
        checkRequest(createDescribeConfigsRequestWithDocumentation(version = 1))
        checkRequest(createDescribeConfigsRequestWithDocumentation(version = 2))
        checkRequest(createDescribeConfigsRequestWithDocumentation(version = 3))
        checkDescribeConfigsResponseVersions()
        // ElectLeaders
        checkRequest(createElectLeadersRequestNullPartitions())
    }

    @Test
    fun testApiVersionsSerialization() {
        for (version in ApiKeys.API_VERSIONS.allVersions()) {
            checkErrorResponse(createApiVersionRequest(version), UnsupportedVersionException("Not Supported"))
            checkResponse(
                response = ApiVersionsResponse.defaultApiVersionsResponse(
                    listenerType = ApiMessageType.ListenerType.ZK_BROKER,
                ),
                version = version,
            )
        }
    }

    @Test
    fun testBatchedFindCoordinatorRequestSerialization() {
        for (version: Short in ApiKeys.FIND_COORDINATOR.allVersions()) {
            checkRequest(createBatchedFindCoordinatorRequest(listOf("group1"), version))
            if (version < FindCoordinatorRequest.MIN_BATCHED_VERSION)
                assertFailsWith<NoBatchedFindCoordinatorsException> {
                    createBatchedFindCoordinatorRequest(
                        coordinatorKeys = listOf("group1", "group2"),
                        version = version,
                    )
                }
            else checkRequest(
                createBatchedFindCoordinatorRequest(
                    coordinatorKeys = listOf("group1", "group2"),
                    version = version,
                ),
            )
        }
    }

    @Test
    fun testResponseHeader() {
        val header = ResponseHeader(correlationId = 10, headerVersion = 1)
        val serializationCache = ObjectSerializationCache()
        val buffer = ByteBuffer.allocate(header.size(serializationCache))
        header.write(buffer, serializationCache)
        buffer.flip()
        val deserialized = ResponseHeader.parse(buffer, header.headerVersion)
        assertEquals(header.size(), deserialized.size())
        assertEquals(header.correlationId, deserialized.correlationId)
    }

    @Test
    fun cannotUseFindCoordinatorV0ToFindTransactionCoordinator() {
        val builder = FindCoordinatorRequest.Builder(
            FindCoordinatorRequestData()
                .setKeyType(CoordinatorType.TRANSACTION.id)
                .setKey("foobar")
        )
        assertFailsWith<UnsupportedVersionException> { builder.build(version = 0) }
    }

    @Test
    fun testProduceRequestPartitionSize() {
        val tp0 = TopicPartition("test", 0)
        val tp1 = TopicPartition("test", 1)
        val records0 = MemoryRecords.withRecords(
            magic = RecordBatch.MAGIC_VALUE_V2,
            compressionType = CompressionType.NONE,
            records = arrayOf(SimpleRecord(value = "woot".toByteArray())),
        )
        val records1 = MemoryRecords.withRecords(
            magic = RecordBatch.MAGIC_VALUE_V2,
            compressionType = CompressionType.NONE,
            records = arrayOf(
                SimpleRecord(value = "woot".toByteArray()),
                SimpleRecord(value = "woot".toByteArray()),
            ),
        )
        val request = ProduceRequest.forMagic(
            magic = RecordBatch.MAGIC_VALUE_V2,
            data = ProduceRequestData()
                .setTopicData(
                    TopicProduceDataCollection(
                        listOf(
                            TopicProduceData().setName(tp0.topic).setPartitionData(
                                listOf(PartitionProduceData().setIndex(tp0.partition).setRecords(records0))
                            ),
                            TopicProduceData().setName(tp1.topic).setPartitionData(
                                listOf(PartitionProduceData().setIndex(tp1.partition).setRecords(records1))
                            )
                        ).iterator()
                    )
                )
                .setAcks(1)
                .setTimeoutMs(5000)
                .setTransactionalId("transactionalId")
        )
            .build(3)
        assertEquals(2, request.partitionSizes.size)
        assertEquals(records0.sizeInBytes(), request.partitionSizes[tp0])
        assertEquals(records1.sizeInBytes(), request.partitionSizes[tp1])
    }

    @Test
    fun produceRequestToStringTest() {
        val request = createProduceRequest(ApiKeys.PRODUCE.latestVersion())
        assertEquals(1, request.data().topicData.size)
        assertFalse(request.toString(verbose = false).contains("partitionSizes"))
        assertTrue(request.toString(verbose = false).contains("numPartitions=1"))
        assertTrue(request.toString(verbose = true).contains("partitionSizes"))
        assertFalse(request.toString(verbose = true).contains("numPartitions"))
        request.clearPartitionRecords()
        assertFailsWith<IllegalStateException>(
            message = "dataOrException should fail after clearPartitionRecords()",
        ) { request.data() }

        // `toString` should behave the same after `clearPartitionRecords`
        assertFalse(request.toString(verbose = false).contains("partitionSizes"))
        assertTrue(request.toString(verbose = false).contains("numPartitions=1"))
        assertTrue(request.toString(verbose = true).contains("partitionSizes"))
        assertFalse(request.toString(verbose = true).contains("numPartitions"))
    }

    @Test
    fun produceRequestGetErrorResponseTest() {
        val request = createProduceRequest(ApiKeys.PRODUCE.latestVersion())
        var errorResponse = request.getErrorResponse(NotEnoughReplicasException()) as ProduceResponse?
        var topicProduceResponse = errorResponse!!.data().responses.first()
        var partitionProduceResponse = topicProduceResponse.partitionResponses.first()
        assertEquals(Errors.NOT_ENOUGH_REPLICAS, Errors.forCode(partitionProduceResponse.errorCode))
        assertEquals(ProduceResponse.INVALID_OFFSET, partitionProduceResponse.baseOffset)
        assertEquals(RecordBatch.NO_TIMESTAMP, partitionProduceResponse.logAppendTimeMs)
        request.clearPartitionRecords()

        // `getErrorResponse` should behave the same after `clearPartitionRecords`
        errorResponse = request.getErrorResponse(NotEnoughReplicasException()) as ProduceResponse?
        topicProduceResponse = errorResponse!!.data().responses.first()
        partitionProduceResponse = topicProduceResponse.partitionResponses.first()
        assertEquals(Errors.NOT_ENOUGH_REPLICAS, Errors.forCode(partitionProduceResponse.errorCode))
        assertEquals(ProduceResponse.INVALID_OFFSET, partitionProduceResponse.baseOffset)
        assertEquals(RecordBatch.NO_TIMESTAMP, partitionProduceResponse.logAppendTimeMs)
    }

    @Test
    fun fetchResponseVersionTest() {
        val id = Uuid.randomUuid()
        val topicNames = mapOf(id to "test")
        val tp = TopicPartition(topic = "test", partition = 0)
        val records = MemoryRecords.readableRecords(ByteBuffer.allocate(10))
        val partitionData = FetchResponseData.PartitionData()
            .setPartitionIndex(0)
            .setHighWatermark(1000000)
            .setLogStartOffset(-1)
            .setRecords(records)

        // Use zero UUID since we are comparing with old request versions
        val responseData = mapOf(TopicIdPartition(Uuid.ZERO_UUID, tp) to partitionData)
        val tpResponseData = mapOf(tp to partitionData)
        val v0Response = FetchResponse.of(
            error = Errors.NONE,
            throttleTimeMs = 0,
            sessionId = FetchMetadata.INVALID_SESSION_ID,
            responseData = responseData,
        )
        val v1Response = FetchResponse.of(
            error = Errors.NONE,
            throttleTimeMs = 10,
            sessionId = FetchMetadata.INVALID_SESSION_ID,
            responseData = responseData,
        )
        val v0Deserialized = FetchResponse.parse(buffer = v0Response.serialize(version = 0), version = 0)
        val v1Deserialized = FetchResponse.parse(buffer = v1Response.serialize(version = 1), version = 1)
        assertEquals(0, v0Deserialized.throttleTimeMs(), "Throttle time must be zero")
        assertEquals(10, v1Deserialized.throttleTimeMs(), "Throttle time must be 10")
        assertEquals(
            expected = tpResponseData,
            actual = v0Deserialized.responseData(topicNames = topicNames, version = 0),
            message = "Response data does not match",
        )
        assertEquals(
            expected = tpResponseData,
            actual = v1Deserialized.responseData(topicNames = topicNames, version = 1),
            message = "Response data does not match",
        )
        val idResponseData = mapOf(
            TopicIdPartition(id, TopicPartition("test", 0)) to FetchResponseData.PartitionData()
                .setPartitionIndex(0)
                .setHighWatermark(1000000)
                .setLogStartOffset(-1)
                .setRecords(records)
        )
        val idTestResponse = FetchResponse.of(
            error = Errors.NONE,
            throttleTimeMs = 0,
            sessionId = FetchMetadata.INVALID_SESSION_ID,
            responseData = idResponseData,
        )
        val v12Deserialized = FetchResponse.parse(
            buffer = idTestResponse.serialize(version = 12),
            version = 12,
        )
        val newestDeserialized = FetchResponse.parse(
            buffer = idTestResponse.serialize(version = ApiKeys.FETCH.latestVersion()),
            version = ApiKeys.FETCH.latestVersion(),
        )
        assertTrue(v12Deserialized.topicIds().isEmpty())
        assertEquals(1, newestDeserialized.topicIds().size)
        assertTrue(newestDeserialized.topicIds().contains(id))
    }

    @Test
    fun testFetchResponseV4() {
        val topicNames = mapOf(
            Uuid.randomUuid() to "bar",
            Uuid.randomUuid() to "foo",
        )
        val records = MemoryRecords.readableRecords(ByteBuffer.allocate(10))
        val abortedTransactions = listOf(
            AbortedTransaction().setProducerId(10).setFirstOffset(100),
            AbortedTransaction().setProducerId(15).setFirstOffset(50)
        )

        // Use zero UUID since this is an old request version.
        val responseData = mapOf(
            TopicIdPartition(
                topicId = Uuid.ZERO_UUID,
                topicPartition = TopicPartition("bar", 0)
            ) to FetchResponseData.PartitionData()
                .setPartitionIndex(0)
                .setHighWatermark(1000000)
                .setAbortedTransactions(abortedTransactions)
                .setRecords(records),
            TopicIdPartition(
                topicId = Uuid.ZERO_UUID,
                topicPartition = TopicPartition("bar", 1),
            ) to FetchResponseData.PartitionData()
                .setPartitionIndex(1)
                .setHighWatermark(900000)
                .setLastStableOffset(5)
                .setRecords(records),
            TopicIdPartition(
                topicId = Uuid.ZERO_UUID,
                topicPartition = TopicPartition("foo", 0),
            ) to FetchResponseData.PartitionData()
                .setPartitionIndex(0)
                .setHighWatermark(70000)
                .setLastStableOffset(6)
                .setRecords(records),
        )
        val response = FetchResponse.of(
            error = Errors.NONE,
            throttleTimeMs = 10,
            sessionId = FetchMetadata.INVALID_SESSION_ID,
            responseData = responseData,
        )
        val deserialized = FetchResponse.parse(buffer = response.serialize(version = 4), version = 4)
        assertEquals(
            responseData.mapKeys { it.key.topicPartition },
            deserialized.responseData(topicNames, 4.toShort())
        )
    }

    @Test
    @Throws(Exception::class)
    fun verifyFetchResponseFullWrites() {
        verifyFetchResponseFullWrite(
            version = ApiKeys.FETCH.latestVersion(),
            fetchResponse = createFetchResponse(123),
        )
        verifyFetchResponseFullWrite(
            version = ApiKeys.FETCH.latestVersion(),
            fetchResponse = createFetchResponse(Errors.FETCH_SESSION_ID_NOT_FOUND, 123),
        )
        for (version in ApiKeys.FETCH.allVersions()) verifyFetchResponseFullWrite(
            version = version,
            fetchResponse = createFetchResponse(version >= 4),
        )
    }

    @Throws(Exception::class)
    private fun verifyFetchResponseFullWrite(version: Short, fetchResponse: FetchResponse) {
        val correlationId = 15
        val responseHeaderVersion = ApiKeys.FETCH.responseHeaderVersion(version)
        val send = fetchResponse.toSend(
            header = ResponseHeader(correlationId = correlationId, headerVersion = responseHeaderVersion),
            version = version,
        )
        val channel = ByteBufferChannel(send.size())
        send.writeTo(channel)
        channel.close()
        val buf = channel.buffer()

        // read the size
        val size = buf.getInt()
        assertTrue(size > 0)

        // read the header
        val responseHeader = ResponseHeader.parse(channel.buffer(), responseHeaderVersion)
        assertEquals(correlationId, responseHeader.correlationId)
        assertEquals(fetchResponse.serialize(version), buf)
        val deserialized = FetchResponseData(ByteBufferAccessor(buf), version)
        val serializationCache = ObjectSerializationCache()
        assertEquals(size, responseHeader.size() + deserialized.size(serializationCache, version))
    }

    @Test
    fun testCreateTopicRequestV0FailsIfValidateOnly() {
        assertFailsWith<UnsupportedVersionException> {
            createCreateTopicRequest(version = 0, validateOnly = true)
        }
    }

    @Test
    fun testCreateTopicRequestV3FailsIfNoPartitionsOrReplicas() {
        val exception = assertFailsWith<UnsupportedVersionException> {
            val data = CreateTopicsRequestData()
                .setTimeoutMs(123)
                .setValidateOnly(false)
            data.topics.add(
                CreatableTopic().setName("foo").setNumPartitions(CreateTopicsRequest.NO_NUM_PARTITIONS)
                    .setReplicationFactor(1)
            )
            data.topics.add(
                CreatableTopic().setName("bar").setNumPartitions(1)
                    .setReplicationFactor(CreateTopicsRequest.NO_REPLICATION_FACTOR)
            )
            CreateTopicsRequest.Builder(data).build(version = 3)
        }
        assertContains(exception.message!!, "supported in CreateTopicRequest version 4+")
        assertContains(exception.message!!, "[foo, bar]")
    }

    @Test
    fun testFetchRequestMaxBytesOldVersions() {
        val version: Short = 1
        val fr = createFetchRequest(version)
        val fr2 = FetchRequest.parse(fr.serialize(), version)
        assertEquals(fr2.maxBytes(), fr.maxBytes())
    }

    @Test
    fun testFetchRequestIsolationLevel() {
        var request = createFetchRequest(version = 4, isolationLevel = IsolationLevel.READ_COMMITTED)
        var deserialized = AbstractRequest.parseRequest(
            apiKey = request.apiKey,
            apiVersion = request.version,
            buffer = request.serialize(),
        ).request as FetchRequest
        assertEquals(request.isolationLevel(), deserialized.isolationLevel())
        request = createFetchRequest(version = 4, isolationLevel = IsolationLevel.READ_UNCOMMITTED)
        deserialized = AbstractRequest.parseRequest(
            apiKey = request.apiKey,
            apiVersion = request.version,
            buffer = request.serialize(),
        ).request as FetchRequest
        assertEquals(request.isolationLevel(), deserialized.isolationLevel())
    }

    @Test
    fun testFetchRequestWithMetadata() {
        var request = createFetchRequest(version = 4, isolationLevel = IsolationLevel.READ_COMMITTED)
        var deserialized = AbstractRequest.parseRequest(
            apiKey = ApiKeys.FETCH,
            apiVersion = request.version,
            buffer = request.serialize(),
        ).request as FetchRequest
        assertEquals(request.isolationLevel(), deserialized.isolationLevel())
        request = createFetchRequest(version = 4, isolationLevel = IsolationLevel.READ_UNCOMMITTED)
        deserialized = AbstractRequest.parseRequest(
            apiKey = ApiKeys.FETCH,
            apiVersion = request.version,
            buffer = request.serialize(),
        ).request as FetchRequest
        assertEquals(request.isolationLevel(), deserialized.isolationLevel())
    }

    @Test
    fun testFetchRequestCompat() {
        val fetchData = mapOf(
            TopicPartition("test", 0) to FetchRequest.PartitionData(
                topicId = Uuid.ZERO_UUID,
                fetchOffset = 100,
                logStartOffset = 2,
                maxBytes = 100,
                currentLeaderEpoch = 42,
            ),
        )
        val req = FetchRequest.Builder
            .forConsumer(
                maxVersion = 2,
                maxWait = 100,
                minBytes = 100,
                fetchData = fetchData,
            )
            .metadata(FetchMetadata(sessionId = 10, epoch = 20))
            .isolationLevel(IsolationLevel.READ_COMMITTED)
            .build(2)
        val data = req.data()
        val cache = ObjectSerializationCache()
        val size = data.size(cache, 2.toShort())
        val writer = ByteBufferAccessor(ByteBuffer.allocate(size))
        data.write(writable = writer, cache = cache, version = 2)
    }

    @Test
    fun testSerializeWithHeader() {
        val topicsToCreate = CreatableTopicCollection(expectedNumElements = 1)
        topicsToCreate.add(
            CreatableTopic()
                .setName("topic")
                .setNumPartitions(3)
                .setReplicationFactor(2)
        )
        val createTopicsRequest = CreateTopicsRequest.Builder(
            CreateTopicsRequestData()
                .setTimeoutMs(10)
                .setTopics(topicsToCreate)
        ).build()
        val requestVersion = ApiKeys.CREATE_TOPICS.latestVersion()
        val requestHeader = RequestHeader(
            requestApiKey = ApiKeys.CREATE_TOPICS,
            requestVersion = requestVersion,
            clientId = "client",
            correlationId = 2,
        )
        val serializedRequest = createTopicsRequest.serializeWithHeader(requestHeader)
        val parsedHeader = RequestHeader.parse(serializedRequest)
        assertEquals(requestHeader.size(), parsedHeader.size())
        assertEquals(requestHeader, parsedHeader)
        val parsedRequest = AbstractRequest.parseRequest(
            apiKey = ApiKeys.CREATE_TOPICS,
            apiVersion = requestVersion,
            buffer = serializedRequest,
        )
        assertEquals(createTopicsRequest.data(), parsedRequest.request.data())
    }

    @Test
    fun testSerializeWithInconsistentHeaderApiKey() {
        val createTopicsRequest = CreateTopicsRequest.Builder(CreateTopicsRequestData()).build()
        val requestVersion = ApiKeys.CREATE_TOPICS.latestVersion()
        val requestHeader = RequestHeader(
            requestApiKey = ApiKeys.DELETE_TOPICS,
            requestVersion = requestVersion,
            clientId = "client",
            correlationId = 2,
        )
        assertFailsWith<IllegalArgumentException> { createTopicsRequest.serializeWithHeader(requestHeader) }
    }

    @Test
    fun testSerializeWithInconsistentHeaderVersion() {
        val createTopicsRequest = CreateTopicsRequest.Builder(CreateTopicsRequestData()).build(version = 2)
        val requestHeader = RequestHeader(
            requestApiKey = ApiKeys.CREATE_TOPICS,
            requestVersion = 1.toShort(),
            clientId = "client",
            correlationId = 2,
        )
        assertFailsWith<IllegalArgumentException> { createTopicsRequest.serializeWithHeader(requestHeader) }
    }

    @Test
    fun testJoinGroupRequestV0RebalanceTimeout() {
        val version: Short = 0
        val jgr = createJoinGroupRequest(version)
        val jgr2 = JoinGroupRequest.parse(jgr.serialize(), version)
        assertEquals(jgr2.data().rebalanceTimeoutMs, jgr.data().rebalanceTimeoutMs)
    }

    @Test
    fun testOffsetFetchRequestBuilderToStringV0ToV7() {
        val stableFlags = listOf(true, false)
        for (requireStable in stableFlags) {
            val allTopicPartitionsString = OffsetFetchRequest.Builder(
                groupId = "someGroup",
                requireStable = requireStable,
                partitions = null,
                throwOnFetchStableOffsetsUnsupported = false,
            ).toString()
            assertContains(
                charSequence = allTopicPartitionsString,
                other = "groupId='someGroup', topics=null, groups=[], requireStable=$requireStable",
            )
            val string = OffsetFetchRequest.Builder(
                groupId = "group1",
                requireStable = requireStable,
                partitions = listOf(TopicPartition("test11", 1)),
                throwOnFetchStableOffsetsUnsupported = false,
            ).toString()
            assertContains(string, "test11")
            assertContains(string, "group1")
            assertContains(string, "requireStable=$requireStable")
        }
    }

    @Test
    fun testOffsetFetchRequestBuilderToStringV8AndAbove() {
        val stableFlags = listOf(true, false)
        for (requireStable in stableFlags) {
            val allTopicPartitionsString = OffsetFetchRequest.Builder(
                groupIdToTopicPartitionMap = mapOf("someGroup" to null),
                requireStable = requireStable,
                throwOnFetchStableOffsetsUnsupported = false,
            ).toString()
            assertContains(
                charSequence = allTopicPartitionsString,
                other = "groups=[OffsetFetchRequestGroup(groupId='someGroup', topics=null)], requireStable=$requireStable",
            )
            val subsetTopicPartitionsString = OffsetFetchRequest.Builder(
                groupIdToTopicPartitionMap = mapOf("group1" to listOf(TopicPartition("test11", 1))),
                requireStable = requireStable,
                throwOnFetchStableOffsetsUnsupported = false,
            ).toString()
            assertContains(subsetTopicPartitionsString, "test11")
            assertContains(subsetTopicPartitionsString, "group1")
            assertContains(subsetTopicPartitionsString, "requireStable=$requireStable")
        }
    }

    @Test
    fun testApiVersionsRequestBeforeV3Validation() {
        for (version in 0..2) {
            val request = ApiVersionsRequest(
                data = ApiVersionsRequestData(),
                version = version.toShort(),
            )
            assertTrue(request.isValid)
        }
    }

    @Test
    fun testValidApiVersionsRequest() {
        var request = ApiVersionsRequest.Builder().build()
        assertTrue(request.isValid)
        request = ApiVersionsRequest(
            data = ApiVersionsRequestData()
                .setClientSoftwareName("apache-kafka.java")
                .setClientSoftwareVersion("0.0.0-SNAPSHOT"),
            version = ApiKeys.API_VERSIONS.latestVersion(),
        )
        assertTrue(request.isValid)
    }

    @Test
    fun testListGroupRequestV3FailsWithStates() {
        val data = ListGroupsRequestData().setStatesFilter(listOf(ConsumerGroupState.STABLE.name))
        assertFailsWith<UnsupportedVersionException> { ListGroupsRequest.Builder(data).build(version = 3) }
    }

    @Test
    fun testInvalidApiVersionsRequest() {
        testInvalidCase("java@apache_kafka", "0.0.0-SNAPSHOT")
        testInvalidCase("apache-kafka-java", "0.0.0@java")
        testInvalidCase("-apache-kafka-java", "0.0.0")
        testInvalidCase("apache-kafka-java.", "0.0.0")
    }

    private fun testInvalidCase(name: String, version: String) {
        val request = ApiVersionsRequest(
            ApiVersionsRequestData()
                .setClientSoftwareName(name)
                .setClientSoftwareVersion(version),
            ApiKeys.API_VERSIONS.latestVersion()
        )
        assertFalse(request.isValid)
    }

    @Test
    fun testApiVersionResponseWithUnsupportedError() {
        for (version: Short in ApiKeys.API_VERSIONS.allVersions()) {
            val request = ApiVersionsRequest.Builder().build(version)
            val response = request.getErrorResponse(
                throttleTimeMs = 0,
                e = Errors.UNSUPPORTED_VERSION.exception!!
            )
            assertEquals(Errors.UNSUPPORTED_VERSION.code, response.data().errorCode)
            val apiVersion = assertNotNull(response.data().apiKeys.find(ApiKeys.API_VERSIONS.id))
            assertEquals(ApiKeys.API_VERSIONS.id, apiVersion.apiKey)
            assertEquals(ApiKeys.API_VERSIONS.oldestVersion(), apiVersion.minVersion)
            assertEquals(ApiKeys.API_VERSIONS.latestVersion(), apiVersion.maxVersion)
        }
    }

    @Test
    fun testApiVersionResponseWithNotUnsupportedError() {
        for (version in ApiKeys.API_VERSIONS.allVersions()) {
            val request = ApiVersionsRequest.Builder().build(version)
            val response = request.getErrorResponse(
                throttleTimeMs = 0,
                e = Errors.INVALID_REQUEST.exception!!,
            )
            assertEquals(response.data().errorCode, Errors.INVALID_REQUEST.code)
            assertTrue(response.data().apiKeys.isEmpty())
        }
    }

    private fun defaultApiVersionsResponse(): ApiVersionsResponse {
        return ApiVersionsResponse.defaultApiVersionsResponse(listenerType = ApiMessageType.ListenerType.ZK_BROKER)
    }

    @Test
    fun testApiVersionResponseParsingFallback() {
        for (version: Short in ApiKeys.API_VERSIONS.allVersions()) {
            val buffer = defaultApiVersionsResponse().serialize(0)
            val response = ApiVersionsResponse.parse(buffer, version)
            assertEquals(Errors.NONE.code, response.data().errorCode)
        }
    }

    @Test
    fun testApiVersionResponseParsingFallbackException() {
        for (version in ApiKeys.API_VERSIONS.allVersions()) {
            assertFailsWith<BufferUnderflowException> {
                ApiVersionsResponse.parse(
                    buffer = ByteBuffer.allocate(0),
                    version = version,
                )
            }
        }
    }

    @Test
    fun testApiVersionResponseParsing() {
        for (version in ApiKeys.API_VERSIONS.allVersions()) {
            val buffer = defaultApiVersionsResponse().serialize(version)
            val response = ApiVersionsResponse.parse(buffer, version)
            assertEquals(Errors.NONE.code, response.data().errorCode)
        }
    }

    @Test
    fun testInitProducerIdRequestVersions() {
        val bld = InitProducerIdRequest.Builder(
            InitProducerIdRequestData()
                .setTransactionTimeoutMs(1000)
                .setTransactionalId("abracadabra")
                .setProducerId(123)
        )
        val exception = assertFailsWith<UnsupportedVersionException> { bld.build(version = 2).serialize() }
        assertTrue(exception.message!!.contains("Attempted to write a non-default producerId at version 2"))
        bld.build(version = 3)
    }

    @Test
    fun testDeletableTopicResultErrorMessageIsNullByDefault() {
        val result = DeletableTopicResult()
            .setName("topic")
            .setErrorCode(Errors.THROTTLING_QUOTA_EXCEEDED.code)
        assertEquals("topic", result.name)
        assertEquals(Errors.THROTTLING_QUOTA_EXCEEDED.code, result.errorCode)
        assertNull(result.errorMessage)
    }

    /**
     * Check that all error codes in the response get included in [AbstractResponse.errorCounts].
     */
    @Test
    fun testErrorCountsIncludesNone() {
        assertEquals(1, createAddOffsetsToTxnResponse().errorCounts()[Errors.NONE])
        assertEquals(1, createAddPartitionsToTxnResponse().errorCounts()[Errors.NONE])
        assertEquals(1, createAlterClientQuotasResponse().errorCounts()[Errors.NONE])
        assertEquals(1, createAlterConfigsResponse().errorCounts()[Errors.NONE])
        assertEquals(2, createAlterPartitionReassignmentsResponse().errorCounts()[Errors.NONE])
        assertEquals(1, createAlterReplicaLogDirsResponse().errorCounts()[Errors.NONE])
        assertEquals(1, createApiVersionResponse().errorCounts()[Errors.NONE])
        assertEquals(1, createBrokerHeartbeatResponse().errorCounts()[Errors.NONE])
        assertEquals(1, createBrokerRegistrationResponse().errorCounts()[Errors.NONE])
        assertEquals(1, createControlledShutdownResponse().errorCounts()[Errors.NONE])
        assertEquals(2, createCreateAclsResponse().errorCounts()[Errors.NONE])
        assertEquals(1, createCreatePartitionsResponse().errorCounts()[Errors.NONE])
        assertEquals(1, createCreateTokenResponse().errorCounts()[Errors.NONE])
        assertEquals(1, createCreateTopicResponse().errorCounts()[Errors.NONE])
        assertEquals(1, createDeleteAclsResponse(ApiKeys.DELETE_ACLS.latestVersion()).errorCounts()[Errors.NONE])
        assertEquals(1, createDeleteGroupsResponse().errorCounts()[Errors.NONE])
        assertEquals(1, createDeleteTopicsResponse().errorCounts()[Errors.NONE])
        assertEquals(1, createDescribeAclsResponse().errorCounts()[Errors.NONE])
        assertEquals(1, createDescribeClientQuotasResponse().errorCounts()[Errors.NONE])
        assertEquals(
            2,
            createDescribeConfigsResponse(ApiKeys.DESCRIBE_CONFIGS.latestVersion()).errorCounts()[Errors.NONE]
        )
        assertEquals(1, createDescribeGroupResponse().errorCounts()[Errors.NONE])
        assertEquals(2, createDescribeLogDirsResponse().errorCounts()[Errors.NONE])
        assertEquals(
            1,
            createDescribeTokenResponse(ApiKeys.DESCRIBE_DELEGATION_TOKEN.latestVersion()).errorCounts()[Errors.NONE]
        )
        assertEquals(2, createElectLeadersResponse().errorCounts()[Errors.NONE])
        assertEquals(1, createEndTxnResponse().errorCounts()[Errors.NONE])
        assertEquals(1, createExpireTokenResponse().errorCounts()[Errors.NONE])
        assertEquals(3, createFetchResponse(123).errorCounts()[Errors.NONE])
        assertEquals(
            1,
            createFindCoordinatorResponse(ApiKeys.FIND_COORDINATOR.oldestVersion()).errorCounts()[Errors.NONE]
        )
        assertEquals(
            1,
            createFindCoordinatorResponse(ApiKeys.FIND_COORDINATOR.latestVersion()).errorCounts()[Errors.NONE]
        )
        assertEquals(1, createHeartBeatResponse().errorCounts()[Errors.NONE])
        assertEquals(1, createIncrementalAlterConfigsResponse().errorCounts()[Errors.NONE])
        assertEquals(1, createJoinGroupResponse(ApiKeys.JOIN_GROUP.latestVersion()).errorCounts()[Errors.NONE])
        assertEquals(2, createLeaderAndIsrResponse(4).errorCounts()[Errors.NONE])
        assertEquals(2, createLeaderAndIsrResponse(5).errorCounts()[Errors.NONE])
        assertEquals(3, createLeaderEpochResponse().errorCounts()[Errors.NONE])
        assertEquals(1, createLeaveGroupResponse().errorCounts()[Errors.NONE])
        assertEquals(1, createListGroupsResponse(ApiKeys.LIST_GROUPS.latestVersion()).errorCounts()[Errors.NONE])
        assertEquals(1, createListOffsetResponse(ApiKeys.LIST_OFFSETS.latestVersion()).errorCounts()[Errors.NONE])
        assertEquals(1, createListPartitionReassignmentsResponse().errorCounts()[Errors.NONE])
        assertEquals(3, createMetadataResponse().errorCounts()[Errors.NONE])
        assertEquals(1, createOffsetCommitResponse().errorCounts()[Errors.NONE])
        assertEquals(2, createOffsetDeleteResponse().errorCounts()[Errors.NONE])
        assertEquals(3, createOffsetFetchResponse(ApiKeys.OFFSET_FETCH.latestVersion()).errorCounts()[Errors.NONE])
        assertEquals(1, createProduceResponse().errorCounts()[Errors.NONE])
        assertEquals(1, createRenewTokenResponse().errorCounts()[Errors.NONE])
        assertEquals(1, createSaslAuthenticateResponse().errorCounts()[Errors.NONE])
        assertEquals(1, createSaslHandshakeResponse().errorCounts()[Errors.NONE])
        assertEquals(2, createStopReplicaResponse().errorCounts()[Errors.NONE])
        assertEquals(1, createSyncGroupResponse(ApiKeys.SYNC_GROUP.latestVersion()).errorCounts()[Errors.NONE])
        assertEquals(1, createTxnOffsetCommitResponse().errorCounts()[Errors.NONE])
        assertEquals(1, createUpdateMetadataResponse().errorCounts()[Errors.NONE])
        assertEquals(1, createWriteTxnMarkersResponse().errorCounts()[Errors.NONE])
    }

    private fun getRequest(apikey: ApiKeys, version: Short): AbstractRequest {
        return when (apikey) {
            ApiKeys.PRODUCE -> createProduceRequest(version)
            ApiKeys.FETCH -> createFetchRequest(version)
            ApiKeys.LIST_OFFSETS -> createListOffsetRequest(version)
            ApiKeys.METADATA -> createMetadataRequest(version, listOf("topic1"))
            ApiKeys.LEADER_AND_ISR -> createLeaderAndIsrRequest(version)
            ApiKeys.STOP_REPLICA -> createStopReplicaRequest(version, true)
            ApiKeys.UPDATE_METADATA -> createUpdateMetadataRequest(version, "rack1")
            ApiKeys.CONTROLLED_SHUTDOWN -> createControlledShutdownRequest(version)
            ApiKeys.OFFSET_COMMIT -> createOffsetCommitRequest(version)
            ApiKeys.OFFSET_FETCH -> createOffsetFetchRequest(version, true)
            ApiKeys.FIND_COORDINATOR -> createFindCoordinatorRequest(version)
            ApiKeys.JOIN_GROUP -> createJoinGroupRequest(version)
            ApiKeys.HEARTBEAT -> createHeartBeatRequest(version)
            ApiKeys.LEAVE_GROUP -> createLeaveGroupRequest(version)
            ApiKeys.SYNC_GROUP -> createSyncGroupRequest(version)
            ApiKeys.DESCRIBE_GROUPS -> createDescribeGroupRequest(version)
            ApiKeys.LIST_GROUPS -> createListGroupsRequest(version)
            ApiKeys.SASL_HANDSHAKE -> createSaslHandshakeRequest(version)
            ApiKeys.API_VERSIONS -> createApiVersionRequest(version)
            ApiKeys.CREATE_TOPICS -> createCreateTopicRequest(version)
            ApiKeys.DELETE_TOPICS -> createDeleteTopicsRequest(version)
            ApiKeys.DELETE_RECORDS -> createDeleteRecordsRequest(version)
            ApiKeys.INIT_PRODUCER_ID -> createInitPidRequest(version)
            ApiKeys.OFFSET_FOR_LEADER_EPOCH -> createLeaderEpochRequestForReplica(version, 1)
            ApiKeys.ADD_PARTITIONS_TO_TXN -> createAddPartitionsToTxnRequest(version)
            ApiKeys.ADD_OFFSETS_TO_TXN -> createAddOffsetsToTxnRequest(version)
            ApiKeys.END_TXN -> createEndTxnRequest(version)
            ApiKeys.WRITE_TXN_MARKERS -> createWriteTxnMarkersRequest(version)
            ApiKeys.TXN_OFFSET_COMMIT -> createTxnOffsetCommitRequest(version)
            ApiKeys.DESCRIBE_ACLS -> createDescribeAclsRequest(version)
            ApiKeys.CREATE_ACLS -> createCreateAclsRequest(version)
            ApiKeys.DELETE_ACLS -> createDeleteAclsRequest(version)
            ApiKeys.DESCRIBE_CONFIGS -> createDescribeConfigsRequest(version)
            ApiKeys.ALTER_CONFIGS -> createAlterConfigsRequest(version)
            ApiKeys.ALTER_REPLICA_LOG_DIRS -> createAlterReplicaLogDirsRequest(version)
            ApiKeys.DESCRIBE_LOG_DIRS -> createDescribeLogDirsRequest(version)
            ApiKeys.SASL_AUTHENTICATE -> createSaslAuthenticateRequest(version)
            ApiKeys.CREATE_PARTITIONS -> createCreatePartitionsRequest(version)
            ApiKeys.CREATE_DELEGATION_TOKEN -> createCreateTokenRequest(version)
            ApiKeys.RENEW_DELEGATION_TOKEN -> createRenewTokenRequest(version)
            ApiKeys.EXPIRE_DELEGATION_TOKEN -> createExpireTokenRequest(version)
            ApiKeys.DESCRIBE_DELEGATION_TOKEN -> createDescribeTokenRequest(version)
            ApiKeys.DELETE_GROUPS -> createDeleteGroupsRequest(version)
            ApiKeys.ELECT_LEADERS -> createElectLeadersRequest(version)
            ApiKeys.INCREMENTAL_ALTER_CONFIGS -> createIncrementalAlterConfigsRequest(version)
            ApiKeys.ALTER_PARTITION_REASSIGNMENTS -> createAlterPartitionReassignmentsRequest(version)
            ApiKeys.LIST_PARTITION_REASSIGNMENTS -> createListPartitionReassignmentsRequest(version)
            ApiKeys.OFFSET_DELETE -> createOffsetDeleteRequest(version)
            ApiKeys.DESCRIBE_CLIENT_QUOTAS -> createDescribeClientQuotasRequest(version)
            ApiKeys.ALTER_CLIENT_QUOTAS -> createAlterClientQuotasRequest(version)
            ApiKeys.DESCRIBE_USER_SCRAM_CREDENTIALS -> createDescribeUserScramCredentialsRequest(version)
            ApiKeys.ALTER_USER_SCRAM_CREDENTIALS -> createAlterUserScramCredentialsRequest(version)
            ApiKeys.VOTE -> createVoteRequest(version)
            ApiKeys.BEGIN_QUORUM_EPOCH -> createBeginQuorumEpochRequest(version)
            ApiKeys.END_QUORUM_EPOCH -> createEndQuorumEpochRequest(version)
            ApiKeys.DESCRIBE_QUORUM -> createDescribeQuorumRequest(version)
            ApiKeys.ALTER_PARTITION -> createAlterPartitionRequest(version)
            ApiKeys.UPDATE_FEATURES -> createUpdateFeaturesRequest(version)
            ApiKeys.ENVELOPE -> createEnvelopeRequest(version)
            ApiKeys.FETCH_SNAPSHOT -> createFetchSnapshotRequest(version)
            ApiKeys.DESCRIBE_CLUSTER -> createDescribeClusterRequest(version)
            ApiKeys.DESCRIBE_PRODUCERS -> createDescribeProducersRequest(version)
            ApiKeys.BROKER_REGISTRATION -> createBrokerRegistrationRequest(version)
            ApiKeys.BROKER_HEARTBEAT -> createBrokerHeartbeatRequest(version)
            ApiKeys.UNREGISTER_BROKER -> createUnregisterBrokerRequest(version)
            ApiKeys.DESCRIBE_TRANSACTIONS -> createDescribeTransactionsRequest(version)
            ApiKeys.LIST_TRANSACTIONS -> createListTransactionsRequest(version)
            ApiKeys.ALLOCATE_PRODUCER_IDS -> createAllocateProducerIdsRequest(version)
            else -> throw IllegalArgumentException("Unknown API key $apikey")
        }
    }

    private fun getResponse(apikey: ApiKeys, version: Short): AbstractResponse {
        return when (apikey) {
            ApiKeys.PRODUCE -> createProduceResponse()
            ApiKeys.FETCH -> createFetchResponse(version)
            ApiKeys.LIST_OFFSETS -> createListOffsetResponse(version)
            ApiKeys.METADATA -> createMetadataResponse()
            ApiKeys.LEADER_AND_ISR -> createLeaderAndIsrResponse(version)
            ApiKeys.STOP_REPLICA -> createStopReplicaResponse()
            ApiKeys.UPDATE_METADATA -> createUpdateMetadataResponse()
            ApiKeys.CONTROLLED_SHUTDOWN -> createControlledShutdownResponse()
            ApiKeys.OFFSET_COMMIT -> createOffsetCommitResponse()
            ApiKeys.OFFSET_FETCH -> createOffsetFetchResponse(version)
            ApiKeys.FIND_COORDINATOR -> createFindCoordinatorResponse(version)
            ApiKeys.JOIN_GROUP -> createJoinGroupResponse(version)
            ApiKeys.HEARTBEAT -> createHeartBeatResponse()
            ApiKeys.LEAVE_GROUP -> createLeaveGroupResponse()
            ApiKeys.SYNC_GROUP -> createSyncGroupResponse(version)
            ApiKeys.DESCRIBE_GROUPS -> createDescribeGroupResponse()
            ApiKeys.LIST_GROUPS -> createListGroupsResponse(version)
            ApiKeys.SASL_HANDSHAKE -> createSaslHandshakeResponse()
            ApiKeys.API_VERSIONS -> createApiVersionResponse()
            ApiKeys.CREATE_TOPICS -> createCreateTopicResponse()
            ApiKeys.DELETE_TOPICS -> createDeleteTopicsResponse()
            ApiKeys.DELETE_RECORDS -> createDeleteRecordsResponse()
            ApiKeys.INIT_PRODUCER_ID -> createInitPidResponse()
            ApiKeys.OFFSET_FOR_LEADER_EPOCH -> createLeaderEpochResponse()
            ApiKeys.ADD_PARTITIONS_TO_TXN -> createAddPartitionsToTxnResponse()
            ApiKeys.ADD_OFFSETS_TO_TXN -> createAddOffsetsToTxnResponse()
            ApiKeys.END_TXN -> createEndTxnResponse()
            ApiKeys.WRITE_TXN_MARKERS -> createWriteTxnMarkersResponse()
            ApiKeys.TXN_OFFSET_COMMIT -> createTxnOffsetCommitResponse()
            ApiKeys.DESCRIBE_ACLS -> createDescribeAclsResponse()
            ApiKeys.CREATE_ACLS -> createCreateAclsResponse()
            ApiKeys.DELETE_ACLS -> createDeleteAclsResponse(version)
            ApiKeys.DESCRIBE_CONFIGS -> createDescribeConfigsResponse(version)
            ApiKeys.ALTER_CONFIGS -> createAlterConfigsResponse()
            ApiKeys.ALTER_REPLICA_LOG_DIRS -> createAlterReplicaLogDirsResponse()
            ApiKeys.DESCRIBE_LOG_DIRS -> createDescribeLogDirsResponse()
            ApiKeys.SASL_AUTHENTICATE -> createSaslAuthenticateResponse()
            ApiKeys.CREATE_PARTITIONS -> createCreatePartitionsResponse()
            ApiKeys.CREATE_DELEGATION_TOKEN -> createCreateTokenResponse()
            ApiKeys.RENEW_DELEGATION_TOKEN -> createRenewTokenResponse()
            ApiKeys.EXPIRE_DELEGATION_TOKEN -> createExpireTokenResponse()
            ApiKeys.DESCRIBE_DELEGATION_TOKEN -> createDescribeTokenResponse(version)
            ApiKeys.DELETE_GROUPS -> createDeleteGroupsResponse()
            ApiKeys.ELECT_LEADERS -> createElectLeadersResponse()
            ApiKeys.INCREMENTAL_ALTER_CONFIGS -> createIncrementalAlterConfigsResponse()
            ApiKeys.ALTER_PARTITION_REASSIGNMENTS -> createAlterPartitionReassignmentsResponse()
            ApiKeys.LIST_PARTITION_REASSIGNMENTS -> createListPartitionReassignmentsResponse()
            ApiKeys.OFFSET_DELETE -> createOffsetDeleteResponse()
            ApiKeys.DESCRIBE_CLIENT_QUOTAS -> createDescribeClientQuotasResponse()
            ApiKeys.ALTER_CLIENT_QUOTAS -> createAlterClientQuotasResponse()
            ApiKeys.DESCRIBE_USER_SCRAM_CREDENTIALS -> createDescribeUserScramCredentialsResponse()
            ApiKeys.ALTER_USER_SCRAM_CREDENTIALS -> createAlterUserScramCredentialsResponse()
            ApiKeys.VOTE -> createVoteResponse()
            ApiKeys.BEGIN_QUORUM_EPOCH -> createBeginQuorumEpochResponse()
            ApiKeys.END_QUORUM_EPOCH -> createEndQuorumEpochResponse()
            ApiKeys.DESCRIBE_QUORUM -> createDescribeQuorumResponse()
            ApiKeys.ALTER_PARTITION -> createAlterPartitionResponse(version.toInt())
            ApiKeys.UPDATE_FEATURES -> createUpdateFeaturesResponse()
            ApiKeys.ENVELOPE -> createEnvelopeResponse()
            ApiKeys.FETCH_SNAPSHOT -> createFetchSnapshotResponse()
            ApiKeys.DESCRIBE_CLUSTER -> createDescribeClusterResponse()
            ApiKeys.DESCRIBE_PRODUCERS -> createDescribeProducersResponse()
            ApiKeys.BROKER_REGISTRATION -> createBrokerRegistrationResponse()
            ApiKeys.BROKER_HEARTBEAT -> createBrokerHeartbeatResponse()
            ApiKeys.UNREGISTER_BROKER -> createUnregisterBrokerResponse()
            ApiKeys.DESCRIBE_TRANSACTIONS -> createDescribeTransactionsResponse()
            ApiKeys.LIST_TRANSACTIONS -> createListTransactionsResponse()
            ApiKeys.ALLOCATE_PRODUCER_IDS -> createAllocateProducerIdsResponse()
            else -> throw IllegalArgumentException("Unknown API key $apikey")
        }
    }

    private fun createFetchSnapshotRequest(version: Short): FetchSnapshotRequest {
        val data = FetchSnapshotRequestData()
            .setClusterId("clusterId")
            .setTopics(
                listOf(
                    FetchSnapshotRequestData.TopicSnapshot()
                        .setName("topic1")
                        .setPartitions(
                            listOf(
                                FetchSnapshotRequestData.PartitionSnapshot()
                                    .setSnapshotId(
                                        FetchSnapshotRequestData.SnapshotId()
                                            .setEndOffset(123L)
                                            .setEpoch(0)
                                    )
                                    .setPosition(123L)
                                    .setPartition(0)
                                    .setCurrentLeaderEpoch(1)
                            )
                        )
                )
            )
            .setMaxBytes(1000)
            .setReplicaId(2)
        return FetchSnapshotRequest.Builder(data).build(version)
    }

    private fun createFetchSnapshotResponse(): FetchSnapshotResponse {
        val data = FetchSnapshotResponseData()
            .setErrorCode(Errors.NONE.code)
            .setTopics(
                listOf(
                    FetchSnapshotResponseData.TopicSnapshot()
                        .setName("topic1")
                        .setPartitions(
                            listOf(
                                FetchSnapshotResponseData.PartitionSnapshot()
                                    .setErrorCode(Errors.NONE.code)
                                    .setIndex(0)
                                    .setCurrentLeader(
                                        FetchSnapshotResponseData.LeaderIdAndEpoch()
                                            .setLeaderEpoch(0)
                                            .setLeaderId(1)
                                    )
                                    .setSnapshotId(
                                        FetchSnapshotResponseData.SnapshotId()
                                            .setEndOffset(123L)
                                            .setEpoch(0)
                                    )
                                    .setPosition(234L)
                                    .setSize(345L)
                                    .setUnalignedRecords(
                                        MemoryRecords.withRecords(
                                            compressionType = CompressionType.NONE,
                                            records = arrayOf(SimpleRecord(value = "blah".toByteArray())),
                                        )
                                    )
                            )
                        )
                )
            )
            .setThrottleTimeMs(123)
        return FetchSnapshotResponse(data)
    }

    private fun createEnvelopeRequest(version: Short): EnvelopeRequest {
        return EnvelopeRequest.Builder(
            requestData = ByteBuffer.wrap("data".toByteArray()),
            serializedPrincipal = "principal".toByteArray(),
            clientAddress = "address".toByteArray(),
        ).build(version)
    }

    private fun createEnvelopeResponse(): EnvelopeResponse {
        val data = EnvelopeResponseData()
            .setResponseData(ByteBuffer.wrap("data".toByteArray()))
            .setErrorCode(Errors.NONE.code)
        return EnvelopeResponse(data)
    }

    private fun createDescribeQuorumRequest(version: Short): DescribeQuorumRequest {
        val data = DescribeQuorumRequestData()
            .setTopics(
                listOf(
                    DescribeQuorumRequestData.TopicData()
                        .setPartitions(
                            listOf(
                                DescribeQuorumRequestData.PartitionData()
                                    .setPartitionIndex(0),
                            )
                        ).setTopicName("topic1")
                )
            )
        return DescribeQuorumRequest.Builder(data).build(version)
    }

    private fun createDescribeQuorumResponse(): DescribeQuorumResponse {
        val data = DescribeQuorumResponseData().setErrorCode(Errors.NONE.code)
        return DescribeQuorumResponse(data)
    }

    private fun createEndQuorumEpochRequest(version: Short): EndQuorumEpochRequest {
        val data = EndQuorumEpochRequestData()
            .setClusterId("clusterId")
            .setTopics(
                listOf(
                    EndQuorumEpochRequestData.TopicData()
                        .setPartitions(
                            listOf(
                                EndQuorumEpochRequestData.PartitionData()
                                    .setLeaderEpoch(0)
                                    .setLeaderId(1)
                                    .setPartitionIndex(2)
                                    .setPreferredSuccessors(intArrayOf(0, 1, 2))
                            )
                        )
                        .setTopicName("topic1")
                )
            )
        return EndQuorumEpochRequest.Builder(data).build(version)
    }

    private fun createEndQuorumEpochResponse(): EndQuorumEpochResponse {
        val data = EndQuorumEpochResponseData()
            .setErrorCode(Errors.NONE.code)
            .setTopics(
                listOf(
                    EndQuorumEpochResponseData.TopicData()
                        .setPartitions(
                            listOf(
                                EndQuorumEpochResponseData.PartitionData()
                                    .setErrorCode(Errors.NONE.code)
                                    .setLeaderEpoch(1)
                            )
                        )
                        .setTopicName("topic1")
                )
            )
        return EndQuorumEpochResponse(data)
    }

    private fun createBeginQuorumEpochRequest(version: Short): BeginQuorumEpochRequest {
        val data = BeginQuorumEpochRequestData()
            .setClusterId("clusterId")
            .setTopics(
                listOf(
                    BeginQuorumEpochRequestData.TopicData()
                        .setPartitions(
                            listOf(
                                BeginQuorumEpochRequestData.PartitionData()
                                    .setLeaderEpoch(0)
                                    .setLeaderId(1)
                                    .setPartitionIndex(2)
                            )
                        )
                )
            )
        return BeginQuorumEpochRequest.Builder(data).build(version)
    }

    private fun createBeginQuorumEpochResponse(): BeginQuorumEpochResponse {
        val data = BeginQuorumEpochResponseData()
            .setErrorCode(Errors.NONE.code)
            .setTopics(
                listOf(
                    BeginQuorumEpochResponseData.TopicData()
                        .setPartitions(
                            listOf(
                                BeginQuorumEpochResponseData.PartitionData()
                                    .setErrorCode(Errors.NONE.code)
                                    .setLeaderEpoch(0)
                                    .setLeaderId(1)
                                    .setPartitionIndex(2)
                            )
                        )
                )
            )
        return BeginQuorumEpochResponse(data)
    }

    private fun createVoteRequest(version: Short): VoteRequest {
        val data = VoteRequestData()
            .setClusterId("clusterId")
            .setTopics(
                listOf(
                    VoteRequestData.TopicData()
                        .setPartitions(
                            listOf(
                                VoteRequestData.PartitionData()
                                    .setPartitionIndex(0)
                                    .setCandidateEpoch(1)
                                    .setCandidateId(2)
                                    .setLastOffset(3L)
                                    .setLastOffsetEpoch(4)
                            )
                        )
                        .setTopicName("topic1")
                )
            )
        return VoteRequest.Builder(data).build(version)
    }

    private fun createVoteResponse(): VoteResponse {
        val data = VoteResponseData()
            .setErrorCode(Errors.NONE.code)
            .setTopics(
                listOf(
                    VoteResponseData.TopicData()
                        .setPartitions(
                            listOf(
                                VoteResponseData.PartitionData()
                                    .setErrorCode(Errors.NONE.code)
                                    .setLeaderEpoch(0)
                                    .setPartitionIndex(1)
                                    .setLeaderId(2)
                                    .setVoteGranted(false)
                            )
                        )
                )
            )
        return VoteResponse(data)
    }

    private fun createAlterUserScramCredentialsRequest(version: Short): AlterUserScramCredentialsRequest {
        val data = AlterUserScramCredentialsRequestData()
            .setDeletions(
                listOf(
                    ScramCredentialDeletion()
                        .setName("user1")
                        .setMechanism(0)
                )
            )
            .setUpsertions(
                listOf(
                    ScramCredentialUpsertion()
                        .setName("user2")
                        .setIterations(1024)
                        .setMechanism(1)
                        .setSalt("salt".toByteArray())
                )
            )
        return AlterUserScramCredentialsRequest.Builder(data).build(version)
    }

    private fun createAlterUserScramCredentialsResponse(): AlterUserScramCredentialsResponse {
        val data = AlterUserScramCredentialsResponseData()
            .setResults(
                listOf(
                    AlterUserScramCredentialsResponseData.AlterUserScramCredentialsResult()
                        .setErrorCode(Errors.NONE.code)
                        .setUser("user1")
                        .setErrorMessage("error message")
                )
            )
        return AlterUserScramCredentialsResponse(data)
    }

    private fun createDescribeUserScramCredentialsRequest(version: Short): DescribeUserScramCredentialsRequest {
        val data = DescribeUserScramCredentialsRequestData()
            .setUsers(
                listOf(
                    UserName()
                        .setName("user1")
                )
            )
        return DescribeUserScramCredentialsRequest.Builder(data).build(version)
    }

    private fun createDescribeUserScramCredentialsResponse(): DescribeUserScramCredentialsResponse {
        val data = DescribeUserScramCredentialsResponseData()
            .setResults(
                listOf(
                    DescribeUserScramCredentialsResponseData.DescribeUserScramCredentialsResult()
                        .setUser("user1")
                        .setErrorCode(Errors.NONE.code)
                        .setErrorMessage("error message")
                        .setCredentialInfos(
                            listOf(
                                CredentialInfo()
                                    .setIterations(1024)
                                    .setMechanism(0)
                            )
                        )
                )
            )
            .setErrorCode(Errors.NONE.code)
            .setErrorMessage("error message")
            .setThrottleTimeMs(123)
        return DescribeUserScramCredentialsResponse(data)
    }

    private fun createAlterPartitionRequest(version: Short): AlterPartitionRequest {
        val partitionData = AlterPartitionRequestData.PartitionData()
            .setPartitionIndex(1)
            .setPartitionEpoch(2)
            .setLeaderEpoch(3)
            .setNewIsr(intArrayOf(1, 2))
        if (version >= 1) {
            // Use the none default value; 1 - RECOVERING
            partitionData.setLeaderRecoveryState(1)
        }
        val data = AlterPartitionRequestData()
            .setBrokerEpoch(123L)
            .setBrokerId(1)
            .setTopics(
                listOf(
                    AlterPartitionRequestData.TopicData()
                        .setTopicName("topic1")
                        .setTopicId(Uuid.randomUuid())
                        .setPartitions(listOf(partitionData))
                )
            )
        return AlterPartitionRequest.Builder(data, version >= 1).build(version)
    }

    private fun createAlterPartitionResponse(version: Int): AlterPartitionResponse {
        val partitionData = AlterPartitionResponseData.PartitionData()
            .setPartitionEpoch(1)
            .setIsr(intArrayOf(0, 1, 2))
            .setErrorCode(Errors.NONE.code)
            .setLeaderEpoch(2)
            .setLeaderId(3)
        if (version >= 1) {
            // Use the none default value; 1 - RECOVERING
            partitionData.setLeaderRecoveryState(1)
        }
        val data = AlterPartitionResponseData()
            .setErrorCode(Errors.NONE.code)
            .setThrottleTimeMs(123)
            .setTopics(
                listOf(
                    AlterPartitionResponseData.TopicData()
                        .setTopicName("topic1")
                        .setTopicId(Uuid.randomUuid())
                        .setPartitions(listOf(partitionData))
                )
            )
        return AlterPartitionResponse(data)
    }

    private fun createUpdateFeaturesRequest(version: Short): UpdateFeaturesRequest {
        val features = FeatureUpdateKeyCollection()
        features.add(
            FeatureUpdateKey()
                .setFeature("feature1")
                .setAllowDowngrade(false)
                .setMaxVersionLevel(1.toShort())
        )
        val data = UpdateFeaturesRequestData()
            .setFeatureUpdates(features)
            .setTimeoutMs(123)
        return UpdateFeaturesRequest.Builder(data).build(version)
    }

    private fun createUpdateFeaturesResponse(): UpdateFeaturesResponse {
        val results = UpdatableFeatureResultCollection()
        results.add(
            UpdatableFeatureResult()
                .setFeature("feature1")
                .setErrorCode(Errors.NONE.code)
                .setErrorMessage("error message")
        )
        val data = UpdateFeaturesResponseData()
            .setErrorCode(Errors.NONE.code)
            .setThrottleTimeMs(123)
            .setResults(results)
            .setErrorMessage("error message")
        return UpdateFeaturesResponse(data)
    }

    private fun createAllocateProducerIdsRequest(version: Short): AllocateProducerIdsRequest {
        val data = AllocateProducerIdsRequestData()
            .setBrokerEpoch(123L)
            .setBrokerId(2)
        return AllocateProducerIdsRequest.Builder(data).build(version)
    }

    private fun createAllocateProducerIdsResponse(): AllocateProducerIdsResponse {
        val data = AllocateProducerIdsResponseData()
            .setErrorCode(Errors.NONE.code)
            .setThrottleTimeMs(123)
            .setProducerIdLen(234)
            .setProducerIdStart(345L)
        return AllocateProducerIdsResponse(data)
    }

    private fun createDescribeLogDirsRequest(version: Short): DescribeLogDirsRequest {
        val topics = DescribableLogDirTopicCollection()
        topics.add(
            DescribableLogDirTopic()
                .setPartitions(intArrayOf(0, 1, 2))
                .setTopic("topic1")
        )
        val data = DescribeLogDirsRequestData()
            .setTopics(topics)
        return DescribeLogDirsRequest.Builder(data).build(version)
    }

    private fun createDescribeLogDirsResponse(): DescribeLogDirsResponse {
        val data = DescribeLogDirsResponseData()
            .setResults(
                listOf(
                    DescribeLogDirsResponseData.DescribeLogDirsResult()
                        .setErrorCode(Errors.NONE.code)
                        .setLogDir("logdir")
                        .setTopics(
                            listOf(
                                DescribeLogDirsTopic()
                                    .setName("topic1")
                                    .setPartitions(
                                        listOf(
                                            DescribeLogDirsPartition()
                                                .setPartitionIndex(0)
                                                .setIsFutureKey(false)
                                                .setOffsetLag(123L)
                                                .setPartitionSize(234L)
                                        )
                                    )
                            )
                        )
                )
            )
            .setThrottleTimeMs(123)
        return DescribeLogDirsResponse(data)
    }

    private fun createDeleteRecordsRequest(version: Short): DeleteRecordsRequest {
        val topic = DeleteRecordsTopic()
            .setName("topic1")
            .setPartitions(
                listOf(
                    DeleteRecordsPartition()
                        .setPartitionIndex(1)
                        .setOffset(123L)
                )
            )
        val data = DeleteRecordsRequestData()
            .setTopics(listOf(topic))
            .setTimeoutMs(123)
        return DeleteRecordsRequest.Builder(data).build(version)
    }

    private fun createDeleteRecordsResponse(): DeleteRecordsResponse {
        val topics = DeleteRecordsTopicResultCollection()
        val partitions = DeleteRecordsPartitionResultCollection()
        partitions.add(
            DeleteRecordsPartitionResult()
                .setErrorCode(Errors.NONE.code)
                .setLowWatermark(123L)
                .setPartitionIndex(0)
        )
        topics.add(
            DeleteRecordsTopicResult()
                .setName("topic1")
                .setPartitions(partitions)
        )
        val data = DeleteRecordsResponseData()
            .setThrottleTimeMs(123)
            .setTopics(topics)
        return DeleteRecordsResponse(data)
    }

    private fun createDescribeClusterRequest(version: Short): DescribeClusterRequest {
        return DescribeClusterRequest.Builder(
            DescribeClusterRequestData()
                .setIncludeClusterAuthorizedOperations(true)
        ).build(version)
    }

    private fun createDescribeClusterResponse(): DescribeClusterResponse {
        return DescribeClusterResponse(
            DescribeClusterResponseData()
                .setBrokers(
                    DescribeClusterBrokerCollection(
                        listOf(
                            DescribeClusterBroker()
                                .setBrokerId(1)
                                .setHost("localhost")
                                .setPort(9092)
                                .setRack("rack1")
                        ).iterator()
                    )
                )
                .setClusterId("clusterId")
                .setControllerId(1)
                .setClusterAuthorizedOperations(10)
        )
    }

    private fun checkOlderFetchVersions() {
        for (version: Short in ApiKeys.FETCH.allVersions()) {
            if (version > 7) {
                checkErrorResponse(createFetchRequest(version), unknownServerException)
            }
            checkRequest(createFetchRequest(version))
            checkResponse(createFetchResponse(version >= 4), version)
        }
    }

    private fun verifyDescribeConfigsResponse(
        expected: DescribeConfigsResponse, actual: DescribeConfigsResponse,
        version: Short,
    ) {
        for (resource in expected.resultMap()) {
            val actualEntries = actual.resultMap()[resource.key]!!.configs
            val expectedEntries = expected.resultMap()[resource.key]!!.configs
            assertEquals(expectedEntries.size, actualEntries.size)
            for (i in actualEntries.indices) {
                val actualEntry = actualEntries[i]
                val expectedEntry = expectedEntries[i]
                assertEquals(expectedEntry.name, actualEntry.name)
                assertEquals(
                    expected = expectedEntry.value,
                    actual = actualEntry.value,
                    message = "Non-matching values for ${actualEntry.name} in version $version",
                )
                assertEquals(
                    expected = expectedEntry.readOnly,
                    actual = actualEntry.readOnly,
                    message = "Non-matching readonly for ${actualEntry.name} in version $version",
                )
                assertEquals(
                    expected = expectedEntry.isSensitive,
                    actual = actualEntry.isSensitive,
                    message = "Non-matching isSensitive for ${actualEntry.name} in version $version",
                )
                if (version < 3) assertEquals(
                    expected = DescribeConfigsResponse.ConfigType.UNKNOWN.id,
                    actual = actualEntry.configType,
                    message = "Non-matching configType for ${actualEntry.name} in version $version",
                )
                else assertEquals(
                    expectedEntry.configType,
                    actualEntry.configType,
                    "Non-matching configType for ${actualEntry.name} in version $version",
                )
                if (version.toInt() == 0) assertEquals(
                    ConfigSource.STATIC_BROKER_CONFIG.id,
                    actualEntry.configSource,
                    "Non matching configSource for ${actualEntry.name} in version $version",
                )
                else assertEquals(
                    expected = expectedEntry.configSource,
                    actual = actualEntry.configSource,
                    message = "Non-matching configSource for ${actualEntry.name} in version $version",
                )
            }
        }
    }

    private fun checkDescribeConfigsResponseVersions() {
        for (version in ApiKeys.DESCRIBE_CONFIGS.allVersions()) {
            val response = createDescribeConfigsResponse(version)
            val deserialized = AbstractResponse.parseResponse(
                apiKey = ApiKeys.DESCRIBE_CONFIGS,
                responseBuffer = response.serialize(version),
                version = version,
            ) as DescribeConfigsResponse
            verifyDescribeConfigsResponse(
                expected = response,
                actual = deserialized,
                version = version,
            )
        }
    }

    private fun checkErrorResponse(req: AbstractRequest, e: Throwable) {
        val response = req.getErrorResponse(e)
        checkResponse(response, req.version)
        val error = Errors.forException(e)
        val errorCounts = response!!.errorCounts()
        assertEquals(
            expected = setOf(error),
            actual = errorCounts.keys,
            message = "API Key ${req.apiKey.name} v${req.version} failed errorCounts test"
        )
        assertTrue(errorCounts[error]!! > 0)
        if (e is UnknownServerException) {
            val responseStr = response.toString()
            assertFalse(
                actual = responseStr.contains((e.message)!!),
                message = "Unknown message included in response for ${req.apiKey}: $responseStr ",
            )
        }
    }

    private fun checkRequest(req: AbstractRequest) {
        // Check that we can serialize, deserialize and serialize again
        // Check for equality of the ByteBuffer only if indicated (it is likely to fail if any of the fields
        // in the request is a HashMap with multiple elements since ordering of the elements may vary)
        try {
            val serializedBytes = req.serialize()
            val deserialized = AbstractRequest.parseRequest(req.apiKey, req.version, serializedBytes).request
            val serializedBytes2 = deserialized.serialize()
            serializedBytes.rewind()
            assertEquals(serializedBytes, serializedBytes2, "Request ${req}failed equality test")
        } catch (e: Exception) {
            throw RuntimeException("Failed to deserialize request $req with type ${req.javaClass}", e)
        }
    }

    private fun checkResponse(response: AbstractResponse?, version: Short) {
        // Check that we can serialize, deserialize and serialize again
        // Check for equality and hashCode of the Struct only if indicated (it is likely to fail if any of the fields
        // in the response is a HashMap with multiple elements since ordering of the elements may vary)
        try {
            val serializedBytes = response!!.serialize(version)
            val deserialized = AbstractResponse.parseResponse(response.apiKey, serializedBytes, version)
            val serializedBytes2 = deserialized.serialize(version)
            serializedBytes.rewind()
            assertEquals(serializedBytes, serializedBytes2, "Response " + response + "failed equality test")
        } catch (e: Exception) {
            throw RuntimeException("Failed to deserialize response $response with type ${response!!.javaClass}", e)
        }
    }

    private fun createFindCoordinatorRequest(version: Short): FindCoordinatorRequest {
        return FindCoordinatorRequest.Builder(
            FindCoordinatorRequestData()
                .setKeyType(CoordinatorType.GROUP.id)
                .setKey("test-group")
        ).build(version)
    }

    private fun createBatchedFindCoordinatorRequest(
        coordinatorKeys: List<String>,
        version: Short,
    ): FindCoordinatorRequest {
        return FindCoordinatorRequest.Builder(
            FindCoordinatorRequestData()
                .setKeyType(CoordinatorType.GROUP.id)
                .setCoordinatorKeys(coordinatorKeys)
        ).build(version)
    }

    private fun createFindCoordinatorResponse(version: Short): FindCoordinatorResponse {
        val node = Node(10, "host1", 2014)
        return if (version < FindCoordinatorRequest.MIN_BATCHED_VERSION)
            FindCoordinatorResponse.prepareOldResponse(Errors.NONE, node)
        else FindCoordinatorResponse.prepareResponse(Errors.NONE, "group", node)
    }

    private fun createFetchRequest(
        version: Short,
        metadata: FetchMetadata,
        toForget: List<TopicIdPartition>,
    ): FetchRequest {
        val fetchData = mapOf(
            TopicPartition("test1", 0) to FetchRequest.PartitionData(
                topicId = Uuid.randomUuid(),
                fetchOffset = 100,
                logStartOffset = -1L,
                maxBytes = 1000000,
                currentLeaderEpoch = null,
            ),
            TopicPartition("test2", 0) to FetchRequest.PartitionData(
                topicId = Uuid.randomUuid(),
                fetchOffset = 200,
                logStartOffset = -1L,
                maxBytes = 1000000,
                currentLeaderEpoch = null,
            ),
        )
        return FetchRequest.Builder.forConsumer(
            maxVersion = version,
            maxWait = 100,
            minBytes = 100000,
            fetchData = fetchData,
        ).metadata(metadata)
            .setMaxBytes(1000)
            .removed(toForget)
            .build(version)
    }

    private fun createFetchRequest(version: Short, isolationLevel: IsolationLevel): FetchRequest {
        val fetchData = mapOf(
            TopicPartition("test1", 0) to FetchRequest.PartitionData(
                topicId = Uuid.randomUuid(),
                fetchOffset = 100,
                logStartOffset = -1L,
                maxBytes = 1000000,
                currentLeaderEpoch = null,
            ),
            TopicPartition("test2", 0) to FetchRequest.PartitionData(
                topicId = Uuid.randomUuid(),
                fetchOffset = 200,
                logStartOffset = -1L,
                maxBytes = 1000000,
                currentLeaderEpoch = null,
            ),
        )
        return FetchRequest.Builder.forConsumer(
            maxVersion = version,
            maxWait = 100,
            minBytes = 100000,
            fetchData = fetchData,
        ).isolationLevel(isolationLevel)
            .setMaxBytes(1000)
            .build(version)
    }

    private fun createFetchRequest(version: Short): FetchRequest {
        val fetchData = mapOf(
            TopicPartition("test1", 0) to FetchRequest.PartitionData(
                topicId = Uuid.randomUuid(),
                fetchOffset = 100,
                logStartOffset = -1L,
                maxBytes = 1000000,
                currentLeaderEpoch = null,
            ),
            TopicPartition("test2", 0) to FetchRequest.PartitionData(
                topicId = Uuid.randomUuid(),
                fetchOffset = 200,
                logStartOffset = -1L,
                maxBytes = 1000000,
                currentLeaderEpoch = null,
            ),
        )
        return FetchRequest.Builder.forConsumer(
            maxVersion = version,
            maxWait = 100,
            minBytes = 100000,
            fetchData = fetchData,
        ).setMaxBytes(1000)
            .build(version)
    }

    private fun createFetchResponse(error: Errors, sessionId: Int): FetchResponse {
        return FetchResponse.parse(
            buffer = FetchResponse.of(
                error = error,
                throttleTimeMs = 25,
                sessionId = sessionId,
                responseData = emptyMap(),
            ).serialize(ApiKeys.FETCH.latestVersion()),
            version = ApiKeys.FETCH.latestVersion(),
        )
    }

    private fun createFetchResponse(sessionId: Int): FetchResponse {
        val topicIds: MutableMap<String, Uuid> = HashMap()
        topicIds["test"] = Uuid.randomUuid()
        val records = MemoryRecords.withRecords(
            compressionType = CompressionType.NONE,
            records = arrayOf(SimpleRecord(value = "blah".toByteArray())),
        )
        val abortedTransactions = listOf(
            AbortedTransaction().setProducerId(234L).setFirstOffset(999L)
        )
        val responseData = mapOf(
            TopicIdPartition(
                topicId = topicIds["test"]!!,
                topicPartition = TopicPartition("test", 0),
            ) to FetchResponseData.PartitionData()
                .setPartitionIndex(0)
                .setHighWatermark(1000000)
                .setLogStartOffset(0)
                .setRecords(records),
            TopicIdPartition(
                topicId = topicIds["test"]!!,
                topicPartition = TopicPartition("test", 1)
            ) to FetchResponseData.PartitionData()
                .setPartitionIndex(1)
                .setHighWatermark(1000000)
                .setLogStartOffset(0)
                .setAbortedTransactions(abortedTransactions),
        )
        return FetchResponse.parse(
            buffer = FetchResponse.of(
                error = Errors.NONE,
                throttleTimeMs = 25,
                sessionId = sessionId,
                responseData = responseData,
            ).serialize(version = ApiKeys.FETCH.latestVersion()),
            version = ApiKeys.FETCH.latestVersion(),
        )
    }

    private fun createFetchResponse(includeAborted: Boolean): FetchResponse {
        val topicId = Uuid.randomUuid()
        val records = MemoryRecords.withRecords(
            compressionType = CompressionType.NONE,
            records = arrayOf(SimpleRecord(value = "blah".toByteArray())),
        )
        var abortedTransactions = emptyList<AbortedTransaction>()
        if (includeAborted) {
            abortedTransactions = listOf(
                AbortedTransaction().setProducerId(234L).setFirstOffset(999L)
            )
        }
        val responseData = mapOf(
            TopicIdPartition(topicId, TopicPartition("test", 0)) to FetchResponseData.PartitionData()
                .setPartitionIndex(0)
                .setHighWatermark(1000000)
                .setLogStartOffset(0)
                .setRecords(records),
            TopicIdPartition(topicId, TopicPartition("test", 1)) to FetchResponseData.PartitionData()
                .setPartitionIndex(1)
                .setHighWatermark(1000000)
                .setLogStartOffset(0)
                .setAbortedTransactions(abortedTransactions),
        )

        return FetchResponse.parse(
            buffer = FetchResponse.of(
                error = Errors.NONE,
                throttleTimeMs = 25,
                sessionId = FetchMetadata.INVALID_SESSION_ID,
                responseData = responseData
            ).serialize(version = ApiKeys.FETCH.latestVersion()),
            version = ApiKeys.FETCH.latestVersion(),
        )
    }

    private fun createFetchResponse(version: Short): FetchResponse {
        val data = FetchResponseData()
        if (version > 0) data.setThrottleTimeMs(345)
        if (version > 6) data.setErrorCode(Errors.NONE.code).setSessionId(123)
        val records = MemoryRecords.withRecords(
            compressionType = CompressionType.NONE,
            records = arrayOf(SimpleRecord(value = "blah".toByteArray())),
        )
        val partition = FetchResponseData.PartitionData()
            .setPartitionIndex(0)
            .setErrorCode(Errors.NONE.code)
            .setHighWatermark(123L)
            .setRecords(records)
        if (version > 3) partition.setLastStableOffset(234L)
        if (version > 4) partition.setLogStartOffset(456L)
        if (version > 10) partition.setPreferredReadReplica(1)
        if (version > 11) partition.setDivergingEpoch(FetchResponseData.EpochEndOffset().setEndOffset(1L).setEpoch(2))
            .setSnapshotId(FetchResponseData.SnapshotId().setEndOffset(1L).setEndOffset(2))
            .setCurrentLeader(FetchResponseData.LeaderIdAndEpoch().setLeaderEpoch(1).setLeaderId(2))
        val response = FetchableTopicResponse()
            .setTopic("topic")
            .setPartitions(listOf(partition))
        if (version > 12) response.setTopicId(Uuid.randomUuid())
        data.setResponses(listOf(response))

        return FetchResponse(data)
    }

    private fun createHeartBeatRequest(version: Short): HeartbeatRequest {
        return HeartbeatRequest.Builder(
            HeartbeatRequestData()
                .setGroupId("group1")
                .setGenerationId(1)
                .setMemberId("consumer1")
        ).build(version)
    }

    private fun createHeartBeatResponse(): HeartbeatResponse {
        return HeartbeatResponse(HeartbeatResponseData().setErrorCode(Errors.NONE.code))
    }

    private fun createJoinGroupRequest(version: Short): JoinGroupRequest {
        val protocols = JoinGroupRequestProtocolCollection(
            setOf(
                JoinGroupRequestProtocol()
                    .setName("consumer-range")
                    .setMetadata(ByteArray(0))
            ).iterator()
        )
        val data = JoinGroupRequestData()
            .setGroupId("group1")
            .setSessionTimeoutMs(30000)
            .setMemberId("consumer1")
            .setProtocolType("consumer")
            .setProtocols(protocols)
            .setReason("reason: test")

        // v1 and above contains rebalance timeout
        if (version >= 1) data.setRebalanceTimeoutMs(60000)

        // v5 and above could set group instance id
        if (version >= 5) data.setGroupInstanceId("groupInstanceId")
        return JoinGroupRequest.Builder(data).build(version)
    }

    private fun createJoinGroupResponse(version: Short): JoinGroupResponse {
        val members: MutableList<JoinGroupResponseMember> = ArrayList()
        for (i in 0..1) {
            val member = JoinGroupResponseMember()
                .setMemberId("consumer$i")
                .setMetadata(ByteArray(0))
                .setGroupInstanceId("instance$i")
            members.add(member)
        }
        val data = JoinGroupResponseData()
            .setErrorCode(Errors.NONE.code)
            .setGenerationId(1)
            .setProtocolType("consumer") // Added in v7 but ignorable
            .setProtocolName("range")
            .setLeader("leader")
            .setMemberId("consumer1")
            .setMembers(members)

        // v1 and above could set throttle time
        if (version >= 1) data.setThrottleTimeMs(1000)
        return JoinGroupResponse(data, version)
    }

    private fun createSyncGroupRequest(version: Short): SyncGroupRequest {
        val assignments = listOf(
            SyncGroupRequestAssignment()
                .setMemberId("member")
                .setAssignment(ByteArray(0))
        )
        val data = SyncGroupRequestData()
            .setGroupId("group1")
            .setGenerationId(1)
            .setMemberId("member")
            .setProtocolType("consumer") // Added in v5 but ignorable
            .setProtocolName("range") // Added in v5 but ignorable
            .setAssignments(assignments)

        // v3 and above could set group instance id
        if (version >= 3) data.setGroupInstanceId("groupInstanceId")
        return SyncGroupRequest.Builder(data).build(version)
    }

    private fun createSyncGroupResponse(version: Short): SyncGroupResponse {
        val data = SyncGroupResponseData()
            .setErrorCode(Errors.NONE.code)
            .setProtocolType("consumer") // Added in v5 but ignorable
            .setProtocolName("range") // Added in v5 but ignorable
            .setAssignment(ByteArray(0))

        // v1 and above could set throttle time
        if (version >= 1) data.setThrottleTimeMs(1000)
        return SyncGroupResponse(data)
    }

    private fun createListGroupsRequest(version: Short): ListGroupsRequest {
        val data = ListGroupsRequestData()
        if (version >= 4) data.setStatesFilter(listOf("Stable"))
        return ListGroupsRequest.Builder(data).build(version)
    }

    private fun createListGroupsResponse(version: Short): ListGroupsResponse {
        val group = ListedGroup()
            .setGroupId("test-group")
            .setProtocolType("consumer")
        if (version >= 4) group.setGroupState("Stable")
        val data = ListGroupsResponseData()
            .setErrorCode(Errors.NONE.code)
            .setGroups(listOf(group))
        return ListGroupsResponse(data)
    }

    private fun createDescribeGroupRequest(version: Short): DescribeGroupsRequest {
        return DescribeGroupsRequest.Builder(
            DescribeGroupsRequestData().setGroups(listOf("test-group"))
        ).build(version)
    }

    private fun createDescribeGroupResponse(): DescribeGroupsResponse {
        val clientId = "consumer-1"
        val clientHost = "localhost"
        val describeGroupsResponseData = DescribeGroupsResponseData()
        val member = DescribeGroupsResponse.groupMember(
            memberId = "memberId",
            groupInstanceId = null,
            clientId = clientId,
            clientHost = clientHost,
            assignment = ByteArray(0),
            metadata = ByteArray(0),
        )
        val metadata = DescribeGroupsResponse.groupMetadata(
            groupId = "test-group",
            error = Errors.NONE,
            state = "STABLE",
            protocolType = "consumer",
            protocol = "roundrobin",
            members = listOf(member),
            authorizedOperations = DescribeGroupsResponse.AUTHORIZED_OPERATIONS_OMITTED,
        )
        describeGroupsResponseData.groups += metadata
        return DescribeGroupsResponse(describeGroupsResponseData)
    }

    private fun createLeaveGroupRequest(version: Short): LeaveGroupRequest {
        val member = MemberIdentity().setMemberId("consumer1").setReason("reason: test")
        return LeaveGroupRequest.Builder("group1", listOf(member)).build(version)
    }

    private fun createLeaveGroupResponse(): LeaveGroupResponse {
        return LeaveGroupResponse(LeaveGroupResponseData().setErrorCode(Errors.NONE.code))
    }

    private fun createDeleteGroupsRequest(version: Short): DeleteGroupsRequest {
        return DeleteGroupsRequest.Builder(
            DeleteGroupsRequestData().setGroupsNames(listOf("test-group"))
        ).build(version)
    }

    private fun createDeleteGroupsResponse(): DeleteGroupsResponse {
        val result = DeletableGroupResultCollection()
        result.add(
            DeletableGroupResult()
                .setGroupId("test-group")
                .setErrorCode(Errors.NONE.code)
        )
        return DeleteGroupsResponse(
            DeleteGroupsResponseData()
                .setResults(result)
        )
    }

    private fun createListOffsetRequest(version: Short): ListOffsetsRequest {
        if (version.toInt() == 0) {
            val topic = ListOffsetsTopic()
                .setName("test")
                .setPartitions(
                    listOf(
                        ListOffsetsPartition()
                            .setPartitionIndex(0)
                            .setTimestamp(1000000L)
                            .setMaxNumOffsets(10)
                            .setCurrentLeaderEpoch(5)
                    )
                )
            return ListOffsetsRequest.Builder
                .forConsumer(
                    requireTimestamp = false,
                    isolationLevel = IsolationLevel.READ_UNCOMMITTED,
                    requireMaxTimestamp = false,
                )
                .setTargetTimes(listOf(topic))
                .build(version)
        } else if (version.toInt() == 1) {
            val topic = ListOffsetsTopic()
                .setName("test")
                .setPartitions(
                    listOf(
                        ListOffsetsPartition()
                            .setPartitionIndex(0)
                            .setTimestamp(1000000L)
                            .setCurrentLeaderEpoch(5),
                    )
                )
            return ListOffsetsRequest.Builder
                .forConsumer(
                    requireTimestamp = true,
                    isolationLevel = IsolationLevel.READ_UNCOMMITTED,
                    requireMaxTimestamp = false,
                )
                .setTargetTimes(listOf(topic))
                .build(version)
        } else if (version >= 2 && version <= ApiKeys.LIST_OFFSETS.latestVersion()) {
            val partition = ListOffsetsPartition()
                .setPartitionIndex(0)
                .setTimestamp(1000000L)
                .setCurrentLeaderEpoch(5)
            val topic = ListOffsetsTopic()
                .setName("test")
                .setPartitions(listOf(partition))
            return ListOffsetsRequest.Builder
                .forConsumer(
                    requireTimestamp = true,
                    isolationLevel = IsolationLevel.READ_COMMITTED,
                    requireMaxTimestamp = false,
                )
                .setTargetTimes(listOf(topic))
                .build(version)
        } else throw IllegalArgumentException("Illegal ListOffsetRequest version $version")
    }

    private fun createListOffsetResponse(version: Short): ListOffsetsResponse {
        if (version.toInt() == 0) {
            val data = ListOffsetsResponseData()
                .setTopics(
                    listOf(
                        ListOffsetsTopicResponse()
                            .setName("test")
                            .setPartitions(
                                listOf(
                                    ListOffsetsPartitionResponse()
                                        .setPartitionIndex(0)
                                        .setErrorCode(Errors.NONE.code)
                                        .setOldStyleOffsets(longArrayOf(100L))
                                )
                            )
                    )
                )
            return ListOffsetsResponse(data)
        } else if (version >= 1 && version <= ApiKeys.LIST_OFFSETS.latestVersion()) {
            val partition = ListOffsetsPartitionResponse()
                .setPartitionIndex(0)
                .setErrorCode(Errors.NONE.code)
                .setTimestamp(10000L)
                .setOffset(100L)
            if (version >= 4) {
                partition.setLeaderEpoch(27)
            }
            val data = ListOffsetsResponseData()
                .setTopics(
                    listOf(
                        ListOffsetsTopicResponse()
                            .setName("test")
                            .setPartitions(listOf(partition))
                    )
                )
            return ListOffsetsResponse(data)
        } else throw IllegalArgumentException("Illegal ListOffsetResponse version $version")
    }

    private fun createMetadataRequest(version: Short, topics: List<String>): MetadataRequest {
        return MetadataRequest.Builder(topics = topics, allowAutoTopicCreation = true).build(version)
    }

    private fun createMetadataResponse(): MetadataResponse {
        val node = Node(1, "host1", 1001)
        val replicas = listOf(node.id)
        val isr = listOf(node.id)
        val offlineReplicas = emptyList<Int>()
        val allTopicMetadata = listOf(
            MetadataResponse.TopicMetadata(
                error = Errors.NONE,
                topic = "__consumer_offsets",
                isInternal = true,
                partitionMetadata = listOf(
                    PartitionMetadata(
                        error = Errors.NONE,
                        topicPartition = TopicPartition("__consumer_offsets", 1),
                        leaderId = node.id,
                        leaderEpoch = 5,
                        replicaIds = replicas,
                        inSyncReplicaIds = isr,
                        offlineReplicaIds = offlineReplicas,
                    ),
                )
            ),
            MetadataResponse.TopicMetadata(
                error = Errors.LEADER_NOT_AVAILABLE,
                topic = "topic2",
                isInternal = false,
                partitionMetadata = emptyList(),
            ),
            MetadataResponse.TopicMetadata(
                error = Errors.NONE,
                topic = "topic3",
                isInternal = false,
                partitionMetadata = listOf(
                    PartitionMetadata(
                        error = Errors.LEADER_NOT_AVAILABLE,
                        topicPartition = TopicPartition("topic3", 0),
                        leaderId = null,
                        leaderEpoch = null,
                        replicaIds = replicas,
                        inSyncReplicaIds = isr,
                        offlineReplicaIds = offlineReplicas,
                    ),
                )
            ),
        )
        return metadataResponse(
            brokers = listOf(node),
            clusterId = null,
            controllerId = MetadataResponse.NO_CONTROLLER_ID,
            topicMetadataList = allTopicMetadata,
        )
    }

    private fun createOffsetCommitRequest(version: Short): OffsetCommitRequest {
        return OffsetCommitRequest.Builder(
            OffsetCommitRequestData()
                .setGroupId("group1")
                .setMemberId("consumer1")
                .setGroupInstanceId(null)
                .setGenerationId(100)
                .setTopics(
                    listOf(
                        OffsetCommitRequestTopic()
                            .setName("test")
                            .setPartitions(
                                listOf(
                                    OffsetCommitRequestPartition()
                                        .setPartitionIndex(0)
                                        .setCommittedOffset(100)
                                        .setCommittedLeaderEpoch(RecordBatch.NO_PARTITION_LEADER_EPOCH)
                                        .setCommittedMetadata(""),
                                    OffsetCommitRequestPartition()
                                        .setPartitionIndex(1)
                                        .setCommittedOffset(200)
                                        .setCommittedLeaderEpoch(RecordBatch.NO_PARTITION_LEADER_EPOCH)
                                        .setCommittedMetadata(null),
                                )
                            )
                    )
                )
        ).build(version)
    }

    private fun createOffsetCommitResponse(): OffsetCommitResponse {
        return OffsetCommitResponse(
            OffsetCommitResponseData()
                .setTopics(
                    listOf(
                        OffsetCommitResponseTopic()
                            .setName("test")
                            .setPartitions(
                                listOf(
                                    OffsetCommitResponsePartition()
                                        .setPartitionIndex(0)
                                        .setErrorCode(Errors.NONE.code),
                                )
                            )
                    )
                )
        )
    }

    private fun createOffsetFetchRequest(version: Short, requireStable: Boolean): OffsetFetchRequest {
        return if (version < 8) {
            OffsetFetchRequest.Builder(
                groupId = "group1",
                requireStable = requireStable,
                partitions = listOf(TopicPartition("test11", 1)),
                throwOnFetchStableOffsetsUnsupported = false,
            ).build(version)
        } else OffsetFetchRequest.Builder(
            groupIdToTopicPartitionMap = mapOf("group1" to listOf(TopicPartition("test11", 1))),
            requireStable = requireStable,
            throwOnFetchStableOffsetsUnsupported = false,
        ).build(version)
    }

    private fun createOffsetFetchRequestWithMultipleGroups(version: Short, requireStable: Boolean): OffsetFetchRequest {
        val topic1 = listOf(TopicPartition("topic1", 0))
        val topic2 = listOf(
            TopicPartition("topic1", 0),
            TopicPartition("topic2", 0),
            TopicPartition("topic2", 1),
        )
        val topic3 = listOf(
            TopicPartition("topic1", 0),
            TopicPartition("topic2", 0),
            TopicPartition("topic2", 1),
            TopicPartition("topic3", 0),
            TopicPartition("topic3", 1),
            TopicPartition("topic3", 2),
        )
        val groupToPartitionMap = mapOf(
            "group1" to topic1,
            "group2" to topic2,
            "group3" to topic3,
            "group4" to null,
            "group5" to null,
        )
        return OffsetFetchRequest.Builder(
            groupIdToTopicPartitionMap = groupToPartitionMap,
            requireStable = requireStable,
            throwOnFetchStableOffsetsUnsupported = false,
        ).build(version)
    }

    private fun createOffsetFetchRequestForAllPartition(version: Short, requireStable: Boolean): OffsetFetchRequest {
        return if (version < 8) {
            OffsetFetchRequest.Builder(
                groupId = "group1",
                requireStable = requireStable,
                partitions = null,
                throwOnFetchStableOffsetsUnsupported = false,
            ).build(version)
        } else OffsetFetchRequest.Builder(
            groupIdToTopicPartitionMap = mapOf("group1" to null),
            requireStable = requireStable,
            throwOnFetchStableOffsetsUnsupported = false,
        ).build(version)
    }

    private fun createOffsetFetchResponse(version: Short): OffsetFetchResponse {
        val responseData = mapOf(
            TopicPartition("test", 0) to OffsetFetchResponse.PartitionData(
                offset = 100L,
                leaderEpoch = null,
                metadata = "",
                error = Errors.NONE,
            ),
            TopicPartition("test", 1) to OffsetFetchResponse.PartitionData(
                offset = 100L,
                leaderEpoch = 10,
                metadata = null,
                error = Errors.NONE,
            )
        )

        if (version < 8) return OffsetFetchResponse(Errors.NONE, responseData)

        val throttleMs = 10
        return OffsetFetchResponse(
            throttleTimeMs = throttleMs,
            errors = mapOf("group1" to Errors.NONE),
            responseData = mapOf("group1" to responseData),
        )
    }

    private fun createProduceRequest(version: Short): ProduceRequest {
        if (version < 2) {
            val records = MemoryRecords.withRecords(
                compressionType = CompressionType.NONE,
                records = arrayOf(SimpleRecord(value = "blah".toByteArray())),
            )
            val data = ProduceRequestData()
                .setAcks(-1)
                .setTimeoutMs(123)
                .setTopicData(
                    TopicProduceDataCollection(
                        listOf(
                            TopicProduceData()
                                .setName("topic1")
                                .setPartitionData(
                                    listOf(
                                        PartitionProduceData()
                                            .setIndex(1)
                                            .setRecords(records)
                                    )
                                )
                        ).iterator()
                    )
                )
            return ProduceRequest.Builder(version, version, data).build(version)
        }
        val magic = if (version.toInt() == 2) RecordBatch.MAGIC_VALUE_V1 else RecordBatch.MAGIC_VALUE_V2
        val records = MemoryRecords.withRecords(
            magic = magic,
            compressionType = CompressionType.NONE,
            records = arrayOf(SimpleRecord(value = "woot".toByteArray())),
        )
        return ProduceRequest.forMagic(
            magic = magic,
            data = ProduceRequestData()
                .setTopicData(
                    TopicProduceDataCollection(
                        listOf(
                            TopicProduceData()
                                .setName("test")
                                .setPartitionData(
                                    listOf(
                                        PartitionProduceData()
                                            .setIndex(0)
                                            .setRecords(records)
                                    )
                                )
                        ).iterator()
                    )
                )
                .setAcks(1.toShort())
                .setTimeoutMs(5000)
                .setTransactionalId(if (version >= 3) "transactionalId" else null)
        ).build(version)
    }

    @Suppress("Deprecation")
    private fun createProduceResponse(): ProduceResponse {
        val responseData = mapOf(
            TopicPartition("test", 0) to ProduceResponse.PartitionResponse(
                error = Errors.NONE,
                baseOffset = 10000,
                logAppendTime = RecordBatch.NO_TIMESTAMP,
                logStartOffset = 100,
            ),
        )
        return ProduceResponse(responses = responseData, throttleTimeMs = 0)
    }

    @Suppress("Deprecation")
    private fun createProduceResponseWithErrorMessage(): ProduceResponse {
        val responseData = mapOf(
            TopicPartition("test", 0) to ProduceResponse.PartitionResponse(
                error = Errors.NONE,
                baseOffset = 10000,
                logAppendTime = RecordBatch.NO_TIMESTAMP,
                logStartOffset = 100,
                recordErrors = listOf(RecordError(0, "error message")),
                errorMessage = "global error message",
            ),
        )
        return ProduceResponse(responses = responseData, throttleTimeMs = 0)
    }

    private fun createStopReplicaRequest(version: Short, deletePartitions: Boolean): StopReplicaRequest {
        val topic1 = StopReplicaTopicState()
            .setTopicName("topic1")
            .setPartitionStates(
                listOf(
                    StopReplicaPartitionState()
                        .setPartitionIndex(0)
                        .setLeaderEpoch(1)
                        .setDeletePartition(deletePartitions)
                )
            )
        val topic2 = StopReplicaTopicState()
            .setTopicName("topic2")
            .setPartitionStates(
                listOf(
                    StopReplicaPartitionState()
                        .setPartitionIndex(1)
                        .setLeaderEpoch(2)
                        .setDeletePartition(deletePartitions)
                )
            )
        val topicStates = listOf(topic1, topic2)
        return StopReplicaRequest.Builder(
            version = version,
            controllerId = 0,
            controllerEpoch = 1,
            brokerEpoch = 0,
            deletePartitions = deletePartitions,
            topicStates = topicStates,
        ).build(version)
    }

    private fun createStopReplicaResponse(): StopReplicaResponse {
        val partitions = listOf(
            StopReplicaPartitionError()
                .setTopicName("test")
                .setPartitionIndex(0)
                .setErrorCode(Errors.NONE.code),
        )
        return StopReplicaResponse(
            StopReplicaResponseData()
                .setErrorCode(Errors.NONE.code)
                .setPartitionErrors(partitions)
        )
    }

    private fun createControlledShutdownRequest(version: Short): ControlledShutdownRequest {
        val data = ControlledShutdownRequestData()
            .setBrokerId(10)
            .setBrokerEpoch(0L)
        return ControlledShutdownRequest.Builder(
            data = data,
            desiredVersion = ApiKeys.CONTROLLED_SHUTDOWN.latestVersion(),
        ).build(version)
    }

    private fun createControlledShutdownResponse(): ControlledShutdownResponse {
        val p1 = RemainingPartition()
            .setTopicName("test2")
            .setPartitionIndex(5)
        val p2 = RemainingPartition()
            .setTopicName("test1")
            .setPartitionIndex(10)
        val pSet = RemainingPartitionCollection()
        pSet.add(p1)
        pSet.add(p2)
        val data = ControlledShutdownResponseData()
            .setErrorCode(Errors.NONE.code)
            .setRemainingPartitions(pSet)
        return ControlledShutdownResponse(data)
    }

    private fun createLeaderAndIsrRequest(version: Short): LeaderAndIsrRequest {
        val partitionStates: MutableList<LeaderAndIsrPartitionState> = ArrayList()
        val isr = intArrayOf(1, 2)
        val replicas = intArrayOf(1, 2, 3, 4)
        partitionStates.add(
            LeaderAndIsrPartitionState()
                .setTopicName("topic5")
                .setPartitionIndex(105)
                .setControllerEpoch(0)
                .setLeader(2)
                .setLeaderEpoch(1)
                .setIsr(isr)
                .setPartitionEpoch(2)
                .setReplicas(replicas)
                .setIsNew(false)
        )
        partitionStates.add(
            LeaderAndIsrPartitionState()
                .setTopicName("topic5")
                .setPartitionIndex(1)
                .setControllerEpoch(1)
                .setLeader(1)
                .setLeaderEpoch(1)
                .setIsr(isr)
                .setPartitionEpoch(2)
                .setReplicas(replicas)
                .setIsNew(false)
        )
        partitionStates.add(
            LeaderAndIsrPartitionState()
                .setTopicName("topic20")
                .setPartitionIndex(1)
                .setControllerEpoch(1)
                .setLeader(0)
                .setLeaderEpoch(1)
                .setIsr(isr)
                .setPartitionEpoch(2)
                .setReplicas(replicas)
                .setIsNew(false)
        )
        val leaders = setOf(
            Node(id = 0, host = "test0", port = 1223),
            Node(id = 1, host = "test1", port = 1223),
        )
        val topicIds = mapOf(
            "topic5" to Uuid.randomUuid(),
            "topic20" to Uuid.randomUuid(),
        )
        return LeaderAndIsrRequest.Builder(
            version = version,
            controllerId = 1,
            controllerEpoch = 10,
            brokerEpoch = 0,
            partitionStates = partitionStates,
            topicIds = topicIds,
            liveLeaders = leaders,
        ).build()
    }

    private fun createLeaderAndIsrResponse(version: Short): LeaderAndIsrResponse {
        if (version < 5) {
            val partitions = listOf(
                LeaderAndIsrPartitionError()
                    .setTopicName("test")
                    .setPartitionIndex(0)
                    .setErrorCode(Errors.NONE.code)
            )
            return LeaderAndIsrResponse(
                data = LeaderAndIsrResponseData()
                    .setErrorCode(Errors.NONE.code)
                    .setPartitionErrors(partitions),
                version = version,
            )
        } else {
            val partition = listOf(
                LeaderAndIsrPartitionError()
                    .setPartitionIndex(0)
                    .setErrorCode(Errors.NONE.code)
            )
            val topics = LeaderAndIsrTopicErrorCollection()
            topics.add(
                LeaderAndIsrTopicError()
                    .setTopicId(Uuid.randomUuid())
                    .setPartitionErrors(partition)
            )
            return LeaderAndIsrResponse(
                LeaderAndIsrResponseData()
                    .setTopics(topics), version
            )
        }
    }

    private fun createUpdateMetadataRequest(version: Short, rack: String?): UpdateMetadataRequest {
        val partitionStates: MutableList<UpdateMetadataPartitionState> = ArrayList()
        val isr = intArrayOf(1, 2)
        val replicas = intArrayOf(1, 2, 3, 4)
        val offlineReplicas = intArrayOf()
        partitionStates.add(
            UpdateMetadataPartitionState()
                .setTopicName("topic5")
                .setPartitionIndex(105)
                .setControllerEpoch(0)
                .setLeader(2)
                .setLeaderEpoch(1)
                .setIsr(isr)
                .setZkVersion(2)
                .setReplicas(replicas)
                .setOfflineReplicas(offlineReplicas)
        )
        partitionStates.add(
            UpdateMetadataPartitionState()
                .setTopicName("topic5")
                .setPartitionIndex(1)
                .setControllerEpoch(1)
                .setLeader(1)
                .setLeaderEpoch(1)
                .setIsr(isr)
                .setZkVersion(2)
                .setReplicas(replicas)
                .setOfflineReplicas(offlineReplicas)
        )
        partitionStates.add(
            UpdateMetadataPartitionState()
                .setTopicName("topic20")
                .setPartitionIndex(1)
                .setControllerEpoch(1)
                .setLeader(0)
                .setLeaderEpoch(1)
                .setIsr(isr)
                .setZkVersion(2)
                .setReplicas(replicas)
                .setOfflineReplicas(offlineReplicas)
        )
        val topicIds = if (version > 6) mapOf(
            "topic5" to Uuid.randomUuid(),
            "topic20" to Uuid.randomUuid(),
        ) else emptyMap()
        val plaintext = SecurityProtocol.PLAINTEXT
        val endpoints1 = listOf(
            UpdateMetadataEndpoint()
                .setHost("host1")
                .setPort(1223)
                .setSecurityProtocol(plaintext.id)
                .setListener(ListenerName.forSecurityProtocol(plaintext).value),
        )
        val endpoints2 = mutableListOf(
            UpdateMetadataEndpoint()
                .setHost("host1")
                .setPort(1244)
                .setSecurityProtocol(plaintext.id)
                .setListener(ListenerName.forSecurityProtocol(plaintext).value),
        )
        if (version > 0) {
            val ssl = SecurityProtocol.SSL
            endpoints2.add(
                UpdateMetadataEndpoint()
                    .setHost("host2")
                    .setPort(1234)
                    .setSecurityProtocol(ssl.id)
                    .setListener(ListenerName.forSecurityProtocol(ssl).value)
            )
            endpoints2.add(
                UpdateMetadataEndpoint()
                    .setHost("host2")
                    .setPort(1334)
                    .setSecurityProtocol(ssl.id)
            )
            if (version >= 3) endpoints2[1].setListener("CLIENT")
        }
        val liveBrokers = listOf(
            UpdateMetadataBroker()
                .setId(0)
                .setEndpoints(endpoints1)
                .setRack(rack),
            UpdateMetadataBroker()
                .setId(1)
                .setEndpoints(endpoints2)
                .setRack(rack)
        )
        return UpdateMetadataRequest.Builder(
            version = version,
            controllerId = 1,
            controllerEpoch = 10,
            brokerEpoch = 0,
            partitionStates = partitionStates,
            liveBrokers = liveBrokers,
            topicIds = topicIds,
        ).build()
    }

    private fun createUpdateMetadataResponse(): UpdateMetadataResponse {
        return UpdateMetadataResponse(UpdateMetadataResponseData().setErrorCode(Errors.NONE.code))
    }

    private fun createSaslHandshakeRequest(version: Short): SaslHandshakeRequest {
        return SaslHandshakeRequest.Builder(
            SaslHandshakeRequestData().setMechanism("PLAIN")
        ).build(version)
    }

    private fun createSaslHandshakeResponse(): SaslHandshakeResponse {
        return SaslHandshakeResponse(
            SaslHandshakeResponseData()
                .setErrorCode(Errors.NONE.code).setMechanisms(listOf("GSSAPI"))
        )
    }

    private fun createSaslAuthenticateRequest(version: Short): SaslAuthenticateRequest {
        val data = SaslAuthenticateRequestData().setAuthBytes(ByteArray(0))
        return SaslAuthenticateRequest(data, version)
    }

    private fun createSaslAuthenticateResponse(): SaslAuthenticateResponse {
        val data = SaslAuthenticateResponseData()
            .setErrorCode(Errors.NONE.code)
            .setAuthBytes(ByteArray(0))
            .setSessionLifetimeMs(Long.MAX_VALUE)
        return SaslAuthenticateResponse(data)
    }

    private fun createApiVersionRequest(version: Short): ApiVersionsRequest {
        return ApiVersionsRequest.Builder().build(version)
    }

    private fun createApiVersionResponse(): ApiVersionsResponse {
        val apiVersions = ApiVersionCollection()
        apiVersions.add(
            ApiVersionsResponseData.ApiVersion()
                .setApiKey(0)
                .setMinVersion(0)
                .setMaxVersion(2)
        )
        return ApiVersionsResponse(
            ApiVersionsResponseData()
                .setErrorCode(Errors.NONE.code)
                .setThrottleTimeMs(0)
                .setApiKeys(apiVersions)
        )
    }

    private fun createCreateTopicRequest(version: Short, validateOnly: Boolean = version >= 1): CreateTopicsRequest {
        val data = CreateTopicsRequestData()
            .setTimeoutMs(123)
            .setValidateOnly(validateOnly)
        data.topics.add(
            CreatableTopic()
                .setNumPartitions(3)
                .setReplicationFactor(5)
        )
        val topic2 = CreatableTopic()
        data.topics.add(topic2)
        topic2.assignments.add(
            CreatableReplicaAssignment()
                .setPartitionIndex(0)
                .setBrokerIds(intArrayOf(1, 2, 3))
        )
        topic2.assignments.add(
            CreatableReplicaAssignment()
                .setPartitionIndex(1)
                .setBrokerIds(intArrayOf(2, 3, 4))
        )
        topic2.configs.add(
            CreateableTopicConfig()
                .setName("config1").setValue("value1")
        )
        return CreateTopicsRequest.Builder(data).build(version)
    }

    private fun createCreateTopicResponse(): CreateTopicsResponse {
        val data = CreateTopicsResponseData()
        data.topics.add(
            CreatableTopicResult()
                .setName("t1")
                .setErrorCode(Errors.INVALID_TOPIC_EXCEPTION.code)
                .setErrorMessage(null)
        )
        data.topics.add(
            CreatableTopicResult()
                .setName("t2")
                .setErrorCode(Errors.LEADER_NOT_AVAILABLE.code)
                .setErrorMessage("Leader with id 5 is not available.")
        )
        data.topics.add(
            CreatableTopicResult()
                .setName("t3")
                .setErrorCode(Errors.NONE.code)
                .setNumPartitions(1)
                .setReplicationFactor(2)
                .setConfigs(
                    listOf(
                        CreatableTopicConfigs()
                            .setName("min.insync.replicas")
                            .setValue("2")
                    )
                )
        )
        return CreateTopicsResponse(data)
    }

    private fun createDeleteTopicsRequest(version: Short): DeleteTopicsRequest {
        return DeleteTopicsRequest.Builder(
            DeleteTopicsRequestData()
                .setTopicNames(listOf("my_t1", "my_t2"))
                .setTimeoutMs(1000)
        ).build(version)
    }

    private fun createDeleteTopicsResponse(): DeleteTopicsResponse {
        val data = DeleteTopicsResponseData()
        data.responses.add(
            DeletableTopicResult()
                .setName("t1")
                .setErrorCode(Errors.INVALID_TOPIC_EXCEPTION.code)
                .setErrorMessage("Error Message")
        )
        data.responses.add(
            DeletableTopicResult()
                .setName("t2")
                .setErrorCode(Errors.TOPIC_AUTHORIZATION_FAILED.code)
                .setErrorMessage("Error Message")
        )
        data.responses.add(
            DeletableTopicResult()
                .setName("t3")
                .setErrorCode(Errors.NOT_CONTROLLER.code)
        )
        data.responses.add(
            DeletableTopicResult()
                .setName("t4")
                .setErrorCode(Errors.NONE.code)
        )
        return DeleteTopicsResponse(data)
    }

    private fun createInitPidRequest(version: Short): InitProducerIdRequest {
        val requestData = InitProducerIdRequestData()
            .setTransactionalId(null)
            .setTransactionTimeoutMs(100)
        return InitProducerIdRequest.Builder(requestData).build(version)
    }

    private fun createInitPidResponse(): InitProducerIdResponse {
        val responseData = InitProducerIdResponseData()
            .setErrorCode(Errors.NONE.code)
            .setProducerEpoch(3)
            .setProducerId(3332)
            .setThrottleTimeMs(0)
        return InitProducerIdResponse(responseData)
    }

    private fun createOffsetForLeaderTopicCollection(): OffsetForLeaderTopicCollection {
        val topics = OffsetForLeaderTopicCollection()
        topics.add(
            OffsetForLeaderTopic()
                .setTopic("topic1")
                .setPartitions(
                    listOf(
                        OffsetForLeaderPartition()
                            .setPartition(0)
                            .setLeaderEpoch(1)
                            .setCurrentLeaderEpoch(0),
                        OffsetForLeaderPartition()
                            .setPartition(1)
                            .setLeaderEpoch(1)
                            .setCurrentLeaderEpoch(0)
                    )
                )
        )
        topics.add(
            OffsetForLeaderTopic()
                .setTopic("topic2")
                .setPartitions(
                    listOf(
                        OffsetForLeaderPartition()
                            .setPartition(2)
                            .setLeaderEpoch(3)
                            .setCurrentLeaderEpoch(RecordBatch.NO_PARTITION_LEADER_EPOCH)
                    )
                )
        )
        return topics
    }

    private fun createLeaderEpochRequestForConsumer(): OffsetsForLeaderEpochRequest {
        val epochs = createOffsetForLeaderTopicCollection()
        return OffsetsForLeaderEpochRequest.Builder.forConsumer(epochs).build()
    }

    private fun createLeaderEpochRequestForReplica(version: Short, replicaId: Int): OffsetsForLeaderEpochRequest {
        val epochs = createOffsetForLeaderTopicCollection()
        return OffsetsForLeaderEpochRequest.Builder.forFollower(version, epochs, replicaId).build()
    }

    private fun createLeaderEpochResponse(): OffsetsForLeaderEpochResponse {
        val data = OffsetForLeaderEpochResponseData()
        data.topics.add(
            OffsetForLeaderTopicResult()
                .setTopic("topic1")
                .setPartitions(
                    listOf(
                        OffsetForLeaderEpochResponseData.EpochEndOffset()
                            .setPartition(0)
                            .setErrorCode(Errors.NONE.code)
                            .setLeaderEpoch(1)
                            .setEndOffset(0),
                        OffsetForLeaderEpochResponseData.EpochEndOffset()
                            .setPartition(1)
                            .setErrorCode(Errors.NONE.code)
                            .setLeaderEpoch(1)
                            .setEndOffset(1)
                    )
                )
        )
        data.topics.add(
            OffsetForLeaderTopicResult()
                .setTopic("topic2")
                .setPartitions(
                    listOf(
                        OffsetForLeaderEpochResponseData.EpochEndOffset()
                            .setPartition(2)
                            .setErrorCode(Errors.NONE.code)
                            .setLeaderEpoch(1)
                            .setEndOffset(1)
                    )
                )
        )
        return OffsetsForLeaderEpochResponse(data)
    }

    private fun createAddPartitionsToTxnRequest(version: Short): AddPartitionsToTxnRequest {
        return AddPartitionsToTxnRequest.Builder(
            "tid", 21L, 42.toShort(),
            listOf(TopicPartition("topic", 73))
        ).build(version)
    }

    private fun createAddPartitionsToTxnResponse(): AddPartitionsToTxnResponse {
        return AddPartitionsToTxnResponse(
            throttleTimeMs = 0,
            errors = mapOf(TopicPartition("t", 0) to Errors.NONE),
        )
    }

    private fun createAddOffsetsToTxnRequest(version: Short): AddOffsetsToTxnRequest {
        return AddOffsetsToTxnRequest.Builder(
            AddOffsetsToTxnRequestData()
                .setTransactionalId("tid")
                .setProducerId(21L)
                .setProducerEpoch(42.toShort())
                .setGroupId("gid")
        ).build(version)
    }

    private fun createAddOffsetsToTxnResponse(): AddOffsetsToTxnResponse {
        return AddOffsetsToTxnResponse(
            AddOffsetsToTxnResponseData()
                .setErrorCode(Errors.NONE.code)
                .setThrottleTimeMs(0)
        )
    }

    private fun createEndTxnRequest(version: Short): EndTxnRequest {
        return EndTxnRequest.Builder(
            EndTxnRequestData()
                .setTransactionalId("tid")
                .setProducerId(21L)
                .setProducerEpoch(42.toShort())
                .setCommitted(TransactionResult.COMMIT.id)
        ).build(version)
    }

    private fun createEndTxnResponse(): EndTxnResponse {
        return EndTxnResponse(
            EndTxnResponseData()
                .setErrorCode(Errors.NONE.code)
                .setThrottleTimeMs(0)
        )
    }

    private fun createWriteTxnMarkersRequest(version: Short): WriteTxnMarkersRequest {
        val partitions = listOf(TopicPartition("topic", 73))
        val txnMarkerEntry = TxnMarkerEntry(
            producerId = 21L,
            producerEpoch = 42.toShort(),
            coordinatorEpoch = 73,
            transactionResult = TransactionResult.ABORT,
            partitions = partitions,
        )
        return WriteTxnMarkersRequest.Builder(
            version = ApiKeys.WRITE_TXN_MARKERS.latestVersion(),
            markers = listOf(txnMarkerEntry),
        ).build(version)
    }

    private fun createWriteTxnMarkersResponse(): WriteTxnMarkersResponse {
        val errorPerPartitions: MutableMap<TopicPartition, Errors> = HashMap()
        errorPerPartitions[TopicPartition("topic", 73)] = Errors.NONE
        val response = mapOf(21L to errorPerPartitions)
        return WriteTxnMarkersResponse(response)
    }

    private fun createTxnOffsetCommitRequest(version: Short): TxnOffsetCommitRequest {
        val offsets = mapOf(
            TopicPartition(topic = "topic", partition = 73) to CommittedOffset(
                offset = 100,
                metadata = null,
                leaderEpoch = null,
            ),
            TopicPartition(topic = "topic", partition = 74) to CommittedOffset(
                offset = 100,
                metadata = "blah",
                leaderEpoch = 27,
            ),
        )

        return if (version < 3) {
            TxnOffsetCommitRequest.Builder(
                transactionalId = "transactionalId",
                consumerGroupId = "groupId",
                producerId = 21L,
                producerEpoch = 42,
                pendingTxnOffsetCommits = offsets,
            ).build()
        } else {
            TxnOffsetCommitRequest.Builder(
                transactionalId = "transactionalId",
                consumerGroupId = "groupId",
                producerId = 21L,
                producerEpoch = 42,
                pendingTxnOffsetCommits = offsets,
                memberId = "member",
                generationId = 2,
                groupInstanceId = "instance",
            ).build(version)
        }
    }

    private fun createTxnOffsetCommitRequestWithAutoDowngrade(): TxnOffsetCommitRequest {
        val offsets = mapOf(
            TopicPartition(topic = "topic", partition = 73) to CommittedOffset(
                offset = 100,
                metadata = null,
                leaderEpoch = null,
            ),
            TopicPartition(topic = "topic", partition = 74) to CommittedOffset(
                offset = 100,
                metadata = "blah",
                leaderEpoch = 27,
            ),
        )
        return TxnOffsetCommitRequest.Builder(
            transactionalId = "transactionalId",
            consumerGroupId = "groupId",
            producerId = 21L,
            producerEpoch = 42,
            pendingTxnOffsetCommits = offsets,
            memberId = "member",
            generationId = 2,
            groupInstanceId = "instance",
        ).build()
    }

    private fun createTxnOffsetCommitResponse(): TxnOffsetCommitResponse {
        val errorPerPartitions = mapOf(TopicPartition("topic", 73) to Errors.NONE)
        return TxnOffsetCommitResponse(requestThrottleMs = 0, responseData = errorPerPartitions)
    }

    private fun createDescribeAclsRequest(version: Short): DescribeAclsRequest {
        return DescribeAclsRequest.Builder(
            AclBindingFilter(
                patternFilter = ResourcePatternFilter(
                    resourceType = ResourceType.TOPIC,
                    name = "mytopic",
                    patternType = PatternType.LITERAL,
                ),
                entryFilter = AccessControlEntryFilter(
                    principal = null,
                    host = null,
                    operation = AclOperation.ANY,
                    permissionType = AclPermissionType.ANY,
                ),
            )
        ).build(version)
    }

    private fun createDescribeAclsResponse(): DescribeAclsResponse {
        val data = DescribeAclsResponseData()
            .setErrorCode(Errors.NONE.code)
            .setErrorMessage(Errors.NONE.message)
            .setThrottleTimeMs(0)
            .setResources(
                listOf(
                    DescribeAclsResource()
                        .setResourceType(ResourceType.TOPIC.code)
                        .setResourceName("mytopic")
                        .setPatternType(PatternType.LITERAL.code)
                        .setAcls(
                            listOf(
                                AclDescription()
                                    .setHost("*")
                                    .setOperation(AclOperation.WRITE.code)
                                    .setPermissionType(AclPermissionType.ALLOW.code)
                                    .setPrincipal("User:ANONYMOUS")
                            )
                        )
                )
            )
        return DescribeAclsResponse(data)
    }

    private fun createCreateAclsRequest(version: Short): CreateAclsRequest {
        val creations = listOf(
            CreateAclsRequest.aclCreation(
                AclBinding(
                    pattern = ResourcePattern(
                        resourceType = ResourceType.TOPIC,
                        name = "mytopic",
                        patternType = PatternType.LITERAL,
                    ),
                    entry = AccessControlEntry(
                        principal = "User:ANONYMOUS",
                        host = "127.0.0.1",
                        operation = AclOperation.READ,
                        permissionType = AclPermissionType.ALLOW,
                    ),
                )
            ),
            CreateAclsRequest.aclCreation(
                AclBinding(
                    pattern = ResourcePattern(
                        resourceType = ResourceType.GROUP,
                        name = "mygroup",
                        patternType = PatternType.LITERAL,
                    ),
                    entry = AccessControlEntry(
                        principal = "User:ANONYMOUS",
                        host = "*",
                        operation = AclOperation.WRITE,
                        permissionType = AclPermissionType.DENY,
                    ),
                )
            ),
        )
        val data = CreateAclsRequestData().setCreations(creations)
        return CreateAclsRequest.Builder(data).build(version)
    }

    private fun createCreateAclsResponse(): CreateAclsResponse {
        return CreateAclsResponse(
            CreateAclsResponseData().setResults(
                listOf(
                    AclCreationResult(),
                    AclCreationResult()
                        .setErrorCode(Errors.NONE.code)
                        .setErrorMessage("Foo bar")
                )
            )
        )
    }

    private fun createDeleteAclsRequest(version: Short): DeleteAclsRequest {
        val data = DeleteAclsRequestData().setFilters(
            listOf(
                DeleteAclsFilter()
                    .setResourceTypeFilter(ResourceType.ANY.code)
                    .setResourceNameFilter(null)
                    .setPatternTypeFilter(PatternType.LITERAL.code)
                    .setPrincipalFilter("User:ANONYMOUS")
                    .setHostFilter(null)
                    .setOperation(AclOperation.ANY.code)
                    .setPermissionType(AclPermissionType.ANY.code),
                DeleteAclsFilter()
                    .setResourceTypeFilter(ResourceType.ANY.code)
                    .setResourceNameFilter(null)
                    .setPatternTypeFilter(PatternType.LITERAL.code)
                    .setPrincipalFilter("User:bob")
                    .setHostFilter(null)
                    .setOperation(AclOperation.ANY.code)
                    .setPermissionType(AclPermissionType.ANY.code)
            )
        )
        return DeleteAclsRequest.Builder(data).build(version)
    }

    private fun createDeleteAclsResponse(version: Short): DeleteAclsResponse {
        val filterResults = listOf(
            DeleteAclsFilterResult().setMatchingAcls(
                listOf(
                    DeleteAclsMatchingAcl()
                        .setResourceType(ResourceType.TOPIC.code)
                        .setResourceName("mytopic3")
                        .setPatternType(PatternType.LITERAL.code)
                        .setPrincipal("User:ANONYMOUS")
                        .setHost("*")
                        .setOperation(AclOperation.DESCRIBE.code)
                        .setPermissionType(AclPermissionType.ALLOW.code),
                    DeleteAclsMatchingAcl()
                        .setResourceType(ResourceType.TOPIC.code)
                        .setResourceName("mytopic4")
                        .setPatternType(PatternType.LITERAL.code)
                        .setPrincipal("User:ANONYMOUS")
                        .setHost("*")
                        .setOperation(AclOperation.DESCRIBE.code)
                        .setPermissionType(AclPermissionType.DENY.code)
                )
            ),
            DeleteAclsFilterResult()
                .setErrorCode(Errors.SECURITY_DISABLED.code)
                .setErrorMessage("No security"),
        )
        return DeleteAclsResponse(
            data = DeleteAclsResponseData()
                .setThrottleTimeMs(0)
                .setFilterResults(filterResults),
            version = version,
        )
    }

    private fun createDescribeConfigsRequest(version: Short): DescribeConfigsRequest {
        return DescribeConfigsRequest.Builder(
            DescribeConfigsRequestData()
                .setResources(
                    listOf(
                        DescribeConfigsResource()
                            .setResourceType(ConfigResource.Type.BROKER.id)
                            .setResourceName("0"),
                        DescribeConfigsResource()
                            .setResourceType(ConfigResource.Type.TOPIC.id)
                            .setResourceName("topic")
                    )
                )
        ).build(version)
    }

    private fun createDescribeConfigsRequestWithConfigEntries(version: Short): DescribeConfigsRequest {
        return DescribeConfigsRequest.Builder(
            DescribeConfigsRequestData()
                .setResources(
                    listOf(
                        DescribeConfigsResource()
                            .setResourceType(ConfigResource.Type.BROKER.id)
                            .setResourceName("0")
                            .setConfigurationKeys(mutableListOf("foo", "bar")),
                        DescribeConfigsResource()
                            .setResourceType(ConfigResource.Type.TOPIC.id)
                            .setResourceName("topic")
                            .setConfigurationKeys(emptyList()),
                        DescribeConfigsResource()
                            .setResourceType(ConfigResource.Type.TOPIC.id)
                            .setResourceName("topic a")
                            .setConfigurationKeys(emptyList())
                    )
                )
        ).build(version)
    }

    private fun createDescribeConfigsRequestWithDocumentation(version: Short): DescribeConfigsRequest {
        val data = DescribeConfigsRequestData()
            .setResources(
                listOf(
                    DescribeConfigsResource()
                        .setResourceType(ConfigResource.Type.BROKER.id)
                        .setResourceName("0")
                        .setConfigurationKeys(mutableListOf("foo", "bar"))
                )
            )
        if (version.toInt() == 3) data.setIncludeDocumentation(true)
        return DescribeConfigsRequest.Builder(data).build(version)
    }

    private fun createDescribeConfigsResponse(version: Short): DescribeConfigsResponse {
        return DescribeConfigsResponse(
            DescribeConfigsResponseData().setResults(
                listOf(
                    DescribeConfigsResponseData.DescribeConfigsResult()
                        .setErrorCode(Errors.NONE.code)
                        .setResourceType(ConfigResource.Type.BROKER.id)
                        .setResourceName("0")
                        .setConfigs(
                            listOf(
                                DescribeConfigsResourceResult()
                                    .setName("config_name")
                                    .setValue("config_value") // Note: the v0 default for this field that should be exposed to callers is
                                    // context-dependent. For example, if the resource is a broker, this should default to 4.
                                    // -1 is just a placeholder value.
                                    .setConfigSource(
                                        if (version.toInt() == 0) ConfigSource.STATIC_BROKER_CONFIG.id
                                        else ConfigSource.DYNAMIC_BROKER_CONFIG.id
                                    )
                                    .setIsSensitive(true).setReadOnly(false)
                                    .setSynonyms(emptyList()),
                                DescribeConfigsResourceResult()
                                    .setName("yet_another_name")
                                    .setValue("yet another value")
                                    .setConfigSource(
                                        if (version.toInt() == 0) ConfigSource.STATIC_BROKER_CONFIG.id
                                        else ConfigSource.DEFAULT_CONFIG.id
                                    )
                                    .setIsSensitive(false).setReadOnly(true)
                                    .setSynonyms(emptyList())
                                    .setConfigType(DescribeConfigsResponse.ConfigType.BOOLEAN.id)
                                    .setDocumentation("some description"),
                                DescribeConfigsResourceResult()
                                    .setName("another_name")
                                    .setValue("another value")
                                    .setConfigSource(
                                        if (version.toInt() == 0) ConfigSource.STATIC_BROKER_CONFIG.id
                                        else ConfigSource.DEFAULT_CONFIG.id
                                    )
                                    .setIsSensitive(false).setReadOnly(true)
                                    .setSynonyms(emptyList())
                            )
                        ),
                    DescribeConfigsResponseData.DescribeConfigsResult()
                        .setErrorCode(Errors.NONE.code)
                        .setResourceType(ConfigResource.Type.TOPIC.id)
                        .setResourceName("topic")
                        .setConfigs(emptyList())
                )
            )
        )
    }

    private fun createAlterConfigsRequest(version: Short): AlterConfigsRequest {
        val configEntries = listOf(
            AlterConfigsRequest.ConfigEntry(name = "config_name", value = "config_value"),
            AlterConfigsRequest.ConfigEntry(name = "another_name", value = "another value")
        )
        val configs = mapOf(
            ConfigResource(ConfigResource.Type.BROKER, "0") to AlterConfigsRequest.Config(configEntries),
            ConfigResource(ConfigResource.Type.TOPIC, "topic") to AlterConfigsRequest.Config(emptyList()),
        )
        return AlterConfigsRequest.Builder(configs, false).build(version)
    }

    private fun createAlterConfigsResponse(): AlterConfigsResponse {
        val data = AlterConfigsResponseData().setThrottleTimeMs(20)
        data.responses += AlterConfigsResponseData.AlterConfigsResourceResponse()
            .setErrorCode(Errors.NONE.code)
            .setErrorMessage(null)
            .setResourceName("0")
            .setResourceType(ConfigResource.Type.BROKER.id)
        data.responses += AlterConfigsResponseData.AlterConfigsResourceResponse()
            .setErrorCode(Errors.INVALID_REQUEST.code)
            .setErrorMessage("This request is invalid")
            .setResourceName("topic")
            .setResourceType(ConfigResource.Type.TOPIC.id)

        return AlterConfigsResponse(data)
    }

    private fun createCreatePartitionsRequest(version: Short): CreatePartitionsRequest {
        val topics = CreatePartitionsTopicCollection()
        topics.add(
            CreatePartitionsTopic()
                .setName("my_topic")
                .setCount(3)
        )
        topics.add(
            CreatePartitionsTopic()
                .setName("my_other_topic")
                .setCount(3)
        )
        val data = CreatePartitionsRequestData()
            .setTimeoutMs(0)
            .setValidateOnly(false)
            .setTopics(topics)
        return CreatePartitionsRequest(data, version)
    }

    private fun createCreatePartitionsRequestWithAssignments(version: Short): CreatePartitionsRequest {
        val topics = CreatePartitionsTopicCollection()
        val myTopicAssignment = CreatePartitionsAssignment()
            .setBrokerIds(intArrayOf(2))
        topics.add(
            CreatePartitionsTopic()
                .setName("my_topic")
                .setCount(3)
                .setAssignments(listOf(myTopicAssignment))
        )
        topics.add(
            CreatePartitionsTopic()
                .setName("my_other_topic")
                .setCount(3)
                .setAssignments(
                    listOf(
                        CreatePartitionsAssignment().setBrokerIds(intArrayOf(2, 3)),
                        CreatePartitionsAssignment().setBrokerIds(intArrayOf(3, 1))
                    )
                )
        )
        val data = CreatePartitionsRequestData()
            .setTimeoutMs(0)
            .setValidateOnly(false)
            .setTopics(topics)
        return CreatePartitionsRequest(data, version)
    }

    private fun createCreatePartitionsResponse(): CreatePartitionsResponse {
        val results = listOf(
            CreatePartitionsTopicResult()
                .setName("my_topic")
                .setErrorCode(Errors.INVALID_REPLICA_ASSIGNMENT.code),
            CreatePartitionsTopicResult()
                .setName("my_topic")
                .setErrorCode(Errors.NONE.code),
        )
        val data = CreatePartitionsResponseData()
            .setThrottleTimeMs(42)
            .setResults(results)

        return CreatePartitionsResponse(data)
    }

    private fun createCreateTokenRequest(version: Short): CreateDelegationTokenRequest {
        val renewers = listOf(
            CreatableRenewers()
                .setPrincipalType("User")
                .setPrincipalName("user1"),
            CreatableRenewers()
                .setPrincipalType("User")
                .setPrincipalName("user2"),
        )

        return CreateDelegationTokenRequest.Builder(
            CreateDelegationTokenRequestData()
                .setRenewers(renewers)
                .setMaxLifetimeMs(System.currentTimeMillis())
        ).build(version)
    }

    private fun createCreateTokenResponse(): CreateDelegationTokenResponse {
        val data = CreateDelegationTokenResponseData()
            .setThrottleTimeMs(20)
            .setErrorCode(Errors.NONE.code)
            .setPrincipalType("User")
            .setPrincipalName("user1")
            .setIssueTimestampMs(System.currentTimeMillis())
            .setExpiryTimestampMs(System.currentTimeMillis())
            .setMaxTimestampMs(System.currentTimeMillis())
            .setTokenId("token1")
            .setHmac("test".toByteArray())

        return CreateDelegationTokenResponse(data)
    }

    private fun createRenewTokenRequest(version: Short): RenewDelegationTokenRequest {
        val data = RenewDelegationTokenRequestData()
            .setHmac("test".toByteArray())
            .setRenewPeriodMs(System.currentTimeMillis())

        return RenewDelegationTokenRequest.Builder(data).build(version)
    }

    private fun createRenewTokenResponse(): RenewDelegationTokenResponse {
        val data = RenewDelegationTokenResponseData()
            .setThrottleTimeMs(20)
            .setErrorCode(Errors.NONE.code)
            .setExpiryTimestampMs(System.currentTimeMillis())

        return RenewDelegationTokenResponse(data)
    }

    private fun createExpireTokenRequest(version: Short): ExpireDelegationTokenRequest {
        val data = ExpireDelegationTokenRequestData()
            .setHmac("test".toByteArray())
            .setExpiryTimePeriodMs(System.currentTimeMillis())

        return ExpireDelegationTokenRequest.Builder(data).build(version)
    }

    private fun createExpireTokenResponse(): ExpireDelegationTokenResponse {
        val data = ExpireDelegationTokenResponseData()
            .setThrottleTimeMs(20)
            .setErrorCode(Errors.NONE.code)
            .setExpiryTimestampMs(System.currentTimeMillis())

        return ExpireDelegationTokenResponse(data)
    }

    private fun createDescribeTokenRequest(version: Short): DescribeDelegationTokenRequest {
        val owners = listOf(
            parseKafkaPrincipal("User:user1"),
            parseKafkaPrincipal("User:user2"),
        )

        return DescribeDelegationTokenRequest.Builder(owners).build(version)
    }

    private fun createDescribeTokenResponse(version: Short): DescribeDelegationTokenResponse {
        val renewers: MutableList<KafkaPrincipal> = ArrayList()
        renewers.add(parseKafkaPrincipal("User:user1"))
        renewers.add(parseKafkaPrincipal("User:user2"))
        val tokenInfo1 = TokenInformation(
            tokenId = "1",
            owner = parseKafkaPrincipal("User:owner"),
            renewers = renewers,
            issueTimestamp = System.currentTimeMillis(),
            maxTimestamp = System.currentTimeMillis(),
            expiryTimestamp = System.currentTimeMillis(),
        )
        val tokenInfo2 = TokenInformation(
            tokenId = "2",
            owner = parseKafkaPrincipal("User:owner1"),
            renewers = renewers,
            issueTimestamp = System.currentTimeMillis(),
            maxTimestamp = System.currentTimeMillis(),
            expiryTimestamp = System.currentTimeMillis(),
        )
        val tokenList = listOf(
            DelegationToken(tokenInfo1, "test".toByteArray()),
            DelegationToken(tokenInfo2, "test".toByteArray()),
        )

        return DescribeDelegationTokenResponse(
            version = version.toInt(),
            throttleTimeMs = 20,
            error = Errors.NONE,
            tokens = tokenList,
        )
    }

    private fun createElectLeadersRequestNullPartitions(): ElectLeadersRequest {
        return ElectLeadersRequest.Builder(
            electionType = ElectionType.PREFERRED,
            topicPartitions = null,
            timeoutMs = 100,
        ).build(version = 1)
    }

    private fun createElectLeadersRequest(version: Short): ElectLeadersRequest {
        val partitions = listOf(
            TopicPartition("data", 1),
            TopicPartition("data", 2),
        )

        return ElectLeadersRequest.Builder(
            electionType = ElectionType.PREFERRED,
            topicPartitions = partitions,
            timeoutMs = 100
        ).build(version)
    }

    private fun createElectLeadersResponse(): ElectLeadersResponse {
        val topic = "myTopic"
        val electionResults: MutableList<ReplicaElectionResult> = ArrayList()
        val electionResult = ReplicaElectionResult()
        electionResults.add(electionResult)
        electionResult.setTopic(topic)
        // Add partition 1 result
        var partitionResult = PartitionResult()
        partitionResult.setPartitionId(0)
        partitionResult.setErrorCode(ApiError.NONE.error.code)
        partitionResult.setErrorMessage(ApiError.NONE.message)
        electionResult.partitionResult += partitionResult

        // Add partition 2 result
        partitionResult = PartitionResult()
        partitionResult.setPartitionId(1)
        partitionResult.setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code)
        partitionResult.setErrorMessage(Errors.UNKNOWN_TOPIC_OR_PARTITION.message)
        electionResult.partitionResult += partitionResult
        return ElectLeadersResponse(
            throttleTimeMs = 200,
            errorCode = Errors.NONE.code,
            electionResults = electionResults,
            version = ApiKeys.ELECT_LEADERS.latestVersion(),
        )
    }

    private fun createIncrementalAlterConfigsRequest(version: Short): IncrementalAlterConfigsRequest {
        val data = IncrementalAlterConfigsRequestData()
        val alterableConfig = IncrementalAlterConfigsRequestData.AlterableConfig()
            .setName("retention.ms")
            .setConfigOperation(0.toByte())
            .setValue("100")
        val alterableConfigs = IncrementalAlterConfigsRequestData.AlterableConfigCollection()
        alterableConfigs.add(alterableConfig)
        data.resources.add(
            IncrementalAlterConfigsRequestData.AlterConfigsResource()
                .setResourceName("testtopic")
                .setResourceType(ResourceType.TOPIC.code)
                .setConfigs(alterableConfigs)
        )

        return IncrementalAlterConfigsRequest.Builder(data).build(version)
    }

    private fun createIncrementalAlterConfigsResponse(): IncrementalAlterConfigsResponse {
        val data = IncrementalAlterConfigsResponseData()
        data.responses += IncrementalAlterConfigsResponseData.AlterConfigsResourceResponse()
            .setResourceName("testtopic")
            .setResourceType(ResourceType.TOPIC.code)
            .setErrorCode(Errors.NONE.code)
            .setErrorMessage("Duplicate Keys")

        return IncrementalAlterConfigsResponse(data)
    }

    private fun createAlterPartitionReassignmentsRequest(version: Short): AlterPartitionReassignmentsRequest {
        val data = AlterPartitionReassignmentsRequestData()
        data.topics += ReassignableTopic()
            .setName("topic")
            .setPartitions(
                listOf(
                    ReassignablePartition().setPartitionIndex(0).setReplicas(intArrayOf())
                )
            )

        return AlterPartitionReassignmentsRequest.Builder(data).build(version)
    }

    private fun createAlterPartitionReassignmentsResponse(): AlterPartitionReassignmentsResponse {
        val data = AlterPartitionReassignmentsResponseData()
        data.responses += ReassignableTopicResponse()
            .setName("topic")
            .setPartitions(
                listOf(
                    ReassignablePartitionResponse()
                        .setPartitionIndex(0)
                        .setErrorCode(Errors.NONE.code)
                        .setErrorMessage("No reassignment is in progress for topic topic partition 0")
                )
            )

        return AlterPartitionReassignmentsResponse(data)
    }

    private fun createListPartitionReassignmentsRequest(version: Short): ListPartitionReassignmentsRequest {
        val data = ListPartitionReassignmentsRequestData()
        data.setTopics(
            listOf(
                ListPartitionReassignmentsTopics()
                    .setName("topic")
                    .setPartitionIndexes(intArrayOf(1))
            )
        )

        return ListPartitionReassignmentsRequest.Builder(data).build(version)
    }

    private fun createListPartitionReassignmentsResponse(): ListPartitionReassignmentsResponse {
        val data = ListPartitionReassignmentsResponseData()
        data.setTopics(
            listOf(
                OngoingTopicReassignment()
                    .setName("topic")
                    .setPartitions(
                        listOf(
                            OngoingPartitionReassignment()
                                .setPartitionIndex(0)
                                .setReplicas(intArrayOf(1, 2))
                                .setAddingReplicas(intArrayOf(2))
                                .setRemovingReplicas(intArrayOf(1))
                        )
                    )
            )
        )

        return ListPartitionReassignmentsResponse(data)
    }

    private fun createOffsetDeleteRequest(version: Short): OffsetDeleteRequest {
        val topics = OffsetDeleteRequestTopicCollection()
        topics.add(
            OffsetDeleteRequestTopic()
                .setName("topic1")
                .setPartitions(
                    listOf(OffsetDeleteRequestPartition().setPartitionIndex(0))
                )
        )
        val data = OffsetDeleteRequestData()
        data.setGroupId("group1")
        data.setTopics(topics)

        return OffsetDeleteRequest.Builder(data).build(version)
    }

    private fun createOffsetDeleteResponse(): OffsetDeleteResponse {
        val partitions = OffsetDeleteResponsePartitionCollection()
        partitions.add(
            OffsetDeleteResponsePartition()
                .setPartitionIndex(0)
                .setErrorCode(Errors.NONE.code)
        )
        val topics = OffsetDeleteResponseTopicCollection()
        topics.add(
            OffsetDeleteResponseTopic()
                .setName("topic1")
                .setPartitions(partitions)
        )
        val data = OffsetDeleteResponseData()
        data.setErrorCode(Errors.NONE.code)
        data.setTopics(topics)
        return OffsetDeleteResponse(data)
    }

    private fun createAlterReplicaLogDirsRequest(version: Short): AlterReplicaLogDirsRequest {
        val data = AlterReplicaLogDirsRequestData()
        data.dirs.add(
            AlterReplicaLogDir()
                .setPath("/data0")
                .setTopics(
                    AlterReplicaLogDirTopicCollection(
                        listOf(
                            AlterReplicaLogDirTopic()
                                .setPartitions(intArrayOf(0))
                                .setName("topic")
                        ).iterator()
                    )
                )
        )
        return AlterReplicaLogDirsRequest.Builder(data).build(version)
    }

    private fun createAlterReplicaLogDirsResponse(): AlterReplicaLogDirsResponse {
        val data = AlterReplicaLogDirsResponseData()
        data.results += AlterReplicaLogDirTopicResult()
            .setTopicName("topic")
            .setPartitions(
                listOf(
                    AlterReplicaLogDirPartitionResult()
                        .setPartitionIndex(0)
                        .setErrorCode(Errors.NONE.code)
                )
            )

        return AlterReplicaLogDirsResponse(data)
    }

    private fun createDescribeClientQuotasRequest(version: Short): DescribeClientQuotasRequest {
        val filter = ClientQuotaFilter.all()
        return DescribeClientQuotasRequest.Builder(filter).build(version)
    }

    private fun createDescribeClientQuotasResponse(): DescribeClientQuotasResponse {
        val data = DescribeClientQuotasResponseData().setEntries(
            listOf(
                DescribeClientQuotasResponseData.EntryData()
                    .setEntity(
                        listOf(
                            DescribeClientQuotasResponseData.EntityData()
                                .setEntityType(ClientQuotaEntity.USER)
                                .setEntityName("user")
                        )
                    )
                    .setValues(
                        listOf(
                            DescribeClientQuotasResponseData.ValueData()
                                .setKey("request_percentage")
                                .setValue(1.0)
                        )
                    )
            )
        )
        return DescribeClientQuotasResponse(data)
    }

    private fun createAlterClientQuotasRequest(version: Short): AlterClientQuotasRequest {
        val entity = ClientQuotaEntity(mapOf(ClientQuotaEntity.USER to "user"))
        val op = ClientQuotaAlteration.Op("request_percentage", 2.0)
        val alteration = ClientQuotaAlteration(entity, setOf(op))

        return AlterClientQuotasRequest.Builder(
            entries = setOf(alteration),
            validateOnly = false,
        ).build(version)
    }

    private fun createAlterClientQuotasResponse(): AlterClientQuotasResponse {
        val data = AlterClientQuotasResponseData().setEntries(
            listOf(
                AlterClientQuotasResponseData.EntryData()
                    .setEntity(
                        listOf(
                            AlterClientQuotasResponseData.EntityData()
                                .setEntityType(ClientQuotaEntity.USER)
                                .setEntityName("user")
                        )
                    )
            )
        )

        return AlterClientQuotasResponse(data)
    }

    private fun createDescribeProducersRequest(version: Short): DescribeProducersRequest {
        val data = DescribeProducersRequestData()
        val topicRequest = TopicRequest()
        topicRequest.setName("test")
        topicRequest.partitionIndexes += listOf(0, 1)
        data.topics += topicRequest

        return DescribeProducersRequest.Builder(data).build(version)
    }

    private fun createDescribeProducersResponse(): DescribeProducersResponse {
        val data = DescribeProducersResponseData()
        val topicResponse = TopicResponse()
        topicResponse.partitions += DescribeProducersResponseData.PartitionResponse()
            .setErrorCode(Errors.NONE.code)
            .setPartitionIndex(0)
            .setActiveProducers(
                listOf(
                    DescribeProducersResponseData.ProducerState()
                        .setProducerId(1234L)
                        .setProducerEpoch(15)
                        .setLastTimestamp(13490218304L)
                        .setCurrentTxnStartOffset(5000),
                    DescribeProducersResponseData.ProducerState()
                        .setProducerId(9876L)
                        .setProducerEpoch(32)
                        .setLastTimestamp(13490218399L)
                )
            )
        data.topics += topicResponse

        return DescribeProducersResponse(data)
    }

    private fun createBrokerHeartbeatRequest(v: Short): BrokerHeartbeatRequest {
        val data = BrokerHeartbeatRequestData()
            .setBrokerId(1)
            .setBrokerEpoch(1)
            .setCurrentMetadataOffset(1)
            .setWantFence(false)
            .setWantShutDown(false)

        return BrokerHeartbeatRequest.Builder(data).build(v)
    }

    private fun createBrokerHeartbeatResponse(): BrokerHeartbeatResponse {
        val data = BrokerHeartbeatResponseData()
            .setIsFenced(false)
            .setShouldShutDown(false)
            .setThrottleTimeMs(0)

        return BrokerHeartbeatResponse(data)
    }

    private fun createBrokerRegistrationRequest(v: Short): BrokerRegistrationRequest {
        val data = BrokerRegistrationRequestData()
            .setBrokerId(1)
            .setClusterId(Uuid.randomUuid().toString())
            .setRack("1")
            .setFeatures(
                FeatureCollection(
                    listOf(BrokerRegistrationRequestData.Feature()).iterator()
                )
            )
            .setListeners(
                ListenerCollection(
                    listOf(BrokerRegistrationRequestData.Listener()).iterator()
                )
            )
            .setIncarnationId(Uuid.randomUuid())

        return BrokerRegistrationRequest.Builder(data).build(v)
    }

    private fun createBrokerRegistrationResponse(): BrokerRegistrationResponse {
        val data = BrokerRegistrationResponseData()
            .setBrokerEpoch(1)
            .setThrottleTimeMs(0)

        return BrokerRegistrationResponse(data)
    }

    private fun createUnregisterBrokerRequest(version: Short): UnregisterBrokerRequest {
        val data = UnregisterBrokerRequestData().setBrokerId(1)

        return UnregisterBrokerRequest.Builder(data).build(version)
    }

    private fun createUnregisterBrokerResponse(): UnregisterBrokerResponse {
        return UnregisterBrokerResponse(UnregisterBrokerResponseData())
    }

    private fun createDescribeTransactionsRequest(version: Short): DescribeTransactionsRequest {
        val data = DescribeTransactionsRequestData()
            .setTransactionalIds(mutableListOf("t1", "t2", "t3"))

        return DescribeTransactionsRequest.Builder(data).build(version)
    }

    private fun createDescribeTransactionsResponse(): DescribeTransactionsResponse {
        val data = DescribeTransactionsResponseData()
        data.setTransactionStates(
            listOf(
                DescribeTransactionsResponseData.TransactionState()
                    .setErrorCode(Errors.NONE.code)
                    .setTransactionalId("t1")
                    .setProducerId(12345L)
                    .setProducerEpoch(15.toShort())
                    .setTransactionStartTimeMs(13490218304L)
                    .setTransactionState("Empty"),
                DescribeTransactionsResponseData.TransactionState()
                    .setErrorCode(Errors.NONE.code)
                    .setTransactionalId("t2")
                    .setProducerId(98765L)
                    .setProducerEpoch(30.toShort())
                    .setTransactionStartTimeMs(13490218304L)
                    .setTransactionState("Ongoing")
                    .setTopics(
                        TopicDataCollection(
                            listOf(
                                DescribeTransactionsResponseData.TopicData()
                                    .setTopic("foo")
                                    .setPartitions(intArrayOf(1, 3, 5, 7)),
                                DescribeTransactionsResponseData.TopicData()
                                    .setTopic("bar")
                                    .setPartitions(intArrayOf(1, 3))
                            ).iterator()
                        )
                    ),
                DescribeTransactionsResponseData.TransactionState()
                    .setErrorCode(Errors.NOT_COORDINATOR.code)
                    .setTransactionalId("t3")
            )
        )

        return DescribeTransactionsResponse(data)
    }

    private fun createListTransactionsRequest(version: Short): ListTransactionsRequest {
        return ListTransactionsRequest.Builder(
            ListTransactionsRequestData()
                .setStateFilters(listOf("Ongoing"))
                .setProducerIdFilters(longArrayOf(1L, 2L, 15L))
        ).build(version)
    }

    private fun createListTransactionsResponse(): ListTransactionsResponse {
        val response = ListTransactionsResponseData()
        response.setErrorCode(Errors.NONE.code)
        response.setTransactionStates(
            listOf(
                ListTransactionsResponseData.TransactionState()
                    .setTransactionalId("foo")
                    .setProducerId(12345L)
                    .setTransactionState("Ongoing"),
                ListTransactionsResponseData.TransactionState()
                    .setTransactionalId("bar")
                    .setProducerId(98765L)
                    .setTransactionState("PrepareAbort")
            )
        )

        return ListTransactionsResponse(response)
    }

    @Test
    fun testInvalidSaslHandShakeRequest() {
        val request: AbstractRequest = SaslHandshakeRequest.Builder(
            SaslHandshakeRequestData().setMechanism("PLAIN")
        ).build()
        val serializedBytes = request.serialize()
        // corrupt the length of the sasl mechanism string
        serializedBytes.putShort(0, Short.MAX_VALUE)
        val error = assertFailsWith<RuntimeException> {
            AbstractRequest.parseRequest(
                apiKey = request.apiKey,
                apiVersion = request.version,
                buffer = serializedBytes
            )
        }
        assertEquals("Error reading byte array of 32767 byte(s): only 5 byte(s) available", error.message)
    }

    @Test
    fun testInvalidSaslAuthenticateRequest() {
        val version = 1.toShort() // choose a version with fixed length encoding, for simplicity
        val b = byteArrayOf(
            0x11, 0x1f, 0x15, 0x2c,
            0x5e, 0x2a, 0x20, 0x26,
            0x6c, 0x39, 0x45, 0x1f,
            0x25, 0x1c, 0x2d, 0x25,
            0x43, 0x2a, 0x11, 0x76,
        )
        val data = SaslAuthenticateRequestData().setAuthBytes(b)
        val request: AbstractRequest = SaslAuthenticateRequest(data, version)
        val serializedBytes = request.serialize()

        // corrupt the length of the bytes array
        serializedBytes.putInt(0, Int.MAX_VALUE)
        val error = assertFailsWith<RuntimeException> {
            AbstractRequest.parseRequest(
                apiKey = request.apiKey,
                apiVersion = request.version,
                buffer = serializedBytes
            )
        }
        assertEquals("Error reading byte array of 2147483647 byte(s): only 20 byte(s) available", error.message)
    }

    @Test
    fun testValidTaggedFieldsWithSaslAuthenticateRequest() {
        val byteArray = ByteArray(11)
        val accessor = ByteBufferAccessor(ByteBuffer.wrap(byteArray))

        //construct a SASL_AUTHENTICATE request
        val authBytes = "test".toByteArray()
        accessor.writeUnsignedVarint(authBytes.size + 1)
        accessor.writeByteArray(authBytes)

        //write total numbers of tags
        accessor.writeUnsignedVarint(1)

        //write first tag
        val taggedField = RawTaggedField(1, byteArrayOf(0x1, 0x2, 0x3))
        accessor.writeUnsignedVarint(taggedField.tag)
        accessor.writeUnsignedVarint(taggedField.size)
        accessor.writeByteArray(taggedField.data)
        accessor.flip()
        val saslAuthenticateRequest = AbstractRequest.parseRequest(
            apiKey = ApiKeys.SASL_AUTHENTICATE,
            apiVersion = ApiKeys.SASL_AUTHENTICATE.latestVersion(),
            buffer = accessor.buffer(),
        ).request as SaslAuthenticateRequest
        assertContentEquals(authBytes, saslAuthenticateRequest.data().authBytes)
        assertEquals(1, saslAuthenticateRequest.data().unknownTaggedFields().size)
        assertEquals(taggedField, saslAuthenticateRequest.data().unknownTaggedFields()[0])
    }

    @Test
    fun testInvalidTaggedFieldsWithSaslAuthenticateRequest() {
        val byteArray = ByteArray(13)
        val accessor = ByteBufferAccessor(ByteBuffer.wrap(byteArray))

        //construct a SASL_AUTHENTICATE request
        val authBytes = "test".toByteArray()
        accessor.writeUnsignedVarint(authBytes.size + 1)
        accessor.writeByteArray(authBytes)

        //write total numbers of tags
        accessor.writeUnsignedVarint(1)

        //write first tag
        val taggedField = RawTaggedField(1, byteArrayOf(0x1, 0x2, 0x3))
        accessor.writeUnsignedVarint(taggedField.tag)
        accessor.writeUnsignedVarint(Short.MAX_VALUE.toInt()) // set wrong size for tagged field
        accessor.writeByteArray(taggedField.data)
        accessor.flip()
        val error = assertFailsWith<RuntimeException> {
            AbstractRequest.parseRequest(
                apiKey = ApiKeys.SASL_AUTHENTICATE,
                apiVersion = ApiKeys.SASL_AUTHENTICATE.latestVersion(),
                buffer = accessor.buffer()
            )
        }
        assertEquals("Error reading byte array of 32767 byte(s): only 3 byte(s) available", error.message)
    }
}
