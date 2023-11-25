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

import org.apache.kafka.clients.ClientDnsLookup
import org.apache.kafka.clients.ClientUtils.parseAndValidateAddresses
import org.apache.kafka.clients.MockClient
import org.apache.kafka.clients.MockClient.RequestMatcher
import org.apache.kafka.clients.NodeApiVersions
import org.apache.kafka.clients.admin.KafkaAdminClient.TimeoutProcessor
import org.apache.kafka.clients.admin.KafkaAdminClient.TimeoutProcessorFactory
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.clients.consumer.internals.ConsumerProtocol
import org.apache.kafka.clients.consumer.internals.ConsumerProtocol.serializeAssignment
import org.apache.kafka.common.Cluster
import org.apache.kafka.common.ConsumerGroupState
import org.apache.kafka.common.ElectionType
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.KafkaFuture
import org.apache.kafka.common.Node
import org.apache.kafka.common.PartitionInfo
import org.apache.kafka.common.TopicCollection
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.TopicPartitionReplica
import org.apache.kafka.common.Uuid
import org.apache.kafka.common.acl.AccessControlEntry
import org.apache.kafka.common.acl.AccessControlEntryFilter
import org.apache.kafka.common.acl.AclBinding
import org.apache.kafka.common.acl.AclBindingFilter
import org.apache.kafka.common.acl.AclOperation
import org.apache.kafka.common.acl.AclPermissionType
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.errors.ApiException
import org.apache.kafka.common.errors.AuthenticationException
import org.apache.kafka.common.errors.ClusterAuthorizationException
import org.apache.kafka.common.errors.FencedInstanceIdException
import org.apache.kafka.common.errors.GroupAuthorizationException
import org.apache.kafka.common.errors.GroupSubscribedToTopicException
import org.apache.kafka.common.errors.InvalidRequestException
import org.apache.kafka.common.errors.InvalidTopicException
import org.apache.kafka.common.errors.LeaderNotAvailableException
import org.apache.kafka.common.errors.LogDirNotFoundException
import org.apache.kafka.common.errors.NotLeaderOrFollowerException
import org.apache.kafka.common.errors.OffsetOutOfRangeException
import org.apache.kafka.common.errors.SaslAuthenticationException
import org.apache.kafka.common.errors.SecurityDisabledException
import org.apache.kafka.common.errors.ThrottlingQuotaExceededException
import org.apache.kafka.common.errors.TimeoutException
import org.apache.kafka.common.errors.TopicAuthorizationException
import org.apache.kafka.common.errors.TopicDeletionDisabledException
import org.apache.kafka.common.errors.TopicExistsException
import org.apache.kafka.common.errors.UnknownMemberIdException
import org.apache.kafka.common.errors.UnknownServerException
import org.apache.kafka.common.errors.UnknownTopicIdException
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException
import org.apache.kafka.common.errors.UnsupportedVersionException
import org.apache.kafka.common.feature.Features
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.common.message.AlterPartitionReassignmentsResponseData
import org.apache.kafka.common.message.AlterPartitionReassignmentsResponseData.ReassignablePartitionResponse
import org.apache.kafka.common.message.AlterPartitionReassignmentsResponseData.ReassignableTopicResponse
import org.apache.kafka.common.message.AlterReplicaLogDirsResponseData
import org.apache.kafka.common.message.AlterReplicaLogDirsResponseData.AlterReplicaLogDirPartitionResult
import org.apache.kafka.common.message.AlterReplicaLogDirsResponseData.AlterReplicaLogDirTopicResult
import org.apache.kafka.common.message.AlterUserScramCredentialsResponseData
import org.apache.kafka.common.message.ApiMessageType
import org.apache.kafka.common.message.ApiVersionsResponseData
import org.apache.kafka.common.message.CreateAclsResponseData
import org.apache.kafka.common.message.CreateAclsResponseData.AclCreationResult
import org.apache.kafka.common.message.CreatePartitionsResponseData
import org.apache.kafka.common.message.CreatePartitionsResponseData.CreatePartitionsTopicResult
import org.apache.kafka.common.message.CreateTopicsResponseData
import org.apache.kafka.common.message.CreateTopicsResponseData.CreatableTopicResult
import org.apache.kafka.common.message.CreateTopicsResponseData.CreatableTopicResultCollection
import org.apache.kafka.common.message.DeleteAclsResponseData
import org.apache.kafka.common.message.DeleteAclsResponseData.DeleteAclsFilterResult
import org.apache.kafka.common.message.DeleteAclsResponseData.DeleteAclsMatchingAcl
import org.apache.kafka.common.message.DeleteGroupsResponseData
import org.apache.kafka.common.message.DeleteGroupsResponseData.DeletableGroupResult
import org.apache.kafka.common.message.DeleteGroupsResponseData.DeletableGroupResultCollection
import org.apache.kafka.common.message.DeleteRecordsResponseData
import org.apache.kafka.common.message.DeleteRecordsResponseData.DeleteRecordsPartitionResult
import org.apache.kafka.common.message.DeleteRecordsResponseData.DeleteRecordsPartitionResultCollection
import org.apache.kafka.common.message.DeleteRecordsResponseData.DeleteRecordsTopicResult
import org.apache.kafka.common.message.DeleteRecordsResponseData.DeleteRecordsTopicResultCollection
import org.apache.kafka.common.message.DeleteTopicsResponseData
import org.apache.kafka.common.message.DeleteTopicsResponseData.DeletableTopicResult
import org.apache.kafka.common.message.DeleteTopicsResponseData.DeletableTopicResultCollection
import org.apache.kafka.common.message.DescribeAclsResponseData
import org.apache.kafka.common.message.DescribeClusterResponseData
import org.apache.kafka.common.message.DescribeClusterResponseData.DescribeClusterBroker
import org.apache.kafka.common.message.DescribeConfigsResponseData
import org.apache.kafka.common.message.DescribeGroupsResponseData
import org.apache.kafka.common.message.DescribeGroupsResponseData.DescribedGroupMember
import org.apache.kafka.common.message.DescribeLogDirsResponseData
import org.apache.kafka.common.message.DescribeLogDirsResponseData.DescribeLogDirsPartition
import org.apache.kafka.common.message.DescribeLogDirsResponseData.DescribeLogDirsTopic
import org.apache.kafka.common.message.DescribeProducersResponseData
import org.apache.kafka.common.message.DescribeProducersResponseData.TopicResponse
import org.apache.kafka.common.message.DescribeQuorumResponseData
import org.apache.kafka.common.message.DescribeTransactionsResponseData
import org.apache.kafka.common.message.DescribeUserScramCredentialsResponseData
import org.apache.kafka.common.message.ElectLeadersResponseData.PartitionResult
import org.apache.kafka.common.message.ElectLeadersResponseData.ReplicaElectionResult
import org.apache.kafka.common.message.FindCoordinatorResponseData
import org.apache.kafka.common.message.IncrementalAlterConfigsResponseData
import org.apache.kafka.common.message.IncrementalAlterConfigsResponseData.AlterConfigsResourceResponse
import org.apache.kafka.common.message.InitProducerIdResponseData
import org.apache.kafka.common.message.LeaveGroupRequestData.MemberIdentity
import org.apache.kafka.common.message.LeaveGroupResponseData
import org.apache.kafka.common.message.LeaveGroupResponseData.MemberResponse
import org.apache.kafka.common.message.ListGroupsResponseData
import org.apache.kafka.common.message.ListGroupsResponseData.ListedGroup
import org.apache.kafka.common.message.ListOffsetsResponseData
import org.apache.kafka.common.message.ListPartitionReassignmentsResponseData
import org.apache.kafka.common.message.ListPartitionReassignmentsResponseData.OngoingPartitionReassignment
import org.apache.kafka.common.message.ListPartitionReassignmentsResponseData.OngoingTopicReassignment
import org.apache.kafka.common.message.ListTransactionsResponseData
import org.apache.kafka.common.message.MetadataResponseData
import org.apache.kafka.common.message.MetadataResponseData.MetadataResponseBroker
import org.apache.kafka.common.message.MetadataResponseData.MetadataResponseBrokerCollection
import org.apache.kafka.common.message.MetadataResponseData.MetadataResponsePartition
import org.apache.kafka.common.message.MetadataResponseData.MetadataResponseTopic
import org.apache.kafka.common.message.MetadataResponseData.MetadataResponseTopicCollection
import org.apache.kafka.common.message.OffsetDeleteResponseData
import org.apache.kafka.common.message.OffsetDeleteResponseData.OffsetDeleteResponsePartition
import org.apache.kafka.common.message.OffsetDeleteResponseData.OffsetDeleteResponsePartitionCollection
import org.apache.kafka.common.message.OffsetDeleteResponseData.OffsetDeleteResponseTopic
import org.apache.kafka.common.message.OffsetDeleteResponseData.OffsetDeleteResponseTopicCollection
import org.apache.kafka.common.message.OffsetFetchRequestData.OffsetFetchRequestGroup
import org.apache.kafka.common.message.OffsetFetchRequestData.OffsetFetchRequestTopics
import org.apache.kafka.common.message.UnregisterBrokerResponseData
import org.apache.kafka.common.message.WriteTxnMarkersResponseData
import org.apache.kafka.common.message.WriteTxnMarkersResponseData.WritableTxnMarkerPartitionResult
import org.apache.kafka.common.message.WriteTxnMarkersResponseData.WritableTxnMarkerResult
import org.apache.kafka.common.message.WriteTxnMarkersResponseData.WritableTxnMarkerTopicResult
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.quota.ClientQuotaAlteration
import org.apache.kafka.common.quota.ClientQuotaEntity
import org.apache.kafka.common.quota.ClientQuotaFilter
import org.apache.kafka.common.quota.ClientQuotaFilterComponent
import org.apache.kafka.common.record.RecordVersion
import org.apache.kafka.common.requests.AlterClientQuotasResponse
import org.apache.kafka.common.requests.AlterPartitionReassignmentsResponse
import org.apache.kafka.common.requests.AlterReplicaLogDirsResponse
import org.apache.kafka.common.requests.AlterUserScramCredentialsResponse
import org.apache.kafka.common.requests.ApiError
import org.apache.kafka.common.requests.ApiVersionsRequest
import org.apache.kafka.common.requests.ApiVersionsResponse
import org.apache.kafka.common.requests.CreateAclsResponse
import org.apache.kafka.common.requests.CreatePartitionsRequest
import org.apache.kafka.common.requests.CreatePartitionsResponse
import org.apache.kafka.common.requests.CreateTopicsRequest
import org.apache.kafka.common.requests.CreateTopicsResponse
import org.apache.kafka.common.requests.DeleteAclsResponse
import org.apache.kafka.common.requests.DeleteGroupsResponse
import org.apache.kafka.common.requests.DeleteRecordsResponse
import org.apache.kafka.common.requests.DeleteTopicsRequest
import org.apache.kafka.common.requests.DeleteTopicsResponse
import org.apache.kafka.common.requests.DescribeAclsResponse
import org.apache.kafka.common.requests.DescribeClientQuotasResponse
import org.apache.kafka.common.requests.DescribeClusterRequest
import org.apache.kafka.common.requests.DescribeClusterResponse
import org.apache.kafka.common.requests.DescribeConfigsResponse
import org.apache.kafka.common.requests.DescribeGroupsResponse
import org.apache.kafka.common.requests.DescribeLogDirsResponse
import org.apache.kafka.common.requests.DescribeLogDirsResponse.LogDirInfo
import org.apache.kafka.common.requests.DescribeProducersRequest
import org.apache.kafka.common.requests.DescribeProducersResponse
import org.apache.kafka.common.requests.DescribeQuorumRequest
import org.apache.kafka.common.requests.DescribeQuorumResponse
import org.apache.kafka.common.requests.DescribeTransactionsRequest
import org.apache.kafka.common.requests.DescribeTransactionsResponse
import org.apache.kafka.common.requests.DescribeUserScramCredentialsResponse
import org.apache.kafka.common.requests.ElectLeadersResponse
import org.apache.kafka.common.requests.FindCoordinatorRequest
import org.apache.kafka.common.requests.FindCoordinatorResponse
import org.apache.kafka.common.requests.IncrementalAlterConfigsResponse
import org.apache.kafka.common.requests.InitProducerIdRequest
import org.apache.kafka.common.requests.InitProducerIdResponse
import org.apache.kafka.common.requests.JoinGroupRequest
import org.apache.kafka.common.requests.LeaveGroupRequest
import org.apache.kafka.common.requests.LeaveGroupResponse
import org.apache.kafka.common.requests.ListGroupsRequest
import org.apache.kafka.common.requests.ListGroupsResponse
import org.apache.kafka.common.requests.ListOffsetsRequest
import org.apache.kafka.common.requests.ListOffsetsResponse
import org.apache.kafka.common.requests.ListPartitionReassignmentsResponse
import org.apache.kafka.common.requests.ListTransactionsRequest
import org.apache.kafka.common.requests.ListTransactionsResponse
import org.apache.kafka.common.requests.MetadataRequest
import org.apache.kafka.common.requests.MetadataResponse
import org.apache.kafka.common.requests.MetadataResponse.PartitionMetadata
import org.apache.kafka.common.requests.OffsetCommitResponse
import org.apache.kafka.common.requests.OffsetDeleteResponse
import org.apache.kafka.common.requests.OffsetFetchRequest
import org.apache.kafka.common.requests.OffsetFetchResponse
import org.apache.kafka.common.requests.RequestTestUtils
import org.apache.kafka.common.requests.UnregisterBrokerResponse
import org.apache.kafka.common.requests.UpdateFeaturesRequest
import org.apache.kafka.common.requests.UpdateFeaturesResponse
import org.apache.kafka.common.requests.WriteTxnMarkersRequest
import org.apache.kafka.common.requests.WriteTxnMarkersResponse
import org.apache.kafka.common.resource.PatternType
import org.apache.kafka.common.resource.ResourcePattern
import org.apache.kafka.common.resource.ResourcePatternFilter
import org.apache.kafka.common.resource.ResourceType
import org.apache.kafka.common.utils.Bytes
import org.apache.kafka.common.utils.MockTime
import org.apache.kafka.common.utils.Time
import org.apache.kafka.common.utils.Utils.stackTrace
import org.apache.kafka.test.MockMetricsReporter
import org.apache.kafka.test.TestUtils
import org.apache.kafka.test.TestUtils.assertFutureError
import org.apache.kafka.test.TestUtils.assertFutureThrows
import org.apache.kafka.test.TestUtils.waitForCondition
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import org.slf4j.LoggerFactory
import java.net.InetSocketAddress
import java.util.LinkedList
import java.util.Properties
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CountDownLatch
import java.util.concurrent.ExecutionException
import java.util.concurrent.Semaphore
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong
import org.apache.kafka.clients.ClientRequest
import org.apache.kafka.test.TestUtils.assertNotFails
import kotlin.test.assertContains
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertNotEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlin.test.fail

/**
 * A unit test for KafkaAdminClient.
 *
 * See AdminClientIntegrationTest for an integration test.
 */
@Timeout(120)
@Suppress("LargeClass", "LongParameterList", "LongMethod")
class KafkaAdminClientTest {

    @Test
    fun testDefaultApiTimeoutAndRequestTimeoutConflicts() {
        val config = newConfMap(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "500")
        val exception = assertFailsWith<KafkaException> { KafkaAdminClient.createInternal(config) }
        assertTrue(exception.cause is ConfigException)
    }

    @Test
    @Deprecated("getOrCreateListValue is replaced with computeIfAbsent")
    fun testGetOrCreateListValue() {
        val map = mutableMapOf<String, MutableList<String>>()
        val fooList: MutableList<String> = map.computeIfAbsent("foo") { LinkedList() }
        assertNotNull(fooList)
        fooList.add("a")
        fooList.add("b")
        val fooList2: List<String> = map.computeIfAbsent("foo") { LinkedList() }
        assertEquals(fooList, fooList2)
        assertTrue(fooList2.contains("a"))
        assertTrue(fooList2.contains("b"))
        val barList: List<String> = map.computeIfAbsent("bar") { LinkedList() }
        assertNotNull(barList)
        assertTrue(barList.isEmpty())
    }

    @Test
    fun testCalcTimeoutMsRemainingAsInt() {
        assertEquals(
            expected = 0,
            actual = KafkaAdminClient.calcTimeoutMsRemainingAsInt(now = 1000, deadlineMs = 1000)
        )
        assertEquals(
            expected = 100,
            actual = KafkaAdminClient.calcTimeoutMsRemainingAsInt(now = 1000, deadlineMs = 1100)
        )
        assertEquals(
            expected = Int.MAX_VALUE,
            actual = KafkaAdminClient.calcTimeoutMsRemainingAsInt(
                now = 0,
                deadlineMs = Long.MAX_VALUE
            )
        )
        assertEquals(
            expected = Int.MIN_VALUE,
            actual = KafkaAdminClient.calcTimeoutMsRemainingAsInt(
                now = Long.MAX_VALUE,
                deadlineMs = 0
            )
        )
    }

    @Test
    fun testPrettyPrintException() {
        assertEquals(
            expected = "Null exception.",
            actual = KafkaAdminClient.prettyPrintException(null),
        )
        assertEquals(
            expected = "TimeoutException",
            actual = KafkaAdminClient.prettyPrintException(TimeoutException()),
        )
        assertEquals(
            expected = "TimeoutException: The foobar timed out.",
            actual = KafkaAdminClient.prettyPrintException(TimeoutException("The foobar timed out.")),
        )
    }

    @Test
    fun testGenerateClientId() {
        val ids: MutableSet<String> = HashSet()
        for (i in 0..9) {
            val id = KafkaAdminClient.generateClientId(
                newConfMap(AdminClientConfig.CLIENT_ID_CONFIG, "")
            )
            assertFalse(ids.contains(id), "Got duplicate id $id")
            ids.add(id)
        }
        assertEquals(
            expected = "myCustomId",
            actual = KafkaAdminClient.generateClientId(
                newConfMap(AdminClientConfig.CLIENT_ID_CONFIG, "myCustomId")
            )
        )
    }

    @Test
    fun testMetricsReporterAutoGeneratedClientId() {
        val props = Properties()
        props.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999")
        props.setProperty(
            AdminClientConfig.METRIC_REPORTER_CLASSES_CONFIG,
            MockMetricsReporter::class.java.getName()
        )
        val admin = AdminClient.create(props) as KafkaAdminClient
        val mockMetricsReporter = admin.metrics.reporters[0] as MockMetricsReporter
        assertEquals(expected = admin.clientId, actual = mockMetricsReporter.clientId)
        assertEquals(expected = 2, actual = admin.metrics.reporters.size)
        admin.close()
    }

    @Test
    @Suppress("Deprecation")
    fun testDisableJmxReporter() {
        val props = Properties()
        props.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999")
        props.setProperty(AdminClientConfig.AUTO_INCLUDE_JMX_REPORTER_CONFIG, "false")
        val admin = AdminClient.create(props) as KafkaAdminClient
        assertTrue(admin.metrics.reporters.isEmpty())
        admin.close()
    }

    @Test
    fun testExplicitlyEnableJmxReporter() {
        val props = Properties()
        props.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999")
        props.setProperty(
            AdminClientConfig.METRIC_REPORTER_CLASSES_CONFIG,
            "org.apache.kafka.common.metrics.JmxReporter"
        )
        val admin = AdminClient.create(props) as KafkaAdminClient
        assertEquals(expected = 1, actual = admin.metrics.reporters.size)
        admin.close()
    }

    @Test
    fun testCloseAdminClient() {
        mockClientEnv().use { }
    }

    /**
     * Test if admin client can be closed in the callback invoked when
     * an api call completes. If calling [Admin.close] in callback, AdminClient thread hangs
     */
    @Test
    @Timeout(10)
    @Throws(InterruptedException::class)
    fun testCloseAdminClientInCallback() {
        val time = MockTime()
        val env = AdminClientUnitTestEnv(time, mockCluster(numNodes = 3, controllerIndex = 0))
        val result = env.adminClient.listTopics(ListTopicsOptions().apply { timeoutMs = 1000 })
        val kafkaFuture = result.listings
        val callbackCalled = Semaphore(0)
        kafkaFuture.whenComplete { _, _ ->
            env.close()
            callbackCalled.release()
        }
        time.sleep(2000) // Advance time to timeout and complete listTopics request
        callbackCalled.acquire()
    }

    /**
     * Test that the client properly times out when we don't receive any metadata.
     */
    @Test
    @Throws(Exception::class)
    fun testTimeoutWithoutMetadata() {
        AdminClientUnitTestEnv(
            Time.SYSTEM,
            mockBootstrapCluster(),
            newStrMap(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "10"),
        ).use { env ->
            env.mockClient.setNodeApiVersions(NodeApiVersions.create())
            env.mockClient.prepareResponse(
                response = prepareCreateTopicsResponse("myTopic", Errors.NONE),
            )
            val future = env.adminClient.createTopics(
                newTopics = setOf(
                    NewTopic(
                        name = "myTopic",
                        replicasAssignments = mapOf(0 to mutableListOf(0, 1, 2)),
                    ),
                ),
                options = CreateTopicsOptions().apply { timeoutMs = 1000 },
            ).all()
            assertFutureError(
                future,
                TimeoutException::class.java
            )
        }
    }

    @Test
    @Throws(Exception::class)
    fun testConnectionFailureOnMetadataUpdate() {
        // This tests the scenario in which we successfully connect to the bootstrap server, but
        // the server disconnects before sending the full response
        val cluster = mockBootstrapCluster()
        AdminClientUnitTestEnv(Time.SYSTEM, cluster).use { env ->
            val discoveredCluster = mockCluster(numNodes = 3, controllerIndex = 0)
            env.mockClient.setNodeApiVersions(NodeApiVersions.create())
            env.mockClient.prepareResponse(
                matcher = { request -> request is MetadataRequest },
                response = null,
                disconnected = true,
            )
            env.mockClient.prepareResponse(
                matcher = { request -> request is MetadataRequest },
                response = RequestTestUtils.metadataResponse(
                    brokers = discoveredCluster.nodes,
                    clusterId = discoveredCluster.clusterResource.clusterId,
                    controllerId = 1,
                    topicMetadataList = emptyList(),
                ),
            )
            env.mockClient.prepareResponse(
                matcher = { body -> body is CreateTopicsRequest },
                response = prepareCreateTopicsResponse("myTopic", Errors.NONE),
            )
            val future = env.adminClient.createTopics(
                newTopics = setOf(
                    NewTopic(
                        name = "myTopic",
                        replicasAssignments = mapOf(0 to mutableListOf(0, 1, 2)),
                    ),
                ),
                options = CreateTopicsOptions().apply { timeoutMs = 10000 },
            ).all()
            future.get()
        }
    }

    @Test
    @Throws(Exception::class)
    fun testUnreachableBootstrapServer() {
        // This tests the scenario in which the bootstrap server is unreachable for a short while,
        // which prevents AdminClient from being able to send the initial metadata request
        val cluster = Cluster.bootstrap(listOf(InetSocketAddress("localhost", 8121)))
        val unreachableNodes = mapOf(cluster.nodes[0] to 200L)
        AdminClientUnitTestEnv(
            Time.SYSTEM,
            cluster,
            AdminClientUnitTestEnv.clientConfigs(),
            unreachableNodes,
        ).use { env ->
            val discoveredCluster: Cluster = mockCluster(3, 0)
            env.mockClient.setNodeApiVersions(NodeApiVersions.create())
            env.mockClient.prepareResponse(
                matcher = { body -> body is MetadataRequest },
                response = RequestTestUtils.metadataResponse(
                    brokers = discoveredCluster.nodes,
                    clusterId = discoveredCluster.clusterResource.clusterId,
                    controllerId = 1,
                    topicMetadataList = emptyList()
                )
            )
            env.mockClient.prepareResponse(
                matcher = { body -> body is CreateTopicsRequest },
                response = prepareCreateTopicsResponse("myTopic", Errors.NONE),
            )
            val future = env.adminClient.createTopics(
                newTopics = setOf(
                    NewTopic(
                        name = "myTopic",
                        replicasAssignments = mapOf(0 to mutableListOf(0, 1, 2)),
                    ),
                ),
                options = CreateTopicsOptions().apply { timeoutMs = 10000 },
            ).all()
            future.get()
        }
    }

    /**
     * Test that we propagate exceptions encountered when fetching metadata.
     */
    @Test
    @Throws(Exception::class)
    fun testPropagatedMetadataFetchException() {
        AdminClientUnitTestEnv(
            Time.SYSTEM,
            mockCluster(3, 0),
            newStrMap(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:8121",
                AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "10",
            )
        ).use { env ->
            env.mockClient.setNodeApiVersions(NodeApiVersions.create())
            env.mockClient.createPendingAuthenticationError(
                env.cluster.nodeById(0)!!,
                TimeUnit.DAYS.toMillis(1)
            )
            env.mockClient
                .prepareResponse(
                    response = prepareCreateTopicsResponse("myTopic", Errors.NONE),
                )
            val future = env.adminClient.createTopics(
                newTopics = setOf(
                    NewTopic(
                        name = "myTopic",
                        replicasAssignments = mapOf(0 to mutableListOf(0, 1, 2)),
                    )
                ),
                options = CreateTopicsOptions().apply { timeoutMs = 1000 },
            ).all()
            assertFutureError(
                future = future,
                exceptionClass = SaslAuthenticationException::class.java,
            )
        }
    }

    @Test
    @Throws(Exception::class)
    fun testCreateTopics() {
        mockClientEnv().use { env ->
            env.mockClient.setNodeApiVersions(NodeApiVersions.create())
            env.mockClient.prepareResponse(
                matcher = expectCreateTopicsRequestWithTopics("myTopic"),
                response = prepareCreateTopicsResponse("myTopic", Errors.NONE),
            )
            val future = env.adminClient.createTopics(
                newTopics = setOf(
                    NewTopic(
                        name = "myTopic",
                        replicasAssignments = mapOf(0 to mutableListOf(0, 1, 2)),
                    )
                ),
                options = CreateTopicsOptions().apply { timeoutMs = 10000 },
            ).all()
            future.get()
        }
    }

    @Test
    @Throws(Exception::class)
    fun testCreateTopicsPartialResponse() {
        mockClientEnv().use { env ->
            env.mockClient.setNodeApiVersions(NodeApiVersions.create())
            env.mockClient.prepareResponse(
                matcher = expectCreateTopicsRequestWithTopics("myTopic", "myTopic2"),
                response = prepareCreateTopicsResponse("myTopic", Errors.NONE),
            )
            val topicsResult = env.adminClient.createTopics(
                newTopics = listOf(
                    NewTopic(
                        name = "myTopic",
                        replicasAssignments = mapOf(0 to mutableListOf(0, 1, 2)),
                    ),
                    NewTopic(
                        name = "myTopic2",
                        replicasAssignments = mapOf(0 to mutableListOf(0, 1, 2)),
                    )
                ),
                options = CreateTopicsOptions().apply { timeoutMs = 10000 },
            )
            topicsResult.values()["myTopic"]!!.get()
            assertFutureThrows(
                future = topicsResult.values()["myTopic2"]!!,
                exceptionCauseClass = ApiException::class.java,
            )
        }
    }

    @Test
    @Throws(Exception::class)
    fun testCreateTopicsRetryBackoff() {
        val time = MockTime()
        val retryBackoff = 100
        AdminClientUnitTestEnv(
            time,
            mockCluster(3, 0),
            newStrMap(AdminClientConfig.RETRY_BACKOFF_MS_CONFIG, "" + retryBackoff)
        ).use { env ->
            val mockClient = env.mockClient
            mockClient.setNodeApiVersions(NodeApiVersions.create())
            val firstAttemptTime = AtomicLong(0)
            val secondAttemptTime = AtomicLong(0)
            mockClient.prepareResponse(
                matcher = { body ->
                    firstAttemptTime.set(time.milliseconds())
                    body is CreateTopicsRequest
                },
                response = null,
                disconnected = true,
            )
            mockClient.prepareResponse(
                matcher = { body ->
                    secondAttemptTime.set(time.milliseconds())
                    body is CreateTopicsRequest
                },
                response = prepareCreateTopicsResponse("myTopic", Errors.NONE),
            )
            val future = env.adminClient.createTopics(
                newTopics = setOf(
                    NewTopic(
                        name = "myTopic",
                        replicasAssignments = mapOf(0 to mutableListOf(0, 1, 2)),
                    )
                ),
                options = CreateTopicsOptions().apply { timeoutMs = 10000 },
            ).all()

            // Wait until the first attempt has failed, then advance the time
            waitForCondition(
                testCondition = { mockClient.numAwaitingResponses() == 1 },
                conditionDetails = "Failed awaiting CreateTopics first request failure",
            )

            // Wait until the retry call added to the queue in AdminClient
            waitForCondition(
                testCondition = { env.adminClient.numPendingCalls() == 1 },
                conditionDetails = "Failed to add retry CreateTopics call",
            )
            time.sleep(retryBackoff.toLong())
            future.get()
            val actualRetryBackoff = secondAttemptTime.get() - firstAttemptTime.get()
            assertEquals(
                expected = retryBackoff.toLong(),
                actual = actualRetryBackoff,
                message = "CreateTopics retry did not await expected backoff",
            )
        }
    }

    @Test
    @Throws(Exception::class)
    fun testCreateTopicsHandleNotControllerException() {
        mockClientEnv().use { env ->
            env.mockClient.setNodeApiVersions(NodeApiVersions.create())
            env.mockClient.prepareResponseFrom(
                response = prepareCreateTopicsResponse("myTopic", Errors.NOT_CONTROLLER),
                node = env.cluster.nodeById(0),
            )
            env.mockClient.prepareResponse(
                response = RequestTestUtils.metadataResponse(
                    brokers = env.cluster.nodes,
                    clusterId = env.cluster.clusterResource.clusterId,
                    controllerId = 1,
                    topicMetadataList = emptyList(),
                ),
            )
            env.mockClient.prepareResponseFrom(
                response = prepareCreateTopicsResponse("myTopic", Errors.NONE),
                node = env.cluster.nodeById(1),
            )
            val future = env.adminClient.createTopics(
                newTopics = setOf(
                    NewTopic(
                        name = "myTopic",
                        replicasAssignments = mapOf(0 to mutableListOf(0, 1, 2)),
                    )
                ),
                options = CreateTopicsOptions().apply { timeoutMs = 10000 }
            ).all()
            future.get()
        }
    }

    @Test
    @Throws(Exception::class)
    fun testCreateTopicsRetryThrottlingExceptionWhenEnabled() {
        mockClientEnv().use { env ->
            env.mockClient.setNodeApiVersions(NodeApiVersions.create())
            env.mockClient.prepareResponse(
                matcher = expectCreateTopicsRequestWithTopics("topic1", "topic2", "topic3"),
                response = prepareCreateTopicsResponse(
                    throttleTimeMs = 1000,
                    creatableTopicResult("topic1", Errors.NONE),
                    creatableTopicResult("topic2", Errors.THROTTLING_QUOTA_EXCEEDED),
                    creatableTopicResult("topic3", Errors.TOPIC_ALREADY_EXISTS),
                ),
            )
            env.mockClient.prepareResponse(
                matcher = expectCreateTopicsRequestWithTopics("topic2"),
                response = prepareCreateTopicsResponse(
                    throttleTimeMs = 1000,
                    creatableTopicResult("topic2", Errors.THROTTLING_QUOTA_EXCEEDED),
                )
            )
            env.mockClient.prepareResponse(
                matcher = expectCreateTopicsRequestWithTopics("topic2"),
                response = prepareCreateTopicsResponse(
                    throttleTimeMs = 0,
                    creatableTopicResult("topic2", Errors.NONE),
                )
            )
            val result = env.adminClient.createTopics(
                listOf(
                    NewTopic("topic1", 1, 1.toShort()),
                    NewTopic("topic2", 1, 1.toShort()),
                    NewTopic("topic3", 1, 1.toShort()),
                ),
                CreateTopicsOptions().apply { retryOnQuotaViolation = true }
            )
            assertNotFails(result.values()["topic1"]!!::get)
            assertNotFails(result.values()["topic2"]!!::get)
            assertFutureThrows(
                future = (result.values()["topic3"])!!,
                exceptionCauseClass = TopicExistsException::class.java,
            )
        }
    }

    @Test
    @Throws(Exception::class)
    fun testCreateTopicsRetryThrottlingExceptionWhenEnabledUntilRequestTimeOut() {
        val defaultApiTimeout = 60000
        val time = MockTime()
        mockClientEnv(
            time,
            AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG,
            defaultApiTimeout.toString()
        ).use { env ->
            env.mockClient.setNodeApiVersions(NodeApiVersions.create())
            env.mockClient.prepareResponse(
                matcher = expectCreateTopicsRequestWithTopics("topic1", "topic2", "topic3"),
                response = prepareCreateTopicsResponse(
                    throttleTimeMs = 1000,
                    creatableTopicResult("topic1", Errors.NONE),
                    creatableTopicResult("topic2", Errors.THROTTLING_QUOTA_EXCEEDED),
                    creatableTopicResult("topic3", Errors.TOPIC_ALREADY_EXISTS),
                )
            )
            env.mockClient.prepareResponse(
                matcher = expectCreateTopicsRequestWithTopics("topic2"),
                response = prepareCreateTopicsResponse(
                    throttleTimeMs = 1000,
                    creatableTopicResult("topic2", Errors.THROTTLING_QUOTA_EXCEEDED),
                )
            )
            val result: CreateTopicsResult = env.adminClient.createTopics(
                listOf(
                    NewTopic("topic1", 1, 1.toShort()),
                    NewTopic("topic2", 1, 1.toShort()),
                    NewTopic("topic3", 1, 1.toShort()),
                ),
                CreateTopicsOptions().apply { retryOnQuotaViolation = true }
            )

            // Wait until the prepared attempts have consumed
            waitForCondition(
                testCondition = { env.mockClient.numAwaitingResponses() == 0 },
                conditionDetails = "Failed awaiting CreateTopics requests",
            )

            // Wait until the next request is sent out
            waitForCondition(
                testCondition = { env.mockClient.inFlightRequestCount() == 1 },
                conditionDetails = "Failed awaiting next CreateTopics request",
            )

            // Advance time past the default api timeout to time out the inflight request
            time.sleep(defaultApiTimeout.toLong() + 1)
            assertNotFails(result.values()["topic1"]!!::get)
            val e: ThrottlingQuotaExceededException =
                assertFutureThrows(
                    future = (result.values()["topic2"])!!,
                    exceptionCauseClass = ThrottlingQuotaExceededException::class.java
                )
            assertEquals(0, e.throttleTimeMs)
            assertFutureThrows(
                future = (result.values()["topic3"])!!,
                exceptionCauseClass = TopicExistsException::class.java
            )
        }
    }

    @Test
    @Throws(Exception::class)
    fun testCreateTopicsDontRetryThrottlingExceptionWhenDisabled() {
        mockClientEnv().use { env ->
            env.mockClient.setNodeApiVersions(NodeApiVersions.create())
            env.mockClient.prepareResponse(
                matcher = expectCreateTopicsRequestWithTopics("topic1", "topic2", "topic3"),
                response = prepareCreateTopicsResponse(
                    throttleTimeMs = 1000,
                    creatableTopicResult("topic1", Errors.NONE),
                    creatableTopicResult("topic2", Errors.THROTTLING_QUOTA_EXCEEDED),
                    creatableTopicResult("topic3", Errors.TOPIC_ALREADY_EXISTS),
                )
            )
            val result: CreateTopicsResult = env.adminClient.createTopics(
                listOf(
                    NewTopic("topic1", 1, 1.toShort()),
                    NewTopic("topic2", 1, 1.toShort()),
                    NewTopic("topic3", 1, 1.toShort())
                ),
                CreateTopicsOptions().apply { retryOnQuotaViolation = false }
            )
            assertNotFails(result.values()["topic1"]!!::get)
            val e = assertFutureThrows(
                future = result.values()["topic2"]!!,
                exceptionCauseClass = ThrottlingQuotaExceededException::class.java,
            )
            assertEquals(1000, e.throttleTimeMs)
            assertFutureThrows(
                future = result.values()["topic3"]!!,
                exceptionCauseClass = TopicExistsException::class.java,
            )
        }
    }

    private fun expectCreateTopicsRequestWithTopics(vararg topics: String): RequestMatcher {
        return RequestMatcher { body ->
            if (body is CreateTopicsRequest) {
                val request: CreateTopicsRequest = body
                for (topic in topics) {
                    if (request.data().topics.find(topic) == null) return@RequestMatcher false
                }
                return@RequestMatcher topics.size == request.data().topics.size
            }
            false
        }
    }

    @Test
    @Throws(Exception::class)
    fun testDeleteTopics() {
        mockClientEnv().use { env ->
            env.mockClient.setNodeApiVersions(NodeApiVersions.create())
            env.mockClient.prepareResponse(
                matcher = expectDeleteTopicsRequestWithTopics("myTopic"),
                response = prepareDeleteTopicsResponse("myTopic", Errors.NONE),
            )
            var future = env.adminClient.deleteTopics(
                topics = listOf("myTopic"),
                options = DeleteTopicsOptions(),
            ).all()
            assertNotFails(future::get)
            env.mockClient.prepareResponse(
                matcher = expectDeleteTopicsRequestWithTopics("myTopic"),
                response = prepareDeleteTopicsResponse(
                    topicName = "myTopic",
                    error = Errors.TOPIC_DELETION_DISABLED,
                )
            )
            future = env.adminClient.deleteTopics(
                topics = listOf("myTopic"),
                options = DeleteTopicsOptions(),
            ).all()
            assertFutureError(
                future = future,
                exceptionClass = TopicDeletionDisabledException::class.java
            )
            env.mockClient.prepareResponse(
                matcher = expectDeleteTopicsRequestWithTopics("myTopic"),
                response = prepareDeleteTopicsResponse(
                    topicName = "myTopic",
                    error = Errors.UNKNOWN_TOPIC_OR_PARTITION,
                ),
            )
            future = env.adminClient.deleteTopics(
                topics = listOf("myTopic"),
                options = DeleteTopicsOptions(),
            ).all()
            assertFutureError(
                future = future,
                exceptionClass = UnknownTopicOrPartitionException::class.java,
            )

            // With topic IDs
            val topicId: Uuid = Uuid.randomUuid()
            env.mockClient.prepareResponse(
                matcher = expectDeleteTopicsRequestWithTopicIds(topicId),
                response = prepareDeleteTopicsResponseWithTopicId(topicId, Errors.NONE),
            )
            future = env.adminClient.deleteTopics(
                topics = TopicCollection.ofTopicIds(listOf(topicId)),
                options = DeleteTopicsOptions(),
            ).all()
            assertNotFails(future::get)
            env.mockClient.prepareResponse(
                matcher = expectDeleteTopicsRequestWithTopicIds(topicId),
                response = prepareDeleteTopicsResponseWithTopicId(
                    id = topicId,
                    error = Errors.TOPIC_DELETION_DISABLED,
                ),
            )
            future = env.adminClient.deleteTopics(
                topics = TopicCollection.ofTopicIds(listOf(topicId)),
                options = DeleteTopicsOptions(),
            ).all()
            assertFutureError(
                future = future,
                exceptionClass = TopicDeletionDisabledException::class.java,
            )
            env.mockClient.prepareResponse(
                matcher = expectDeleteTopicsRequestWithTopicIds(topicId),
                response = prepareDeleteTopicsResponseWithTopicId(
                    id = topicId,
                    error = Errors.UNKNOWN_TOPIC_ID,
                ),
            )
            future = env.adminClient.deleteTopics(
                topics = TopicCollection.ofTopicIds(listOf(topicId)),
                options = DeleteTopicsOptions(),
            ).all()
            assertFutureError(
                future = future,
                exceptionClass = UnknownTopicIdException::class.java,
            )
        }
    }

    @Test
    @Throws(Exception::class)
    fun testDeleteTopicsPartialResponse() {
        mockClientEnv().use { env ->
            env.mockClient.setNodeApiVersions(NodeApiVersions.create())
            env.mockClient.prepareResponse(
                matcher = expectDeleteTopicsRequestWithTopics("myTopic", "myOtherTopic"),
                response = prepareDeleteTopicsResponse(
                    throttleTimeMs = 1000,
                    deletableTopicResult("myTopic", Errors.NONE),
                ),
            )
            val result = env.adminClient.deleteTopics(
                topics = mutableListOf("myTopic", "myOtherTopic"),
                options = DeleteTopicsOptions(),
            )
            result.nameFutures!!["myTopic"]!!.get()
            assertFutureThrows(
                future = result.nameFutures!!["myOtherTopic"]!!,
                exceptionCauseClass = ApiException::class.java,
            )

            // With topic IDs
            val topicId1 = Uuid.randomUuid()
            val topicId2 = Uuid.randomUuid()
            env.mockClient.prepareResponse(
                matcher = expectDeleteTopicsRequestWithTopicIds(topicId1, topicId2),
                response = prepareDeleteTopicsResponse(
                    throttleTimeMs = 1000,
                    deletableTopicResultWithId(topicId1, Errors.NONE),
                )
            )
            val resultIds = env.adminClient.deleteTopics(
                topics = TopicCollection.ofTopicIds(listOf(topicId1, topicId2)),
                options = DeleteTopicsOptions()
            )
            resultIds.topicIdFutures!![topicId1]!!.get()
            assertFutureThrows(
                future = resultIds.topicIdFutures!![topicId2]!!,
                exceptionCauseClass = ApiException::class.java,
            )
        }
    }

    @Test
    @Throws(Exception::class)
    fun testDeleteTopicsRetryThrottlingExceptionWhenEnabled() {
        mockClientEnv().use { env ->
            env.mockClient.setNodeApiVersions(NodeApiVersions.create())
            env.mockClient.prepareResponse(
                matcher = expectDeleteTopicsRequestWithTopics("topic1", "topic2", "topic3"),
                response = prepareDeleteTopicsResponse(
                    throttleTimeMs = 1000,
                    deletableTopicResult("topic1", Errors.NONE),
                    deletableTopicResult("topic2", Errors.THROTTLING_QUOTA_EXCEEDED),
                    deletableTopicResult("topic3", Errors.TOPIC_ALREADY_EXISTS),
                ),
            )
            env.mockClient.prepareResponse(
                matcher = expectDeleteTopicsRequestWithTopics("topic2"),
                response = prepareDeleteTopicsResponse(
                    throttleTimeMs = 1000,
                    deletableTopicResult("topic2", Errors.THROTTLING_QUOTA_EXCEEDED),
                ),
            )
            env.mockClient.prepareResponse(
                matcher = expectDeleteTopicsRequestWithTopics("topic2"),
                response = prepareDeleteTopicsResponse(
                    throttleTimeMs = 0,
                    deletableTopicResult("topic2", Errors.NONE),
                )
            )
            val result = env.adminClient.deleteTopics(
                topics = mutableListOf("topic1", "topic2", "topic3"),
                options = DeleteTopicsOptions().apply { retryOnQuotaViolation = true }
            )
            assertNotFails(result.nameFutures!!["topic1"]!!::get)
            assertNotFails(result.nameFutures!!["topic2"]!!::get)
            assertFutureThrows(
                future = result.nameFutures!!["topic3"]!!,
                exceptionCauseClass = TopicExistsException::class.java,
            )

            // With topic IDs
            val topicId1 = Uuid.randomUuid()
            val topicId2 = Uuid.randomUuid()
            val topicId3 = Uuid.randomUuid()
            env.mockClient.prepareResponse(
                matcher = expectDeleteTopicsRequestWithTopicIds(topicId1, topicId2, topicId3),
                response = prepareDeleteTopicsResponse(
                    throttleTimeMs = 1000,
                    deletableTopicResultWithId(topicId1, Errors.NONE),
                    deletableTopicResultWithId(topicId2, Errors.THROTTLING_QUOTA_EXCEEDED),
                    deletableTopicResultWithId(topicId3, Errors.UNKNOWN_TOPIC_ID),
                ),
            )
            env.mockClient.prepareResponse(
                matcher = expectDeleteTopicsRequestWithTopicIds(topicId2),
                response = prepareDeleteTopicsResponse(
                    throttleTimeMs = 1000,
                    deletableTopicResultWithId(topicId2, Errors.THROTTLING_QUOTA_EXCEEDED),
                ),
            )
            env.mockClient.prepareResponse(
                matcher = expectDeleteTopicsRequestWithTopicIds(topicId2),
                response = prepareDeleteTopicsResponse(
                    throttleTimeMs = 0,
                    deletableTopicResultWithId(topicId2, Errors.NONE),
                ),
            )
            val resultIds = env.adminClient.deleteTopics(
                topics = TopicCollection.ofTopicIds(listOf(topicId1, topicId2, topicId3)),
                options = DeleteTopicsOptions().apply { retryOnQuotaViolation = true },
            )
            assertNotFails(resultIds.topicIdFutures!![topicId1]!!::get)
            assertNotFails(resultIds.topicIdFutures!![topicId2]!!::get)
            assertFutureThrows(
                future = resultIds.topicIdFutures!![topicId3]!!,
                exceptionCauseClass = UnknownTopicIdException::class.java,
            )
        }
    }

    @Test
    @Throws(Exception::class)
    fun testDeleteTopicsRetryThrottlingExceptionWhenEnabledUntilRequestTimeOut() {
        val defaultApiTimeout = 60000
        val time = MockTime()
        mockClientEnv(
            time,
            AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG,
            defaultApiTimeout.toString(),
        ).use { env ->
            env.mockClient.setNodeApiVersions(NodeApiVersions.create())
            env.mockClient.prepareResponse(
                matcher = expectDeleteTopicsRequestWithTopics("topic1", "topic2", "topic3"),
                response = prepareDeleteTopicsResponse(
                    throttleTimeMs = 1000,
                    deletableTopicResult("topic1", Errors.NONE),
                    deletableTopicResult("topic2", Errors.THROTTLING_QUOTA_EXCEEDED),
                    deletableTopicResult("topic3", Errors.TOPIC_ALREADY_EXISTS),
                ),
            )
            env.mockClient.prepareResponse(
                matcher = expectDeleteTopicsRequestWithTopics("topic2"),
                response = prepareDeleteTopicsResponse(
                    throttleTimeMs = 1000,
                    deletableTopicResult("topic2", Errors.THROTTLING_QUOTA_EXCEEDED),
                ),
            )
            val result = env.adminClient.deleteTopics(
                mutableListOf("topic1", "topic2", "topic3"),
                DeleteTopicsOptions().apply { retryOnQuotaViolation = true }
            )

            // Wait until the prepared attempts have consumed
            waitForCondition(
                testCondition = { env.mockClient.numAwaitingResponses() == 0 },
                conditionDetails = "Failed awaiting DeleteTopics requests",
            )

            // Wait until the next request is sent out
            waitForCondition(
                testCondition = { env.mockClient.inFlightRequestCount() == 1 },
                conditionDetails = "Failed awaiting next DeleteTopics request",
            )

            // Advance time past the default api timeout to time out the inflight request
            time.sleep(defaultApiTimeout.toLong() + 1)
            assertNotFails(result.nameFutures!!["topic1"]!!::get)
            var e = assertFutureThrows(
                future = result.nameFutures!!["topic2"]!!,
                exceptionCauseClass = ThrottlingQuotaExceededException::class.java
            )
            assertEquals(expected = 0, actual = e.throttleTimeMs)
            assertFutureThrows(
                future = result.nameFutures!!["topic3"]!!,
                exceptionCauseClass = TopicExistsException::class.java,
            )

            // With topic IDs
            val topicId1 = Uuid.randomUuid()
            val topicId2 = Uuid.randomUuid()
            val topicId3 = Uuid.randomUuid()
            env.mockClient.prepareResponse(
                matcher = expectDeleteTopicsRequestWithTopicIds(topicId1, topicId2, topicId3),
                response = prepareDeleteTopicsResponse(
                    throttleTimeMs = 1000,
                    deletableTopicResultWithId(topicId1, Errors.NONE),
                    deletableTopicResultWithId(topicId2, Errors.THROTTLING_QUOTA_EXCEEDED),
                    deletableTopicResultWithId(topicId3, Errors.UNKNOWN_TOPIC_ID),
                ),
            )
            env.mockClient.prepareResponse(
                matcher = expectDeleteTopicsRequestWithTopicIds(topicId2),
                response = prepareDeleteTopicsResponse(
                    throttleTimeMs = 1000,
                    deletableTopicResultWithId(topicId2, Errors.THROTTLING_QUOTA_EXCEEDED),
                ),
            )
            val resultIds = env.adminClient.deleteTopics(
                topics = TopicCollection.ofTopicIds(listOf(topicId1, topicId2, topicId3)),
                options = DeleteTopicsOptions().apply { retryOnQuotaViolation = true },
            )

            // Wait until the prepared attempts have consumed
            waitForCondition(
                testCondition = { env.mockClient.numAwaitingResponses() == 0 },
                conditionDetails = "Failed awaiting DeleteTopics requests",
            )

            // Wait until the next request is sent out
            waitForCondition(
                testCondition = { env.mockClient.inFlightRequestCount() == 1 },
                conditionDetails = "Failed awaiting next DeleteTopics request",
            )

            // Advance time past the default api timeout to time out the inflight request
            time.sleep(defaultApiTimeout.toLong() + 1)
            assertNotFails(resultIds.topicIdFutures!![topicId1]!!::get)
            e = assertFutureThrows(
                future = resultIds.topicIdFutures!![topicId2]!!,
                exceptionCauseClass = ThrottlingQuotaExceededException::class.java,
            )
            assertEquals(0, e.throttleTimeMs)
            assertFutureThrows(
                future = resultIds.topicIdFutures!![topicId3]!!,
                exceptionCauseClass = UnknownTopicIdException::class.java,
            )
        }
    }

    @Test
    @Throws(Exception::class)
    fun testDeleteTopicsDontRetryThrottlingExceptionWhenDisabled() {
        mockClientEnv().use { env ->
            env.mockClient.setNodeApiVersions(NodeApiVersions.create())
            env.mockClient.prepareResponse(
                matcher = expectDeleteTopicsRequestWithTopics("topic1", "topic2", "topic3"),
                response = prepareDeleteTopicsResponse(
                    throttleTimeMs = 1000,
                    deletableTopicResult("topic1", Errors.NONE),
                    deletableTopicResult("topic2", Errors.THROTTLING_QUOTA_EXCEEDED),
                    deletableTopicResult("topic3", Errors.TOPIC_ALREADY_EXISTS),
                )
            )
            val result = env.adminClient.deleteTopics(
                topics = mutableListOf("topic1", "topic2", "topic3"),
                options = DeleteTopicsOptions().apply { retryOnQuotaViolation = false },
            )
            assertNotFails(result.nameFutures!!["topic1"]!!::get)
            var e = assertFutureThrows(
                future = result.nameFutures!!["topic2"]!!,
                exceptionCauseClass = ThrottlingQuotaExceededException::class.java,
            )
            assertEquals(1000, e.throttleTimeMs)
            assertFutureError(
                future = result.nameFutures!!["topic3"]!!,
                exceptionClass = TopicExistsException::class.java,
            )

            // With topic IDs
            val topicId1 = Uuid.randomUuid()
            val topicId2 = Uuid.randomUuid()
            val topicId3 = Uuid.randomUuid()
            env.mockClient.prepareResponse(
                matcher = expectDeleteTopicsRequestWithTopicIds(topicId1, topicId2, topicId3),
                response = prepareDeleteTopicsResponse(
                    throttleTimeMs = 1000,
                    deletableTopicResultWithId(topicId1, Errors.NONE),
                    deletableTopicResultWithId(topicId2, Errors.THROTTLING_QUOTA_EXCEEDED),
                    deletableTopicResultWithId(topicId3, Errors.UNKNOWN_TOPIC_ID),
                ),
            )
            val resultIds = env.adminClient.deleteTopics(
                topics = TopicCollection.ofTopicIds(listOf(topicId1, topicId2, topicId3)),
                options = DeleteTopicsOptions().apply { retryOnQuotaViolation = false },
            )
            assertNotFails(resultIds.topicIdFutures!![topicId1]!!::get)
            e = assertFutureThrows(
                future = resultIds.topicIdFutures!![topicId2]!!,
                exceptionCauseClass = ThrottlingQuotaExceededException::class.java,
            )
            assertEquals(1000, e.throttleTimeMs)
            assertFutureError(
                future = resultIds.topicIdFutures!![topicId3]!!,
                exceptionClass = UnknownTopicIdException::class.java,
            )
        }
    }

    private fun expectDeleteTopicsRequestWithTopics(vararg topics: String): RequestMatcher {
        return RequestMatcher { body ->
            if (body is DeleteTopicsRequest) {
                return@RequestMatcher (body.topicNames() == topics.toList())
            }
            false
        }
    }

    private fun expectDeleteTopicsRequestWithTopicIds(vararg topicIds: Uuid): RequestMatcher {
        return RequestMatcher { body ->
            if (body is DeleteTopicsRequest) {
                return@RequestMatcher (body.topicIds() == topicIds.toList())
            }
            false
        }
    }

    @Test
    @Throws(Exception::class)
    fun testInvalidTopicNames() {
        mockClientEnv().use { env ->
            env.mockClient.setNodeApiVersions(NodeApiVersions.create())
            val sillyTopicNames = listOf("")
            val deleteFutures = env.adminClient.deleteTopics(sillyTopicNames).nameFutures!!
            for (sillyTopicName in sillyTopicNames) {
                assertFutureError(
                    future = deleteFutures[sillyTopicName]!!,
                    exceptionClass = InvalidTopicException::class.java,
                )
            }
            assertEquals(expected = 0, actual = env.mockClient.inFlightRequestCount())
            val describeFutures =
                env.adminClient.describeTopics(sillyTopicNames).topicNameValues()!!
            for (sillyTopicName in sillyTopicNames) {
                assertFutureError(
                    future = describeFutures[sillyTopicName]!!,
                    exceptionClass = InvalidTopicException::class.java
                )
            }
            assertEquals(expected = 0, actual = env.mockClient.inFlightRequestCount())
            val newTopics: MutableList<NewTopic> = ArrayList()
            for (sillyTopicName in sillyTopicNames) {
                newTopics.add(NewTopic(sillyTopicName, 1, 1.toShort()))
            }
            val createFutures = env.adminClient.createTopics(newTopics).values()
            for (sillyTopicName in sillyTopicNames) {
                assertFutureError(
                    future = createFutures[sillyTopicName]!!,
                    exceptionClass = InvalidTopicException::class.java
                )
            }
            assertEquals(expected = 0, actual = env.mockClient.inFlightRequestCount())
        }
    }

    @Test
    @Throws(Exception::class)
    fun testMetadataRetries() {
        // We should continue retrying on metadata update failures in spite of retry configuration
        val topic = "topic"
        val bootstrapCluster = Cluster.bootstrap(listOf(InetSocketAddress("localhost", 9999)))
        val initializedCluster = mockCluster(3, 0)
        AdminClientUnitTestEnv(
            Time.SYSTEM, bootstrapCluster,
            newStrMap(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999",
                AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "10000000",
                AdminClientConfig.RETRIES_CONFIG, "0"
            )
        ).use { env ->

            // The first request fails with a disconnect
            env.mockClient.prepareResponse(response = null, disconnected = true)

            // The next one succeeds and gives us the controller id
            env.mockClient.prepareResponse(
                response = RequestTestUtils.metadataResponse(
                    brokers = initializedCluster.nodes,
                    clusterId = initializedCluster.clusterResource.clusterId,
                    controllerId = initializedCluster.controller!!.id,
                    topicMetadataList = emptyList(),
                ),
            )

            // Then we respond to the DescribeTopic request
            val leader = initializedCluster.nodes[0]
            val partitionMetadata = PartitionMetadata(
                error = Errors.NONE,
                topicPartition = TopicPartition(topic, 0),
                leaderId = leader.id,
                leaderEpoch = 10,
                replicaIds = listOf(leader.id),
                inSyncReplicaIds = listOf(leader.id),
                offlineReplicaIds = listOf(leader.id),
            )
            env.mockClient.prepareResponse(
                response = RequestTestUtils.metadataResponse(
                    brokers = initializedCluster.nodes,
                    clusterId = initializedCluster.clusterResource.clusterId,
                    controllerId = 1,
                    topicMetadataList = listOf(
                        MetadataResponse.TopicMetadata(
                            error = Errors.NONE,
                            topic = topic,
                            topicId = Uuid.ZERO_UUID,
                            isInternal = false,
                            partitionMetadata = listOf(partitionMetadata),
                            authorizedOperations = MetadataResponse.AUTHORIZED_OPERATIONS_OMITTED,
                        ),
                    )
                )
            )
            val result = env.adminClient.describeTopics(setOf(topic))
            val topicDescriptions = result.allTopicNames().get()
            assertEquals(leader, topicDescriptions[topic]!!.partitions.get(0).leader)
            assertNull(topicDescriptions[topic]!!.authorizedOperations)
        }
    }

    @Test
    fun testAdminClientApisAuthenticationFailure() {
        val cluster = mockBootstrapCluster()
        AdminClientUnitTestEnv(
            Time.SYSTEM, cluster,
            newStrMap(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "1000")
        ).use { env ->
            env.mockClient.setNodeApiVersions(NodeApiVersions.create())
            env.mockClient.createPendingAuthenticationError(
                node = cluster.nodes[0],
                backoffMs = TimeUnit.DAYS.toMillis(1),
            )
            callAdminClientApisAndExpectAnAuthenticationError(env)
            callClientQuotasApisAndExpectAnAuthenticationError(env)
        }
    }

    private fun callAdminClientApisAndExpectAnAuthenticationError(env: AdminClientUnitTestEnv) {
        var e = Assertions.assertThrows(
            ExecutionException::class.java
        ) {
            env.adminClient.createTopics(
                newTopics = setOf(
                    NewTopic(
                        name = "myTopic",
                        replicasAssignments = mapOf(0 to mutableListOf(0, 1, 2)),
                    )
                ),
                options = CreateTopicsOptions().apply { timeoutMs = 10000 },
            ).all().get()
        }
        assertTrue(
            actual = e.cause is AuthenticationException,
            message = "Expected an authentication error, but got " + stackTrace(e),
        )
        val counts: MutableMap<String, NewPartitions> = HashMap()
        counts["my_topic"] = NewPartitions.increaseTo(3)
        counts["other_topic"] = NewPartitions.increaseTo(
            totalCount = 3,
            newAssignments = listOf(listOf(2), listOf(3)),
        )
        e = assertFailsWith<ExecutionException> {
            env.adminClient
                .createPartitions(counts)
                .all()
                .get()
        }
        assertTrue(
            actual = e.cause is AuthenticationException,
            message = "Expected an authentication error, but got " + stackTrace(e),
        )
        e = assertFailsWith<ExecutionException> {
            env.adminClient
                .createAcls(listOf(ACL1, ACL2))
                .all()
                .get()
        }
        assertTrue(
            actual = e.cause is AuthenticationException,
            message = "Expected an authentication error, but got " + stackTrace(e),
        )
        e = assertFailsWith<ExecutionException> {
            env.adminClient
                .describeAcls(FILTER1)
                .values()
                .get()
        }
        assertTrue(
            actual = e.cause is AuthenticationException,
            message = "Expected an authentication error, but got " + stackTrace(e),
        )
        e = assertFailsWith<ExecutionException> {
            env.adminClient
                .deleteAcls(listOf(FILTER1, FILTER2))
                .all()
                .get()
        }
        assertTrue(
            actual = e.cause is AuthenticationException,
            message = "Expected an authentication error, but got " + stackTrace(e),
        )
        e = assertFailsWith<ExecutionException> {
            env.adminClient.describeConfigs(
                setOf(ConfigResource(ConfigResource.Type.BROKER, "0"))
            ).all().get()
        }
        assertTrue(
            actual = e.cause is AuthenticationException,
            message = "Expected an authentication error, but got " + stackTrace(e),
        )
    }

    private fun callClientQuotasApisAndExpectAnAuthenticationError(env: AdminClientUnitTestEnv) {
        var e = assertFailsWith<ExecutionException> {
            env.adminClient
                .describeClientQuotas(ClientQuotaFilter.all())
                .entities
                .get()
        }
        assertTrue(
            actual = e.cause is AuthenticationException,
            message = "Expected an authentication error, but got " + stackTrace(e)
        )
        val entity = ClientQuotaEntity(mapOf(ClientQuotaEntity.USER to "user"))
        val alteration = ClientQuotaAlteration(
            entity = entity,
            ops = listOf(ClientQuotaAlteration.Op("consumer_byte_rate", 1000.0)),
        )
        e = assertFailsWith<ExecutionException> {
            env.adminClient
                .alterClientQuotas(listOf(alteration))
                .all()
                .get()
        }
        assertTrue(
            actual = e.cause is AuthenticationException,
            message = "Expected an authentication error, but got " + stackTrace(e),
        )
    }

    @Test
    @Throws(Exception::class)
    fun testDescribeAcls() {
        mockClientEnv().use { env ->
            env.mockClient.setNodeApiVersions(NodeApiVersions.create())

            // Test a call where we get back ACL1 and ACL2.
            env.mockClient.prepareResponse(
                response = DescribeAclsResponse(
                    data = DescribeAclsResponseData().setResources(
                        DescribeAclsResponse.aclsResources(listOf(ACL1, ACL2))
                    ),
                    version = ApiKeys.DESCRIBE_ACLS.latestVersion(),
                ),
            )
            assertCollectionIs(
                env.adminClient
                    .describeAcls(FILTER1)
                    .values()
                    .get(),
                ACL1,
                ACL2,
            )

            // Test a call where we get back no results.
            env.mockClient.prepareResponse(
                response = DescribeAclsResponse(
                    data = DescribeAclsResponseData(),
                    version = ApiKeys.DESCRIBE_ACLS.latestVersion(),
                ),
            )
            assertTrue(
                env.adminClient
                    .describeAcls(FILTER2)
                    .values()
                    .get()
                    .isEmpty()
            )

            // Test a call where we get back an error.
            env.mockClient.prepareResponse(
                response = DescribeAclsResponse(
                    data = DescribeAclsResponseData()
                        .setErrorCode(Errors.SECURITY_DISABLED.code)
                        .setErrorMessage("Security is disabled"),
                    version = ApiKeys.DESCRIBE_ACLS.latestVersion()
                ),
            )
            assertFutureError(
                future = env.adminClient.describeAcls(FILTER2).values(),
                exceptionClass = SecurityDisabledException::class.java,
            )

            // Test a call where we supply an invalid filter.
            assertFutureError(
                future = env.adminClient
                    .describeAcls(UNKNOWN_FILTER)
                    .values(),
                exceptionClass = InvalidRequestException::class.java,
            )
        }
    }

    @Test
    @Throws(Exception::class)
    fun testCreateAcls() {
        mockClientEnv().use { env ->
            env.mockClient.setNodeApiVersions(NodeApiVersions.create())

            // Test a call where we successfully create two ACLs.
            env.mockClient.prepareResponse(
                response = CreateAclsResponse(
                    CreateAclsResponseData().setResults(
                        listOf(AclCreationResult(), AclCreationResult())
                    )
                )
            )
            var results = env.adminClient.createAcls(listOf(ACL1, ACL2))
            assertCollectionIs(results.futures.keys, ACL1, ACL2)
            for (future in results.futures.values) future.get()
            results.all().get()

            // Test a call where we fail to create one ACL.
            env.mockClient.prepareResponse(
                response = CreateAclsResponse(
                    CreateAclsResponseData().setResults(
                        listOf(
                            AclCreationResult()
                                .setErrorCode(Errors.SECURITY_DISABLED.code)
                                .setErrorMessage("Security is disabled"),
                            AclCreationResult(),
                        ),
                    )
                )
            )
            results = env.adminClient.createAcls(listOf(ACL1, ACL2))
            assertCollectionIs(results.futures.keys, ACL1, ACL2)
            assertFutureError(
                future = results.futures[ACL1]!!,
                exceptionClass = SecurityDisabledException::class.java,
            )
            results.futures[ACL2]!!.get()
            assertFutureError(
                future = results.all(),
                exceptionClass = SecurityDisabledException::class.java,
            )
        }
    }

    @Test
    @Throws(Exception::class)
    fun testDeleteAcls() {
        mockClientEnv().use { env ->
            env.mockClient.setNodeApiVersions(NodeApiVersions.create())

            // Test a call where one filter has an error.
            env.mockClient.prepareResponse(
                response = DeleteAclsResponse(
                    data = DeleteAclsResponseData()
                        .setThrottleTimeMs(0)
                        .setFilterResults(
                            listOf(
                                DeleteAclsFilterResult().setMatchingAcls(
                                    listOf(
                                        DeleteAclsResponse.matchingAcl(ACL1, ApiError.NONE),
                                        DeleteAclsResponse.matchingAcl(ACL2, ApiError.NONE),
                                    )
                                ),
                                DeleteAclsFilterResult()
                                    .setErrorCode(Errors.SECURITY_DISABLED.code)
                                    .setErrorMessage("No security"),
                            ),
                        ),
                    version = ApiKeys.DELETE_ACLS.latestVersion(),
                )
            )
            var results = env.adminClient.deleteAcls(listOf(FILTER1, FILTER2))
            val filterResults = results.values()
            val filter1Results = filterResults[FILTER1]!!.get()
            assertNull(filter1Results.values[0].exception)
            assertEquals(expected = ACL1, actual = filter1Results.values[0].binding)
            assertNull(filter1Results.values.get(1).exception)
            assertEquals(expected = ACL2, actual = filter1Results.values[1].binding)
            assertFutureError(
                future = filterResults[FILTER2]!!,
                exceptionClass = SecurityDisabledException::class.java,
            )
            assertFutureError(
                future = results.all(),
                exceptionClass = SecurityDisabledException::class.java,
            )

            // Test a call where one deletion result has an error.
            env.mockClient.prepareResponse(
                response = DeleteAclsResponse(
                    data = DeleteAclsResponseData()
                        .setThrottleTimeMs(0)
                        .setFilterResults(
                            listOf(
                                DeleteAclsFilterResult().setMatchingAcls(
                                    listOf(
                                        DeleteAclsResponse.matchingAcl(ACL1, ApiError.NONE),
                                        DeleteAclsMatchingAcl()
                                            .setErrorCode(Errors.SECURITY_DISABLED.code)
                                            .setErrorMessage("No security")
                                            .setPermissionType(AclPermissionType.ALLOW.code)
                                            .setOperation(AclOperation.ALTER.code)
                                            .setResourceType(ResourceType.CLUSTER.code)
                                            .setPatternType(FILTER2.patternFilter.patternType.code)
                                    )
                                ),
                                DeleteAclsFilterResult(),
                            ),
                        ),
                    version = ApiKeys.DELETE_ACLS.latestVersion(),
                ),
            )
            results = env.adminClient.deleteAcls(listOf(FILTER1, FILTER2))
            assertTrue(results.values()[FILTER2]!!.get().values.isEmpty())
            assertFutureError(
                future = results.all(),
                exceptionClass = SecurityDisabledException::class.java,
            )

            // Test a call where there are no errors.
            env.mockClient.prepareResponse(
                response = DeleteAclsResponse(
                    data = DeleteAclsResponseData()
                        .setThrottleTimeMs(0)
                        .setFilterResults(
                            listOf(
                                DeleteAclsFilterResult().setMatchingAcls(
                                    listOf(DeleteAclsResponse.matchingAcl(ACL1, ApiError.NONE)),
                                ),
                                DeleteAclsFilterResult().setMatchingAcls(
                                    listOf(DeleteAclsResponse.matchingAcl(ACL2, ApiError.NONE)),
                                ),
                            )
                        ),
                    version = ApiKeys.DELETE_ACLS.latestVersion(),
                )
            )
            results = env.adminClient.deleteAcls(listOf(FILTER1, FILTER2))
            val deleted = results.all().get()
            assertCollectionIs(deleted, ACL1, ACL2)
        }
    }

    @Test
    @Throws(Exception::class)
    fun testElectLeaders() {
        val topic1 = TopicPartition("topic", 0)
        val topic2 = TopicPartition("topic", 2)
        mockClientEnv().use { env ->
            for (electionType in ElectionType.values()) {
                env.mockClient.setNodeApiVersions(NodeApiVersions.create())

                // Test a call where one partition has an error.
                val value: ApiError = ApiError.fromThrowable(ClusterAuthorizationException(null))
                val electionResults: MutableList<ReplicaElectionResult> = ArrayList()
                val electionResult = ReplicaElectionResult()
                electionResult.setTopic(topic1.topic)
                // Add partition 1 result
                val partition1Result = PartitionResult()
                partition1Result.setPartitionId(topic1.partition)
                partition1Result.setErrorCode(value.error.code)
                partition1Result.setErrorMessage(value.message)
                electionResult.partitionResult += partition1Result

                // Add partition 2 result
                val partition2Result: PartitionResult = PartitionResult()
                partition2Result.setPartitionId(topic2.partition)
                partition2Result.setErrorCode(value.error.code)
                partition2Result.setErrorMessage(value.message)
                electionResult.partitionResult += partition2Result
                electionResults.add(electionResult)
                env.mockClient.prepareResponse(
                    response = ElectLeadersResponse(
                        throttleTimeMs = 0,
                        errorCode = Errors.NONE.code,
                        electionResults = electionResults,
                        version = ApiKeys.ELECT_LEADERS.latestVersion(),
                    ),
                )
                var results = env.adminClient.electLeaders(
                    electionType = electionType,
                    partitions = setOf(topic1, topic2),
                )
                assertIs<ClusterAuthorizationException>(results.partitions().get()[topic2])

                // Test a call where there are no errors. By mutating the internal of election results
                partition1Result.setErrorCode(ApiError.NONE.error.code)
                partition1Result.setErrorMessage(ApiError.NONE.message)
                partition2Result.setErrorCode(ApiError.NONE.error.code)
                partition2Result.setErrorMessage(ApiError.NONE.message)
                env.mockClient.prepareResponse(
                    response = ElectLeadersResponse(
                        throttleTimeMs = 0,
                        errorCode = Errors.NONE.code,
                        electionResults = electionResults,
                        version = ApiKeys.ELECT_LEADERS.latestVersion(),
                    ),
                )
                results = env.adminClient.electLeaders(
                    electionType = electionType,
                    partitions = setOf(topic1, topic2),
                )
                assertNull(results.partitions().get()[topic1])
                assertNull(results.partitions().get()[topic2])

                // Now try a timeout
                results = env.adminClient.electLeaders(
                    electionType = electionType,
                    partitions = setOf(topic1, topic2),
                    options = ElectLeadersOptions().apply { timeoutMs = 100 },
                )
                assertFutureError(
                    future = results.partitions(),
                    exceptionClass = TimeoutException::class.java,
                )
            }
        }
    }

    @Test
    @Throws(Exception::class)
    fun testDescribeBrokerConfigs() {
        val broker0Resource = ConfigResource(ConfigResource.Type.BROKER, "0")
        val broker1Resource = ConfigResource(ConfigResource.Type.BROKER, "1")
        mockClientEnv().use { env ->
            env.mockClient.setNodeApiVersions(NodeApiVersions.create())
            env.mockClient.prepareResponseFrom(
                response = DescribeConfigsResponse(
                    DescribeConfigsResponseData().setResults(
                        listOf(
                            DescribeConfigsResponseData.DescribeConfigsResult()
                                .setResourceName(broker0Resource.name)
                                .setResourceType(broker0Resource.type.id)
                                .setErrorCode(Errors.NONE.code)
                                .setConfigs(emptyList())
                        )
                    )
                ),
                node = env.cluster.nodeById(0),
            )
            env.mockClient.prepareResponseFrom(
                response = DescribeConfigsResponse(
                    DescribeConfigsResponseData().setResults(
                        listOf(
                            DescribeConfigsResponseData.DescribeConfigsResult()
                                .setResourceName(broker1Resource.name)
                                .setResourceType(broker1Resource.type.id)
                                .setErrorCode(Errors.NONE.code)
                                .setConfigs(emptyList())
                        )
                    )
                ),
                node = env.cluster.nodeById(1),
            )
            val result = env.adminClient
                .describeConfigs(listOf(broker0Resource, broker1Resource))
                .values()

            assertEquals(
                expected = setOf(broker0Resource, broker1Resource),
                actual = result.keys,
            )
            result[broker0Resource]!!.get()
            result[broker1Resource]!!.get()
        }
    }

    @Test
    @Throws(Exception::class)
    fun testDescribeBrokerAndLogConfigs() {
        val brokerResource = ConfigResource(ConfigResource.Type.BROKER, "0")
        val brokerLoggerResource = ConfigResource(ConfigResource.Type.BROKER_LOGGER, "0")
        mockClientEnv().use { env ->
            env.mockClient.setNodeApiVersions(NodeApiVersions.create())
            env.mockClient.prepareResponseFrom(
                response = DescribeConfigsResponse(
                    DescribeConfigsResponseData().setResults(
                        listOf(
                            DescribeConfigsResponseData.DescribeConfigsResult()
                                .setResourceName(brokerResource.name)
                                .setResourceType(brokerResource.type.id)
                                .setErrorCode(Errors.NONE.code)
                                .setConfigs(emptyList()),
                            DescribeConfigsResponseData.DescribeConfigsResult()
                                .setResourceName(brokerLoggerResource.name)
                                .setResourceType(brokerLoggerResource.type.id)
                                .setErrorCode(Errors.NONE.code)
                                .setConfigs(emptyList()),
                        )
                    )
                ),
                node = env.cluster.nodeById(0),
            )
            val result: Map<ConfigResource, KafkaFuture<Config>> =
                env.adminClient
                    .describeConfigs(listOf(brokerResource, brokerLoggerResource))
                    .values()
            assertEquals(
                expected = setOf(brokerResource, brokerLoggerResource),
                actual = result.keys,
            )
            result[brokerResource]!!.get()
            result[brokerLoggerResource]!!.get()
        }
    }

    @Test
    fun testDescribeConfigsPartialResponse() {
        val topic = ConfigResource(ConfigResource.Type.TOPIC, "topic")
        val topic2 = ConfigResource(ConfigResource.Type.TOPIC, "topic2")
        mockClientEnv().use { env ->
            env.mockClient.setNodeApiVersions(NodeApiVersions.create())
            env.mockClient.prepareResponse(
                response = DescribeConfigsResponse(
                    DescribeConfigsResponseData().setResults(
                        listOf(
                            DescribeConfigsResponseData.DescribeConfigsResult()
                                .setResourceName(topic.name)
                                .setResourceType(topic.type.id)
                                .setErrorCode(Errors.NONE.code)
                                .setConfigs(emptyList())
                        )
                    )
                ),
            )
            val result = env.adminClient.describeConfigs(listOf(topic, topic2)).values()
            assertEquals(
                expected = setOf(topic, topic2),
                actual = result.keys,
            )
            result[topic]
            assertFutureThrows(
                future = result[topic2]!!,
                exceptionCauseClass = ApiException::class.java,
            )
        }
    }

    @Test
    @Throws(Exception::class)
    fun testDescribeConfigsUnrequested() {
        val topic = ConfigResource(ConfigResource.Type.TOPIC, "topic")
        val unrequested = ConfigResource(ConfigResource.Type.TOPIC, "unrequested")
        mockClientEnv().use { env ->
            env.mockClient.setNodeApiVersions(NodeApiVersions.create())
            env.mockClient.prepareResponse(
                response = DescribeConfigsResponse(
                    DescribeConfigsResponseData().setResults(
                        listOf(
                            DescribeConfigsResponseData.DescribeConfigsResult()
                                .setResourceName(topic.name)
                                .setResourceType(topic.type.id)
                                .setErrorCode(Errors.NONE.code)
                                .setConfigs(emptyList()),
                            DescribeConfigsResponseData.DescribeConfigsResult()
                                .setResourceName(unrequested.name)
                                .setResourceType(unrequested.type.id)
                                .setErrorCode(Errors.NONE.code)
                                .setConfigs(emptyList()),
                        )
                    )
                ),
            )
            val result = env.adminClient.describeConfigs(listOf(topic)).values()
            assertEquals(
                expected = setOf(topic),
                actual = result.keys,
            )
            assertNotNull(result[topic]!!.get())
            assertNull(result[unrequested])
        }
    }

    @Test
    @Throws(ExecutionException::class, InterruptedException::class)
    fun testDescribeLogDirs() {
        val brokers = setOf(0)
        val logDir = "/var/data/kafka"
        val tp = TopicPartition("topic", 12)
        val partitionSize = 1234567890
        val offsetLag = 24
        mockClientEnv().use { env ->
            env.mockClient.setNodeApiVersions(NodeApiVersions.create())
            env.mockClient.prepareResponseFrom(
                response = prepareDescribeLogDirsResponse(
                    error = Errors.NONE,
                    logDir = logDir,
                    tp = tp,
                    partitionSize = partitionSize.toLong(),
                    offsetLag = offsetLag.toLong(),
                ),
                node = env.cluster.nodeById(0),
            )
            val result = env.adminClient.describeLogDirs(brokers)
            val descriptions = result.descriptions()
            assertEquals(expected = brokers, actual = descriptions.keys)
            assertNotNull(descriptions[0])
            assertDescriptionContains(
                descriptionsMap = descriptions[0]!!.get(),
                logDir = logDir,
                tp = tp,
                partitionSize = partitionSize.toLong(),
                offsetLag = offsetLag.toLong(),
            )
            val allDescriptions = result.allDescriptions().get()
            assertEquals(brokers, allDescriptions.keys)
            assertDescriptionContains(
                descriptionsMap = allDescriptions.get(0)!!,
                logDir = logDir,
                tp = tp,
                partitionSize = partitionSize.toLong(),
                offsetLag = offsetLag.toLong(),
            )

            // Empty results when not authorized with version < 3
            env.mockClient.prepareResponseFrom(
                response = prepareEmptyDescribeLogDirsResponse(null),
                node = env.cluster.nodeById(0),
            )
            val errorResult = env.adminClient.describeLogDirs(brokers)
            var exception = assertFailsWith<ExecutionException> {
                errorResult.allDescriptions().get()
            }
            assertIs<ClusterAuthorizationException>(exception.cause)

            // Empty results with an error with version >= 3
            env.mockClient.prepareResponseFrom(
                response = prepareEmptyDescribeLogDirsResponse(Errors.UNKNOWN_SERVER_ERROR),
                node = env.cluster.nodeById(0),
            )
            val errorResult2: DescribeLogDirsResult =
                env.adminClient.describeLogDirs(brokers)
            exception = assertFailsWith<ExecutionException> { errorResult2.allDescriptions().get() }
            assertIs<UnknownServerException>(exception.cause)
        }
    }

    @Test
    @Throws(ExecutionException::class, InterruptedException::class)
    fun testDescribeLogDirsWithVolumeBytes() {
        val brokers = setOf(0)
        val logDir = "/var/data/kafka"
        val tp = TopicPartition("topic", 12)
        val partitionSize = 1234567890
        val offsetLag = 24
        val totalBytes = 123L
        val usableBytes = 456L
        mockClientEnv().use { env ->
            env.mockClient.setNodeApiVersions(NodeApiVersions.create())
            env.mockClient.prepareResponseFrom(
                response = prepareDescribeLogDirsResponse(
                    error = Errors.NONE,
                    logDir = logDir,
                    tp = tp,
                    partitionSize = partitionSize.toLong(),
                    offsetLag = offsetLag.toLong(),
                    totalBytes = totalBytes,
                    usableBytes = usableBytes,
                ),
                node = env.cluster.nodeById(0),
            )
            val result = env.adminClient.describeLogDirs(brokers)
            val descriptions = result.descriptions()
            assertEquals(expected = brokers, actual = descriptions.keys)
            assertNotNull(descriptions[0])
            assertDescriptionContains(
                descriptionsMap = descriptions[0]!!.get(),
                logDir = logDir,
                tp = tp,
                partitionSize = partitionSize.toLong(),
                offsetLag = offsetLag.toLong(),
                totalBytes = totalBytes,
                usableBytes = usableBytes,
            )
            val allDescriptions = result.allDescriptions().get()
            assertEquals(expected = brokers, actual = allDescriptions.keys)
            assertDescriptionContains(
                descriptionsMap = allDescriptions[0]!!,
                logDir = logDir,
                tp = tp,
                partitionSize = partitionSize.toLong(),
                offsetLag = offsetLag.toLong(),
                totalBytes = totalBytes,
                usableBytes = usableBytes,
            )

            // Empty results when not authorized with version < 3
            env.mockClient.prepareResponseFrom(
                response = prepareEmptyDescribeLogDirsResponse(null),
                node = env.cluster.nodeById(0),
            )
            val errorResult = env.adminClient.describeLogDirs(brokers)
            var exception = assertFailsWith<ExecutionException> {
                errorResult.allDescriptions().get()
            }
            assertIs<ClusterAuthorizationException>(exception.cause)

            // Empty results with an error with version >= 3
            env.mockClient.prepareResponseFrom(
                response = prepareEmptyDescribeLogDirsResponse(Errors.UNKNOWN_SERVER_ERROR),
                node = env.cluster.nodeById(0),
            )
            val errorResult2 = env.adminClient.describeLogDirs(brokers)
            exception = assertFailsWith<ExecutionException> {
                errorResult2.allDescriptions().get()
            }
            assertIs<UnknownServerException>(exception.cause)
        }
    }

    @Suppress("Deprecation")
    @Test
    @Throws(
        ExecutionException::class,
        InterruptedException::class
    )
    fun testDescribeLogDirsDeprecated() {
        val brokers = setOf(0)
        val tp = TopicPartition("topic", 12)
        val logDir = "/var/data/kafka"
        val error = Errors.NONE
        val offsetLag = 24
        val partitionSize = 1234567890
        mockClientEnv().use { env ->
            env.mockClient.setNodeApiVersions(NodeApiVersions.create())
            env.mockClient.prepareResponseFrom(
                response = prepareDescribeLogDirsResponse(
                    error = error,
                    logDir = logDir,
                    tp = tp,
                    partitionSize = partitionSize.toLong(),
                    offsetLag = offsetLag.toLong()
                ),
                node = env.cluster.nodeById(0),
            )
            val result = env.adminClient.describeLogDirs(brokers)
            val deprecatedValues = result.values()
            assertEquals(expected = brokers, actual = deprecatedValues.keys)
            assertNotNull(deprecatedValues[0])
            assertDescriptionContains(
                descriptionsMap = deprecatedValues[0]!!.get(),
                logDir = logDir,
                tp = tp,
                error = error,
                offsetLag = offsetLag,
                partitionSize = partitionSize.toLong(),
            )
            val deprecatedAll = result.all().get()
            assertEquals(expected = brokers, actual = deprecatedAll.keys)
            assertDescriptionContains(
                descriptionsMap = deprecatedAll[0]!!,
                logDir = logDir,
                tp = tp,
                error = error,
                offsetLag = offsetLag,
                partitionSize = partitionSize.toLong(),
            )
        }
    }

    @Test
    @Throws(ExecutionException::class, InterruptedException::class)
    fun testDescribeLogDirsOfflineDir() {
        val brokers = setOf(0)
        val logDir = "/var/data/kafka"
        val error = Errors.KAFKA_STORAGE_ERROR
        mockClientEnv().use { env ->
            env.mockClient.setNodeApiVersions(NodeApiVersions.create())
            env.mockClient.prepareResponseFrom(
                response = prepareDescribeLogDirsResponse(
                    error = error,
                    logDir = logDir,
                    topics = emptyList(),
                ),
                node = env.cluster.nodeById(0),
            )
            val result = env.adminClient.describeLogDirs(brokers)
            val descriptions = result.descriptions()
            assertEquals(expected = brokers, actual = descriptions.keys)
            assertNotNull(descriptions.get(0))
            val descriptionsMap = descriptions.get(0)!!.get()
            assertEquals(expected = setOf(logDir), actual = descriptionsMap.keys)
            assertEquals(
                expected = error.exception!!.javaClass,
                actual = descriptionsMap.get(logDir)!!.error()!!.javaClass,
            )
            assertEquals(
                expected = emptySet<Any>(),
                actual = descriptionsMap.get(logDir)!!.replicaInfos().keys,
            )
            val allDescriptions = result.allDescriptions().get()
            assertEquals(brokers, allDescriptions.keys)
            val allMap = allDescriptions.get(0)!!
            assertNotNull(allMap)
            assertEquals(
                expected = setOf(logDir),
                actual = allMap.keys,
            )
            assertEquals(
                expected = error.exception!!.javaClass,
                actual = allMap.get(logDir)!!.error()!!.javaClass,
            )
            assertEquals(
                emptySet<Any>(),
                allMap.get(logDir)!!.replicaInfos().keys,
            )
        }
    }

    @Suppress("Deprecation")
    @Test
    @Throws(ExecutionException::class, InterruptedException::class)
    fun testDescribeLogDirsOfflineDirDeprecated() {
        val brokers = setOf(0)
        val logDir = "/var/data/kafka"
        val error = Errors.KAFKA_STORAGE_ERROR
        mockClientEnv().use { env ->
            env.mockClient.setNodeApiVersions(NodeApiVersions.create())
            env.mockClient.prepareResponseFrom(
                response = prepareDescribeLogDirsResponse(
                    error = error,
                    logDir = logDir,
                    topics = emptyList(),
                ),
                node = env.cluster.nodeById(0),
            )
            val result = env.adminClient.describeLogDirs(brokers)
            val deprecatedValues = result.values()
            assertEquals(expected = brokers, actual = deprecatedValues.keys)
            assertNotNull(deprecatedValues[0])
            val valuesMap = deprecatedValues[0]!!.get()
            assertEquals(
                expected = setOf(logDir),
                actual = valuesMap.keys,
            )
            assertEquals(error, valuesMap[logDir]!!.error)
            assertEquals(
                expected = emptySet<Any>(),
                actual = valuesMap[logDir]!!.replicaInfos.keys,
            )
            val deprecatedAll = result.all().get()
            assertEquals(expected = brokers, actual = deprecatedAll.keys)
            val allMap = deprecatedAll[0]!!
            assertNotNull(allMap)
            assertEquals(expected = setOf(logDir), actual = allMap.keys)
            assertEquals(expected = error, actual = allMap[logDir]!!.error)
            assertEquals(
                expected = emptySet<Any>(),
                actual = allMap[logDir]!!.replicaInfos.keys,
            )
        }
    }

    @Test
    @Throws(ExecutionException::class, InterruptedException::class)
    fun testDescribeReplicaLogDirs() {
        val tpr1 = TopicPartitionReplica("topic", 12, 1)
        val tpr2 = TopicPartitionReplica("topic", 12, 2)
        mockClientEnv().use { env ->
            env.mockClient.setNodeApiVersions(NodeApiVersions.create())
            val broker1log0 = "/var/data/kafka0"
            val broker1log1 = "/var/data/kafka1"
            val broker2log0 = "/var/data/kafka2"
            val broker1Log0OffsetLag = 24
            val broker1Log0PartitionSize = 987654321
            val broker1Log1PartitionSize = 123456789
            val broker1Log1OffsetLag = 4321
            env.mockClient.prepareResponseFrom(
                response = DescribeLogDirsResponse(
                    DescribeLogDirsResponseData().setResults(
                        listOf(
                            prepareDescribeLogDirsResult(
                                tpr = tpr1,
                                logDir = broker1log0,
                                partitionSize = broker1Log0PartitionSize,
                                offsetLag = broker1Log0OffsetLag,
                                isFuture = false,
                            ),
                            prepareDescribeLogDirsResult(
                                tpr = tpr1,
                                logDir = broker1log1,
                                partitionSize = broker1Log1PartitionSize,
                                offsetLag = broker1Log1OffsetLag,
                                isFuture = true,
                            )
                        )
                    )
                ),
                node = env.cluster.nodeById(tpr1.brokerId),
            )
            env.mockClient.prepareResponseFrom(
                response = prepareDescribeLogDirsResponse(
                    error = Errors.KAFKA_STORAGE_ERROR,
                    logDir = broker2log0,
                ),
                node = env.cluster.nodeById(tpr2.brokerId),
            )
            val result = env.adminClient.describeReplicaLogDirs(listOf(tpr1, tpr2))
            val values = result.futures
            assertEquals(expected = setOf(tpr1, tpr2), actual = values.keys)
            assertNotNull(values.get(tpr1))
            assertEquals(
                expected = broker1log0,
                actual = values.get(tpr1)!!.get().currentReplicaLogDir,
            )
            assertEquals(
                expected = broker1Log0OffsetLag.toLong(),
                actual = values.get(tpr1)!!.get().currentReplicaOffsetLag,
            )
            assertEquals(
                expected = broker1log1,
                actual = values.get(tpr1)!!.get().futureReplicaLogDir,
            )
            assertEquals(
                expected = broker1Log1OffsetLag.toLong(),
                actual = values.get(tpr1)!!.get().futureReplicaOffsetLag,
            )
            assertNotNull(values.get(tpr2))
            assertNull(values.get(tpr2)!!.get().currentReplicaLogDir)
            assertEquals(expected = -1, actual = values.get(tpr2)!!.get().currentReplicaOffsetLag)
            assertNull(values.get(tpr2)!!.get().futureReplicaLogDir)
            assertEquals(expected = -1, actual = values.get(tpr2)!!.get().futureReplicaOffsetLag)
        }
    }

    @Test
    @Throws(ExecutionException::class, InterruptedException::class)
    fun testDescribeReplicaLogDirsUnexpected() {
        val expected = TopicPartitionReplica("topic", 12, 1)
        val unexpected = TopicPartitionReplica("topic", 12, 2)
        mockClientEnv().use { env ->
            env.mockClient.setNodeApiVersions(NodeApiVersions.create())
            val broker1log0 = "/var/data/kafka0"
            val broker1log1 = "/var/data/kafka1"
            val broker1Log0PartitionSize = 987654321
            val broker1Log0OffsetLag = 24
            val broker1Log1PartitionSize = 123456789
            val broker1Log1OffsetLag = 4321
            env.mockClient.prepareResponseFrom(
                response = DescribeLogDirsResponse(
                    DescribeLogDirsResponseData().setResults(
                        listOf(
                            prepareDescribeLogDirsResult(
                                tpr = expected,
                                logDir = broker1log0,
                                partitionSize = broker1Log0PartitionSize,
                                offsetLag = broker1Log0OffsetLag,
                                isFuture = false,
                            ),
                            prepareDescribeLogDirsResult(
                                tpr = unexpected,
                                logDir = broker1log1,
                                partitionSize = broker1Log1PartitionSize,
                                offsetLag = broker1Log1OffsetLag,
                                isFuture = true,
                            ),
                        )
                    )
                ),
                node = env.cluster.nodeById(expected.brokerId),
            )
            val result = env.adminClient.describeReplicaLogDirs(listOf(expected))
            val values = result.futures
            assertEquals(expected = setOf(expected), actual = values.keys)
            assertNotNull(values[expected])
            assertEquals(
                expected = broker1log0,
                actual = values[expected]!!.get().currentReplicaLogDir
            )
            assertEquals(
                expected = broker1Log0OffsetLag.toLong(),
                actual = values[expected]!!.get().currentReplicaOffsetLag,
            )
            assertEquals(
                expected = broker1log1,
                actual = values[expected]!!.get().futureReplicaLogDir,
            )
            assertEquals(
                expected = broker1Log1OffsetLag.toLong(),
                actual = values[expected]!!.get().futureReplicaOffsetLag
            )
        }
    }

    @Test
    @Throws(Exception::class)
    fun testCreatePartitions() {
        mockClientEnv().use { env ->
            env.mockClient.setNodeApiVersions(NodeApiVersions.create())

            // Test a call where one filter has an error.
            env.mockClient.prepareResponse(
                matcher = expectCreatePartitionsRequestWithTopics("my_topic", "other_topic"),
                response = prepareCreatePartitionsResponse(
                    throttleTimeMs = 1000,
                    createPartitionsTopicResult("my_topic", Errors.NONE),
                    createPartitionsTopicResult(
                        name = "other_topic",
                        error = Errors.INVALID_TOPIC_EXCEPTION,
                        errorMessage = "some detailed reason",
                    )
                )
            )
            val counts: MutableMap<String, NewPartitions> = HashMap()
            counts["my_topic"] = NewPartitions.increaseTo(3)
            counts["other_topic"] = NewPartitions.increaseTo(
                totalCount = 3,
                newAssignments = listOf(listOf(2), listOf(3)),
            )
            val results = env.adminClient.createPartitions(counts)
            val values = results.futures
            val myTopicResult = values["my_topic"]!!
            myTopicResult.get()
            val otherTopicResult = values["other_topic"]!!
            try {
                otherTopicResult.get()
                fail("get() should throw ExecutionException")
            } catch (e0: ExecutionException) {
                assertIs<InvalidTopicException>(e0.cause)
                val e = e0.cause as InvalidTopicException
                assertEquals(expected = "some detailed reason", actual = e.message)
            }
        }
    }

    @Test
    @Throws(Exception::class)
    fun testCreatePartitionsRetryThrottlingExceptionWhenEnabled() {
        mockClientEnv().use { env ->
            env.mockClient.setNodeApiVersions(NodeApiVersions.create())
            env.mockClient.prepareResponse(
                matcher = expectCreatePartitionsRequestWithTopics("topic1", "topic2", "topic3"),
                response = prepareCreatePartitionsResponse(
                    throttleTimeMs = 1000,
                    createPartitionsTopicResult("topic1", Errors.NONE),
                    createPartitionsTopicResult("topic2", Errors.THROTTLING_QUOTA_EXCEEDED),
                    createPartitionsTopicResult("topic3", Errors.TOPIC_ALREADY_EXISTS),
                )
            )
            env.mockClient.prepareResponse(
                matcher = expectCreatePartitionsRequestWithTopics("topic2"),
                response = prepareCreatePartitionsResponse(
                    throttleTimeMs = 1000,
                    createPartitionsTopicResult("topic2", Errors.THROTTLING_QUOTA_EXCEEDED),
                )
            )
            env.mockClient.prepareResponse(
                matcher = expectCreatePartitionsRequestWithTopics("topic2"),
                response = prepareCreatePartitionsResponse(
                    throttleTimeMs = 0,
                    createPartitionsTopicResult("topic2", Errors.NONE),
                )
            )
            val counts: MutableMap<String, NewPartitions> = HashMap()
            counts["topic1"] = NewPartitions.increaseTo(1)
            counts["topic2"] = NewPartitions.increaseTo(2)
            counts["topic3"] = NewPartitions.increaseTo(3)
            val result = env.adminClient.createPartitions(
                newPartitions = counts,
                options = CreatePartitionsOptions().apply { retryOnQuotaViolation = true },
            )
            assertNotFails(result.futures["topic1"]!!::get)
            assertNotFails(result.futures["topic2"]!!::get)
            assertFutureThrows(
                future = result.futures["topic3"]!!,
                exceptionCauseClass = TopicExistsException::class.java,
            )
        }
    }

    @Test
    @Throws(Exception::class)
    fun testCreatePartitionsRetryThrottlingExceptionWhenEnabledUntilRequestTimeOut() {
        val defaultApiTimeout = 60000
        val time = MockTime()
        mockClientEnv(
            time,
            AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG,
            defaultApiTimeout.toString(),
        ).use { env ->
            env.mockClient.setNodeApiVersions(NodeApiVersions.create())
            env.mockClient.prepareResponse(
                matcher = expectCreatePartitionsRequestWithTopics("topic1", "topic2", "topic3"),
                response = prepareCreatePartitionsResponse(
                    throttleTimeMs = 1000,
                    createPartitionsTopicResult("topic1", Errors.NONE),
                    createPartitionsTopicResult("topic2", Errors.THROTTLING_QUOTA_EXCEEDED),
                    createPartitionsTopicResult("topic3", Errors.TOPIC_ALREADY_EXISTS),
                )
            )
            env.mockClient.prepareResponse(
                matcher = expectCreatePartitionsRequestWithTopics("topic2"),
                response = prepareCreatePartitionsResponse(
                    throttleTimeMs = 1000,
                    createPartitionsTopicResult("topic2", Errors.THROTTLING_QUOTA_EXCEEDED),
                )
            )
            val counts: MutableMap<String, NewPartitions> = HashMap()
            counts["topic1"] = NewPartitions.increaseTo(1)
            counts["topic2"] = NewPartitions.increaseTo(2)
            counts["topic3"] = NewPartitions.increaseTo(3)
            val result = env.adminClient.createPartitions(
                newPartitions = counts,
                options = CreatePartitionsOptions().apply { retryOnQuotaViolation = true }
            )

            // Wait until the prepared attempts have consumed
            waitForCondition(
                testCondition = { env.mockClient.numAwaitingResponses() == 0 },
                conditionDetails = "Failed awaiting CreatePartitions requests",
            )

            // Wait until the next request is sent out
            waitForCondition(
                testCondition = { env.mockClient.inFlightRequestCount() == 1 },
                conditionDetails = "Failed awaiting next CreatePartitions request",
            )

            // Advance time past the default api timeout to time out the inflight request
            time.sleep(defaultApiTimeout.toLong() + 1)
            assertNotFails(result.futures["topic1"]!!::get)
            val e = assertFutureThrows(
                future = (result.futures["topic2"])!!,
                exceptionCauseClass = ThrottlingQuotaExceededException::class.java,
            )
            assertEquals(expected = 0, actual = e.throttleTimeMs)
            assertFutureThrows(
                future = result.futures["topic3"]!!,
                exceptionCauseClass = TopicExistsException::class.java,
            )
        }
    }

    @Test
    @Throws(Exception::class)
    fun testCreatePartitionsDontRetryThrottlingExceptionWhenDisabled() {
        mockClientEnv().use { env ->
            env.mockClient.setNodeApiVersions(NodeApiVersions.create())
            env.mockClient.prepareResponse(
                matcher = expectCreatePartitionsRequestWithTopics("topic1", "topic2", "topic3"),
                response = prepareCreatePartitionsResponse(
                    throttleTimeMs = 1000,
                    createPartitionsTopicResult("topic1", Errors.NONE),
                    createPartitionsTopicResult("topic2", Errors.THROTTLING_QUOTA_EXCEEDED),
                    createPartitionsTopicResult("topic3", Errors.TOPIC_ALREADY_EXISTS),
                )
            )
            val counts: MutableMap<String, NewPartitions> = HashMap()
            counts["topic1"] = NewPartitions.increaseTo(1)
            counts["topic2"] = NewPartitions.increaseTo(2)
            counts["topic3"] = NewPartitions.increaseTo(3)
            val result = env.adminClient.createPartitions(
                newPartitions = counts,
                options = CreatePartitionsOptions().apply { retryOnQuotaViolation = false }
            )
            assertNotFails(result.futures["topic1"]!!::get)
            val e = assertFutureThrows(
                future = result.futures["topic2"]!!,
                exceptionCauseClass = ThrottlingQuotaExceededException::class.java,
            )
            assertEquals(expected = 1000, actual = e.throttleTimeMs)
            assertFutureThrows(
                future = result.futures["topic3"]!!,
                exceptionCauseClass = TopicExistsException::class.java,
            )
        }
    }

    private fun expectCreatePartitionsRequestWithTopics(vararg topics: String): RequestMatcher {
        return RequestMatcher { body ->
            if (body is CreatePartitionsRequest) {
                val request: CreatePartitionsRequest = body
                for (topic in topics) {
                    if (request.data().topics.find(topic) == null) return@RequestMatcher false
                }
                return@RequestMatcher topics.size == request.data().topics.size
            }
            false
        }
    }

    @Test
    fun testDeleteRecordsTopicAuthorizationError() {
        val topic = "foo"
        val partition = TopicPartition(topic, 0)
        mockClientEnv().use { env ->
            val topics: MutableList<MetadataResponse.TopicMetadata> = ArrayList()
            topics.add(
                MetadataResponse.TopicMetadata(
                    error = Errors.TOPIC_AUTHORIZATION_FAILED,
                    topic = topic,
                    isInternal = false,
                    partitionMetadata = emptyList(),
                )
            )
            env.mockClient.prepareResponse(
                response = RequestTestUtils.metadataResponse(
                    brokers = env.cluster.nodes,
                    clusterId = env.cluster.clusterResource.clusterId,
                    controllerId = env.cluster.controller!!.id,
                    topicMetadataList = topics,
                )
            )
            val recordsToDelete: MutableMap<TopicPartition, RecordsToDelete> = HashMap()
            recordsToDelete[partition] = RecordsToDelete(beforeOffset = 10L)
            val results = env.adminClient.deleteRecords(recordsToDelete)
            assertFutureThrows(
                future = results.lowWatermarks()[partition]!!,
                exceptionCauseClass = TopicAuthorizationException::class.java,
            )
        }
    }

    @Test
    @Throws(Exception::class)
    fun testDeleteRecordsMultipleSends() {
        val topic = "foo"
        val tp0 = TopicPartition(topic, 0)
        val tp1 = TopicPartition(topic, 1)
        val time = MockTime()
        AdminClientUnitTestEnv(time, mockCluster(3, 0)).use { env ->
            val nodes = env.cluster.nodes
            val partitionMetadata: MutableList<PartitionMetadata> = ArrayList()
            partitionMetadata.add(
                PartitionMetadata(
                    error = Errors.NONE,
                    topicPartition = tp0,
                    leaderId = nodes[0].id,
                    leaderEpoch = 5,
                    replicaIds = listOf(nodes[0].id),
                    inSyncReplicaIds = listOf(nodes[0].id),
                    offlineReplicaIds = emptyList(),
                )
            )
            partitionMetadata.add(
                PartitionMetadata(
                    error = Errors.NONE,
                    topicPartition = tp1,
                    leaderId = nodes[1].id,
                    leaderEpoch = 5,
                    replicaIds = listOf(nodes[1].id),
                    inSyncReplicaIds = listOf(nodes[1].id),
                    offlineReplicaIds = emptyList(),
                )
            )
            val topicMetadata: MutableList<MetadataResponse.TopicMetadata> = ArrayList()
            topicMetadata.add(
                MetadataResponse.TopicMetadata(
                    error = Errors.NONE,
                    topic = topic,
                    isInternal = false,
                    partitionMetadata = partitionMetadata,
                )
            )
            env.mockClient.prepareResponse(
                response = RequestTestUtils.metadataResponse(
                    brokers = env.cluster.nodes,
                    clusterId = env.cluster.clusterResource.clusterId,
                    controllerId = env.cluster.controller!!.id,
                    topicMetadataList = topicMetadata,
                )
            )
            env.mockClient.prepareResponseFrom(
                response = DeleteRecordsResponse(
                    DeleteRecordsResponseData().setTopics(
                        DeleteRecordsTopicResultCollection(
                            listOf(
                                DeleteRecordsTopicResult()
                                    .setName(tp0.topic)
                                    .setPartitions(
                                        DeleteRecordsPartitionResultCollection(
                                            listOf(
                                                DeleteRecordsPartitionResult()
                                                    .setPartitionIndex(tp0.partition)
                                                    .setErrorCode(Errors.NONE.code)
                                                    .setLowWatermark(3)
                                            ).iterator()
                                        )
                                    )
                            ).iterator()
                        )
                    )
                ),
                node = nodes.get(0),
            )
            env.mockClient.disconnect(nodes[1].idString())
            env.mockClient.createPendingAuthenticationError(nodes[1], 100)
            val recordsToDelete: MutableMap<TopicPartition, RecordsToDelete> = HashMap()
            recordsToDelete[tp0] = RecordsToDelete(beforeOffset = 10L)
            recordsToDelete[tp1] = RecordsToDelete(beforeOffset = 10L)
            val results: DeleteRecordsResult = env.adminClient.deleteRecords(recordsToDelete)
            assertEquals(
                expected = 3L,
                actual = results.lowWatermarks()[tp0]!!.get().lowWatermark(),
            )
            assertFutureThrows(
                future = results.lowWatermarks()[tp1]!!,
                exceptionCauseClass = AuthenticationException::class.java,
            )
        }
    }

    @Test
    @Throws(Exception::class)
    fun testDeleteRecords() {
        val node = Node(id = 0, host = "localhost", port = 8121)
        val nodes = mapOf(0 to node)
        val partitionInfos = listOf(
            PartitionInfo(
                topic = "my_topic",
                partition = 0,
                leader = node,
                replicas = listOf(node),
                inSyncReplicas = listOf(node),
            ),
            PartitionInfo(
                topic = "my_topic",
                partition = 1,
                leader = node,
                replicas = listOf(node),
                inSyncReplicas = listOf(node),
            ),
            PartitionInfo(
                topic = "my_topic",
                partition = 2,
                leader = null,
                replicas = listOf(node),
                inSyncReplicas = listOf(node),
            ),
            PartitionInfo(
                topic = "my_topic",
                partition = 3,
                leader = node,
                replicas = listOf(node),
                inSyncReplicas = listOf(node),
            ),
            PartitionInfo(
                topic = "my_topic",
                partition = 4,
                leader = node,
                replicas = listOf(node),
                inSyncReplicas = listOf(node),
            ),
        )
        val cluster = Cluster(
            clusterId = "mockClusterId",
            nodes = nodes.values,
            partitions = partitionInfos,
            controller = node,
        )
        val myTopicPartition0 = TopicPartition(topic = "my_topic", partition = 0)
        val myTopicPartition1 = TopicPartition(topic = "my_topic", partition = 1)
        val myTopicPartition2 = TopicPartition(topic = "my_topic", partition = 2)
        val myTopicPartition3 = TopicPartition(topic = "my_topic", partition = 3)
        val myTopicPartition4 = TopicPartition(topic = "my_topic", partition = 4)
        AdminClientUnitTestEnv(cluster).use { env ->
            env.mockClient.setNodeApiVersions(NodeApiVersions.create())
            val m = DeleteRecordsResponseData()
            m.topics.add(
                DeleteRecordsTopicResult()
                    .setName(myTopicPartition0.topic)
                    .setPartitions(
                        DeleteRecordsPartitionResultCollection(
                            listOf(
                                DeleteRecordsPartitionResult()
                                    .setPartitionIndex(myTopicPartition0.partition)
                                    .setLowWatermark(3)
                                    .setErrorCode(Errors.NONE.code),
                                DeleteRecordsPartitionResult()
                                    .setPartitionIndex(myTopicPartition1.partition)
                                    .setLowWatermark(DeleteRecordsResponse.INVALID_LOW_WATERMARK)
                                    .setErrorCode(Errors.OFFSET_OUT_OF_RANGE.code),
                                DeleteRecordsPartitionResult()
                                    .setPartitionIndex(myTopicPartition3.partition)
                                    .setLowWatermark(DeleteRecordsResponse.INVALID_LOW_WATERMARK)
                                    .setErrorCode(Errors.NOT_LEADER_OR_FOLLOWER.code),
                                DeleteRecordsPartitionResult()
                                    .setPartitionIndex(myTopicPartition4.partition)
                                    .setLowWatermark(DeleteRecordsResponse.INVALID_LOW_WATERMARK)
                                    .setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code)
                            ).iterator()
                        )
                    )
            )
            val t: MutableList<MetadataResponse.TopicMetadata> = ArrayList()
            val p: MutableList<PartitionMetadata> = ArrayList()
            p.add(
                PartitionMetadata(
                    error = Errors.NONE,
                    topicPartition = myTopicPartition0,
                    leaderId = nodes[0]!!.id,
                    leaderEpoch = 5,
                    replicaIds = listOf(nodes[0]!!.id),
                    inSyncReplicaIds = listOf(nodes[0]!!.id),
                    offlineReplicaIds = emptyList(),
                )
            )
            p.add(
                PartitionMetadata(
                    error = Errors.NONE,
                    topicPartition = myTopicPartition1,
                    leaderId = nodes[0]!!.id,
                    leaderEpoch = 5,
                    replicaIds = listOf(nodes[0]!!.id),
                    inSyncReplicaIds = listOf(nodes[0]!!.id),
                    offlineReplicaIds = emptyList(),
                )
            )
            p.add(
                PartitionMetadata(
                    error = Errors.LEADER_NOT_AVAILABLE,
                    topicPartition = myTopicPartition2,
                    leaderId = null,
                    leaderEpoch = null,
                    replicaIds = listOf(nodes.get(0)!!.id),
                    inSyncReplicaIds = listOf(nodes.get(0)!!.id),
                    offlineReplicaIds = emptyList(),
                )
            )
            p.add(
                PartitionMetadata(
                    error = Errors.NONE,
                    topicPartition = myTopicPartition3,
                    leaderId = nodes[0]!!.id,
                    leaderEpoch = 5,
                    replicaIds = listOf(nodes[0]!!.id),
                    inSyncReplicaIds = listOf(nodes[0]!!.id),
                    offlineReplicaIds = emptyList(),
                )
            )
            p.add(
                PartitionMetadata(
                    error = Errors.NONE,
                    topicPartition = myTopicPartition4,
                    leaderId = nodes[0]!!.id,
                    leaderEpoch = 5,
                    replicaIds = listOf(nodes[0]!!.id),
                    inSyncReplicaIds = listOf(nodes[0]!!.id),
                    offlineReplicaIds = emptyList()
                )
            )
            t.add(
                MetadataResponse.TopicMetadata(
                    error = Errors.NONE,
                    topic = "my_topic",
                    isInternal = false,
                    partitionMetadata = p,
                )
            )
            env.mockClient.prepareResponse(
                response = RequestTestUtils.metadataResponse(
                    brokers = cluster.nodes,
                    clusterId = cluster.clusterResource.clusterId,
                    controllerId = cluster.controller!!.id,
                    topicMetadataList = t,
                )
            )
            env.mockClient.prepareResponse(response = DeleteRecordsResponse(m))
            val recordsToDelete = mapOf(
                myTopicPartition0 to RecordsToDelete(beforeOffset = 3L),
                myTopicPartition1 to RecordsToDelete(beforeOffset = 10L),
                myTopicPartition2 to RecordsToDelete(beforeOffset = 10L),
                myTopicPartition3 to RecordsToDelete(beforeOffset = 10L),
                myTopicPartition4 to RecordsToDelete(beforeOffset = 10L),
            )
            val results = env.adminClient.deleteRecords(recordsToDelete)

            // success on records deletion for partition 0
            val values = results.lowWatermarks()
            val myTopicPartition0Result = values.get(myTopicPartition0)!!
            val lowWatermark = myTopicPartition0Result.get().lowWatermark()
            assertEquals(expected = lowWatermark, actual = 3)

            // "offset out of range" failure on records deletion for partition 1
            val myTopicPartition1Result = values[myTopicPartition1]!!
            try {
                myTopicPartition1Result.get()
                fail("get() should throw ExecutionException")
            } catch (e0: ExecutionException) {
                assertIs<OffsetOutOfRangeException>(e0.cause)
            }

            // "leader not available" failure on metadata request for partition 2
            val myTopicPartition2Result = values[myTopicPartition2]!!
            try {
                myTopicPartition2Result.get()
                fail("get() should throw ExecutionException")
            } catch (e1: ExecutionException) {
                assertIs<LeaderNotAvailableException>(e1.cause)
            }

            // "not leader for partition" failure on records deletion for partition 3
            val myTopicPartition3Result = values[myTopicPartition3]!!
            try {
                myTopicPartition3Result.get()
                fail("get() should throw ExecutionException")
            } catch (e1: ExecutionException) {
                assertTrue(e1.cause is NotLeaderOrFollowerException)
            }

            // "unknown topic or partition" failure on records deletion for partition 4
            val myTopicPartition4Result: KafkaFuture<DeletedRecords> =
                (values.get(myTopicPartition4))!!
            try {
                myTopicPartition4Result.get()
                fail("get() should throw ExecutionException")
            } catch (e1: ExecutionException) {
                assertTrue(e1.cause is UnknownTopicOrPartitionException)
            }
        }
    }

    @Test
    @Throws(Exception::class)
    fun testDescribeCluster() {
        AdminClientUnitTestEnv(
            mockCluster(4, 0),
            AdminClientConfig.RETRIES_CONFIG, "2"
        ).use { env ->
            env.mockClient.setNodeApiVersions(NodeApiVersions.create())

            // Prepare the describe cluster response used for the first describe cluster
            env.mockClient.prepareResponse(
                response = prepareDescribeClusterResponse(
                    throttleTimeMs = 0,
                    brokers = env.cluster.nodes,
                    clusterId = env.cluster.clusterResource.clusterId,
                    controllerId = 2,
                    clusterAuthorizedOperations = MetadataResponse.AUTHORIZED_OPERATIONS_OMITTED,
                ),
            )

            // Prepare the describe cluster response used for the second describe cluster
            env.mockClient.prepareResponse(
                response = prepareDescribeClusterResponse(
                    throttleTimeMs = 0,
                    brokers = env.cluster.nodes,
                    clusterId = env.cluster.clusterResource.clusterId,
                    controllerId = 3,
                    clusterAuthorizedOperations = (1 shl AclOperation.DESCRIBE.code.toInt()) or (1 shl AclOperation.ALTER.code.toInt())
                ),
            )

            // Test DescribeCluster with the authorized operations omitted.
            val result = env.adminClient.describeCluster()
            assertEquals(
                expected = env.cluster.clusterResource.clusterId,
                actual = result.clusterId.get()
            )
            assertEquals(
                expected = HashSet(env.cluster.nodes),
                actual = HashSet(result.nodes.get()),
            )
            assertEquals(expected = 2, actual = result.controller.get()!!.id)
            assertNull(result.authorizedOperations.get())

            // Test DescribeCluster with the authorized operations included.
            val result2 = env.adminClient.describeCluster()
            assertEquals(
                expected = env.cluster.clusterResource.clusterId,
                actual = result2.clusterId.get(),
            )
            assertEquals(
                expected = HashSet(env.cluster.nodes),
                actual = HashSet(result2.nodes.get())
            )
            assertEquals(3, result2.controller.get()!!.id)
            assertEquals(
                expected = HashSet(listOf(AclOperation.DESCRIBE, AclOperation.ALTER)),
                actual = result2.authorizedOperations.get(),
            )
        }
    }

    @Test
    fun testDescribeClusterHandleError() {
        AdminClientUnitTestEnv(
            mockCluster(4, 0),
            AdminClientConfig.RETRIES_CONFIG, "2"
        ).use { env ->
            env.mockClient.setNodeApiVersions(NodeApiVersions.create())

            // Prepare the describe cluster response used for the first describe cluster
            val errorMessage = "my error"
            env.mockClient.prepareResponse(
                response = DescribeClusterResponse(
                    DescribeClusterResponseData()
                        .setErrorCode(Errors.INVALID_REQUEST.code)
                        .setErrorMessage(errorMessage)
                ),
            )
            val result: DescribeClusterResult = env.adminClient.describeCluster()
            assertFutureThrows(
                future = result.clusterId,
                expectedCauseClassApiException = InvalidRequestException::class.java,
                expectedMessage = errorMessage,
            )
            assertFutureThrows(
                future = result.controller,
                expectedCauseClassApiException = InvalidRequestException::class.java,
                expectedMessage = errorMessage,
            )
            assertFutureThrows(
                future = result.nodes,
                expectedCauseClassApiException = InvalidRequestException::class.java,
                expectedMessage = errorMessage,
            )
            assertFutureThrows(
                future = result.authorizedOperations,
                expectedCauseClassApiException = InvalidRequestException::class.java,
                expectedMessage = errorMessage,
            )
        }
    }

    @Test
    @Throws(Exception::class)
    fun testDescribeClusterFailBack() {
        AdminClientUnitTestEnv(
            mockCluster(numNodes = 4, controllerIndex = 0),
            AdminClientConfig.RETRIES_CONFIG, "2",
        ).use { env ->
            env.mockClient.setNodeApiVersions(NodeApiVersions.create())

            // Reject the describe cluster request with an unsupported exception
            env.mockClient.prepareUnsupportedVersionResponse { request ->
                request is DescribeClusterRequest
            }

            // Prepare the metadata response used for the first describe cluster
            env.mockClient.prepareResponse(
                response = RequestTestUtils.metadataResponse(
                    throttleTimeMs = 0,
                    brokers = env.cluster.nodes,
                    clusterId = env.cluster.clusterResource.clusterId,
                    controllerId = 2,
                    topicMetadataList = emptyList(),
                    clusterAuthorizedOperations = MetadataResponse.AUTHORIZED_OPERATIONS_OMITTED,
                    responseVersion = ApiKeys.METADATA.latestVersion(),
                )
            )
            val result: DescribeClusterResult = env.adminClient.describeCluster()
            assertEquals(
                expected = env.cluster.clusterResource.clusterId,
                actual = result.clusterId.get(),
            )
            assertEquals(
                expected = HashSet(env.cluster.nodes),
                actual = HashSet(result.nodes.get()),
            )
            assertEquals(expected = 2, actual = result.controller.get()!!.id)
            assertNull(result.authorizedOperations.get())
        }
    }

    @Test
    @Throws(Exception::class)
    fun testListConsumerGroups() {
        AdminClientUnitTestEnv(
            mockCluster(4, 0),
            AdminClientConfig.RETRIES_CONFIG, "2"
        ).use { env ->
            env.mockClient.setNodeApiVersions(NodeApiVersions.create())

            // Empty metadata response should be retried
            env.mockClient.prepareResponse(
                response = RequestTestUtils.metadataResponse(
                    brokers = emptyList(),
                    clusterId = env.cluster.clusterResource.clusterId,
                    controllerId = -1,
                    topicMetadataList = emptyList(),
                )
            )
            env.mockClient.prepareResponse(
                response = RequestTestUtils.metadataResponse(
                    brokers = env.cluster.nodes,
                    clusterId = env.cluster.clusterResource.clusterId,
                    controllerId = env.cluster.controller!!.id,
                    topicMetadataList = emptyList(),
                )
            )
            env.mockClient.prepareResponseFrom(
                response = ListGroupsResponse(
                    ListGroupsResponseData()
                        .setErrorCode(Errors.NONE.code)
                        .setGroups(
                            listOf(
                                ListedGroup()
                                    .setGroupId("group-1")
                                    .setProtocolType(ConsumerProtocol.PROTOCOL_TYPE)
                                    .setGroupState("Stable"),
                                ListedGroup()
                                    .setGroupId("group-connect-1")
                                    .setProtocolType("connector")
                                    .setGroupState("Stable"),
                            )
                        )
                ),
                node = env.cluster.nodeById(0),
            )

            // handle retriable errors
            env.mockClient.prepareResponseFrom(
                response = ListGroupsResponse(
                    ListGroupsResponseData()
                        .setErrorCode(Errors.COORDINATOR_NOT_AVAILABLE.code)
                        .setGroups(emptyList())
                ),
                node = env.cluster.nodeById(1),
            )
            env.mockClient.prepareResponseFrom(
                response = ListGroupsResponse(
                    ListGroupsResponseData()
                        .setErrorCode(Errors.COORDINATOR_LOAD_IN_PROGRESS.code)
                        .setGroups(emptyList())
                ),
                node = env.cluster.nodeById(1),
            )
            env.mockClient.prepareResponseFrom(
                response = ListGroupsResponse(
                    ListGroupsResponseData()
                        .setErrorCode(Errors.NONE.code)
                        .setGroups(
                            listOf(
                                ListedGroup()
                                    .setGroupId("group-2")
                                    .setProtocolType(ConsumerProtocol.PROTOCOL_TYPE)
                                    .setGroupState("Stable"),
                                ListedGroup()
                                    .setGroupId("group-connect-2")
                                    .setProtocolType("connector")
                                    .setGroupState("Stable"),
                            )
                        )
                ),
                node = env.cluster.nodeById(1),
            )
            env.mockClient.prepareResponseFrom(
                response = ListGroupsResponse(
                    ListGroupsResponseData()
                        .setErrorCode(Errors.NONE.code)
                        .setGroups(
                            listOf(
                                ListedGroup()
                                    .setGroupId("group-3")
                                    .setProtocolType(ConsumerProtocol.PROTOCOL_TYPE)
                                    .setGroupState("Stable"),
                                ListedGroup()
                                    .setGroupId("group-connect-3")
                                    .setProtocolType("connector")
                                    .setGroupState("Stable")
                            )
                        )
                ),
                node = env.cluster.nodeById(2),
            )

            // fatal error
            env.mockClient.prepareResponseFrom(
                response = ListGroupsResponse(
                    ListGroupsResponseData()
                        .setErrorCode(Errors.UNKNOWN_SERVER_ERROR.code)
                        .setGroups(emptyList())
                ),
                node = env.cluster.nodeById(3),
            )
            val result: ListConsumerGroupsResult = env.adminClient.listConsumerGroups()
            assertFutureError(
                future = result.all,
                exceptionClass = UnknownServerException::class.java
            )
            val listings: Collection<ConsumerGroupListing> = result.valid().get()
            assertEquals(expected = 3, actual = listings.size)
            val groupIds: MutableSet<String> = HashSet()
            for (listing in listings) {
                groupIds.add(listing.groupId)
                assertNotNull(listing.state)
            }
            assertEquals(
                expected = setOf("group-1", "group-2", "group-3"),
                actual = groupIds,
            )
            assertEquals(expected = 1, actual = result.errors().get().size)
        }
    }

    @Test
    @Throws(Exception::class)
    fun testListConsumerGroupsMetadataFailure() {
        val cluster = mockCluster(3, 0)
        val time = MockTime()
        AdminClientUnitTestEnv(
            time,
            cluster,
            AdminClientConfig.RETRIES_CONFIG,
            "0",
        ).use { env ->
            env.mockClient.setNodeApiVersions(NodeApiVersions.create())

            // Empty metadata causes the request to fail since we have no list of brokers
            // to send the ListGroups requests to
            env.mockClient.prepareResponse(
                response = RequestTestUtils.metadataResponse(
                    brokers = emptyList(),
                    clusterId = env.cluster.clusterResource.clusterId,
                    controllerId = -1,
                    topicMetadataList = emptyList(),
                ),
            )
            val result = env.adminClient.listConsumerGroups()
            assertFutureError(
                future = result.all,
                exceptionClass = KafkaException::class.java,
            )
        }
    }

    @Test
    @Throws(Exception::class)
    fun testListConsumerGroupsWithStates() {
        AdminClientUnitTestEnv(mockCluster(1, 0)).use { env ->
            env.mockClient.setNodeApiVersions(NodeApiVersions.create())
            env.mockClient.prepareResponse(
                response = prepareMetadataResponse(env.cluster, Errors.NONE),
            )
            env.mockClient.prepareResponseFrom(
                response = ListGroupsResponse(
                    ListGroupsResponseData()
                        .setErrorCode(Errors.NONE.code)
                        .setGroups(
                            listOf(
                                ListedGroup()
                                    .setGroupId("group-1")
                                    .setProtocolType(ConsumerProtocol.PROTOCOL_TYPE)
                                    .setGroupState("Stable"),
                                ListedGroup()
                                    .setGroupId("group-2")
                                    .setGroupState("Empty"),
                            )
                        ),
                ),
                node = env.cluster.nodeById(0),
            )
            val options = ListConsumerGroupsOptions()
            val result = env.adminClient.listConsumerGroups(options)
            val listings = result.valid().get()
            assertEquals(2, listings.size)
            val expected: MutableList<ConsumerGroupListing> = ArrayList()
            expected.add(
                ConsumerGroupListing(
                    groupId = "group-2",
                    isSimpleConsumerGroup = true,
                    state = ConsumerGroupState.EMPTY,
                )
            )
            expected.add(
                ConsumerGroupListing(
                    groupId = "group-1",
                    isSimpleConsumerGroup = false,
                    state = ConsumerGroupState.STABLE,
                )
            )
            assertEquals(expected = expected, actual = listings)
            assertEquals(expected = 0, actual = result.errors().get().size)
        }
    }

    @Test
    @Throws(Exception::class)
    fun testListConsumerGroupsWithStatesOlderBrokerVersion() {
        val listGroupV3 = ApiVersionsResponseData.ApiVersion()
            .setApiKey(ApiKeys.LIST_GROUPS.id)
            .setMinVersion(0.toShort())
            .setMaxVersion(3.toShort())
        AdminClientUnitTestEnv(mockCluster(1, 0)).use { env ->
            env.mockClient
                .setNodeApiVersions(NodeApiVersions.create(listOf(listGroupV3)))
            env.mockClient
                .prepareResponse(response = prepareMetadataResponse(env.cluster, Errors.NONE))

            // Check we can list groups with older broker if we don't specify states
            env.mockClient.prepareResponseFrom(
                response = ListGroupsResponse(
                    ListGroupsResponseData()
                        .setErrorCode(Errors.NONE.code)
                        .setGroups(
                            listOf(
                                ListedGroup()
                                    .setGroupId("group-1")
                                    .setProtocolType(ConsumerProtocol.PROTOCOL_TYPE)
                            )
                        )
                ),
                node = env.cluster.nodeById(0),
            )
            var options: ListConsumerGroupsOptions? = ListConsumerGroupsOptions()
            var result: ListConsumerGroupsResult = env.adminClient.listConsumerGroups((options)!!)
            val listing: Collection<ConsumerGroupListing> = result.all.get()
            assertEquals(1, listing.size)
            val expected = listOf(
                ConsumerGroupListing(
                    groupId = "group-1",
                    isSimpleConsumerGroup = false,
                    state = null,
                )
            )
            assertEquals(expected, listing)

            // But we cannot set a state filter with older broker
            env.mockClient.prepareResponse(
                response = prepareMetadataResponse(env.cluster, Errors.NONE),
            )
            env.mockClient.prepareUnsupportedVersionResponse { body ->
                body is ListGroupsRequest
            }
            options = ListConsumerGroupsOptions().inStates(
                setOf(ConsumerGroupState.STABLE)
            )
            result = env.adminClient.listConsumerGroups(options)
            assertFutureThrows(
                future = result.all,
                exceptionCauseClass = UnsupportedVersionException::class.java,
            )
        }
    }

    @Test
    @Throws(Exception::class)
    fun testOffsetCommitNumRetries() {
        val cluster = mockCluster(numNodes = 3, controllerIndex = 0)
        val time = MockTime()
        AdminClientUnitTestEnv(
            time,
            cluster,
            AdminClientConfig.RETRIES_CONFIG,
            "0",
        ).use { env ->
            env.mockClient.setNodeApiVersions(NodeApiVersions.create())
            val tp1 = TopicPartition("foo", 0)
            env.mockClient
                .prepareResponse(
                    response = prepareFindCoordinatorResponse(
                        error = Errors.NONE,
                        node = env.cluster.controller!!,
                    )
                )
            env.mockClient
                .prepareResponse(
                    response = prepareOffsetCommitResponse(
                        tp = tp1,
                        error = Errors.NOT_COORDINATOR
                    )
                )
            env.mockClient
                .prepareResponse(
                    response = prepareFindCoordinatorResponse(
                        error = Errors.NONE,
                        node = env.cluster.controller!!
                    )
                )
            val offsets: MutableMap<TopicPartition, OffsetAndMetadata> = HashMap()
            offsets[tp1] = OffsetAndMetadata(123L)
            val result = env.adminClient.alterConsumerGroupOffsets(GROUP_ID, offsets)
            assertFutureError(
                future = result.all(),
                exceptionClass = TimeoutException::class.java,
            )
        }
    }

    @Test
    @Throws(Exception::class)
    fun testOffsetCommitWithMultipleErrors() {
        val cluster = mockCluster(numNodes = 3, controllerIndex = 0)
        val time = MockTime()
        AdminClientUnitTestEnv(
            time, cluster,
            AdminClientConfig.RETRIES_CONFIG, "0"
        ).use { env ->
            env.mockClient.setNodeApiVersions(NodeApiVersions.create())
            val foo0 = TopicPartition("foo", 0)
            val foo1 = TopicPartition("foo", 1)
            env.mockClient.prepareResponse(
                response = prepareFindCoordinatorResponse(
                    error = Errors.NONE,
                    node = env.cluster.controller!!,
                ),
            )
            val responseData: MutableMap<TopicPartition, Errors> = HashMap()
            responseData[foo0] = Errors.NONE
            responseData[foo1] = Errors.UNKNOWN_TOPIC_OR_PARTITION
            env.mockClient.prepareResponse(
                response = OffsetCommitResponse(
                    requestThrottleMs = 0,
                    responseData = responseData
                ),
            )
            val offsets: MutableMap<TopicPartition, OffsetAndMetadata> = HashMap()
            offsets[foo0] = OffsetAndMetadata(123L)
            offsets[foo1] = OffsetAndMetadata(456L)
            val result = env.adminClient.alterConsumerGroupOffsets(GROUP_ID, offsets)
            assertNotFails(result.partitionResult(foo0)::get)
            assertFutureError(
                future = result.partitionResult(foo1),
                exceptionClass = UnknownTopicOrPartitionException::class.java,
            )
            assertFutureError(
                future = result.all(),
                exceptionClass = UnknownTopicOrPartitionException::class.java,
            )
        }
    }

    @Test
    @Throws(Exception::class)
    fun testOffsetCommitRetryBackoff() {
        val time = MockTime()
        val retryBackoff = 100
        AdminClientUnitTestEnv(
            time,
            mockCluster(numNodes = 3, controllerIndex = 0),
            newStrMap(AdminClientConfig.RETRY_BACKOFF_MS_CONFIG, "" + retryBackoff)
        ).use { env ->
            val mockClient = env.mockClient
            mockClient.setNodeApiVersions(NodeApiVersions.create())
            val firstAttemptTime = AtomicLong(0)
            val secondAttemptTime = AtomicLong(0)
            val tp1 = TopicPartition("foo", 0)
            mockClient.prepareResponse(
                response = prepareFindCoordinatorResponse(Errors.NONE, env.cluster.controller!!)
            )
            mockClient.prepareResponse(
                matcher = { _ ->
                    firstAttemptTime.set(time.milliseconds())
                    true
                },
                response = prepareOffsetCommitResponse(tp1, Errors.NOT_COORDINATOR)
            )
            mockClient.prepareResponse(
                response = prepareFindCoordinatorResponse(Errors.NONE, env.cluster.controller!!)
            )
            mockClient.prepareResponse(
                matcher = { body ->
                    secondAttemptTime.set(time.milliseconds())
                    true
                },
                response = prepareOffsetCommitResponse(tp1, Errors.NONE)
            )
            val offsets: MutableMap<TopicPartition, OffsetAndMetadata> = HashMap()
            offsets[tp1] = OffsetAndMetadata(123L)
            val future = env.adminClient
                .alterConsumerGroupOffsets(GROUP_ID, offsets)
                .all()
            waitForCondition(
                testCondition = { mockClient.numAwaitingResponses() == 1 },
                conditionDetails = "Failed awaiting CommitOffsets first request failure",
            )
            waitForCondition(
                testCondition = { env.adminClient.numPendingCalls() == 1 },
                conditionDetails = "Failed to add retry CommitOffsets call on first failure",
            )
            time.sleep(retryBackoff.toLong())
            future.get()
            val actualRetryBackoff = secondAttemptTime.get() - firstAttemptTime.get()
            assertEquals(
                expected = retryBackoff.toLong(),
                actual = actualRetryBackoff,
                message = "CommitOffsets retry did not await expected backoff",
            )
        }
    }

    @Test
    @Throws(Exception::class)
    fun testDescribeConsumerGroupNumRetries() {
        val cluster = mockCluster(numNodes = 3, controllerIndex = 0)
        val time = MockTime()
        AdminClientUnitTestEnv(
            time,
            cluster,
            AdminClientConfig.RETRIES_CONFIG,
            "0"
        ).use { env ->
            env.mockClient.setNodeApiVersions(NodeApiVersions.create())
            env.mockClient.prepareResponse(
                response = prepareFindCoordinatorResponse(Errors.NONE, env.cluster.controller!!)
            )
            val data = DescribeGroupsResponseData()
            data.groups += DescribeGroupsResponse.groupMetadata(
                groupId = GROUP_ID,
                error = Errors.NOT_COORDINATOR,
                state = "",
                protocolType = "",
                protocol = "",
                members = emptyList(),
                authorizedOperations = emptySet(),
            )
            env.mockClient.prepareResponse(response = DescribeGroupsResponse(data))
            env.mockClient.prepareResponse(
                response = prepareFindCoordinatorResponse(Errors.NONE, env.cluster.controller!!),
            )
            val result = env.adminClient.describeConsumerGroups(listOf(GROUP_ID))
            assertFutureError(
                future = result.all(),
                exceptionClass = TimeoutException::class.java
            )
        }
    }

    @Test
    @Throws(Exception::class)
    fun testDescribeConsumerGroupRetryBackoff() {
        val time = MockTime()
        val retryBackoff = 100
        AdminClientUnitTestEnv(
            time,
            mockCluster(numNodes = 3, controllerIndex = 0),
            newStrMap(AdminClientConfig.RETRY_BACKOFF_MS_CONFIG, "" + retryBackoff)
        ).use { env ->
            val mockClient = env.mockClient
            mockClient.setNodeApiVersions(NodeApiVersions.create())
            val firstAttemptTime = AtomicLong(0)
            val secondAttemptTime = AtomicLong(0)
            mockClient.prepareResponse(
                response = prepareFindCoordinatorResponse(Errors.NONE, env.cluster.controller!!),
            )
            var data = DescribeGroupsResponseData()
            data.groups += DescribeGroupsResponse.groupMetadata(
                groupId = GROUP_ID,
                error = Errors.NOT_COORDINATOR,
                state = "",
                protocolType = "",
                protocol = "",
                members = emptyList(),
                authorizedOperations = emptySet(),
            )
            mockClient.prepareResponse(
                matcher = {
                    firstAttemptTime.set(time.milliseconds())
                    true
                },
                response = DescribeGroupsResponse(data),
            )
            mockClient.prepareResponse(
                response = prepareFindCoordinatorResponse(Errors.NONE, env.cluster.controller!!),
            )
            data = DescribeGroupsResponseData()
            data.groups += DescribeGroupsResponse.groupMetadata(
                groupId = GROUP_ID,
                error = Errors.NONE,
                state = "",
                protocolType = ConsumerProtocol.PROTOCOL_TYPE,
                protocol = "",
                members = emptyList(),
                authorizedOperations = emptySet(),
            )

            mockClient.prepareResponse(
                matcher = { body ->
                    secondAttemptTime.set(time.milliseconds())
                    true
                },
                response = DescribeGroupsResponse(data),
            )
            val future = env.adminClient
                .describeConsumerGroups(listOf(GROUP_ID))
                .all()
            waitForCondition(
                testCondition = { mockClient.numAwaitingResponses() == 1 },
                conditionDetails = "Failed awaiting DescribeConsumerGroup first request failure",
            )
            waitForCondition(
                testCondition = { env.adminClient.numPendingCalls() == 1 },
                conditionDetails = "Failed to add retry DescribeConsumerGroup call on first failure",
            )
            time.sleep(retryBackoff.toLong())
            future.get()
            val actualRetryBackoff = secondAttemptTime.get() - firstAttemptTime.get()
            assertEquals(
                expected = retryBackoff.toLong(),
                actual = actualRetryBackoff,
                message = "DescribeConsumerGroup retry did not await expected backoff!"
            )
        }
    }

    @Test
    @Throws(Exception::class)
    fun testDescribeConsumerGroups() {
        AdminClientUnitTestEnv(mockCluster(numNodes = 1, controllerIndex = 0)).use { env ->
            env.mockClient.setNodeApiVersions(NodeApiVersions.create())

            // Retriable FindCoordinatorResponse errors should be retried
            env.mockClient.prepareResponse(
                response = prepareFindCoordinatorResponse(
                    error = Errors.COORDINATOR_NOT_AVAILABLE,
                    node = Node.noNode(),
                )
            )
            env.mockClient.prepareResponse(
                response = prepareFindCoordinatorResponse(
                    error = Errors.COORDINATOR_LOAD_IN_PROGRESS,
                    node = Node.noNode(),
                )
            )
            env.mockClient.prepareResponse(
                response = prepareFindCoordinatorResponse(Errors.NONE, env.cluster.controller!!)
            )
            var data = DescribeGroupsResponseData()

            //Retriable errors should be retried
            data.groups += DescribeGroupsResponse.groupMetadata(
                groupId = GROUP_ID,
                error = Errors.COORDINATOR_LOAD_IN_PROGRESS,
                state = "",
                protocolType = "",
                protocol = "",
                members = emptyList(),
                authorizedOperations = emptySet(),
            )
            env.mockClient.prepareResponse(response = DescribeGroupsResponse(data))

            /*
             * We need to return two responses here, one with NOT_COORDINATOR error when calling
             * describe consumer group api using coordinator that has moved. This will retry whole
             *  operation. So we need to again respond with a FindCoordinatorResponse.
             *
             * And the same reason for COORDINATOR_NOT_AVAILABLE error response
             */
            data = DescribeGroupsResponseData()
            data.groups += DescribeGroupsResponse.groupMetadata(
                groupId = GROUP_ID,
                error = Errors.NOT_COORDINATOR,
                state = "",
                protocolType = "",
                protocol = "",
                members = emptyList(),
                authorizedOperations = emptySet(),
            )
            env.mockClient.prepareResponse(response = DescribeGroupsResponse(data))
            env.mockClient.prepareResponse(
                response = prepareFindCoordinatorResponse(
                    error = Errors.NONE,
                    node = env.cluster.controller!!,
                )
            )
            data = DescribeGroupsResponseData()
            data.groups += DescribeGroupsResponse.groupMetadata(
                groupId = GROUP_ID,
                error = Errors.COORDINATOR_NOT_AVAILABLE,
                state = "",
                protocolType = "",
                protocol = "",
                members = emptyList(),
                authorizedOperations = emptySet()
            )
            env.mockClient.prepareResponse(response = DescribeGroupsResponse(data))
            env.mockClient.prepareResponse(
                response = prepareFindCoordinatorResponse(
                    error = Errors.NONE,
                    node = env.cluster.controller!!,
                )
            )
            data = DescribeGroupsResponseData()
            val topicPartitions = listOf(
                TopicPartition("my_topic", 0),
                TopicPartition("my_topic", 1),
                TopicPartition("my_topic", 2),
            )
            val memberAssignment = serializeAssignment(
                ConsumerPartitionAssignor.Assignment(topicPartitions)
            )
            val memberAssignmentBytes = ByteArray(memberAssignment.remaining())
            memberAssignment.get(memberAssignmentBytes)
            val memberOne = DescribeGroupsResponse.groupMember(
                memberId = "0",
                groupInstanceId = "instance1",
                clientId = "clientId0",
                clientHost = "clientHost",
                assignment = memberAssignmentBytes,
                metadata = Bytes.EMPTY,
            )
            val memberTwo = DescribeGroupsResponse.groupMember(
                memberId = "1",
                groupInstanceId = "instance2",
                clientId = "clientId1",
                clientHost = "clientHost",
                assignment = memberAssignmentBytes,
                metadata = Bytes.EMPTY,
            )
            val expectedMemberDescriptions = listOf(
                convertToMemberDescriptions(
                    member = memberOne,
                    assignment = MemberAssignment(topicPartitions.toSet()),
                ),
                convertToMemberDescriptions(
                    member = memberTwo,
                    assignment = MemberAssignment(topicPartitions.toSet()),
                ),
            )
            data.groups += DescribeGroupsResponse.groupMetadata(
                groupId = GROUP_ID,
                error = Errors.NONE,
                state = "",
                protocolType = ConsumerProtocol.PROTOCOL_TYPE,
                protocol = "",
                members = listOf(memberOne, memberTwo),
                authorizedOperations = emptySet(),
            )
            env.mockClient.prepareResponse(response = DescribeGroupsResponse(data))
            val result = env.adminClient.describeConsumerGroups(listOf(GROUP_ID))
            val groupDescription = result.describedGroups()[GROUP_ID]!!.get()
            assertEquals(expected = 1, actual = result.describedGroups().size)
            assertEquals(expected = GROUP_ID, actual = groupDescription.groupId)
            assertEquals(expected = 2, actual = groupDescription.members.size)
            assertEquals(expected = expectedMemberDescriptions, actual = groupDescription.members)
        }
    }

    @Test
    fun testDescribeMultipleConsumerGroups() {
        AdminClientUnitTestEnv(mockCluster(numNodes = 1, controllerIndex = 0)).use { env ->
            env.mockClient.setNodeApiVersions(NodeApiVersions.create())
            env.mockClient.prepareResponse(
                response = prepareFindCoordinatorResponse(
                    error = Errors.NONE,
                    node = env.cluster.controller!!
                )
            )
            val topicPartitions = listOf(
                TopicPartition("my_topic", 0),
                TopicPartition("my_topic", 1),
                TopicPartition("my_topic", 2),
            )
            val memberAssignment = serializeAssignment(
                ConsumerPartitionAssignor.Assignment(topicPartitions)
            )
            val memberAssignmentBytes = ByteArray(memberAssignment.remaining())
            memberAssignment.get(memberAssignmentBytes)
            val group0Data = DescribeGroupsResponseData()
            group0Data.groups += DescribeGroupsResponse.groupMetadata(
                groupId = GROUP_ID,
                error = Errors.NONE,
                state = "",
                protocolType = ConsumerProtocol.PROTOCOL_TYPE,
                protocol = "",
                members = listOf(
                    DescribeGroupsResponse.groupMember(
                        memberId = "0",
                        groupInstanceId = null,
                        clientId = "clientId0",
                        clientHost = "clientHost",
                        assignment = memberAssignmentBytes,
                        metadata = Bytes.EMPTY,
                    ),
                    DescribeGroupsResponse.groupMember(
                        memberId = "1",
                        groupInstanceId = null,
                        clientId = "clientId1",
                        clientHost = "clientHost",
                        assignment = memberAssignmentBytes,
                        metadata = Bytes.EMPTY,
                    )
                ),
                authorizedOperations = emptySet(),
            )
            val groupConnectData = DescribeGroupsResponseData()
            group0Data.groups += DescribeGroupsResponse.groupMetadata(
                groupId = "group-connect-0",
                error = Errors.NONE,
                state = "",
                protocolType = "connect",
                protocol = "",
                members = listOf(
                    DescribeGroupsResponse.groupMember(
                        memberId = "0",
                        groupInstanceId = null,
                        clientId = "clientId0",
                        clientHost = "clientHost",
                        assignment = memberAssignmentBytes,
                        metadata = Bytes.EMPTY,
                    ),
                    DescribeGroupsResponse.groupMember(
                        memberId = "1",
                        groupInstanceId = null,
                        clientId = "clientId1",
                        clientHost = "clientHost",
                        assignment = memberAssignmentBytes,
                        metadata = Bytes.EMPTY,
                    )
                ),
                authorizedOperations = emptySet(),
            )
            env.mockClient.prepareResponse(response = DescribeGroupsResponse(group0Data))
            env.mockClient.prepareResponse(response = DescribeGroupsResponse(groupConnectData))
            val groups = setOf(GROUP_ID, "group-connect-0")
            val result = env.adminClient.describeConsumerGroups(groups)
            assertEquals(expected = 2, actual = result.describedGroups().size)
            assertEquals(expected = groups, actual = result.describedGroups().keys)
        }
    }

    @Test
    @Throws(Exception::class)
    fun testDescribeConsumerGroupsWithAuthorizedOperationsOmitted() {
        AdminClientUnitTestEnv(mockCluster(numNodes = 1, controllerIndex = 0)).use { env ->
            env.mockClient.setNodeApiVersions(NodeApiVersions.create())
            env.mockClient.prepareResponse(
                response = prepareFindCoordinatorResponse(
                    error = Errors.NONE,
                    node = env.cluster.controller!!,
                )
            )
            val data = DescribeGroupsResponseData()
            data.groups += DescribeGroupsResponse.groupMetadata(
                groupId = GROUP_ID,
                error = Errors.NONE,
                state = "",
                protocolType = ConsumerProtocol.PROTOCOL_TYPE,
                protocol = "",
                members = emptyList(),
                authorizedOperations = MetadataResponse.AUTHORIZED_OPERATIONS_OMITTED,
            )
            env.mockClient.prepareResponse(response = DescribeGroupsResponse(data))
            val result = env.adminClient.describeConsumerGroups(listOf(GROUP_ID))
            val groupDescription = result.describedGroups()[GROUP_ID]!!.get()
            assertEquals(emptySet(), groupDescription.authorizedOperations)
        }
    }

    @Test
    @Throws(Exception::class)
    fun testDescribeNonConsumerGroups() {
        AdminClientUnitTestEnv(mockCluster(numNodes = 1, controllerIndex = 0)).use { env ->
            env.mockClient.setNodeApiVersions(NodeApiVersions.create())
            env.mockClient.prepareResponse(
                response = prepareFindCoordinatorResponse(
                    error = Errors.NONE,
                    node = env.cluster.controller!!,
                )
            )
            val data = DescribeGroupsResponseData()
            data.groups += DescribeGroupsResponse.groupMetadata(
                groupId = GROUP_ID,
                error = Errors.NONE,
                state = "",
                protocolType = "non-consumer",
                protocol = "",
                members = mutableListOf(),
                authorizedOperations = emptySet(),
            )
            env.mockClient.prepareResponse(response = DescribeGroupsResponse(data))
            val result = env.adminClient.describeConsumerGroups(listOf(GROUP_ID))
            assertFutureError(
                future = result.describedGroups()[GROUP_ID]!!,
                exceptionClass = IllegalArgumentException::class.java,
            )
        }
    }

    @Test
    @Throws(Exception::class)
    fun testListConsumerGroupOffsetsOptionsWithUnbatchedApi() {
        verifyListConsumerGroupOffsetsOptions(false)
    }

    @Test
    @Throws(Exception::class)
    fun testListConsumerGroupOffsetsOptionsWithBatchedApi() {
        verifyListConsumerGroupOffsetsOptions(true)
    }

    @Suppress("Deprecation")
    @Throws(Exception::class)
    private fun verifyListConsumerGroupOffsetsOptions(batchedApi: Boolean) {
        val cluster = mockCluster(numNodes = 3, controllerIndex = 0)
        val time = MockTime()
        AdminClientUnitTestEnv(
            time,
            cluster,
            AdminClientConfig.RETRIES_CONFIG,
            "0",
        ).use { env ->
            env.mockClient.setNodeApiVersions(NodeApiVersions.create())
            env.mockClient.prepareResponse(
                response = prepareFindCoordinatorResponse(Errors.NONE, env.cluster.controller!!)
            )
            val partitions = listOf(TopicPartition(topic = "A", partition = 0))
            val options = ListConsumerGroupOffsetsOptions().apply {
                requireStable = true
                timeoutMs = 300
            }
            if (batchedApi) {
                val groupSpec = ListConsumerGroupOffsetsSpec().apply {
                    topicPartitions = partitions
                }
                env.adminClient.listConsumerGroupOffsets(
                    groupSpecs = mapOf(GROUP_ID to groupSpec),
                    options = options,
                )
            } else env.adminClient.listConsumerGroupOffsets(
                groupId = GROUP_ID,
                options = options.apply { topicPartitions = partitions },
            )

            val mockClient = env.mockClient
            waitForRequest(mockClient, ApiKeys.OFFSET_FETCH)
            val clientRequest = mockClient.requests().peek()
            assertNotNull(clientRequest)
            assertEquals(expected = 300, actual = clientRequest.requestTimeoutMs)
            val data = (clientRequest.requestBuilder() as OffsetFetchRequest.Builder).data
            assertTrue(data.requireStable)
            assertEquals(
                expected = listOf(GROUP_ID),
                actual = data.groups.map(OffsetFetchRequestGroup::groupId),
            )
            assertEquals(
                expected = listOf("A"),
                actual = data.groups[0].topics.map(OffsetFetchRequestTopics::name),
            )
            assertContentEquals(
                expected = intArrayOf(0),
                actual = data.groups[0].topics[0].partitionIndexes,
            )
        }
    }

    @Test
    @Throws(Exception::class)
    fun testListConsumerGroupOffsetsNumRetries() {
        val cluster = mockCluster(numNodes = 3, controllerIndex = 0)
        val time = MockTime()
        AdminClientUnitTestEnv(
            time,
            cluster,
            AdminClientConfig.RETRIES_CONFIG,
            "0",
        ).use { env ->
            env.mockClient.setNodeApiVersions(NodeApiVersions.create())
            env.mockClient.prepareResponse(
                response = prepareFindCoordinatorResponse(Errors.NONE, env.cluster.controller!!),
            )
            env.mockClient.prepareResponse(
                response = offsetFetchResponse(Errors.NOT_COORDINATOR, emptyMap()),
            )
            env.mockClient.prepareResponse(
                response = prepareFindCoordinatorResponse(Errors.NONE, env.cluster.controller!!),
            )
            val result = env.adminClient.listConsumerGroupOffsets(GROUP_ID)
            assertFutureError(
                future = result.partitionsToOffsetAndMetadata(),
                exceptionClass = TimeoutException::class.java,
            )
        }
    }

    @Test
    @Throws(Exception::class)
    fun testListConsumerGroupOffsetsRetryBackoff() {
        val time = MockTime()
        val retryBackoff = 100
        AdminClientUnitTestEnv(
            time,
            mockCluster(numNodes = 3, controllerIndex = 0),
            newStrMap(AdminClientConfig.RETRY_BACKOFF_MS_CONFIG, "" + retryBackoff)
        ).use { env ->
            val mockClient = env.mockClient
            mockClient.setNodeApiVersions(NodeApiVersions.create())
            val firstAttemptTime = AtomicLong(0)
            val secondAttemptTime = AtomicLong(0)
            mockClient.prepareResponse(
                response = prepareFindCoordinatorResponse(Errors.NONE, env.cluster.controller!!),
            )
            mockClient.prepareResponse(
                matcher = {
                    firstAttemptTime.set(time.milliseconds())
                    true
                },
                response = offsetFetchResponse(Errors.NOT_COORDINATOR, emptyMap()),
            )
            mockClient.prepareResponse(
                response = prepareFindCoordinatorResponse(Errors.NONE, env.cluster.controller!!),
            )
            mockClient.prepareResponse(
                matcher = { body ->
                    secondAttemptTime.set(time.milliseconds())
                    true
                },
                response = offsetFetchResponse(Errors.NONE, emptyMap()),
            )
            val future = env.adminClient
                .listConsumerGroupOffsets(GROUP_ID)
                .partitionsToOffsetAndMetadata()
            waitForCondition(
                testCondition = { mockClient.numAwaitingResponses() == 1 },
                conditionDetails = "Failed awaiting ListConsumerGroupOffsets first request failure",
            )
            waitForCondition(
                testCondition = { env.adminClient.numPendingCalls() == 1 },
                conditionDetails = "Failed to add retry ListConsumerGroupOffsets call on first failure",
            )
            time.sleep(retryBackoff.toLong())
            future.get()
            val actualRetryBackoff = secondAttemptTime.get() - firstAttemptTime.get()
            assertEquals(
                expected = retryBackoff.toLong(),
                actual = actualRetryBackoff,
                message = "ListConsumerGroupOffsets retry did not await expected backoff!",
            )
        }
    }

    @Test
    @Throws(Exception::class)
    fun testListConsumerGroupOffsetsRetriableErrors() {
        // Retriable errors should be retried
        AdminClientUnitTestEnv(mockCluster(numNodes = 1, controllerIndex = 0)).use { env ->
            env.mockClient.setNodeApiVersions(NodeApiVersions.create())
            env.mockClient.prepareResponse(
                response = prepareFindCoordinatorResponse(Errors.NONE, env.cluster.controller!!),
            )
            env.mockClient.prepareResponse(
                response = offsetFetchResponse(Errors.COORDINATOR_LOAD_IN_PROGRESS, emptyMap()),
            )

            /*
             * We need to return two responses here, one for NOT_COORDINATOR call when calling list
             * consumer offsets api using coordinator that has moved. This will retry whole
             * operation. So we need to again respond with a FindCoordinatorResponse.
             *
             * And the same reason for the following COORDINATOR_NOT_AVAILABLE error response
             */
            env.mockClient.prepareResponse(
                response = offsetFetchResponse(Errors.NOT_COORDINATOR, emptyMap()),
            )
            env.mockClient.prepareResponse(
                response = prepareFindCoordinatorResponse(Errors.NONE, env.cluster.controller!!),
            )
            env.mockClient.prepareResponse(
                response = offsetFetchResponse(Errors.COORDINATOR_NOT_AVAILABLE, emptyMap()),
            )
            env.mockClient.prepareResponse(
                response = prepareFindCoordinatorResponse(Errors.NONE, env.cluster.controller!!),
            )
            env.mockClient.prepareResponse(
                response = offsetFetchResponse(Errors.NONE, emptyMap()),
            )
            val errorResult1 = env.adminClient.listConsumerGroupOffsets(GROUP_ID)
            assertEquals(
                expected = emptyMap<TopicPartition, OffsetAndMetadata?>(),
                actual = errorResult1.partitionsToOffsetAndMetadata().get(),
            )
        }
    }

    @Test
    @Throws(Exception::class)
    fun testListConsumerGroupOffsetsNonRetriableErrors() {
        // Non-retriable errors throw an exception
        val nonRetriableErrors = listOf(
            Errors.GROUP_AUTHORIZATION_FAILED,
            Errors.INVALID_GROUP_ID,
            Errors.GROUP_ID_NOT_FOUND,
        )
        AdminClientUnitTestEnv(mockCluster(numNodes = 1, controllerIndex = 0)).use { env ->
            env.mockClient.setNodeApiVersions(NodeApiVersions.create())
            for (error in nonRetriableErrors) {
                env.mockClient.prepareResponse(
                    prepareFindCoordinatorResponse(Errors.NONE, env.cluster.controller!!),
                )
                env.mockClient.prepareResponse(
                    response = offsetFetchResponse(error, emptyMap()),
                )
                val errorResult = env.adminClient.listConsumerGroupOffsets(GROUP_ID)
                assertFutureError(
                    future = errorResult.partitionsToOffsetAndMetadata(),
                    exceptionClass = error.exception!!.javaClass,
                )
            }
        }
    }

    @Test
    @Throws(Exception::class)
    fun testListConsumerGroupOffsets() {
        AdminClientUnitTestEnv(mockCluster(numNodes = 1, controllerIndex = 0)).use { env ->
            env.mockClient.setNodeApiVersions(NodeApiVersions.create())

            // Retriable FindCoordinatorResponse errors should be retried
            env.mockClient.prepareResponse(
                response = prepareFindCoordinatorResponse(
                    error = Errors.COORDINATOR_NOT_AVAILABLE,
                    node = Node.noNode(),
                )
            )
            env.mockClient.prepareResponse(
                response = prepareFindCoordinatorResponse(Errors.NONE, env.cluster.controller!!),
            )

            // Retriable errors should be retried
            env.mockClient.prepareResponse(
                response = offsetFetchResponse(Errors.COORDINATOR_LOAD_IN_PROGRESS, emptyMap()),
            )

            /*
             * We need to return two responses here, one for NOT_COORDINATOR error when calling list
             * consumer group offsets api using coordinator that has moved. This will retry whole
             * operation. So we need to again respond with a FindCoordinatorResponse.
             *
             * And the same reason for the following COORDINATOR_NOT_AVAILABLE error response
             */
            env.mockClient.prepareResponse(
                response = offsetFetchResponse(Errors.NOT_COORDINATOR, emptyMap()),
            )
            env.mockClient.prepareResponse(
                response = prepareFindCoordinatorResponse(Errors.NONE, env.cluster.controller!!),
            )
            env.mockClient.prepareResponse(
                response = offsetFetchResponse(Errors.COORDINATOR_NOT_AVAILABLE, emptyMap()),
            )
            env.mockClient.prepareResponse(
                response = prepareFindCoordinatorResponse(Errors.NONE, env.cluster.controller!!),
            )
            val myTopicPartition0 = TopicPartition(topic = "my_topic", partition = 0)
            val myTopicPartition1 = TopicPartition(topic = "my_topic", partition = 1)
            val myTopicPartition2 = TopicPartition(topic = "my_topic", partition = 2)
            val myTopicPartition3 = TopicPartition(topic = "my_topic", partition = 3)
            val responseData = mapOf(
                myTopicPartition0 to OffsetFetchResponse.PartitionData(
                    offset = 10,
                    leaderEpoch = null,
                    metadata = "",
                    error = Errors.NONE,
                ),
                myTopicPartition1 to OffsetFetchResponse.PartitionData(
                    offset = 0,
                    leaderEpoch = null,
                    metadata = "",
                    error = Errors.NONE,
                ),
                myTopicPartition2 to OffsetFetchResponse.PartitionData(
                    offset = 20,
                    leaderEpoch = null,
                    metadata = "",
                    error = Errors.NONE,
                ),
                myTopicPartition3 to OffsetFetchResponse.PartitionData(
                    offset = OffsetFetchResponse.INVALID_OFFSET,
                    leaderEpoch = null,
                    metadata = "",
                    error = Errors.NONE,
                )
            )
            env.mockClient.prepareResponse(
                response = offsetFetchResponse(Errors.NONE, responseData),
            )
            val result = env.adminClient.listConsumerGroupOffsets(GROUP_ID)
            val partitionToOffsetAndMetadata = result.partitionsToOffsetAndMetadata().get()
            assertEquals(expected = 4, actual = partitionToOffsetAndMetadata.size)
            assertEquals(
                expected = 10,
                actual = partitionToOffsetAndMetadata[myTopicPartition0]!!.offset,
            )
            assertEquals(
                expected = 0,
                actual = partitionToOffsetAndMetadata[myTopicPartition1]!!.offset,
            )
            assertEquals(
                expected = 20,
                actual = partitionToOffsetAndMetadata[myTopicPartition2]!!.offset,
            )
            assertTrue(partitionToOffsetAndMetadata.containsKey(myTopicPartition3))
            assertNull(partitionToOffsetAndMetadata[myTopicPartition3])
        }
    }

    @Test
    @Throws(Exception::class)
    fun testBatchedListConsumerGroupOffsets() {
        val cluster = mockCluster(numNodes = 1, controllerIndex = 0)
        val time = MockTime()
        val groupSpecs = batchedListConsumerGroupOffsetsSpec()
        AdminClientUnitTestEnv(time, cluster, AdminClientConfig.RETRIES_CONFIG, "0").use { env ->
            env.mockClient.setNodeApiVersions(NodeApiVersions.create())
            env.mockClient.prepareResponse(
                response = prepareBatchedFindCoordinatorResponse(
                    error = Errors.NONE,
                    node = env.cluster.controller,
                    groups = groupSpecs.keys,
                ),
            )
            val result = env.adminClient.listConsumerGroupOffsets(
                groupSpecs = groupSpecs,
                options = ListConsumerGroupOffsetsOptions(),
            )
            sendOffsetFetchResponse(
                mockClient = env.mockClient,
                groupSpecs = groupSpecs,
                batched = true,
                error = Errors.NONE
            )
            verifyListOffsetsForMultipleGroups(groupSpecs, result)
        }
    }

    @Test
    @Throws(Exception::class)
    fun testBatchedListConsumerGroupOffsetsWithNoFindCoordinatorBatching() {
        val cluster = mockCluster(numNodes = 1, controllerIndex = 0)
        val time = MockTime()
        val groupSpecs = batchedListConsumerGroupOffsetsSpec()
        val findCoordinatorV3 = ApiVersionsResponseData.ApiVersion()
            .setApiKey(ApiKeys.FIND_COORDINATOR.id)
            .setMinVersion(0)
            .setMaxVersion(3)
        val offsetFetchV7 = ApiVersionsResponseData.ApiVersion()
            .setApiKey(ApiKeys.OFFSET_FETCH.id)
            .setMinVersion(0)
            .setMaxVersion(7)
        AdminClientUnitTestEnv(
            time = time,
            cluster = cluster,
            vals = arrayOf(AdminClientConfig.RETRY_BACKOFF_MS_CONFIG, "0"),
        ).use { env ->
            env.mockClient.setNodeApiVersions(
                NodeApiVersions.create(listOf(findCoordinatorV3, offsetFetchV7))
            )
            env.mockClient.prepareResponse(
                response = prepareOldFindCoordinatorResponse(
                    error = Errors.COORDINATOR_NOT_AVAILABLE,
                    node = Node.noNode(),
                )
            )
            env.mockClient.prepareResponse(
                response = prepareOldFindCoordinatorResponse(Errors.NONE, env.cluster.controller!!),
            )
            env.mockClient.prepareResponse(
                response = prepareOldFindCoordinatorResponse(Errors.NONE, env.cluster.controller!!),
            )
            val result = env.adminClient.listConsumerGroupOffsets(groupSpecs)

            // Fail the first request in order to ensure that the group is not batched when retried.
            sendOffsetFetchResponse(
                mockClient = env.mockClient,
                groupSpecs = groupSpecs,
                batched = false,
                error = Errors.COORDINATOR_LOAD_IN_PROGRESS,
            )

            sendOffsetFetchResponse(
                mockClient = env.mockClient,
                groupSpecs = groupSpecs,
                batched = false,
                error = Errors.NONE,
            )
            sendOffsetFetchResponse(
                mockClient = env.mockClient,
                groupSpecs = groupSpecs,
                batched = false,
                error = Errors.NONE,
            )

            verifyListOffsetsForMultipleGroups(groupSpecs, result)
        }
    }

    @Test
    @Throws(Exception::class)
    fun testBatchedListConsumerGroupOffsetsWithNoOffsetFetchBatching() {
        val cluster = mockCluster(numNodes = 1, controllerIndex = 0)
        val time = MockTime()
        val groupSpecs = batchedListConsumerGroupOffsetsSpec()
        val offsetFetchV7 = ApiVersionsResponseData.ApiVersion()
            .setApiKey(ApiKeys.OFFSET_FETCH.id)
            .setMinVersion(0.toShort())
            .setMaxVersion(7.toShort())
        AdminClientUnitTestEnv(
            time,
            cluster,
            AdminClientConfig.RETRY_BACKOFF_MS_CONFIG,
            "0"
        ).use { env ->
            env.mockClient.setNodeApiVersions(NodeApiVersions.create(setOf(offsetFetchV7)))
            env.mockClient.prepareResponse(
                response = prepareBatchedFindCoordinatorResponse(
                    error = Errors.NONE,
                    node = env.cluster.controller,
                    groups = groupSpecs.keys,
                )
            )
            // Prepare a response to force client to attempt batched request creation that throws
            // NoBatchedOffsetFetchRequestException. This triggers creation of non-batched requests.
            env.mockClient.prepareResponse(
                response = offsetFetchResponse(Errors.COORDINATOR_NOT_AVAILABLE, emptyMap()),
            )
            val result = env.adminClient.listConsumerGroupOffsets(groupSpecs)

            // The request handler attempts both FindCoordinator and OffsetFetch requests. This seems
            // ok since since we expect this scenario only during upgrades from versions < 3.0.0 where
            // some upgraded brokers could handle batched FindCoordinator while non-upgraded coordinators
            // rejected batched OffsetFetch requests.
            sendFindCoordinatorResponse(env.mockClient, env.cluster.controller!!)
            sendFindCoordinatorResponse(env.mockClient, env.cluster.controller!!)
            sendOffsetFetchResponse(
                mockClient = env.mockClient,
                groupSpecs = groupSpecs,
                batched = false,
                error = Errors.NONE,
            )
            sendOffsetFetchResponse(
                mockClient = env.mockClient,
                groupSpecs = groupSpecs,
                batched = false,
                error = Errors.NONE,
            )
            verifyListOffsetsForMultipleGroups(groupSpecs, result)
        }
    }

    private fun batchedListConsumerGroupOffsetsSpec(): Map<String, ListConsumerGroupOffsetsSpec> {
        val groupAPartitions = setOf(TopicPartition("A", 1))
        val groupBPartitions = setOf(TopicPartition("B", 2))
        val groupASpec = ListConsumerGroupOffsetsSpec().apply { topicPartitions = groupAPartitions }
        val groupBSpec = ListConsumerGroupOffsetsSpec().apply { topicPartitions = groupBPartitions }
        return mapOf(
            "groupA" to groupASpec,
            "groupB" to groupBSpec,
        )
    }

    @Throws(Exception::class)
    private fun waitForRequest(mockClient: MockClient, apiKeys: ApiKeys) {
        waitForCondition(
            testCondition = {
                val clientRequest = mockClient.requests().peek()
                clientRequest != null && clientRequest.apiKey === apiKeys
            },
            conditionDetails = "Failed awaiting $apiKeys request",
        )
    }

    @Throws(Exception::class)
    private fun sendFindCoordinatorResponse(mockClient: MockClient, coordinator: Node) {
        waitForRequest(mockClient, ApiKeys.FIND_COORDINATOR)
        val (_, requestBuilder) = mockClient.requests().peek()
        val data = (requestBuilder as FindCoordinatorRequest.Builder).data()
        mockClient.respond(prepareFindCoordinatorResponse(Errors.NONE, data.key, coordinator))
    }

    @Throws(Exception::class)
    private fun sendOffsetFetchResponse(
        mockClient: MockClient,
        groupSpecs: Map<String, ListConsumerGroupOffsetsSpec>,
        batched: Boolean,
        error: Errors,
    ) {
        waitForRequest(mockClient, ApiKeys.OFFSET_FETCH)
        val clientRequest = mockClient.requests().peek()
        val data = (clientRequest.requestBuilder as OffsetFetchRequest.Builder).data
        val results = mutableMapOf<String, Map<TopicPartition, OffsetFetchResponse.PartitionData>>()
        val errors = mutableMapOf<String, Errors>()
        data.groups.forEach { group ->
            results[group.groupId] = groupSpecs[group.groupId]!!.topicPartitions!!.associateWith {
                OffsetFetchResponse.PartitionData(
                    offset = 10,
                    leaderEpoch = null,
                    metadata = "",
                    error = Errors.NONE,
                )
            }
            errors[group.groupId] = error
        }
        if (!batched) {
            assertEquals(expected = 1, actual = data.groups.size)
            mockClient.respond(
                OffsetFetchResponse(
                    throttleTimeMs = THROTTLE,
                    error = error,
                    responseData = results.values.first(),
                )
            )
        } else mockClient.respond(OffsetFetchResponse(THROTTLE, errors, results))
    }

    @Throws(Exception::class)
    private fun verifyListOffsetsForMultipleGroups(
        groupSpecs: Map<String, ListConsumerGroupOffsetsSpec>,
        result: ListConsumerGroupOffsetsResult,
    ) {
        assertEquals(groupSpecs.size, result.all()[10, TimeUnit.SECONDS].size)
        for ((key, value) in groupSpecs) assertEquals(
            expected = value.topicPartitions,
            actual = result.partitionsToOffsetAndMetadata(key).get().keys,
        )
    }

    @Test
    @Throws(Exception::class)
    fun testDeleteConsumerGroupsNumRetries() {
        val cluster = mockCluster(numNodes = 3, controllerIndex = 0)
        val time = MockTime()
        val groupIds = listOf("groupId")
        AdminClientUnitTestEnv(
            time,
            cluster,
            AdminClientConfig.RETRIES_CONFIG,
            "0",
        ).use { env ->
            env.mockClient.setNodeApiVersions(NodeApiVersions.create())
            env.mockClient.prepareResponse(
                response = prepareFindCoordinatorResponse(Errors.NONE, env.cluster.controller!!),
            )
            val validResponse = DeletableGroupResultCollection()
            validResponse.add(
                DeletableGroupResult()
                    .setGroupId("groupId")
                    .setErrorCode(Errors.NOT_COORDINATOR.code)
            )
            env.mockClient.prepareResponse(
                response = DeleteGroupsResponse(
                    DeleteGroupsResponseData().setResults(validResponse)
                )
            )
            env.mockClient.prepareResponse(
                response = prepareFindCoordinatorResponse(Errors.NONE, env.cluster.controller!!),
            )
            val result = env.adminClient.deleteConsumerGroups(groupIds)
            assertFutureError(
                future = result.all(),
                exceptionClass = TimeoutException::class.java,
            )
        }
    }

    @Test
    @Throws(Exception::class)
    fun testDeleteConsumerGroupsRetryBackoff() {
        val time = MockTime()
        val retryBackoff = 100
        val groupIds = listOf(GROUP_ID)
        AdminClientUnitTestEnv(
            time,
            mockCluster(numNodes = 3, controllerIndex = 0),
            newStrMap(AdminClientConfig.RETRY_BACKOFF_MS_CONFIG, "" + retryBackoff),
        ).use { env ->
            val mockClient = env.mockClient
            mockClient.setNodeApiVersions(NodeApiVersions.create())
            val firstAttemptTime = AtomicLong(0)
            val secondAttemptTime = AtomicLong(0)
            mockClient.prepareResponse(
                response = prepareFindCoordinatorResponse(Errors.NONE, env.cluster.controller!!),
            )
            var validResponse = DeletableGroupResultCollection()
            validResponse.add(
                DeletableGroupResult()
                    .setGroupId(GROUP_ID)
                    .setErrorCode(Errors.NOT_COORDINATOR.code)
            )
            mockClient.prepareResponse(
                matcher = {
                    firstAttemptTime.set(time.milliseconds())
                    true
                },
                response = DeleteGroupsResponse(
                    DeleteGroupsResponseData().setResults(validResponse)
                ),
            )
            mockClient.prepareResponse(
                response = prepareFindCoordinatorResponse(Errors.NONE, env.cluster.controller!!),
            )
            validResponse = DeletableGroupResultCollection()
            validResponse.add(
                DeletableGroupResult()
                    .setGroupId(GROUP_ID)
                    .setErrorCode(Errors.NONE.code)
            )
            mockClient.prepareResponse(
                matcher = { _ ->
                    secondAttemptTime.set(time.milliseconds())
                    true
                },
                response = DeleteGroupsResponse(
                    DeleteGroupsResponseData().setResults(validResponse)
                ),
            )
            val future = env.adminClient.deleteConsumerGroups(groupIds).all()
            waitForCondition(
                testCondition = { mockClient.numAwaitingResponses() == 1 },
                conditionDetails = "Failed awaiting DeleteConsumerGroups first request failure",
            )
            waitForCondition(
                testCondition = { env.adminClient.numPendingCalls() == 1 },
                conditionDetails = "Failed to add retry DeleteConsumerGroups call on first failure",
            )
            time.sleep(retryBackoff.toLong())
            future.get()
            val actualRetryBackoff = secondAttemptTime.get() - firstAttemptTime.get()
            assertEquals(
                expected = retryBackoff.toLong(),
                actual = actualRetryBackoff,
                message = "DeleteConsumerGroups retry did not await expected backoff!",
            )
        }
    }

    @Test
    @Throws(Exception::class)
    fun testDeleteConsumerGroupsWithOlderBroker() {
        val groupIds = listOf("groupId")
        val findCoordinatorV3 = ApiVersionsResponseData.ApiVersion()
            .setApiKey(ApiKeys.FIND_COORDINATOR.id)
            .setMinVersion(0.toShort())
            .setMaxVersion(3.toShort())
        val describeGroups = ApiVersionsResponseData.ApiVersion()
            .setApiKey(ApiKeys.DESCRIBE_GROUPS.id)
            .setMinVersion(0.toShort())
            .setMaxVersion(ApiKeys.DELETE_GROUPS.latestVersion())
        AdminClientUnitTestEnv(mockCluster(numNodes = 1, controllerIndex = 0)).use { env ->
            env.mockClient.setNodeApiVersions(
                NodeApiVersions.create(listOf(findCoordinatorV3, describeGroups))
            )

            // Retriable FindCoordinatorResponse errors should be retried
            env.mockClient.prepareResponse(
                response = prepareOldFindCoordinatorResponse(
                    error = Errors.COORDINATOR_NOT_AVAILABLE,
                    node = Node.noNode(),
                ),
            )
            env.mockClient.prepareResponse(
                response = prepareOldFindCoordinatorResponse(
                    error = Errors.COORDINATOR_LOAD_IN_PROGRESS,
                    node = Node.noNode()
                ),
            )
            env.mockClient.prepareResponse(
                response = prepareOldFindCoordinatorResponse(Errors.NONE, env.cluster.controller!!),
            )
            val validResponse = DeletableGroupResultCollection()
            validResponse.add(
                DeletableGroupResult()
                    .setGroupId("groupId")
                    .setErrorCode(Errors.NONE.code)
            )
            env.mockClient.prepareResponse(
                response = DeleteGroupsResponse(
                    DeleteGroupsResponseData().setResults(validResponse)
                )
            )
            val result = env.adminClient.deleteConsumerGroups(groupIds)
            val results = result.deletedGroups()["groupId"]!!
            assertNotFails(results::get)

            // should throw error for non-retriable errors
            env.mockClient.prepareResponse(
                response = prepareOldFindCoordinatorResponse(
                    error = Errors.GROUP_AUTHORIZATION_FAILED,
                    node = Node.noNode()
                )
            )
            var errorResult = env.adminClient.deleteConsumerGroups(groupIds)
            assertFutureError(
                future = errorResult.deletedGroups()["groupId"]!!,
                exceptionClass = GroupAuthorizationException::class.java,
            )

            // Retriable errors should be retried
            env.mockClient.prepareResponse(
                response = prepareOldFindCoordinatorResponse(Errors.NONE, env.cluster.controller!!),
            )
            val errorResponse = DeletableGroupResultCollection()
            errorResponse.add(
                DeletableGroupResult()
                    .setGroupId("groupId")
                    .setErrorCode(Errors.COORDINATOR_LOAD_IN_PROGRESS.code)
            )
            env.mockClient.prepareResponse(
                response = DeleteGroupsResponse(
                    DeleteGroupsResponseData().setResults(errorResponse)
                )
            )

            /*
             * We need to return two responses here, one for NOT_COORDINATOR call when calling
             * delete a consumer group api using coordinator that has moved. This will retry whole
             * operation. So we need to again respond with a FindCoordinatorResponse.
             *
             * And the same reason for the following COORDINATOR_NOT_AVAILABLE error response
             */
            var coordinatorMoved = DeletableGroupResultCollection()
            coordinatorMoved.add(
                DeletableGroupResult()
                    .setGroupId("groupId")
                    .setErrorCode(Errors.NOT_COORDINATOR.code)
            )
            env.mockClient.prepareResponse(
                response = DeleteGroupsResponse(
                    DeleteGroupsResponseData().setResults(coordinatorMoved)
                )
            )
            env.mockClient.prepareResponse(
                response = prepareOldFindCoordinatorResponse(Errors.NONE, env.cluster.controller!!),
            )
            coordinatorMoved = DeletableGroupResultCollection()
            coordinatorMoved.add(
                DeletableGroupResult()
                    .setGroupId("groupId")
                    .setErrorCode(Errors.COORDINATOR_NOT_AVAILABLE.code)
            )
            env.mockClient.prepareResponse(
                response = DeleteGroupsResponse(
                    DeleteGroupsResponseData().setResults(coordinatorMoved)
                )
            )
            env.mockClient.prepareResponse(
                response = prepareOldFindCoordinatorResponse(Errors.NONE, env.cluster.controller!!),
            )
            env.mockClient.prepareResponse(
                response = DeleteGroupsResponse(
                    DeleteGroupsResponseData().setResults(validResponse)
                )
            )
            errorResult = env.adminClient.deleteConsumerGroups(groupIds)
            val errorResults = errorResult.deletedGroups()["groupId"]!!
            assertNotFails(errorResults::get)
        }
    }

    @Test
    @Throws(Exception::class)
    fun testDeleteMultipleConsumerGroupsWithOlderBroker() {
        val groupIds: List<String> = mutableListOf("group1", "group2")
        val findCoordinatorV3 = ApiVersionsResponseData.ApiVersion()
            .setApiKey(ApiKeys.FIND_COORDINATOR.id)
            .setMinVersion(0.toShort())
            .setMaxVersion(3.toShort())
        val describeGroups = ApiVersionsResponseData.ApiVersion()
            .setApiKey(ApiKeys.DESCRIBE_GROUPS.id)
            .setMinVersion(0.toShort())
            .setMaxVersion(ApiKeys.DELETE_GROUPS.latestVersion())
        AdminClientUnitTestEnv(mockCluster(1, 0)).use { env ->
            env.mockClient.setNodeApiVersions(
                NodeApiVersions.create(listOf(findCoordinatorV3, describeGroups))
            )

            // Dummy response for MockClient to handle the UnsupportedVersionException correctly to switch from batched to un-batched
            env.mockClient.prepareResponse(response = null)
            // Retriable FindCoordinatorResponse errors should be retried
            for (i in groupIds.indices) {
                env.mockClient.prepareResponse(
                    response = prepareOldFindCoordinatorResponse(
                        Errors.COORDINATOR_NOT_AVAILABLE,
                        Node.noNode()
                    ),
                )
            }
            for (i in groupIds.indices) {
                env.mockClient.prepareResponse(
                    prepareOldFindCoordinatorResponse(Errors.NONE, env.cluster.controller!!)
                )
            }
            val validResponse = DeletableGroupResultCollection()
            validResponse.add(
                DeletableGroupResult()
                    .setGroupId("group1")
                    .setErrorCode(Errors.NONE.code)
            )
            validResponse.add(
                DeletableGroupResult()
                    .setGroupId("group2")
                    .setErrorCode(Errors.NONE.code)
            )
            env.mockClient.prepareResponse(
                response = DeleteGroupsResponse(
                    DeleteGroupsResponseData().setResults(validResponse)
                )
            )
            val result: DeleteConsumerGroupsResult = env.adminClient
                .deleteConsumerGroups(groupIds)
            val results = result.deletedGroups()["group1"]!!
            assertNotFails { results.get(5, TimeUnit.SECONDS) }
        }
    }

    @Test
    @Throws(Exception::class)
    fun testDeleteConsumerGroupOffsetsNumRetries() {
        val cluster = mockCluster(numNodes = 3, controllerIndex = 0)
        val time = MockTime()
        AdminClientUnitTestEnv(
            time,
            cluster,
            AdminClientConfig.RETRIES_CONFIG,
            "0",
        ).use { env ->
            val tp1 = TopicPartition(topic = "foo", partition = 0)
            env.mockClient.setNodeApiVersions(NodeApiVersions.create())
            env.mockClient.prepareResponse(
                response = prepareFindCoordinatorResponse(Errors.NONE, env.cluster.controller!!),
            )
            env.mockClient.prepareResponse(
                response = prepareOffsetDeleteResponse(Errors.NOT_COORDINATOR),
            )
            env.mockClient.prepareResponse(
                response = prepareFindCoordinatorResponse(Errors.NONE, env.cluster.controller!!),
            )
            val result = env.adminClient.deleteConsumerGroupOffsets(GROUP_ID, setOf(tp1))
            assertFutureError(
                future = result.all(),
                exceptionClass = TimeoutException::class.java,
            )
        }
    }

    @Test
    @Throws(Exception::class)
    fun testDeleteConsumerGroupOffsetsRetryBackoff() {
        val time = MockTime()
        val retryBackoff = 100
        AdminClientUnitTestEnv(
            time,
            mockCluster(numNodes = 3, controllerIndex = 0),
            newStrMap(AdminClientConfig.RETRY_BACKOFF_MS_CONFIG, "" + retryBackoff)
        ).use { env ->
            val mockClient = env.mockClient
            mockClient.setNodeApiVersions(NodeApiVersions.create())
            val firstAttemptTime = AtomicLong(0)
            val secondAttemptTime = AtomicLong(0)
            val tp1 = TopicPartition("foo", 0)
            mockClient.prepareResponse(
                response = prepareFindCoordinatorResponse(Errors.NONE, env.cluster.controller!!),
            )
            mockClient.prepareResponse(
                matcher = {
                    firstAttemptTime.set(time.milliseconds())
                    true
                },
                response = prepareOffsetDeleteResponse(Errors.NOT_COORDINATOR)
            )
            mockClient.prepareResponse(
                response = prepareFindCoordinatorResponse(Errors.NONE, env.cluster.controller!!),
            )
            mockClient.prepareResponse(
                matcher = {
                    secondAttemptTime.set(time.milliseconds())
                    true
                },
                response = prepareOffsetDeleteResponse(
                    topic = "foo",
                    partition = 0,
                    error = Errors.NONE,
                )
            )
            val future = env.adminClient
                .deleteConsumerGroupOffsets(GROUP_ID, setOf(tp1))
                .all()
            waitForCondition(
                testCondition = { mockClient.numAwaitingResponses() == 1 },
                conditionDetails = "Failed awaiting DeleteConsumerGroupOffsets first request failure",
            )
            waitForCondition(
                testCondition = { env.adminClient.numPendingCalls() == 1 },
                conditionDetails = "Failed to add retry DeleteConsumerGroupOffsets call on first failure",
            )
            time.sleep(retryBackoff.toLong())
            future.get()
            val actualRetryBackoff = secondAttemptTime.get() - firstAttemptTime.get()
            assertEquals(
                expected = retryBackoff.toLong(),
                actual = actualRetryBackoff,
                message = "DeleteConsumerGroupOffsets retry did not await expected backoff!",
            )
        }
    }

    @Test
    @Throws(Exception::class)
    fun testDeleteConsumerGroupOffsets() {
        // Happy path
        val tp1 = TopicPartition("foo", 0)
        val tp2 = TopicPartition("bar", 0)
        val tp3 = TopicPartition("foobar", 0)
        AdminClientUnitTestEnv(mockCluster(numNodes = 1, controllerIndex = 0)).use { env ->
            env.mockClient.setNodeApiVersions(NodeApiVersions.create())
            env.mockClient.prepareResponse(
                response = prepareFindCoordinatorResponse(Errors.NONE, env.cluster.controller!!),
            )
            env.mockClient.prepareResponse(
                response = OffsetDeleteResponse(
                    OffsetDeleteResponseData().setTopics(
                        OffsetDeleteResponseTopicCollection(
                            listOf(
                                OffsetDeleteResponseTopic()
                                    .setName("foo")
                                    .setPartitions(
                                        OffsetDeleteResponsePartitionCollection(
                                            listOf(
                                                OffsetDeleteResponsePartition()
                                                    .setPartitionIndex(0)
                                                    .setErrorCode(Errors.NONE.code)
                                            ).iterator()
                                        )
                                    ),
                                OffsetDeleteResponseTopic()
                                    .setName("bar")
                                    .setPartitions(
                                        OffsetDeleteResponsePartitionCollection(
                                            listOf(
                                                OffsetDeleteResponsePartition()
                                                    .setPartitionIndex(0)
                                                    .setErrorCode(Errors.GROUP_SUBSCRIBED_TO_TOPIC.code)
                                            ).iterator()
                                        )
                                    )
                            ).iterator()
                        )
                    )
                )
            )
            val errorResult = env.adminClient.deleteConsumerGroupOffsets(
                groupId = GROUP_ID,
                partitions = setOf(tp1, tp2),
            )
            assertNotFails(errorResult.partitionResult(tp1)::get)
            assertFutureError(
                future = errorResult.all(),
                exceptionClass = GroupSubscribedToTopicException::class.java
            )
            assertFutureError(
                future = errorResult.partitionResult(tp2),
                exceptionClass = GroupSubscribedToTopicException::class.java
            )
            assertFailsWith<IllegalArgumentException> { errorResult.partitionResult(tp3) }
        }
    }

    @Test
    @Throws(Exception::class)
    fun testDeleteConsumerGroupOffsetsRetriableErrors() {
        // Retriable errors should be retried
        val tp1 = TopicPartition(topic = "foo", partition = 0)
        AdminClientUnitTestEnv(mockCluster(numNodes = 1, controllerIndex = 0)).use { env ->
            env.mockClient.setNodeApiVersions(NodeApiVersions.create())
            env.mockClient.prepareResponse(
                response = prepareFindCoordinatorResponse(Errors.NONE, env.cluster.controller!!),
            )
            env.mockClient.prepareResponse(
                response = prepareOffsetDeleteResponse(Errors.COORDINATOR_LOAD_IN_PROGRESS),
            )

            /*
             * We need to return two responses here, one for NOT_COORDINATOR call when calling
             * delete a consumer group api using coordinator that has moved. This will retry whole
             * operation. So we need to again respond with a FindCoordinatorResponse.
             *
             * And the same reason for the following COORDINATOR_NOT_AVAILABLE error response
             */
            env.mockClient.prepareResponse(
                response = prepareOffsetDeleteResponse(Errors.NOT_COORDINATOR),
            )
            env.mockClient.prepareResponse(
                response = prepareFindCoordinatorResponse(Errors.NONE, env.cluster.controller!!),
            )
            env.mockClient.prepareResponse(
                response = prepareOffsetDeleteResponse(Errors.COORDINATOR_NOT_AVAILABLE),
            )
            env.mockClient.prepareResponse(
                response = prepareFindCoordinatorResponse(Errors.NONE, env.cluster.controller!!),
            )
            env.mockClient.prepareResponse(
                response = prepareOffsetDeleteResponse(
                    "foo",
                    0,
                    Errors.NONE
                )
            )
            val errorResult1 = env.adminClient.deleteConsumerGroupOffsets(GROUP_ID, setOf(tp1))
            assertNotFails(errorResult1.all()::get)
            assertNotFails(errorResult1.partitionResult(tp1)::get)
        }
    }

    @Test
    @Throws(Exception::class)
    fun testDeleteConsumerGroupOffsetsNonRetriableErrors() {
        // Non-retriable errors throw an exception
        val tp1 = TopicPartition("foo", 0)
        val nonRetriableErrors = listOf(
            Errors.GROUP_AUTHORIZATION_FAILED,
            Errors.INVALID_GROUP_ID,
            Errors.GROUP_ID_NOT_FOUND,
        )
        AdminClientUnitTestEnv(mockCluster(numNodes = 1, controllerIndex = 0)).use { env ->
            env.mockClient.setNodeApiVersions(NodeApiVersions.create())
            for (error in nonRetriableErrors) {
                env.mockClient.prepareResponse(
                    prepareFindCoordinatorResponse(Errors.NONE, env.cluster.controller!!),
                )
                env.mockClient.prepareResponse(prepareOffsetDeleteResponse(error))
                val errorResult = env.adminClient.deleteConsumerGroupOffsets(GROUP_ID, setOf(tp1))
                assertFutureError(
                    future = errorResult.all(),
                    exceptionClass = error.exception!!.javaClass,
                )
                assertFutureError(
                    future = errorResult.partitionResult(tp1),
                    exceptionClass = error.exception!!.javaClass,
                )
            }
        }
    }

    @Test
    @Throws(Exception::class)
    fun testDeleteConsumerGroupOffsetsFindCoordinatorRetriableErrors() {
        // Retriable FindCoordinatorResponse errors should be retried
        val tp1 = TopicPartition("foo", 0)
        AdminClientUnitTestEnv(mockCluster(1, 0)).use { env ->
            env.mockClient.setNodeApiVersions(NodeApiVersions.create())
            env.mockClient.prepareResponse(
                prepareFindCoordinatorResponse(
                    Errors.COORDINATOR_NOT_AVAILABLE,
                    Node.noNode()
                )
            )
            env.mockClient.prepareResponse(
                prepareFindCoordinatorResponse(Errors.COORDINATOR_LOAD_IN_PROGRESS, Node.noNode())
            )
            env.mockClient.prepareResponse(
                prepareFindCoordinatorResponse(Errors.NONE, env.cluster.controller!!)
            )
            env.mockClient.prepareResponse(
                prepareOffsetDeleteResponse(
                    topic = "foo",
                    partition = 0,
                    error = Errors.NONE,
                )
            )
            val result: DeleteConsumerGroupOffsetsResult = env.adminClient
                .deleteConsumerGroupOffsets(GROUP_ID, setOf(tp1))
            assertNotFails(result.all()::get)
            assertNotFails(result.partitionResult(tp1)::get)
        }
    }

    @Test
    @Throws(Exception::class)
    fun testDeleteConsumerGroupOffsetsFindCoordinatorNonRetriableErrors() {
        // Non-retriable FindCoordinatorResponse errors throw an exception
        val tp1 = TopicPartition("foo", 0)
        AdminClientUnitTestEnv(mockCluster(1, 0)).use { env ->
            env.mockClient.setNodeApiVersions(NodeApiVersions.create())
            env.mockClient.prepareResponse(
                prepareFindCoordinatorResponse(
                    Errors.GROUP_AUTHORIZATION_FAILED,
                    Node.noNode()
                )
            )
            val errorResult: DeleteConsumerGroupOffsetsResult = env.adminClient
                .deleteConsumerGroupOffsets(GROUP_ID, setOf(tp1))
            assertFutureError(
                errorResult.all(),
                GroupAuthorizationException::class.java
            )
            assertFutureError(
                errorResult.partitionResult(tp1),
                GroupAuthorizationException::class.java
            )
        }
    }

    @Test
    @Throws(Exception::class)
    fun testIncrementalAlterConfigs() {
        mockClientEnv().use { env ->
            env.mockClient.setNodeApiVersions(NodeApiVersions.create())

            //test error scenarios
            var responseData = IncrementalAlterConfigsResponseData().apply {
                responses = listOf(
                    AlterConfigsResourceResponse()
                        .setResourceName("")
                        .setResourceType(ConfigResource.Type.BROKER.id)
                        .setErrorCode(Errors.CLUSTER_AUTHORIZATION_FAILED.code)
                        .setErrorMessage("authorization error"),
                    AlterConfigsResourceResponse()
                        .setResourceName("topic1")
                        .setResourceType(ConfigResource.Type.TOPIC.id)
                        .setErrorCode(Errors.INVALID_REQUEST.code)
                        .setErrorMessage("Config value append is not allowed for config"),
                )
            }
            env.mockClient.prepareResponse(IncrementalAlterConfigsResponse(responseData))
            val brokerResource = ConfigResource(ConfigResource.Type.BROKER, "")
            val topicResource = ConfigResource(ConfigResource.Type.TOPIC, "topic1")
            val alterConfigOp1 = AlterConfigOp(
                ConfigEntry("log.segment.bytes", "1073741"),
                AlterConfigOp.OpType.SET
            )
            val alterConfigOp2 = AlterConfigOp(
                ConfigEntry("compression.type", "gzip"),
                AlterConfigOp.OpType.APPEND
            )
            val configs = mapOf(
                brokerResource to listOf(alterConfigOp1),
                topicResource to listOf(alterConfigOp2)
            )
            val result = env.adminClient.incrementalAlterConfigs(configs)
            assertFutureError(
                future = result.futures[brokerResource]!!,
                exceptionClass = ClusterAuthorizationException::class.java,
            )
            assertFutureError(
                future = result.futures[topicResource]!!,
                exceptionClass = InvalidRequestException::class.java,
            )

            // Test a call where there are no errors.
            responseData = IncrementalAlterConfigsResponseData()
            responseData.responses += AlterConfigsResourceResponse()
                .setResourceName("")
                .setResourceType(ConfigResource.Type.BROKER.id)
                .setErrorCode(Errors.NONE.code)
                .setErrorMessage(ApiError.NONE.message)

            env.mockClient.prepareResponse(IncrementalAlterConfigsResponse(responseData))
            env.adminClient
                .incrementalAlterConfigs(mapOf(brokerResource to listOf(alterConfigOp1)))
                .all()
                .get()
        }
    }

    @Test
    @Throws(Exception::class)
    fun testRemoveMembersFromGroupNumRetries() {
        val cluster = mockCluster(numNodes = 3, controllerIndex = 0)
        val time = MockTime()
        AdminClientUnitTestEnv(
            time,
            cluster,
            AdminClientConfig.RETRIES_CONFIG,
            "0",
        ).use { env ->
            env.mockClient.setNodeApiVersions(NodeApiVersions.create())
            env.mockClient.prepareResponse(
                prepareFindCoordinatorResponse(Errors.NONE, env.cluster.controller!!),
            )
            env.mockClient.prepareResponse(
                LeaveGroupResponse(LeaveGroupResponseData().setErrorCode(Errors.NOT_COORDINATOR.code)),
            )
            env.mockClient.prepareResponse(
                prepareFindCoordinatorResponse(Errors.NONE, env.cluster.controller!!),
            )
            val membersToRemove = setOf(
                MemberToRemove("instance-1"),
                MemberToRemove("instance-2"),
            )
            val result = env.adminClient.removeMembersFromConsumerGroup(
                groupId = GROUP_ID,
                options = RemoveMembersFromConsumerGroupOptions(membersToRemove)
            )
            assertFutureError(
                future = result.all(),
                exceptionClass = TimeoutException::class.java
            )
        }
    }

    @Test
    @Throws(Exception::class)
    fun testRemoveMembersFromGroupRetryBackoff() {
        val time = MockTime()
        val retryBackoff = 100
        AdminClientUnitTestEnv(
            time,
            mockCluster(numNodes = 3, controllerIndex = 0),
            newStrMap(AdminClientConfig.RETRY_BACKOFF_MS_CONFIG, "" + retryBackoff)
        ).use { env ->
            val mockClient = env.mockClient
            mockClient.setNodeApiVersions(NodeApiVersions.create())
            val firstAttemptTime = AtomicLong(0)
            val secondAttemptTime = AtomicLong(0)
            mockClient.prepareResponse(
                prepareFindCoordinatorResponse(Errors.NONE, env.cluster.controller!!)
            )
            env.mockClient.prepareResponse(
                matcher = {
                    firstAttemptTime.set(time.milliseconds())
                    true
                },
                response = LeaveGroupResponse(
                    LeaveGroupResponseData().setErrorCode(Errors.NOT_COORDINATOR.code)
                )
            )
            mockClient.prepareResponse(
                prepareFindCoordinatorResponse(Errors.NONE, env.cluster.controller!!)
            )
            val responseOne = MemberResponse()
                .setGroupInstanceId("instance-1")
                .setErrorCode(Errors.NONE.code)
            env.mockClient.prepareResponse(
                matcher = { body ->
                    secondAttemptTime.set(time.milliseconds())
                    true
                },
                response = LeaveGroupResponse(
                    LeaveGroupResponseData()
                        .setErrorCode(Errors.NONE.code)
                        .setMembers(listOf(responseOne))
                ),
            )
            val membersToRemove = setOf(MemberToRemove("instance-1"))
            val future = env.adminClient.removeMembersFromConsumerGroup(
                groupId = GROUP_ID,
                options = RemoveMembersFromConsumerGroupOptions(membersToRemove)
            ).all()
            waitForCondition(
                testCondition = { mockClient.numAwaitingResponses() == 1 },
                conditionDetails = "Failed awaiting RemoveMembersFromGroup first request failure",
            )
            waitForCondition(
                testCondition = { env.adminClient.numPendingCalls() == 1 },
                conditionDetails = "Failed to add retry RemoveMembersFromGroup call on first failure",
            )
            time.sleep(retryBackoff.toLong())
            future.get()
            val actualRetryBackoff = secondAttemptTime.get() - firstAttemptTime.get()
            assertEquals(
                retryBackoff.toLong(),
                actualRetryBackoff,
                "RemoveMembersFromGroup retry did not await expected backoff!"
            )
        }
    }

    @Test
    @Throws(Exception::class)
    fun testRemoveMembersFromGroupRetriableErrors() {
        // Retriable errors should be retried
        AdminClientUnitTestEnv(mockCluster(numNodes = 1, controllerIndex = 0)).use { env ->
            env.mockClient.setNodeApiVersions(NodeApiVersions.create())
            env.mockClient.prepareResponse(
                prepareFindCoordinatorResponse(Errors.NONE, env.cluster.controller!!)
            )
            env.mockClient.prepareResponse(
                LeaveGroupResponse(
                    LeaveGroupResponseData().setErrorCode(Errors.COORDINATOR_LOAD_IN_PROGRESS.code)
                )
            )

            /*
             * We need to return two responses here, one for NOT_COORDINATOR call when calling remove member
             * api using coordinator that has moved. This will retry whole operation. So we need to again respond with a
             * FindCoordinatorResponse.
             *
             * And the same reason for the following COORDINATOR_NOT_AVAILABLE error response
             */
            env.mockClient.prepareResponse(
                LeaveGroupResponse(
                    LeaveGroupResponseData().setErrorCode(Errors.NOT_COORDINATOR.code)
                )
            )
            env.mockClient.prepareResponse(
                prepareFindCoordinatorResponse(Errors.NONE, env.cluster.controller!!)
            )
            env.mockClient.prepareResponse(
                LeaveGroupResponse(
                    LeaveGroupResponseData().setErrorCode(Errors.COORDINATOR_NOT_AVAILABLE.code)
                )
            )
            env.mockClient.prepareResponse(
                prepareFindCoordinatorResponse(Errors.NONE, env.cluster.controller!!)
            )
            val memberResponse = MemberResponse()
                .setGroupInstanceId("instance-1")
                .setErrorCode(Errors.NONE.code)
            env.mockClient.prepareResponse(
                LeaveGroupResponse(
                    LeaveGroupResponseData()
                        .setErrorCode(Errors.NONE.code)
                        .setMembers(listOf(memberResponse))
                )
            )
            val memberToRemove = MemberToRemove("instance-1")
            val membersToRemove = setOf(memberToRemove)
            val result = env.adminClient.removeMembersFromConsumerGroup(
                groupId = GROUP_ID,
                options = RemoveMembersFromConsumerGroupOptions(membersToRemove)
            )
            assertNotFails(result.all()::get)
            assertNotFails(result.memberResult(memberToRemove)::get)
        }
    }

    @Test
    @Throws(Exception::class)
    fun testRemoveMembersFromGroupNonRetriableErrors() {
        // Non-retriable errors throw an exception
        val nonRetriableErrors = listOf(
            Errors.GROUP_AUTHORIZATION_FAILED,
            Errors.INVALID_GROUP_ID,
            Errors.GROUP_ID_NOT_FOUND,
        )
        AdminClientUnitTestEnv(mockCluster(numNodes = 1, controllerIndex = 0)).use { env ->
            env.mockClient.setNodeApiVersions(NodeApiVersions.create())
            for (error in nonRetriableErrors) {
                env.mockClient.prepareResponse(
                    prepareFindCoordinatorResponse(Errors.NONE, env.cluster.controller!!)
                )
                env.mockClient.prepareResponse(
                    LeaveGroupResponse(LeaveGroupResponseData().setErrorCode(error.code))
                )
                val memberToRemove = MemberToRemove("instance-1")
                val membersToRemove = setOf(memberToRemove)
                val result = env.adminClient.removeMembersFromConsumerGroup(
                    groupId = GROUP_ID,
                    options = RemoveMembersFromConsumerGroupOptions(membersToRemove),
                )
                assertFutureError(
                    future = result.all(),
                    exceptionClass = error.exception!!.javaClass,
                )
                assertFutureError(
                    future = result.memberResult(memberToRemove),
                    exceptionClass = error.exception!!.javaClass,
                )
            }
        }
    }

    @Test
    @Throws(Exception::class)
    fun testRemoveMembersFromGroup() {
        mockClientEnv().use { env ->
            val instanceOne = "instance-1"
            val instanceTwo = "instance-2"
            env.mockClient.setNodeApiVersions(NodeApiVersions.create())

            // Retriable FindCoordinatorResponse errors should be retried
            env.mockClient.prepareResponse(
                prepareFindCoordinatorResponse(Errors.COORDINATOR_LOAD_IN_PROGRESS, Node.noNode())
            )
            env.mockClient.prepareResponse(
                prepareFindCoordinatorResponse(Errors.NONE, env.cluster.controller!!)
            )

            // Retriable errors should be retried
            env.mockClient.prepareResponse(
                LeaveGroupResponse(
                    LeaveGroupResponseData().setErrorCode(Errors.COORDINATOR_LOAD_IN_PROGRESS.code)
                )
            )

            // Inject a top-level non-retriable error
            env.mockClient.prepareResponse(
                LeaveGroupResponse(
                    LeaveGroupResponseData().setErrorCode(Errors.UNKNOWN_SERVER_ERROR.code)
                )
            )
            val membersToRemove = setOf(
                MemberToRemove(instanceOne),
                MemberToRemove(instanceTwo),
            )
            val unknownErrorResult = env.adminClient.removeMembersFromConsumerGroup(
                groupId = GROUP_ID,
                options = RemoveMembersFromConsumerGroupOptions(membersToRemove),
            )
            val memberOne = MemberToRemove(instanceOne)
            val memberTwo = MemberToRemove(instanceTwo)
            assertFutureError(
                future = unknownErrorResult.memberResult(memberOne),
                exceptionClass = UnknownServerException::class.java
            )
            assertFutureError(
                future = unknownErrorResult.memberResult(memberTwo),
                exceptionClass = UnknownServerException::class.java
            )
            val responseOne = MemberResponse()
                .setGroupInstanceId(instanceOne)
                .setErrorCode(Errors.UNKNOWN_MEMBER_ID.code)
            val responseTwo = MemberResponse()
                .setGroupInstanceId(instanceTwo)
                .setErrorCode(Errors.NONE.code)

            // Inject one member level error.
            env.mockClient.prepareResponse(
                prepareFindCoordinatorResponse(Errors.NONE, env.cluster.controller!!)
            )
            env.mockClient.prepareResponse(
                LeaveGroupResponse(
                    LeaveGroupResponseData()
                        .setErrorCode(Errors.NONE.code)
                        .setMembers(listOf(responseOne, responseTwo))
                )
            )
            val memberLevelErrorResult = env.adminClient.removeMembersFromConsumerGroup(
                groupId = GROUP_ID,
                options = RemoveMembersFromConsumerGroupOptions(membersToRemove),
            )
            assertFutureError(
                future = memberLevelErrorResult.all(),
                exceptionClass = UnknownMemberIdException::class.java
            )
            assertFutureError(
                future = memberLevelErrorResult.memberResult(memberOne),
                exceptionClass = UnknownMemberIdException::class.java,
            )
            assertNotFails(memberLevelErrorResult.memberResult(memberTwo)::get)

            // Return with missing member.
            env.mockClient.prepareResponse(
                prepareFindCoordinatorResponse(Errors.NONE, env.cluster.controller!!)
            )
            env.mockClient.prepareResponse(
                LeaveGroupResponse(
                    LeaveGroupResponseData()
                        .setErrorCode(Errors.NONE.code)
                        .setMembers(listOf(responseTwo))
                )
            )
            val missingMemberResult = env.adminClient.removeMembersFromConsumerGroup(
                groupId = GROUP_ID,
                options = RemoveMembersFromConsumerGroupOptions(membersToRemove),
            )
            assertFutureError(
                future = missingMemberResult.all(),
                exceptionClass = IllegalArgumentException::class.java,
            )
            // The memberOne was not included in the response.
            assertFutureError(
                future = missingMemberResult.memberResult(memberOne),
                exceptionClass = IllegalArgumentException::class.java,
            )
            assertNotFails(missingMemberResult.memberResult(memberTwo)::get)

            // Return with success.
            env.mockClient.prepareResponse(
                prepareFindCoordinatorResponse(Errors.NONE, env.cluster.controller!!)
            )
            env.mockClient.prepareResponse(
                LeaveGroupResponse(
                    LeaveGroupResponseData()
                        .setErrorCode(Errors.NONE.code)
                        .setMembers(
                            listOf(
                                responseTwo,
                                MemberResponse()
                                    .setGroupInstanceId(instanceOne)
                                    .setErrorCode(Errors.NONE.code)
                            )
                        )
                )
            )
            val noErrorResult = env.adminClient.removeMembersFromConsumerGroup(
                groupId = GROUP_ID,
                options = RemoveMembersFromConsumerGroupOptions(membersToRemove)
            )
            assertNotFails(noErrorResult.all()::get)
            assertNotFails(noErrorResult.memberResult(memberOne)::get)
            assertNotFails(noErrorResult.memberResult(memberTwo)::get)

            // Test the "removeAll" scenario
            val topicPartitions = listOf(1, 2, 3).map { partition ->
                TopicPartition("my_topic", partition)
            }
            // construct the DescribeGroupsResponse
            val data = prepareDescribeGroupsResponseData(
                groupId = GROUP_ID,
                groupInstances = listOf(instanceOne, instanceTwo),
                topicPartitions = topicPartitions,
            )

            // Return with partial failure for "removeAll" scenario
            // 1 prepare response for AdminClient.describeConsumerGroups
            env.mockClient.prepareResponse(
                prepareFindCoordinatorResponse(Errors.NONE, env.cluster.controller!!)
            )
            env.mockClient.prepareResponse(DescribeGroupsResponse(data))

            // 2 KafkaAdminClient encounter partial failure when trying to delete all members
            env.mockClient.prepareResponse(
                prepareFindCoordinatorResponse(Errors.NONE, env.cluster.controller!!),
            )
            env.mockClient.prepareResponse(
                LeaveGroupResponse(
                    LeaveGroupResponseData()
                        .setErrorCode(Errors.NONE.code)
                        .setMembers(listOf(responseOne, responseTwo))
                )
            )
            val partialFailureResults = env.adminClient.removeMembersFromConsumerGroup(
                groupId = GROUP_ID,
                options = RemoveMembersFromConsumerGroupOptions()
            )
            val exception = assertFailsWith<ExecutionException> {
                partialFailureResults.all().get()
            }
            assertIs<KafkaException>(exception.cause)
            assertIs<UnknownMemberIdException>(exception.cause!!.cause)

            // Return with success for "removeAll" scenario
            // 1 prepare response for AdminClient.describeConsumerGroups
            env.mockClient.prepareResponse(
                prepareFindCoordinatorResponse(Errors.NONE, env.cluster.controller!!)
            )
            env.mockClient.prepareResponse(DescribeGroupsResponse(data))

            // 2. KafkaAdminClient should delete all members correctly
            env.mockClient.prepareResponse(
                prepareFindCoordinatorResponse(Errors.NONE, env.cluster.controller!!)
            )
            env.mockClient.prepareResponse(
                LeaveGroupResponse(
                    LeaveGroupResponseData()
                        .setErrorCode(Errors.NONE.code)
                        .setMembers(
                            listOf(
                                responseTwo,
                                MemberResponse()
                                    .setGroupInstanceId(instanceOne)
                                    .setErrorCode(Errors.NONE.code)
                            )
                        )
                )
            )
            val successResult = env.adminClient.removeMembersFromConsumerGroup(
                groupId = GROUP_ID,
                options = RemoveMembersFromConsumerGroupOptions()
            )
            assertNotFails(successResult.all()::get)
        }
    }

    @Throws(Exception::class)
    private fun testRemoveMembersFromGroup(reason: String?, expectedReason: String) {
        val cluster = mockCluster(numNodes = 3, controllerIndex = 0)
        val time = MockTime()
        AdminClientUnitTestEnv(time, cluster).use { env ->
            env.mockClient.setNodeApiVersions(NodeApiVersions.create())
            env.mockClient.prepareResponse(
                prepareFindCoordinatorResponse(Errors.NONE, env.cluster.controller!!)
            )
            env.mockClient.prepareResponse(
                matcher = { body ->
                    if (body !is LeaveGroupRequest) return@prepareResponse false
                    val leaveGroupRequest = body.data()
                    leaveGroupRequest.members.all { member ->
                        member.reason.equals(expectedReason)
                    }
                },
                response = LeaveGroupResponse(
                    LeaveGroupResponseData()
                        .setErrorCode(Errors.NONE.code)
                        .setMembers(
                            listOf(
                                MemberResponse().setGroupInstanceId("instance-1"),
                                MemberResponse().setGroupInstanceId("instance-2"),
                            )
                        )
                )
            )
            val memberToRemove1 = MemberToRemove("instance-1")
            val memberToRemove2 = MemberToRemove("instance-2")
            val options = RemoveMembersFromConsumerGroupOptions(
                members = setOf(memberToRemove1, memberToRemove2)
            ).apply { this.reason = reason }
            val result = env.adminClient.removeMembersFromConsumerGroup(GROUP_ID, options)
            assertNotFails(result.all()::get)
            assertNotFails(result.memberResult(memberToRemove1)::get)
            assertNotFails(result.memberResult(memberToRemove2)::get)
        }
    }

    @Test
    @Throws(Exception::class)
    fun testRemoveMembersFromGroupReason() {
        testRemoveMembersFromGroup("testing remove members reason", "testing remove members reason")
    }

    @Test
    @Throws(Exception::class)
    fun testRemoveMembersFromGroupTruncatesReason() {
        val reason =
            "Very looooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooong reason that is 271 characters long to make sure that length limit logic handles the scenario nicely"
        val truncatedReason = reason.substring(0, 255)
        testRemoveMembersFromGroup(reason, truncatedReason)
    }

    @Test
    @Throws(Exception::class)
    fun testRemoveMembersFromGroupDefaultReason() {
        testRemoveMembersFromGroup(null, KafkaAdminClient.DEFAULT_LEAVE_GROUP_REASON)
        testRemoveMembersFromGroup("", KafkaAdminClient.DEFAULT_LEAVE_GROUP_REASON)
    }

    @Test
    @Throws(Exception::class)
    fun testAlterPartitionReassignments() {
        mockClientEnv().use { env ->
            env.mockClient.setNodeApiVersions(NodeApiVersions.create())
            val tp1 = TopicPartition("A", 0)
            val tp2 = TopicPartition("B", 0)
            val reassignments = mapOf(
                tp1 to null,
                tp2 to NewPartitionReassignment(listOf(1, 2, 3)),
            )

            // 1. server returns less responses than number of partitions we sent
            val responseData1 = AlterPartitionReassignmentsResponseData()
            val normalPartitionResponse = ReassignablePartitionResponse().setPartitionIndex(0)
            responseData1.setResponses(
                listOf(
                    ReassignableTopicResponse()
                        .setName("A")
                        .setPartitions(listOf(normalPartitionResponse))
                )
            )
            env.mockClient.prepareResponse(AlterPartitionReassignmentsResponse(responseData1))
            val result1 = env.adminClient.alterPartitionReassignments(reassignments)
            val future1 = result1.all()
            val future2 = result1.futures[tp1]
            assertFutureError(
                future = future1,
                exceptionClass = UnknownServerException::class.java,
            )
            assertFutureError(
                future = future2!!,
                exceptionClass = UnknownServerException::class.java,
            )

            // 2. NOT_CONTROLLER error handling
            val controllerErrResponseData = AlterPartitionReassignmentsResponseData()
                .setErrorCode(Errors.NOT_CONTROLLER.code)
                .setErrorMessage(Errors.NOT_CONTROLLER.message)
                .setResponses(
                    listOf(
                        ReassignableTopicResponse()
                            .setName("A")
                            .setPartitions(listOf(normalPartitionResponse)),
                        ReassignableTopicResponse()
                            .setName("B")
                            .setPartitions(listOf(normalPartitionResponse))
                    )
                )
            val controllerNodeResponse = RequestTestUtils.metadataResponse(
                brokers = env.cluster.nodes,
                clusterId = env.cluster.clusterResource.clusterId,
                controllerId = 1,
                topicMetadataList = emptyList(),
            )
            val normalResponse = AlterPartitionReassignmentsResponseData()
                .setResponses(
                    listOf(
                        ReassignableTopicResponse()
                            .setName("A")
                            .setPartitions(listOf(normalPartitionResponse)),
                        ReassignableTopicResponse()
                            .setName("B")
                            .setPartitions(listOf(normalPartitionResponse))
                    )
                )
            env.mockClient.prepareResponse(
                AlterPartitionReassignmentsResponse(controllerErrResponseData)
            )
            env.mockClient.prepareResponse(controllerNodeResponse)
            env.mockClient.prepareResponse(AlterPartitionReassignmentsResponse(normalResponse))
            val controllerErrResult = env.adminClient.alterPartitionReassignments(reassignments)
            controllerErrResult.all().get()
            controllerErrResult.futures[tp1]!!.get()
            controllerErrResult.futures[tp2]!!.get()

            // 3. partition-level error
            val partitionLevelErrData = AlterPartitionReassignmentsResponseData().setResponses(
                listOf(
                    ReassignableTopicResponse()
                        .setName("A")
                        .setPartitions(
                            listOf(
                                ReassignablePartitionResponse()
                                    .setPartitionIndex(0)
                                    .setErrorMessage(Errors.INVALID_REPLICA_ASSIGNMENT.message)
                                    .setErrorCode(Errors.INVALID_REPLICA_ASSIGNMENT.code)
                            )
                        ),
                    ReassignableTopicResponse()
                        .setName("B")
                        .setPartitions(listOf(normalPartitionResponse))
                )
            )
            env.mockClient.prepareResponse(
                AlterPartitionReassignmentsResponse(partitionLevelErrData)
            )
            val partitionLevelErrResult =
                env.adminClient.alterPartitionReassignments(reassignments)
            assertFutureError(
                future = partitionLevelErrResult.futures.get(tp1)!!,
                exceptionClass = Errors.INVALID_REPLICA_ASSIGNMENT.exception!!.javaClass,
            )
            partitionLevelErrResult.futures.get(tp2)!!.get()

            // 4. top-level error
            val errorMessage = "this is custom error message"
            val topLevelErrResponseData = AlterPartitionReassignmentsResponseData()
                .setErrorCode(Errors.CLUSTER_AUTHORIZATION_FAILED.code)
                .setErrorMessage(errorMessage)
                .setResponses(
                    listOf(
                        ReassignableTopicResponse()
                            .setName("A")
                            .setPartitions(listOf(normalPartitionResponse)),
                        ReassignableTopicResponse()
                            .setName("B")
                            .setPartitions(listOf(normalPartitionResponse))
                    )
                )
            env.mockClient.prepareResponse(
                AlterPartitionReassignmentsResponse(topLevelErrResponseData)
            )
            val topLevelErrResult = env.adminClient.alterPartitionReassignments(reassignments)
            assertEquals(
                expected = errorMessage,
                actual = assertFutureThrows(
                    future = topLevelErrResult.all(),
                    exceptionCauseClass = Errors.CLUSTER_AUTHORIZATION_FAILED.exception!!.javaClass,
                ).message,
            )
            assertEquals(
                expected = errorMessage,
                actual = assertFutureThrows(
                    future = topLevelErrResult.futures.get(tp1)!!,
                    exceptionCauseClass = Errors.CLUSTER_AUTHORIZATION_FAILED.exception!!.javaClass,
                ).message,
            )
            assertEquals(
                expected = errorMessage,
                actual = assertFutureThrows(
                    future = topLevelErrResult.futures.get(tp2)!!,
                    exceptionCauseClass = Errors.CLUSTER_AUTHORIZATION_FAILED.exception!!.javaClass,
                ).message,
            )

            // 5. unrepresentable topic name error
            val invalidTopicTP = TopicPartition("", 0)
            val invalidPartitionTP = TopicPartition("ABC", -1)
            val invalidTopicReassignments = mapOf(
                invalidPartitionTP to NewPartitionReassignment(listOf(1, 2, 3)),
                invalidTopicTP to NewPartitionReassignment(listOf(1, 2, 3)),
                tp1 to NewPartitionReassignment(listOf(1, 2, 3)),
            )
            val singlePartResponseData = AlterPartitionReassignmentsResponseData()
                .setResponses(
                    listOf(
                        ReassignableTopicResponse()
                            .setName("A")
                            .setPartitions(listOf(normalPartitionResponse))
                    )
                )
            env.mockClient.prepareResponse(
                AlterPartitionReassignmentsResponse(singlePartResponseData)
            )
            val unrepresentableTopicResult =
                env.adminClient.alterPartitionReassignments(invalidTopicReassignments)
            assertFutureError(
                future = unrepresentableTopicResult.futures.get(invalidTopicTP)!!,
                exceptionClass = InvalidTopicException::class.java
            )
            assertFutureError(
                future = unrepresentableTopicResult.futures.get(invalidPartitionTP)!!,
                exceptionClass = InvalidTopicException::class.java,
            )
            unrepresentableTopicResult.futures.get(tp1)!!.get()

            // Test success scenario
            val noErrResponseData = AlterPartitionReassignmentsResponseData()
                .setErrorCode(Errors.NONE.code)
                .setErrorMessage(Errors.NONE.message)
                .setResponses(
                    listOf(
                        ReassignableTopicResponse()
                            .setName("A")
                            .setPartitions(listOf(normalPartitionResponse)),
                        ReassignableTopicResponse()
                            .setName("B")
                            .setPartitions(listOf(normalPartitionResponse))
                    )
                )
            env.mockClient.prepareResponse(
                AlterPartitionReassignmentsResponse(noErrResponseData)
            )
            val noErrResult = env.adminClient.alterPartitionReassignments(reassignments)
            noErrResult.all().get()
            noErrResult.futures[tp1]!!.get()
            noErrResult.futures[tp2]!!.get()
        }
    }

    @Test
    @Throws(Exception::class)
    fun testListPartitionReassignments() {
        mockClientEnv().use { env ->
            env.mockClient.setNodeApiVersions(NodeApiVersions.create())
            val tp1 = TopicPartition("A", 0)
            val tp1PartitionReassignment = OngoingPartitionReassignment()
                .setPartitionIndex(0)
                .setRemovingReplicas(intArrayOf(1, 2, 3))
                .setAddingReplicas(intArrayOf(4, 5, 6))
                .setReplicas(intArrayOf(1, 2, 3, 4, 5, 6))
            val tp1Reassignment = OngoingTopicReassignment()
                .setName("A")
                .setPartitions(listOf(tp1PartitionReassignment))
            val tp2 = TopicPartition("B", 0)
            val tp2PartitionReassignment = OngoingPartitionReassignment()
                .setPartitionIndex(0)
                .setRemovingReplicas(intArrayOf(1, 2, 3))
                .setAddingReplicas(intArrayOf(4, 5, 6))
                .setReplicas(intArrayOf(1, 2, 3, 4, 5, 6))
            val tp2Reassignment = OngoingTopicReassignment().setName("B")
                .setPartitions(listOf(tp2PartitionReassignment))

            // 1. NOT_CONTROLLER error handling
            val notControllerData = ListPartitionReassignmentsResponseData()
                .setErrorCode(Errors.NOT_CONTROLLER.code)
                .setErrorMessage(Errors.NOT_CONTROLLER.message)
            val controllerNodeResponse = RequestTestUtils.metadataResponse(
                brokers = env.cluster.nodes,
                clusterId = env.cluster.clusterResource.clusterId,
                controllerId = 1,
                topicMetadataList = emptyList()
            )
            val reassignmentsData = ListPartitionReassignmentsResponseData()
                .setTopics(listOf(tp1Reassignment, tp2Reassignment))
            env.mockClient.prepareResponse(ListPartitionReassignmentsResponse(notControllerData))
            env.mockClient.prepareResponse(controllerNodeResponse)
            env.mockClient.prepareResponse(ListPartitionReassignmentsResponse(reassignmentsData))
            val noControllerResult = env.adminClient.listPartitionReassignments()
            noControllerResult.reassignments().get() // no error

            // 2. UNKNOWN_TOPIC_OR_EXCEPTION_ERROR
            val unknownTpData = ListPartitionReassignmentsResponseData()
                .setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code)
                .setErrorMessage(Errors.UNKNOWN_TOPIC_OR_PARTITION.message)
            env.mockClient.prepareResponse(ListPartitionReassignmentsResponse(unknownTpData))
            val unknownTpResult = env.adminClient.listPartitionReassignments(setOf(tp1, tp2))
            assertFutureError(
                future = unknownTpResult.reassignments(),
                exceptionClass = UnknownTopicOrPartitionException::class.java,
            )

            // 3. Success
            val responseData = ListPartitionReassignmentsResponseData()
                .setTopics(listOf(tp1Reassignment, tp2Reassignment))
            env.mockClient.prepareResponse(ListPartitionReassignmentsResponse(responseData))
            val responseResult = env.adminClient.listPartitionReassignments()
            val reassignments = responseResult.reassignments().get()
            val tp1Result = reassignments[tp1]
            assertEquals(
                expected = tp1PartitionReassignment.addingReplicas.toList(),
                actual = tp1Result!!.addingReplicas,
            )
            assertEquals(
                expected = tp1PartitionReassignment.removingReplicas.toList(),
                actual = tp1Result.removingReplicas,
            )
            assertEquals(
                expected = tp1PartitionReassignment.replicas.toList(),
                actual = tp1Result.replicas,
            )
            assertEquals(
                expected = tp1PartitionReassignment.replicas.toList(),
                actual = tp1Result.replicas,
            )
            val tp2Result = reassignments[tp2]
            assertEquals(
                expected = tp2PartitionReassignment.addingReplicas.toList(),
                actual = tp2Result!!.addingReplicas,
            )
            assertEquals(
                expected = tp2PartitionReassignment.removingReplicas.toList(),
                actual = tp2Result.removingReplicas,
            )
            assertEquals(
                expected = tp2PartitionReassignment.replicas.toList(),
                actual = tp2Result.replicas,
            )
            assertEquals(
                expected = tp2PartitionReassignment.replicas.toList(),
                actual = tp2Result.replicas,
            )
        }
    }

    @Test
    @Throws(Exception::class)
    fun testAlterConsumerGroupOffsets() {
        // Happy path
        val tp1 = TopicPartition("foo", 0)
        val tp2 = TopicPartition("bar", 0)
        val tp3 = TopicPartition("foobar", 0)
        AdminClientUnitTestEnv(mockCluster(numNodes = 1, controllerIndex = 0)).use { env ->
            env.mockClient.setNodeApiVersions(NodeApiVersions.create())
            env.mockClient.prepareResponse(
                prepareFindCoordinatorResponse(Errors.NONE, env.cluster.controller!!)
            )
            val responseData = mapOf(
                tp1 to Errors.NONE,
                tp2 to Errors.NONE,
            )
            env.mockClient.prepareResponse(OffsetCommitResponse(0, responseData))
            val offsets = mapOf(
                tp1 to OffsetAndMetadata(123L),
                tp2 to OffsetAndMetadata(456L),
            )
            val result = env.adminClient.alterConsumerGroupOffsets(GROUP_ID, offsets)
            assertNotFails(result.all()::get)
            assertNotFails(result.partitionResult(tp1)::get)
            assertNotFails(result.partitionResult(tp2)::get)
            assertFutureError(
                future = result.partitionResult(tp3),
                exceptionClass = IllegalArgumentException::class.java,
            )
        }
    }

    @Test
    @Throws(Exception::class)
    fun testAlterConsumerGroupOffsetsRetriableErrors() {
        // Retriable errors should be retried
        val tp1 = TopicPartition("foo", 0)
        AdminClientUnitTestEnv(mockCluster(numNodes = 1, controllerIndex = 0)).use { env ->
            env.mockClient.setNodeApiVersions(NodeApiVersions.create())
            env.mockClient.prepareResponse(
                prepareFindCoordinatorResponse(Errors.NONE, env.cluster.controller!!)
            )
            env.mockClient.prepareResponse(
                prepareOffsetCommitResponse(tp1, Errors.COORDINATOR_NOT_AVAILABLE)
            )
            env.mockClient.prepareResponse(
                prepareFindCoordinatorResponse(Errors.NONE, env.cluster.controller!!)
            )
            env.mockClient.prepareResponse(
                prepareOffsetCommitResponse(tp1, Errors.COORDINATOR_LOAD_IN_PROGRESS)
            )
            env.mockClient.prepareResponse(
                prepareOffsetCommitResponse(tp1, Errors.NOT_COORDINATOR)
            )
            env.mockClient.prepareResponse(
                prepareFindCoordinatorResponse(Errors.NONE, env.cluster.controller!!)
            )
            env.mockClient.prepareResponse(
                prepareOffsetCommitResponse(tp1, Errors.REBALANCE_IN_PROGRESS)
            )
            env.mockClient.prepareResponse(prepareOffsetCommitResponse(tp1, Errors.NONE))
            val offsets = mapOf(
                tp1 to OffsetAndMetadata(123L),
            )
            val result1 = env.adminClient.alterConsumerGroupOffsets(GROUP_ID, offsets)
            assertNotFails(result1.all()::get)
            assertNotFails(result1.partitionResult(tp1)::get)
        }
    }

    @Test
    @Throws(Exception::class)
    fun testAlterConsumerGroupOffsetsNonRetriableErrors() {
        // Non-retriable errors throw an exception
        val tp1 = TopicPartition(topic = "foo", partition = 0)
        val nonRetriableErrors = listOf(
            Errors.GROUP_AUTHORIZATION_FAILED,
            Errors.INVALID_GROUP_ID,
            Errors.GROUP_ID_NOT_FOUND,
        )
        AdminClientUnitTestEnv(mockCluster(numNodes = 1, controllerIndex = 0)).use { env ->
            env.mockClient.setNodeApiVersions(NodeApiVersions.create())
            for (error in nonRetriableErrors) {
                env.mockClient.prepareResponse(
                    prepareFindCoordinatorResponse(Errors.NONE, env.cluster.controller!!)
                )
                env.mockClient.prepareResponse(prepareOffsetCommitResponse(tp1, error))
                val offsets = mapOf(tp1 to OffsetAndMetadata(123L))
                val errorResult = env.adminClient.alterConsumerGroupOffsets(GROUP_ID, offsets)
                assertFutureError(
                    future = errorResult.all(),
                    exceptionClass = error.exception!!.javaClass,
                )
                assertFutureError(
                    future = errorResult.partitionResult(tp1),
                    exceptionClass = error.exception!!.javaClass,
                )
            }
        }
    }

    @Test
    @Throws(Exception::class)
    fun testAlterConsumerGroupOffsetsFindCoordinatorRetriableErrors() {
        // Retriable FindCoordinatorResponse errors should be retried
        val tp1 = TopicPartition(topic = "foo", partition = 0)
        AdminClientUnitTestEnv(mockCluster(numNodes = 1, controllerIndex = 0)).use { env ->
            env.mockClient.setNodeApiVersions(NodeApiVersions.create())
            env.mockClient.prepareResponse(
                prepareFindCoordinatorResponse(Errors.COORDINATOR_NOT_AVAILABLE, Node.noNode())
            )
            env.mockClient.prepareResponse(
                prepareFindCoordinatorResponse(Errors.COORDINATOR_LOAD_IN_PROGRESS, Node.noNode())
            )
            env.mockClient.prepareResponse(
                prepareFindCoordinatorResponse(Errors.NONE, env.cluster.controller!!)
            )
            env.mockClient.prepareResponse(prepareOffsetCommitResponse(tp1, Errors.NONE))
            val offsets = mapOf(tp1 to OffsetAndMetadata(123L))
            val result = env.adminClient.alterConsumerGroupOffsets(GROUP_ID, offsets)
            assertNotFails(result.all()::get)
            assertNotFails(result.partitionResult(tp1)::get)
        }
    }

    @Test
    @Throws(Exception::class)
    fun testAlterConsumerGroupOffsetsFindCoordinatorNonRetriableErrors() {
        // Non-retriable FindCoordinatorResponse errors throw an exception
        val tp1 = TopicPartition("foo", 0)
        AdminClientUnitTestEnv(mockCluster(numNodes = 1, controllerIndex = 0)).use { env ->
            env.mockClient.setNodeApiVersions(NodeApiVersions.create())
            env.mockClient.prepareResponse(
                prepareFindCoordinatorResponse(Errors.GROUP_AUTHORIZATION_FAILED, Node.noNode())
            )
            val offsets = mapOf(tp1 to OffsetAndMetadata(123L))
            val errorResult = env.adminClient.alterConsumerGroupOffsets(GROUP_ID, offsets)
            assertFutureError(
                future = errorResult.all(),
                exceptionClass = GroupAuthorizationException::class.java,
            )
            assertFutureError(
                future = errorResult.partitionResult(tp1),
                exceptionClass = GroupAuthorizationException::class.java,
            )
        }
    }

    @Test
    @Throws(Exception::class)
    fun testListOffsets() {
        // Happy path
        val node0 = Node(0, "localhost", 8120)
        val pInfos: MutableList<PartitionInfo> = ArrayList()
        pInfos.add(PartitionInfo("foo", 0, node0, listOf(node0), listOf(node0)))
        pInfos.add(PartitionInfo("bar", 0, node0, listOf(node0), listOf(node0)))
        pInfos.add(PartitionInfo("baz", 0, node0, listOf(node0), listOf(node0)))
        pInfos.add(PartitionInfo("qux", 0, node0, listOf(node0), listOf(node0)))
        val cluster = Cluster(
            clusterId = "mockClusterId",
            nodes = listOf(node0),
            partitions = pInfos,
            controller = node0,
        )
        val tp0 = TopicPartition("foo", 0)
        val tp1 = TopicPartition("bar", 0)
        val tp2 = TopicPartition("baz", 0)
        val tp3 = TopicPartition("qux", 0)
        AdminClientUnitTestEnv(cluster).use { env ->
            env.mockClient.setNodeApiVersions(NodeApiVersions.create())
            env.mockClient.prepareResponse(prepareMetadataResponse(cluster, Errors.NONE))
            val t0 = ListOffsetsResponse.singletonListOffsetsTopicResponse(
                tp = tp0,
                error = Errors.NONE,
                timestamp = -1L,
                offset = 123L,
                epoch = 321,
            )
            val t1 = ListOffsetsResponse.singletonListOffsetsTopicResponse(
                tp = tp1,
                error = Errors.NONE,
                timestamp = -1L,
                offset = 234L,
                epoch = 432,
            )
            val t2 = ListOffsetsResponse.singletonListOffsetsTopicResponse(
                tp = tp2,
                error = Errors.NONE,
                timestamp = 123456789L,
                offset = 345L,
                epoch = 543,
            )
            val t3 = ListOffsetsResponse.singletonListOffsetsTopicResponse(
                tp = tp3,
                error = Errors.NONE,
                timestamp = 234567890L,
                offset = 456L,
                epoch = 654,
            )
            val responseData = ListOffsetsResponseData()
                .setThrottleTimeMs(0)
                .setTopics(listOf(t0, t1, t2, t3))
            env.mockClient.prepareResponse(ListOffsetsResponse(responseData))
            val partitions = mapOf(
                tp0 to OffsetSpec.latest(),
                tp1 to OffsetSpec.earliest(),
                tp2 to OffsetSpec.forTimestamp(System.currentTimeMillis()),
                tp3 to OffsetSpec.maxTimestamp(),
            )
            val result = env.adminClient.listOffsets(partitions)
            val offsets = result.all().get()
            assertFalse(offsets.isEmpty())
            assertEquals(expected = 123L, actual = offsets[tp0]!!.offset)
            assertEquals(expected = 321, actual = offsets[tp0]!!.leaderEpoch)
            assertEquals(expected = -1L, actual = offsets[tp0]!!.timestamp)
            assertEquals(expected = 234L, actual = offsets[tp1]!!.offset)
            assertEquals(expected = 432, actual = offsets[tp1]!!.leaderEpoch)
            assertEquals(expected = -1L, actual = offsets[tp1]!!.timestamp)
            assertEquals(expected = 345L, actual = offsets[tp2]!!.offset)
            assertEquals(expected = 543, actual = offsets[tp2]!!.leaderEpoch)
            assertEquals(expected = 123456789L, actual = offsets[tp2]!!.timestamp)
            assertEquals(expected = 456L, actual = offsets[tp3]!!.offset)
            assertEquals(expected = 654, actual = offsets[tp3]!!.leaderEpoch)
            assertEquals(expected = 234567890L, actual = offsets[tp3]!!.timestamp)
            assertEquals(expected = offsets[tp0], actual = result.partitionResult(tp0).get())
            assertEquals(expected = offsets[tp1], actual = result.partitionResult(tp1).get())
            assertEquals(expected = offsets[tp2], actual = result.partitionResult(tp2).get())
            assertEquals(expected = offsets[tp3], actual = result.partitionResult(tp3).get())
            try {
                result.partitionResult(TopicPartition("unknown", 0)).get()
                fail("should have thrown IllegalArgumentException")
            } catch (expected: IllegalArgumentException) {
            }
        }
    }

    @Test
    @Throws(Exception::class)
    fun testListOffsetsRetriableErrors() {
        val node0 = Node(0, "localhost", 8120)
        val node1 = Node(1, "localhost", 8121)
        val nodes = listOf(node0, node1)
        val pInfos = listOf(
            PartitionInfo(
                topic = "foo",
                partition = 0,
                leader = node0,
                replicas = listOf(node0, node1),
                inSyncReplicas = listOf(node0, node1)
            ),
            PartitionInfo(
                topic = "foo",
                partition = 1,
                leader = node0,
                replicas = listOf(node0, node1),
                inSyncReplicas = listOf(node0, node1)
            ),
            PartitionInfo(
                topic = "bar",
                partition = 0,
                leader = node1,
                replicas = listOf(node1, node0),
                inSyncReplicas = listOf(node1, node0)
            ),
        )
        val cluster = Cluster(
            clusterId = "mockClusterId",
            nodes = nodes,
            partitions = pInfos,
            controller = node0,
        )
        val tp0 = TopicPartition("foo", 0)
        val tp1 = TopicPartition("foo", 1)
        val tp2 = TopicPartition("bar", 0)
        AdminClientUnitTestEnv(cluster).use { env ->
            env.mockClient.setNodeApiVersions(NodeApiVersions.create())
            env.mockClient.prepareResponse(prepareMetadataResponse(cluster, Errors.NONE))
            // listoffsets response from broker 0
            var t0 = ListOffsetsResponse.singletonListOffsetsTopicResponse(
                tp = tp0,
                error = Errors.LEADER_NOT_AVAILABLE,
                timestamp = -1L,
                offset = 123L,
                epoch = 321,
            )
            val t1 = ListOffsetsResponse.singletonListOffsetsTopicResponse(
                tp = tp1,
                error = Errors.NONE,
                timestamp = -1L,
                offset = 987L,
                epoch = 789,
            )
            var responseData = ListOffsetsResponseData()
                .setThrottleTimeMs(0)
                .setTopics(listOf(t0, t1))
            env.mockClient.prepareResponseFrom(
                response = ListOffsetsResponse(responseData),
                node = node0,
            )
            // listoffsets response from broker 1
            val t2 = ListOffsetsResponse.singletonListOffsetsTopicResponse(
                tp = tp2,
                error = Errors.NONE,
                timestamp = -1L,
                offset = 456L,
                epoch = 654,
            )
            responseData = ListOffsetsResponseData()
                .setThrottleTimeMs(0)
                .setTopics(listOf(t2))
            env.mockClient.prepareResponseFrom(
                response = ListOffsetsResponse(responseData),
                node = node1,
            )

            // metadata refresh because of LEADER_NOT_AVAILABLE
            env.mockClient.prepareResponse(prepareMetadataResponse(cluster, Errors.NONE))
            // listoffsets response from broker 0
            t0 = ListOffsetsResponse.singletonListOffsetsTopicResponse(
                tp = tp0,
                error = Errors.NONE,
                timestamp = -1L,
                offset = 345L,
                epoch = 543,
            )
            responseData = ListOffsetsResponseData()
                .setThrottleTimeMs(0)
                .setTopics(listOf(t0))
            env.mockClient.prepareResponseFrom(
                response = ListOffsetsResponse(responseData),
                node = node0,
            )
            val partitions = mapOf(
                tp0 to OffsetSpec.latest(),
                tp1 to OffsetSpec.latest(),
                tp2 to OffsetSpec.latest(),
            )
            val result = env.adminClient.listOffsets(partitions)
            val offsets = result.all().get()
            assertFalse(offsets.isEmpty())
            assertEquals(expected = 345L, actual = offsets[tp0]!!.offset)
            assertEquals(expected = 543, actual = offsets[tp0]!!.leaderEpoch)
            assertEquals(expected = -1L, actual = offsets[tp0]!!.timestamp)
            assertEquals(expected = 987L, actual = offsets[tp1]!!.offset)
            assertEquals(expected = 789, actual = offsets[tp1]!!.leaderEpoch)
            assertEquals(expected = -1L, actual = offsets[tp1]!!.timestamp)
            assertEquals(expected = 456L, actual = offsets[tp2]!!.offset)
            assertEquals(expected = 654, actual = offsets[tp2]!!.leaderEpoch)
            assertEquals(expected = -1L, actual = offsets[tp2]!!.timestamp)
        }
    }

    @Test
    @Throws(Exception::class)
    fun testListOffsetsNonRetriableErrors() {
        val node0 = Node(id = 0, host = "localhost", port = 8120)
        val node1 = Node(id = 1, host = "localhost", port = 8121)
        val nodes = listOf(node0, node1)
        val pInfos = listOf(
            PartitionInfo(
                topic = "foo",
                partition = 0,
                leader = node0,
                replicas = listOf(node0, node1),
                inSyncReplicas = listOf(node0, node1),
            )
        )
        val cluster = Cluster(
            clusterId = "mockClusterId",
            nodes = nodes,
            partitions = pInfos,
            controller = node0,
        )
        val tp0 = TopicPartition("foo", 0)
        AdminClientUnitTestEnv(cluster).use { env ->
            env.mockClient.setNodeApiVersions(NodeApiVersions.create())
            env.mockClient.prepareResponse(prepareMetadataResponse(cluster, Errors.NONE))
            val t0 = ListOffsetsResponse.singletonListOffsetsTopicResponse(
                tp = tp0,
                error = Errors.TOPIC_AUTHORIZATION_FAILED,
                timestamp = -1L,
                offset = -1L,
                epoch = -1,
            )
            val responseData = ListOffsetsResponseData()
                .setThrottleTimeMs(0)
                .setTopics(listOf(t0))
            env.mockClient.prepareResponse(ListOffsetsResponse(responseData))
            val partitions = mapOf(tp0 to OffsetSpec.latest())
            val result = env.adminClient.listOffsets(partitions)
            assertFutureError(
                future = result.all(),
                exceptionClass = TopicAuthorizationException::class.java,
            )
        }
    }

    @Test
    fun testListOffsetsMaxTimestampUnsupportedSingleOffsetSpec() {
        val node = Node(0, "localhost", 8120)
        val nodes = listOf(node)
        val cluster = Cluster(
            clusterId = "mockClusterId",
            nodes = nodes,
            partitions = setOf(
                PartitionInfo(
                    topic = "foo",
                    partition = 0,
                    leader = node,
                    replicas = listOf(node),
                    inSyncReplicas = listOf(node),
                ),
            ),
            controller = node,
        )
        val tp0 = TopicPartition(topic = "foo", partition = 0)
        AdminClientUnitTestEnv(cluster, AdminClientConfig.RETRIES_CONFIG, "2").use { env ->
            env.mockClient.setNodeApiVersions(
                NodeApiVersions.create(
                    apiKey = ApiKeys.LIST_OFFSETS.id,
                    minVersion = 0,
                    maxVersion = 6,
                )
            )
            env.mockClient.prepareResponse(prepareMetadataResponse(cluster, Errors.NONE))

            // listoffsets response from broker 0
            env.mockClient.prepareUnsupportedVersionResponse { it is ListOffsetsRequest }
            val result = env.adminClient.listOffsets(mapOf(tp0 to OffsetSpec.maxTimestamp()))
            assertFutureThrows(
                future = result.all(),
                exceptionCauseClass = UnsupportedVersionException::class.java,
            )
        }
    }

    @Test
    @Throws(Exception::class)
    fun testListOffsetsMaxTimestampUnsupportedMultipleOffsetSpec() {
        val node = Node(0, "localhost", 8120)
        val nodes = listOf(node)
        val pInfos = listOf(
            PartitionInfo(
                topic = "foo",
                partition = 0,
                leader = node,
                replicas = listOf(node),
                inSyncReplicas = listOf(node),
            ),
            PartitionInfo(
                topic = "foo",
                partition = 1,
                leader = node,
                replicas = listOf(node),
                inSyncReplicas = listOf(node)
            ),
        )
        val cluster = Cluster(
            clusterId = "mockClusterId",
            nodes = nodes,
            partitions = pInfos,
            controller = node,
        )
        val tp0 = TopicPartition(topic = "foo", partition = 0)
        val tp1 = TopicPartition(topic = "foo", partition = 1)
        AdminClientUnitTestEnv(
            cluster,
            AdminClientConfig.RETRIES_CONFIG,
            "2",
        ).use { env ->
            env.mockClient.setNodeApiVersions(
                NodeApiVersions.create(
                    apiKey = ApiKeys.LIST_OFFSETS.id,
                    minVersion = 0,
                    maxVersion = 6,
                )
            )
            env.mockClient.prepareResponse(prepareMetadataResponse(cluster, Errors.NONE))

            // listoffsets response from broker 0
            env.mockClient.prepareUnsupportedVersionResponse { it is ListOffsetsRequest }
            val topicResponse = ListOffsetsResponse.singletonListOffsetsTopicResponse(
                tp = tp1,
                error = Errors.NONE,
                timestamp = -1L,
                offset = 345L,
                epoch = 543,
            )
            val responseData = ListOffsetsResponseData()
                .setThrottleTimeMs(0)
                .setTopics(listOf(topicResponse))
            env.mockClient
                .prepareResponseFrom(
                    // ensure that no max timestamp requests are retried
                    matcher = { request ->
                        request is ListOffsetsRequest && request.topics
                            .flatMap { t -> t.partitions }
                            .none { p -> p.timestamp == ListOffsetsRequest.MAX_TIMESTAMP }
                    },
                    response = ListOffsetsResponse(responseData),
                    node = node,
                )
            val result = env.adminClient.listOffsets(
                mapOf(
                    tp0 to OffsetSpec.maxTimestamp(),
                    tp1 to OffsetSpec.latest(),
                )
            )
            assertFutureThrows(
                future = result.partitionResult(tp0),
                exceptionCauseClass = UnsupportedVersionException::class.java,
            )
            val tp1Offset = result.partitionResult(tp1).get()
            assertEquals(expected = 345L, actual = tp1Offset.offset)
            assertEquals(expected = 543, actual = tp1Offset.leaderEpoch)
            assertEquals(expected = -1L, actual = tp1Offset.timestamp)
        }
    }

    @Test
    fun testListOffsetsUnsupportedNonMaxTimestamp() {
        val node = Node(0, "localhost", 8120)
        val nodes = listOf(node)
        val pInfos = listOf(
            PartitionInfo(
                topic = "foo",
                partition = 0,
                leader = node,
                replicas = listOf(node),
                inSyncReplicas = listOf(node),
            )
        )
        val cluster = Cluster(
            clusterId = "mockClusterId",
            nodes = nodes,
            partitions = pInfos,
            controller = node,
        )
        val tp0 = TopicPartition(topic = "foo", partition = 0)
        AdminClientUnitTestEnv(
            cluster,
            AdminClientConfig.RETRIES_CONFIG,
            "2",
        ).use { env ->
            env.mockClient.setNodeApiVersions(
                NodeApiVersions.create(
                    apiKey = ApiKeys.LIST_OFFSETS.id,
                    minVersion = 0,
                    maxVersion = 0,
                )
            )
            env.mockClient.prepareResponse(prepareMetadataResponse(cluster, Errors.NONE))

            // listoffsets response from broker 0
            env.mockClient.prepareUnsupportedVersionResponse { it is ListOffsetsRequest }
            val result = env.adminClient.listOffsets(
                mapOf(tp0 to OffsetSpec.latest())
            )
            assertFutureThrows(
                future = result.partitionResult(tp0),
                exceptionCauseClass = UnsupportedVersionException::class.java,
            )
        }
    }

    @Test
    @Throws(Exception::class)
    fun testListOffsetsNonMaxTimestampDowngradedImmediately() {
        val node = Node(0, "localhost", 8120)
        val nodes = listOf(node)
        val pInfos = listOf(
            PartitionInfo(
                topic = "foo",
                partition = 0,
                leader = node,
                replicas = listOf(node),
                inSyncReplicas = listOf(node),
            ),
        )
        val cluster = Cluster(
            clusterId = "mockClusterId",
            nodes = nodes,
            partitions = pInfos,
            controller = node,
        )
        val tp0 = TopicPartition("foo", 0)
        AdminClientUnitTestEnv(
            cluster,
            AdminClientConfig.RETRIES_CONFIG,
            "2"
        ).use { env ->
            env.mockClient.setNodeApiVersions(
                NodeApiVersions.create(
                    apiKey = ApiKeys.LIST_OFFSETS.id,
                    minVersion = 0,
                    maxVersion = 6,
                )
            )
            env.mockClient.prepareResponse(prepareMetadataResponse(cluster, Errors.NONE))
            val t0 = ListOffsetsResponse.singletonListOffsetsTopicResponse(
                tp = tp0,
                error = Errors.NONE,
                timestamp = -1L,
                offset = 123L,
                epoch = 321,
            )
            val responseData = ListOffsetsResponseData()
                .setThrottleTimeMs(0)
                .setTopics(listOf(t0))

            // listoffsets response from broker 0
            env.mockClient.prepareResponse(
                matcher = { request -> request is ListOffsetsRequest },
                response = ListOffsetsResponse(responseData),
            )
            val result = env.adminClient.listOffsets(mapOf(tp0 to OffsetSpec.latest()))
            val tp0Offset = result.partitionResult(tp0).get()
            assertEquals(expected = 123L, actual = tp0Offset.offset)
            assertEquals(expected = 321, actual = tp0Offset.leaderEpoch)
            assertEquals(expected = -1L, actual = tp0Offset.timestamp)
        }
    }

    private fun makeTestFeatureUpdates(): Map<String, FeatureUpdate> {
        return mapOf(
            "test_feature_1" to FeatureUpdate(
                maxVersionLevel = 2,
                upgradeType = FeatureUpdate.UpgradeType.UPGRADE,
            ),
            "test_feature_2" to FeatureUpdate(
                maxVersionLevel = 3,
                upgradeType = FeatureUpdate.UpgradeType.SAFE_DOWNGRADE,
            ),
        )
    }

    private fun makeTestFeatureUpdateErrors(
        updates: Map<String, FeatureUpdate>,
        error: Errors,
    ): Map<String, ApiError> = updates.mapValues { ApiError(error) }

    @Throws(Exception::class)
    private fun testUpdateFeatures(
        featureUpdates: Map<String, FeatureUpdate>,
        topLevelError: ApiError,
        featureUpdateErrors: Map<String, ApiError>,
    ) {
        mockClientEnv().use { env ->
            env.mockClient.prepareResponse(
                matcher = { body -> body is UpdateFeaturesRequest },
                response = UpdateFeaturesResponse.createWithErrors(
                    topLevelError = topLevelError,
                    updateErrors = featureUpdateErrors,
                    throttleTimeMs = 0,
                ),
            )
            val futures = env.adminClient.updateFeatures(
                featureUpdates = featureUpdates,
                options = UpdateFeaturesOptions().apply { timeoutMs = 10000 },
            ).futures
            for ((key, future) in futures) {
                val error = featureUpdateErrors[key]
                if (topLevelError.error === Errors.NONE) {
                    assertNotNull(error)
                    if (error.error === Errors.NONE) future.get()
                    else {
                        val e = assertFailsWith<ExecutionException> { future.get() }
                        assertEquals(
                            expected = e.cause!!::class,
                            actual = error.exception()!!::class,
                        )
                    }
                } else {
                    val e = assertFailsWith<ExecutionException> { future.get() }
                    assertEquals(
                        expected = e.cause!!::class.java,
                        actual = topLevelError.exception()!!::class.java,
                    )
                }
            }
        }
    }

    @Test
    @Throws(Exception::class)
    fun testUpdateFeaturesDuringSuccess() {
        val updates = makeTestFeatureUpdates()
        testUpdateFeatures(
            featureUpdates = updates,
            topLevelError = ApiError.NONE,
            featureUpdateErrors = makeTestFeatureUpdateErrors(updates, Errors.NONE),
        )
    }

    @Test
    @Throws(Exception::class)
    fun testUpdateFeaturesTopLevelError() {
        val updates = makeTestFeatureUpdates()
        testUpdateFeatures(updates, ApiError(Errors.INVALID_REQUEST), emptyMap())
    }

    @Test
    @Throws(Exception::class)
    fun testUpdateFeaturesInvalidRequestError() {
        val updates = makeTestFeatureUpdates()
        testUpdateFeatures(
            featureUpdates = updates,
            topLevelError = ApiError.NONE,
            featureUpdateErrors = makeTestFeatureUpdateErrors(updates, Errors.INVALID_REQUEST),
        )
    }

    @Test
    @Throws(Exception::class)
    fun testUpdateFeaturesUpdateFailedError() {
        val updates = makeTestFeatureUpdates()
        testUpdateFeatures(
            featureUpdates = updates,
            topLevelError = ApiError.NONE,
            featureUpdateErrors = makeTestFeatureUpdateErrors(
                updates = updates,
                error = Errors.FEATURE_UPDATE_FAILED,
            ),
        )
    }

    @Test
    @Throws(Exception::class)
    fun testUpdateFeaturesPartialSuccess() {
        val errors = makeTestFeatureUpdateErrors(makeTestFeatureUpdates(), Errors.NONE).toMutableMap()
        errors["test_feature_2"] = ApiError(Errors.INVALID_REQUEST)
        testUpdateFeatures(makeTestFeatureUpdates(), ApiError.NONE, errors)
    }

    @Test
    @Throws(Exception::class)
    fun testUpdateFeaturesHandleNotControllerException() {
        mockClientEnv().use { env ->
            env.mockClient.prepareResponseFrom(
                matcher = { request -> request is UpdateFeaturesRequest },
                response = UpdateFeaturesResponse.createWithErrors(
                    ApiError(Errors.NOT_CONTROLLER),
                    emptyMap(),
                    0,
                ),
                node = env.cluster.nodeById(0),
            )
            val controllerId = 1
            env.mockClient.prepareResponse(
                RequestTestUtils.metadataResponse(
                    brokers = env.cluster.nodes,
                    clusterId = env.cluster.clusterResource.clusterId,
                    controllerId = controllerId,
                    topicMetadataList = emptyList(),
                )
            )
            env.mockClient.prepareResponseFrom(
                matcher = { request -> request is UpdateFeaturesRequest },
                response = UpdateFeaturesResponse.createWithErrors(
                    topLevelError = ApiError.NONE,
                    updateErrors = mapOf(
                        "test_feature_1" to ApiError.NONE,
                        "test_feature_2" to ApiError.NONE,
                    ),
                    throttleTimeMs = 0,
                ),
                node = env.cluster.nodeById(controllerId),
            )
            val future = env.adminClient.updateFeatures(
                featureUpdates = mapOf(
                    "test_feature_1" to FeatureUpdate(
                        maxVersionLevel = 2,
                        upgradeType = FeatureUpdate.UpgradeType.UPGRADE,
                    ),
                    "test_feature_2" to FeatureUpdate(
                        maxVersionLevel = 3,
                        upgradeType = FeatureUpdate.UpgradeType.SAFE_DOWNGRADE,
                    ),
                ),
                options = UpdateFeaturesOptions().apply { timeoutMs = 10000 },
            ).all()
            future.get()
        }
    }

    @Test
    fun testUpdateFeaturesShouldFailRequestForEmptyUpdates() {
        mockClientEnv().use { env ->
            assertFailsWith<IllegalArgumentException> {
                env.adminClient.updateFeatures(emptyMap(), UpdateFeaturesOptions())
            }
        }
    }

    @Test
    fun testUpdateFeaturesShouldFailRequestForInvalidFeatureName() {
        mockClientEnv().use { env ->
            assertFailsWith<IllegalArgumentException> {
                env.adminClient.updateFeatures(
                    featureUpdates = mapOf(
                        "feature" to FeatureUpdate(
                            maxVersionLevel = 2,
                            upgradeType = FeatureUpdate.UpgradeType.UPGRADE
                        ),
                        "" to FeatureUpdate(
                            maxVersionLevel = 2,
                            upgradeType = FeatureUpdate.UpgradeType.UPGRADE,
                        ),
                    ),
                    options = UpdateFeaturesOptions(),
                )
            }
        }
    }

    @Test
    fun testUpdateFeaturesShouldFailRequestInClientWhenDowngradeFlagIsNotSetDuringDeletion() {
        assertFailsWith<IllegalArgumentException> {
            FeatureUpdate(
                maxVersionLevel = 0,
                upgradeType = FeatureUpdate.UpgradeType.UPGRADE,
            )
        }
    }

    @Test
    @Throws(Exception::class)
    fun testDescribeFeaturesSuccess() {
        mockClientEnv().use { env ->
            env.mockClient.prepareResponse(
                matcher = { body -> body is ApiVersionsRequest },
                response = prepareApiVersionsResponseForDescribeFeatures(Errors.NONE),
            )
            val future = env.adminClient.describeFeatures(
                DescribeFeaturesOptions().apply { timeoutMs = 10000 }
            ).featureMetadata()
            val metadata: FeatureMetadata = future.get()
            assertEquals(
                expected = defaultFeatureMetadata(),
                actual = metadata
            )
        }
    }

    @Test
    fun testDescribeFeaturesFailure() {
        mockClientEnv().use { env ->
            env.mockClient.prepareResponse(
                matcher = { body -> body is ApiVersionsRequest },
                response = prepareApiVersionsResponseForDescribeFeatures(Errors.INVALID_REQUEST),
            )
            val options = DescribeFeaturesOptions().apply { timeoutMs = 10000 }
            val future = env.adminClient.describeFeatures(options).featureMetadata()
            val e = assertFailsWith<ExecutionException> { future.get() }
            assertEquals(
                expected = e.cause!!::class.java,
                actual = Errors.INVALID_REQUEST.exception!!::class.java,
            )
        }
    }

    @Test
    @Throws(Exception::class)
    fun testDescribeMetadataQuorumSuccess() {
        mockClientEnv().use { env ->
            env.mockClient.setNodeApiVersions(
                NodeApiVersions.create(
                    apiKey = ApiKeys.DESCRIBE_QUORUM.id,
                    minVersion = ApiKeys.DESCRIBE_QUORUM.oldestVersion(),
                    maxVersion = ApiKeys.DESCRIBE_QUORUM.latestVersion(),
                )
            )

            // Test with optional fields set
            env.mockClient.prepareResponse(
                matcher = { body -> body is DescribeQuorumRequest },
                response = prepareDescribeQuorumResponse(
                    topLevelError = Errors.NONE,
                    partitionLevelError = Errors.NONE,
                    topicCountError = false,
                    topicNameError = false,
                    partitionCountError = false,
                    partitionIndexError = false,
                    emptyOptionals = false,
                )
            )
            var future = env.adminClient.describeMetadataQuorum().quorumInfo
            var quorumInfo = future.get()
            assertEquals(expected = defaultQuorumInfo(false), actual = quorumInfo)

            // Test with optional fields empty
            env.mockClient.prepareResponse(
                matcher = { body -> body is DescribeQuorumRequest },
                response = prepareDescribeQuorumResponse(
                    topLevelError = Errors.NONE,
                    partitionLevelError = Errors.NONE,
                    topicCountError = false,
                    topicNameError = false,
                    partitionCountError = false,
                    partitionIndexError = false,
                    emptyOptionals = true,
                )
            )
            future = env.adminClient.describeMetadataQuorum().quorumInfo
            quorumInfo = future.get()
            assertEquals(expected = defaultQuorumInfo(true), actual = quorumInfo)
        }
    }

    @Test
    @Throws(Exception::class)
    fun testDescribeMetadataQuorumRetriableError() {
        mockClientEnv().use { env ->
            env.mockClient.setNodeApiVersions(
                NodeApiVersions.create(
                    apiKey = ApiKeys.DESCRIBE_QUORUM.id,
                    minVersion = ApiKeys.DESCRIBE_QUORUM.oldestVersion(),
                    maxVersion = ApiKeys.DESCRIBE_QUORUM.latestVersion(),
                )
            )

            // First request fails with a NOT_LEADER_OR_FOLLOWER error (which is retriable)
            env.mockClient.prepareResponse(
                matcher = { body -> body is DescribeQuorumRequest },
                response = prepareDescribeQuorumResponse(
                    topLevelError = Errors.NONE,
                    partitionLevelError = Errors.NOT_LEADER_OR_FOLLOWER,
                    topicCountError = false,
                    topicNameError = false,
                    partitionCountError = false,
                    partitionIndexError = false,
                    emptyOptionals = false,
                )
            )

            // The second request succeeds
            env.mockClient.prepareResponse(
                matcher = { body -> body is DescribeQuorumRequest },
                response = prepareDescribeQuorumResponse(
                    topLevelError = Errors.NONE,
                    partitionLevelError = Errors.NONE,
                    topicCountError = false,
                    topicNameError = false,
                    partitionCountError = false,
                    partitionIndexError = false,
                    emptyOptionals = false
                )
            )
            val future = env.adminClient.describeMetadataQuorum().quorumInfo
            val quorumInfo = future.get()
            assertEquals(expected = defaultQuorumInfo(false), actual = quorumInfo)
        }
    }

    @Test
    fun testDescribeMetadataQuorumFailure() {
        mockClientEnv().use { env ->
            env.mockClient.setNodeApiVersions(
                NodeApiVersions.create(
                    apiKey = ApiKeys.DESCRIBE_QUORUM.id,
                    minVersion = ApiKeys.DESCRIBE_QUORUM.oldestVersion(),
                    maxVersion = ApiKeys.DESCRIBE_QUORUM.latestVersion(),
                )
            )

            // Test top level error
            env.mockClient.prepareResponse(
                matcher = { body -> body is DescribeQuorumRequest },
                response = prepareDescribeQuorumResponse(
                    topLevelError = Errors.INVALID_REQUEST,
                    partitionLevelError = Errors.NONE,
                    topicCountError = false,
                    topicNameError = false,
                    partitionCountError = false,
                    partitionIndexError = false,
                    emptyOptionals = false,
                )
            )
            var future = env.adminClient.describeMetadataQuorum().quorumInfo
            assertFutureThrows(
                future = future,
                exceptionCauseClass = InvalidRequestException::class.java,
            )

            // Test incorrect topic count
            env.mockClient.prepareResponse(
                matcher = { body -> body is DescribeQuorumRequest },
                response = prepareDescribeQuorumResponse(
                    topLevelError = Errors.NONE,
                    partitionLevelError = Errors.NONE,
                    topicCountError = true,
                    topicNameError = false,
                    partitionCountError = false,
                    partitionIndexError = false,
                    emptyOptionals = false,
                )
            )
            future = env.adminClient.describeMetadataQuorum().quorumInfo
            assertFutureThrows(
                future = future,
                exceptionCauseClass = UnknownServerException::class.java,
            )

            // Test incorrect topic name
            env.mockClient.prepareResponse(
                matcher = { body -> body is DescribeQuorumRequest },
                response = prepareDescribeQuorumResponse(
                    topLevelError = Errors.NONE,
                    partitionLevelError = Errors.NONE,
                    topicCountError = false,
                    topicNameError = true,
                    partitionCountError = false,
                    partitionIndexError = false,
                    emptyOptionals = false,
                )
            )
            future = env.adminClient.describeMetadataQuorum().quorumInfo
            assertFutureThrows(
                future = future,
                exceptionCauseClass = UnknownServerException::class.java,
            )

            // Test incorrect partition count
            env.mockClient.prepareResponse(
                matcher = { body -> body is DescribeQuorumRequest },
                response = prepareDescribeQuorumResponse(
                    topLevelError = Errors.NONE,
                    partitionLevelError = Errors.NONE,
                    topicCountError = false,
                    topicNameError = false,
                    partitionCountError = true,
                    partitionIndexError = false,
                    emptyOptionals = false,
                )
            )
            future = env.adminClient.describeMetadataQuorum().quorumInfo
            assertFutureThrows(
                future = future,
                exceptionCauseClass = UnknownServerException::class.java,
            )

            // Test incorrect partition index
            env.mockClient.prepareResponse(
                matcher = { body -> body is DescribeQuorumRequest },
                response = prepareDescribeQuorumResponse(
                    topLevelError = Errors.NONE,
                    partitionLevelError = Errors.NONE,
                    topicCountError = false,
                    topicNameError = false,
                    partitionCountError = false,
                    partitionIndexError = true,
                    emptyOptionals = false,
                )
            )
            future = env.adminClient.describeMetadataQuorum().quorumInfo
            assertFutureThrows(
                future = future,
                exceptionCauseClass = UnknownServerException::class.java,
            )

            // Test partition level error
            env.mockClient.prepareResponse(
                matcher = { body -> body is DescribeQuorumRequest },
                response = prepareDescribeQuorumResponse(
                    topLevelError = Errors.NONE,
                    partitionLevelError = Errors.INVALID_REQUEST,
                    topicCountError = false,
                    topicNameError = false,
                    partitionCountError = false,
                    partitionIndexError = false,
                    emptyOptionals = false,
                )
            )
            future = env.adminClient.describeMetadataQuorum().quorumInfo
            assertFutureThrows(
                future = future,
                exceptionCauseClass = InvalidRequestException::class.java,
            )

            // Test all incorrect and no errors
            env.mockClient.prepareResponse(
                matcher = { body -> body is DescribeQuorumRequest },
                response = prepareDescribeQuorumResponse(
                    topLevelError = Errors.NONE,
                    partitionLevelError = Errors.NONE,
                    topicCountError = true,
                    topicNameError = true,
                    partitionCountError = true,
                    partitionIndexError = true,
                    emptyOptionals = false
                ),
            )
            future = env.adminClient.describeMetadataQuorum().quorumInfo
            assertFutureThrows(
                future = future,
                exceptionCauseClass = UnknownServerException::class.java,
            )

            // Test all incorrect and both errors
            env.mockClient.prepareResponse(
                matcher = { body -> body is DescribeQuorumRequest },
                response = prepareDescribeQuorumResponse(
                    topLevelError = Errors.INVALID_REQUEST,
                    partitionLevelError = Errors.INVALID_REQUEST,
                    topicCountError = true,
                    topicNameError = true,
                    partitionCountError = true,
                    partitionIndexError = true,
                    emptyOptionals = false,
                )
            )
            future = env.adminClient.describeMetadataQuorum().quorumInfo
            assertFutureThrows(
                future = future,
                exceptionCauseClass = Errors.INVALID_REQUEST.exception!!.javaClass,
            )
        }
    }

    @Test
    @Throws(Exception::class)
    fun testListOffsetsMetadataRetriableErrors() {
        val node0 = Node(0, "localhost", 8120)
        val node1 = Node(1, "localhost", 8121)
        val nodes = listOf(node0, node1)
        val pInfos = listOf(
            PartitionInfo(
                topic = "foo",
                partition = 0,
                leader = node0,
                replicas = listOf(node0),
                inSyncReplicas = listOf(node0),
            ),
            PartitionInfo(
                topic = "foo",
                partition = 1,
                leader = node1,
                replicas = listOf(node1),
                inSyncReplicas = listOf(node1),
            ),
        )
        val cluster = Cluster(
            clusterId = "mockClusterId",
            nodes = nodes,
            partitions = pInfos,
            controller = node0,
        )
        val tp0 = TopicPartition(topic = "foo", partition = 0)
        val tp1 = TopicPartition(topic = "foo", partition = 1)
        AdminClientUnitTestEnv(cluster).use { env ->
            env.mockClient.setNodeApiVersions(NodeApiVersions.create())
            env.mockClient.prepareResponse(
                prepareMetadataResponse(cluster, Errors.LEADER_NOT_AVAILABLE)
            )
            env.mockClient.prepareResponse(
                prepareMetadataResponse(cluster, Errors.UNKNOWN_TOPIC_OR_PARTITION)
            )
            env.mockClient.prepareResponse(prepareMetadataResponse(cluster, Errors.NONE))

            // listoffsets response from broker 0
            val t0 = ListOffsetsResponse.singletonListOffsetsTopicResponse(
                tp = tp0,
                error = Errors.NONE,
                timestamp = -1L,
                offset = 345L,
                epoch = 543,
            )
            var responseData = ListOffsetsResponseData()
                .setThrottleTimeMs(0)
                .setTopics(listOf(t0))
            env.mockClient.prepareResponseFrom(
                response = ListOffsetsResponse(responseData),
                node = node0,
            )
            // listoffsets response from broker 1
            val t1 = ListOffsetsResponse.singletonListOffsetsTopicResponse(
                tp = tp1,
                error = Errors.NONE,
                timestamp = -1L,
                offset = 789L,
                epoch = 987,
            )
            responseData = ListOffsetsResponseData()
                .setThrottleTimeMs(0)
                .setTopics(listOf(t1))
            env.mockClient.prepareResponseFrom(
                response = ListOffsetsResponse(responseData),
                node = node1,
            )
            val partitions = mapOf(
                tp0 to OffsetSpec.latest(),
                tp1 to OffsetSpec.latest(),
            )
            val result = env.adminClient.listOffsets(partitions)
            val offsets = result.all().get()
            assertFalse(offsets.isEmpty())
            assertEquals(expected = 345L, actual = offsets[tp0]!!.offset)
            assertEquals(expected = 543, actual = offsets[tp0]!!.leaderEpoch)
            assertEquals(expected = -1L, actual = offsets[tp0]!!.timestamp)
            assertEquals(expected = 789L, actual = offsets[tp1]!!.offset)
            assertEquals(expected = 987, actual = offsets[tp1]!!.leaderEpoch)
            assertEquals(expected = -1L, actual = offsets[tp1]!!.timestamp)
        }
    }

    @Test
    @Throws(Exception::class)
    fun testListOffsetsWithMultiplePartitionsLeaderChange() {
        val node0 = Node(id = 0, host = "localhost", port = 8120)
        val node1 = Node(id = 1, host = "localhost", port = 8121)
        val node2 = Node(id = 2, host = "localhost", port = 8122)
        val nodes = listOf(node0, node1, node2)
        val oldPInfo1 = PartitionInfo(
            topic = "foo",
            partition = 0,
            leader = node0,
            replicas = listOf(node0, node1, node2),
            inSyncReplicas = listOf(node0, node1, node2),
        )
        val oldPnfo2 = PartitionInfo(
            topic = "foo",
            partition = 1,
            leader = node0,
            replicas = listOf(node0, node1, node2),
            inSyncReplicas = listOf(node0, node1, node2),
        )
        val oldPInfos = listOf(oldPInfo1, oldPnfo2)
        val oldCluster = Cluster(
            clusterId = "mockClusterId",
            nodes = nodes,
            partitions = oldPInfos,
            controller = node0,
        )
        val tp0 = TopicPartition(topic = "foo", partition = 0)
        val tp1 = TopicPartition(topic = "foo", partition = 1)
        AdminClientUnitTestEnv(oldCluster).use { env ->
            env.mockClient.setNodeApiVersions(NodeApiVersions.create())
            env.mockClient.prepareResponse(prepareMetadataResponse(oldCluster, Errors.NONE))
            var t0 = ListOffsetsResponse.singletonListOffsetsTopicResponse(
                tp = tp0,
                error = Errors.NOT_LEADER_OR_FOLLOWER,
                timestamp = -1L,
                offset = 345L,
                epoch = 543,
            )
            var t1 = ListOffsetsResponse.singletonListOffsetsTopicResponse(
                tp = tp1,
                error = Errors.LEADER_NOT_AVAILABLE,
                timestamp = -2L,
                offset = 123L,
                epoch = 456,
            )
            var responseData = ListOffsetsResponseData()
                .setThrottleTimeMs(0)
                .setTopics(listOf(t0, t1))
            env.mockClient.prepareResponseFrom(
                response = ListOffsetsResponse(responseData),
                node = node0,
            )
            val newPInfo1 = PartitionInfo(
                topic = "foo",
                partition = 0,
                leader = node1,
                replicas = listOf(node0, node1, node2),
                inSyncReplicas = listOf(node0, node1, node2)
            )
            val newPInfo2 = PartitionInfo(
                topic = "foo",
                partition = 1,
                leader = node2,
                replicas = listOf(node0, node1, node2),
                inSyncReplicas = listOf(node0, node1, node2)
            )
            val newPInfos: List<PartitionInfo> = listOf(newPInfo1, newPInfo2)
            val newCluster = Cluster(
                clusterId = "mockClusterId",
                nodes = nodes,
                partitions = newPInfos,
                controller = node0,
            )
            env.mockClient.prepareResponse(prepareMetadataResponse(newCluster, Errors.NONE))
            t0 = ListOffsetsResponse.singletonListOffsetsTopicResponse(
                tp = tp0,
                error = Errors.NONE,
                timestamp = -1L,
                offset = 345L,
                epoch = 543,
            )
            responseData = ListOffsetsResponseData()
                .setThrottleTimeMs(0)
                .setTopics(listOf(t0))
            env.mockClient.prepareResponseFrom(
                response = ListOffsetsResponse(responseData),
                node = node1,
            )
            t1 = ListOffsetsResponse.singletonListOffsetsTopicResponse(
                tp = tp1,
                error = Errors.NONE,
                timestamp = -2L,
                offset = 123L,
                epoch = 456,
            )
            responseData = ListOffsetsResponseData()
                .setThrottleTimeMs(0)
                .setTopics(listOf(t1))
            env.mockClient.prepareResponseFrom(
                response = ListOffsetsResponse(responseData),
                node = node2,
            )
            val partitions = mapOf(
                tp0 to OffsetSpec.latest(),
                tp1 to OffsetSpec.latest(),
            )
            val result = env.adminClient.listOffsets(partitions)
            val offsets = result.all().get()
            assertFalse(offsets.isEmpty())
            assertEquals(expected = 345L, actual = offsets[tp0]!!.offset)
            assertEquals(expected = 543, actual = offsets[tp0]!!.leaderEpoch)
            assertEquals(expected = -1L, actual = offsets[tp0]!!.timestamp)
            assertEquals(expected = 123L, actual = offsets[tp1]!!.offset)
            assertEquals(expected = 456, actual = offsets[tp1]!!.leaderEpoch)
            assertEquals(expected = -2L, actual = offsets[tp1]!!.timestamp)
        }
    }

    @Test
    @Throws(Exception::class)
    fun testListOffsetsWithLeaderChange() {
        val node0 = Node(id = 0, host = "localhost", port = 8120)
        val node1 = Node(id = 1, host = "localhost", port = 8121)
        val node2 = Node(id = 2, host = "localhost", port = 8122)
        val nodes = listOf(node0, node1, node2)
        val oldPartitionInfo = PartitionInfo(
            topic = "foo",
            partition = 0,
            leader = node0,
            replicas = listOf(node0, node1, node2),
            inSyncReplicas = listOf(node0, node1, node2),
        )
        val oldCluster = Cluster(
            clusterId = "mockClusterId",
            nodes = nodes,
            partitions = listOf(oldPartitionInfo),
            controller = node0,
        )
        val tp0 = TopicPartition(topic = "foo", partition = 0)
        AdminClientUnitTestEnv(oldCluster).use { env ->
            env.mockClient.setNodeApiVersions(NodeApiVersions.create())
            env.mockClient.prepareResponse(prepareMetadataResponse(oldCluster, Errors.NONE))
            var t0 = ListOffsetsResponse.singletonListOffsetsTopicResponse(
                tp = tp0,
                error = Errors.NOT_LEADER_OR_FOLLOWER,
                timestamp = -1L,
                offset = 345L,
                epoch = 543
            )
            var responseData = ListOffsetsResponseData()
                .setThrottleTimeMs(0)
                .setTopics(listOf(t0))
            env.mockClient.prepareResponseFrom(
                response = ListOffsetsResponse(responseData),
                node = node0,
            )

            // updating leader from node0 to node1 and metadata refresh because of NOT_LEADER_OR_FOLLOWER
            val newPartitionInfo = PartitionInfo(
                topic = "foo",
                partition = 0,
                leader = node1,
                replicas = listOf(node0, node1, node2),
                inSyncReplicas = listOf(node0, node1, node2),
            )
            val newCluster = Cluster(
                clusterId = "mockClusterId",
                nodes = nodes,
                partitions = listOf(newPartitionInfo),
                controller = node0,
            )
            env.mockClient.prepareResponse(prepareMetadataResponse(newCluster, Errors.NONE))
            t0 = ListOffsetsResponse.singletonListOffsetsTopicResponse(
                tp = tp0,
                error = Errors.NONE,
                timestamp = -2L,
                offset = 123L,
                epoch = 456,
            )
            responseData = ListOffsetsResponseData()
                .setThrottleTimeMs(0)
                .setTopics(listOf(t0))
            env.mockClient.prepareResponseFrom(
                response = ListOffsetsResponse(responseData),
                node = node1,
            )
            val partitions = mapOf(tp0 to OffsetSpec.latest())
            val result = env.adminClient.listOffsets(partitions)
            val offsets = result.all().get()
            assertFalse(offsets.isEmpty())
            assertEquals(expected = 123L, actual = offsets[tp0]!!.offset)
            assertEquals(expected = 456, actual = offsets[tp0]!!.leaderEpoch)
            assertEquals(expected = -2L, actual = offsets[tp0]!!.timestamp)
        }
    }

    @Test
    @Throws(Exception::class)
    fun testListOffsetsMetadataNonRetriableErrors() {
        val node0 = Node(id = 0, host = "localhost", port = 8120)
        val node1 = Node(id = 1, host = "localhost", port = 8121)
        val nodes = listOf(node0, node1)
        val pInfos = listOf(
            PartitionInfo(
                topic = "foo",
                partition = 0,
                leader = node0,
                replicas = listOf(node0, node1),
                inSyncReplicas = listOf(node0, node1),
            )
        )
        val cluster = Cluster(
            clusterId = "mockClusterId",
            nodes = nodes,
            partitions = pInfos,
            controller = node0,
        )
        val tp1 = TopicPartition(topic = "foo", partition = 0)
        AdminClientUnitTestEnv(cluster).use { env ->
            env.mockClient.setNodeApiVersions(NodeApiVersions.create())
            env.mockClient.prepareResponse(
                prepareMetadataResponse(cluster, Errors.TOPIC_AUTHORIZATION_FAILED)
            )
            val partitions = mapOf(tp1 to OffsetSpec.latest())
            val result = env.adminClient.listOffsets(partitions)
            assertFutureError(
                future = result.all(),
                exceptionClass = TopicAuthorizationException::class.java,
            )
        }
    }

    @Test
    @Throws(Exception::class)
    fun testListOffsetsPartialResponse() {
        val node0 = Node(id = 0, host = "localhost", port = 8120)
        val node1 = Node(id = 1, host = "localhost", port = 8121)
        val nodes = listOf(node0, node1)
        val pInfos = listOf(
            PartitionInfo(
                topic = "foo",
                partition = 0,
                leader = node0,
                replicas = listOf(node0, node1),
                inSyncReplicas = listOf(node0, node1),
            ),
            PartitionInfo(
                topic = "foo",
                partition = 1,
                leader = node0,
                replicas = listOf(node0, node1),
                inSyncReplicas = listOf(node0, node1),
            ),
        )
        val cluster = Cluster(
            clusterId = "mockClusterId",
            nodes = nodes,
            partitions = pInfos,
            controller = node0,
        )
        val tp0 = TopicPartition(topic = "foo", partition = 0)
        val tp1 = TopicPartition(topic = "foo", partition = 1)
        AdminClientUnitTestEnv(cluster).use { env ->
            env.mockClient.setNodeApiVersions(NodeApiVersions.create())
            env.mockClient.prepareResponse(prepareMetadataResponse(cluster, Errors.NONE))
            val t0 = ListOffsetsResponse.singletonListOffsetsTopicResponse(
                tp = tp0,
                error = Errors.NONE,
                timestamp = -2L,
                offset = 123L,
                epoch = 456,
            )
            val data = ListOffsetsResponseData()
                .setThrottleTimeMs(0)
                .setTopics(listOf(t0))
            env.mockClient.prepareResponseFrom(
                response = ListOffsetsResponse(data),
                node = node0,
            )
            val partitions = mapOf(
                tp0 to OffsetSpec.latest(),
                tp1 to OffsetSpec.latest(),
            )
            val result = env.adminClient.listOffsets(partitions)
            assertNotNull(result.partitionResult(tp0).get())
            assertFutureThrows(
                future = result.partitionResult(tp1),
                exceptionCauseClass = ApiException::class.java,
            )
            assertFutureThrows(
                future = result.all(),
                exceptionCauseClass = ApiException::class.java,
            )
        }
    }

    @Test
    fun testGetSubLevelError() {
        val memberIdentities = listOf(
            MemberIdentity().setGroupInstanceId("instance-0"),
            MemberIdentity().setGroupInstanceId("instance-1")
        )
        val errorsMap = mapOf(
            memberIdentities[0] to Errors.NONE,
            memberIdentities[1] to Errors.FENCED_INSTANCE_ID,
        )
        assertIs<IllegalArgumentException>(
            KafkaAdminClient.getSubLevelError(
                subLevelErrors = errorsMap,
                subKey = MemberIdentity().setGroupInstanceId("non-exist-id"),
                keyNotFoundMsg = "For unit test",
            )
        )
        assertNull(
            KafkaAdminClient.getSubLevelError(
                subLevelErrors = errorsMap,
                subKey = memberIdentities[0],
                keyNotFoundMsg = "For unit test",
            )
        )
        assertIs<FencedInstanceIdException>(
            KafkaAdminClient.getSubLevelError(
                subLevelErrors = errorsMap,
                subKey = memberIdentities[1],
                keyNotFoundMsg = "For unit test",
            )
        )
    }

    @Test
    @Throws(Exception::class)
    fun testSuccessfulRetryAfterRequestTimeout() {
        val nodes = HashMap<Int, Node>()
        val time = MockTime()
        val node0 = Node(0, "localhost", 8121)
        nodes[0] = node0
        val cluster = Cluster(
            clusterId = "mockClusterId",
            nodes = nodes.values,
            partitions = listOf(
                PartitionInfo(
                    topic = "foo",
                    partition = 0,
                    leader = node0,
                    replicas = listOf(node0),
                    inSyncReplicas = listOf(node0),
                ),
            ),
            controller = nodes[0]
        )
        val requestTimeoutMs = 1000
        val retryBackoffMs = 100
        val apiTimeoutMs = 3000
        AdminClientUnitTestEnv(
            time,
            cluster,
            AdminClientConfig.RETRY_BACKOFF_MS_CONFIG,
            retryBackoffMs.toString(),
            AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG,
            requestTimeoutMs.toString(),
        ).use { env ->
            env.mockClient.setNodeApiVersions(NodeApiVersions.create())
            val result = env.adminClient.listTopics(
                ListTopicsOptions().apply { timeoutMs = apiTimeoutMs }
            )

            // Wait until the first attempt has been sent, then advance the time
            waitForCondition(
                testCondition = { env.mockClient.hasInFlightRequests() },
                conditionDetails = "Timed out waiting for Metadata request to be sent",
            )
            time.sleep((requestTimeoutMs + 1).toLong())

            // Wait for the request to be timed out before backing off
            waitForCondition(
                testCondition = { !env.mockClient.hasInFlightRequests() },
                conditionDetails = "Timed out waiting for inFlightRequests to be timed out",
            )
            time.sleep(retryBackoffMs.toLong())

            // Since api timeout bound is not hit, AdminClient should retry
            waitForCondition(
                testCondition = { env.mockClient.hasInFlightRequests() },
                conditionDetails = "Failed to retry Metadata request",
            )
            env.mockClient.respond(prepareMetadataResponse(cluster, Errors.NONE))
            assertEquals(expected = 1, actual = result.listings.get().size)
            assertEquals(expected = "foo", actual = result.listings.get().first().name)
        }
    }

    @Test
    @Throws(Exception::class)
    fun testDefaultApiTimeout() {
        testApiTimeout(
            requestTimeoutMs = 1500,
            defaultApiTimeoutMs = 3000,
            overrideApiTimeoutMs = null,
        )
    }

    @Test
    @Throws(Exception::class)
    fun testDefaultApiTimeoutOverride() {
        testApiTimeout(
            requestTimeoutMs = 1500,
            defaultApiTimeoutMs = 10000,
            overrideApiTimeoutMs = 3000,
        )
    }

    @Suppress("SameParameterValue")
    @Throws(Exception::class)
    private fun testApiTimeout(
        requestTimeoutMs: Int,
        defaultApiTimeoutMs: Int,
        overrideApiTimeoutMs: Int?,
    ) {
        val nodes = HashMap<Int, Node>()
        val time = MockTime()
        val node0 = Node(0, "localhost", 8121)
        nodes[0] = node0
        val cluster = Cluster(
            clusterId = "mockClusterId",
            nodes = nodes.values,
            partitions = listOf(
                PartitionInfo(
                    topic = "foo",
                    partition = 0,
                    leader = node0,
                    replicas = listOf(node0),
                    inSyncReplicas = listOf(node0),
                ),
            ),
            controller = nodes[0]
        )
        val retryBackoffMs = 100
        val effectiveTimeoutMs = overrideApiTimeoutMs ?: defaultApiTimeoutMs
        assertEquals(
            expected = 2 * requestTimeoutMs,
            actual = effectiveTimeoutMs,
            message = "This test expects the effective timeout to be twice the request timeout",
        )
        AdminClientUnitTestEnv(
            time,
            cluster,
            AdminClientConfig.RETRY_BACKOFF_MS_CONFIG,
            retryBackoffMs.toString(),
            AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG,
            requestTimeoutMs.toString(),
            AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG,
            defaultApiTimeoutMs.toString(),
        ).use { env ->
            env.mockClient.setNodeApiVersions(NodeApiVersions.create())
            val options = ListTopicsOptions()
            overrideApiTimeoutMs?.let { timeoutMs ->
                options.apply { this.timeoutMs = timeoutMs }
            }
            val result = env.adminClient.listTopics(options)

            // Wait until the first attempt has been sent, then advance the time
            waitForCondition(
                testCondition = { env.mockClient.hasInFlightRequests() },
                conditionDetails = "Timed out waiting for Metadata request to be sent",
            )
            time.sleep((requestTimeoutMs + 1).toLong())

            // Wait for the request to be timed out before backing off
            waitForCondition(
                testCondition = { !env.mockClient.hasInFlightRequests() },
                conditionDetails = "Timed out waiting for inFlightRequests to be timed out",
            )

            // Since api timeout bound is not hit, AdminClient should retry
            waitForCondition(
                testCondition = {
                    val hasInflightRequests: Boolean = env.mockClient.hasInFlightRequests()
                    if (!hasInflightRequests) time.sleep(retryBackoffMs.toLong())
                    hasInflightRequests
                },
                conditionDetails = "Timed out waiting for Metadata request to be sent",
            )
            time.sleep((requestTimeoutMs + 1).toLong())
            assertFutureThrows(
                future = result.future,
                exceptionCauseClass = TimeoutException::class.java,
            )
        }
    }

    @Test
    @Throws(Exception::class)
    fun testRequestTimeoutExceedingDefaultApiTimeout() {
        val nodes = HashMap<Int, Node>()
        val time = MockTime()
        val node0 = Node(id = 0, host = "localhost", port = 8121)
        nodes[0] = node0
        val cluster = Cluster(
            clusterId = "mockClusterId",
            nodes = nodes.values,
            partitions = listOf(
                PartitionInfo(
                    topic = "foo",
                    partition = 0,
                    leader = node0,
                    replicas = listOf(node0),
                    inSyncReplicas = listOf(node0),
                ),
            ),
            unauthorizedTopics = emptySet(),
            invalidTopics = emptySet(),
            internalTopics = emptySet(),
            controller = nodes[0],
        )

        // This test assumes the default api timeout value of 60000. When the request timeout
        // is set to something larger, we should adjust the api timeout accordingly for compatibility.
        val retryBackoffMs = 100
        val requestTimeoutMs = 120000
        AdminClientUnitTestEnv(
            time,
            cluster,
            AdminClientConfig.RETRY_BACKOFF_MS_CONFIG,
            retryBackoffMs.toString(),
            AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG,
            requestTimeoutMs.toString(),
        ).use { env ->
            env.mockClient.setNodeApiVersions(NodeApiVersions.create())
            val options = ListTopicsOptions()
            val result = env.adminClient.listTopics(options)

            // Wait until the first attempt has been sent, then advance the time by the default api timeout
            waitForCondition(
                testCondition = { env.mockClient.hasInFlightRequests() },
                conditionDetails = "Timed out waiting for Metadata request to be sent",
            )
            time.sleep(60001)

            // The in-flight request should not be cancelled
            assertTrue(env.mockClient.hasInFlightRequests())

            // Now sleep the remaining time for the request timeout to expire
            time.sleep(60000)
            assertFutureThrows(
                future = result.future,
                exceptionCauseClass = TimeoutException::class.java,
            )
        }
    }

    private fun newClientQuotaEntity(vararg args: String): ClientQuotaEntity {
        assertTrue(args.size % 2 == 0)
        val entityMap: MutableMap<String, String?> = HashMap(args.size / 2)
        var index = 0
        while (index < args.size) {
            entityMap[args[index]] = args[index + 1]
            index += 2
        }
        return ClientQuotaEntity(entityMap)
    }

    @Test
    @Throws(Exception::class)
    fun testDescribeClientQuotas() {
        mockClientEnv().use { env ->
            env.mockClient.setNodeApiVersions(NodeApiVersions.create())
            val value = "value"
            val responseData: MutableMap<ClientQuotaEntity, Map<String, Double>> =
                HashMap()
            val entity1 = newClientQuotaEntity(
                ClientQuotaEntity.USER,
                "user-1",
                ClientQuotaEntity.CLIENT_ID,
                value,
            )
            val entity2: ClientQuotaEntity = newClientQuotaEntity(
                ClientQuotaEntity.USER,
                "user-2",
                ClientQuotaEntity.CLIENT_ID,
                value,
            )
            responseData[entity1] = mapOf("consumer_byte_rate" to 10000.0)
            responseData[entity2] = mapOf("producer_byte_rate" to 20000.0)
            env.mockClient.prepareResponse(
                DescribeClientQuotasResponse.fromQuotaEntities(responseData, 0)
            )
            val filter = ClientQuotaFilter.contains(
                listOf(
                    ClientQuotaFilterComponent(entityType = ClientQuotaEntity.USER, match = value)
                )
            )
            val result = env.adminClient.describeClientQuotas(filter)
            val resultData = result.entities.get()
            assertEquals(expected = 2, actual = resultData.size)
            assertTrue(resultData.containsKey(entity1))
            val config1 = resultData[entity1]!!
            assertEquals(expected = 1, actual = config1.size)
            assertEquals(
                expected = 10000.0,
                actual = config1["consumer_byte_rate"]!!,
                absoluteTolerance = 1e-6,
            )
            assertTrue(resultData.containsKey(entity2))
            val config2 = resultData[entity2]!!
            assertEquals(expected = 1, actual = config2.size)
            assertEquals(
                expected = 20000.0,
                actual = config2["producer_byte_rate"]!!,
                absoluteTolerance = 1e-6,
            )
        }
    }

    @Test
    fun testEqualsOfClientQuotaFilterComponent() {
        assertEquals(
            ClientQuotaFilterComponent.ofDefaultEntity(ClientQuotaEntity.USER),
            ClientQuotaFilterComponent.ofDefaultEntity(ClientQuotaEntity.USER)
        )
        assertEquals(
            ClientQuotaFilterComponent.ofEntityType(ClientQuotaEntity.USER),
            ClientQuotaFilterComponent.ofEntityType(ClientQuotaEntity.USER)
        )

        // match = null is different from match = Empty
        assertNotEquals(
            illegal = ClientQuotaFilterComponent.ofDefaultEntity(ClientQuotaEntity.USER),
            actual = ClientQuotaFilterComponent.ofEntityType(ClientQuotaEntity.USER),
        )
        assertEquals(
            expected = ClientQuotaFilterComponent.ofEntity(ClientQuotaEntity.USER, "user"),
            actual = ClientQuotaFilterComponent.ofEntity(ClientQuotaEntity.USER, "user"),
        )
        assertNotEquals(
            illegal = ClientQuotaFilterComponent.ofEntity(ClientQuotaEntity.USER, "user"),
            actual = ClientQuotaFilterComponent.ofDefaultEntity(ClientQuotaEntity.USER),
        )
        assertNotEquals(
            illegal = ClientQuotaFilterComponent.ofEntity(ClientQuotaEntity.USER, "user"),
            actual = ClientQuotaFilterComponent.ofEntityType(ClientQuotaEntity.USER),
        )
    }

    @Test
    @Throws(Exception::class)
    fun testAlterClientQuotas() {
        mockClientEnv().use { env ->
            env.mockClient.setNodeApiVersions(NodeApiVersions.create())
            val goodEntity = newClientQuotaEntity(ClientQuotaEntity.USER, "user-1")
            val unauthorizedEntity = newClientQuotaEntity(ClientQuotaEntity.USER, "user-0")
            val invalidEntity = newClientQuotaEntity("", "user-0")
            val responseData = mapOf(
                goodEntity to ApiError(
                    error = Errors.CLUSTER_AUTHORIZATION_FAILED,
                    message = "Authorization failed",
                ),
                unauthorizedEntity to ApiError(
                    error = Errors.CLUSTER_AUTHORIZATION_FAILED,
                    message = "Authorization failed",
                ),
                invalidEntity to ApiError(
                    error = Errors.INVALID_REQUEST,
                    message = "Invalid quota entity",
                ),
            )
            env.mockClient.prepareResponse(
                AlterClientQuotasResponse.fromQuotaEntities(responseData, 0)
            )
            val entries = listOf(
                ClientQuotaAlteration(
                    entity = goodEntity,
                    ops = setOf(ClientQuotaAlteration.Op("consumer_byte_rate", 10000.0)),
                ),
                ClientQuotaAlteration(
                    entity = unauthorizedEntity,
                    ops = setOf(ClientQuotaAlteration.Op("producer_byte_rate", 10000.0)),
                ),
                ClientQuotaAlteration(
                    entity = invalidEntity,
                    ops = setOf(ClientQuotaAlteration.Op("producer_byte_rate", 100.0)),
                ),
            )
            val result = env.adminClient.alterClientQuotas(entries)
            result.values()[goodEntity]
            assertFutureError(
                future = result.values()[unauthorizedEntity]!!,
                exceptionClass = ClusterAuthorizationException::class.java,
            )
            assertFutureError(
                future = result.values().get(invalidEntity)!!,
                exceptionClass = InvalidRequestException::class.java,
            )
        }
    }

    @Test
    @Throws(Exception::class)
    fun testAlterReplicaLogDirsSuccess() {
        mockClientEnv().use { env ->
            createAlterLogDirsResponse(
                env,
                env.cluster.nodeById(0),
                Errors.NONE,
                0,
            )
            createAlterLogDirsResponse(
                env,
                env.cluster.nodeById(1),
                Errors.NONE,
                0,
            )
            val tpr0 = TopicPartitionReplica("topic", 0, 0)
            val tpr1 = TopicPartitionReplica("topic", 0, 1)
            val logDirs = mapOf(
                tpr0 to "/data0",
                tpr1 to "/data1",
            )
            val result = env.adminClient.alterReplicaLogDirs(logDirs)
            assertNotFails(result.futures[tpr0]!!::get)
            assertNotFails(result.futures[tpr1]!!::get)
        }
    }

    @Test
    @Throws(Exception::class)
    fun testAlterReplicaLogDirsLogDirNotFound() {
        mockClientEnv().use { env ->
            createAlterLogDirsResponse(
                env,
                env.cluster.nodeById(0),
                Errors.NONE,
                0,
            )
            createAlterLogDirsResponse(
                env,
                env.cluster.nodeById(1),
                Errors.LOG_DIR_NOT_FOUND,
                0,
            )
            val tpr0 = TopicPartitionReplica("topic", 0, 0)
            val tpr1 = TopicPartitionReplica("topic", 0, 1)
            val logDirs = mapOf(
                tpr0 to "/data0",
                tpr1 to "/data1",
            )
            val result = env.adminClient.alterReplicaLogDirs(logDirs)
            assertNotFails(result.futures[tpr0]!!::get)
            assertFutureError(
                future = result.futures[tpr1]!!,
                exceptionClass = LogDirNotFoundException::class.java,
            )
        }
    }

    @Test
    @Throws(Exception::class)
    fun testAlterReplicaLogDirsUnrequested() {
        mockClientEnv().use { env ->
            createAlterLogDirsResponse(
                env,
                env.cluster.nodeById(0),
                Errors.NONE,
                1,
                2,
            )
            val tpr1 = TopicPartitionReplica(topic = "topic", partition = 1, brokerId = 0)
            val logDirs = mapOf(tpr1 to "/data1")
            val result = env.adminClient.alterReplicaLogDirs(logDirs)
            assertNotFails(result.futures[tpr1]!!::get)
        }
    }

    @Test
    @Throws(Exception::class)
    fun testAlterReplicaLogDirsPartialResponse() {
        mockClientEnv().use { env ->
            createAlterLogDirsResponse(
                env,
                env.cluster.nodeById(0),
                Errors.NONE,
                1,
            )
            val tpr1 = TopicPartitionReplica("topic", 1, 0)
            val tpr2 = TopicPartitionReplica("topic", 2, 0)
            val logDirs = mapOf(
                tpr1 to "/data1",
                tpr2 to "/data1",
            )
            val result = env.adminClient.alterReplicaLogDirs(logDirs)
            assertNotFails(result.futures[tpr1]!!::get)
            assertFutureThrows(
                future = result.futures[tpr2]!!,
                exceptionCauseClass = ApiException::class.java,
            )
        }
    }

    @Test
    @Throws(Exception::class)
    fun testAlterReplicaLogDirsPartialFailure() {
        val defaultApiTimeout = 60000
        val time = MockTime()
        mockClientEnv(time, AdminClientConfig.RETRIES_CONFIG, "0").use { env ->

            // Provide only one prepared response from node 1
            env.mockClient.prepareResponseFrom(
                response = prepareAlterLogDirsResponse(Errors.NONE, "topic", 2),
                node = env.cluster.nodeById(1),
            )
            val tpr1 = TopicPartitionReplica(topic = "topic", partition = 1, brokerId = 0)
            val tpr2 = TopicPartitionReplica(topic = "topic", partition = 2, brokerId = 1)
            val logDirs = mapOf(
                tpr1 to "/data1",
                tpr2 to "/data1",
            )
            val result = env.adminClient.alterReplicaLogDirs(logDirs)

            // Wait until the prepared attempt has been consumed
            waitForCondition(
                testCondition = { env.mockClient.numAwaitingResponses() == 0 },
                conditionDetails = "Failed awaiting requests",
            )

            // Wait until the request is sent out
            waitForCondition(
                testCondition = { env.mockClient.inFlightRequestCount() == 1 },
                conditionDetails = "Failed awaiting request",
            )

            // Advance time past the default api timeout to time out the inflight request
            time.sleep(defaultApiTimeout.toLong() + 1)
            assertFutureThrows(
                future = result.futures[tpr1]!!,
                exceptionCauseClass = ApiException::class.java,
            )
            assertNotFails(result.futures[tpr2]!!::get)
        }
    }

    @Test
    @Throws(Exception::class)
    fun testDescribeUserScramCredentials() {
        mockClientEnv().use { env ->
            env.mockClient.setNodeApiVersions(NodeApiVersions.create())
            val user0Name = "user0"
            val user0ScramMechanism0 = ScramMechanism.SCRAM_SHA_256
            val user0Iterations0 = 4096
            val user0ScramMechanism1 = ScramMechanism.SCRAM_SHA_512
            val user0Iterations1 = 8192
            val user0CredentialInfo0 = DescribeUserScramCredentialsResponseData.CredentialInfo()
            user0CredentialInfo0.setMechanism(user0ScramMechanism0.type)
            user0CredentialInfo0.setIterations(user0Iterations0)
            val user0CredentialInfo1 = DescribeUserScramCredentialsResponseData.CredentialInfo()
            user0CredentialInfo1.setMechanism(user0ScramMechanism1.type)
            user0CredentialInfo1.setIterations(user0Iterations1)
            val user1Name = "user1"
            val user1ScramMechanism = ScramMechanism.SCRAM_SHA_256
            val user1Iterations = 4096
            val user1CredentialInfo = DescribeUserScramCredentialsResponseData.CredentialInfo()
            user1CredentialInfo.setMechanism(user1ScramMechanism.type)
            user1CredentialInfo.setIterations(user1Iterations)
            val responseData = DescribeUserScramCredentialsResponseData()
            responseData.setResults(
                listOf(
                    DescribeUserScramCredentialsResponseData.DescribeUserScramCredentialsResult()
                        .setUser(user0Name)
                        .setCredentialInfos(listOf(user0CredentialInfo0, user0CredentialInfo1)),
                    DescribeUserScramCredentialsResponseData.DescribeUserScramCredentialsResult()
                        .setUser(user1Name)
                        .setCredentialInfos(listOf(user1CredentialInfo))
                )
            )
            val response = DescribeUserScramCredentialsResponse(responseData)
            val usersRequestedSet = setOf(user0Name, user1Name)
            for (users in listOf(
                null,
                emptyList(),
                listOf(user0Name, null, user1Name),
            )) {
                env.mockClient.prepareResponse(response)
                val result = env.adminClient.describeUserScramCredentials(users)
                val descriptionResults = result.all().get()
                val user0DescriptionFuture = result.description(user0Name)
                val user1DescriptionFuture = result.description(user1Name)
                val usersDescribedFromUsersSet = HashSet(result.users().get())
                assertEquals(
                    expected = usersRequestedSet,
                    actual = usersDescribedFromUsersSet,
                )
                val usersDescribedFromMapKeySet = descriptionResults.keys
                assertEquals(
                    expected = usersRequestedSet,
                    actual = usersDescribedFromMapKeySet,
                )
                val userScramCredentialsDescription0 = descriptionResults[user0Name]
                assertEquals(
                    expected = user0Name,
                    actual = userScramCredentialsDescription0!!.name,
                )
                assertEquals(
                    expected = 2,
                    actual = userScramCredentialsDescription0.credentialInfos.size,
                )
                assertEquals(
                    expected = user0ScramMechanism0,
                    actual = userScramCredentialsDescription0.credentialInfos[0].mechanism,
                )
                assertEquals(
                    expected = user0Iterations0,
                    actual = userScramCredentialsDescription0.credentialInfos[0].iterations,
                )
                assertEquals(
                    expected = user0ScramMechanism1,
                    actual = userScramCredentialsDescription0.credentialInfos[1].mechanism,
                )
                assertEquals(
                    expected = user0Iterations1,
                    actual = userScramCredentialsDescription0.credentialInfos[1].iterations,
                )
                assertEquals(
                    expected = userScramCredentialsDescription0,
                    actual = user0DescriptionFuture.get(),
                )
                val userScramCredentialsDescription1 = descriptionResults[user1Name]
                assertEquals(
                    expected = user1Name,
                    actual = userScramCredentialsDescription1!!.name,
                )
                assertEquals(
                    expected = 1,
                    actual = userScramCredentialsDescription1.credentialInfos.size,
                )
                assertEquals(
                    expected = user1ScramMechanism,
                    actual = userScramCredentialsDescription1.credentialInfos[0].mechanism,
                )
                assertEquals(
                    expected = user1Iterations,
                    actual = userScramCredentialsDescription1.credentialInfos[0].iterations,
                )
                assertEquals(
                    expected = userScramCredentialsDescription1,
                    actual = user1DescriptionFuture.get(),
                )
            }
        }
    }

    @Test
    fun testAlterUserScramCredentialsUnknownMechanism() {
        mockClientEnv().use { env ->
            env.mockClient.setNodeApiVersions(NodeApiVersions.create())
            val user0Name = "user0"
            val user0ScramMechanism0 = ScramMechanism.UNKNOWN
            val user1Name = "user1"
            val user1ScramMechanism0 = ScramMechanism.UNKNOWN
            val user2Name = "user2"
            val user2ScramMechanism0 = ScramMechanism.SCRAM_SHA_256
            val responseData = AlterUserScramCredentialsResponseData()
            responseData.setResults(
                listOf(
                    AlterUserScramCredentialsResponseData.AlterUserScramCredentialsResult()
                        .setUser(user2Name)
                )
            )
            env.mockClient.prepareResponse(AlterUserScramCredentialsResponse(responseData))
            val result = env.adminClient.alterUserScramCredentials(
                listOf(
                    UserScramCredentialDeletion(user0Name, user0ScramMechanism0),
                    UserScramCredentialUpsertion(
                        user = user1Name,
                        credentialInfo = ScramCredentialInfo(user1ScramMechanism0, 8192),
                        password = "password",
                    ),
                    UserScramCredentialUpsertion(
                        user = user2Name,
                        credentialInfo = ScramCredentialInfo(user2ScramMechanism0, 4096),
                        password = "password",
                    )
                )
            )
            val resultData = result.futures
            assertEquals(expected = 3, actual = resultData.size)
            listOf(user0Name, user1Name).forEach { u ->
                assertTrue(resultData.containsKey(u))
                try {
                    resultData.get(u)!!.get()
                    fail("Expected request for user $u to complete exceptionally, but it did not")
                } catch (expected: Exception) {
                    // ignore
                }
            }
            assertTrue(resultData.containsKey(user2Name))
            try {
                resultData[user2Name]!!.get()
            } catch (e: Exception) {
                fail("Expected request for user $user2Name to NOT complete excdptionally, but it did")
            }
            try {
                result.all().get()
                fail("Expected 'result.all().get()' to throw an exception since at least one user failed, but it did not")
            } catch (expected: Exception) {
                // ignore, expected
            }
        }
    }

    @Test
    @Throws(Exception::class)
    fun testAlterUserScramCredentials() {
        mockClientEnv().use { env ->
            env.mockClient.setNodeApiVersions(NodeApiVersions.create())
            val user0Name = "user0"
            val user0ScramMechanism0 = ScramMechanism.SCRAM_SHA_256
            val user0ScramMechanism1 = ScramMechanism.SCRAM_SHA_512
            val user1Name = "user1"
            val user1ScramMechanism0 = ScramMechanism.SCRAM_SHA_256
            val user2Name = "user2"
            val user2ScramMechanism0 = ScramMechanism.SCRAM_SHA_512
            val responseData = AlterUserScramCredentialsResponseData()
            responseData.setResults(
                listOf(user0Name, user1Name, user2Name).map { u ->
                    AlterUserScramCredentialsResponseData.AlterUserScramCredentialsResult()
                        .setUser(u)
                        .setErrorCode(Errors.NONE.code)
                }
            )
            env.mockClient.prepareResponse(AlterUserScramCredentialsResponse(responseData))
            val result = env.adminClient.alterUserScramCredentials(
                listOf(
                    UserScramCredentialDeletion(user0Name, user0ScramMechanism0),
                    UserScramCredentialUpsertion(
                        user = user0Name,
                        credentialInfo = ScramCredentialInfo(user0ScramMechanism1, 8192),
                        password = "password"
                    ),
                    UserScramCredentialUpsertion(
                        user = user1Name,
                        credentialInfo = ScramCredentialInfo(user1ScramMechanism0, 8192),
                        password = "password"
                    ),
                    UserScramCredentialDeletion(user2Name, user2ScramMechanism0),
                )
            )
            val resultData = result.futures
            assertEquals(expected = 3, actual = resultData.size)
            listOf(user0Name, user1Name, user2Name).forEach { u ->
                assertTrue(resultData.containsKey(u))
                assertFalse(resultData[u]!!.isCompletedExceptionally)
            }
        }
    }

    private fun createAlterLogDirsResponse(
        env: AdminClientUnitTestEnv,
        node: Node?,
        error: Errors,
        vararg partitions: Int,
    ) {
        env.mockClient.prepareResponseFrom(
            response = prepareAlterLogDirsResponse(error, "topic", *partitions),
            node = node,
        )
    }

    @Suppress("SameParameterValue")
    private fun prepareAlterLogDirsResponse(
        error: Errors,
        topic: String,
        vararg partitions: Int,
    ): AlterReplicaLogDirsResponse {
        return AlterReplicaLogDirsResponse(
            AlterReplicaLogDirsResponseData().setResults(
                listOf(
                    AlterReplicaLogDirTopicResult()
                        .setTopicName(topic)
                        .setPartitions(
                            partitions.map { partitionId ->
                                AlterReplicaLogDirPartitionResult()
                                    .setPartitionIndex(partitionId)
                                    .setErrorCode(error.code)
                            }
                        )
                )
            )
        )
    }

    @Test
    @Throws(Exception::class)
    fun testDescribeLogDirsPartialFailure() {
        val defaultApiTimeout = 60000
        val time = MockTime()
        mockClientEnv(time, AdminClientConfig.RETRIES_CONFIG, "0").use { env ->
            env.mockClient.prepareResponseFrom(
                response = prepareDescribeLogDirsResponse(Errors.NONE, "/data"),
                node = env.cluster.nodeById(1),
            )
            val result = env.adminClient.describeLogDirs(listOf(0, 1))

            // Wait until the prepared attempt has been consumed
            waitForCondition(
                testCondition = { env.mockClient.numAwaitingResponses() == 0 },
                conditionDetails = "Failed awaiting requests",
            )

            // Wait until the request is sent out
            waitForCondition(
                testCondition = { env.mockClient.inFlightRequestCount() == 1 },
                conditionDetails = "Failed awaiting request",
            )

            // Advance time past the default api timeout to time out the inflight request
            time.sleep(defaultApiTimeout.toLong() + 1)
            assertFutureThrows(
                future = result.descriptions()[0]!!,
                exceptionCauseClass = ApiException::class.java,
            )
            assertNotNull(result.descriptions()[1]!!.get())
        }
    }

    @Test
    @Throws(InterruptedException::class, ExecutionException::class)
    fun testUnregisterBrokerSuccess() {
        val nodeId = 1
        mockClientEnv().use { env ->
            env.mockClient.setNodeApiVersions(
                NodeApiVersions.create(
                    apiKey = ApiKeys.UNREGISTER_BROKER.id,
                    minVersion = 0,
                    maxVersion = 0,
                )
            )
            env.mockClient.prepareResponse(
                prepareUnregisterBrokerResponse(Errors.NONE, 0)
            )
            val result = env.adminClient.unregisterBroker(nodeId)
            // Validate response
            assertNotNull(result.all())
            result.all().get()
        }
    }

    @Test
    fun testUnregisterBrokerFailure() {
        val nodeId = 1
        mockClientEnv().use { env ->
            env.mockClient.setNodeApiVersions(
                NodeApiVersions.create(
                    apiKey = ApiKeys.UNREGISTER_BROKER.id,
                    minVersion = 0,
                    maxVersion = 0,
                )
            )
            env.mockClient.prepareResponse(
                prepareUnregisterBrokerResponse(Errors.UNKNOWN_SERVER_ERROR, 0)
            )
            val result: UnregisterBrokerResult = env.adminClient.unregisterBroker(nodeId)
            // Validate response
            assertNotNull(result.all())
            assertFutureThrows(
                future = result.all(),
                exceptionCauseClass = Errors.UNKNOWN_SERVER_ERROR.exception!!.javaClass,
            )
        }
    }

    @Test
    @Throws(ExecutionException::class, InterruptedException::class)
    fun testUnregisterBrokerTimeoutAndSuccessRetry() {
        val nodeId = 1
        mockClientEnv().use { env ->
            env.mockClient.setNodeApiVersions(
                NodeApiVersions.create(
                    apiKey = ApiKeys.UNREGISTER_BROKER.id,
                    minVersion = 0,
                    maxVersion = 0,
                )
            )
            env.mockClient.prepareResponse(
                prepareUnregisterBrokerResponse(Errors.REQUEST_TIMED_OUT, 0)
            )
            env.mockClient.prepareResponse(
                prepareUnregisterBrokerResponse(Errors.NONE, 0),
            )
            val result = env.adminClient.unregisterBroker(nodeId)

            // Validate response
            assertNotNull(result.all())
            result.all().get()
        }
    }

    @Test
    fun testUnregisterBrokerTimeoutAndFailureRetry() {
        val nodeId = 1
        mockClientEnv().use { env ->
            env.mockClient.setNodeApiVersions(
                NodeApiVersions.create(
                    apiKey = ApiKeys.UNREGISTER_BROKER.id,
                    minVersion = 0,
                    maxVersion = 0,
                )
            )
            env.mockClient.prepareResponse(
                prepareUnregisterBrokerResponse(Errors.REQUEST_TIMED_OUT, 0)
            )
            env.mockClient.prepareResponse(
                prepareUnregisterBrokerResponse(Errors.UNKNOWN_SERVER_ERROR, 0)
            )
            val result = env.adminClient.unregisterBroker(nodeId)

            // Validate response
            assertNotNull(result.all())
            assertFutureThrows(
                future = result.all(),
                exceptionCauseClass = Errors.UNKNOWN_SERVER_ERROR.exception!!.javaClass,
            )
        }
    }

    @Test
    fun testUnregisterBrokerTimeoutMaxRetry() {
        val nodeId = 1
        mockClientEnv(Time.SYSTEM, AdminClientConfig.RETRIES_CONFIG, "1").use { env ->
            env.mockClient.setNodeApiVersions(
                NodeApiVersions.create(
                    apiKey = ApiKeys.UNREGISTER_BROKER.id,
                    minVersion = 0,
                    maxVersion = 0,
                )
            )
            env.mockClient.prepareResponse(
                prepareUnregisterBrokerResponse(Errors.REQUEST_TIMED_OUT, 0)
            )
            env.mockClient.prepareResponse(
                prepareUnregisterBrokerResponse(Errors.REQUEST_TIMED_OUT, 0)
            )
            val result = env.adminClient.unregisterBroker(nodeId)

            // Validate response
            assertNotNull(result.all())
            assertFutureThrows(
                future = result.all(),
                exceptionCauseClass = Errors.REQUEST_TIMED_OUT.exception!!.javaClass,
            )
        }
    }

    @Test
    fun testUnregisterBrokerTimeoutMaxWait() {
        val nodeId = 1
        mockClientEnv().use { env ->
            env.mockClient.setNodeApiVersions(
                NodeApiVersions.create(
                    apiKey = ApiKeys.UNREGISTER_BROKER.id,
                    minVersion = 0,
                    maxVersion = 0,
                )
            )
            val options = UnregisterBrokerOptions().apply {
                timeoutMs = 10
            }
            val result = env.adminClient.unregisterBroker(nodeId, options)

            // Validate response
            assertNotNull(result.all())
            assertFutureThrows(
                future = result.all(),
                exceptionCauseClass = Errors.REQUEST_TIMED_OUT.exception!!.javaClass,
            )
        }
    }

    @Test
    @Throws(Exception::class)
    fun testDescribeProducers() {
        mockClientEnv().use { env ->
            val topicPartition = TopicPartition("foo", 0)
            val leader = env.cluster.nodes.first()
            expectMetadataRequest(env, topicPartition, leader)
            val expected = listOf(
                ProducerState(
                    producerId = 12345L,
                    producerEpoch = 15,
                    lastSequence = 30,
                    lastTimestamp = env.time.milliseconds(),
                    coordinatorEpoch = 99,
                    currentTransactionStartOffset = null,
                ),
                ProducerState(
                    producerId = 12345L,
                    producerEpoch = 15,
                    lastSequence = 30,
                    lastTimestamp = env.time.milliseconds(),
                    coordinatorEpoch = null,
                    currentTransactionStartOffset = 23423L,
                )
            )
            val response = buildDescribeProducersResponse(topicPartition, expected)
            env.mockClient.prepareResponseFrom(
                matcher = { request -> request is DescribeProducersRequest },
                response = response,
                node = leader,
            )
            val result = env.adminClient.describeProducers(setOf(topicPartition))
            val partitionFuture = result.partitionResult(topicPartition)
            assertEquals(
                expected = expected.toSet(),
                actual = partitionFuture.get().activeProducers().toSet(),
            )
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = [true, false])
    @Throws(Exception::class)
    fun testDescribeProducersTimeout(timeoutInMetadataLookup: Boolean) {
        val time = MockTime()
        mockClientEnv(time).use { env ->
            val topicPartition = TopicPartition("foo", 0)
            val requestTimeoutMs = 15000
            if (!timeoutInMetadataLookup) {
                val leader = env.cluster.nodes.first()
                expectMetadataRequest(env, topicPartition, leader)
            }
            val options = DescribeProducersOptions().apply { timeoutMs = requestTimeoutMs }
            val result = env.adminClient.describeProducers(
                partitions = setOf(topicPartition),
                options = options,
            )
            assertFalse(result.all().isDone())
            time.sleep(requestTimeoutMs.toLong())
            waitForCondition(
                testCondition = { result.all().isDone() },
                conditionDetails = "Future failed to timeout after expiration of timeout",
            )
            assertTrue(result.all().isCompletedExceptionally)
            assertFutureThrows(
                future = result.all(),
                exceptionCauseClass = TimeoutException::class.java,
            )
            assertFalse(env.mockClient.hasInFlightRequests())
        }
    }

    @Test
    @Throws(Exception::class)
    fun testDescribeProducersRetryAfterDisconnect() {
        val time = MockTime()
        val retryBackoffMs = 100
        val cluster = mockCluster(numNodes = 3, controllerIndex = 0)
        val configOverride =
            newStrMap(AdminClientConfig.RETRY_BACKOFF_MS_CONFIG, "" + retryBackoffMs)
        AdminClientUnitTestEnv(time, cluster, configOverride).use { env ->
            val topicPartition = TopicPartition("foo", 0)
            val nodeIterator = env.cluster.nodes.iterator()
            val initialLeader = nodeIterator.next()
            expectMetadataRequest(env, topicPartition, initialLeader)
            val expected = listOf(
                ProducerState(
                    producerId = 12345L,
                    producerEpoch = 15,
                    lastSequence = 30,
                    lastTimestamp = env.time.milliseconds(),
                    coordinatorEpoch = 99,
                    currentTransactionStartOffset = null,
                ),
                ProducerState(
                    producerId = 12345L,
                    producerEpoch = 15,
                    lastSequence = 30,
                    lastTimestamp = env.time.milliseconds(),
                    coordinatorEpoch = null,
                    currentTransactionStartOffset = 23423L,
                )
            )
            val response = buildDescribeProducersResponse(topicPartition, expected)
            env.mockClient.prepareResponseFrom(
                matcher = { request ->
                    // We need a sleep here because the client will attempt to
                    // backoff after the disconnect
                    env.time.sleep(retryBackoffMs.toLong())
                    request is DescribeProducersRequest
                },
                response = response,
                node = initialLeader,
                disconnected = true,
            )
            val retryLeader = nodeIterator.next()
            expectMetadataRequest(env, topicPartition, retryLeader)
            env.mockClient.prepareResponseFrom(
                matcher = { request -> request is DescribeProducersRequest },
                response = response,
                node = retryLeader,
            )
            val result = env.adminClient.describeProducers(setOf(topicPartition))
            val partitionFuture = result.partitionResult(topicPartition)
            assertEquals(
                expected = expected.toSet(),
                actual = partitionFuture.get().activeProducers().toSet(),
            )
        }
    }

    @Test
    @Throws(Exception::class)
    fun testDescribeTransactions() {
        mockClientEnv().use { env ->
            val transactionalId = "foo"
            val coordinator = env.cluster.nodes.first()
            val expected = TransactionDescription(
                coordinatorId = coordinator.id,
                state = TransactionState.COMPLETE_COMMIT,
                producerId = 12345L,
                producerEpoch = 15,
                transactionTimeoutMs = 10000L,
                transactionStartTimeMs = null,
                topicPartitions = emptySet(),
            )
            env.mockClient.prepareResponse(
                matcher = { request -> request is FindCoordinatorRequest },
                response = prepareFindCoordinatorResponse(
                    error = Errors.NONE,
                    key = transactionalId,
                    node = coordinator,
                )
            )
            env.mockClient.prepareResponseFrom(
                matcher = { request -> request is DescribeTransactionsRequest },
                response = DescribeTransactionsResponse(
                    DescribeTransactionsResponseData().setTransactionStates(
                        listOf(
                            DescribeTransactionsResponseData.TransactionState()
                                .setErrorCode(Errors.NONE.code)
                                .setProducerEpoch(expected.producerEpoch.toShort())
                                .setProducerId(expected.producerId)
                                .setTransactionalId(transactionalId)
                                .setTransactionTimeoutMs(10000)
                                .setTransactionStartTimeMs(-1)
                                .setTransactionState(expected.state.toString())
                        )
                    )
                ),
                node = coordinator,
            )
            val result = env.adminClient.describeTransactions(setOf(transactionalId))
            val future = result.description(transactionalId)
            assertEquals(expected, future.get())
        }
    }

    @Test
    @Throws(Exception::class)
    fun testRetryDescribeTransactionsAfterNotCoordinatorError() {
        val time = MockTime()
        val retryBackoffMs = 100
        val cluster = mockCluster(3, 0)
        val configOverride =
            newStrMap(AdminClientConfig.RETRY_BACKOFF_MS_CONFIG, "" + retryBackoffMs)
        AdminClientUnitTestEnv(time, cluster, configOverride).use { env ->
            val transactionalId = "foo"
            val nodeIterator = env.cluster.nodes.iterator()
            val coordinator1 = nodeIterator.next()
            val coordinator2 = nodeIterator.next()
            env.mockClient.prepareResponse(
                matcher = { request -> request is FindCoordinatorRequest },
                response = FindCoordinatorResponse(
                    FindCoordinatorResponseData().setCoordinators(
                        listOf(
                            FindCoordinatorResponseData.Coordinator()
                                .setKey(transactionalId)
                                .setErrorCode(Errors.NONE.code)
                                .setNodeId(coordinator1.id)
                                .setHost(coordinator1.host)
                                .setPort(coordinator1.port)
                        )
                    )
                ),
            )
            env.mockClient.prepareResponseFrom(
                matcher = { request ->
                    if (request !is DescribeTransactionsRequest) {
                        return@prepareResponseFrom false
                    } else {
                        // Backoff needed here for the retry of FindCoordinator
                        time.sleep(retryBackoffMs.toLong())
                        return@prepareResponseFrom true
                    }
                },
                response = DescribeTransactionsResponse(
                    DescribeTransactionsResponseData().setTransactionStates(
                        listOf(
                            DescribeTransactionsResponseData.TransactionState()
                                .setErrorCode(Errors.NOT_COORDINATOR.code)
                                .setTransactionalId(transactionalId)
                        )
                    )
                ),
                node = coordinator1,
            )
            env.mockClient.prepareResponse(
                matcher = { request -> request is FindCoordinatorRequest },
                response = FindCoordinatorResponse(
                    FindCoordinatorResponseData().setCoordinators(
                        listOf(
                            FindCoordinatorResponseData.Coordinator()
                                .setKey(transactionalId)
                                .setErrorCode(Errors.NONE.code)
                                .setNodeId(coordinator2.id)
                                .setHost(coordinator2.host)
                                .setPort(coordinator2.port)
                        )
                    )
                ),
            )
            val expected = TransactionDescription(
                coordinatorId = coordinator2.id,
                state = TransactionState.COMPLETE_COMMIT,
                producerId = 12345L,
                producerEpoch = 15,
                transactionTimeoutMs = 10000L,
                transactionStartTimeMs = null,
                topicPartitions = emptySet(),
            )
            env.mockClient.prepareResponseFrom(
                matcher = { request -> request is DescribeTransactionsRequest },
                response = DescribeTransactionsResponse(
                    DescribeTransactionsResponseData().setTransactionStates(
                        listOf(
                            DescribeTransactionsResponseData.TransactionState()
                                .setErrorCode(Errors.NONE.code)
                                .setProducerEpoch(expected.producerEpoch.toShort())
                                .setProducerId(expected.producerId)
                                .setTransactionalId(transactionalId)
                                .setTransactionTimeoutMs(10000)
                                .setTransactionStartTimeMs(-1)
                                .setTransactionState(expected.state.toString())
                        )
                    )
                ),
                node = coordinator2,
            )
            val result = env.adminClient.describeTransactions(setOf(transactionalId))
            val future = result.description(transactionalId)
            assertEquals(expected = expected, actual = future.get())
        }
    }

    @Test
    @Throws(Exception::class)
    fun testAbortTransaction() {
        mockClientEnv().use { env ->
            val topicPartition = TopicPartition(topic = "foo", partition = 13)
            val abortSpec = AbortTransactionSpec(
                producerId = 12345L,
                producerEpoch = 15,
                coordinatorEpoch = 200,
                topicPartition = topicPartition,
            )
            val leader = env.cluster.nodes.first()
            expectMetadataRequest(env, topicPartition, leader)
            env.mockClient.prepareResponseFrom(
                matcher = { request -> request is WriteTxnMarkersRequest },
                response = writeTxnMarkersResponse(abortSpec, Errors.NONE),
                node = leader,
            )
            val result = env.adminClient.abortTransaction(abortSpec)
            assertNotFails(result.all()::get)
        }
    }

    @Test
    @Throws(Exception::class)
    fun testAbortTransactionFindLeaderAfterDisconnect() {
        val time = MockTime()
        val retryBackoffMs = 100
        val cluster = mockCluster(3, 0)
        val configOverride =
            newStrMap(AdminClientConfig.RETRY_BACKOFF_MS_CONFIG, "" + retryBackoffMs)
        AdminClientUnitTestEnv(time, cluster, configOverride).use { env ->
            val topicPartition = TopicPartition("foo", 13)
            val abortSpec = AbortTransactionSpec(
                producerId = 12345L,
                producerEpoch = 15.toShort(),
                coordinatorEpoch = 200,
                topicPartition = topicPartition,
            )
            val nodeIterator = env.cluster.nodes.iterator()
            val firstLeader = nodeIterator.next()
            expectMetadataRequest(env, topicPartition, firstLeader)
            val response = writeTxnMarkersResponse(abortSpec, Errors.NONE)
            env.mockClient.prepareResponseFrom(
                matcher = { request ->
                    // We need a sleep here because the client will attempt to
                    // backoff after the disconnect
                    time.sleep(retryBackoffMs.toLong())
                    request is WriteTxnMarkersRequest
                },
                response = response,
                node = firstLeader,
                disconnected = true,
            )
            val retryLeader = nodeIterator.next()
            expectMetadataRequest(env, topicPartition, retryLeader)
            env.mockClient.prepareResponseFrom(
                matcher = { request -> request is WriteTxnMarkersRequest },
                response = response,
                node = retryLeader,
            )
            val result = env.adminClient.abortTransaction(abortSpec)
            assertNotFails(result.all()::get)
        }
    }

    @Test
    @Throws(Exception::class)
    fun testListTransactions() {
        mockClientEnv().use { env ->
            val brokers = MetadataResponseBrokerCollection()
            env.cluster
                .nodes
                .forEach { node ->
                    brokers.add(
                        MetadataResponseBroker()
                            .setHost(node.host)
                            .setNodeId(node.id)
                            .setPort(node.port)
                            .setRack(node.rack)
                    )
                }
            env.mockClient.prepareResponse(
                matcher = { request -> request is MetadataRequest },
                response = MetadataResponse(
                    MetadataResponseData().setBrokers(brokers),
                    MetadataResponseData.HIGHEST_SUPPORTED_VERSION,
                )
            )
            val expected = listOf(
                TransactionListing(
                    transactionalId = "foo",
                    producerId = 12345L,
                    state = TransactionState.ONGOING,
                ),
                TransactionListing(
                    transactionalId = "bar",
                    producerId = 98765L,
                    state = TransactionState.PREPARE_ABORT,
                ),
                TransactionListing(
                    transactionalId = "baz",
                    producerId = 13579L,
                    state = TransactionState.COMPLETE_COMMIT,
                )
            )
            assertEquals(
                expected = setOf(0, 1, 2),
                actual = env.cluster.nodes.map { obj -> obj.id }.toSet(),
            )
            env.cluster
                .nodes
                .forEach { node ->
                    val response = ListTransactionsResponseData().setErrorCode(Errors.NONE.code)
                    val listing = expected[node.id]
                    response.transactionStates += ListTransactionsResponseData.TransactionState()
                        .setTransactionalId(listing.transactionalId)
                        .setProducerId(listing.producerId)
                        .setTransactionState(listing.state.toString())
                    env.mockClient.prepareResponseFrom(
                        matcher = { request -> request is ListTransactionsRequest },
                        response = ListTransactionsResponse(response),
                        node = node,
                    )
                }
            val result = env.adminClient.listTransactions()
            assertEquals(
                expected = expected.toSet(),
                actual = result.all().get().toSet(),
            )
        }
    }

    @Suppress("SameParameterValue")
    private fun writeTxnMarkersResponse(
        abortSpec: AbortTransactionSpec,
        error: Errors,
    ): WriteTxnMarkersResponse {
        val partitionResponse = WritableTxnMarkerPartitionResult()
            .setPartitionIndex(abortSpec.topicPartition.partition)
            .setErrorCode(error.code)
        val topicResponse = WritableTxnMarkerTopicResult().setName(abortSpec.topicPartition.topic)
        topicResponse.partitions += partitionResponse
        val markerResponse = WritableTxnMarkerResult().setProducerId(abortSpec.producerId)
        markerResponse.topics += topicResponse
        val response = WriteTxnMarkersResponseData()
        response.markers += markerResponse
        return WriteTxnMarkersResponse(response)
    }

    private fun buildDescribeProducersResponse(
        topicPartition: TopicPartition,
        producerStates: List<ProducerState>,
    ): DescribeProducersResponse {
        val response = DescribeProducersResponseData()
        val topicResponse = TopicResponse()
            .setName(topicPartition.topic)
        response.topics += topicResponse
        val partitionResponse = DescribeProducersResponseData.PartitionResponse()
            .setPartitionIndex(topicPartition.partition)
            .setErrorCode(Errors.NONE.code)
        topicResponse.partitions += partitionResponse
        partitionResponse.setActiveProducers(
            producerStates.map { producerState ->
                DescribeProducersResponseData.ProducerState()
                    .setProducerId(producerState.producerId)
                    .setProducerEpoch(producerState.producerEpoch)
                    .setCoordinatorEpoch(producerState.coordinatorEpoch ?: -1)
                    .setLastSequence(producerState.lastSequence)
                    .setLastTimestamp(producerState.lastTimestamp)
                    .setCurrentTxnStartOffset(
                        producerState.currentTransactionStartOffset ?: -1L
                    )
            }
        )
        return DescribeProducersResponse(response)
    }

    private fun expectMetadataRequest(
        env: AdminClientUnitTestEnv,
        topicPartition: TopicPartition,
        leader: Node,
    ) {
        val responseTopics = MetadataResponseTopicCollection()
        val responseTopic = MetadataResponseTopic()
            .setName(topicPartition.topic)
            .setErrorCode(Errors.NONE.code)
        responseTopics.add(responseTopic)
        val responsePartition = MetadataResponsePartition()
            .setErrorCode(Errors.NONE.code)
            .setPartitionIndex(topicPartition.partition)
            .setLeaderId(leader.id)
            .setReplicaNodes(intArrayOf(leader.id))
            .setIsrNodes(intArrayOf(leader.id))
        responseTopic.partitions += responsePartition
        env.mockClient.prepareResponse(
            matcher = { request ->
                if (request !is MetadataRequest) return@prepareResponse false
                request.topics() == listOf(topicPartition.topic)
            },
            response = MetadataResponse(
                data = MetadataResponseData().setTopics(responseTopics),
                version = MetadataResponseData.HIGHEST_SUPPORTED_VERSION,
            )
        )
    }

    /**
     * Test that if the client can obtain a node assignment, but can't send to the given
     * node, it will disconnect and try a different node.
     */
    @Test
    @Throws(Exception::class)
    fun testClientSideTimeoutAfterFailureToSend() {
        val cluster = mockCluster(numNodes = 3, controllerIndex = 0)
        val disconnectFuture = CompletableFuture<String>()
        val time = MockTime()
        AdminClientUnitTestEnv(
            time,
            cluster,
            newStrMap(
                AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "1",
                AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "100000",
                AdminClientConfig.RETRY_BACKOFF_MS_CONFIG, "1",
            )
        ).use { env ->
            env.mockClient.setNodeApiVersions(NodeApiVersions.create())
            for (node: Node? in cluster.nodes) {
                env.mockClient.delayReady((node)!!, 100)
            }

            // We use a countdown latch to ensure that we get to the first
            // call to `ready` before we increment the time below to trigger
            // the disconnect.
            val readyLatch = CountDownLatch(2)
            env.mockClient.setDisconnectFuture(disconnectFuture)
            env.mockClient.setReadyCallback { readyLatch.countDown() }
            env.mockClient.prepareResponse(prepareMetadataResponse(cluster, Errors.NONE))
            val result: ListTopicsResult = env.adminClient.listTopics()
            readyLatch.await(TestUtils.DEFAULT_MAX_WAIT_MS, TimeUnit.MILLISECONDS)
            log.debug("Advancing clock by 25 ms to trigger client-side disconnect.")
            time.sleep(25)
            disconnectFuture.get()
            log.debug("Enabling nodes to send requests again.")
            for (node in cluster.nodes) {
                env.mockClient.delayReady(node, 0)
            }
            time.sleep(5)
            log.info("Waiting for result.")
            assertEquals(0, result.listings.get().size)
        }
    }

    /**
     * Test that if the client can send to a node, but doesn't receive a response, it will
     * disconnect and try a different node.
     */
    @Test
    @Throws(Exception::class)
    fun testClientSideTimeoutAfterFailureToReceiveResponse() {
        val cluster = mockCluster(3, 0)
        val disconnectFuture = CompletableFuture<String>()
        val time = MockTime()
        AdminClientUnitTestEnv(
            time,
            cluster,
            newStrMap(
                AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "1",
                AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "100000",
                AdminClientConfig.RETRY_BACKOFF_MS_CONFIG, "0",
            )
        ).use { env ->
            env.mockClient.setNodeApiVersions(NodeApiVersions.create())
            env.mockClient.setDisconnectFuture(disconnectFuture)
            val result = env.adminClient.listTopics()
            waitForCondition(
                testCondition = {
                    time.sleep(1)
                    disconnectFuture.isDone
                },
                maxWaitMs = 5000,
                pollIntervalMs = 1,
                conditionDetailsSupplier = { "Timed out waiting for expected disconnect" },
            )
            assertFalse(disconnectFuture.isCompletedExceptionally())
            assertFalse(result.future.isDone())
            waitForCondition(
                testCondition = env.mockClient::hasInFlightRequests,
                conditionDetails = "Timed out waiting for retry",
            )
            env.mockClient.respond(prepareMetadataResponse(cluster, Errors.NONE))
            assertEquals(expected = 0, actual = result.listings.get().size)
        }
    }

    @Test
    @Throws(Exception::class)
    fun testFenceProducers() {
        mockClientEnv().use { env ->
            val transactionalId = "copyCat"
            val transactionCoordinator = env.cluster.nodes.first()

            // fail to find the coordinator at first with a retriable error
            env.mockClient.prepareResponse(
                prepareFindCoordinatorResponse(
                    error = Errors.COORDINATOR_NOT_AVAILABLE,
                    key = transactionalId,
                    node = transactionCoordinator,
                )
            )
            // and then succeed in the attempt to find the transaction coordinator
            env.mockClient.prepareResponse(
                prepareFindCoordinatorResponse(
                    error = Errors.NONE,
                    key = transactionalId,
                    node = transactionCoordinator,
                )
            )
            // unfortunately, a coordinator load is in progress and we need to retry our init PID request
            env.mockClient.prepareResponseFrom(
                matcher = { request -> request is InitProducerIdRequest },
                response = InitProducerIdResponse(
                    InitProducerIdResponseData().setErrorCode(Errors.COORDINATOR_LOAD_IN_PROGRESS.code)
                ),
                node = transactionCoordinator,
            )
            // then find out that the coordinator has changed since then
            env.mockClient.prepareResponseFrom(
                matcher = { request -> request is InitProducerIdRequest },
                response = InitProducerIdResponse(
                    InitProducerIdResponseData().setErrorCode(Errors.NOT_COORDINATOR.code)
                ),
                node = transactionCoordinator,
            )
            // and as a result, try once more to locate the coordinator (this time succeeding on the first try)
            env.mockClient.prepareResponse(
                prepareFindCoordinatorResponse(
                    error = Errors.NONE,
                    key = transactionalId,
                    node = transactionCoordinator,
                )
            )
            // and finally, complete the init PID request
            val initProducerIdResponseData = InitProducerIdResponseData()
                .setProducerId(4761)
                .setProducerEpoch(489.toShort())
            env.mockClient.prepareResponseFrom(
                matcher = { request -> request is InitProducerIdRequest },
                response = InitProducerIdResponse(initProducerIdResponseData),
                node = transactionCoordinator,
            )
            val result = env.adminClient.fenceProducers(setOf(transactionalId))
            assertNotFails(result.all()::get)
            assertEquals(
                expected = 4761,
                actual = result.producerId(transactionalId).get(),
            )
            assertEquals(
                expected = 489.toShort(),
                actual = result.epochId(transactionalId).get(),
            )
        }
    }

    @Suppress("SameParameterValue")
    private fun prepareUnregisterBrokerResponse(
        error: Errors,
        throttleTimeMs: Int,
    ): UnregisterBrokerResponse {
        return UnregisterBrokerResponse(
            UnregisterBrokerResponseData()
                .setErrorCode(error.code)
                .setErrorMessage(error.message)
                .setThrottleTimeMs(throttleTimeMs)
        )
    }

    private fun prepareDescribeLogDirsResponse(
        error: Errors,
        logDir: String,
    ): DescribeLogDirsResponse {
        return DescribeLogDirsResponse(
            DescribeLogDirsResponseData().setResults(
                listOf(
                    DescribeLogDirsResponseData.DescribeLogDirsResult()
                        .setErrorCode(error.code)
                        .setLogDir(logDir)
                )
            )
        )
    }

    private fun offsetFetchResponse(
        error: Errors,
        responseData: Map<TopicPartition, OffsetFetchResponse.PartitionData>,
    ): OffsetFetchResponse {
        return OffsetFetchResponse(
            throttleTimeMs = THROTTLE,
            errors = mapOf(GROUP_ID to error),
            responseData = mapOf(GROUP_ID to responseData),
        )
    }

    class FailureInjectingTimeoutProcessorFactory : TimeoutProcessorFactory() {

        private var numTries = 0

        private var failuresInjected = 0

        override fun create(now: Long): TimeoutProcessor {
            return FailureInjectingTimeoutProcessor(now)
        }

        @Synchronized
        fun shouldInjectFailure(): Boolean {
            numTries++
            if (numTries == 1) {
                failuresInjected++
                return true
            }
            return false
        }

        @Synchronized
        fun failuresInjected(): Int = failuresInjected

        inner class FailureInjectingTimeoutProcessor(now: Long) : TimeoutProcessor(now) {
            override fun callHasExpired(call: KafkaAdminClient.Call): Boolean {
                return if (!call.internal && shouldInjectFailure()) {
                    log.debug("Injecting timeout for {}.", call)
                    true
                } else {
                    val ret = super.callHasExpired(call)
                    log.debug("callHasExpired({}) = {}", call, ret)
                    ret
                }
            }
        }
    }

    companion object {

        private val log = LoggerFactory.getLogger(KafkaAdminClientTest::class.java)

        private const val GROUP_ID = "group-0"

        private const val THROTTLE = 10

        private fun newStrMap(vararg vals: String): Map<String, Any?> {
            val map: MutableMap<String, Any?> = HashMap()
            map[AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG] = "localhost:8121"
            map[AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG] = "1000"
            check(vals.size % 2 == 0)
            var i = 0
            while (i < vals.size) {
                map[vals[i]] = vals[i + 1]
                i += 2
            }
            return map
        }

        private fun newConfMap(vararg vals: String): AdminClientConfig {
            return AdminClientConfig(newStrMap(*vals))
        }

        @Suppress("SameParameterValue")
        private fun mockCluster(numNodes: Int, controllerIndex: Int): Cluster {
            val nodes = HashMap<Int, Node>()
            for (i in 0 until numNodes) nodes[i] = Node(i, "localhost", 8121 + i)
            return Cluster(
                clusterId = "mockClusterId",
                nodes = nodes.values,
                controller = nodes[controllerIndex],
            )
        }

        private fun mockBootstrapCluster(): Cluster {
            return Cluster.bootstrap(
                parseAndValidateAddresses(
                    urls = listOf("localhost:8121"),
                    clientDnsLookup = ClientDnsLookup.USE_ALL_DNS_IPS,
                )
            )
        }

        private fun mockClientEnv(vararg configVals: String): AdminClientUnitTestEnv {
            return AdminClientUnitTestEnv(
                mockCluster(numNodes = 3, controllerIndex = 0),
                *configVals,
            )
        }

        private fun mockClientEnv(time: Time, vararg configVals: String): AdminClientUnitTestEnv {
            return AdminClientUnitTestEnv(
                time,
                mockCluster(numNodes = 3, controllerIndex = 0),
                *configVals,
            )
        }

        private fun prepareOffsetDeleteResponse(error: Errors): OffsetDeleteResponse {
            return OffsetDeleteResponse(
                OffsetDeleteResponseData()
                    .setErrorCode(error.code)
                    .setTopics(OffsetDeleteResponseTopicCollection())
            )
        }

        @Suppress("SameParameterValue")
        private fun prepareOffsetDeleteResponse(
            topic: String,
            partition: Int,
            error: Errors,
        ): OffsetDeleteResponse {
            return OffsetDeleteResponse(
                OffsetDeleteResponseData()
                    .setErrorCode(Errors.NONE.code)
                    .setTopics(
                        OffsetDeleteResponseTopicCollection(
                            listOf(
                                OffsetDeleteResponseTopic()
                                    .setName(topic)
                                    .setPartitions(
                                        OffsetDeleteResponsePartitionCollection(
                                            listOf(
                                                OffsetDeleteResponsePartition()
                                                    .setPartitionIndex(partition)
                                                    .setErrorCode(error.code)
                                            ).iterator()
                                        )
                                    )
                            ).iterator()
                        )
                    )
            )
        }

        private fun prepareOffsetCommitResponse(
            tp: TopicPartition,
            error: Errors,
        ): OffsetCommitResponse {
            val responseData: MutableMap<TopicPartition, Errors> = HashMap()
            responseData[tp] = error
            return OffsetCommitResponse(
                requestThrottleMs = 0,
                responseData = responseData,
            )
        }

        @Suppress("SameParameterValue")
        private fun prepareCreateTopicsResponse(
            topicName: String,
            error: Errors,
        ): CreateTopicsResponse {
            val data = CreateTopicsResponseData()
            data.topics += CreatableTopicResult()
                .setName(topicName)
                .setErrorCode(error.code)
            return CreateTopicsResponse(data)
        }

        fun prepareCreateTopicsResponse(
            throttleTimeMs: Int,
            vararg topics: CreatableTopicResult,
        ): CreateTopicsResponse {
            val data = CreateTopicsResponseData()
                .setThrottleTimeMs(throttleTimeMs)
                .setTopics(CreatableTopicResultCollection(topics.iterator()))
            return CreateTopicsResponse(data)
        }

        fun creatableTopicResult(name: String, error: Errors): CreatableTopicResult {
            return CreatableTopicResult()
                .setName(name)
                .setErrorCode(error.code)
        }

        fun prepareDeleteTopicsResponse(
            throttleTimeMs: Int,
            vararg topics: DeletableTopicResult,
        ): DeleteTopicsResponse {
            val data = DeleteTopicsResponseData()
                .setThrottleTimeMs(throttleTimeMs)
                .setResponses(DeletableTopicResultCollection(topics.iterator()))
            return DeleteTopicsResponse(data)
        }

        fun deletableTopicResult(topicName: String?, error: Errors): DeletableTopicResult {
            return DeletableTopicResult()
                .setName(topicName)
                .setErrorCode(error.code)
        }

        fun deletableTopicResultWithId(topicId: Uuid?, error: Errors): DeletableTopicResult {
            return DeletableTopicResult()
                .setTopicId(topicId!!)
                .setErrorCode(error.code)
        }

        fun prepareCreatePartitionsResponse(
            throttleTimeMs: Int,
            vararg topics: CreatePartitionsTopicResult,
        ): CreatePartitionsResponse {
            val data = CreatePartitionsResponseData()
                .setThrottleTimeMs(throttleTimeMs)
                .setResults(topics.toList())
            return CreatePartitionsResponse(data)
        }

        @JvmOverloads
        fun createPartitionsTopicResult(
            name: String,
            error: Errors,
            errorMessage: String? = null,
        ): CreatePartitionsTopicResult {
            return CreatePartitionsTopicResult()
                .setName(name)
                .setErrorCode(error.code)
                .setErrorMessage(errorMessage)
        }

        @Suppress("SameParameterValue")
        private fun prepareDeleteTopicsResponse(
            topicName: String,
            error: Errors,
        ): DeleteTopicsResponse {
            val data = DeleteTopicsResponseData()
            data.responses += DeletableTopicResult()
                .setName(topicName)
                .setErrorCode(error.code)
            return DeleteTopicsResponse(data)
        }

        private fun prepareDeleteTopicsResponseWithTopicId(
            id: Uuid,
            error: Errors,
        ): DeleteTopicsResponse {
            val data = DeleteTopicsResponseData()
            data.responses += DeletableTopicResult()
                .setTopicId(id)
                .setErrorCode(error.code)
            return DeleteTopicsResponse(data)
        }

        private fun prepareFindCoordinatorResponse(
            error: Errors,
            node: Node,
        ): FindCoordinatorResponse {
            return prepareFindCoordinatorResponse(error = error, key = GROUP_ID, node = node)
        }

        private fun prepareFindCoordinatorResponse(
            error: Errors,
            key: String,
            node: Node,
        ): FindCoordinatorResponse {
            return FindCoordinatorResponse.prepareResponse(error = error, key = key, node = node)
        }

        private fun prepareOldFindCoordinatorResponse(
            error: Errors,
            node: Node,
        ): FindCoordinatorResponse {
            return FindCoordinatorResponse.prepareOldResponse(error, node)
        }

        @Suppress("SameParameterValue")
        private fun prepareBatchedFindCoordinatorResponse(
            error: Errors,
            node: Node?,
            groups: Collection<String>,
        ): FindCoordinatorResponse {
            val data = FindCoordinatorResponseData()
            val coordinators = groups.map { group ->
                FindCoordinatorResponseData.Coordinator()
                    .setErrorCode(error.code)
                    .setErrorMessage(error.message)
                    .setKey(group)
                    .setHost(node!!.host)
                    .setPort(node.port)
                    .setNodeId(node.id)
            }
            data.setCoordinators(coordinators)
            return FindCoordinatorResponse(data)
        }

        private fun prepareMetadataResponse(cluster: Cluster, error: Errors): MetadataResponse {
            val metadata: MutableList<MetadataResponseTopic> = ArrayList()
            for (topic in cluster.topics) {
                val pms: MutableList<MetadataResponsePartition> = ArrayList()
                for (p in cluster.availablePartitionsForTopic(topic)) with(p) {
                    val pm = MetadataResponsePartition()
                        .setErrorCode(error.code)
                        .setPartitionIndex(partition)
                        .setLeaderId(leader!!.id)
                        .setLeaderEpoch(234)
                        .setReplicaNodes(replicas.map { obj -> obj.id }.toIntArray())
                        .setIsrNodes(inSyncReplicas.map { obj -> obj.id }.toIntArray())
                        .setOfflineReplicas(offlineReplicas.map { obj -> obj.id }.toIntArray())
                    pms.add(pm)
                }
                val tm = MetadataResponseTopic()
                    .setErrorCode(error.code)
                    .setName(topic)
                    .setIsInternal(false)
                    .setPartitions(pms)
                metadata.add(tm)
            }
            return MetadataResponse.prepareResponse(
                hasReliableEpoch = true,
                throttleTimeMs = 0,
                brokers = cluster.nodes,
                clusterId = cluster.clusterResource.clusterId,
                controllerId = cluster.controller!!.id,
                topics = metadata,
                clusterAuthorizedOperations = MetadataResponse.AUTHORIZED_OPERATIONS_OMITTED,
            )
        }

        @Suppress("SameParameterValue")
        private fun prepareDescribeGroupsResponseData(
            groupId: String,
            groupInstances: List<String>,
            topicPartitions: List<TopicPartition>,
        ): DescribeGroupsResponseData {
            val memberAssignment =
                serializeAssignment(ConsumerPartitionAssignor.Assignment(topicPartitions))
            val describedGroupMembers = groupInstances.map { groupInstance ->
                DescribeGroupsResponse.groupMember(
                    memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID,
                    groupInstanceId = groupInstance,
                    clientId = "clientId0",
                    clientHost = "clientHost",
                    assignment = ByteArray(memberAssignment.remaining()),
                    metadata = Bytes.EMPTY,
                )
            }
            val data = DescribeGroupsResponseData()
            data.groups += DescribeGroupsResponse.groupMetadata(
                groupId = groupId,
                error = Errors.NONE,
                state = "",
                protocolType = ConsumerProtocol.PROTOCOL_TYPE,
                protocol = "",
                members = describedGroupMembers,
                authorizedOperations = emptySet(),
            )
            return data
        }

        private fun defaultFeatureMetadata(): FeatureMetadata {
            return FeatureMetadata(
                finalizedFeatures = mapOf(
                    "test_feature_1" to FinalizedVersionRange(
                        minVersionLevel = 2,
                        maxVersionLevel = 2,
                    ),
                ),
                finalizedFeaturesEpoch = 1L,
                supportedFeatures = mapOf(
                    "test_feature_1" to SupportedVersionRange(
                        minVersion = 1,
                        maxVersion = 5,
                    ),
                ),
            )
        }

        private fun convertSupportedFeaturesMap(
            features: Map<String, SupportedVersionRange>,
        ): Features<org.apache.kafka.common.feature.SupportedVersionRange> {
            val featuresMap: MutableMap<String, org.apache.kafka.common.feature.SupportedVersionRange> =
                HashMap()
            for ((key, versionRange) in features) {
                featuresMap[key] = org.apache.kafka.common.feature.SupportedVersionRange(
                    minVersion = versionRange.minVersion,
                    maxVersion = versionRange.maxVersion,
                )
            }
            return Features.supportedFeatures(featuresMap)
        }

        private fun prepareApiVersionsResponseForDescribeFeatures(error: Errors): ApiVersionsResponse {
            return if (error === Errors.NONE) {
                ApiVersionsResponse.createApiVersionsResponse(
                    throttleTimeMs = 0,
                    apiVersions = ApiVersionsResponse.filterApis(
                        minRecordVersion = RecordVersion.current(),
                        listenerType = ApiMessageType.ListenerType.ZK_BROKER,
                    ),
                    latestSupportedFeatures = convertSupportedFeaturesMap(
                        defaultFeatureMetadata().supportedFeatures(),
                    ),
                    finalizedFeatures = mapOf("test_feature_1" to 2.toShort()),
                    finalizedFeaturesEpoch = defaultFeatureMetadata().finalizedFeaturesEpoch!!,
                )
            } else ApiVersionsResponse(
                ApiVersionsResponseData()
                    .setThrottleTimeMs(0)
                    .setErrorCode(error.code)
            )
        }

        private fun defaultQuorumInfo(emptyOptionals: Boolean): QuorumInfo {
            return QuorumInfo(
                leaderId = 1,
                leaderEpoch = 1,
                highWatermark = 1L,
                voters = listOf(
                    QuorumInfo.ReplicaState(
                        replicaId = 1,
                        logEndOffset = 100,
                        lastFetchTimestamp = if (emptyOptionals) null else 1000,
                        lastCaughtUpTimestamp = if (emptyOptionals) null else 1000,
                    )
                ),
                observers = listOf(
                    QuorumInfo.ReplicaState(
                        replicaId = 1,
                        logEndOffset = 100,
                        lastFetchTimestamp = if (emptyOptionals) null else 1000,
                        lastCaughtUpTimestamp = if (emptyOptionals) null else 1000,
                    )
                ),
            )
        }

        private fun prepareDescribeQuorumResponse(
            topLevelError: Errors,
            partitionLevelError: Errors,
            topicCountError: Boolean,
            topicNameError: Boolean,
            partitionCountError: Boolean,
            partitionIndexError: Boolean,
            emptyOptionals: Boolean,
        ): DescribeQuorumResponse {
            val topicName = if (topicNameError) "RANDOM" else Topic.CLUSTER_METADATA_TOPIC_NAME
            val partitionIndex =
                if (partitionIndexError) 1
                else Topic.CLUSTER_METADATA_TOPIC_PARTITION.partition
            val topics: MutableList<DescribeQuorumResponseData.TopicData> = ArrayList()
            val partitions: MutableList<DescribeQuorumResponseData.PartitionData> = ArrayList()
            for (i in 0 until if (partitionCountError) 2 else 1) {
                val replica = DescribeQuorumResponseData.ReplicaState()
                    .setReplicaId(1)
                    .setLogEndOffset(100)
                replica.setLastFetchTimestamp(if (emptyOptionals) -1 else 1000)
                replica.setLastCaughtUpTimestamp(if (emptyOptionals) -1 else 1000)
                partitions.add(
                    DescribeQuorumResponseData.PartitionData().setPartitionIndex(partitionIndex)
                        .setLeaderId(1)
                        .setLeaderEpoch(1)
                        .setHighWatermark(1)
                        .setCurrentVoters(listOf(replica))
                        .setObservers(listOf(replica))
                        .setErrorCode(partitionLevelError.code)
                )
            }
            for (i in 0 until if (topicCountError) 2 else 1) {
                topics.add(
                    DescribeQuorumResponseData.TopicData()
                        .setTopicName(topicName)
                        .setPartitions(partitions)
                )
            }
            return DescribeQuorumResponse(
                DescribeQuorumResponseData()
                    .setTopics(topics)
                    .setErrorCode(topLevelError.code)
            )
        }

        private val ACL1 = AclBinding(
            ResourcePattern(
                resourceType = ResourceType.TOPIC,
                name = "mytopic3",
                patternType = PatternType.LITERAL,
            ),
            AccessControlEntry(
                principal = "User:ANONYMOUS",
                host = "*",
                operation = AclOperation.DESCRIBE,
                permissionType = AclPermissionType.ALLOW,
            )
        )
        private val ACL2 = AclBinding(
            ResourcePattern(
                resourceType = ResourceType.TOPIC,
                name = "mytopic4",
                patternType = PatternType.LITERAL,
            ),
            AccessControlEntry(
                principal = "User:ANONYMOUS",
                host = "*",
                operation = AclOperation.DESCRIBE,
                permissionType = AclPermissionType.DENY,
            )
        )
        private val FILTER1 = AclBindingFilter(
            ResourcePatternFilter(
                resourceType = ResourceType.ANY,
                name = null,
                patternType = PatternType.LITERAL,
            ),
            AccessControlEntryFilter(
                principal = "User:ANONYMOUS",
                host = null,
                operation = AclOperation.ANY,
                permissionType = AclPermissionType.ANY,
            )
        )
        private val FILTER2 = AclBindingFilter(
            ResourcePatternFilter(
                resourceType = ResourceType.ANY,
                name = null,
                patternType = PatternType.LITERAL,
            ),
            AccessControlEntryFilter(
                principal = "User:bob",
                host = null,
                operation = AclOperation.ANY,
                permissionType = AclPermissionType.ANY,
            )
        )
        private val UNKNOWN_FILTER = AclBindingFilter(
            ResourcePatternFilter(
                resourceType = ResourceType.UNKNOWN,
                name = null,
                patternType = PatternType.LITERAL,
            ),
            AccessControlEntryFilter(
                principal = "User:bob",
                host = null,
                operation = AclOperation.ANY,
                permissionType = AclPermissionType.ANY,
            )
        )

        @Suppress("SameParameterValue")
        private fun prepareDescribeLogDirsResponse(
            error: Errors,
            logDir: String,
            tp: TopicPartition,
            partitionSize: Long,
            offsetLag: Long,
        ): DescribeLogDirsResponse {
            return prepareDescribeLogDirsResponse(
                error = error,
                logDir = logDir,
                topics = prepareDescribeLogDirsTopics(
                    partitionSize = partitionSize,
                    offsetLag = offsetLag,
                    topic = tp.topic,
                    partition = tp.partition,
                    isFuture = false,
                )
            )
        }

        @Suppress("SameParameterValue")
        private fun prepareDescribeLogDirsResponse(
            error: Errors,
            logDir: String,
            tp: TopicPartition,
            partitionSize: Long,
            offsetLag: Long,
            totalBytes: Long,
            usableBytes: Long,
        ): DescribeLogDirsResponse {
            return prepareDescribeLogDirsResponse(
                error = error,
                logDir = logDir,
                topics = prepareDescribeLogDirsTopics(
                    partitionSize = partitionSize,
                    offsetLag = offsetLag,
                    topic = tp.topic,
                    partition = tp.partition,
                    isFuture = false,
                ),
                totalBytes = totalBytes,
                usableBytes = usableBytes,
            )
        }

        private fun prepareDescribeLogDirsTopics(
            partitionSize: Long,
            offsetLag: Long,
            topic: String,
            partition: Int,
            isFuture: Boolean,
        ): List<DescribeLogDirsTopic> {
            return listOf(
                DescribeLogDirsTopic()
                    .setName(topic)
                    .setPartitions(
                        listOf(
                            DescribeLogDirsPartition()
                                .setPartitionIndex(partition)
                                .setPartitionSize(partitionSize)
                                .setIsFutureKey(isFuture)
                                .setOffsetLag(offsetLag)
                        )
                    )
            )
        }

        private fun prepareDescribeLogDirsResponse(
            error: Errors, logDir: String,
            topics: List<DescribeLogDirsTopic>,
        ): DescribeLogDirsResponse {
            return DescribeLogDirsResponse(
                DescribeLogDirsResponseData().setResults(
                    listOf(
                        DescribeLogDirsResponseData.DescribeLogDirsResult()
                            .setErrorCode(error.code)
                            .setLogDir(logDir)
                            .setTopics(topics)
                    )
                )
            )
        }

        private fun prepareDescribeLogDirsResponse(
            error: Errors,
            logDir: String,
            topics: List<DescribeLogDirsTopic>,
            totalBytes: Long,
            usableBytes: Long,
        ): DescribeLogDirsResponse {
            return DescribeLogDirsResponse(
                DescribeLogDirsResponseData().setResults(
                    listOf(
                        DescribeLogDirsResponseData.DescribeLogDirsResult()
                            .setErrorCode(error.code)
                            .setLogDir(logDir)
                            .setTopics(topics)
                            .setTotalBytes(totalBytes)
                            .setUsableBytes(usableBytes)
                    )
                )
            )
        }

        private fun prepareEmptyDescribeLogDirsResponse(error: Errors?): DescribeLogDirsResponse {
            val data = DescribeLogDirsResponseData()
            if (error != null) data.setErrorCode(error.code)
            return DescribeLogDirsResponse(data)
        }

        private fun assertDescriptionContains(
            descriptionsMap: Map<String, LogDirDescription>,
            logDir: String,
            tp: TopicPartition,
            partitionSize: Long,
            offsetLag: Long,
            totalBytes: Long? = null,
            usableBytes: Long? = null,
        ) {
            assertNotNull(descriptionsMap)
            assertEquals(setOf(logDir), descriptionsMap.keys)
            assertNull(descriptionsMap[logDir]!!.error())
            val descriptionsReplicaInfos = descriptionsMap[logDir]!!.replicaInfos()
            assertEquals(setOf(tp), descriptionsReplicaInfos.keys)
            assertEquals(
                expected = partitionSize,
                actual = descriptionsReplicaInfos[tp]!!.size,
            )
            assertEquals(
                expected = offsetLag,
                actual = descriptionsReplicaInfos[tp]!!.offsetLag,
            )
            assertFalse(descriptionsReplicaInfos[tp]!!.isFuture)
            assertEquals(
                expected = totalBytes,
                actual = descriptionsMap[logDir]!!.totalBytes,
            )
            assertEquals(
                expected = usableBytes,
                actual = descriptionsMap[logDir]!!.usableBytes,
            )
        }

        @Suppress("Deprecation", "SameParameterValue")
        private fun assertDescriptionContains(
            descriptionsMap: Map<String, LogDirInfo>,
            logDir: String,
            tp: TopicPartition,
            error: Errors,
            offsetLag: Int,
            partitionSize: Long,
        ) {
            assertNotNull(descriptionsMap)
            assertEquals(expected = setOf(logDir), actual = descriptionsMap.keys)
            assertEquals(expected = error, actual = descriptionsMap[logDir]!!.error)
            val allReplicaInfos = descriptionsMap[logDir]!!.replicaInfos
            assertEquals(expected = setOf(tp), actual = allReplicaInfos.keys)
            assertEquals(expected = partitionSize, actual = allReplicaInfos[tp]!!.size)
            assertEquals(expected = offsetLag.toLong(), actual = allReplicaInfos[tp]!!.offsetLag)
            assertFalse(allReplicaInfos[tp]!!.isFuture)
        }

        private fun prepareDescribeLogDirsResult(
            tpr: TopicPartitionReplica,
            logDir: String,
            partitionSize: Int,
            offsetLag: Int,
            isFuture: Boolean,
        ): DescribeLogDirsResponseData.DescribeLogDirsResult {
            return DescribeLogDirsResponseData.DescribeLogDirsResult()
                .setErrorCode(Errors.NONE.code)
                .setLogDir(logDir)
                .setTopics(
                    prepareDescribeLogDirsTopics(
                        partitionSize = partitionSize.toLong(),
                        offsetLag = offsetLag.toLong(),
                        topic = tpr.topic,
                        partition = tpr.partition,
                        isFuture = isFuture,
                    )
                )
        }

        @Suppress("SameParameterValue")
        private fun prepareDescribeClusterResponse(
            throttleTimeMs: Int,
            brokers: Collection<Node?>,
            clusterId: String?,
            controllerId: Int,
            clusterAuthorizedOperations: Int,
        ): DescribeClusterResponse {
            val data = DescribeClusterResponseData()
                .setErrorCode(Errors.NONE.code)
                .setThrottleTimeMs(throttleTimeMs)
                .setControllerId(controllerId)
                .setClusterId(clusterId!!)
                .setClusterAuthorizedOperations(clusterAuthorizedOperations)
            brokers.forEach { broker ->
                data.brokers.add(
                    DescribeClusterBroker()
                        .setHost(broker!!.host)
                        .setPort(broker.port)
                        .setBrokerId(broker.id)
                        .setRack(broker.rack)
                )
            }
            return DescribeClusterResponse(data)
        }

        private fun convertToMemberDescriptions(
            member: DescribedGroupMember,
            assignment: MemberAssignment,
        ): MemberDescription {
            return MemberDescription(
                memberId = member.memberId,
                groupInstanceId = member.groupInstanceId,
                clientId = member.clientId,
                host = member.clientHost,
                assignment = assignment,
            )
        }

        @Suppress("SameParameterValue")
        @SafeVarargs
        private fun <T> assertCollectionIs(collection: Collection<T>, vararg elements: T) {
            for (element in elements) {
                assertContains(
                    iterable = collection,
                    element = element,
                    message = "Did not find $element",
                )
            }
            assertEquals(
                expected = elements.size,
                actual = collection.size,
                message = "There are unexpected extra elements in the collection.",
            )
        }

        fun createInternal(
            config: AdminClientConfig,
            timeoutProcessorFactory: TimeoutProcessorFactory,
        ): KafkaAdminClient {
            return KafkaAdminClient.createInternal(config, timeoutProcessorFactory)
        }
    }
}
